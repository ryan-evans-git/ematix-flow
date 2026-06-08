//! PV.M.5 Phase-0 §3.1 — scan-decode BALANCE probe (the cheapest kill).
//!
//! Extracts the lineitem `EmatixFastParquetExec` from the Q15 SCALAR plan and
//! runs ALL its partitions CONCURRENTLY, recording each partition's FINISH time
//! relative to a shared start. The spread of finish times *is* the idle-core
//! tail:
//!   * finishes CLUSTERED (wall ≈ mean) → decode is already balanced → the
//!     1.4×-off-ideal is NOT straggling; it's per-byte decode / a tokio-can't-
//!     overlap property → RED-FLAG (a) → PV.M.5 likely NO-GO.
//!   * finishes SPREAD with a tail (wall ≫ mean) → stragglers strand cores →
//!     RED-FLAG (b) → the sub-RG work-stealing de-risk stays open → run §3.2.
//!
//! Non-invasive: no scan internals edited. Sweep PARTS to see whether finer
//! partitions actually balance (they were wall-flat in PV.M.4 — this shows WHY).
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 PARTS=14 \
//!     cargo run --release -p ematix-flow-core --example ws_scan_balance

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::{
    EmatixFastParquetExec, EmatixFastParquetTableProvider,
};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use futures_util::StreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

const SCALAR_SQL: &str = "select sum(l_extendedprice * (1 - l_discount)) from lineitem \
     where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'";

fn build_ctx(data_dir: &Path, parts: usize) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(parts))
        .with_default_features();
    let state = ematix_flow_core::preset::with_optimizer_rules(builder).build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let p = data_dir.join(format!("{t}.parquet"));
        if *t == "lineitem" || *t == "orders" {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    p.to_string_lossy(),
                )?),
            )?;
        } else {
            ctx.register_table(
                *t,
                Arc::new(FastParquetTableProvider::try_new(p.to_string_lossy())?),
            )?;
        }
    }
    Ok(ctx)
}

/// Recursively find the lineitem scan node.
fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan
        .as_any()
        .downcast_ref::<EmatixFastParquetExec>()
        .is_some()
    {
        return Some(plan.clone());
    }
    for c in plan.children() {
        if let Some(s) = find_scan(c) {
            return Some(s);
        }
    }
    None
}

fn pctl(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[i]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let parts = std::env::var("PARTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| std::thread::available_parallelism().map(|n| n.get()).ok())
        .unwrap_or(14);
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);

    let ctx = build_ctx(&data_dir, parts)?;
    let plan = ctx.sql(SCALAR_SQL).await?.create_physical_plan().await?;
    let scan = find_scan(&plan).ok_or("no EmatixFastParquetExec in plan")?;
    let nparts = scan.output_partitioning().partition_count();
    let tctx = ctx.task_ctx();

    println!(
        "PV.M.5 Phase-0 §3.1 scan balance — target_partitions={parts} cores={cores}\n  scan emits {nparts} partitions"
    );

    // Warmup: drain all partitions a few times (prime OS cache).
    for _ in 0..3 {
        let mut hs = Vec::new();
        for p in 0..nparts {
            let s = scan.clone();
            let tc = tctx.clone();
            hs.push(tokio::spawn(async move {
                let mut st = s.execute(p, tc).unwrap();
                let mut r = 0usize;
                while let Some(b) = st.next().await {
                    r += b.unwrap().num_rows();
                }
                r
            }));
        }
        for h in hs {
            let _ = h.await;
        }
    }

    // Timed: spawn ALL partitions concurrently; record each finish relative to
    // the shared start. Median over a few rounds for stability of the SHAPE.
    let rounds = 7;
    let mut walls = Vec::new();
    let mut means = Vec::new();
    let mut tails = Vec::new();
    let mut last_finishes: Vec<(usize, usize, f64)> = Vec::new();
    for round in 0..rounds {
        let start = Instant::now();
        let mut hs = Vec::new();
        for p in 0..nparts {
            let s = scan.clone();
            let tc = tctx.clone();
            hs.push(tokio::spawn(async move {
                let mut st = s.execute(p, tc).unwrap();
                let mut rows = 0usize;
                while let Some(b) = st.next().await {
                    rows += b.unwrap().num_rows();
                }
                (p, rows, start.elapsed().as_secs_f64() * 1e3)
            }));
        }
        let mut fin: Vec<(usize, usize, f64)> = Vec::new();
        for h in hs {
            fin.push(h.await.unwrap());
        }
        let finish_ms: Vec<f64> = fin.iter().map(|x| x.2).collect();
        let wall = finish_ms.iter().cloned().fold(0.0, f64::max);
        let mean = finish_ms.iter().sum::<f64>() / finish_ms.len() as f64;
        walls.push(wall);
        means.push(mean);
        tails.push(wall - mean);
        if round == rounds - 1 {
            last_finishes = fin;
        }
    }
    walls.sort_by(|a, b| a.partial_cmp(b).unwrap());
    means.sort_by(|a, b| a.partial_cmp(b).unwrap());
    tails.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let wall = walls[walls.len() / 2];
    let mean = means[means.len() / 2];
    let tail = tails[tails.len() / 2];

    // Finish-time distribution of the last round.
    last_finishes.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap());
    let fins: Vec<f64> = last_finishes.iter().map(|x| x.2).collect();
    println!("\nconcurrent finish times (ms, last round, sorted):");
    println!(
        "  min {:.1}  p25 {:.1}  p50 {:.1}  p75 {:.1}  p90 {:.1}  max {:.1}",
        pctl(&fins, 0.0),
        pctl(&fins, 0.25),
        pctl(&fins, 0.5),
        pctl(&fins, 0.75),
        pctl(&fins, 0.9),
        pctl(&fins, 1.0),
    );
    let total_rows: usize = last_finishes.iter().map(|x| x.1).sum();
    println!(
        "  total survivor rows={total_rows}  (≈{:.1}% of 60M)",
        100.0 * total_rows as f64 / 59_986_052.0
    );

    println!("\n=== balance summary (median over {rounds} rounds) ===");
    println!("wall (max finish)   = {wall:.1} ms   ← concurrent decode wall");
    println!("mean finish         = {mean:.1} ms   ← balanced-ideal proxy");
    println!(
        "tail waste (wall−mean) = {tail:.1} ms   = idle-core time in the straggler tail ({:.0}% of wall)",
        100.0 * tail / wall.max(0.001)
    );
    let verdict = if tail / wall.max(0.001) >= 0.20 {
        "RED-FLAG (b): real straggler tail → cores idle waiting → sub-RG work-stealing de-risk STAYS OPEN → run §3.2 spike"
    } else {
        "RED-FLAG (a): decode already BALANCED (tight finishes) → the 1.4×-off-ideal is NOT straggling → likely NO-GO (gap is per-byte decode / tokio overlap). Confirm with §3.2 before closing."
    };
    println!("VERDICT: {verdict}");
    Ok(())
}
