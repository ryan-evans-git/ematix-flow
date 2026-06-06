//! POL.0 Phase-2 gate — 22q SF=10 serial-vs-parallel CSE-drain A/B.
//!
//! Q15 won −12.6% from concurrent CSE-cache populate (q15_cse_parallel_ab).
//! Before flipping `EMAT_CSE_PARALLEL` default-on, confirm the OTHER
//! SharedSubtreeExec consumers — Σ.Z generalised the dedupe/CSE to the
//! non-aggregate subtrees in Q11/Q17/Q21 — don't regress from per-partition
//! spawn overhead, and that the flag is INERT on the ~18 queries with no CSE.
//!
//! Two arms, interleaved, FRESH CTX PER TRIAL (no Σ.P replay):
//!   serial   — control                      (no env)
//!   parallel — concurrent drain             (EMAT_CSE_PARALLEL=1)
//!
//! Per query: median wall, delta%, whether the parallel populate path
//! actually fired (CSE_PARALLEL_POPULATES delta), populate count, and a
//! serial-vs-parallel checksum guard. Reports geomean over CSE queries +
//! regressions. A query with 0 populates is the flag being inert (expected
//! for non-CSE queries) and is excluded from the kernel geomean.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example cse_parallel_22q_ab

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::shared_subtree_exec::{CSE_PARALLEL_POPULATES, CSE_POPULATES};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn set_parallel(on: bool) {
    // Explicit "0" for the off arm — the populate default is now ON, so
    // remove_var would leave the parallel path enabled.
    unsafe {
        std::env::set_var("EMAT_CSE_PARALLEL", if on { "1" } else { "0" });
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

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

async fn run_once(
    data_dir: &Path,
    parts: usize,
    sql: &str,
) -> Result<(f64, f64, usize), Box<dyn std::error::Error>> {
    let ctx = build_ctx(data_dir, parts)?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let t = Instant::now();
    let batches = collect(plan, ctx.task_ctx()).await?;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let mut sum = 0.0f64;
    let mut rows = 0usize;
    for b in &batches {
        rows += b.num_rows();
        for c in b.columns() {
            if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                for v in a.values() {
                    sum += *v;
                }
            }
        }
    }
    Ok((ms, sum, rows))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let queries_dir = PathBuf::from("examples/tpch/queries");
    let parts = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);
    let warmups = 2;
    let trials = 7;

    println!(
        "POL.0 Phase-2 — 22q SF=10 serial-vs-parallel CSE A/B — data={} parts={parts} trials={trials}\n",
        data_dir.display()
    );
    println!(
        "{:>4}  {:>10}  {:>10}  {:>8}  {:>6}  {:>4}",
        "Q", "serial_ms", "par_ms", "delta%", "ppop", "ok"
    );

    let mut log_ratio_sum = 0.0f64;
    let mut counted = 0usize;
    let mut regressions: Vec<(u8, f64)> = Vec::new();
    let mut wins: Vec<(u8, f64)> = Vec::new();
    let mut mismatches: Vec<u8> = Vec::new();

    for q in 1..=22u8 {
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let sql = match std::fs::read_to_string(&sql_path) {
            Ok(s) => s,
            Err(_) => continue,
        };

        for _ in 0..warmups {
            set_parallel(false);
            let _ = run_once(&data_dir, parts, &sql).await;
            set_parallel(true);
            let _ = run_once(&data_dir, parts, &sql).await;
        }

        let mut ser = Vec::with_capacity(trials);
        let mut par = Vec::with_capacity(trials);
        let (mut cs_s, mut rows_s) = (0.0f64, 0usize);
        let (mut cs_p, mut rows_p) = (0.0f64, 0usize);
        let mut parallel_populates = 0u64;
        let mut errored: Option<String> = None;
        for i in 0..trials {
            set_parallel(false);
            match run_once(&data_dir, parts, &sql).await {
                Ok((ms, sum, rows)) => {
                    ser.push(ms);
                    if i == 0 {
                        cs_s = sum;
                        rows_s = rows;
                    }
                }
                Err(e) => {
                    errored = Some(format!("serial: {e}"));
                    break;
                }
            }
            set_parallel(true);
            let ppop_before = CSE_PARALLEL_POPULATES.load(Ordering::Relaxed);
            let _ = CSE_POPULATES.load(Ordering::Relaxed);
            match run_once(&data_dir, parts, &sql).await {
                Ok((ms, sum, rows)) => {
                    par.push(ms);
                    parallel_populates +=
                        CSE_PARALLEL_POPULATES.load(Ordering::Relaxed) - ppop_before;
                    if i == 0 {
                        cs_p = sum;
                        rows_p = rows;
                    }
                }
                Err(e) => {
                    errored = Some(format!("parallel: {e}"));
                    break;
                }
            }
        }
        set_parallel(false);

        if let Some(e) = errored {
            println!("Q{q:02}: ERROR — {e}");
            mismatches.push(q);
            continue;
        }

        let s = median(ser);
        let p = median(par);
        let delta = (p - s) / s * 100.0;
        let ok = (cs_s - cs_p).abs() / cs_s.abs().max(1.0) <= 1e-9 && rows_s == rows_p;
        if !ok {
            mismatches.push(q);
        }
        // Only queries that actually exercised the parallel path count
        // toward the geomean; the rest are the flag being (correctly) inert.
        if ok && parallel_populates > 0 {
            log_ratio_sum += (p / s).ln();
            counted += 1;
            if delta > 3.0 {
                regressions.push((q, delta));
            } else if delta < -3.0 {
                wins.push((q, delta));
            }
        }
        println!(
            "Q{q:02}  {s:>10.1}  {p:>10.1}  {delta:>+7.1}  {parallel_populates:>6}  {:>4}",
            if ok { "ok" } else { "DIFF" }
        );
    }

    let geomean = if counted > 0 {
        (log_ratio_sum / counted as f64).exp()
    } else {
        1.0
    };
    println!("\n=== POL.0 Phase-2 22q summary ===");
    println!(
        "parallel/serial geomean (the {counted} CSE queries that fired) = {geomean:.4}  ({:+.1}%)",
        (geomean - 1.0) * 100.0
    );
    println!(
        "wins (≤−3%): {}",
        if wins.is_empty() {
            "none".to_string()
        } else {
            wins.iter()
                .map(|(q, d)| format!("Q{q:02}({d:+.0}%)"))
                .collect::<Vec<_>>()
                .join(" ")
        }
    );
    println!(
        "regressions (≥+3%): {}",
        if regressions.is_empty() {
            "NONE".to_string()
        } else {
            regressions
                .iter()
                .map(|(q, d)| format!("Q{q:02}({d:+.0}%)"))
                .collect::<Vec<_>>()
                .join(" ")
        }
    );
    if !mismatches.is_empty() {
        println!("⚠️ CORRECTNESS DIFF / ERROR on: {mismatches:?} — investigate before default-on");
    }
    let verdict = if !mismatches.is_empty() {
        "BLOCKED — correctness diff/error"
    } else if regressions.is_empty() {
        "GO — CSE queries net-neutral-or-win, no regressions, inert elsewhere → default-on candidate"
    } else {
        "MIXED — a CSE consumer regressed → gate on n_parts / subtree cost, not blanket default-on"
    };
    println!("VERDICT: {verdict}");
    Ok(())
}
