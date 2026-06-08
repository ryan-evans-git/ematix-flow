//! PV.M.8 Stage-4a — validate the `CombineAggExec` prize end-to-end on the Q15
//! groupby. Interleaved A/B (`EMAT_COMBINE_AGG` off vs on) at P=14 for:
//!   REVENUE  — `SELECT l_suppkey, SUM(extprice*(1-disc)) GROUP BY l_suppkey`
//!              (the plain two-phase agg — the rule fires here, no CSE)
//!   REVMAX   — + the max consumer
//!   FULL     — the whole Q15 (agg is CSE-wrapped → rule does NOT fire yet;
//!              shows the Phase-1b wiring gap, must stay correct + unchanged)
//!
//! Reports per arm: median ms, whether CombineAggExec fired (COMBINE_AGG_FIRES
//! delta), and a checksum (Σ of all f64 output cells) that MUST match off-vs-on.
//! GO if REVENUE-ON ≪ REVENUE-OFF (recovers the ~13ms two-phase groupby toward
//! the ~4.5ms kernel floor) with matching checksums.
//!
//! Usage: TRIALS=11 TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!   cargo run --release -p ematix-flow-core --example q15_combine_agg_ab

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::combine_agg_exec::{
    COMBINE_AGG_BUILD_NANOS, COMBINE_AGG_COMBINE_NANOS, COMBINE_AGG_FIRES, EnableCombineAggRule,
};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

const REVENUE: &str = "\
SELECT l_suppkey, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue \
FROM lineitem \
WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01' \
GROUP BY l_suppkey";

const REVMAX: &str = "\
WITH revenue (supplier_no, total_revenue) AS ( \
  SELECT l_suppkey, SUM(l_extendedprice * (1 - l_discount)) \
  FROM lineitem \
  WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01' \
  GROUP BY l_suppkey) \
SELECT max(total_revenue) FROM revenue";

const FULL: &str = "\
WITH revenue (supplier_no, total_revenue) AS ( \
  SELECT l_suppkey, SUM(l_extendedprice * (1 - l_discount)) \
  FROM lineitem \
  WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01' \
  GROUP BY l_suppkey) \
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue \
FROM supplier, revenue \
WHERE s_suppkey = supplier_no \
  AND total_revenue = (SELECT max(total_revenue) FROM revenue) \
ORDER BY s_suppkey";

fn set_combine(on: bool) {
    unsafe {
        std::env::set_var("EMAT_COMBINE_AGG", if on { "1" } else { "0" });
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn build_ctx(data_dir: &Path, parts: usize) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let cfg = SessionConfig::new().with_target_partitions(parts);
    let builder = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features();
    // Append the (self-env-gated) CombineAgg rule after the preset chain.
    let state = ematix_flow_core::preset::with_optimizer_rules(builder)
        .with_physical_optimizer_rule(Arc::new(EnableCombineAggRule))
        .build();
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

/// Checksum = Σ over every f64 cell of every output batch (order-independent,
/// catches a wrong aggregate). Returns (ms, rows, checksum).
async fn run_once(
    data_dir: &Path,
    parts: usize,
    sql: &str,
) -> Result<(f64, usize, f64), Box<dyn std::error::Error>> {
    let ctx = build_ctx(data_dir, parts)?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let t = Instant::now();
    let batches = collect(plan, ctx.task_ctx()).await?;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let mut rows = 0usize;
    let mut checksum = 0.0f64;
    for b in &batches {
        rows += b.num_rows();
        for c in b.columns() {
            if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        checksum += a.value(i);
                    }
                }
            }
        }
    }
    Ok((ms, rows, checksum))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(11);
    let rounds: usize = std::env::var("ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let parts = 14usize;
    let warmups = 3;

    // EXPLAIN ANALYZE diagnostic: dump per-operator metrics for REVENUE with
    // the rule ON, to see CombineAggExec's own timing + what surrounds it.
    if std::env::var_os("EXPLAIN").is_some() {
        set_combine(true);
        let which = std::env::var("EXPLAIN_PLAN").unwrap_or_else(|_| "REVENUE".to_string());
        let sql = match which.as_str() {
            "REVMAX" => REVMAX,
            "FULL" => FULL,
            _ => REVENUE,
        };
        let _ = run_once(&data_dir, parts, sql).await; // warm
        let ctx = build_ctx(&data_dir, parts)?;
        let batches = ctx
            .sql(&format!("EXPLAIN ANALYZE {sql}"))
            .await?
            .collect()
            .await?;
        println!("=== EXPLAIN ANALYZE {which} (EMAT_COMBINE_AGG=1) @ P={parts} ===");
        for b in &batches {
            let col = b.column(b.num_columns() - 1);
            if let Some(a) = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
            {
                for i in 0..a.len() {
                    println!("{}", a.value(i));
                }
            }
        }
        return Ok(());
    }

    // SPLIT diagnostic: REVENUE ON ×N, report build-phase vs combine-phase wall
    // (per fire) — distinguishes intrinsic barrier (build≈decode, combine tiny)
    // from fixable combine overhead.
    if std::env::var_os("SPLIT").is_some() {
        set_combine(true);
        let n = 9usize;
        for _ in 0..warmups {
            let _ = run_once(&data_dir, parts, REVENUE).await;
        }
        COMBINE_AGG_BUILD_NANOS.store(0, Ordering::Relaxed);
        COMBINE_AGG_COMBINE_NANOS.store(0, Ordering::Relaxed);
        COMBINE_AGG_FIRES.store(0, Ordering::Relaxed);
        let mut walls = Vec::new();
        for _ in 0..n {
            let (ms, _r, _c) = run_once(&data_dir, parts, REVENUE).await?;
            walls.push(ms);
        }
        let fires = COMBINE_AGG_FIRES.load(Ordering::Relaxed).max(1);
        let build_ms = COMBINE_AGG_BUILD_NANOS.load(Ordering::Relaxed) as f64 / 1e6 / fires as f64;
        let comb_ms = COMBINE_AGG_COMBINE_NANOS.load(Ordering::Relaxed) as f64 / 1e6 / fires as f64;
        println!(
            "SPLIT REVENUE ON @ P={parts}: total≈{:.2}ms  build_phase={build_ms:.2}ms  combine={comb_ms:.3}ms  (fires={fires})",
            median(walls)
        );
        println!(
            "(build_phase = drain all partitions + local agg, decode-bound; combine = parallel shard merge)"
        );
        return Ok(());
    }

    println!(
        "PV.M.8 Stage-4a — CombineAggExec A/B (off vs on) @ P={parts}  data={}",
        data_dir.display()
    );
    println!(
        "{:<9} {:<4} {:>10} {:>7} {:>14}",
        "plan", "arm", "ms", "fires", "checksum"
    );

    for (label, sql) in [("REVENUE", REVENUE), ("REVMAX", REVMAX), ("FULL", FULL)] {
        // Interleaved off/on across rounds → beats within-process drift.
        let mut off = Vec::new();
        let mut on = Vec::new();
        let mut cs_off = 0.0;
        let mut cs_on = 0.0;
        let mut fires_off = 0u64;
        let mut fires_on = 0u64;
        // warm both arms once
        for armon in [false, true] {
            set_combine(armon);
            for _ in 0..warmups {
                let _ = run_once(&data_dir, parts, sql).await;
            }
        }
        for _ in 0..rounds {
            for armon in [false, true] {
                set_combine(armon);
                let mut ts = Vec::with_capacity(trials);
                for _ in 0..trials {
                    let f0 = COMBINE_AGG_FIRES.load(Ordering::Relaxed);
                    let (ms, _rows, cs) = run_once(&data_dir, parts, sql).await?;
                    let fired = COMBINE_AGG_FIRES.load(Ordering::Relaxed) - f0;
                    ts.push(ms);
                    if armon {
                        cs_on = cs;
                        fires_on = fired;
                    } else {
                        cs_off = cs;
                        fires_off = fired;
                    }
                }
                let m = median(ts);
                if armon {
                    on.push(m);
                } else {
                    off.push(m);
                }
            }
        }
        let m_off = median(off);
        let m_on = median(on);
        println!(
            "{label:<9} {:<4} {m_off:>10.2} {fires_off:>7} {cs_off:>14.1}",
            "OFF"
        );
        println!(
            "{label:<9} {:<4} {m_on:>10.2} {fires_on:>7} {cs_on:>14.1}",
            "ON"
        );
        let delta = (m_on - m_off) / m_off * 100.0;
        let cs_ok = (cs_off - cs_on).abs() / cs_off.abs().max(1.0) < 1e-9;
        println!(
            "{label:<9} ==>  on/off = {:.3}×  ({delta:+.1}%)   checksum_match={cs_ok}\n",
            m_on / m_off
        );
    }
    println!("GO if REVENUE ON fires>0, on/off < 1.0 (faster), checksum_match=true.");
    println!(
        "FULL is expected fires=0 (agg is CSE-wrapped → Phase-1b); must stay correct + ~unchanged."
    );
    Ok(())
}
