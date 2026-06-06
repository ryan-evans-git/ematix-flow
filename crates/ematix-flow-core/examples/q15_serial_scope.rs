//! PV.M.8 Phase-0 — localize Q15's ~40ms serial section. Full Q15 walls at
//! ~5.8× on 14 cores (M4 Max 10P+4E); Polars 7.5×. Candidates: (a) a serial
//! tail INSIDE the revenue agg (FinalPartitioned merge / decode straggler),
//! or (b) the post-CSE-barrier consumer tail (SharedSubtreeExec replays as
//! UnknownPartitioning(1) → join/max/sort run single-threaded).
//!
//! This times THREE plans at configurable PARTITIONS, fresh ctx/trial:
//!   REVENUE  — the CTE alone (decode+filter+groupby, NO CSE/join/max/sort)
//!   REVMAX   — revenue + the =max scalar (adds the 2nd consumer + agg)
//!   FULL     — the whole Q15 (adds supplier join + sort)
//! Compare each plan's scaling curve (P=1 vs P=14). The stage whose ADDITION
//! collapses the speedup is the serial section.
//!
//! If REVENUE scales ~linearly to 14 but FULL walls → serial = the tail
//! (CSE-barrier + 1-partition consumers) → prototype streaming the replay.
//! If REVENUE ALSO walls → serial is inside the agg (decode/merge) → different
//! lever (decode work-steal, PV.M.5).
//!
//! Usage: PARTITIONS=14 TRIALS=11 TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!   cargo run --release -p ematix-flow-core --example q15_serial_scope

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::Array;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

// COUNT: scan + shipdate filter only (decodes just the filter col).
const COUNTQ: &str = "\
SELECT COUNT(*) FROM lineitem \
WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01'";

// SCALAR: + extprice/discount decode + multiply-sum, NO group-by (1 row).
const SCALAR: &str = "\
SELECT SUM(l_extendedprice * (1 - l_discount)) FROM lineitem \
WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01'";

const REVENUE: &str = "\
SELECT l_suppkey, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue \
FROM lineitem \
WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01' \
GROUP BY l_suppkey";

// Revenue + the =max consumer (forces the value to materialize; no join).
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

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn build_ctx(data_dir: &Path, parts: usize) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let mut cfg = SessionConfig::new().with_target_partitions(parts);
    // PV.M.8 de-risk: force DataFusion's adaptive skip-partial-aggregation
    // to engage earlier. Default probe_ratio_threshold=0.8 (skip when
    // observed output/input > 0.8). Q15's Partial reduces to 0.49 so it
    // never skips; lowering the threshold makes it skip the near-useless
    // Partial and go straight to repartition→final.
    if let Ok(v) = std::env::var("EMAT_SKIP_PARTIAL_RATIO") {
        if let Ok(r) = v.parse::<f64>() {
            cfg.options_mut()
                .execution
                .skip_partial_aggregation_probe_ratio_threshold = r;
            cfg.options_mut()
                .execution
                .skip_partial_aggregation_probe_rows_threshold = 1;
        }
    }
    let builder = SessionStateBuilder::new().with_config(cfg).with_default_features();
    let state = ematix_flow_core::preset::with_optimizer_rules(builder).build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let p = data_dir.join(format!("{t}.parquet"));
        if *t == "lineitem" || *t == "orders" {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(p.to_string_lossy())?),
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
) -> Result<(f64, usize), Box<dyn std::error::Error>> {
    let ctx = build_ctx(data_dir, parts)?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let t = Instant::now();
    let batches = collect(plan, ctx.task_ctx()).await?;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    Ok((ms, rows))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let trials: usize = std::env::var("TRIALS").ok().and_then(|s| s.parse().ok()).unwrap_or(11);
    let warmups = 3;
    let plist: Vec<usize> = std::env::var("PARTS_SWEEP")
        .ok()
        .map(|s| s.split(',').filter_map(|x| x.parse().ok()).collect())
        .unwrap_or_else(|| vec![1, 14]);

    // EXPLAIN ANALYZE mode: dump per-operator metrics for REVENUE at PARTITIONS.
    if std::env::var_os("EXPLAIN").is_some() {
        let p: usize = std::env::var("PARTITIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(14);
        let which = std::env::var("EXPLAIN_PLAN").unwrap_or_else(|_| "REVENUE".to_string());
        let sql = match which.as_str() {
            "SCALAR" => SCALAR,
            "REVMAX" => REVMAX,
            "FULL" => FULL,
            _ => REVENUE,
        };
        // Warm once (decode-cache / file open), then EXPLAIN ANALYZE.
        let _ = run_once(&data_dir, p, sql).await;
        let ctx = build_ctx(&data_dir, p)?;
        let batches = ctx
            .sql(&format!("EXPLAIN ANALYZE {sql}"))
            .await?
            .collect()
            .await?;
        println!("=== EXPLAIN ANALYZE {which} @ PARTITIONS={p} ===");
        for b in &batches {
            let col = b.column(b.num_columns() - 1);
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            for i in 0..arr.len() {
                println!("{}", arr.value(i));
            }
        }
        return Ok(());
    }

    println!("PV.M.8 Phase-0 — Q15 serial-section scope — data={} trials={trials}", data_dir.display());
    println!("plans: REVENUE (agg only) / REVMAX (+2nd consumer+max) / FULL (+join+sort)\n");
    println!("{:<9} {:>6} {:>10} {:>10} {:>10}", "plan", "P", "ms", "speedup", "rows");

    for (label, sql) in [
        ("COUNT", COUNTQ),
        ("SCALAR", SCALAR),
        ("REVENUE", REVENUE),
        ("REVMAX", REVMAX),
        ("FULL", FULL),
    ] {
        let mut base = 0.0f64;
        for (i, &p) in plist.iter().enumerate() {
            for _ in 0..warmups {
                let _ = run_once(&data_dir, p, sql).await;
            }
            let mut ts = Vec::with_capacity(trials);
            let mut rows = 0;
            for _ in 0..trials {
                let (ms, r) = run_once(&data_dir, p, sql).await?;
                ts.push(ms);
                rows = r;
            }
            let m = median(ts);
            if i == 0 {
                base = m;
            }
            println!("{label:<9} {p:>6} {m:>10.2} {:>9.2}× {rows:>10}", base / m);
        }
        println!();
    }
    Ok(())
}
