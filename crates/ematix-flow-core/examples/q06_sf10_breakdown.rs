//! Q06 SF=10 breakdown: figure out where the 26% gap to Polars lives.
//!
//! Times four progressive stages so per-stage delta isolates the cost:
//!   1. scan-only `SELECT COUNT(*) FROM lineitem`
//!   2. + filter   `SELECT COUNT(*) FROM lineitem WHERE <Q06 preds>`
//!   3. + project  `SELECT l_extendedprice, l_discount FROM lineitem WHERE <preds>` (collect)
//!   4. full Q06   `SELECT SUM(l_extendedprice * l_discount) FROM lineitem WHERE <preds>`
//!
//! Notes:
//! - `l_discount` is in BOTH WHERE and SELECT, so the production path
//!   decodes it twice (once in BridgeFilter for the bitmap, once as
//!   projection via masked_decode). The stage-3 vs stage-2 delta
//!   exposes that overhead.
//! - Use EMAT_EXPLAIN=1 to also print the physical plan.
//!
//! Run:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example q06_sf10_breakdown

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::physical_plan::{ExecutionPlanProperties, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use futures_util::TryStreamExt;

const STAGES: &[(&str, &str)] = &[
    ("1. scan-only (count)", "SELECT COUNT(*) FROM lineitem"),
    (
        "2a. + 1 pred: shipdate range",
        "SELECT COUNT(*) FROM lineitem \
         WHERE l_shipdate >= DATE '1994-01-01' \
           AND l_shipdate <  DATE '1995-01-01'",
    ),
    (
        "2b. + 1 pred: discount range",
        "SELECT COUNT(*) FROM lineitem \
         WHERE l_discount BETWEEN 0.05 AND 0.07",
    ),
    (
        "2c. + 1 pred: quantity range",
        "SELECT COUNT(*) FROM lineitem \
         WHERE l_quantity < 24",
    ),
    (
        "2d. + 2 preds: shipdate + discount",
        "SELECT COUNT(*) FROM lineitem \
         WHERE l_shipdate >= DATE '1994-01-01' \
           AND l_shipdate <  DATE '1995-01-01' \
           AND l_discount BETWEEN 0.05 AND 0.07",
    ),
    (
        "2e. + 3 preds (Q06 filter)",
        "SELECT COUNT(*) FROM lineitem \
         WHERE l_shipdate >= DATE '1994-01-01' \
           AND l_shipdate <  DATE '1995-01-01' \
           AND l_discount BETWEEN 0.05 AND 0.07 \
           AND l_quantity <  24",
    ),
    (
        "3a. SUM(price) — no l_discount in projection",
        "SELECT SUM(l_extendedprice) FROM lineitem \
         WHERE l_shipdate >= DATE '1994-01-01' \
           AND l_shipdate <  DATE '1995-01-01' \
           AND l_discount BETWEEN 0.05 AND 0.07 \
           AND l_quantity <  24",
    ),
    (
        "3b. SUM(price * discount) — full Q06",
        "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem \
         WHERE l_shipdate >= DATE '1994-01-01' \
           AND l_shipdate <  DATE '1995-01-01' \
           AND l_discount BETWEEN 0.05 AND 0.07 \
           AND l_quantity <  24",
    ),
    (
        "3c. SUM(price + discount) — same projection cols, different op",
        "SELECT SUM(l_extendedprice + l_discount) FROM lineitem \
         WHERE l_shipdate >= DATE '1994-01-01' \
           AND l_shipdate <  DATE '1995-01-01' \
           AND l_discount BETWEEN 0.05 AND 0.07 \
           AND l_quantity <  24",
    ),
    (
        "4. SUM(price) — no filter, scan-all-rows agg",
        "SELECT SUM(l_extendedprice) FROM lineitem",
    ),
];

async fn time_query(ctx: &SessionContext, sql: &str, reps: usize) -> (f64, usize) {
    // Warm-up
    let _ = run_once(ctx, sql).await;
    let mut times = Vec::with_capacity(reps);
    let mut rows = 0;
    for _ in 0..reps {
        let t = Instant::now();
        rows = run_once(ctx, sql).await;
        times.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[times.len() / 2], rows)
}

async fn run_once(ctx: &SessionContext, sql: &str) -> usize {
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let mut n = 0;
    for p in 0..plan.output_partitioning().partition_count() {
        let mut s = plan.execute(p, ctx.task_ctx()).unwrap();
        while let Some(b) = s.try_next().await.unwrap() {
            n += b.num_rows();
        }
    }
    n
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let path = PathBuf::from(&dir).join("lineitem.parquet");
    if !path.exists() {
        eprintln!("missing {}", path.display());
        std::process::exit(2);
    }

    let cfg = SessionConfig::new().with_target_partitions(
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8),
    );
    let ctx = SessionContext::new_with_config(cfg);
    let prov = EmatixFastParquetTableProvider::try_new(path.to_str().unwrap()).unwrap();
    ctx.register_table("lineitem", Arc::new(prov)).unwrap();

    let reps: usize = std::env::var("REPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    println!(
        "=== Q06 SF=10 breakdown ({} reps each, fresh warmup) ===",
        reps
    );
    println!("Data: {}\n", path.display());

    let explain = std::env::var("EMAT_EXPLAIN").is_ok();

    let mut prev_ms = 0.0_f64;
    for (label, sql) in STAGES {
        let (ms, rows) = time_query(&ctx, sql, reps).await;
        let delta = if prev_ms > 0.0 {
            format!(" (Δ {:+.2} ms)", ms - prev_ms)
        } else {
            String::new()
        };
        println!("  {label:<36} {ms:>7.2} ms  rows={rows}{delta}");
        if explain {
            let df = ctx.sql(sql).await.unwrap();
            let plan = df.create_physical_plan().await.unwrap();
            println!("    plan:\n{}", displayable(plan.as_ref()).indent(true));
        }
        prev_ms = ms;
    }

    println!();
    println!("Reading the stages:");
    println!("  Δ(2-1) = filter cost (bitmap build for 3 predicates)");
    println!("  Δ(3-2) = projection masked-decode cost (price + discount)");
    println!("  Δ(4-3) = aggregate cost (mul + scalar sum)");
}
