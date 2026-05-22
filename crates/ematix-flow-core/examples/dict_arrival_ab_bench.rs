//! Σ.K.1 — A/B bench: dict-preservation OFF vs ON on TPC-H queries
//! that touch string columns as group-by keys or filter predicates.
//!
//! Background: `EnableDictGroupCountRule` + `DictFilterExec` only fire
//! when string columns arrive at the Arrow surface as
//! `Dictionary(UInt32, Utf8)`. By default `EmatixFastParquetTableProvider::try_new`
//! materialises strings as `Utf8View`, so the rules are no-ops on the
//! 22-query bench. Flipping `with_dict_preservation(true)` rewrites the
//! provider schema and uses the dict-preserved decode façade — Dictionary
//! arrives, rule fires.
//!
//! This bench measures whether flipping dict-preservation ON is a net win
//! across queries with string group-by keys (Q01/Q12/Q13/Q16/Q21) and
//! string-filter shapes (Q03/Q19). Output is per-query wall time for each
//! mode plus a delta.
//!
//! Honest framing: this is the *diagnostic* step before any default flip.
//! We expect SOME queries to regress because Dictionary↔Utf8View
//! materialization isn't always cheap in downstream operators DataFusion
//! doesn't have a dict-specialised path for.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example dict_arrival_ab_bench

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use futures_util::TryStreamExt;

/// (label, sql) — picked to exercise dict-arrival paths.
const QUERIES: &[(&str, &str)] = &[
    // Q01: GROUP BY (l_returnflag, l_linestatus) — two short strings.
    (
        "Q01",
        "SELECT l_returnflag, l_linestatus, COUNT(*), SUM(l_quantity) \
           FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' \
           GROUP BY l_returnflag, l_linestatus",
    ),
    // Q12: GROUP BY l_shipmode (7-element dict).
    (
        "Q12",
        "SELECT l_shipmode, COUNT(*) \
           FROM lineitem \
          WHERE l_shipmode IN ('MAIL', 'SHIP') \
            AND l_receiptdate >= DATE '1994-01-01' \
            AND l_receiptdate <  DATE '1995-01-01' \
          GROUP BY l_shipmode",
    ),
    // Q13: GROUP BY o_orderpriority shape; LIKE filter on o_comment too.
    (
        "Q13",
        "SELECT o_orderpriority, COUNT(*) \
           FROM orders \
           GROUP BY o_orderpriority",
    ),
    // Q16: GROUP BY p_brand, p_type — both dict-friendly.
    (
        "Q16",
        "SELECT p_brand, p_type, COUNT(*) \
           FROM part \
           WHERE p_size IN (49, 14, 23, 45, 19, 3, 36, 9) \
           GROUP BY p_brand, p_type",
    ),
    // Q19: OR-of-AND with three string-equality branches on p_brand.
    (
        "Q19",
        "SELECT SUM(l_extendedprice) \
           FROM lineitem JOIN part ON l_partkey = p_partkey \
          WHERE (p_brand = 'Brand#12' AND l_quantity BETWEEN 1 AND 11) \
             OR (p_brand = 'Brand#23' AND l_quantity BETWEEN 10 AND 20) \
             OR (p_brand = 'Brand#34' AND l_quantity BETWEEN 20 AND 30)",
    ),
];

const REPS: usize = 5;

async fn run_one(ctx: &SessionContext, sql: &str) -> Duration {
    let t = Instant::now();
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let mut total_rows = 0usize;
    for p in 0..plan.output_partitioning().partition_count() {
        let mut s = plan.execute(p, ctx.task_ctx()).unwrap();
        while let Some(b) = s.try_next().await.unwrap() {
            total_rows += b.num_rows();
        }
    }
    let d = t.elapsed();
    std::hint::black_box(total_rows);
    d
}

async fn build_ctx(dir: &str, dict_on: bool) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8),
    );
    let ctx = SessionContext::new_with_config(cfg);
    for t in ["lineitem", "orders", "part"].iter() {
        let path = format!("{dir}/{t}.parquet");
        let prov = if dict_on {
            EmatixFastParquetTableProvider::try_new(&path)
                .unwrap()
                .with_dict_preservation(true)
        } else {
            EmatixFastParquetTableProvider::try_new(&path).unwrap()
        };
        ctx.register_table(*t, Arc::new(prov)).unwrap();
    }
    ctx
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf1".to_string());
    println!("=== Σ.K.1: dict-preservation OFF vs ON ({}) ===\n", dir);
    println!("{:<6} {:>12} {:>12} {:>10}", "Query", "OFF (ms)", "ON (ms)", "Δ%");
    println!("{}", "-".repeat(46));

    let mut wins = 0;
    let mut losses = 0;

    for (label, sql) in QUERIES {
        // Warm-up + measurement for each mode.
        let off_ctx = build_ctx(&dir, false).await;
        let _ = run_one(&off_ctx, sql).await;
        let mut off_times = Vec::with_capacity(REPS);
        for _ in 0..REPS {
            off_times.push(run_one(&off_ctx, sql).await);
        }

        let on_ctx = build_ctx(&dir, true).await;
        let _ = run_one(&on_ctx, sql).await;
        let mut on_times = Vec::with_capacity(REPS);
        for _ in 0..REPS {
            on_times.push(run_one(&on_ctx, sql).await);
        }

        off_times.sort();
        on_times.sort();
        let off_med = off_times[REPS / 2];
        let on_med = on_times[REPS / 2];
        let delta = (on_med.as_secs_f64() - off_med.as_secs_f64())
            / off_med.as_secs_f64()
            * 100.0;
        if delta < -1.0 {
            wins += 1;
        } else if delta > 1.0 {
            losses += 1;
        }
        println!(
            "{:<6} {:>12.2} {:>12.2} {:>9.1}%",
            label,
            off_med.as_secs_f64() * 1000.0,
            on_med.as_secs_f64() * 1000.0,
            delta,
        );
    }

    println!();
    println!("Wins (≥1% faster ON): {wins}");
    println!("Losses (≥1% slower ON): {losses}");
    println!();
    println!("Decision criterion: flip dict-preservation default if wins outnumber");
    println!("losses AND no single loss is catastrophic (≥10%).");
}
