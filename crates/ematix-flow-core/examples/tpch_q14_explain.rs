//! Σ.E1 day-1 profiling: EXPLAIN ANALYZE Q14 to identify where the
//! 27.21 ms goes vs Polars's 12.53 ms.
//!
//! Issue [#56]. Σ.D5's hand-written agg-step measurement was 0.10 ms
//! single-thread on the post-join data; DataFusion's agg-only path was
//! 0.74 ms (7.4× speedup). End-to-end Q14 was 27.21 ms — so ~26.5 ms is
//! spent on filter + parquet decode + the 2-way `lineitem ⋈ part` join.
//! Polars's whole pipeline finishes in 12.53 ms, so the gap is somewhere
//! in that pre-aggregate portion.
//!
//! This prints DataFusion's `EXPLAIN ANALYZE` for Q14 — `time_elapsed_*`
//! per operator, batch counts, selectivity. The dominant operator
//! identifies the optimization target.
//!
//! [#56]: https://github.com/ryan-evans-git/ematix-flow/issues/56

use std::path::PathBuf;
use std::time::Instant;

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::prelude::{SessionConfig, SessionContext};

const Q14: &str = include_str!("../../../examples/tpch/queries/q14.sql");

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let data_dir = manifest
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/tpch/data/sf1");
    let data_dir = data_dir.to_str().unwrap();
    println!("==> Σ.E1: EXPLAIN ANALYZE Q14 (M3 Pro / SF=1)");
    println!("==> data: {data_dir}");
    println!("==> reference: Polars 12.53 ms, DataFusion 27.21 ms (the gap)");
    println!();

    let ctx = SessionContext::new();
    for table in [
        "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
    ] {
        let path = format!("{data_dir}/{table}.parquet");
        ctx.register_parquet(table, &path, Default::default())
            .await
            .unwrap();
    }

    // Warm-up so the EXPLAIN ANALYZE reflects steady-state.
    for _ in 0..3 {
        let _: Vec<RecordBatch> = ctx.sql(Q14).await.unwrap().collect().await.unwrap();
    }

    let sql = format!(
        "EXPLAIN ANALYZE {}",
        Q14.trim().trim_end_matches(';').trim()
    );
    let plan: Vec<RecordBatch> = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    for batch in &plan {
        let plan_col = batch.column(1).as_string::<i32>();
        for i in 0..plan_col.len() {
            for line in plan_col.value(i).lines() {
                println!("{line}");
            }
            println!();
        }
    }

    // ----- Σ.E1 hypothesis test: does target_partitions=1 close the gap? -----
    println!();
    println!("==> Σ.E1 hypothesis test — does target_partitions=1 close the gap?");
    println!("    If yes → RepartitionExec is the overhead; broadcast-join is the fix.");
    println!("    If no  → the gap is elsewhere (scan / hash kernel).");
    println!();

    for (label, cfg) in [
        ("default (target_partitions = ncpu)", SessionConfig::new()),
        (
            "target_partitions = 1",
            SessionConfig::new().with_target_partitions(1),
        ),
        (
            "target_partitions = 2",
            SessionConfig::new().with_target_partitions(2),
        ),
        (
            "target_partitions = 4",
            SessionConfig::new().with_target_partitions(4),
        ),
    ] {
        let ctx = SessionContext::new_with_config(cfg);
        for table in [
            "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
        ] {
            let path = format!("{data_dir}/{table}.parquet");
            ctx.register_parquet(table, &path, Default::default())
                .await
                .unwrap();
        }
        for _ in 0..2 {
            let _: Vec<RecordBatch> = ctx.sql(Q14).await.unwrap().collect().await.unwrap();
        }
        let mut times = Vec::with_capacity(5);
        for _ in 0..5 {
            let start = Instant::now();
            let _: Vec<RecordBatch> = ctx.sql(Q14).await.unwrap().collect().await.unwrap();
            times.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "  {label:<40}  median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
            times[2], times[0], times[4],
        );
    }
}
