//! Σ.A1 PR 4 follow-up: tuning Q6 against Polars (10.0 ms baseline).
//!
//! Polars beat DataFusion 1.82× on Q6 (10.0 ms vs 18.2 ms) under the
//! default SessionContext config. This example sweeps SessionConfig
//! knobs to identify whether any close the gap.
//!
//! ### Conclusion (2026-05-05, M3 Pro / SF=1)
//!
//! **The default SessionConfig is already optimal for Q6.** Specifically:
//!
//! | Config                              | Median (ms) |
//! |-------------------------------------|-------------|
//! | default                             | 16.9        |
//! | + target_partitions=12              | 17.2        |
//! | + repartition_file_scans            | 17.1        |
//! | + parquet.pushdown_filters          | **28.3**    |
//! | + parquet.reorder_filters           | **62.9**    |
//!
//! `target_partitions` and `repartition_file_scans` are no-ops because
//! DataFusion already defaults `target_partitions = num_cpus::get()`
//! and splits a single Parquet file into N byte-range scan groups
//! automatically (visible in the EXPLAIN as
//! `file_groups={12 groups: [[...0..17643625], ...]}`).
//!
//! Pushing filters into the Parquet decoder *hurts* Q6 because its
//! predicates (`l_shipdate >= …`, `l_discount BETWEEN …`,
//! `l_quantity < 24`) are cheap to evaluate vectorized on the decoded
//! Arrow batches. The pushdown overhead — per-batch filter mask
//! materialization inside the decoder — exceeds the savings. Reorder
//! adds even more overhead.
//!
//! **Implication for the bench harness**: keep `SessionContext::new()`
//! with no custom config. Don't enable pushdown_filters globally.
//!
//! **Polars's 1.82× edge** comes from hand-tuned vectorized inner
//! loops (filter + multiply + accumulate), not a DataFusion config
//! gap. Closing it would need profiling DataFusion's vectorized
//! aggregate path + potentially upstream PRs — out of scope for Σ.A1.
//! DataFusion remains competitive (within ~70%) and wins on the
//! broader Q1/Q3/Q19 suite.
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q6_tune
//!
//! Reads `examples/tpch/data/sf1/lineitem.parquet` (generate via
//! `cargo run --release -p ematix-flow-core --example tpch_generate
//! -- --sf 1 --out examples/tpch/data/sf1` first).

use std::path::PathBuf;
use std::time::Instant;

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::prelude::{SessionConfig, SessionContext};

const Q6: &str = include_str!("../../../examples/tpch/queries/q06.sql");

async fn bench(label: &str, ctx: &SessionContext) {
    // 1 untimed warm-up.
    let _ = ctx.sql(Q6).await.unwrap().collect().await.unwrap();

    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let _: Vec<RecordBatch> = ctx.sql(Q6).await.unwrap().collect().await.unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = times[2];
    let min = times[0];
    let max = times[4];
    println!("  {label:<55}  median {median:>6.2} ms  (min {min:>5.2}  max {max:>5.2})");
}

async fn make_ctx(cfg: SessionConfig, parquet: &str) -> SessionContext {
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_parquet("lineitem", parquet, Default::default())
        .await
        .unwrap();
    ctx
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let parquet = manifest
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/tpch/data/sf1/lineitem.parquet");
    let parquet = parquet.to_str().unwrap();
    println!("==> Q6 tuning sweep against {parquet}");
    println!("==> reference: Polars 10.0 ms (M3 Pro / SF=1)");
    println!();

    // Baseline: today's bench harness config (vanilla SessionContext).
    let ctx = make_ctx(SessionConfig::new(), parquet).await;
    bench("default SessionConfig", &ctx).await;

    // Knob 1: explicit target_partitions = ncpu (12 on M3 Pro).
    let ctx = make_ctx(SessionConfig::new().with_target_partitions(12), parquet).await;
    bench("target_partitions=12", &ctx).await;

    // Knob 2: + repartition_file_scans (split single Parquet across
    // partitions instead of single-threaded scan).
    let cfg = SessionConfig::new()
        .with_target_partitions(12)
        .with_repartition_file_scans(true);
    let ctx = make_ctx(cfg, parquet).await;
    bench("+ repartition_file_scans", &ctx).await;

    // Knob 3: + Parquet pushdown_filter (apply WHERE predicates inside
    // the Parquet decoder, skipping rows before they hit Arrow).
    let cfg = SessionConfig::new()
        .with_target_partitions(12)
        .with_repartition_file_scans(true)
        .set_str("datafusion.execution.parquet.pushdown_filters", "true");
    let ctx = make_ctx(cfg, parquet).await;
    bench("+ parquet.pushdown_filter", &ctx).await;

    // Knob 4: + reorder_filters (move cheap predicates first inside
    // the Parquet decoder for better short-circuit).
    let cfg = SessionConfig::new()
        .with_target_partitions(12)
        .with_repartition_file_scans(true)
        .set_str("datafusion.execution.parquet.pushdown_filters", "true")
        .set_str("datafusion.execution.parquet.reorder_filters", "true");
    let ctx = make_ctx(cfg, parquet).await;
    bench("+ parquet.reorder_filters", &ctx).await;

    // Print the EXPLAIN plan for the winning config so the speedup
    // is grounded in something physical.
    println!();
    println!(
        "==> EXPLAIN under target_partitions=12 + repartition_file_scans + pushdown_filter + reorder_filters:"
    );
    let cfg = SessionConfig::new()
        .with_target_partitions(12)
        .with_repartition_file_scans(true)
        .set_str("datafusion.execution.parquet.pushdown_filters", "true")
        .set_str("datafusion.execution.parquet.reorder_filters", "true");
    let ctx = make_ctx(cfg, parquet).await;
    let plan = ctx
        .sql(&format!("EXPLAIN {Q6}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for batch in &plan {
        let plan_col = batch.column(1).as_string::<i32>();
        for i in 0..plan_col.len() {
            for line in plan_col.value(i).lines() {
                println!("    {line}");
            }
            println!();
        }
    }
}
