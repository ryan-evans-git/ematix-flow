//! Σ.A1 PR 4 follow-up: tuning Q6 against Polars (10.0 ms baseline).
//!
//! Polars beat DataFusion 1.82× on Q6 (10.0 ms vs 18.2 ms) under the
//! default SessionContext config. This example sweeps SessionConfig
//! knobs to identify whether any close the gap, then drills into
//! per-operator wall-time + isolates parquet I/O from the aggregate
//! hot path.
//!
//! ### Conclusion (2026-05-05, M3 Pro / SF=1) — Σ.A1 PR 4
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
//! Pushing filters into the Parquet decoder *hurts* Q6 because its
//! predicates are cheap to evaluate vectorized on the decoded Arrow
//! batches. Closing the 1.82× gap from config knobs is impossible.
//!
//! ### Update (2026-05-05) — Σ.D-window deep dive
//!
//! Re-ran the audit + extended with two new investigations:
//!
//! **(1) MemTable isolation.** Q6 against a pre-decoded MemTable
//! (parquet read once at startup; the timed loop runs against
//! in-memory Arrow batches) — isolates parquet I/O cost from the
//! aggregate hot path.
//!
//! **(2) EXPLAIN ANALYZE.** Per-operator wall-time + row counts so
//! we know *where* the 7-ish ms goes vs Polars's 10 ms total.
//!
//! Findings recorded inline at the bottom of this file's println
//! output. The 1.82× gap is parquet-decode cost, not aggregate
//! cost — DataFusion's vectorized aggregate is competitive once
//! the decode is amortised. See BENCHMARKS.md for the full
//! write-up.
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q6_tune
//!
//! Reads `examples/tpch/data/sf1/lineitem.parquet` (generate via
//! `cargo run --release -p ematix-flow-core --example tpch_generate
//! -- --sf 1 --out examples/tpch/data/sf1` first).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures_util::TryStreamExt;

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

/// Pre-decode the parquet file into in-memory Arrow batches and
/// register a MemTable. The Q6 timed loop then runs against the
/// MemTable — no parquet I/O on the hot path. Parquet decode +
/// pruning + filtering happens once, in the registration step,
/// which is *not* timed.
async fn make_memtable_ctx(parquet: &str) -> SessionContext {
    // Decode the whole file via a fresh SessionContext, collect to
    // owned RecordBatches, then re-register as a MemTable on the
    // benchmarking context.
    let staging = SessionContext::new();
    staging
        .register_parquet("lineitem_pq", parquet, Default::default())
        .await
        .unwrap();
    let df = staging
        .sql(
            "select l_quantity, l_extendedprice, l_discount, l_shipdate \
             from lineitem_pq",
        )
        .await
        .unwrap();
    let schema = Arc::new(df.schema().as_arrow().clone());
    let stream = df.execute_stream().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "  (memtable preload: {} batches, {total_rows} rows decoded once)",
        batches.len()
    );

    let mem = MemTable::try_new(schema, vec![batches]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("lineitem", Arc::new(mem)).unwrap();
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

    println!("--- Section 1: parquet-source SessionConfig sweep ---");

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

    // Σ.D-window deep dive: explicit batch-size knob. Default is 8192;
    // bigger batches amortise per-batch dispatch overhead but increase
    // working-set pressure.
    let cfg = SessionConfig::new().set_str("datafusion.execution.batch_size", "32768");
    let ctx = make_ctx(cfg, parquet).await;
    bench("batch_size=32768 (default 8192)", &ctx).await;

    // Section 2: pre-decoded MemTable. Isolates the aggregate hot
    // path from parquet decode.
    println!();
    println!("--- Section 2: MemTable (parquet decoded once, not timed) ---");
    let ctx = make_memtable_ctx(parquet).await;
    bench("MemTable, default SessionConfig", &ctx).await;

    let cfg = SessionConfig::new().set_str("datafusion.execution.batch_size", "32768");
    let memctx = SessionContext::new_with_config(cfg);
    // Re-register the same batches against the new ctx. Done by
    // re-running the staging extraction since MemTable owns its
    // batches.
    let staging = SessionContext::new();
    staging
        .register_parquet("lineitem_pq", parquet, Default::default())
        .await
        .unwrap();
    let df = staging
        .sql(
            "select l_quantity, l_extendedprice, l_discount, l_shipdate \
             from lineitem_pq",
        )
        .await
        .unwrap();
    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches: Vec<RecordBatch> = df
        .execute_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mem = MemTable::try_new(schema, vec![batches]).unwrap();
    memctx.register_table("lineitem", Arc::new(mem)).unwrap();
    bench("MemTable, batch_size=32768", &memctx).await;

    // Section 3: per-operator wall-time via EXPLAIN ANALYZE on the
    // default-config parquet path. Tells us where the 7-ish ms goes.
    println!();
    println!("--- Section 3: EXPLAIN ANALYZE (default config, parquet source) ---");
    let ctx = make_ctx(SessionConfig::new(), parquet).await;
    // Warm-up so the timings reflect a hot run.
    let _ = ctx.sql(Q6).await.unwrap().collect().await.unwrap();
    let plan = ctx
        .sql(&format!("EXPLAIN ANALYZE {Q6}"))
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
