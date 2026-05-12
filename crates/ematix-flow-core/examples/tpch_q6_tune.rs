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
//! ### Σ.D1 spike (2026-05-11) — fused filter+sum lower bound
//!
//! The 2026-05-05 deep-dive's "DataFusion wins on MemTable" claim was
//! a measurement bug (parquet-vs-MemTable across engines). Apples-to-
//! apples MemTable on 2026-05-11 puts Polars at 1.9 ms vs DataFusion
//! 5.96 ms — Polars wins by 3.13×. The gap is the
//! `FilterExec → AggregateExec` boundary, which materializes a
//! selection mask between the predicate and the sum.
//!
//! Section 4 below adds a hand-written fused filter-+-sum loop that
//! never materializes a mask: single pass over the four primitive
//! arrays, predicate evaluated inline, running f64 sum accumulated
//! per matching row. Numbers on M3 Pro / SF=1 / 14 threads:
//!
//! | Path                                       | Median (ms) |
//! |--------------------------------------------|-------------|
//! | DataFusion MemTable (default)              | 5.96 → 6.10 |
//! | Polars MemTable                            | 1.9         |
//! | Hand-written fused, single-thread          | 7–10        |
//! | **Hand-written fused, 14-thread**          | **1.0**     |
//! | **`FusedFilterSumExec`, in DF runtime**    | **1.24**    |
//!
//! Section 5 below drives the same fused loop through a wrapped
//! `FusedFilterSumExec` `ExecutionPlan` (`crate::fused`). The 0.28 ms
//! delta from the hand-written floor is the `SendableRecordBatchStream`
//! plumbing + the `tokio::spawn_blocking` hop. The wrapped operator
//! hits ~1.24 ms — 1.5× faster than Polars and ~5× faster than
//! DataFusion's default path, well under the ≤ 2 ms decision gate
//! from issue #44.
//!
//! Day-3 follow-on (issue #44): a `PhysicalOptimizerRule` that
//! recognizes `Aggregate(SUM) over Filter(predicate)` plan shapes and
//! rewrites them to `FusedFilterSumExec` so the win flows through
//! transparently without callers constructing the exec by hand.
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

use datafusion::arrow::array::{Array, AsArray, Date32Array, Float64Array, RecordBatch};
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

/// Single-pass fused filter-+-sum over the four Q6 columns. Caller
/// is responsible for matching column ordering: 0=l_quantity,
/// 1=l_extendedprice, 2=l_discount, 3=l_shipdate. No null handling —
/// TPC-H lineitem has no nulls on these columns.
fn run_fused(
    batches: &[RecordBatch],
    date_lo: i32,
    date_hi: i32,
    disc_lo: f64,
    disc_hi: f64,
    qty_hi: f64,
) -> f64 {
    let mut sum: f64 = 0.0;
    for batch in batches {
        let qty = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let price = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let disc = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ship = batch
            .column(3)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let qty_v = qty.values();
        let price_v = price.values();
        let disc_v = disc.values();
        let ship_v = ship.values();
        let n = batch.num_rows();
        for i in 0..n {
            let s = ship_v[i];
            let d = disc_v[i];
            let q = qty_v[i];
            // Order: date range (most selective), then discount, then
            // quantity. Short-circuits cheap; autovectorizer should
            // still vectorize the comparisons + multiply.
            if s >= date_lo && s < date_hi && d >= disc_lo && d <= disc_hi && q < qty_hi {
                sum += price_v[i] * d;
            }
        }
    }
    sum
}

/// Parallel variant: shard the batch slice evenly across `workers`
/// threads, each running `run_fused` on its shard, then sum the
/// per-shard partials on the main thread. Uses `std::thread::scope`
/// so we can borrow `batches` without `Arc`.
fn run_fused_parallel(
    batches: &[RecordBatch],
    workers: usize,
    date_lo: i32,
    date_hi: i32,
    disc_lo: f64,
    disc_hi: f64,
    qty_hi: f64,
) -> f64 {
    let n = batches.len();
    let chunk = n.div_ceil(workers);
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let lo = (w * chunk).min(n);
                let hi = ((w + 1) * chunk).min(n);
                let slice = &batches[lo..hi];
                s.spawn(move || run_fused(slice, date_lo, date_hi, disc_lo, disc_hi, qty_hi))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).sum()
    })
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

    // Section 4: Σ.D1 lower bound — hand-written fused filter-+-sum
    // loop over the same pre-decoded Arrow batches. Sets the floor
    // for what a future `FusedFilterSumExec` physical operator could
    // hit. No DataFusion planning on the hot path — just iterate the
    // four primitive arrays, evaluate the predicate inline, accumulate
    // the running sum. The single-pass loop never materializes a
    // BooleanArray selection mask; the autovectorizer should fold the
    // cheap integer comparisons into SIMD lanes.
    println!();
    println!("--- Section 4: hand-written fused filter+sum (Σ.D1 lower bound) ---");

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
    let batches: Vec<RecordBatch> = df
        .execute_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    println!(
        "  (preload: {} batches, {} rows)",
        batches.len(),
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
    );

    // Q6 predicate constants.
    //   l_shipdate ∈ [1994-01-01, 1995-01-01) — Date32 = days since
    //     1970-01-01: 1994-01-01 = 8766, 1995-01-01 = 9131.
    //   l_discount / l_quantity / l_extendedprice are Float64 in the
    //   generator's output (TPC-H spec allows it).
    let date_lo: i32 = 8766;
    let date_hi: i32 = 9131;
    let disc_lo: f64 = 0.05;
    let disc_hi: f64 = 0.07;
    let qty_hi: f64 = 24.0;

    let answer = run_fused(&batches, date_lo, date_hi, disc_lo, disc_hi, qty_hi);
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let s = run_fused(&batches, date_lo, date_hi, disc_lo, disc_hi, qty_hi);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
        assert!(
            (s - answer).abs() < 1e-6,
            "fused loop produced inconsistent results"
        );
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  hand-written fused filter+sum (single-thread)            median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );

    // Parallel variant: split batches across N threads via
    // `std::thread::scope` (no new workspace dep). One worker per
    // logical core. Each worker runs the same single-pass fused
    // loop over its slice; the main thread sums the per-worker
    // partials. This is what a `FusedFilterSumExec` would do at
    // the operator level.
    let ncpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let _ = run_fused_parallel(&batches, ncpu, date_lo, date_hi, disc_lo, disc_hi, qty_hi);
    let mut times_par = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let s = run_fused_parallel(&batches, ncpu, date_lo, date_hi, disc_lo, disc_hi, qty_hi);
        times_par.push(start.elapsed().as_secs_f64() * 1000.0);
        // f64 sum is non-associative; parallel partial-sum reorders
        // additions vs single-thread, expect ~1e-4 relative drift.
        assert!(
            (s - answer).abs() / answer.abs() < 1e-9,
            "parallel fused mismatch: {s} vs {answer}"
        );
    }
    times_par.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  hand-written fused filter+sum ({ncpu}-thread)              median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times_par[2], times_par[0], times_par[4],
    );
    println!("  (sanity: revenue ≈ {answer:.4} — canonical Q6 SF=1 ≈ 123141078.2283)");

    // Section 5: Σ.D1 wrapped — drive the same hand-written fused
    // loop through `FusedFilterSumExec` (a real DataFusion
    // `ExecutionPlan`). The child plan is the existing MemTable;
    // the wrapped exec drains its partition stream, runs the
    // parallel fused loop, and emits one row. If the day-2 wiring
    // is correct, this should land within noise of Section 4's
    // 1.0 ms — the only added work is the SendableRecordBatchStream
    // plumbing and `tokio::spawn_blocking` hop.
    println!();
    println!("--- Section 5: FusedFilterSumExec (Σ.D1 wrapped operator) ---");

    use ematix_flow_core::fused::{FusedFilterSumExec, Q6Predicate};

    let predicate = Q6Predicate {
        date_lo,
        date_hi,
        disc_lo,
        disc_hi,
        qty_hi,
    };
    let ctx = make_memtable_ctx(parquet).await;
    let logical = ctx
        .sql("SELECT l_quantity, l_extendedprice, l_discount, l_shipdate FROM lineitem")
        .await
        .unwrap();
    let child = logical.create_physical_plan().await.unwrap();
    let fused: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
        Arc::new(FusedFilterSumExec::try_new_q6(child, predicate).unwrap());

    // Warm-up.
    let task_ctx = ctx.task_ctx();
    let mut s = fused.execute(0, task_ctx.clone()).unwrap();
    let _ = s.try_next().await.unwrap();

    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let mut s = fused.execute(0, task_ctx.clone()).unwrap();
        let start = Instant::now();
        let b = s.try_next().await.unwrap().expect("single row");
        times.push(start.elapsed().as_secs_f64() * 1000.0);
        let revenue = b
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap()
            .value(0);
        assert!(
            (revenue - 123_141_078.228_3).abs() < 1e-3,
            "FusedFilterSumExec returned wrong revenue: {revenue}",
        );
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  FusedFilterSumExec (MemTable child, in DataFusion runtime)  median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );

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
