//! Σ.G.2c operator-level perf-equivalence gate.
//!
//! The Σ.G.2a/b benches (#92, #93) timed the *trait dispatch boundary*
//! on synthetic batches and proved Q6Spec / Q1Spec match their hand
//! kernels to within 1 %. This bench closes the loop one level up:
//! does the generic `FusedAggregateExec<S>` operator (the actual
//! DataFusion `ExecutionPlan`) match the hand operators
//! `FusedFilterSumExec` / `FusedFilterMultiAggExec` on real TPC-H SF=1
//! lineitem data end-to-end through DataFusion's streaming runtime?
//!
//! If this gate passes, the planner rule to auto-route Q6/Q1 plans
//! through `FusedAggregateExec<S>` is safe to write. If it fails,
//! either the operator's `execute()` body or the trait's stream
//! plumbing is adding overhead and the planner rule should not land.
//!
//! ## Methodology (post-2026-05-17 revision)
//!
//! At SF=1 the hand kernels finish in 13–24 ms — small enough that
//! single-trial measurement is dominated by OS scheduler jitter,
//! parquet page-cache state, and thermal drift. The first cut of this
//! bench (11 trials, median, sequential per-op) saw ±10 % swing and
//! couldn't distinguish a real 3 % delta from noise.
//!
//! Three methodology changes:
//!   1. **MIN-of-K instead of median.** Perf gates measure lower-bound
//!      capability; noise only adds latency, never subtracts it. The
//!      minimum is the cleanest estimate of the "no interference" cost.
//!   2. **Interleaved trials.** Alternate hand → unified → hand → …
//!      every round so any systemic drift (CPU thermal, page-cache
//!      warm-up) hits both equally.
//!   3. **K rounds per trial.** Each timed trial drains the stream
//!      `ROUNDS_PER_TRIAL` times and divides; amortises any per-trial
//!      framework cost (stream setup, tokio task allocation).
//!
//! 41 interleaved trials × 3 rounds each = 123 stream pulls per op.
//! Empirically this drops the run-to-run delta variance well below
//! the 3 % gate. Both medians (informational) and mins (gate) are
//! printed; the gate uses MIN.
//!
//! ### Why the earlier "+3-5 % Q1 regression" was wrong
//!
//! The first version of this bench measured medians of single
//! invocations. Run right after a 1+ min release build it would
//! show Q1 unified +3-5 % vs hand; re-runs on a thermally-stable
//! system showed -2 % to +2 % (noise). The methodology fix above
//! (warmups + interleaved + MIN-of-K) makes the gate reliable so
//! we don't draw false conclusions from build-then-bench artifacts.
//!
//! Runs:
//!     cargo run --release -p ematix-flow-core \
//!         --example sigma_g2c_operator_vs_hand

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused::{FusedFilterSumExec, Q6Predicate};
use ematix_flow_core::fused_aggregate::{Q1Spec, Q6Spec};
use ematix_flow_core::fused_aggregate_exec::FusedAggregateExec;
use ematix_flow_core::fused_multi_agg::{FusedFilterMultiAggExec, Q1Predicate};
use futures_util::stream::TryStreamExt;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const TRIALS: usize = 41;
const ROUNDS_PER_TRIAL: usize = 3;
const WARMUPS: usize = 3;
const GATE_PCT: f64 = 3.0;

const Q6_PREDICATE: Q6Predicate = Q6Predicate {
    date_lo: 8766,
    date_hi: 9131,
    disc_lo: 0.05,
    disc_hi: 0.07,
    qty_hi: 24.0,
};
const Q1_PREDICATE: Q1Predicate = Q1Predicate {
    shipdate_cutoff: 10471, // 1998-09-02
};

fn data_dir() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => s,
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1")
            .to_string_lossy()
            .into_owned(),
    }
}

async fn make_ctx(parquet_dir: &str) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    for table in TPCH_TABLES {
        let path = format!("{parquet_dir}/{table}.parquet");
        if std::path::Path::new(&path).exists() {
            let prov = FastParquetTableProvider::try_new(path).unwrap();
            ctx.register_table(*table, Arc::new(prov)).unwrap();
        }
    }
    ctx
}

async fn run_to_first_batch(exec: Arc<dyn ExecutionPlan>) -> RecordBatch {
    let ctx = SessionContext::new();
    let mut s = exec.execute(0, ctx.task_ctx()).unwrap();
    s.try_next()
        .await
        .expect("stream yielded an Err")
        .expect("stream yielded None")
}

/// Time one trial: ROUNDS_PER_TRIAL consecutive stream pulls.
async fn time_one_trial(exec: Arc<dyn ExecutionPlan>) -> f64 {
    let start = Instant::now();
    for _ in 0..ROUNDS_PER_TRIAL {
        let _ = run_to_first_batch(exec.clone()).await;
    }
    start.elapsed().as_secs_f64() * 1000.0 / ROUNDS_PER_TRIAL as f64
}

async fn q6_input(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
    let df = ctx
        .sql("SELECT l_quantity, l_extendedprice, l_discount, l_shipdate FROM lineitem")
        .await
        .unwrap();
    df.create_physical_plan().await.unwrap()
}

async fn q1_input(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
    let df = ctx
        .sql(
            "SELECT l_returnflag, l_linestatus, l_quantity, l_extendedprice, \
                    l_discount, l_tax, l_shipdate FROM lineitem",
        )
        .await
        .unwrap();
    df.create_physical_plan().await.unwrap()
}

struct ShapeResult {
    label: &'static str,
    hand_median: f64,
    hand_min: f64,
    unif_median: f64,
    unif_min: f64,
}

impl ShapeResult {
    fn ratio_median(&self) -> f64 {
        self.unif_median / self.hand_median
    }
    fn ratio_min(&self) -> f64 {
        self.unif_min / self.hand_min
    }
}

/// Interleaved bench: hand, unified, hand, unified, … per trial.
async fn bench_interleaved(
    label: &'static str,
    hand: Arc<dyn ExecutionPlan>,
    unif: Arc<dyn ExecutionPlan>,
) -> ShapeResult {
    // Warmup both paths so the parquet page cache, malloc arenas, and
    // JIT (if any) are equilibrated before we start collecting samples.
    for _ in 0..WARMUPS {
        let _ = run_to_first_batch(hand.clone()).await;
        let _ = run_to_first_batch(unif.clone()).await;
    }

    let mut hand_times = Vec::with_capacity(TRIALS);
    let mut unif_times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        hand_times.push(time_one_trial(hand.clone()).await);
        unif_times.push(time_one_trial(unif.clone()).await);
    }
    hand_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    unif_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    ShapeResult {
        label,
        hand_median: hand_times[TRIALS / 2],
        hand_min: hand_times[0],
        unif_median: unif_times[TRIALS / 2],
        unif_min: unif_times[0],
    }
}

async fn bench_q6(ctx: &SessionContext) -> ShapeResult {
    let hand_in = q6_input(ctx).await;
    let unif_in = q6_input(ctx).await;
    let hand: Arc<dyn ExecutionPlan> =
        Arc::new(FusedFilterSumExec::try_new_q6(hand_in, Q6_PREDICATE).unwrap());
    let spec = Q6Spec::try_new(Q6_PREDICATE, &unif_in.schema()).unwrap();
    let unif: Arc<dyn ExecutionPlan> =
        Arc::new(FusedAggregateExec::try_new(unif_in, spec).unwrap());

    let hand_out = run_to_first_batch(hand.clone()).await;
    let unif_out = run_to_first_batch(unif.clone()).await;
    let h = hand_out
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    let u = unif_out
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert!(
        (h - u).abs() / h.abs().max(1.0) < 1e-12,
        "Q6 SF=1 hand {h} vs unified {u}",
    );

    bench_interleaved("Q6", hand, unif).await
}

async fn bench_q1(ctx: &SessionContext) -> ShapeResult {
    let hand_in = q1_input(ctx).await;
    let unif_in = q1_input(ctx).await;
    let hand: Arc<dyn ExecutionPlan> =
        Arc::new(FusedFilterMultiAggExec::try_new_q1(hand_in, Q1_PREDICATE).unwrap());
    let spec = Q1Spec::try_new(Q1_PREDICATE, &unif_in.schema()).unwrap();
    let unif: Arc<dyn ExecutionPlan> =
        Arc::new(FusedAggregateExec::try_new(unif_in, spec).unwrap());

    let hand_out = run_to_first_batch(hand.clone()).await;
    let unif_out = run_to_first_batch(unif.clone()).await;
    assert_eq!(
        hand_out.num_rows(),
        unif_out.num_rows(),
        "Q1 row-count diverges"
    );
    let h = hand_out
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    let u = unif_out
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert!(
        (h - u).abs() / h.abs().max(1.0) < 1e-12,
        "Q1 SF=1 sum_qty[0] hand {h} vs unified {u}",
    );

    bench_interleaved("Q1", hand, unif).await
}

fn report(r: &ShapeResult) -> bool {
    let r_min = r.ratio_min();
    let r_med = r.ratio_median();
    let ok = (r_min - 1.0) * 100.0 <= GATE_PCT;
    println!(
        "  {:<3} hand     median {:>7.3} ms   min {:>7.3} ms",
        r.label, r.hand_median, r.hand_min
    );
    println!(
        "      unified  median {:>7.3} ms   min {:>7.3} ms",
        r.unif_median, r.unif_min
    );
    println!(
        "      ratio    median {:>5.3} ({:+5.2} %)   min {:>5.3} ({:+5.2} %)   [gate: min]   {}",
        r_med,
        (r_med - 1.0) * 100.0,
        r_min,
        (r_min - 1.0) * 100.0,
        if ok { "PASS" } else { "FAIL" }
    );
    ok
}

#[tokio::main]
async fn main() {
    let dir = data_dir();
    if !std::path::Path::new(&format!("{dir}/lineitem.parquet")).exists() {
        eprintln!(
            "lineitem.parquet not found under {dir} — set TPCH_DATA_DIR or generate examples/tpch/data/sf1"
        );
        std::process::exit(2);
    }
    let ctx = make_ctx(&dir).await;

    println!("== Σ.G.2c operator-level perf-equivalence gate ==");
    println!("  dataset: TPC-H SF=1 lineitem ({dir})");
    println!(
        "  trials:  {} interleaved × {} rounds/trial (after {} warmups)",
        TRIALS, ROUNDS_PER_TRIAL, WARMUPS
    );
    println!(
        "  gate:    unified.min ≤ hand.min × {:.2}",
        1.0 + GATE_PCT / 100.0
    );
    println!();

    let q6 = bench_q6(&ctx).await;
    let ok6 = report(&q6);
    println!();
    let q1 = bench_q1(&ctx).await;
    let ok1 = report(&q1);
    println!();

    if ok6 && ok1 {
        println!(
            "  GATE PASS — operator-level Σ.G.2 within {:.0} % on both shapes (MIN-of-{}).",
            GATE_PCT, TRIALS
        );
        println!("  Safe to write the planner rule that auto-routes existing Q6/Q1");
        println!("  plans through FusedAggregateExec<S>.");
    } else {
        println!("  GATE FAIL — DO NOT write the planner rule yet. Investigate:");
        println!("    - stream-pull overhead in FusedAggregateExec::execute()");
        println!("    - trait dispatch at the per-partition boundary");
        println!("    - per-batch accumulator allocation cost");
        std::process::exit(1);
    }
}
