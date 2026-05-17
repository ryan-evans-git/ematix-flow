//! Σ.G.3a operator-level perf gate: `FusedAggregateExec<Q6Spec(JIT)>`
//! vs `FusedFilterSumExec(JIT)`.
//!
//! Σ.G.2c proved the *hand* path through the trait is within 3 % of the
//! hand-coded `FusedFilterSumExec` on TPC-H SF=1 Q6. Σ.G.3a adds an
//! optional Cranelift JIT handle to `Q6Spec` and dispatches on
//! `self.jit` in `process_batch`. This bench is the gate that decides
//! whether that dispatch carries any cost over the hand operator's
//! identical JIT integration.
//!
//! Both sides JIT-build the *same* `FusedFilterAggSpec::q6(...)`, so the
//! hot loop is bit-identical. The only difference is the dispatch site:
//!   - hand:    `match &jit_p { Some(j) => process_q6_batch_jit(...), None => ... }`
//!   - unified: `match &self.jit { Some(j) => process_q6_batch_jit(...), None => ... }`
//!
//! Same fixture, same predicate, same methodology as the Σ.G.2c bench
//! (41 interleaved trials × 3 rounds × MIN-of-K, 3 warmups). Gate at 3 %.
//!
//! Runs:
//!     cargo run --release -p ematix-flow-core \
//!         --example sigma_g3a_q6spec_jit_vs_hand_jit

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused::{FusedFilterSumExec, Q6Predicate};
use ematix_flow_core::fused_aggregate::Q6Spec;
use ematix_flow_core::fused_aggregate_exec::FusedAggregateExec;
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

struct Result_ {
    hand_median: f64,
    hand_min: f64,
    unif_median: f64,
    unif_min: f64,
}

async fn bench_interleaved(hand: Arc<dyn ExecutionPlan>, unif: Arc<dyn ExecutionPlan>) -> Result_ {
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

    Result_ {
        hand_median: hand_times[TRIALS / 2],
        hand_min: hand_times[0],
        unif_median: unif_times[TRIALS / 2],
        unif_min: unif_times[0],
    }
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

    println!("== Σ.G.3a Q6Spec-JIT vs hand-JIT perf gate ==");
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

    let hand_in = q6_input(&ctx).await;
    let unif_in = q6_input(&ctx).await;
    let hand: Arc<dyn ExecutionPlan> =
        Arc::new(FusedFilterSumExec::try_new_q6_jit(hand_in, Q6_PREDICATE).unwrap());
    let spec = Q6Spec::try_new_jit(Q6_PREDICATE, &unif_in.schema()).unwrap();
    let unif: Arc<dyn ExecutionPlan> =
        Arc::new(FusedAggregateExec::try_new(unif_in, spec).unwrap());

    // Correctness — both paths must produce identical revenue.
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
        "Q6 JIT SF=1 hand {h} vs unified {u}"
    );

    let r = bench_interleaved(hand, unif).await;
    let ratio_min = r.unif_min / r.hand_min;
    let ratio_med = r.unif_median / r.hand_median;
    let ok = (ratio_min - 1.0) * 100.0 <= GATE_PCT;

    println!(
        "  Q6  hand     median {:>7.3} ms   min {:>7.3} ms",
        r.hand_median, r.hand_min
    );
    println!(
        "      unified  median {:>7.3} ms   min {:>7.3} ms",
        r.unif_median, r.unif_min
    );
    println!(
        "      ratio    median {:>5.3} ({:+5.2} %)   min {:>5.3} ({:+5.2} %)   [gate: min]   {}",
        ratio_med,
        (ratio_med - 1.0) * 100.0,
        ratio_min,
        (ratio_min - 1.0) * 100.0,
        if ok { "PASS" } else { "FAIL" }
    );
    println!();

    if ok {
        println!(
            "  GATE PASS — Q6Spec-JIT through the trait is within {:.0} % of the hand JIT path.",
            GATE_PCT
        );
        println!("  Safe to extend EnableFusedAggregateExecRule to handle JIT instances (Σ.G.3c).");
    } else {
        println!(
            "  GATE FAIL — Q6Spec-JIT is {:.2} % slower than hand-JIT (> {:.0} %).",
            (ratio_min - 1.0) * 100.0,
            GATE_PCT
        );
        println!("  Investigate the dispatch on `self.jit` in Q6Spec::process_batch — LLVM");
        println!("  should be hoisting the branch but the bench says otherwise.");
        std::process::exit(1);
    }
}
