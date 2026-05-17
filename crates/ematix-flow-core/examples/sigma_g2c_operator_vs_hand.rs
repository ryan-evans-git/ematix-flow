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
//! Methodology mirrors `tpch_fused_jit_bench.rs`:
//!   - Materialise the scan via `FastParquetTableProvider`
//!   - 11 trials after one warm-up, sorted median wall-clock
//!   - Same `Q6_PREDICATE` / `Q1_PREDICATE` constants used elsewhere
//!     so the matched-row counts are TPC-H canonical.
//!
//! Gate: each shape's `unified / hand` ratio must be ≤ 1.03.
//!
//! **Σ.G.2c observed-noise note (2026-05-17):** at the SF=1 / SF=10
//! scale this bench shows ±10 % run-to-run variance with median over
//! 11 trials — system noise (scheduler, thermal, parquet cache) sits
//! around the same order of magnitude as the perf-delta signal. Two
//! engineering fixes that closed real overhead were validated by
//! the persistent direction of the delta (consistent +5-18 % before,
//! consistent ±5 % after), but the per-run pass/fail signal isn't
//! reliable. Two pre-fix engineering finds:
//!   1. `Arc<S>` deref per batch — fixed by `S: Clone` value-capture
//!      into the spawn closure, matching the hand operators' `Copy`
//!      shape (cut Q1 +18 % → +5 %).
//!   2. Distinct nominal `Q6ColumnIndices` struct converting to
//!      `fused::ColumnIndices` per batch — fixed by aliasing the
//!      type (cut Q6 +5-6 % → noise).
//!
//! The planner rule that auto-routes existing plans into the generic
//! exec should land only after a definitive PASS — e.g. a pinned-CPU
//! run, or a larger fixture where the signal exceeds the noise floor.
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

const TRIALS: usize = 11;
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

async fn time_exec(exec: Arc<dyn ExecutionPlan>) -> f64 {
    let _ = run_to_first_batch(exec.clone()).await;
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        let _ = run_to_first_batch(exec.clone()).await;
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[TRIALS / 2]
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

async fn bench_q6(ctx: &SessionContext) -> (f64, f64) {
    let hand_in = q6_input(ctx).await;
    let unif_in = q6_input(ctx).await;
    let hand = Arc::new(FusedFilterSumExec::try_new_q6(hand_in, Q6_PREDICATE).unwrap());
    let spec = Q6Spec::try_new(Q6_PREDICATE, &unif_in.schema()).unwrap();
    let unif: Arc<dyn ExecutionPlan> =
        Arc::new(FusedAggregateExec::try_new(unif_in, spec).unwrap());

    // Correctness
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

    let t_hand = time_exec(hand).await;
    let t_unif = time_exec(unif).await;
    (t_hand, t_unif)
}

async fn bench_q1(ctx: &SessionContext) -> (f64, f64) {
    let hand_in = q1_input(ctx).await;
    let unif_in = q1_input(ctx).await;
    let hand = Arc::new(FusedFilterMultiAggExec::try_new_q1(hand_in, Q1_PREDICATE).unwrap());
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
    // Spot-check sum_qty (col 2) for the first group.
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

    let t_hand = time_exec(hand).await;
    let t_unif = time_exec(unif).await;
    (t_hand, t_unif)
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
    println!("  trials:  {} (after one warm-up)", TRIALS);
    println!();

    let (h6, u6) = bench_q6(&ctx).await;
    let ratio6 = u6 / h6;
    let ok6 = ratio6 <= 1.03;
    println!(
        "  Q6  FusedFilterSumExec        {h6:>7.3} ms   FusedAggregateExec<Q6Spec> {u6:>7.3} ms   Δ {:+.2} %  {}",
        (ratio6 - 1.0) * 100.0,
        if ok6 { "PASS" } else { "FAIL" }
    );

    let (h1, u1) = bench_q1(&ctx).await;
    let ratio1 = u1 / h1;
    let ok1 = ratio1 <= 1.03;
    println!(
        "  Q1  FusedFilterMultiAggExec   {h1:>7.3} ms   FusedAggregateExec<Q1Spec> {u1:>7.3} ms   Δ {:+.2} %  {}",
        (ratio1 - 1.0) * 100.0,
        if ok1 { "PASS" } else { "FAIL" }
    );
    println!();

    if ok6 && ok1 {
        println!("  GATE PASS — operator-level Σ.G.2 within 3 % on both shapes.");
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
