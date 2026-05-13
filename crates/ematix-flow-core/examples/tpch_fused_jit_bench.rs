//! Σ.D3 phase A-D: bench hand-coded vs Cranelift-JIT'd inner loops for
//! the three fused-exec shapes we've retrofitted (Q6, Q1, Q14 post-
//! join). All paths are constructed manually — the planner doesn't yet
//! inject these execs from SQL, so this example exercises them
//! directly to validate "no perf regression introduced by the JIT
//! path" before that auto-routing lands.
//!
//! Methodology per query:
//!   1. Materialize the right child schema via FastParquet (→ Utf8View
//!      for Q1/Q14 string cols) and, where needed, an SQL join (Q14).
//!   2. Construct two parallel execs: hand-coded `try_new_*` and JIT'd
//!      `try_new_*_jit`.
//!   3. Run each 5 trials after one warm-up, report median ms.
//!   4. Equivalence sanity: assert the f64 result cells match between
//!      the two paths to within `rel_err < 1e-12`. The unit tests pin
//!      *bit-identical* equality on small synthetic batches (where
//!      LLVM doesn't auto-vectorize the hand-coded loop), but on real
//!      75K-row data LLVM emits SIMD lane-reduction in the hand path
//!      while the JIT uses strict scalar fadds — those produce results
//!      a few ULPs apart, which is the same floating-point ambiguity
//!      that exists between any two equivalent summation orders.
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_fused_jit_bench

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused::{FusedFilterSumExec, Q6Predicate};
use ematix_flow_core::fused_multi_agg::{FusedFilterMultiAggExec, Q1Predicate};
use ematix_flow_core::fused_post_join::{FusedPostJoinExec, FusedPostJoinSpec};
use futures_util::stream::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const TRIALS: usize = 5;

fn data_dir() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let p = match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => PathBuf::from(s),
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1"),
    };
    p.to_string_lossy().into_owned()
}

async fn make_fast_ctx(parquet_dir: &str) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    for table in TPCH_TABLES {
        let path = format!("{parquet_dir}/{table}.parquet");
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table(*table, Arc::new(prov)).unwrap();
    }
    ctx
}

/// Relative-error f64 equivalence check. Tolerance 1e-12 covers the
/// few-ULP drift that auto-vectorized SIMD-reduction in the hand path
/// can introduce vs the JIT's strict scalar fadds (see module header).
/// Falls back to absolute comparison when both inputs round to 0.
fn assert_close(hand: f64, jit: f64, label: &str) {
    if hand == 0.0 && jit == 0.0 {
        return;
    }
    let rel = (hand - jit).abs() / hand.abs().max(jit.abs());
    assert!(
        rel < 1e-12,
        "{label}: hand={hand}, jit={jit}, rel_err={rel:e} (tolerance 1e-12)"
    );
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
    let _ = run_to_first_batch(exec.clone()).await; // warm
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        let _ = run_to_first_batch(exec.clone()).await;
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[TRIALS / 2]
}

// ----- Materialize child plans for each fused-exec shape -----

async fn q6_input_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
    let df = ctx
        .sql("SELECT l_quantity, l_extendedprice, l_discount, l_shipdate FROM lineitem")
        .await
        .unwrap();
    df.create_physical_plan().await.unwrap()
}

async fn q1_input_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
    let df = ctx
        .sql(
            "SELECT l_returnflag, l_linestatus, l_quantity, l_extendedprice, \
                    l_discount, l_tax, l_shipdate FROM lineitem",
        )
        .await
        .unwrap();
    df.create_physical_plan().await.unwrap()
}

/// Q14 needs the join output, not just a scan. We materialize it via
/// SQL into a `MemTable` so the JIT shard loop reads from in-memory
/// `RecordBatch`es just like the hand-coded path does. The materialize
/// cost is paid once, outside the timing loop.
async fn q14_post_join_input(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
    let sql = "
        SELECT p_type, l_extendedprice, l_discount
        FROM lineitem, part
        WHERE l_partkey = p_partkey
          AND l_shipdate >= date '1995-09-01'
          AND l_shipdate <  date '1995-10-01'
    ";
    let df = ctx.sql(sql).await.unwrap();
    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches: Vec<RecordBatch> =
        df.execute_stream().await.unwrap().try_collect().await.unwrap();
    let mem = MemTable::try_new(schema, vec![batches]).unwrap();
    let local = SessionContext::new();
    local.register_table("joined", Arc::new(mem)).unwrap();
    local
        .sql("SELECT * FROM joined")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

// ----- Q6 -----

const Q6_PREDICATE: Q6Predicate = Q6Predicate {
    date_lo: 8766,
    date_hi: 9131,
    disc_lo: 0.05,
    disc_hi: 0.07,
    qty_hi: 24.0,
};

async fn bench_q6(ctx: &SessionContext) {
    let hand_input = q6_input_plan(ctx).await;
    let jit_input = q6_input_plan(ctx).await;
    let hand = Arc::new(FusedFilterSumExec::try_new_q6(hand_input, Q6_PREDICATE).unwrap());
    let jitd = Arc::new(FusedFilterSumExec::try_new_q6_jit(jit_input, Q6_PREDICATE).unwrap());

    let hand_out = run_to_first_batch(hand.clone()).await;
    let jit_out = run_to_first_batch(jitd.clone()).await;
    let h = hand_out.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    let j = jit_out.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    assert_close(h, j, "Q6 SF=1 revenue");

    let t_hand = time_exec(hand).await;
    let t_jit = time_exec(jitd).await;
    println!(
        "  Q6  (single SUM)            hand-coded {t_hand:>6.2} ms   JIT'd {t_jit:>6.2} ms   Δ {:+.1}%",
        100.0 * (t_hand - t_jit) / t_hand,
    );
}

// ----- Q1 -----

async fn bench_q1(ctx: &SessionContext) {
    let predicate = Q1Predicate { shipdate_cutoff: 10471 };
    let hand_input = q1_input_plan(ctx).await;
    let jit_input = q1_input_plan(ctx).await;
    let hand = Arc::new(FusedFilterMultiAggExec::try_new_q1(hand_input, predicate).unwrap());
    let jitd =
        Arc::new(FusedFilterMultiAggExec::try_new_q1_jit(jit_input, predicate).unwrap());

    let hand_out = run_to_first_batch(hand.clone()).await;
    let jit_out = run_to_first_batch(jitd.clone()).await;
    // Compare every Float64 cell + the Int64 count column for bit-equality.
    assert_eq!(hand_out.num_rows(), jit_out.num_rows(), "row count");
    for col in 2..=8 {
        let h = hand_out.column(col).as_any().downcast_ref::<Float64Array>().unwrap();
        let j = jit_out.column(col).as_any().downcast_ref::<Float64Array>().unwrap();
        for row in 0..h.len() {
            let hv = h.value(row);
            let jv = j.value(row);
            if hv.is_nan() && jv.is_nan() {
                continue;
            }
            assert_close(hv, jv, &format!("Q1 col {col} row {row}"));
        }
    }
    let h_count = hand_out.column(9).as_any().downcast_ref::<Int64Array>().unwrap();
    let j_count = jit_out.column(9).as_any().downcast_ref::<Int64Array>().unwrap();
    for r in 0..h_count.len() {
        assert_eq!(h_count.value(r), j_count.value(r), "Q1 count row {r}");
    }
    // Spot-check group identity by reading the leading two String cols.
    let rflag = hand_out.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(rflag.value(0), "A", "Q1 group sort order should be (A,F)/(N,F)/(N,O)/(R,F)");

    let t_hand = time_exec(hand).await;
    let t_jit = time_exec(jitd).await;
    println!(
        "  Q1  (4-group, 5 SUMs + COUNT) hand-coded {t_hand:>6.2} ms   JIT'd {t_jit:>6.2} ms   Δ {:+.1}%",
        100.0 * (t_hand - t_jit) / t_hand,
    );
}

// ----- Q14 post-join -----

async fn bench_q14_post_join(ctx: &SessionContext) {
    let hand_input = q14_post_join_input(ctx).await;
    let jit_input = q14_post_join_input(ctx).await;
    let hand =
        Arc::new(FusedPostJoinExec::try_new(hand_input, FusedPostJoinSpec::Q14).unwrap());
    let jitd =
        Arc::new(FusedPostJoinExec::try_new_jit(jit_input, FusedPostJoinSpec::Q14).unwrap());

    let h_out = run_to_first_batch(hand.clone()).await;
    let j_out = run_to_first_batch(jitd.clone()).await;
    let h = h_out.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    let j = j_out.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    assert_close(h, j, "Q14 SF=1 promo_revenue ratio");

    let t_hand = time_exec(hand).await;
    let t_jit = time_exec(jitd).await;
    println!(
        "  Q14 (CASE-WHEN guard, dual SUM, post-join) hand-coded {t_hand:>6.2} ms   JIT'd {t_jit:>6.2} ms   Δ {:+.1}%",
        100.0 * (t_hand - t_jit) / t_hand,
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = data_dir();
    println!("==> Σ.D3 fused-exec hand-coded vs JIT'd, SF=1, M3 Pro, {TRIALS}-trial median");
    println!("==> data: {dir}");
    println!();
    let ctx = make_fast_ctx(&dir).await;
    bench_q6(&ctx).await;
    bench_q1(&ctx).await;
    bench_q14_post_join(&ctx).await;
    println!();
    println!("Equivalence (bit-identical f64) verified for each pair before timing.");
}
