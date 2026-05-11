//! Σ.D3: TPC-H Q6 benchmark for the cranelift-JIT'd predicate evaluator.
//!
//! Compares the JIT'd Q6 predicate against the Σ.D1 hard-coded reference
//! (1.0 ms parallel, in `tpch_q6_tune.rs::run_fused_parallel`). The JIT
//! emits machine code per-plan that has the same shape as the hand-
//! written inner loop, with the predicate's literal bounds baked in as
//! immediates rather than struct fields.
//!
//! If the JIT'd path lands within ~1.2× of the Σ.D1 hand-written floor,
//! the architectural premise for Σ.D3 (generic operator with no
//! per-shape performance cliff) is confirmed.
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q6_jit_bench
//!
//! Requires `examples/tpch/data/sf1/lineitem.parquet`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Date32Array, Float64Array, RecordBatch};
use datafusion::prelude::SessionContext;
use ematix_flow_core::fused_jit::Q6JitFn;
use futures_util::TryStreamExt;

/// Extracted column slices for one batch — pointer + length suitable for
/// passing into the JIT'd function. We allocate Vec<>s once for the
/// whole bench so the timed region doesn't pay per-batch allocation.
struct BatchPtrs {
    n: i64,
    shipdate: *const i32,
    discount: *const f64,
    quantity: *const f64,
    extprice: *const f64,
}

unsafe impl Send for BatchPtrs {}
unsafe impl Sync for BatchPtrs {}

fn extract_ptrs(b: &RecordBatch) -> BatchPtrs {
    // Column order matches the projection in `make_memtable_ctx` from
    // `tpch_q6_tune.rs`: 0=quantity, 1=extendedprice, 2=discount,
    // 3=shipdate.
    let qty = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    let price = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
    let disc = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    let ship = b.column(3).as_any().downcast_ref::<Date32Array>().unwrap();
    BatchPtrs {
        n: b.num_rows() as i64,
        shipdate: ship.values().as_ptr(),
        discount: disc.values().as_ptr(),
        quantity: qty.values().as_ptr(),
        extprice: price.values().as_ptr(),
    }
}

fn run_jit_single_thread(jit: &Q6JitFn, ptrs: &[BatchPtrs]) -> f64 {
    let mut sum: f64 = 0.0;
    for p in ptrs {
        // SAFETY: ptrs reference the underlying Arrow buffers held in
        // `batches`, which outlive this call.
        unsafe {
            jit.run(
                p.n, p.shipdate, p.discount, p.quantity, p.extprice, &mut sum,
            );
        }
    }
    sum
}

fn run_jit_parallel(jit: &Q6JitFn, ptrs: &[BatchPtrs], workers: usize) -> f64 {
    let n = ptrs.len();
    let chunk = n.div_ceil(workers.max(1));
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let lo = (w * chunk).min(n);
                let hi = ((w + 1) * chunk).min(n);
                let slice = &ptrs[lo..hi];
                // SAFETY: see `run_jit_single_thread`. The slice is
                // borrowed for the scoped thread's lifetime, which
                // ends before `scope` returns.
                s.spawn(move || run_jit_single_thread(jit, slice))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).sum()
    })
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
    println!("==> Σ.D3: Q6 cranelift-JIT'd predicate bench");
    println!("==> reference: Σ.D1 hand-written hardcoded parallel ~1.0 ms");
    println!();

    // Pre-decode the four Q6 columns into in-memory Arrow batches —
    // same MemTable shape used by Σ.D1.
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
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("==> preload: {} batches, {total} rows", batches.len());
    println!();

    // JIT-compile the canonical Q6 predicate. The compile itself is
    // a plan-time cost — measure it.
    let build_start = Instant::now();
    let jit = Q6JitFn::try_build_q6_canonical().expect("JIT build");
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    println!("--- JIT compile ---");
    println!("  Q6 predicate emit + verify + JIT-compile time: {build_ms:.2} ms");
    println!();

    // Extract column pointers once so the timed region doesn't pay
    // per-iteration downcast cost. (The Σ.D1 hand-written shard
    // function does the downcast inside its row loop; this is what a
    // generated operator would do too — downcast once at execute
    // start, hold typed slice references for the inner loop.)
    let ptrs: Vec<BatchPtrs> = batches.iter().map(extract_ptrs).collect();

    let ncpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);

    println!("--- Single-thread JIT'd predicate ---");
    let warm = run_jit_single_thread(&jit, &ptrs);
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let s = run_jit_single_thread(&jit, &ptrs);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
        assert!((s - warm).abs() < 1e-3, "JIT result drift: {s} vs {warm}");
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  Σ.D3 JIT, single-thread                                    median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );
    println!();

    println!("--- {ncpu}-thread JIT'd predicate ---");
    let warm = run_jit_parallel(&jit, &ptrs, ncpu);
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let s = run_jit_parallel(&jit, &ptrs, ncpu);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
        // Floating-point sum order differs between runs at this
        // parallelism; assert relative drift, not bit-equality.
        assert!(
            (s - warm).abs() / warm.abs() < 1e-9,
            "JIT parallel result drift: {s} vs {warm}",
        );
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  Σ.D3 JIT, {ncpu}-thread                                       median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );
    println!("  (sanity: revenue ≈ {warm:.4} — canonical Q6 SF=1 ≈ 123141078.2283)");
}
