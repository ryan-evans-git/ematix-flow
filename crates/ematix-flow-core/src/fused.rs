//! Q6 fused-aggregate substrate: predicate + column indices + the
//! hand-coded and Cranelift-JIT'd per-batch kernels.
//!
//! Σ.G.3d retired the `FusedFilterSumExec` operator that originally
//! lived here. Its role — the single-pass fused filter+SUM pipeline —
//! is now performed by the generic [`crate::fused_aggregate_exec::FusedAggregateExec`]
//! parameterised by [`crate::fused_aggregate::Q6Spec`]. The per-batch
//! kernels in this module are the substrate `Q6Spec::process_batch`
//! delegates to; they are stable, fast, and reused unchanged.
//!
//! Original Σ.D1 history: 1.0 ms on TPC-H Q6 SF=1 / 14 threads (vs
//! DataFusion's `FilterExec → AggregateExec` at 5.96 ms and Polars at
//! 1.9 ms). The hand kernel + JIT kernel below are the same code; the
//! Σ.G.3a perf gate confirmed identical wall-clock through the trait.
//!
//! See `docs/PHASE_SIGMA_G3_JIT_IN_TRAIT.md` for the retirement plan
//! that landed this consolidation.

use datafusion::arrow::array::{Date32Array, Float64Array, RecordBatch};

/// Closed-range parameters for the canonical TPC-H Q6 predicate.
///
/// ```text
/// l_shipdate ∈ [date_lo, date_hi)        — Date32, days since 1970-01-01
/// l_discount ∈ [disc_lo, disc_hi]        — Float64
/// l_quantity <  qty_hi                   — Float64
/// ```
///
/// Carried as plain values; the planner rule extracts these from a
/// `PhysicalExpr` AST after pattern-matching on the predicate shape
/// (see `crate::fused_jit_rule::extract_q6_predicate`).
#[derive(Debug, Clone, Copy)]
pub struct Q6Predicate {
    pub date_lo: i32,
    pub date_hi: i32,
    pub disc_lo: f64,
    pub disc_hi: f64,
    pub qty_hi: f64,
}

/// Column indices into the input `RecordBatch` for the four Q6 columns.
/// Resolved once at spec construction (via `Q6Spec::try_new` /
/// `Q6Spec::try_new_jit`) so the hot loop runs on raw `usize` indices.
#[derive(Debug, Clone, Copy)]
pub struct ColumnIndices {
    pub qty: usize,
    pub price: usize,
    pub disc: usize,
    pub ship: usize,
}

/// Per-batch fused filter + sum (hand-coded Rust). LLVM auto-vectorises
/// the inner loop; on real lineitem batches this matches the JIT path
/// to within rel_err 1e-12.
#[inline]
pub fn process_q6_batch_hand(batch: &RecordBatch, p: Q6Predicate, idx: ColumnIndices) -> f64 {
    let qty = batch
        .column(idx.qty)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("validated as Float64");
    let price = batch
        .column(idx.price)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("validated as Float64");
    let disc = batch
        .column(idx.disc)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("validated as Float64");
    let ship = batch
        .column(idx.ship)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("validated as Date32");
    let qty_v = qty.values();
    let price_v = price.values();
    let disc_v = disc.values();
    let ship_v = ship.values();
    let mut sum: f64 = 0.0;
    for i in 0..batch.num_rows() {
        let s = ship_v[i];
        let d = disc_v[i];
        let q = qty_v[i];
        if s >= p.date_lo && s < p.date_hi && d >= p.disc_lo && d <= p.disc_hi && q < p.qty_hi {
            sum += price_v[i] * d;
        }
    }
    sum
}

/// Per-batch fused filter + sum via the Cranelift-JIT'd kernel. The
/// JIT runs the same predicate-and-multiply-and-add the hand-coded
/// path does, with the constants baked in as immediates.
#[inline]
pub fn process_q6_batch_jit(
    batch: &RecordBatch,
    idx: ColumnIndices,
    jit: &crate::fused_jit::FusedFilterAggJit,
) -> f64 {
    let qty = batch
        .column(idx.qty)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("validated as Float64");
    let price = batch
        .column(idx.price)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("validated as Float64");
    let disc = batch
        .column(idx.disc)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("validated as Float64");
    let ship = batch
        .column(idx.ship)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("validated as Date32");
    // Column order must match `FusedFilterAggSpec::q6()`: shipdate,
    // discount, quantity, extprice.
    let inputs: [*const u8; 4] = [
        ship.values().as_ptr().cast::<u8>(),
        disc.values().as_ptr().cast::<u8>(),
        qty.values().as_ptr().cast::<u8>(),
        price.values().as_ptr().cast::<u8>(),
    ];
    let mut sum: [f64; 1] = [0.0];
    // SAFETY: each slice has at least `batch.num_rows()` elements
    // (Arrow's array invariant); pointer alignment is upheld by the
    // source slices' element type; `sum` has one element matching
    // the spec's single SUM aggregate.
    unsafe {
        jit.run(batch.num_rows() as i64, inputs.as_ptr(), sum.as_mut_ptr());
    }
    sum[0]
}
