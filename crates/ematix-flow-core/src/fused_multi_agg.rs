//! Q1 fused-aggregate substrate: predicate + group-routing + the
//! hand-coded and Cranelift-JIT'd per-batch kernels.
//!
//! Σ.G.3d retired the `FusedFilterMultiAggExec` operator that originally
//! lived here. Its role — the single-pass fused filter + 5-group
//! multi-aggregate pipeline — is now performed by the generic
//! [`crate::fused_aggregate_exec::FusedAggregateExec`] parameterised by
//! [`crate::fused_aggregate::Q1Spec`]. The per-batch kernels in this
//! module are the substrate `Q1Spec::process_batch` delegates to; they
//! are stable, fast, and reused unchanged.
//!
//! Original Σ.D2 history: 3.08 ms on TPC-H Q1 SF=1 / 14 threads, 15.5×
//! faster than DataFusion's `FilterExec → AggregateExec` path. The
//! Σ.G.3b perf gate confirmed identical wall-clock through the trait.
//!
//! See `docs/PHASE_SIGMA_G3_JIT_IN_TRAIT.md` for the retirement plan
//! that landed this consolidation.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Date32Array, Float64Array, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
    StringViewArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DfResult;

/// Filter parameters for the Q1 shape.
///
/// The TPC-H spec's `l_shipdate <= date '1998-12-01' - interval '90' day`
/// reduces to `l_shipdate <= Date32(10471)`. The operator's hot loop
/// evaluates `ship_v[i] > shipdate_cutoff { continue; }` inline.
#[derive(Debug, Clone, Copy)]
pub struct Q1Predicate {
    pub shipdate_cutoff: i32,
}

/// Per-group running aggregates. Five SUMs + a row COUNT cover all eight
/// Q1 output columns — AVG(qty), AVG(price), AVG(disc) are computed from
/// the sums divided by `count` at finalize time.
///
/// `pub` so [`crate::fused_aggregate::Q1Spec`] can carry it as its
/// `Accumulator` type. The fields stay public for the same reason —
/// the Σ.G.2 unified path needs to read/write them directly without
/// paying for accessor calls in the per-batch merge.
#[derive(Default, Debug, Clone, Copy)]
pub struct Q1Aggs {
    pub sum_qty: f64,
    pub sum_price: f64,
    pub sum_disc_price: f64,
    pub sum_charge: f64,
    pub sum_disc: f64,
    pub count: u64,
}

impl Q1Aggs {
    pub fn merge(&mut self, other: &Q1Aggs) {
        self.sum_qty += other.sum_qty;
        self.sum_price += other.sum_price;
        self.sum_disc_price += other.sum_disc_price;
        self.sum_charge += other.sum_charge;
        self.sum_disc += other.sum_disc;
        self.count += other.count;
    }
}

/// Renamed from `ColumnIndices` for Σ.G.2 to avoid name shadowing with
/// `fused::ColumnIndices` once both are `pub`. Fields stay public so
/// the bench / Q1Spec can construct one without a builder.
#[derive(Debug, Clone, Copy)]
pub struct Q1ColumnIndices {
    pub rflag: usize,
    pub lstatus: usize,
    pub qty: usize,
    pub price: usize,
    pub disc: usize,
    pub tax: usize,
    pub ship: usize,
}

/// Hardcoded TPC-H Q1 group routing. Index 4 is a junk catch-all so the
/// inner loop's `groups[g]` indexing stays branchless even on adversarial
/// input. Order: (R,F)=0, (N,F)=1, (N,O)=2, (A,F)=3, other=4.
#[inline(always)]
fn q1_group_idx(rflag: u8, lstatus: u8) -> usize {
    match (rflag, lstatus) {
        (b'R', b'F') => 0,
        (b'N', b'F') => 1,
        (b'N', b'O') => 2,
        (b'A', b'F') => 3,
        _ => 4,
    }
}

/// Per-batch fused filter + 5-group multi-aggregate update via the
/// Cranelift-JIT'd kernel. `cells` is the running 30-cell accumulator
/// (5 groups × 6 aggs, row-major). The JIT pre-seeds from `cells` at
/// entry and stores back at exit, so calling this in a loop accumulates
/// across batches.
///
/// `pub` for Σ.G.3b so [`crate::fused_aggregate::Q1Spec`] can drive its
/// JIT path through the same kernel the hand operator uses. The hand
/// path's worker manages the 30-cell buffer itself and converts to
/// `[Q1Aggs; 5]` once at end-of-stream; the trait path keeps the
/// canonical `[Q1Aggs; 5]` accumulator and uses [`process_q1_batch_jit_into_groups`]
/// to handle the per-batch conversion.
#[inline]
pub fn process_q1_batch_jit(
    batch: &RecordBatch,
    idx: Q1ColumnIndices,
    jit: &crate::fused_jit::FusedFilterAggJit,
    cells: &mut [f64; 30],
) {
    let rflag = batch
        .column(idx.rflag)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("validated as Utf8View");
    let lstatus = batch
        .column(idx.lstatus)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("validated as Utf8View");
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
    let tax = batch
        .column(idx.tax)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("validated as Float64");
    let ship = batch
        .column(idx.ship)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("validated as Date32");

    // Column order must match `FusedFilterAggSpec::q1()`: returnflag,
    // linestatus, quantity, extprice, discount, tax, shipdate. For
    // Utf8View columns we pass the raw views buffer (16 bytes per row).
    let inputs: [*const u8; 7] = [
        rflag.views().as_ptr().cast::<u8>(),
        lstatus.views().as_ptr().cast::<u8>(),
        qty.values().as_ptr().cast::<u8>(),
        price.values().as_ptr().cast::<u8>(),
        disc.values().as_ptr().cast::<u8>(),
        tax.values().as_ptr().cast::<u8>(),
        ship.values().as_ptr().cast::<u8>(),
    ];
    debug_assert_eq!(jit.n_outputs(), 30);
    // SAFETY: Arrow guarantees each slice has at least `batch.num_rows()`
    // elements; views() is also `num_rows()` views long; cells holds
    // exactly `jit.n_outputs() == 30` f64 cells.
    unsafe {
        jit.run(batch.num_rows() as i64, inputs.as_ptr(), cells.as_mut_ptr());
    }
}

/// Σ.G.3b adapter: run the Q1 JIT and add the result into a
/// `[Q1Aggs; 5]` accumulator.
///
/// The JIT writes its 30-cell scratch in row-major (group, agg) order
/// matching `FusedFilterAggSpec::q1()`:
///   `(sum_qty, sum_price, sum_disc_price, sum_charge, sum_disc, count)`
/// per group. We seed the scratch with the *current* accumulator
/// values so the JIT's read-modify-store semantics keep accumulating
/// across batches, then write the updated cells back to the typed
/// `Q1Aggs` representation.
///
/// Cost: 30 f64 reads to seed + 30 f64 reads to write back, ~120 bytes
/// of cache traffic per batch — negligible vs the JIT itself
/// (validated by [`sigma_g3b_q1spec_jit_vs_hand_jit`]).
#[inline]
pub fn process_q1_batch_jit_into_groups(
    batch: &RecordBatch,
    idx: Q1ColumnIndices,
    jit: &crate::fused_jit::FusedFilterAggJit,
    groups: &mut [Q1Aggs; 5],
) {
    // Seed the 30-cell scratch from the current per-group running totals.
    let mut cells: [f64; 30] = [0.0; 30];
    for (g, group) in groups.iter().enumerate() {
        let base = g * 6;
        cells[base] = group.sum_qty;
        cells[base + 1] = group.sum_price;
        cells[base + 2] = group.sum_disc_price;
        cells[base + 3] = group.sum_charge;
        cells[base + 4] = group.sum_disc;
        cells[base + 5] = group.count as f64;
    }
    process_q1_batch_jit(batch, idx, jit, &mut cells);
    // Write the updated cells back to the typed groups.
    for (g, group) in groups.iter_mut().enumerate() {
        let base = g * 6;
        group.sum_qty = cells[base];
        group.sum_price = cells[base + 1];
        group.sum_disc_price = cells[base + 2];
        group.sum_charge = cells[base + 3];
        group.sum_disc = cells[base + 4];
        group.count = cells[base + 5] as u64;
    }
}

/// Per-batch fused filter + 5-group multi-aggregate update (hand-coded
/// Rust). Same semantics as `process_q1_batch_jit` but accumulates
/// directly into `[Q1Aggs; 5]`.
/// Exposed `pub` for Σ.G.2 — `fused_aggregate::Q1Spec::process_batch`
/// delegates here. `#[inline]` matches the hint on the Q1Spec wrapper
/// so cross-crate inlining is fair on both sides (the perf-gate bench
/// is in `examples/`, which is its own crate).
#[inline]
pub fn process_q1_batch_hand(
    batch: &RecordBatch,
    p: Q1Predicate,
    idx: Q1ColumnIndices,
    groups: &mut [Q1Aggs; 5],
) {
    let rflag = batch
        .column(idx.rflag)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("validated as Utf8View");
    let lstatus = batch
        .column(idx.lstatus)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("validated as Utf8View");
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
    let tax = batch
        .column(idx.tax)
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
    let tax_v = tax.values();
    let ship_v = ship.values();
    for i in 0..batch.num_rows() {
        if ship_v[i] > p.shipdate_cutoff {
            continue;
        }
        let r = rflag.value(i).as_bytes()[0];
        let l = lstatus.value(i).as_bytes()[0];
        let g = q1_group_idx(r, l);

        let q = qty_v[i];
        let pr = price_v[i];
        let d = disc_v[i];
        let t = tax_v[i];

        let omd = 1.0 - d;
        let disc_price = pr * omd;
        let charge = disc_price * (1.0 + t);

        let a = &mut groups[g];
        a.sum_qty += q;
        a.sum_price += pr;
        a.sum_disc_price += disc_price;
        a.sum_charge += charge;
        a.sum_disc += d;
        a.count += 1;
    }
}

/// The four TPC-H Q1 groups in canonical `ORDER BY l_returnflag,
/// l_linestatus` order — the catch-all bucket (index 4) is dropped from
/// output. Returning an empty group is permitted (zero matching rows
/// for some predicates / inputs); we emit it with `count_order = 0` and
/// `NaN` for AVGs — matches DataFusion's group_by-result conventions.
const Q1_OUTPUT_ORDER: [(usize, &str, &str); 4] = [
    (3, "A", "F"), // sorted first: 'A' < 'N' < 'R'
    (1, "N", "F"),
    (2, "N", "O"),
    (0, "R", "F"),
];

/// Exposed `pub` for Σ.G.2 — `fused_aggregate::Q1Spec::finalize`
/// delegates here so the unified-vs-hand bench compares apples to
/// apples for the build-output path too, not just the hot loop.
pub fn q1_groups_to_record_batch(schema: SchemaRef, groups: &[Q1Aggs; 5]) -> DfResult<RecordBatch> {
    let mut rflag_b = StringBuilder::with_capacity(4, 4);
    let mut lstatus_b = StringBuilder::with_capacity(4, 4);
    let mut sum_qty_b = Float64Builder::with_capacity(4);
    let mut sum_price_b = Float64Builder::with_capacity(4);
    let mut sum_disc_price_b = Float64Builder::with_capacity(4);
    let mut sum_charge_b = Float64Builder::with_capacity(4);
    let mut avg_qty_b = Float64Builder::with_capacity(4);
    let mut avg_price_b = Float64Builder::with_capacity(4);
    let mut avg_disc_b = Float64Builder::with_capacity(4);
    let mut count_b = Int64Builder::with_capacity(4);

    for (g, rflag, lstatus) in Q1_OUTPUT_ORDER {
        let a = &groups[g];
        rflag_b.append_value(rflag);
        lstatus_b.append_value(lstatus);
        sum_qty_b.append_value(a.sum_qty);
        sum_price_b.append_value(a.sum_price);
        sum_disc_price_b.append_value(a.sum_disc_price);
        sum_charge_b.append_value(a.sum_charge);
        let cnt = a.count as f64;
        if cnt > 0.0 {
            avg_qty_b.append_value(a.sum_qty / cnt);
            avg_price_b.append_value(a.sum_price / cnt);
            avg_disc_b.append_value(a.sum_disc / cnt);
        } else {
            avg_qty_b.append_value(f64::NAN);
            avg_price_b.append_value(f64::NAN);
            avg_disc_b.append_value(f64::NAN);
        }
        count_b.append_value(a.count as i64);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(rflag_b.finish()),
        Arc::new(lstatus_b.finish()),
        Arc::new(sum_qty_b.finish()),
        Arc::new(sum_price_b.finish()),
        Arc::new(sum_disc_price_b.finish()),
        Arc::new(sum_charge_b.finish()),
        Arc::new(avg_qty_b.finish()),
        Arc::new(avg_price_b.finish()),
        Arc::new(avg_disc_b.finish()),
        Arc::new(count_b.finish()),
    ];
    Ok(RecordBatch::try_new(schema, cols)?)
}
