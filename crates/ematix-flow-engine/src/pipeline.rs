//! Push operators for the P0 spine: a filter that narrows the deferred
//! selection (no compaction) and a sum-product sink. The operators are
//! general; the Q6-specific constants live in the spike driver
//! (`crate::run_tpch_q6`), honoring "no TPC-H hardcoding in engine code
//! — scaffolds/tests OK".

use crate::chunk::{DataChunk, Selection};

/// Narrow `chunk`'s current selection to the rows satisfying `pred`.
/// `pred(row)` reads columns through the chunk (via borrowed slices the
/// caller hoists out). Returns a new [`Selection`] — columns are never
/// compacted (deferred gather / anti-materialization).
pub fn filter(chunk: &DataChunk, pred: impl Fn(usize) -> bool) -> Selection {
    let mut out = Vec::new();
    chunk.sel.for_each(|i| {
        if pred(i as usize) {
            out.push(i);
        }
    });
    Selection::Indices(out)
}

/// Sum `a[i] * b[i]` over the live rows of `sel` (both F64 columns).
/// This is the terminal sink for Q6's `sum(l_extendedprice * l_discount)`.
pub fn sum_product(chunk: &DataChunk, sel: &Selection, a: usize, b: usize) -> f64 {
    let av = chunk.col(a).as_f64();
    let bv = chunk.col(b).as_f64();
    let mut acc = 0.0_f64;
    sel.for_each(|i| {
        let i = i as usize;
        acc += av[i] * bv[i];
    });
    acc
}
