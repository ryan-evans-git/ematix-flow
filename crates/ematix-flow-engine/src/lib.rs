//! `ematix-flow-engine` — the clean-room, push-based, DataFusion-free
//! analytical engine. See `docs/plans/NATIVE_ENGINE.md`.
//!
//! **P0** (this milestone) proves the substrate spine on TPC-H Q6:
//! native decode → native vectors → push filter → push sum → result,
//! with no Arrow and no DataFusion in the data path. Kill-gate:
//! `tests/q6_killgate.rs`.

pub mod chunk;
pub mod join;
pub mod pipeline;
pub mod scan;
pub mod vector;

use std::path::Path;

use crate::scan::ColKind;
use crate::vector::LogicalType;

/// Result of the Q6 spine: the aggregate revenue and how many rows
/// passed the filter (a cheap structural check against the oracle's
/// matched-row count).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Q6Result {
    pub revenue: f64,
    pub matched: u64,
}

/// Run TPC-H Q6 on `lineitem_path` through the native push spine.
///
/// ```sql
/// select sum(l_extendedprice * l_discount)
/// from lineitem
/// where l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01'
///   and l_discount between 0.06 - 0.01 and 0.06 + 0.01
///   and l_quantity < 24
/// ```
pub fn run_tpch_q6(lineitem_path: &Path) -> Result<Q6Result, String> {
    // date32 days-since-epoch: 1994-01-01 = 8766, 1995-01-01 = 9131
    // (24y × 365 + 6 leap days = 8766; +365 for common-year 1994). The
    // matched-row assertion in the kill-gate catches any off-by-one.
    const SHIPDATE_LO: i32 = 8766;
    const SHIPDATE_HI: i32 = 9131;
    // Q6's `between 0.06 - 0.01 and 0.06 + 0.01` = between 0.05 and 0.07.
    // The reference engine folds these DECIMAL literals exactly (→ 0.05,
    // 0.07) and casts to double for the compare. Computing `0.06 + 0.01`
    // in f64 instead yields 0.069999999999999996 — one ULP below the
    // stored 0.07 — which silently drops the whole 0.07 bucket (~1/3 of
    // matches). LESSON for the P3 binder: constant-fold decimal literals
    // in decimal, not f64. For P0 we pin the exact folded boundaries.
    let disc_lo = 0.05_f64;
    let disc_hi = 0.07_f64;
    const QTY_HI: f64 = 24.0;

    // Column order in each chunk: 0=shipdate, 1=discount,
    // 2=extendedprice, 3=quantity.
    let chunks = scan::scan_columns(
        lineitem_path,
        &[
            ("l_shipdate", ColKind::I32(LogicalType::Date32)),
            ("l_discount", ColKind::F64),
            ("l_extendedprice", ColKind::F64),
            ("l_quantity", ColKind::F64),
        ],
    )?;

    let mut revenue = 0.0_f64;
    let mut matched = 0_u64;
    for chunk in &chunks {
        // Hoist the column slices out of the per-row predicate.
        let shipdate = chunk.col(0).as_i32();
        let discount = chunk.col(1).as_f64();
        let quantity = chunk.col(3).as_f64();

        let sel = pipeline::filter(chunk, |i| {
            (SHIPDATE_LO..SHIPDATE_HI).contains(&shipdate[i])
                && (disc_lo..=disc_hi).contains(&discount[i])
                && quantity[i] < QTY_HI
        });

        matched += sel.len() as u64;
        // sum(l_extendedprice * l_discount) = cols 2 * 1 over the selection.
        revenue += pipeline::sum_product(chunk, &sel, 2, 1);
    }

    Ok(Q6Result { revenue, matched })
}
