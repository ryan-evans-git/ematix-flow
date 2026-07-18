//! `ematix-flow-engine` — the clean-room, push-based, DataFusion-free
//! analytical engine. See `docs/plans/NATIVE_ENGINE.md`.
//!
//! - **P0** proved the substrate spine on TPC-H Q6 (native vectors +
//!   push filter/sum; kill-gate `tests/q6_killgate.rs`).
//! - **P1** proved the no-materialization thesis on Q08 (`join::probe_narrow`
//!   beats the DF pull path; harness in ematix-flow-core examples).
//! - **P2** (in progress) gives the engine its own native decode
//!   (`scan_native`, DF-free) and generalizes the operators.

pub mod chunk;
pub mod exec;
pub mod join;
pub mod pipeline;
pub mod scan;
pub mod scan_native;
pub mod sched;
pub mod vector;

use std::path::Path;

use crate::chunk::DataChunk;
use crate::scan::ColKind;
use crate::scan_native::NativeColKind;
use crate::vector::LogicalType;

/// Result of the Q6 spine: the aggregate revenue and how many rows
/// passed the filter (a cheap structural check against the oracle's
/// matched-row count).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Q6Result {
    pub revenue: f64,
    pub matched: u64,
}

/// The Q6 compute over already-decoded chunks — filter then
/// `sum(l_extendedprice * l_discount)`:
///
/// ```sql
/// select sum(l_extendedprice * l_discount)
/// from lineitem
/// where l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01'
///   and l_discount between 0.06 - 0.01 and 0.06 + 0.01
///   and l_quantity < 24
/// ```
///
/// Shared by the stock-parquet and native-decode drivers so both exercise
/// one Q6 implementation. Chunk column order: 0=shipdate, 1=discount,
/// 2=extendedprice, 3=quantity.
pub fn q6_over_chunks(chunks: &[DataChunk]) -> Q6Result {
    // date32 days-since-epoch: 1994-01-01 = 8766, 1995-01-01 = 9131.
    const SHIPDATE_LO: i32 = 8766;
    const SHIPDATE_HI: i32 = 9131;
    // `between 0.06 - 0.01 and 0.06 + 0.01` folded in DECIMAL = [0.05, 0.07].
    // Folding in f64 yields 0.069999999999999996 — one ULP below stored
    // 0.07 — and silently drops the whole 0.07 bucket (~1/3 of matches).
    // LESSON for the P3 binder: constant-fold decimal literals in decimal.
    let disc_lo = 0.05_f64;
    let disc_hi = 0.07_f64;
    const QTY_HI: f64 = 24.0;

    let mut revenue = 0.0_f64;
    let mut matched = 0_u64;
    for chunk in chunks {
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
    Q6Result { revenue, matched }
}

/// Run TPC-H Q6 via the P0 stock-parquet scan (kept as a cross-check
/// reference for the native decoder).
pub fn run_tpch_q6(lineitem_path: &Path) -> Result<Q6Result, String> {
    let chunks = scan::scan_columns(
        lineitem_path,
        &[
            ("l_shipdate", ColKind::I32(LogicalType::Date32)),
            ("l_discount", ColKind::F64),
            ("l_extendedprice", ColKind::F64),
            ("l_quantity", ColKind::F64),
        ],
    )?;
    Ok(q6_over_chunks(&chunks))
}

/// Run TPC-H Q6 via the native ematix-parquet scan (the engine's real,
/// DF-free decode path). Columns by TPC-H lineitem leaf index —
/// shipdate=10, discount=6, extendedprice=5, quantity=4 — decoded in the
/// chunk order `q6_over_chunks` expects.
pub fn run_tpch_q6_native(lineitem_path: &Path) -> Result<Q6Result, String> {
    let chunks = scan_native::scan_row_groups(
        lineitem_path,
        &[
            (10, NativeColKind::I32(LogicalType::Date32)),
            (6, NativeColKind::F64),
            (5, NativeColKind::F64),
            (4, NativeColKind::F64),
        ],
    )?;
    Ok(q6_over_chunks(&chunks))
}
