//! Σ.H.1d.1 — scaffolding for numeric-keyed `FilterMultiAggSpec`.
//!
//! ## Why this lives in a separate module
//!
//! Σ.H.1b tried to extend `FilterMultiAggSpec`'s existing
//! `GroupKeyKind` enum with `Int64`, `Int32`, `Date32`, `Float64`
//! variants. The 5×20 deep-bench (see
//! `docs/PHASE_SIGMA_H1D_DIAGNOSIS_AND_DESIGN.md`) decomposed the
//! regression into:
//!
//! - **Binary cost (~5%)**: the enum-variant additions changed
//!   LLVM's codegen for paths that don't use the new variants. Q03
//!   was +9.1% slower **even with the rule runtime-disabled**.
//! - **Exec cost (~5%)**: the rule's FilterMultiAggSpec routing was
//!   genuinely slower than DataFusion's default for the new shapes,
//!   especially multi-key (Q10 with 7 keys: +11.7% exec cost).
//!
//! Σ.H.1d's fix isolates the numeric-key handling. The existing
//! `GroupKeyKind` / `GroupKeyAccessor` / `FilterMultiAggSpec` stay
//! byte-for-byte identical to v0.3.0; this new module hosts a
//! parallel `NumericKeyKind` / `NumericKeyAccessor` /
//! `FilterMultiAggSpecNumeric` that the rule dispatches to when a
//! query has all-numeric group keys.
//!
//! The Dict-single, two-key-Utf8View, and perfect-hash templates
//! never see the new types — their codegen is unaffected.
//!
//! ## Status: scaffold only
//!
//! This module ships only the type definitions. The `AggregateSpec`
//! implementation lands in Σ.H.1d.2 (Int64-keyed); subsequent slices
//! extend to Int32 / Date32 / Float64 and add the rule's dispatch
//! logic.

use arrow_array::{Date32Array, Float64Array, Int32Array, Int64Array};

/// Group-key kind for numeric primitive columns. Mirrors the role
/// of `GroupKeyKind` in `fused_aggregate_filter_multi_agg`, but
/// the two enums never appear in the same `match` — keeping them
/// disjoint preserves codegen of the existing string-keyed path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericKeyKind {
    /// Fixed-width 8-byte primitive group key. Reads `Int64Array`
    /// per row.
    Int64,
    /// Fixed-width 4-byte primitive. Reads `Int32Array`.
    Int32,
    /// Fixed-width 4-byte primitive. Reads `Date32Array`
    /// (days-since-epoch i32).
    Date32,
    /// Fixed-width 8-byte primitive. Reads `Float64Array` and uses
    /// the raw bit pattern as the hash key (NaN/-0.0 collide as
    /// per `f64::to_bits`).
    Float64,
}

impl NumericKeyKind {
    /// Byte width of one packed key cell. Used by Σ.H.1d.2's
    /// composite key buffer when multiple numeric keys are packed.
    pub fn byte_width(self) -> usize {
        match self {
            NumericKeyKind::Int64 | NumericKeyKind::Float64 => 8,
            NumericKeyKind::Int32 | NumericKeyKind::Date32 => 4,
        }
    }
}

/// Per-batch typed accessor for numeric group-key columns. Built
/// once per batch by downcasting each `ArrayRef` into its concrete
/// primitive array. The hot loop indexes the borrowed array
/// directly — no enum dispatch needed once the accessor is in
/// hand because each variant carries its own typed reference.
#[allow(dead_code)]
pub(crate) enum NumericKeyAccessor<'a> {
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Date32(&'a Date32Array),
    Float64(&'a Float64Array),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_width_matches_native_size() {
        assert_eq!(NumericKeyKind::Int64.byte_width(), 8);
        assert_eq!(NumericKeyKind::Float64.byte_width(), 8);
        assert_eq!(NumericKeyKind::Int32.byte_width(), 4);
        assert_eq!(NumericKeyKind::Date32.byte_width(), 4);
    }
}
