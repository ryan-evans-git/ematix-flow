//! Σ.T V5 L13 Story 2.2 — build-side cardinality selector.
//!
//! Pure function over per-side stats. No DataFusion or Arrow dep —
//! the operator (Story 2.5) supplies the stats; this module decides
//! which side becomes the hash-table build and which becomes the
//! probe.
//!
//! ## OQ-L13-B resolution: stats source order
//!
//! Per `docs/plans/CURRENT.md` Phase 2 OQ-L13-B, the selector
//! consumes three potential stats sources in confidence order:
//!
//! - **Σ.O.c `RowGroupDecodeCache`** (highest confidence): measured
//!   row count + decoded bytes-per-row from a prior batch in the
//!   same session. Story 2.5's planner integration prefers this.
//! - **Parquet footer statistics**: file-level row-count totals +
//!   per-column type sizes. Universally available; uncalibrated for
//!   decoded sizes (no compression awareness), so the `source` tag
//!   downgrades the confidence.
//! - **Σ.L workload-log observations** (Phase 3 placeholder):
//!   historical per-shape cardinality from `dict_verdicts`-equivalent
//!   persistence; wires in fully when Story 3.1's `WorkloadLog`
//!   extension lands.
//!
//! When BOTH sides have stats (any source), the selector compares
//! row counts directly. When one side is `None`, the safest default
//! is left-side-as-build (DataFusion's stock convention) — picking
//! the known side arbitrarily could regress shapes the operator's
//! per-query opt-out (Story 2.5's `EMAT_HASH_JOIN=0`) hasn't yet
//! validated.
//!
//! ## Selection rule
//!
//! - `(None, None)` → Left + `LeftFallbackNoStats` (metric tag
//!   `no_stats_fallback`).
//! - `(Some, None)` or `(None, Some)` → Left +
//!   `LeftFallbackOneSideUnknown` (metric tag
//!   `one_side_unknown_fallback`).
//! - `(Some(l), Some(r))` and `|l.rows - r.rows| / max(l.rows, r.rows)
//!   < 10%` (tie band) → pick the side with smaller `bytes_per_row`
//!   → `SmallerBytesPerRow`.
//! - `(Some(l), Some(r))` otherwise → pick the side with smaller
//!   `expected_row_count` → `SmallerRowCount`. Ties at exactly equal
//!   row counts go left.

/// Confidence-tagged source for a side's cardinality estimate.
///
/// Carried through the selector so the metric layer can record
/// *why* the choice was made, not just *what*. Σ.L workload-log
/// per-shape verdicts will become a fourth source in Phase 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatsSource {
    /// Σ.O.c `RowGroupDecodeCache` observation from an earlier
    /// batch in the same session. Highest confidence.
    DecodeCache,
    /// Parquet file footer statistics. Always available for parquet
    /// scans; uncalibrated for decoded sizes.
    ParquetFooter,
    /// Σ.L workload-log persisted observation. Phase 3 wiring;
    /// the variant exists now so the selector's type surface is
    /// stable when L14 lands.
    WorkloadLog,
}

/// Per-side cardinality + row-size estimate. The operator computes
/// this from whichever source is available at planning time and
/// passes it to [`choose`].
#[derive(Debug, Clone, Copy)]
pub struct SideStats {
    /// Estimated row count for the side. Upper bound is fine —
    /// the selector compares relative magnitudes, not absolutes.
    pub expected_row_count: u64,
    /// Estimated bytes per row of the join-key payload. Used for
    /// tie-breaking in the ±10% row-count band: smaller payload
    /// means smaller hash-table footprint on the build side.
    pub expected_bytes_per_row: u64,
    /// Confidence tag. Surfaced via metric for observability;
    /// does not affect the selection rule itself (the same row
    /// count from any source produces the same choice — the
    /// **caller** controls which source it pulls from).
    pub source: StatsSource,
}

/// Which side of the join becomes the hash-table build side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuildSide {
    Left,
    Right,
}

/// Why the selector picked the side it did. Carried through to the
/// `build_side_selection_reason` metric per Story 2.2's task list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuildSideReason {
    /// Both sides have stats, picked side has the smaller
    /// `expected_row_count` (outside the ±10% tie band).
    SmallerRowCount,
    /// Both sides have stats, row counts are within ±10% of each
    /// other; picked side has the smaller `expected_bytes_per_row`
    /// → smaller hash-table footprint.
    SmallerBytesPerRow,
    /// Neither side has stats. Defaulted to left side per
    /// DataFusion's stock convention.
    LeftFallbackNoStats,
    /// Exactly one side has stats; cannot compare row counts
    /// confidently, so defaulted to left side.
    LeftFallbackOneSideUnknown,
}

impl BuildSideReason {
    /// Stable metric-tag string for the observability layer. The
    /// `build_side_selection_reason` Counter/Label uses this as
    /// the dimension value.
    pub fn metric_tag(self) -> &'static str {
        match self {
            BuildSideReason::SmallerRowCount => "smaller_row_count",
            BuildSideReason::SmallerBytesPerRow => "smaller_bytes_per_row",
            BuildSideReason::LeftFallbackNoStats => "no_stats_fallback",
            BuildSideReason::LeftFallbackOneSideUnknown => "one_side_unknown_fallback",
        }
    }
}

/// Tie-band: row counts whose relative difference is below this
/// threshold are treated as tied for the purpose of build-side
/// selection. Within the band, `expected_bytes_per_row` breaks
/// the tie.
///
/// Expressed as a numerator-over-100 so the comparison is integer
/// arithmetic: `(max - min) * 100 < TIE_BAND_NUMERATOR * max`.
const TIE_BAND_NUMERATOR: u64 = 10; // 10% relative

/// Decide which side becomes the hash-table build, given each
/// side's cardinality estimate (or `None` if no stats available).
///
/// See module docstring for the full selection rule.
pub fn choose(left: Option<&SideStats>, right: Option<&SideStats>) -> (BuildSide, BuildSideReason) {
    match (left, right) {
        (None, None) => (BuildSide::Left, BuildSideReason::LeftFallbackNoStats),
        (Some(_), None) | (None, Some(_)) => {
            (BuildSide::Left, BuildSideReason::LeftFallbackOneSideUnknown)
        }
        (Some(l), Some(r)) => choose_with_stats(l, r),
    }
}

fn choose_with_stats(l: &SideStats, r: &SideStats) -> (BuildSide, BuildSideReason) {
    let lc = l.expected_row_count;
    let rc = r.expected_row_count;
    let max = lc.max(rc);
    let min = lc.min(rc);

    // Edge: both zero → conventional left.
    if max == 0 {
        return (BuildSide::Left, BuildSideReason::SmallerRowCount);
    }

    // Tie band: |max - min| * 100 < 10 * max  ⟺  diff < 10% of max.
    let in_tie_band = (max - min) * 100 < TIE_BAND_NUMERATOR * max;

    if in_tie_band {
        if l.expected_bytes_per_row <= r.expected_bytes_per_row {
            (BuildSide::Left, BuildSideReason::SmallerBytesPerRow)
        } else {
            (BuildSide::Right, BuildSideReason::SmallerBytesPerRow)
        }
    } else if lc <= rc {
        (BuildSide::Left, BuildSideReason::SmallerRowCount)
    } else {
        (BuildSide::Right, BuildSideReason::SmallerRowCount)
    }
}

// ---------------------------------------------------------------------
// In-module unit tests for the helpers that aren't part of the
// integration TDD anchors in tests/build_side_selection.rs.
// ---------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_zero_picks_left_smaller_row_count() {
        let z = SideStats {
            expected_row_count: 0,
            expected_bytes_per_row: 16,
            source: StatsSource::DecodeCache,
        };
        let (side, reason) = choose(Some(&z), Some(&z));
        assert_eq!(side, BuildSide::Left);
        assert!(matches!(reason, BuildSideReason::SmallerRowCount));
    }

    #[test]
    fn exact_equal_row_counts_pick_left() {
        let s = SideStats {
            expected_row_count: 1_000,
            expected_bytes_per_row: 16,
            source: StatsSource::DecodeCache,
        };
        let (side, reason) = choose(Some(&s), Some(&s));
        assert_eq!(side, BuildSide::Left);
        // Equal counts are within the tie band → SmallerBytesPerRow
        // (with equal bytes, falls to Left per the `<=` rule).
        assert!(matches!(reason, BuildSideReason::SmallerBytesPerRow));
    }

    #[test]
    fn just_outside_tie_band_picks_smaller_row_count() {
        // 11% relative difference — outside the 10% tie band.
        let l = SideStats {
            expected_row_count: 100,
            expected_bytes_per_row: 64, // bigger row, smaller table
            source: StatsSource::DecodeCache,
        };
        let r = SideStats {
            expected_row_count: 113, // 13% bigger
            expected_bytes_per_row: 16,
            source: StatsSource::DecodeCache,
        };
        let (side, reason) = choose(Some(&l), Some(&r));
        assert_eq!(side, BuildSide::Left);
        assert!(matches!(reason, BuildSideReason::SmallerRowCount));
    }

    #[test]
    fn metric_tags_are_distinct() {
        let tags = [
            BuildSideReason::SmallerRowCount.metric_tag(),
            BuildSideReason::SmallerBytesPerRow.metric_tag(),
            BuildSideReason::LeftFallbackNoStats.metric_tag(),
            BuildSideReason::LeftFallbackOneSideUnknown.metric_tag(),
        ];
        let mut sorted = tags.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), tags.len(), "metric tags must be distinct");
    }
}
