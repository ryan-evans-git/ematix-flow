//! Story 2.2 TDD anchors for `BuildSideSelector`.
//!
//! Pin the cardinality-based build/probe selection rules before
//! Story 2.5 wires the selector into `EmatixHashJoinExec`. The
//! function is pure (slice-free, no I/O) so the tests are fast and
//! the surface is independent of any DataFusion / Arrow types.

use ematix_flow_hash_join::build_side::{
    BuildSide, BuildSideReason, SideStats, StatsSource, choose,
};

// ---------------------------------------------------------------------
// picks_smaller_side_when_cardinality_known
// ---------------------------------------------------------------------
#[test]
fn picks_smaller_side_when_cardinality_known() {
    let left = SideStats {
        expected_row_count: 1_000,
        expected_bytes_per_row: 16,
        source: StatsSource::DecodeCache,
    };
    let right = SideStats {
        expected_row_count: 10_000_000,
        expected_bytes_per_row: 16,
        source: StatsSource::DecodeCache,
    };
    let (side, reason) = choose(Some(&left), Some(&right));
    assert_eq!(side, BuildSide::Left);
    assert!(
        matches!(reason, BuildSideReason::SmallerRowCount),
        "expected SmallerRowCount, got {reason:?}"
    );

    // Symmetry — flip the inputs.
    let (side, reason) = choose(Some(&right), Some(&left));
    assert_eq!(side, BuildSide::Right);
    assert!(matches!(reason, BuildSideReason::SmallerRowCount));
}

// ---------------------------------------------------------------------
// picks_via_stats_when_one_side_unknown
//
// "Unknown" here means the side has only a weak stats source
// (parquet footer) rather than the high-confidence Σ.O.c
// RowGroupDecodeCache observation. The choose function still has a
// row-count estimate for both sides; the source only changes the
// confidence signal logged via metric.
// ---------------------------------------------------------------------
#[test]
fn picks_via_stats_when_one_side_unknown() {
    let known = SideStats {
        expected_row_count: 500,
        expected_bytes_per_row: 16,
        source: StatsSource::DecodeCache,
    };
    let unknown = SideStats {
        // "Unknown" w.r.t. Σ.O.c, but parquet footer still provides
        // a row-count estimate.
        expected_row_count: 10_000,
        expected_bytes_per_row: 16,
        source: StatsSource::ParquetFooter,
    };

    // known (500) is ≤10% of unknown (10_000); pick known side.
    let (side, _reason) = choose(Some(&known), Some(&unknown));
    assert_eq!(side, BuildSide::Left);

    let (side, _reason) = choose(Some(&unknown), Some(&known));
    assert_eq!(side, BuildSide::Right);
}

// ---------------------------------------------------------------------
// falls_back_to_left_side_when_no_stats
//
// DataFusion's stock default is left-side-as-build; we mirror it
// when stats are completely absent on both sides AND log the
// reason so observability picks it up.
// ---------------------------------------------------------------------
#[test]
fn falls_back_to_left_side_when_no_stats() {
    let (side, reason) = choose(None, None);
    assert_eq!(side, BuildSide::Left);
    assert!(
        matches!(reason, BuildSideReason::LeftFallbackNoStats),
        "expected LeftFallbackNoStats, got {reason:?}"
    );

    // The metric string surface — what the observability layer
    // reads to tag the run.
    assert_eq!(reason.metric_tag(), "no_stats_fallback");
}

// ---------------------------------------------------------------------
// one_side_truly_unknown_falls_back_left
//
// If one side has no stats at all (no Σ.O.c observation and no
// parquet footer — possible with synthetic / streaming sources),
// we can't compare row counts. Safer to default left than to pick
// arbitrarily.
// ---------------------------------------------------------------------
#[test]
fn one_side_truly_unknown_falls_back_left() {
    let l = SideStats {
        expected_row_count: 1_000,
        expected_bytes_per_row: 16,
        source: StatsSource::DecodeCache,
    };
    let (side, reason) = choose(Some(&l), None);
    assert_eq!(side, BuildSide::Left);
    assert!(matches!(
        reason,
        BuildSideReason::LeftFallbackOneSideUnknown
    ));

    let (side, reason) = choose(None, Some(&l));
    assert_eq!(side, BuildSide::Left);
    assert!(matches!(
        reason,
        BuildSideReason::LeftFallbackOneSideUnknown
    ));
}

// ---------------------------------------------------------------------
// tie_band_breaks_on_bytes_per_row
//
// Within ±10% relative row-count difference, the smaller hash-table
// footprint wins (smaller bytes_per_row → less memory pressure on
// the build side).
// ---------------------------------------------------------------------
#[test]
fn tie_band_breaks_on_bytes_per_row() {
    let left = SideStats {
        expected_row_count: 1_000_000,
        expected_bytes_per_row: 64, // bigger per-row payload
        source: StatsSource::DecodeCache,
    };
    let right = SideStats {
        expected_row_count: 1_050_000, // 5% bigger row count
        expected_bytes_per_row: 16,    // smaller per-row payload
        source: StatsSource::DecodeCache,
    };

    let (side, reason) = choose(Some(&left), Some(&right));
    assert_eq!(side, BuildSide::Right, "tie-band should pick smaller bytes");
    assert!(
        matches!(reason, BuildSideReason::SmallerBytesPerRow),
        "expected SmallerBytesPerRow, got {reason:?}"
    );
}

// ---------------------------------------------------------------------
// emits_metric_tag_for_each_reason
//
// Story 2.2 task list calls for a `build_side_selection_reason`
// metric; the tag string is what the observability layer attaches.
// ---------------------------------------------------------------------
#[test]
fn emits_metric_tag_for_each_reason() {
    assert_eq!(
        BuildSideReason::SmallerRowCount.metric_tag(),
        "smaller_row_count"
    );
    assert_eq!(
        BuildSideReason::SmallerBytesPerRow.metric_tag(),
        "smaller_bytes_per_row"
    );
    assert_eq!(
        BuildSideReason::LeftFallbackNoStats.metric_tag(),
        "no_stats_fallback"
    );
    assert_eq!(
        BuildSideReason::LeftFallbackOneSideUnknown.metric_tag(),
        "one_side_unknown_fallback"
    );
}
