//! Σ.AΩ Phase 1.1 — Plan-time `target_partitions` routing.
//!
//! Detects high-cardinality `GROUP BY` aggregations in a `LogicalPlan`
//! and recommends a `target_partitions` value that lets the
//! FinalPartitioned aggregate's per-partition hash table fit cache.
//!
//! ## Why this approach (vs. Σ.AN.1's late rewrite)
//!
//! Σ.AN.1 ran AFTER DataFusion's `EnforceDistribution` and tried to
//! rewrite individual `RepartitionExec(Hash)` partition counts. That
//! required inserting a restore-Repartition to bring the count back
//! to session default for downstream HashJoinExec(Partitioned)
//! consumers — and the restore-Repartition's cost on Q18's 15M-row
//! intermediate equaled the agg's cache-fit savings (net neutral).
//!
//! This module instead sets the SESSION'S `target_partitions` BEFORE
//! the physical optimizer runs. DataFusion's `EnforceDistribution`
//! then naturally propagates the partition count through the whole
//! plan — no manual restore needed.
//!
//! The trade-off: ALL operators in the query run at the boosted
//! partition count, not just the agg. Per Σ.AN.0's data this is
//! exactly the regime where Q18-shape queries win (-50ms wall at
//! P=112) while small-output queries (Q02, Q16, Q20) regress. The
//! plan-time detector picks the right per-query value so only the
//! agg-heavy queries get boosted.
//!
//! ## Detection
//!
//! Walks the `LogicalPlan` looking for an `Aggregate` with non-empty
//! `group_expr`. For each such node, walks deeper to find the
//! transitive input `TableScan`s, accumulating the largest table's
//! `num_rows` as an upper bound on group cardinality.
//!
//! Uses the SAME formula as Σ.AN.1:
//! `clamp(ceil(card / 50_000), cores, 8 * cores)`
//!
//! ## Opt-in
//!
//! Default OFF via `EMAT_AUTO_TARGET_PARTITIONS=1`. Follows the
//! Σ.AK/Σ.AL/Σ.AM.1 banked-infrastructure pattern: ship as opt-in
//! while we measure; flip default-on only after strict A/B confirms
//! the 22q SF=10 net is positive.

use std::sync::Arc;

use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, TableScan};

use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
use crate::fast_parquet::FastParquetTableProvider;

/// Target group count per output partition. Same constant as
/// `agg_partition_boost::TARGET_GROUPS_PER_PARTITION` (50K) — derived
/// from M3 Pro L3=12 MB.
pub const TARGET_GROUPS_PER_PARTITION: usize = 50_000;

/// Multiplier ceiling on partition count. Same constant as
/// `agg_partition_boost::MAX_PARTITIONS_MULTIPLIER` (8) — derived
/// from Σ.AN.0's Q18 partition sweep inflection.
pub const MAX_PARTITIONS_MULTIPLIER: usize = 8;

/// Minimum underlying TableScan row count for a boost to be considered.
/// Below this, query work is too small for boost overhead to amortize.
/// Σ.AΩ Phase 1.3 (2026-05-28): empirically derived from the Q11 SF=10
/// regression — partsupp at 8M boosted to 40 partitions made Q11 +33%
/// slower because the small final output couldn't absorb the
/// per-partition setup cost. 10M cleanly excludes Q11/Q16/Q22 cases
/// while keeping Q18-style high-cardinality lineitem aggregations.
pub const MIN_TABLE_SCAN_ROWS_FOR_BOOST: usize = 10_000_000;

/// Inspects a LogicalPlan and returns the recommended
/// `target_partitions` for this query. The recommendation respects
/// `session_cores` as the floor.
///
/// Returns the session_cores unchanged when no `Aggregate` is found
/// in the plan, or when the detected group cardinality fits at the
/// default partition count.
///
/// Cardinality estimate sources:
/// - For TableScans whose source is `EmatixFastParquetTableProvider`
///   or `FastParquetTableProvider`, uses `num_rows()` exactly.
/// - Fallback to a 1M default when stats are unavailable.
pub fn recommend_target_partitions(
    plan: &LogicalPlan,
    session_cores: usize,
) -> usize {
    let max_card = walk_for_max_agg_input_cardinality(plan).unwrap_or(0);
    if max_card == 0 {
        return session_cores;
    }
    compute_optimal_partitions(max_card, session_cores)
}

/// Walks `plan` looking for `Aggregate(group_expr=non-empty)` whose
/// shape is "boost-safe" — all group_expr are bare Column references
/// (no function calls / derived expressions), AND no Filter sits
/// between the Aggregate and its underlying TableScans. For each
/// safe aggregate, returns the max TableScan row count.
///
/// Returns the MAX across all qualifying aggregates, or None if no
/// aggregate qualifies.
///
/// ## Why the safety gates
///
/// Phase 1.2 strict A/B (without these gates) measured 17/22 query
/// regressions and a net +11.87% 22q SF=10. Two pathologies:
///
/// 1. **Function-call group_expr** (Q22 `substring`, Q08 `year(...)`)
///    have group cardinalities dramatically smaller than the input
///    table row count. Boosting based on input rows over-shards;
///    the small final output can't absorb the per-partition overhead.
///
/// 2. **Filtered aggregates** (Q03, Q11, Q02, Q20) have actual
///    post-filter group counts much smaller than input rows. Boost
///    again over-shards. Without runtime selectivity, we can't tell.
///
/// The bare-Column + no-Filter gate is conservative — it MISSES some
/// genuine wins (e.g., Q13's LEFT OUTER with a NOT LIKE filter) but
/// reliably AVOIDS the regressions.
fn walk_for_max_agg_input_cardinality(plan: &LogicalPlan) -> Option<usize> {
    let mut best: Option<usize> = None;
    walk_inner(plan, &mut best);
    best
}

fn walk_inner(plan: &LogicalPlan, best: &mut Option<usize>) {
    if let LogicalPlan::Aggregate(agg) = plan {
        if !agg.group_expr.is_empty()
            && all_group_expr_are_bare_columns(&agg.group_expr)
            && !has_filter_in_subtree(&agg.input)
        {
            if let Some(card) = max_table_scan_rows(&agg.input) {
                if card >= MIN_TABLE_SCAN_ROWS_FOR_BOOST {
                    *best = Some(best.map(|prev| prev.max(card)).unwrap_or(card));
                }
            }
        }
    }
    // Recurse — nested Aggregates (decorrelated subqueries) get
    // checked even if the outer Aggregate failed the gate.
    for child in plan.inputs() {
        walk_inner(child, best);
    }
}

/// Returns true iff every group_expr is `Expr::Column(_)` — no
/// function calls, no scalar expressions, no aliases over computed
/// values. Rejects Q22's `substring(c_phone, 1, 2)`, Q08's
/// `year(o_orderdate)`, and similar function-call group keys.
fn all_group_expr_are_bare_columns(group_expr: &[Expr]) -> bool {
    group_expr.iter().all(|e| matches!(e, Expr::Column(_)))
}

/// Returns true iff there's a `Filter` anywhere in the subtree
/// reachable from `plan`, OR a `TableScan` whose `filters` list is
/// non-empty (predicate pushed into the scan). Used to detect
/// aggregates whose actual group count after filtering is likely
/// much smaller than the input table size. A coarse heuristic but
/// reliable — for queries that genuinely benefit from boost
/// (Q18's subquery agg), the upstream is filter-free.
///
/// ## Why both Filter nodes AND TableScan.filters
///
/// DataFusion's logical optimizer pushes some filter predicates into
/// `TableScan.filters` (rather than keeping them as separate Filter
/// nodes) when the table provider supports filter pushdown. Without
/// also checking TableScan.filters, Phase 1.3 v2 missed Q17's
/// p_brand/p_container filter (pushed into the part TableScan) and
/// boosted Q17, causing a +24.76 ms regression.
fn has_filter_in_subtree(plan: &LogicalPlan) -> bool {
    if matches!(plan, LogicalPlan::Filter(_)) {
        return true;
    }
    if let LogicalPlan::TableScan(scan) = plan {
        if !scan.filters.is_empty() {
            return true;
        }
    }
    // For LeftSemi / LeftAnti joins, only the LEFT input carries data
    // — the RIGHT is a filter-key source (e.g., Σ.Q.L10's
    // PushDownLeftSemi puts the outer chain's filters there). Walking
    // into the right would falsely classify Q18 as filtered when its
    // inner SUM-agg's actual data side (lineitem) is unfiltered.
    if let LogicalPlan::Join(j) = plan {
        if matches!(j.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
            return has_filter_in_subtree(&j.left);
        }
    }
    plan.inputs().iter().any(|i| has_filter_in_subtree(i))
}

/// Walks `plan` downward, collecting the row count of the LARGEST
/// `TableScan` found. Walking through Filter / Projection / Join /
/// SubqueryAlias etc. — anything `inputs()` returns.
fn max_table_scan_rows(plan: &LogicalPlan) -> Option<usize> {
    let mut best: Option<usize> = None;
    walk_for_table_scans(plan, &mut best);
    best
}

fn walk_for_table_scans(plan: &LogicalPlan, best: &mut Option<usize>) {
    if let LogicalPlan::TableScan(scan) = plan {
        if let Some(n) = num_rows_for_table_scan(scan) {
            *best = Some(best.map(|prev| prev.max(n)).unwrap_or(n));
        }
    }
    for child in plan.inputs() {
        walk_for_table_scans(child, best);
    }
}

/// Get exact num_rows from one of our parquet table providers via
/// the DefaultTableSource wrapper. Returns None if the TableSource
/// is not one of our recognized providers.
fn num_rows_for_table_scan(scan: &TableScan) -> Option<usize> {
    let dts = scan.source.as_any().downcast_ref::<DefaultTableSource>()?;
    let provider = Arc::clone(&dts.table_provider);
    if let Some(emat) = provider
        .as_any()
        .downcast_ref::<EmatixFastParquetTableProvider>()
    {
        return Some(emat.num_rows());
    }
    if let Some(fp) = provider
        .as_any()
        .downcast_ref::<FastParquetTableProvider>()
    {
        return Some(fp.num_rows());
    }
    None
}

/// Compute optimal partitions via the same clamp formula as Σ.AN.1.
fn compute_optimal_partitions(expected_groups: usize, session_cores: usize) -> usize {
    if session_cores == 0 {
        return 1;
    }
    let raw = expected_groups.div_ceil(TARGET_GROUPS_PER_PARTITION);
    let ceiling = session_cores.saturating_mul(MAX_PARTITIONS_MULTIPLIER);
    raw.clamp(session_cores, ceiling)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_aggregate_returns_session_cores() {
        // We don't have a synthetic plan builder here — just verify
        // the formula and walker work in isolation.
        assert_eq!(compute_optimal_partitions(0, 14), 14); // div_ceil(0/50K)=0 → clamp(0,14,112)=14
    }

    #[test]
    fn small_aggregate_returns_session_cores() {
        // 100K rows / 50K = 2 → clamp(2, 14, 112) = 14
        assert_eq!(compute_optimal_partitions(100_000, 14), 14);
    }

    #[test]
    fn medium_aggregate_partial_boost() {
        // 1.5M rows / 50K = 30 → clamp(30, 14, 112) = 30
        assert_eq!(compute_optimal_partitions(1_500_000, 14), 30);
    }

    #[test]
    fn q18_shape_caps_at_ceiling() {
        // 60M rows / 50K = 1200 → clamp(1200, 14, 112) = 112
        assert_eq!(compute_optimal_partitions(60_000_000, 14), 112);
    }

    #[test]
    fn xeon_class_machine() {
        // 56 cores, 60M rows: ceiling = 56 × 8 = 448
        // raw = 1200, clamp(1200, 56, 448) = 448
        assert_eq!(compute_optimal_partitions(60_000_000, 56), 448);
    }

    /// Integration test against real Q18 SF=10 logical plan. Verifies
    /// the recommender finds the lineitem TableScan (60M rows) under
    /// the Aggregate, computes 112 as the optimum, and skips queries
    /// that don't have a GROUP BY. Skipped if SF=10 data is missing.
    #[tokio::test]
    async fn recommends_q18_sf10_boost() -> Result<(), Box<dyn std::error::Error>> {
        use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
        use crate::fast_parquet::FastParquetTableProvider;
        use datafusion::prelude::SessionContext;
        use std::path::PathBuf;
        use std::sync::Arc;

        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("examples/tpch/data/sf10"));
        let Some(dir) = dir else { return Ok(()) };
        if !dir.exists() {
            eprintln!("skip: sf10 data missing");
            return Ok(());
        }
        let ctx = SessionContext::new();
        for t in [
            "region", "nation", "supplier", "customer", "part", "partsupp", "orders",
            "lineitem",
        ] {
            let path = dir.join(format!("{t}.parquet"));
            if t == "lineitem" {
                let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
                ctx.register_table(t, Arc::new(prov))?;
            } else {
                let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
                ctx.register_table(t, Arc::new(prov))?;
            }
        }

        // Q18 — SUM/GROUP BY pattern over lineitem (60M rows).
        let q18 = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q18.sql"),
        )?;
        let plan_q18 = ctx.sql(&q18).await?.into_optimized_plan()?;
        let n_q18 = recommend_target_partitions(&plan_q18, 14);
        assert_eq!(n_q18, 112, "Q18 should clamp to ceiling 112 (60M ÷ 50K = 1200 clamped to 14×8)");

        // Q11 — partsupp aggregation with WHERE n_name='GERMANY' filter.
        // Phase 1.3 rejects via has_filter_in_subtree gate (avoiding
        // Q11's +32.64% regression observed without the gate).
        let q11 = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q11.sql"),
        )?;
        let plan_q11 = ctx.sql(&q11).await?.into_optimized_plan()?;
        let n_q11 = recommend_target_partitions(&plan_q11, 14);
        assert_eq!(
            n_q11, 14,
            "Q11 has filter in agg subtree → recommender should return cores"
        );

        // Q06 — no GROUP BY at all (SELECT SUM(...) without groups).
        // The Aggregate has empty group_expr; recommender returns cores.
        let q06 = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q06.sql"),
        )?;
        let plan_q06 = ctx.sql(&q06).await?.into_optimized_plan()?;
        let n_q06 = recommend_target_partitions(&plan_q06, 14);
        assert_eq!(
            n_q06, 14,
            "Q06 has no GROUP BY → recommender should return cores"
        );

        Ok(())
    }
}
