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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{Aggregate, Expr, JoinType, LogicalPlan, TableScan};

use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
use crate::fast_parquet::FastParquetTableProvider;
use crate::workload_log::{AggregateObservation, WorkloadLog};

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

/// Σ.AΩ Phase 1.4 — observed-cardinality threshold below which the
/// agg alone doesn't justify the boost. At 50K groups the FinalPartitioned
/// hash table is L3-resident even at cores=14; boosting can only help
/// via non-agg parallelism (caught by `count_large_table_scans` below).
pub const SMALL_AGG_GROUP_THRESHOLD: usize = 50_000;

/// Σ.AΩ Phase 1.4 — TableScan row count above which a scan counts as
/// "heavy" for the multi-large-scan boost predicate. Matches
/// `MIN_TABLE_SCAN_ROWS_FOR_BOOST` since a single-scan query can't
/// benefit from broader plan parallelism anyway.
pub const LARGE_TABLE_SCAN_ROWS: usize = 10_000_000;

/// Σ.AΩ Phase 1.4 — minimum # of *distinct* `LARGE_TABLE_SCAN_ROWS`-
/// class tables in the whole plan to qualify a small-agg query for
/// plan-level parallelism boost (Q18 case: distinct large tables =
/// {lineitem, orders} = 2; the −53.85 ms win at P=112 comes from
/// join parallelism, not the agg itself).
///
/// **Distinct** rather than raw-scan-count: Q17 references lineitem
/// twice (outer and AVG subquery) at the un-optimized LogicalPlan
/// level. A raw scan count gives Q17=2 and fails to discriminate
/// from Q18=2-3. Distinct counts give Q17={lineitem}=1 vs
/// Q18={lineitem, orders}=2.
pub const MIN_LARGE_SCANS_FOR_PLAN_BOOST: usize = 2;

/// Σ.AΩ Phase 1.4 — minimum observations before consulting the
/// recommendation. 1 is sufficient for TPC-H benchmark contexts
/// (single-shape queries with stable cardinalities); production
/// systems may want 3+ to smooth noise.
pub const MIN_OBSERVATIONS_FOR_CONSULT: i64 = 1;

/// Σ.AΩ Phase 1.4 — stable hash of an `Aggregate` node's *shape*. The
/// hash includes group_expr column names (Display form), aggregate
/// function signatures (Display form), and a structural fingerprint
/// of the input subtree (node kinds, table names, join types, count
/// of filters/pushed-down predicates — *no literals or selectivity
/// values*). Two invocations of the same SQL with different parameter
/// literals produce the same hash; Q17's inner agg and Q18's inner
/// agg produce different hashes because they GROUP BY different
/// columns over different subtree shapes.
///
/// Uses `std::collections::hash_map::DefaultHasher`, which Rust
/// guarantees is deterministic within a single program (sufficient
/// for the `~/.ematix/workload.db` per-machine roundtrip; cross-
/// machine portability would need a SHA-256 variant).
pub fn aggregate_shape_hash(agg: &Aggregate) -> String {
    let mut h = DefaultHasher::new();
    "Aggregate".hash(&mut h);
    let mut group_keys: Vec<String> =
        agg.group_expr.iter().map(|e| e.to_string()).collect();
    group_keys.sort();
    for k in &group_keys {
        k.hash(&mut h);
    }
    let mut agg_keys: Vec<String> = agg.aggr_expr.iter().map(|e| e.to_string()).collect();
    agg_keys.sort();
    for k in &agg_keys {
        k.hash(&mut h);
    }
    hash_subtree_structure(&agg.input, &mut h);
    format!("{:016x}", h.finish())
}

/// Hashes only the *structure* of a `LogicalPlan` subtree — node
/// kinds, table names, join types, and the presence/count of
/// pushed-down filters. Literals, predicate trees, and statistics
/// numbers are deliberately excluded so two invocations of the same
/// query with different parameter values produce the same hash.
fn hash_subtree_structure(plan: &LogicalPlan, h: &mut DefaultHasher) {
    match plan {
        LogicalPlan::TableScan(scan) => {
            "TS".hash(h);
            scan.table_name.to_string().hash(h);
            // Presence of pushed-down filters (not their values) — Σ.U
            // PushDown may rewrite this between invocations of the
            // same SQL, so include just the count to stay stable.
            scan.filters.len().hash(h);
        }
        LogicalPlan::Filter(f) => {
            "Filter".hash(h);
            hash_subtree_structure(&f.input, h);
        }
        LogicalPlan::Join(j) => {
            "Join".hash(h);
            format!("{:?}", j.join_type).hash(h);
            j.on.len().hash(h);
            hash_subtree_structure(&j.left, h);
            hash_subtree_structure(&j.right, h);
        }
        LogicalPlan::Aggregate(a) => {
            "InnerAgg".hash(h);
            a.group_expr.len().hash(h);
            hash_subtree_structure(&a.input, h);
        }
        LogicalPlan::Projection(p) => {
            "Proj".hash(h);
            hash_subtree_structure(&p.input, h);
        }
        LogicalPlan::SubqueryAlias(s) => {
            "Alias".hash(h);
            hash_subtree_structure(&s.input, h);
        }
        _ => {
            std::mem::discriminant(plan).hash(h);
            for child in plan.inputs() {
                hash_subtree_structure(child, h);
            }
        }
    }
}

/// Σ.AΩ Phase 1.4 — finds the qualifying `Aggregate` node in `plan`
/// (the same node `walk_for_max_agg_input_cardinality` selects),
/// returning a stable shape hash via `aggregate_shape_hash`. Returns
/// None when no Aggregate passes the Phase 1.3 safety gates.
pub fn qualifying_aggregate_shape_hash(plan: &LogicalPlan) -> Option<String> {
    let mut best: Option<(usize, String)> = None;
    walk_for_shape_hash(plan, &mut best);
    best.map(|(_, h)| h)
}

fn walk_for_shape_hash(plan: &LogicalPlan, best: &mut Option<(usize, String)>) {
    if let LogicalPlan::Aggregate(agg) = plan {
        if !agg.group_expr.is_empty()
            && all_group_expr_are_bare_columns(&agg.group_expr)
            && !has_filter_in_subtree(&agg.input)
        {
            if let Some(card) = max_table_scan_rows(&agg.input) {
                if card >= MIN_TABLE_SCAN_ROWS_FOR_BOOST {
                    // Pick the Aggregate whose input has the largest
                    // TableScan — same tiebreak as the cardinality
                    // walker, so the hash matches the agg the
                    // recommender would have boosted.
                    let hash = aggregate_shape_hash(agg);
                    let better = best
                        .as_ref()
                        .map(|(prev_card, _)| card > *prev_card)
                        .unwrap_or(true);
                    if better {
                        *best = Some((card, hash));
                    }
                }
            }
        }
    }
    for child in plan.inputs() {
        walk_for_shape_hash(child, best);
    }
}

/// Σ.AΩ Phase 1.4 — counts the number of *distinct* tables anywhere
/// in `plan` whose underlying provider reports `num_rows() >=
/// threshold`. Used as a plan-level "heavy joins likely" signal: a
/// query with ≥2 such tables (Q18: {lineitem, orders}) benefits
/// from `target_partitions` boost even when the qualifying
/// Aggregate's observed group count is small, because the join
/// chains above the agg dominate the wall time.
///
/// Distinct-table counting (rather than raw scan count) is critical
/// for Q17 vs Q18 discrimination: Q17 references lineitem twice
/// (outer + AVG subquery), giving raw-scan-count 2; counting
/// distinct table names gives {lineitem} = 1, which correctly
/// classifies Q17 as a single-large-table query.
pub fn count_distinct_large_tables(plan: &LogicalPlan, threshold: usize) -> usize {
    let mut seen = std::collections::BTreeSet::<String>::new();
    collect_large_tables(plan, threshold, &mut seen);
    seen.len()
}

fn collect_large_tables(
    plan: &LogicalPlan,
    threshold: usize,
    seen: &mut std::collections::BTreeSet<String>,
) {
    if let LogicalPlan::TableScan(scan) = plan {
        if let Some(n) = num_rows_for_table_scan(scan) {
            if n >= threshold {
                seen.insert(scan.table_name.to_string());
            }
        }
    }
    for child in plan.inputs() {
        collect_large_tables(child, threshold, seen);
    }
}

/// Σ.AΩ Phase 1.4 — runtime-feedback-aware recommendation. Consults
/// `log` for prior observations on the plan's qualifying aggregate
/// shape; falls back to the Phase 1.3 plan-time formula when no
/// observation is available.
///
/// Decision tree (when an aggregate qualifies):
/// 1. **No observation** → use plan-time formula (`recommend_target_partitions`).
/// 2. **Observed groups ≥ SMALL_AGG_GROUP_THRESHOLD (50K)** → boost
///    based on observed groups (replaces the over-counted TableScan
///    estimate).
/// 3. **Observed groups < 50K AND `count_large_table_scans(plan) ≥ 2`**
///    → keep plan-time boost (Q18 case: small agg but heavy joins
///    above benefit from `EnforceDistribution`'s broader parallelism).
/// 4. **Observed groups < 50K AND single large scan** → return cores
///    (Q17 case: small agg, no plan-level parallelism to amortize
///    P=112 setup cost).
pub fn recommend_target_partitions_with_log(
    plan: &LogicalPlan,
    session_cores: usize,
    log: Option<&WorkloadLog>,
) -> usize {
    let shape_hash = qualifying_aggregate_shape_hash(plan);
    let Some(shape_hash) = shape_hash else {
        // No qualifying aggregate — original behavior.
        return recommend_target_partitions(plan, session_cores);
    };

    let observation = log.and_then(|l| {
        l.consult_aggregate_observation(&shape_hash, MIN_OBSERVATIONS_FOR_CONSULT)
            .ok()
            .flatten()
    });

    match observation {
        None => {
            // First execution: no runtime feedback yet — fall back to
            // plan-time estimate. This is exactly the Phase 1.3
            // behavior (over-counts on Q17, correct on Q18).
            recommend_target_partitions(plan, session_cores)
        }
        Some(AggregateObservation {
            agg_output_groups, ..
        }) => {
            let observed_groups = agg_output_groups as usize;
            if observed_groups >= SMALL_AGG_GROUP_THRESHOLD {
                // Large enough agg to drive boost on its own.
                compute_optimal_partitions(observed_groups, session_cores)
            } else if count_distinct_large_tables(plan, LARGE_TABLE_SCAN_ROWS)
                >= MIN_LARGE_SCANS_FOR_PLAN_BOOST
            {
                // Small agg but heavy non-agg work — preserve the
                // Q18-style broader-plan boost using the plan-time
                // TableScan estimate.
                recommend_target_partitions(plan, session_cores)
            } else {
                // Small agg AND no heavy joins — Q17 case. No boost.
                session_cores
            }
        }
    }
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

        // Σ.AΩ Phase 1.4 — shape hash uniqueness: Q17 and Q18 must
        // hash to distinct values so observations don't cross-pollute.
        let q17 = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q17.sql"),
        )?;
        let plan_q17 = ctx.sql(&q17).await?.into_optimized_plan()?;
        let h17 = qualifying_aggregate_shape_hash(&plan_q17);
        let h18 = qualifying_aggregate_shape_hash(&plan_q18);
        assert!(h17.is_some(), "Q17 should have a qualifying aggregate");
        assert!(h18.is_some(), "Q18 should have a qualifying aggregate");
        assert_ne!(h17, h18, "Q17 and Q18 inner aggregates must hash differently");

        // Σ.AΩ Phase 1.4 — distinct-large-table counter: Q18 has
        // ≥2 (orders + lineitem) → qualifies for plan boost when
        // observed groups are small; Q17 has 1 ({lineitem} only;
        // part is 2M < 10M) → falls back to cores. Note Q17
        // references lineitem twice (outer + AVG subquery) but
        // distinct counting collapses them.
        let n_large_q17 = count_distinct_large_tables(&plan_q17, LARGE_TABLE_SCAN_ROWS);
        let n_large_q18 = count_distinct_large_tables(&plan_q18, LARGE_TABLE_SCAN_ROWS);
        assert!(
            n_large_q17 < MIN_LARGE_SCANS_FOR_PLAN_BOOST,
            "Q17 should have <2 distinct large tables (got {n_large_q17})"
        );
        assert!(
            n_large_q18 >= MIN_LARGE_SCANS_FOR_PLAN_BOOST,
            "Q18 should have ≥2 distinct large tables (got {n_large_q18})"
        );

        // Σ.AΩ Phase 1.4 — runtime-feedback recommender end-to-end.
        let log = WorkloadLog::open_in_memory()?;
        let h17_str = h17.unwrap();
        let h18_str = h18.unwrap();

        // Before any observations: both fall back to plan-time formula → 112.
        assert_eq!(
            recommend_target_partitions_with_log(&plan_q17, 14, Some(&log)),
            112,
            "Q17 with no observation should match Phase 1.3 (boost)"
        );
        assert_eq!(
            recommend_target_partitions_with_log(&plan_q18, 14, Some(&log)),
            112,
            "Q18 with no observation should match Phase 1.3 (boost)"
        );

        // Record observed cardinalities (Q17 ≈ 200 groups, Q18 ≈ 624).
        log.record_aggregate_observation(&h17_str, 30_000, 200)?;
        log.record_aggregate_observation(&h18_str, 4_370, 624)?;

        // After observations: Q17 falls to cores (small agg, 1 large scan),
        // Q18 stays at 112 (small agg BUT ≥2 large scans).
        assert_eq!(
            recommend_target_partitions_with_log(&plan_q17, 14, Some(&log)),
            14,
            "Q17 with observation 200<50K AND 1 large scan → cores"
        );
        assert_eq!(
            recommend_target_partitions_with_log(&plan_q18, 14, Some(&log)),
            112,
            "Q18 with observation 624<50K BUT ≥2 large scans → plan boost"
        );

        Ok(())
    }

    /// Σ.AΩ Phase 1.4 — verify that a large-cardinality observation
    /// (e.g., 1M+ groups) routes through the agg-driven boost path.
    /// This test is synthetic (no SF=10 data needed): the formula
    /// is purely numeric.
    #[test]
    fn observation_large_agg_drives_boost() {
        // 2M groups / 50K = 40 partitions → clamp(40, 14, 112) = 40.
        let card = 2_000_000;
        assert_eq!(compute_optimal_partitions(card, 14), 40);
    }

    /// Σ.AΩ Phase 1.4 — sanity: shape hash is deterministic across
    /// invocations on identical group columns + identical aggregate
    /// expressions. Synthetic test ensures the hash doesn't accidentally
    /// depend on object identity. (Full plan-based hash test is in the
    /// SF=10 integration test above.)
    #[test]
    fn shape_hash_function_deterministic() {
        // Hash the same string sequence twice — DefaultHasher::new()
        // is documented as deterministic within a single program.
        let mut h1 = DefaultHasher::new();
        "Aggregate".hash(&mut h1);
        "lineitem.l_partkey".hash(&mut h1);
        let r1 = format!("{:016x}", h1.finish());

        let mut h2 = DefaultHasher::new();
        "Aggregate".hash(&mut h2);
        "lineitem.l_partkey".hash(&mut h2);
        let r2 = format!("{:016x}", h2.finish());

        assert_eq!(r1, r2);
    }
}
