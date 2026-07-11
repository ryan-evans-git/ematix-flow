//! `SampledJoinSideRule` — Σ.JS.1 (2026-07-09) sampled join-side
//! correction for Inner `Partitioned` hash joins.
//!
//! ## Why
//!
//! TPC-H Q09 at SF=100 on a 32 GB box collapses from ~6.5 s to 16–75 s
//! because DataFusion's `JoinSelection` keeps orders (150M rows) and
//! partsupp (80M rows) as hash-join BUILD sides. The probe-side
//! estimates it compares against are inflated ~15×, for two reasons:
//!
//! 1. `p_name LIKE '%green%'` gets the flat `default_selectivity`
//!    (20%) — DF's interval analyzer cannot model string patterns —
//!    while the true pass rate is 5.4%.
//! 2. The composite-key `partsupp ⋈ (part⋈lineitem…)` join estimate
//!    fans 120M → 480M rows when the true output is ~32M: the join is
//!    row-preserving (an FK containment join), but DF's cross-product
//!    style cardinality model can't see that.
//!
//! With both errors stacked, the filtered intermediate looks BIGGER
//! than the raw fact tables, so `JoinSelection` builds hash tables on
//! orders + partsupp — a ~12 GB operator peak that evicts the 13.4 GB
//! parquet page-cache working set. Every subsequent scan goes back to
//! disk and the query falls off a cliff.
//!
//! ## What this rule does
//!
//! Runs as a post-`EnforceDistribution` physical pass. For each Inner
//! `HashJoinExec` in `PartitionMode::Partitioned` (CollectLeft joins
//! are skipped — those builds were already proven small), it computes
//! an HONEST row estimate for both children with a bottom-up
//! estimator:
//!
//! - `EmatixFastParquetExec`: `partition_statistics(None).num_rows`.
//! - `FilterExec` with a string-pattern conjunct (LIKE / ILIKE /
//!   `contains`) over a Utf8-family column that resolves to an
//!   `EmatixFastParquetExec` column: SAMPLE the true selectivity —
//!   decode the FIRST row group of that column (plan-time only;
//!   one-RG decode is tens of ms and results are cached process-wide
//!   by `(file_path, predicate)`), evaluate the pattern conjunct
//!   against it, and use the measured pass rate. Row groups are
//!   bounded by the writer's RG size (typically ≤ ~1M rows; even a
//!   large 131072+-row RG stays a one-shot plan-time cost). Any
//!   non-pattern conjuncts alongside a sampled pattern are IGNORED —
//!   that over-estimates the filtered side, which biases AGAINST
//!   swapping (the safe direction). Filters with no sampleable
//!   pattern fall back to the ratio implied by DF's own statistics
//!   (`filter rows / input rows`), else the flat 0.2 default.
//! - Inner `HashJoinExec`: `est(probe) × containment multiplicity`,
//!   evaluated in BOTH orientations (a join's output cardinality is
//!   orientation-independent, and the rule swaps bottom-up — a
//!   parent join must price an already-swapped child identically).
//!   GROUNDED orientations (provably dense-unique build key) are
//!   preferred over the multiplicity-1.0 assumption; the min is
//!   taken within a grounding class (see
//!   `inner_join_output_estimate`). When a
//!   build side's SINGLE join key column is provably dense-unique
//!   from the underlying scan's pre-filter column statistics (Exact
//!   min/max/null_count with `max−min+1 == unfiltered_rows` and zero
//!   nulls — e.g. `o_orderkey`, `p_partkey`), the multiplicity is
//!   `est(build_after_filters) / key_domain` — the fraction of the
//!   FK domain that survives the build's filters. Composite or
//!   non-provably-dense keys (e.g. partsupp's `(ps_partkey,
//!   ps_suppkey)`) use multiplicity 1.0 — the pragmatic FK
//!   row-preservation assumption; the 2× swap margin protects
//!   against it being wrong.
//! - Pass-throughs (Projection / Repartition / CoalesceBatches /
//!   CoalescePartitions) inherit the child estimate.
//! - Anything else (aggregates, other join types, unknown operators):
//!   DF's own `partition_statistics(None).num_rows` if present, else
//!   UNKNOWN — and a join with an unknown side makes NO swap decision
//!   (safe default: do nothing).
//!
//! If `est(build/left) ≥ SWAP_MARGIN × est(probe/right)` (margin 2.0,
//! `EMAT_JOIN_SIDE_MARGIN`), the join's inputs are swapped via
//! `HashJoinExec::swap_inputs(PartitionMode::Partitioned)`.
//!
//! ## Partitioning repair
//!
//! `HashJoinExec::swap_inputs` documents that swapping AFTER
//! `RepartitionExec` insertion may break the join's partitioning
//! requirements — and this rule runs after `EnforceDistribution`. So
//! we mirror the `force_collect_left_semi_build_rule` repair pattern:
//! rewrite structurally, then (gated on `transformed`) re-run
//! `EnforceDistribution` on the result so the hash exchanges are
//! re-derived for the flipped on-keys. Two hardenings over the bare
//! re-run (see `optimize`): the repair is bracketed with the stock
//! pipeline's `OutputRequirements` add/remove modes (without it the
//! re-run strips the root `SortPreservingMergeExec` and ORDER BY
//! output comes back unsorted), and `EnforceSorting` re-runs after
//! `EnforceDistribution` (which is not ordering-preserving).
//!
//! ## Evidence
//!
//! Q09 SF=100, 32 GB box: builds on orders (150M) + partsupp (80M) ≈
//! 12 GB operator peak, evicting the 13.4 GB parquet page-cache
//! working set → 16–75 s (cold-cache collapse). Swapping both joins
//! onto the filtered intermediate (~32M rows) keeps the build peak
//! ~1 GB and preserves the page cache.
//!
//! ## Opt-out
//!
//! Default ON (production == bench). Kill-switch:
//! `EMAT_JOIN_SIDE_FIX=0`. Trace: `EMAT_JOIN_SIDE_TRACE=1`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use datafusion::arrow::array::{Array, BooleanArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::JoinType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, LikeExpr, Literal};
use datafusion::physical_expr::utils::split_conjunction;
use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::scalar::ScalarValue;

use crate::ematix_fast_parquet::EmatixFastParquetExec;

/// Flat DataFusion default filter selectivity — the last-resort
/// fallback when neither sampling nor DF's own statistics can price a
/// `FilterExec`.
const DEFAULT_FILTER_SELECTIVITY: f64 = 0.2;

/// See module docs.
#[derive(Debug)]
pub struct SampledJoinSideRule {
    /// Kill-switch (default ON): `EMAT_JOIN_SIDE_FIX=0` disables the
    /// rule. Snapshotted at construction (session build), like the
    /// sibling `ForceCollectLeftForSemiBoundedBuildRule` fields.
    pub enabled: bool,
    /// Swap margin: only swap when `est(build) ≥ margin × est(probe)`
    /// (default `2.0`, `EMAT_JOIN_SIDE_MARGIN`; clamped ≥ 1.0 — the
    /// flap guard that also absorbs the multiplicity-1.0 composite-key
    /// assumption being wrong).
    pub swap_margin: f64,
}

impl Default for SampledJoinSideRule {
    fn default() -> Self {
        Self {
            enabled: crate::flags::enabled("EMAT_JOIN_SIDE_FIX"),
            swap_margin: crate::flags::f64_or("EMAT_JOIN_SIDE_MARGIN", 2.0).max(1.0),
        }
    }
}

impl PhysicalOptimizerRule for SampledJoinSideRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !self.enabled {
            return Ok(plan);
        }
        let trace = crate::flags::present("EMAT_JOIN_SIDE_TRACE");
        let margin = self.swap_margin.max(1.0);
        let rewritten = plan.transform_up(|node| {
            let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            // Inner + Partitioned ONLY. CollectLeft builds were already
            // proven small (either by JoinSelection's stats or by the
            // force-collect-left rule); semi/anti swaps flip output
            // sides and are SwapSemiJoinBuildSideRule's territory.
            if !matches!(hj.join_type(), JoinType::Inner) {
                return Ok(Transformed::no(node));
            }
            if !matches!(hj.partition_mode(), PartitionMode::Partitioned) {
                return Ok(Transformed::no(node));
            }
            // Honest bottom-up estimates for BOTH sides; either
            // unknown → no decision (safe default).
            let (Some(build_est), Some(probe_est)) =
                (estimate_rows(hj.left()), estimate_rows(hj.right()))
            else {
                return Ok(Transformed::no(node));
            };
            if build_est < probe_est * margin {
                return Ok(Transformed::no(node));
            }
            if trace {
                eprintln!(
                    "[join_side] swap (est build={build_est:.0} probe={probe_est:.0}, \
                     margin={margin}); on={:?}",
                    hj.on()
                );
            }
            Ok(Transformed::yes(
                hj.swap_inputs(PartitionMode::Partitioned)?,
            ))
        })?;

        if !rewritten.transformed {
            return Ok(rewritten.data);
        }
        // Repair partitioning after the structural rewrite (the Σ.BS
        // pattern, mirroring force_collect_left_semi_build_rule):
        // swap_inputs after EnforceDistribution may leave the hash
        // exchanges keyed for the pre-swap orientation; re-running
        // EnforceDistribution re-derives them for the flipped on-keys.
        //
        // Two hardenings over the bare re-run (both bitten in this
        // rule's own correctness test):
        // - EnforceDistribution/EnforceSorting only respect the plan
        //   ROOT's ordering/distribution requirements when the stock
        //   pipeline's `OutputRequirements` wrapper is present (it is
        //   added first and removed last there). A bare re-run strips
        //   the top `SortPreservingMergeExec` and returns UNSORTED
        //   output for ORDER BY queries. Mirror the stock bracketing:
        //   add-mode → distribution → sorting → remove-mode.
        // - EnforceDistribution alone is not ordering-preserving (in
        //   the stock pipeline EnforceSorting always runs after it),
        //   so EnforceSorting must re-run as well.
        use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
        use datafusion::physical_optimizer::output_requirements::OutputRequirements;
        let repaired = OutputRequirements::new_add_mode().optimize(rewritten.data, config)?;
        let repaired = EnforceDistribution::new().optimize(repaired, config)?;
        let repaired = EnforceSorting::new().optimize(repaired, config)?;
        OutputRequirements::new_remove_mode().optimize(repaired, config)
    }

    fn name(&self) -> &str {
        "ematix_flow_sampled_join_side"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// ============================================================
// Bottom-up honest row estimator
// ============================================================

/// Honest row estimate for `plan`, or `None` when the subtree contains
/// an operator we can't price (see module docs for the node table).
pub(crate) fn estimate_rows(plan: &Arc<dyn ExecutionPlan>) -> Option<f64> {
    let any = plan.as_any();
    if any.is::<EmatixFastParquetExec>() {
        return stats_rows(plan);
    }
    if let Some(filter) = any.downcast_ref::<FilterExec>() {
        let input = estimate_rows(filter.input())?;
        return Some(input * filter_selectivity(filter));
    }
    if let Some(hj) = any.downcast_ref::<HashJoinExec>() {
        if matches!(hj.join_type(), JoinType::Inner) {
            return inner_join_output_estimate(hj);
        }
        // Semi/anti/outer joins: fall through to DF's own stats.
        return stats_rows(plan);
    }
    if is_pass_through(plan) {
        return estimate_rows(plan.children().first()?);
    }
    // Aggregates, sorts, unknown operators: trust DF's statistics if
    // it has any; otherwise the estimate is unknown and the enclosing
    // join makes no decision.
    stats_rows(plan)
}

/// `partition_statistics(None).num_rows` as f64 (Exact or Inexact).
fn stats_rows(plan: &Arc<dyn ExecutionPlan>) -> Option<f64> {
    plan.partition_statistics(None)
        .ok()
        .and_then(|s| s.num_rows.get_value().copied())
        .map(|n| n as f64)
}

/// Row-preserving pass-through operators: the estimate is the child's.
/// `CooperativeExec` is DF 53's cooperative-scheduling wrapper that
/// `EnsureCooperative` inserts around leaves — schema- and
/// row-preserving.
#[allow(deprecated)] // CoalesceBatchesExec still appears in DF 53 plans
fn is_pass_through(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let any = plan.as_any();
    any.is::<datafusion::physical_plan::projection::ProjectionExec>()
        || any.is::<datafusion::physical_plan::repartition::RepartitionExec>()
        || any.is::<datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec>()
        || any.is::<datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec>()
        || any.is::<datafusion::physical_plan::coop::CooperativeExec>()
}

/// Output estimate for an Inner hash join: `est(probe) × containment
/// multiplicity(build)`, evaluated in BOTH orientations. A join's
/// output cardinality is orientation-independent — and orientation
/// independence matters here because this rule swaps joins bottom-up,
/// so a parent join estimates over an already-swapped child. Pricing
/// only the current probe side would LOSE the containment
/// information after a swap (Q09's partsupp join: pre-swap the
/// l-side intermediate (~32M, carrying the p_name filter through the
/// dense-unique p_partkey key) is the probe; post-swap raw partsupp
/// (80M) is — and the parent orders join would then miss its own
/// inversion).
///
/// An orientation is GROUNDED when its build key is provably
/// dense-unique ([`containment_multiplicity`] returns `Some`) —
/// `probe × est(build)/domain` is then a real containment estimate.
/// An ungrounded orientation rides the multiplicity-1.0 FK
/// row-preservation ASSUMPTION, which is only plausible when the
/// probe is the FK (many) side; when the probe is the dim side the
/// build fans out and 1.0 underestimates badly (part⋈lineitem with
/// lineitem building: 107k instead of 3.2M at SF=10). So grounded
/// orientations are preferred outright; the min is taken only within
/// the same grounding class (the containment principle: the filtered
/// FK side bounds the output).
fn inner_join_output_estimate(hj: &HashJoinExec) -> Option<f64> {
    let l = estimate_rows(hj.left());
    let r = estimate_rows(hj.right());
    // Orientation A: left builds, right probes.
    let ma = containment_multiplicity(hj.left(), hj.on(), true);
    let mb = containment_multiplicity(hj.right(), hj.on(), false);
    let a = r.map(|r| (r * ma.unwrap_or(1.0), ma.is_some()));
    let b = l.map(|l| (l * mb.unwrap_or(1.0), mb.is_some()));
    match (a, b) {
        (Some((ea, ga)), Some((eb, gb))) => {
            if ga == gb {
                Some(ea.min(eb))
            } else if ga {
                Some(ea)
            } else {
                Some(eb)
            }
        }
        (Some((ea, _)), None) => Some(ea),
        (None, Some((eb, _))) => Some(eb),
        (None, None) => None,
    }
}

/// Containment multiplicity of one side of an Inner join: how many
/// output rows each opposite-side (probe) row contributes when
/// `build` is the hash side. `Some(est(build)/key_domain(build))`
/// when the single build key is provably dense-unique on its
/// underlying scan (see module docs) — capped at 1.0, a dense-unique
/// build key can never fan out. `None` when the key is composite
/// (the partsupp shape), non-column, or not provably dense: the
/// caller then falls back to the FK row-preservation assumption
/// (multiplicity 1.0, UNGROUNDED).
fn containment_multiplicity(
    build: &Arc<dyn ExecutionPlan>,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    build_is_left: bool,
) -> Option<f64> {
    if on.len() != 1 {
        return None;
    }
    let key = if build_is_left { &on[0].0 } else { &on[0].1 };
    let col = key.as_any().downcast_ref::<Column>()?;
    let domain = dense_unique_domain(build, col.index())?;
    let build_est = estimate_rows(build)?;
    Some((build_est / domain).clamp(0.0, 1.0))
}

/// If the column at `output_index` of `plan` resolves (through
/// pass-throughs, filters and column-forwarding projections) to a
/// scan-leaf column that is provably dense-unique BEFORE filtering —
/// Exact `null_count == 0` and Exact integer min/max with
/// `max − min + 1 == unfiltered_rows` — return the key domain size.
///
/// Σ.JS.2 (Q21 SF100 parted, 2026-07-11): grounding accepts BOTH scan
/// flavors — `EmatixFastParquetExec` (the single-node fast path) and
/// arrow parquet leaves (`register_parquet`/ListingTable, the
/// distributed/mesh registration), the latter through DF's own
/// statistics and ONLY where they are Exact. Before this, arrow
/// plans could never ground a PK side, every PK-FK join fell to the
/// ungrounded `min(l, r)` fallback (supplier ⨝ lineitem priced at
/// |supplier|), and the enclosing join swapped its build onto a
/// ~600M-row intermediate — a 48.6 GB peak the 32 GB bench
/// coordinators answered with a kernel OOM.
fn dense_unique_domain(plan: &Arc<dyn ExecutionPlan>, output_index: usize) -> Option<f64> {
    let (leaf, idx) = resolve_column_to_leaf(plan, output_index)?;
    let (cs, unfiltered_rows) = leaf_column_stats(leaf, idx)?;
    if !stat_zero_nulls(&cs.null_count) {
        return None;
    }
    let min = stat_int(&cs.min_value)?;
    let max = stat_int(&cs.max_value)?;
    if max < min {
        return None;
    }
    let domain = max - min + 1;
    if domain == unfiltered_rows as i128 {
        Some(domain as f64)
    } else {
        None
    }
}

/// Column statistics + unfiltered row count at a resolved scan leaf.
/// Exact-only for arrow leaves: exactness IS the honesty criterion —
/// an Inexact merge (pruned scan, stats-less file in a multi-file
/// group) must fall back to ungrounded, never fake-ground.
fn leaf_column_stats(
    leaf: &Arc<dyn ExecutionPlan>,
    idx: usize,
) -> Option<(datafusion::common::ColumnStatistics, u64)> {
    if let Some(scan) = leaf.as_any().downcast_ref::<EmatixFastParquetExec>() {
        let cs = scan.column_stats().get(idx)?.clone();
        return Some((cs, scan.num_rows() as u64));
    }
    // Union of same-schema scans — the multi-file (parted) provider
    // shape: width-pinned `EmatixInterleaveUnionExec` (Σ.MW.2), or a
    // stock `UnionExec` from any other plan source. Merge the
    // children's Exact stats: min-of-mins, max-of-maxes, summed rows
    // and nulls. Overlapping part files are naturally rejected by
    // the caller's `domain == rows` density check (their row sum
    // exceeds the merged domain), so merging stays conservative.
    if leaf
        .as_any()
        .is::<datafusion::physical_plan::union::UnionExec>()
        || leaf
            .as_any()
            .is::<crate::ematix_fast_parquet_multi::EmatixInterleaveUnionExec>()
    {
        let mut rows: u64 = 0;
        let mut min = i128::MAX;
        let mut max = i128::MIN;
        for child in leaf.children() {
            let (cleaf, cidx) = resolve_column_to_leaf(child, idx)?;
            let (cs, r) = leaf_column_stats(cleaf, cidx)?;
            if !stat_zero_nulls(&cs.null_count) {
                return None;
            }
            min = min.min(stat_int(&cs.min_value)?);
            max = max.max(stat_int(&cs.max_value)?);
            rows = rows.checked_add(r)?;
        }
        if rows == 0 || min > max || i64::try_from(min).is_err() || i64::try_from(max).is_err() {
            return None;
        }
        let cs = datafusion::common::ColumnStatistics {
            null_count: Precision::Exact(0),
            min_value: Precision::Exact(ScalarValue::Int64(Some(min as i64))),
            max_value: Precision::Exact(ScalarValue::Int64(Some(max as i64))),
            ..Default::default()
        };
        return Some((cs, rows));
    }
    // Arrow scan leaves (DataSourceExec parquet). Leaf-ness is the
    // shape guarantee: no children means the reported statistics are
    // the file footers', not something derived.
    if !leaf.children().is_empty() {
        return None;
    }
    let stats = leaf.partition_statistics(None).ok()?;
    if crate::flags::present("EMAT_JOIN_SIDE_TRACE") {
        eprintln!(
            "[join_side] arrow leaf stats: rows={:?} col[{idx}]={:?}",
            stats.num_rows,
            stats.column_statistics.get(idx),
        );
    }
    // Exact OR Inexact — see `stat_int`: a pushed predicate (even an
    // empty DynamicFilter placeholder) downgrades the tag while the
    // values stay footer-true; the caller's density identity is the
    // actual proof and safely refuses estimated counts.
    let rows = match stats.num_rows {
        Precision::Exact(n) | Precision::Inexact(n) => n as u64,
        Precision::Absent => return None,
    };
    let cs = stats.column_statistics.get(idx)?.clone();
    Some((cs, rows))
}

/// Integer bound from a `ColumnStatistics` min/max, Exact OR Inexact.
/// Σ.JS.2: DF downgrades a scan's statistics to Inexact the moment a
/// predicate (even an empty `DynamicFilter` placeholder) is pushed
/// into it, while the reported VALUES stay the footer truth. The
/// density identity (`max − min + 1 == rows`) is the proof grounding
/// leans on — a genuinely estimated count essentially never satisfies
/// it, and a post-filter-scaled row count breaks it and safely
/// refuses (see `dense_unique_grounds_on_inexact_footer_values`).
fn stat_int(p: &Precision<ScalarValue>) -> Option<i128> {
    match p {
        Precision::Exact(v) | Precision::Inexact(v) => match v {
            ScalarValue::Int64(Some(v)) => Some(*v as i128),
            ScalarValue::Int32(Some(v)) => Some(*v as i128),
            ScalarValue::UInt64(Some(v)) => Some(*v as i128),
            ScalarValue::UInt32(Some(v)) => Some(*v as i128),
            _ => None,
        },
        Precision::Absent => None,
    }
}

/// Null-count is zero, Exact or Inexact (same Σ.JS.2 rationale).
fn stat_zero_nulls(p: &Precision<usize>) -> bool {
    matches!(p, Precision::Exact(0) | Precision::Inexact(0))
}

/// Trace the column at `output_index` of `plan` down to the underlying
/// `EmatixFastParquetExec`, returning the scan and the column's index
/// in the scan's (projected) output schema. Descends through
/// pass-throughs / `FilterExec` (schema-preserving) and through
/// `ProjectionExec`s that forward the column unchanged.
fn resolve_column_to_scan(
    plan: &Arc<dyn ExecutionPlan>,
    output_index: usize,
) -> Option<(&EmatixFastParquetExec, usize)> {
    let (leaf, idx) = resolve_column_to_leaf(plan, output_index)?;
    let scan = leaf.as_any().downcast_ref::<EmatixFastParquetExec>()?;
    if idx < scan.schema().fields().len() {
        Some((scan, idx))
    } else {
        None
    }
}

/// Walk `plan`'s column at `output_index` down through pass-throughs,
/// filters and column-forwarding projections to the first
/// non-wrapper node, returning that node and the column's index in
/// its schema. Shared by sampling (which then requires an ematix
/// scan) and dense-unique grounding (which accepts any honest leaf).
fn resolve_column_to_leaf(
    plan: &Arc<dyn ExecutionPlan>,
    output_index: usize,
) -> Option<(&Arc<dyn ExecutionPlan>, usize)> {
    let any = plan.as_any();
    if let Some(proj) = any.downcast_ref::<datafusion::physical_plan::projection::ProjectionExec>()
    {
        let proj_expr = proj.expr().get(output_index)?;
        let col = proj_expr.expr.as_any().downcast_ref::<Column>()?;
        return resolve_column_to_leaf(proj.input(), col.index());
    }
    if let Some(filter) = any.downcast_ref::<FilterExec>() {
        // A DF 53 FilterExec may carry an embedded projection
        // (`FilterExec: pred, projection=[...]`): output indices are
        // then positions into that projection, which itself indexes
        // the filter's INPUT schema.
        let input_index = match filter.projection().as_ref() {
            Some(proj) => *proj.get(output_index)?,
            None => output_index,
        };
        return resolve_column_to_leaf(filter.input(), input_index);
    }
    if is_pass_through(plan) {
        return resolve_column_to_leaf(plan.children().first()?, output_index);
    }
    (output_index < plan.schema().fields().len()).then_some((plan, output_index))
}

// ============================================================
// Filter selectivity: sampled string patterns, DF-stats fallback
// ============================================================

/// Selectivity for a `FilterExec` (see module docs): measured sample
/// pass rate when a string-pattern conjunct resolves to a parquet
/// column; else DF's own stats ratio; else the flat 0.2 default.
pub(crate) fn filter_selectivity(filter: &FilterExec) -> f64 {
    let input_schema = filter.input().schema();
    let mut measured: Option<f64> = None;
    for conj in split_conjunction(filter.predicate()) {
        let Some(col) = string_pattern_column(conj, input_schema.as_ref()) else {
            continue;
        };
        let Some((scan, idx)) = resolve_column_to_scan(filter.input(), col.index()) else {
            continue;
        };
        let Some(rate) = sampled_pass_rate(scan, idx, conj) else {
            continue;
        };
        *measured.get_or_insert(1.0) *= rate;
    }
    if let Some(m) = measured {
        return m;
    }
    df_stats_selectivity(filter).unwrap_or(DEFAULT_FILTER_SELECTIVITY)
}

/// The ratio DF's own statistics imply for this filter
/// (`filter rows / input rows`), when both are known.
fn df_stats_selectivity(filter: &FilterExec) -> Option<f64> {
    let out = filter
        .partition_statistics(None)
        .ok()?
        .num_rows
        .get_value()
        .copied()?;
    let inp = filter
        .input()
        .partition_statistics(None)
        .ok()?
        .num_rows
        .get_value()
        .copied()?;
    if inp == 0 {
        return None;
    }
    Some(out as f64 / inp as f64)
}

/// If `conj` is a string-pattern predicate (LIKE / ILIKE via
/// `LikeExpr` or a `LikeMatch`-family `BinaryExpr`, or a
/// `contains(col, lit)` scalar call) over a single Utf8-family
/// `Column` with a literal pattern, return that column.
fn string_pattern_column<'a>(
    conj: &'a Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Option<&'a Column> {
    let any = conj.as_any();
    let (col, pattern): (&Column, &Arc<dyn PhysicalExpr>) =
        if let Some(like) = any.downcast_ref::<LikeExpr>() {
            (
                like.expr().as_any().downcast_ref::<Column>()?,
                like.pattern(),
            )
        } else if let Some(bin) = any.downcast_ref::<BinaryExpr>() {
            if !matches!(
                bin.op(),
                Operator::LikeMatch
                    | Operator::ILikeMatch
                    | Operator::NotLikeMatch
                    | Operator::NotILikeMatch
            ) {
                return None;
            }
            (bin.left().as_any().downcast_ref::<Column>()?, bin.right())
        } else {
            let f = any.downcast_ref::<ScalarFunctionExpr>()?;
            if f.name() != "contains" || f.args().len() != 2 {
                return None;
            }
            (f.args()[0].as_any().downcast_ref::<Column>()?, &f.args()[1])
        };
    // Pattern must be a literal — a per-row pattern can't be priced.
    pattern.as_any().downcast_ref::<Literal>()?;
    let dt = input_schema.field(col.index()).data_type();
    is_string_family(dt).then_some(col)
}

fn is_string_family(dt: &DataType) -> bool {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, inner) => is_string_family(inner),
        _ => false,
    }
}

/// Process-global sampled-selectivity cache keyed by
/// `(file_path, predicate display)` so repeated planning is free.
static SAMPLED_SELECTIVITY_CACHE: LazyLock<Mutex<HashMap<(String, String), f64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Measured pass rate of `conj` over a first-row-group sample of the
/// scan column at (projected) index `idx`. Plan-time only; cached.
/// The sample is bounded by the file's row-group size — one RG decode
/// is tens of ms even for large (131072+ row) RGs, and it happens once
/// per (file, predicate) per process.
fn sampled_pass_rate(
    scan: &EmatixFastParquetExec,
    idx: usize,
    conj: &Arc<dyn PhysicalExpr>,
) -> Option<f64> {
    let key = (scan.path().to_string(), format!("{conj}"));
    if let Some(rate) = SAMPLED_SELECTIVITY_CACHE.lock().ok()?.get(&key).copied() {
        return Some(rate);
    }
    let field = scan.schema().field(idx).clone();
    let leaf = scan.file_schema().index_of(field.name()).ok()?;
    let array = crate::emat_arrow_reader::decode_first_rg_column_for_sampling(
        scan.path(),
        leaf,
        field.data_type(),
    )
    .ok()?;
    let len = array.len();
    if len == 0 {
        return None;
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            field.name(),
            field.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .ok()?;
    // Re-point every Column in the conjunct at index 0 of the
    // single-column sample batch, then evaluate the ORIGINAL
    // predicate expression against real data.
    let rewritten = Arc::clone(conj)
        .transform(|e| {
            if let Some(c) = e.as_any().downcast_ref::<Column>() {
                Ok(Transformed::yes(
                    Arc::new(Column::new(c.name(), 0)) as Arc<dyn PhysicalExpr>
                ))
            } else {
                Ok(Transformed::no(e))
            }
        })
        .ok()?
        .data;
    let value = rewritten.evaluate(&batch).ok()?;
    let arr = value.into_array(len).ok()?;
    let bools = arr.as_any().downcast_ref::<BooleanArray>()?;
    // Floor at one matching row: a measured zero would zero out the
    // whole side estimate and manufacture maximal swaps from a sample
    // that may simply have missed the matches.
    let rate = (bools.true_count().max(1)) as f64 / len as f64;
    if let Ok(mut cache) = SAMPLED_SELECTIVITY_CACHE.lock() {
        cache.insert(key, rate);
    }
    Some(rate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use datafusion::execution::context::SessionContext;
    use datafusion::parquet::basic::{
        Compression, ConvertedType, Repetition, Type as PhysicalType,
    };
    use datafusion::parquet::column::writer::ColumnWriter;
    use datafusion::parquet::data_type::ByteArray;
    use datafusion::parquet::file::properties::WriterProperties;
    use datafusion::parquet::file::writer::SerializedFileWriter;
    use datafusion::parquet::schema::types::Type as PType;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionConfig;
    use std::fs::File;
    use std::path::{Path, PathBuf};

    // Repo trap (de-flake 23078a3a): NEVER name fixture columns
    // anything ending in `key` — collides with the scale-gated
    // EMAT_DOWNCAST_KEYS narrowing toggled by concurrent tests.
    // Columns here are `ident`, `ref_a`, `pname`, `val`.

    fn tmp_parquet(name: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "join_side_rule_test_{}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    fn i64_field(name: &str) -> Arc<PType> {
        Arc::new(
            PType::primitive_type_builder(name, PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        )
    }

    /// Dimension table: dense-unique `ident` 0..rows, string `pname`
    /// where one row in `match_every` contains "xyz".
    fn write_dim(path: &Path, rows: usize, match_every: usize) {
        write_dim_range(path, 0, rows, match_every);
    }

    /// `write_dim` with an explicit `ident` range `lo..hi` — parted
    /// PART FILES are this: contiguous, non-overlapping slices of one
    /// dense-unique domain.
    fn write_dim_range(path: &Path, lo: usize, hi: usize, match_every: usize) {
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![
                    i64_field("ident"),
                    Arc::new(
                        PType::primitive_type_builder("pname", PhysicalType::BYTE_ARRAY)
                            .with_repetition(Repetition::REQUIRED)
                            .with_converted_type(ConvertedType::UTF8)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        let ident: Vec<i64> = (lo as i64..hi as i64).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
            t.write_batch(&ident, None, None).unwrap();
        }
        col.close().unwrap();
        let names: Vec<ByteArray> = (lo..hi)
            .map(|i| {
                let s = if i % match_every == 0 {
                    format!("shiny xyz widget {i}")
                } else {
                    format!("plain widget {i}")
                };
                ByteArray::from(s.into_bytes())
            })
            .collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::ByteArrayColumnWriter(t) = col.untyped() {
            t.write_batch(&names, None, None).unwrap();
        }
        col.close().unwrap();
        rg.close().unwrap();
        writer.close().unwrap();
    }

    /// Fact table: dense-unique `ref_a` 0..rows, plus an f64 payload
    /// with exactly-summable integer values.
    fn write_fact(path: &Path, rows: usize) {
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![
                    i64_field("ref_a"),
                    Arc::new(
                        PType::primitive_type_builder("val", PhysicalType::DOUBLE)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        let ref_a: Vec<i64> = (0..rows as i64).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
            t.write_batch(&ref_a, None, None).unwrap();
        }
        col.close().unwrap();
        let val: Vec<f64> = (0..rows).map(|i| (i % 97) as f64).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::DoubleColumnWriter(t) = col.untyped() {
            t.write_batch(&val, None, None).unwrap();
        }
        col.close().unwrap();
        rg.close().unwrap();
        writer.close().unwrap();
    }

    /// Session whose JoinSelection can never pick CollectLeft (both
    /// thresholds zeroed) so the rule's Partitioned-only target shape
    /// is what the planner produces.
    fn ctx_with(tables: &[(&str, &Path)]) -> SessionContext {
        let mut config = SessionConfig::new().with_target_partitions(2);
        {
            let opts = config.options_mut();
            opts.optimizer.hash_join_single_partition_threshold = 0;
            opts.optimizer.hash_join_single_partition_threshold_rows = 0;
        }
        let ctx = SessionContext::new_with_config(config);
        for (name, path) in tables {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_str().unwrap().to_string())
                .unwrap();
            ctx.register_table(*name, Arc::new(prov)).unwrap();
        }
        ctx
    }

    async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!("{}", displayable(plan.as_ref()).indent(true))
    }

    fn find_first<T: 'static>(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.as_any().is::<T>() {
            return Some(Arc::clone(plan));
        }
        for c in plan.children() {
            if let Some(f) = find_first::<T>(c) {
                return Some(f);
            }
        }
        None
    }

    fn rule() -> SampledJoinSideRule {
        SampledJoinSideRule {
            enabled: true,
            swap_margin: 2.0,
        }
    }

    /// ConfigOptions matching `ctx_with`'s session config — the
    /// repair passes re-run with these, so target_partitions must
    /// agree with the session or the re-run rescales the exchanges.
    fn config_opts() -> ConfigOptions {
        let mut c = ConfigOptions::default();
        c.execution.target_partitions = 2;
        c.optimizer.hash_join_single_partition_threshold = 0;
        c.optimizer.hash_join_single_partition_threshold_rows = 0;
        c
    }

    /// Estimator: a bare Emat scan reports its statistics row count.
    #[tokio::test(flavor = "multi_thread")]
    async fn estimator_scan_reports_stats_rows() {
        let dim = tmp_parquet("est_scan");
        write_dim(&dim, 1_000, 100);
        let ctx = ctx_with(&[("dim", &dim)]);
        let plan = physical_plan(&ctx, "SELECT ident FROM dim").await;
        let scan = find_first::<EmatixFastParquetExec>(&plan).expect("emat scan in plan");
        let est = estimate_rows(&scan).expect("scan estimate known");
        assert!(
            (est - 1_000.0).abs() < 1e-9,
            "scan estimate should be the file row count, got {est}"
        );
    }

    /// Sampling: a LIKE '%xyz%' that truly passes 5 of 100 rows must
    /// be priced at the MEASURED ≈0.05, not the flat 0.2 default.
    #[tokio::test(flavor = "multi_thread")]
    async fn sampled_like_selectivity_beats_default() {
        let dim = tmp_parquet("est_like");
        write_dim(&dim, 100, 20); // 5 of 100 contain "xyz"
        let ctx = ctx_with(&[("dim", &dim)]);
        let plan = physical_plan(&ctx, "SELECT ident FROM dim WHERE pname LIKE '%xyz%'").await;
        let filter_node =
            find_first::<FilterExec>(&plan).expect("FilterExec retained for string LIKE");
        let filter = filter_node.as_any().downcast_ref::<FilterExec>().unwrap();
        let sel = filter_selectivity(filter);
        assert!(
            (sel - 0.05).abs() < 1e-9,
            "sampled selectivity should be the measured 5/100 = 0.05, got {sel}"
        );
        // And the full estimator prices the filter node with it.
        let est = estimate_rows(&filter_node).expect("filter estimate known");
        assert!(
            (est - 5.0).abs() < 1e-6,
            "filter estimate should be 100 × 0.05 = 5, got {est}"
        );
    }

    /// Estimator: an Inner join over a dense-unique single-key build
    /// is priced with containment multiplicity, not a cross-product
    /// fan-out. After the rule swaps the filtered dim onto the build,
    /// est(join) = est(fact probe) × est(filtered dim)/domain.
    #[tokio::test(flavor = "multi_thread")]
    async fn estimator_join_uses_containment_multiplicity() {
        let dim = tmp_parquet("est_join_dim");
        let fact = tmp_parquet("est_join_fact");
        write_dim(&dim, 100_000, 100); // 1000 match "xyz" (1%)
        write_fact(&fact, 10_000);
        let ctx = ctx_with(&[("dim", &dim), ("fact", &fact)]);
        let plan = physical_plan(
            &ctx,
            "SELECT f.ref_a FROM fact f JOIN dim d ON f.ref_a = d.ident \
             WHERE d.pname LIKE '%xyz%'",
        )
        .await;
        let out = rule().optimize(plan, &config_opts()).unwrap();
        let hj_node = find_first::<HashJoinExec>(&out).expect("hash join in plan");
        let est = estimate_rows(&hj_node).expect("join estimate known");
        // build = filtered dim (est 100_000 × 0.01 = 1000), key domain
        // = 100_000 (dense-unique ident) → multiplicity 0.01;
        // probe = fact (10_000) → est = 100.
        assert!(
            (est - 100.0).abs() < 1.0,
            "join estimate should be probe × est(build)/domain = 100, got {est}"
        );
    }

    /// Plan shape (the Q09 shape in miniature): JoinSelection keeps
    /// the big fact side as BUILD because the LIKE-filtered dim is
    /// over-estimated at 20%; the rule must swap the build onto the
    /// filtered intermediate. With `enabled: false` (the
    /// EMAT_JOIN_SIDE_FIX=0 snapshot) the plan must stay untouched.
    #[tokio::test(flavor = "multi_thread")]
    async fn q09_shape_swaps_build_to_filtered_side() {
        let dim = tmp_parquet("shape_dim");
        let fact = tmp_parquet("shape_fact");
        write_dim(&dim, 100_000, 100); // true 1%, DF prices 20%
        write_fact(&fact, 10_000);
        let ctx = ctx_with(&[("dim", &dim), ("fact", &fact)]);
        let sql = "SELECT f.ref_a, d.pname FROM fact f JOIN dim d \
                   ON f.ref_a = d.ident WHERE d.pname LIKE '%xyz%'";
        let plan = physical_plan(&ctx, sql).await;

        // Precondition: JoinSelection put the FACT scan on the build
        // (left) side — the inversion this rule exists to fix.
        let hj0 = find_first::<HashJoinExec>(&plan).expect("hash join");
        let hj0 = hj0.as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert!(
            matches!(hj0.partition_mode(), PartitionMode::Partitioned),
            "precondition: join is Partitioned\n{}",
            plan_text(&plan)
        );
        assert!(
            !plan_text(hj0.left()).contains("pname"),
            "precondition: build (left) side is the unfiltered fact:\n{}",
            plan_text(&plan)
        );

        // Kill-switch honored: enabled=false leaves the plan as-is.
        let off = SampledJoinSideRule {
            enabled: false,
            swap_margin: 2.0,
        };
        let untouched = off.optimize(Arc::clone(&plan), &config_opts()).unwrap();
        assert_eq!(
            plan_text(&plan),
            plan_text(&untouched),
            "EMAT_JOIN_SIDE_FIX=0 must leave the plan untouched"
        );

        // Rule ON: build side becomes the filtered dim intermediate.
        let out = rule().optimize(plan, &config_opts()).unwrap();
        let hj1 = find_first::<HashJoinExec>(&out).expect("hash join after rule");
        let hj1 = hj1.as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert!(
            plan_text(hj1.left()).contains("pname"),
            "after the rule the build (left) side must be the filtered \
             dim intermediate:\n{}",
            plan_text(&out)
        );
        assert!(
            matches!(hj1.partition_mode(), PartitionMode::Partitioned),
            "swap keeps Partitioned mode"
        );
    }

    /// Correctness: the swapped plan returns IDENTICAL results to the
    /// unswapped plan (deterministic aggregate + ORDER BY on both).
    #[tokio::test(flavor = "multi_thread")]
    async fn swapped_plan_returns_identical_results() {
        use datafusion::arrow::util::pretty::pretty_format_batches;
        use datafusion::physical_plan::collect;

        let dim = tmp_parquet("corr_dim");
        let fact = tmp_parquet("corr_fact");
        write_dim(&dim, 100_000, 100);
        write_fact(&fact, 10_000);
        let ctx = ctx_with(&[("dim", &dim), ("fact", &fact)]);
        let sql = "SELECT f.ref_a AS k, count(*) AS c, sum(f.val) AS s \
                   FROM fact f JOIN dim d ON f.ref_a = d.ident \
                   WHERE d.pname LIKE '%xyz%' \
                   GROUP BY f.ref_a ORDER BY k";
        let plan = physical_plan(&ctx, sql).await;
        let swapped = rule().optimize(Arc::clone(&plan), &config_opts()).unwrap();
        assert_ne!(
            plan_text(&plan),
            plan_text(&swapped),
            "control: the rule must actually have swapped something"
        );

        let task_ctx = ctx.task_ctx();
        let base = collect(plan, Arc::clone(&task_ctx)).await.unwrap();
        let after = collect(swapped, task_ctx).await.unwrap();
        let base_rows: usize = base.iter().map(|b| b.num_rows()).sum();
        assert!(base_rows > 0, "fixture query returns rows");
        assert_eq!(
            pretty_format_batches(&base).unwrap().to_string(),
            pretty_format_batches(&after).unwrap().to_string(),
            "swapped plan must return identical results"
        );
    }

    /// No-swap safety: when the margin isn't met (sides within 2×),
    /// the join stays untouched.
    #[tokio::test(flavor = "multi_thread")]
    async fn no_swap_below_margin() {
        let a = tmp_parquet("margin_a");
        let b = tmp_parquet("margin_b");
        write_fact(&a, 1_000);
        write_fact(&b, 800);
        let ctx = ctx_with(&[("ta", &a), ("tb", &b)]);
        let plan = physical_plan(
            &ctx,
            "SELECT ta.ref_a FROM ta JOIN tb ON ta.ref_a = tb.ref_a",
        )
        .await;
        let out = rule().optimize(Arc::clone(&plan), &config_opts()).unwrap();
        assert_eq!(
            plan_text(&plan),
            plan_text(&out),
            "sides within the 2× margin must not be swapped"
        );
    }

    /// No-swap safety: when one side's estimate is UNKNOWN (an
    /// aggregate subtree — DF reports Absent and the estimator stops),
    /// the rule makes no decision.
    #[tokio::test(flavor = "multi_thread")]
    async fn no_swap_when_estimate_unknown() {
        let dim = tmp_parquet("unk_dim");
        let fact = tmp_parquet("unk_fact");
        write_dim(&dim, 10_000, 100);
        write_fact(&fact, 50_000);
        let ctx = ctx_with(&[("dim", &dim), ("fact", &fact)]);
        let plan = physical_plan(
            &ctx,
            "SELECT f.ref_a FROM fact f JOIN \
             (SELECT ident, count(*) AS c FROM dim GROUP BY ident) g \
             ON f.ref_a = g.ident",
        )
        .await;
        let out = rule().optimize(Arc::clone(&plan), &config_opts()).unwrap();
        assert_eq!(
            plan_text(&plan),
            plan_text(&out),
            "a join with an unknown-side estimate must stay untouched"
        );
    }

    /// Fact table whose FK REPEATS (`ref_a = i % fk_mod`,
    /// `ref_b = i % b_mod`) — the lineitem shape, where the FK column
    /// is provably NOT dense-unique. `write_fact`'s sequential
    /// `ref_a` can't model this: it is accidentally dense-unique.
    fn write_fact_fk(path: &Path, rows: usize, fk_mod: usize, b_mod: usize) {
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![i64_field("ref_a"), i64_field("ref_b")])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        for m in [fk_mod, b_mod] {
            let vals: Vec<i64> = (0..rows).map(|i| (i % m) as i64).collect();
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
                t.write_batch(&vals, None, None).unwrap();
            }
            col.close().unwrap();
        }
        rg.close().unwrap();
        writer.close().unwrap();
    }

    /// Arrow-path session: the same session knobs as `ctx_with`, but
    /// tables registered through DF's own `register_parquet`
    /// (ListingTable) — the registration the distributed/mesh
    /// campaign path uses. Statistics collection ON so the leaves
    /// carry the Exact footer stats the estimator must ground on.
    async fn arrow_ctx_with(tables: &[(&str, &Path)]) -> SessionContext {
        let mut config = SessionConfig::new().with_target_partitions(2);
        {
            let opts = config.options_mut();
            opts.optimizer.hash_join_single_partition_threshold = 0;
            opts.optimizer.hash_join_single_partition_threshold_rows = 0;
            opts.execution.collect_statistics = true;
        }
        let ctx = SessionContext::new_with_config(config);
        for (name, path) in tables {
            ctx.register_parquet(*name, path.to_str().unwrap(), Default::default())
                .await
                .unwrap();
        }
        ctx
    }

    /// Σ.JS.2 (Q21 SF100 parted regression, 2026-07-11): a PK-FK
    /// Inner join estimated over ARROW scans must not collapse to the
    /// PK side's row count. supplier(1M) ⨝ lineitem(600M) is
    /// row-preserving on LINEITEM; the ungrounded `min(l, r)`
    /// fallback priced it at 1M, and the enclosing o_orderkey join
    /// then swapped its build onto the ~600M-row intermediate —
    /// 48.6 GB peak, kernel OOM on the 32 GB bench coordinators.
    /// Arrow parquet leaves carry Exact footer stats, so the
    /// dense-unique PK side MUST ground and the estimate MUST be the
    /// FK side.
    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_pk_fk_join_estimate_is_fk_side() {
        let dim = tmp_parquet("arrow_pkfk_dim");
        let fact = tmp_parquet("arrow_pkfk_fact");
        write_dim(&dim, 100, 100); // dense-unique ident 0..100 (the PK side)
        write_fact_fk(&fact, 10_000, 100, 2_000); // ref_a repeats ×100 (the FK side)
        let ctx = arrow_ctx_with(&[("dim", &dim), ("fact", &fact)]).await;
        let plan = physical_plan(
            &ctx,
            "SELECT f.ref_a FROM fact f JOIN dim d ON f.ref_a = d.ident",
        )
        .await;
        let hj = find_first::<HashJoinExec>(&plan).expect("hash join in arrow plan");
        let est = estimate_rows(&hj).expect("arrow PK-FK join estimate must be known");
        assert!(
            est >= 9_000.0,
            "PK-FK join over arrow scans is row-preserving on the FK \
             side (10 000 rows); the estimate must not collapse to the \
             PK side's 100, got {est}\n{}",
            plan_text(&plan)
        );
    }

    /// Q21 in miniature on the ARROW path: filtered ord JOIN
    /// (sup ⨝ li). The honest probe estimate is the 10 000-row li
    /// intermediate — far bigger than the filtered ord build — so the
    /// rule must leave the plan untouched. Under the ungrounded-min
    /// bug the probe subtree priced at |sup| = 100 and the rule
    /// swapped the build onto the li intermediate: Q21's 48.6 GB
    /// blow-up in miniature.
    #[tokio::test(flavor = "multi_thread")]
    async fn q21_shape_arrow_path_does_not_swap() {
        let sup = tmp_parquet("q21_sup");
        let li = tmp_parquet("q21_li");
        let ord = tmp_parquet("q21_ord");
        write_dim(&sup, 100, 100); // dense-unique ident 0..100
        write_fact_fk(&li, 10_000, 100, 2_000); // ref_a → sup, ref_b → ord
        write_dim(&ord, 2_000, 100); // dense-unique ident 0..2000, pname filterable
        let ctx = arrow_ctx_with(&[("sup", &sup), ("li", &li), ("ord", &ord)]).await;
        let plan = physical_plan(
            &ctx,
            "SELECT o.ident FROM ord o \
             JOIN (SELECT li.ref_b FROM sup JOIN li ON sup.ident = li.ref_a) x \
             ON o.ident = x.ref_b \
             WHERE o.pname LIKE '%xyz%'",
        )
        .await;
        let out = rule().optimize(Arc::clone(&plan), &config_opts()).unwrap();
        assert_eq!(
            plan_text(&plan),
            plan_text(&out),
            "no join in the Q21 shape may swap: every honest estimate \
             says the current builds are already the small sides"
        );
    }

    /// The literal Q21 parted PK-side shape: a parted DIRECTORY —
    /// `UnionExec[EmatixFastParquetExec × N]` from the multi-file
    /// provider — with contiguous per-file `ident` ranges, joined to
    /// an arrow-registered fact (the campaign's exact mixed
    /// registration). Grounding must merge stats across the union so
    /// the PK-FK estimate stays the FK side. This is the shape that
    /// mis-priced supplier ⨝ lineitem at |supplier| in production
    /// and swapped Q21 onto a ~600M-row build.
    #[tokio::test(flavor = "multi_thread")]
    async fn parted_union_pk_side_grounds_estimate() {
        let part1 = tmp_parquet("dim-0001");
        let dir = part1.parent().unwrap().to_path_buf();
        write_dim_range(&part1, 0, 50, 50);
        write_dim_range(&dir.join("dim-0002.parquet"), 50, 100, 50);
        let fact = tmp_parquet("parted_union_fact");
        write_fact_fk(&fact, 10_000, 100, 2_000);

        let mut config = SessionConfig::new().with_target_partitions(2);
        {
            let opts = config.options_mut();
            opts.optimizer.hash_join_single_partition_threshold = 0;
            opts.optimizer.hash_join_single_partition_threshold_rows = 0;
            opts.execution.collect_statistics = true;
        }
        let ctx = SessionContext::new_with_config(config);
        let prov =
            crate::ematix_fast_parquet_multi::EmatixFastParquetMultiTableProvider::try_new_dir(
                &dir,
            )
            .unwrap();
        ctx.register_table("dim", Arc::new(prov)).unwrap();
        ctx.register_parquet("fact", fact.to_str().unwrap(), Default::default())
            .await
            .unwrap();

        let plan = physical_plan(
            &ctx,
            "SELECT f.ref_a FROM fact f JOIN dim d ON f.ref_a = d.ident",
        )
        .await;
        let hj = find_first::<HashJoinExec>(&plan).expect("hash join in parted plan");
        let est = estimate_rows(&hj).expect("parted PK-FK join estimate must be known");
        assert!(
            est >= 9_000.0,
            "union-of-parts PK side must ground; the estimate is the \
             10 000-row FK side, not |dim| = 100 — got {est}\n{}",
            plan_text(&plan)
        );
    }

    /// The distributed/mesh registration verbatim: `register_parquet`
    /// on the parted DIRECTORY. DF's ListingTable merges the part
    /// files' footer stats and downgrades them to Inexact — the
    /// VALUES are still the footer truth, and the density identity
    /// (max − min + 1 == rows) is the proof grounding leans on.
    /// This is the exact session shape whose ungrounded estimate
    /// swapped Q21 onto a ~600M-row build on the live mesh.
    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_dir_registration_inexact_stats_still_ground() {
        let part1 = tmp_parquet("adim-0001");
        let dir = part1.parent().unwrap().to_path_buf();
        write_dim_range(&part1, 0, 50, 50);
        write_dim_range(&dir.join("adim-0002.parquet"), 50, 100, 50);
        let fact = tmp_parquet("adir_fact");
        write_fact_fk(&fact, 10_000, 100, 2_000);
        // Production-faithful session: high target_partitions (files
        // split into byte ranges) and collect_statistics LEFT AT ITS
        // DEFAULT — the campaign session never enables it, so the
        // parquet listing reports lazily-merged stats downgraded to
        // Inexact. The VALUES are still the footer truth; only the
        // Precision tag drops.
        let mut config = SessionConfig::new().with_target_partitions(16);
        {
            let opts = config.options_mut();
            opts.optimizer.hash_join_single_partition_threshold = 0;
            opts.optimizer.hash_join_single_partition_threshold_rows = 0;
        }
        let ctx = SessionContext::new_with_config(config);
        ctx.register_parquet("dim", dir.to_str().unwrap(), Default::default())
            .await
            .unwrap();
        ctx.register_parquet("fact", fact.to_str().unwrap(), Default::default())
            .await
            .unwrap();
        let plan = physical_plan(
            &ctx,
            "SELECT f.ref_a FROM fact f JOIN dim d ON f.ref_a = d.ident",
        )
        .await;
        let hj = find_first::<HashJoinExec>(&plan).expect("hash join in dir-registered plan");
        let est = estimate_rows(&hj).expect("dir-registered PK-FK estimate must be known");
        assert!(
            est >= 9_000.0,
            "multi-file dir registration (Inexact merged stats) must \
             still ground the dense-unique PK side — got {est}\n{}",
            plan_text(&plan)
        );
    }

    /// Bare leaf reporting fixed statistics — stands in for the
    /// production mesh scans, whose stats DF downgrades to Inexact
    /// once a DynamicFilter is pushed onto them (every probe-side
    /// scan under a CollectLeft join). Values remain footer truth.
    #[derive(Debug)]
    struct MockLeafExec {
        props: Arc<datafusion::physical_plan::PlanProperties>,
        stats: datafusion::common::Statistics,
    }

    impl MockLeafExec {
        fn new(rows: Precision<usize>, cs: datafusion::common::ColumnStatistics) -> Self {
            use datafusion::arrow::datatypes::{DataType, Field, Schema};
            use datafusion::physical_expr::EquivalenceProperties;
            use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
            use datafusion::physical_plan::{Partitioning, PlanProperties};
            let schema = Arc::new(Schema::new(vec![Field::new(
                "ident",
                DataType::Int64,
                false,
            )]));
            let stats = datafusion::common::Statistics {
                num_rows: rows,
                total_byte_size: Precision::Absent,
                column_statistics: vec![cs],
            };
            Self {
                props: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(Arc::clone(&schema)),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
                stats,
            }
        }
    }

    impl datafusion::physical_plan::DisplayAs for MockLeafExec {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "MockLeafExec")
        }
    }

    impl ExecutionPlan for MockLeafExec {
        fn name(&self) -> &str {
            "MockLeafExec"
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
            &self.props
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _c: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _p: usize,
            _cx: Arc<datafusion::execution::TaskContext>,
        ) -> Result<datafusion::execution::SendableRecordBatchStream> {
            unreachable!("plan-time only")
        }
        fn partition_statistics(
            &self,
            _p: Option<usize>,
        ) -> Result<datafusion::common::Statistics> {
            Ok(self.stats.clone())
        }
    }

    fn inexact_cs(min: i64, max: i64) -> datafusion::common::ColumnStatistics {
        datafusion::common::ColumnStatistics {
            null_count: Precision::Inexact(0),
            min_value: Precision::Inexact(ScalarValue::Int64(Some(min))),
            max_value: Precision::Inexact(ScalarValue::Int64(Some(max))),
            ..Default::default()
        }
    }

    /// Σ.JS.2: Inexact-tagged stats whose VALUES satisfy the density
    /// identity (max − min + 1 == rows) must ground — the identity,
    /// not the Precision tag, is the proof. The production mesh
    /// session tags every DynamicFilter-carrying scan Inexact while
    /// still reporting footer-true values; refusing them re-opens
    /// the Q21 mis-swap. Values that BREAK the identity (a truly
    /// estimated count) must keep refusing.
    #[test]
    fn dense_unique_grounds_on_inexact_footer_values() {
        let dense: Arc<dyn ExecutionPlan> = Arc::new(MockLeafExec::new(
            Precision::Inexact(1_000_000),
            inexact_cs(1, 1_000_000),
        ));
        assert_eq!(
            dense_unique_domain(&dense, 0),
            Some(1_000_000.0),
            "Inexact footer-true values satisfying the density identity must ground"
        );
        // A post-filter-scaled row estimate breaks the identity → refuse.
        let scaled: Arc<dyn ExecutionPlan> = Arc::new(MockLeafExec::new(
            Precision::Inexact(200_000),
            inexact_cs(1, 1_000_000),
        ));
        assert_eq!(
            dense_unique_domain(&scaled, 0),
            None,
            "estimated (identity-breaking) stats must NOT ground"
        );
        // Absent stats must refuse regardless.
        let absent: Arc<dyn ExecutionPlan> = Arc::new(MockLeafExec::new(
            Precision::Absent,
            datafusion::common::ColumnStatistics::new_unknown(),
        ));
        assert_eq!(dense_unique_domain(&absent, 0), None);
    }

    /// Default construction snapshots the EMAT_JOIN_SIDE_FIX env gate
    /// (default ON, `=0` opt-out) — the kill-switch contract.
    #[test]
    fn default_snapshots_env_kill_switch() {
        let _env = crate::flags::EMAT_ENV_TEST_LOCK.blocking_lock();
        unsafe { std::env::remove_var("EMAT_JOIN_SIDE_FIX") };
        assert!(
            SampledJoinSideRule::default().enabled,
            "unset => rule ON (production default)"
        );
        unsafe { std::env::set_var("EMAT_JOIN_SIDE_FIX", "0") };
        assert!(!SampledJoinSideRule::default().enabled, "=0 => rule OFF");
        unsafe { std::env::remove_var("EMAT_JOIN_SIDE_FIX") };
        // Margin default + clamp.
        assert!((SampledJoinSideRule::default().swap_margin - 2.0).abs() < 1e-9);
    }
}
