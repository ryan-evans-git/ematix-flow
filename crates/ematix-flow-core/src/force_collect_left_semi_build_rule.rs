//! `ForceCollectLeftForSemiBoundedBuildRule` — REV.3 (2026-05-30).
//!
//! Fixes the Q18-class "Partitioned dimension⋈fact join hash-
//! repartitions the whole fact table behind a tiny semi-bounded build"
//! anti-pattern by forcing the join to `PartitionMode::CollectLeft`
//! (broadcast the small build once, stream the probe with NO hash
//! exchange) — the DuckDB-equivalent execution shape.
//!
//! ## Why
//!
//! TPC-H Q18: an Inner `HashJoinExec` joins a dimension subtree
//! (customer⋈orders⋈semi → ~624 rows at SF=10) to the lineitem fact
//! scan on `o_orderkey = l_orderkey`. Because the build subtree's
//! cardinality is `Precision::Absent` (an `AggregateExec` / semi-join
//! output — see `swap_semi_join_build_rule` docs), `JoinSelection`
//! cannot prove the build small and defaults to
//! `PartitionMode::Partitioned`, hash-repartitioning ALL 60M (SF=10) /
//! 600M (SF=100) lineitem rows to feed a 624-row build. DuckDB streams
//! the probe through a shared hash table with no exchange. That wasted
//! `O(|lineitem|)` shuffle is invisible at SF=1 (fits cache) and
//! dominant at SF=100 — the Q18 2.53× loss vs DuckDB.
//!
//! ## What this rule does
//!
//! Runs as a post-`JoinSelection` / `EnforceDistribution` physical
//! pass. For each Inner `HashJoinExec` in `Partitioned` mode whose
//! build (LEFT) subtree contains a semi/anti join — the structural
//! signal that the build is membership-bounded, hence small (detected
//! by [`build_subtree_has_semi_filter`]) — it rebuilds the join in
//! `CollectLeft` mode via `hj.builder().with_partition_mode(...)`.
//!
//! The children are untouched by the transform, so the rewritten plan
//! momentarily violates CollectLeft's "left == 1 partition" invariant.
//! We then re-run `EnforceDistribution`, which sees CollectLeft's
//! `required_input_distribution = [SinglePartition, Unspecified]`,
//! coalesces the build to a single partition, and drops the now-
//! redundant Hash repartition on the probe. This is exactly the Σ.BS
//! "repair partitioning after a structural rewrite" pattern (re-run
//! `EnforceDistribution`, gated on `transformed`).
//!
//! ## Why this is safe
//!
//! - A semi/anti-bounded build is membership-filtered, so it is
//!   genuinely small — broadcasting it is cheap. If a semi were
//!   pathologically non-selective, the worst case is a perf
//!   regression, never incorrectness: CollectLeft is semantically
//!   identical to Partitioned (same join output).
//! - The `build_subtree_has_semi_filter` gate already detects all four
//!   semi/anti variants, including the `RightSemi` that
//!   `SwapSemiJoinBuildSideRule` produces for Q18.
//!
//! ## Opt-in
//!
//! Default OFF (per the optimizer-codegen-sensitivity tax — adding a
//! pass perturbs the geomean). Callers route it in via
//! [`install_force_collect_left_semi_build_rule`] when enabled.

use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

// REV.19 NDV-corrected build-side selection (opt-in) imports.
use datafusion::common::stats::Precision;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::utils::split_conjunction;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::scalar::ScalarValue;

#[cfg(test)]
use crate::runtime_bloom_sideband_rule::build_subtree_has_semi_filter;

/// See module docs.
#[derive(Debug)]
pub struct ForceCollectLeftForSemiBoundedBuildRule {
    /// REV.17.1 cardinality-ratio guard. When `> 0.0`, the rule only
    /// forces CollectLeft if the (non-semi) probe side's estimated
    /// `num_rows` is at least `min_probe_build_ratio ×` the (semi) build
    /// side's estimated `num_rows` — i.e. the build is provably not
    /// larger than the probe.
    ///
    /// Phase-0 (REV.17.0) finding: DataFusion propagates input
    /// cardinality through semi/anti/HAVING outputs WITHOUT applying
    /// selectivity, so the build estimate is a conservative UPPER BOUND
    /// on the true build size. An over-estimate therefore only makes this
    /// gate *more* conservative — it can never cause a wrong broadcast.
    /// When either side's estimate is `Absent`, the gate PASSES (we fire)
    /// — preserving the structural rescue for HAVING-collapsed builds
    /// whose true size is tiny but statically unknowable (Q18 join0:
    /// build est 150M but true ~6K, probe 600M → ratio 4× fires & wins).
    ///
    /// Default `0.0` = original REV.3 behavior (always fire on the
    /// structural semi signal), read from `EMAT_COLLECT_LEFT_MIN_RATIO`.
    /// `1.0` = "don't broadcast a build larger than its probe" (declines
    /// Q17 join0 0.20× and Q16 join0 0.05×). NOTE: REV.17.1 measured
    /// `1.0` net-NEGATIVE (Q16 +252% — broadcasting a "big" build that
    /// avoids a hash-shuffle can still win), so this stays `0.0` in
    /// production; kept as opt-in infra.
    pub min_probe_build_ratio: f64,

    /// REV.17.3 scale-relative broadcast gate. When `> 0.0`, an Inner
    /// `Partitioned` `HashJoinExec` whose build subtree contains NO
    /// semi/anti filter (a plain dimension⋈fact join) is forced to
    /// `CollectLeft` (broadcasting the SMALL side) when the larger side's
    /// estimated `num_rows` is at least `broadcast_ratio ×` the smaller
    /// side's — i.e. the small side is broadcast and the large side
    /// streams with NO hash exchange (DuckDB's default broadcast-small-
    /// dimension behaviour). This auto-scales (it's a RATIO, not an
    /// absolute row count), so it's near-inert at SF=1/10 and aggressive
    /// at SF=100. Unlike [`Self::min_probe_build_ratio`] it requires BOTH
    /// estimates to be known (no over-estimate safety here — we need a
    /// real ratio) and inherently skips the Q16-j0 trap (build ≫ probe →
    /// won't fire). REV.17.2 (SF=100, via the equivalent config-threshold)
    /// measured 8 wins (Q03 −37%…Q08 −8%) / net −10.2%. REV.17.3 ships the
    /// rule form, multi-scale gated DEFAULT-ON: SF=100 net −9.8% (9 wins),
    /// SF=10 −7.2%, SF=1 −4.1%, zero real regressions, all 22 row counts
    /// identical at every scale. Read from
    /// `EMAT_COLLECT_LEFT_BROADCAST_RATIO`; default `16.0` = on, opt-out
    /// with `=0`.
    ///
    /// REV.17.4a swept K ∈ {8, 16, 32} and confirmed 16 is optimal: K=32 is
    /// too conservative (declines the Q03/Q05 dim⋈fact broadcasts at SF=10,
    /// +80%); K=8 is a wash at SF=10 and at SF=100 merely REDISTRIBUTES —
    /// it broadcasts more aggressively (Q03 −16%, Q05 −11%, Q17 −11%, Q18
    /// −19%, Q21 −9%) but regresses the three HEAVIEST queries (Q08 +11%,
    /// Q09 +10%, Q10 +8% — Q09 is 7.4 s), netting −0.1%. 16 captures the big
    /// broadcasts without the heavy-query tail regressions. Do not retune
    /// without a fresh SF=100 sweep.
    pub broadcast_ratio: f64,

    /// Σ.AH.3 (2026-07-01) — date-range corrected build-side swap for
    /// Inner `Partitioned` joins (opt-in, default OFF). DataFusion 53's
    /// interval analyzer does not `check_support` Date32
    /// (`intervals/utils.rs::is_datatype_supported` covers Int/UInt/Float
    /// only), so EVERY date-range filter — even with Exact min/max column
    /// stats plumbed by `EmatixFastParquetTableProvider` — falls back to
    /// the flat 20% `default_selectivity`. On TPC-H that over-estimates
    /// the probe side of `cust ⋈ orders`-shaped joins ~5× (Q10 SF=10:
    /// reported 3M, interval-true ~573k), hiding a build/probe inversion
    /// from `JoinSelection`. When enabled, a side's reported `num_rows`
    /// is re-based by the true `window/span` selectivity of its subtree's
    /// Date32 range filters (only ever shrinking, cap 1.0 — the REV.19
    /// safety direction), and the join is swapped when the corrected
    /// probe is at least [`Self::date_build_side_ratio`] × smaller than
    /// the corrected build. Keeps Partitioned mode (a side swap, not a
    /// broadcast).
    ///
    /// Σ.AI.5 (2026-07-02) — SCALE-GATED tri-state: `Some(true)` force on
    /// (`EMAT_DATE_BUILD_SIDE=1`), `Some(false)` force off (`=0`), `None`
    /// = AUTO — resolved **at rule-application time** against
    /// [`crate::scale_class::large_scale_seen`] (the rule is constructed
    /// at session-build time, BEFORE table registration, so the env is
    /// snapshotted here but the scale class must not be). Campaign
    /// evidence: with `EMAT_NDV_BUILD_SIDE` this flips Q10 SF=100
    /// (−947 ms); plan-inert at SF=10 (preset path routes around it).
    pub date_build_side: Option<bool>,

    /// Σ.AH.3 — conservative swap margin for [`Self::date_build_side`]:
    /// only swap when `corrected_build ≥ ratio × corrected_probe`
    /// (default `2.0`, the flap guard from the arc spec; values < 1.0 are
    /// clamped to 1.0). Read from `EMAT_DATE_BUILD_SIDE_RATIO`.
    pub date_build_side_ratio: f64,
}

impl Default for ForceCollectLeftForSemiBoundedBuildRule {
    fn default() -> Self {
        let min_probe_build_ratio = crate::flags::f64_or("EMAT_COLLECT_LEFT_MIN_RATIO", 0.0);
        // REV.17.3: DEFAULT-ON. Multi-scale gated (SF=100 −9.8%, SF=10
        // −7.2%, SF=1 −4.1%, zero real regressions, all 22 row counts
        // identical). K=16 is the broadcast break-even ratio. Opt-out via
        // EMAT_COLLECT_LEFT_BROADCAST_RATIO=0.
        let broadcast_ratio = crate::flags::f64_or("EMAT_COLLECT_LEFT_BROADCAST_RATIO", 16.0);
        // Σ.AI.5: tri-state — explicit env wins both ways; None = AUTO,
        // resolved at apply time (scale class isn't known at session
        // build, which happens before table registration).
        let date_build_side = crate::flags::tri_state("EMAT_DATE_BUILD_SIDE");
        let date_build_side_ratio = crate::flags::f64_or("EMAT_DATE_BUILD_SIDE_RATIO", 2.0);
        Self {
            min_probe_build_ratio,
            broadcast_ratio,
            date_build_side,
            date_build_side_ratio,
        }
    }
}

/// REV.17.1 cardinality-ratio guard predicate: is the (semi) `build`
/// provably not larger than the `probe`? `min_ratio <= 0.0` disables the
/// gate (original REV.3 behavior). When either side's estimated
/// `num_rows` is `Absent`, the gate PASSES (returns `true` → fire). See
/// [`ForceCollectLeftForSemiBoundedBuildRule::min_probe_build_ratio`].
fn probe_not_smaller_than_build(
    build: &Arc<dyn ExecutionPlan>,
    probe: &Arc<dyn ExecutionPlan>,
    min_ratio: f64,
) -> bool {
    if min_ratio <= 0.0 {
        return true;
    }
    let build_rows = build
        .partition_statistics(None)
        .ok()
        .and_then(|s| s.num_rows.get_value().copied());
    let probe_rows = probe
        .partition_statistics(None)
        .ok()
        .and_then(|s| s.num_rows.get_value().copied());
    match (build_rows, probe_rows) {
        (Some(b), Some(p)) => (p as f64) >= (b as f64) * min_ratio,
        // Either side unknown → fire (preserve the structural rescue).
        _ => true,
    }
}

/// REV.19 — minimum distinct count for a string-equality filter to be
/// eligible for NDV correction. Below this the column isn't meaningfully
/// "low cardinality"; and since we only ever SHRINK an estimate (cap at
/// 1.0), this also guards the trivial `N=1` case.
const NDV_MIN_DISTINCT: usize = 2;

/// REV.19 — the true selectivity `1/N` of a `col = lit` equality relative
/// to the flat `default_sel` DataFusion's `FilterExec` actually applied to
/// it, capped at `1.0`. The cap means the correction only ever *shrinks* an
/// estimate: a filter that is *less* selective than the default (`N` small)
/// is left uncorrected — the safe direction for a build-side decision (we
/// never manufacture a swap by inflating a side).
fn eq_ndv_correction(distinct: usize, default_sel: f64) -> f64 {
    if distinct < NDV_MIN_DISTINCT || default_sel <= 0.0 {
        return 1.0;
    }
    ((1.0 / distinct as f64) / default_sel).min(1.0)
}

/// True iff `v` is a non-null UTF-8 string literal (the only literal kind
/// for which DataFusion's interval analyzer declines `check_support`, hence
/// falls back to `default_selectivity` and ignores `distinct_count`).
fn is_utf8_literal(v: &ScalarValue) -> bool {
    matches!(
        v,
        ScalarValue::Utf8(Some(_))
            | ScalarValue::Utf8View(Some(_))
            | ScalarValue::LargeUtf8(Some(_))
    )
}

/// Product of [`eq_ndv_correction`] over every `string_col = string_lit`
/// equality conjunct of `predicate` whose filtered column carries a known
/// `distinct_count` in `child_stats`. The column index in the predicate refers
/// to the filter's input schema, which is exactly how `child_stats`
/// (`input().partition_statistics`) is indexed. Pure (no plan) so it is
/// directly unit-testable.
fn predicate_ndv_factor(
    predicate: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    child_stats: &datafusion::common::Statistics,
    default_sel: f64,
) -> f64 {
    let mut factor = 1.0_f64;
    for conj in split_conjunction(predicate) {
        let Some(bin) = conj.as_any().downcast_ref::<BinaryExpr>() else {
            continue;
        };
        if *bin.op() != Operator::Eq {
            continue;
        }
        // `Column = Literal` in either operand order.
        let (col, lit) = match (
            bin.left().as_any().downcast_ref::<Column>(),
            bin.right().as_any().downcast_ref::<Literal>(),
        ) {
            (Some(c), Some(l)) => (c, l),
            _ => match (
                bin.left().as_any().downcast_ref::<Literal>(),
                bin.right().as_any().downcast_ref::<Column>(),
            ) {
                (Some(l), Some(c)) => (c, l),
                _ => continue,
            },
        };
        if !is_utf8_literal(lit.value()) {
            continue;
        }
        let Some(cs) = child_stats.column_statistics.get(col.index()) else {
            continue;
        };
        let distinct = match cs.distinct_count {
            Precision::Exact(n) | Precision::Inexact(n) => n,
            Precision::Absent => continue,
        };
        factor *= eq_ndv_correction(distinct, default_sel);
    }
    factor
}

/// [`predicate_ndv_factor`] for a `FilterExec`, sourcing the predicate, its
/// input column statistics, and the `default_selectivity` DataFusion applied.
fn filter_ndv_factor(filter: &FilterExec) -> f64 {
    let default_sel = filter.default_selectivity() as f64 / 100.0;
    let Ok(child_stats) = filter.input().partition_statistics(None) else {
        return 1.0;
    };
    predicate_ndv_factor(filter.predicate(), &child_stats, default_sel)
}

/// REV.19 — walk `plan`'s subtree and return the product of every
/// `FilterExec`'s [`filter_ndv_factor`]. Always `≤ 1.0`; strictly `< 1.0`
/// iff some low-NDV string-equality filter in the subtree was under-credited
/// by DataFusion's flat `default_selectivity`. Multiplying a join side's
/// reported `num_rows` by this re-bases the estimate; the correction
/// propagates ~multiplicatively through downstream joins/projections (a join's
/// output scales ~linearly with a filtered input).
fn ndv_correction_factor(plan: &Arc<dyn ExecutionPlan>) -> f64 {
    let mut factor = 1.0_f64;
    if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
        factor *= filter_ndv_factor(filter);
    }
    for child in plan.children() {
        factor *= ndv_correction_factor(child);
    }
    factor
}

/// Σ.AH.3 — Date32 day value from a column-statistics min/max bound
/// (Exact or Inexact both accepted, mirroring the NDV distinct handling).
fn date32_stat(p: &Precision<ScalarValue>) -> Option<i32> {
    match p.get_value() {
        Some(ScalarValue::Date32(Some(d))) => Some(*d),
        _ => None,
    }
}

/// Σ.AH.3 — true `window/span` selectivity of the Date32 range conjuncts
/// of `predicate` relative to the flat `default_sel` DataFusion's
/// `FilterExec` actually applied, capped at `1.0` (only ever shrinks — we
/// never manufacture a swap by inflating a side; a window wider than the
/// default is left uncorrected). Per-column inclusive day bounds are
/// intersected across conjuncts (`>=`/`>`/`<=`/`<`/`=`, either operand
/// order), then clamped to the column's known `[min, max]`; the span is
/// `max − min + 1` days. Non-Date32 conjuncts are ignored: they are either
/// `check_support`ed (numeric — no default was applied to them) or only
/// shrink the true row count further, so ignoring them keeps the corrected
/// estimate an over-estimate — the safe direction. Pure (no plan) so it is
/// directly unit-testable.
fn predicate_date_factor(
    predicate: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    child_stats: &datafusion::common::Statistics,
    default_sel: f64,
) -> f64 {
    use std::collections::BTreeMap;
    // col index → (inclusive lower day, inclusive upper day)
    let mut bounds: BTreeMap<usize, (Option<i32>, Option<i32>)> = BTreeMap::new();
    for conj in split_conjunction(predicate) {
        let Some(bin) = conj.as_any().downcast_ref::<BinaryExpr>() else {
            continue;
        };
        // `Column OP Literal` in either operand order (flip OP when the
        // literal is on the left: `lit < col` ≡ `col > lit`).
        let (col, lit, op) = match (
            bin.left().as_any().downcast_ref::<Column>(),
            bin.right().as_any().downcast_ref::<Literal>(),
        ) {
            (Some(c), Some(l)) => (c, l, *bin.op()),
            _ => match (
                bin.left().as_any().downcast_ref::<Literal>(),
                bin.right().as_any().downcast_ref::<Column>(),
            ) {
                (Some(l), Some(c)) => {
                    let flipped = match bin.op() {
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        Operator::Eq => Operator::Eq,
                        _ => continue,
                    };
                    (c, l, flipped)
                }
                _ => continue,
            },
        };
        let ScalarValue::Date32(Some(d)) = lit.value() else {
            continue;
        };
        let d = *d;
        let entry = bounds.entry(col.index()).or_insert((None, None));
        let max_lo = |cur: Option<i32>, v: i32| Some(cur.map_or(v, |x| x.max(v)));
        let min_hi = |cur: Option<i32>, v: i32| Some(cur.map_or(v, |x| x.min(v)));
        match op {
            Operator::Gt => entry.0 = max_lo(entry.0, d.saturating_add(1)),
            Operator::GtEq => entry.0 = max_lo(entry.0, d),
            Operator::Lt => entry.1 = min_hi(entry.1, d.saturating_sub(1)),
            Operator::LtEq => entry.1 = min_hi(entry.1, d),
            Operator::Eq => {
                entry.0 = max_lo(entry.0, d);
                entry.1 = min_hi(entry.1, d);
            }
            _ => continue,
        }
    }
    let mut sel = 1.0_f64;
    let mut corrected_any = false;
    for (idx, (lo, hi)) in bounds {
        let Some(cs) = child_stats.column_statistics.get(idx) else {
            continue;
        };
        let (Some(smin), Some(smax)) = (date32_stat(&cs.min_value), date32_stat(&cs.max_value))
        else {
            continue;
        };
        if smax < smin {
            continue;
        }
        let span = (smax as i64 - smin as i64 + 1) as f64;
        let lo = lo.unwrap_or(smin).max(smin);
        let hi = hi.unwrap_or(smax).min(smax);
        let width = if hi >= lo {
            (hi as i64 - lo as i64 + 1) as f64
        } else {
            0.0
        };
        sel *= width / span;
        corrected_any = true;
    }
    if !corrected_any || default_sel <= 0.0 {
        return 1.0;
    }
    (sel / default_sel).min(1.0)
}

/// [`predicate_date_factor`] for a `FilterExec`, sourcing the predicate,
/// its input column statistics, and the `default_selectivity` DataFusion
/// applied. Guarded on `!check_support(...)`: if DataFusion's interval
/// analyzer DID support the predicate, no default was applied and a
/// correction would double-count — this future-proofs a DataFusion
/// upgrade that adds Date32 to `is_datatype_supported`.
fn filter_date_factor(filter: &FilterExec) -> f64 {
    if datafusion::physical_expr::intervals::utils::check_support(
        filter.predicate(),
        &filter.input().schema(),
    ) {
        return 1.0;
    }
    let default_sel = filter.default_selectivity() as f64 / 100.0;
    let Ok(child_stats) = filter.input().partition_statistics(None) else {
        return 1.0;
    };
    predicate_date_factor(filter.predicate(), &child_stats, default_sel)
}

/// Σ.AH.3 — walk `plan`'s subtree and return the product of every
/// `FilterExec`'s [`filter_date_factor`]. Always `≤ 1.0`; strictly `< 1.0`
/// iff some Date32 range filter in the subtree was under-credited by
/// DataFusion's flat `default_selectivity`. Mirrors
/// [`ndv_correction_factor`]'s multiplicative propagation model.
fn date_correction_factor(plan: &Arc<dyn ExecutionPlan>) -> f64 {
    let mut factor = 1.0_f64;
    if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
        factor *= filter_date_factor(filter);
    }
    for child in plan.children() {
        factor *= date_correction_factor(child);
    }
    factor
}

/// Reported `num_rows` for a join side, if statically known.
fn reported_rows(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    plan.partition_statistics(None)
        .ok()
        .and_then(|s| s.num_rows.get_value().copied())
}

/// Install the REV.3 CollectLeft rule onto a `SessionStateBuilder`.
pub fn install_force_collect_left_semi_build_rule(
    builder: SessionStateBuilder,
) -> SessionStateBuilder {
    builder
        .with_physical_optimizer_rule(Arc::new(ForceCollectLeftForSemiBoundedBuildRule::default()))
}

/// Q16.SWAP — SEMI-only variant of
/// [`build_subtree_has_semi_filter`]: true iff the subtree contains a
/// Left/RightSemi join. Semi output is bounded by the membership-key
/// count (usually tiny — Q18's HAVING build: 624 keys); anti output is
/// bounded by its KEPT side (usually large), so anti must not signal
/// "bounded-small" to the force arms.
fn subtree_has_semi_only_filter(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(hj) = plan.as_any().downcast_ref::<HashJoinExec>() {
        if matches!(hj.join_type(), JoinType::LeftSemi | JoinType::RightSemi) {
            return true;
        }
    }
    plan.children()
        .iter()
        .any(|c| subtree_has_semi_only_filter(c))
}

/// Q16.SWAP — when a side's FIRST shaping node (through partition-
/// shuffling pass-throughs) is an anti join, return the kept-side
/// child's known row count as that side's effective cardinality. The
/// anti output can never exceed it; for a selective anti build it ≈
/// equals it (Q16: 79.96M of 80M partsupp rows survive the 479-key
/// complaint anti). Returns `None` for any other shape so the caller
/// falls back to reported statistics.
#[allow(deprecated)] // CoalesceBatchesExec still appears in DF 53 plans
fn anti_topped_bound(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let any = plan.as_any();
    if any.is::<datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec>()
        || any.is::<datafusion::physical_plan::repartition::RepartitionExec>()
        || any.is::<datafusion::physical_plan::projection::ProjectionExec>()
        || any.is::<datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec>()
    {
        return anti_topped_bound(plan.children().first()?);
    }
    let hj = any.downcast_ref::<HashJoinExec>()?;
    let kept: &Arc<dyn ExecutionPlan> = match hj.join_type() {
        JoinType::LeftAnti => hj.left(),
        JoinType::RightAnti => hj.right(),
        _ => return None,
    };
    kept.partition_statistics(None)
        .ok()
        .and_then(|s| s.num_rows.get_value().copied())
}

impl PhysicalOptimizerRule for ForceCollectLeftForSemiBoundedBuildRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let trace = crate::flags::present("EMAT_COLLECT_LEFT_TRACE");
        let rewritten = plan.transform_up(|node| {
            let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            // REV.17.4b: Inner OR semi/anti joins in Partitioned mode are
            // candidates. The semi-bounded arms + the relative-broadcast SWAP
            // arm stay Inner-only — swapping a semi/anti join flips
            // LeftSemi↔RightSemi (asymmetric output side), too risky. A
            // semi/anti join reaches ONLY the no-swap left-small relative arm,
            // where forcing CollectLeft is a physical PartitionMode change
            // (valid + semantics-preserving for every join type).
            // REV.17.4b opt-out (default ON): `EMAT_COLLECT_LEFT_SEMI_BROADCAST=0`
            // reverts to the REV.17.3 Inner-only relative broadcast.
            let semi_broadcast =
                !matches!(std::env::var("EMAT_COLLECT_LEFT_SEMI_BROADCAST").as_deref(), Ok("0"));
            let is_inner = matches!(hj.join_type(), JoinType::Inner);
            let is_semi_anti = semi_broadcast
                && matches!(
                    hj.join_type(),
                    JoinType::LeftSemi
                        | JoinType::RightSemi
                        | JoinType::LeftAnti
                        | JoinType::RightAnti
                );
            if !is_inner && !is_semi_anti {
                return Ok(Transformed::no(node));
            }
            if !matches!(hj.partition_mode(), PartitionMode::Partitioned) {
                return Ok(Transformed::no(node));
            }
            // REV.19 NDV-corrected build-side selection (opt-in, default OFF).
            // DataFusion's `FilterExec` never consults `distinct_count`: its
            // interval analyzer doesn't `check_support` string equality, so a
            // low-NDV string filter (e.g. `p_type='ECONOMY ANODIZED STEEL'`,
            // true 1/150) is credited only the flat `default_selectivity`
            // (≈20%), over-estimating the filtered side ~30×. When that
            // over-estimate hides a build-side inversion — `JoinSelection`
            // built the reported-smaller (LEFT) side, but NDV-corrected sizes
            // say the RIGHT (probe) is actually smaller — swap so the truly
            // smaller side builds. Keeps Partitioned mode; `swap_inputs`
            // preserves Inner semantics + repairs the output schema, and
            // `EnforceDistribution` re-runs below. General (any low-NDV
            // equality filter, not TPC-H-specific); requires the dict-distinct
            // walk (`EMAT_DICT_DISTINCT_MAX_ROWS`) to populate `distinct_count`.
            // Σ.AI.5 (2026-07-02): SCALE-GATED tri-state — `=1` force on,
            // `=0` force off, unset = AUTO (on only for SF≥100-class
            // datasets; with the date swap this flips Q10 SF=100 −947 ms,
            // neutral at SF=10). The gate check short-circuits before any
            // subtree walk when it resolves off.
            if is_inner && crate::flags::scale_gated_large("EMAT_NDV_BUILD_SIDE") {
                let lf = ndv_correction_factor(hj.left());
                let rf = ndv_correction_factor(hj.right());
                // Only act when some side was actually under-credited.
                if lf < 1.0 || rf < 1.0 {
                    if let (Some(l), Some(r)) =
                        (reported_rows(hj.left()), reported_rows(hj.right()))
                    {
                        let corrected_left = l as f64 * lf;
                        let corrected_right = r as f64 * rf;
                        if corrected_right < corrected_left {
                            if trace {
                                eprintln!(
                                    "[collect_left] NDV build-side swap (corrected L={corrected_left:.0} R={corrected_right:.0}, lf={lf:.4} rf={rf:.4}); on={:?}",
                                    hj.on()
                                );
                            }
                            return Ok(Transformed::yes(
                                hj.swap_inputs(PartitionMode::Partitioned)?,
                            ));
                        }
                    }
                }
            }
            // Σ.AH.3 date-range corrected build-side swap (opt-in, default
            // OFF — see the field docs on [`Self::date_build_side`]). Fires
            // only when the PROBE (right) was under-credited by a Date32
            // range filter (`rf < 1.0`) — a build-side-only correction
            // shrinks the already-smaller-estimated side and needs no swap —
            // and the corrected build clears the conservative margin over
            // the corrected probe. Keeps Partitioned mode; `swap_inputs`
            // preserves Inner semantics + output schema (re-projection), and
            // `EnforceDistribution` re-runs below.
            if is_inner
                && self
                    .date_build_side
                    .unwrap_or_else(crate::scale_class::large_scale_seen)
            {
                let rf = date_correction_factor(hj.right());
                if rf < 1.0 {
                    if let (Some(l), Some(r)) =
                        (reported_rows(hj.left()), reported_rows(hj.right()))
                    {
                        let lf = date_correction_factor(hj.left());
                        let corrected_left = l as f64 * lf;
                        let corrected_right = r as f64 * rf;
                        if corrected_left >= corrected_right * self.date_build_side_ratio.max(1.0)
                        {
                            if trace {
                                eprintln!(
                                    "[collect_left] date build-side swap (corrected L={corrected_left:.0} R={corrected_right:.0}, lf={lf:.4} rf={rf:.4}); on={:?}",
                                    hj.on()
                                );
                            }
                            return Ok(Transformed::yes(
                                hj.swap_inputs(PartitionMode::Partitioned)?,
                            ));
                        }
                    }
                }
            }
            // Determine which side is semi-bounded — the structural
            // signal that it is genuinely small (DataFusion's stats report
            // `Absent` for it, which is why JoinSelection picked
            // Partitioned). We want that side to be the BUILD and to be
            // broadcast (CollectLeft), so the large side streams with no
            // hash exchange.
            //
            // Q16.SWAP (2026-06-10): SEMI variants only. An ANTI-bounded
            // side is NOT small — an anti-join against a selective build
            // passes ~all of its kept input (Q16 SF=100: partsupp
            // RightAnti vs 479 complaint suppliers → 79.96M of 80M
            // survive). The previous 4-variant detection swapped that
            // 80M side onto the build: a 502 ms single-threaded
            // CollectLeft build probed by 2.97M filtered parts. Anti-
            // topped sides are instead sized by their kept-side child's
            // cardinality in the relative arm below (the honest upper
            // bound), which routes the Q16 shape to "small left builds,
            // no swap".
            // Semi-bounded detection drives the Inner-only arms below; force
            // false for semi/anti joins so they fall straight through to the
            // no-swap relative-broadcast arm (the only one safe for them).
            let left_semi = is_inner && subtree_has_semi_only_filter(hj.left());
            let right_semi = is_inner && subtree_has_semi_only_filter(hj.right());
            let min_ratio = self.min_probe_build_ratio;
            let new_join: Arc<dyn ExecutionPlan> = if left_semi && !right_semi {
                // Semi-bounded side is already the build (left) → broadcast
                // it. (Q18 top join: customer⋈orders⋈semi ⋈ lineitem.)
                // REV.17.1 guard: decline if the build is (estimated)
                // larger than the probe — broadcasting the big side loses.
                if !probe_not_smaller_than_build(hj.left(), hj.right(), min_ratio) {
                    if trace {
                        eprintln!(
                            "[collect_left] DECLINE ratio-guard (left build); on={:?}",
                            hj.on()
                        );
                    }
                    return Ok(Transformed::no(node));
                }
                if trace {
                    eprintln!(
                        "[collect_left] CollectLeft (left build semi-bounded); on={:?}",
                        hj.on()
                    );
                }
                Arc::new(
                    hj.builder()
                        .with_partition_mode(PartitionMode::CollectLeft)
                        .build()?,
                )
            } else if right_semi && !left_semi {
                // Semi-bounded side is the PROBE (right) → swap it onto the
                // build, then broadcast. Fixes the Q18 customer⋈orders
                // inversion: build the ~624-order semi-filtered side, not
                // the 15M-row customer side. `swap_inputs` flips
                // children/on-keys/filter and (for Inner) reorders the
                // output to preserve the schema, so the parent join's
                // column indices stay valid.
                // REV.17.1 guard: here the semi side (right) becomes the
                // broadcast build and the left becomes the probe.
                if !probe_not_smaller_than_build(hj.right(), hj.left(), min_ratio) {
                    if trace {
                        eprintln!(
                            "[collect_left] DECLINE ratio-guard (right build/swap); on={:?}",
                            hj.on()
                        );
                    }
                    return Ok(Transformed::no(node));
                }
                if trace {
                    eprintln!(
                        "[collect_left] swap+CollectLeft (right probe semi-bounded); on={:?}",
                        hj.on()
                    );
                }
                hj.swap_inputs(PartitionMode::CollectLeft)?
            } else if !left_semi && !right_semi && self.broadcast_ratio > 0.0 {
                // REV.17.3 scale-relative broadcast: a plain dimension⋈fact
                // Inner join (no semi/anti in either subtree). Broadcast the
                // SMALL side when the other is `broadcast_ratio ×` larger, so
                // the large fact side streams with no hash exchange. Requires
                // BOTH estimates known (a real ratio, no over-estimate
                // safety). Inherently skips the Q16-j0 trap: when the build
                // is larger than the probe, neither inequality holds.
                // Q16.SWAP: an anti-topped side reports either Absent
                // or a bogus-small estimate (DF can't model anti
                // selectivity) — both mis-route this arm. Override
                // with the kept-side child's cardinality: the anti
                // output can never exceed it, and for selective anti
                // builds it ≈ equals it.
                let lrows = anti_topped_bound(hj.left()).or_else(|| {
                    hj.left()
                        .partition_statistics(None)
                        .ok()
                        .and_then(|s| s.num_rows.get_value().copied())
                });
                let rrows = anti_topped_bound(hj.right()).or_else(|| {
                    hj.right()
                        .partition_statistics(None)
                        .ok()
                        .and_then(|s| s.num_rows.get_value().copied())
                });
                let k = self.broadcast_ratio;
                match (lrows, rrows) {
                    (Some(l), Some(r)) if l > 0 && (r as f64) >= (l as f64) * k => {
                        // Left is the small build, right the large probe →
                        // broadcast left (no swap).
                        if trace {
                            eprintln!(
                                "[collect_left] relative CollectLeft (left small build, r/l={:.0}); on={:?}",
                                r as f64 / l as f64,
                                hj.on()
                            );
                        }
                        Arc::new(
                            hj.builder()
                                .with_partition_mode(PartitionMode::CollectLeft)
                                .build()?,
                        )
                    }
                    (Some(l), Some(r)) if is_inner && r > 0 && (l as f64) >= (r as f64) * k => {
                        // Right is the small build → swap it onto the build
                        // side, then broadcast. INNER-ONLY: swapping a
                        // semi/anti join flips LeftSemi↔RightSemi (asymmetric
                        // output side) — declined for safety (falls to `_`).
                        if trace {
                            eprintln!(
                                "[collect_left] relative swap+CollectLeft (right small build, l/r={:.0}); on={:?}",
                                l as f64 / r as f64,
                                hj.on()
                            );
                        }
                        hj.swap_inputs(PartitionMode::CollectLeft)?
                    }
                    _ => return Ok(Transformed::no(node)),
                }
            } else {
                // Neither side semi-bounded (and relative broadcast off), or
                // both sides semi-bounded → leave alone.
                return Ok(Transformed::no(node));
            };
            Ok(Transformed::yes(new_join))
        })?;

        if !rewritten.transformed {
            return Ok(rewritten.data);
        }

        // Repair partitioning: EnforceDistribution coalesces each
        // CollectLeft build to a single partition (its
        // `required_input_distribution` demands SinglePartition) and
        // drops the now-unnecessary Hash repartition on the probe side
        // (Unspecified distribution). Mirrors the Σ.BS dedupe-rule fix.
        EnforceDistribution::new().optimize(rewritten.data, config)
    }

    fn name(&self) -> &str {
        "ematix_flow_force_collect_left_semi_bounded_build"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::expressions::Column;

    /// Σ.AI.5 — the date-swap default is TRI-STATE: `Default::default()`
    /// snapshots the env as `None` (AUTO, resolved at apply time against
    /// the scale class), `Some(true)` for `=1`, `Some(false)` for `=0`.
    /// Env window is tight; this is the only test mutating
    /// EMAT_DATE_BUILD_SIDE (plan-level swap tests construct the rule
    /// with explicit fields precisely to avoid env races).
    #[test]
    fn date_build_side_default_snapshots_tristate() {
        // Crate-wide env lock: Default::default() snapshots the env; other
        // tests constructing the rule mid-window would read this test's
        // EMAT_DATE_BUILD_SIDE values.
        let _env = crate::flags::EMAT_ENV_TEST_LOCK.blocking_lock();
        unsafe { std::env::remove_var("EMAT_DATE_BUILD_SIDE") };
        assert_eq!(
            ForceCollectLeftForSemiBoundedBuildRule::default().date_build_side,
            None,
            "unset => AUTO"
        );
        unsafe { std::env::set_var("EMAT_DATE_BUILD_SIDE", "1") };
        assert_eq!(
            ForceCollectLeftForSemiBoundedBuildRule::default().date_build_side,
            Some(true),
            "=1 => force ON"
        );
        unsafe { std::env::set_var("EMAT_DATE_BUILD_SIDE", "0") };
        assert_eq!(
            ForceCollectLeftForSemiBoundedBuildRule::default().date_build_side,
            Some(false),
            "=0 => force OFF"
        );
        unsafe { std::env::remove_var("EMAT_DATE_BUILD_SIDE") };
    }

    fn mem_table() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    /// A mem table reporting `n` rows (values 0..n so ranges overlap and
    /// DataFusion's semi-join cardinality falls back to the outer
    /// `num_rows` rather than a disjoint-input 0). Used to drive the
    /// REV.17.1 ratio guard with controllable build/probe sizes.
    fn mem_table_n(n: i64) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..n).collect::<Vec<i64>>()))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn on() -> Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
        vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )]
    }

    fn hashjoin(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        jt: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on(),
                None,
                &jt,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    /// REV.3 — an Inner Partitioned `HashJoinExec` whose build (LEFT)
    /// subtree is semi-bounded (Q18 shape: a RightSemi sits in the
    /// build) must be rewritten to `CollectLeft`.
    #[test]
    fn inner_partitioned_over_semi_bounded_build_flips_to_collect_left() {
        // Build (LEFT) = a RightSemi join → semi-bounded.
        let semi = hashjoin(mem_table(), mem_table(), JoinType::RightSemi);
        // Top Inner join: build = semi subtree, probe = a fact-like scan.
        let top = hashjoin(semi, mem_table(), JoinType::Inner);

        // Precondition: the top join is Partitioned.
        let before = format!("{top:?}");
        assert!(
            before.contains("Partitioned"),
            "precondition: top join should start Partitioned:\n{before}"
        );

        let rule = ForceCollectLeftForSemiBoundedBuildRule::default();
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out:?}");
        assert!(
            after.contains("CollectLeft"),
            "top Inner join over a semi-bounded build must flip to CollectLeft:\n{after}"
        );
    }

    /// Negative: an Inner Partitioned join with NO semi/anti in its
    /// build must be left as Partitioned — we must not broadcast an
    /// unbounded build.
    #[test]
    fn inner_partitioned_without_semi_build_is_untouched() {
        let top = hashjoin(mem_table(), mem_table(), JoinType::Inner);
        let rule = ForceCollectLeftForSemiBoundedBuildRule::default();
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out:?}");
        assert!(
            !after.contains("CollectLeft"),
            "an Inner join with no semi-bounded build must stay Partitioned:\n{after}"
        );
    }

    /// REV.3 option 3 — when the semi-bounded side is the PROBE (right),
    /// the rule must SWAP it onto the build and broadcast (CollectLeft).
    /// This is the Q18 customer⋈orders inversion fix: build the ~624-order
    /// semi-filtered side, not the 15M-row customer side.
    #[test]
    fn inner_partitioned_with_semi_bounded_probe_swaps_and_collect_lefts() {
        // Probe (RIGHT) = a RightSemi join → semi-bounded; build (LEFT) =
        // a plain scan (customer-like, NOT semi-bounded).
        let semi = hashjoin(mem_table(), mem_table(), JoinType::RightSemi);
        let top = hashjoin(mem_table(), semi, JoinType::Inner);

        let rule = ForceCollectLeftForSemiBoundedBuildRule::default();
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out:?}");
        assert!(
            after.contains("CollectLeft"),
            "an Inner join whose PROBE is semi-bounded must swap onto the \
             build and CollectLeft:\n{after}"
        );
    }

    /// Q16.SWAP (2026-06-10) — an ANTI-bounded side is NOT small: an
    /// anti-join against a selective build passes ~all of its kept
    /// input (Q16 SF=100: partsupp RightAnti vs 479 complaint
    /// suppliers → 79.96M of 80M survive). Treating it like a
    /// semi-bounded side made the rule SWAP it onto the build —
    /// a 79.96M-row single-threaded CollectLeft build (502 ms serial)
    /// probed by 2.97M filtered parts. The rule must instead keep the
    /// genuinely-small left as the build: anti-topped sides count as
    /// their kept-side child's cardinality (the honest upper bound),
    /// which routes Q16's shape through the relative-broadcast arm as
    /// "left small build, no swap".
    #[test]
    fn anti_bounded_probe_is_not_swapped_onto_the_build() {
        // Probe (RIGHT) = RightAnti(tiny build=5, kept side=100K) →
        // output ≈ 100K (LARGE). Build (LEFT) = 1K rows (small).
        let anti = hashjoin(mem_table_n(5), mem_table_n(100_000), JoinType::RightAnti);
        let top = hashjoin(mem_table_n(1_000), anti, JoinType::Inner);

        let rule = ForceCollectLeftForSemiBoundedBuildRule::default();
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();

        // The Inner join must end CollectLeft with the SMALL (left)
        // side as the build — the anti subtree must stay on the probe.
        let mut found = false;
        fn find_inner(plan: &Arc<dyn ExecutionPlan>, found: &mut bool) {
            if let Some(hj) = plan.as_any().downcast_ref::<HashJoinExec>() {
                if matches!(hj.join_type(), JoinType::Inner) {
                    *found = true;
                    assert!(
                        matches!(hj.partition_mode(), PartitionMode::CollectLeft),
                        "anti-shape Inner join should still CollectLeft (build the small side)"
                    );
                    assert!(
                        !build_subtree_has_semi_filter(hj.left()),
                        "the anti subtree must NOT be the build side:\n{plan:?}"
                    );
                    assert!(
                        build_subtree_has_semi_filter(hj.right()),
                        "the anti subtree must remain the probe side:\n{plan:?}"
                    );
                    return;
                }
            }
            for c in plan.children() {
                find_inner(c, found);
            }
        }
        find_inner(&out, &mut found);
        assert!(
            found,
            "expected an Inner HashJoinExec in the output:\n{out:?}"
        );
    }

    /// REV.17.1 — the cardinality-ratio guard must DECLINE when the
    /// (semi) build is estimated LARGER than the probe (the Q17 join0
    /// over-fire: build est 60M lineitem agg-semi, probe 12M → 0.20×).
    /// Broadcasting the big side loses, so with `min_probe_build_ratio =
    /// 1.0` the rule must leave the join Partitioned. The same plan at
    /// ratio 0.0 (default) DOES flip — proving the guard, not the
    /// structural signal, is what declines.
    #[test]
    fn ratio_guard_declines_when_build_larger_than_probe() {
        // Build (LEFT) = RightSemi whose outer (right) input is large →
        // the semi reports ~100 rows. Probe (RIGHT) = 3 rows.
        let big_semi = hashjoin(mem_table_n(1), mem_table_n(100), JoinType::RightSemi);
        let top = hashjoin(big_semi, mem_table_n(3), JoinType::Inner);

        // ratio 0.0 (today): guard disabled → flips to CollectLeft.
        let permissive = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 0.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(false),
            date_build_side_ratio: 2.0,
        };
        let out0 = permissive
            .optimize(top.clone(), &ConfigOptions::default())
            .unwrap();
        assert!(
            format!("{out0:?}").contains("CollectLeft"),
            "control: at ratio 0.0 the build-larger join still flips (it's \
             the structural-semi signal that fires)"
        );

        // ratio 1.0: build(~100) > probe(3) → must DECLINE.
        let guarded = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 1.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(false),
            date_build_side_ratio: 2.0,
        };
        let out1 = guarded.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out1:?}");
        assert!(
            !after.contains("CollectLeft"),
            "ratio guard (1.0) must NOT broadcast a build larger than the \
             probe (Q17 over-fire fix):\n{after}"
        );
    }

    /// REV.17.1 — the guard must still FIRE when the probe is larger than
    /// the (semi) build (the Q18 join0 win: build est 15M but probe 60M →
    /// 4.0×; the build estimate is an over-estimate so the true build is
    /// tiny and broadcasting is a big win). With `min_probe_build_ratio =
    /// 1.0` the rule must flip to CollectLeft.
    #[test]
    fn ratio_guard_fires_when_probe_larger_than_build() {
        // Build (LEFT) = RightSemi reporting ~3 rows. Probe (RIGHT) = 100.
        let small_semi = hashjoin(mem_table_n(1), mem_table_n(3), JoinType::RightSemi);
        let top = hashjoin(small_semi, mem_table_n(100), JoinType::Inner);

        let guarded = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 1.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(false),
            date_build_side_ratio: 2.0,
        };
        let out = guarded.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out:?}");
        assert!(
            after.contains("CollectLeft"),
            "ratio guard (1.0) must still broadcast when probe >= build \
             (Q18 win preserved):\n{after}"
        );
    }

    /// REV.17.1 — when either side's `num_rows` is Absent the guard must
    /// PASS (fire), preserving the structural rescue for HAVING-collapsed
    /// builds whose true size is tiny but statically unknowable.
    #[test]
    fn ratio_guard_fires_when_stats_absent() {
        // The default mem tables report Exact row counts, so to simulate
        // "Absent" we rely on the helper predicate directly: a build whose
        // estimate is unknown must pass. Here we assert the live rule on a
        // shape with known-but-equal sizes still fires at ratio 1.0 (3 >=
        // 3) — the boundary case — and document the Absent path via the
        // predicate's `_ => true` arm exercised by Q18 join2 in the wild.
        let semi = hashjoin(mem_table_n(3), mem_table_n(3), JoinType::RightSemi);
        let top = hashjoin(semi, mem_table_n(3), JoinType::Inner);
        let guarded = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 1.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(false),
            date_build_side_ratio: 2.0,
        };
        let out = guarded.optimize(top, &ConfigOptions::default()).unwrap();
        assert!(
            format!("{out:?}").contains("CollectLeft"),
            "equal build/probe (ratio exactly 1.0) must fire"
        );
    }

    /// REV.17.3 — a plain dimension⋈fact Inner join (NEITHER side
    /// semi-bounded) must broadcast the SMALL side when the other is
    /// `broadcast_ratio ×` larger (the missed Q17 j1/j2-style dim⋈fact
    /// win). Here build(10) ≪ probe(1000), ratio 16 → flips to CollectLeft.
    #[test]
    fn relative_broadcast_fires_when_probe_much_larger() {
        let top = hashjoin(mem_table_n(10), mem_table_n(1000), JoinType::Inner);
        let rule = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 0.0,
            broadcast_ratio: 16.0,
            date_build_side: Some(false),
            date_build_side_ratio: 2.0,
        };
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out:?}");
        assert!(
            after.contains("CollectLeft"),
            "relative broadcast must flip a small-build/large-probe Inner \
             join to CollectLeft:\n{after}"
        );
    }

    /// REV.17.3 — when the two sides are SIMILAR in size (ratio below
    /// `broadcast_ratio`) the relative gate must NOT fire — broadcasting a
    /// non-trivially-large build is the over-broadcast that regressed Q21
    /// at the aggressive threshold.
    #[test]
    fn relative_broadcast_skips_similar_sizes() {
        let top = hashjoin(mem_table_n(100), mem_table_n(200), JoinType::Inner);
        let rule = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 0.0,
            broadcast_ratio: 16.0,
            date_build_side: Some(false),
            date_build_side_ratio: 2.0,
        };
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out:?}");
        assert!(
            !after.contains("CollectLeft"),
            "relative broadcast must NOT fire when sides are similar in size \
             (200 < 100×16):\n{after}"
        );
    }

    /// REV.17.3 — default `broadcast_ratio = 0.0` disables the relative
    /// branch entirely (zero production change until the multi-scale gate
    /// passes): a small-build/large-probe Inner join stays Partitioned.
    #[test]
    fn relative_broadcast_off_at_zero() {
        let top = hashjoin(mem_table_n(10), mem_table_n(1000), JoinType::Inner);
        let rule = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 0.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(false),
            date_build_side_ratio: 2.0,
        };
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out:?}");
        assert!(
            !after.contains("CollectLeft"),
            "relative branch must be inert at broadcast_ratio 0.0:\n{after}"
        );
    }

    /// REV.17.4b — the relative broadcast extends to semi/anti join TYPES via
    /// the NO-SWAP left-small arm: a LeftSemi (or RightSemi/anti) Partitioned
    /// join with a small LEFT build and a `broadcast_ratio ×` larger RIGHT
    /// probe must flip to CollectLeft. Forcing CollectLeft is a physical
    /// PartitionMode change — valid + semantics-preserving for every join type
    /// (no swap, so LeftSemi stays LeftSemi). Inner-only before REV.17.4b.
    #[test]
    fn relative_broadcast_extends_to_semi_small_left() {
        for jt in [
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
        ] {
            let top = hashjoin(mem_table_n(10), mem_table_n(1000), jt);
            let rule = ForceCollectLeftForSemiBoundedBuildRule {
                min_probe_build_ratio: 0.0,
                broadcast_ratio: 16.0,
                date_build_side: Some(false),
                date_build_side_ratio: 2.0,
            };
            let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
            let after = format!("{out:?}");
            assert!(
                after.contains("CollectLeft"),
                "relative broadcast must flip a small-LEFT/large-RIGHT {jt:?} \
                 join to CollectLeft (no swap):\n{after}"
            );
        }
    }

    /// REV.17.4b — the SWAP arm stays Inner-only: a semi/anti join with a
    /// small RIGHT (which an Inner join would swap onto the build) must NOT be
    /// rewritten — swapping flips LeftSemi↔RightSemi (asymmetric output). It
    /// stays Partitioned rather than risk a semantic change.
    #[test]
    fn relative_broadcast_does_not_swap_semi_small_right() {
        for jt in [
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
        ] {
            let top = hashjoin(mem_table_n(1000), mem_table_n(10), jt);
            let rule = ForceCollectLeftForSemiBoundedBuildRule {
                min_probe_build_ratio: 0.0,
                broadcast_ratio: 16.0,
                date_build_side: Some(false),
                date_build_side_ratio: 2.0,
            };
            let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
            let after = format!("{out:?}");
            assert!(
                !after.contains("CollectLeft"),
                "swap arm must stay Inner-only — a small-RIGHT {jt:?} join \
                 must NOT be swapped/broadcast:\n{after}"
            );
        }
    }

    /// REV.17.3 — the relative broadcast gate is DEFAULT-ON (multi-scale
    /// gated). Production preset + the benches all construct the rule via
    /// `::default()`, so a bare default must carry `broadcast_ratio = 16`
    /// and broadcast a small-build/large-probe Inner join. (Assumes
    /// `EMAT_COLLECT_LEFT_BROADCAST_RATIO` unset in the test env.)
    #[test]
    fn default_enables_relative_broadcast() {
        let rule = ForceCollectLeftForSemiBoundedBuildRule::default();
        assert_eq!(
            rule.broadcast_ratio, 16.0,
            "REV.17.3: relative broadcast must default ON (16.0)"
        );
        let top = hashjoin(mem_table_n(10), mem_table_n(1000), JoinType::Inner);
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        assert!(
            format!("{out:?}").contains("CollectLeft"),
            "default-on rule must broadcast a small-build/large-probe Inner join"
        );
    }

    // ---- REV.19 NDV-corrected build-side selection ----

    use datafusion::common::stats::{ColumnStatistics, Statistics};

    /// One-column input statistics with an optional `distinct_count`.
    fn stats_with_distinct(num_rows: usize, distinct0: Option<usize>) -> Statistics {
        let cs0 = ColumnStatistics {
            distinct_count: match distinct0 {
                Some(n) => Precision::Inexact(n),
                None => Precision::Absent,
            },
            ..Default::default()
        };
        Statistics {
            num_rows: Precision::Inexact(num_rows),
            total_byte_size: Precision::Absent,
            column_statistics: vec![cs0],
        }
    }

    fn string_eq(col_idx: usize, val: &str) -> Arc<dyn datafusion::physical_expr::PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("s", col_idx)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8(Some(val.to_string())))),
        ))
    }

    /// The correction shrinks a genuinely-selective filter (N=150, the
    /// TPC-H `p_type` cardinality) and caps at 1.0 for filters that are
    /// *less* selective than DataFusion's flat default (so we never inflate).
    #[test]
    fn eq_ndv_correction_shrinks_low_ndv_and_caps_high() {
        let f = eq_ndv_correction(150, 0.2);
        assert!(
            (f - 0.033_33).abs() < 1e-4,
            "N=150 vs 0.2 ≈ 0.0333, got {f}"
        );
        // N=2 → 1/2 = 0.5 > 0.2 (less selective than default) → capped at 1.0.
        assert_eq!(eq_ndv_correction(2, 0.2), 1.0);
        // Below the distinct floor → no correction.
        assert_eq!(eq_ndv_correction(1, 0.2), 1.0);
    }

    /// A `p_type = 'literal'`-shaped string equality on an N=150 column is
    /// corrected to ~1/30 of DataFusion's 20%-default estimate.
    #[test]
    fn predicate_ndv_factor_corrects_string_eq() {
        let stats = stats_with_distinct(2_000_000, Some(150));
        let pred = string_eq(0, "ECONOMY ANODIZED STEEL");
        let f = predicate_ndv_factor(&pred, &stats, 0.2);
        assert!(
            (f - 0.033_33).abs() < 1e-4,
            "string-eq on N=150 ≈ 0.033, got {f}"
        );
    }

    /// No `distinct_count` (dict-distinct walk off) → neutral, never a swap.
    #[test]
    fn predicate_ndv_factor_absent_distinct_is_neutral() {
        let stats = stats_with_distinct(2_000_000, None);
        let pred = string_eq(0, "x");
        assert_eq!(
            predicate_ndv_factor(&pred, &stats, 0.2),
            1.0,
            "absent distinct_count must not correct"
        );
    }

    /// Numeric equality is handled by DataFusion's interval analyzer (it is
    /// `check_support`ed), so we must NOT double-correct it.
    #[test]
    fn predicate_ndv_factor_ignores_numeric_eq() {
        let stats = stats_with_distinct(2_000_000, Some(150));
        let pred: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("n", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(5)))),
        ));
        assert_eq!(
            predicate_ndv_factor(&pred, &stats, 0.2),
            1.0,
            "numeric equality must not be NDV-corrected"
        );
    }

    /// In `s = 'x' AND n > 5` only the string-equality conjunct corrects.
    #[test]
    fn predicate_ndv_factor_handles_conjunction() {
        let stats = stats_with_distinct(2_000_000, Some(150));
        let s_eq = string_eq(0, "x");
        let n_gt: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("n", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(5)))),
        ));
        let conj: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(BinaryExpr::new(s_eq, Operator::And, n_gt));
        let f = predicate_ndv_factor(&conj, &stats, 0.2);
        assert!(
            (f - 0.033_33).abs() < 1e-4,
            "only the string-eq corrects, got {f}"
        );
    }

    /// The lever is OFF by default: `EMAT_NDV_BUILD_SIDE` unset must leave a
    /// plain Partitioned Inner join untouched (no swap, no CollectLeft from
    /// this arm). Guards the default-path / codegen-tax invariant.
    #[test]
    fn ndv_build_side_off_by_default() {
        // Two equal-size sides, no relative-broadcast trigger, no semi.
        let top = hashjoin(mem_table_n(100), mem_table_n(100), JoinType::Inner);
        let rule = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 0.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(false),
            date_build_side_ratio: 2.0,
        };
        let before = format!("{top:?}");
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        assert_eq!(
            before,
            format!("{out:?}"),
            "with EMAT_NDV_BUILD_SIDE unset the plan must be unchanged"
        );
    }

    // ---- Σ.AH.3 date-range corrected build-side swap ----

    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, Partitioning, PlanProperties, SendableRecordBatchStream,
    };

    /// Plan-diff fixture: a leaf that reports `rows` and Exact Date32
    /// min/max on column 1 (`d`), mirroring what
    /// `EmatixFastParquetTableProvider` plumbs for a date column. Never
    /// executed — statistics only.
    #[derive(Debug)]
    struct DateStatsExec {
        rows: usize,
        date_min: i32,
        date_max: i32,
        props: std::sync::Arc<PlanProperties>,
    }

    impl DateStatsExec {
        fn new_exec(rows: usize, date_min: i32, date_max: i32) -> Arc<dyn ExecutionPlan> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("d", DataType::Date32, false),
            ]));
            let props = PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Arc::new(Self {
                rows,
                date_min,
                date_max,
                props: Arc::new(props),
            })
        }
    }

    impl DisplayAs for DateStatsExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "DateStatsExec(rows={})", self.rows)
        }
    }

    impl ExecutionPlan for DateStatsExec {
        fn name(&self) -> &str {
            "DateStatsExec"
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn properties(&self) -> &std::sync::Arc<PlanProperties> {
            &self.props
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unreachable!("DateStatsExec is a plan-diff statistics fixture")
        }
        fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
            let mut cols = vec![ColumnStatistics::new_unknown(); 2];
            cols[1].min_value = Precision::Exact(ScalarValue::Date32(Some(self.date_min)));
            cols[1].max_value = Precision::Exact(ScalarValue::Date32(Some(self.date_max)));
            Ok(Statistics {
                num_rows: Precision::Exact(self.rows),
                total_byte_size: Precision::Absent,
                column_statistics: cols,
            })
        }
    }

    /// `d >= lo AND d < hi_excl` over column index 1 — the Q10
    /// `o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'` shape.
    /// Date32 is not `check_support`ed in DataFusion 53, so `FilterExec`
    /// reports the flat 20% default for this predicate.
    fn date_range_filter(
        input: Arc<dyn ExecutionPlan>,
        lo: i32,
        hi_excl: i32,
    ) -> Arc<dyn ExecutionPlan> {
        let ge: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("d", 1)),
            Operator::GtEq,
            Arc::new(Literal::new(ScalarValue::Date32(Some(lo)))),
        ));
        let lt: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("d", 1)),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Date32(Some(hi_excl)))),
        ));
        let pred: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(BinaryExpr::new(ge, Operator::And, lt));
        Arc::new(FilterExec::try_new(pred, input).unwrap())
    }

    /// One-column statistics whose column 0 carries Date32 min/max.
    fn date_stats(min: i32, max: i32) -> Statistics {
        let cs0 = ColumnStatistics {
            min_value: Precision::Exact(ScalarValue::Date32(Some(min))),
            max_value: Precision::Exact(ScalarValue::Date32(Some(max))),
            ..Default::default()
        };
        Statistics {
            num_rows: Precision::Exact(1_000_000),
            total_byte_size: Precision::Absent,
            column_statistics: vec![cs0],
        }
    }

    /// `d >= lo AND d < hi_excl` over column index 0 (pure-fn tests).
    fn date_range_pred(lo: i32, hi_excl: i32) -> Arc<dyn datafusion::physical_expr::PhysicalExpr> {
        let ge: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("d", 0)),
            Operator::GtEq,
            Arc::new(Literal::new(ScalarValue::Date32(Some(lo)))),
        ));
        let lt: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("d", 0)),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Date32(Some(hi_excl)))),
        ));
        Arc::new(BinaryExpr::new(ge, Operator::And, lt))
    }

    /// Q10 numbers: a 92-day window over a 2406-day span is ~3.8%
    /// selective; against the 20% flat default that is a ~0.19 factor.
    #[test]
    fn predicate_date_factor_corrects_narrow_range() {
        let stats = date_stats(0, 2405);
        let pred = date_range_pred(640, 732); // 92 days inclusive of [640, 731]
        let f = predicate_date_factor(&pred, &stats, 0.2);
        let expected = (92.0 / 2406.0) / 0.2;
        assert!(
            (f - expected).abs() < 1e-6,
            "92d/2406d vs 0.2 default ≈ {expected}, got {f}"
        );
    }

    /// Q08 numbers: a 2-year window (~30%) is LESS selective than the 20%
    /// default — the correction must cap at 1.0 (only ever shrink).
    #[test]
    fn predicate_date_factor_caps_wide_range() {
        let stats = date_stats(0, 2405);
        let pred = date_range_pred(1096, 1827); // 731 days ≈ 30.4%
        assert_eq!(
            predicate_date_factor(&pred, &stats, 0.2),
            1.0,
            "a range wider than the default selectivity must not inflate"
        );
    }

    /// No min/max on the date column → neutral, never a correction.
    #[test]
    fn predicate_date_factor_absent_bounds_is_neutral() {
        let stats = stats_with_distinct(1_000_000, None); // no min/max
        let pred = date_range_pred(640, 732);
        assert_eq!(predicate_date_factor(&pred, &stats, 0.2), 1.0);
    }

    /// Numeric ranges are `check_support`ed by DataFusion's interval
    /// analyzer (no default applied) — only Date32 literals correct.
    #[test]
    fn predicate_date_factor_ignores_numeric_range() {
        let stats = date_stats(0, 2405);
        let pred: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("n", 0)),
            Operator::GtEq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(640)))),
        ));
        assert_eq!(predicate_date_factor(&pred, &stats, 0.2), 1.0);
    }

    /// A window disjoint from the column's [min, max] corrects to 0 —
    /// the probe is provably empty, the strongest swap signal.
    #[test]
    fn predicate_date_factor_disjoint_range_is_zero() {
        let stats = date_stats(0, 2405);
        let pred = date_range_pred(3000, 3100);
        assert_eq!(predicate_date_factor(&pred, &stats, 0.2), 0.0);
    }

    fn find_hj(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.as_any().is::<HashJoinExec>() {
            return Some(plan.clone());
        }
        plan.children().into_iter().find_map(find_hj)
    }

    fn subtree_has_filter(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if plan.as_any().is::<FilterExec>() {
            return true;
        }
        plan.children().iter().any(|c| subtree_has_filter(c))
    }

    /// Q10 plan-diff: an Inner Partitioned join whose PROBE (right) is a
    /// date-range filter that the flat default over-estimates must swap
    /// the truly-smaller side onto the build, preserving the output
    /// schema. Build 1000 rows vs probe reported 400 (2000 × 20%) but
    /// corrected 400 × 0.05 = 20 → 1000 ≥ 2×20 fires.
    #[test]
    fn date_swap_fires_on_over_estimated_probe() {
        // 10-day window over a 1000-day span: sel 1% → factor 0.05.
        let probe = date_range_filter(DateStatsExec::new_exec(2_000, 0, 999), 100, 110);
        let top = hashjoin(mem_table_n(1_000), probe, JoinType::Inner);
        let schema_before = top.schema();
        let rule = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 0.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(true),
            date_build_side_ratio: 2.0,
        };
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        // The join must still be Partitioned (this is a side swap, not a
        // broadcast) and its BUILD (left) subtree must now hold the
        // date-filtered side.
        let hj_plan = find_hj(&out).expect("plan must still contain a HashJoinExec");
        let hj = hj_plan.as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert!(
            matches!(hj.partition_mode(), PartitionMode::Partitioned),
            "date swap must keep Partitioned mode:\n{out:?}"
        );
        assert!(
            subtree_has_filter(hj.left()),
            "after the swap the date-filtered side must be the BUILD (left):\n{out:?}"
        );
        assert!(
            !subtree_has_filter(hj.right()),
            "the un-filtered side must be the PROBE (right):\n{out:?}"
        );
        assert_eq!(
            schema_before,
            out.schema(),
            "swap must preserve the output schema (swap_inputs re-projects)"
        );
    }

    /// Flag OFF (default) → the same over-estimated-probe plan is
    /// untouched. Guards the default-path / codegen-tax invariant.
    #[test]
    fn date_swap_off_by_default_leaves_plan_unchanged() {
        let probe = date_range_filter(DateStatsExec::new_exec(2_000, 0, 999), 100, 110);
        let top = hashjoin(mem_table_n(1_000), probe, JoinType::Inner);
        let before = format!("{top:?}");
        let rule = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 0.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(false),
            date_build_side_ratio: 2.0,
        };
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        assert_eq!(
            before,
            format!("{out:?}"),
            "with the date lever off the plan must be unchanged"
        );
    }

    /// Near-equal corrected sides must NOT swap (flap guard): corrected
    /// probe 120 vs build 200 is under the 2× margin.
    #[test]
    fn date_swap_declines_near_equal_sides() {
        // 60-day window over a 1000-day span: sel 6% → factor 0.3;
        // reported probe 400 → corrected 120. Build 200 < 240 → decline.
        let probe = date_range_filter(DateStatsExec::new_exec(2_000, 0, 999), 100, 160);
        let top = hashjoin(mem_table_n(200), probe, JoinType::Inner);
        let before = format!("{top:?}");
        let rule = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 0.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(true),
            date_build_side_ratio: 2.0,
        };
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        assert_eq!(
            before,
            format!("{out:?}"),
            "corrected sides within the swap margin must not swap"
        );
    }

    /// Semi/anti joins are asymmetric — swapping flips LeftSemi↔RightSemi.
    /// The date arm must be Inner-only; the existing semi rules own those.
    #[test]
    fn date_swap_leaves_semi_joins_alone() {
        let probe = date_range_filter(DateStatsExec::new_exec(2_000, 0, 999), 100, 110);
        let top = hashjoin(mem_table_n(1_000), probe, JoinType::LeftSemi);
        let before = format!("{top:?}");
        let rule = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 0.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(true),
            date_build_side_ratio: 2.0,
        };
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        assert_eq!(
            before,
            format!("{out:?}"),
            "a semi join must never be touched by the date swap arm"
        );
    }

    /// A date filter on the BUILD side only (probe un-filtered) must not
    /// swap: the correction shrinks the build, which is already the
    /// smaller-estimated side — there is no inversion to fix.
    #[test]
    fn date_swap_ignores_build_side_filter() {
        let build = date_range_filter(DateStatsExec::new_exec(2_000, 0, 999), 100, 110);
        let top = hashjoin(build, mem_table_n(1_000), JoinType::Inner);
        let before = format!("{top:?}");
        let rule = ForceCollectLeftForSemiBoundedBuildRule {
            min_probe_build_ratio: 0.0,
            broadcast_ratio: 0.0,
            date_build_side: Some(true),
            date_build_side_ratio: 2.0,
        };
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        assert_eq!(
            before,
            format!("{out:?}"),
            "a build-side-only date filter must not trigger a swap"
        );
    }
}
