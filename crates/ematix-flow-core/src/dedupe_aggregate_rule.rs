//! `DedupeAggregateForFloatDeterminism` — `PhysicalOptimizerRule` that
//! detects structurally-identical f64-aggregate subtrees in the plan
//! and rewrites both locations to share a single cached computation
//! via [`SharedSubtreeExec`](crate::shared_subtree_exec::SharedSubtreeExec).
//!
//! ## Why
//!
//! TPC-H Q15's optimizer output materializes the `revenue_s` CTE as
//! two completely separate `Aggregate(SUM groupBy=l_suppkey)` subtrees
//! — once in the outer FROM and once inside the scalar `MAX(...)`
//! subquery. The two SUMs run in parallel across 14 partitions, and
//! parallel f64 summation reorders operands depending on thread
//! scheduling, producing values that differ by ULP. The outer query's
//! `WHERE total_revenue = (SELECT MAX(total_revenue) FROM revenue_s)`
//! then drops the matching row about 40% of the time.
//!
//! DuckDB and Polars don't show this — DuckDB materializes the CTE
//! once; Polars performs subquery-CSE at logical-planning. DataFusion
//! 53 has no non-recursive CTE materialization. This rule supplies the
//! missing materialization at the physical layer.
//!
//! ## What this rule does
//!
//! 1. Walk the physical plan, structurally hash each `AggregateExec`
//!    subtree containing any f64 column.
//! 2. Find any hash appearing 2+ times — these are duplicated
//!    aggregate computations.
//! 3. For each Final/FinalPartitioned `AggregateExec` whose subtree
//!    hash matches a duplicate, wrap it in a `SharedSubtreeExec` whose
//!    cache is keyed on the structural hash. Both duplicate locations
//!    resolve to the same `Arc<CachedBatches>` through the
//!    [`SharedSubtreeRegistry`](crate::shared_subtree_exec::SharedSubtreeRegistry).
//!    First execution populates; the second replays. Result: ONE
//!    aggregate computation, served bit-identical to both consumers.
//!
//! ## Why this is safe
//!
//! - Structural identity is conservative: false negatives (don't fire
//!   when we could) are fine; false positives would silently change
//!   plans we shouldn't.
//! - Cache contents come from running the original Final aggregate
//!   subtree exactly as the planner produced it — no semantic
//!   rewriting, no `mode=Single` substitution. Whatever DataFusion
//!   computed once is what both consumers see.
//! - Only fires when an aggregate is *actually* duplicated, which is
//!   a rare structural shape (only Q15 in TPC-H 22).
//!
//! ## Query-scoping (BF, 2026-06-09)
//!
//! Caching is scoped PER-QUERY: `optimize()` allocates a fresh
//! `SharedSubtreeRegistry` per call, so the cache only collapses THIS query's
//! duplicate subtrees (Q15's two f64 SUMs) into one computation. Cross-query
//! sharing was removed — on a long-lived `SessionContext` it (a) accumulated
//! `CachedBatches` across queries (degrading later queries) and (b) memoized
//! across queries, serving a STALE cached result on a re-run. The `registry`
//! field / [`with_registry`](DedupeAggregateForFloatDeterminism::with_registry)
//! are retained for API back-compat but no longer drive caching.
//!
//! ## Structural hash
//!
//! Treats `RepartitionExec`, `CoalesceBatchesExec`, and `SortExec` as
//! semantically transparent — two subtrees that differ only in those
//! partitioning/ordering wrappers hash the same. Unknown node types
//! fall back to `displayable` + recursive children, which catches
//! `TableScan` source path and pushed-down predicates.
//!
//! ## Σ.Q15 — DynamicFilter normalization + canonical copy (2026-07-12)
//!
//! DataFusion's post-optimization `FilterPushdown` injects join-key
//! `DynamicFilter [..]` placeholders into probe-side scans — pushed
//! THROUGH group-by aggregates when the join key is the group key. In
//! sessions whose scans stay stock `DataSourceExec` (the distributed
//! campaign session; plain sessions were shielded only because the
//! fast-scan resolver replaces the node, which pushdown can't touch),
//! exactly ONE of Q15's two revenue subtrees is such a probe. The
//! placeholder split the structural hashes, the rule silently
//! disengaged (0 wraps), and the two independent parallel f64 SUMs
//! ULP-diverged — `total_revenue = (SELECT max(..))` dropped every row
//! on a large fraction of runs. Fix, two halves (both required):
//!
//! 1. the structural hash strips `DynamicFilter` fragments (they are
//!    per-consumer runtime pruning hints, not part of the computation's
//!    identity), so the twin sites hash equal again;
//! 2. every wrap in a duplicate group holds the SAME canonical
//!    dynamic-filter-FREE copy of the subtree. Removing a dynamic
//!    filter is always sound (it only prunes rows its owning join would
//!    reject anyway); imposing one site's filter on another consumer is
//!    not — and with per-site subtrees the cache content would depend
//!    on which wrap populated first. Groups where every copy carries a
//!    DynamicFilter are skipped (no safe copy to share).
//!
//! See `project_tpch_correctness_gaps` for diagnosis, the
//! `shared_subtree_exec` module for the cache primitive, and
//! `crates/ematix-flow-core/examples/q21_inspect.rs` for the
//! reproducer (env: `Q=15 PARTITIONS=14`).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DfResult;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
// CoalesceBatchesExec is deprecated in favor of arrow-rs BatchCoalescer
// but DataFusion's planner still emits it in plan trees. We treat it as
// a transparent wrapper in our structural-hash walk.
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;

use crate::shared_subtree_exec::{SharedSubtreeExec, SharedSubtreeRegistry};

/// `PhysicalOptimizerRule` that wraps duplicated f64-aggregate subtrees
/// in `SharedSubtreeExec` so both consumers share one cached computation.
///
/// BF (2026-06-09): caching is now scoped PER-QUERY — `optimize()` allocates a
/// fresh `SharedSubtreeRegistry` per call. The `registry` field / [`with_registry`]
/// / [`registry`](Self::registry) accessor are retained for API back-compat but
/// no longer share cache across queries (cross-query sharing degraded long-lived
/// contexts and served stale memoized results). `default()` is the normal way to
/// construct the rule.
#[derive(Debug)]
pub struct DedupeAggregateForFloatDeterminism {
    /// Retained for API back-compat; not used for caching (see struct doc).
    registry: Arc<SharedSubtreeRegistry>,
}

impl Default for DedupeAggregateForFloatDeterminism {
    fn default() -> Self {
        Self::with_registry(Arc::new(SharedSubtreeRegistry::new()))
    }
}

impl DedupeAggregateForFloatDeterminism {
    pub fn with_registry(registry: Arc<SharedSubtreeRegistry>) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &Arc<SharedSubtreeRegistry> {
        &self.registry
    }
}

impl PhysicalOptimizerRule for DedupeAggregateForFloatDeterminism {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Pass 1: walk plan top-down, hash each Final-mode AggregateExec
        // subtree that contains an f64 column. Count occurrences, and
        // record the first copy whose subtree carries no DynamicFilter —
        // the canonical copy every wrap in the group will share (Σ.Q15,
        // see below). The walk is cheap — 11-trial bench (2026-05-22)
        // shows zero measurable cost on Q22 vs the rule not being
        // installed at all. Earlier "Q22 +7%" measurements were
        // 3/7-trial noise.
        type Site = (usize, Option<Arc<dyn ExecutionPlan>>);
        let mut sites: std::collections::HashMap<u64, Site> = std::collections::HashMap::new();
        let _ = plan.clone().transform_down(|node| {
            if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>()
                && matches!(
                    agg.mode(),
                    AggregateMode::Final | AggregateMode::FinalPartitioned
                )
                && has_float_aggregate(agg)
            {
                let h = subtree_hash(&node);
                let entry = sites.entry(h).or_insert((0, None));
                entry.0 += 1;
                if entry.1.is_none() && !subtree_has_dynamic_filter(&node) {
                    entry.1 = Some(node.clone());
                }
            }
            Ok(Transformed::no(node))
        })?;

        // Σ.Q15 (2026-07-12): the structural hash ignores `DynamicFilter`
        // placeholders (per-consumer runtime pruning hints — see
        // `strip_dynamic_filters`), so a probe-side copy hashes equal to
        // its filter-free twin. That makes it MANDATORY that every wrap in
        // a group hold the SAME canonical dynamic-filter-FREE subtree:
        // with per-site subtrees the shared cache's content would depend
        // on which wrap executed first, and a dynamic-filtered stream is
        // only valid for the consumer whose join installed the filter.
        // Removing a dynamic filter is always sound (it only prunes rows
        // its owning join would reject anyway); imposing one site's filter
        // on another consumer is not. A group where every copy carries a
        // DynamicFilter has no safe copy to share — skip it.
        //
        // PV.M.7 filter fusion is applied ONCE per group here (not per
        // wrap site) for the same reason: every wrap must hold one Arc.
        // The fusion is schema-preserving (a ProjectionExec replaces the
        // FilterExec's projection), so the registry key stays valid.
        // Sealed CSE subtrees are never InjectFused-eligible, so there is
        // no collision.
        let mut dupes: std::collections::HashMap<u64, Arc<dyn ExecutionPlan>> =
            std::collections::HashMap::new();
        for (h, (n, canonical)) in sites {
            if n < 2 {
                continue;
            }
            match canonical {
                Some(c) => {
                    let fused = if cse_filter_fusion_enabled() {
                        crate::drop_redundant_filter_rule::fuse_redundant_bridge_filters(c.clone())?
                    } else {
                        c.clone()
                    };
                    debug_assert_eq!(
                        fused.schema(),
                        c.schema(),
                        "PV.M.7 CSE filter-fusion must preserve subtree schema"
                    );
                    dupes.insert(h, fused);
                }
                None => {
                    tracing::info!(
                        sites = n,
                        "dedupe-f64: skipping duplicate aggregate group — every copy \
                         carries a DynamicFilter, so no safe canonical subtree exists"
                    );
                }
            }
        }
        if dupes.is_empty() {
            return Ok(plan);
        }

        // Pass 2: walk top-down. For every Final-mode aggregate whose
        // subtree hash is in `dupes`, wrap it in a SharedSubtreeExec
        // keyed on that hash. All duplicates with the same hash resolve
        // to the SAME Arc<CachedBatches> via the registry, so first
        // execute() populates and the rest replay — one computation
        // total, bit-identical reads on both sides.
        //
        // The hash is computed BEFORE wrapping. SharedSubtreeExec is a
        // leaf to subsequent plan walks (children() = []), so the walk
        // stops once we replace and doesn't descend into the wrapped
        // subtree.
        // BF (2026-06-09): use a FRESH registry per optimize() call (= per
        // query), NOT the ctx-lifetime `self.registry`. A shared registry
        // (a) accumulates CachedBatches across queries → a long-lived ctx
        // degrades 1.3-1.5× (SF=10: Q13 116→170ms, Q22 25→45ms), and (b)
        // cross-query MEMOIZES — `get_or_create(h)` returns a prior query's
        // cached batches on a hash match, so re-running a query serves a
        // STALE result (Q15 SF=10: 7.9ms stale vs 78ms honest). The registry
        // is only needed to make THIS query's duplicate subtrees (same hash,
        // same optimize() call) share one computation; a per-call registry
        // preserves that within-query CSE while being concurrent-safe and
        // free of cross-query state. (`self.registry` is retained for API
        // back-compat but no longer drives caching.)
        let registry = Arc::new(SharedSubtreeRegistry::new());
        let rewritten = plan.transform_down(|node| {
            if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>()
                && matches!(
                    agg.mode(),
                    AggregateMode::Final | AggregateMode::FinalPartitioned
                )
                && has_float_aggregate(agg)
            {
                let h = subtree_hash(&node);
                if let Some(canonical) = dupes.get(&h) {
                    // Conservative guard: a hash-equal site whose schema
                    // differs from the canonical copy would corrupt the
                    // parent plan — leave it alone. (Structural identity
                    // implies schema identity, so this never fires in
                    // practice; it backstops a hash collision or a
                    // display-normalization overreach.)
                    if canonical.schema() != node.schema() {
                        return Ok(Transformed::no(node));
                    }
                    let cached = registry.get_or_create(h, canonical.schema());
                    // Σ.Q15: every wrap holds the SAME canonical subtree
                    // Arc (PV.M.7 fusion already applied once, above) —
                    // see the canonical-copy comment on pass 1.5. After
                    // the wrap, SharedSubtreeExec.children()==[] seals
                    // the subtree from every later physical rule.
                    let wrapped: Arc<dyn ExecutionPlan> =
                        Arc::new(SharedSubtreeExec::new(canonical.clone(), cached));
                    return Ok(Transformed::yes(wrapped));
                }
            }
            Ok(Transformed::no(node))
        })?;

        if !rewritten.transformed {
            return Ok(rewritten.data);
        }

        // Σ.BS — repair partitioning after the wrap.
        //
        // This rule runs LAST in the physical pipeline (custom rules are
        // appended after `JoinSelection` / `EnforceDistribution` /
        // `SanityCheckPlan`). When a duplicated f64 aggregate feeds a
        // `HashJoinExec(PartitionMode::Partitioned)` — which happens once
        // the build side is large enough, i.e. at SF≥100 — the optimizer
        // has already committed both join inputs to N hash-partitions.
        // The agg's own `Hash[key]` output satisfied the join's
        // requirement directly, so there is NO `RepartitionExec` above it
        // to act as a buffer. Wrapping the agg in `SharedSubtreeExec`
        // (which reports `UnknownPartitioning(1)`) then collapses that
        // side from N → 1 partitions, and the join's execute()-time
        // invariant `left_partitions == right_partitions` fails with
        // "Invalid HashJoinExec, partition count mismatch N!=1". At
        // SF=1/10 the build side is small, `JoinSelection` picks
        // `CollectLeft` (left must be 1 partition — which the wrapper
        // satisfies), and the bug stays hidden.
        //
        // Re-running `EnforceDistribution` on the wrapped plan re-derives
        // each parent's required input distribution from scratch and
        // inserts the `RepartitionExec` (Hash on the join key) above the
        // `SharedSubtreeExec` for the join consumer, while leaving the
        // scalar-MAX consumer coalesced to 1 partition — restoring a
        // valid, correct plan. The re-hash of the collapsed single stream
        // by the join key reproduces the hash distribution the sibling
        // side already uses, so results are unchanged.
        //
        // Gated on `rewritten.transformed`, so this only runs on plans
        // that actually contain a duplicated f64 aggregate (Q15 in TPC-H);
        // the other 21 queries returned early via the `dupes.is_empty()`
        // check above and never reach here.
        EnforceDistribution::new().optimize(rewritten.data, config)
    }

    fn name(&self) -> &str {
        "ematix_flow_dedupe_aggregate_for_float_determinism"
    }

    fn schema_check(&self) -> bool {
        // SharedSubtreeExec.schema() == input.schema(); wrapping is a
        // pure pass-through at the schema level.
        true
    }
}

/// Returns true if `agg`'s output schema contains any Float64 column —
/// narrows the rule to f64-determinism-sensitive aggregates and avoids
/// touching integer COUNT/SUM that are bit-exact regardless of ordering.
fn has_float_aggregate(agg: &AggregateExec) -> bool {
    agg.schema()
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Float64))
}

/// PV.M.7 — gate for fusing redundant bridge FilterExecs into the masked
/// scan inside CSE-sealed subtrees. DEFAULT-ON (#308); `EMAT_CSE_FILTER_FUSION=0`
/// (or `false`) disables — the A/B off-arm and a regression escape hatch.
///
/// Default-on is safe and free, but NOT a wall-time win at the operating
/// points we ship/bench. The masked-fusion projection-prunes the filter-only
/// column out of the scan's DECODE projection, removing one redundant Snappy
/// decompress of `l_shipdate` — real CPU work, but on a 14-core box (and even
/// at SF=100) that saving lands OFF the wall-clock critical path, so Q15 is
/// wall-neutral: order-balanced interleaved A/B measured −0.6% (SF=10 Phase-2
/// on), −0.3% (SF=10 Phase-2 off), −0.7%/+0.7% median/trimmed (SF=100) — all
/// within jitter. (The "−8%" from the original spike was cross-process thermal
/// drift in the two-process q15_full_ab, since absorbed by REV.22/REV.23.)
///
/// It ships default-on anyway because it is provably zero-cost-zero-risk and
/// banks the CPU saving + a generalizable shape rule: blast radius is EXACTLY
/// Q15 (the only CSE'd `Agg→Filter(i32-range,no-nulls)→EmatixScan` shape) —
/// 21 of 22 queries fire 0× (identical code path, two back-to-back 22q A/Bs
/// confirm the inert band is pure noise), correctness is byte-identical at
/// SF=10 AND SF=100, and there is NO codegen tax (the helper is compiled in
/// regardless; this is a runtime branch, so the binary is identical to the
/// opt-in build). Any future CSE'd-filter-on-fact-scan query where decode IS
/// on the critical path (lower core count, higher SF, CPU-contended
/// concurrency) gets the win automatically.
fn cse_filter_fusion_enabled() -> bool {
    crate::flags::enabled("EMAT_CSE_FILTER_FUSION")
}

/// Structural hash of an ExecutionPlan subtree. Two subtrees with the
/// same hash represent the same logical computation. Partitioning /
/// ordering wrappers (`RepartitionExec`, `CoalesceBatchesExec`,
/// `SortExec`) are semantically transparent — they don't contribute
/// to the hash and the walk passes through to the child.
fn subtree_hash(node: &Arc<dyn ExecutionPlan>) -> u64 {
    let mut h = DefaultHasher::new();
    hash_node(node, &mut h);
    h.finish()
}

#[allow(deprecated)] // CoalesceBatchesExec
fn hash_node(node: &Arc<dyn ExecutionPlan>, h: &mut DefaultHasher) {
    // Pass-through wrappers: hash skips straight to the child.
    if node.as_any().is::<RepartitionExec>()
        || node.as_any().is::<CoalesceBatchesExec>()
        || node.as_any().is::<SortExec>()
    {
        if let Some(child) = node.children().into_iter().next() {
            hash_node(child, h);
        }
        return;
    }

    if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
        h.write_u8(1);
        // Hash mode as a discriminator number. AggregateMode is enum
        // derive-Hash, but we want explicit bytes for forward-stability.
        h.write_u8(match agg.mode() {
            AggregateMode::Partial => 0,
            AggregateMode::Final => 1,
            AggregateMode::FinalPartitioned => 2,
            AggregateMode::Single => 3,
            AggregateMode::SinglePartitioned => 4,
            AggregateMode::PartialReduce => 5,
        });
        // group_expr
        let groups = agg.group_expr().expr();
        h.write_usize(groups.len());
        for (expr, name) in groups {
            expr.hash(h);
            name.hash(h);
        }
        // aggr_expr
        let aggs = agg.aggr_expr();
        h.write_usize(aggs.len());
        for af in aggs {
            af.name().hash(h);
            af.fun().name().hash(h);
            for arg in af.expressions() {
                arg.hash(h);
            }
        }
        // filter_expr
        let filters = agg.filter_expr();
        h.write_usize(filters.len());
        for filt in filters {
            match filt {
                Some(e) => {
                    h.write_u8(1);
                    e.hash(h);
                }
                None => h.write_u8(0),
            }
        }
        hash_node(agg.input(), h);
        return;
    }

    if let Some(filt) = node.as_any().downcast_ref::<FilterExec>() {
        h.write_u8(2);
        filt.predicate().hash(h);
        if let Some(child) = node.children().into_iter().next() {
            hash_node(child, h);
        }
        return;
    }

    if let Some(proj) = node.as_any().downcast_ref::<ProjectionExec>() {
        h.write_u8(3);
        let exprs = proj.expr();
        h.write_usize(exprs.len());
        for pe in exprs {
            pe.expr.hash(h);
            pe.alias.hash(h);
        }
        if let Some(child) = node.children().into_iter().next() {
            hash_node(child, h);
        }
        return;
    }

    // Fallback for leaf-like nodes (TableScan, DataSourceExec, parquet
    // providers, etc.) and any node type we don't model explicitly:
    // hash a one-line display of the node + recurse into children. The
    // display string captures column projections, predicates pushed
    // into the scan, and the source path / table name. Σ.Q15:
    // `DynamicFilter` placeholders are stripped first — they are
    // per-consumer runtime pruning hints injected into probe-side scans
    // by DataFusion's post-optimization FilterPushdown, not part of the
    // computation's identity (pass 1.5 guarantees the copy actually
    // shared is a filter-free one).
    h.write_u8(255);
    let disp = datafusion::physical_plan::displayable(node.as_ref())
        .one_line()
        .to_string();
    strip_dynamic_filters(&disp).hash(h);
    for child in node.children() {
        hash_node(child, h);
    }
}

/// Σ.Q15 (2026-07-12) — strip `DynamicFilter [ .. ]` placeholders from a
/// node's display line before hashing. DataFusion's post-optimization
/// `FilterPushdown` injects these join-key runtime-pruning hints into
/// probe-side scans; they are owned by ONE consumer's join and say nothing
/// about the computation's identity, so a probe-side copy of a duplicated
/// subtree must hash equal to its filter-free twin. Handles a preceding
/// ` AND ` (mid/trailing conjunct) or, failing that, a following ` AND `
/// (leading conjunct), and balances nested `[..]` in the payload.
fn strip_dynamic_filters(disp: &str) -> String {
    const TOKEN: &str = "DynamicFilter";
    let mut out = String::with_capacity(disp.len());
    let mut rest = disp;
    while let Some(pos) = rest.find(TOKEN) {
        let (before, after_token) = (&rest[..pos], &rest[pos + TOKEN.len()..]);
        let (before, stripped_preceding_and) = match before.strip_suffix(" AND ") {
            Some(b) => (b, true),
            None => (before, false),
        };
        out.push_str(before);
        // Consume the bracketed payload (`[ .. ]`, balanced); a bare token
        // consumes only itself.
        let bytes = after_token.as_bytes();
        let mut i = 0;
        while i < bytes.len() && bytes[i] == b' ' {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b'[' {
            let mut depth = 0usize;
            while i < bytes.len() {
                match bytes[i] {
                    b'[' => depth += 1,
                    b']' => {
                        depth -= 1;
                        if depth == 0 {
                            i += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
        } else {
            i = 0;
        }
        let mut tail = &after_token[i..];
        if !stripped_preceding_and {
            tail = tail.strip_prefix(" AND ").unwrap_or(tail);
        }
        rest = tail;
    }
    out.push_str(rest);
    out
}

/// True if any node in the subtree displays a `DynamicFilter` placeholder —
/// the detection surface matches the hash's display-based fallback, so a
/// copy this returns `false` for is exactly a copy whose hash needed no
/// normalization.
fn subtree_has_dynamic_filter(node: &Arc<dyn ExecutionPlan>) -> bool {
    let disp = datafusion::physical_plan::displayable(node.as_ref())
        .one_line()
        .to_string();
    if disp.contains("DynamicFilter") {
        return true;
    }
    node.children().into_iter().any(subtree_has_dynamic_filter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_dynamic_filters_normalizes_display() {
        // Trailing conjunct — the shape DataFusion 53 actually produces on
        // a probe-side parquet scan (static predicate first).
        assert_eq!(
            strip_dynamic_filters("predicate=a@0 >= 5 AND DynamicFilter [ empty ]"),
            "predicate=a@0 >= 5"
        );
        // Leading conjunct.
        assert_eq!(
            strip_dynamic_filters("predicate=DynamicFilter [ empty ] AND a@0 >= 5"),
            "predicate=a@0 >= 5"
        );
        // Mid conjunct keeps the surrounding conjunction intact.
        assert_eq!(
            strip_dynamic_filters("x AND DynamicFilter [ p [ q ] r ] AND y"),
            "x AND y"
        );
        // Bare token, no payload.
        assert_eq!(strip_dynamic_filters("DynamicFilter"), "");
        // Multiple occurrences.
        assert_eq!(
            strip_dynamic_filters("s1 AND DynamicFilter [ empty ], t=[DynamicFilter [ x ] AND s2]"),
            "s1, t=[s2]"
        );
        // Identity on filter-free displays.
        assert_eq!(
            strip_dynamic_filters("FilterExec: a@0 > 1, projection=[a@0]"),
            "FilterExec: a@0 > 1, projection=[a@0]"
        );
    }

    /// Σ.Q15 (2026-07-12): DataFusion's post-optimization `FilterPushdown`
    /// injects a join-key `DynamicFilter [..]` placeholder into the
    /// probe-side PARQUET scan of the dimension join — pushed THROUGH the
    /// aggregation because the join key is the group key. Only one of the
    /// two duplicated revenue subtrees is a probe of that join, so the
    /// placeholder split the structural hashes and the rule silently
    /// disengaged (0 wraps) → two independent parallel f64 SUMs → ULP
    /// mismatch → the Q15 equality dropped every row. (Plain campaign
    /// sessions were shielded only because the fast-scan resolver replaces
    /// `DataSourceExec`, which pushdown can't touch; distributed sessions
    /// keep the stock codec-serializable scan and flaked.)
    ///
    /// Guards, in order: the plan really carries a DynamicFilter
    /// (precondition — this fixture reproduces the hazard); the rule still
    /// engages (2 wraps); both wraps hold the SAME filter-free canonical
    /// subtree (cache content must not depend on which site populates
    /// first, and must never be another consumer's pruned stream); the
    /// query returns its 1 row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dynamic_filter_on_one_site_does_not_split_dedupe() {
        use crate::shared_subtree_exec::SharedSubtreeExec;
        use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

        let dir = tempfile::tempdir().unwrap();

        // Fact table: 20 supplier groups × 200 rows. Prices rise with the
        // group id so group sums are strictly increasing — the MAX is
        // unique and the expected result is exactly 1 row.
        let li_schema = Arc::new(Schema::new(vec![
            Field::new("supp_id", DataType::Int64, false),
            Field::new("ship_day", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("disc", DataType::Float64, false),
        ]));
        let mut supp_ids = Vec::new();
        let mut ship_days = Vec::new();
        let mut prices = Vec::new();
        let mut discs = Vec::new();
        for g in 0..20_i64 {
            for r in 0..200_i64 {
                supp_ids.push(g);
                ship_days.push(r);
                // Not exactly representable → sum is order-sensitive.
                prices.push(100.0 + (g as f64) * 10.0 + (r as f64) * 0.1);
                discs.push(0.01 + (r as f64) * 0.0001);
            }
        }
        let li_batch = RecordBatch::try_new(
            li_schema.clone(),
            vec![
                Arc::new(Int64Array::from(supp_ids)),
                Arc::new(Int64Array::from(ship_days)),
                Arc::new(Float64Array::from(prices)),
                Arc::new(Float64Array::from(discs)),
            ],
        )
        .unwrap();
        let li_path = dir.path().join("li.parquet");
        let mut w = ArrowWriter::try_new(std::fs::File::create(&li_path).unwrap(), li_schema, None)
            .unwrap();
        w.write(&li_batch).unwrap();
        w.close().unwrap();

        // Dimension table: small → JoinSelection picks it as the build
        // side, and its join keys become the DynamicFilter pushed into the
        // fact-scan probe side.
        let supp_schema = Arc::new(Schema::new(vec![
            Field::new("supp_id", DataType::Int64, false),
            Field::new("sname", DataType::Utf8, false),
        ]));
        let supp_batch = RecordBatch::try_new(
            supp_schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..20_i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..20).map(|i| format!("s{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let supp_path = dir.path().join("supp.parquet");
        let mut w = ArrowWriter::try_new(
            std::fs::File::create(&supp_path).unwrap(),
            supp_schema,
            None,
        )
        .unwrap();
        w.write(&supp_batch).unwrap();
        w.close().unwrap();

        let make_ctx = |with_rule: bool| {
            let li = li_path.clone();
            let supp = supp_path.clone();
            async move {
                let mut b = SessionStateBuilder::new()
                    .with_config(SessionConfig::new().with_target_partitions(4))
                    .with_default_features();
                if with_rule {
                    b = b.with_physical_optimizer_rule(Arc::new(
                        DedupeAggregateForFloatDeterminism::default(),
                    ));
                }
                let ctx = SessionContext::new_with_state(b.build());
                ctx.register_parquet("li", li.to_str().unwrap(), ParquetReadOptions::default())
                    .await
                    .unwrap();
                ctx.register_parquet(
                    "supp",
                    supp.to_str().unwrap(),
                    ParquetReadOptions::default(),
                )
                .await
                .unwrap();
                ctx
            }
        };

        // TPC-H Q15 shape: the CTE materializes twice (DataFusion 53 has
        // no CTE materialization) — once joined with the dimension table
        // (the probe that receives the DynamicFilter) and once inside the
        // scalar MAX subquery (filter-free).
        let sql = "
            WITH r AS (
                SELECT supp_id AS supplier_no, sum(price * (1 - disc)) AS total_revenue
                FROM li
                WHERE ship_day >= 50 AND ship_day < 150
                GROUP BY supp_id
            )
            SELECT s.supp_id, r.total_revenue
            FROM supp s, r
            WHERE s.supp_id = r.supplier_no
              AND r.total_revenue = (SELECT max(total_revenue) FROM r)
            ORDER BY s.supp_id
        ";

        // Precondition, on a RULE-FREE session: the fixture really does
        // reproduce the one-sided DynamicFilter pushdown. (With the rule
        // installed the placeholder is gone from the final plan BY DESIGN
        // — the probe site's subtree is replaced by the canonical
        // filter-free copy — so the hazard must be asserted before the
        // rule runs.) If a DataFusion upgrade stops producing the
        // placeholder here, this test needs a new reproduction — do not
        // weaken the guard.
        let bare_ctx = make_ctx(false).await;
        let bare_plan = bare_ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let bare_disp = datafusion::physical_plan::displayable(bare_plan.as_ref())
            .indent(false)
            .to_string();
        assert!(
            bare_disp.contains("DynamicFilter"),
            "fixture must reproduce the DynamicFilter pushdown; plan:\n{bare_disp}"
        );

        let ctx = make_ctx(true).await;
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let disp = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string();

        fn collect_wrap_inputs(p: &Arc<dyn ExecutionPlan>, out: &mut Vec<Arc<dyn ExecutionPlan>>) {
            if let Some(s) = p.as_any().downcast_ref::<SharedSubtreeExec>() {
                out.push(s.input().clone());
            }
            for c in p.children() {
                collect_wrap_inputs(c, out);
            }
        }
        let mut wraps = Vec::new();
        collect_wrap_inputs(&plan, &mut wraps);
        assert_eq!(
            wraps.len(),
            2,
            "dedupe must engage despite the one-sided DynamicFilter; plan:\n{disp}"
        );
        // Both wraps share the SAME canonical filter-free subtree: cache
        // content is deterministic regardless of which wrap populates
        // first, and is never another consumer's pruned stream.
        assert!(
            Arc::ptr_eq(&wraps[0], &wraps[1]),
            "wraps must hold one canonical Arc"
        );
        assert!(
            !subtree_has_dynamic_filter(&wraps[0]),
            "the shared subtree must be the dynamic-filter-FREE copy"
        );

        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 1, "Q15 shape must return exactly its unique-max row");
    }

    // PV.M.7 #308: the masked-fusion default flipped ON. It is wall-time-
    // neutral on Q15 at SF=10/100 (a CPU-work saving — one fewer l_shipdate
    // Snappy decompress — that lands off the wall-clock critical path) but
    // proven zero-regression: blast radius is exactly Q15 (2 fuse sites),
    // 21 queries fire 0×, byte-identical correctness at SF=10+SF=100, no
    // codegen tax (runtime branch). Default-on banks the free CPU saving and
    // the generalizable shape rule; explicit `=0`/`false` still disables (the
    // A/B off-arm + any future regression escape hatch).
    #[test]
    fn cse_filter_fusion_defaults_on_and_respects_explicit_off() {
        // SAFETY: mutation window serialized by the crate-wide env lock;
        // saved + restored around the assertions.
        let _env = crate::flags::EMAT_ENV_TEST_LOCK.blocking_lock();
        let saved = std::env::var_os("EMAT_CSE_FILTER_FUSION");
        unsafe {
            std::env::remove_var("EMAT_CSE_FILTER_FUSION");
            assert!(cse_filter_fusion_enabled(), "absent var → default ON");

            std::env::set_var("EMAT_CSE_FILTER_FUSION", "0");
            assert!(!cse_filter_fusion_enabled(), "\"0\" → OFF");

            std::env::set_var("EMAT_CSE_FILTER_FUSION", "false");
            assert!(!cse_filter_fusion_enabled(), "\"false\" → OFF");

            std::env::set_var("EMAT_CSE_FILTER_FUSION", "1");
            assert!(cse_filter_fusion_enabled(), "\"1\" → ON");

            match saved {
                Some(v) => std::env::set_var("EMAT_CSE_FILTER_FUSION", v),
                None => std::env::remove_var("EMAT_CSE_FILTER_FUSION"),
            }
        }
    }

    #[test]
    fn rule_name_smoke() {
        let rule = DedupeAggregateForFloatDeterminism::default();
        assert_eq!(
            PhysicalOptimizerRule::name(&rule),
            "ematix_flow_dedupe_aggregate_for_float_determinism"
        );
        assert!(rule.schema_check());
    }

    #[test]
    fn empty_plan_passthrough() {
        // Rule on an EmptyExec (no aggregates) is a strict no-op:
        // counts is empty, dupes is empty, returns plan unchanged.
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::physical_plan::empty::EmptyExec;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let rule = DedupeAggregateForFloatDeterminism::default();
        let opt = rule
            .optimize(plan.clone(), &ConfigOptions::default())
            .expect("optimize");
        assert!(Arc::ptr_eq(&plan, &opt));
    }

    /// Q15-shape integration test. Two structurally-identical f64 SUM
    /// aggregates over the same logical input must both wrap in
    /// `SharedSubtreeExec`, sharing one cached computation. End-to-end
    /// execution must be deterministic across 10 runs (one query each
    /// — separate SessionContexts → separate caches; the determinism
    /// comes from the WITHIN-query cache, not the cross-query cache).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn q15_shape_becomes_deterministic() {
        use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::{SessionConfig, SessionContext};

        // Build a fixture table with the Q15 SUM shape: an integer
        // grouping key + a float column to sum. 14 supplier groups ×
        // 100 rows each — enough to hit the parallel-SUM ULP issue
        // when partitioning is non-trivial.
        let schema = Arc::new(Schema::new(vec![
            Field::new("supplier", DataType::Int64, false),
            Field::new("revenue", DataType::Float64, false),
        ]));
        let mut suppliers: Vec<i64> = Vec::new();
        let mut revenues: Vec<f64> = Vec::new();
        for s in 0..14_i64 {
            for r in 0..100_i64 {
                suppliers.push(s);
                // Numbers chosen to exercise f64 precision (not
                // exactly representable, sensitive to sum order).
                revenues.push((r as f64 + 1.0) * 0.1 + (s as f64) * 17.3);
            }
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(suppliers)),
                Arc::new(Float64Array::from(revenues)),
            ],
        )
        .unwrap();
        let mt = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

        let make_ctx = || async {
            let state = SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_default_features()
                .with_physical_optimizer_rule(Arc::new(
                    DedupeAggregateForFloatDeterminism::default(),
                ))
                .build();
            let ctx = SessionContext::new_with_state(state);
            ctx.register_table("revenue_t", mt.clone()).unwrap();
            ctx
        };

        // Q15-shape SQL: outer SUM joined with scalar MAX of the same
        // sub-aggregate. Without the rule, two parallel f64 SUMs over
        // the same logical input produce ULP-differing results and
        // the equality drops rows.
        let sql = "
            WITH r AS (
                SELECT supplier, sum(revenue) AS total
                FROM revenue_t
                GROUP BY supplier
            )
            SELECT r.supplier, r.total
            FROM r
            WHERE r.total = (SELECT max(total) FROM r)
            ORDER BY r.supplier
        ";

        // Run 10 times and verify each returns exactly 1 row (the
        // max-revenue supplier).
        let mut rows_per_run: Vec<usize> = Vec::with_capacity(10);
        for _ in 0..10 {
            let ctx = make_ctx().await;
            let df = ctx.sql(sql).await.unwrap();
            let batches = df.collect().await.unwrap();
            let n: usize = batches.iter().map(|b| b.num_rows()).sum();
            rows_per_run.push(n);
        }
        assert!(
            rows_per_run.iter().all(|&n| n == 1),
            "Q15-shape must return exactly 1 row deterministically; \
             got per-run row counts: {rows_per_run:?}"
        );
    }

    /// BF (2026-06-09): the SharedSubtreeRegistry MUST be scoped PER-QUERY, not
    /// shared across queries on a long-lived ctx. A ctx-lifetime registry (a)
    /// accumulates `CachedBatches` across queries → degrades a reused ctx
    /// 1.3-1.5× (measured SF=10: Q13 116→170, Q22 25→45), and (b) cross-query
    /// MEMOIZES → re-running a query returns STALE cached batches (Q15 SF=10
    /// reused-ctx 7.9ms stale vs 78ms honest = a correctness bug). The fix:
    /// `optimize()` uses a fresh registry per call. Guard: two physical plans
    /// of the SAME query on ONE ctx (same rule instance) must NOT share a
    /// `CachedBatches` Arc — else query N memoizes query N-1's result.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_subtree_registry_is_scoped_per_query() {
        use crate::shared_subtree_exec::{CachedBatches, SharedSubtreeExec};
        use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let schema = Arc::new(Schema::new(vec![
            Field::new("supplier", DataType::Int64, false),
            Field::new("revenue", DataType::Float64, false),
        ]));
        let mut suppliers: Vec<i64> = Vec::new();
        let mut revenues: Vec<f64> = Vec::new();
        for s in 0..14_i64 {
            for r in 0..100_i64 {
                suppliers.push(s);
                revenues.push((r as f64 + 1.0) * 0.1 + (s as f64) * 17.3);
            }
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(suppliers)),
                Arc::new(Float64Array::from(revenues)),
            ],
        )
        .unwrap();
        let mt = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

        // ONE ctx, ONE rule instance → its registry persists across queries.
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("revenue_t", mt).unwrap();

        let sql = "
            WITH r AS (SELECT supplier, sum(revenue) AS total FROM revenue_t GROUP BY supplier)
            SELECT r.supplier, r.total FROM r WHERE r.total = (SELECT max(total) FROM r)
            ORDER BY r.supplier
        ";

        fn find_cached(p: &Arc<dyn ExecutionPlan>) -> Option<Arc<CachedBatches>> {
            if let Some(s) = p.as_any().downcast_ref::<SharedSubtreeExec>() {
                return Some(s.cached().clone());
            }
            for c in p.children() {
                if let Some(f) = find_cached(c) {
                    return Some(f);
                }
            }
            None
        }

        let plan1 = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan2 = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let c1 = find_cached(&plan1).expect("query 1 must wrap a SharedSubtreeExec");
        let c2 = find_cached(&plan2).expect("query 2 must wrap a SharedSubtreeExec");
        assert!(
            !Arc::ptr_eq(&c1, &c2),
            "two plans of the same query on one ctx share a CachedBatches Arc — \
             registry is not per-query scoped (cross-query stale memoization + accumulation)"
        );
    }

    /// Σ.BS — Q15 SF=100 partition-mismatch regression guard.
    ///
    /// At SF=1/10 the `supplier ⋈ revenue0` join is small enough that
    /// `JoinSelection` picks `PartitionMode::CollectLeft` (which only
    /// requires the build side to be 1 partition — satisfied by
    /// `SharedSubtreeExec`'s `UnknownPartitioning(1)`). At SF=100 the
    /// build side exceeds the single-partition threshold, so
    /// `JoinSelection` picks `PartitionMode::Partitioned`, and
    /// `EnforceDistribution` repartitions both join inputs to N
    /// hash-partitions. The agg's `Hash[l_suppkey]` output already
    /// satisfies the join's `Hash[supplier_no]` requirement, so NO
    /// `RepartitionExec` is inserted above it. This rule then wraps that
    /// agg in `SharedSubtreeExec`, collapsing its side from N → 1
    /// partitions with nothing above to restore the count. The
    /// `HashJoinExec(Partitioned)` then fails its execute()-time
    /// assertion: "Invalid HashJoinExec, partition count mismatch N!=1".
    ///
    /// We reproduce the SF=100 plan shape cheaply at SF=1 data size by
    /// zeroing the single-partition threshold (forcing `Partitioned`).
    /// Before the fix this errors; after it returns the single
    /// max-revenue supplier.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn q15_partitioned_join_survives_shared_subtree_collapse() {
        use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::physical_plan::{collect, displayable};
        use datafusion::prelude::{SessionConfig, SessionContext};

        // supplier(s_suppkey, s_name) — 64 suppliers.
        let n_suppliers = 64_i64;
        let supplier_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
        ]));
        let supplier_batch = RecordBatch::try_new(
            supplier_schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..n_suppliers).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..n_suppliers)
                        .map(|i| format!("Supplier#{i:03}"))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let supplier_mt =
            Arc::new(MemTable::try_new(supplier_schema, vec![vec![supplier_batch]]).unwrap());

        // lineitem(l_suppkey, l_extendedprice, l_discount) — 32 rows/supplier.
        let lineitem_schema = Arc::new(Schema::new(vec![
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        let (mut l_supp, mut l_price, mut l_disc) = (Vec::new(), Vec::new(), Vec::new());
        for s in 0..n_suppliers {
            for r in 0..32_i64 {
                l_supp.push(s);
                l_price.push((r as f64 + 1.0) * 1000.0 + (s as f64) * 7.0);
                l_disc.push(((r % 10) as f64) * 0.01);
            }
        }
        let lineitem_batch = RecordBatch::try_new(
            lineitem_schema.clone(),
            vec![
                Arc::new(Int64Array::from(l_supp)),
                Arc::new(Float64Array::from(l_price)),
                Arc::new(Float64Array::from(l_disc)),
            ],
        )
        .unwrap();
        let lineitem_mt =
            Arc::new(MemTable::try_new(lineitem_schema, vec![vec![lineitem_batch]]).unwrap());

        // Force PartitionMode::Partitioned even on tiny inputs — this is
        // what SF=100 does naturally (build side exceeds the single-
        // partition threshold).
        let mut config = SessionConfig::new().with_target_partitions(4);
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("supplier", supplier_mt).unwrap();
        ctx.register_table("lineitem", lineitem_mt).unwrap();

        // Real TPC-H Q15 shape: revenue0 CTE referenced twice (joined to
        // supplier + inside the scalar MAX subquery) → duplicated f64 agg.
        let sql = "
            WITH revenue0 AS (
                SELECT l_suppkey AS supplier_no,
                       sum(l_extendedprice * (1 - l_discount)) AS total_revenue
                FROM lineitem
                GROUP BY l_suppkey
            )
            SELECT s.s_suppkey, s.s_name, r.total_revenue
            FROM supplier s, revenue0 r
            WHERE s.s_suppkey = r.supplier_no
              AND r.total_revenue = (SELECT max(total_revenue) FROM revenue0)
            ORDER BY s.s_suppkey
        ";

        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        // Guard against vacuous pass: we must actually be exercising a
        // Partitioned hash join AND the dedupe wrap, else the bug can't
        // manifest.
        assert!(
            plan_str.contains("mode=Partitioned"),
            "test must exercise PartitionMode::Partitioned; plan:\n{plan_str}"
        );
        assert!(
            plan_str.contains("SharedSubtreeExec"),
            "dedupe rule must have wrapped the duplicated revenue0 agg; plan:\n{plan_str}"
        );

        let batches = collect(plan, ctx.task_ctx())
            .await
            .expect("Q15 Partitioned plan must execute without a partition-count mismatch");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 1,
            "Q15 returns exactly one (max-revenue) supplier; got {rows} rows"
        );
    }

    /// BF (2026-06-09): caching is now PER-QUERY. An externally-provided
    /// `SharedSubtreeRegistry` installed via `with_registry` is NO LONGER used
    /// for cross-query caching — that degraded long-lived contexts (accumulation)
    /// and served stale memoized results. Two consecutive Q15-shape queries on
    /// the SAME ctx must: (1) leave the external registry EMPTY, and (2) each
    /// return the correct single row (within-query CSE still works via the
    /// fresh per-query registry created inside `optimize()`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_registry_not_used_for_cross_query_cache() {
        use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let schema = Arc::new(Schema::new(vec![
            Field::new("supplier", DataType::Int64, false),
            Field::new("revenue", DataType::Float64, false),
        ]));
        let mut suppliers: Vec<i64> = Vec::new();
        let mut revenues: Vec<f64> = Vec::new();
        for s in 0..14_i64 {
            for r in 0..100_i64 {
                suppliers.push(s);
                revenues.push((r as f64 + 1.0) * 0.1 + (s as f64) * 17.3);
            }
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(suppliers)),
                Arc::new(Float64Array::from(revenues)),
            ],
        )
        .unwrap();
        let mt = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

        // Hold the registry locally so we can inspect cache state
        // between queries. Same instance is installed on the
        // SessionState, so cross-query sharing is exercised.
        let registry = Arc::new(SharedSubtreeRegistry::new());
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(
                DedupeAggregateForFloatDeterminism::with_registry(registry.clone()),
            ))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("revenue_t", mt).unwrap();

        let sql = "
            WITH r AS (
                SELECT supplier, sum(revenue) AS total
                FROM revenue_t
                GROUP BY supplier
            )
            SELECT r.supplier, r.total
            FROM r
            WHERE r.total = (SELECT max(total) FROM r)
            ORDER BY r.supplier
        ";

        assert_eq!(registry.len(), 0, "external registry starts empty");

        // First query — within-query CSE works, but the EXTERNAL registry is
        // never touched (optimize() uses a fresh per-query registry).
        let df = ctx.sql(sql).await.unwrap();
        let batches1 = df.collect().await.unwrap();
        let rows1: usize = batches1.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows1, 1, "Q15-shape returns exactly one row");
        assert_eq!(
            registry.len(),
            0,
            "external registry must stay empty — caching is per-query, not cross-query",
        );

        // Second query — same shape, same ctx. No cross-query state, no stale
        // memoization, no accumulation.
        let df = ctx.sql(sql).await.unwrap();
        let batches2 = df.collect().await.unwrap();
        let rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows2, 1, "second run is independently correct");
        assert_eq!(
            registry.len(),
            0,
            "external registry must remain empty after a second query (no accumulation)",
        );
    }
}
