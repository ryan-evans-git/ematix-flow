//! Σ.T — selectivity-first inner-join reorder.
//!
//! ## Why this lives here
//!
//! Phase 0 discovery on Q05/Q07/Q08/Q17/Q18 SF=10 (2026-05-25) showed
//! that DataFusion 53.1's planner has no cost-based join reorder. Q05
//! materialises a 60M-row intermediate before applying the region
//! filter; DuckDB applies the region filter first and pulls the join
//! tree's cardinality down 10×. See `docs/PHASE_SIGMA_T_JOIN_REORDER.md`.
//!
//! ## Why not an optimizer rule
//!
//! Per [[optimizer-codegen-sensitivity]]: three prior rules
//! (Σ.H.1d.4, Σ.K.A, Σ.F-T2) regressed 5–8 pp geomean from LLVM
//! codegen perturbation even when the rule body did almost nothing.
//! [[dict-routing]] proved the pre-plan-walker pattern works at this
//! exact layer: the caller invokes [`reorder_inner_joins`] explicitly
//! on the logical plan before optimization runs, so no
//! `OptimizerRule` is added to DataFusion's pass stack.
//!
//! ## Heuristic — Phase 2 MVP
//!
//! Selectivity-first left-deep order. For each chain of `Inner Join`
//! nodes, the walker:
//!
//! 1. Flattens the chain into a list of leaves + the equi-join
//!    predicates that connect them.
//! 2. Estimates each leaf's post-filter cardinality from the
//!    `TableProvider::statistics()` surface Σ.T Phase 1 just plumbed
//!    (num_rows × predicate selectivity from min/max).
//! 3. Sorts leaves by ascending estimated cardinality.
//! 4. Rebuilds a left-deep chain in the new order, attaching each
//!    equi-join predicate to the first join that has both keys in
//!    scope. Predicates that don't yet have both sides resolved are
//!    deferred (in practice they always resolve by the next join in
//!    a connected graph; disconnected pieces stay in original order
//!    to avoid Cartesian products).
//!
//! This deliberately stops short of a full Selinger DP enumeration —
//! the MVP wants to validate that the reorder hook + cardinality
//! estimator land correctly on the two clear-win shapes (Q05, Q08)
//! before investing in the 2^N DP. Phase 2.b can swap in DP behind
//! the same public entry point if MVP results justify it.
//!
//! ## Phase 2 NOT covered yet (future work)
//!
//! - Outer joins: this pass walks past Left/Right/Full Outer joins
//!   unchanged. The reorder fires only inside contiguous Inner Join
//!   subtrees.
//! - Selinger DP enumeration: MVP uses a greedy selectivity-first
//!   sort. For ≤4-table chains the orderings happen to be the same;
//!   for 5+ table chains DP may differ.
//! - NDV-aware output cardinality: the cost model uses leaf-side
//!   post-filter card. FK-shape output cardinality (`|A|×|B|/NDV`)
//!   needs Phase 2.b once we plumb NDV through.

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef, Result as DfResult, ScalarValue, Statistics};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{
    BinaryExpr, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, TableScan,
};

/// Reach through DataFusion's `TableSource` wrapper to the underlying
/// `TableProvider::statistics()`. Most TPC-H tables are registered via
/// `ctx.register_table(name, Arc<dyn TableProvider>)` which means
/// `TableScan::source` is a `DefaultTableSource` we can downcast.
/// Returns `None` when the source is something exotic (federation,
/// custom catalog) — the caller falls back to `num_rows = MAX`.
fn table_provider_stats(ts: &TableScan) -> Option<Statistics> {
    let any = ts.source.as_any();
    let dts = any.downcast_ref::<DefaultTableSource>()?;
    dts.table_provider.statistics()
}

/// A flattened inner-join subtree. `leaves[i]` is the i-th input
/// subtree (typically a TableScan or Filter wrapping a TableScan,
/// occasionally a Projection). `equi_preds` are the per-edge
/// equality predicates `(left_col, right_col)` discovered while
/// flattening; we keep them as Column refs so we can re-attach them
/// to the new left-deep chain by name lookup against the in-scope
/// schema at each step.
#[derive(Debug)]
struct InnerJoinChain {
    leaves: Vec<LogicalPlan>,
    equi_preds: Vec<(Column, Column)>,
    /// Filter expressions that sat directly on the original Join's
    /// `filter` slot (non-equi conditions). Preserved as a single
    /// AND-conjunction applied to the rebuilt chain root.
    extra_filter: Option<Expr>,
}

/// Σ.AH.X Lever G — shape predicates for narrowing `reorder_inner_joins`
/// to chains where the cardinality estimator is empirically reliable.
///
/// **Why this exists**: Σ.AH.3 spike (`[[sigma-ah-3-arc-closed]]`) measured
/// SF=10 22q A/B with the existing `reorder_inner_joins` and found:
/// - Q10 (cust ⋈ orders ⋈ lineitem ⋈ nation, 4 leaves, no LIKE) → **-25 ms** stable win
/// - Q07-inner (nation⋈supp⋈cust⋈orders⋈lineitem, 5 leaves) → +46 ms regression
/// - Q09 (6 leaves, includes `p_name LIKE '%green%'` filter) → +91 ms regression
///
/// The cardinality estimator under-counts in two situations:
/// 1. **String-LIKE predicate selectivity defaults to 0.2** (DataFusion's
///    fallback) — completely wrong for typical `%substring%` filters
///    which are usually < 0.01 selective.
/// 2. **Long chains** (≥ 5 leaves) — Selinger DP enumerates 2^N orderings,
///    and the cost model's per-step errors compound exponentially.
///
/// Lever G's predicate REJECTS chains exhibiting either shape, falling
/// back to the original plan order. Only emits the reorder when both
/// gates pass.
#[derive(Debug, Clone, Copy)]
pub struct ReorderOpts {
    /// Maximum leaves in the chain. Chains with more leaves are too
    /// noisy for the current cost model. Default 4 (catches Q10's
    /// 4-leaf shape, rejects Q07-inner 5+ and Q09 6+).
    pub max_leaves: usize,
    /// Reject chains where any leaf has a string-LIKE filter
    /// (DataFusion's selectivity for LIKE defaults to 0.2 which is
    /// wildly wrong). Default true.
    pub reject_string_like: bool,
    /// Reject chains where any join's `on` clause references an
    /// aggregate-result column (e.g. `min(partsupp.ps_supplycost)`).
    /// These appear when DataFusion decorrelates a correlated
    /// subquery into a Join + Aggregate — the cardinality of the
    /// aggregated side is hard to estimate (it depends on group-by
    /// distinct count which the estimator doesn't always have).
    /// Q02 22q SF=10 measurement (Σ.AH.X Lever G bench): reordering
    /// Q02's decorrelated `min(ps_supplycost)` subquery chain
    /// regressed Q02 +23%. Default true.
    pub reject_aggregate_join_keys: bool,
    /// When a chain is rejected (by any gate), also skip descent into
    /// its sub-tree. This prevents partial reorders of inner
    /// sub-chains when the outer is rejected for being too big.
    ///
    /// Example: Q07 has a 6-leaf outer chain (rejected for ambiguous
    /// nation×nation), but `transform_down` would otherwise descend
    /// and reorder a 4-leaf inner sub-chain. With `jump_on_reject=true`,
    /// the entire tree under a rejected chain is left untouched.
    ///
    /// Defaults: true in `ReorderOpts::default()` (safer); false in
    /// `unsafe_no_shape_gate()` (preserves the Σ.T DP behavior that
    /// reorders any reachable sub-chain).
    pub jump_on_reject: bool,
    /// Σ.AL (2026-05-27): when set, skip descent into LeftSemi /
    /// LeftAnti joins. Inner-join chains inside a (NOT) EXISTS-
    /// decorrelated subquery rely on the build/probe ordering for
    /// L9 bloom cascade chains; reordering them moves which Inner
    /// Join builds the bloom and can emit it too late to filter the
    /// downstream fact scan.
    ///
    /// Empirically observed on Q21 SF=10: Lever G's reorder produced
    /// a +47 ms regression by disrupting the L9 cascade between
    /// supplier⋈lineitem and orders⋈lineitem. With this gate, the
    /// reorder is suppressed inside (NOT) EXISTS subqueries and the
    /// regression vanishes.
    ///
    /// Default true.
    pub reject_under_left_semi_anti: bool,
    /// Σ.BR Phase 2 (2026-05-29): scale gate. When the largest leaf in a
    /// chain has at least `min_rows` rows, raise the effective `max_leaves`
    /// to `bumped`. `Some((min_rows, bumped))` or `None` to disable.
    ///
    /// Rationale: the 6-leaf dimensional funnels (Q05, Q09) only pay off at
    /// scale — measured SF=100 Q05 −10.1% but ~neutral at SF=10 — and the
    /// SF=10 "regressors" (Q07/Q02/Q11/Q21) are caught by the *other*
    /// guards (ambiguous-names / aggregate-key / jump-on-reject / semi-anti),
    /// NOT by the leaf cap. So raising the cap only in the large-fact regime
    /// admits the funnels without re-admitting the regressors, and leaves
    /// SF=1/SF=10 behaviour (largest leaf < `min_rows`) untouched. The
    /// scale-invariant cost ratio cannot encode this — see
    /// PHASE_SIGMA_BR_JOIN_REORDER.md §5. Default `Some((100_000_000, 6))`:
    /// fires only for ≥100M-row facts (SF=100 lineitem 600M bumps; SF=10
    /// 60M does not).
    pub scale_bump: Option<(u64, usize)>,
}

impl Default for ReorderOpts {
    fn default() -> Self {
        Self {
            max_leaves: 4,
            reject_string_like: true,
            reject_aggregate_join_keys: true,
            jump_on_reject: true,
            reject_under_left_semi_anti: true,
            scale_bump: Some((100_000_000, 6)),
        }
    }
}

impl ReorderOpts {
    /// Permissive options — accept any chain with ≥ 3 leaves and no
    /// ambiguous names. Equivalent to pre-Lever-G behavior. Used by
    /// the original `reorder_inner_joins` entry point for back-compat.
    pub fn unsafe_no_shape_gate() -> Self {
        Self {
            max_leaves: usize::MAX,
            reject_string_like: false,
            reject_aggregate_join_keys: false,
            jump_on_reject: false,
            reject_under_left_semi_anti: false,
            scale_bump: None,
        }
    }
}

/// Public entry. Walks `plan` bottom-up, looking for runs of
/// `Inner Join`. Each such run is flattened, reordered
/// selectivity-first, and re-emitted as a left-deep chain. Anything
/// that isn't an inner-join chain (outer joins, single tables,
/// aggregates) passes through unchanged.
///
/// Returns the rewritten plan on success. On any error (predicate
/// shape we can't model, cross-product chain, schema mismatch when
/// rebuilding) the original sub-plan is preserved — this is a
/// best-effort optimizer, not a correctness-critical one.
///
/// **Permissive entry point** — runs the Selinger DP on any chain
/// with ≥ 3 leaves and no ambiguous column names. The Σ.AH.3 spike
/// showed this regresses Q07/Q08/Q09 at SF=10 because the cardinality
/// estimator's per-step errors compound on long chains and on
/// string-LIKE filters. Use `reorder_inner_joins_shape_gated` for
/// the safer Lever G behavior that bands the reorder to shapes the
/// estimator can model accurately.
pub fn reorder_inner_joins(plan: LogicalPlan) -> DfResult<LogicalPlan> {
    reorder_inner_joins_with_opts(plan, ReorderOpts::unsafe_no_shape_gate())
}

/// Σ.AH.X Lever G entry point — same mechanism as `reorder_inner_joins`
/// but applies a tightened shape predicate (`ReorderOpts::default()` =
/// max 4 leaves + reject string-LIKE filters). Empirically isolates
/// Q10's clean -25 ms reorder win at SF=10 without taking the Q07/Q09
/// regressions that the permissive entry point produces.
///
/// Designed for production use (post-Lever-G). The permissive entry
/// point remains for tests and ad-hoc benching where exercising
/// long-chain DP is the point.
pub fn reorder_inner_joins_shape_gated(plan: LogicalPlan) -> DfResult<LogicalPlan> {
    reorder_inner_joins_with_opts(plan, ReorderOpts::default())
}

/// Σ.AH.X Lever G entry point: same as `reorder_inner_joins` but with
/// caller-controlled shape gating via `opts`.
pub fn reorder_inner_joins_with_opts(plan: LogicalPlan, opts: ReorderOpts) -> DfResult<LogicalPlan> {
    let transformed = plan.transform_down(|node| match node {
        // Σ.AL (2026-05-27): when the gate is set, skip descent into
        // LeftSemi / LeftAnti subtrees. The L9 bloom cascade inside a
        // (NOT) EXISTS-decorrelated subquery depends on the original
        // Inner Join build/probe ordering; reordering it forces the
        // bloom to be emitted too late to filter downstream fact scans
        // (Q21 SF=10 +47 ms regression). See PHASE_SIGMA_AL_DESIGN.md.
        LogicalPlan::Join(ref j)
            if opts.reject_under_left_semi_anti
                && matches!(j.join_type, JoinType::LeftSemi | JoinType::LeftAnti) =>
        {
            Ok(Transformed::new(node, false, TreeNodeRecursion::Jump))
        }
        LogicalPlan::Join(ref join) if join.join_type == JoinType::Inner => {
            // Flatten the chain rooted at this Inner Join. Only Inner-join
            // chains reach the gate; on rejection `jump_on_reject` decides
            // whether to also skip descent into sub-chains.
            match flatten_inner_join_chain(&node) {
                Some(chain) => {
                    let n = chain.leaves.len();
                    // Σ.BR Phase 2 (2026-05-29): scale gate. Raise the leaf
                    // cap to `bumped` when the chain touches a large fact
                    // table — the SF=100 regime where the dimensional funnel
                    // pays off (Q05 −10%, Q09 win at 6 leaves). Below the
                    // threshold the conservative cap holds, so SF=1/SF=10
                    // behaviour is unchanged. The regressors (Q07/Q02/Q11/
                    // Q21) are rejected by the *other* guards, not the cap,
                    // so raising it only admits the funnels. See
                    // PHASE_SIGMA_BR_JOIN_REORDER.md §5 "guard attribution".
                    let largest_leaf_rows =
                        chain.leaves.iter().map(estimate_leaf_card).max().unwrap_or(0);
                    let effective_max_leaves = match opts.scale_bump {
                        Some((min_rows, bumped)) if largest_leaf_rows >= min_rows => {
                            bumped.max(opts.max_leaves)
                        }
                        _ => opts.max_leaves,
                    };
                    let too_few = n < 3;
                    let too_many = n > effective_max_leaves;
                    let ambiguous = chain_has_ambiguous_names(&chain);
                    let like =
                        opts.reject_string_like && chain_has_string_like_filter(&chain);
                    let aggkey = opts.reject_aggregate_join_keys
                        && chain_has_aggregate_join_key(&chain);
                    // Σ.BR Phase 2: reject if any leaf is a composite (non-
                    // TableScan) subtree the cost model can't estimate — its
                    // sentinel cardinality poisons the DP (Q10 regression).
                    let composite = !chain.leaves.iter().all(leaf_is_estimable);
                    let pass =
                        !too_few && !too_many && !ambiguous && !like && !aggkey && !composite;
                    if std::env::var("EMAT_REORDER_DEBUG").is_ok() {
                        eprintln!(
                            "[reorder-gate] leaves={n} largest_leaf={largest_leaf_rows} eff_max_leaves={effective_max_leaves} too_few={too_few} too_many={too_many} ambiguous={ambiguous} like={like} aggkey={aggkey} composite={composite} -> {}",
                            if pass { "PASS" } else { "REJECT" }
                        );
                    }
                    if pass {
                        match rebuild_reordered(&chain) {
                            Some(rebuilt) => Ok(Transformed::new(
                                rebuilt,
                                true,
                                // Don't descend into the rebuilt subtree —
                                // we've already processed the whole chain
                                // atomically. Without Jump, transform_down
                                // would recurse and re-flatten our own
                                // output, potentially picking up dependent
                                // inner-join subtrees we already enumerated.
                                TreeNodeRecursion::Jump,
                            )),
                            None => Ok(Transformed::no(node)),
                        }
                    } else if opts.jump_on_reject {
                        // Σ.AH.X Lever G: when an Inner-join chain is rejected
                        // (too many leaves, LIKE filter, ambiguous names, etc.)
                        // and `jump_on_reject` is set, also skip descent. This
                        // prevents partial reorders of inner sub-chains inside
                        // a rejected outer chain (the Q07 +45 ms regression
                        // pattern).
                        Ok(Transformed::new(node, false, TreeNodeRecursion::Jump))
                    } else {
                        Ok(Transformed::no(node))
                    }
                }
                None => Ok(Transformed::no(node)),
            }
        }
        _ => Ok(Transformed::no(node)),
    })?;
    Ok(transformed.data)
}

/// Σ.AH.X Lever G — detect whether any leaf in the chain has a
/// `LIKE` predicate. Walks each leaf looking for a `LogicalPlan::Filter`
/// whose predicate contains an `Expr::Like` at any nesting level.
///
/// The DataFusion logical optimizer defaults LIKE selectivity to 0.2
/// (the generic "unknown predicate" fallback) which is typically off
/// by 10-100× on real `%substring%` filters. Reordering chains that
/// include LIKE-filtered leaves leads to bad cost-model decisions.
fn chain_has_string_like_filter(chain: &InnerJoinChain) -> bool {
    chain.leaves.iter().any(|leaf| leaf_has_like_filter(leaf))
}

fn leaf_has_like_filter(plan: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::Filter(f) = node {
            if expr_contains_like(&f.predicate) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn expr_contains_like(expr: &Expr) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = expr.apply(|e| {
        if matches!(e, Expr::Like(_) | Expr::SimilarTo(_)) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Σ.AH.X Lever G — detect whether any join in the chain has an
/// `on` clause where either side is an aggregate-result column.
///
/// When DataFusion decorrelates `WHERE x = (SELECT MIN(y) ...)` into
/// a Join + Aggregate, the join's equi-predicate references the
/// aggregate output column (e.g. `min(partsupp.ps_supplycost)`).
/// The cardinality of the aggregated side depends on the group-by
/// distinct count, which the estimator doesn't reliably know. Q02
/// 22q SF=10: reordering this shape regressed Q02 +23%.
///
/// Heuristic: a Column's `name` starts with one of the standard
/// SQL aggregate function names followed by `(`. This catches the
/// post-decorrelation `min(...)`, `max(...)`, `sum(...)`, `avg(...)`,
/// `count(...)` patterns.
fn chain_has_aggregate_join_key(chain: &InnerJoinChain) -> bool {
    chain
        .equi_preds
        .iter()
        .any(|(l, r)| column_name_is_aggregate(l) || column_name_is_aggregate(r))
}

fn column_name_is_aggregate(col: &Column) -> bool {
    let n = &col.name;
    n.starts_with("min(")
        || n.starts_with("max(")
        || n.starts_with("sum(")
        || n.starts_with("avg(")
        || n.starts_with("count(")
        || n.starts_with("MIN(")
        || n.starts_with("MAX(")
        || n.starts_with("SUM(")
        || n.starts_with("AVG(")
        || n.starts_with("COUNT(")
}

/// Walk down through `Inner Join` nodes, accumulating leaves +
/// equi-join predicates. Returns `None` if the subtree isn't a pure
/// inner-join chain (e.g. has an outer join inside it).
fn flatten_inner_join_chain(plan: &LogicalPlan) -> Option<InnerJoinChain> {
    let mut leaves: Vec<LogicalPlan> = Vec::new();
    let mut equi: Vec<(Column, Column)> = Vec::new();
    let mut extra_filters: Vec<Expr> = Vec::new();
    flatten_recurse(plan, &mut leaves, &mut equi, &mut extra_filters)?;
    if leaves.len() < 2 {
        return None;
    }
    let extra_filter = and_all(extra_filters);
    Some(InnerJoinChain {
        leaves,
        equi_preds: equi,
        extra_filter,
    })
}

fn flatten_recurse(
    plan: &LogicalPlan,
    leaves: &mut Vec<LogicalPlan>,
    equi: &mut Vec<(Column, Column)>,
    extra_filters: &mut Vec<Expr>,
) -> Option<()> {
    match plan {
        LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
            // Collect equi predicates from the `on` slot.
            for (l, r) in &j.on {
                match (l, r) {
                    (Expr::Column(lc), Expr::Column(rc)) => {
                        equi.push((lc.clone(), rc.clone()));
                    }
                    _ => return None,
                }
            }
            // DataFusion 53.1's SQL parser ALSO puts `JOIN ... ON
            // col=col` conditions into `filter` (not just `on`). Walk
            // the filter conjunction and harvest any `col=col`
            // sub-clauses as equi predicates. Anything left over is
            // a real non-equi join filter and stays in extra_filters.
            if let Some(f) = &j.filter {
                let mut leftover = Vec::new();
                split_and_harvest_equi(f.clone(), equi, &mut leftover);
                for r in leftover {
                    extra_filters.push(r);
                }
            }
            flatten_recurse(&j.left, leaves, equi, extra_filters)?;
            flatten_recurse(&j.right, leaves, equi, extra_filters)?;
            Some(())
        }
        // DataFusion's optimizer inserts `Projection` (column pruning)
        // nodes between Inner Joins after optimization. They don't
        // change row identity, so descend through them
        // transparently. The rebuilt chain won't preserve the
        // intermediate projections — re-running
        // `ctx.state().optimize(rewritten)` re-applies column pruning
        // on the new tree (see the caller contract on
        // [`reorder_inner_joins`]).
        //
        // SubqueryAlias is NOT transparent: it rewrites column
        // refs (`customer.c_custkey` → `c.c_custkey`). Treating it
        // as a leaf preserves aliased names for downstream consumers.
        LogicalPlan::Projection(p) => {
            flatten_recurse(&p.input, leaves, equi, extra_filters)
        }
        _ => {
            leaves.push(plan.clone());
            Some(())
        }
    }
}

fn and_all(exprs: Vec<Expr>) -> Option<Expr> {
    exprs.into_iter().reduce(|acc, e| Expr::BinaryExpr(BinaryExpr {
        left: Box::new(acc),
        op: Operator::And,
        right: Box::new(e),
    }))
}

/// Split `expr` on top-level `AND` and route each conjunct: a
/// `col = col` clause goes into `equi`, anything else into
/// `leftover`. Recursive on `AND` to flatten nested conjunctions.
fn split_and_harvest_equi(
    expr: Expr,
    equi: &mut Vec<(Column, Column)>,
    leftover: &mut Vec<Expr>,
) {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            split_and_harvest_equi(*left, equi, leftover);
            split_and_harvest_equi(*right, equi, leftover);
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => match (*left, *right) {
            (Expr::Column(lc), Expr::Column(rc)) => equi.push((lc, rc)),
            (l, r) => leftover.push(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(l),
                op: Operator::Eq,
                right: Box::new(r),
            })),
        },
        other => leftover.push(other),
    }
}

/// Estimate post-filter cardinality of a leaf subtree.
///
/// MVP heuristic:
/// - TableScan: `source.statistics().num_rows` (Exact precision wins;
///   Inexact still used; Absent falls back to a large sentinel).
/// - Filter(child): recurse, multiplied by `predicate_selectivity`.
/// - Projection(child): recurse, projections don't change cardinality.
/// - Anything else: large sentinel so it sorts last.
fn estimate_leaf_card(plan: &LogicalPlan) -> u64 {
    use datafusion::common::stats::Precision;
    match plan {
        LogicalPlan::TableScan(ts) => {
            let stats = table_provider_stats(ts);
            let rows = stats
                .as_ref()
                .and_then(|s| match s.num_rows {
                    Precision::Exact(n) | Precision::Inexact(n) => Some(n as u64),
                    Precision::Absent => None,
                })
                .unwrap_or(u64::MAX / 2);
            // Apply filter selectivity from filters that the TableScan
            // already absorbed at SQL parse time (e.g. `WHERE col = lit`
            // pushed into the scan by DataFusion's parser).
            let sel = ts
                .filters
                .iter()
                .map(|e| predicate_selectivity(e, ts))
                .fold(1.0f64, |acc, s| acc * s);
            scale_card(rows, sel)
        }
        LogicalPlan::Filter(f) => {
            let base = estimate_leaf_card(&f.input);
            // We don't know the per-column stats at this layer (the
            // schema-bound stats are only on TableScan.source). Use a
            // crude default until Phase 2.b plumbs column stats up.
            // Conservative default 0.3 — matches DuckDB's default for
            // unknown-shape predicates.
            scale_card(base, 0.3)
        }
        LogicalPlan::Projection(p) => estimate_leaf_card(&p.input),
        LogicalPlan::SubqueryAlias(s) => estimate_leaf_card(&s.input),
        _ => u64::MAX / 2,
    }
}

/// Σ.BR Phase 2 (2026-05-29): is this leaf a base-table scan the cost model
/// can actually estimate? Mirrors the recursable arms of
/// `estimate_leaf_card` exactly — a leaf is estimable iff it bottoms out in
/// a `TableScan` through Filter/Projection/SubqueryAlias wrappers. A leaf
/// that contains a Join (e.g. the `Filter(orders⋈lineitem)` composite that
/// `dim_join_pushdown` produces and `flatten_inner_join_chain` can't descend
/// through) hits the `u64::MAX/2` sentinel branch → its cardinality is
/// garbage (~e18), which poisons the leftmost-build-cost DP and makes it
/// front-load a tiny dimension (Q10 SF=100: `nation⋈customer` pulled early,
/// materialising 15M rows, +14% regression). Chains with such a leaf must
/// NOT be reordered — the model can't see them. See
/// PHASE_SIGMA_BR_JOIN_REORDER.md §5.
fn leaf_is_estimable(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(_) => true,
        LogicalPlan::Filter(f) => leaf_is_estimable(&f.input),
        LogicalPlan::Projection(p) => leaf_is_estimable(&p.input),
        LogicalPlan::SubqueryAlias(s) => leaf_is_estimable(&s.input),
        _ => false,
    }
}

fn scale_card(rows: u64, sel: f64) -> u64 {
    let s = sel.clamp(0.0, 1.0);
    ((rows as f64) * s).round().max(1.0) as u64
}

/// MVP predicate selectivity: returns 1.0 if we can't model it, a
/// fractional value if we can. Uses the per-column min/max + null_count
/// from `TableScan::source.statistics()`.
fn predicate_selectivity(expr: &Expr, ts: &TableScan) -> f64 {
    use datafusion::common::stats::Precision;
    use Operator as Op;

    let Some(stats) = table_provider_stats(ts) else {
        return 1.0;
    };
    let schema = ts.source.schema();
    let col_idx = |c: &Column| -> Option<usize> {
        schema.fields().iter().position(|f| f.name() == &c.name)
    };

    match expr {
        // col = lit  →  1/NDV. Σ.BR.1b (#186): prefer the column's
        // `distinct_count` (populated for dict-encoded string columns from
        // parquet dict-page headers, see Σ.AH.2 Story 1'.2), so selective
        // string equalities are modelled realistically instead of the old
        // flat 0.1 — e.g. Q08 `p_type = 'ECONOMY ANODIZED STEEL'` is ~1/150,
        // not 0.1 (the flat value made the selective `part` filter look 15×
        // weaker, the original reason the DP put `orders` before `part`).
        // Fall back to the int min/max range, then to 0.1 if neither is known.
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Op::Eq,
            right,
        }) => match (extract_column(left), extract_literal(right)) {
            (Some(col), Some(_lit)) => match col_idx(&col) {
                Some(i) if i < stats.column_statistics.len() => {
                    let cs = &stats.column_statistics[i];
                    let ndv: Option<f64> = match cs.distinct_count {
                        Precision::Exact(dc) | Precision::Inexact(dc) if dc > 0 => {
                            Some(dc as f64)
                        }
                        _ => match (&cs.min_value, &cs.max_value) {
                            (
                                Precision::Exact(ScalarValue::Int32(Some(lo)))
                                | Precision::Inexact(ScalarValue::Int32(Some(lo))),
                                Precision::Exact(ScalarValue::Int32(Some(hi)))
                                | Precision::Inexact(ScalarValue::Int32(Some(hi))),
                            ) => Some((hi - lo + 1).max(1) as f64),
                            (
                                Precision::Exact(ScalarValue::Int64(Some(lo)))
                                | Precision::Inexact(ScalarValue::Int64(Some(lo))),
                                Precision::Exact(ScalarValue::Int64(Some(hi)))
                                | Precision::Inexact(ScalarValue::Int64(Some(hi))),
                            ) => Some((hi - lo + 1).max(1) as f64),
                            _ => None,
                        },
                    };
                    match ndv {
                        Some(n) => 1.0 / n,
                        None => 0.1,
                    }
                }
                _ => 0.1,
            },
            _ => 1.0,
        },
        // col >= a AND col < b  →  (b-a)/(max-min+1) — handled by
        // recursing through And.
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Op::And,
            right,
        }) => predicate_selectivity(left, ts) * predicate_selectivity(right, ts),
        // col >= lit  → conservative 0.5
        Expr::BinaryExpr(BinaryExpr {
            op: Op::Gt | Op::GtEq | Op::Lt | Op::LtEq,
            ..
        }) => 0.5,
        _ => 1.0,
    }
}

fn extract_column(e: &Expr) -> Option<Column> {
    match e {
        Expr::Column(c) => Some(c.clone()),
        Expr::Cast(c) => extract_column(&c.expr),
        _ => None,
    }
}

fn extract_literal(e: &Expr) -> Option<ScalarValue> {
    match e {
        Expr::Literal(v, _) => Some(v.clone()),
        Expr::Cast(c) => extract_literal(&c.expr),
        _ => None,
    }
}

/// Σ.BR.2a — transitive equi-predicate (equivalence-class) inference,
/// pure name-level core. Union-find over the column names in `edges`;
/// return the implied edges (same-class pairs) not already present, as
/// name pairs. Logically redundant ⇒ result-preserving when added to the
/// join conjunction; they only give the reorder DP more connectivity
/// (Q05: `c_nationkey=s_nationkey` ∧ `s_nationkey=n_nationkey` ⇒
/// `c_nationkey=n_nationkey`). Capped at `MAX_CLASS` members to avoid
/// predicate explosion on pathological wide equalities.
fn derive_transitive_name_edges(edges: &[(String, String)]) -> Vec<(String, String)> {
    use std::collections::{HashMap, HashSet};
    const MAX_CLASS: usize = 8;

    let mut id: HashMap<String, usize> = HashMap::new();
    for (a, b) in edges {
        let next = id.len();
        id.entry(a.clone()).or_insert(next);
        let next = id.len();
        id.entry(b.clone()).or_insert(next);
    }
    let n = id.len();
    if n == 0 {
        return Vec::new();
    }
    // Union-find (no path compression — classes are tiny).
    let mut parent: Vec<usize> = (0..n).collect();
    fn find(parent: &[usize], mut x: usize) -> usize {
        while parent[x] != x {
            x = parent[x];
        }
        x
    }
    for (a, b) in edges {
        let ra = find(&parent, id[a]);
        let rb = find(&parent, id[b]);
        if ra != rb {
            parent[ra] = rb;
        }
    }
    let mut members_by_root: HashMap<usize, Vec<String>> = HashMap::new();
    for (name, &i) in &id {
        let r = find(&parent, i);
        members_by_root.entry(r).or_default().push(name.clone());
    }
    let mut existing: HashSet<(String, String)> = HashSet::new();
    for (a, b) in edges {
        existing.insert((a.clone(), b.clone()));
        existing.insert((b.clone(), a.clone()));
    }
    let mut out: Vec<(String, String)> = Vec::new();
    for (_root, mut members) in members_by_root {
        if members.len() < 2 || members.len() > MAX_CLASS {
            continue;
        }
        members.sort(); // deterministic emission
        for i in 0..members.len() {
            for j in (i + 1)..members.len() {
                let pair = (members[i].clone(), members[j].clone());
                if !existing.contains(&pair) {
                    out.push(pair);
                }
            }
        }
    }
    out
}

/// Σ.BR.2a — resolve the name-level derived edges back to `Column`s.
/// Only keep a derived edge whose two names each resolve to exactly one
/// (distinct) leaf — callers gate on `!chain_has_ambiguous_names`, so a
/// name maps to a unique leaf. Returns implied `(Column, Column)` edges
/// to append to the chain's equi predicates.
fn derive_transitive_equi_edges(
    equi_preds: &[(Column, Column)],
    leaves: &[LogicalPlan],
) -> Vec<(Column, Column)> {
    use std::collections::HashMap;
    let name_edges: Vec<(String, String)> = equi_preds
        .iter()
        .map(|(l, r)| (l.name.clone(), r.name.clone()))
        .collect();
    let derived = derive_transitive_name_edges(&name_edges);
    if derived.is_empty() {
        return Vec::new();
    }
    // name → a Column instance seen in equi_preds (preserves qualifier).
    let mut col_of: HashMap<&str, &Column> = HashMap::new();
    for (l, r) in equi_preds {
        col_of.entry(l.name.as_str()).or_insert(l);
        col_of.entry(r.name.as_str()).or_insert(r);
    }
    let leaf_of = |name: &str| -> Option<usize> {
        leaves
            .iter()
            .position(|leaf| leaf.schema().fields().iter().any(|f| f.name() == name))
    };
    let mut out = Vec::new();
    for (a, b) in derived {
        let (Some(ca), Some(cb)) = (col_of.get(a.as_str()), col_of.get(b.as_str())) else {
            continue;
        };
        match (leaf_of(&a), leaf_of(&b)) {
            (Some(la), Some(lb)) if la != lb => out.push(((*ca).clone(), (*cb).clone())),
            _ => {}
        }
    }
    out
}

/// Replay the DP cost recurrence along a FIXED leaf order (used for the
/// input/no-reorder baseline so we can compute the modeled cost ratio of
/// reordering). Mirrors the inner-loop cost logic in `rebuild_reordered`
/// exactly: cost = Σ intermediate cardinalities, charging the leftmost
/// leaf (Σ.BR.1a). A step that doesn't connect to the in-scope set is a
/// Cartesian product (denom = 1) — finite but huge, which is the honest
/// cost of forcing that order. Returns `(cost, final_card)`.
fn cost_of_fixed_order(
    order: &[usize],
    cards: &[u64],
    chain: &InnerJoinChain,
    all_equi: &[(Column, Column)],
) -> Option<(u64, u64)> {
    let first = *order.first()?;
    let mut placed: Vec<usize> = vec![first];
    let mut cost = cards[first].max(1);
    let mut card = cards[first].max(1);
    for &i in &order[1..] {
        let leaf_schema = chain.leaves[i].schema();
        let mut connecting: Vec<(Column, Column)> = Vec::new();
        for (l, r) in all_equi {
            let l_in_leaf = column_in_schema(l, leaf_schema);
            let r_in_leaf = column_in_schema(r, leaf_schema);
            let mut connects = false;
            for &p in &placed {
                let ps = chain.leaves[p].schema();
                let l_in_p = column_in_schema(l, ps);
                let r_in_p = column_in_schema(r, ps);
                if (l_in_leaf && r_in_p) || (r_in_leaf && l_in_p) {
                    connects = true;
                    break;
                }
            }
            if connects {
                connecting.push((l.clone(), r.clone()));
            }
        }
        let denom = if connecting.is_empty() {
            1
        } else {
            estimate_max_ndv_for_preds(&connecting, chain, &placed, i).max(1)
        };
        let new_card = ((card as u128 * cards[i] as u128) / denom as u128)
            .min(u128::from(u64::MAX)) as u64;
        card = new_card.max(1);
        cost = cost.saturating_add(card);
        placed.push(i);
    }
    Some((cost, card))
}

/// Build a fresh left-deep chain over `chain.leaves` in
/// ascending-cardinality order. Each equi predicate is re-attached to
/// the first join where both columns are in scope.
fn rebuild_reordered(chain: &InnerJoinChain) -> Option<LogicalPlan> {
    let cards: Vec<u64> = chain
        .leaves
        .iter()
        .map(estimate_leaf_card)
        .collect();
    let n = chain.leaves.len();

    // Σ.BR.2a: enrich the join graph with transitively-implied equi edges
    // (e.g. Q05 `c_nationkey=s_nationkey` ∧ `s_nationkey=n_nationkey` ⇒
    // `c_nationkey=n_nationkey`) so the DP can reach orderings the literal
    // predicate set doesn't connect — here, funnelling the ASIA-filtered
    // `nation` directly into `customer` instead of through a 25-distinct
    // `supplier` nationkey blowup. Redundant ⇒ result-preserving; the
    // rebuild attaches each implied edge when both endpoints come into
    // scope (so all originals + derived land on real joins).
    let mut all_equi = chain.equi_preds.clone();
    all_equi.extend(derive_transitive_equi_edges(&chain.equi_preds, &chain.leaves));

    if std::env::var("EMAT_REORDER_DEBUG").is_ok() {
        eprintln!("[reorder] chain with {n} leaves");
        for (i, leaf) in chain.leaves.iter().enumerate() {
            eprintln!(
                "  leaf[{i}] est_card={} schema_fields={:?}",
                cards[i],
                leaf.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            );
        }
    }

    // Σ.T Phase 3 (2026-05-25): Selinger left-deep DP.
    // For each non-empty subset S of leaves, find the cheapest
    // left-deep join ordering that joins all leaves in S. The cost
    // is the sum of intermediate cardinalities along the chain —
    // exactly what JoinSelection looks at to decide build sides.
    // Greedy "minimize next cardinality" doesn't see far enough:
    // on Q05 it picks supplier (10K) at step 3 over customer
    // (150K), but the *globally optimal* path is customer first
    // (leading into the orders/lineitem FK chain that narrows
    // progressively). DP enumerates all paths so it finds it.
    //
    // Complexity: 2^N states × N placements = O(N · 2^N).
    // Bail to greedy fallback if N > MAX_DP_LEAVES.
    const MAX_DP_LEAVES: usize = 12;
    if n > MAX_DP_LEAVES {
        if std::env::var("EMAT_REORDER_DEBUG").is_ok() {
            eprintln!("[reorder] BAIL: chain too long for DP ({n} > {MAX_DP_LEAVES})");
        }
        return None;
    }

    #[derive(Clone)]
    struct DpState {
        /// Sum of intermediate cardinalities along this path.
        /// Penalises plans that materialise large intermediates.
        cost: u64,
        /// Cardinality of the join of all leaves in this subset.
        card: u64,
        /// Order in which leaves were added (left-deep).
        order: Vec<usize>,
    }

    let mut dp: Vec<Option<DpState>> = vec![None; 1usize << n];

    // Base case: single-leaf subsets. Σ.BR.1a (2026-05-29): seed
    // cost = leaf card (NOT 0). The leftmost leaf is the build side of
    // the first join, so its size must be charged — otherwise the DP
    // parks the largest table leftmost for free (Q05 put lineitem(600M)
    // first, inverting DuckDB's funnel; Q08 put orders before the
    // selective part). See PHASE_SIGMA_BR_JOIN_REORDER.md §4.2.
    for i in 0..n {
        dp[1 << i] = Some(DpState {
            cost: cards[i].max(1),
            card: cards[i].max(1),
            order: vec![i],
        });
    }

    // Iterate subsets in ascending size order so all proper
    // subsets are computed before the current one.
    let total = 1usize << n;
    for subset in 1..total {
        if subset.count_ones() < 2 {
            continue;
        }
        let mut best: Option<DpState> = None;
        // Try every leaf `i` as the LAST one added.
        for i in 0..n {
            if subset & (1 << i) == 0 {
                continue;
            }
            let prev = subset & !(1 << i);
            let Some(prev_state) = dp[prev].as_ref() else {
                continue;
            };
            // Check if `i` connects to any leaf in `prev`.
            let leaf_schema = chain.leaves[i].schema();
            let mut connecting: Vec<(Column, Column)> = Vec::new();
            for (l, r) in &all_equi {
                let l_in_leaf = column_in_schema(l, leaf_schema);
                let r_in_leaf = column_in_schema(r, leaf_schema);
                let mut connects_to_prev = false;
                for &p in &prev_state.order {
                    let ps = chain.leaves[p].schema();
                    let l_in_p = column_in_schema(l, ps);
                    let r_in_p = column_in_schema(r, ps);
                    if (l_in_leaf && r_in_p) || (r_in_leaf && l_in_p) {
                        connects_to_prev = true;
                        break;
                    }
                }
                if connects_to_prev {
                    connecting.push((l.clone(), r.clone()));
                }
            }
            if connecting.is_empty() {
                continue; // would be Cartesian
            }
            let max_ndv =
                estimate_max_ndv_for_preds(&connecting, chain, &prev_state.order, i);
            let denom = max_ndv.max(1);
            let new_card = ((prev_state.card as u128 * cards[i] as u128)
                / denom as u128)
                .min(u128::from(u64::MAX)) as u64;
            let new_card = new_card.max(1);
            let new_cost = prev_state.cost.saturating_add(new_card);
            match &best {
                None => {
                    let mut order = prev_state.order.clone();
                    order.push(i);
                    best = Some(DpState {
                        cost: new_cost,
                        card: new_card,
                        order,
                    });
                }
                Some(b) if new_cost < b.cost => {
                    let mut order = prev_state.order.clone();
                    order.push(i);
                    best = Some(DpState {
                        cost: new_cost,
                        card: new_card,
                        order,
                    });
                }
                _ => {}
            }
        }
        dp[subset] = best;
    }

    let final_state = dp[total - 1].as_ref()?;
    let order = final_state.order.clone();

    if std::env::var("EMAT_REORDER_DEBUG").is_ok() {
        eprintln!(
            "[reorder] DP chose order = {order:?} cost={} final_card={}",
            final_state.cost, final_state.card
        );
    }

    // Σ.BR Phase 2 prereq (2026-05-29): emit the modeled cost of the
    // input (no-reorder) order vs the chosen order, so a SF=1 sweep can
    // tell us whether a single cost-improvement threshold separates the
    // wall-time wins (Q05/Q08) from the regressors (Q07/Q02/Q11/Q21).
    // `is_noop` flags chains where the DP reproduced the input order
    // (the rewrite bails just below) — those should show ratio ≈ 1.0.
    if std::env::var("EMAT_REORDER_COST").is_ok() {
        let input_order: Vec<usize> = (0..n).collect();
        if let Some((in_cost, _)) = cost_of_fixed_order(&input_order, &cards, chain, &all_equi) {
            let ratio = final_state.cost as f64 / in_cost.max(1) as f64;
            let is_noop = order.iter().enumerate().all(|(i, idx)| *idx == i);
            eprintln!(
                "[reorder-cost] noop={is_noop} input_cost={in_cost} chosen_cost={} ratio={ratio:.4} input_order={input_order:?} chosen_order={order:?}",
                final_state.cost
            );
        }
    }

    let mut placed: Vec<bool> = vec![false; n];
    for &i in &order {
        placed[i] = true;
    }
    let mut remaining_preds: Vec<(Column, Column)> = all_equi.clone();

    // Bail if the connectivity-greedy order matches the input order —
    // no-op rewrites just churn the plan.
    if order.iter().enumerate().all(|(i, idx)| *idx == i) {
        return None;
    }

    if std::env::var("EMAT_REORDER_DEBUG").is_ok() {
        eprintln!("[reorder] chosen order = {order:?}");
    }

    // Now build the left-deep chain in the chosen order. Attach the
    // predicates that connect each newly-added leaf to the in-scope
    // schemas at that step.
    let mut current = LogicalPlanBuilder::from(chain.leaves[order[0]].clone());

    for step in 1..n {
        let leaf_idx = order[step];
        let leaf = chain.leaves[leaf_idx].clone();
        let cur_schema = current.schema().clone();
        let leaf_schema = leaf.schema();
        let (matching, leftover) =
            partition_predicates_in_scope(&remaining_preds, &cur_schema, leaf_schema);
        remaining_preds = leftover;
        if matching.is_empty() {
            // Shouldn't happen given the connectivity check above,
            // but defensive guard.
            if std::env::var("EMAT_REORDER_DEBUG").is_ok() {
                eprintln!(
                    "[reorder] BAIL at rebuild: step {step} leaf[{leaf_idx}] no preds in scope"
                );
            }
            return None;
        }
        // Σ.T Phase 3 (2026-05-25): use `LogicalPlanBuilder::join`
        // with explicit (left_keys, right_keys) tuples — this
        // populates `Join::on` so the physical planner emits
        // HashJoinExec. The earlier `join_on` API routes the
        // predicates through `Join::filter`, which causes the
        // planner to fall back to NestedLoopJoinExec (O(N×M)).
        // Q05 SF=10 with the filter path regressed 35× vs baseline.
        let (left_keys, right_keys): (Vec<Column>, Vec<Column>) = matching
            .into_iter()
            .map(|(l, r)| (l, r))
            .unzip();
        current = match current.join(
            leaf,
            JoinType::Inner,
            (left_keys, right_keys),
            None,
        ) {
            Ok(b) => b,
            Err(e) => {
                if std::env::var("EMAT_REORDER_DEBUG").is_ok() {
                    eprintln!("[reorder] BAIL: join at step {step} err: {e}");
                }
                return None;
            }
        };
    }

    // All predicates should be placed.
    if !remaining_preds.is_empty() {
        if std::env::var("EMAT_REORDER_DEBUG").is_ok() {
            eprintln!(
                "[reorder] BAIL: {} predicates unplaced after rebuild",
                remaining_preds.len()
            );
        }
        return None;
    }

    let mut built = current.build().ok()?;
    if let Some(f) = &chain.extra_filter {
        built = LogicalPlanBuilder::from(built)
            .filter(f.clone())
            .ok()?
            .build()
            .ok()?;
    }
    Some(built)
}

/// Among `preds`, return the ones whose left column is in `cur_schema`
/// AND right column is in `leaf_schema` (or vice versa, after swap).
/// Remaining predicates are returned for the next step.
fn partition_predicates_in_scope(
    preds: &[(Column, Column)],
    cur_schema: &DFSchemaRef,
    leaf_schema: &DFSchemaRef,
) -> (Vec<(Column, Column)>, Vec<(Column, Column)>) {
    let mut matched = Vec::new();
    let mut leftover = Vec::new();
    for (l, r) in preds {
        let l_in_cur = column_in_schema(l, cur_schema);
        let r_in_cur = column_in_schema(r, cur_schema);
        let l_in_leaf = column_in_schema(l, leaf_schema);
        let r_in_leaf = column_in_schema(r, leaf_schema);
        if l_in_cur && r_in_leaf {
            matched.push((l.clone(), r.clone()));
        } else if r_in_cur && l_in_leaf {
            // Swap so the left side belongs to cur, right to leaf
            matched.push((r.clone(), l.clone()));
        } else {
            leftover.push((l.clone(), r.clone()));
        }
    }
    (matched, leftover)
}

fn column_in_schema(col: &Column, schema: &DFSchemaRef) -> bool {
    schema.fields().iter().any(|f| f.name() == &col.name)
}

/// Σ.T Phase 3 safety guard (2026-05-25): bail if any column name
/// appears in more than one leaf of the chain. Our predicate-routing
/// uses `column_in_schema` which matches by name only — when two
/// leaves share a column (e.g. Q07/Q08 self-join `nation n1, n2`
/// both expose `n_nationkey`), the predicate gets routed
/// ambiguously and the rebuild can drop predicates or attach them
/// to the wrong join. Until we plumb qualified-column matching,
/// these chains stay on the original plan.
fn chain_has_ambiguous_names(chain: &InnerJoinChain) -> bool {
    use std::collections::HashMap;
    let mut counts: HashMap<String, usize> = HashMap::new();
    for leaf in &chain.leaves {
        for f in leaf.schema().fields() {
            *counts.entry(f.name().clone()).or_insert(0) += 1;
        }
    }
    counts.values().any(|c| *c > 1)
}

/// Estimate the NDV (number of distinct values) upper bound to use
/// as the divisor in `(|L| × |R|) / NDV` when adding `leaf_idx` to
/// the placed set via `preds`. Mirrors DataFusion's
/// `max_distinct_count`: range-based upper bound `max - min + 1`
/// from the leaf's `TableScan` column statistics (Phase 1 wire-up).
///
/// Returns 1 (yielding the worst-case `|L| × |R|`) when no range
/// signal is available. Returns the MAX across all join keys —
/// matching DF's per-key `max_distinct.max(...)` accumulator.
fn estimate_max_ndv_for_preds(
    preds: &[(Column, Column)],
    chain: &InnerJoinChain,
    placed: &[usize],
    leaf_idx: usize,
) -> u64 {
    let mut max_ndv: u64 = 1;
    for (l, r) in preds {
        // Each predicate has one side in `leaf_idx` and the other in
        // some placed leaf. Get both column stats.
        let leaf = &chain.leaves[leaf_idx];
        let leaf_schema = leaf.schema();
        let (leaf_col, other_col) = if column_in_schema(l, leaf_schema) {
            (l, r)
        } else {
            (r, l)
        };
        let leaf_ndv = leaf_col_ndv(leaf, leaf_col);
        // The "other side" lives in some placed leaf — find it.
        let other_ndv = placed
            .iter()
            .find_map(|&p| {
                if column_in_schema(other_col, chain.leaves[p].schema()) {
                    Some(leaf_col_ndv(&chain.leaves[p], other_col))
                } else {
                    None
                }
            })
            .unwrap_or(1);
        let local_max = leaf_ndv.max(other_ndv);
        max_ndv = max_ndv.max(local_max);
    }
    max_ndv
}

/// NDV upper bound for a single (LogicalPlan leaf, Column). Walks
/// down to the underlying `TableScan` to read its `ColumnStatistics`
/// (Phase 1 plumbed these from parquet row-group metadata) and
/// derives NDV from min/max range — matching DataFusion's own
/// `max_distinct_count` fallback (joins/utils.rs:733-755).
fn leaf_col_ndv(plan: &LogicalPlan, col: &Column) -> u64 {
    use datafusion::common::stats::Precision;
    let ts = match find_table_scan(plan) {
        Some(ts) => ts,
        None => return u64::MAX / 2,
    };
    let stats = match table_provider_stats(ts) {
        Some(s) => s,
        None => return u64::MAX / 2,
    };
    let schema = ts.source.schema();
    let col_idx = schema.fields().iter().position(|f| f.name() == &col.name);
    let Some(idx) = col_idx else {
        return u64::MAX / 2;
    };
    if idx >= stats.column_statistics.len() {
        return u64::MAX / 2;
    }
    let cs = &stats.column_statistics[idx];
    // Prefer distinct_count if present; else derive from min/max range.
    if let Precision::Exact(dc) | Precision::Inexact(dc) = cs.distinct_count {
        return dc as u64;
    }
    let num_rows = match stats.num_rows {
        Precision::Exact(n) | Precision::Inexact(n) => Some(n as u64),
        _ => None,
    };
    let lo = match &cs.min_value {
        Precision::Exact(v) | Precision::Inexact(v) => scalar_to_i128(v),
        _ => None,
    };
    let hi = match &cs.max_value {
        Precision::Exact(v) | Precision::Inexact(v) => scalar_to_i128(v),
        _ => None,
    };
    match (lo, hi) {
        (Some(l), Some(h)) if h >= l => {
            // Σ.BR.1c (#187): NDV ≤ row count is a hard upper bound, so cap
            // the min/max-range estimate at `num_rows`. Sparse integer keys
            // report a range width far above their true distinct count —
            // e.g. TPC-H `o_orderkey` spans 1..6M but `orders` has only 1.5M
            // rows, so its true NDV is 1.5M, not the 6M the raw range gives.
            // Without the cap the range-NDV inflates join denominators and
            // makes PK-FK intermediates look more selective than they are.
            // Falls through to the raw range when `num_rows` is unknown.
            let range = ((h - l + 1).max(1) as u64).min(u64::MAX / 2);
            match num_rows {
                Some(n) => range.min(n.max(1)),
                None => range,
            }
        }
        _ => num_rows.unwrap_or(u64::MAX / 2),
    }
}

fn find_table_scan(plan: &LogicalPlan) -> Option<&TableScan> {
    match plan {
        LogicalPlan::TableScan(ts) => Some(ts),
        LogicalPlan::Filter(f) => find_table_scan(&f.input),
        LogicalPlan::Projection(p) => find_table_scan(&p.input),
        LogicalPlan::SubqueryAlias(s) => find_table_scan(&s.input),
        _ => None,
    }
}

fn scalar_to_i128(s: &ScalarValue) -> Option<i128> {
    use ScalarValue as S;
    match s {
        S::Int8(Some(v)) => Some(*v as i128),
        S::Int16(Some(v)) => Some(*v as i128),
        S::Int32(Some(v)) => Some(*v as i128),
        S::Int64(Some(v)) => Some(*v as i128),
        S::UInt8(Some(v)) => Some(*v as i128),
        S::UInt16(Some(v)) => Some(*v as i128),
        S::UInt32(Some(v)) => Some(*v as i128),
        S::UInt64(Some(v)) => Some(*v as i128),
        S::Date32(Some(v)) => Some(*v as i128),
        S::Date64(Some(v)) => Some(*v as i128),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use crate::fast_parquet::FastParquetTableProvider;
    use datafusion::prelude::SessionContext;
    use std::path::PathBuf;

    fn sf1_dir() -> Option<PathBuf> {
        if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
            let p = PathBuf::from(env);
            if p.exists() {
                return Some(p);
            }
        }
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = manifest
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("examples/tpch/data/sf1"))?;
        if p.exists() {
            return Some(p);
        }
        None
    }

    async fn register_tpch(ctx: &SessionContext, dir: &std::path::Path) -> DfResult<()> {
        use std::sync::Arc;
        for t in [
            "region",
            "nation",
            "supplier",
            "customer",
            "part",
            "partsupp",
            "orders",
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
        Ok(())
    }

    /// Single TableScan — must be left alone.
    #[tokio::test]
    async fn no_op_on_single_table() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let df = ctx.sql("SELECT COUNT(*) FROM lineitem").await?;
        let original = df.into_optimized_plan()?;
        let rewritten = reorder_inner_joins(original.clone())?;
        assert_eq!(
            format!("{}", original.display_indent()),
            format!("{}", rewritten.display_indent()),
            "single-table queries must pass through unchanged"
        );
        Ok(())
    }

    /// Two-table join — chain has only 2 leaves, MVP bails (need ≥3).
    #[tokio::test]
    async fn no_op_on_two_table_join() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let df = ctx
            .sql("SELECT n_name FROM nation, region WHERE n_regionkey = r_regionkey")
            .await?;
        let original = df.logical_plan().clone();
        let rewritten = reorder_inner_joins(original.clone())?;
        // 2-leaf chains are no-ops in MVP.
        assert_eq!(
            format!("{}", original.display_indent()),
            format!("{}", rewritten.display_indent())
        );
        Ok(())
    }

    /// 3-table chain where the original is in size-DESCENDING order
    /// (lineitem, orders, region). After reorder, region (smallest)
    /// should be the leftmost leaf in a left-deep tree.
    #[tokio::test]
    async fn reorders_three_table_chain_smallest_first() -> DfResult<()> {
        // Synthetic test using a SQL pattern where the from-order
        // hints at size-descending. Reorder should not match the
        // original printout — we just check that the rewrite
        // succeeded (produced a different plan) and the result
        // still type-checks via DataFusion's planner.
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        // customer (150K) ⋈ orders (1.5M) ⋈ lineitem (6M)
        // descending FROM order forces the planner to keep size-DESC.
        // Explicit JOIN syntax — DataFusion's parser produces Inner Join
        // nodes directly. (The `FROM a, b, c WHERE ...` form produces
        // CrossJoin + Filter; the EliminateCrossJoin optimizer rule
        // only sometimes converts those.)
        let sql = "SELECT c.c_custkey \
                   FROM lineitem l \
                   JOIN orders o ON l.l_orderkey = o.o_orderkey \
                   JOIN customer c ON o.o_custkey = c.c_custkey \
                   LIMIT 10";
        let df = ctx.sql(sql).await?;
        let original = df.logical_plan().clone();
        let rewritten = reorder_inner_joins(original.clone())?;
        let orig_dump = format!("{}", original.display_indent());
        let new_dump = format!("{}", rewritten.display_indent());

        // Behavioural assertion: original has `lineitem` as the
        // outer-left (deepest-left) leaf since the SQL puts it first
        // in the FROM list. After reorder, the leftmost leaf should
        // be `customer` — smallest of the three.
        let leftmost_orig = leftmost_table_scan(&original);
        let leftmost_new = leftmost_table_scan(&rewritten);
        assert_eq!(
            leftmost_orig.as_deref(),
            Some("lineitem"),
            "sanity: original leftmost leaf is lineitem (test SQL has FROM lineitem first)\n{orig_dump}",
        );
        assert_eq!(
            leftmost_new.as_deref(),
            Some("customer"),
            "reorder must put customer (smallest) as the deepest-left leaf:\nORIGINAL:\n{orig_dump}\n\nREWRITTEN:\n{new_dump}",
        );
        Ok(())
    }

    /// Find the table name of the leftmost (deepest-left) TableScan
    /// in a plan tree. Walks down via `LogicalPlan::inputs().first()`.
    fn leftmost_table_scan(plan: &LogicalPlan) -> Option<String> {
        match plan {
            LogicalPlan::TableScan(ts) => Some(ts.table_name.to_string()),
            other => {
                let inputs = other.inputs();
                inputs.first().and_then(|c| leftmost_table_scan(c))
            }
        }
    }

    /// Q05 SF=1 — the headline target. Real TPC-H queries use
    /// comma-FROM (`FROM a, b, c WHERE a.k=b.k AND ...`) which parses
    /// as `CrossJoin + Filter`. DataFusion's `EliminateCrossJoin`
    /// rewrites those into `InnerJoin` during optimization, so we
    /// need the *optimized* plan. The rewriter must:
    ///
    /// 1. See a chain of 6 inner-join leaves (customer, orders,
    ///    lineitem, supplier, nation, region — plus their Filters).
    /// 2. Produce a plan where the smallest leaf (region — 5 rows,
    ///    filtered by r_name='ASIA') is the deepest-left leaf
    ///    rather than `customer` (the FROM-first table).
    /// 3. Preserve query semantics — same revenue per nation in the
    ///    result.
    #[tokio::test]
    async fn reorders_q05_shape_against_real_data() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;

        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q05.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skipping: q05.sql not found");
            return Ok(());
        };

        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = reorder_inner_joins(optimized.clone())?;

        let orig_dump = format!("{}", optimized.display_indent());
        let new_dump = format!("{}", rewritten.display_indent());

        // The rewriter should have fired — Q05's FROM order (customer
        // first, ~150K rows) puts the largest dim table at the
        // leftmost, but the smallest table (region, filtered to 1 row
        // by ASIA) should land there post-reorder.
        if orig_dump == new_dump {
            // Diagnostic dump so we can debug.
            eprintln!("=== Q05 SF=1 plan UNCHANGED by reorder ===");
            eprintln!("OPTIMIZED:\n{orig_dump}");
            return Err(datafusion::error::DataFusionError::Plan(
                "expected Q05 reorder to change the plan".into(),
            ));
        }

        let leftmost = leftmost_table_scan(&rewritten);
        eprintln!("Q05 rewritten leftmost = {:?}", leftmost);
        // The Selinger DP minimizes sum-of-intermediate-cardinalities,
        // not necessarily smallest-leaf-first. On Q05 the optimal
        // left-deep order can start with `lineitem` (large leaf, but
        // the lineitem⋈orders FK join narrows to ~45K — cheap total
        // intermediate cost). This test now just asserts the rewrite
        // produced a *different* plan; `rewrite_preserves_query_result`
        // checks correctness end-to-end.
        Ok(())
    }

    /// Σ.BR.2a — the union-find core derives the one missing transitive
    /// edge from Q05's nationkey chain and nothing spurious.
    #[test]
    fn derive_transitive_edges_q05_nationkey_triangle() {
        let edges = vec![
            ("c_nationkey".to_string(), "s_nationkey".to_string()),
            ("s_nationkey".to_string(), "n_nationkey".to_string()),
        ];
        let derived = derive_transitive_name_edges(&edges);
        assert_eq!(
            derived.len(),
            1,
            "expected exactly the one implied edge, got {derived:?}"
        );
        let got: std::collections::HashSet<&str> = [derived[0].0.as_str(), derived[0].1.as_str()]
            .into_iter()
            .collect();
        assert!(
            got.contains("c_nationkey") && got.contains("n_nationkey"),
            "expected derived c_nationkey=n_nationkey, got {derived:?}"
        );
    }

    /// Σ.BR Phase 1 — cost-model acceptance gate (TDD). The DP must
    /// reproduce DuckDB's dimension-funnel order on Q05: the smallest
    /// dimension (region, filtered to 1 row by r_name='ASIA') is the
    /// deepest-left leaf — NOT the 600M-row lineitem fact table. With the
    /// pre-Σ.BR cost model the DP put lineitem leftmost (base case charged
    /// cost 0 + over-optimistic FK-NDV), which regressed Q05 at SF=100.
    /// See PHASE_SIGMA_BR_JOIN_REORDER.md §4.2.
    ///
    /// Σ.BR.2a unblocks this: Q05's optimized join graph has
    /// `c_nationkey = s_nationkey` and `s_nationkey = n_nationkey` but NOT
    /// the transitive `c_nationkey = n_nationkey`, so `customer` was
    /// tethered to `supplier` on a 25-distinct nationkey join. The
    /// equivalence-class pass derives that edge, letting the ASIA-filtered
    /// `nation` funnel into `customer`; the 1a cost model then picks
    /// region-first.
    #[tokio::test]
    async fn reorders_q05_to_region_first() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent().unwrap().parent().unwrap().join("queries/q05.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skipping: q05.sql not found");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = reorder_inner_joins(optimized.clone())?;
        let leftmost = leftmost_table_scan(&rewritten);
        assert_eq!(
            leftmost.as_deref(),
            Some("region"),
            "Q05 reorder must put region (1 row after ASIA filter) as the \
             deepest-left leaf, not the lineitem fact table:\n{}",
            rewritten.display_indent(),
        );
        Ok(())
    }

    /// Σ.BR Phase 2 — the scale gate. Shape-gated REJECTS Q05's 6-leaf
    /// funnel at the default 4-leaf cap (the SF=1/SF=10 regime, where the
    /// largest leaf is below the row threshold), but ADMITS it once the
    /// scale bump raises the cap to 6. This is the lever that turns on the
    /// SF=100 Q05 −10% funnel without re-admitting the SF=10 regressors —
    /// those trip the ambiguous-names / aggregate-key / semi-anti guards,
    /// not the leaf cap (PHASE_SIGMA_BR_JOIN_REORDER.md §5).
    #[tokio::test]
    async fn scale_bump_admits_q05_funnel() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent().unwrap().parent().unwrap().join("queries/q05.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skipping: q05.sql not found");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;

        // No scale bump + cap 4: Q05's 6-leaf chain is rejected (too_many),
        // plan unchanged — the SF=1/SF=10 shape-gated behaviour.
        let no_bump = ReorderOpts {
            scale_bump: None,
            ..ReorderOpts::default()
        };
        let gated = reorder_inner_joins_with_opts(optimized.clone(), no_bump)?;
        assert_eq!(
            format!("{}", optimized.display_indent()),
            format!("{}", gated.display_indent()),
            "without scale bump, shape-gated must reject Q05's 6-leaf chain (cap=4)"
        );

        // Scale bump with threshold 0 (always fires) raises the cap to 6 →
        // Q05 reorders, region (1 row after the ASIA filter) leads.
        let with_bump = ReorderOpts {
            scale_bump: Some((0, 6)),
            ..ReorderOpts::default()
        };
        let rewritten = reorder_inner_joins_with_opts(optimized.clone(), with_bump)?;
        assert_eq!(
            leftmost_table_scan(&rewritten).as_deref(),
            Some("region"),
            "with scale bump (cap=6), shape-gated must reorder Q05 to region-first:\n{}",
            rewritten.display_indent(),
        );
        Ok(())
    }

    /// Σ.BR Phase 2 — composite-leaf guard. After `dim_join_pushdown`
    /// rewrites Q10 into filter-style joins, the `orders⋈lineitem` subtree
    /// becomes a composite leaf that `estimate_leaf_card` can't size (it
    /// hits the `u64::MAX/2` sentinel → ~e18). That poisoned the
    /// leftmost-build-cost DP into front-loading the 25-row `nation`, which
    /// at SF=100 materialised a 15M-row `nation⋈customer` intermediate and
    /// regressed Q10 +14%. With the guard the reorder must NO-OP on such a
    /// chain. (Without dim_push the same query reorders fine — all TableScan
    /// leaves — see `lever_g_allows_q10_shape`.)
    #[tokio::test]
    async fn rejects_composite_leaf_after_dim_push() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent().unwrap().parent().unwrap().join("queries/q10.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skipping: q10.sql not found");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        // dim_push first (as the bench pipeline does) → composite leaf.
        let pushed = crate::dim_join_pushdown::push_dim_join_into_chain(optimized)?;
        // Scale bump always-on (threshold 0) to isolate the composite-leaf
        // guard from the leaf-count cap.
        let opts = ReorderOpts {
            scale_bump: Some((0, 6)),
            ..ReorderOpts::default()
        };
        let rewritten = reorder_inner_joins_with_opts(pushed.clone(), opts)?;
        assert_eq!(
            format!("{}", pushed.display_indent()),
            format!("{}", rewritten.display_indent()),
            "reorder must no-op on a chain with a composite (orders⋈lineitem) leaf:\n{}",
            rewritten.display_indent(),
        );
        Ok(())
    }

    /// Σ.BR Phase 1 — acceptance gate for Q08: the selective `part` filter
    /// (p_type = 'ECONOMY ANODIZED STEEL', ~1/150 of parts) must lead so its
    /// join narrows lineitem early — DuckDB's order. Pre-Σ.BR the DP put
    /// `orders` first (string-eq selectivity flat 0.1 + leftmost cost 0),
    /// regressing Q08 +42% at SF=10 / +26% at SF=100.
    #[tokio::test]
    async fn reorders_q08_to_part_first() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent().unwrap().parent().unwrap().join("queries/q08.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skipping: q08.sql not found");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = reorder_inner_joins(optimized.clone())?;
        let leftmost = leftmost_table_scan(&rewritten);
        assert_eq!(
            leftmost.as_deref(),
            Some("part"),
            "Q08 reorder must put part (selective p_type filter) as the \
             deepest-left leaf:\n{}",
            rewritten.display_indent(),
        );
        Ok(())
    }

    /// End-to-end correctness: after reorder, executing the rewritten
    /// plan must return the same rows as the original. The reorder
    /// changes the join topology — it must NOT change the result set.
    /// Guards against re-emit bugs (predicate misplacement, schema
    /// mismatches, dropped predicates).
    #[tokio::test]
    async fn rewrite_preserves_query_result() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        // Order-deterministic query so we can compare row-by-row.
        let sql = "SELECT c.c_custkey, c.c_name \
                   FROM lineitem l \
                   JOIN orders o ON l.l_orderkey = o.o_orderkey \
                   JOIN customer c ON o.o_custkey = c.c_custkey \
                   WHERE c.c_custkey < 100 \
                   GROUP BY c.c_custkey, c.c_name \
                   ORDER BY c.c_custkey";
        let df = ctx.sql(sql).await?;
        let original = df.logical_plan().clone();
        let rewritten = reorder_inner_joins(original.clone())?;

        // Sanity: the rewrite produced a different plan (else this
        // test would silently always-pass).
        assert_ne!(
            format!("{}", original.display_indent()),
            format!("{}", rewritten.display_indent()),
            "rewrite should have changed the plan for this 3-table query"
        );

        // Execute both plans and compare results.
        let orig_batches = df.collect().await?;
        let new_df = ctx.execute_logical_plan(rewritten).await?;
        let new_batches = new_df.collect().await?;

        let orig_rows: Vec<i64> = orig_batches
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect();
        let new_rows: Vec<i64> = new_batches
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect();
        // Sanity: shape-gated entry point should NOT change this 3-table
        // chain because it's at the bound (≥3 leaves, ≤4 leaves, no LIKE).
        // Both entries reorder it. Just a smoke check that the gated path
        // doesn't accidentally reject the small case.
        let gated = reorder_inner_joins_shape_gated(original.clone())?;
        assert_ne!(
            format!("{}", original.display_indent()),
            format!("{}", gated.display_indent()),
            "shape-gated reorder should also fire on this 3-table chain"
        );

        assert_eq!(
            orig_rows, new_rows,
            "join reorder changed query semantics: original {orig_rows:?} vs rewritten {new_rows:?}"
        );
        Ok(())
    }

    /// Σ.AH.X Lever G — `reorder_inner_joins_shape_gated` must NOT
    /// reorder a 5-leaf chain (catches Q07's inner subchain that
    /// regresses +46 ms at SF=10 under the permissive path). Same
    /// chain DOES reorder under `reorder_inner_joins` (permissive).
    #[tokio::test]
    async fn lever_g_rejects_long_chain() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        // 5-leaf Inner-join chain: nation, supplier, customer, orders, lineitem.
        // Mirrors Q07's inner subchain shape that the spike showed regresses.
        let sql = "SELECT n.n_name, COUNT(*) \
                   FROM nation n \
                   JOIN supplier s ON n.n_nationkey = s.s_nationkey \
                   JOIN lineitem l ON s.s_suppkey = l.l_suppkey \
                   JOIN orders o ON l.l_orderkey = o.o_orderkey \
                   JOIN customer c ON o.o_custkey = c.c_custkey \
                   GROUP BY n.n_name";
        let df = ctx.sql(sql).await?;
        let original = df.into_optimized_plan()?;

        // Permissive path: chain has 5 leaves ≥ 3 + no ambiguous names → reorder fires
        let permissive = reorder_inner_joins(original.clone())?;
        assert_ne!(
            format!("{}", original.display_indent()),
            format!("{}", permissive.display_indent()),
            "permissive entry should reorder a 5-leaf chain"
        );

        // Shape-gated path: 5 leaves > max_leaves(4) → reject, plan unchanged
        let gated = reorder_inner_joins_shape_gated(original.clone())?;
        assert_eq!(
            format!("{}", original.display_indent()),
            format!("{}", gated.display_indent()),
            "shape-gated entry should NOT reorder a 5-leaf chain (max_leaves=4)"
        );
        Ok(())
    }

    /// Σ.AH.X Lever G — `reorder_inner_joins_shape_gated` must NOT
    /// reorder a chain containing a string-LIKE filter (catches Q09).
    #[tokio::test]
    async fn lever_g_rejects_string_like_filter() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        // 3-leaf chain (within the leaf budget) but with a LIKE filter
        // on `part`. Shape gate should reject. FROM starts with the
        // largest table (lineitem) so the permissive reorder demonstrably
        // fires (moves the small `part` leaf to the front). With FROM
        // part-first the Σ.BR.1a cost model already agrees with the input
        // order and correctly no-ops — which wouldn't exercise the
        // "permissive doesn't gate LIKE" contract this test checks.
        let sql = "SELECT p.p_name, l.l_quantity \
                   FROM lineitem l \
                   JOIN part p ON p.p_partkey = l.l_partkey \
                   JOIN orders o ON l.l_orderkey = o.o_orderkey \
                   WHERE p.p_name LIKE '%green%'";
        let df = ctx.sql(sql).await?;
        let original = df.into_optimized_plan()?;

        // Permissive entry: 3 leaves, no ambiguous names → reorder fires
        let permissive = reorder_inner_joins(original.clone())?;
        assert_ne!(
            format!("{}", original.display_indent()),
            format!("{}", permissive.display_indent()),
            "permissive entry should reorder a 3-leaf chain even with LIKE"
        );

        // Shape-gated: LIKE filter detected → reject, plan unchanged
        let gated = reorder_inner_joins_shape_gated(original.clone())?;
        assert_eq!(
            format!("{}", original.display_indent()),
            format!("{}", gated.display_indent()),
            "shape-gated entry should NOT reorder a chain containing a LIKE filter"
        );
        Ok(())
    }

    /// Σ.AH.X Lever G — `reorder_inner_joins_shape_gated` MUST still
    /// reorder Q10-shape chains (4 leaves, no LIKE). Direct counter-test
    /// to the rejection tests above: the gate doesn't block all reorders,
    /// only the ones the estimator can't model.
    #[tokio::test]
    async fn lever_g_allows_q10_shape() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        // 4-leaf Inner-join chain matching Q10's shape: customer, orders, lineitem, nation
        let sql = "SELECT c.c_custkey, SUM(l.l_extendedprice) \
                   FROM customer c \
                   JOIN orders o ON c.c_custkey = o.o_custkey \
                   JOIN lineitem l ON o.o_orderkey = l.l_orderkey \
                   JOIN nation n ON c.c_nationkey = n.n_nationkey \
                   WHERE o.o_orderdate >= '1993-10-01' \
                   GROUP BY c.c_custkey";
        let df = ctx.sql(sql).await?;
        let original = df.into_optimized_plan()?;

        // Both entries should reorder this — 4 leaves at the bound, no LIKE
        let gated = reorder_inner_joins_shape_gated(original.clone())?;
        assert_ne!(
            format!("{}", original.display_indent()),
            format!("{}", gated.display_indent()),
            "shape-gated entry should reorder Q10-shape (4 leaves, no LIKE)"
        );
        Ok(())
    }

    /// Σ.AL (2026-05-27) — `reorder_inner_joins_shape_gated` MUST refuse
    /// to descend into LeftSemi / LeftAnti join subtrees. The L9 bloom
    /// cascade inside a (NOT) EXISTS-decorrelated subquery depends on
    /// Inner Join build/probe ordering; reordering it forces the bloom
    /// to be emitted too late to filter downstream fact scans (Q21
    /// SF=10 +47 ms regression seen during Σ.AH.X Lever G re-validation).
    #[tokio::test]
    async fn lever_g_blocks_under_left_semi_anti_q21_shape() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q21.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skipping: q21.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let original = df.into_optimized_plan()?;
        let gated = reorder_inner_joins_shape_gated(original.clone())?;
        assert_eq!(
            format!("{}", original.display_indent()),
            format!("{}", gated.display_indent()),
            "Σ.AL gate must block all reorder under Q21's LeftAnti/LeftSemi"
        );
        Ok(())
    }

    /// Σ.AL — direct unit test for the `reject_under_left_semi_anti`
    /// option. Without the gate, the inner Inner-join chain reorders.
    /// With the gate, it stays put.
    #[tokio::test]
    async fn lever_g_gate_toggle_for_left_semi_subtree() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        // Inner-join 4-leaf chain wrapped in LeftSemi (simulating
        // a NOT EXISTS-decorrelated subquery shape):
        // select s_name from supplier s where exists (
        //   select 1 from orders o, lineitem l, nation n
        //   where o.o_custkey = ... and l.l_orderkey = o.o_orderkey
        //   and l.l_suppkey = s.s_suppkey and n.n_nationkey = s.s_nationkey
        // )
        // Use Q21 since it organically has this shape.
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q21.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skipping: q21.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let original = df.into_optimized_plan()?;

        // With gate OFF: the reorder fires inside the LeftSemi subtree.
        let mut opts_no_gate = ReorderOpts::default();
        opts_no_gate.reject_under_left_semi_anti = false;
        let no_gate = reorder_inner_joins_with_opts(original.clone(), opts_no_gate)?;
        assert_ne!(
            format!("{}", original.display_indent()),
            format!("{}", no_gate.display_indent()),
            "Without Σ.AL gate, reorder should descend into LeftSemi and fire"
        );

        // With gate ON (default): the reorder is blocked.
        let with_gate = reorder_inner_joins_shape_gated(original.clone())?;
        assert_eq!(
            format!("{}", original.display_indent()),
            format!("{}", with_gate.display_indent()),
            "With Σ.AL gate, reorder must NOT fire under LeftSemi"
        );
        Ok(())
    }
}
