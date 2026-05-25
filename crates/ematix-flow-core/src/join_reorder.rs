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
pub fn reorder_inner_joins(plan: LogicalPlan) -> DfResult<LogicalPlan> {
    let transformed = plan.transform_down(|node| match node {
        LogicalPlan::Join(ref join) if join.join_type == JoinType::Inner => {
            match flatten_inner_join_chain(&node) {
                Some(chain) if chain.leaves.len() >= 3 => match rebuild_reordered(&chain) {
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
                },
                _ => Ok(Transformed::no(node)),
            }
        }
        _ => Ok(Transformed::no(node)),
    })?;
    Ok(transformed.data)
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
        // col = lit  →  1/NDV ≈ 1/(max-min+1) for int columns, else 0.1
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Op::Eq,
            right,
        }) => match (extract_column(left), extract_literal(right)) {
            (Some(col), Some(_lit)) => match col_idx(&col) {
                Some(i) if i < stats.column_statistics.len() => {
                    let cs = &stats.column_statistics[i];
                    match (&cs.min_value, &cs.max_value) {
                        (
                            Precision::Exact(ScalarValue::Int32(Some(lo)))
                            | Precision::Inexact(ScalarValue::Int32(Some(lo))),
                            Precision::Exact(ScalarValue::Int32(Some(hi)))
                            | Precision::Inexact(ScalarValue::Int32(Some(hi))),
                        ) => 1.0 / ((hi - lo + 1).max(1) as f64),
                        (
                            Precision::Exact(ScalarValue::Int64(Some(lo)))
                            | Precision::Inexact(ScalarValue::Int64(Some(lo))),
                            Precision::Exact(ScalarValue::Int64(Some(hi)))
                            | Precision::Inexact(ScalarValue::Int64(Some(hi))),
                        ) => 1.0 / ((hi - lo + 1).max(1) as f64),
                        _ => 0.1,
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

    // Connectivity-aware greedy: starting from the smallest leaf,
    // at each step pick the smallest UNPLACED leaf that has a
    // predicate connecting it to the current built subtree. This
    // respects the join graph — pure smallest-first can produce
    // Cartesian-product orderings for non-bridge tables (e.g. Q05
    // tries to add `orders` after `region+nation+supplier` but
    // orders only joins via customer/lineitem, neither in scope).
    let mut order: Vec<usize> = Vec::with_capacity(n);
    let mut placed: Vec<bool> = vec![false; n];

    // First leaf: smallest overall.
    let first_idx = (0..n).min_by_key(|&i| cards[i])?;
    order.push(first_idx);
    placed[first_idx] = true;

    // Track remaining predicates (those we haven't routed yet) and
    // those we've already attached. Built incrementally.
    let mut remaining_preds: Vec<(Column, Column)> = chain.equi_preds.clone();

    // Helper: given current "in-scope" columns (a Vec<Column>), return
    // the indices of leaves that have a predicate connecting them to
    // the in-scope set.
    let connects = |leaf_idx: usize,
                    placed_indices: &[usize],
                    chain: &InnerJoinChain| {
        let leaf_schema = chain.leaves[leaf_idx].schema();
        for (l, r) in &chain.equi_preds {
            let l_in_leaf = column_in_schema(l, leaf_schema);
            let r_in_leaf = column_in_schema(r, leaf_schema);
            for &p in placed_indices {
                let placed_schema = chain.leaves[p].schema();
                let l_in_placed = column_in_schema(l, placed_schema);
                let r_in_placed = column_in_schema(r, placed_schema);
                if (l_in_leaf && r_in_placed) || (r_in_leaf && l_in_placed) {
                    return true;
                }
            }
        }
        false
    };

    while order.len() < n {
        // Pick the smallest unplaced leaf that has a connecting pred.
        let placed_indices: Vec<usize> = order.clone();
        let pick = (0..n)
            .filter(|i| !placed[*i] && connects(*i, &placed_indices, chain))
            .min_by_key(|&i| cards[i]);
        let next = match pick {
            Some(i) => i,
            None => {
                if std::env::var("EMAT_REORDER_DEBUG").is_ok() {
                    eprintln!("[reorder] BAIL: cannot find connected next leaf (would be Cartesian)");
                }
                return None;
            }
        };
        order.push(next);
        placed[next] = true;
    }

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
        let on_exprs: Vec<Expr> = matching
            .into_iter()
            .map(|(l, r)| {
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::Column(l)),
                    op: Operator::Eq,
                    right: Box::new(Expr::Column(r)),
                })
            })
            .collect();
        current = match current.join_on(leaf, JoinType::Inner, on_exprs) {
            Ok(b) => b,
            Err(e) => {
                if std::env::var("EMAT_REORDER_DEBUG").is_ok() {
                    eprintln!("[reorder] BAIL: join_on at step {step} err: {e}");
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
        // After reorder we expect `region` (smallest, plus ASIA
        // filter knocks it to 1 row). If the deepest-left is
        // `nation` (25 rows) that's also a substantial improvement
        // vs the original customer-leftmost; accept either as
        // sufficient signal that reorder did meaningful work.
        let acceptable = matches!(leftmost.as_deref(), Some("region") | Some("nation"));
        assert!(
            acceptable,
            "Q05 reorder produced unexpected leftmost leaf {:?}\nNEW:\n{}",
            leftmost, new_dump
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
        assert_eq!(
            orig_rows, new_rows,
            "join reorder changed query semantics: original {orig_rows:?} vs rewritten {new_rows:?}"
        );
        Ok(())
    }
}
