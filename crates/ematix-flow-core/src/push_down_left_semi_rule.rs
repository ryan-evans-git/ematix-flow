//! Σ.Q.L10 — push `LeftSemi Join` past inner joins down to its target table.
//!
//! ## Why
//!
//! TPC-H Q18 shape:
//!
//! ```sql
//! SELECT ...
//! FROM customer, orders, lineitem
//! WHERE
//!   o_orderkey IN (SELECT l_orderkey FROM lineitem
//!                  GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
//!   AND c_custkey = o_custkey
//!   AND o_orderkey = l_orderkey
//! ...
//! ```
//!
//! DataFusion's `decorrelate_predicate_subquery` turns the
//! `o_orderkey IN (...)` into a `LeftSemi Join` placed at the TOP of
//! the join tree:
//!
//! ```text
//! LeftSemi Join: orders.o_orderkey = sq.l_orderkey
//!   Inner Join: orders.o_orderkey = lineitem.l_orderkey      ← 60M rows
//!     Inner Join: customer.c_custkey = orders.o_custkey       ← 15M rows
//!       TableScan: customer
//!       TableScan: orders                                     ← 15M rows
//!     TableScan: lineitem                                     ← 60M rows
//!   SubqueryAlias: __correlated_sq_1
//!     ... (624 surviving keys)
//! ```
//!
//! The semi-condition only references `orders.o_orderkey`. If we push
//! the LeftSemi to sit DIRECTLY above the orders TableScan, orders is
//! filtered to ~3M rows BEFORE any other join runs:
//!
//! ```text
//! Inner Join: orders.o_orderkey = lineitem.l_orderkey         ← 12M rows
//!   Inner Join: customer.c_custkey = orders.o_custkey         ← 3M rows
//!     TableScan: customer
//!     LeftSemi Join: orders.o_orderkey = sq.l_orderkey        ← pushed here
//!       TableScan: orders
//!       SubqueryAlias: __correlated_sq_1
//!         ...
//!   TableScan: lineitem
//! ```
//!
//! That's the structural change DuckDB makes for Q18; it's the
//! difference between materialising a 60 M-row × 1617 GB intermediate
//! and 12 M rows. See [[project_q18_sf10_duckdb_plan_diff]].
//!
//! ## Correctness
//!
//! `LeftSemi Join(left, right, condition)` outputs a subset of `left`'s
//! rows (those that have a matching row in `right`). Its output schema
//! equals `left`'s schema. Therefore replacing a `TableScan: T` with
//! `LeftSemi Join(TableScan: T, sq, cond)` is type-preserving — every
//! upstream operator continues to see the same columns. Only the row
//! count shrinks.
//!
//! Precondition: every join key on the LEFT side of `semi.on` must
//! resolve to columns of a SINGLE target table T, and the LeftSemi's
//! RIGHT subtree must be independent of the rest of the LEFT tree
//! (i.e., no correlated columns escape the subquery). DataFusion's
//! `__correlated_sq_*` aliases produced by the predicate-subquery
//! decorrelator always satisfy this invariant.
//!
//! ## Opt-in
//!
//! Like every other Σ.Q rule per [[optimizer-codegen-sensitivity]] —
//! ships opt-in via [`install_push_down_left_semi_rule`]. Tests +
//! triangulation bench enable it via `EMAT_PUSH_SEMI=1`.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::expr::Expr;
use datafusion::logical_expr::{Join, JoinType, LogicalPlan, TableScan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// Install the Σ.Q.L10 rule.
pub fn install_push_down_left_semi_rule(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.with_optimizer_rule(Arc::new(PushDownLeftSemiRule))
}

#[derive(Debug, Default)]
pub struct PushDownLeftSemiRule;

impl OptimizerRule for PushDownLeftSemiRule {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "ematix_flow_push_down_left_semi"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DfResult<Transformed<LogicalPlan>> {
        // Walk every node in the tree. transform_up handles recursion,
        // mutating bottom-up; each visit can return Transformed::yes to
        // replace the current node.
        plan.transform_up(try_push_down)
    }
}

/// Try to push the LeftSemi/LeftAnti down through its LEFT subtree.
/// Returns `Transformed::yes(new_plan)` if successful, otherwise
/// `Transformed::no(node)`.
///
/// The pushdown is identical for both join types: each preserves the
/// left input's schema and outputs a subset of left rows. LeftSemi
/// keeps rows that DO have a right match (IN / EXISTS); LeftAnti
/// keeps rows that DO NOT (NOT IN / NOT EXISTS). The structural
/// transformation is the same — we just need to preserve the
/// `join_type` when re-wrapping the target TableScan.
fn try_push_down(node: LogicalPlan) -> DfResult<Transformed<LogicalPlan>> {
    let LogicalPlan::Join(Join {
        left,
        right,
        on,
        filter,
        join_type,
        join_constraint,
        schema,
        null_equality,
        null_aware,
    }) = node
    else {
        return Ok(Transformed::no(node));
    };
    if !matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
        // Reconstruct and bail.
        let restored = LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint,
            schema,
            null_equality,
            null_aware,
        });
        return Ok(Transformed::no(restored));
    }

    // The "filter" expression on the Join (post-condition) — if present
    // it's not pushable without further analysis; leave alone.
    if filter.is_some() {
        let restored = LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint,
            schema,
            null_equality,
            null_aware,
        });
        return Ok(Transformed::no(restored));
    }

    // Resolve target table: every LEFT-side equi-key must reference
    // exactly one base relation, and they must all agree on which one.
    let Some(target_qualifier) = resolve_single_target_table(&on, left.as_ref()) else {
        let restored = LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint,
            schema,
            null_equality,
            null_aware,
        });
        return Ok(Transformed::no(restored));
    };

    // Walk LEFT subtree, rewriting the TableScan that matches
    // `target_qualifier` into a LeftSemi(scan, right, on). If we find
    // it, the OUTER LeftSemi node disappears (its job is now done at
    // the inner position). Schema is identical to the LEFT input
    // because LeftSemi preserves left schema.
    let outer_on = on.clone();
    let right_arc = Arc::clone(&right);
    let outer_null_equality = null_equality;
    let outer_join_constraint = join_constraint;
    let outer_null_aware = null_aware;

    let pushed = push_through_subtree(
        Arc::unwrap_or_clone(left.clone()),
        &target_qualifier,
        right_arc,
        outer_on,
        outer_join_constraint,
        outer_null_equality,
        outer_null_aware,
        join_type,
    )?;

    match pushed {
        PushResult::Pushed(new_plan) => Ok(Transformed::yes(new_plan)),
        PushResult::NotApplicable(reverted_left) => {
            // Restore the unmodified semi/anti join.
            let restored = LogicalPlan::Join(Join {
                left: Arc::new(reverted_left),
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equality,
                null_aware,
            });
            Ok(Transformed::no(restored))
        }
    }
}

enum PushResult {
    /// Successfully pushed; the new subtree replaces the original
    /// LeftSemi node.
    Pushed(LogicalPlan),
    /// Couldn't find the target table; subtree is returned unchanged
    /// so the caller can rebuild the original LeftSemi node.
    NotApplicable(LogicalPlan),
}

/// Walk `plan` looking for the `TableScan(target)` and replace it with
/// `LeftSemi(TableScan(target), right, on)`. Recurses through joins
/// and other 1-or-2-child plan nodes that preserve column identity.
#[allow(clippy::too_many_arguments)]
fn push_through_subtree(
    plan: LogicalPlan,
    target: &TableQualifier,
    right: Arc<LogicalPlan>,
    on: Vec<(Expr, Expr)>,
    join_constraint: datafusion::logical_expr::JoinConstraint,
    null_equality: datafusion::common::NullEquality,
    null_aware: bool,
    join_type: JoinType,
) -> DfResult<PushResult> {
    // Direct hit: this node IS the target TableScan.
    if let LogicalPlan::TableScan(ref scan) = plan {
        if matches_qualifier(scan, target) {
            return wrap_with_semi(
                plan,
                right,
                on,
                join_constraint,
                null_equality,
                null_aware,
                join_type,
            );
        }
        // A different TableScan; not the target.
        return Ok(PushResult::NotApplicable(plan));
    }

    // Projection / SubqueryAlias / Filter / etc — 1-child wrappers. We
    // descend if the target column survives the projection.
    if let LogicalPlan::Projection(ref proj) = plan {
        // Only descend if every left-side semi-key column survives this
        // projection's output. (For Q18 the top-level Projection above
        // the inner-join chain just selects from below; the
        // o_orderkey column survives.)
        let proj_input = proj.input.as_ref().clone();
        let projection_expr = proj.expr.clone();
        let projection_schema = proj.schema.clone();
        match push_through_subtree(
            proj_input,
            target,
            right,
            on,
            join_constraint,
            null_equality,
            null_aware,
            join_type,
        )? {
            PushResult::Pushed(new_input) => {
                // Rebuild the projection with the new input.
                let new_proj = datafusion::logical_expr::Projection::try_new_with_schema(
                    projection_expr,
                    Arc::new(new_input),
                    projection_schema,
                )?;
                return Ok(PushResult::Pushed(LogicalPlan::Projection(new_proj)));
            }
            PushResult::NotApplicable(orig_input) => {
                let restored = datafusion::logical_expr::Projection::try_new_with_schema(
                    projection_expr,
                    Arc::new(orig_input),
                    projection_schema,
                )?;
                return Ok(PushResult::NotApplicable(LogicalPlan::Projection(restored)));
            }
        }
    }

    // Σ.Q.L10 — DO NOT descend through Filter.
    //
    // DataFusion's `PushDownFilter` rule has already pushed Filters
    // down to be adjacent to the scan (in `Filter → TableScan`
    // shape). The Filter usually has higher selectivity than the
    // pending LeftSemi/LeftAnti (a date range narrows by ~5%; a
    // semi-join on PK keys can narrow only moderately). Pushing the
    // semi-join BELOW the filter forces the semi-join to process the
    // un-filtered scan output — which on Q4 SF=10 is the difference
    // between 600K post-filter orders and 15M unfiltered orders
    // (+170% wall-time regression in the first L10 bench).
    //
    // Bailing here also makes the rule a no-op on Q4 (and similar
    // shapes where there's no inner join chain to traverse), so the
    // outer LeftSemi/LeftAnti stays in its original position
    // immediately above the Projection that wraps the filtered scan.
    if matches!(plan, LogicalPlan::Filter(_)) {
        return Ok(PushResult::NotApplicable(plan));
    }

    // Inner Join — descend into whichever child contains the target.
    // Don't descend into BOTH children: the LeftSemi can only land on
    // one of them. If the target is in the LEFT child, push there. If
    // in RIGHT child, push there. Inner Joins are symmetric so either
    // is correct.
    if let LogicalPlan::Join(join) = &plan {
        if matches!(join.join_type, JoinType::Inner) {
            let left_has = subtree_contains_target(join.left.as_ref(), target);
            let right_has = subtree_contains_target(join.right.as_ref(), target);

            // Clone parts we'll need either way.
            let join_left = join.left.clone();
            let join_right = join.right.clone();
            let join_on = join.on.clone();
            let join_filter = join.filter.clone();
            let join_jt = join.join_type;
            let join_jc = join.join_constraint;
            let join_schema = join.schema.clone();
            let join_ne = join.null_equality;
            let join_na = join.null_aware;

            if left_has && !right_has {
                let left_inner = Arc::unwrap_or_clone(join_left);
                match push_through_subtree(
                    left_inner,
                    target,
                    right,
                    on,
                    join_constraint,
                    null_equality,
                    null_aware,
                    join_type,
                )? {
                    PushResult::Pushed(new_left) => {
                        let new_join = LogicalPlan::Join(Join {
                            left: Arc::new(new_left),
                            right: join_right,
                            on: join_on,
                            filter: join_filter,
                            join_type: join_jt,
                            join_constraint: join_jc,
                            schema: join_schema,
                            null_equality: join_ne,
                            null_aware: join_na,
                        });
                        return Ok(PushResult::Pushed(new_join));
                    }
                    PushResult::NotApplicable(reverted) => {
                        let restored = LogicalPlan::Join(Join {
                            left: Arc::new(reverted),
                            right: join_right,
                            on: join_on,
                            filter: join_filter,
                            join_type: join_jt,
                            join_constraint: join_jc,
                            schema: join_schema,
                            null_equality: join_ne,
                            null_aware: join_na,
                        });
                        return Ok(PushResult::NotApplicable(restored));
                    }
                }
            } else if right_has && !left_has {
                let right_inner = Arc::unwrap_or_clone(join_right);
                match push_through_subtree(
                    right_inner,
                    target,
                    right,
                    on,
                    join_constraint,
                    null_equality,
                    null_aware,
                    join_type,
                )? {
                    PushResult::Pushed(new_right) => {
                        let new_join = LogicalPlan::Join(Join {
                            left: join_left,
                            right: Arc::new(new_right),
                            on: join_on,
                            filter: join_filter,
                            join_type: join_jt,
                            join_constraint: join_jc,
                            schema: join_schema,
                            null_equality: join_ne,
                            null_aware: join_na,
                        });
                        return Ok(PushResult::Pushed(new_join));
                    }
                    PushResult::NotApplicable(reverted) => {
                        let restored = LogicalPlan::Join(Join {
                            left: join_left,
                            right: Arc::new(reverted),
                            on: join_on,
                            filter: join_filter,
                            join_type: join_jt,
                            join_constraint: join_jc,
                            schema: join_schema,
                            null_equality: join_ne,
                            null_aware: join_na,
                        });
                        return Ok(PushResult::NotApplicable(restored));
                    }
                }
            }
            // Both sides have it (self-join shape) or neither —
            // ambiguous, leave alone.
        }
    }

    Ok(PushResult::NotApplicable(plan))
}

/// Wrap `target_scan` with a LeftSemi or LeftAnti against `right`.
/// `join_type` must be `JoinType::LeftSemi` or `JoinType::LeftAnti` —
/// both preserve the left input's schema, so we just thread the
/// caller's choice through.
fn wrap_with_semi(
    target_scan: LogicalPlan,
    right: Arc<LogicalPlan>,
    on: Vec<(Expr, Expr)>,
    join_constraint: datafusion::logical_expr::JoinConstraint,
    null_equality: datafusion::common::NullEquality,
    null_aware: bool,
    join_type: JoinType,
) -> DfResult<PushResult> {
    debug_assert!(
        matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti),
        "wrap_with_semi called with non-semi/anti join_type {join_type:?}"
    );
    let scan_schema = target_scan.schema().clone();
    let new_join = Join {
        left: Arc::new(target_scan),
        right,
        on,
        filter: None,
        join_type,
        join_constraint,
        // LeftSemi/LeftAnti output schema = left's schema.
        schema: scan_schema,
        null_equality,
        null_aware,
    };
    Ok(PushResult::Pushed(LogicalPlan::Join(new_join)))
}

/// Identifier for a base relation referenced from the plan tree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableQualifier {
    /// `table_name` as it appears in `TableScan::table_name`. The
    /// optional schema/catalog parts are folded into the OwnedTableReference
    /// debug repr; comparing by string is enough for TPC-H.
    name: String,
}

/// Determine whether every LEFT-side equi-key in `on` resolves to the
/// same base table, and return that table's qualifier. Returns None on
/// no equi-keys, multi-table keys, or expressions that aren't simple
/// column references.
fn resolve_single_target_table(
    on: &[(Expr, Expr)],
    left_subtree: &LogicalPlan,
) -> Option<TableQualifier> {
    if on.is_empty() {
        return None;
    }
    let mut candidates: HashSet<TableQualifier> = HashSet::new();
    for (left_expr, _right_expr) in on {
        let col = match left_expr {
            Expr::Column(c) => c,
            _ => return None,
        };
        // Find the TableScan in left_subtree whose schema has this
        // column (matched by qualifier + name).
        let qualifier = find_qualifier_for_column(left_subtree, col)?;
        candidates.insert(qualifier);
    }
    if candidates.len() == 1 {
        candidates.into_iter().next()
    } else {
        None
    }
}

fn find_qualifier_for_column(
    plan: &LogicalPlan,
    col: &datafusion::common::Column,
) -> Option<TableQualifier> {
    // Prefer the column's relation qualifier if present.
    if let Some(rel) = &col.relation {
        return Some(TableQualifier {
            name: rel.to_string(),
        });
    }
    // Otherwise scan the plan tree for a TableScan whose schema
    // contains `col.name`.
    let mut found: Option<TableQualifier> = None;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            for f in scan.projected_schema.fields() {
                if f.name() == &col.name {
                    found = Some(TableQualifier {
                        name: scan.table_name.to_string(),
                    });
                    return Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    found
}

fn matches_qualifier(scan: &TableScan, target: &TableQualifier) -> bool {
    scan.table_name.to_string() == target.name
}

fn subtree_contains_target(plan: &LogicalPlan, target: &TableQualifier) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            if matches_qualifier(scan, target) {
                found = true;
                return Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop);
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::tree_node::TreeNode;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "push_down_left_semi_{}_{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    fn write_customer(path: &std::path::Path) {
        let c_custkey: Vec<i64> = (0..100i64).collect();
        write_table_to_path(
            path,
            &[("c_custkey", ColumnData::I64(&c_custkey))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    fn write_orders(path: &std::path::Path) {
        // 500 orders; o_orderkey ∈ [0,500), o_custkey ∈ [0,100).
        let o_orderkey: Vec<i64> = (0..500i64).collect();
        let o_custkey: Vec<i64> = (0..500i64).map(|i| i % 100).collect();
        write_table_to_path(
            path,
            &[
                ("o_orderkey", ColumnData::I64(&o_orderkey)),
                ("o_custkey", ColumnData::I64(&o_custkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    fn write_filter_set(path: &std::path::Path) {
        // Whitelist of o_orderkey values to keep — Q18's "624 surviving keys" shape.
        let target_orderkey: Vec<i64> = vec![3, 7, 11];
        write_table_to_path(
            path,
            &[("target_orderkey", ColumnData::I64(&target_orderkey))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    fn count_semi_above_scan(plan: &LogicalPlan, table: &str) -> usize {
        count_join_above_scan(plan, table, JoinType::LeftSemi)
    }

    fn count_join_above_scan(plan: &LogicalPlan, table: &str, jt: JoinType) -> usize {
        // Counts joins of the given type whose LEFT input is the
        // TableScan of `table`. Structural signature of
        // "semi/anti-join pushed to the scan."
        let mut count = 0;
        let _ = plan.apply(|node| {
            if let LogicalPlan::Join(j) = node {
                if j.join_type == jt {
                    if let LogicalPlan::TableScan(scan) = j.left.as_ref() {
                        if scan.table_name.table() == table {
                            count += 1;
                        }
                    }
                }
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        });
        count
    }

    #[tokio::test]
    async fn pushes_semi_join_down_to_orders_scan() {
        let cu = tmp_parquet("customer");
        let or = tmp_parquet("orders");
        let fs = tmp_parquet("filter_set");
        write_customer(&cu);
        write_orders(&or);
        write_filter_set(&fs);

        let cfg = SessionConfig::new();
        let state = install_push_down_left_semi_rule(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(cfg),
        )
        .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_parquet(
            "customer",
            cu.to_string_lossy().as_ref(),
            Default::default(),
        )
        .await
        .unwrap();
        ctx.register_parquet("orders", or.to_string_lossy().as_ref(), Default::default())
            .await
            .unwrap();
        ctx.register_parquet(
            "filter_set",
            fs.to_string_lossy().as_ref(),
            Default::default(),
        )
        .await
        .unwrap();

        // Q18-shape miniature: customer × orders with a semi-filter on
        // orders.o_orderkey.
        let sql = "SELECT customer.c_custkey, orders.o_orderkey \
                   FROM customer, orders \
                   WHERE customer.c_custkey = orders.o_custkey \
                     AND orders.o_orderkey IN (SELECT target_orderkey FROM filter_set)";

        let df = ctx.sql(sql).await.unwrap();
        let logical_plan = df.logical_plan().clone();

        // Trigger optimization — calling create_physical_plan does it.
        let _physical = df.clone().create_physical_plan().await.unwrap();
        let optimized = ctx.state().optimize(&logical_plan).unwrap();

        // The optimized plan must have a LeftSemi sitting directly
        // above the orders TableScan.
        let pushed_count = count_semi_above_scan(&optimized, "orders");
        assert!(
            pushed_count >= 1,
            "expected ≥1 LeftSemi above orders TableScan in optimized plan — got 0.\n\
             Plan:\n{optimized:?}"
        );

        // Output correctness: rows where orderkey ∈ {3, 7, 11}.
        // For each surviving order, one row × matching customer = 1 row.
        // orderkey 3 → custkey 3, orderkey 7 → custkey 7, orderkey 11 → custkey 11.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(row_count, 3, "expected 3 surviving orders, got {row_count}");
    }

    /// Σ.Q.L10 LeftAnti extension — `NOT IN`/`NOT EXISTS` shapes
    /// decorrelate to LeftAnti. The pushdown is identical structurally
    /// (preserve left schema, output subset) — we just need to keep
    /// the join_type intact when re-wrapping the target TableScan.
    #[tokio::test]
    async fn pushes_anti_join_down_to_orders_scan() {
        let cu = tmp_parquet("customer_anti");
        let or = tmp_parquet("orders_anti");
        let fs = tmp_parquet("filter_set_anti");
        write_customer(&cu);
        write_orders(&or);
        write_filter_set(&fs);

        let cfg = SessionConfig::new();
        let state = install_push_down_left_semi_rule(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(cfg),
        )
        .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_parquet(
            "customer",
            cu.to_string_lossy().as_ref(),
            Default::default(),
        )
        .await
        .unwrap();
        ctx.register_parquet("orders", or.to_string_lossy().as_ref(), Default::default())
            .await
            .unwrap();
        ctx.register_parquet(
            "filter_set",
            fs.to_string_lossy().as_ref(),
            Default::default(),
        )
        .await
        .unwrap();

        // Q21/Q22-shape miniature: customer × orders with a NOT IN
        // anti-filter on orders.o_orderkey. Excludes orderkey ∈ {3,7,11}.
        let sql = "SELECT customer.c_custkey, orders.o_orderkey \
                   FROM customer, orders \
                   WHERE customer.c_custkey = orders.o_custkey \
                     AND orders.o_orderkey NOT IN (SELECT target_orderkey FROM filter_set)";

        let df = ctx.sql(sql).await.unwrap();
        let logical_plan = df.logical_plan().clone();
        let _physical = df.clone().create_physical_plan().await.unwrap();
        let optimized = ctx.state().optimize(&logical_plan).unwrap();

        // The optimized plan must have a LeftAnti sitting directly
        // above the orders TableScan.
        let pushed_count = count_join_above_scan(&optimized, "orders", JoinType::LeftAnti);
        assert!(
            pushed_count >= 1,
            "expected ≥1 LeftAnti above orders TableScan in optimized plan — got 0.\n\
             Plan:\n{optimized:?}"
        );

        // Output correctness: orders ∈ [0,500), exclude {3,7,11} → 497 orders.
        // Each joins to one customer via o_custkey = c_custkey.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            row_count, 497,
            "expected 497 surviving orders after NOT IN, got {row_count}"
        );
    }
}
