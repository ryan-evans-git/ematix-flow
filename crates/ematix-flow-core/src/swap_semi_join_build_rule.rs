//! `SwapSemiJoinBuildSideRule` — `PhysicalOptimizerRule` that fixes the
//! Q18-shape semi-join build-side inversion.
//!
//! ## Why
//!
//! TPC-H Q18 has the canonical "IN (SELECT key FROM table GROUP BY key
//! HAVING agg)" subquery shape:
//!
//! ```sql
//! SELECT … FROM customer, orders, lineitem
//! WHERE o_orderkey IN (
//!     SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300
//! )
//! ```
//!
//! DataFusion's planner lifts the IN-subquery into a `HashJoinExec`
//! with `join_type = LeftSemi`. After `JoinSelection`, the plan is:
//!
//! ```text
//! HashJoinExec LeftSemi
//!   LEFT  (BUILD): customer × orders × lineitem inner-joined → ~60M rows @ SF=10
//!   RIGHT (PROBE): FilterExec → AggregateExec(FinalPartitioned) → … → ~624 rows
//! ```
//!
//! The BUILD side is on the LEFT in DataFusion's HashJoinExec. Building
//! a 60M-row hash table to probe with 624 rows is exactly inverted —
//! we want to build on the small side and probe with the big side.
//!
//! `JoinSelection` *would* swap this via `should_swap_join_order`, but
//! that helper requires `num_rows.get_value()` on BOTH sides.
//! `AggregateExec` returns `Precision::Absent` for `num_rows`, so the
//! swap doesn't fire. The plan ships inverted.
//!
//! ## What this rule does
//!
//! Walks the physical plan. For each `HashJoinExec` whose `join_type`
//! is semi/anti and supports swap, checks whether ONE side contains an
//! `AggregateExec` and the OTHER does not. If the "aggregate-bounded"
//! side is currently the PROBE side, swap so it becomes the BUILD side.
//!
//! - LeftSemi / LeftAnti: BUILD is on LEFT. Swap if RIGHT has the agg.
//! - RightSemi / RightAnti: BUILD is on RIGHT. Swap if LEFT has the agg.
//!
//! `HashJoinExec::swap_inputs` handles join-type flip (LeftSemi ↔
//! RightSemi), input reorder, `on`-key reorder, filter swap, and
//! projection. For semi/anti the swap preserves output column order
//! (DataFusion's own swap handles that explicitly).
//!
//! ## Why this is safe
//!
//! - Semi/anti joins are commutative in the sense that swapping is
//!   semantically a no-op (just changes which input is the hash table
//!   and which is the stream).
//! - The structural signal ("the side with an Aggregate is small") is
//!   conservative — an Aggregate caps output cardinality at the
//!   group-key cardinality. For TPC-H's `o_orderkey` shape that's
//!   1.5M @ SF=10, but the HAVING `sum > 300` filter narrows further
//!   to ~624. Probably small.
//! - In the rare pathological case where the Aggregate side actually
//!   has higher cardinality than the Inner-join side, we'd build a
//!   slightly larger hash but the asymptotic cost of HashJoin is
//!   `O(build + probe)` either way — we just shift between them.
//! - We never swap a `null_aware` join (e.g. NULL-aware LeftAnti has
//!   specific side requirements that JoinSelection also exempts).
//!
//! ## Why a custom rule and not "fix JoinSelection"
//!
//! Patching upstream DataFusion's `should_swap_join_order` to fall
//! back to structural heuristics when stats are absent would be the
//! right long-term fix, but lives outside our control. The local rule
//! is small, well-scoped, and runs as a post-pass.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::JoinType;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::HashJoinExec;

/// See module docs.
#[derive(Debug, Default)]
pub struct SwapSemiJoinBuildSideRule;

impl PhysicalOptimizerRule for SwapSemiJoinBuildSideRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let Some(hj) = p.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(p));
            };
            let jt = *hj.join_type();
            let is_left_semi_like = matches!(jt, JoinType::LeftSemi | JoinType::LeftAnti);
            let is_right_semi_like = matches!(jt, JoinType::RightSemi | JoinType::RightAnti);
            if !is_left_semi_like && !is_right_semi_like {
                return Ok(Transformed::no(p));
            }
            if hj.null_equality() == datafusion::common::NullEquality::NullEqualsNull {
                // Don't touch null-aware joins — JoinSelection exempts them too.
                return Ok(Transformed::no(p));
            }
            if !jt.supports_swap() {
                return Ok(Transformed::no(p));
            }

            let left_has_agg = subtree_has_aggregate(hj.left().as_ref());
            let right_has_agg = subtree_has_aggregate(hj.right().as_ref());

            let want_swap = if is_left_semi_like {
                // BUILD is on LEFT. Swap if RIGHT (probe) is the bounded side.
                !left_has_agg && right_has_agg
            } else {
                // BUILD is on RIGHT. Swap if LEFT (probe) is the bounded side.
                !right_has_agg && left_has_agg
            };

            if !want_swap {
                return Ok(Transformed::no(p));
            }

            let swapped = hj.swap_inputs(hj.partition_mode().clone())?;
            Ok(Transformed::yes(swapped))
        })
        .data()
    }

    fn name(&self) -> &str {
        "swap_semi_join_build_side"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Returns true if `plan` or any descendant is an `AggregateExec`,
/// stopping the descent at `HashJoinExec` / nested-loop boundaries so
/// we only see aggregates "above" the next join layer (those are the
/// ones that bound this join's input cardinality).
fn subtree_has_aggregate(plan: &dyn ExecutionPlan) -> bool {
    if plan.as_any().is::<AggregateExec>() {
        return true;
    }
    for child in plan.children() {
        let cany = child.as_any();
        if cany.is::<HashJoinExec>() {
            // Don't recurse past a join — aggregates below another join
            // bound that join's output, not this join's input.
            continue;
        }
        if subtree_has_aggregate(child.as_ref()) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::joins::PartitionMode;

    fn mem_table(name: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    #[test]
    fn left_semi_with_right_agg_swaps_to_right_semi() {
        let left = mem_table("a");
        let right_scan = mem_table("a");
        // wrap right side in an AggregateExec to simulate the
        // IN-subquery shape.
        let gb = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            "a".to_string(),
        )]);
        let agg = Arc::new(
            AggregateExec::try_new(
                AggregateMode::FinalPartitioned,
                gb,
                vec![],
                vec![],
                right_scan.clone(),
                right_scan.schema(),
            )
            .unwrap(),
        );

        let on = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let hj = Arc::new(
            HashJoinExec::try_new(
                left,
                agg,
                on,
                None,
                &JoinType::LeftSemi,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );

        let rule = SwapSemiJoinBuildSideRule;
        let out = rule.optimize(hj, &ConfigOptions::default()).unwrap();
        let out_hj = out.as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(*out_hj.join_type(), JoinType::RightSemi);
    }

    #[test]
    fn left_semi_without_right_agg_does_not_swap() {
        let left = mem_table("a");
        let right = mem_table("a");

        let on = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let hj = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::LeftSemi,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );

        let rule = SwapSemiJoinBuildSideRule;
        let out = rule.optimize(hj, &ConfigOptions::default()).unwrap();
        let out_hj = out.as_any().downcast_ref::<HashJoinExec>().unwrap();
        // No swap — still LeftSemi.
        assert_eq!(*out_hj.join_type(), JoinType::LeftSemi);
    }

    #[test]
    fn inner_join_is_untouched() {
        let left = mem_table("a");
        let right_scan = mem_table("a");
        let gb = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            "a".to_string(),
        )]);
        let agg = Arc::new(
            AggregateExec::try_new(
                AggregateMode::FinalPartitioned,
                gb,
                vec![],
                vec![],
                right_scan.clone(),
                right_scan.schema(),
            )
            .unwrap(),
        );
        let on = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let hj = Arc::new(
            HashJoinExec::try_new(
                left,
                agg,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );
        let rule = SwapSemiJoinBuildSideRule;
        let out = rule.optimize(hj, &ConfigOptions::default()).unwrap();
        let out_hj = out.as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(*out_hj.join_type(), JoinType::Inner);
    }
}
