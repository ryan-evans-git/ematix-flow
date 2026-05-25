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

use datafusion::common::JoinType;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
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

            // Σ.Q.L2 (2026-05-23): partition-mode-aware gate.
            //
            // DataFusion's HashJoinExec asserts two invariants in
            // execute():
            //   * mode != CollectLeft   || left_partitions == 1
            //   * mode != Partitioned   || left_partitions == right_partitions
            //
            // The original L2 rule passed `hj.partition_mode().clone()`
            // unchanged to `swap_inputs`. That works for joins where
            // both sides are already Partitioned (the Q18-shape this
            // rule was designed for) but explodes on `CollectLeft`
            // joins whose old RIGHT — the new LEFT after swap — has
            // >1 partition (Q20 shape: the agg-bounded side is itself
            // the output of a 14-partition Inner subquery).
            //
            // The non-trivial question is what to do in that case.
            // Two options were considered:
            //
            //   (a) Upgrade mode to `Partitioned` and synthesize
            //       `RepartitionExec(Hash([key], N))` wrappers on both
            //       new children. This is "make the swap work
            //       everywhere" but it duplicates JoinSelection's
            //       partition-planning work, and crucially: if the
            //       original `CollectLeft` was chosen because
            //       JoinSelection had stats showing the original LEFT
            //       was small, then forcing Partitioned mode after the
            //       swap produces a strictly-worse plan (now both
            //       sides pay hash-repartition cost, and the
            //       agg-bounded subtree we swapped onto LEFT may not
            //       actually be smaller than the original LEFT was).
            //
            //   (b) Refuse the swap when CollectLeft can't be
            //       preserved. This honours JoinSelection's
            //       partition-mode choice — JoinSelection knows which
            //       side is bounded based on real plan stats; our
            //       "agg means bounded" heuristic was only intended to
            //       fill the gap when JoinSelection's stats were
            //       absent (the case it picks `Auto` / arbitrary).
            //       Keeping CollectLeft means the swap is plan-hygiene
            //       only when JoinSelection already validated the left
            //       side as small.
            //
            // We go with (b). The rule fires safely on Q18 (its
            // designed target — Partitioned mode, swap preserves
            // shape) and stays quiet on Q20 (CollectLeft chosen for a
            // reason — original LEFT IS small after the nation filter;
            // swapping would produce a regression even if it didn't
            // crash). L2 has no recorded TPC-H wins yet (per Σ.Q.L2
            // bench notes), so this conservative gate doesn't sacrifice
            // any measured perf.
            use datafusion::physical_plan::ExecutionPlanProperties;
            use datafusion::physical_plan::joins::PartitionMode;
            match hj.partition_mode() {
                PartitionMode::CollectLeft => {
                    // After swap, new LEFT = old RIGHT.
                    let new_left_partitions = hj.right().output_partitioning().partition_count();
                    if new_left_partitions != 1 {
                        return Ok(Transformed::no(p));
                    }
                }
                PartitionMode::Partitioned => {
                    // For Partitioned joins, the assertion is
                    // `left_partitions == right_partitions`. The swap
                    // swaps inputs symmetrically, so if both children
                    // already match (which any well-formed Partitioned
                    // plan does), the swap preserves that property.
                    let lp = hj.left().output_partitioning().partition_count();
                    let rp = hj.right().output_partitioning().partition_count();
                    if lp != rp {
                        // Already-malformed plan — leave it alone.
                        return Ok(Transformed::no(p));
                    }
                }
                _ => {
                    // PartitionMode::Auto and any future variants —
                    // play it safe.
                    return Ok(Transformed::no(p));
                }
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
    use datafusion::physical_plan::ExecutionPlanProperties;
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

    /// Σ.Q.L2 fix regression test (Q20 shape, 2026-05-23): a
    /// `CollectLeft` `LeftSemi` join whose RIGHT side has >1
    /// partition must NOT be swapped — the swap would put a
    /// multi-partition subtree on the new LEFT and violate
    /// HashJoinExec's `mode != CollectLeft || left_partitions == 1`
    /// assertion.
    #[test]
    fn collect_left_with_multi_partition_right_does_not_swap() {
        use datafusion::arrow::array::Int64Array;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        // Single-partition LEFT (typical CollectLeft shape).
        let left: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
            &[vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
                )
                .unwrap(),
            ]],
            schema.clone(),
            None,
        )
        .unwrap();
        // Multi-partition RIGHT (Q20-shape: would become new LEFT
        // post-swap and violate the CollectLeft invariant).
        let right_scan: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
            &[
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
                    )
                    .unwrap(),
                ],
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![3i64, 4]))],
                    )
                    .unwrap(),
                ],
            ],
            schema.clone(),
            None,
        )
        .unwrap();
        assert!(right_scan.output_partitioning().partition_count() >= 2);

        // Wrap RIGHT in an AggregateExec to trigger L2's "swap-if-
        // agg-side" condition.
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
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );

        let rule = SwapSemiJoinBuildSideRule;
        let out = rule.optimize(hj, &ConfigOptions::default()).unwrap();
        let out_hj = out.as_any().downcast_ref::<HashJoinExec>().unwrap();
        // Original shape preserved — rule refused the unsafe swap.
        assert_eq!(*out_hj.join_type(), JoinType::LeftSemi);
        assert!(matches!(
            out_hj.partition_mode(),
            PartitionMode::CollectLeft
        ));
    }

    /// Σ.Q.L2 fix regression test (Q18-shape stays winning,
    /// 2026-05-23): a `Partitioned` `LeftSemi` join with matching
    /// partition counts on both sides MUST still swap. The new gate
    /// shouldn't block the L2 lever's designed target.
    #[test]
    fn partitioned_mode_with_matching_partition_counts_still_swaps() {
        use datafusion::arrow::array::Int64Array;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let two_part = || -> Arc<dyn ExecutionPlan> {
            MemorySourceConfig::try_new_exec(
                &[
                    vec![
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
                        )
                        .unwrap(),
                    ],
                    vec![
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(Int64Array::from(vec![3i64, 4]))],
                        )
                        .unwrap(),
                    ],
                ],
                schema.clone(),
                None,
            )
            .unwrap()
        };
        let left = two_part();
        let right_scan = two_part();
        assert_eq!(
            left.output_partitioning().partition_count(),
            right_scan.output_partitioning().partition_count()
        );

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
        // Swap fired — L2 still works on its designed target shape.
        assert_eq!(*out_hj.join_type(), JoinType::RightSemi);
    }
}
