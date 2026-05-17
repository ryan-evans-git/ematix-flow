//! Σ.G.2d: `EnableFusedAggregateExecRule` — `PhysicalOptimizerRule`
//! that swaps hand-coded `FusedFilterSumExec` (Q6) / `FusedFilterMultiAggExec`
//! (Q1) instances for the generic `FusedAggregateExec<S>` parameterised
//! by the matching [`AggregateSpec`].
//!
//! ## Scope
//!
//! `transform_up` over the physical plan tree. For each non-JIT
//! `FusedFilterSumExec` / `FusedFilterMultiAggExec` we find, construct
//! the equivalent `FusedAggregateExec<Q6Spec>` / `FusedAggregateExec<Q1Spec>`
//! over the same child plan and predicate. JIT instances pass through
//! unchanged (Σ.G.2 doesn't subsume JIT — that's a follow-up).
//!
//! ## Why this exists
//!
//! The Σ.G.2 work introduced an `AggregateSpec` trait + a generic
//! `FusedAggregateExec<S>` operator that subsumes the structural pattern
//! shared by the hand-coded Q6 / Q1 / Q12 execs. The per-shape
//! sub-classes (`FusedFilterSumExec`, `FusedFilterMultiAggExec`,
//! `FusedPostJoinExec`) were the original substrate, and the existing
//! `InjectFused*Rule` family rewrites raw SQL plans into them. This rule
//! sits one slot later in the pipeline: by then the hand exec is already
//! in place, and we lift it onto the generic operator without changing
//! the physical-plan shape downstream.
//!
//! Bench gate before landing: `examples/sigma_g2c_operator_vs_hand.rs`
//! at 41 trials × 3 rounds × MIN-of-K, both Q6 and Q1 within 3 % of the
//! hand operator on TPC-H SF=1 lineitem.
//!
//! ## Composition with other rules
//!
//! Mutually exclusive with `EnableFusedJitRule`: only one of "lift to
//! JIT" or "lift to generic" makes sense per exec instance. Callers who
//! want JIT keep the existing rule; callers who want the generic add
//! this one. A future Σ.G.3 will let the generic exec hold an
//! `Option<JitKernel>` so the two stop being mutually exclusive.
//!
//! Order vs `InjectFusedQ{6,1}Rule`: this rule must run AFTER the
//! injection rules, because it only matches `FusedFilter*Exec` (not raw
//! SQL plans). Add it after the injection rules in `SessionContext`.
//!
//! ## Schema preservation
//!
//! Both `FusedAggregateExec<Q6Spec>` and `FusedFilterSumExec` emit a
//! one-column `revenue: Float64` batch. Both `FusedAggregateExec<Q1Spec>`
//! and `FusedFilterMultiAggExec` emit the canonical 10-column Q1 SELECT
//! list. `schema_check` is safe to leave on.

use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use crate::fused::FusedFilterSumExec;
use crate::fused_aggregate::{Q1Spec, Q6Spec};
use crate::fused_aggregate_exec::FusedAggregateExec;
use crate::fused_multi_agg::FusedFilterMultiAggExec;

/// Rewrite hand-coded `FusedFilterSumExec` / `FusedFilterMultiAggExec`
/// instances to the generic `FusedAggregateExec<S>`. Idempotent; JIT
/// instances are left untouched. See module-level docs.
#[derive(Debug, Default)]
pub struct EnableFusedAggregateExecRule;

impl PhysicalOptimizerRule for EnableFusedAggregateExecRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|node| {
            // Q6 shape: FusedFilterSumExec → FusedAggregateExec<Q6Spec>
            if let Some(e) = node.as_any().downcast_ref::<FusedFilterSumExec>() {
                if !e.has_jit() {
                    let input = e.input().clone();
                    let spec = Q6Spec::try_new(e.predicate(), &input.schema())?;
                    let new = FusedAggregateExec::try_new(input, spec)?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            // Q1 shape: FusedFilterMultiAggExec → FusedAggregateExec<Q1Spec>
            if let Some(e) = node.as_any().downcast_ref::<FusedFilterMultiAggExec>() {
                if !e.has_jit() {
                    let input = e.input().clone();
                    let spec = Q1Spec::try_new(e.predicate(), &input.schema())?;
                    let new = FusedAggregateExec::try_new(input, spec)?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            Ok(Transformed::no(node))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_fused_aggregate_exec"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fused::Q6Predicate;
    use crate::fused_multi_agg::Q1Predicate;
    use arrow_array::{Date32Array, Float64Array, RecordBatch, StringViewArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use futures_util::stream::TryStreamExt;

    fn q6_predicate() -> Q6Predicate {
        Q6Predicate {
            date_lo: 8766,
            date_hi: 9131,
            disc_lo: 0.05,
            disc_hi: 0.07,
            qty_hi: 24.0,
        }
    }

    fn q1_predicate() -> Q1Predicate {
        Q1Predicate {
            shipdate_cutoff: 10471,
        }
    }

    fn small_q6_batch() -> (RecordBatch, Arc<Schema>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        let qty = Float64Array::from(vec![
            23.0, 30.0, 10.0, 25.0, 15.0, 24.0, 20.0, 35.0, 22.0, 5.0,
        ]);
        let price = Float64Array::from(vec![100.0; 10]);
        let disc = Float64Array::from(vec![
            0.06, 0.05, 0.07, 0.05, 0.06, 0.05, 0.05, 0.05, 0.04, 0.10,
        ]);
        let ship = Date32Array::from(vec![
            9000, 8000, 9100, 9200, 8900, 9000, 8800, 9050, 9020, 9080,
        ]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(qty),
                Arc::new(price),
                Arc::new(disc),
                Arc::new(ship),
            ],
        )
        .unwrap();
        (batch, schema)
    }

    fn small_q1_batch() -> (RecordBatch, Arc<Schema>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8View, false),
            Field::new("l_linestatus", DataType::Utf8View, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        let rflag = StringViewArray::from(vec!["N", "N", "A", "R"]);
        let lstatus = StringViewArray::from(vec!["F", "F", "F", "F"]);
        let qty = Float64Array::from(vec![10.0, 10.0, 20.0, 5.0]);
        let price = Float64Array::from(vec![100.0, 100.0, 200.0, 50.0]);
        let disc = Float64Array::from(vec![0.05, 0.05, 0.10, 0.02]);
        let tax = Float64Array::from(vec![0.10, 0.10, 0.05, 0.05]);
        let ship = Date32Array::from(vec![8800, 8800, 8800, 20000]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(rflag),
                Arc::new(lstatus),
                Arc::new(qty),
                Arc::new(price),
                Arc::new(disc),
                Arc::new(tax),
                Arc::new(ship),
            ],
        )
        .unwrap();
        (batch, schema)
    }

    /// Build a Q6 scan plan we can wrap with FusedFilterSumExec.
    async fn q6_scan_plan() -> (Arc<dyn ExecutionPlan>, SessionContext) {
        let (batch, schema) = small_q6_batch();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("lineitem", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM lineitem").await.unwrap();
        (df.create_physical_plan().await.unwrap(), ctx)
    }

    async fn q1_scan_plan() -> (Arc<dyn ExecutionPlan>, SessionContext) {
        let (batch, schema) = small_q1_batch();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("lineitem", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM lineitem").await.unwrap();
        (df.create_physical_plan().await.unwrap(), ctx)
    }

    #[tokio::test]
    async fn rule_lifts_q6_hand_to_generic_and_preserves_result() {
        let (scan, ctx) = q6_scan_plan().await;
        let hand: Arc<dyn ExecutionPlan> = Arc::new(
            FusedFilterSumExec::try_new_q6(scan.clone(), q6_predicate()).unwrap(),
        );

        let rule = EnableFusedAggregateExecRule;
        let lifted = rule.optimize(hand.clone(), &ConfigOptions::default()).unwrap();

        let is_generic = lifted
            .as_any()
            .downcast_ref::<FusedAggregateExec<Q6Spec>>()
            .is_some();
        assert!(is_generic, "rule should have lifted hand exec to generic");

        // Same result.
        let hand_out = run_exec(hand, &ctx).await;
        let lift_out = run_exec(lifted, &ctx).await;
        let h = hand_out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let u = lift_out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((h - u).abs() < 1e-9, "Q6 revenue diverges: {h} vs {u}");
    }

    #[tokio::test]
    async fn rule_lifts_q1_hand_to_generic_and_preserves_row_count() {
        let (scan, ctx) = q1_scan_plan().await;
        let hand: Arc<dyn ExecutionPlan> = Arc::new(
            FusedFilterMultiAggExec::try_new_q1(scan.clone(), q1_predicate()).unwrap(),
        );

        let rule = EnableFusedAggregateExecRule;
        let lifted = rule.optimize(hand.clone(), &ConfigOptions::default()).unwrap();

        let is_generic = lifted
            .as_any()
            .downcast_ref::<FusedAggregateExec<Q1Spec>>()
            .is_some();
        assert!(is_generic, "rule should have lifted Q1 hand exec to generic");

        let hand_out = run_exec(hand, &ctx).await;
        let lift_out = run_exec(lifted, &ctx).await;
        assert_eq!(hand_out.num_rows(), lift_out.num_rows());
        assert_eq!(hand_out.num_columns(), lift_out.num_columns());
    }

    #[tokio::test]
    async fn rule_leaves_jit_q6_alone() {
        let (scan, _ctx) = q6_scan_plan().await;
        let jit: Arc<dyn ExecutionPlan> = Arc::new(
            FusedFilterSumExec::try_new_q6_jit(scan, q6_predicate()).unwrap(),
        );

        let rule = EnableFusedAggregateExecRule;
        let after = rule.optimize(jit, &ConfigOptions::default()).unwrap();

        // Should still be the original JIT exec, not the generic.
        let is_hand = after
            .as_any()
            .downcast_ref::<FusedFilterSumExec>()
            .is_some();
        assert!(is_hand, "JIT instance should pass through unchanged");
    }

    #[tokio::test]
    async fn rule_passes_through_unrelated_plans() {
        let (scan, _ctx) = q6_scan_plan().await;
        let rule = EnableFusedAggregateExecRule;
        let after = rule.optimize(scan.clone(), &ConfigOptions::default()).unwrap();
        // Same Arc address — no rewrite happened.
        assert!(Arc::ptr_eq(&after, &scan));
    }

    async fn run_exec(exec: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> RecordBatch {
        let mut s = exec.execute(0, ctx.task_ctx()).unwrap();
        s.try_next()
            .await
            .unwrap()
            .expect("stream yielded at least one batch")
    }
}
