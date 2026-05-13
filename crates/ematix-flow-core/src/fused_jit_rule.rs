//! Σ.D3 phase D: `PhysicalOptimizerRule` that auto-routes hand-coded
//! fused execs to their Cranelift-JIT'd variants.
//!
//! ## Scope
//!
//! This rule walks the physical plan tree with `transform_up` and, for
//! each `FusedFilterSumExec` / `FusedFilterMultiAggExec` /
//! `FusedPostJoinExec(Q14)` that's still in hand-coded mode, rebuilds
//! it with the matching `try_new_*_jit` constructor. Other plan nodes
//! pass through unchanged.
//!
//! That's the minimum viable auto-routing: callers who manually
//! construct a fused exec (in tune examples / future planner paths)
//! get the JIT speedup automatically by adding one
//! `add_physical_optimizer_rule` call to their SessionContext.
//!
//! ## What's NOT here
//!
//! The "real" Σ.D3 phase D — pattern-matching DataFusion's default SQL
//! plan output (`AggregateExec(Final) ← CoalescePartitionsExec ←
//! AggregateExec(Partial) ← ProjectionExec ← FilterExec ← ParquetExec`)
//! and extracting predicate constants from the `PhysicalExpr` AST to
//! INJECT a fused exec where none was hand-constructed — is deferred.
//! That requires walking `BinaryExpr`/`Column`/`Literal` nodes,
//! recognising AND-chains of column-vs-literal comparisons, mapping
//! aggregate function calls to `AggExpr` variants, and aligning column
//! indices between the physical plan and the spec. None of those are
//! intrinsically hard but each is a meaningful chunk of code; this
//! commit lands the optimizer-wiring substrate so that work can build
//! on a tested foundation.

use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use crate::fused::FusedFilterSumExec;
use crate::fused_multi_agg::FusedFilterMultiAggExec;
use crate::fused_post_join::{FusedPostJoinExec, FusedPostJoinSpec};

/// Σ.D3 phase D rule: rewrite hand-coded fused execs into their JIT
/// variants in-place. Idempotent — execs already in JIT mode pass
/// through unchanged.
#[derive(Debug, Default)]
pub struct EnableFusedJitRule;

impl PhysicalOptimizerRule for EnableFusedJitRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|node| {
            // FusedFilterSumExec (Σ.D1 / Q6 shape)
            if let Some(e) = node.as_any().downcast_ref::<FusedFilterSumExec>() {
                if !e.has_jit() {
                    let new = FusedFilterSumExec::try_new_q6_jit(
                        e.input().clone(),
                        e.predicate(),
                    )?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            // FusedFilterMultiAggExec (Σ.D2 / Q1 shape)
            if let Some(e) = node.as_any().downcast_ref::<FusedFilterMultiAggExec>() {
                if !e.has_jit() {
                    let new = FusedFilterMultiAggExec::try_new_q1_jit(
                        e.input().clone(),
                        e.predicate(),
                    )?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            // FusedPostJoinExec — JIT only currently supports Q14.
            if let Some(e) = node.as_any().downcast_ref::<FusedPostJoinExec>() {
                if !e.has_jit() && matches!(e.spec(), FusedPostJoinSpec::Q14) {
                    let new =
                        FusedPostJoinExec::try_new_jit(e.input().clone(), e.spec())?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            Ok(Transformed::no(node))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_fused_jit"
    }

    fn schema_check(&self) -> bool {
        // The rule swaps an exec for one that produces an identical
        // output schema — both `try_new_q6` and `try_new_q6_jit` use
        // the same Schema constructor, etc. Safe to leave validation on.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fused::Q6Predicate;
    use datafusion::arrow::array::{Date32Builder, Float64Builder, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use futures_util::stream::TryStreamExt;

    fn one_row_match() -> RecordBatch {
        // Single row that satisfies the canonical Q6 predicate:
        // shipdate=8800 (in [8766, 9131)), discount=0.06 (in [0.05, 0.07]),
        // quantity=10 (< 24), extprice=100 → contributes 100*0.06 = 6.0.
        let mut qty = Float64Builder::new();
        let mut price = Float64Builder::new();
        let mut disc = Float64Builder::new();
        let mut ship = Date32Builder::new();
        qty.append_value(10.0);
        price.append_value(100.0);
        disc.append_value(0.06);
        ship.append_value(8800);
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(qty.finish()),
                Arc::new(price.finish()),
                Arc::new(disc.finish()),
                Arc::new(ship.finish()),
            ],
        )
        .unwrap()
    }

    async fn input_plan_for_q6() -> Arc<dyn ExecutionPlan> {
        let b = one_row_match();
        let schema = b.schema();
        let mem = MemTable::try_new(schema, vec![vec![b]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    fn q6_predicate_canonical() -> Q6Predicate {
        Q6Predicate {
            date_lo: 8766,
            date_hi: 9131,
            disc_lo: 0.05,
            disc_hi: 0.07,
            qty_hi: 24.0,
        }
    }

    /// Rule converts an existing hand-coded FusedFilterSumExec into
    /// its JIT variant: same structural ExecutionPlan, but `has_jit()`
    /// flips from false to true.
    #[tokio::test]
    async fn rule_enables_jit_on_hand_coded_q6_exec() {
        let input = input_plan_for_q6().await;
        let hand = Arc::new(
            FusedFilterSumExec::try_new_q6(input, q6_predicate_canonical()).unwrap(),
        );
        assert!(!hand.has_jit(), "starting state: hand-coded");

        let rule = EnableFusedJitRule;
        let cfg = ConfigOptions::new();
        let optimized = rule.optimize(hand.clone(), &cfg).unwrap();
        let downcast = optimized
            .as_any()
            .downcast_ref::<FusedFilterSumExec>()
            .expect("rule preserves the operator type");
        assert!(downcast.has_jit(), "rule should enable JIT");
    }

    /// Rule is idempotent: a plan already in JIT mode passes through
    /// unchanged (no double-build, no panics).
    #[tokio::test]
    async fn rule_is_idempotent() {
        let input = input_plan_for_q6().await;
        let already_jit = Arc::new(
            FusedFilterSumExec::try_new_q6_jit(input, q6_predicate_canonical()).unwrap(),
        );
        let rule = EnableFusedJitRule;
        let cfg = ConfigOptions::new();
        let optimized = rule.optimize(already_jit, &cfg).unwrap();
        let downcast = optimized
            .as_any()
            .downcast_ref::<FusedFilterSumExec>()
            .unwrap();
        assert!(downcast.has_jit());
    }

    /// End-to-end: rule-rewritten plan produces the same final
    /// f64 result as the original hand-coded plan. Validates that the
    /// rule preserves the output (no schema drift, no FP surprises).
    #[tokio::test]
    async fn rule_rewritten_plan_produces_same_result_as_hand_coded() {
        use datafusion::arrow::array::Float64Array;

        let hand_input = input_plan_for_q6().await;
        let hand_exec: Arc<dyn ExecutionPlan> = Arc::new(
            FusedFilterSumExec::try_new_q6(hand_input, q6_predicate_canonical()).unwrap(),
        );

        let rule_input = input_plan_for_q6().await;
        let pre_rule: Arc<dyn ExecutionPlan> = Arc::new(
            FusedFilterSumExec::try_new_q6(rule_input, q6_predicate_canonical()).unwrap(),
        );
        let cfg = ConfigOptions::new();
        let rewritten = EnableFusedJitRule.optimize(pre_rule, &cfg).unwrap();

        let session = SessionContext::new();
        let mut hand_s = hand_exec.execute(0, session.task_ctx()).unwrap();
        let mut rule_s = rewritten.execute(0, session.task_ctx()).unwrap();
        let hand_v = hand_s
            .try_next()
            .await
            .unwrap()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let rule_v = rule_s
            .try_next()
            .await
            .unwrap()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            hand_v.to_bits(),
            rule_v.to_bits(),
            "hand={hand_v} rule={rule_v} (must be bit-identical)"
        );
    }

    /// SessionContext registration smoke-test: the rule can be added
    /// via the public API and is named consistently. Doesn't validate
    /// auto-firing on SQL queries (that requires pattern-matching the
    /// DataFusion-default plan, which is the deferred work).
    #[tokio::test]
    async fn rule_registers_into_session_state() {
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::SessionConfig;
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(EnableFusedJitRule))
            .build();
        let ctx = SessionContext::new_with_state(state);
        // A trivial SQL query through the customized SessionContext.
        // The rule itself doesn't fire (no FusedFilterSumExec in the
        // plan) but we confirm registration didn't break anything.
        let df = ctx.sql("SELECT 1 AS x").await.unwrap();
        let batches: Vec<RecordBatch> = df.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }
}
