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
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

use crate::fused_aggregate::{Q1Spec, Q6Spec};
use crate::fused_aggregate_exec::FusedAggregateExec;
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
            // Σ.G.3d: FusedAggregateExec<Q6Spec> — the Q6 shape now lives
            // here, not in the retired FusedFilterSumExec.
            if let Some(e) = node.as_any().downcast_ref::<FusedAggregateExec<Q6Spec>>() {
                if !e.spec().has_jit() {
                    let input = e.input().clone();
                    let new_spec = Q6Spec::try_new_jit(e.spec().predicate, &input.schema())?;
                    let new = FusedAggregateExec::try_new(input, new_spec)?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            // Σ.G.3d: FusedAggregateExec<Q1Spec> — same shape, now under
            // the generic operator.
            if let Some(e) = node.as_any().downcast_ref::<FusedAggregateExec<Q1Spec>>() {
                if !e.spec().has_jit() {
                    let input = e.input().clone();
                    let new_spec = Q1Spec::try_new_jit(e.spec().predicate, &input.schema())?;
                    let new = FusedAggregateExec::try_new(input, new_spec)?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            // FusedPostJoinExec — JIT only currently supports Q14.
            if let Some(e) = node.as_any().downcast_ref::<FusedPostJoinExec>() {
                if !e.has_jit() && matches!(e.spec(), FusedPostJoinSpec::Q14) {
                    let new = FusedPostJoinExec::try_new_jit(e.input().clone(), e.spec())?;
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

// Σ.G.2e-4: `InjectFusedQ6Rule` was retired in favour of
// `InjectFilterSumRule` (`fused_aggregate_filter_sum_rule.rs`), whose
// generalised matcher subsumes the Q6-shape SQL pattern and was
// bench-gated at 1.50 % delta on TPC-H SF=1 Q6 (see
// `examples/sigma_g2e_filter_sum_vs_q6.rs`). The substrate
// `Q6Spec(JIT)` type is still exported from `fused_aggregate` so the
// direct-construction examples (`tpch_q6_jit_bench`, `tpch_q6_tune`)
// continue to work; only the SQL-pattern auto-injection rule + its
// Q6-specific predicate/aggregate extractors are gone. The shared
// `flip_op` helper survives because the Q1 path still uses it.

fn dferr(msg: &str) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Internal(msg.into())
}

// ===== Σ.D3 phase D follow-up (B): generalised plan-shape matcher =====
//
// The six per-query matchers (Q1/Q3/Q5/Q6/Q12/Q14) share most of their
// walk skeleton: optional SortMerge → Sort → Projection at the top, an
// AggregateExec(Final*) → optional CoalescePartitions/RepartitionExec
// → AggregateExec(Partial) stack, then optional ProjectionExec(CSE)
// and a tail that's either a FilterExec → scan (Q1/Q6) or a join
// chain (Q3/Q5/Q12/Q14). Only the query-specific *validation* of each
// step differs.
//
// `AggregateShapeConfig` declares the structural expectations a rule
// has on the plan tree; `match_aggregate_query_shape` does the walk
// and returns the matched nodes + the body (post-aggregate-stack
// plan node) for the per-rule code to inspect.

/// Position of the AggregateExec(Final*) bridge. Most queries with an
/// ORDER BY use `FinalPartitioned` (and have a hash repartition
/// between Partial and Final); queries without ORDER BY use `Final`
/// (and have a `CoalescePartitionsExec` instead).
#[derive(Debug, Clone, Copy)]
pub enum FinalAggMode {
    /// Plain `AggregateMode::Final`, expecting a `CoalescePartitionsExec`
    /// between it and the AggregateExec(Partial).
    Final,
    /// `AggregateMode::FinalPartitioned`, expecting a
    /// `RepartitionExec(Hash([...]))` between it and the
    /// AggregateExec(Partial).
    FinalPartitioned,
}

/// Declarative configuration for the structural plan walk shared by
/// the InjectFused*Rule family. Per-rule code reads
/// [`MatchedAggregateShape`] (the result of running this config) for
/// the matched AggregateExec nodes + body, then does query-specific
/// validation (predicate extraction, aggregate-name checks,
/// replacement-exec construction).
#[derive(Debug, Clone, Copy)]
pub struct AggregateShapeConfig {
    /// True if we should expect (and strip) a
    /// `SortPreservingMergeExec → SortExec` pair at the top. Queries
    /// with `ORDER BY` set this; otherwise false.
    pub expect_top_sort: bool,
    /// True if we should expect (and capture) a `ProjectionExec` above
    /// the AggregateExec(Final*). The per-rule code can then check the
    /// output column names.
    pub expect_top_projection: bool,
    /// Expected aggregate-final mode (drives whether we look for a
    /// `CoalescePartitionsExec` or a `RepartitionExec(Hash)`).
    pub expect_final_mode: FinalAggMode,
    /// Expected number of group-by columns at both aggregate levels.
    pub expect_group_by_count: usize,
    /// Expected number of aggregate expressions at both aggregate
    /// levels.
    pub expect_agg_count: usize,
    /// True if we should expect (and strip) a `ProjectionExec`
    /// between AggregateExec(Partial) and the body. This is the CSE
    /// projection DataFusion inserts when the aggregate argument
    /// references a common sub-expression (e.g. `extprice * (1 -
    /// discount)`).
    pub expect_cse_projection: bool,
}

/// Result of running [`match_aggregate_query_shape`] on a plan tree.
/// Per-rule code reads these and does query-specific validation.
#[derive(Debug, Clone)]
pub struct MatchedAggregateShape {
    /// The top ProjectionExec (if `expect_top_projection`). Per-rule
    /// code checks the output schema's column names.
    pub top_projection: Option<Arc<ProjectionExec>>,
    /// The AggregateExec(Final*). Per-rule code reads `group_expr()`
    /// and `aggr_expr()` for query-specific validation.
    pub final_agg: Arc<AggregateExec>,
    /// The AggregateExec(Partial).
    pub partial_agg: Arc<AggregateExec>,
    /// The CSE ProjectionExec (if `expect_cse_projection`). Often
    /// holds the `__common_expr_1 = extprice * (1 - discount)`
    /// rewrite; per-rule code rarely needs to inspect it.
    pub cse_projection: Option<Arc<ProjectionExec>>,
    /// The plan node below the aggregate stack. For
    /// FusedFilterSumExec / FusedFilterMultiAggExec-shaped rules
    /// this is typically the `FilterExec`; for FusedPostJoinExec-
    /// shaped rules this is the join chain or projection above it.
    pub body: Arc<dyn ExecutionPlan>,
}

/// Walk `node` top-down and try to match the structural plan shape
/// described by `cfg`. Returns `Ok(Some(MatchedAggregateShape))` on
/// match, `Ok(None)` if any step diverges from `cfg`. Never errors
/// except on internal-invariant violations (children missing).
pub(crate) fn match_aggregate_query_shape(
    node: &Arc<dyn ExecutionPlan>,
    cfg: &AggregateShapeConfig,
) -> DfResult<Option<MatchedAggregateShape>> {
    // Top: optional SortPreservingMergeExec.
    let after_merge: Arc<dyn ExecutionPlan> = if cfg.expect_top_sort
        && node
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>()
            .is_some()
    {
        match node.children().first() {
            Some(c) => (*c).clone(),
            None => return Ok(None),
        }
    } else {
        node.clone()
    };

    // SortExec (only when expect_top_sort).
    let after_sort: Arc<dyn ExecutionPlan> = if cfg.expect_top_sort {
        let Some(_) = after_merge.as_any().downcast_ref::<SortExec>() else {
            return Ok(None);
        };
        after_merge
            .children()
            .first()
            .map(|c| (*c).clone())
            .ok_or_else(|| dferr("shape match: SortExec missing input"))?
    } else {
        after_merge
    };

    // Optional top ProjectionExec.
    let (top_projection, after_top_proj): (Option<Arc<ProjectionExec>>, Arc<dyn ExecutionPlan>) =
        if cfg.expect_top_projection {
            let Some(proj) = after_sort.as_any().downcast_ref::<ProjectionExec>() else {
                return Ok(None);
            };
            let next = after_sort
                .children()
                .first()
                .map(|c| (*c).clone())
                .ok_or_else(|| dferr("shape match: ProjectionExec missing input"))?;
            // We need an owned Arc<ProjectionExec> for the result struct;
            // `downcast_ref` only gives a borrow. Rebuild by cloning.
            let owned: Arc<ProjectionExec> = Arc::new(ProjectionExec::try_new(
                proj.expr().to_vec(),
                proj.children()[0].clone(),
            )?);
            (Some(owned), next)
        } else {
            (None, after_sort)
        };

    // AggregateExec(Final*).
    let Some(final_agg_ref) = after_top_proj.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    let mode_ok = match cfg.expect_final_mode {
        FinalAggMode::Final => matches!(final_agg_ref.mode(), AggregateMode::Final),
        FinalAggMode::FinalPartitioned => {
            matches!(final_agg_ref.mode(), AggregateMode::FinalPartitioned)
        }
    };
    if !mode_ok {
        return Ok(None);
    }
    if final_agg_ref.group_expr().expr().len() != cfg.expect_group_by_count
        || final_agg_ref.aggr_expr().len() != cfg.expect_agg_count
    {
        return Ok(None);
    }
    let final_agg: Arc<AggregateExec> = Arc::new(final_agg_ref.clone());

    // Bridge: CoalescePartitionsExec (Final) or RepartitionExec(Hash) (FinalPartitioned).
    let after_final: Arc<dyn ExecutionPlan> = after_top_proj
        .children()
        .first()
        .map(|c| (*c).clone())
        .ok_or_else(|| dferr("shape match: AggregateExec(Final) missing input"))?;
    let after_bridge: Arc<dyn ExecutionPlan> = match cfg.expect_final_mode {
        FinalAggMode::Final => {
            // CoalescePartitionsExec may or may not be present (it's
            // skipped when the input is already single-partition);
            // strip it if so.
            if after_final
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
                .is_some()
            {
                after_final
                    .children()
                    .first()
                    .map(|c| (*c).clone())
                    .ok_or_else(|| dferr("shape match: CoalescePartitionsExec missing input"))?
            } else {
                after_final
            }
        }
        FinalAggMode::FinalPartitioned => {
            // Hash repartition is required for FinalPartitioned.
            if after_final
                .as_any()
                .downcast_ref::<RepartitionExec>()
                .is_none()
            {
                return Ok(None);
            }
            after_final
                .children()
                .first()
                .map(|c| (*c).clone())
                .ok_or_else(|| dferr("shape match: RepartitionExec(Hash) missing input"))?
        }
    };

    // AggregateExec(Partial).
    let Some(partial_agg_ref) = after_bridge.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    if !matches!(partial_agg_ref.mode(), AggregateMode::Partial) {
        return Ok(None);
    }
    if partial_agg_ref.group_expr().expr().len() != cfg.expect_group_by_count
        || partial_agg_ref.aggr_expr().len() != cfg.expect_agg_count
    {
        return Ok(None);
    }
    let partial_agg: Arc<AggregateExec> = Arc::new(partial_agg_ref.clone());

    // Optional CSE ProjectionExec.
    let after_partial: Arc<dyn ExecutionPlan> = after_bridge
        .children()
        .first()
        .map(|c| (*c).clone())
        .ok_or_else(|| dferr("shape match: AggregateExec(Partial) missing input"))?;
    let (cse_projection, body): (Option<Arc<ProjectionExec>>, Arc<dyn ExecutionPlan>) =
        if cfg.expect_cse_projection {
            // CSE projection is sometimes elided; treat as optional even
            // when expected. Per-rule code can re-tighten if needed.
            match after_partial.as_any().downcast_ref::<ProjectionExec>() {
                Some(p) => {
                    let owned: Arc<ProjectionExec> = Arc::new(ProjectionExec::try_new(
                        p.expr().to_vec(),
                        p.children()[0].clone(),
                    )?);
                    let next = after_partial
                        .children()
                        .first()
                        .map(|c| (*c).clone())
                        .ok_or_else(|| dferr("shape match: CSE ProjectionExec missing input"))?;
                    (Some(owned), next)
                }
                None => (None, after_partial),
            }
        } else {
            (None, after_partial)
        };

    Ok(Some(MatchedAggregateShape {
        top_projection,
        final_agg,
        partial_agg,
        cse_projection,
        body,
    }))
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

    /// Build a non-JIT `FusedAggregateExec<Q6Spec>` directly — the Σ.G.3d
    /// substrate the EnableFusedJitRule now lifts to JIT.
    async fn fused_agg_q6_no_jit() -> Arc<dyn ExecutionPlan> {
        let input = input_plan_for_q6().await;
        let spec = Q6Spec::try_new(q6_predicate_canonical(), &input.schema()).unwrap();
        Arc::new(FusedAggregateExec::try_new(input, spec).unwrap())
    }

    async fn fused_agg_q6_with_jit() -> Arc<dyn ExecutionPlan> {
        let input = input_plan_for_q6().await;
        let spec = Q6Spec::try_new_jit(q6_predicate_canonical(), &input.schema()).unwrap();
        Arc::new(FusedAggregateExec::try_new(input, spec).unwrap())
    }

    /// Rule converts a non-JIT `FusedAggregateExec<Q6Spec>` into its
    /// JIT variant: same structural ExecutionPlan, but the spec's
    /// `has_jit()` flips from false to true.
    #[tokio::test]
    async fn rule_enables_jit_on_hand_coded_q6_exec() {
        let hand = fused_agg_q6_no_jit().await;
        assert!(
            !hand
                .as_any()
                .downcast_ref::<FusedAggregateExec<Q6Spec>>()
                .unwrap()
                .spec()
                .has_jit(),
            "starting state: hand-coded"
        );

        let rule = EnableFusedJitRule;
        let cfg = ConfigOptions::new();
        let optimized = rule.optimize(hand.clone(), &cfg).unwrap();
        let downcast = optimized
            .as_any()
            .downcast_ref::<FusedAggregateExec<Q6Spec>>()
            .expect("rule preserves the operator type");
        assert!(downcast.spec().has_jit(), "rule should enable JIT");
    }

    /// Rule is idempotent: a plan already in JIT mode passes through
    /// unchanged (no double-build, no panics).
    #[tokio::test]
    async fn rule_is_idempotent() {
        let already_jit = fused_agg_q6_with_jit().await;
        let rule = EnableFusedJitRule;
        let cfg = ConfigOptions::new();
        let optimized = rule.optimize(already_jit, &cfg).unwrap();
        let downcast = optimized
            .as_any()
            .downcast_ref::<FusedAggregateExec<Q6Spec>>()
            .unwrap();
        assert!(downcast.spec().has_jit());
    }

    /// End-to-end: rule-rewritten plan produces the same final
    /// f64 result as the original hand-coded plan. Validates that the
    /// rule preserves the output (no schema drift, no FP surprises).
    #[tokio::test]
    async fn rule_rewritten_plan_produces_same_result_as_hand_coded() {
        use datafusion::arrow::array::Float64Array;

        let hand_exec = fused_agg_q6_no_jit().await;

        let pre_rule = fused_agg_q6_no_jit().await;
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
