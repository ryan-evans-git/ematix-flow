//! Σ.N.d — PhysicalOptimizerRule that rewrites Q12-shape
//! `AggregateExec(Final*) → ... → AggregateExec(Partial)` sub-plans
//! to [`crate::robin_hood_agg::RobinHoodAggregateExec`] when the
//! group column is `Int64` + the aggregate is `COUNT(*)`.
//!
//! ## Plan shape detected
//!
//! ```text
//! AggregateExec(mode=FinalPartitioned,
//!               gby=[Column(col, idx)],          // Int64
//!               aggr=[count(Int64(1))])
//!   RepartitionExec(Hash([col]))                 // optional
//!     AggregateExec(mode=Partial,
//!                   gby=[Column(col, idx)],
//!                   aggr=[count(Int64(1))])
//!       <child>
//! ```
//!
//! Mirrors [`crate::dict_aggregate_rule::EnableDictGroupCountRule`]
//! but for Int64 group keys instead of Dictionary. The rewritten
//! operator wins 1.16-1.54× vs hashbrown depending on key
//! cardinality (see `robin_hood_vs_hashbrown_bench`).
//!
//! ## Strictly speculative
//!
//! Any departure from the recognised shape — wrong agg, multi-group,
//! multi-agg, non-COUNT(Int64(1)), non-Int64 group column — is a
//! no-op. The rule cannot produce wrong answers.
//!
//! ## NOT in the default optimizer chain
//!
//! Per [[optimizer-codegen-sensitivity]], adding any new
//! PhysicalOptimizerRule costs ~7% geomean from LLVM codegen
//! perturbation alone, before the rule does any work. So this rule
//! ships **as opt-in only**: callers who know their workload
//! benefits invoke [`install_robin_hood_rule`] explicitly when
//! building their `SessionStateBuilder`. The 22-query bench default
//! path is unchanged.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};

use crate::robin_hood_agg::RobinHoodAggregateExec;

/// Σ.N.d — opt-in installer. Adds the rule to a SessionStateBuilder.
/// Callers who don't invoke this never pay the codegen tax.
///
/// Example:
///
/// ```ignore
/// let state = SessionStateBuilder::new()
///     .with_default_features()
///     .with_physical_optimizer_rule(Arc::new(EnableRobinHoodAggregateRule));
/// let ctx = SessionContext::new_with_state(state.build());
/// ```
pub fn install_robin_hood_rule(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodAggregateRule))
}

/// Σ.N.d rule.
#[derive(Debug, Default)]
pub struct EnableRobinHoodAggregateRule;

impl PhysicalOptimizerRule for EnableRobinHoodAggregateRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|node| {
            let Some(final_agg) = node.as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            if !matches!(
                final_agg.mode(),
                AggregateMode::Final | AggregateMode::FinalPartitioned
            ) {
                return Ok(Transformed::no(node));
            }
            let Some((real_input, col_idx, group_out_name, count_out_name)) =
                match_robin_hood_shape(final_agg)
            else {
                return Ok(Transformed::no(node));
            };
            let new = RobinHoodAggregateExec::try_new_with_names(
                real_input,
                col_idx,
                group_out_name,
                count_out_name,
            )?;
            Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_robin_hood_agg"
    }

    fn schema_check(&self) -> bool {
        // The rewrite preserves output names; types are Int64 (group)
        // + Int64 (count) on both sides if the original FinalAgg
        // takes an Int64 input. The match guard checks the input
        // type below.
        true
    }
}

/// Σ.N.d shape matcher. Returns Some((real_input, col_idx,
/// group_out_name, count_out_name)) if the sub-plan matches:
/// `AggregateExec(Final*) → ... → AggregateExec(Partial) → real`
/// with a single Int64 group + COUNT.
fn match_robin_hood_shape(
    final_agg: &AggregateExec,
) -> Option<(Arc<dyn ExecutionPlan>, usize, String, String)> {
    // Single group column.
    let groups = final_agg.group_expr().expr();
    if groups.len() != 1 {
        return None;
    }
    let (group_expr, group_out_name) = &groups[0];
    let col = group_expr.as_any().downcast_ref::<Column>()?;
    let col_idx = col.index();

    // Single COUNT aggregate.
    let aggs = final_agg.aggr_expr();
    if aggs.len() != 1 {
        return None;
    }
    let agg = &aggs[0];
    if !agg.fun().name().eq_ignore_ascii_case("count") {
        return None;
    }
    let count_out_name = agg.name().to_string();

    // Walk down: FinalAgg → ?(pass-through) → AggregateExec(Partial) → real.
    let mut cur: Arc<dyn ExecutionPlan> = final_agg.input().clone();
    loop {
        if let Some(partial) = cur.as_any().downcast_ref::<AggregateExec>() {
            if !matches!(partial.mode(), AggregateMode::Partial) {
                return None;
            }
            let pgroups = partial.group_expr().expr();
            if pgroups.len() != 1 {
                return None;
            }
            let (pgexp, _) = &pgroups[0];
            let pcol = pgexp.as_any().downcast_ref::<Column>()?;
            if pcol.index() != col_idx {
                return None;
            }
            // The real input's group column must be Int64.
            let real = partial.input().clone();
            let real_schema = real.schema();
            if pcol.index() >= real_schema.fields().len() {
                return None;
            }
            if real_schema.field(pcol.index()).data_type() != &DataType::Int64 {
                return None;
            }
            // Σ.N.d partition guard (post-bench fix): RobinHood-
            // AggregateExec emits a single output partition and
            // iterates input partitions serially on one thread. If
            // the input has multiple partitions, the rewrite would
            // serialise what was parallel — catastrophic regression.
            // Σ.N.e will add per-partition Partial/Final modes; until
            // then, only fire when the input is already single-
            // partition. See [[sigma-nd-partition-blocker]].
            use datafusion::physical_plan::ExecutionPlanProperties;
            if real.output_partitioning().partition_count() > 1 {
                return None;
            }
            return Some((real, col_idx, group_out_name.clone(), count_out_name));
        }
        // Walk down through single-child pass-through nodes.
        let children = cur.children();
        if children.len() != 1 {
            return None;
        }
        cur = children[0].clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use futures_util::TryStreamExt;
    use std::sync::Arc;

    /// Build a ctx with target_partitions=4 so DataFusion splits the
    /// plan into Partial+Final (mode=Single is used at
    /// target_partitions=1, which doesn't match the rule's expected
    /// FinalPartitioned→Partial shape).
    fn make_ctx_with_rule() -> SessionContext {
        let cfg = datafusion::prelude::SessionConfig::new().with_target_partitions(4);
        let state = install_robin_hood_rule(
            SessionStateBuilder::new().with_default_features().with_config(cfg),
        )
        .build();
        SessionContext::new_with_state(state)
    }

    /// Single-batch MemTable → input is always 1-partition regardless
    /// of `target_partitions`. The rule's partition guard checks the
    /// raw input partition count, which for parquet scans equals
    /// target_partitions but for MemTable equals batch count.
    fn register_int64_t(ctx: &SessionContext) {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                1i64, 2, 3, 1, 2, 1, 5, 5, 2, 3, 3, 3,
            ]))],
        )
        .unwrap();
        let mt = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
    }

    /// Multi-batch MemTable → multi-partition input. The partition
    /// guard should refuse the rewrite here (would serialise parallel
    /// scans onto one thread).
    fn register_int64_t_multi_partition(ctx: &SessionContext) {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let schema_for_batches = schema.clone();
        let make_batch = move |vals: Vec<i64>| {
            RecordBatch::try_new(
                schema_for_batches.clone(),
                vec![Arc::new(Int64Array::from(vals))],
            )
            .unwrap()
        };
        let mt = MemTable::try_new(
            schema,
            vec![
                vec![make_batch(vec![1, 2, 3, 1])],
                vec![make_batch(vec![2, 1, 5, 5])],
                vec![make_batch(vec![2, 3, 3, 3])],
                vec![make_batch(vec![1, 1, 5, 5])],
            ],
        )
        .unwrap();
        ctx.register_table("t_mp", Arc::new(mt)).unwrap();
    }

    #[tokio::test]
    async fn rule_installs_and_produces_correct_output() {
        let ctx = make_ctx_with_rule();
        register_int64_t(&ctx);
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let mut pairs: Vec<(i64, i64)> = Vec::new();
        for b in &batches {
            let ks = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let cs = b
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                pairs.push((ks.value(i), cs.value(i)));
            }
        }
        pairs.sort();
        // 12 rows: k=1×3, k=2×3, k=3×4, k=5×2
        assert_eq!(pairs, vec![(1, 3), (2, 3), (3, 4), (5, 2)]);
    }

    #[tokio::test]
    async fn rule_actually_installs_robin_hood_in_plan() {
        let ctx = make_ctx_with_rule();
        register_int64_t(&ctx);
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            s.contains("RobinHoodAggregateExec"),
            "plan didn't contain RobinHoodAggregateExec — rule didn't fire. Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_no_op_on_non_int64_groupby() {
        let ctx = make_ctx_with_rule();
        // Register a Utf8 group column — rule should NOT match.
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(vec!["a", "b", "a"]))],
        )
        .unwrap();
        let mt = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t_str", Arc::new(mt)).unwrap();
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t_str GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodAggregateExec"),
            "rule fired on non-Int64 group — that's wrong. Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_no_op_on_multi_partition_input() {
        // Σ.N.d partition guard: when the scan emits multiple partitions
        // (a multi-batch MemTable here, or multi-row-group parquet in
        // production), the rewrite would serialise parallel scans onto
        // one thread. Rule must refuse.
        let ctx = make_ctx_with_rule();
        register_int64_t_multi_partition(&ctx);
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t_mp GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodAggregateExec"),
            "rule fired on multi-partition input — partition guard failed. Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_no_op_on_multi_agg() {
        let ctx = make_ctx_with_rule();
        register_int64_t(&ctx);
        // COUNT + SUM → multi-agg, rule should NOT match.
        let df = ctx
            .sql("SELECT k, COUNT(*), SUM(k) FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodAggregateExec"),
            "rule fired on multi-agg — that's wrong. Got:\n{s}"
        );
    }
}
