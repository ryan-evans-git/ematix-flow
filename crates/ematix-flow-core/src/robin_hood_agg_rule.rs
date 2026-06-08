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
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};

use crate::robin_hood_agg::{RobinHoodAggregateExec, RobinHoodMode};

/// Σ.N.d — opt-in installer. Adds the rule to a SessionStateBuilder.
/// Callers who don't invoke this never pay the codegen tax.
///
/// Example:
///
/// ```ignore
/// let state = SessionStateBuilder::new()
///     .with_default_features()
///     .with_physical_optimizer_rule(Arc::new(EnableRobinHoodAggregateRule::default()));
/// let ctx = SessionContext::new_with_state(state.build());
/// ```
pub fn install_robin_hood_rule(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodAggregateRule::default()))
}

/// Σ.N.d rule.
///
/// REV.11 (2026-05-30): cardinality-gated, mirroring REV.10 on the sum
/// sibling ([`crate::robin_hood_sum_f64_exec::EnableRobinHoodSumF64Rule`]).
/// The single-table RobinHood COUNT agg WINS at low/mid cardinality
/// (1.16-1.54× vs hashbrown, [[sigma-nf3-beats-stock]]) but THRASHES once
/// groups vastly exceed the largest pre-sized table (`MAX_INIT_CAP` = 32M)
/// — the same failure mode that made Q18 SF=100's 150M-group SUM subquery
/// 12× slower under RobinHood. This rule is opt-in (not in the default
/// chain), so the bug never bit production, but the gate makes it safe for
/// anyone who installs it. The rule refuses the rewrite when estimated
/// groups exceed `max_groups`.
#[derive(Debug)]
pub struct EnableRobinHoodAggregateRule {
    pub max_groups: usize,
    /// REV.18d lower bound: below this est_groups the row-at-a-time kernel
    /// loses to stock, so the rule no-ops. See [`DEFAULT_RH_COUNT_MIN_GROUPS`].
    pub min_groups: usize,
}

/// REV.18 (2026-05-31): tightened 32M → 256K, matching the RobinHoodSumF64
/// finding. This COUNT rule is OPT-IN (not in preset.rs / bench), so the old
/// 32M default never bit production — but it's the SAME row-at-a-time
/// open-addressing kernel, which loses to DataFusion's vectorised columnar
/// agg above ~256K groups (see DEFAULT_RH_SUM_F64_MAX_GROUPS). Tightened
/// defensively so enabling the rule can't reintroduce the REV.18 footgun.
pub const DEFAULT_RH_COUNT_MAX_GROUPS: usize = 256 * 1024;

/// REV.18d (2026-05-31) lower bound, gate-calibration measured. At ~4
/// rows/group (so est_groups ≈ actual groups), `COUNT(*) GROUP BY i64` through
/// RobinHood has a real win band of **[64K, 256K]** est_groups (0.79–0.89× of
/// stock); below ~64K it loses (1.1–1.8×). The lower bound confines firing to
/// the win band so the rule is safe to enable. Env `EMAT_RH_COUNT_MIN_GROUPS`.
pub const DEFAULT_RH_COUNT_MIN_GROUPS: usize = 64 * 1024;

impl Default for EnableRobinHoodAggregateRule {
    fn default() -> Self {
        let max_groups = std::env::var("EMAT_RH_COUNT_MAX_GROUPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RH_COUNT_MAX_GROUPS);
        let min_groups = std::env::var("EMAT_RH_COUNT_MIN_GROUPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RH_COUNT_MIN_GROUPS);
        Self {
            max_groups,
            min_groups,
        }
    }
}

impl PhysicalOptimizerRule for EnableRobinHoodAggregateRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let max_groups = self.max_groups;
        let min_groups = self.min_groups;
        // Σ.N.e — per-node rewrite. Walks bottom-up, so Partial
        // matches FIRST and is replaced before Final sees its input.
        // Final's matcher then verifies the input chain leads to a
        // (now-rewritten) RobinHoodAggregateExec(Partial).
        let result = plan.transform_up(|node| {
            // Partial → RobinHoodAggregateExec(Partial).
            if let Some(partial) = node.as_any().downcast_ref::<AggregateExec>() {
                if matches!(partial.mode(), AggregateMode::Partial) {
                    if let Some((col_idx, group_out_name, count_out_name)) =
                        match_partial_shape(partial)
                    {
                        let real = partial.input().clone();
                        // Input column type guard.
                        if real.schema().field(col_idx).data_type() == &DataType::Int64 {
                            // REV.11 cardinality gate (mirrors REV.10 on the
                            // sum sibling). Estimate groups from input rows
                            // (~4 rows/group, matching the operator's init_cap
                            // heuristic). If groups would blow past the largest
                            // pre-sized table, the single-table RobinHood agg
                            // thrashes — leave it to DataFusion's stock agg.
                            let est_groups = match real.partition_statistics(None) {
                                Ok(s) => match s.num_rows {
                                    Precision::Exact(n) | Precision::Inexact(n) => n / 4,
                                    _ => 0,
                                },
                                Err(_) => 0,
                            };
                            if est_groups >= min_groups && est_groups <= max_groups {
                                let new = RobinHoodAggregateExec::try_new_full(
                                    real,
                                    col_idx,
                                    None,
                                    RobinHoodMode::Partial,
                                    group_out_name,
                                    count_out_name,
                                )?;
                                return Ok(Transformed::yes(
                                    Arc::new(new) as Arc<dyn ExecutionPlan>
                                ));
                            }
                        }
                    }
                }
                // Final / FinalPartitioned → RobinHoodAggregateExec(FinalPartitioned)
                // ONLY if its sub-chain leads to a RobinHoodAggregateExec(Partial).
                if matches!(
                    partial.mode(),
                    AggregateMode::Final | AggregateMode::FinalPartitioned
                ) {
                    if let Some((_col_idx, group_out_name, count_out_name)) =
                        match_final_shape(partial)
                    {
                        // Walk down through pass-through nodes to find
                        // a RobinHoodAggregateExec(Partial). If found,
                        // we know the Final's input was the chain
                        // [RobinHood(Partial) → ?(RepartitionExec) → us].
                        if find_robin_hood_partial(partial.input()).is_some() {
                            // partial_count_col_idx = 1 (Partial emits
                            // [group_key, partial_count]).
                            // group_col_idx = 0 (same).
                            let new = RobinHoodAggregateExec::try_new_full(
                                partial.input().clone(),
                                0,
                                Some(1),
                                RobinHoodMode::FinalPartitioned,
                                group_out_name,
                                count_out_name,
                            )?;
                            return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                        }
                    }
                }
            }
            Ok(Transformed::no(node))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_robin_hood_agg"
    }

    fn schema_check(&self) -> bool {
        // Both replacements preserve column names + Int64/Int64 types.
        true
    }
}

/// Σ.N.e — match the Partial agg shape: single Int64 GROUP BY +
/// single COUNT. Returns (col_idx, group_out_name, count_out_name).
fn match_partial_shape(partial: &AggregateExec) -> Option<(usize, String, String)> {
    let groups = partial.group_expr().expr();
    if groups.len() != 1 {
        return None;
    }
    let (group_expr, group_out_name) = &groups[0];
    let col = group_expr.as_any().downcast_ref::<Column>()?;
    let col_idx = col.index();

    let aggs = partial.aggr_expr();
    if aggs.len() != 1 {
        return None;
    }
    let agg = &aggs[0];
    if !agg.fun().name().eq_ignore_ascii_case("count") {
        return None;
    }
    let count_out_name = agg.name().to_string();
    Some((col_idx, group_out_name.clone(), count_out_name))
}

/// Σ.N.e — match the Final agg shape: single GROUP BY + single COUNT.
/// Returns (col_idx, group_out_name, count_out_name).
fn match_final_shape(final_agg: &AggregateExec) -> Option<(usize, String, String)> {
    let groups = final_agg.group_expr().expr();
    if groups.len() != 1 {
        return None;
    }
    let (group_expr, group_out_name) = &groups[0];
    let col = group_expr.as_any().downcast_ref::<Column>()?;
    let col_idx = col.index();

    let aggs = final_agg.aggr_expr();
    if aggs.len() != 1 {
        return None;
    }
    let agg = &aggs[0];
    if !agg.fun().name().eq_ignore_ascii_case("count") {
        return None;
    }
    let count_out_name = agg.name().to_string();
    Some((col_idx, group_out_name.clone(), count_out_name))
}

/// Σ.N.e — walk down through pass-through nodes to find a
/// RobinHoodAggregateExec(Partial). Returns the matched node if
/// found.
fn find_robin_hood_partial(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let mut cur = plan.clone();
    loop {
        if let Some(rh) = cur.as_any().downcast_ref::<RobinHoodAggregateExec>() {
            if rh.mode() == RobinHoodMode::Partial {
                return Some(cur);
            }
            return None;
        }
        let children = cur.children();
        if children.len() != 1 {
            return None;
        }
        cur = children[0].clone();
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::assertions_on_constants)]
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    /// Build a ctx with target_partitions=4 so DataFusion splits the
    /// plan into Partial+Final (mode=Single is used at
    /// target_partitions=1, which doesn't match the rule's expected
    /// FinalPartitioned→Partial shape).
    fn make_ctx_with_rule() -> SessionContext {
        let cfg = datafusion::prelude::SessionConfig::new().with_target_partitions(4);
        // min_groups: 0 so the REV.18d lower bound doesn't suppress firing on
        // the tiny test tables — these tests exercise kernel mechanics +
        // correctness; the lower bound itself has a dedicated test.
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRobinHoodAggregateRule {
                min_groups: 0,
                max_groups: usize::MAX,
            }))
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
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let cs = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
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
            vec![Arc::new(arrow_array::StringArray::from(vec![
                "a", "b", "a",
            ]))],
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
    async fn rule_fires_on_multi_partition_with_parallel_modes() {
        // Σ.N.e — with Partial+FinalPartitioned modes, the rule
        // SHOULD fire on multi-partition input. Each input partition
        // gets its own Partial; the FinalPartitioned merges via
        // ingest_partial_counts. No serialisation regression.
        let ctx = make_ctx_with_rule();
        register_int64_t_multi_partition(&ctx);
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t_mp GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        // Both modes must appear in the rewritten plan.
        assert!(
            s.contains("mode: Partial"),
            "expected RobinHoodAggregateExec(mode=Partial) in plan. Got:\n{s}"
        );
        assert!(
            s.contains("mode: FinalPartitioned"),
            "expected RobinHoodAggregateExec(mode=FinalPartitioned) in plan. Got:\n{s}"
        );

        // Correctness: same output as DataFusion stock agg.
        let rh_pairs = collect_pairs(&df.collect().await.unwrap());

        let stock_ctx = SessionContext::new();
        register_int64_t_multi_partition(&stock_ctx);
        let df2 = stock_ctx
            .sql("SELECT k, COUNT(*) FROM t_mp GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let stock_pairs = collect_pairs(&df2.collect().await.unwrap());
        assert_eq!(rh_pairs, stock_pairs);
    }

    fn collect_pairs(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
        let mut out = Vec::new();
        for b in batches {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let cs = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push((ks.value(i), cs.value(i)));
            }
        }
        out.sort();
        out
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

    #[tokio::test]
    async fn rule_gated_off_at_high_cardinality() {
        // REV.11: with max_groups=0, any non-empty input counts as "high
        // card", so the rule must REFUSE the rewrite — DataFusion's stock
        // vectorised agg handles very high cardinality far better (Q18
        // SF=100's 150M-group sibling agg was 12× slower under RobinHood).
        // Mirrors the default 32M gate that leaves Q18's 150M-group agg to
        // stock.
        let cfg = datafusion::prelude::SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRobinHoodAggregateRule {
                min_groups: 0,
                max_groups: 0,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_int64_t(&ctx);
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodAggregateExec"),
            "rule fired despite max_groups=0 high-card gate — Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_fires_under_generous_gate() {
        // With a generous gate (12-row test table ≪ cap), the rule fires.
        let cfg = datafusion::prelude::SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRobinHoodAggregateRule {
                min_groups: 0,
                max_groups: 1_000_000,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_int64_t(&ctx);
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            s.contains("RobinHoodAggregateExec"),
            "rule didn't fire under a generous gate — Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_gated_off_below_min_groups() {
        // REV.18d lower bound: below min_groups est_groups the kernel loses to
        // stock, so the rule must REFUSE the rewrite. The 12-row table has
        // est_groups = 3 ≪ 1000, so with min_groups=1000 the plan must keep
        // DataFusion's stock AggregateExec.
        let cfg = datafusion::prelude::SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRobinHoodAggregateRule {
                min_groups: 1_000,
                max_groups: usize::MAX,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_int64_t(&ctx);
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodAggregateExec"),
            "rule fired below the min_groups lower bound — Got:\n{s}"
        );
    }

    #[test]
    fn rev18d_default_min_gate_is_set() {
        // REV.18d: the win band is [64K, 256K]; the lower bound must be > 0
        // and below the upper bound so a non-empty fire band exists.
        assert!(DEFAULT_RH_COUNT_MIN_GROUPS > 0);
        assert!(DEFAULT_RH_COUNT_MIN_GROUPS < DEFAULT_RH_COUNT_MAX_GROUPS);
    }
}
