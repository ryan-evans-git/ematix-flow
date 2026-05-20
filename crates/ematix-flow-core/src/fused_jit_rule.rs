//! Generalised aggregate plan-shape matcher used by
//! [`crate::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule`]
//! and [`crate::fused_aggregate_filter_sum_rule::InjectFilterSumRule`].
//!
//! Σ.G.2f.3 cleanup (2026-05-19): this module used to contain
//! `EnableFusedJitRule` + 4 per-query injection rules
//! (`InjectFusedQ{1,3,5,12}Rule`). All retired in favour of the two
//! generalised rules above — see [[feedback-no-tpch-hardcoding]].
//!
//! What's left here is the shared structural matcher
//! [`match_aggregate_query_shape`] that walks the optional
//! `SortPreservingMergeExec → SortExec → ProjectionExec → AggregateExec(Final*)
//! → CoalescePartitionsExec|RepartitionExec(Hash) → AggregateExec(Partial)
//! → ProjectionExec(CSE) → body` skeleton DataFusion produces for
//! aggregate-shaped SQL. Both remaining injection rules consume this
//! matcher's output and do their own predicate / agg-expr validation
//! against the matched nodes.

use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

fn dferr(msg: &str) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Internal(msg.into())
}

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
/// the InjectFilter*Rule family. Per-rule code reads
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
    /// The plan node below the aggregate stack. Typically the
    /// `FilterExec` for filter+aggregate rules; per-rule code
    /// inspects it for the predicate.
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
