//! Σ.BR Phase 2 / #194 — production wiring for the ematix pre-plan walkers.
//!
//! [`FlowQueryPlanner`] wraps DataFusion's `DefaultPhysicalPlanner`: it applies
//! the ematix logical-plan walker pipeline to the already-optimized
//! `LogicalPlan` — in the same order the bench harness applies them —
//!
//!   1. `agg_semi`  (`agg_filter_pushdown::push_filter_into_agg`)
//!   2. `dim_push`  (`dim_join_pushdown::push_dim_join_into_chain`)
//!   3. `reorder`   (`join_reorder::reorder_inner_joins_shape_gated`)
//!
//! then delegates to the default physical planner. Installed via
//! `SessionStateBuilder::with_query_planner` in [`crate::preset`], so library
//! users (anyone through `preset::with_optimizer_rules`) get the same
//! pre-plan rewrites the bench validates — previously these walkers ran ONLY
//! in the bench harness, leaving the library path in a different (slower) plan
//! regime for the queries that use them (Q17/Q08/Q18 via agg_semi, Q10 via
//! dim_push, Q05 via reorder).
//!
//! ## Why a `QueryPlanner` and not `OptimizerRule`s
//!
//! Registering each walker as an `OptimizerRule` joins the optimizer's
//! compiled rule loop, which has empirically cost 5–8% geomean across
//! *unrelated* queries from LLVM codegen perturbation (Σ.H.1d.4 / Σ.K.A /
//! Σ.F-T2 — the `optimizer-codegen-sensitivity` note). A QueryPlanner runs
//! once per query at physical-planning time, *outside* that loop, and
//! **post-optimization** — exactly where the bench applies them, so the
//! production path reproduces the validated configuration.
//!
//! Each step is best-effort (falls back to the prior plan on error) and
//! env-gated to mirror the bench's flags, all default ON (opt-OUT):
//!   - `EMAT_AGG_SEMI=0`   disables the agg-semi pushdown
//!   - `EMAT_DIM_PUSH=0`   disables the dim-join pushdown
//!   - `EMAT_REORDER_QP=0` disables the join reorder

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

/// `true` unless the env var is set to `0`/`false` (default ON, opt-out).
fn enabled(var: &str) -> bool {
    std::env::var(var)
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true)
}

/// A [`QueryPlanner`] that applies the ematix pre-plan walker pipeline
/// (agg-semi → dim-push → reorder) to the optimized logical plan before
/// physical planning, then delegates to the stock `DefaultPhysicalPlanner`.
#[derive(Debug, Default)]
pub struct FlowQueryPlanner;

impl FlowQueryPlanner {
    /// Apply the enabled walkers in bench order. Pure function over the plan
    /// so it can be unit-tested without a physical planner. Each step is
    /// best-effort: an `Err` leaves the plan from the previous step intact.
    pub fn rewrite(plan: LogicalPlan) -> LogicalPlan {
        let mut plan = plan;
        if enabled("EMAT_AGG_SEMI") {
            if let Ok(p) = crate::agg_filter_pushdown::push_filter_into_agg(plan.clone()) {
                plan = p;
            }
        }
        if enabled("EMAT_DIM_PUSH") {
            if let Ok(p) = crate::dim_join_pushdown::push_dim_join_into_chain(plan.clone()) {
                plan = p;
            }
        }
        if enabled("EMAT_REORDER_QP") {
            if let Ok(p) = crate::join_reorder::reorder_inner_joins_shape_gated(plan.clone()) {
                plan = p;
            }
        }
        plan
    }
}

#[async_trait]
impl QueryPlanner for FlowQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planned = Self::rewrite(logical_plan.clone());
        DefaultPhysicalPlanner::default()
            .create_physical_plan(&planned, session_state)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};
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
        p.exists().then_some(p)
    }

    async fn ctx_with_planner() -> Option<(SessionContext, PathBuf)> {
        let dir = sf1_dir()?;
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_default_features()
            .with_query_planner(Arc::new(FlowQueryPlanner))
            .build();
        let ctx = SessionContext::new_with_state(state);
        for t in [
            "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
        ] {
            let path = dir.join(format!("{t}.parquet"));
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy()).ok()?;
            ctx.register_table(t, Arc::new(prov)).ok()?;
        }
        Some((ctx, dir))
    }

    /// The planner must plan every TPC-H query through the library path
    /// without error (the walkers are best-effort + result-preserving). A
    /// crash or planning error on any shape would surface here.
    #[tokio::test]
    async fn plans_all_tpch_queries_through_library_path() {
        let Some((ctx, dir)) = ctx_with_planner().await else {
            eprintln!("skipping: sf1 data missing");
            return;
        };
        for q in 1..=22u8 {
            let path = dir
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join(format!("queries/q{q:02}.sql"));
            let Ok(sql) = std::fs::read_to_string(&path) else {
                continue;
            };
            let df = ctx.sql(&sql).await.unwrap_or_else(|e| panic!("Q{q:02} sql: {e}"));
            let plan = df
                .create_physical_plan()
                .await
                .unwrap_or_else(|e| panic!("Q{q:02} physical plan: {e}"));
            let rendered = format!("{}", displayable(plan.as_ref()).indent(true));
            assert!(!rendered.is_empty(), "Q{q:02} produced an empty plan");
        }
    }
}
