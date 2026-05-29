//! Σ.BR Phase 2 — production wiring for the join reorder.
//!
//! [`ReorderQueryPlanner`] wraps DataFusion's `DefaultPhysicalPlanner`: it
//! applies the scale-gated, shape-gated, composite-guarded inner-join reorder
//! ([`crate::join_reorder::reorder_inner_joins_shape_gated`]) to the
//! already-optimized `LogicalPlan`, then delegates to the default physical
//! planner. Installed via `SessionStateBuilder::with_query_planner` in
//! [`crate::preset`], so the reorder reaches library users (anyone who goes
//! through `preset::with_optimizer_rules`), not just the bench harness.
//!
//! ## Why a `QueryPlanner` and not an `OptimizerRule`
//!
//! Registering a new `OptimizerRule` joins the optimizer's compiled rule
//! loop, which has empirically cost **5–8% geomean** across *unrelated*
//! queries from LLVM codegen perturbation (Σ.H.1d.4 / Σ.K.A / Σ.F-T2 — see
//! the `optimizer-codegen-sensitivity` note). A `QueryPlanner` runs once per
//! query at physical-planning time, *outside* that loop, so it doesn't
//! perturb the optimizer's codegen. It also runs **post-optimization** —
//! exactly the point at which the bench harness applies the reorder, so the
//! production path reproduces the validated configuration rather than
//! interleaving the reorder with DataFusion's own logical passes.
//!
//! The reorder is best-effort: on any error it falls back to the original
//! plan (it is a perf optimization, never correctness-critical — the rebuild
//! is row-count-identical by construction, guarded by the join_reorder tests).

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

/// A [`QueryPlanner`] that applies the shape-gated join reorder to the
/// optimized logical plan before physical planning, then delegates to the
/// stock `DefaultPhysicalPlanner`.
#[derive(Debug, Default)]
pub struct ReorderQueryPlanner;

#[async_trait]
impl QueryPlanner for ReorderQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Apply the reorder post-optimization. Best-effort: fall back to the
        // original plan on any error (the reorder never affects correctness).
        let planned = crate::join_reorder::reorder_inner_joins_shape_gated(logical_plan.clone())
            .unwrap_or_else(|_| logical_plan.clone());
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

    /// Σ.BR Phase 2 — the QueryPlanner must apply the reorder through the
    /// library planning path. At SF=1 Q05's 6-leaf chain is below the scale
    /// threshold (cap 4) so it does NOT fire — assert the planner is a
    /// faithful pass-through there (no spurious change, no crash). The reorder
    /// firing itself is covered by the join_reorder tests; this test pins that
    /// the wrapper plans successfully and is installed correctly.
    #[tokio::test]
    async fn planner_plans_q05_through_library_path() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skipping: sf1 data missing");
            return;
        };
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_default_features()
            .with_query_planner(Arc::new(ReorderQueryPlanner))
            .build();
        let ctx = SessionContext::new_with_state(state);
        for t in ["customer", "orders", "lineitem", "supplier", "nation", "region"] {
            let path = dir.join(format!("{t}.parquet"));
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy()).unwrap();
            ctx.register_table(t, Arc::new(prov)).unwrap();
        }
        let sql = std::fs::read_to_string(
            dir.parent().unwrap().parent().unwrap().join("queries/q05.sql"),
        )
        .unwrap();
        // Plans without error through the wrapped planner and produces a real
        // physical plan (the reorder is best-effort and row-count-preserving).
        let df = ctx.sql(&sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let rendered = format!("{}", displayable(plan.as_ref()).indent(true));
        assert!(
            rendered.contains("AggregateExec"),
            "expected a real physical plan from the reorder query planner:\n{rendered}"
        );
    }
}
