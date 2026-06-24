//! Σ.BR Phase 2 / #194 — production wiring for the ematix pre-plan walkers.
//!
//! [`FlowQueryPlanner`] wraps DataFusion's `DefaultPhysicalPlanner`: it applies
//! the ematix logical-plan walker pipeline to the already-optimized
//! `LogicalPlan` — in the same order the bench harness applies them —
//!
//!   1. `agg_semi`  (`agg_filter_pushdown::push_filter_into_agg`)
//!   2. `dim_push`  (`dim_join_pushdown::push_dim_join_into_chain`)
//!   3. `q20_semi`  (`agg_filter_pushdown::push_transitive_semi_into_agg`)
//!   4. `reorder`   (`join_reorder::reorder_inner_joins_shape_gated`)
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
//!   - `EMAT_Q20_SEMI=0`   disables the q20 transitive semi-pushdown
//!   - `EMAT_TRANSITIVE_DIM_SEMI=0` disables the Q05 transitive dim-semi splice
//!   - `EMAT_REORDER_QP=0` disables the join reorder

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

// Default-ON gate reader (opt out with `=0`/`false`). Canonical definition lives
// in `crate::flags`; re-exported here so the planner's gate call sites are unchanged.
use crate::flags::enabled;

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
        // Σ.Q20 / Q20.WIRE (EMAT_Q20_SEMI=0 to disable): transitive semi-pushdown.
        // Mirrors the bench order (agg_semi → dim_push → q20_semi). When a
        // correlated agg's join key is transitively constrained by a semi-join
        // to a small filtered dim (Q20: lineitem⋉partsupp⋉forest-parts), it
        // splices a `LeftSemi(lineitem, forest_parts, l_partkey=p_partkey)` below
        // the correlated agg — the −34% SF=10 Q20 win the bench measured but the
        // preset previously did NOT ship. Inert on every other TPC-H shape
        // (proven by `agg_filter_pushdown::tests::transitive_no_op_on_q02_q17`
        // + `rewrite_preserves_q20_result`). Best-effort; runs before reorder.
        if enabled("EMAT_Q20_SEMI") {
            if let Ok(p) = crate::agg_filter_pushdown::push_transitive_semi_into_agg(plan.clone()) {
                plan = p;
            }
        }
        // Σ.Q05 / #352 (DEFAULT-ON, opt-out EMAT_TRANSITIVE_DIM_SEMI=0):
        // transitive dim-semi into deep join inputs. When a filtered dim
        // constrains a mid-dim scan and the key class reaches a deep scan only
        // transitively (Q05: customer via c_nationkey = s_nationkey =
        // n_nationkey), splice `T ⋉ (mid-dim ⋈ dim)` below — Q05's
        // orders⋈customer build drops 2.28M → 456K keys at SF=10, which the L9
        // tight rescue (deep-semi cap) then turns into a ~4%-pass-rate lineitem
        // scan wrap (60M → 2.56M), DuckDB's runtime-scan-filter shape. No-op on
        // the other 21 TPC-H shapes (pinned by
        // `transitive_join_semi_no_op_on_direct_dim_shapes` + verified Q17
        // plan-identity). Flipped default-on after the 2026-06-21 22q SF=10
        // sweep gate: Q05 147→131 ms (DuckDB 141) = the 22nd SF=10 win; depends
        // on the build_side_bloom drop-cap fix (#159) so the 456K-key build
        // emits a right-sized (~0.85% FP) bloom rather than a saturated one.
        if enabled("EMAT_TRANSITIVE_DIM_SEMI") {
            if let Ok(p) =
                crate::agg_filter_pushdown::push_transitive_dim_semi_into_join_chain(plan.clone())
            {
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
        // Scalar-agg partition oversubscription (morsel-engine down-payment,
        // 2026-06-20): a query whose result is a SCALAR aggregate (no GROUP BY
        // — TPC-H Q06/Q14/Q17/Q19) plans with 2× `target_partitions` so the
        // upstream scan-decode and joins parallelize (hiding per-row-group
        // decode latency) while the single-row final merge stays ~free.
        // Measured M4 Max SF=10: Q17 −28%, Q06 −10%, no regressions. Disjoint
        // from every GROUP BY query by construction (`is_scalar_aggregation`
        // matches ONLY empty group_expr), so it cannot touch the
        // high-cardinality aggregations that regress under oversubscription.
        //
        // Mechanism: clone the session state with the boosted count and thread
        // it through BOTH the re-optimize below AND the physical planner, so
        // `EnforceDistribution` propagates the count and each `scan()` reads it
        // (the scan's partition count = min(num_row_groups, target_partitions)).
        // Opt out: `EMAT_SCALAR_AGG_BOOST=0`.
        let session_default = session_state.config().options().execution.target_partitions;
        let boosted =
            crate::auto_target_partitions::scalar_agg_target_partitions(&planned, session_default);
        let boosted_state = if boosted > session_default {
            let mut s = session_state.clone();
            s.config_mut().options_mut().execution.target_partitions = boosted;
            Some(s)
        } else {
            None
        };
        let session_state = boosted_state.as_ref().unwrap_or(session_state);
        // Σ.BR Phase 2 / #194b (2026-05-29): re-optimize the rewritten plan.
        // `SessionState::create_physical_plan` optimizes the plan and *then*
        // invokes the query planner, so the plan we receive is already
        // optimized — but our walkers (esp. the reorder) restructure the joins
        // AFTER that pass, leaving filters/projections positioned for the old
        // order. The bench re-optimizes implicitly (it applies the walkers,
        // then `execute_logical_plan(...).collect()` re-runs the optimizer on
        // the reordered plan); the QueryPlanner does not. Without this, the
        // reordered Q05 funnel ran ~0.5s slower through preset than the bench
        // and the −14.7% SF=100 win evaporated to ~neutral. Re-running the
        // logical optimizer here re-pushes filters/projections through the new
        // join order, matching the bench. Best-effort: fall back to the
        // un-re-optimized plan on error (correctness is unaffected either way).
        let planned = session_state.optimize(&planned).unwrap_or(planned);

        // prod-C (EMAT_LATE_MAT_AGG=1, default OFF): wide-string late
        // materialization. When the optimized plan exposes an FD-reducible wide
        // aggregate over a star whose anchor PK functionally determines the wide
        // group columns (Q10: customer⋈nation → 7 wide group cols), rewrite the
        // Aggregate into a `LateMatAggNode` and plan it with the
        // `LateMatAggPlanner`: the wide strings are carried only as a u32
        // build-rowid through the join + aggregate and gathered at the (far
        // smaller) aggregate outputs from the resident build — no re-scan.
        // Best-effort: `reconstruct` returns None on every other shape, and the
        // extension planner is registered ONLY on this gated path, so the
        // default planner stays byte-identical when the flag is off.
        if crate::late_mat_agg::enabled() {
            if let Some(rewritten) = crate::late_mat_agg::reconstruct(&planned) {
                let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                    crate::late_mat_agg_planner::LateMatAggPlanner,
                )]);
                let plan = planner
                    .create_physical_plan(&rewritten, session_state)
                    .await?;
                // Query-scoped large batch: the LateGather reattach (Utf8View
                // interleave over the retained build batches) AND the probe fact
                // decode are cheap only at a large batch — but a GLOBAL bump
                // regresses the other 21 queries (prod-D). Wrap ONLY this query's
                // root in a BatchSizeOverrideExec so the large batch is scoped to
                // the recognized late-mat query (Q10): the win lands at the
                // session default batch with no 22q regression. Default 1M;
                // `EMAT_LATE_MAT_BATCH=0` disables (use the session batch, for A/B).
                let batch = crate::flags::usize_or("EMAT_LATE_MAT_BATCH", 1_048_576);
                if batch > 0 {
                    return Ok(Arc::new(
                        crate::batch_size_override_exec::BatchSizeOverrideExec::new(plan, batch),
                    ));
                }
                return Ok(plan);
            }
        }

        // PV.3b (EMAT_PUSH_PIPELINE=1, default OFF): rebuild the star's
        // fact-probing dim groups as a single fused push pipeline. The reorder
        // runs AFTER the logical optimizer (so the optimizer never sees — and
        // can't undo — the FusedProbeNode) and is planned by FusedProbePlanner.
        // Best-effort: reconstruct returns None on any unsupported shape, and
        // the FusedProbePlanner is registered only on this gated path so the
        // default planner stays byte-identical when OFF.
        if crate::push_fusion_rule::enabled() {
            let planned = match crate::push_fusion_rule::reconstruct(&planned) {
                Some(p) => p,
                None => planned,
            };
            let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                crate::fused_probe_node::FusedProbePlanner,
            )]);
            let plan = planner
                .create_physical_plan(&planned, session_state)
                .await?;
            return match crate::fuse_push_pipeline_rule::fuse_push_pipelines(plan.clone()) {
                Ok(fused) => Ok(fused),
                Err(_) => Ok(plan),
            };
        }

        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(&planned, session_state)
            .await?;
        // PV.3 (EMAT_PUSH_PIPELINE=1, default OFF): fuse CollectLeft
        // membership-reduction joins on a sideband-free EMAT fact scan into
        // EmatPushPipelineExec. Runs LAST, after the physical optimizer has
        // assigned partition modes + any L9 sideband (its gates read those).
        // Best-effort: a transform error falls back to the stock plan.
        match crate::fuse_push_pipeline_rule::fuse_push_pipelines(plan.clone()) {
            Ok(fused) => Ok(fused),
            Err(_) => Ok(plan),
        }
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

    /// Q10 tables registered WITH declared PKs (customer/orders/nation) +
    /// FlowQueryPlanner — the production path the prod-C late-mat wiring needs.
    async fn q10_ctx_with_planner() -> Option<(SessionContext, PathBuf)> {
        let dir = sf1_dir()?;
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_default_features()
            .with_query_planner(Arc::new(FlowQueryPlanner))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let reg = |t: &str, pk: Option<usize>| {
            let path = dir.join(format!("{t}.parquet"));
            let mut p =
                EmatixFastParquetTableProvider::try_new(path.to_string_lossy().into_owned())
                    .unwrap();
            if let Some(i) = pk {
                p = p.with_primary_key(vec![i]);
            }
            ctx.register_table(t, Arc::new(p)).unwrap();
        };
        reg("customer", Some(0));
        reg("orders", Some(0));
        reg("lineitem", None);
        reg("nation", Some(0));
        Some((ctx, dir))
    }

    /// prod-C WIRING: through the production `FlowQueryPlanner`, with
    /// `EMAT_LATE_MAT_AGG=1` Q10 plans into the late-materialization subtree
    /// (EmatixHashJoinExec + LateGatherExec) AND returns results identical to the
    /// flag-off (stock) path. With the flag off the plan is unchanged (no
    /// LateGatherExec). This is the sole test mutating `EMAT_LATE_MAT_AGG`.
    #[tokio::test]
    async fn flow_planner_wires_late_mat_agg_on_q10() {
        use datafusion::arrow::array::{Array, Float64Array};
        use datafusion::physical_plan::collect;

        let Some((ctx, dir)) = q10_ctx_with_planner().await else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let sql = std::fs::read_to_string("examples/tpch/queries/q10.sql")
            .or_else(|_| std::fs::read_to_string(dir.join("../../queries/q10.sql")))
            .unwrap();
        let sql = sql.trim().trim_end_matches(';');

        let checksum = |batches: &[datafusion::arrow::record_batch::RecordBatch]| {
            let (mut rows, mut sum) = (0usize, 0.0f64);
            for b in batches {
                rows += b.num_rows();
                if let Ok(i) = b.schema().index_of("revenue") {
                    if let Some(a) = b.column(i).as_any().downcast_ref::<Float64Array>() {
                        for j in 0..a.len() {
                            if a.is_valid(j) {
                                sum += a.value(j);
                            }
                        }
                    }
                }
            }
            (rows, sum)
        };

        // SAFETY: test-local toggle; no other test reads/writes EMAT_LATE_MAT_AGG.
        // The flag is now DEFAULT-ON (opt-out), so the OFF arm must set `=0`
        // explicitly (removing the var would leave it on).
        unsafe { std::env::set_var("EMAT_LATE_MAT_AGG", "0") };
        let off_plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let off_dump = format!("{}", displayable(off_plan.as_ref()).indent(true));
        assert!(
            !off_dump.contains("LateGatherExec"),
            "flag OFF must NOT rewrite the plan:\n{off_dump}"
        );
        let off_out = collect(off_plan, ctx.task_ctx()).await.unwrap();

        unsafe { std::env::set_var("EMAT_LATE_MAT_AGG", "1") };
        let on_plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let on_dump = format!("{}", displayable(on_plan.as_ref()).indent(true));
        unsafe { std::env::remove_var("EMAT_LATE_MAT_AGG") };

        assert!(
            on_dump.contains("LateGatherExec") && on_dump.contains("EmatixHashJoinExec"),
            "flag ON must wire the late-mat subtree:\n{on_dump}"
        );
        let on_out = collect(on_plan, ctx.task_ctx()).await.unwrap();

        let (or, os) = checksum(&off_out);
        let (nr, ns) = checksum(&on_out);
        assert_eq!(or, nr, "row count: stock {or} vs late-mat {nr}");
        assert!(
            (os - ns).abs() < os.abs() * 1e-9 + 1e-6,
            "revenue sum: stock {os:.4} vs late-mat {ns:.4}"
        );
        assert!(or > 0, "sanity: Q10 SF1 returns rows");
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
            let df = ctx
                .sql(&sql)
                .await
                .unwrap_or_else(|e| panic!("Q{q:02} sql: {e}"));
            let plan = df
                .create_physical_plan()
                .await
                .unwrap_or_else(|e| panic!("Q{q:02} physical plan: {e}"));
            let rendered = format!("{}", displayable(plan.as_ref()).indent(true));
            assert!(!rendered.is_empty(), "Q{q:02} produced an empty plan");
        }
    }

    /// Q20.WIRE: production (preset) must SHIP the q20 transitive-semi win the
    /// bench measured (−34% SF=10 Q20). `FlowQueryPlanner::rewrite` must apply
    /// `push_transitive_semi_into_agg`, which on the Q20 shape splices an extra
    /// `LeftSemi` (keyed on `l_partkey`) below the lineitem correlated agg.
    /// Compared against the exact PRE-wiring chain (agg_semi → dim_push →
    /// reorder), the wired rewrite must produce strictly more LeftSemi joins.
    #[tokio::test]
    async fn rewrite_applies_q20_transitive_semi() {
        let Some((ctx, dir)) = ctx_with_planner().await else {
            eprintln!("skipping: sf1 data missing");
            return;
        };
        let path = dir
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("queries/q20.sql");
        let Ok(sql) = std::fs::read_to_string(&path) else {
            eprintln!("skipping: q20.sql missing");
            return;
        };
        let optimized = ctx.sql(&sql).await.unwrap().into_optimized_plan().unwrap();

        // The exact PRE-Q20.WIRE chain (agg_semi → dim_push → reorder).
        let mut no_q20 = optimized.clone();
        no_q20 = crate::agg_filter_pushdown::push_filter_into_agg(no_q20.clone()).unwrap_or(no_q20);
        no_q20 =
            crate::dim_join_pushdown::push_dim_join_into_chain(no_q20.clone()).unwrap_or(no_q20);
        no_q20 =
            crate::join_reorder::reorder_inner_joins_shape_gated(no_q20.clone()).unwrap_or(no_q20);
        let no_q20_dump = format!("{}", no_q20.display_indent());

        let after = format!("{}", FlowQueryPlanner::rewrite(optimized).display_indent());

        assert!(
            after.matches("LeftSemi").count() > no_q20_dump.matches("LeftSemi").count(),
            "FlowQueryPlanner::rewrite must apply the q20 transitive semi (an added LeftSemi on \
             l_partkey vs the no-q20 chain).\nno_q20:\n{no_q20_dump}\nafter:\n{after}"
        );
    }

    /// Σ.Q05 / #352: the planner rewrite splices `customer ⋉ (nation ⋈
    /// region[ASIA])` into Q05 BY DEFAULT (the 2026-06-21 22q SF=10 sweep gate
    /// flipped it default-on — it turns Q05 from a ~7% DuckDB loss into a
    /// ~1.07× win and is plan-inert on the other 21 shapes). Opt-out via
    /// `EMAT_TRANSITIVE_DIM_SEMI=0`.
    #[tokio::test]
    async fn rewrite_applies_q05_transitive_dim_semi_default_on() {
        let Some((ctx, dir)) = ctx_with_planner().await else {
            eprintln!("skipping: sf1 data missing");
            return;
        };
        let path = dir
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("queries/q05.sql");
        let Ok(sql) = std::fs::read_to_string(&path) else {
            eprintln!("skipping: q05.sql missing");
            return;
        };
        let optimized = ctx.sql(&sql).await.unwrap().into_optimized_plan().unwrap();

        // SAFETY: test-local toggle; the only env reader is rewrite(), and this
        // is the sole test mutating EMAT_TRANSITIVE_DIM_SEMI.
        // Default (env unset): the splice fires.
        unsafe { std::env::remove_var("EMAT_TRANSITIVE_DIM_SEMI") };
        let on = format!(
            "{}",
            FlowQueryPlanner::rewrite(optimized.clone()).display_indent()
        );
        // Opt-out (env=0): the splice stays off.
        unsafe { std::env::set_var("EMAT_TRANSITIVE_DIM_SEMI", "0") };
        let off = format!("{}", FlowQueryPlanner::rewrite(optimized).display_indent());
        unsafe { std::env::remove_var("EMAT_TRANSITIVE_DIM_SEMI") };

        assert!(
            on.contains("customer.c_nationkey = nation.n_nationkey"),
            "splice must fire BY DEFAULT (default-on):\n{on}"
        );
        assert!(
            !off.contains("customer.c_nationkey = nation.n_nationkey"),
            "EMAT_TRANSITIVE_DIM_SEMI=0 must opt OUT:\n{off}"
        );
        assert!(
            on.matches("LeftSemi").count() > off.matches("LeftSemi").count(),
            "the splice must ADD a LeftSemi when on.\noff:\n{off}\non:\n{on}"
        );
    }
}
