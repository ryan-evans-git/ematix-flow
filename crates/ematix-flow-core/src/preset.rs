//! Task #481, Shape B: a one-call setup for the dict-aware fast path.
//!
//! Users who want every optimisation ematix-flow exposes shouldn't
//! have to memorise the seven rules + the right `TableProvider` + the
//! right `with_dict_preservation` flag. This module gives them two
//! short calls:
//!
//! ```no_run
//! # async fn demo() -> datafusion::common::Result<()> {
//! use datafusion::execution::session_state::SessionStateBuilder;
//! use datafusion::prelude::{SessionConfig, SessionContext};
//! use ematix_flow_core::preset;
//!
//! let state = preset::with_optimizer_rules(
//!     SessionStateBuilder::new()
//!         .with_config(SessionConfig::new().with_target_partitions(14))
//!         .with_default_features(),
//! )
//! .build();
//! let ctx = SessionContext::new_with_state(state);
//! preset::register_dict_aware_parquet(&ctx, "lineitem", "data/lineitem.parquet")?;
//! # Ok(())
//! # }
//! ```
//!
//! The two halves are split so callers can mix and match — e.g. the
//! TPC-H triangulation bench installs rules but registers the table
//! itself via `FastParquetTableProvider` (different provider mix); a
//! production user might register many parquet tables but only want
//! the rule chain installed once.
//!
//! ## What "dict-aware fast path" means here
//!
//! - **Reader**: [`crate::ematix_fast_parquet::EmatixFastParquetTableProvider`]
//!   with `with_dict_preservation(true)` — string columns arrive as
//!   `Dictionary(UInt32, Utf8)` instead of `Utf8View`, which lets the
//!   dict-aware operators in the rule chain actually fire on TPC-H
//!   data (rather than silently no-op'ing as they would on default
//!   `DataSourceExec`/parquet-rs output — see memory
//!   `[[dict-arrival-blocker]]`).
//! - **Rules**: the canonical chain landed across Σ.D/Σ.E/Σ.G work —
//!   per-shape SQL injection rules + the dict-aware operator rule +
//!   the hand→JIT promoter. Each one is mutually-exclusive-by-design
//!   on the plan shapes it matches; together they cover the
//!   currently-recognised SUM-over-Filter, multi-agg + group-by, and
//!   string-COUNT-GROUP-BY shapes.

use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionContext;

use crate::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use crate::dict_aggregate_rule::EnableDictGroupCountRule;
use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
use crate::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use crate::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use crate::push_down_left_semi_rule::PushDownLeftSemiRule;
use crate::robin_hood_sum_f64_exec::EnableRobinHoodSumF64Rule;
use crate::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;
use crate::shared_subtree_exec::SharedSubtreeRegistry;

/// Attach ematix-flow's optimiser rule chain to a `SessionStateBuilder`.
///
/// Rules installed (in registration order):
///
/// 1. `DedupeAggregateForFloatDeterminism` — detects f64-aggregate
///    subtrees that appear twice in the same plan (TPC-H Q15's
///    `revenue_s` CTE shape) and wraps both in `SharedSubtreeExec` so
///    they share one cached computation. **Must register FIRST** —
///    `InjectFilterMultiAggRule` / `InjectFilterSumRule` would
///    otherwise consume one side of the duplicate pair (rewriting it
///    to a fused operator with a different structural hash) and leave
///    the other as the parallel-SUM source of non-determinism. The
///    rule is a no-op on the 21/22 TPC-H queries without duplicate
///    aggregates.
/// 2. `EnableDictGroupCountRule` — `AggregateExec(Final+Partial)` on
///    `Dictionary(UInt32, Utf8|Utf8View)` + `COUNT(*)` →
///    `DictGroupCountExec`. Runs after dedupe because its shape is a
///    strict subset of `InjectFilterMultiAggRule`'s matcher.
/// 3. `InjectFilterMultiAggRule` — generic filter + group-by + multi-
///    aggregate SQL pattern → `FusedAggregateExec<FilterMultiAggSpec>`.
/// 4. `InjectFilterSumRule` — generic SUM-over-Filter SQL →
///    `FusedAggregateExec<FilterSumSpec>`.
/// 5. `SwapSemiJoinBuildSideRule` — when one input to a semi/anti
///    join has stronger cardinality stats than the other, swap so
///    the smaller side becomes build. Closes Q18's inverted
///    build-side bug.
/// 6. `PushDownLeftSemiRule` (Σ.Q.L10, logical) — pushes a top-level
///    LeftSemi join through Inner joins down to the target table,
///    eliminating an N×M intermediate that would otherwise be built
///    first.
/// 7. `EnableRobinHoodSumF64Rule` (Σ.Q.L1b) — routes
///    SUM(Float64)/COUNT GROUP BY Int64 through `RobinHoodSumF64Exec`
///    which beats DataFusion's stock vectorised AggregateExec by 1-5%.
/// 8. `EnableRuntimeBloomSidebandRule` (Σ.Q.L9 / Σ.Q.L15) — threads
///    a runtime bloom between HashJoinExec build and probe-side
///    EmatixFastParquetExec, pruning the probe scan at decode time.
///    Configured with ratio=1024, allow_inner_join=true,
///    require_filtered_build=true (the milestone config).
///
/// Σ.V alignment (2026-05-26): rules 6/7/8 were previously default-on
/// in the bench but missing from this preset — library users got
/// different plans than what we benchmarked. Now aligned.
///
/// The dedupe rule's `SharedSubtreeRegistry` is freshly allocated per
/// call to `with_optimizer_rules` and lives for the resulting
/// `SessionState`. Multiple queries on the same `SessionContext`
/// reuse the same registry — so a repeat Q15 hits the cache populated
/// by the first run.
///
/// If you need to inspect / clear the cache from outside (tests,
/// admin tools), use [`with_optimizer_rules_and_registry`] instead
/// and keep the returned `Arc<SharedSubtreeRegistry>` handle.
pub fn with_optimizer_rules(builder: SessionStateBuilder) -> SessionStateBuilder {
    with_optimizer_rules_and_registry(builder).0
}

/// Same as [`with_optimizer_rules`], but also returns the
/// `SharedSubtreeRegistry` so callers can probe cache state across
/// queries. Useful for benches that want to verify cross-query hits,
/// or for clearing the cache after a table is re-registered with
/// different data.
pub fn with_optimizer_rules_and_registry(
    builder: SessionStateBuilder,
) -> (SessionStateBuilder, Arc<SharedSubtreeRegistry>) {
    let registry = Arc::new(SharedSubtreeRegistry::new());
    let builder = builder
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::with_registry(
            registry.clone(),
        )))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
        .with_physical_optimizer_rule(Arc::new(
            crate::swap_semi_join_build_rule::SwapSemiJoinBuildSideRule,
        ))
        // ForceCollectLeftForSemiBoundedBuildRule (REV.3): forces
        // PartitionMode::CollectLeft on Inner joins whose build side is
        // semi-bounded (shrunk by a pushed LeftSemi/RightSemi), then
        // re-runs EnforceDistribution to drop the now-unnecessary
        // probe-side repartition (the Σ.BS "repair partitioning after a
        // structural rewrite" pattern). Q18 SF=100: 6652 -> 398 ms
        // (16.7x), value-validated — all 22 queries PASS vs DuckDB at
        // SF=100 with this on. Runs right after SwapSemi so it sees the
        // RightSemi. Was opt-in (EMAT_FORCE_COLLECT_LEFT); now default-on
        // to match the settled decision + ship the banked win.
        .with_physical_optimizer_rule(Arc::new(
            crate::force_collect_left_semi_build_rule::ForceCollectLeftForSemiBoundedBuildRule::default(),
        ))
        // Σ.V (2026-05-26): align preset with the bench's milestone
        // config. The bench has these three rules default-on but
        // preset.rs was missing them — library users got materially
        // different plan shapes than what we benchmarked. See
        // [[Σ.V — preset alignment]].
        //
        // PushDownLeftSemiRule (Σ.Q.L10): LOGICAL rule that pushes a
        // top-level LeftSemi join through Inner joins down to its
        // target table, eliminating the N×M intermediate that would
        // otherwise be built first. Closes Q18-shape gap to DuckDB
        // when the rule was introduced (memory: Q18 SF=10 -54% at
        // commit 50825c9, before other levers stacked).
        .with_optimizer_rule(Arc::new(PushDownLeftSemiRule))
        // EnableRobinHoodSumF64Rule (Σ.Q.L1b): routes
        // SUM(Float64)/COUNT GROUP BY Int64 through
        // RobinHoodSumF64Exec which beats DataFusion's stock
        // vectorised AggregateExec by 1-5% (memory: project_sigma_nf3_beats_stock).
        .with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule::default()))
        // EnableRuntimeBloomSidebandRule (Σ.Q.L9 / Σ.Q.L15):
        // threads a runtime bloom sideband between HashJoinExec
        // build and probe-side EmatixFastParquetExec. The
        // milestone config uses ratio=1024 (gates out L4'-style
        // net-negative s⋈l firings while keeping small-dim→fact
        // wins), allow_inner_join=true (Σ.Q.L15), and
        // require_filtered_build=true (don't bloom on FK joins —
        // [[l9_bloom_consumer_findings]]).
        .with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
            min_probe_to_build_ratio: 1024,
            allow_inner_join: true,
            require_filtered_build: true,
            // Σ.AH.3 Story 2a: per-partition absolute build-size ceiling.
            // OPT-IN — default 0 (disabled). Story 2a measured no baseline
            // wall-time effect (existing require_filtered_build + ratio gate
            // already screen out FK-bloom emits; this gate only sees Inner
            // joins with pre-filtered builds which turn out to be net-positive).
            // Override via `EMAT_L9_MAX_EXPECTED_KEYS=N` if a future shape
            // exposes a regression the existing gates miss.
            max_expected_keys_per_partition: 0,
            // L9.WIDTH (2026-06-10): probe-payoff gate, env-defaulted (4).
            // See the field docs in runtime_bloom_sideband_rule.rs.
            min_probe_proj_cols: EnableRuntimeBloomSidebandRule::default().min_probe_proj_cols,
        }));
    // Σ.AJ.1 Lever B POC: opt-in via EMAT_L9_BROADCAST_SIBLINGS=1.
    // Default OFF. See `crates/ematix-flow-core/src/broadcast_sibling_blooms_rule.rs`.
    let builder = if std::env::var_os("EMAT_L9_BROADCAST_SIBLINGS").is_some() {
        crate::broadcast_sibling_blooms_rule::install_broadcast_sibling_blooms_rule(builder)
    } else {
        builder
    };
    // Σ.BR Phase 2 / #194 (2026-05-29): production wiring for the ematix
    // pre-plan walker pipeline (agg_semi → dim_push → reorder). These walkers
    // were previously applied only in the bench harness, so library users got
    // a different (slower) plan regime for the queries that use them
    // (Q17/Q08/Q18 via agg_semi, Q10 via dim_push, Q05 via reorder). Install
    // them as a QueryPlanner (NOT OptimizerRules — that would re-open the 5–8%
    // codegen-tax, [[optimizer-codegen-sensitivity]]) so they run
    // post-optimization, matching the validated bench config. Each step is
    // self-gated inside FlowQueryPlanner (EMAT_AGG_SEMI / EMAT_DIM_PUSH /
    // EMAT_REORDER_QP, all default ON, opt-OUT). Installed unless all three
    // are disabled.
    let flow_qp_on = ["EMAT_AGG_SEMI", "EMAT_DIM_PUSH", "EMAT_REORDER_QP"]
        .iter()
        .any(|var| {
            std::env::var(var)
                .ok()
                .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
                .unwrap_or(true)
        });
    let builder = if flow_qp_on {
        builder.with_query_planner(Arc::new(crate::flow_query_planner::FlowQueryPlanner))
    } else {
        builder
    };
    (builder, registry)
}

/// Register a parquet file as a table on `ctx` via
/// `EmatixFastParquetTableProvider` with dict preservation enabled.
///
/// Two differences from bare `SessionContext::register_parquet`:
///
/// 1. The reader is the ematix-parquet-backed
///    `EmatixFastParquetTableProvider` rather than DataFusion's default
///    parquet integration. This gives us the row-group-parallel scan,
///    bitmap-driven predicate pushdown, and direct Arrow-bridge path
///    that DataFusion's default doesn't expose.
/// 2. `with_dict_preservation(true)` is set, so Utf8 columns arrive as
///    `Dictionary(UInt32, Utf8)` at the Arrow surface. This is the
///    precondition for `EnableDictGroupCountRule` (and future dict-aware
///    operators) to actually fire on real TPC-H data — see the
///    `[[dict-arrival-blocker]]` memory note for the history.
///
/// Pair with [`with_optimizer_rules`] on the same `SessionStateBuilder`
/// before constructing the `SessionContext` — see the module-level
/// example.
pub fn register_dict_aware_parquet(
    ctx: &SessionContext,
    name: &str,
    path: impl Into<String>,
) -> DfResult<()> {
    let prov = EmatixFastParquetTableProvider::try_new(path)?.with_dict_preservation(true);
    ctx.register_table(name, Arc::new(prov))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionConfig;
    use std::path::PathBuf;

    fn sf1_lineitem() -> Option<String> {
        let env = std::env::var("TPCH_DATA_DIR").ok().map(PathBuf::from);
        let dir = match env {
            Some(p) if p.exists() => p,
            _ => {
                let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                let real = manifest.parent()?.parent()?.join("examples/tpch/data/sf1");
                if real.exists() {
                    real
                } else {
                    PathBuf::from(crate::test_support::tpch_mini_dir())
                }
            }
        };
        let p = dir.join("lineitem.parquet");
        p.exists().then(|| p.to_string_lossy().into_owned())
    }

    /// The preset's reader emits `Dictionary(UInt32, Utf8)` for string
    /// columns. (Without the preset users get Utf8View from bare
    /// FastParquet / default DataFusion — verified by
    /// `examples/probe_dict_arrival`.)
    #[tokio::test(flavor = "multi_thread")]
    async fn register_dict_aware_parquet_emits_dictionary_for_string_columns() {
        use datafusion::arrow::datatypes::DataType;

        let Some(path) = sf1_lineitem() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx = SessionContext::new();
        register_dict_aware_parquet(&ctx, "lineitem", path).unwrap();
        let provider = ctx.table_provider("lineitem").await.unwrap();
        let schema = provider.schema();
        let returnflag = schema.field_with_name("l_returnflag").unwrap();
        assert!(
            matches!(returnflag.data_type(), DataType::Dictionary(_, _)),
            "l_returnflag should be Dictionary, got {:?}",
            returnflag.data_type()
        );
    }

    /// End-to-end: with the preset's rule chain + dict-aware provider,
    /// a `COUNT(*) GROUP BY string_col` query routes through
    /// `DictGroupCountExec`. Without the preset, that rewrite no-ops
    /// because the scan emits Utf8View.
    #[tokio::test(flavor = "multi_thread")]
    async fn preset_activates_dict_group_count_exec_end_to_end() {
        let Some(path) = sf1_lineitem() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let state = with_optimizer_rules(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(14))
                .with_default_features(),
        )
        .build();
        let ctx = SessionContext::new_with_state(state);
        register_dict_aware_parquet(&ctx, "lineitem", path).unwrap();

        let plan = ctx
            .sql("SELECT l_returnflag, COUNT(*) FROM lineitem GROUP BY l_returnflag")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_str.contains("DictGroupCountExec"),
            "preset didn't activate the dict-aware operator.\nPlan:\n{plan_str}"
        );
    }

    /// BF (2026-06-09): caching is now PER-QUERY (the dedupe rule allocates a
    /// fresh registry per `optimize()` call). The externally-exposed registry
    /// handle from `with_optimizer_rules_and_registry` therefore stays EMPTY —
    /// cross-query caching was removed because it degraded long-lived contexts
    /// (accumulation) and served stale memoized results. Two Q15-shape queries
    /// on the same ctx must each return 1 row (within-query CSE still works),
    /// and the handle must remain empty (no cross-query accumulation).
    #[tokio::test(flavor = "multi_thread")]
    async fn registry_handle_stays_empty_caching_is_per_query() {
        use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;

        let schema = Arc::new(Schema::new(vec![
            Field::new("supplier", DataType::Int64, false),
            Field::new("revenue", DataType::Float64, false),
        ]));
        let mut suppliers: Vec<i64> = Vec::new();
        let mut revenues: Vec<f64> = Vec::new();
        for s in 0..14_i64 {
            for r in 0..100_i64 {
                suppliers.push(s);
                revenues.push((r as f64 + 1.0) * 0.1 + (s as f64) * 17.3);
            }
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(suppliers)),
                Arc::new(Float64Array::from(revenues)),
            ],
        )
        .unwrap();
        let mt = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

        let (builder, registry) = with_optimizer_rules_and_registry(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_default_features(),
        );
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_table("revenue_t", mt).unwrap();

        let sql = "
            WITH r AS (
                SELECT supplier, sum(revenue) AS total
                FROM revenue_t
                GROUP BY supplier
            )
            SELECT r.supplier, r.total
            FROM r
            WHERE r.total = (SELECT max(total) FROM r)
        ";

        assert_eq!(registry.len(), 0, "registry handle starts empty");
        let n1: usize = ctx
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(n1, 1, "Q15-shape returns one row (within-query CSE works)");
        assert_eq!(
            registry.len(),
            0,
            "external registry handle stays empty — caching is per-query, not cross-query",
        );

        let n2: usize = ctx
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(
            n2, 1,
            "second run independently correct (no stale memoization)"
        );
        assert_eq!(
            registry.len(),
            0,
            "registry handle still empty after a second query (no cross-query accumulation)",
        );
    }

    /// Inverse: without `register_dict_aware_parquet` (using bare
    /// DataFusion `register_parquet`), the dict-aware operator does NOT
    /// fire even with the rule chain installed — the scan emits
    /// Utf8View which the rule correctly rejects. Documents the
    /// substrate dependency.
    #[tokio::test(flavor = "multi_thread")]
    async fn preset_rules_alone_do_not_activate_dict_path_on_default_reader() {
        let Some(path) = sf1_lineitem() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let state = with_optimizer_rules(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(14))
                .with_default_features(),
        )
        .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_parquet("lineitem", &path, Default::default())
            .await
            .unwrap();

        let plan = ctx
            .sql("SELECT l_returnflag, COUNT(*) FROM lineitem GROUP BY l_returnflag")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            !plan_str.contains("DictGroupCountExec"),
            "dict-aware operator wrongly fired without dict-preserving reader.\nPlan:\n{plan_str}"
        );
    }
}
