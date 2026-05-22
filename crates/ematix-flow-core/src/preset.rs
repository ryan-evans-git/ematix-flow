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
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
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

    /// `with_optimizer_rules_and_registry` exposes the registry so
    /// callers can probe cross-query cache hits. Two Q15-shape queries
    /// on the same SessionContext should produce 1 row each and the
    /// registry should hold ≥1 entry after the first query (the
    /// duplicated f64 aggregate), with no growth on the second.
    #[tokio::test(flavor = "multi_thread")]
    async fn registry_handle_observes_cross_query_cache_hit() {
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

        assert_eq!(registry.len(), 0);
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
        assert_eq!(n1, 1);
        let after_first = registry.len();
        assert!(
            after_first >= 1,
            "expected ≥1 cache entry, got {after_first}"
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
        assert_eq!(n2, 1);
        assert_eq!(
            registry.len(),
            after_first,
            "second query must reuse the cache, not allocate new entries",
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
