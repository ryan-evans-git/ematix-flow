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

use crate::dict_aggregate_rule::EnableDictGroupCountRule;
use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
use crate::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use crate::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use crate::fused_jit_rule::EnableFusedJitRule;

/// Attach ematix-flow's optimiser rule chain to a `SessionStateBuilder`.
///
/// Idempotent in the sense that the chain is order-stable: re-applying
/// it would just register the same rules twice (DataFusion runs each
/// in turn but the second pass is a no-op since the first pass already
/// transformed every match). Callers usually want one call per session.
///
/// Rules installed (in registration order, which is the order they run
/// during physical optimisation):
///
/// 1. `InjectFilterMultiAggRule` — generic filter + group-by + multi-aggregate
///    SQL pattern → `FusedAggregateExec<FilterMultiAggSpec>`. Replaces the
///    retired `InjectFusedQ1Rule` (Σ.G.2f.3, task #486).
/// 2. `InjectFilterSumRule` — generic SUM-over-Filter SQL (Σ.G.2e-3
///    generalised matcher) → `FusedAggregateExec<FilterSumSpec>`.
///    Subsumes the retired `InjectFusedQ6Rule`.
/// 3. `EnableDictGroupCountRule` — `AggregateExec(Final+Partial)` on
///    `Dictionary(UInt32, Utf8|Utf8View)` + `COUNT(*)` →
///    `DictGroupCountExec`. Only fires when the upstream scan emits a
///    `DictionaryArray`, which is why `register_dict_aware_parquet`
///    uses `with_dict_preservation(true)`.
/// 4. `EnableFusedJitRule` — promotes any hand-mode `FusedAggregateExec`
///    in the plan tree to its JIT-mode variant.
///    Idempotent; execs already in JIT mode pass through.
pub fn with_optimizer_rules(builder: SessionStateBuilder) -> SessionStateBuilder {
    // Σ.G.2f.3 cleanup (2026-05-19): EnableDictGroupCountRule runs
    // FIRST because its shape (COUNT(*) GROUP BY single dict-encoded
    // string col) is a strict subset of `InjectFilterMultiAggRule`'s
    // matcher — the dict op is the more specialised path and must
    // claim the plan before the generic filter+multi-agg rule absorbs
    // it. Same ordering reason that the (now-retired) per-query rules
    // used to run before the dict rule.
    builder
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
        .with_physical_optimizer_rule(Arc::new(EnableFusedJitRule))
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
