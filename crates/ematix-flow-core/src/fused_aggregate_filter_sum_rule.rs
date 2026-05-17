//! Σ.G.2e-2: `InjectFilterSumRule` — `PhysicalOptimizerRule` that
//! pattern-matches a SUM-over-Filter SQL plan shape, extracts the
//! literals + clauses, and constructs a `FusedAggregateExec<FilterSumSpec>`
//! over the scan beneath the FilterExec.
//!
//! ## Scope of this slice
//!
//! Σ.G.2e-2 ships the rule that proves [`crate::fused_aggregate_filter_sum::FilterSumSpec`]
//! works end-to-end on real SQL through DataFusion. To keep the SQL
//! pattern matching from blowing up the diff, this first cut recognises
//! exactly the canonical Q6 shape — same predicate columns + same
//! aggregate as the existing `InjectFusedQ6Rule`, but routed through
//! `FilterSumSpec` (built from a `FusedFilterAggSpec::q6(...)` IR
//! description) instead of `Q6Spec`. The literal *values* are extracted
//! from the AST exactly as in the Q6 rule — what changes is the
//! resulting spec type, which is runtime-configured rather than
//! Q6-hardcoded.
//!
//! ## Why ship this if it's "no more general than InjectFusedQ6Rule"
//!
//! Two reasons:
//!
//! 1. **Bench gate.** The new `FilterSumSpec::process_batch` allocates
//!    a `Vec<*const u8>` per batch (vs Q6Spec's statically-typed
//!    fixed-size array). The Σ.G.2e bench (`sigma_g2e_filter_sum_vs_q6`)
//!    measures whether that allocation is a real cost. If it stays
//!    within 3 % of the Q6Spec path on TPC-H SF=1 Q6, the spec is
//!    confirmed perf-equivalent and future slices can broaden the SQL
//!    pattern coverage without re-running the same gate.
//!
//! 2. **Substrate for generalisation.** Σ.G.2e-3 (next slice) extends
//!    this rule's matcher to accept arbitrary column sets / arbitrary
//!    clause counts on numeric columns. That follow-up changes only
//!    the matcher; `FilterSumSpec` itself was already general at
//!    Σ.G.2e-1 land time. Splitting it this way keeps each slice
//!    individually bench-gated.
//!
//! ## Composition with the existing Q6 rule
//!
//! Mutually exclusive with `InjectFusedQ6Rule`: only one of them
//! should be registered on a SessionContext. The Q6 rule produces a
//! `FusedAggregateExec<Q6Spec(JIT)>`; this rule produces a
//! `FusedAggregateExec<FilterSumSpec>`. Both share the generic
//! operator scaffold, both go through the same Cranelift kernel —
//! they're alternate constructions of the same final exec node, and
//! letting both fire would double-rewrite the plan.
//!
//! Once Σ.G.2e-3 broadens the matcher AND the bench gate confirms
//! parity, the Q6 rule retires (Σ.G.2e-4).

use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;

use crate::fused::Q6Predicate;
use crate::fused_aggregate_exec::FusedAggregateExec;
use crate::fused_aggregate_filter_sum::FilterSumSpec;
use crate::fused_jit::FusedFilterAggSpec;

/// SQL-pattern injection rule for the generic FilterSum shape. See
/// module-level docs.
#[derive(Debug, Default)]
pub struct InjectFilterSumRule;

impl PhysicalOptimizerRule for InjectFilterSumRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_down(|node| {
            if let Some(new) = try_match_filter_sum_plan(&node)? {
                Ok(Transformed::yes(new))
            } else {
                Ok(Transformed::no(node))
            }
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_inject_filter_sum"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Try to match the canonical Q6-shape SQL plan and return a
/// `FusedAggregateExec<FilterSumSpec>` over the scan beneath. Returns
/// `Ok(None)` for any non-matching plan — the rule falls through and
/// DataFusion's default plan runs.
fn try_match_filter_sum_plan(
    node: &Arc<dyn ExecutionPlan>,
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    // Reuse the existing Q6 shape walker — same plan tree shape
    // (Projection → Final agg → Coalesce → Partial agg → body).
    let cfg = crate::fused_jit_rule::AggregateShapeConfig {
        expect_top_sort: false,
        expect_top_projection: true,
        expect_final_mode: crate::fused_jit_rule::FinalAggMode::Final,
        expect_group_by_count: 0,
        expect_agg_count: 1,
        expect_cse_projection: false,
    };
    let Some(shape) = crate::fused_jit_rule::match_aggregate_query_shape(node, &cfg)? else {
        return Ok(None);
    };

    // Output column name: `revenue` — canonical Q6 alias.
    let proj = shape
        .top_projection
        .as_ref()
        .expect("config requires top projection");
    if proj.schema().field(0).name() != "revenue" {
        return Ok(None);
    }

    // Aggregate: SUM(extprice * discount).
    if !crate::fused_jit_rule::is_sum_extprice_times_discount(&shape.final_agg.aggr_expr()[0]) {
        return Ok(None);
    }
    if !crate::fused_jit_rule::is_sum_extprice_times_discount(&shape.partial_agg.aggr_expr()[0]) {
        return Ok(None);
    }

    // Body: FilterExec with Q6-extractable predicate.
    let Some(filter) = shape.body.as_any().downcast_ref::<FilterExec>() else {
        return Ok(None);
    };
    let Some(predicate) = crate::fused_jit_rule::extract_q6_predicate(filter.predicate()) else {
        return Ok(None);
    };

    // Walk down to a scan that has the four required Q6 columns. The
    // `extract_q6_predicate` extractor handles the literal values; we
    // need the scan's schema for `FilterSumSpec::try_new` to resolve
    // column indices.
    let mut scan: Arc<dyn ExecutionPlan> = filter
        .children()
        .first()
        .map(|c| (*c).clone())
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "InjectFilterSumRule: FilterExec missing input".into(),
            )
        })?;
    loop {
        if scan_has_required_q6_columns(&scan.schema()) {
            break;
        }
        let children = scan.children();
        if children.len() != 1 {
            return Ok(None);
        }
        scan = children[0].clone();
        if !scan_has_required_q6_columns(&scan.schema()) && scan.children().is_empty() {
            return Ok(None);
        }
    }

    // Build the generic spec, route through FusedAggregateExec.
    let fused = build_filter_sum_exec(scan, predicate)?;
    Ok(Some(fused))
}

/// Mirror of `crate::fused_jit_rule::scan_has_required_q6_columns`,
/// duplicated here so the rule is self-contained without re-exporting
/// a private item across modules.
fn scan_has_required_q6_columns(schema: &datafusion::arrow::datatypes::SchemaRef) -> bool {
    use datafusion::arrow::datatypes::DataType;
    let cols = [
        ("l_quantity", DataType::Float64),
        ("l_extendedprice", DataType::Float64),
        ("l_discount", DataType::Float64),
        ("l_shipdate", DataType::Date32),
    ];
    cols.iter().all(|(n, ty)| {
        schema
            .field_with_name(n)
            .map(|f| f.data_type() == ty)
            .unwrap_or(false)
    })
}

/// Construct a `FusedAggregateExec<FilterSumSpec>` from a Q6 predicate +
/// the resolved scan. Builds the JIT IR via `FusedFilterAggSpec::q6` so
/// the kernel is bit-equivalent to `Q6Spec::try_new_jit`'s kernel; the
/// only difference is the spec wrapper around it.
fn build_filter_sum_exec(
    scan: Arc<dyn ExecutionPlan>,
    predicate: Q6Predicate,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let jit_spec = FusedFilterAggSpec::q6(
        predicate.date_lo,
        predicate.date_hi,
        predicate.disc_lo,
        predicate.disc_hi,
        predicate.qty_hi,
    );
    let spec = FilterSumSpec::try_new(
        jit_spec,
        // Input order from `FusedFilterAggSpec::q6`: shipdate (0),
        // discount (1), quantity (2), extprice (3). The rule resolves
        // these by name in `FilterSumSpec::try_new`.
        &["l_shipdate", "l_discount", "l_quantity", "l_extendedprice"],
        &scan.schema(),
        "revenue",
    )?;
    let exec = FusedAggregateExec::try_new(scan, spec)?;
    Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fast_parquet::FastParquetTableProvider;
    use crate::fused_aggregate_filter_sum::FilterSumSpec;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::PathBuf;

    const Q6_SQL: &str = "
        SELECT sum(l_extendedprice * l_discount) AS revenue
        FROM lineitem
        WHERE l_shipdate >= DATE '1994-01-01'
          AND l_shipdate <  DATE '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity <  24
    ";

    /// Resolve the SF=1 lineitem.parquet path: env var TPCH_DATA_DIR
    /// takes priority, then `examples/tpch/data/sf1/lineitem.parquet`,
    /// then the test_support mini-fixture (used in CI).
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

    /// Build a SessionContext over real SF=1 lineitem.parquet
    /// (FastParquetTableProvider — same shape the InjectFusedQ6Rule
    /// tests use) and optionally register the new rule. Returns None
    /// if the data isn't on disk (CI without TPC-H + without the
    /// mini-fixture should skip).
    async fn ctx_with_rule(register_rule: bool) -> Option<SessionContext> {
        let path = sf1_lineitem()?;
        let config = SessionConfig::new().with_target_partitions(14);
        let state = if register_rule {
            SessionStateBuilder::new()
                .with_config(config)
                .with_default_features()
                .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
                .build()
        } else {
            SessionStateBuilder::new()
                .with_config(config)
                .with_default_features()
                .build()
        };
        let ctx = SessionContext::new_with_state(state);
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        Some(ctx)
    }

    /// The rule fires on canonical Q6 SQL: the resulting physical plan
    /// contains a `FusedAggregateExec` (specifically the FilterSumSpec
    /// variant) and the answer matches the un-rewritten plan to within
    /// floating-point tolerance.
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_filter_sum_rule_rewrites_real_q6_plan_and_preserves_result() {
        use datafusion::arrow::array::Float64Array;

        let Some(ctx_with) = ctx_with_rule(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx_without = ctx_with_rule(false).await.unwrap();

        // 1. Verify the rule's effect on the physical plan.
        let df = ctx_with.sql(Q6_SQL).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_str.contains("FusedAggregateExec"),
            "InjectFilterSumRule did not fire on the canonical Q6 plan.\nPlan:\n{plan_str}"
        );

        // The FusedAggregateExec wraps FilterSumSpec (not Q6Spec) —
        // type-name check via Debug since the operator's DisplayAs
        // uses std::any::type_name::<S>().
        assert!(
            plan_str.contains("FilterSumSpec"),
            "FusedAggregateExec is not wrapping FilterSumSpec.\nPlan:\n{plan_str}"
        );

        // 2. Verify the answer matches the un-rewritten plan.
        let r_with = ctx_with.sql(Q6_SQL).await.unwrap().collect().await.unwrap();
        let r_without = ctx_without
            .sql(Q6_SQL)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let v_with = r_with[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let v_without = r_without[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        // Relative-error tolerance matches the existing
        // InjectFusedQ6Rule test — the JIT path sums in a different
        // order than DataFusion's default plan, so absolute equality
        // is too tight for SF=1 lineitem totals.
        let rel = ((v_with - v_without) / v_without).abs();
        assert!(
            rel < 1e-10,
            "rule-rewritten Q6 result diverges: with={v_with}, without={v_without}, rel_err={rel:e}"
        );
    }

    /// Rule does NOT fire on an unrelated query.
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_filter_sum_rule_does_not_fire_on_unrelated_query() {
        let Some(ctx) = ctx_with_rule(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        // No filter stack, different alias.
        let plan = ctx
            .sql("SELECT sum(l_extendedprice * l_discount) AS total FROM lineitem")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            !plan_str.contains("FusedAggregateExec"),
            "InjectFilterSumRule wrongly fired:\n{plan_str}"
        );
    }

    /// Substrate spot-check: build a scan via the SessionContext, then
    /// call the rule's `build_filter_sum_exec` helper directly. This
    /// verifies the constructed exec downcasts to the expected generic
    /// FilterSumSpec parameterisation, independent of the SQL pattern
    /// matcher.
    #[tokio::test(flavor = "multi_thread")]
    async fn build_filter_sum_exec_constructs_expected_type() {
        let Some(ctx) = ctx_with_rule(false).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let scan = ctx
            .sql("SELECT l_quantity, l_extendedprice, l_discount, l_shipdate FROM lineitem")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let pred = Q6Predicate {
            date_lo: 8766,
            date_hi: 9131,
            disc_lo: 0.05,
            disc_hi: 0.07,
            qty_hi: 24.0,
        };
        let exec = build_filter_sum_exec(scan, pred).unwrap();
        // Should be downcastable to FusedAggregateExec<FilterSumSpec>.
        assert!(
            exec.as_any()
                .downcast_ref::<FusedAggregateExec<FilterSumSpec>>()
                .is_some(),
            "build_filter_sum_exec didn't produce FusedAggregateExec<FilterSumSpec>"
        );
    }
}
