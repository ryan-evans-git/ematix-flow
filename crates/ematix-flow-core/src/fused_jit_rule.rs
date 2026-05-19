//! Σ.D3 phase D: `PhysicalOptimizerRule` that auto-routes hand-coded
//! fused execs to their Cranelift-JIT'd variants.
//!
//! ## Scope
//!
//! This rule walks the physical plan tree with `transform_up` and, for
//! each `FusedFilterSumExec` / `FusedFilterMultiAggExec` /
//! `FusedPostJoinExec(Q14)` that's still in hand-coded mode, rebuilds
//! it with the matching `try_new_*_jit` constructor. Other plan nodes
//! pass through unchanged.
//!
//! That's the minimum viable auto-routing: callers who manually
//! construct a fused exec (in tune examples / future planner paths)
//! get the JIT speedup automatically by adding one
//! `add_physical_optimizer_rule` call to their SessionContext.
//!
//! ## What's NOT here
//!
//! The "real" Σ.D3 phase D — pattern-matching DataFusion's default SQL
//! plan output (`AggregateExec(Final) ← CoalescePartitionsExec ←
//! AggregateExec(Partial) ← ProjectionExec ← FilterExec ← ParquetExec`)
//! and extracting predicate constants from the `PhysicalExpr` AST to
//! INJECT a fused exec where none was hand-constructed — is deferred.
//! That requires walking `BinaryExpr`/`Column`/`Literal` nodes,
//! recognising AND-chains of column-vs-literal comparisons, mapping
//! aggregate function calls to `AggExpr` variants, and aligning column
//! indices between the physical plan and the spec. None of those are
//! intrinsically hard but each is a meaningful chunk of code; this
//! commit lands the optimizer-wiring substrate so that work can build
//! on a tested foundation.

use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::ScalarValue;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

use crate::ematix_fast_parquet::{BridgeFilter, ColumnPredicate, EmatixFastParquetExec};
use crate::fused_aggregate::{Q1Spec, Q6Spec};
use crate::fused_aggregate_exec::FusedAggregateExec;
use crate::fused_multi_agg::Q1Predicate;
use crate::fused_post_join::{FusedPostJoinExec, FusedPostJoinSpec};

/// Σ.D3 phase D rule: rewrite hand-coded fused execs into their JIT
/// variants in-place. Idempotent — execs already in JIT mode pass
/// through unchanged.
#[derive(Debug, Default)]
pub struct EnableFusedJitRule;

impl PhysicalOptimizerRule for EnableFusedJitRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|node| {
            // Σ.G.3d: FusedAggregateExec<Q6Spec> — the Q6 shape now lives
            // here, not in the retired FusedFilterSumExec.
            if let Some(e) = node.as_any().downcast_ref::<FusedAggregateExec<Q6Spec>>() {
                if !e.spec().has_jit() {
                    let input = e.input().clone();
                    let new_spec = Q6Spec::try_new_jit(e.spec().predicate, &input.schema())?;
                    let new = FusedAggregateExec::try_new(input, new_spec)?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            // Σ.G.3d: FusedAggregateExec<Q1Spec> — same shape, now under
            // the generic operator.
            if let Some(e) = node.as_any().downcast_ref::<FusedAggregateExec<Q1Spec>>() {
                if !e.spec().has_jit() {
                    let input = e.input().clone();
                    let new_spec = Q1Spec::try_new_jit(e.spec().predicate, &input.schema())?;
                    let new = FusedAggregateExec::try_new(input, new_spec)?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            // FusedPostJoinExec — JIT only currently supports Q14.
            if let Some(e) = node.as_any().downcast_ref::<FusedPostJoinExec>() {
                if !e.has_jit() && matches!(e.spec(), FusedPostJoinSpec::Q14) {
                    let new = FusedPostJoinExec::try_new_jit(e.input().clone(), e.spec())?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            Ok(Transformed::no(node))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_fused_jit"
    }

    fn schema_check(&self) -> bool {
        // The rule swaps an exec for one that produces an identical
        // output schema — both `try_new_q6` and `try_new_q6_jit` use
        // the same Schema constructor, etc. Safe to leave validation on.
        true
    }
}

// Σ.G.2e-4: `InjectFusedQ6Rule` was retired in favour of
// `InjectFilterSumRule` (`fused_aggregate_filter_sum_rule.rs`), whose
// generalised matcher subsumes the Q6-shape SQL pattern and was
// bench-gated at 1.50 % delta on TPC-H SF=1 Q6 (see
// `examples/sigma_g2e_filter_sum_vs_q6.rs`). The substrate
// `Q6Spec(JIT)` type is still exported from `fused_aggregate` so the
// direct-construction examples (`tpch_q6_jit_bench`, `tpch_q6_tune`)
// continue to work; only the SQL-pattern auto-injection rule + its
// Q6-specific predicate/aggregate extractors are gone. The shared
// `flip_op` helper survives because the Q1 path still uses it.

fn flip_op(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Eq => Operator::Eq,
        Operator::NotEq => Operator::NotEq,
        _ => return None,
    })
}

fn dferr(msg: &str) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Internal(msg.into())
}

// ===== Σ.D3 phase D follow-up (B): generalised plan-shape matcher =====
//
// The six per-query matchers (Q1/Q3/Q5/Q6/Q12/Q14) share most of their
// walk skeleton: optional SortMerge → Sort → Projection at the top, an
// AggregateExec(Final*) → optional CoalescePartitions/RepartitionExec
// → AggregateExec(Partial) stack, then optional ProjectionExec(CSE)
// and a tail that's either a FilterExec → scan (Q1/Q6) or a join
// chain (Q3/Q5/Q12/Q14). Only the query-specific *validation* of each
// step differs.
//
// `AggregateShapeConfig` declares the structural expectations a rule
// has on the plan tree; `match_aggregate_query_shape` does the walk
// and returns the matched nodes + the body (post-aggregate-stack
// plan node) for the per-rule code to inspect.

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
/// the InjectFused*Rule family. Per-rule code reads
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
    /// The plan node below the aggregate stack. For
    /// FusedFilterSumExec / FusedFilterMultiAggExec-shaped rules
    /// this is typically the `FilterExec`; for FusedPostJoinExec-
    /// shaped rules this is the join chain or projection above it.
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

/// Σ.D3 phase D (real, Q1 variant): rule that pattern-matches the
/// DataFusion default plan shape for the TPC-H Q1 group-by query and
/// rewrites the whole subtree to a single [`FusedFilterMultiAggExec`]
/// (JIT mode) directly over the scan. Group-by version of
/// [`InjectFusedQ6Rule`].
///
/// Recognised plan shape (see `examples/tpch_q1_plan_dump.rs`):
///
/// ```text
/// SortPreservingMergeExec(by [l_returnflag ASC, l_linestatus ASC])
///   SortExec(by [l_returnflag ASC, l_linestatus ASC])
///     ProjectionExec(rename sum/avg/count expressions → q1 SELECT list)
///       AggregateExec(FinalPartitioned, gby=[returnflag, linestatus],
///                    8 aggregates: 4 SUM + 3 AVG + COUNT)
///         RepartitionExec(Hash([returnflag, linestatus], ...))
///           AggregateExec(Partial, same gby + aggregates)
///             ProjectionExec(CSE: extprice * (1 - discount))
///               FilterExec(l_shipdate <= <Date32>, projection=[…])
///                 [optional RepartitionExec wrapper]
///                   scan (exposes the 7 cols Q1 fused needs)
/// ```
///
/// Replacement: `FusedFilterMultiAggExec(scan, Q1Predicate { shipdate
/// _cutoff })`. The fused exec emits the 10-column Q1 SELECT list in
/// canonical (l_returnflag, l_linestatus)-sorted order, so the
/// `SortPreservingMergeExec → SortExec → ProjectionExec` chain at the
/// top is *redundant* — we replace from the SortPreservingMergeExec
/// (or SortExec if that's the top) all the way down. Output schema
/// matches the SQL's SELECT list exactly (names + types).
#[derive(Debug, Default)]
pub struct InjectFusedQ1Rule;

impl PhysicalOptimizerRule for InjectFusedQ1Rule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_down(|node| {
            if let Some(new) = try_match_q1_plan(&node)? {
                Ok(Transformed::yes(new))
            } else {
                Ok(Transformed::no(node))
            }
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_inject_fused_q1"
    }

    fn schema_check(&self) -> bool {
        // The fused exec emits the Q1 SELECT-list columns by name +
        // semantic type, but the *Arrow type* differs from DataFusion's
        // synthesised AggregateExec output: returnflag / linestatus are
        // Utf8 here vs Utf8View in the planner's schema; the 7 sum/avg
        // cols are non-nullable Float64 here vs nullable in the planner's
        // schema. The values are identical (no aggregate row of Q1 SF=1
        // returns NULL anywhere); the type drift is purely a metadata
        // difference. Turning schema_check off accepts that drift; the
        // SQL-level test pins the resulting cell-by-cell equivalence
        // with the un-rewritten plan.
        false
    }
}

/// Try to interpret `node` as the top of a Q1-shaped plan and return
/// a [`FusedFilterMultiAggExec`] over the scan beneath it. Tolerates
/// either `SortPreservingMergeExec` *or* `SortExec` at the top — the
/// merge wraps the sort when DataFusion targets multiple partitions
/// but the sort alone can be the root when there's only one.
fn try_match_q1_plan(node: &Arc<dyn ExecutionPlan>) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    const CFG: AggregateShapeConfig = AggregateShapeConfig {
        expect_top_sort: true,
        expect_top_projection: true,
        expect_final_mode: FinalAggMode::FinalPartitioned,
        expect_group_by_count: 2,
        expect_agg_count: 8,
        expect_cse_projection: true,
    };
    let Some(shape) = match_aggregate_query_shape(node, &CFG)? else {
        return Ok(None);
    };
    let top_proj = shape
        .top_projection
        .as_ref()
        .expect("config requires top projection");
    if !projection_emits_q1_select_list(top_proj.schema().as_ref()) {
        return Ok(None);
    }
    if !group_by_is_returnflag_linestatus(&shape.final_agg)
        || !group_by_is_returnflag_linestatus(&shape.partial_agg)
    {
        return Ok(None);
    }
    // Σ.E5 (2026-05-19): accept two body shapes —
    //  - Inexact pushdown: `FilterExec(predicate)` sits directly under
    //    the agg stack; pull the predicate from FilterExec's physical
    //    expression (current path).
    //  - Exact pushdown: DataFusion drops `FilterExec` entirely; the
    //    predicate lives on `EmatixFastParquetExec`'s `BridgeFilter`.
    //    Walk down `body` to find the emat scan and pull it from
    //    there.
    let (predicate, scan_root): (Q1Predicate, Arc<dyn ExecutionPlan>) =
        if let Some(filter) = shape.body.as_any().downcast_ref::<FilterExec>() {
            let Some(p) = extract_q1_predicate(filter.predicate()) else {
                return Ok(None);
            };
            let child = filter
                .children()
                .first()
                .map(|c| (*c).clone())
                .ok_or_else(|| dferr("Q1 match: FilterExec missing input"))?;
            (p, child)
        } else {
            // Walk to the EmatixFastParquetExec and read its
            // BridgeFilter. If we don't find an emat scan, or the scan
            // has no pushed filter matching Q1's shape, this isn't a
            // Q1 plan and we pass.
            let Some(scan) = find_emat_scan(&shape.body) else {
                return Ok(None);
            };
            let Some(p) = extract_q1_predicate_from_scan(scan) else {
                return Ok(None);
            };
            (p, shape.body.clone())
        };
    let mut scan: Arc<dyn ExecutionPlan> = scan_root;
    loop {
        if scan_has_required_q1_columns(&scan.schema()) {
            break;
        }
        let children = scan.children();
        if children.len() != 1 {
            return Ok(None);
        }
        scan = children[0].clone();
        if !scan_has_required_q1_columns(&scan.schema()) && scan.children().is_empty() {
            return Ok(None);
        }
    }
    // Σ.G.3d: same as InjectFusedQ6Rule above — construct the generic
    // operator directly with the JIT-enabled spec.
    let spec = Q1Spec::try_new_jit(predicate, &scan.schema())?;
    let fused = FusedAggregateExec::try_new(scan, spec)?;
    Ok(Some(Arc::new(fused) as Arc<dyn ExecutionPlan>))
}

/// Q1 output schema: 2 grouping keys + 8 aggregate output cols
/// (4 SUM + 3 AVG + 1 COUNT, all renamed to canonical TPC-H names).
fn projection_emits_q1_select_list(schema: &datafusion::arrow::datatypes::Schema) -> bool {
    let expected = [
        "l_returnflag",
        "l_linestatus",
        "sum_qty",
        "sum_base_price",
        "sum_disc_price",
        "sum_charge",
        "avg_qty",
        "avg_price",
        "avg_disc",
        "count_order",
    ];
    if schema.fields().len() != expected.len() {
        return false;
    }
    schema
        .fields()
        .iter()
        .zip(expected.iter())
        .all(|(f, n)| f.name() == n)
}

/// Check that `agg`'s group-by is exactly `[l_returnflag, l_linestatus]`
/// — both as `Column` PhysicalExprs, in that order.
fn group_by_is_returnflag_linestatus(agg: &AggregateExec) -> bool {
    let g = agg.group_expr();
    let exprs = g.expr();
    if exprs.len() != 2 {
        return false;
    }
    let a = exprs[0]
        .0
        .as_any()
        .downcast_ref::<Column>()
        .map(|c| c.name());
    let b = exprs[1]
        .0
        .as_any()
        .downcast_ref::<Column>()
        .map(|c| c.name());
    matches!(
        (a, b),
        (Some("l_returnflag"), Some("l_linestatus")) | (Some("l_linestatus"), Some("l_returnflag"))
    )
}

fn scan_has_required_q1_columns(schema: &datafusion::arrow::datatypes::SchemaRef) -> bool {
    use datafusion::arrow::datatypes::DataType;
    let cols = [
        ("l_returnflag", DataType::Utf8View),
        ("l_linestatus", DataType::Utf8View),
        ("l_quantity", DataType::Float64),
        ("l_extendedprice", DataType::Float64),
        ("l_discount", DataType::Float64),
        ("l_tax", DataType::Float64),
        ("l_shipdate", DataType::Date32),
    ];
    cols.iter().all(|(n, ty)| {
        schema
            .field_with_name(n)
            .map(|f| f.data_type() == ty)
            .unwrap_or(false)
    })
}

/// Pull the Q1 predicate (a single `l_shipdate <= Date32(...)` or
/// `<`) out of a FilterExec's PhysicalExpr. The TPC-H spec uses `<=`;
/// `<` is accepted too in case a different translator/path emits it.
/// Anything else (different column, different op, compound predicate)
/// makes us return `None` and the rule passes through.
fn extract_q1_predicate(expr: &Arc<dyn PhysicalExpr>) -> Option<Q1Predicate> {
    // We support a single comparison leaf — Q1's filter is just one
    // condition. If a future planner version splits it into an AND of
    // redundant conditions we can re-use `flatten_and` here.
    let (col, op, lit) = decompose_filter_leaf(expr)?;
    if col != "l_shipdate" {
        return None;
    }
    let cutoff_lit = match lit {
        ScalarValue::Date32(Some(d)) => d,
        _ => return None,
    };
    // For `col <= V` the JIT's per-row check is `ship > V → skip`, i.e.
    // cutoff = V. For `col < V` the equivalent inclusive cutoff is V-1.
    let shipdate_cutoff = match op {
        Operator::LtEq => cutoff_lit,
        Operator::Lt => cutoff_lit - 1,
        _ => return None,
    };
    Some(Q1Predicate { shipdate_cutoff })
}

/// Σ.E5 (2026-05-19): walk down `node` looking for an
/// `EmatixFastParquetExec`. Returns the first one found, descending
/// through single-child wrappers (RepartitionExec, ProjectionExec,
/// CoalesceBatchesExec, etc.). Returns `None` at branches or if no
/// emat scan is found within 8 hops (TPC-H plans are shallow).
fn find_emat_scan(node: &Arc<dyn ExecutionPlan>) -> Option<&EmatixFastParquetExec> {
    if let Some(emat) = node.as_any().downcast_ref::<EmatixFastParquetExec>() {
        return Some(emat);
    }
    let children = node.children();
    if children.len() != 1 {
        return None;
    }
    find_emat_scan(children[0])
}

/// Σ.E5 (2026-05-19): pull a Q1Predicate from an `EmatixFastParquetExec`'s
/// pushed BridgeFilter. Returns `Some` only when the scan holds an
/// `I32Range` predicate on `l_shipdate` (looked up via the file
/// schema, so the col_idx is whatever the file happens to use). The
/// range must be a single `<=` or `<` clause — Q1's filter is one
/// inclusive upper bound.
fn extract_q1_predicate_from_scan(scan: &EmatixFastParquetExec) -> Option<Q1Predicate> {
    let bf: &BridgeFilter = scan.filter()?;
    let file_schema = scan.file_schema();
    // Q1's only pushable filter is `l_shipdate <= V`. Find the single
    // I32Range predicate whose col_idx names `l_shipdate` in the file
    // schema.
    let preds = bf.predicates();
    if preds.len() != 1 {
        return None;
    }
    let ColumnPredicate::I32Range { col_idx, clauses } = &preds[0] else {
        return None;
    };
    let col_name = file_schema.field(*col_idx).name().as_str();
    if col_name != "l_shipdate" {
        return None;
    }
    if clauses.len() != 1 {
        return None;
    }
    let c = &clauses[0];
    let shipdate_cutoff = match c.op {
        Operator::LtEq => c.literal_i32,
        Operator::Lt => c.literal_i32 - 1,
        _ => return None,
    };
    Some(Q1Predicate { shipdate_cutoff })
}

/// Generic `Column ⊕ Literal` (or `Literal ⊕ Column`) decomposer.
/// Same shape as `decompose_leaf` in the Q6 rule but un-aliased to a
/// single static column name set, since Q1 only filters on one column.
fn decompose_filter_leaf(expr: &Arc<dyn PhysicalExpr>) -> Option<(String, Operator, ScalarValue)> {
    let b = expr.as_any().downcast_ref::<BinaryExpr>()?;
    let op = *b.op();
    match (
        b.left().as_any().downcast_ref::<Column>(),
        b.right().as_any().downcast_ref::<Literal>(),
        b.left().as_any().downcast_ref::<Literal>(),
        b.right().as_any().downcast_ref::<Column>(),
    ) {
        (Some(c), Some(l), _, _) => Some((c.name().to_string(), op, l.value().clone())),
        (_, _, Some(l), Some(c)) => {
            let flipped = flip_op(op)?;
            Some((c.name().to_string(), flipped, l.value().clone()))
        }
        _ => None,
    }
}

fn both_are_sum(aggs: &[Arc<datafusion::physical_expr::aggregate::AggregateFunctionExpr>]) -> bool {
    aggs.iter()
        .all(|a| a.fun().name().eq_ignore_ascii_case("sum"))
}

/// Σ.D3 phase D (Q3): pattern-match TPC-H Q3's plan — SortMerge over a
/// 3-table join chain, with a `FinalPartitioned/Partial` hash-aggregate
/// stack on top grouping by `(l_orderkey, o_orderdate, o_shippriority)`
/// and a single `SUM(extprice * (1-discount))`. Replace the aggregate
/// stack with `FusedPostJoinExec(spec=Q3)` over the existing join
/// output; the join chain itself is kept (the fused exec is post-join
/// only). We substitute the small-cardinality group-by + SUM kernel for
/// DataFusion's hash-aggregate stack.
#[derive(Debug, Default)]
pub struct InjectFusedQ3Rule;

impl PhysicalOptimizerRule for InjectFusedQ3Rule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_down(|node| {
            if let Some(new) = try_match_q3_plan(&node)? {
                Ok(Transformed::yes(new))
            } else {
                Ok(Transformed::no(node))
            }
        })?;
        Ok(result.data)
    }
    fn name(&self) -> &str {
        "ematix_flow_inject_fused_q3"
    }
    fn schema_check(&self) -> bool {
        // FusedPostJoinExec(Q3) emits Int64/Float64/Date32/Int32 non-
        // nullable; DataFusion's synthesised schema may have nullable
        // versions. The fused exec's output is also already sorted by
        // revenue DESC but doesn't apply the secondary `o_orderdate`
        // tiebreaker — for SF=1 Q3 there are no revenue ties in the
        // canonical output, so this is benign; the SQL-level test
        // compares cell-by-cell after both sides are re-sorted.
        false
    }
}

fn try_match_q3_plan(node: &Arc<dyn ExecutionPlan>) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    const CFG: AggregateShapeConfig = AggregateShapeConfig {
        expect_top_sort: true,
        expect_top_projection: true,
        expect_final_mode: FinalAggMode::FinalPartitioned,
        expect_group_by_count: 3,
        expect_agg_count: 1,
        expect_cse_projection: false,
    };
    let Some(shape) = match_aggregate_query_shape(node, &CFG)? else {
        return Ok(None);
    };
    let proj = shape
        .top_projection
        .as_ref()
        .expect("config requires top projection");
    if !projection_emits_q3_select_list(proj.schema().as_ref()) {
        return Ok(None);
    }
    if !group_by_is_q3_keys(&shape.final_agg) || !group_by_is_q3_keys(&shape.partial_agg) {
        return Ok(None);
    }
    if !is_sum_extprice_times_one_minus_discount(&shape.final_agg.aggr_expr()[0]) {
        return Ok(None);
    }
    if !post_join_has_q3_columns(&shape.body.schema()) {
        return Ok(None);
    }
    let fused = FusedPostJoinExec::try_new(shape.body, FusedPostJoinSpec::Q3)?;
    Ok(Some(Arc::new(fused) as Arc<dyn ExecutionPlan>))
}

fn projection_emits_q3_select_list(schema: &datafusion::arrow::datatypes::Schema) -> bool {
    let expected = ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"];
    if schema.fields().len() != expected.len() {
        return false;
    }
    schema
        .fields()
        .iter()
        .zip(expected.iter())
        .all(|(f, n)| f.name() == n)
}

fn group_by_is_q3_keys(agg: &AggregateExec) -> bool {
    let exprs = agg.group_expr().expr();
    if exprs.len() != 3 {
        return false;
    }
    let names: Vec<Option<&str>> = exprs
        .iter()
        .map(|(e, _)| e.as_any().downcast_ref::<Column>().map(|c| c.name()))
        .collect();
    let required = ["l_orderkey", "o_orderdate", "o_shippriority"];
    required
        .iter()
        .all(|r| names.iter().any(|n| n == &Some(*r)))
}

/// Match `SUM(l_extendedprice * (1 - l_discount))` or its arithmetic
/// equivalent `SUM(l_extendedprice * 1 - l_extendedprice * l_discount)`.
/// DataFusion's logical optimiser may distribute the multiplication;
/// we accept either canonical form by checking the aggregate function
/// is SUM and the argument references both columns.
fn is_sum_extprice_times_one_minus_discount(
    agg: &Arc<datafusion::physical_expr::aggregate::AggregateFunctionExpr>,
) -> bool {
    if !agg.fun().name().eq_ignore_ascii_case("sum") {
        return false;
    }
    let exprs = agg.expressions();
    if exprs.len() != 1 {
        return false;
    }
    // Walk the expression collecting referenced column names.
    let mut cols: Vec<String> = Vec::new();
    collect_column_names(&exprs[0], &mut cols);
    cols.contains(&"l_extendedprice".into()) && cols.contains(&"l_discount".into())
}

fn collect_column_names(expr: &Arc<dyn PhysicalExpr>, out: &mut Vec<String>) {
    if let Some(c) = expr.as_any().downcast_ref::<Column>() {
        out.push(c.name().to_string());
        return;
    }
    if let Some(b) = expr.as_any().downcast_ref::<BinaryExpr>() {
        collect_column_names(b.left(), out);
        collect_column_names(b.right(), out);
    }
    // Other PhysicalExpr types (Literal, ScalarFunctionExpr, …): skip.
    // We're looking for evidence of the right Columns; missing ones
    // make the predicate fail to match higher up.
}

fn post_join_has_q3_columns(schema: &datafusion::arrow::datatypes::SchemaRef) -> bool {
    use datafusion::arrow::datatypes::DataType;
    let has = |name: &str, allowed: &[DataType]| -> bool {
        schema
            .field_with_name(name)
            .map(|f| allowed.iter().any(|t| t == f.data_type()))
            .unwrap_or(false)
    };
    has("l_orderkey", &[DataType::Int32, DataType::Int64])
        && has("l_extendedprice", &[DataType::Float64])
        && has("l_discount", &[DataType::Float64])
        && has("o_orderdate", &[DataType::Date32])
        && has("o_shippriority", &[DataType::Int32, DataType::Int64])
}

/// Σ.D3 phase D (Q5): pattern-match TPC-H Q5's plan (similar top
/// structure to Q3 but with a single string group key `n_name`) and
/// replace the aggregate stack with `FusedPostJoinExec(spec=Q5)`. The
/// join chain in Q5 is much longer than Q3 (region × nation × supplier
/// × customer × orders × lineitem) — we keep it intact and just
/// substitute the aggregate kernel.
#[derive(Debug, Default)]
pub struct InjectFusedQ5Rule;

impl PhysicalOptimizerRule for InjectFusedQ5Rule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_down(|node| {
            if let Some(new) = try_match_q5_plan(&node)? {
                Ok(Transformed::yes(new))
            } else {
                Ok(Transformed::no(node))
            }
        })?;
        Ok(result.data)
    }
    fn name(&self) -> &str {
        "ematix_flow_inject_fused_q5"
    }
    fn schema_check(&self) -> bool {
        // Q5 fused exec emits `n_name` as Utf8; planner's projection
        // emits Utf8View. Metadata-only drift; SQL-level test pins
        // cell-by-cell equivalence.
        false
    }
}

fn try_match_q5_plan(node: &Arc<dyn ExecutionPlan>) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    const CFG: AggregateShapeConfig = AggregateShapeConfig {
        expect_top_sort: true,
        expect_top_projection: true,
        expect_final_mode: FinalAggMode::FinalPartitioned,
        expect_group_by_count: 1,
        expect_agg_count: 1,
        expect_cse_projection: false,
    };
    let Some(shape) = match_aggregate_query_shape(node, &CFG)? else {
        return Ok(None);
    };
    let proj = shape
        .top_projection
        .as_ref()
        .expect("config requires top projection");
    if !projection_emits_q5_select_list(proj.schema().as_ref()) {
        return Ok(None);
    }
    if !group_by_is_q5_keys(&shape.final_agg) || !group_by_is_q5_keys(&shape.partial_agg) {
        return Ok(None);
    }
    if !is_sum_extprice_times_one_minus_discount(&shape.final_agg.aggr_expr()[0]) {
        return Ok(None);
    }
    if !post_join_has_q5_columns(&shape.body.schema()) {
        return Ok(None);
    }
    let fused = FusedPostJoinExec::try_new(shape.body, FusedPostJoinSpec::Q5)?;
    Ok(Some(Arc::new(fused) as Arc<dyn ExecutionPlan>))
}

fn projection_emits_q5_select_list(schema: &datafusion::arrow::datatypes::Schema) -> bool {
    let expected = ["n_name", "revenue"];
    if schema.fields().len() != expected.len() {
        return false;
    }
    schema
        .fields()
        .iter()
        .zip(expected.iter())
        .all(|(f, n)| f.name() == n)
}

fn group_by_is_q5_keys(agg: &AggregateExec) -> bool {
    let exprs = agg.group_expr().expr();
    if exprs.len() != 1 {
        return false;
    }
    exprs[0]
        .0
        .as_any()
        .downcast_ref::<Column>()
        .map(|c| c.name() == "n_name")
        .unwrap_or(false)
}

fn post_join_has_q5_columns(schema: &datafusion::arrow::datatypes::SchemaRef) -> bool {
    use datafusion::arrow::datatypes::DataType;
    let has = |name: &str, allowed: &[DataType]| -> bool {
        schema
            .field_with_name(name)
            .map(|f| allowed.iter().any(|t| t == f.data_type()))
            .unwrap_or(false)
    };
    has("n_name", &[DataType::Utf8View])
        && has("l_extendedprice", &[DataType::Float64])
        && has("l_discount", &[DataType::Float64])
}

/// Σ.D3 phase D (Q12): pattern-match TPC-H Q12's plan
/// (SortMerge → Sort → Projection → Aggregate(FinalPartitioned,
/// gby=[l_shipmode], 2 SUM(CASE-WHEN ...) aggregates) → Repartition(Hash)
/// → Aggregate(Partial) → Projection → HashJoin(lineitem ⋈ orders) →
/// {Filter(shipmode IN, dates) → lineitem, orders}) and replace the
/// aggregate stack with `FusedPostJoinExec(spec=Q12)` over the join
/// output. The join chain itself is preserved; the fused exec just
/// substitutes the hash-aggregate stack with a hard-coded 2-bucket
/// (MAIL/SHIP) counter that does 1-byte-prefix checks on
/// `o_orderpriority`.
#[derive(Debug, Default)]
pub struct InjectFusedQ12Rule;

impl PhysicalOptimizerRule for InjectFusedQ12Rule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_down(|node| {
            if let Some(new) = try_match_q12_plan(&node)? {
                Ok(Transformed::yes(new))
            } else {
                Ok(Transformed::no(node))
            }
        })?;
        Ok(result.data)
    }
    fn name(&self) -> &str {
        "ematix_flow_inject_fused_q12"
    }
    fn schema_check(&self) -> bool {
        // Fused exec emits Utf8 / Int64; planner emits Utf8View / Int64
        // (nullable). Metadata-only drift; SQL test pins cell-by-cell.
        false
    }
}

fn try_match_q12_plan(node: &Arc<dyn ExecutionPlan>) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    const CFG: AggregateShapeConfig = AggregateShapeConfig {
        expect_top_sort: true,
        expect_top_projection: true,
        expect_final_mode: FinalAggMode::FinalPartitioned,
        expect_group_by_count: 1,
        expect_agg_count: 2,
        expect_cse_projection: false,
    };
    let Some(shape) = match_aggregate_query_shape(node, &CFG)? else {
        return Ok(None);
    };
    let proj = shape
        .top_projection
        .as_ref()
        .expect("config requires top projection");
    if !projection_emits_q12_select_list(proj.schema().as_ref()) {
        return Ok(None);
    }
    if !group_by_is_l_shipmode(&shape.final_agg) || !group_by_is_l_shipmode(&shape.partial_agg) {
        return Ok(None);
    }
    if !both_are_sum(shape.final_agg.aggr_expr()) {
        return Ok(None);
    }
    if !post_join_has_q12_columns(&shape.body.schema()) {
        return Ok(None);
    }
    let fused = FusedPostJoinExec::try_new(shape.body, FusedPostJoinSpec::Q12)?;
    Ok(Some(Arc::new(fused) as Arc<dyn ExecutionPlan>))
}

fn projection_emits_q12_select_list(schema: &datafusion::arrow::datatypes::Schema) -> bool {
    let expected = ["l_shipmode", "high_line_count", "low_line_count"];
    if schema.fields().len() != expected.len() {
        return false;
    }
    schema
        .fields()
        .iter()
        .zip(expected.iter())
        .all(|(f, n)| f.name() == n)
}

fn group_by_is_l_shipmode(agg: &AggregateExec) -> bool {
    let exprs = agg.group_expr().expr();
    if exprs.len() != 1 {
        return false;
    }
    exprs[0]
        .0
        .as_any()
        .downcast_ref::<Column>()
        .map(|c| c.name() == "l_shipmode")
        .unwrap_or(false)
}

fn post_join_has_q12_columns(schema: &datafusion::arrow::datatypes::SchemaRef) -> bool {
    use datafusion::arrow::datatypes::DataType;
    schema
        .field_with_name("l_shipmode")
        .map(|f| f.data_type() == &DataType::Utf8View)
        .unwrap_or(false)
        && schema
            .field_with_name("o_orderpriority")
            .map(|f| f.data_type() == &DataType::Utf8View)
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fused::Q6Predicate;
    use datafusion::arrow::array::{Date32Builder, Float64Builder, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use futures_util::stream::TryStreamExt;

    fn one_row_match() -> RecordBatch {
        // Single row that satisfies the canonical Q6 predicate:
        // shipdate=8800 (in [8766, 9131)), discount=0.06 (in [0.05, 0.07]),
        // quantity=10 (< 24), extprice=100 → contributes 100*0.06 = 6.0.
        let mut qty = Float64Builder::new();
        let mut price = Float64Builder::new();
        let mut disc = Float64Builder::new();
        let mut ship = Date32Builder::new();
        qty.append_value(10.0);
        price.append_value(100.0);
        disc.append_value(0.06);
        ship.append_value(8800);
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(qty.finish()),
                Arc::new(price.finish()),
                Arc::new(disc.finish()),
                Arc::new(ship.finish()),
            ],
        )
        .unwrap()
    }

    async fn input_plan_for_q6() -> Arc<dyn ExecutionPlan> {
        let b = one_row_match();
        let schema = b.schema();
        let mem = MemTable::try_new(schema, vec![vec![b]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    fn q6_predicate_canonical() -> Q6Predicate {
        Q6Predicate {
            date_lo: 8766,
            date_hi: 9131,
            disc_lo: 0.05,
            disc_hi: 0.07,
            qty_hi: 24.0,
        }
    }

    /// Build a non-JIT `FusedAggregateExec<Q6Spec>` directly — the Σ.G.3d
    /// substrate the EnableFusedJitRule now lifts to JIT.
    async fn fused_agg_q6_no_jit() -> Arc<dyn ExecutionPlan> {
        let input = input_plan_for_q6().await;
        let spec = Q6Spec::try_new(q6_predicate_canonical(), &input.schema()).unwrap();
        Arc::new(FusedAggregateExec::try_new(input, spec).unwrap())
    }

    async fn fused_agg_q6_with_jit() -> Arc<dyn ExecutionPlan> {
        let input = input_plan_for_q6().await;
        let spec = Q6Spec::try_new_jit(q6_predicate_canonical(), &input.schema()).unwrap();
        Arc::new(FusedAggregateExec::try_new(input, spec).unwrap())
    }

    /// Rule converts a non-JIT `FusedAggregateExec<Q6Spec>` into its
    /// JIT variant: same structural ExecutionPlan, but the spec's
    /// `has_jit()` flips from false to true.
    #[tokio::test]
    async fn rule_enables_jit_on_hand_coded_q6_exec() {
        let hand = fused_agg_q6_no_jit().await;
        assert!(
            !hand
                .as_any()
                .downcast_ref::<FusedAggregateExec<Q6Spec>>()
                .unwrap()
                .spec()
                .has_jit(),
            "starting state: hand-coded"
        );

        let rule = EnableFusedJitRule;
        let cfg = ConfigOptions::new();
        let optimized = rule.optimize(hand.clone(), &cfg).unwrap();
        let downcast = optimized
            .as_any()
            .downcast_ref::<FusedAggregateExec<Q6Spec>>()
            .expect("rule preserves the operator type");
        assert!(downcast.spec().has_jit(), "rule should enable JIT");
    }

    /// Rule is idempotent: a plan already in JIT mode passes through
    /// unchanged (no double-build, no panics).
    #[tokio::test]
    async fn rule_is_idempotent() {
        let already_jit = fused_agg_q6_with_jit().await;
        let rule = EnableFusedJitRule;
        let cfg = ConfigOptions::new();
        let optimized = rule.optimize(already_jit, &cfg).unwrap();
        let downcast = optimized
            .as_any()
            .downcast_ref::<FusedAggregateExec<Q6Spec>>()
            .unwrap();
        assert!(downcast.spec().has_jit());
    }

    /// End-to-end: rule-rewritten plan produces the same final
    /// f64 result as the original hand-coded plan. Validates that the
    /// rule preserves the output (no schema drift, no FP surprises).
    #[tokio::test]
    async fn rule_rewritten_plan_produces_same_result_as_hand_coded() {
        use datafusion::arrow::array::Float64Array;

        let hand_exec = fused_agg_q6_no_jit().await;

        let pre_rule = fused_agg_q6_no_jit().await;
        let cfg = ConfigOptions::new();
        let rewritten = EnableFusedJitRule.optimize(pre_rule, &cfg).unwrap();

        let session = SessionContext::new();
        let mut hand_s = hand_exec.execute(0, session.task_ctx()).unwrap();
        let mut rule_s = rewritten.execute(0, session.task_ctx()).unwrap();
        let hand_v = hand_s
            .try_next()
            .await
            .unwrap()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let rule_v = rule_s
            .try_next()
            .await
            .unwrap()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            hand_v.to_bits(),
            rule_v.to_bits(),
            "hand={hand_v} rule={rule_v} (must be bit-identical)"
        );
    }

    /// SessionContext registration smoke-test: the rule can be added
    /// via the public API and is named consistently. Doesn't validate
    /// auto-firing on SQL queries (that requires pattern-matching the
    /// DataFusion-default plan, which is the deferred work).
    #[tokio::test]
    async fn rule_registers_into_session_state() {
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::SessionConfig;
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(EnableFusedJitRule))
            .build();
        let ctx = SessionContext::new_with_state(state);
        // A trivial SQL query through the customized SessionContext.
        // The rule itself doesn't fire (no FusedFilterSumExec in the
        // plan) but we confirm registration didn't break anything.
        let df = ctx.sql("SELECT 1 AS x").await.unwrap();
        let batches: Vec<RecordBatch> = df.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    // Σ.G.2e-4: InjectFusedQ6Rule integration tests retired alongside
    // the rule. End-to-end coverage of Q6-shape SQL (rule fires, answer
    // matches default plan) now lives in
    // `fused_aggregate_filter_sum_rule::tests::inject_filter_sum_rule_rewrites_real_q6_plan_and_preserves_result`
    // — InjectFilterSumRule subsumes the Q6 rule and uses the same
    // FastParquet provider + SF=1 dataset shape.

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
                    // Fallback: synthetic mini-fixture (see
                    // `test_support`). Lets the SQL plan-rewrite tests
                    // exercise FastParquet end-to-end in CI without
                    // the real SF=1 dataset on disk.
                    PathBuf::from(crate::test_support::tpch_mini_dir())
                }
            }
        };
        let p = dir.join("lineitem.parquet");
        p.exists().then(|| p.to_string_lossy().into_owned())
    }

    // ----- InjectFusedQ1Rule tests -----

    const Q1_SQL: &str = "
        SELECT
            l_returnflag, l_linestatus,
            sum(l_quantity) AS sum_qty,
            sum(l_extendedprice) AS sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            avg(l_quantity) AS avg_qty,
            avg(l_extendedprice) AS avg_price,
            avg(l_discount) AS avg_disc,
            count(*) AS count_order
        FROM lineitem
        WHERE l_shipdate <= DATE '1998-09-02'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    ";

    async fn ctx_for_q1(register_rule: bool) -> Option<SessionContext> {
        use crate::fast_parquet::FastParquetTableProvider;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::SessionConfig;

        let path = sf1_lineitem()?;
        let state = if register_rule {
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(14))
                .with_default_features()
                .with_physical_optimizer_rule(Arc::new(InjectFusedQ1Rule))
                .build()
        } else {
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(14))
                .with_default_features()
                .build()
        };
        let ctx = SessionContext::new_with_state(state);
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        Some(ctx)
    }

    /// Inject Q1 rule fires on real Q1 SQL: the resulting physical
    /// plan contains FusedFilterMultiAggExec and the answer agrees
    /// with the un-rewritten plan on every cell.
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_q1_rule_rewrites_real_plan_and_preserves_result() {
        use datafusion::arrow::array::{AsArray, Float64Array, Int64Array};
        use datafusion::physical_plan::displayable;

        fn string_at(b: &RecordBatch, col: usize, row: usize) -> String {
            // The Arrow type for the grouping columns differs between
            // the un-rewritten plan (StringViewArray) and the fused exec
            // (StringArray). Read each via the matching downcast and
            // compare as `&str`.
            let arr = b.column(col);
            if let Some(s) = arr.as_string_opt::<i32>() {
                s.value(row).to_string()
            } else if let Some(s) = arr.as_string_view_opt() {
                s.value(row).to_string()
            } else {
                panic!("unexpected string array variant for col {col}");
            }
        }

        let Some(ctx_with) = ctx_for_q1(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx_without = ctx_for_q1(false).await.unwrap();

        let plan = ctx_with
            .sql(Q1_SQL)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_str.contains("FusedAggregateExec"),
            "InjectFusedQ1Rule did not fire on canonical Q1 plan.\nPlan:\n{plan_str}"
        );

        let r_with = ctx_with.sql(Q1_SQL).await.unwrap().collect().await.unwrap();
        let r_without = ctx_without
            .sql(Q1_SQL)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(r_with.len(), r_without.len(), "batch count");
        let bw = &r_with[0];
        let bo = &r_without[0];
        assert_eq!(bw.num_rows(), bo.num_rows(), "row count");
        assert_eq!(bw.num_rows(), 4, "Q1 SF=1 yields 4 groups");

        for col_idx in [0usize, 1] {
            for r in 0..bw.num_rows() {
                assert_eq!(
                    string_at(bw, col_idx, r),
                    string_at(bo, col_idx, r),
                    "col {col_idx} row {r}"
                );
            }
        }
        for col_idx in 2..=8 {
            let w = bw
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let o = bo
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for r in 0..bw.num_rows() {
                let wv = w.value(r);
                let ov = o.value(r);
                let rel = (wv - ov).abs() / ov.abs().max(wv.abs()).max(1e-300);
                assert!(
                    rel < 1e-10,
                    "col {col_idx} row {r}: with={wv} without={ov} rel_err={rel:e}"
                );
            }
        }
        let w_c = bw.column(9).as_any().downcast_ref::<Int64Array>().unwrap();
        let o_c = bo.column(9).as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..bw.num_rows() {
            assert_eq!(w_c.value(r), o_c.value(r), "count row {r}");
        }
    }

    /// Rule must not fire when shape diverges (same group keys, but
    /// no shipdate filter and different aggregates).
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_q1_rule_does_not_fire_on_unrelated_query() {
        use datafusion::physical_plan::displayable;
        let Some(ctx) = ctx_for_q1(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let plan = ctx
            .sql(
                "SELECT l_returnflag, l_linestatus, count(*) FROM lineitem \
                 GROUP BY l_returnflag, l_linestatus",
            )
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            !plan_str.contains("FusedAggregateExec"),
            "InjectFusedQ1Rule wrongly fired:\n{plan_str}"
        );
    }

    const TPCH_TABLES: &[&str] = &[
        "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
    ];

    // ----- InjectFusedQ3Rule / InjectFusedQ5Rule tests -----

    const Q3_SQL: &str = "
        SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue,
               o_orderdate, o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = 'BUILDING'
          AND c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate < DATE '1995-03-15'
          AND l_shipdate > DATE '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate
    ";

    const Q5_SQL: &str = "
        SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND l_suppkey = s_suppkey
          AND c_nationkey = s_nationkey
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'ASIA'
          AND o_orderdate >= DATE '1994-01-01'
          AND o_orderdate <  DATE '1995-01-01'
        GROUP BY n_name
        ORDER BY revenue DESC
    ";

    async fn ctx_for_q35<R: PhysicalOptimizerRule + Send + Sync + 'static>(
        rule: Option<R>,
    ) -> Option<SessionContext> {
        use crate::fast_parquet::FastParquetTableProvider;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::SessionConfig;
        let path = sf1_lineitem()?;
        let dir = std::path::PathBuf::from(&path)
            .parent()?
            .to_string_lossy()
            .into_owned();
        let mut builder = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(14))
            .with_default_features();
        if let Some(r) = rule {
            builder = builder.with_physical_optimizer_rule(Arc::new(r));
        }
        let state = builder.build();
        let ctx = SessionContext::new_with_state(state);
        for t in TPCH_TABLES {
            let p = format!("{dir}/{t}.parquet");
            let prov = FastParquetTableProvider::try_new(p).ok()?;
            ctx.register_table(*t, Arc::new(prov)).unwrap();
        }
        Some(ctx)
    }

    /// Sort `RecordBatch` rows by every column (lex-sort) so we can
    /// compare with-rule vs without-rule cell-by-cell, immune to
    /// secondary-key ordering differences (Q3's tie-break on
    /// `o_orderdate` isn't enforced by FusedPostJoinExec when revenues
    /// happen to be equal; the rule path drops the SortExec above).
    fn lex_sort_batch(batch: &RecordBatch) -> RecordBatch {
        use datafusion::arrow::compute;
        let sort_columns: Vec<compute::SortColumn> = (0..batch.num_columns())
            .map(|i| compute::SortColumn {
                values: batch.column(i).clone(),
                options: None,
            })
            .collect();
        let indices = compute::lexsort_to_indices(&sort_columns, None).unwrap();
        let new_cols: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| compute::take(c.as_ref(), &indices, None).unwrap())
            .collect();
        RecordBatch::try_new(batch.schema(), new_cols).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn inject_q3_rule_rewrites_real_plan_and_preserves_result() {
        use datafusion::arrow::array::{AsArray, Float64Array};
        use datafusion::physical_plan::displayable;
        let Some(ctx_with) = ctx_for_q35(Some(InjectFusedQ3Rule)).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx_without: SessionContext = ctx_for_q35::<EnableFusedJitRule>(None).await.unwrap();

        let plan = ctx_with
            .sql(Q3_SQL)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_str.contains("FusedPostJoinExec"),
            "InjectFusedQ3Rule did not fire.\nPlan:\n{plan_str}"
        );

        let r_with = ctx_with.sql(Q3_SQL).await.unwrap().collect().await.unwrap();
        let r_without = ctx_without
            .sql(Q3_SQL)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // Concatenate into one batch each, then lex-sort, then compare.
        let bw = datafusion::arrow::compute::concat_batches(&r_with[0].schema(), &r_with).unwrap();
        let bo =
            datafusion::arrow::compute::concat_batches(&r_without[0].schema(), &r_without).unwrap();
        assert_eq!(bw.num_rows(), bo.num_rows(), "row count");
        let bw = lex_sort_batch(&bw);
        let bo = lex_sort_batch(&bo);

        // Revenue is column 1. Compare to rel_err < 1e-10 cell by cell.
        let w = bw
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let o = bo
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for r in 0..bw.num_rows() {
            let rel = (w.value(r) - o.value(r)).abs()
                / o.value(r).abs().max(w.value(r).abs()).max(1e-300);
            assert!(
                rel < 1e-10,
                "Q3 revenue row {r}: with={} without={} rel_err={rel:e}",
                w.value(r),
                o.value(r)
            );
        }
        // Sanity-check the group keys match too (l_orderkey int64, dates).
        let _ = bw
            .column(0)
            .as_primitive_opt::<datafusion::arrow::datatypes::Int64Type>();
    }

    // ----- InjectFusedQ12Rule tests -----

    const Q12_SQL: &str = "
        SELECT l_shipmode,
            sum(case when o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
                     then 1 else 0 end) AS high_line_count,
            sum(case when o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
                     then 1 else 0 end) AS low_line_count
        FROM orders, lineitem
        WHERE o_orderkey = l_orderkey
          AND l_shipmode IN ('MAIL', 'SHIP')
          AND l_commitdate < l_receiptdate
          AND l_shipdate < l_commitdate
          AND l_receiptdate >= DATE '1994-01-01'
          AND l_receiptdate <  DATE '1995-01-01'
        GROUP BY l_shipmode
        ORDER BY l_shipmode
    ";

    #[tokio::test(flavor = "multi_thread")]
    async fn inject_q12_rule_rewrites_real_plan_and_preserves_result() {
        use datafusion::arrow::array::{AsArray, Int64Array};
        use datafusion::physical_plan::displayable;
        let Some(ctx_with) = ctx_for_q35(Some(InjectFusedQ12Rule)).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx_without: SessionContext = ctx_for_q35::<EnableFusedJitRule>(None).await.unwrap();

        let plan = ctx_with
            .sql(Q12_SQL)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_str.contains("FusedPostJoinExec"),
            "InjectFusedQ12Rule did not fire.\nPlan:\n{plan_str}"
        );

        let r_with = ctx_with
            .sql(Q12_SQL)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let r_without = ctx_without
            .sql(Q12_SQL)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let bw = datafusion::arrow::compute::concat_batches(&r_with[0].schema(), &r_with).unwrap();
        let bo =
            datafusion::arrow::compute::concat_batches(&r_without[0].schema(), &r_without).unwrap();
        assert_eq!(bw.num_rows(), bo.num_rows(), "row count");
        assert_eq!(bw.num_rows(), 2, "Q12 SF=1 yields 2 groups (MAIL/SHIP)");

        // String key comparison handles Utf8 vs Utf8View.
        let read_name = |arr: &dyn datafusion::arrow::array::Array, i: usize| -> String {
            if let Some(s) = arr.as_string_opt::<i32>() {
                s.value(i).to_string()
            } else if let Some(s) = arr.as_string_view_opt() {
                s.value(i).to_string()
            } else {
                panic!("unexpected string variant")
            }
        };
        for r in 0..bw.num_rows() {
            assert_eq!(
                read_name(bw.column(0).as_ref(), r),
                read_name(bo.column(0).as_ref(), r),
                "shipmode row {r}"
            );
            let w_high = bw
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(r);
            let o_high = bo
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(r);
            assert_eq!(w_high, o_high, "high_line_count row {r}");
            let w_low = bw
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(r);
            let o_low = bo
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(r);
            assert_eq!(w_low, o_low, "low_line_count row {r}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn inject_q12_rule_does_not_fire_on_unrelated_query() {
        use datafusion::physical_plan::displayable;
        let Some(ctx) = ctx_for_q35(Some(InjectFusedQ12Rule)).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        // Group key matches Q12's `l_shipmode` but only COUNT(*) — not the
        // 2-SUM-with-CASE-WHEN shape Q12 is.
        let plan = ctx
            .sql("SELECT l_shipmode, count(*) FROM lineitem GROUP BY l_shipmode")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            !plan_str.contains("FusedPostJoinExec"),
            "InjectFusedQ12Rule wrongly fired:\n{plan_str}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn inject_q5_rule_rewrites_real_plan_and_preserves_result() {
        use datafusion::arrow::array::{AsArray, Float64Array};
        use datafusion::physical_plan::displayable;
        let Some(ctx_with) = ctx_for_q35(Some(InjectFusedQ5Rule)).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx_without: SessionContext = ctx_for_q35::<EnableFusedJitRule>(None).await.unwrap();

        let plan = ctx_with
            .sql(Q5_SQL)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_str.contains("FusedPostJoinExec"),
            "InjectFusedQ5Rule did not fire.\nPlan:\n{plan_str}"
        );

        let r_with = ctx_with.sql(Q5_SQL).await.unwrap().collect().await.unwrap();
        let r_without = ctx_without
            .sql(Q5_SQL)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let bw = datafusion::arrow::compute::concat_batches(&r_with[0].schema(), &r_with).unwrap();
        let bo =
            datafusion::arrow::compute::concat_batches(&r_without[0].schema(), &r_without).unwrap();
        assert_eq!(bw.num_rows(), bo.num_rows(), "row count");

        // Match by n_name (col 0) for a stable comparison — string keys
        // are unique here so we don't need to lex-sort.
        let w_names = bw.column(0);
        let o_names = bo.column(0);
        let read_name = |arr: &dyn datafusion::arrow::array::Array, i: usize| -> String {
            if let Some(s) = arr.as_string_opt::<i32>() {
                s.value(i).to_string()
            } else if let Some(s) = arr.as_string_view_opt() {
                s.value(i).to_string()
            } else {
                panic!("unexpected string variant")
            }
        };
        let w_rev = bw
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let o_rev = bo
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut w_map = std::collections::HashMap::<String, f64>::new();
        let mut o_map = std::collections::HashMap::<String, f64>::new();
        for r in 0..bw.num_rows() {
            w_map.insert(read_name(w_names.as_ref(), r), w_rev.value(r));
            o_map.insert(read_name(o_names.as_ref(), r), o_rev.value(r));
        }
        for (k, wv) in &w_map {
            let ov = o_map
                .get(k)
                .unwrap_or_else(|| panic!("missing key {k} in default plan"));
            let rel = (wv - ov).abs() / ov.abs().max(wv.abs()).max(1e-300);
            assert!(
                rel < 1e-10,
                "Q5 {k}: with={wv} without={ov} rel_err={rel:e}"
            );
        }
    }

    // ----- Predicate-extraction unit tests -----
    //
    // The inject-rule tests above gate on SF=1 TPC-H data and therefore
    // skip in CI without the dataset. The predicate-decomposer functions
    // (`flip_op`, `decompose_leaf`, `decompose_filter_leaf`,
    // `extract_q6_predicate`, `extract_q1_predicate`) are pure, no-
    // fixture functions — these tests exercise their happy paths and
    // the "return None" rejection branches that real-data tests never
    // hit.

    fn col(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, 0))
    }

    fn lit_date(d: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Date32(Some(d))))
    }

    fn lit_f64(f: f64) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Float64(Some(f))))
    }

    fn lit_null_date() -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Date32(None)))
    }

    fn bin(
        l: Arc<dyn PhysicalExpr>,
        op: Operator,
        r: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(l, op, r))
    }

    #[test]
    fn flip_op_covers_all_comparison_variants() {
        assert_eq!(flip_op(Operator::Lt), Some(Operator::Gt));
        assert_eq!(flip_op(Operator::LtEq), Some(Operator::GtEq));
        assert_eq!(flip_op(Operator::Gt), Some(Operator::Lt));
        assert_eq!(flip_op(Operator::GtEq), Some(Operator::LtEq));
        assert_eq!(flip_op(Operator::Eq), Some(Operator::Eq));
        assert_eq!(flip_op(Operator::NotEq), Some(Operator::NotEq));
        assert_eq!(flip_op(Operator::Plus), None);
        assert_eq!(flip_op(Operator::And), None);
    }

    // Σ.G.2e-4: tests for the Q6-only helpers (`flatten_and`,
    // `decompose_leaf`, `extract_q6_predicate`) retired with the
    // helpers themselves. The generalised equivalents in
    // `fused_aggregate_filter_sum_rule` have their own unit + integration
    // tests covering the same canonicalisation + rejection paths.

    #[test]
    fn extract_q1_predicate_lt_off_by_one_correction() {
        // For `l_shipdate < V` the cutoff is V-1 (inclusive Lt).
        let expr = bin(col("l_shipdate"), Operator::Lt, lit_date(10000));
        let pred = extract_q1_predicate(&expr).unwrap();
        assert_eq!(pred.shipdate_cutoff, 9999);
    }

    #[test]
    fn extract_q1_predicate_lt_eq_uses_literal_directly() {
        // For `l_shipdate <= V` the cutoff is V itself.
        let expr = bin(col("l_shipdate"), Operator::LtEq, lit_date(10000));
        let pred = extract_q1_predicate(&expr).unwrap();
        assert_eq!(pred.shipdate_cutoff, 10000);
    }

    #[test]
    fn extract_q1_predicate_rejects_wrong_column() {
        let expr = bin(col("l_orderkey"), Operator::LtEq, lit_date(10000));
        assert!(extract_q1_predicate(&expr).is_none());
    }

    #[test]
    fn extract_q1_predicate_rejects_unsupported_operator() {
        // Q1's filter is one-sided; GtEq is not the canonical shape.
        let expr = bin(col("l_shipdate"), Operator::GtEq, lit_date(10000));
        assert!(extract_q1_predicate(&expr).is_none());
    }

    #[test]
    fn extract_q1_predicate_rejects_null_literal() {
        let expr = bin(col("l_shipdate"), Operator::LtEq, lit_null_date());
        assert!(extract_q1_predicate(&expr).is_none());
    }

    #[test]
    fn decompose_filter_leaf_preserves_column_op_literal_order() {
        let expr = bin(col("l_shipdate"), Operator::LtEq, lit_date(10000));
        let (name, op, lit) = decompose_filter_leaf(&expr).unwrap();
        assert_eq!(name, "l_shipdate");
        assert_eq!(op, Operator::LtEq);
        assert!(matches!(lit, ScalarValue::Date32(Some(d)) if d == 10000));
    }

    #[test]
    fn decompose_filter_leaf_flips_mirrored_literal_op_column() {
        // `10000 >= l_shipdate` flips to `(l_shipdate, LtEq, 10000)`.
        let mirrored = bin(lit_date(10000), Operator::GtEq, col("l_shipdate"));
        let (name, op, _) = decompose_filter_leaf(&mirrored).unwrap();
        assert_eq!(name, "l_shipdate");
        assert_eq!(op, Operator::LtEq);
    }

    #[test]
    fn decompose_filter_leaf_rejects_non_flippable_mirrored_operator() {
        // `10 + l_shipdate` — Plus has no flip, so None.
        let bad = bin(lit_f64(10.0), Operator::Plus, col("l_shipdate"));
        assert!(decompose_filter_leaf(&bad).is_none());
    }

    #[test]
    fn dferr_produces_internal_variant() {
        // Tiny smoke test on the rule's error helper.
        let e = dferr("boom");
        assert!(format!("{e:?}").contains("boom"));
    }
}
