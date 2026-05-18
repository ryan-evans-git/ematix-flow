//! Σ.G.2e-2/3: `InjectFilterSumRule` — `PhysicalOptimizerRule` that
//! pattern-matches a SUM-over-Filter SQL plan shape, extracts the
//! literals + clauses + aggregate, and constructs a
//! `FusedAggregateExec<FilterSumSpec>` over the scan beneath the
//! FilterExec.
//!
//! ## Supported shape (Σ.G.2e-3)
//!
//! ```sql
//! SELECT sum(<numeric_expr>) AS <alias>
//! FROM <scan>
//! WHERE <and_chain_of_(col ⊕ literal)_clauses>
//! ```
//!
//! Concretely:
//!
//! - **Aggregate**: `SUM(col)` (Float64 column) or `SUM(col_a * col_b)`
//!   (both Float64 columns). The projection alias is arbitrary —
//!   "revenue", "total", anything — propagated to the output schema
//!   field name.
//! - **Predicate**: any AND-chain of `Column ⊕ Literal` (or its
//!   mirror) where:
//!     - column type ∈ `{Float64, Date32, Int32}`
//!     - operator ∈ `{<, <=, >, >=}` (Eq/NotEq are not supported
//!       — the underlying JIT IR's `ClauseOp` lacks them).
//! - **Plan shape**: Projection → AggregateExec(Final) → CoalescePartitions
//!   → AggregateExec(Partial) → FilterExec → scan (the standard
//!   DataFusion plan for the SQL above). The exec stack is matched via
//!   the shared `match_aggregate_query_shape` walker.
//!
//! Anything else (group-by, multi-aggregate, SUM-of-product-of-three,
//! BoolExpr branches, sub-queries) makes the rule no-op so DataFusion's
//! default plan runs unchanged.
//!
//! ## Why this generalisation can't widen the JIT IR
//!
//! `FusedFilterAggSpec`'s `ClauseOp` enum lists exactly the ops the
//! Cranelift emitter knows how to render — adding Eq/NotEq would
//! require extending the IR (small change, but a separate slice).
//! Similarly, exotic aggregate shapes (`SUM(col * (1 - col))`,
//! `SUM(CASE WHEN …)`, etc.) have dedicated `AggExpr` variants the
//! generalised matcher could be taught later. The bench-gated parity
//! confirmed by `sigma_g2e_filter_sum_vs_q6` covers the supported
//! subset above; broader shapes need their own gates.
//!
//! ## Composition with `InjectFusedQ6Rule`
//!
//! Σ.G.2e-3's generalised matcher subsumes `InjectFusedQ6Rule` —
//! canonical Q6 SQL is a special case of the general shape above.
//! Σ.G.2e-4 retires the Q6 rule. Until then they remain mutually
//! exclusive (only one should be registered on a SessionContext) since
//! both rewrite the same plan node to alternate constructions of
//! `FusedAggregateExec<S>`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;

use crate::fused_aggregate_exec::FusedAggregateExec;
use crate::fused_aggregate_filter_sum::FilterSumSpec;
use crate::fused_jit::{AggExpr, Clause, ClauseOp, ColumnTy, FusedFilterAggSpec};

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

// ===== matcher entry point =====

fn try_match_filter_sum_plan(
    node: &Arc<dyn ExecutionPlan>,
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    // Plan-tree shape: Projection → Final agg → Coalesce → Partial agg → body.
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

    // Output column name = projection's first (only) field. Propagates
    // the user's SQL alias to the FilterSumSpec output schema. Aggregate
    // shapes that don't produce a single output field are excluded by
    // the `expect_agg_count: 1` config above.
    let proj = shape
        .top_projection
        .as_ref()
        .expect("config requires top projection");
    let output_name = proj.schema().field(0).name().to_string();

    // Aggregate: SUM(col) or SUM(col_a * col_b). Partial + final must
    // describe the same shape (DataFusion's two-stage SUM splits keep
    // the inner expression identical between modes, but a mismatch is
    // either a planner bug or a non-SUM-shape we shouldn't touch).
    let Some(agg) = extract_filter_sum_aggregate(&shape.final_agg.aggr_expr()[0]) else {
        return Ok(None);
    };
    let Some(agg_partial) = extract_filter_sum_aggregate(&shape.partial_agg.aggr_expr()[0]) else {
        return Ok(None);
    };
    if !agg_shapes_equal(&agg, &agg_partial) {
        return Ok(None);
    }

    // Body: FilterExec with AND-chain of (col ⊕ literal) leaves.
    let Some(filter) = shape.body.as_any().downcast_ref::<FilterExec>() else {
        return Ok(None);
    };
    let Some(clauses) = extract_filter_sum_clauses(filter.predicate()) else {
        return Ok(None);
    };

    // The required columns are everything referenced by predicate or
    // aggregate. Walk past projections/repartitions inside the filter's
    // input to find the scan whose schema contains all of them.
    let referenced: Vec<String> = {
        let mut s: Vec<String> = clauses.iter().map(|(n, _, _)| n.clone()).collect();
        match &agg {
            AggShape::SumColumn(n) => s.push(n.clone()),
            AggShape::SumProductColumns(a, b) => {
                s.push(a.clone());
                s.push(b.clone());
            }
        }
        s
    };

    let mut scan: Arc<dyn ExecutionPlan> = filter
        .children()
        .first()
        .map(|c| (*c).clone())
        .ok_or_else(|| {
            DataFusionError::Internal("InjectFilterSumRule: FilterExec missing input".into())
        })?;
    loop {
        if scan_has_all_columns(&scan.schema(), &referenced) {
            break;
        }
        let children = scan.children();
        if children.len() != 1 {
            return Ok(None);
        }
        scan = children[0].clone();
        if scan.children().is_empty() && !scan_has_all_columns(&scan.schema(), &referenced) {
            return Ok(None);
        }
    }

    // Build the JIT spec + input-name list from the extracted predicate
    // + aggregate. Returns None on any type / op the JIT IR can't lower.
    let Some((jit_spec, input_names)) = build_jit_spec_and_inputs(clauses, agg, &scan.schema())
    else {
        return Ok(None);
    };

    let input_name_refs: Vec<&str> = input_names.iter().map(String::as_str).collect();
    let spec = FilterSumSpec::try_new(jit_spec, &input_name_refs, &scan.schema(), &output_name)?;
    let exec = FusedAggregateExec::try_new(scan, spec)?;
    Ok(Some(Arc::new(exec) as Arc<dyn ExecutionPlan>))
}

// ===== aggregate extraction =====

/// Recognised SUM aggregate shapes for FilterSumSpec.
#[derive(Debug, Clone)]
enum AggShape {
    /// `SUM(col)` — `col` must be Float64.
    SumColumn(String),
    /// `SUM(col_a * col_b)` — both columns must be Float64.
    SumProductColumns(String, String),
}

fn agg_shapes_equal(a: &AggShape, b: &AggShape) -> bool {
    match (a, b) {
        (AggShape::SumColumn(x), AggShape::SumColumn(y)) => x == y,
        (AggShape::SumProductColumns(x1, x2), AggShape::SumProductColumns(y1, y2)) => {
            // Multiply is commutative — accept either operand order.
            (x1 == y1 && x2 == y2) || (x1 == y2 && x2 == y1)
        }
        _ => false,
    }
}

/// Match a SUM aggregate over a single column or product of two columns.
/// Returns None for any other shape (SUM(expr) with non-Column leaves,
/// COUNT, AVG, multi-arg, etc.).
fn extract_filter_sum_aggregate(agg: &Arc<AggregateFunctionExpr>) -> Option<AggShape> {
    if !agg.fun().name().eq_ignore_ascii_case("sum") {
        return None;
    }
    let exprs = agg.expressions();
    if exprs.len() != 1 {
        return None;
    }
    // SUM(col)
    if let Some(c) = exprs[0].as_any().downcast_ref::<Column>() {
        return Some(AggShape::SumColumn(c.name().to_string()));
    }
    // SUM(col_a * col_b)
    if let Some(b) = exprs[0].as_any().downcast_ref::<BinaryExpr>() {
        if matches!(b.op(), Operator::Multiply) {
            let l = b.left().as_any().downcast_ref::<Column>()?;
            let r = b.right().as_any().downcast_ref::<Column>()?;
            return Some(AggShape::SumProductColumns(
                l.name().to_string(),
                r.name().to_string(),
            ));
        }
    }
    None
}

// ===== predicate extraction =====

/// One leaf of the AND-chain after canonicalisation: column-on-the-left,
/// op, literal-on-the-right.
type ClauseLeaf = (String, Operator, ScalarValue);

/// Walk an arbitrary AND-chain and decompose each leaf as
/// `(column_name, op, literal)`. Returns None on any malformed leaf or
/// unsupported operator.
fn extract_filter_sum_clauses(expr: &Arc<dyn PhysicalExpr>) -> Option<Vec<ClauseLeaf>> {
    let mut leaves: Vec<&Arc<dyn PhysicalExpr>> = Vec::new();
    flatten_and(expr, &mut leaves);
    if leaves.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        let (col, op, lit) = decompose_leaf(leaf)?;
        // The JIT IR's ClauseOp enum only has the four range operators —
        // Eq/NotEq would need new IR variants (small change, separate slice).
        if !matches!(
            op,
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
        ) {
            return None;
        }
        out.push((col, op, lit));
    }
    Some(out)
}

fn flatten_and<'a>(expr: &'a Arc<dyn PhysicalExpr>, out: &mut Vec<&'a Arc<dyn PhysicalExpr>>) {
    if let Some(b) = expr.as_any().downcast_ref::<BinaryExpr>() {
        if matches!(b.op(), Operator::And) {
            flatten_and(b.left(), out);
            flatten_and(b.right(), out);
            return;
        }
    }
    out.push(expr);
}

/// Decompose `Column ⊕ Literal` (or its mirror `Literal ⊕ Column`) into
/// `(column_name, op, literal)` with the column canonically on the left.
fn decompose_leaf(expr: &Arc<dyn PhysicalExpr>) -> Option<ClauseLeaf> {
    let b = expr.as_any().downcast_ref::<BinaryExpr>()?;
    let op = *b.op();
    let lcol = b.left().as_any().downcast_ref::<Column>();
    let rlit = b.right().as_any().downcast_ref::<Literal>();
    let llit = b.left().as_any().downcast_ref::<Literal>();
    let rcol = b.right().as_any().downcast_ref::<Column>();
    let (col_name, lit, flipped) = match (lcol, rlit, llit, rcol) {
        (Some(c), Some(l), _, _) => (c.name().to_string(), l.value().clone(), false),
        (_, _, Some(l), Some(c)) => (c.name().to_string(), l.value().clone(), true),
        _ => return None,
    };
    let op = if flipped { flip_op(op)? } else { op };
    Some((col_name, op, lit))
}

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

// ===== JIT spec construction =====

/// Compose the extracted clauses + aggregate + scan schema into a fully
/// validated `FusedFilterAggSpec` plus the ordered list of input column
/// names. Returns None on any type / op the JIT IR can't lower.
fn build_jit_spec_and_inputs(
    clauses: Vec<ClauseLeaf>,
    agg: AggShape,
    scan_schema: &SchemaRef,
) -> Option<(FusedFilterAggSpec, Vec<String>)> {
    // Dedup + index columns referenced anywhere. Predicate columns
    // come first in order of appearance, then aggregate columns not
    // already present.
    let mut input_names: Vec<String> = Vec::new();
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    let mut register = |name: &str| {
        if !name_to_idx.contains_key(name) {
            name_to_idx.insert(name.to_string(), input_names.len());
            input_names.push(name.to_string());
        }
    };
    for (name, _, _) in &clauses {
        register(name);
    }
    match &agg {
        AggShape::SumColumn(name) => register(name),
        AggShape::SumProductColumns(a, b) => {
            register(a);
            register(b);
        }
    }

    // Resolve each input's ColumnTy from the scan schema. Reject up
    // front on missing columns or unsupported types.
    let mut input_tys: Vec<ColumnTy> = Vec::with_capacity(input_names.len());
    for name in &input_names {
        let field = scan_schema.field_with_name(name).ok()?;
        let ty = map_arrow_to_column_ty(field.data_type())?;
        input_tys.push(ty);
    }

    // Translate clauses to JIT IR.
    let mut jit_clauses: Vec<Clause> = Vec::with_capacity(clauses.len());
    for (name, op, lit) in clauses {
        let col_idx = name_to_idx[&name];
        let col_ty = input_tys[col_idx];
        jit_clauses.push(build_clause(col_idx, col_ty, op, &lit)?);
    }

    // Translate aggregate to JIT IR. Both shapes require Float64 cols.
    let jit_agg = match agg {
        AggShape::SumColumn(name) => {
            let idx = name_to_idx[&name];
            if input_tys[idx] != ColumnTy::Float64 {
                return None;
            }
            AggExpr::SumColumn(idx)
        }
        AggShape::SumProductColumns(a, b) => {
            let a_idx = name_to_idx[&a];
            let b_idx = name_to_idx[&b];
            if input_tys[a_idx] != ColumnTy::Float64 || input_tys[b_idx] != ColumnTy::Float64 {
                return None;
            }
            AggExpr::SumProductColumns(a_idx, b_idx)
        }
    };

    let spec = FusedFilterAggSpec {
        inputs: input_tys,
        predicate: jit_clauses,
        aggregates: vec![jit_agg],
        group: None,
    };
    Some((spec, input_names))
}

fn map_arrow_to_column_ty(dt: &DataType) -> Option<ColumnTy> {
    match dt {
        DataType::Float64 => Some(ColumnTy::Float64),
        DataType::Date32 => Some(ColumnTy::Date32),
        DataType::Int32 => Some(ColumnTy::Int32),
        // Int64 columns map to ColumnTy::Int64 in the IR, but the
        // current ClauseOp variants are only Int32 immediates — we
        // can't emit a clause against an Int64 column without an Int64
        // immediate variant. Reject here; broaden when the IR adds them.
        _ => None,
    }
}

fn build_clause(
    col_idx: usize,
    col_ty: ColumnTy,
    op: Operator,
    lit: &ScalarValue,
) -> Option<Clause> {
    let (imm_i32, imm_f64) = match (col_ty, lit) {
        (ColumnTy::Float64, ScalarValue::Float64(Some(f))) => (0, *f),
        (ColumnTy::Date32, ScalarValue::Date32(Some(d))) => (*d, 0.0),
        (ColumnTy::Int32, ScalarValue::Int32(Some(i))) => (*i, 0.0),
        _ => return None,
    };
    let clause_op = match (col_ty, op) {
        (ColumnTy::Float64, Operator::GtEq) => ClauseOp::F64Ge,
        (ColumnTy::Float64, Operator::LtEq) => ClauseOp::F64Le,
        (ColumnTy::Float64, Operator::Lt) => ClauseOp::F64Lt,
        (ColumnTy::Float64, Operator::Gt) => ClauseOp::F64Gt,
        (ColumnTy::Date32 | ColumnTy::Int32, Operator::GtEq) => ClauseOp::I32Ge,
        (ColumnTy::Date32 | ColumnTy::Int32, Operator::LtEq) => ClauseOp::I32Le,
        (ColumnTy::Date32 | ColumnTy::Int32, Operator::Lt) => ClauseOp::I32Lt,
        (ColumnTy::Date32 | ColumnTy::Int32, Operator::Gt) => ClauseOp::I32Gt,
        _ => return None,
    };
    Some(Clause {
        column: col_idx,
        op: clause_op,
        imm_i32,
        imm_f64,
    })
}

fn scan_has_all_columns(schema: &SchemaRef, names: &[String]) -> bool {
    names.iter().all(|n| schema.column_with_name(n).is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fast_parquet::FastParquetTableProvider;
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

    /// Non-Q6 SQL that exercises the generalised matcher: arbitrary
    /// alias, SUM(col) (not product), single-column predicate.
    const SIMPLE_SUM_SQL: &str = "
        SELECT sum(l_extendedprice) AS total
        FROM lineitem
        WHERE l_shipdate >= DATE '1994-01-01'
          AND l_shipdate <  DATE '1995-01-01'
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

    /// Rule fires on canonical Q6 SQL (generalised matcher must subsume
    /// the original Q6 shape). Plan contains a FusedAggregateExec
    /// wrapping FilterSumSpec, and the answer matches the default plan
    /// to floating-point relative tolerance.
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_filter_sum_rule_rewrites_real_q6_plan_and_preserves_result() {
        use datafusion::arrow::array::Float64Array;

        let Some(ctx_with) = ctx_with_rule(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx_without = ctx_with_rule(false).await.unwrap();

        let df = ctx_with.sql(Q6_SQL).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_str.contains("FusedAggregateExec"),
            "InjectFilterSumRule did not fire on the canonical Q6 plan.\nPlan:\n{plan_str}"
        );
        assert!(
            plan_str.contains("FilterSumSpec"),
            "FusedAggregateExec is not wrapping FilterSumSpec.\nPlan:\n{plan_str}"
        );

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
        let rel = ((v_with - v_without) / v_without).abs();
        assert!(
            rel < 1e-10,
            "rule-rewritten Q6 result diverges: with={v_with}, without={v_without}, rel_err={rel:e}"
        );
    }

    /// Σ.G.2e-3 coverage: rule fires on a non-Q6 SUM-over-Filter shape
    /// (arbitrary alias, single-column SUM, two-clause predicate).
    /// Answer must match the default plan.
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_filter_sum_rule_fires_on_simple_sum_col() {
        use datafusion::arrow::array::Float64Array;

        let Some(ctx_with) = ctx_with_rule(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx_without = ctx_with_rule(false).await.unwrap();

        let plan = ctx_with
            .sql(SIMPLE_SUM_SQL)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_str.contains("FusedAggregateExec"),
            "Generalised matcher missed a SUM(col) shape.\nPlan:\n{plan_str}"
        );
        assert!(
            plan_str.contains("FilterSumSpec"),
            "FusedAggregateExec is not wrapping FilterSumSpec.\nPlan:\n{plan_str}"
        );

        let r_with = ctx_with
            .sql(SIMPLE_SUM_SQL)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let r_without = ctx_without
            .sql(SIMPLE_SUM_SQL)
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
        let rel = ((v_with - v_without) / v_without).abs();
        assert!(
            rel < 1e-10,
            "SUM(col) result diverges: with={v_with}, without={v_without}, rel_err={rel:e}"
        );
    }

    /// Rule does not fire on an aggregate it can't lower (COUNT).
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_filter_sum_rule_skips_count_aggregate() {
        let Some(ctx) = ctx_with_rule(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let plan = ctx
            .sql(
                "SELECT count(*) AS n FROM lineitem
                 WHERE l_shipdate >= DATE '1994-01-01'",
            )
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            !plan_str.contains("FusedAggregateExec"),
            "InjectFilterSumRule wrongly fired on COUNT(*):\n{plan_str}"
        );
    }

    /// Rule does not fire on equality predicates (JIT IR has no Eq).
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_filter_sum_rule_skips_equality_predicate() {
        let Some(ctx) = ctx_with_rule(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let plan = ctx
            .sql(
                "SELECT sum(l_extendedprice) AS total FROM lineitem
                 WHERE l_quantity = 5",
            )
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            !plan_str.contains("FusedAggregateExec"),
            "InjectFilterSumRule wrongly fired on equality predicate:\n{plan_str}"
        );
    }

    /// Rule does not fire on a no-filter aggregate.
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_filter_sum_rule_does_not_fire_on_unrelated_query() {
        let Some(ctx) = ctx_with_rule(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
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

    // ===== unit tests for the pure helpers =====

    #[test]
    fn flip_op_swaps_inequalities() {
        assert_eq!(flip_op(Operator::Lt), Some(Operator::Gt));
        assert_eq!(flip_op(Operator::LtEq), Some(Operator::GtEq));
        assert_eq!(flip_op(Operator::Gt), Some(Operator::Lt));
        assert_eq!(flip_op(Operator::GtEq), Some(Operator::LtEq));
        assert_eq!(flip_op(Operator::Eq), Some(Operator::Eq));
        assert_eq!(flip_op(Operator::Plus), None);
    }

    #[test]
    fn agg_shapes_equal_handles_commutativity() {
        let a = AggShape::SumProductColumns("x".into(), "y".into());
        let b = AggShape::SumProductColumns("y".into(), "x".into());
        let c = AggShape::SumProductColumns("x".into(), "z".into());
        assert!(agg_shapes_equal(&a, &b));
        assert!(!agg_shapes_equal(&a, &c));
        assert!(!agg_shapes_equal(
            &AggShape::SumColumn("x".into()),
            &AggShape::SumProductColumns("x".into(), "x".into())
        ));
    }

    #[test]
    fn build_clause_rejects_eq_and_neq() {
        assert!(
            build_clause(
                0,
                ColumnTy::Float64,
                Operator::Eq,
                &ScalarValue::Float64(Some(1.0))
            )
            .is_none()
        );
        assert!(
            build_clause(
                0,
                ColumnTy::Float64,
                Operator::NotEq,
                &ScalarValue::Float64(Some(1.0))
            )
            .is_none()
        );
    }

    #[test]
    fn build_clause_rejects_mismatched_literal_type() {
        // Date32 column with a Float64 literal — not interpretable.
        assert!(
            build_clause(
                0,
                ColumnTy::Date32,
                Operator::Lt,
                &ScalarValue::Float64(Some(1.0))
            )
            .is_none()
        );
    }

    #[test]
    fn build_clause_produces_canonical_ops() {
        let c = build_clause(
            2,
            ColumnTy::Float64,
            Operator::Lt,
            &ScalarValue::Float64(Some(24.0)),
        )
        .unwrap();
        assert_eq!(c.column, 2);
        assert!(matches!(c.op, ClauseOp::F64Lt));
        assert_eq!(c.imm_f64, 24.0);

        let c = build_clause(
            0,
            ColumnTy::Date32,
            Operator::GtEq,
            &ScalarValue::Date32(Some(8766)),
        )
        .unwrap();
        assert_eq!(c.column, 0);
        assert!(matches!(c.op, ClauseOp::I32Ge));
        assert_eq!(c.imm_i32, 8766);
    }

    #[test]
    fn map_arrow_to_column_ty_supported_set() {
        assert_eq!(
            map_arrow_to_column_ty(&DataType::Float64),
            Some(ColumnTy::Float64)
        );
        assert_eq!(
            map_arrow_to_column_ty(&DataType::Date32),
            Some(ColumnTy::Date32)
        );
        assert_eq!(
            map_arrow_to_column_ty(&DataType::Int32),
            Some(ColumnTy::Int32)
        );
        // Int64 not yet supported — no Int64 immediate variant in
        // ClauseOp at this slice.
        assert_eq!(map_arrow_to_column_ty(&DataType::Int64), None);
        assert_eq!(map_arrow_to_column_ty(&DataType::Utf8View), None);
        assert_eq!(map_arrow_to_column_ty(&DataType::Boolean), None);
    }

    #[test]
    fn build_jit_spec_dedups_columns_referenced_twice() {
        use datafusion::arrow::datatypes::{Field, Schema};
        // `l_extendedprice` appears in both predicate and aggregate.
        let clauses: Vec<ClauseLeaf> = vec![(
            "l_extendedprice".into(),
            Operator::Gt,
            ScalarValue::Float64(Some(100.0)),
        )];
        let agg = AggShape::SumColumn("l_extendedprice".into());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l_extendedprice",
            DataType::Float64,
            true,
        )]));
        let (spec, names) = build_jit_spec_and_inputs(clauses, agg, &schema).unwrap();
        assert_eq!(names, vec!["l_extendedprice"]);
        assert_eq!(spec.inputs, vec![ColumnTy::Float64]);
        assert_eq!(spec.predicate.len(), 1);
        assert_eq!(spec.aggregates.len(), 1);
        match &spec.aggregates[0] {
            AggExpr::SumColumn(idx) => assert_eq!(*idx, 0),
            _ => panic!("expected SumColumn"),
        }
    }
}
