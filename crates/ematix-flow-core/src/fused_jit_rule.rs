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
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::ScalarValue;
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

use crate::fused::{FusedFilterSumExec, Q6Predicate};
use crate::fused_multi_agg::FusedFilterMultiAggExec;
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
            // FusedFilterSumExec (Σ.D1 / Q6 shape)
            if let Some(e) = node.as_any().downcast_ref::<FusedFilterSumExec>() {
                if !e.has_jit() {
                    let new = FusedFilterSumExec::try_new_q6_jit(
                        e.input().clone(),
                        e.predicate(),
                    )?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            // FusedFilterMultiAggExec (Σ.D2 / Q1 shape)
            if let Some(e) = node.as_any().downcast_ref::<FusedFilterMultiAggExec>() {
                if !e.has_jit() {
                    let new = FusedFilterMultiAggExec::try_new_q1_jit(
                        e.input().clone(),
                        e.predicate(),
                    )?;
                    return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                }
            }
            // FusedPostJoinExec — JIT only currently supports Q14.
            if let Some(e) = node.as_any().downcast_ref::<FusedPostJoinExec>() {
                if !e.has_jit() && matches!(e.spec(), FusedPostJoinSpec::Q14) {
                    let new =
                        FusedPostJoinExec::try_new_jit(e.input().clone(), e.spec())?;
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

/// Σ.D3 phase D (real): rule that pattern-matches the DataFusion
/// default physical plan for the TPC-H Q6 shape and rewrites the
/// subtree to a [`FusedFilterSumExec`] (JIT mode) directly over the
/// underlying scan. This is what makes the fused-exec library
/// user-visible — SQL users who go through `SessionContext::sql(...)`
/// get the fused operator automatically, without hand-constructing it.
///
/// Recognised plan shape (see `examples/tpch_q6_plan_dump.rs`):
///
/// ```text
/// ProjectionExec(rename sum() → "revenue")
///   AggregateExec(Final, no group-by, single SUM)
///     CoalescePartitionsExec
///       AggregateExec(Partial, no group-by, single SUM)
///         FilterExec(AND-chain on shipdate/discount/quantity)
///           [optional RepartitionExec/CoalesceBatchesExec wrappers]
///             scan (must expose l_quantity/l_extendedprice/l_discount/l_shipdate)
/// ```
///
/// Replacement output: `FusedFilterSumExec(scan, Q6Predicate { ... })`.
/// The fused exec validates the scan's schema by name (so the column
/// order in the scan's projection doesn't matter) and re-applies the
/// filter internally as part of its fused inner loop, so dropping the
/// FilterExec/Aggregate stack is correctness-preserving.
///
/// When the shape doesn't match (different aggregate, missing
/// columns, unsupported operator in the predicate, etc.) the rule
/// passes the node through unchanged.
#[derive(Debug, Default)]
pub struct InjectFusedQ6Rule;

impl PhysicalOptimizerRule for InjectFusedQ6Rule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_down(|node| {
            if let Some(new) = try_match_q6_plan(&node)? {
                Ok(Transformed::yes(new))
            } else {
                Ok(Transformed::no(node))
            }
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_inject_fused_q6"
    }

    fn schema_check(&self) -> bool {
        // FusedFilterSumExec emits a single Float64 column named
        // "revenue", which matches the SQL's `AS revenue` alias on Q6.
        // For other aliases the rule wouldn't fire (the outer projection
        // wouldn't match the "revenue" column name guard), so DataFusion
        // can keep schema-checking turned on.
        true
    }
}

/// Try to interpret `node` as the top of a Q6-shaped plan and return
/// a [`FusedFilterSumExec`] (JIT mode) over the scan beneath it.
/// Returns `Ok(None)` if anything in the shape doesn't match — the
/// node will be left unchanged by the caller.
fn try_match_q6_plan(node: &Arc<dyn ExecutionPlan>) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    // Top: ProjectionExec with a single output column named "revenue".
    // This catches the SQL `SUM(...) AS revenue` alias and avoids
    // overlapping with non-Q6 plans that happen to share intermediate
    // shape but produce different output names.
    let Some(proj) = node.as_any().downcast_ref::<ProjectionExec>() else {
        return Ok(None);
    };
    if proj.expr().len() != 1 || proj.schema().field(0).name() != "revenue" {
        return Ok(None);
    }

    // Down: AggregateExec(Final), no group-by, single SUM agg.
    let agg_final_child = proj.children().first().copied().cloned();
    let Some(agg_final_child) = agg_final_child else {
        return Ok(None);
    };
    let Some(agg_final) = agg_final_child.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    if !matches!(agg_final.mode(), AggregateMode::Final) {
        return Ok(None);
    }
    if !agg_final.group_expr().is_empty() || agg_final.aggr_expr().len() != 1 {
        return Ok(None);
    }
    if !is_sum_extprice_times_discount(&agg_final.aggr_expr()[0]) {
        return Ok(None);
    }

    // Down: CoalescePartitionsExec (optional wrapper around the partial agg).
    let after_coalesce: Arc<dyn ExecutionPlan> = agg_final
        .children()
        .first()
        .map(|c| (*c).clone())
        .ok_or_else(|| dferr("Q6 match: AggregateExec missing input"))?;
    let after_coalesce = strip_optional_coalesce(&after_coalesce);

    // Down: AggregateExec(Partial), same shape as Final.
    let Some(agg_partial) = after_coalesce.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    if !matches!(agg_partial.mode(), AggregateMode::Partial) {
        return Ok(None);
    }
    if !agg_partial.group_expr().is_empty() || agg_partial.aggr_expr().len() != 1 {
        return Ok(None);
    }
    if !is_sum_extprice_times_discount(&agg_partial.aggr_expr()[0]) {
        return Ok(None);
    }

    // Down: FilterExec with extractable Q6 predicate.
    let after_filter: Arc<dyn ExecutionPlan> = agg_partial
        .children()
        .first()
        .map(|c| (*c).clone())
        .ok_or_else(|| dferr("Q6 match: AggregateExec(Partial) missing input"))?;
    let Some(filter) = after_filter.as_any().downcast_ref::<FilterExec>() else {
        return Ok(None);
    };
    let Some(predicate) = extract_q6_predicate(filter.predicate()) else {
        return Ok(None);
    };

    // Strip wrappers between the filter and the scan: typically
    // RepartitionExec, sometimes CoalesceBatchesExec. We keep walking
    // until we hit a node whose schema has the four required columns
    // — that's the scan (FastParquetExec, DataSourceExec, or MemTable).
    let mut scan: Arc<dyn ExecutionPlan> = filter
        .children()
        .first()
        .map(|c| (*c).clone())
        .ok_or_else(|| dferr("Q6 match: FilterExec missing input"))?;
    loop {
        if scan_has_required_q6_columns(&scan.schema()) {
            break;
        }
        // Try to descend through a single-child wrapper. If we can't,
        // the plan doesn't fit the Q6 shape — bail out cleanly.
        let children = scan.children();
        if children.len() != 1 {
            return Ok(None);
        }
        let next = children[0].clone();
        // Repartition / CoalesceBatches / Projection — anything with
        // one child and no semantic-altering effect for our purposes.
        // We don't validate the wrapper type, just descend.
        let _is_known_wrapper = scan.as_any().downcast_ref::<RepartitionExec>().is_some();
        scan = next;
        // Safety belt: don't recurse forever.
        if !scan_has_required_q6_columns(&scan.schema()) && scan.children().is_empty() {
            return Ok(None);
        }
    }

    // Validate via the constructor — schema check by name, JIT codegen
    // confirms the predicate constants are well-formed.
    let fused = FusedFilterSumExec::try_new_q6_jit(scan, predicate)?;
    Ok(Some(Arc::new(fused) as Arc<dyn ExecutionPlan>))
}

fn strip_optional_coalesce(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if let Some(coal) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        if let Some(child) = coal.children().first() {
            return (*child).clone();
        }
    }
    plan.clone()
}

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

/// Matches a `SUM(l_extendedprice * l_discount)` aggregate expression,
/// regardless of the partial/final mode wrapping.
fn is_sum_extprice_times_discount(
    agg: &Arc<datafusion::physical_expr::aggregate::AggregateFunctionExpr>,
) -> bool {
    if !agg.fun().name().eq_ignore_ascii_case("sum") {
        return false;
    }
    let exprs = agg.expressions();
    if exprs.len() != 1 {
        return false;
    }
    let Some(b) = exprs[0].as_any().downcast_ref::<BinaryExpr>() else {
        return false;
    };
    if !matches!(b.op(), Operator::Multiply) {
        return false;
    }
    let lname = b.left().as_any().downcast_ref::<Column>().map(|c| c.name());
    let rname = b.right().as_any().downcast_ref::<Column>().map(|c| c.name());
    matches!(
        (lname, rname),
        (Some("l_extendedprice"), Some("l_discount"))
            | (Some("l_discount"), Some("l_extendedprice"))
    )
}

/// Walk an AND-chain of comparisons and try to populate a `Q6Predicate`.
/// Accepts the five canonical Q6 leaves in any order:
/// * `l_shipdate >= <Date32>` and `l_shipdate < <Date32>`
/// * `l_discount >= <Float64>` and `l_discount <= <Float64>`
/// * `l_quantity <  <Float64>`
///
/// Anything else makes us return `None` — the rule then leaves the
/// plan unchanged, and DataFusion runs its normal Filter+Aggregate.
fn extract_q6_predicate(expr: &Arc<dyn PhysicalExpr>) -> Option<Q6Predicate> {
    let mut leaves: Vec<&Arc<dyn PhysicalExpr>> = Vec::new();
    flatten_and(expr, &mut leaves);

    let mut date_lo: Option<i32> = None;
    let mut date_hi: Option<i32> = None;
    let mut disc_lo: Option<f64> = None;
    let mut disc_hi: Option<f64> = None;
    let mut qty_hi: Option<f64> = None;

    for leaf in leaves {
        let (col, op, lit) = decompose_leaf(leaf)?;
        match (col, op) {
            ("l_shipdate", Operator::GtEq) => match lit {
                ScalarValue::Date32(Some(d)) => date_lo = Some(d),
                _ => return None,
            },
            ("l_shipdate", Operator::Lt) => match lit {
                ScalarValue::Date32(Some(d)) => date_hi = Some(d),
                _ => return None,
            },
            ("l_discount", Operator::GtEq) => match lit {
                ScalarValue::Float64(Some(f)) => disc_lo = Some(f),
                _ => return None,
            },
            ("l_discount", Operator::LtEq) => match lit {
                ScalarValue::Float64(Some(f)) => disc_hi = Some(f),
                _ => return None,
            },
            ("l_quantity", Operator::Lt) => match lit {
                ScalarValue::Float64(Some(f)) => qty_hi = Some(f),
                _ => return None,
            },
            _ => return None,
        }
    }

    Some(Q6Predicate {
        date_lo: date_lo?,
        date_hi: date_hi?,
        disc_lo: disc_lo?,
        disc_hi: disc_hi?,
        qty_hi: qty_hi?,
    })
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

/// Try to interpret `expr` as `Column ⊕ Literal` (or its mirror) and
/// return the canonicalised `(column_name, op, literal)`. Returns
/// `None` for any other shape.
fn decompose_leaf(
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<(&'static str, Operator, ScalarValue)> {
    let b = expr.as_any().downcast_ref::<BinaryExpr>()?;
    let op = *b.op();
    let (col, lit, flipped) = match (
        b.left().as_any().downcast_ref::<Column>(),
        b.right().as_any().downcast_ref::<Literal>(),
        b.left().as_any().downcast_ref::<Literal>(),
        b.right().as_any().downcast_ref::<Column>(),
    ) {
        (Some(c), Some(l), _, _) => (c.name(), l.value().clone(), false),
        (_, _, Some(l), Some(c)) => (c.name(), l.value().clone(), true),
        _ => return None,
    };
    let op = if flipped { flip_op(op)? } else { op };
    let canonical: &'static str = match col {
        "l_shipdate" => "l_shipdate",
        "l_discount" => "l_discount",
        "l_quantity" => "l_quantity",
        _ => return None,
    };
    Some((canonical, op, lit))
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

fn dferr(msg: &str) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Internal(msg.into())
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

    /// Rule converts an existing hand-coded FusedFilterSumExec into
    /// its JIT variant: same structural ExecutionPlan, but `has_jit()`
    /// flips from false to true.
    #[tokio::test]
    async fn rule_enables_jit_on_hand_coded_q6_exec() {
        let input = input_plan_for_q6().await;
        let hand = Arc::new(
            FusedFilterSumExec::try_new_q6(input, q6_predicate_canonical()).unwrap(),
        );
        assert!(!hand.has_jit(), "starting state: hand-coded");

        let rule = EnableFusedJitRule;
        let cfg = ConfigOptions::new();
        let optimized = rule.optimize(hand.clone(), &cfg).unwrap();
        let downcast = optimized
            .as_any()
            .downcast_ref::<FusedFilterSumExec>()
            .expect("rule preserves the operator type");
        assert!(downcast.has_jit(), "rule should enable JIT");
    }

    /// Rule is idempotent: a plan already in JIT mode passes through
    /// unchanged (no double-build, no panics).
    #[tokio::test]
    async fn rule_is_idempotent() {
        let input = input_plan_for_q6().await;
        let already_jit = Arc::new(
            FusedFilterSumExec::try_new_q6_jit(input, q6_predicate_canonical()).unwrap(),
        );
        let rule = EnableFusedJitRule;
        let cfg = ConfigOptions::new();
        let optimized = rule.optimize(already_jit, &cfg).unwrap();
        let downcast = optimized
            .as_any()
            .downcast_ref::<FusedFilterSumExec>()
            .unwrap();
        assert!(downcast.has_jit());
    }

    /// End-to-end: rule-rewritten plan produces the same final
    /// f64 result as the original hand-coded plan. Validates that the
    /// rule preserves the output (no schema drift, no FP surprises).
    #[tokio::test]
    async fn rule_rewritten_plan_produces_same_result_as_hand_coded() {
        use datafusion::arrow::array::Float64Array;

        let hand_input = input_plan_for_q6().await;
        let hand_exec: Arc<dyn ExecutionPlan> = Arc::new(
            FusedFilterSumExec::try_new_q6(hand_input, q6_predicate_canonical()).unwrap(),
        );

        let rule_input = input_plan_for_q6().await;
        let pre_rule: Arc<dyn ExecutionPlan> = Arc::new(
            FusedFilterSumExec::try_new_q6(rule_input, q6_predicate_canonical()).unwrap(),
        );
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

    // ----- InjectFusedQ6Rule tests -----

    use std::path::PathBuf;

    fn sf1_lineitem() -> Option<String> {
        let env = std::env::var("TPCH_DATA_DIR").ok().map(PathBuf::from);
        let dir = match env {
            Some(p) if p.exists() => p,
            _ => {
                let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                manifest
                    .parent()?
                    .parent()?
                    .join("examples/tpch/data/sf1")
            }
        };
        let p = dir.join("lineitem.parquet");
        p.exists().then(|| p.to_string_lossy().into_owned())
    }

    const Q6_SQL: &str = "
        SELECT sum(l_extendedprice * l_discount) AS revenue
        FROM lineitem
        WHERE l_shipdate >= DATE '1994-01-01'
          AND l_shipdate <  DATE '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity <  24
    ";

    /// Build a SessionContext over real SF=1 lineitem.parquet
    /// (FastParquetTableProvider for parity with the bench harness) and
    /// optionally register the InjectFusedQ6Rule.
    async fn ctx_for_q6(register_rule: bool) -> Option<SessionContext> {
        use crate::fast_parquet::FastParquetTableProvider;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::SessionConfig;

        let path = sf1_lineitem()?;
        let state = if register_rule {
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(14))
                .with_default_features()
                .with_physical_optimizer_rule(Arc::new(InjectFusedQ6Rule))
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

    /// Inject rule fires on real Q6 SQL: the resulting physical plan
    /// contains a `FusedFilterSumExec` (the Filter+Aggregate stack got
    /// replaced) and the answer matches the unmodified plan to within
    /// floating-point tolerance.
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_rule_rewrites_real_q6_plan_and_preserves_result() {
        use datafusion::arrow::array::Float64Array;
        use datafusion::physical_plan::displayable;

        let Some(ctx_with) = ctx_for_q6(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx_without = ctx_for_q6(false).await.unwrap();

        // 1. Verify the rule's effect on the physical plan: tree
        //    should contain "FusedFilterSumExec" once.
        let df = ctx_with.sql(Q6_SQL).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_str.contains("FusedFilterSumExec"),
            "InjectFusedQ6Rule did not fire on the canonical Q6 plan.\nPlan:\n{plan_str}"
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
        let rel = ((v_with - v_without) / v_without).abs();
        assert!(
            rel < 1e-10,
            "rule-rewritten Q6 result diverges: with={v_with}, without={v_without}, rel_err={rel:e}"
        );
    }

    /// Rule must NOT fire on a query that shares some structural
    /// features but isn't Q6 (here: same SUM aggregate but no filter
    /// stack and different alias). The plan should pass through
    /// unchanged.
    #[tokio::test(flavor = "multi_thread")]
    async fn inject_rule_does_not_fire_on_unrelated_query() {
        use datafusion::physical_plan::displayable;
        let Some(ctx) = ctx_for_q6(true).await else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        // No filter stack, different alias — shouldn't match Q6.
        let plan = ctx
            .sql("SELECT sum(l_extendedprice * l_discount) AS total FROM lineitem")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            !plan_str.contains("FusedFilterSumExec"),
            "InjectFusedQ6Rule wrongly fired on a non-Q6 query:\n{plan_str}"
        );
    }
}
