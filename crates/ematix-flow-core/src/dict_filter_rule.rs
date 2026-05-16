//! Σ.E3a: `PhysicalOptimizerRule` that rewrites matching `FilterExec`
//! nodes to [`crate::dict_filter::DictFilterExec`].
//!
//! Patterns detected (any one of, all referencing the same column,
//! which must have `Dictionary(UInt32, Utf8)` in the child schema):
//!
//! 1. `column[i] IN (utf8_lit, ...)` — `InListExpr(negated=false)`
//!    (large IN-lists stay as InListExpr).
//! 2. `column[i] = lit OR column[i] = lit OR ...` — DataFusion's
//!    planner unfolds short IN-lists (typically ≤ 3 elements) into
//!    an OR-tree of Eq nodes.
//! 3. `column[i] = lit` — single equality.
//!
//! Each literal must be `Utf8(Some)`, `LargeUtf8(Some)`, or
//! `Dictionary(_, Utf8(Some))` — the last form is what DataFusion's
//! planner emits when it casts a string literal to match the dict
//! column's type during predicate unification.
//!
//! Out of scope for Σ.E3a (deferred):
//! * Projecting FilterExec nodes (`FilterExec { projection: Some }`).
//!   Rewriting those without preserving the projection produces
//!   wrong-schema output downstream; a future commit wraps the
//!   rewrite in a ProjectionExec.
//! * `NOT IN`, mixed-type lists, NULL literals.
//!
//! The rule is **strictly speculative**: any departure from the
//! patterns above is a no-op — the FilterExec passes through
//! unchanged and DataFusion's default evaluation runs.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::common::ScalarValue;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr, Literal};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;

use crate::dict_filter::{DictFilterExec, DictInListPredicate};

/// Walks the physical plan and rewrites every `FilterExec(InList on
/// Dictionary(UInt32, Utf8) column)` to a [`DictFilterExec`].
/// Idempotent — DictFilterExec nodes pass through unchanged.
#[derive(Debug, Default)]
pub struct EnableDictFilterRule;

impl PhysicalOptimizerRule for EnableDictFilterRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|node| {
            let Some(filter) = node.as_any().downcast_ref::<FilterExec>() else {
                return Ok(Transformed::no(node));
            };
            // Try to extract a (col_idx, allowed_values) pair from the
            // FilterExec's predicate. Bail to "no rewrite" on any
            // mismatch — never wrong-answer.
            let Some(predicate) = match_in_list_on_dict_column(filter) else {
                return Ok(Transformed::no(node));
            };
            let new = DictFilterExec::try_new(filter.input().clone(), predicate)?;
            Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_dict_filter"
    }

    fn schema_check(&self) -> bool {
        // FilterExec and DictFilterExec produce identical output
        // schemas (filter doesn't change types or names).
        true
    }
}

/// Inspect a `FilterExec`'s predicate. Returns `Some(predicate)` iff
/// the predicate has either shape — all referencing the same column,
/// which must have `Dictionary(UInt32, Utf8)` in the child schema:
///
/// 1. `column[i] IN (utf8_literal, ...)` — `InListExpr(negated=false)`.
/// 2. `column[i] = lit OR column[i] = lit OR ...` — DataFusion
///    short-IN-list expansion (≤ ~3 elements). Recursive OR-tree.
/// 3. `column[i] = lit` — single equality.
///
/// All other shapes — NOT IN, mixed-type lists, NULL literals,
/// non-Utf8 literals, comparisons on non-dict columns — return
/// `None`, leaving the rule a no-op.
fn match_in_list_on_dict_column(filter: &FilterExec) -> Option<DictInListPredicate> {
    // Σ.E3a scope: defer handling FilterExecs with inline projection
    // (e.g. `SELECT col FROM t WHERE ...` collapses filter+project
    // into one node). Rewriting those without preserving the projection
    // produces wrong-schema output downstream. Follow-up will wrap
    // the rewrite in a ProjectionExec.
    if filter.projection().is_some() {
        return None;
    }
    let child_schema = filter.input().schema();

    // Collect (col_idx, allowed_values) from whichever predicate shape
    // we can recognise. Bail to None on ambiguity (different columns
    // appearing in different OR branches).
    let mut col_idx: Option<usize> = None;
    let mut allowed: Vec<String> = Vec::new();
    extract_dict_or_chain(filter.predicate(), &mut col_idx, &mut allowed).ok()?;

    let col_idx = col_idx?;
    if allowed.is_empty() {
        return None;
    }

    // Sanity check the column type.
    if col_idx >= child_schema.fields().len() {
        return None;
    }
    match child_schema.field(col_idx).data_type() {
        DataType::Dictionary(key, value)
            if **key == DataType::UInt32 && **value == DataType::Utf8 => {}
        _ => return None,
    }

    Some(DictInListPredicate {
        col_idx,
        allowed_values: allowed,
    })
}

/// Walk a predicate expression, appending `(col_idx, literal)` pairs
/// into `col_idx_out` + `allowed`. Returns `Err(())` if the shape
/// doesn't match (anything other than InList / OR-chain / single
/// equality on Utf8 literals).
///
/// `col_idx_out` is `Option<usize>` — set on first hit, then must
/// remain the same for every subsequent leaf. Different column
/// indices across OR branches → `Err(())`.
fn extract_dict_or_chain(
    expr: &Arc<dyn PhysicalExpr>,
    col_idx_out: &mut Option<usize>,
    allowed: &mut Vec<String>,
) -> Result<(), ()> {
    // Case A: InListExpr (large IN-lists stay as InListExpr).
    if let Some(in_list) = expr.as_any().downcast_ref::<InListExpr>() {
        if in_list.negated() {
            return Err(());
        }
        let column = in_list.expr().as_any().downcast_ref::<Column>().ok_or(())?;
        set_or_check_col_idx(col_idx_out, column.index())?;
        for item in in_list.list() {
            let lit = item.as_any().downcast_ref::<Literal>().ok_or(())?;
            allowed.push(extract_utf8_literal(lit.value()).ok_or(())?);
        }
        return Ok(());
    }

    // Case B/C: BinaryExpr — either OR-chain or single Eq.
    if let Some(bin) = expr.as_any().downcast_ref::<BinaryExpr>() {
        match bin.op() {
            Operator::Or => {
                // Recurse into both sides; both must match shape.
                extract_dict_or_chain(bin.left(), col_idx_out, allowed)?;
                extract_dict_or_chain(bin.right(), col_idx_out, allowed)?;
                return Ok(());
            }
            Operator::Eq => {
                // `Column = Literal(Utf8)` (literal can be either side).
                let (column, lit) = match (
                    bin.left().as_any().downcast_ref::<Column>(),
                    bin.right().as_any().downcast_ref::<Literal>(),
                ) {
                    (Some(c), Some(l)) => (c, l),
                    _ => match (
                        bin.left().as_any().downcast_ref::<Literal>(),
                        bin.right().as_any().downcast_ref::<Column>(),
                    ) {
                        (Some(l), Some(c)) => (c, l),
                        _ => return Err(()),
                    },
                };
                set_or_check_col_idx(col_idx_out, column.index())?;
                allowed.push(extract_utf8_literal(lit.value()).ok_or(())?);
                return Ok(());
            }
            _ => return Err(()),
        }
    }

    Err(())
}

/// Extract a Utf8 string from a literal `ScalarValue`. Handles bare
/// `Utf8(Some)` and the DataFusion-planner-emitted
/// `Dictionary(_, Box<Utf8(Some)>)` wrapper (the planner re-casts
/// string literals to match a dict column's type during predicate
/// type unification).
fn extract_utf8_literal(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        ScalarValue::Dictionary(_, inner) => extract_utf8_literal(inner),
        _ => None,
    }
}

fn set_or_check_col_idx(slot: &mut Option<usize>, idx: usize) -> Result<(), ()> {
    match *slot {
        None => {
            *slot = Some(idx);
            Ok(())
        }
        Some(existing) if existing == idx => Ok(()),
        _ => Err(()), // Different columns across OR branches → don't rewrite.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict_filter::DictFilterExec;
    use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringDictionaryBuilder};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use futures_util::stream::TryStreamExt;

    /// Build the same `l_shipmode`-style fixture as the operator
    /// tests, but routed through SessionContext so the planner
    /// constructs a real `FilterExec(InList)` we can rewrite.
    async fn ctx_with_shipmode_fixture() -> SessionContext {
        let mut keys: StringDictionaryBuilder<UInt32Type> = StringDictionaryBuilder::new();
        for v in ["MAIL", "AIR", "MAIL", "REG AIR", "SHIP", "TRUCK"] {
            keys.append(v).unwrap();
        }
        let dict = keys.finish();
        let payload = Int64Array::from(vec![10i64, 20, 30, 40, 50, 60]);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "l_shipmode",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("payload", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(dict) as ArrayRef, Arc::new(payload)],
        )
        .unwrap();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        ctx
    }

    /// The rule walks the plan tree and replaces the FilterExec with
    /// a DictFilterExec. Verified by inspecting the post-optimize
    /// plan's root types.
    #[tokio::test]
    async fn rule_rewrites_filter_to_dict_filter() {
        let ctx = ctx_with_shipmode_fixture().await;
        let df = ctx
            .sql("SELECT * FROM t WHERE l_shipmode IN ('MAIL', 'SHIP')")
            .await
            .unwrap();
        let physical = df.create_physical_plan().await.unwrap();

        let rewritten = EnableDictFilterRule
            .optimize(physical.clone(), &ConfigOptions::default())
            .unwrap();

        // Walk down until we find either FilterExec or DictFilterExec.
        // (There's a ProjectionExec on top from the SELECT payload.)
        fn find_filter_or_dict(node: &Arc<dyn ExecutionPlan>) -> Option<&'static str> {
            if node.as_any().is::<DictFilterExec>() {
                return Some("dict");
            }
            if node.as_any().is::<FilterExec>() {
                return Some("filter");
            }
            for child in node.children() {
                if let Some(name) = find_filter_or_dict(child) {
                    return Some(name);
                }
            }
            None
        }
        let before = find_filter_or_dict(&physical);
        let after = find_filter_or_dict(&rewritten);
        assert_eq!(
            before,
            Some("filter"),
            "default plan should have FilterExec"
        );
        assert_eq!(after, Some("dict"), "rule should rewrite to DictFilterExec");
    }

    /// End-to-end: running the rewritten plan produces the same rows
    /// as DataFusion's default execution. The rule must be
    /// behaviour-preserving.
    #[tokio::test]
    async fn rule_preserves_result() {
        // Default execution.
        let ctx = ctx_with_shipmode_fixture().await;
        let default_rows: Vec<i64> = ctx
            .sql("SELECT payload FROM t WHERE l_shipmode IN ('MAIL', 'SHIP') ORDER BY payload")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .flat_map(|b| {
                let a = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();
                (0..a.len()).map(move |i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();

        // Rule-rewritten execution.
        let ctx2 = ctx_with_shipmode_fixture().await;
        let df = ctx2
            .sql("SELECT * FROM t WHERE l_shipmode IN ('MAIL', 'SHIP')")
            .await
            .unwrap();
        let physical = df.create_physical_plan().await.unwrap();
        let rewritten = EnableDictFilterRule
            .optimize(physical, &ConfigOptions::default())
            .unwrap();
        // SELECT * keeps both columns: l_shipmode (dict) at 0,
        // payload at 1. We compare on payload.
        let mut rule_rows: Vec<i64> = Vec::new();
        let parts = rewritten.properties().partitioning.partition_count();
        for p in 0..parts {
            let mut s = rewritten.execute(p, ctx2.task_ctx()).unwrap();
            while let Some(b) = s.try_next().await.unwrap() {
                let a = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..a.len() {
                    rule_rows.push(a.value(i));
                }
            }
        }
        rule_rows.sort();
        let mut def = default_rows.clone();
        def.sort();
        assert_eq!(
            rule_rows, def,
            "rule output differs: default={def:?}, rule={rule_rows:?}"
        );
    }

    /// NOT IN must NOT be rewritten — the rule's IN-list path doesn't
    /// handle negation. Plan passes through unchanged.
    #[tokio::test]
    async fn rule_skips_not_in_list() {
        let ctx = ctx_with_shipmode_fixture().await;
        let df = ctx
            .sql("SELECT payload FROM t WHERE l_shipmode NOT IN ('MAIL')")
            .await
            .unwrap();
        let physical = df.create_physical_plan().await.unwrap();
        let rewritten = EnableDictFilterRule
            .optimize(physical, &ConfigOptions::default())
            .unwrap();

        fn has_dict_filter(node: &Arc<dyn ExecutionPlan>) -> bool {
            if node.as_any().is::<DictFilterExec>() {
                return true;
            }
            node.children().iter().any(|c| has_dict_filter(c))
        }
        assert!(
            !has_dict_filter(&rewritten),
            "NOT IN must not be rewritten to DictFilterExec",
        );
    }

    /// A FilterExec over a column whose type is NOT dict-encoded
    /// must be left alone. Same SQL shape, different child type.
    #[tokio::test]
    async fn rule_skips_non_dict_column() {
        let ctx = SessionContext::new();
        // Same shape but l_shipmode is plain Utf8.
        ctx.sql(
            "CREATE TABLE t (l_shipmode VARCHAR, payload BIGINT) AS \
             SELECT * FROM (VALUES ('MAIL', 1), ('AIR', 2)) AS x(l_shipmode, payload)",
        )
        .await
        .unwrap();
        let df = ctx
            .sql("SELECT payload FROM t WHERE l_shipmode IN ('MAIL')")
            .await
            .unwrap();
        let physical = df.create_physical_plan().await.unwrap();
        let rewritten = EnableDictFilterRule
            .optimize(physical, &ConfigOptions::default())
            .unwrap();

        fn has_dict_filter(node: &Arc<dyn ExecutionPlan>) -> bool {
            if node.as_any().is::<DictFilterExec>() {
                return true;
            }
            node.children().iter().any(|c| has_dict_filter(c))
        }
        assert!(
            !has_dict_filter(&rewritten),
            "Utf8 (non-dict) column must not be rewritten",
        );
    }
}
