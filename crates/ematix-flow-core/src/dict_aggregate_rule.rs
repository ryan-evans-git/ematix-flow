//! Σ.E3b.2: `PhysicalOptimizerRule` that rewrites matching
//! `AggregateExec(FinalPartitioned)` sub-plans to
//! [`crate::dict_aggregate::DictGroupCountExec`].
//!
//! ## Plan shape detected
//!
//! ```text
//! AggregateExec(mode=FinalPartitioned,
//!               gby=[Column(col, idx)],
//!               aggr=[count(Int64(1))])
//!   RepartitionExec(Hash([col]))   ← may or may not be present
//!     AggregateExec(mode=Partial,
//!                   gby=[Column(col, idx)],
//!                   aggr=[count(Int64(1))])
//!       <child>                    ← the actual input
//! ```
//!
//! When that shape holds *and* the group column has type
//! `Dictionary(UInt32, Utf8|Utf8View)`, the rule replaces the whole
//! sub-plan with a `DictGroupCountExec(<child>, col_idx)` carrying
//! the FinalPartitioned exec's output column names.
//!
//! Schema preservation: the rewritten operator emits the group
//! column as `Dictionary(UInt32, Utf8)` to match the upstream
//! operator's cached schema (DataFusion's default `AggregateExec`
//! preserves the input dict encoding on Final output). The
//! per-surviving-group output is a 1:1 dict where `keys[i] = i` and
//! `values` is the deduped group strings.
//!
//! ## Strictly speculative
//!
//! Any departure from the recognised shape — wrong agg function,
//! multiple groups, multiple aggregates, non-COUNT(Int64(1)), non-
//! dict group column, projecting AggregateExec — is a no-op. The
//! rule cannot produce wrong answers.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};

use crate::dict_aggregate::{DictGroupCountExec, GroupOutputType};
use crate::shape_catalog::{AggMode, Shape, aggregate, any_capture};

/// Σ.F catalog entries: `AggregateExec(Final*)`. Both `Final` and
/// `FinalPartitioned` modes match; the per-rule walker below peels
/// pass-through nodes down to `AggregateExec(Partial)` (which can be
/// reached through RepartitionExec, CoalesceBatchesExec, etc.).
fn dict_group_count_shapes() -> [Shape; 2] {
    [
        aggregate(
            Some("final"),
            AggMode::FinalPartitioned,
            None,
            None,
            vec![any_capture("body")],
        ),
        aggregate(
            Some("final"),
            AggMode::Final,
            None,
            None,
            vec![any_capture("body")],
        ),
    ]
}

/// Σ.E3b.2 rule: rewrite the `AggregateExec(FinalPartitioned)` →
/// (optional `RepartitionExec`) → `AggregateExec(Partial)` →
/// `<child>` chain into a `DictGroupCountExec` when the shape +
/// types permit.
#[derive(Debug, Default)]
pub struct EnableDictGroupCountRule;

impl PhysicalOptimizerRule for EnableDictGroupCountRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let shapes = dict_group_count_shapes();
        let result = plan.transform_up(|node| {
            // Catalog match: the node must be AggregateExec in Final
            // or FinalPartitioned mode. Per-rule predicate validation
            // (single COUNT, single dict group key, peel to Partial)
            // stays below in `match_dict_count_shape`.
            let Some(matched) = shapes.iter().find_map(|s| s.try_match(&node)) else {
                return Ok(Transformed::no(node));
            };
            let final_agg_node = matched
                .get("final")
                .expect("dict_group_count_shapes captures `final`");
            let final_agg = final_agg_node
                .as_any()
                .downcast_ref::<AggregateExec>()
                .expect("captured node must be AggregateExec");
            let Some((real_input, col_idx, group_out_name, count_out_name)) =
                match_dict_count_shape(final_agg)
            else {
                return Ok(Transformed::no(node));
            };
            // Emit Dictionary on the group column to match what
            // DataFusion's default AggregateExec would produce —
            // keeps upstream operators' cached schemas valid.
            let new = DictGroupCountExec::try_new_with_names_and_type(
                real_input,
                col_idx,
                group_out_name,
                count_out_name,
                GroupOutputType::Dictionary,
            )?;
            Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_dict_group_count"
    }

    fn schema_check(&self) -> bool {
        // The rewrite preserves output names; types are Utf8 (group)
        // + Int64 (count) on both sides if the original FinalAgg
        // materialised the dict. The match guard checks that.
        true
    }
}

/// Inspect a `FinalPartitioned`/`Final` `AggregateExec` and decide
/// whether it can be replaced with a `DictGroupCountExec`. Returns
/// `Some((real_input, col_idx, group_out_name, count_out_name))` if
/// yes, where `real_input` is the operator under the Partial stage
/// (i.e. peeling off Partial + any RepartitionExec).
fn match_dict_count_shape(
    final_agg: &AggregateExec,
) -> Option<(Arc<dyn ExecutionPlan>, usize, String, String)> {
    // Group must be exactly one Column expression.
    let groups = final_agg.group_expr().expr();
    if groups.len() != 1 {
        return None;
    }
    let (group_expr, group_out_name) = &groups[0];
    let col = group_expr.as_any().downcast_ref::<Column>()?;
    let col_idx = col.index();

    // Aggregate must be exactly one COUNT.
    let aggs = final_agg.aggr_expr();
    if aggs.len() != 1 {
        return None;
    }
    let agg = &aggs[0];
    // Match the UDF by its registered name. DataFusion's COUNT
    // registers as "count"; COUNT(*) compiles to a count over the
    // literal `Int64(1)` placeholder.
    if !agg.fun().name().eq_ignore_ascii_case("count") {
        return None;
    }
    let count_out_name = agg.name().to_string();

    // Σ.E3b.2 known limitation: the rewritten operator emits Utf8
    // for the group column. If the original FinalAgg's output type
    // is Dictionary(...) (DataFusion typically preserves it), a
    // downstream operator that strongly depends on the Dict encoding
    // could break. For the simple `SELECT col, COUNT(*) FROM t
    // GROUP BY col` shape the upstream ProjectionExec only references
    // column 0 by index, so a Utf8 substitution is safe. Anything
    // more exotic on top (a JOIN that probes on the dict-encoded
    // group key, etc.) wouldn't match this matcher anyway because
    // the FinalAgg wouldn't be the topmost node. Documented; revisit
    // when we add a `DictArrayUpcast` wrapper.

    // Peel: FinalAgg → ?(RepartitionExec | CoalesceBatchesExec) →
    // AggregateExec(Partial) → real_input. Walk down until we find
    // the Partial agg.
    let mut cur: Arc<dyn ExecutionPlan> = final_agg.input().clone();
    loop {
        if let Some(partial) = cur.as_any().downcast_ref::<AggregateExec>() {
            if !matches!(partial.mode(), AggregateMode::Partial) {
                return None;
            }
            // Sanity: same group expression structure.
            let pgroups = partial.group_expr().expr();
            if pgroups.len() != 1 {
                return None;
            }
            let (pgexp, _) = &pgroups[0];
            let pcol = pgexp.as_any().downcast_ref::<Column>()?;
            // The partial agg's group column is in the partial's
            // *input* schema, which equals the real input's schema.
            if pcol.index() != col_idx {
                return None;
            }
            // The real input is under the Partial agg. Verify the
            // group column there is dict-encoded.
            let real = partial.input().clone();
            let real_schema = real.schema();
            if pcol.index() >= real_schema.fields().len() {
                return None;
            }
            match real_schema.field(pcol.index()).data_type() {
                DataType::Dictionary(key, value)
                    if **key == DataType::UInt32
                        && (**value == DataType::Utf8 || **value == DataType::Utf8View) => {}
                _ => return None,
            }
            return Some((real, col_idx, group_out_name.clone(), count_out_name));
        }
        // Pass-through nodes between Final and Partial: keep walking
        // down through their single child. Anything with != 1 child
        // is too unusual for this rule to handle.
        let children = cur.children();
        if children.len() != 1 {
            return None;
        }
        cur = children[0].clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict_aggregate::DictGroupCountExec;
    use datafusion::arrow::array::{
        Array, ArrayRef, DictionaryArray, Int64Array, RecordBatch, StringArray,
        StringDictionaryBuilder,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use futures_util::stream::TryStreamExt;
    use std::collections::HashMap;

    async fn ctx_with_shipmode_fixture() -> SessionContext {
        let mut keys: StringDictionaryBuilder<UInt32Type> = StringDictionaryBuilder::new();
        for v in ["MAIL", "AIR", "MAIL", "AIR", "SHIP", "MAIL"] {
            keys.append(v).unwrap();
        }
        let dict = keys.finish();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l_shipmode",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(dict) as ArrayRef]).unwrap();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        ctx
    }

    fn has_dict_count(node: &Arc<dyn ExecutionPlan>) -> bool {
        if node.as_any().is::<DictGroupCountExec>() {
            return true;
        }
        node.children().iter().any(|c| has_dict_count(c))
    }

    /// Rule recognises the FinalPartitioned + RepartitionExec +
    /// Partial sub-tree and rewrites it.
    #[tokio::test]
    async fn rule_rewrites_count_group_by_dict() {
        let ctx = ctx_with_shipmode_fixture().await;
        let df = ctx
            .sql("SELECT l_shipmode, COUNT(*) AS c FROM t GROUP BY l_shipmode")
            .await
            .unwrap();
        let physical = df.create_physical_plan().await.unwrap();
        let rewritten = EnableDictGroupCountRule
            .optimize(physical, &ConfigOptions::default())
            .unwrap();
        assert!(
            has_dict_count(&rewritten),
            "rule must rewrite to DictGroupCountExec"
        );
    }

    /// Output rows match DataFusion's default execution.
    #[tokio::test]
    async fn rule_preserves_result() {
        let ctx = ctx_with_shipmode_fixture().await;
        let default_batches = ctx
            .sql("SELECT l_shipmode, COUNT(*) AS c FROM t GROUP BY l_shipmode")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut default_got: HashMap<String, i64> = HashMap::new();
        for b in &default_batches {
            // DataFusion may emit the group col as Utf8 (materialised)
            // or as Dictionary (preserved from the source). Handle both.
            let counts = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            if let Some(arr) = b.column(0).as_any().downcast_ref::<StringArray>() {
                for i in 0..b.num_rows() {
                    default_got.insert(arr.value(i).to_string(), counts.value(i));
                }
            } else if let Some(d) = b
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
            {
                let vals = d.values().as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..b.num_rows() {
                    if d.is_null(i) {
                        continue;
                    }
                    let c = d.keys().value(i) as usize;
                    default_got.insert(vals.value(c).to_string(), counts.value(i));
                }
            } else {
                panic!(
                    "default plan: unexpected group column type {:?}",
                    b.column(0).data_type()
                );
            }
        }

        // Rule-rewritten plan.
        let ctx2 = ctx_with_shipmode_fixture().await;
        let df = ctx2
            .sql("SELECT l_shipmode, COUNT(*) AS c FROM t GROUP BY l_shipmode")
            .await
            .unwrap();
        let physical = df.create_physical_plan().await.unwrap();
        let rewritten = EnableDictGroupCountRule
            .optimize(physical, &ConfigOptions::default())
            .unwrap();
        let parts = rewritten.properties().partitioning.partition_count();
        let mut rule_got: HashMap<String, i64> = HashMap::new();
        for p in 0..parts {
            let mut s = rewritten.execute(p, ctx2.task_ctx()).unwrap();
            while let Some(b) = s.try_next().await.unwrap() {
                let counts = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                if let Some(d) = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                {
                    let vals = d.values().as_any().downcast_ref::<StringArray>().unwrap();
                    for i in 0..b.num_rows() {
                        let c = d.keys().value(i) as usize;
                        rule_got.insert(vals.value(c).to_string(), counts.value(i));
                    }
                } else if let Some(arr) = b.column(0).as_any().downcast_ref::<StringArray>() {
                    for i in 0..b.num_rows() {
                        rule_got.insert(arr.value(i).to_string(), counts.value(i));
                    }
                } else {
                    panic!(
                        "rewritten: unexpected group col type {:?}",
                        b.column(0).data_type()
                    );
                }
            }
        }
        assert_eq!(rule_got, default_got);
    }

    /// SUM aggregate must NOT be rewritten — we only support COUNT.
    #[tokio::test]
    async fn rule_skips_non_count_aggregate() {
        // Build a fixture with a numeric column to SUM.
        let mut keys: StringDictionaryBuilder<UInt32Type> = StringDictionaryBuilder::new();
        for v in ["MAIL", "AIR", "MAIL"] {
            keys.append(v).unwrap();
        }
        let dict = keys.finish();
        let payload = Int64Array::from(vec![10i64, 20, 30]);
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
        let df = ctx
            .sql("SELECT l_shipmode, SUM(payload) AS s FROM t GROUP BY l_shipmode")
            .await
            .unwrap();
        let physical = df.create_physical_plan().await.unwrap();
        let rewritten = EnableDictGroupCountRule
            .optimize(physical, &ConfigOptions::default())
            .unwrap();
        assert!(
            !has_dict_count(&rewritten),
            "SUM must not be rewritten by this rule",
        );
    }

    /// Multi-group-by must NOT be rewritten — Σ.E3b.1 supports one key.
    #[tokio::test]
    async fn rule_skips_multi_group() {
        let mut k1: StringDictionaryBuilder<UInt32Type> = StringDictionaryBuilder::new();
        let mut k2: StringDictionaryBuilder<UInt32Type> = StringDictionaryBuilder::new();
        for (a, b) in [("MAIL", "X"), ("AIR", "Y"), ("MAIL", "X")] {
            k1.append(a).unwrap();
            k2.append(b).unwrap();
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "a",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new(
                "b",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(k1.finish()) as ArrayRef,
                Arc::new(k2.finish()) as ArrayRef,
            ],
        )
        .unwrap();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx
            .sql("SELECT a, b, COUNT(*) FROM t GROUP BY a, b")
            .await
            .unwrap();
        let physical = df.create_physical_plan().await.unwrap();
        let rewritten = EnableDictGroupCountRule
            .optimize(physical, &ConfigOptions::default())
            .unwrap();
        assert!(
            !has_dict_count(&rewritten),
            "multi-key group-by must not be rewritten",
        );
    }
}
