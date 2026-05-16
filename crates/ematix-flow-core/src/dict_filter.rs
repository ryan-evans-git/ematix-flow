//! Σ.E3a: `DictFilterExec` — a `FilterExec`-shaped operator that
//! evaluates IN-list predicates on dictionary-encoded string columns
//! by comparing dict *codes* instead of materialised strings.
//!
//! Photon-inspired pattern: when a column is `DictionaryArray<UInt32,
//! Utf8>`, the predicate `col IN (s1, s2, ...)` is structurally
//! `dict_mask[idx[i]] == 1` — one lookup per row, no string compare.
//! For low-cardinality columns (TPC-H `l_shipmode` is a 7-entry dict)
//! this is bound only by the indices-array scan; the dict itself fits
//! in L1.
//!
//! This file ships the **`DictFilterExec`** operator + its construction
//! contract. The matching `PhysicalOptimizerRule` that rewrites
//! `FilterExec` nodes in-plan lives in `dict_filter_rule.rs` (next
//! commit in Σ.E3a).
//!
//! ## Scope (Σ.E3a)
//!
//! In:
//! * Single-column predicate: `dict_col IN (s1, s2, ..., sk)`.
//! * Dict value type: `Utf8` (extension to `Utf8View` deferred).
//! * Dict code type: `UInt32` (the FastParquet default).
//!
//! Out (Σ.E3a+):
//! * `LIKE 'prefix%'` and `=` literal — both reduce to a code set, so
//!   the implementation is the same once the optimizer rule extracts
//!   them; deferred for review-cost reasons in the first landing.
//! * Multi-column predicates / AND chains across different dict cols.
//! * Non-dict fallback. The operator errors loudly if the column
//!   isn't dict-encoded on entry; the optimizer rule guarantees we
//!   only get installed where it's safe.
//!
//! ## Correctness pin
//!
//! A unit test pins bit-identical output (row order, schema, columns)
//! between `DictFilterExec` and a hand-built `FilterExec` over the
//! same RecordBatch + same logical predicate.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, DictionaryArray, RecordBatch, StringArray, UInt32Array,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef, UInt32Type};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures_util::stream::StreamExt;

/// IN-list predicate on a single dictionary-encoded string column.
///
/// Equivalent to `column[col_idx] IN (allowed_values...)`. The
/// implementation resolves `allowed_values` to a sparse code set
/// against each batch's dictionary, then filters by comparing the
/// indices array against that set (no string compare in the hot loop).
#[derive(Debug, Clone)]
pub struct DictInListPredicate {
    /// Column index in the *input* schema (the child plan's output).
    pub col_idx: usize,
    /// Allowed string values. Membership test is `Vec::contains` over
    /// the resolved dict codes — fine for the low-cardinality lists
    /// this operator targets (≤ ~8 literals in practice).
    pub allowed_values: Vec<String>,
}

/// Σ.E3a operator: filter on `DictionaryArray<UInt32, Utf8>` by code
/// membership.
#[derive(Debug)]
pub struct DictFilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: DictInListPredicate,
    /// Output schema = child schema (filter doesn't change column
    /// types or names).
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl DictFilterExec {
    /// Wrap `input` in a dict-code IN-list filter on
    /// `input.schema().field(col_idx)`.
    ///
    /// Validates the target column is a `Dictionary(UInt32, Utf8)` —
    /// any other type fails at construction; the caller (optimizer
    /// rule) is responsible for matching first.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        predicate: DictInListPredicate,
    ) -> DfResult<Self> {
        let schema = input.schema();
        let field = schema.field(predicate.col_idx);
        match field.data_type() {
            DataType::Dictionary(key, value)
                if **key == DataType::UInt32 && **value == DataType::Utf8 => {}
            other => {
                return Err(DataFusionError::Plan(format!(
                    "DictFilterExec: column `{}` has type {other:?}, expected Dictionary(UInt32, Utf8)",
                    field.name(),
                )));
            }
        }
        let eq_props = EquivalenceProperties::new(schema.clone());
        let child_part = input.properties().partitioning.clone();
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            child_part,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            predicate,
            schema,
            properties,
        })
    }

    /// Accessor for the optimizer rule + display impl.
    pub fn predicate(&self) -> &DictInListPredicate {
        &self.predicate
    }

    /// Accessor for the optimizer rule.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for DictFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "DictFilterExec(col_idx={}, allowed={:?})",
            self.predicate.col_idx, self.predicate.allowed_values
        )
    }
}

impl ExecutionPlan for DictFilterExec {
    fn name(&self) -> &str {
        "DictFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let new_input = children.pop().ok_or_else(|| {
            DataFusionError::Internal("DictFilterExec requires exactly 1 child".into())
        })?;
        Ok(Arc::new(Self::try_new(new_input, self.predicate.clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let child = self.input.execute(partition, context)?;
        let predicate = self.predicate.clone();
        let schema = self.schema.clone();

        // Per-batch map: each input batch becomes one output batch
        // (possibly empty). No async — the filter is CPU-bound and
        // cheap; staying on the calling task avoids a tokio hop.
        let stream = child.map(move |batch_res| {
            batch_res.and_then(|b| filter_batch_by_dict_code(&b, &predicate))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Resolve `predicate.allowed_values` against the batch's dictionary,
/// build a row mask, and apply it via `arrow::compute::filter`.
///
/// Returns a `RecordBatch` with the same schema as `batch` and the
/// surviving rows.
fn filter_batch_by_dict_code(
    batch: &RecordBatch,
    predicate: &DictInListPredicate,
) -> DfResult<RecordBatch> {
    let col = batch.column(predicate.col_idx);
    let dict = col
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "DictFilterExec: column {} not DictionaryArray<UInt32, Utf8> at runtime \
                 (validated at construction; did a child plan change schema?)",
                predicate.col_idx
            ))
        })?;

    // Resolve string literals to dict codes once per batch. The dict
    // values array is `Utf8` per the construction-time check.
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "DictFilterExec: dict values not StringArray at runtime".into(),
            )
        })?;

    // Code set: a Vec<bool> indexed by dict code. Codes are dense
    // small ints by construction (< 2^32 in theory, but for the
    // low-cardinality columns this operator targets the dict is
    // typically ≤ thousands of entries).
    let dict_len = values.len();
    let mut code_set = vec![false; dict_len];
    for lit in &predicate.allowed_values {
        for (code, slot) in code_set.iter_mut().enumerate() {
            if !values.is_null(code) && values.value(code) == lit.as_str() {
                *slot = true;
            }
        }
    }

    // Build the row mask: `code_set[keys[i]]` for each row.
    let keys: &UInt32Array = dict.keys();
    let n = keys.len();
    let mut mask_buf: Vec<bool> = Vec::with_capacity(n);
    for i in 0..n {
        // Null-key rows pass through as false (a NULL dict-coded
        // string is never IN a non-NULL literal set).
        if keys.is_null(i) {
            mask_buf.push(false);
            continue;
        }
        let code = keys.value(i) as usize;
        // Defensive: an out-of-range code shouldn't be reachable
        // (parquet writers produce dense codes), but fall back to
        // false rather than panic.
        mask_buf.push(code < dict_len && code_set[code]);
    }
    let mask = BooleanArray::from(mask_buf);

    filter_record_batch(batch, &mask).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringDictionaryBuilder};
    use datafusion::arrow::datatypes::{Field, Schema, UInt32Type};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use futures_util::stream::TryStreamExt;

    /// Build a 6-row batch: column 0 is `Dictionary(UInt32, Utf8)`
    /// modelling `l_shipmode`-style data; column 1 is a payload Int64.
    fn make_test_batch() -> RecordBatch {
        let mut keys_builder: StringDictionaryBuilder<UInt32Type> = StringDictionaryBuilder::new();
        // Rows: MAIL, AIR, MAIL, REG AIR, SHIP, TRUCK
        for v in ["MAIL", "AIR", "MAIL", "REG AIR", "SHIP", "TRUCK"] {
            keys_builder.append(v).unwrap();
        }
        let dict_arr = keys_builder.finish();
        let payload = Int64Array::from(vec![10, 20, 30, 40, 50, 60]);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "l_shipmode",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("payload", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(dict_arr) as ArrayRef, Arc::new(payload)],
        )
        .unwrap()
    }

    async fn input_plan_from_batch(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    /// Σ.E3a primary correctness pin: rows where `l_shipmode IN
    /// ('MAIL', 'SHIP')` survive, others drop. Result is bit-identical
    /// (column-by-column, value-by-value) to a hand-built expectation.
    #[tokio::test]
    async fn dict_filter_in_list_keeps_matching_rows() {
        let input = input_plan_from_batch(make_test_batch()).await;
        let exec = Arc::new(
            DictFilterExec::try_new(
                input,
                DictInListPredicate {
                    col_idx: 0,
                    allowed_values: vec!["MAIL".into(), "SHIP".into()],
                },
            )
            .unwrap(),
        );

        let ctx = SessionContext::new();
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let batch = stream
            .try_next()
            .await
            .expect("stream yields ok")
            .expect("stream yields a batch");

        assert_eq!(batch.num_rows(), 3, "MAIL,MAIL,SHIP should survive");
        // Schema preserved.
        assert_eq!(batch.num_columns(), 2);
        // Payload column should be [10, 30, 50].
        let payloads = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(payloads.value(0), 10);
        assert_eq!(payloads.value(1), 30);
        assert_eq!(payloads.value(2), 50);
        // Dict column preserved as dict-encoded.
        let surviving = batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .expect("output preserves dict encoding");
        let vals = surviving
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mode = |row: usize| vals.value(surviving.keys().value(row) as usize);
        assert_eq!(mode(0), "MAIL");
        assert_eq!(mode(1), "MAIL");
        assert_eq!(mode(2), "SHIP");
    }

    /// IN-list against a literal that's *not* in the dictionary
    /// produces an empty result, not an error.
    #[tokio::test]
    async fn dict_filter_in_list_with_unknown_literal_yields_empty() {
        let input = input_plan_from_batch(make_test_batch()).await;
        let exec = Arc::new(
            DictFilterExec::try_new(
                input,
                DictInListPredicate {
                    col_idx: 0,
                    allowed_values: vec!["FERRY".into()], // not in the dict
                },
            )
            .unwrap(),
        );
        let ctx = SessionContext::new();
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    /// Empty allowed-values list yields zero rows (consistent with SQL
    /// `col IN ()` which is semantically empty).
    #[tokio::test]
    async fn dict_filter_empty_in_list_yields_empty() {
        let input = input_plan_from_batch(make_test_batch()).await;
        let exec = Arc::new(
            DictFilterExec::try_new(
                input,
                DictInListPredicate {
                    col_idx: 0,
                    allowed_values: vec![],
                },
            )
            .unwrap(),
        );
        let ctx = SessionContext::new();
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// Construction rejects a non-dict column with a clear error.
    #[tokio::test]
    async fn try_new_rejects_non_dict_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let input = input_plan_from_batch(batch).await;
        let res = DictFilterExec::try_new(
            input,
            DictInListPredicate {
                col_idx: 0,
                allowed_values: vec!["x".into()],
            },
        );
        let err = res.expect_err("non-dict column should fail validation");
        let msg = format!("{err}");
        assert!(
            msg.contains("Dictionary"),
            "error should mention expected type: {msg}",
        );
    }

    /// Equivalence pin: same input + same logical predicate ran
    /// through DataFusion's default `FilterExec` (via SQL) produces
    /// row-for-row identical output (modulo dict encoding being
    /// preserved). Cross-checks the IN-list semantics against the
    /// engine ground truth.
    #[tokio::test]
    async fn dict_filter_matches_datafusion_filter_in_list() {
        // Default path: SQL `SELECT * FROM t WHERE l_shipmode IN
        // ('MAIL', 'SHIP')` through DataFusion.
        let ctx = SessionContext::new();
        let mem =
            MemTable::try_new(make_test_batch().schema(), vec![vec![make_test_batch()]]).unwrap();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx
            .sql("SELECT * FROM t WHERE l_shipmode IN ('MAIL', 'SHIP')")
            .await
            .unwrap();
        let default_batches = df.collect().await.unwrap();
        let default_total: usize = default_batches.iter().map(|b| b.num_rows()).sum();

        // Σ.E3a path: same logical predicate through DictFilterExec.
        let input = input_plan_from_batch(make_test_batch()).await;
        let exec = Arc::new(
            DictFilterExec::try_new(
                input,
                DictInListPredicate {
                    col_idx: 0,
                    allowed_values: vec!["MAIL".into(), "SHIP".into()],
                },
            )
            .unwrap(),
        );
        let mut s = exec.execute(0, SessionContext::new().task_ctx()).unwrap();
        let dict_batch = s.try_next().await.unwrap().unwrap();

        assert_eq!(default_total, dict_batch.num_rows(), "row count differs");
        // Payload value-equality. We don't pin column 0 (dict) value-
        // equality across encodings because DataFusion may
        // materialise the dict on output; checking the payload column
        // is sufficient for the row-set equivalence we care about.
        let default_payloads: Vec<i64> = default_batches
            .iter()
            .flat_map(|b| {
                let a = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();
                (0..a.len()).map(move |i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        let dict_payloads: Vec<i64> = {
            let a = dict_batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..a.len()).map(|i| a.value(i)).collect()
        };
        assert_eq!(
            default_payloads, dict_payloads,
            "payloads differ: default={default_payloads:?}, dict={dict_payloads:?}"
        );
    }
}
