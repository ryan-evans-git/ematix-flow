//! Σ.E3b.1: `DictGroupCountExec` — single-key dict-aware
//! `COUNT(*) GROUP BY dict_col` operator.
//!
//! The smallest landable bite of Σ.E3b (dict-aware hash aggregate).
//! Scope cut to one group-by key + one aggregate (COUNT(*)) so the
//! pattern is reviewable in one diff. Extension to SUM / MIN / MAX
//! and multi-key group-by is Σ.E3b.2+ on top of this skeleton.
//!
//! ## Algorithm
//!
//! For each input batch:
//! 1. Downcast the group column to `DictionaryArray<UInt32, Utf8|
//!    Utf8View>` and read its dictionary values.
//! 2. Resolve each *unique* dict code in this batch to the group's
//!    *canonical string* once. The first time a string appears, it
//!    gets a fresh slot in the global counter table; subsequent
//!    batches reuse that slot via a `HashMap<String, slot_idx>`.
//!    This is the dict-aware win: ≤ `dict.len()` string-hashes per
//!    batch instead of one per row.
//! 3. Per row: look up its dict code → batch's code-to-slot table →
//!    increment `counts[slot]`.
//!
//! On `output()`: emit one RecordBatch with `(group_value: Utf8,
//! count: Int64)` columns; one row per slot.
//!
//! ## Why a HashMap<String, slot> rather than direct Vec<u64> indexed
//! by dict code?
//!
//! Codes are stable *within* a parquet column chunk but not *across*
//! batches in the general case (every batch a scan emits may carry
//! a fresh dictionary). The HashMap keyed by string value is the
//! correctness floor; the *per-batch* lookup table built from each
//! batch's dict gives back the constant-time-per-row property the
//! Photon pattern wants.
//!
//! ## Out of scope (Σ.E3b.2+)
//!
//! - Multi-key group-by (pack two dict codes into a u64).
//! - SUM/MIN/MAX/AVG aggregates.
//! - Partial vs Final aggregation modes (multi-partition merge).
//!   The current operator collects all input into one accumulator
//!   inside one task, then emits one batch — fine for SF=1 / SF=10
//!   single-host benches, not for distributed Σ.B execution.
//! - Spilling under memory pressure.
//! - Optimizer rule injection. Today's operator is hand-constructed
//!   in tests + bench harnesses; a follow-up rule will rewrite
//!   matching `AggregateExec` shapes.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, Int64Array, RecordBatch, StringArray, StringViewArray,
    UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt32Type};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};

/// Σ.E3b.1 operator: `SELECT col, COUNT(*) FROM child GROUP BY col`
/// where `col` is `Dictionary(UInt32, Utf8|Utf8View)`.
///
/// Output schema is `[<col-name>: Utf8, count: Int64]`. Output is
/// emitted as a single batch on partition 0 after all input is
/// drained — `EmissionType::Final`, `Boundedness::Bounded`.
#[derive(Debug)]
pub struct DictGroupCountExec {
    input: Arc<dyn ExecutionPlan>,
    /// Column index in the *input* schema.
    group_col_idx: usize,
    /// How to materialise the group column in output: bare Utf8 or
    /// re-wrapped as Dictionary(UInt32, Utf8).
    group_out_type: GroupOutputType,
    /// Pre-built output schema (`[group_value, count]`).
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

/// Output materialisation for the group column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupOutputType {
    /// Plain `Utf8` — simplest, the default for hand-construction.
    Utf8,
    /// `Dictionary(UInt32, Utf8)` — the form DataFusion's default
    /// `AggregateExec` produces when the input is dict-encoded. The
    /// optimizer rule uses this so the rewritten plan matches the
    /// upstream operator's cached schema.
    Dictionary,
}

impl DictGroupCountExec {
    /// Construct with default output column names: `[<input-col-name>,
    /// "count"]`.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, group_col_idx: usize) -> DfResult<Self> {
        let group_name = input.schema().field(group_col_idx).name().clone();
        Self::try_new_with_names(input, group_col_idx, group_name, "count".into())
    }

    /// Construct with caller-chosen output column names. Used by the
    /// optimizer rule so the rewritten plan emits the same column
    /// names the original `AggregateExec` would have, keeping
    /// downstream operators / projections happy.
    pub fn try_new_with_names(
        input: Arc<dyn ExecutionPlan>,
        group_col_idx: usize,
        group_out_name: String,
        count_out_name: String,
    ) -> DfResult<Self> {
        Self::try_new_with_names_and_type(
            input,
            group_col_idx,
            group_out_name,
            count_out_name,
            GroupOutputType::Utf8,
        )
    }

    /// Most flexible constructor. The optimizer rule calls this with
    /// `GroupOutputType::Dictionary` so the rewritten plan matches
    /// DataFusion's default-aggregate output schema (which preserves
    /// the input dict's encoding) — keeping downstream operators'
    /// cached schemas valid.
    pub fn try_new_with_names_and_type(
        input: Arc<dyn ExecutionPlan>,
        group_col_idx: usize,
        group_out_name: String,
        count_out_name: String,
        group_out_type: GroupOutputType,
    ) -> DfResult<Self> {
        let in_schema = input.schema();
        let field = in_schema.field(group_col_idx);
        let field_name = field.name().clone();
        match field.data_type() {
            DataType::Dictionary(key, value)
                if **key == DataType::UInt32
                    && (**value == DataType::Utf8 || **value == DataType::Utf8View) => {}
            other => {
                return Err(DataFusionError::Plan(format!(
                    "DictGroupCountExec: column `{field_name}` has type {other:?}, expected Dictionary(UInt32, Utf8|Utf8View)",
                )));
            }
        }
        let out_dtype = match group_out_type {
            GroupOutputType::Utf8 => DataType::Utf8,
            GroupOutputType::Dictionary => {
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
            }
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new(group_out_name, out_dtype, false),
            Field::new(count_out_name, DataType::Int64, false),
        ]));
        let eq_props = EquivalenceProperties::new(schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            group_col_idx,
            group_out_type,
            schema,
            properties,
        })
    }

    pub fn group_col_idx(&self) -> usize {
        self.group_col_idx
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for DictGroupCountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "DictGroupCountExec(group_col_idx={})",
            self.group_col_idx
        )
    }
}

impl ExecutionPlan for DictGroupCountExec {
    fn name(&self) -> &str {
        "DictGroupCountExec"
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
            DataFusionError::Internal("DictGroupCountExec requires exactly 1 child".into())
        })?;
        // Preserve the existing output column names + materialisation.
        let group_out_name = self.schema.field(0).name().clone();
        let count_out_name = self.schema.field(1).name().clone();
        Ok(Arc::new(Self::try_new_with_names_and_type(
            new_input,
            self.group_col_idx,
            group_out_name,
            count_out_name,
            self.group_out_type,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "DictGroupCountExec emits only partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let group_col_idx = self.group_col_idx;
        let group_out_type = self.group_out_type;
        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();

        let fut = async move {
            // One accumulator keyed by canonical string. Each batch
            // contributes via a *batch-local* (code -> slot) table
            // resolved once from the batch's dictionary values.
            let mut accum = GroupCountAccumulator::default();
            let in_parts = input.properties().partitioning.partition_count();
            for p in 0..in_parts {
                let mut s = input.execute(p, context.clone())?;
                while let Some(batch) = s.try_next().await? {
                    accum.consume_batch(&batch, group_col_idx)?;
                }
            }
            accum.finish(schema_for_stream, group_out_type)
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

#[derive(Default)]
struct GroupCountAccumulator {
    /// Map from canonical string to slot index in `counts`.
    slots: HashMap<String, usize>,
    /// Ordered list of group strings; `counts[i]` ≡ count for
    /// `groups[i]`. Output preserves insertion order, which matches
    /// the first-seen order of each group across the input batches.
    groups: Vec<String>,
    counts: Vec<i64>,
}

impl GroupCountAccumulator {
    fn consume_batch(&mut self, batch: &RecordBatch, group_col_idx: usize) -> DfResult<()> {
        let arr = batch.column(group_col_idx);
        let dict = arr
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "DictGroupCountExec: column not DictionaryArray<UInt32, _> at runtime".into(),
                )
            })?;
        let values = dict.values();
        let dict_len = values.len();

        // Resolve each code in this batch's dict to a *slot*. Built
        // once per batch — the per-row hot loop is just `code -> slot
        // -> counts[slot] += 1`, no string hash per row.
        let mut code_to_slot: Vec<i32> = vec![-1; dict_len]; // -1 = null
        if let Some(arr) = values.as_any().downcast_ref::<StringArray>() {
            for (code, slot) in code_to_slot.iter_mut().enumerate() {
                if arr.is_null(code) {
                    continue;
                }
                *slot = self.intern_slot(arr.value(code)) as i32;
            }
        } else if let Some(arr) = values.as_any().downcast_ref::<StringViewArray>() {
            for (code, slot) in code_to_slot.iter_mut().enumerate() {
                if arr.is_null(code) {
                    continue;
                }
                *slot = self.intern_slot(arr.value(code)) as i32;
            }
        } else {
            return Err(DataFusionError::Internal(
                "DictGroupCountExec: dict values neither StringArray nor StringViewArray".into(),
            ));
        }

        // Per-row: dict.keys()[i] → slot → counts[slot] += 1.
        let keys: &UInt32Array = dict.keys();
        for i in 0..keys.len() {
            if keys.is_null(i) {
                continue;
            }
            let code = keys.value(i) as usize;
            if code >= dict_len {
                continue;
            }
            let slot = code_to_slot[code];
            if slot < 0 {
                continue;
            }
            self.counts[slot as usize] += 1;
        }
        Ok(())
    }

    /// Resolve `value` to a slot, allocating one if first-seen.
    /// Returns the slot index. Each new slot extends `groups`,
    /// `counts`, and `slots` in lockstep.
    fn intern_slot(&mut self, value: &str) -> usize {
        if let Some(&s) = self.slots.get(value) {
            return s;
        }
        let s = self.groups.len();
        self.groups.push(value.to_string());
        self.counts.push(0);
        self.slots.insert(value.to_string(), s);
        s
    }

    fn finish(self, schema: SchemaRef, group_out_type: GroupOutputType) -> DfResult<RecordBatch> {
        let n = self.groups.len();
        let count_arr: ArrayRef = Arc::new(Int64Array::from(self.counts));
        let group_arr: ArrayRef = match group_out_type {
            GroupOutputType::Utf8 => Arc::new(StringArray::from(self.groups)),
            GroupOutputType::Dictionary => {
                // Each surviving group gets its own dict-code:
                //   keys = [0, 1, 2, ..., n-1]
                //   values = groups (one entry per group).
                let keys = UInt32Array::from((0u32..n as u32).collect::<Vec<_>>());
                let values: ArrayRef = Arc::new(StringArray::from(self.groups));
                let dict = DictionaryArray::<UInt32Type>::try_new(keys, values)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Arc::new(dict)
            }
        };
        RecordBatch::try_new(schema, vec![group_arr, count_arr])
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringDictionaryBuilder;
    use datafusion::arrow::datatypes::UInt32Type;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use futures_util::stream::TryStreamExt;

    fn make_test_batch() -> RecordBatch {
        let mut keys: StringDictionaryBuilder<UInt32Type> = StringDictionaryBuilder::new();
        // 8 rows: 3× MAIL, 2× AIR, 2× SHIP, 1× TRUCK.
        for v in [
            "MAIL", "AIR", "MAIL", "AIR", "SHIP", "MAIL", "SHIP", "TRUCK",
        ] {
            keys.append(v).unwrap();
        }
        let dict = keys.finish();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l_shipmode",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(dict) as ArrayRef]).unwrap()
    }

    async fn input_plan_from_batch(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    /// Primary correctness pin: each distinct group's count must
    /// match the hand-counted answer. Output schema is `[col, count]`.
    #[tokio::test]
    async fn dict_group_count_emits_correct_counts() {
        let input = input_plan_from_batch(make_test_batch()).await;
        let exec = Arc::new(DictGroupCountExec::try_new(input, 0).unwrap());
        let ctx = SessionContext::new();
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let batch = stream.try_next().await.unwrap().unwrap();

        assert_eq!(batch.num_rows(), 4, "MAIL,AIR,SHIP,TRUCK");
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "l_shipmode");
        assert_eq!(batch.schema().field(1).name(), "count");

        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Collect into HashMap for order-agnostic comparison.
        let mut got: HashMap<String, i64> = HashMap::new();
        for i in 0..batch.num_rows() {
            got.insert(groups.value(i).to_string(), counts.value(i));
        }
        assert_eq!(got.get("MAIL"), Some(&3));
        assert_eq!(got.get("AIR"), Some(&2));
        assert_eq!(got.get("SHIP"), Some(&2));
        assert_eq!(got.get("TRUCK"), Some(&1));
    }

    /// Multi-batch correctness: spanning two batches with overlapping
    /// dict-encoded groups (each batch may have its own dict).
    /// The accumulator must merge across batches by string identity,
    /// not by dict code.
    #[tokio::test]
    async fn dict_group_count_merges_across_batches() {
        let mut k1: StringDictionaryBuilder<UInt32Type> = StringDictionaryBuilder::new();
        for v in ["MAIL", "AIR", "MAIL"] {
            k1.append(v).unwrap();
        }
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "l_shipmode",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            )])),
            vec![Arc::new(k1.finish()) as ArrayRef],
        )
        .unwrap();

        // Second batch: different dict order — AIR comes first now.
        let mut k2: StringDictionaryBuilder<UInt32Type> = StringDictionaryBuilder::new();
        for v in ["AIR", "MAIL", "SHIP"] {
            k2.append(v).unwrap();
        }
        let b2 =
            RecordBatch::try_new(b1.schema(), vec![Arc::new(k2.finish()) as ArrayRef]).unwrap();

        let mem = MemTable::try_new(b1.schema(), vec![vec![b1, b2]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        let input = df.create_physical_plan().await.unwrap();

        let exec = Arc::new(DictGroupCountExec::try_new(input, 0).unwrap());
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let batch = stream.try_next().await.unwrap().unwrap();

        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut got: HashMap<String, i64> = HashMap::new();
        for i in 0..batch.num_rows() {
            got.insert(groups.value(i).to_string(), counts.value(i));
        }
        // b1: MAIL×2 + AIR×1; b2: AIR×1 + MAIL×1 + SHIP×1
        assert_eq!(got.get("MAIL"), Some(&3));
        assert_eq!(got.get("AIR"), Some(&2));
        assert_eq!(got.get("SHIP"), Some(&1));
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
        let res = DictGroupCountExec::try_new(input, 0);
        let err = res.expect_err("non-dict column should fail validation");
        let msg = format!("{err}");
        assert!(msg.contains("Dictionary"), "{msg}");
    }

    /// Equivalence pin: results match DataFusion's default
    /// `SELECT l_shipmode, COUNT(*) FROM t GROUP BY l_shipmode`.
    #[tokio::test]
    async fn dict_group_count_matches_datafusion_aggregate() {
        // Default execution.
        let ctx = SessionContext::new();
        let mem =
            MemTable::try_new(make_test_batch().schema(), vec![vec![make_test_batch()]]).unwrap();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let default_batches = ctx
            .sql("SELECT l_shipmode, COUNT(*) c FROM t GROUP BY l_shipmode")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut default_got: HashMap<String, i64> = HashMap::new();
        for b in &default_batches {
            // DataFusion may materialize the group column to Utf8 or
            // keep it as Dictionary — handle both.
            let groups: Vec<Option<String>> =
                if let Some(a) = b.column(0).as_any().downcast_ref::<StringArray>() {
                    (0..a.len())
                        .map(|i| {
                            if a.is_null(i) {
                                None
                            } else {
                                Some(a.value(i).to_string())
                            }
                        })
                        .collect()
                } else if let Some(d) = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                {
                    let vals = d.values().as_any().downcast_ref::<StringArray>().unwrap();
                    (0..d.len())
                        .map(|i| {
                            if d.is_null(i) {
                                None
                            } else {
                                let c = d.keys().value(i) as usize;
                                Some(vals.value(c).to_string())
                            }
                        })
                        .collect()
                } else {
                    panic!("unexpected group array type");
                };
            let counts = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for (i, g) in groups.iter().enumerate() {
                if let Some(g) = g {
                    default_got.insert(g.clone(), counts.value(i));
                }
            }
        }

        // Σ.E3b path.
        let input = input_plan_from_batch(make_test_batch()).await;
        let exec = Arc::new(DictGroupCountExec::try_new(input, 0).unwrap());
        let ctx2 = SessionContext::new();
        let batch = exec
            .execute(0, ctx2.task_ctx())
            .unwrap()
            .try_next()
            .await
            .unwrap()
            .unwrap();
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut dict_got: HashMap<String, i64> = HashMap::new();
        for i in 0..batch.num_rows() {
            dict_got.insert(groups.value(i).to_string(), counts.value(i));
        }
        assert_eq!(dict_got, default_got);
    }
}
