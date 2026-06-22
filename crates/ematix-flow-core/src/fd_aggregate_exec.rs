//! `FdAggregateExec` — single-phase `SUM(Float64) GROUP BY <i64 key + FD-payload>`
//! operator built on [`crate::fd_aggregate::FdSumAccumulator`] (the Q10 SF=100 lever).
//!
//! ## Shape
//!
//! Replaces a DataFusion 2-phase `AggregateExec(gby=[key, p1..pN], sum(v))` when the
//! payload columns `p1..pN` are functionally determined by the `Int64` `key` (proven
//! by [`crate::fd_aggregate_rule`]). The operator hashes ONLY the i64 key and carries
//! the payload by first-occurrence, avoiding the wide-string key encoding that costs
//! DataFusion ~4.86 CPU-s on Q10.
//!
//! ## Distribution
//!
//! Single-phase: it declares [`Distribution::HashPartitioned`] on the key, so each key
//! lands in exactly one input partition (disjoint groups per partition, no merge). The
//! repartition hashes the cheap i64 key, not the 7-column tuple.
//!
//! ## Output schema
//!
//! `[key, p1..pN, sum]` — exactly the column order of the `AggregateExec` it replaces
//! (group exprs in `GROUP BY` order, then the aggregate), so downstream column
//! references stay valid.
//!
//! ## NOT in the default chain
//!
//! Per [[optimizer-codegen-sensitivity]] the swap rule is opt-in (`EMAT_FD_AGG`).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};

use crate::fd_aggregate::FdSumAccumulator;

/// `SUM(Float64) GROUP BY (Int64 key ∪ FD-determined payload)` — single phase.
#[derive(Debug)]
pub struct FdAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    /// Index (in the input schema) of the Int64 group key.
    key_col_idx: usize,
    /// Index of the Float64 column being summed.
    sum_col_idx: usize,
    /// Indices of the payload group columns (FD-determined by `key`), carried by
    /// first-occurrence. In output-schema order.
    payload_col_idxs: Vec<usize>,
    /// Output schema: `[key, payload..., sum]`.
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FdAggregateExec {
    /// Build the operator. `out_names` are the output column names in
    /// `[key, payload..., sum]` order (length = `payload_col_idxs.len() + 2`).
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        key_col_idx: usize,
        sum_col_idx: usize,
        payload_col_idxs: Vec<usize>,
        out_names: Vec<String>,
    ) -> DfResult<Self> {
        let in_schema = input.schema();
        let nfields = in_schema.fields().len();
        for &i in std::iter::once(&key_col_idx)
            .chain(std::iter::once(&sum_col_idx))
            .chain(payload_col_idxs.iter())
        {
            if i >= nfields {
                return Err(DataFusionError::Internal(format!(
                    "FdAggregateExec: col idx {i} out of bounds ({nfields} fields)"
                )));
            }
        }
        if in_schema.field(key_col_idx).data_type()
            != &datafusion::arrow::datatypes::DataType::Int64
        {
            return Err(DataFusionError::Internal(
                "FdAggregateExec: key column must be Int64".into(),
            ));
        }
        if in_schema.field(sum_col_idx).data_type()
            != &datafusion::arrow::datatypes::DataType::Float64
        {
            return Err(DataFusionError::Internal(
                "FdAggregateExec: sum column must be Float64".into(),
            ));
        }
        if out_names.len() != payload_col_idxs.len() + 2 {
            return Err(DataFusionError::Internal(
                "FdAggregateExec: out_names count mismatch".into(),
            ));
        }
        // Output schema: key (nullable to allow a null group), payloads (preserve
        // input nullability/type), sum (nullable — all-null group => NULL).
        let mut out_fields: Vec<Field> = Vec::with_capacity(out_names.len());
        out_fields.push(Field::new(
            &out_names[0],
            datafusion::arrow::datatypes::DataType::Int64,
            true,
        ));
        for (oi, &pi) in payload_col_idxs.iter().enumerate() {
            let f = in_schema.field(pi);
            out_fields.push(Field::new(&out_names[oi + 1], f.data_type().clone(), true));
        }
        out_fields.push(Field::new(
            &out_names[payload_col_idxs.len() + 1],
            datafusion::arrow::datatypes::DataType::Float64,
            true,
        ));
        let schema = Arc::new(Schema::new(out_fields));

        let n_parts = input.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(n_parts),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            key_col_idx,
            sum_col_idx,
            payload_col_idxs,
            schema,
            properties,
        })
    }

    fn key_expr(&self) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(
            self.input.schema().field(self.key_col_idx).name(),
            self.key_col_idx,
        ))
    }
}

impl DisplayAs for FdAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "FdAggregateExec(key_col_idx={}, sum_col_idx={}, payload={:?})",
            self.key_col_idx, self.sum_col_idx, self.payload_col_idxs
        )
    }
}

impl ExecutionPlan for FdAggregateExec {
    fn name(&self) -> &str {
        "FdAggregateExec"
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
    /// Require the input hash-partitioned on the i64 key, so each key lands in
    /// exactly one partition (disjoint groups → no cross-partition merge). The
    /// repartition hashes the cheap i64 key, not the wide 7-column tuple.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::HashPartitioned(vec![self.key_expr()])]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let new_input = children.pop().ok_or_else(|| {
            DataFusionError::Internal("FdAggregateExec requires exactly 1 child".into())
        })?;
        let out_names: Vec<String> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        Ok(Arc::new(Self::try_new(
            new_input,
            self.key_col_idx,
            self.sum_col_idx,
            self.payload_col_idxs.clone(),
            out_names,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.clone();
        let key_col_idx = self.key_col_idx;
        let sum_col_idx = self.sum_col_idx;
        let payload_col_idxs = self.payload_col_idxs.clone();
        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();

        let fut = async move {
            let mut acc = FdSumAccumulator::new();
            let mut s = input.execute(partition, context)?;
            let mut any = false;
            while let Some(batch) = s.try_next().await? {
                any = true;
                let key = batch
                    .column(key_col_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "FdAggregateExec: col {key_col_idx} not Int64Array"
                        ))
                    })?
                    .clone();
                let sum_in = batch
                    .column(sum_col_idx)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "FdAggregateExec: col {sum_col_idx} not Float64Array"
                        ))
                    })?
                    .clone();
                let payload: Vec<ArrayRef> = payload_col_idxs
                    .iter()
                    .map(|&i| batch.column(i).clone())
                    .collect();
                acc.ingest(&key, &sum_in, payload)?;
            }
            // Empty partition (no batches): the kernel never learned the payload
            // column count, so emit a correctly-typed 0-row batch from the schema.
            if !any {
                return Ok(RecordBatch::new_empty(schema_for_stream));
            }
            let (key_arr, sum_arr, payload_arrs) = acc.finalize()?;
            // Assemble [key, payload..., sum] in the output schema order.
            let mut cols: Vec<ArrayRef> = Vec::with_capacity(payload_arrs.len() + 2);
            cols.push(key_arr);
            cols.extend(payload_arrs);
            cols.push(sum_arr);
            let out = RecordBatch::try_new(schema_for_stream.clone(), cols)?;
            Ok::<_, DataFusionError>(out)
        };

        let strm = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, strm)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    // FD-consistent fixture: key k -> payload "name{k}", several batches.
    fn fixture() -> (Arc<Schema>, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let mk = |ks: Vec<i64>, vs: Vec<f64>| {
            let names: Vec<String> = ks.iter().map(|k| format!("name{k}")).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(StringArray::from(names)),
                    Arc::new(Float64Array::from(vs)),
                ],
            )
            .unwrap()
        };
        let batches = vec![
            mk(vec![1, 2, 3, 1], vec![10.0, 20.0, 30.0, 1.0]),
            mk(vec![2, 1, 5, 5], vec![2.0, 0.5, 7.0, 3.0]),
            mk(vec![2, 3, 3, 3], vec![2.0, 1.0, 1.0, 1.0]),
        ];
        (schema, batches)
    }

    /// Reference via stock DataFusion GROUP BY on the same data.
    async fn stock_result(
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> BTreeMap<i64, (String, f64)> {
        let ctx = SessionContext::new();
        let mt = MemTable::try_new(schema, vec![batches]).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let df = ctx
            .sql("SELECT k, name, SUM(v) AS s FROM t GROUP BY k, name")
            .await
            .unwrap();
        rows_to_map(&df.collect().await.unwrap())
    }

    fn rows_to_map(batches: &[RecordBatch]) -> BTreeMap<i64, (String, f64)> {
        let mut m = BTreeMap::new();
        for b in batches {
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let nm = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let s = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                m.insert(k.value(i), (nm.value(i).to_string(), s.value(i)));
            }
        }
        m
    }

    /// Build child scan → RepartitionExec(Hash[k]) → FdAggregateExec and run it.
    async fn fd_result(
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        nparts: usize,
    ) -> BTreeMap<i64, (String, f64)> {
        let cfg = SessionConfig::new().with_target_partitions(nparts);
        let ctx = SessionContext::new_with_config(cfg);
        let mt = MemTable::try_new(schema, vec![batches]).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        // Child = plain scan/projection [k, name, v].
        let child = ctx
            .sql("SELECT k, name, v FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        // Repartition on the i64 key so each key is in one partition.
        let key_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let repart = Arc::new(
            RepartitionExec::try_new(child, Partitioning::Hash(vec![key_expr], nparts)).unwrap(),
        );
        let op = Arc::new(
            FdAggregateExec::try_new(
                repart,
                0,
                2,
                vec![1],
                vec!["k".into(), "name".into(), "s".into()],
            )
            .unwrap(),
        );
        let task_ctx = ctx.task_ctx();
        rows_to_map(&collect(op, task_ctx).await.unwrap())
    }

    #[tokio::test]
    async fn matches_stock_groupby_multi_partition() {
        let (schema, batches) = fixture();
        let want = stock_result(schema.clone(), batches.clone()).await;
        let got = fd_result(schema, batches, 4).await;
        assert_eq!(got.len(), want.len(), "group count");
        for (k, (wn, ws)) in &want {
            let (gn, gs) = got.get(k).unwrap_or_else(|| panic!("missing key {k}"));
            assert_eq!(gn, wn, "payload for key {k}");
            assert!((gs - ws).abs() < 1e-9, "sum for key {k}: {gs} vs {ws}");
        }
    }

    #[tokio::test]
    async fn matches_stock_groupby_single_partition() {
        let (schema, batches) = fixture();
        let want = stock_result(schema.clone(), batches.clone()).await;
        let got = fd_result(schema, batches, 1).await;
        assert_eq!(got, want);
    }
}
