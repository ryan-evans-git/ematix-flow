//! `FdAggregateExec` — single-phase `SUM(Float64) GROUP BY <group exprs>` operator
//! that groups on an **FD-minimal key subset** of the group expressions, built on
//! [`crate::fd_aggregate::FdSumAccumulator`] (the Q10 SF=100 lever).
//!
//! ## Shape
//!
//! Replaces a DataFusion 2-phase `AggregateExec(gby=[g0..gN], sum(v))` when a subset
//! of the group expressions (`key_positions`) functionally determines the rest
//! (proven by [`crate::fd_aggregate_rule`]). The operator row-encodes ONLY that
//! narrow key subset and carries every group column by first-occurrence, avoiding
//! the wide-string key encoding that costs DataFusion ~4.86 CPU-s on Q10.
//!
//! For Q10 the group exprs are `[c_custkey, c_name, c_acctbal, c_phone, n_name,
//! c_address, c_comment]` and the FD-minimal key is `{c_custkey, n_name}`
//! (`key_positions = [0, 4]`): `c_custkey` determines the 5 wide customer columns,
//! and `n_name` is included because the FD machinery can't prove `c_custkey ->
//! n_name` transitively across the join even though it holds.
//!
//! ## Distribution
//!
//! Single-phase: it declares [`Distribution::HashPartitioned`] on the key subset, so
//! each distinct key lands in exactly one input partition (disjoint groups per
//! partition, no cross-partition merge). The repartition hashes the narrow key, not
//! the full wide tuple.
//!
//! ## Output schema
//!
//! `[g0..gN, sum]` — the group columns in `GROUP BY` order followed by the
//! aggregate, exactly matching the `AggregateExec` it replaces, so the projection
//! above keeps its column references.
//!
//! ## NOT in the default chain
//!
//! Per [[optimizer-codegen-sensitivity]] the swap rule is opt-in (`EMAT_FD_AGG`).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};

use crate::fd_aggregate::FdSumAccumulator;

/// `SUM(Float64) GROUP BY <group exprs>`, grouping on an FD-minimal key subset.
#[derive(Debug)]
pub struct FdAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    /// The full group-by expressions with their output names, in `GROUP BY` (output)
    /// order. Each is evaluated per batch and carried by first-occurrence.
    group_exprs: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Positions within `group_exprs` that form the FD-minimal grouping key (the
    /// subset that functionally determines all the others). Non-empty.
    key_positions: Vec<usize>,
    /// The `SUM` input expression (must evaluate to `Float64`) and its output name.
    sum_expr: Arc<dyn PhysicalExpr>,
    sum_name: String,
    /// Output schema: `[group exprs..., sum]`.
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FdAggregateExec {
    /// Build the operator.
    ///
    /// - `group_exprs`: the group-by expressions + output names, in output order.
    /// - `key_positions`: indices into `group_exprs` of the FD-minimal key subset.
    /// - `sum_expr` / `sum_name`: the `Float64` sum input and its output name.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_exprs: Vec<(Arc<dyn PhysicalExpr>, String)>,
        key_positions: Vec<usize>,
        sum_expr: Arc<dyn PhysicalExpr>,
        sum_name: String,
    ) -> DfResult<Self> {
        if group_exprs.is_empty() {
            return Err(DataFusionError::Internal(
                "FdAggregateExec: needs ≥1 group expr".into(),
            ));
        }
        if key_positions.is_empty() {
            return Err(DataFusionError::Internal(
                "FdAggregateExec: needs ≥1 key position".into(),
            ));
        }
        if key_positions.iter().any(|&p| p >= group_exprs.len()) {
            return Err(DataFusionError::Internal(
                "FdAggregateExec: key position out of range".into(),
            ));
        }
        let in_schema = input.schema();
        // Output schema: each group expr's (type, nullability) — identical to what
        // the replaced AggregateExec derives for its group columns — then the sum
        // (Float64, nullable: an all-null group yields NULL).
        let mut out_fields: Vec<Field> = Vec::with_capacity(group_exprs.len() + 1);
        for (expr, name) in &group_exprs {
            out_fields.push(Field::new(
                name,
                expr.data_type(&in_schema)?,
                expr.nullable(&in_schema)?,
            ));
        }
        // The sum input must be Float64.
        if sum_expr.data_type(&in_schema)? != DataType::Float64 {
            return Err(DataFusionError::Internal(
                "FdAggregateExec: sum expression must be Float64".into(),
            ));
        }
        out_fields.push(Field::new(&sum_name, DataType::Float64, true));
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
            group_exprs,
            key_positions,
            sum_expr,
            sum_name,
            schema,
            properties,
        })
    }

    /// The key-subset expressions, for the required input distribution.
    fn key_exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.key_positions
            .iter()
            .map(|&p| self.group_exprs[p].0.clone())
            .collect()
    }
}

impl DisplayAs for FdAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let names: Vec<&str> = self.group_exprs.iter().map(|(_, n)| n.as_str()).collect();
        write!(
            f,
            "FdAggregateExec(groups={names:?}, key_positions={:?}, sum={})",
            self.key_positions, self.sum_name
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
    /// Require the input hash-partitioned on the FD-minimal key subset, so each key
    /// lands in exactly one partition (disjoint groups → no cross-partition merge).
    /// The repartition hashes the narrow key, not the wide group tuple.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::HashPartitioned(self.key_exprs())]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let new_input = children.pop().ok_or_else(|| {
            DataFusionError::Internal("FdAggregateExec requires exactly 1 child".into())
        })?;
        Ok(Arc::new(Self::try_new(
            new_input,
            self.group_exprs.clone(),
            self.key_positions.clone(),
            self.sum_expr.clone(),
            self.sum_name.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.clone();
        let group_exprs = self.group_exprs.clone();
        let key_positions = self.key_positions.clone();
        let sum_expr = self.sum_expr.clone();
        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();

        let fut = async move {
            let mut acc = FdSumAccumulator::new();
            let mut s = input.execute(partition, context)?;
            while let Some(batch) = s.try_next().await? {
                let n = batch.num_rows();
                if n == 0 {
                    continue;
                }
                // Evaluate every group expr → output group columns (carried by
                // first-occurrence).
                let group_cols: Vec<ArrayRef> = group_exprs
                    .iter()
                    .map(|(e, _)| e.evaluate(&batch).and_then(|v| v.into_array(n)))
                    .collect::<DfResult<_>>()?;
                // The key subset is a subset of those columns (Arc clones — cheap).
                let key_cols: Vec<ArrayRef> = key_positions
                    .iter()
                    .map(|&p| group_cols[p].clone())
                    .collect();
                // The sum input.
                let sum_val = sum_expr.evaluate(&batch)?.into_array(n)?;
                let sum_arr = sum_val
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "FdAggregateExec: sum expression did not evaluate to Float64Array"
                                .into(),
                        )
                    })?;
                acc.ingest(&key_cols, sum_arr, group_cols)?;
            }
            // Empty partition (no rows): emit a correctly-typed 0-row batch.
            if acc.num_groups() == 0 {
                return Ok(RecordBatch::new_empty(schema_for_stream));
            }
            let (group_arrs, sum_arr) = acc.finalize()?;
            // Assemble [group cols..., sum] in output-schema order.
            let mut cols: Vec<ArrayRef> = Vec::with_capacity(group_arrs.len() + 1);
            cols.extend(group_arrs);
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
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// FD-consistent fixture: (custkey, nation) -> name (name FD-determined by
    /// custkey); several batches. Columns: [custkey, nation, name, v].
    fn fixture() -> (Arc<Schema>, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("custkey", DataType::Int64, false),
            Field::new("nation", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let mk = |ks: Vec<i64>, nats: Vec<&str>, vs: Vec<f64>| {
            let names: Vec<String> = ks.iter().map(|k| format!("name{k}")).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(StringArray::from(nats)),
                    Arc::new(StringArray::from(names)),
                    Arc::new(Float64Array::from(vs)),
                ],
            )
            .unwrap()
        };
        let batches = vec![
            mk(
                vec![1, 2, 3, 1],
                vec!["US", "FR", "DE", "US"],
                vec![10.0, 20.0, 30.0, 1.0],
            ),
            mk(
                vec![2, 1, 5, 5],
                vec!["FR", "US", "JP", "JP"],
                vec![2.0, 0.5, 7.0, 3.0],
            ),
            mk(
                vec![2, 3, 3, 3],
                vec!["FR", "DE", "DE", "DE"],
                vec![2.0, 1.0, 1.0, 1.0],
            ),
        ];
        (schema, batches)
    }

    /// Reference via stock DataFusion GROUP BY on the full 3-column key.
    async fn stock_result(
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> BTreeMap<(i64, String, String), f64> {
        let ctx = SessionContext::new();
        let mt = MemTable::try_new(schema, vec![batches]).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let df = ctx
            .sql("SELECT custkey, nation, name, SUM(v) AS s FROM t GROUP BY custkey, nation, name")
            .await
            .unwrap();
        rows_to_map(&df.collect().await.unwrap())
    }

    fn rows_to_map(batches: &[RecordBatch]) -> BTreeMap<(i64, String, String), f64> {
        let mut m = BTreeMap::new();
        for b in batches {
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let nat = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let nm = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            let s = b.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                m.insert(
                    (
                        k.value(i),
                        nat.value(i).to_string(),
                        nm.value(i).to_string(),
                    ),
                    s.value(i),
                );
            }
        }
        m
    }

    /// Build child scan → RepartitionExec(Hash[key]) → FdAggregateExec and run it.
    /// `key_positions` selects which group columns form the FD-minimal key.
    async fn fd_result(
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        nparts: usize,
        key_positions: Vec<usize>,
    ) -> BTreeMap<(i64, String, String), f64> {
        let cfg = SessionConfig::new().with_target_partitions(nparts);
        let ctx = SessionContext::new_with_config(cfg);
        let mt = MemTable::try_new(schema, vec![batches]).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let child = ctx
            .sql("SELECT custkey, nation, name, v FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        // group exprs in output order: [custkey, nation, name]; sum = v.
        let group_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("custkey", 0)), "custkey".to_string()),
            (Arc::new(Column::new("nation", 1)), "nation".to_string()),
            (Arc::new(Column::new("name", 2)), "name".to_string()),
        ];
        let key_exprs: Vec<Arc<dyn PhysicalExpr>> = key_positions
            .iter()
            .map(|&p| group_exprs[p].0.clone())
            .collect();
        let repart = Arc::new(
            RepartitionExec::try_new(child, Partitioning::Hash(key_exprs, nparts)).unwrap(),
        );
        let op = Arc::new(
            FdAggregateExec::try_new(
                repart,
                group_exprs,
                key_positions,
                Arc::new(Column::new("v", 3)),
                "s".to_string(),
            )
            .unwrap(),
        );
        rows_to_map(&collect(op, ctx.task_ctx()).await.unwrap())
    }

    /// Composite key {custkey, nation} (the Q10 shape): name is the FD-carried
    /// payload. Multi-partition.
    #[tokio::test]
    async fn matches_stock_composite_key_multi_partition() {
        let (schema, batches) = fixture();
        let want = stock_result(schema.clone(), batches.clone()).await;
        let got = fd_result(schema, batches, 4, vec![0, 1]).await;
        assert_eq!(got.len(), want.len(), "group count");
        for (k, ws) in &want {
            let gs = got.get(k).unwrap_or_else(|| panic!("missing key {k:?}"));
            assert!((gs - ws).abs() < 1e-9, "sum for {k:?}: {gs} vs {ws}");
        }
    }

    #[tokio::test]
    async fn matches_stock_composite_key_single_partition() {
        let (schema, batches) = fixture();
        let want = stock_result(schema.clone(), batches.clone()).await;
        let got = fd_result(schema, batches, 1, vec![0, 1]).await;
        assert_eq!(got, want);
    }

    /// Single-column key {custkey}: valid here because the fixture is FD-consistent
    /// (each custkey maps to one nation and one name), so grouping by custkey alone
    /// yields the same groups as the full key. Exercises the degenerate composite.
    #[tokio::test]
    async fn matches_stock_single_key_multi_partition() {
        let (schema, batches) = fixture();
        let want = stock_result(schema.clone(), batches.clone()).await;
        let got = fd_result(schema, batches, 4, vec![0]).await;
        assert_eq!(got.len(), want.len(), "group count");
        for (k, ws) in &want {
            let gs = got.get(k).unwrap_or_else(|| panic!("missing key {k:?}"));
            assert!((gs - ws).abs() < 1e-9, "sum for {k:?}: {gs} vs {ws}");
        }
    }
}
