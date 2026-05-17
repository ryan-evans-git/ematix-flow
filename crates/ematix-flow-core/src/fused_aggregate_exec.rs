//! Σ.G.2 third slice: `FusedAggregateExec<S: AggregateSpec>` — the
//! generic single-pass fused aggregate operator that the trait + Q6/Q1
//! specs were built to enable.
//!
//! What this is:
//!   The structural pattern shared by `FusedFilterSumExec` (Q6) and
//!   `FusedFilterMultiAggExec` (Q1), lifted into a single operator
//!   parameterised by an [`AggregateSpec`]. Per-shape work — the inner
//!   loop, the accumulator type, the finalize-to-RecordBatch step —
//!   lives in the spec impl. Per-batch dispatch happens at the batch
//!   boundary, so LLVM's per-shape auto-vectoriser stays intact (the
//!   Σ.G.2 unit benches gate at < 1 %; see #92 / #93).
//!
//! Streaming model: one `tokio::spawn` task per input partition, each
//! draining its stream and accumulating into a local `Accumulator`.
//! When every partition's stream ends we fold via `spec.merge`, then
//! call `spec.finalize` and emit a single batch on partition 0.
//! Mirrors the post-Σ.D3-phase-D shape used by `FusedFilterSumExec`,
//! `FusedFilterMultiAggExec`, and `FusedPostJoinExec` — the same
//! decision that closed the original "fused exec slower than default
//! pipeline" gap by streaming the scan instead of materialising it.
//!
//! What this is NOT (yet):
//!   - A `PhysicalOptimizerRule` that rewires existing Q6/Q1 plans to
//!     route through this operator. That's the next slice — without
//!     the rule, this is only reachable via direct construction in
//!     tests / benches. Existing query paths still use the original
//!     specialised operators.
//!   - Cranelift-JIT'd. The hand path is the substrate; JIT is a
//!     follow-up that plugs in beneath the trait without changing
//!     this operator.
//!   - Benchmarked end-to-end on real TPC-H. The unit-level perf
//!     gates from #92 / #93 prove the trait dispatch doesn't regress
//!     the hot loop; an operator-level bench (this exec vs the hand
//!     operators on real lineitem batches through DataFusion) is the
//!     gate before the planner rule lands.

use std::any::Any;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};

use crate::fused_aggregate::AggregateSpec;

/// Single-pass fused aggregate operator parameterised by an
/// [`AggregateSpec`]. See module-level docs.
///
/// **Operator-level bench finding (2026-05-17):** capturing the spec
/// as `Arc<S>` and dereferencing it per batch added 5-18 % wall-clock
/// vs the hand operators on TPC-H SF=1 Q6/Q1, even though the unit
/// trait-dispatch bench gates at < 1 %. Fix: require `S: Clone` and
/// pass a value-clone into each per-partition worker, matching the
/// hand operators' shape where they capture `Copy` `Predicate` +
/// `Indices` values directly into the spawn closure. Spec clones are
/// cheap (Q6Spec ≈ 48 B, Q1Spec ≈ 80 B; both contain only `Copy`
/// fields plus an `Arc<Schema>` whose clone is an atomic refcount bump).
#[derive(Debug)]
pub struct FusedAggregateExec<S: AggregateSpec + Clone> {
    input: Arc<dyn ExecutionPlan>,
    spec: S,
    properties: Arc<PlanProperties>,
}

impl<S: AggregateSpec + Clone> FusedAggregateExec<S> {
    /// Construct a `FusedAggregateExec` over `input` using `spec`.
    /// Validates the child schema against `spec.validate_input_schema`
    /// up front so type errors surface at planning, not at first batch.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, spec: S) -> DfResult<Self> {
        spec.validate_input_schema(&input.schema())?;
        let schema = spec.output_schema();
        let eq = EquivalenceProperties::new(schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            spec,
            properties,
        })
    }

    pub fn spec(&self) -> &S {
        &self.spec
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl<S: AggregateSpec + Clone> DisplayAs for FusedAggregateExec<S> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FusedAggregateExec<{}>", std::any::type_name::<S>())
    }
}

impl<S: AggregateSpec + Clone> ExecutionPlan for FusedAggregateExec<S> {
    fn name(&self) -> &str {
        "FusedAggregateExec"
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
            DataFusionError::Internal("FusedAggregateExec requires exactly 1 child".into())
        })?;
        self.spec.validate_input_schema(&new_input.schema())?;
        Ok(Arc::new(Self {
            input: new_input,
            spec: self.spec.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "FusedAggregateExec emits only partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let spec = self.spec.clone();
        let output_schema = self.spec.output_schema();
        let input_partitions = input.properties().partitioning.partition_count();

        let schema_for_stream = output_schema.clone();
        let fut = async move {
            // One streaming worker per input partition. Each pulls
            // batches from its stream and accumulates into a local
            // `Accumulator`. Reducing happens after every worker has
            // drained its stream.
            //
            // The spec is cloned by VALUE into each worker (not
            // captured via Arc) — see the type-level doc on this
            // operator for the bench finding that motivated this. The
            // hand operators capture `Predicate` + `Indices` by Copy;
            // we match that shape so LLVM sees the spec state as a
            // local stack value in the inner loop.
            let mut handles = Vec::with_capacity(input_partitions);
            for p in 0..input_partitions {
                let mut s = input.execute(p, context.clone())?;
                let spec_p = spec.clone();
                handles.push(tokio::spawn(async move {
                    let mut acc = <S as AggregateSpec>::Accumulator::default();
                    while let Some(batch) = s.try_next().await? {
                        spec_p.process_batch(&batch, &mut acc)?;
                    }
                    Ok::<<S as AggregateSpec>::Accumulator, DataFusionError>(acc)
                }));
            }
            let mut merged = <S as AggregateSpec>::Accumulator::default();
            for h in handles {
                let partial = h.await.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "FusedAggregateExec: worker join failed: {e}"
                    ))
                })??;
                merged = spec.merge(merged, partial);
            }
            spec.finalize(merged)
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_stream,
            s,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fused::Q6Predicate;
    use crate::fused_aggregate::{Q1Spec, Q6Spec};
    use crate::fused_multi_agg::Q1Predicate;
    use arrow_array::{Date32Array, Float64Array, RecordBatch, StringViewArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    /// Build a 10-row Q6-shaped batch with a known-good answer.
    fn small_q6_batch() -> (RecordBatch, Arc<Schema>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        // Matching rows: 0, 2, 4, 6 → 100*0.06 + 100*0.07 + 100*0.06 + 100*0.05 = 24
        let qty = Float64Array::from(vec![
            23.0, 30.0, 10.0, 25.0, 15.0, 24.0, 20.0, 35.0, 22.0, 5.0,
        ]);
        let price = Float64Array::from(vec![100.0; 10]);
        let disc = Float64Array::from(vec![
            0.06, 0.05, 0.07, 0.05, 0.06, 0.05, 0.05, 0.05, 0.04, 0.10,
        ]);
        let ship = Date32Array::from(vec![
            9000, 8000, 9100, 9200, 8900, 9000, 8800, 9050, 9020, 9080,
        ]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(qty),
                Arc::new(price),
                Arc::new(disc),
                Arc::new(ship),
            ],
        )
        .unwrap();
        (batch, schema)
    }

    #[tokio::test]
    async fn fused_aggregate_exec_runs_q6spec_end_to_end() {
        let (batch, schema) = small_q6_batch();
        let mem = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("lineitem", Arc::new(mem)).unwrap();

        let df = ctx.sql("SELECT * FROM lineitem").await.unwrap();
        let physical = df.create_physical_plan().await.unwrap();

        let predicate = Q6Predicate {
            date_lo: 8766,
            date_hi: 9131,
            disc_lo: 0.05,
            disc_hi: 0.07,
            qty_hi: 24.0,
        };
        let spec = Q6Spec::try_new(predicate, &physical.schema()).unwrap();
        let exec = FusedAggregateExec::try_new(physical, spec).unwrap();

        let mut s = exec.execute(0, ctx.task_ctx()).unwrap();
        let result = s.try_next().await.unwrap().expect("at least one batch");
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 1);
        let f = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (f.value(0) - 24.0).abs() < 1e-9,
            "expected revenue 24.0, got {}",
            f.value(0)
        );
    }

    fn small_q1_batch() -> (RecordBatch, Arc<Schema>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8View, false),
            Field::new("l_linestatus", DataType::Utf8View, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        let rflag = StringViewArray::from(vec!["N", "N", "A", "R"]);
        let lstatus = StringViewArray::from(vec!["F", "F", "F", "F"]);
        let qty = Float64Array::from(vec![10.0, 10.0, 20.0, 5.0]);
        let price = Float64Array::from(vec![100.0, 100.0, 200.0, 50.0]);
        let disc = Float64Array::from(vec![0.05, 0.05, 0.10, 0.02]);
        let tax = Float64Array::from(vec![0.10, 0.10, 0.05, 0.05]);
        // First 3 in-window, last filtered out.
        let ship = Date32Array::from(vec![8800, 8800, 8800, 20000]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(rflag),
                Arc::new(lstatus),
                Arc::new(qty),
                Arc::new(price),
                Arc::new(disc),
                Arc::new(tax),
                Arc::new(ship),
            ],
        )
        .unwrap();
        (batch, schema)
    }

    #[tokio::test]
    async fn fused_aggregate_exec_runs_q1spec_end_to_end() {
        let (batch, schema) = small_q1_batch();
        let mem = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("lineitem", Arc::new(mem)).unwrap();

        let df = ctx.sql("SELECT * FROM lineitem").await.unwrap();
        let physical = df.create_physical_plan().await.unwrap();

        let predicate = Q1Predicate {
            shipdate_cutoff: 10471,
        };
        let spec = Q1Spec::try_new(predicate, &physical.schema()).unwrap();
        let exec = FusedAggregateExec::try_new(physical, spec).unwrap();

        let mut s = exec.execute(0, ctx.task_ctx()).unwrap();
        let result = s.try_next().await.unwrap().expect("at least one batch");
        // Q1 always emits 4 rows × 10 cols (one per (rflag,lstatus) group).
        assert_eq!(result.num_rows(), 4);
        assert_eq!(result.num_columns(), 10);
    }

    #[tokio::test]
    async fn fused_aggregate_exec_rejects_non_zero_partition() {
        let (batch, schema) = small_q6_batch();
        let mem = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("lineitem", Arc::new(mem)).unwrap();

        let df = ctx.sql("SELECT * FROM lineitem").await.unwrap();
        let physical = df.create_physical_plan().await.unwrap();

        let predicate = Q6Predicate {
            date_lo: 0,
            date_hi: 30000,
            disc_lo: 0.0,
            disc_hi: 1.0,
            qty_hi: 100.0,
        };
        let spec = Q6Spec::try_new(predicate, &physical.schema()).unwrap();
        let exec = FusedAggregateExec::try_new(physical, spec).unwrap();
        assert!(exec.execute(1, ctx.task_ctx()).is_err());
    }
}
