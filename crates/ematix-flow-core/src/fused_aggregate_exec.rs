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

