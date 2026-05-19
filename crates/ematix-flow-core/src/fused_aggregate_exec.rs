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
        // Σ.G.2f.3 perf (2026-05-19): internal MPMC fan-out via
        // async-channel. n_producers = input partitions (one tokio
        // task per scan partition pulling and forwarding); n_workers
        // = target_partitions × 1.3 (the same overshoot ratio that
        // beat the RepartitionExec-based fanout on Q01). The shared
        // channel lets any free worker grab the next batch the moment
        // any producer pushes — no per-output-partition round-robin
        // serialization.
        let target_partitions: usize = context
            .session_config()
            .options()
            .execution
            .target_partitions
            .max(1);
        let n_workers: usize = std::env::var("EMAT_FILTER_MULTI_AGG_FANOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or((target_partitions * 13 / 10).max(target_partitions));

        let schema_for_stream = output_schema.clone();
        let fut = async move {
            let timing = std::env::var_os("EMAT_AGG_TIMING").is_some();
            let t_total = if timing { Some(std::time::Instant::now()) } else { None };

            // Bounded MPMC channel. Capacity = n_workers * 2 keeps
            // workers fed (one in-flight + one queued each) without
            // letting producers run too far ahead and blow memory.
            let (tx, rx) = async_channel::bounded::<arrow_array::RecordBatch>(n_workers * 2);

            // Producers: one tokio task per scan partition that
            // drains its stream into the shared channel.
            let mut producers = Vec::with_capacity(input_partitions);
            for p in 0..input_partitions {
                let mut s = input.execute(p, context.clone())?;
                let tx_p = tx.clone();
                producers.push(tokio::spawn(async move {
                    let mut pull_ns: u128 = 0;
                    while let Some(batch) =
                        {
                            let t = std::time::Instant::now();
                            let n = s.try_next().await?;
                            pull_ns += t.elapsed().as_nanos();
                            n
                        }
                    {
                        // If the send fails the consumer side closed
                        // (cancellation / error); stop producing.
                        if tx_p.send(batch).await.is_err() {
                            break;
                        }
                    }
                    Ok::<u128, DataFusionError>(pull_ns)
                }));
            }
            // Drop the local sender so the channel closes when all
            // producer-clones drop.
            drop(tx);

            // Workers: tokio tasks that drain the channel via async
            // recv. Using tokio::spawn (not std::thread) keeps us
            // inside tokio's worker pool — no OS thread storm if many
            // queries run concurrently, and cancellation works.
            let mut workers = Vec::with_capacity(n_workers);
            for _ in 0..n_workers {
                let rx_w = rx.clone();
                let spec_w = spec.clone();
                workers.push(tokio::spawn(async move {
                    let mut acc = <S as AggregateSpec>::Accumulator::default();
                    let mut proc_ns: u128 = 0;
                    let mut n_batches: usize = 0;
                    let mut n_rows: usize = 0;
                    while let Ok(batch) = rx_w.recv().await {
                        n_batches += 1;
                        n_rows += batch.num_rows();
                        let t = std::time::Instant::now();
                        spec_w.process_batch(&batch, &mut acc)?;
                        proc_ns += t.elapsed().as_nanos();
                    }
                    Ok::<(<S as AggregateSpec>::Accumulator, u128, usize, usize), DataFusionError>(
                        (acc, proc_ns, n_batches, n_rows),
                    )
                }));
            }
            drop(rx);

            // Wait for producers (so we surface any scan errors).
            let mut sum_pull_ms = 0.0;
            for p in producers {
                let pull_ns = p.await.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "FusedAggregateExec: producer join failed: {e}"
                    ))
                })??;
                sum_pull_ms += pull_ns as f64 / 1_000_000.0;
            }

            // Wait for workers and merge their accumulators.
            let t_merge = if timing { Some(std::time::Instant::now()) } else { None };
            let mut merged = <S as AggregateSpec>::Accumulator::default();
            let mut sum_proc_ms = 0.0;
            let mut sum_rows = 0usize;
            let mut sum_batches = 0usize;
            for w in workers {
                let (partial, proc_ns, n_batches, n_rows) = w.await.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "FusedAggregateExec: worker join failed: {e}"
                    ))
                })??;
                sum_proc_ms += proc_ns as f64 / 1_000_000.0;
                sum_rows += n_rows;
                sum_batches += n_batches;
                merged = spec.merge(merged, partial);
            }
            let merge_ms = t_merge
                .map(|t| t.elapsed().as_secs_f64() * 1000.0)
                .unwrap_or(0.0);

            let t_fin = if timing { Some(std::time::Instant::now()) } else { None };
            let result = spec.finalize(merged);
            let fin_ms = t_fin
                .map(|t| t.elapsed().as_secs_f64() * 1000.0)
                .unwrap_or(0.0);
            if let Some(t) = t_total {
                let total_ms = t.elapsed().as_secs_f64() * 1000.0;
                let spec_name = std::any::type_name::<S>().rsplit("::").next().unwrap_or("?");
                eprintln!(
                    "[emat.agg] spec={spec_name} producers={input_partitions} workers={n_workers} \
                     batches={sum_batches} rows={sum_rows} sum_pull={sum_pull_ms:.2}ms \
                     sum_proc={sum_proc_ms:.2}ms join+merge={merge_ms:.2}ms finalize={fin_ms:.3}ms \
                     total={total_ms:.2}ms"
                );
            }
            result
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_stream,
            s,
        )))
    }
}

