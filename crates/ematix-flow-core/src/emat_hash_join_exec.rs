//! HJ.3 — `EmatixHashJoinExec`: a CollectLeft-style Inner-join operator
//! that probes the L13 RobinHood kernel instead of DataFusion's
//! `JoinHashMap`. The build (LEFT) side is collected once into a shared
//! `OnceCell<Arc<EmatHashJoiner>>`; every probe (RIGHT) partition streams
//! its batches through `joiner.probe()`, which the Q08 microbench showed
//! is 2.36× faster than DataFusion's probe on the part⋈lineitem shape.
//!
//! Scope (v1): Inner, single i64-widenable equi-key, build = LEFT. The
//! pre-plan swap rule installs this ONLY on that shape; every other join
//! stays on stock DataFusion (keeps blast radius + codegen tax local).
//!
//! Output partitioning mirrors CollectLeft: the probe (right) side's
//! partitioning is preserved (build is broadcast/collected once).

use std::any::Any;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures_util::{StreamExt, TryFutureExt, TryStreamExt};
use tokio::sync::OnceCell;

use crate::emat_hash_join::{EmatHashJoiner, JoinColumn};

pub struct EmatixHashJoinExec {
    /// Build side (LEFT). Collected once.
    left: Arc<dyn ExecutionPlan>,
    /// Probe side (RIGHT). Streamed per output partition.
    right: Arc<dyn ExecutionPlan>,
    build_key_idx: usize,
    probe_key_idx: usize,
    output: Vec<JoinColumn>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// Shared across probe partitions: the first to execute builds the
    /// hash table; the rest await the same `Arc<EmatHashJoiner>`.
    build_once: Arc<OnceCell<Arc<EmatHashJoiner>>>,
    /// Q10-flip increment 1: surface the otherwise-hidden build/probe cost
    /// (the operator reported `metrics=[]`, blocking the parallel-build and
    /// late-materialization de-risk). `build_time` is the serial hash-table
    /// insert (timed once inside the build closure); `probe_time` the probe.
    metrics: ExecutionPlanMetricsSet,
}

impl EmatixHashJoinExec {
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        build_key_idx: usize,
        probe_key_idx: usize,
        output: Vec<JoinColumn>,
        output_schema: SchemaRef,
    ) -> Self {
        let eq = EquivalenceProperties::new(output_schema.clone());
        // CollectLeft semantics: output keeps the probe side's partitioning.
        let partitioning = right.output_partitioning().clone();
        let properties = Arc::new(PlanProperties::new(
            eq,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            left,
            right,
            build_key_idx,
            probe_key_idx,
            output,
            schema: output_schema,
            properties,
            build_once: Arc::new(OnceCell::new()),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn build_key_idx(&self) -> usize {
        self.build_key_idx
    }
    pub fn probe_key_idx(&self) -> usize {
        self.probe_key_idx
    }

    /// Q10-flip increment 3 (wiring): hand the SHARED build handle to a
    /// downstream [`crate::late_gather_exec::LateGatherExec`] so it can
    /// `gather_build_cols` the wide build columns at the (far smaller) aggregate
    /// output from the IDENTICAL resident build batches — no customer re-scan.
    /// Valid only when this exact operator instance stays in the executed plan
    /// (the build is initialized lazily on first `execute`); do not call
    /// `with_new_children` on it after sharing, as that mints a fresh `OnceCell`.
    pub fn build_once(&self) -> Arc<OnceCell<Arc<EmatHashJoiner>>> {
        self.build_once.clone()
    }
}

impl std::fmt::Debug for EmatixHashJoinExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EmatixHashJoinExec(build_key={}, probe_key={}, out_cols={})",
            self.build_key_idx,
            self.probe_key_idx,
            self.output.len()
        )
    }
}

impl DisplayAs for EmatixHashJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "EmatixHashJoinExec: mode=CollectLeft, join_type=Inner, on=[(build@{}, probe@{})]",
            self.build_key_idx, self.probe_key_idx
        )
    }
}

impl ExecutionPlan for EmatixHashJoinExec {
    fn name(&self) -> &str {
        "EmatixHashJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "EmatixHashJoinExec expects 2 children, got {}",
                children.len()
            )));
        }
        // PRESERVE the shared `build_once` (and metrics) across a child swap
        // rather than minting a fresh `OnceCell`. The physical optimizer
        // (EnforceDistribution/CoalesceBatches) rewrites this node's children
        // AFTER a downstream `LateGatherExec` has captured `build_once()` — a
        // fresh cell there would leave the gatherer pointing at a never-
        // initialized build ("shared join build not initialized"). The cell is
        // empty pre-execution and keyed to the (logically equivalent) build
        // side, so carrying it forward is sound; mirrors `LateGatherExec`.
        let left = children[0].clone();
        let right = children[1].clone();
        let eq = EquivalenceProperties::new(self.schema.clone());
        let partitioning = right.output_partitioning().clone();
        let properties = Arc::new(PlanProperties::new(
            eq,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(Self {
            left,
            right,
            build_key_idx: self.build_key_idx,
            probe_key_idx: self.probe_key_idx,
            output: self.output.clone(),
            schema: self.schema.clone(),
            properties,
            build_once: self.build_once.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let build_once = self.build_once.clone();
        let left = self.left.clone();
        let right = self.right.clone();
        let build_key_idx = self.build_key_idx;
        let probe_key_idx = self.probe_key_idx;
        let output = self.output.clone();
        let out_schema = self.schema.clone();
        let stream_schema = self.schema.clone();
        let build_ctx = ctx.clone();
        let metrics = self.metrics.clone();

        let fut = async move {
            // Q10-flip increment 1: time the (serial) build insert once + the probe.
            let build_time = MetricBuilder::new(&metrics).subset_time("build_time", partition);
            let probe_time = MetricBuilder::new(&metrics).subset_time("probe_time", partition);
            // Build once, shared across probe partitions.
            //
            // SF100.6 v2: parallel build-side drain. v1 (sequential `for p in
            // 0..nparts`) serialized the decode/drain of all 14 build partitions
            // into one task → caught the predicted single-threaded-build wall on
            // Q10 SF=100 (5.73M wide-string cust⋈orders rows, never completed a
            // trial in ~45min). Now: spawn `nparts` concurrent drain tasks via
            // `try_join_all` so the build side decodes in parallel (matches the
            // probe side's parallelism). The hash-table INSERT stays serial — a
            // smaller next-step lever; reclaiming the drain is what the dig's
            // metrics localized as dominant.
            // P2' (gated, EMAT_HJ_OVERLAP=1): start the probe-side decode CONCURRENTLY
            // with the build. Otherwise the LEFT build fully completes before
            // `right.execute()` is polled, serializing ~build_time of LEFT decode that
            // could overlap the (long-pole) probe-side decode. Measured on Q10 SF=100:
            // build_time=1.08s ran ENTIRELY before the 148M-lineitem probe side started
            // (~40% of wall near-serial → eff 7 vs the narrow plan's 12). Spawn the
            // right drain so it overlaps the build; buffer the probe input (the
            // downstream agg/sort breaks the pipeline anyway). Default OFF — the blast
            // radius is buffering the probe side, which only some shapes can afford.
            let overlap = std::env::var("EMAT_HJ_OVERLAP")
                .map(|v| v != "0")
                .unwrap_or(false);
            let right_pre = if overlap {
                let right = right.clone();
                let ctx = ctx.clone();
                Some(tokio::spawn(async move {
                    right
                        .execute(partition, ctx)?
                        .try_collect::<Vec<RecordBatch>>()
                        .await
                }))
            } else {
                None
            };

            let joiner = build_once
                .get_or_try_init(|| async {
                    let _build_timer = build_time.timer();
                    let nparts = left.output_partitioning().partition_count();
                    let drain_futs = (0..nparts).map(|p| {
                        let left = left.clone();
                        let ctx = build_ctx.clone();
                        async move {
                            let s = left.execute(p, ctx)?;
                            let v: Vec<RecordBatch> = s.try_collect().await?;
                            Ok::<Vec<RecordBatch>, DataFusionError>(v)
                        }
                    });
                    let per_part: Vec<Vec<RecordBatch>> =
                        futures_util::future::try_join_all(drain_futs).await?;
                    let batches: Vec<RecordBatch> = per_part.into_iter().flatten().collect();
                    EmatHashJoiner::try_build(
                        &batches,
                        build_key_idx,
                        probe_key_idx,
                        output,
                        out_schema,
                    )
                    .map(Arc::new)
                    .map_err(DataFusionError::Internal)
                })
                .await?
                .clone();

            // P2' overlap: the probe side was decoded concurrently with the build →
            // probe the buffered batches now (the build is hidden behind it).
            if let Some(handle) = right_pre {
                let batches = handle
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("probe-drain task: {e}")))??;
                let _t = probe_time.timer();
                if joiner.is_radix() {
                    let out = joiner
                        .probe_radix_all(&batches)
                        .map_err(DataFusionError::Internal)?;
                    return Ok::<_, DataFusionError>(
                        futures_util::stream::iter(vec![Ok::<RecordBatch, DataFusionError>(out)])
                            .boxed(),
                    );
                }
                let outs: Vec<Result<RecordBatch, DataFusionError>> = batches
                    .iter()
                    .map(|rb| joiner.probe(rb).map_err(DataFusionError::Internal))
                    .collect();
                return Ok(futures_util::stream::iter(outs).boxed());
            }

            // Probe. RADIX.2 spike: if the build is radix-partitioned, BUFFER this
            // partition's probe batches then B-scatter-probe them in one call —
            // each execute() gets temporal cache locality (one ~L2 sub-table active
            // at a time; 14 execute()s → ~14 sub-tables ≈ cache-resident, no global
            // shuffle). This is a pipeline-breaker (collect-before-probe) → it pays
            // the decode/probe-overlap loss the wall A/B is measuring. Otherwise:
            // the streaming per-batch probe that overlaps decode.
            if joiner.is_radix() {
                let batches: Vec<RecordBatch> =
                    right.execute(partition, ctx)?.try_collect().await?;
                let _t = probe_time.timer();
                let out = joiner
                    .probe_radix_all(&batches)
                    .map_err(DataFusionError::Internal)?;
                Ok::<_, DataFusionError>(
                    futures_util::stream::iter(vec![Ok::<RecordBatch, DataFusionError>(out)])
                        .boxed(),
                )
            } else {
                let right_stream = right.execute(partition, ctx)?;
                let mapped = right_stream.map(move |rb| {
                    let rb: RecordBatch = rb?;
                    let _t = probe_time.timer();
                    joiner.probe(&rb).map_err(DataFusionError::Internal)
                });
                Ok::<_, DataFusionError>(mapped.boxed())
            }
        };

        let s = fut.try_flatten_stream();
        Ok(Box::pin(RecordBatchStreamAdapter::new(stream_schema, s)))
    }
}
