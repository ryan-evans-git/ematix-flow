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
        }
    }

    pub fn build_key_idx(&self) -> usize {
        self.build_key_idx
    }
    pub fn probe_key_idx(&self) -> usize {
        self.probe_key_idx
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
        Ok(Arc::new(Self::new(
            children[0].clone(),
            children[1].clone(),
            self.build_key_idx,
            self.probe_key_idx,
            self.output.clone(),
            self.schema.clone(),
        )))
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

        let fut = async move {
            // Build once, shared across probe partitions.
            let joiner = build_once
                .get_or_try_init(|| async {
                    let nparts = left.output_partitioning().partition_count();
                    let mut batches = Vec::new();
                    for p in 0..nparts {
                        let mut s = left.execute(p, build_ctx.clone())?;
                        while let Some(b) = s.try_next().await? {
                            batches.push(b);
                        }
                    }
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

            // Probe: stream this partition's right batches through the kernel.
            let right_stream = right.execute(partition, ctx)?;
            let mapped = right_stream.map(move |rb| {
                let rb: RecordBatch = rb?;
                joiner.probe(&rb).map_err(DataFusionError::Internal)
            });
            Ok::<_, DataFusionError>(mapped)
        };

        let s = fut.try_flatten_stream();
        Ok(Box::pin(RecordBatchStreamAdapter::new(stream_schema, s)))
    }
}
