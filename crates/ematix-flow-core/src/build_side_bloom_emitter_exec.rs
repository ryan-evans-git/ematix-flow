//! Σ.Q.L9 slice 2 — pass-through wrapper that observes batches flowing
//! from a HashJoinExec build child, accumulates the join-key column
//! into a `BloomFilter`, and publishes a
//! `ColumnPredicate::I64InBloom` to a runtime sideband once all
//! build-side partitions have drained.
//!
//! ## Architecture
//!
//! The wrapper sits between the original build subtree and the
//! HashJoinExec. From DataFusion's perspective, the wrapper is just
//! another `ExecutionPlan` that forwards batches unchanged — the join
//! sees the same data, in the same partition layout, in the same
//! order. The bloom emission is a pure side-effect.
//!
//! Per partition:
//!   1. Allocate a local BloomFilter sized for
//!      `expected_keys / n_partitions`.
//!   2. Pull batches from the input. For each batch: insert the i64
//!      key column into the local bloom; forward the batch downstream
//!      unchanged.
//!   3. On stream-end: acquire the shared `local_blooms` lock, push
//!      our local bloom, increment the `completed` counter.
//!   4. If `completed == n_partitions`, drain the vec, OR-merge all
//!      locals into one union bloom, wrap as
//!      `ColumnPredicate::I64InBloom { col_idx: target_col_idx,
//!      bloom }`, and publish to the sideband.
//!
//! Mutex contention is per-partition-finish (once each), not per-row.
//!
//! ## Timing guarantee
//!
//! HashJoinExec consumes the build side fully before issuing probe
//! batches. So by the time the probe-side scan's `execute()` runs and
//! peeks the sideband, every build partition has run through step (4)
//! and the publish is complete. No reads-before-write races.

use std::any::Any;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::Array;
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures_util::TryStreamExt;
use futures_util::stream::StreamExt;

use crate::bloom::BloomFilter;
use crate::bridge_filter_sideband::BridgeFilterSideband;
use crate::ematix_fast_parquet::ColumnPredicate;

/// Σ.Q.L9 — Wrapper exec that emits a build-side bloom to a sideband.
#[derive(Debug)]
pub struct BuildSideBloomEmitterExec {
    input: Arc<dyn ExecutionPlan>,
    /// Index into INPUT schema of the i64 join-key column.
    key_col_idx: usize,
    /// Index into the PROBE scan's schema where the I64InBloom
    /// predicate will be applied. Set by the planner rule when it
    /// matches the HashJoin's other side to an EmatixFastParquetExec.
    target_col_idx: usize,
    sideband: BridgeFilterSideband,
    /// Per-partition local blooms, pushed in by each finished
    /// partition. Drained + merged + published when `completed`
    /// reaches `n_partitions`.
    local_blooms: Arc<Mutex<Vec<BloomFilter>>>,
    completed: Arc<AtomicUsize>,
    n_partitions: usize,
    /// Used to size each partition's local bloom.
    expected_keys_per_partition: usize,
    properties: Arc<PlanProperties>,
}

impl BuildSideBloomEmitterExec {
    /// Construct the wrapper.
    ///
    /// - `key_col_idx`: column index in `input.schema()` carrying
    ///   the i64 join-key values to bloom.
    /// - `target_col_idx`: column index in the probe scan's schema
    ///   where the `I64InBloom` predicate will land at execute() time.
    /// - `sideband`: shared channel to publish into when all
    ///   partitions complete.
    /// - `expected_total_keys`: caller's estimate of the total
    ///   distinct build-side key count. Used to size the bloom (1%
    ///   FPR target).
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        key_col_idx: usize,
        target_col_idx: usize,
        sideband: BridgeFilterSideband,
        expected_total_keys: usize,
    ) -> DfResult<Self> {
        let in_schema = input.schema();
        if key_col_idx >= in_schema.fields().len() {
            return Err(DataFusionError::Internal(format!(
                "BuildSideBloomEmitterExec: key_col_idx={key_col_idx} out of bounds"
            )));
        }
        if in_schema.field(key_col_idx).data_type() != &datafusion::arrow::datatypes::DataType::Int64
        {
            return Err(DataFusionError::Internal(format!(
                "BuildSideBloomEmitterExec: key column must be Int64, got {:?}",
                in_schema.field(key_col_idx).data_type()
            )));
        }
        let n_partitions = input.output_partitioning().partition_count().max(1);
        let expected_keys_per_partition = (expected_total_keys / n_partitions).max(64);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(in_schema.clone()),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            key_col_idx,
            target_col_idx,
            sideband,
            local_blooms: Arc::new(Mutex::new(Vec::with_capacity(n_partitions))),
            completed: Arc::new(AtomicUsize::new(0)),
            n_partitions,
            expected_keys_per_partition,
            properties,
        })
    }

    pub fn key_col_idx(&self) -> usize {
        self.key_col_idx
    }
    pub fn target_col_idx(&self) -> usize {
        self.target_col_idx
    }
    pub fn sideband(&self) -> &BridgeFilterSideband {
        &self.sideband
    }
}

impl DisplayAs for BuildSideBloomEmitterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "BuildSideBloomEmitterExec(key_col_idx={}, target_col_idx={}, expected_keys_per_partition={})",
            self.key_col_idx, self.target_col_idx, self.expected_keys_per_partition
        )
    }
}

impl ExecutionPlan for BuildSideBloomEmitterExec {
    fn name(&self) -> &str {
        "BuildSideBloomEmitterExec"
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
            DataFusionError::Internal(
                "BuildSideBloomEmitterExec requires exactly 1 child".into(),
            )
        })?;
        Ok(Arc::new(Self::try_new(
            new_input,
            self.key_col_idx,
            self.target_col_idx,
            self.sideband.clone(),
            self.expected_keys_per_partition * self.n_partitions,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let key_col_idx = self.key_col_idx;
        let target_col_idx = self.target_col_idx;
        let sideband = self.sideband.clone();
        let local_blooms = self.local_blooms.clone();
        let completed = self.completed.clone();
        let n_partitions = self.n_partitions;
        let expected_per_part = self.expected_keys_per_partition;
        let in_schema: SchemaRef = self.input.schema();

        // Per-partition local bloom — wrapped in Arc<Mutex<…>> so the
        // map closure (per-batch updates) and the stream-end finalize
        // (transfer-into-shared) can both touch it.
        let local_inner = Arc::new(Mutex::new(BloomFilter::for_keys(expected_per_part)));
        let local_for_map = local_inner.clone();

        let upstream = self.input.execute(partition, context)?;
        // For each batch: insert keys into local bloom, forward
        // unchanged. The bloom mutex is uncontended (one partition
        // owns the local).
        let mapped = upstream.map(move |batch_res| match batch_res {
            Ok(batch) => {
                let arr = batch.column(key_col_idx);
                if let Some(i64s) = arr.as_any().downcast_ref::<Int64Array>() {
                    let mut guard = local_for_map.lock().unwrap();
                    if i64s.null_count() == 0 {
                        for i in 0..i64s.len() {
                            guard.insert_i64(i64s.value(i));
                        }
                    } else {
                        for i in 0..i64s.len() {
                            if !i64s.is_null(i) {
                                guard.insert_i64(i64s.value(i));
                            }
                        }
                    }
                }
                Ok(batch)
            }
            Err(e) => Err(e),
        });

        // Wrap with an unfold that forwards items + fires the
        // finalize closure exactly once when upstream returns None.
        // Without this, there's no clean place to hook "stream
        // ended" — the Map combinator doesn't expose one.
        let stream = futures_util::stream::unfold(
            (
                Box::pin(mapped),
                false,
                local_inner,
                local_blooms,
                completed,
                sideband,
                target_col_idx,
                n_partitions,
            ),
            move |(
                mut inner,
                mut finalized,
                local,
                shared,
                completed,
                sideband,
                target_col_idx,
                n_partitions,
            )| async move {
                match inner.next().await {
                    Some(item) => Some((
                        item,
                        (
                            inner,
                            finalized,
                            local,
                            shared,
                            completed,
                            sideband,
                            target_col_idx,
                            n_partitions,
                        ),
                    )),
                    None => {
                        if !finalized {
                            finalized = true;
                            // Extract local bloom (replace with a
                            // tiny placeholder so the Arc<Mutex> stays
                            // valid for any stray clones).
                            let bloom = std::mem::replace(
                                &mut *local.lock().unwrap(),
                                BloomFilter::for_keys(1),
                            );
                            shared.lock().unwrap().push(bloom);
                            let prev = completed.fetch_add(1, Ordering::SeqCst);
                            if prev + 1 == n_partitions {
                                // Last partition — drain + union-merge
                                // + publish.
                                let mut all = shared.lock().unwrap();
                                if let Some(mut merged) = all.pop() {
                                    while let Some(other) = all.pop() {
                                        let _ = merged.union_with(&other);
                                    }
                                    sideband.publish(vec![ColumnPredicate::I64InBloom {
                                        col_idx: target_col_idx,
                                        bloom: Arc::new(merged),
                                    }]);
                                }
                            }
                        }
                        None
                    }
                }
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(in_schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    fn make_batch(keys: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(keys))]).unwrap()
    }

    #[tokio::test]
    async fn publishes_bloom_after_all_partitions_drain() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mt = MemTable::try_new(
            schema.clone(),
            vec![
                vec![make_batch(vec![1, 2, 3])],
                vec![make_batch(vec![4, 5])],
                vec![make_batch(vec![6, 7, 8, 9])],
            ],
        )
        .unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let df = ctx.sql("SELECT k FROM t").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let sideband = BridgeFilterSideband::new();
        let wrapper = BuildSideBloomEmitterExec::try_new(
            plan.clone(),
            0,  // key_col_idx
            42, // target_col_idx (arbitrary; tested via the predicate)
            sideband.clone(),
            16,
        )
        .unwrap();

        // Execute every partition to drain.
        use datafusion::physical_plan::ExecutionPlanProperties;
        let n = wrapper.properties().output_partitioning().partition_count();
        for p in 0..n {
            let mut s = wrapper.execute(p, Arc::new(TaskContext::default())).unwrap();
            while let Some(_batch) = s.try_next().await.unwrap() {}
        }
        // Sideband should now have a published predicate.
        let preds = sideband.peek().expect("sideband was not published to");
        assert_eq!(preds.len(), 1);
        match &preds[0] {
            ColumnPredicate::I64InBloom { col_idx, bloom } => {
                assert_eq!(*col_idx, 42);
                for k in 1i64..=9 {
                    assert!(bloom.might_contain_i64(k), "missing key {k}");
                }
                // Definite-miss key (unlikely false positive).
                assert!(!bloom.might_contain_i64(999_999));
            }
            other => panic!("expected I64InBloom, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn forwards_batches_unchanged() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mt = MemTable::try_new(
            schema.clone(),
            vec![vec![make_batch(vec![100, 200, 300])]],
        )
        .unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let plan = ctx
            .sql("SELECT k FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let wrapper = BuildSideBloomEmitterExec::try_new(
            plan,
            0,
            0,
            BridgeFilterSideband::new(),
            10,
        )
        .unwrap();
        use datafusion::physical_plan::ExecutionPlanProperties;
        let n = wrapper.properties().output_partitioning().partition_count();
        let mut all_keys: Vec<i64> = Vec::new();
        for p in 0..n {
            let mut s = wrapper.execute(p, Arc::new(TaskContext::default())).unwrap();
            while let Some(batch) = s.try_next().await.unwrap() {
                let arr = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    all_keys.push(arr.value(i));
                }
            }
        }
        all_keys.sort();
        assert_eq!(all_keys, vec![100, 200, 300]);
    }
}
