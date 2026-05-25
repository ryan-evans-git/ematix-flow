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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::Array;
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::common::Result as DfResult;
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
use crate::i64_set::I64Set;

/// L9.HashSet (2026-05-24) — publish an exact `I64Set` instead of a
/// probabilistic `BloomFilter` when the total accumulated build size
/// is at or below this many keys. Default = 32_768 (256 KB / set);
/// override via `EMAT_L9_SET_THRESHOLD`. See [`i64_set`] module docs
/// for why this is faster on small builds.
const DEFAULT_L9_SET_THRESHOLD: usize = 32_768;

#[inline]
fn l9_set_threshold() -> usize {
    std::env::var("EMAT_L9_SET_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_L9_SET_THRESHOLD)
}

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
    /// Σ.S.B (2026-05-24) — cascading sidebands. The bloom is built
    /// once on the build side; the publish step shares the bloom Arc
    /// and emits one predicate per extra target into its own sideband.
    /// Each `(col_idx, sideband)` corresponds to a *different* probe-
    /// side scan reached by walking past the immediate probe via an
    /// FK chain. Empty in the non-cascading L9 path (default).
    extra_targets: Vec<(usize, BridgeFilterSideband)>,
    /// Per-partition local blooms, pushed in by each finished
    /// partition. Drained + merged + published when `completed`
    /// reaches `n_partitions`.
    local_blooms: Arc<Mutex<Vec<BloomFilter>>>,
    /// L9.HashSet — per-partition exact set, pushed in alongside the
    /// bloom at finalize. An entry is `Some(set)` if the partition
    /// stayed under [`l9_set_threshold`]; `None` if it overflowed.
    /// At publish time, if every partition contributed `Some`, the
    /// emitter publishes `I64InSet` (faster + zero FP). If any
    /// partition's `Option` is `None`, we fall back to publishing
    /// `I64InBloom` from the local_blooms.
    local_sets: Arc<Mutex<Vec<Option<I64Set>>>>,
    /// Lever 3 — per-partition (min, max) of build-side keys. An
    /// entry is `None` when the partition saw zero keys. At publish
    /// time the global (min, max) is the union of per-partition
    /// (min, max) pairs, and is emitted as an `I64Range` predicate
    /// alongside the bloom/set.
    local_ranges: Arc<Mutex<Vec<Option<(i64, i64)>>>>,
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
        Self::try_new_with_extras(
            input,
            key_col_idx,
            target_col_idx,
            sideband,
            Vec::new(),
            expected_total_keys,
        )
    }

    /// Σ.S.B — construct an emitter with one primary target plus a
    /// list of cascading extras. The bloom is built ONCE; the publish
    /// step shares the bloom Arc across the primary and every extra,
    /// emitting one predicate per sideband with that sideband's
    /// target col_idx.
    ///
    /// `extras` is `Vec<(target_col_idx, sideband)>`; each entry must
    /// reference a *distinct* probe-side scan than the primary and
    /// than every other extra. The caller (cascading rule) is
    /// responsible for that uniqueness check.
    pub fn try_new_with_extras(
        input: Arc<dyn ExecutionPlan>,
        key_col_idx: usize,
        target_col_idx: usize,
        sideband: BridgeFilterSideband,
        extra_targets: Vec<(usize, BridgeFilterSideband)>,
        expected_total_keys: usize,
    ) -> DfResult<Self> {
        let in_schema = input.schema();
        if key_col_idx >= in_schema.fields().len() {
            return Err(DataFusionError::Internal(format!(
                "BuildSideBloomEmitterExec: key_col_idx={key_col_idx} out of bounds"
            )));
        }
        if in_schema.field(key_col_idx).data_type()
            != &datafusion::arrow::datatypes::DataType::Int64
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
            extra_targets,
            local_blooms: Arc::new(Mutex::new(Vec::with_capacity(n_partitions))),
            local_sets: Arc::new(Mutex::new(Vec::with_capacity(n_partitions))),
            local_ranges: Arc::new(Mutex::new(Vec::with_capacity(n_partitions))),
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
    /// Σ.S.B — the list of cascading extra `(col_idx, sideband)`
    /// pairs. Empty in the non-cascading L9 path.
    pub fn extra_targets(&self) -> &[(usize, BridgeFilterSideband)] {
        &self.extra_targets
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
            DataFusionError::Internal("BuildSideBloomEmitterExec requires exactly 1 child".into())
        })?;
        Ok(Arc::new(Self::try_new_with_extras(
            new_input,
            self.key_col_idx,
            self.target_col_idx,
            self.sideband.clone(),
            self.extra_targets.clone(),
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
        // Σ.S.B — clone the cascade extras so the publish closure can
        // iterate them on the last-partition finalize. Each extra is
        // `(col_idx, sideband)` for a downstream FK-chained scan.
        let extra_targets = self.extra_targets.clone();
        let local_blooms = self.local_blooms.clone();
        let local_sets = self.local_sets.clone();
        let local_ranges = self.local_ranges.clone();
        let completed = self.completed.clone();
        let n_partitions = self.n_partitions;
        let expected_per_part = self.expected_keys_per_partition;
        let in_schema: SchemaRef = self.input.schema();
        // L9.HashSet — abandon the small-set path on this partition
        // once it exceeds the global threshold. The threshold is read
        // once at execute() time so the map closure stays branch-free
        // on env lookups.
        let set_threshold = l9_set_threshold();

        // Per-partition local bloom + set. Both are wrapped in
        // Arc<Mutex<…>> so the map closure (per-batch updates) and the
        // stream-end finalize (transfer-into-shared) can both touch
        // them. Mutex is uncontended in practice — one partition owns
        // its locals.
        let local_inner = Arc::new(Mutex::new(BloomFilter::for_keys(expected_per_part)));
        let local_for_map = local_inner.clone();
        // Set option: `Some(_)` while still under threshold; replaced
        // with `None` (and dropped) once it overflows. Sized for the
        // expected per-partition keys but bounded by the threshold so
        // a wildly-undersized stats estimate can't blow allocation.
        let initial_set_cap = expected_per_part.min(set_threshold);
        let local_set_inner: Arc<Mutex<Option<I64Set>>> =
            Arc::new(Mutex::new(Some(I64Set::with_keys(initial_set_cap))));
        let local_set_for_map = local_set_inner.clone();
        // Lever 3 — per-partition (min, max). `None` until the first
        // non-null key arrives.
        let local_range_inner: Arc<Mutex<Option<(i64, i64)>>> = Arc::new(Mutex::new(None));
        let local_range_for_map = local_range_inner.clone();

        let upstream = self.input.execute(partition, context)?;
        // For each batch: insert keys into local bloom + set (until
        // overflow), forward unchanged. The mutex is uncontended (one
        // partition owns the locals).
        let mapped = upstream.map(move |batch_res| match batch_res {
            Ok(batch) => {
                let arr = batch.column(key_col_idx);
                if let Some(i64s) = arr.as_any().downcast_ref::<Int64Array>() {
                    let mut bloom_guard = local_for_map.lock().unwrap();
                    let mut set_guard = local_set_for_map.lock().unwrap();
                    let mut range_guard = local_range_for_map.lock().unwrap();
                    let null_count = i64s.null_count();
                    if null_count == 0 {
                        let vals = i64s.values();
                        // Lever 3 — fold batch min/max in a single
                        // pass before per-row bloom/set work. For TPC-H
                        // builds this is ~free vs the bloom inserts.
                        if !vals.is_empty() {
                            let mut mn = vals[0];
                            let mut mx = vals[0];
                            for &v in vals.iter() {
                                if v < mn {
                                    mn = v;
                                }
                                if v > mx {
                                    mx = v;
                                }
                            }
                            let (cur_mn, cur_mx) = range_guard.unwrap_or((mn, mx));
                            *range_guard = Some((cur_mn.min(mn), cur_mx.max(mx)));
                        }
                        for &v in vals.iter() {
                            bloom_guard.insert_i64(v);
                            if let Some(s) = set_guard.as_mut() {
                                s.insert(v);
                                if s.len() > set_threshold {
                                    *set_guard = None;
                                }
                            }
                        }
                    } else {
                        for i in 0..i64s.len() {
                            if !i64s.is_null(i) {
                                let v = i64s.value(i);
                                bloom_guard.insert_i64(v);
                                if let Some(s) = set_guard.as_mut() {
                                    s.insert(v);
                                    if s.len() > set_threshold {
                                        *set_guard = None;
                                    }
                                }
                                let (cur_mn, cur_mx) = range_guard.unwrap_or((v, v));
                                *range_guard = Some((cur_mn.min(v), cur_mx.max(v)));
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
                local_set_inner,
                local_range_inner,
                local_blooms,
                local_sets,
                local_ranges,
                completed,
                sideband,
                target_col_idx,
                extra_targets,
                n_partitions,
                set_threshold,
            ),
            move |(
                mut inner,
                mut finalized,
                local_bloom,
                local_set,
                local_range,
                shared_blooms,
                shared_sets,
                shared_ranges,
                completed,
                sideband,
                target_col_idx,
                extra_targets,
                n_partitions,
                set_threshold,
            )| async move {
                match inner.next().await {
                    Some(item) => Some((
                        item,
                        (
                            inner,
                            finalized,
                            local_bloom,
                            local_set,
                            local_range,
                            shared_blooms,
                            shared_sets,
                            shared_ranges,
                            completed,
                            sideband,
                            target_col_idx,
                            extra_targets,
                            n_partitions,
                            set_threshold,
                        ),
                    )),
                    None => {
                        if !finalized {
                            finalized = true;
                            // Extract local bloom + set + range
                            // (replace with tiny placeholders so the
                            // Arc<Mutex> stays valid for any stray
                            // clones).
                            let bloom = std::mem::replace(
                                &mut *local_bloom.lock().unwrap(),
                                BloomFilter::for_keys(1),
                            );
                            let set_opt = local_set.lock().unwrap().take();
                            let range_opt = local_range.lock().unwrap().take();
                            shared_blooms.lock().unwrap().push(bloom);
                            shared_sets.lock().unwrap().push(set_opt);
                            shared_ranges.lock().unwrap().push(range_opt);
                            let prev = completed.fetch_add(1, Ordering::SeqCst);
                            if prev + 1 == n_partitions {
                                // Last partition — drain + union-merge
                                // + publish.
                                let mut all_sets = shared_sets.lock().unwrap();
                                let sets: Vec<Option<I64Set>> = all_sets.drain(..).collect();
                                drop(all_sets);
                                let mut all_ranges = shared_ranges.lock().unwrap();
                                let ranges: Vec<Option<(i64, i64)>> =
                                    all_ranges.drain(..).collect();
                                drop(all_ranges);

                                // Lever 3 — global (min, max) is the
                                // union across all partitions that saw
                                // any keys.
                                let mut global_range: Option<(i64, i64)> = None;
                                for r in &ranges {
                                    if let Some((mn, mx)) = r {
                                        global_range = Some(match global_range {
                                            None => (*mn, *mx),
                                            Some((gmn, gmx)) => (gmn.min(*mn), gmx.max(*mx)),
                                        });
                                    }
                                }
                                let range_pred =
                                    global_range.map(|(lo, hi)| ColumnPredicate::I64Range {
                                        col_idx: target_col_idx,
                                        lo,
                                        hi,
                                    });

                                // L9.HashSet — if every partition kept
                                // its set (none overflowed) AND the
                                // union stays under the threshold,
                                // build the shared exact-set payload.
                                // Otherwise fall back to a shared bloom
                                // payload merged from all partitions.
                                //
                                // Σ.S.B (2026-05-24): the payload is
                                // built ONCE and shared across the
                                // primary sideband and every extra
                                // cascade target. The col_idx differs
                                // per-target so we construct one
                                // predicate per (col_idx, sideband)
                                // pair below.
                                let all_some = sets.iter().all(|s| s.is_some());
                                let shared_set: Option<Arc<I64Set>> = if all_some {
                                    let mut merged = I64Set::with_keys(set_threshold);
                                    let mut overflow = false;
                                    for s in &sets {
                                        if let Some(s) = s {
                                            merged.extend(s);
                                            if merged.len() > set_threshold {
                                                overflow = true;
                                                break;
                                            }
                                        }
                                    }
                                    if overflow {
                                        None
                                    } else {
                                        Some(Arc::new(merged))
                                    }
                                } else {
                                    None
                                };

                                let shared_bloom: Option<Arc<BloomFilter>> = if shared_set.is_none()
                                {
                                    let mut all_blooms = shared_blooms.lock().unwrap();
                                    if let Some(mut merged) = all_blooms.pop() {
                                        while let Some(other) = all_blooms.pop() {
                                            let _ = merged.union_with(&other);
                                        }
                                        Some(Arc::new(merged))
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };
                                let shared_range: Option<(i64, i64)> = global_range;
                                let _ = range_pred; // legacy local; replaced by per-target construction below

                                // Publish primary + (optionally) the
                                // range predicate. The range pred IS
                                // accumulated (~free) but emitting it
                                // is gated behind EMAT_L9_EMIT_RANGE=1
                                // because the always-on emit was net-
                                // negative on TPC-H SF=10 (Q17 −53%,
                                // Q05/Q07/Q08 also slower). Cause:
                                // each predicate triggers an extra
                                // ParquetFile::open + metadata read in
                                // build_bitmap, and when the build's
                                // range fully overlaps the column's
                                // value distribution (the common case
                                // in TPC-H — Q17's filtered-part keys
                                // span ~100% of l_partkey) no RG can
                                // be skipped, so the range's only
                                // effect is doubling the per-RG
                                // metadata cost.
                                //
                                // The infra (predicate variant + RG-
                                // skip dispatch + accumulator) is kept
                                // so a future bench-positive case (or
                                // a build_bitmap refactor that caches
                                // file metadata) can flip the default.
                                let emit_range = std::env::var("EMAT_L9_EMIT_RANGE")
                                    .ok()
                                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                                    .unwrap_or(false);

                                // Σ.S.B — emit to primary AND each
                                // cascade extra. The bloom/set Arcs are
                                // shared (cheap refcount clone); only
                                // the col_idx changes per-sideband.
                                let mut all_targets: Vec<(usize, &BridgeFilterSideband)> =
                                    Vec::with_capacity(1 + extra_targets.len());
                                all_targets.push((target_col_idx, &sideband));
                                for (ci, sb) in extra_targets.iter() {
                                    all_targets.push((*ci, sb));
                                }
                                for (col_idx, sb) in all_targets.into_iter() {
                                    let mut preds: Vec<ColumnPredicate> = Vec::with_capacity(2);
                                    if let Some(set_arc) = &shared_set {
                                        preds.push(ColumnPredicate::I64InSet {
                                            col_idx,
                                            set: Arc::clone(set_arc),
                                        });
                                    } else if let Some(bloom_arc) = &shared_bloom {
                                        preds.push(ColumnPredicate::I64InBloom {
                                            col_idx,
                                            bloom: Arc::clone(bloom_arc),
                                        });
                                    }
                                    if emit_range {
                                        if let Some((lo, hi)) = shared_range {
                                            preds.push(ColumnPredicate::I64Range {
                                                col_idx,
                                                lo,
                                                hi,
                                            });
                                        }
                                    }
                                    if !preds.is_empty() {
                                        sb.publish(preds);
                                    }
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
    async fn publishes_small_build_as_i64_in_set() {
        // L9.HashSet (2026-05-24): the small-build path now publishes
        // `I64InSet` (exact, 0 FP rate) instead of `I64InBloom`. The
        // build here is 9 keys — well under the 32K threshold — so we
        // expect the set path.
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
            let mut s = wrapper
                .execute(p, Arc::new(TaskContext::default()))
                .unwrap();
            while let Some(_batch) = s.try_next().await.unwrap() {}
        }
        let preds = sideband.peek().expect("sideband was not published to");
        // Lever 3 is gated behind EMAT_L9_EMIT_RANGE=1; default-off
        // means we get exactly the primary predicate here.
        assert_eq!(preds.len(), 1, "expected I64InSet only, got {preds:?}");
        match &preds[0] {
            ColumnPredicate::I64InSet { col_idx, set } => {
                assert_eq!(*col_idx, 42);
                assert_eq!(set.len(), 9, "expected 9 distinct keys");
                for k in 1i64..=9 {
                    assert!(set.contains(k), "missing key {k}");
                }
                assert!(!set.contains(999_999));
                assert!(!set.contains(0));
            }
            other => panic!("expected I64InSet, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn publishes_bloom_when_build_exceeds_set_threshold() {
        // L9.HashSet — with a build that genuinely exceeds the 32K
        // default threshold, every partition's set overflows and we
        // fall back to the bloom path. We use 40_000 distinct keys
        // (well past 32_768) to drive the overflow without mutating
        // the global env var (which would race with other parallel
        // tests in this crate).
        let n_keys = 40_000i64;
        let keys: Vec<i64> = (0..n_keys).collect();
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mt = MemTable::try_new(schema.clone(), vec![vec![make_batch(keys.clone())]]).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let plan = ctx
            .sql("SELECT k FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let sideband = BridgeFilterSideband::new();
        let wrapper = BuildSideBloomEmitterExec::try_new(
            plan.clone(),
            0,
            42,
            sideband.clone(),
            n_keys as usize,
        )
        .unwrap();
        use datafusion::physical_plan::ExecutionPlanProperties;
        let n = wrapper.properties().output_partitioning().partition_count();
        for p in 0..n {
            let mut s = wrapper
                .execute(p, Arc::new(TaskContext::default()))
                .unwrap();
            while let Some(_batch) = s.try_next().await.unwrap() {}
        }
        let preds = sideband.peek().expect("sideband was not published to");
        // Lever 3 default-off; expect only the bloom predicate here.
        assert_eq!(preds.len(), 1, "expected I64InBloom only, got {preds:?}");
        match &preds[0] {
            ColumnPredicate::I64InBloom { col_idx, bloom } => {
                assert_eq!(*col_idx, 42);
                // Spot-check a few keys made it into the bloom.
                for &k in &[0i64, 1, 1000, 39_999] {
                    assert!(bloom.might_contain_i64(k), "missing key {k}");
                }
            }
            other => panic!("expected I64InBloom after overflow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn forwards_batches_unchanged() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mt =
            MemTable::try_new(schema.clone(), vec![vec![make_batch(vec![100, 200, 300])]]).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let plan = ctx
            .sql("SELECT k FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let wrapper =
            BuildSideBloomEmitterExec::try_new(plan, 0, 0, BridgeFilterSideband::new(), 10)
                .unwrap();
        use datafusion::physical_plan::ExecutionPlanProperties;
        let n = wrapper.properties().output_partitioning().partition_count();
        let mut all_keys: Vec<i64> = Vec::new();
        for p in 0..n {
            let mut s = wrapper
                .execute(p, Arc::new(TaskContext::default()))
                .unwrap();
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

    /// Σ.S.B — extras share the bloom/set Arc and receive a predicate
    /// with their own col_idx. Build runs once; publish iterates each
    /// `(col_idx, sideband)` pair.
    #[tokio::test]
    async fn publishes_to_extras_with_per_target_col_idx() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mt =
            MemTable::try_new(schema.clone(), vec![vec![make_batch(vec![10, 20, 30])]]).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let plan = ctx
            .sql("SELECT k FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let primary_sb = BridgeFilterSideband::new();
        let extra1_sb = BridgeFilterSideband::new();
        let extra2_sb = BridgeFilterSideband::new();
        let wrapper = BuildSideBloomEmitterExec::try_new_with_extras(
            plan,
            0, // key_col_idx
            7, // primary target_col_idx
            primary_sb.clone(),
            vec![(11, extra1_sb.clone()), (22, extra2_sb.clone())],
            16,
        )
        .unwrap();

        use datafusion::physical_plan::ExecutionPlanProperties;
        let n = wrapper.properties().output_partitioning().partition_count();
        for p in 0..n {
            let mut s = wrapper
                .execute(p, Arc::new(TaskContext::default()))
                .unwrap();
            while let Some(_batch) = s.try_next().await.unwrap() {}
        }

        // Primary publishes with col_idx=7.
        let primary = primary_sb
            .peek()
            .expect("primary sideband was not published to");
        assert_eq!(primary.len(), 1);
        match &primary[0] {
            ColumnPredicate::I64InSet { col_idx, set } => {
                assert_eq!(*col_idx, 7);
                for k in [10, 20, 30] {
                    assert!(set.contains(k));
                }
            }
            other => panic!("expected I64InSet @ col_idx=7, got {other:?}"),
        }

        // Extras each get their own col_idx with the SAME underlying
        // set values.
        for (sb, expected_col_idx) in &[(&extra1_sb, 11usize), (&extra2_sb, 22usize)] {
            let preds = sb.peek().expect("extra sideband was not published to");
            assert_eq!(preds.len(), 1);
            match &preds[0] {
                ColumnPredicate::I64InSet { col_idx, set } => {
                    assert_eq!(*col_idx, *expected_col_idx);
                    for k in [10, 20, 30] {
                        assert!(set.contains(k));
                    }
                }
                other => panic!("expected I64InSet @ col_idx={expected_col_idx}, got {other:?}"),
            }
        }
    }

    /// Σ.S.B — when the build overflows the set threshold, both
    /// primary and extras receive an I64InBloom predicate with the
    /// SAME shared bloom Arc (verified via `Arc::ptr_eq`) but their
    /// own col_idx.
    #[tokio::test]
    async fn extras_share_bloom_arc_when_overflowing() {
        let n_keys = 40_000i64; // overflows default 32K threshold
        let keys: Vec<i64> = (0..n_keys).collect();
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mt = MemTable::try_new(schema.clone(), vec![vec![make_batch(keys)]]).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let plan = ctx
            .sql("SELECT k FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let primary_sb = BridgeFilterSideband::new();
        let extra_sb = BridgeFilterSideband::new();
        let wrapper = BuildSideBloomEmitterExec::try_new_with_extras(
            plan,
            0,
            3,
            primary_sb.clone(),
            vec![(99, extra_sb.clone())],
            n_keys as usize,
        )
        .unwrap();
        use datafusion::physical_plan::ExecutionPlanProperties;
        let n = wrapper.properties().output_partitioning().partition_count();
        for p in 0..n {
            let mut s = wrapper
                .execute(p, Arc::new(TaskContext::default()))
                .unwrap();
            while let Some(_batch) = s.try_next().await.unwrap() {}
        }

        let primary = primary_sb.peek().expect("primary not published to");
        let extra = extra_sb.peek().expect("extra not published to");
        let (primary_bloom_ptr, primary_col) = match &primary[0] {
            ColumnPredicate::I64InBloom { col_idx, bloom } => (Arc::as_ptr(bloom), *col_idx),
            other => panic!("expected I64InBloom on primary, got {other:?}"),
        };
        let (extra_bloom_ptr, extra_col) = match &extra[0] {
            ColumnPredicate::I64InBloom { col_idx, bloom } => (Arc::as_ptr(bloom), *col_idx),
            other => panic!("expected I64InBloom on extra, got {other:?}"),
        };
        assert_eq!(primary_col, 3);
        assert_eq!(extra_col, 99);
        assert_eq!(
            primary_bloom_ptr, extra_bloom_ptr,
            "primary + extra should share the SAME bloom Arc"
        );
    }
}
