//! Σ.P.1 — `SharedSubtreeExec` + `SharedSubtreeRegistry`.
//!
//! Session-scoped subquery CSE. When the optimizer detects that two
//! plan locations compute the same subtree (e.g. Q15's `revenue_s` CTE
//! materialized twice as separate `AggregateExec` chains), it wraps
//! BOTH locations in `SharedSubtreeExec` instances that share the same
//! `Arc<CachedBatches>`. The first `execute()` populates the cache;
//! subsequent reads — from the other duplicate, or from a future
//! query in the same session — replay the cached batches.
//!
//! ## What this is NOT
//!
//! - **Not** a logical-CTE materialization. DataFusion 53 has no
//!   non-recursive CTE materialization at the logical layer; this
//!   sidesteps that by sharing at the physical layer through the
//!   registry.
//! - **Not** transparent to subsequent physical optimizer rules.
//!   `children()` returns empty — the cached subtree is sealed off so
//!   later rules can't rewrite under us and silently desync the cache
//!   key from the cached payload.
//! - **Not** invalidated on schema or data change. The session-scoped
//!   contract is: if you re-register a table under the same name, you
//!   should also rebuild the `SessionContext`. Within a single
//!   `SessionContext`, table identity is assumed stable.
//!
//! ## Output partitioning
//!
//! Always `UnknownPartitioning(1)`. The cache is a flat `Vec<RecordBatch>`;
//! the planner only calls `execute(0)`. Downstream operators that need
//! partitioning can be repartitioned above us — but for the Q15 shape,
//! the consumers are aggregate-on-coalesced-input and don't need it.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures_util::TryFutureExt;
use futures_util::stream::{self, TryStreamExt};
use tokio::sync::OnceCell;

/// POL.0 Phase-2 instrumentation — bench/gate counters for the CSE
/// populate path. `CSE_POPULATES` bumps on every first-execute populate
/// (any SharedSubtreeExec); `CSE_PARALLEL_POPULATES` bumps only when the
/// concurrent-drain path actually ran (`EMAT_CSE_PARALLEL=1` AND n_parts>1).
/// The 22q A/B reads these to prove the flag is INERT on non-CSE queries
/// and LIVE on the CSE ones (Q15 + Σ.Z's Q11/Q17/Q21).
pub static CSE_POPULATES: AtomicU64 = AtomicU64::new(0);
pub static CSE_PARALLEL_POPULATES: AtomicU64 = AtomicU64::new(0);

/// One cached subtree output: schema + lazily-populated batches. The
/// `OnceCell` serializes concurrent `get_or_try_init` callers; only the
/// first one runs the closure, the others await the result.
pub struct CachedBatches {
    schema: SchemaRef,
    once: OnceCell<Vec<RecordBatch>>,
}

impl CachedBatches {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            once: OnceCell::new(),
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn is_populated(&self) -> bool {
        self.once.initialized()
    }
}

/// Session-scoped registry of cached subtree outputs, keyed by the
/// structural hash of the subtree. Multiple `SharedSubtreeExec`
/// instances pointing at the same key receive the same
/// `Arc<CachedBatches>` — so the first to execute populates, and all
/// the rest replay.
///
/// Hold an `Arc<SharedSubtreeRegistry>` for the duration of a session.
/// The dedupe rule holds a clone; bench harnesses and unit tests can
/// inspect `len()` / `is_empty()` to verify caching behaviour.
#[derive(Default)]
pub struct SharedSubtreeRegistry {
    by_key: Mutex<HashMap<u64, Arc<CachedBatches>>>,
}

impl std::fmt::Debug for SharedSubtreeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.by_key.lock().map(|g| g.len()).unwrap_or(0);
        write!(f, "SharedSubtreeRegistry(entries={n})")
    }
}

impl SharedSubtreeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the `Arc<CachedBatches>` for `key`, creating a fresh
    /// empty one with the given schema if absent. All callers with the
    /// same key receive a clone of the same `Arc`.
    pub fn get_or_create(&self, key: u64, schema: SchemaRef) -> Arc<CachedBatches> {
        let mut g = self
            .by_key
            .lock()
            .expect("SharedSubtreeRegistry mutex poisoned");
        g.entry(key)
            .or_insert_with(|| Arc::new(CachedBatches::new(schema)))
            .clone()
    }

    /// Number of cache entries currently registered. Bench / test
    /// instrumentation.
    pub fn len(&self) -> usize {
        self.by_key.lock().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop every cached subtree. Useful when re-registering tables
    /// under the same name with different data.
    pub fn clear(&self) {
        if let Ok(mut g) = self.by_key.lock() {
            g.clear();
        }
    }
}

/// `ExecutionPlan` that wraps a child subtree and serves its output
/// from a shared cache. First call to `execute(0)` runs the child
/// across all its input partitions and stores the collected batches
/// in `cached.once`. Subsequent calls (from this instance, or from
/// any other `SharedSubtreeExec` sharing the same `Arc<CachedBatches>`)
/// replay the cached batches without re-executing the child.
pub struct SharedSubtreeExec {
    input: Arc<dyn ExecutionPlan>,
    cached: Arc<CachedBatches>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SharedSubtreeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, cached: Arc<CachedBatches>) -> Self {
        let schema = input.schema();
        let eq_props = EquivalenceProperties::new(schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            cached,
            schema,
            properties,
        }
    }

    /// For inspection.
    pub fn cached(&self) -> &Arc<CachedBatches> {
        &self.cached
    }

    /// The shared subtree this node computes (once) and serves from.
    /// `children()` deliberately hides it — optimizer rules must not
    /// rewrite a shared subtree per consumer — so plan analyses that
    /// need to SEE through the sharing (e.g. the mesh gate's Σ.Q15.FP
    /// hazard walk) descend via this accessor instead.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl std::fmt::Debug for SharedSubtreeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SharedSubtreeExec(populated={}, fields={})",
            self.cached.is_populated(),
            self.schema.fields().len()
        )
    }
}

impl DisplayAs for SharedSubtreeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SharedSubtreeExec(populated={})",
            self.cached.is_populated()
        )
    }
}

impl ExecutionPlan for SharedSubtreeExec {
    fn name(&self) -> &str {
        "SharedSubtreeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// Opaque to plan walks. Hiding the child from the downstream
    /// optimizer prevents post-rules from rewriting the input under
    /// us — which would silently desync the structural cache key from
    /// the actual computation, breaking the sharing invariant for
    /// the OTHER duplicate location that still points at the original
    /// cached subtree.
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "SharedSubtreeExec: partition={partition}, only 0 is valid (UnknownPartitioning(1))"
            )));
        }
        let cached = self.cached.clone();
        let input = self.input.clone();
        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();
        // Read the env knobs on the calling thread, before the populate
        // future is built (cheap; avoids re-reading per partition).
        let parallel = cse_parallel_enabled();
        let trace = std::env::var_os("EMAT_CSE_TRACE").is_some();

        let fut = async move {
            // OnceCell::get_or_try_init: first caller populates,
            // concurrent callers await the same future, all see the
            // same result.
            let batches = cached
                .once
                .get_or_try_init(|| populate(&input, &ctx, parallel, trace))
                .await?;
            // Replay: clone the Arc<RecordBatch> headers (Arrow data
            // buffers are themselves Arc-shared, so this is cheap).
            let owned: Vec<RecordBatch> = batches.to_vec();
            Ok::<_, DataFusionError>(stream::iter(
                owned.into_iter().map(Ok::<_, DataFusionError>),
            ))
        };

        let s = fut.try_flatten_stream();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_stream,
            s,
        )))
    }
}

/// POL.0 Phase-2 — whether to drive the wrapped subtree's partitions
/// CONCURRENTLY when first populating the cache. Default ON (opt OUT with
/// `EMAT_CSE_PARALLEL=0`).
///
/// The serial `for p in 0..n_parts` drain (the `else` arm of `populate`)
/// pulls each output partition of the wrapped aggregate to completion
/// before starting the next. When that aggregate is `FinalPartitioned`
/// (N hash partitions — the SF≥10 Q15 shape), the `RepartitionExec`
/// beneath it backpressures the N-1 un-drained channels, so the lineitem
/// decode underneath cannot run N-wide — it stalls once those channels
/// fill. Concurrent drain removes that throttle.
///
/// Default-on is safe: the parallel path is a strict no-op where it
/// doesn't fire (`n_parts <= 1` → identical serial branch) and produces a
/// byte-identical cache where it does (per-partition gather in index
/// order). 22q SF=10 gate (cse_parallel_22q_ab): ONLY Q15 fires (−13.9%),
/// every other query has 0 parallel-populates (provably inert), zero
/// regressions, tpch_validate 22/22. The win grows with scale; there is no
/// scale-dependent regression to gate against.
fn cse_parallel_enabled() -> bool {
    std::env::var("EMAT_CSE_PARALLEL")
        .ok()
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(true)
}

/// Run the wrapped subtree and collect ALL its output batches into the
/// flat cache vector.
///
/// `parallel` spawns one drain task per input partition so CPU-bound
/// decode runs across the tokio threadpool instead of one-partition-at-a-
/// time. Batches are gathered in partition-index order, so the output
/// multiset AND order are byte-identical to the serial path — this
/// preserves the float-sum-determinism contract that this CSE exists to
/// provide (`DedupeAggregateForFloatDeterminism`). With `n_parts <= 1`
/// the parallel path is a no-op (falls through to the serial drain).
async fn populate(
    input: &Arc<dyn ExecutionPlan>,
    ctx: &Arc<TaskContext>,
    parallel: bool,
    trace: bool,
) -> DfResult<Vec<RecordBatch>> {
    let n_parts = input.output_partitioning().partition_count();
    CSE_POPULATES.fetch_add(1, Ordering::Relaxed);
    if trace {
        eprintln!("[CSE] populate: n_parts={n_parts} parallel={parallel}");
    }
    // `tokio::spawn` panics if there is no current runtime. Every
    // ematix-flow execution path drives streams on a multi-thread tokio
    // runtime, but guard anyway so default-on can never panic a caller
    // that drives us on a bare executor → fall back to the serial drain.
    let can_spawn = tokio::runtime::Handle::try_current().is_ok();
    if parallel && n_parts > 1 && can_spawn {
        CSE_PARALLEL_POPULATES.fetch_add(1, Ordering::Relaxed);
        // Spawn a drain task per partition. Each owns its stream and runs
        // on whatever worker thread tokio schedules it to → real
        // cross-thread parallelism for the decode, and the N output
        // channels of the RepartitionExec below all drain at once.
        let mut handles = Vec::with_capacity(n_parts);
        for p in 0..n_parts {
            let inp = input.clone();
            let c = ctx.clone();
            handles.push(tokio::spawn(async move {
                let mut s = inp.execute(p, c)?;
                let mut part = Vec::new();
                while let Some(b) = s.try_next().await? {
                    part.push(b);
                }
                Ok::<_, DataFusionError>(part)
            }));
        }
        // Gather in partition order → deterministic concatenation even
        // though tasks finish out of order.
        let mut all = Vec::new();
        for h in handles {
            let part = h
                .await
                .map_err(|e| DataFusionError::Execution(format!("CSE drain task failed: {e}")))??;
            all.extend(part);
        }
        Ok(all)
    } else {
        let mut all = Vec::new();
        for p in 0..n_parts {
            let mut s = input.execute(p, ctx.clone())?;
            while let Some(b) = s.try_next().await? {
                all.push(b);
            }
        }
        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    // Exec-level behaviour (populate-on-first-execute, replay-on-second)
    // is covered by `dedupe_aggregate_rule::tests::q15_*` end-to-end and
    // `cross_query_cache_hit`, which exercise SharedSubtreeExec through
    // a real `SessionContext` + `MemTable` plan. The tests below cover
    // only the pure registry data-structure invariants.

    #[test]
    fn registry_dedups_by_key() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let reg = SharedSubtreeRegistry::new();
        let a = reg.get_or_create(7, schema.clone());
        let b = reg.get_or_create(7, schema.clone());
        let c = reg.get_or_create(8, schema);
        assert!(Arc::ptr_eq(&a, &b));
        assert!(!Arc::ptr_eq(&a, &c));
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn registry_clear_drops_entries() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let reg = SharedSubtreeRegistry::new();
        let _ = reg.get_or_create(1, schema.clone());
        let _ = reg.get_or_create(2, schema);
        assert_eq!(reg.len(), 2);
        reg.clear();
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn cached_batches_starts_unpopulated() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let c = CachedBatches::new(schema);
        assert!(!c.is_populated());
        assert_eq!(c.schema().fields().len(), 1);
    }

    /// POL.0 Phase-2 — the concurrent-drain populate path must produce the
    /// IDENTICAL batch sequence as the serial path on a multi-partition
    /// input. (Same multiset is necessary for correctness; same ORDER is
    /// what preserves the float-determinism contract — we gather per
    /// partition in index order, so even out-of-order task completion can't
    /// reshuffle the concatenation.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn populate_parallel_matches_serial() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        // Three partitions with distinct, ordered value ranges so any
        // cross-partition reshuffle would be detectable as a row-order diff.
        let mk = |base: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(
                    (base..base + 1000).collect::<Vec<_>>(),
                ))],
            )
            .unwrap()
        };
        let parts = vec![vec![mk(0)], vec![mk(1000)], vec![mk(2000)]];
        let mt = Arc::new(MemTable::try_new(schema.clone(), parts).unwrap());
        let ctx = SessionContext::new();
        ctx.register_table("t", mt).unwrap();

        let plan = ctx
            .sql("SELECT x FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert!(
            plan.output_partitioning().partition_count() > 1,
            "test needs a multi-partition input to exercise the parallel path"
        );
        let tc = ctx.task_ctx();

        let serial = populate(&plan, &tc, false, false).await.unwrap();
        let parallel = populate(&plan, &tc, true, false).await.unwrap();

        let flat = |bs: &[RecordBatch]| -> Vec<i64> {
            bs.iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect()
        };
        // Byte-identical row sequence, not just same multiset.
        assert_eq!(flat(&serial), flat(&parallel));
        assert_eq!(flat(&serial).len(), 3000);
    }
}
