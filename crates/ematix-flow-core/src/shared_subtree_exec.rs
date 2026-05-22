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
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures_util::TryFutureExt;
use futures_util::stream::{self, TryStreamExt};
use tokio::sync::OnceCell;

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
        let mut g = self.by_key.lock().expect("SharedSubtreeRegistry mutex poisoned");
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

        let fut = async move {
            // OnceCell::get_or_try_init: first caller populates,
            // concurrent callers await the same future, all see the
            // same result.
            let batches = cached
                .once
                .get_or_try_init(|| async {
                    let n_parts = input.output_partitioning().partition_count();
                    let mut all = Vec::new();
                    for p in 0..n_parts {
                        let mut s = input.execute(p, ctx.clone())?;
                        while let Some(b) = s.try_next().await? {
                            all.push(b);
                        }
                    }
                    Ok::<_, DataFusionError>(all)
                })
                .await?;
            // Replay: clone the Arc<RecordBatch> headers (Arrow data
            // buffers are themselves Arc-shared, so this is cheap).
            let owned: Vec<RecordBatch> = batches.iter().cloned().collect();
            Ok::<_, DataFusionError>(stream::iter(
                owned.into_iter().map(Ok::<_, DataFusionError>),
            ))
        };

        let s = fut.try_flatten_stream();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema_for_stream, s)))
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
}
