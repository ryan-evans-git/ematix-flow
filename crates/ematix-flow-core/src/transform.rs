//! Π.4b-1: SQL transform layer (DataFusion).
//!
//! `BatchTransform` sits between `source.read_arrow_stream` and
//! `target.write_arrow_stream` in the streaming pipeline. The
//! reference implementation, [`DataFusionTransform`], compiles a
//! user-supplied `SELECT ... FROM source` once at construction
//! time and re-runs the cached plan against each incoming batch.
//!
//! Zero-transform pipelines (the common case) skip this module
//! entirely — the `Option<Arc<dyn BatchTransform>>` field on the
//! pipeline config defaults to `None`, and `StreamingPipeline::run`
//! short-circuits to today's read→write path.
//!
//! See `docs/SQL_TRANSFORMS_PLAN.md` for the full design.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
pub use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use tokio::sync::{Mutex, OnceCell};

use crate::backend::BackendError;

/// Π.4b-1 / Phase 39.4: per-call context handed to every
/// [`BatchTransform`] method. Replaces the bare `transform(input)`
/// shape so future per-batch metadata (watermarks, source identity,
/// retraction signals) can be threaded through without further
/// trait churn.
///
/// Today's only field is `global_wm` — the pipeline's current
/// watermark, computed as the `min` of per-source watermarks
/// excluding idle sources (Phase 39.4 design). Existing transforms
/// (filter / project / cast / lookup-join) ignore it; the windowed
/// aggregator (Phase 39.4) drives all its emit logic from this
/// field.
///
/// Defaulted to `None` watermark so non-windowed callers can
/// construct the context without thinking about time semantics.
#[derive(Debug, Clone, Default)]
pub struct BatchContext {
    /// Pipeline's current watermark in microseconds since the
    /// Unix epoch. `None` when the pipeline isn't tracking event
    /// time (no windowed transforms configured, or first
    /// iteration before any source has produced a batch).
    pub global_wm: Option<i64>,
    /// Phase 39.5b: identifier of the source that produced this
    /// batch — typically the source's `query` field (Kafka topic,
    /// Pub/Sub subscription, etc.). Stream-stream joins use this
    /// to route each batch to the correct side; non-join
    /// transforms ignore it.
    ///
    /// `None` for callers that don't preserve per-source identity
    /// (e.g. legacy code paths that merge batches before invoking
    /// the transform).
    pub source_id: Option<String>,
}

/// Per-batch transform invoked between read and write in the
/// streaming pipeline.
///
/// Implementations may produce 0..N output batches per input —
/// a `WHERE` clause that drops every row yields 0; a window
/// aggregator that buffers across calls yields 0 until the
/// window fires, then 1+.
#[async_trait]
pub trait BatchTransform: Send + Sync + std::fmt::Debug {
    /// Schema the transform expects on its input. The pipeline
    /// validates the source's output schema matches this at
    /// construction time.
    fn input_schema(&self) -> SchemaRef;

    /// Schema the transform produces. The pipeline validates the
    /// target accepts this layout.
    fn output_schema(&self) -> SchemaRef;

    /// Apply the transform to one batch. The pipeline-supplied
    /// [`BatchContext`] carries per-call metadata (watermark, in
    /// future: source identity / retraction signals). Filter /
    /// project / cast transforms ignore it; the windowed
    /// aggregator (Phase 39.4) reads `ctx.global_wm` to decide
    /// which windows are ready to emit.
    async fn transform(
        &self,
        input: RecordBatch,
        ctx: &BatchContext,
    ) -> Result<Vec<RecordBatch>, BackendError>;

    /// Hook for time-driven emission (windows that fire on a
    /// timer rather than a row arrival). Default no-op; only
    /// stateful aggregators need to override. Receives the same
    /// [`BatchContext`] as `transform` so windowed implementations
    /// can read the current watermark and emit windows whose
    /// `end ≤ ctx.global_wm`.
    async fn on_idle_tick(&self, _ctx: &BatchContext) -> Result<Vec<RecordBatch>, BackendError> {
        Ok(Vec::new())
    }

    /// Phase 39.3: refresh a registered lookup table in place.
    /// The transform must atomically swap the registered
    /// `MemTable` so any in-flight plan sees a consistent view —
    /// no torn reads. Default impl returns an error so transforms
    /// that don't support refresh fail loudly when called.
    async fn refresh_lookup(
        &self,
        _name: &str,
        _schema: SchemaRef,
        _batches: Vec<RecordBatch>,
    ) -> Result<(), BackendError> {
        Err(BackendError::Other(
            "refresh_lookup not supported by this BatchTransform".into(),
        ))
    }

    /// Phase 39.5a PR 3: drain pending session-state changes for
    /// commit to a `StateStore`. Default impl returns empty —
    /// non-stateful transforms have nothing to commit. Session
    /// transforms override; see
    /// [`crate::windowed::WindowedAggregateTransform::take_state_commit`].
    ///
    /// Returns `(state_upserts, state_deletes)` ready to fold into
    /// a [`crate::state_store::CommitSnapshot`].
    async fn take_state_commit(
        &self,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>), BackendError> {
        Ok((Vec::new(), Vec::new()))
    }

    /// Phase 39.5a PR 3: install recovered session state from a
    /// `StateStore` load. Default impl is a no-op — transforms
    /// without state ignore recovered state.
    async fn recover_state(
        &self,
        _state_by_key: &std::collections::HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    /// Phase 39.5a P1.6: drain late rows that the transform has
    /// captured under `LateDataPolicy::Dlq`. The pipeline calls
    /// this after every `transform()` / `on_idle_tick()` and
    /// forwards the batches to the configured `dead_letter_topic`.
    /// Default returns empty — only the windowed transform under
    /// the `Dlq` policy populates anything.
    async fn take_dlq_rows(&self) -> Result<Vec<RecordBatch>, BackendError> {
        Ok(Vec::new())
    }
}

/// Π.4b-3: a static lookup table registered alongside `source`
/// in the transform's `SessionContext`. The transform's SQL can
/// reference it by name (`SELECT s.event_id, u.country FROM
/// source s LEFT JOIN users u ON s.user_id = u.id`).
///
/// Loaded once at construction; never re-registered. Refreshing
/// lookups would belong to a future phase (the design doc's
/// Phase 39.3) — for now lookups are immutable for the
/// pipeline's lifetime.
#[derive(Debug, Clone)]
pub struct LookupTable {
    /// The name the SQL references (`users` in the example
    /// above). Must be unique across all lookups in a transform
    /// and must not be `source` (reserved).
    pub name: String,
    /// Schema of every batch in `batches`. All batches must share
    /// this schema — enforced by `MemTable::try_new`.
    pub schema: SchemaRef,
    /// The lookup's contents. May be empty (a `MemTable` with
    /// zero rows joins as zero matches), but the schema must
    /// still be well-formed.
    pub batches: Vec<RecordBatch>,
}

impl LookupTable {
    pub fn new(name: impl Into<String>, schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            name: name.into(),
            schema,
            batches,
        }
    }
}

/// DataFusion-backed `BatchTransform`. Compiles SQL once;
/// per-batch the source `MemTable` gets swapped and the cached
/// plan runs against the new contents.
///
/// Trivial transforms (a bare `SELECT col1, col2 FROM source`
/// with no expressions, filters, joins, or aggregations) bypass
/// DataFusion entirely and use `RecordBatch::project` instead —
/// keeping the no-op-projection cost near-zero.
pub struct DataFusionTransform {
    sql: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    /// `Some(_)` when the SQL is a column-projection-only plan
    /// — bypasses DataFusion at runtime. `None` triggers the
    /// full DataFusion path.
    trivial_indices: Option<Vec<usize>>,
    /// SessionContext that owns the registered `source` table.
    /// `None` for the trivial-bypass path (DataFusion not used).
    ///
    /// Phase 39.3: wrapped in a `tokio::sync::Mutex` so
    /// `refresh_lookup` can swap a registered `MemTable` while
    /// `transform()` is potentially mid-execute on another tokio
    /// task. The streaming pipeline already serializes batches,
    /// so contention is zero in practice — the lock is there for
    /// correctness against the refresh-task background loader.
    ctx: Option<Mutex<SessionContext>>,
    /// Per-lookup schema captured at `new_with_lookups` time and
    /// updated by `refresh_lookup` when a graceful re-plan accepts
    /// a drifted shape (P3 #24). `refresh_lookup` cross-checks each
    /// refresh against the tracked shape; the cached SQL plan is
    /// bound to the original column names + types, so a mismatch
    /// triggers a tentative re-plan against the new shape and
    /// either accepts (output schema unchanged) or rolls back with
    /// a clear error.
    ///
    /// Mutex because `refresh_lookup(&self, ...)` mutates the entry
    /// when accepting drift; the lock is held only briefly inside
    /// the refresh body.
    lookup_schemas: Mutex<std::collections::HashMap<String, SchemaRef>>,
}

impl std::fmt::Debug for DataFusionTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // SessionContext doesn't implement Debug; render the
        // user-meaningful fields by hand.
        f.debug_struct("DataFusionTransform")
            .field("sql", &self.sql)
            .field("input_schema", &self.input_schema)
            .field("output_schema", &self.output_schema)
            .field("trivial", &self.trivial_indices.is_some())
            .finish()
    }
}

impl DataFusionTransform {
    /// Build a transform from SQL + the source schema. Compiles
    /// the plan, performs the trivial-projection check, captures
    /// the output schema. The SQL must reference a single table
    /// named `source` whose schema matches `input_schema`.
    pub async fn new(sql: &str, input_schema: SchemaRef) -> Result<Self, BackendError> {
        Self::new_with_lookups(sql, input_schema, Vec::new()).await
    }

    /// Π.4b-3: build a transform with one or more static lookup
    /// tables registered alongside `source`. Each lookup is a
    /// pre-loaded `MemTable` referenced by the SQL.
    ///
    /// Returns an error if two lookups share a name, or if any
    /// lookup uses the reserved name `source`.
    pub async fn new_with_lookups(
        sql: &str,
        input_schema: SchemaRef,
        lookups: Vec<LookupTable>,
    ) -> Result<Self, BackendError> {
        Self::new_with_lookups_and_udfs(sql, input_schema, lookups, Vec::new()).await
    }

    /// Build a transform with custom scalar UDFs registered into the
    /// SessionContext alongside any lookups. The escape hatch for
    /// math + domain functions DataFusion's stdlib doesn't cover
    /// (cumulative-normal CDF for Black-Scholes deltas, custom
    /// hashing, financial day-count conventions, etc.).
    ///
    /// UDFs are owned by the transform — the cached SQL plan binds
    /// to them at construction time, so dropping a reference to a
    /// `ScalarUDF` in user code after registration is fine. Two
    /// UDFs with the same name in the input slice raises an error;
    /// duplicate-against-DataFusion's-builtin overrides the builtin
    /// (DataFusion's documented behavior).
    pub async fn new_with_lookups_and_udfs(
        sql: &str,
        input_schema: SchemaRef,
        lookups: Vec<LookupTable>,
        udfs: Vec<Arc<ScalarUDF>>,
    ) -> Result<Self, BackendError> {
        Self::new_with_lookups_udfs_and_aggregate_udfs(sql, input_schema, lookups, udfs, Vec::new())
            .await
    }

    /// Build a transform with both scalar **and** aggregate UDFs.
    /// Scalar UDFs cover per-row math (Black-Scholes delta, custom
    /// hashing); aggregate UDFs cover per-group reductions (volume-
    /// weighted average price, custom percentiles, distinct-by-
    /// cardinality) that DataFusion's stdlib doesn't ship.
    ///
    /// Same lifetime story as `new_with_lookups_and_udfs`: both
    /// kinds register on the inner `SessionContext` at construction
    /// time; the cached SQL plan binds to them so dropping caller-
    /// side references after the call is fine. Two UDFs of either
    /// kind sharing a name is rejected at construction with a
    /// pointer at the offending name.
    pub async fn new_with_lookups_udfs_and_aggregate_udfs(
        sql: &str,
        input_schema: SchemaRef,
        lookups: Vec<LookupTable>,
        udfs: Vec<Arc<ScalarUDF>>,
        aggregate_udfs: Vec<Arc<AggregateUDF>>,
    ) -> Result<Self, BackendError> {
        let ctx = SessionContext::new();

        // Register UDFs first so the SQL planner resolves any
        // function references in the FROM-clause / column list to
        // the user-supplied implementations rather than failing.
        let mut udf_seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for udf in &udfs {
            let name = udf.name().to_string();
            if !udf_seen.insert(name.clone()) {
                return Err(BackendError::Other(format!(
                    "transform: duplicate UDF name `{name}` — each UDF must register \
                     under a unique name"
                )));
            }
            ctx.register_udf((**udf).clone());
        }

        // Aggregate UDFs share the function namespace with scalar
        // UDFs from a planner perspective, but DataFusion exposes a
        // separate registration call. Track names independently so
        // duplicate-name errors blame the right kind.
        let mut udaf_seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for udaf in &aggregate_udfs {
            let name = udaf.name().to_string();
            if !udaf_seen.insert(name.clone()) {
                return Err(BackendError::Other(format!(
                    "transform: duplicate aggregate UDF name `{name}` — each aggregate UDF \
                     must register under a unique name"
                )));
            }
            ctx.register_udaf((**udaf).clone());
        }

        // Register lookups first so the planner sees them when
        // resolving table names in the SQL.
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for lookup in &lookups {
            if lookup.name == "source" {
                return Err(BackendError::Other(
                    "transform: lookup name `source` is reserved".into(),
                ));
            }
            if !seen.insert(lookup.name.as_str()) {
                return Err(BackendError::Other(format!(
                    "transform: duplicate lookup name `{}`",
                    lookup.name
                )));
            }
            let mem = MemTable::try_new(lookup.schema.clone(), vec![lookup.batches.clone()])
                .map_err(|e| {
                    BackendError::Other(format!(
                        "transform: lookup `{}` MemTable: {e}",
                        lookup.name
                    ))
                })?;
            ctx.register_table(lookup.name.as_str(), Arc::new(mem))
                .map_err(|e| {
                    BackendError::Other(format!(
                        "transform: register lookup `{}`: {e}",
                        lookup.name
                    ))
                })?;
        }

        // Empty MemTable for plan compilation. Per-batch
        // execution swaps the contents via `register_table`.
        let empty: Vec<RecordBatch> = Vec::new();
        let mem = MemTable::try_new(input_schema.clone(), vec![empty])
            .map_err(|e| BackendError::Other(format!("transform: empty MemTable: {e}")))?;
        ctx.register_table("source", Arc::new(mem))
            .map_err(|e| BackendError::Other(format!("transform: register source: {e}")))?;

        // Plan-compile.
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| BackendError::Other(format!("transform: parse SQL: {e}")))?;
        let logical = df.logical_plan().clone();
        let output_schema: SchemaRef = logical.schema().inner().clone();

        // Trivial-projection detection: a single Projection node
        // whose expressions are all bare column references over
        // a TableScan with no filter pushdown. Lookups make the
        // plan non-trivial by definition (joins are not bare
        // projections), so the bypass naturally never fires when
        // lookups are present.
        let trivial_indices = if lookups.is_empty() {
            detect_trivial_projection(&logical, &input_schema)
        } else {
            None
        };
        let ctx = if trivial_indices.is_some() {
            None
        } else {
            Some(Mutex::new(ctx))
        };

        let mut lookup_schemas = std::collections::HashMap::new();
        for lookup in &lookups {
            lookup_schemas.insert(lookup.name.clone(), lookup.schema.clone());
        }

        Ok(Self {
            sql: sql.to_string(),
            input_schema,
            output_schema,
            trivial_indices,
            ctx,
            lookup_schemas: Mutex::new(lookup_schemas),
        })
    }

    /// True iff this transform was recognized as a trivial
    /// projection and skips DataFusion at runtime.
    pub fn is_trivial(&self) -> bool {
        self.trivial_indices.is_some()
    }

    /// Read access to the configured SQL — for diagnostics + log
    /// lines.
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

#[async_trait]
impl BatchTransform for DataFusionTransform {
    fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    async fn transform(
        &self,
        input: RecordBatch,
        _ctx: &BatchContext,
    ) -> Result<Vec<RecordBatch>, BackendError> {
        // DataFusionTransform is stateless w.r.t. time semantics —
        // _ctx is unused for filter / project / cast / lookup-join.
        // The windowed aggregator (Phase 39.4) is a separate impl
        // that reads ctx.global_wm.
        if input.schema() != self.input_schema {
            return Err(BackendError::Other(format!(
                "transform input schema mismatch: expected {:?}, got {:?}",
                self.input_schema,
                input.schema()
            )));
        }

        // Trivial-bypass path: pure column projection.
        if let Some(indices) = &self.trivial_indices {
            let projected = input
                .project(indices)
                .map_err(|e| BackendError::Other(format!("transform project: {e}")))?;
            return Ok(vec![projected]);
        }

        // DataFusion path: re-register the `source` table with
        // this batch's contents, then re-execute the cached SQL.
        // The lock is held across the whole batch so a concurrent
        // refresh_lookup() sees a consistent point-in-time.
        let ctx_lock = self
            .ctx
            .as_ref()
            .expect("DataFusionTransform: ctx is Some when not trivial");
        let ctx = ctx_lock.lock().await;
        let mem = MemTable::try_new(self.input_schema.clone(), vec![vec![input]])
            .map_err(|e| BackendError::Other(format!("transform: per-batch MemTable: {e}")))?;
        ctx.deregister_table("source")
            .map_err(|e| BackendError::Other(format!("transform: deregister source: {e}")))?;
        ctx.register_table("source", Arc::new(mem))
            .map_err(|e| BackendError::Other(format!("transform: re-register source: {e}")))?;

        let df = ctx
            .sql(&self.sql)
            .await
            .map_err(|e| BackendError::Other(format!("transform: re-plan: {e}")))?;
        let out: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| BackendError::Other(format!("transform: execute: {e}")))?;
        Ok(out)
    }

    async fn refresh_lookup(
        &self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<(), BackendError> {
        if name == "source" {
            return Err(BackendError::Other(
                "transform: cannot refresh reserved name `source`".into(),
            ));
        }
        let ctx_lock = self.ctx.as_ref().ok_or_else(|| {
            BackendError::Other(
                "transform: trivial-bypass path has no DataFusion ctx — \
                 lookups can't be present, so refresh is meaningless"
                    .into(),
            )
        })?;
        let ctx = ctx_lock.lock().await;

        let mut lookup_schemas = self.lookup_schemas.lock().await;
        let original = lookup_schemas.get(name).cloned().ok_or_else(|| {
            BackendError::Other(format!(
                "transform: refresh: lookup `{name}` is not registered"
            ))
        })?;
        let drifted = !schemas_equivalent(original.as_ref(), schema.as_ref());

        let mem = MemTable::try_new(schema.clone(), vec![batches]).map_err(|e| {
            BackendError::Other(format!("transform: refresh `{name}` MemTable: {e}"))
        })?;

        // DataFusion's deregister_table returns Ok(None) silently
        // when the table isn't registered. We want a clean "unknown
        // lookup" error rather than registering a stranger under
        // that name.
        let prior = ctx
            .deregister_table(name)
            .map_err(|e| {
                BackendError::Other(format!("transform: refresh `{name}` deregister: {e}"))
            })?
            .ok_or_else(|| {
                BackendError::Other(format!(
                    "transform: refresh: lookup `{name}` is not registered"
                ))
            })?;

        if let Err(e) = ctx.register_table(name, Arc::new(mem)) {
            // Restore the prior provider so the ctx isn't left
            // empty. Errors here are best-effort.
            let _ = ctx.register_table(name, prior);
            return Err(BackendError::Other(format!(
                "transform: refresh `{name}` register: {e}"
            )));
        }

        if !drifted {
            // Common path: same shape → nothing else to do.
            return Ok(());
        }

        // P3 #24: graceful re-plan path. Drift is now allowed when
        // the cached SQL still produces the same output shape after
        // re-binding to the new lookup schema. Try planning + compare
        // the new logical-plan output schema to the cached one.
        let plan_result = ctx.sql(&self.sql).await;
        let outcome: Result<(), String> = match plan_result {
            Ok(df) => {
                let new_output: SchemaRef = df.logical_plan().schema().inner().clone();
                if schemas_equivalent(self.output_schema.as_ref(), new_output.as_ref()) {
                    Ok(())
                } else {
                    Err(format!(
                        "would change output schema from {} fields to {} fields",
                        self.output_schema.fields().len(),
                        new_output.fields().len()
                    ))
                }
            }
            Err(e) => Err(format!("re-plan failed: {e}")),
        };

        match outcome {
            Ok(()) => {
                tracing::info!(
                    lookup = %name,
                    original_fields = original.fields().len(),
                    new_fields = schema.fields().len(),
                    "transform: refresh `{name}` schema drift accepted — \
                     output schema unchanged after re-plan"
                );
                lookup_schemas.insert(name.to_string(), schema);
                Ok(())
            }
            Err(reason) => {
                // Roll back to the prior MemTable so the pipeline
                // keeps serving against the original-shape SQL until
                // the supervisor restarts it.
                let _ = ctx.deregister_table(name);
                let _ = ctx.register_table(name, prior);
                Err(BackendError::Other(format!(
                    "transform: refresh `{name}` schema drift — {reason}. \
                     The cached SQL plan is bound to the original schema; \
                     restart the pipeline to pick up the new shape."
                )))
            }
        }
    }
}

/// Phase 39.5a P2.16: compare two `Schema`s structurally. We
/// don't use `Schema::eq` directly because it includes metadata
/// (`HashMap<String, String>`) which lookup-load paths often
/// don't preserve. The drift check that matters is field
/// names + types + nullability — schema metadata changes are
/// benign for query planning.
fn schemas_equivalent(a: &arrow_schema::Schema, b: &arrow_schema::Schema) -> bool {
    if a.fields().len() != b.fields().len() {
        return false;
    }
    for (fa, fb) in a.fields().iter().zip(b.fields().iter()) {
        if fa.name() != fb.name()
            || fa.data_type() != fb.data_type()
            || fa.is_nullable() != fb.is_nullable()
        {
            return false;
        }
    }
    true
}

/// SQL transform whose `DataFusionTransform` is constructed on the
/// first batch — once the source has produced a `RecordBatch` whose
/// `schema()` we can use as the input schema.
///
/// Streaming sources don't generally know their schema until they
/// decode the first message, so the TOML / CLI / Python paths can't
/// build a `DataFusionTransform` at config-load time. This wrapper
/// closes that gap: callers supply only the SQL string, and the
/// pipeline lazy-builds the inner transform on first call.
///
/// Subsequent batches with a different schema produce the same
/// "input schema mismatch" error that `DataFusionTransform` returns
/// directly — first-batch schema is captured and held.
pub struct LazySqlTransform {
    sql: String,
    /// Π.4b-3: lookups loaded eagerly at config-load time and
    /// passed through to the inner `DataFusionTransform` on first
    /// build. Empty Vec = no lookups (plain filter/project/cast).
    lookups: Vec<LookupTable>,
    /// Custom scalar UDFs registered into the inner transform's
    /// `SessionContext` on first build. Same lifetime story as
    /// `lookups` — held until first batch, then handed off.
    udfs: Vec<Arc<ScalarUDF>>,
    /// Custom aggregate UDFs (the `@udaf` analog of `@udf`).
    /// Registered alongside scalar UDFs on first build; held
    /// until then via the same Arc-clone pattern.
    aggregate_udfs: Vec<Arc<AggregateUDF>>,
    inner: OnceCell<DataFusionTransform>,
}

impl std::fmt::Debug for LazySqlTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazySqlTransform")
            .field("sql", &self.sql)
            .field(
                "lookup_names",
                &self.lookups.iter().map(|l| &l.name).collect::<Vec<_>>(),
            )
            .field("initialized", &self.inner.initialized())
            .finish()
    }
}

impl LazySqlTransform {
    /// Build from SQL only. The inner `DataFusionTransform` is
    /// compiled on the first `transform()` call using that batch's
    /// schema.
    pub fn new(sql: impl Into<String>) -> Self {
        Self::new_with_lookups(sql, Vec::new())
    }

    /// Π.4b-3: build with one or more static lookup tables. The
    /// lookups are held until first batch, then passed through to
    /// `DataFusionTransform::new_with_lookups` along with the
    /// captured input schema.
    pub fn new_with_lookups(sql: impl Into<String>, lookups: Vec<LookupTable>) -> Self {
        Self::new_with_lookups_and_udfs(sql, lookups, Vec::new())
    }

    /// Build with both static lookups + custom scalar UDFs. The
    /// streaming-pipeline shape that needs UDFs in user-supplied
    /// SQL (e.g. a `bs_delta(strike, spot, vol, rate, expiry)`
    /// math function) registers them here at config-load time;
    /// they're applied to the inner DataFusion `SessionContext` on
    /// the first batch's schema.
    pub fn new_with_lookups_and_udfs(
        sql: impl Into<String>,
        lookups: Vec<LookupTable>,
        udfs: Vec<Arc<ScalarUDF>>,
    ) -> Self {
        Self::new_with_lookups_udfs_and_aggregate_udfs(sql, lookups, udfs, Vec::new())
    }

    /// Build with lookups + scalar UDFs + aggregate UDFs. The
    /// aggregate-UDF analog of `@udaf` registers here so user SQL
    /// can call them — `SELECT vwap(price, qty) FROM source GROUP BY 1`.
    pub fn new_with_lookups_udfs_and_aggregate_udfs(
        sql: impl Into<String>,
        lookups: Vec<LookupTable>,
        udfs: Vec<Arc<ScalarUDF>>,
        aggregate_udfs: Vec<Arc<AggregateUDF>>,
    ) -> Self {
        Self {
            sql: sql.into(),
            lookups,
            udfs,
            aggregate_udfs,
            inner: OnceCell::new(),
        }
    }

    /// Read access to the SQL — for diagnostics / log lines.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// True iff the inner `DataFusionTransform` has been built.
    /// Used by tests; production callers shouldn't need to peek.
    pub fn is_initialized(&self) -> bool {
        self.inner.initialized()
    }

    async fn ensure_inner(&self, schema: SchemaRef) -> Result<&DataFusionTransform, BackendError> {
        self.inner
            .get_or_try_init(|| async {
                DataFusionTransform::new_with_lookups_udfs_and_aggregate_udfs(
                    &self.sql,
                    schema.clone(),
                    self.lookups.clone(),
                    self.udfs.clone(),
                    self.aggregate_udfs.clone(),
                )
                .await
            })
            .await
    }
}

#[async_trait]
impl BatchTransform for LazySqlTransform {
    fn input_schema(&self) -> SchemaRef {
        self.inner
            .get()
            .expect(
                "LazySqlTransform::input_schema called before first transform() — \
                 schema is captured on first batch",
            )
            .input_schema()
    }

    fn output_schema(&self) -> SchemaRef {
        self.inner
            .get()
            .expect(
                "LazySqlTransform::output_schema called before first transform() — \
                 schema is captured on first batch",
            )
            .output_schema()
    }

    async fn transform(
        &self,
        input: RecordBatch,
        ctx: &BatchContext,
    ) -> Result<Vec<RecordBatch>, BackendError> {
        let inner = self.ensure_inner(input.schema()).await?;
        inner.transform(input, ctx).await
    }

    /// Phase 39.3: refresh delegates to the inner
    /// `DataFusionTransform` once it has been built. Before init,
    /// the source schema is unknown and the SessionContext doesn't
    /// exist yet — refresh has nothing to refresh, so the call is
    /// rejected with a clear error. The CLI's refresh-task tolerates
    /// this and retries on its next interval.
    async fn refresh_lookup(
        &self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<(), BackendError> {
        let Some(inner) = self.inner.get() else {
            return Err(BackendError::Other(
                "LazySqlTransform::refresh_lookup: not initialized — \
                 first batch must be processed before any refresh"
                    .into(),
            ));
        };
        inner.refresh_lookup(name, schema, batches).await
    }
}

/// Detect a `Projection(TableScan(source))` plan whose projection
/// expressions are bare column references — i.e. no functions,
/// no casts, no filters. Returns `Some(indices)` mapping output
/// column index → input column index when trivial, `None`
/// otherwise.
///
/// The detector is intentionally conservative — anything more
/// complex than a flat column projection falls through to the
/// DataFusion path. False negatives are fine (slower but
/// correct); false positives would skip work the SQL asked for.
fn detect_trivial_projection(plan: &LogicalPlan, input_schema: &SchemaRef) -> Option<Vec<usize>> {
    use datafusion::logical_expr::Expr;
    let LogicalPlan::Projection(proj) = plan else {
        return None;
    };
    // The projection's input must be a bare TableScan over
    // `source` with no filters or limit.
    match proj.input.as_ref() {
        LogicalPlan::TableScan(scan) => {
            if scan.table_name.table() != "source"
                || !scan.filters.is_empty()
                || scan.fetch.is_some()
                || scan.projection.is_some()
            {
                return None;
            }
        }
        _ => return None,
    }
    let mut indices: Vec<usize> = Vec::with_capacity(proj.expr.len());
    for expr in &proj.expr {
        let Expr::Column(col) = expr else {
            return None;
        };
        let idx = input_schema.index_of(col.name.as_str()).ok()?;
        indices.push(idx);
    }
    Some(indices)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema_id_name() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn batch_two_rows() -> RecordBatch {
        let schema = schema_id_name();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn select_star_passes_through_unchanged() {
        let t = DataFusionTransform::new("SELECT * FROM source", schema_id_name())
            .await
            .expect("construct transform");
        let out = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("apply transform");
        assert_eq!(out.len(), 1, "one input batch → one output batch");
        let b = &out[0];
        assert_eq!(b.num_rows(), 2);
        assert_eq!(b.num_columns(), 2);
        assert_eq!(b.schema().field(0).name(), "id");
        assert_eq!(b.schema().field(1).name(), "name");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn select_columns_only_uses_trivial_bypass() {
        let t = DataFusionTransform::new("SELECT id, name FROM source", schema_id_name())
            .await
            .expect("construct transform");
        assert!(
            t.is_trivial(),
            "bare SELECT col1, col2 should hit the trivial-projection bypass"
        );
        let out = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("apply transform");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn where_filter_drops_rows() {
        let t =
            DataFusionTransform::new("SELECT id, name FROM source WHERE id = 1", schema_id_name())
                .await
                .expect("construct transform");
        assert!(
            !t.is_trivial(),
            "WHERE filter must take the DataFusion path"
        );
        let out = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("apply");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "WHERE id=1 → exactly one row out of two");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn projection_renames_column() {
        let t =
            DataFusionTransform::new("SELECT id AS user_id, name FROM source", schema_id_name())
                .await
                .expect("construct transform");
        let out = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("apply");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].schema().field(0).name(), "user_id");
        assert_eq!(out[0].schema().field(1).name(), "name");
    }

    /// Custom scalar UDF support: a user-defined `add_one(Int32)`
    /// must register into the SessionContext, plan through the SQL,
    /// and execute against per-batch data — same path a
    /// Black-Scholes-delta UDF would ride for the
    /// option-chain-snapshot pivot example in the README. This is
    /// the foundation for the "DataFusion UDFs through transform.rs"
    /// roadmap item.
    #[tokio::test(flavor = "multi_thread")]
    async fn scalar_udf_round_trips_through_transform_sql() {
        use std::any::Any;
        use std::sync::Arc;

        use arrow_array::Int32Array;
        use datafusion::common::Result as DfResult;
        use datafusion::logical_expr::{
            ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
        };

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct AddOne {
            signature: Signature,
        }

        impl AddOne {
            fn new() -> Self {
                Self {
                    signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
                }
            }
        }

        impl ScalarUDFImpl for AddOne {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "add_one"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
                Ok(DataType::Int32)
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
                let array = args.args[0].clone().into_array(args.number_rows)?;
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("add_one expects Int32 input");
                let out: Int32Array = arr.iter().map(|v| v.map(|x| x + 1)).collect();
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
        }

        let udf = Arc::new(ScalarUDF::from(AddOne::new()));
        let t = DataFusionTransform::new_with_lookups_and_udfs(
            "SELECT add_one(user_id) AS next_id FROM source",
            schema_user_id_event(),
            Vec::new(),
            vec![udf],
        )
        .await
        .expect("construct transform with UDF");
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("apply UDF");
        assert_eq!(out.len(), 1);
        let arr = out[0]
            .column_by_name("next_id")
            .expect("output has next_id column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("UDF returns Int32");
        // batch_events has user_ids [1, 2, 99]; add_one → [2, 3, 100].
        assert_eq!(arr.value(0), 2);
        assert_eq!(arr.value(1), 3);
        assert_eq!(arr.value(2), 100);
    }

    /// Two UDFs registered under the same name is a config error
    /// caught at construction — surfacing it loud means a user
    /// can't accidentally shadow their own function and wonder
    /// why a Black-Scholes call returns the wrong number.
    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_udf_names_rejected_at_construction() {
        use std::any::Any;
        use std::sync::Arc;

        use datafusion::common::Result as DfResult;
        use datafusion::logical_expr::{
            ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
        };

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct Stub {
            signature: Signature,
        }
        impl ScalarUDFImpl for Stub {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "noop"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
                Ok(DataType::Int32)
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
                Ok(args.args[0].clone())
            }
        }

        let make = || {
            Arc::new(ScalarUDF::from(Stub {
                signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
            }))
        };
        let err = DataFusionTransform::new_with_lookups_and_udfs(
            "SELECT user_id FROM source",
            schema_user_id_event(),
            Vec::new(),
            vec![make(), make()],
        )
        .await
        .expect_err("duplicate UDF names rejected");
        let msg = err.to_string();
        assert!(msg.contains("duplicate UDF name"), "got: {msg}");
        assert!(msg.contains("noop"), "got: {msg}");
    }

    /// Custom **aggregate** UDF support — the `bs_call_delta`
    /// scalar UDF closes per-row math, but per-group aggregates
    /// (volume-weighted average price, custom percentiles,
    /// distinct-by-cardinality) need an `AggregateUDF`. A trivial
    /// `sum_of_squares(Int64)` aggregator proves the wiring: same
    /// `SessionContext::register_udaf` path that DataFusion's
    /// builtins use, threaded through
    /// `new_with_lookups_udfs_and_aggregate_udfs`.
    #[tokio::test(flavor = "multi_thread")]
    async fn aggregate_udf_round_trips_through_transform_sql() {
        use std::any::Any;
        use std::sync::Arc;

        use arrow_array::Int64Array;
        use datafusion::common::Result as DfResult;
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
        use datafusion::logical_expr::{
            Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
        };
        // sum_of_squares(Int64) → Int64. State is one Int64 (the
        // running sum). Distinct from DataFusion's builtin SUM so
        // we know the test is hitting the user-registered path.
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct SumOfSquares {
            signature: Signature,
        }
        impl SumOfSquares {
            fn new() -> Self {
                Self {
                    signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
                }
            }
        }
        impl AggregateUDFImpl for SumOfSquares {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "sum_of_squares"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
                Ok(DataType::Int64)
            }
            fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
                Ok(Box::new(SumOfSquaresAcc { sum: 0 }))
            }
            fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<arrow_schema::FieldRef>> {
                use arrow_schema::Field;
                let n = args.name;
                Ok(vec![Arc::new(Field::new(
                    format!("{n}[sum]"),
                    DataType::Int64,
                    true,
                ))])
            }
        }

        #[derive(Debug)]
        struct SumOfSquaresAcc {
            sum: i64,
        }
        impl Accumulator for SumOfSquaresAcc {
            fn update_batch(&mut self, values: &[arrow_array::ArrayRef]) -> DfResult<()> {
                let arr = values[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 input");
                for v in arr.iter().flatten() {
                    self.sum += v * v;
                }
                Ok(())
            }
            fn evaluate(&mut self) -> DfResult<ScalarValue> {
                Ok(ScalarValue::Int64(Some(self.sum)))
            }
            fn size(&self) -> usize {
                std::mem::size_of::<Self>()
            }
            fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
                Ok(vec![ScalarValue::Int64(Some(self.sum))])
            }
            fn merge_batch(&mut self, states: &[arrow_array::ArrayRef]) -> DfResult<()> {
                let arr = states[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 state");
                for v in arr.iter().flatten() {
                    self.sum += v;
                }
                Ok(())
            }
        }

        let udaf = Arc::new(AggregateUDF::from(SumOfSquares::new()));
        let t = DataFusionTransform::new_with_lookups_udfs_and_aggregate_udfs(
            "SELECT sum_of_squares(user_id) AS sq FROM source",
            schema_user_id_event(),
            Vec::new(),
            Vec::new(),
            vec![udaf],
        )
        .await
        .expect("construct transform with aggregate UDF");
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("apply aggregate UDF");
        assert_eq!(out.len(), 1);
        let arr = out[0]
            .column_by_name("sq")
            .expect("output has sq column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("aggregate emits Int64");
        // batch_events user_ids = [1, 2, 99]; squares = [1, 4, 9801];
        // sum_of_squares = 9806.
        assert_eq!(arr.len(), 1, "aggregate collapses to one row");
        assert_eq!(arr.value(0), 9806);
    }

    /// Two aggregate UDFs registered under the same name is a
    /// config-load error — symmetric with the scalar-UDF check.
    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_aggregate_udf_names_rejected_at_construction() {
        use std::any::Any;
        use std::sync::Arc;

        use datafusion::common::Result as DfResult;
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
        use datafusion::logical_expr::{
            Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
        };

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct Stub {
            signature: Signature,
        }
        impl AggregateUDFImpl for Stub {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "my_agg"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
                Ok(DataType::Int64)
            }
            fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
                Ok(Box::new(Noop))
            }
            fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<arrow_schema::FieldRef>> {
                use arrow_schema::Field;
                Ok(vec![Arc::new(Field::new(args.name, DataType::Int64, true))])
            }
        }

        #[derive(Debug)]
        struct Noop;
        impl Accumulator for Noop {
            fn update_batch(&mut self, _values: &[arrow_array::ArrayRef]) -> DfResult<()> {
                Ok(())
            }
            fn evaluate(&mut self) -> DfResult<ScalarValue> {
                Ok(ScalarValue::Int64(Some(0)))
            }
            fn size(&self) -> usize {
                0
            }
            fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
                Ok(vec![ScalarValue::Int64(Some(0))])
            }
            fn merge_batch(&mut self, _states: &[arrow_array::ArrayRef]) -> DfResult<()> {
                Ok(())
            }
        }

        let make = || {
            Arc::new(AggregateUDF::from(Stub {
                signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            }))
        };
        let err = DataFusionTransform::new_with_lookups_udfs_and_aggregate_udfs(
            "SELECT user_id FROM source",
            schema_user_id_event(),
            Vec::new(),
            Vec::new(),
            vec![make(), make()],
        )
        .await
        .expect_err("duplicate aggregate UDF names rejected");
        let msg = err.to_string();
        assert!(msg.contains("duplicate aggregate UDF name"), "got: {msg}");
        assert!(msg.contains("my_agg"), "got: {msg}");
    }

    /// Σ.A1 audit: `EXPLAIN`/`EXPLAIN ANALYZE` must round-trip
    /// cleanly through `DataFusionTransform` so users can ask "what
    /// would this plan look like?" without bypassing the framework's
    /// SQL pipeline. The transform's output_schema is whatever
    /// DataFusion advertises for the EXPLAIN node — typically
    /// `(plan_type Utf8, plan Utf8)`. We don't pin the exact field
    /// names (DataFusion has churned them across releases); we
    /// assert two `Utf8` columns and that at least one row is
    /// produced.
    #[tokio::test(flavor = "multi_thread")]
    async fn explain_select_round_trips() {
        let t = DataFusionTransform::new("EXPLAIN SELECT id, name FROM source", schema_id_name())
            .await
            .expect("EXPLAIN construct");
        assert!(
            !t.is_trivial(),
            "EXPLAIN must take the DataFusion path (no trivial-projection bypass)"
        );
        assert_eq!(t.output_schema().fields().len(), 2);
        for f in t.output_schema().fields() {
            assert_eq!(f.data_type(), &DataType::Utf8);
        }
        let out = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("EXPLAIN execute");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert!(total >= 1, "EXPLAIN must emit at least one plan row");
    }

    /// Σ.A1 audit: `EXPLAIN ANALYZE` runs the plan and returns
    /// per-stage metrics. Same shape as `EXPLAIN` from the framework's
    /// perspective — two Utf8 columns — but with row count tied to
    /// the executed plan's metrics. Smoke-only; we just need it to
    /// not error.
    #[tokio::test(flavor = "multi_thread")]
    async fn explain_analyze_select_round_trips() {
        let t = DataFusionTransform::new(
            "EXPLAIN ANALYZE SELECT id, name FROM source",
            schema_id_name(),
        )
        .await
        .expect("EXPLAIN ANALYZE construct");
        let out = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("EXPLAIN ANALYZE execute");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert!(
            total >= 1,
            "EXPLAIN ANALYZE must emit at least one metrics row"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn output_schema_matches_collected_batches() {
        let t = DataFusionTransform::new("SELECT name, id FROM source", schema_id_name())
            .await
            .expect("construct transform");
        let out = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("apply");
        assert_eq!(out[0].schema(), t.output_schema());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cast_changes_column_type() {
        let t = DataFusionTransform::new(
            "SELECT CAST(id AS BIGINT) AS id, name FROM source",
            schema_id_name(),
        )
        .await
        .expect("construct transform");
        assert!(!t.is_trivial(), "CAST must take the DataFusion path");
        let out = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("apply");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(out[0].schema().field(1).data_type(), &DataType::Utf8);
        assert_eq!(out[0].num_rows(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn schema_mismatch_errors() {
        let t = DataFusionTransform::new("SELECT * FROM source", schema_id_name())
            .await
            .expect("construct transform");
        // Build a batch with a different schema.
        let other_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let other =
            RecordBatch::try_new(other_schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let err = t
            .transform(other, &BatchContext::default())
            .await
            .expect_err("schema mismatch");
        assert!(err.to_string().contains("schema mismatch"));
    }

    // --- Π.4b-3: stream-table enrichment join ---------------------------

    fn schema_user_id_event() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int32, false),
            Field::new("event_type", DataType::Utf8, false),
        ]))
    }

    fn batch_events() -> RecordBatch {
        RecordBatch::try_new(
            schema_user_id_event(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 99])),
                Arc::new(StringArray::from(vec!["click", "view", "click"])),
            ],
        )
        .unwrap()
    }

    fn users_lookup() -> LookupTable {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();
        LookupTable::new("users", schema, vec![batch])
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lookup_inner_join_filters_rows_without_match() {
        let t = DataFusionTransform::new_with_lookups(
            "SELECT s.user_id, s.event_type, u.name \
             FROM source s INNER JOIN users u ON s.user_id = u.id",
            schema_user_id_event(),
            vec![users_lookup()],
        )
        .await
        .expect("construct transform");
        assert!(!t.is_trivial(), "join must take the DataFusion path");
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("apply");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "INNER JOIN drops user_id=99 (no match)");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lookup_left_join_keeps_unmatched_rows_with_nulls() {
        let t = DataFusionTransform::new_with_lookups(
            "SELECT s.user_id, u.name \
             FROM source s LEFT JOIN users u ON s.user_id = u.id",
            schema_user_id_event(),
            vec![users_lookup()],
        )
        .await
        .expect("construct transform");
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("apply");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "LEFT JOIN keeps user_id=99 with NULL name");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_lookup_name_errors() {
        let lookup = users_lookup();
        let dup = users_lookup();
        let err = DataFusionTransform::new_with_lookups(
            "SELECT 1 FROM source",
            schema_user_id_event(),
            vec![lookup, dup],
        )
        .await
        .expect_err("duplicate names rejected");
        assert!(err.to_string().contains("duplicate lookup name"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reserved_lookup_name_source_errors() {
        let bad_lookup = LookupTable::new("source", schema_user_id_event(), Vec::new());
        let err = DataFusionTransform::new_with_lookups(
            "SELECT * FROM source",
            schema_user_id_event(),
            vec![bad_lookup],
        )
        .await
        .expect_err("reserved name rejected");
        assert!(err.to_string().contains("reserved"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_lookup_swaps_registered_contents() {
        let t = DataFusionTransform::new_with_lookups(
            "SELECT s.user_id, u.name \
             FROM source s INNER JOIN users u ON s.user_id = u.id",
            schema_user_id_event(),
            vec![users_lookup()],
        )
        .await
        .expect("construct transform");

        // Initial state: 2 of 3 events match (user_id 1, 2; 99 has no match).
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("apply");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);

        // Refresh users with a row covering id=99 too — now 3 of 3 match.
        let new_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let new_batch = RecordBatch::try_new(
            new_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 99])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap();
        t.refresh_lookup("users", new_schema, vec![new_batch])
            .await
            .expect("refresh users");
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("apply post-refresh");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "post-refresh users includes id=99");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_unknown_lookup_errors() {
        let t = DataFusionTransform::new_with_lookups(
            "SELECT s.user_id, u.name \
             FROM source s INNER JOIN users u ON s.user_id = u.id",
            schema_user_id_event(),
            vec![users_lookup()],
        )
        .await
        .expect("construct transform");
        let users_schema = users_lookup().schema;
        let err = t
            .refresh_lookup("nope", users_schema, Vec::new())
            .await
            .expect_err("unknown lookup");
        let msg = err.to_string();
        assert!(
            msg.contains("nope") || msg.contains("not found") || msg.contains("not registered"),
            "unexpected error: {msg}"
        );
    }

    /// P3 #24: extra lookup column that the SQL doesn't reference
    /// is now *accepted* (output schema unchanged after re-plan).
    /// Replaces the prior fail-loud behavior — restart-to-recover is
    /// only required when the drift would actually change downstream.
    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_lookup_accepts_extra_unused_column() {
        let t = DataFusionTransform::new_with_lookups(
            "SELECT s.user_id, u.name \
             FROM source s INNER JOIN users u ON s.user_id = u.id",
            schema_user_id_event(),
            vec![users_lookup()],
        )
        .await
        .expect("construct transform");

        // Add a `country` column the SQL doesn't reference; output
        // shape is (user_id, name) regardless.
        let drifted_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("country", DataType::Utf8, false),
        ]));
        let drifted_batch = RecordBatch::try_new(
            drifted_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(StringArray::from(vec!["us", "uk"])),
            ],
        )
        .unwrap();
        t.refresh_lookup("users", drifted_schema, vec![drifted_batch])
            .await
            .expect("graceful re-plan accepts an unused-column drift");

        // Pipeline keeps serving — the SQL still produces the same
        // (user_id, name) output shape.
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("post-drift transform succeeds");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    /// P3 #24: drift that the SQL *does* surface (e.g. `SELECT u.*`)
    /// must still fail — accepting it would change the output schema
    /// the target was reflected against. Verifies the rollback leaves
    /// the OLD lookup contents queryable so the pipeline keeps
    /// running until the supervisor restarts it.
    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_lookup_rejects_drift_that_changes_output_shape() {
        let t = DataFusionTransform::new_with_lookups(
            "SELECT s.user_id, u.* \
             FROM source s INNER JOIN users u ON s.user_id = u.id",
            schema_user_id_event(),
            vec![users_lookup()],
        )
        .await
        .expect("construct transform");

        let drifted_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("country", DataType::Utf8, false),
        ]));
        let drifted_batch = RecordBatch::try_new(
            drifted_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(StringArray::from(vec!["us", "uk"])),
            ],
        )
        .unwrap();
        let err = t
            .refresh_lookup("users", drifted_schema, vec![drifted_batch])
            .await
            .expect_err("output-changing drift rejected");
        let msg = err.to_string();
        assert!(msg.contains("drift"), "got: {msg}");
        assert!(
            msg.contains("output schema") || msg.contains("fields"),
            "error should explain the output-shape change: {msg}"
        );

        // Rollback: the OLD lookup is still registered, transform
        // still works against the original-shape SQL.
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("post-rollback transform still succeeds");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    /// P3 #24: type change on a join-key column that DataFusion can
    /// absorb (Int32 → Int64 with implicit cast) is now accepted —
    /// the output schema is unaffected since the SELECT only
    /// references `s.user_id` (still Int32) and `u.name` (still Utf8).
    /// The graceful re-plan path validates this end-to-end.
    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_lookup_accepts_join_key_type_widening() {
        let t = DataFusionTransform::new_with_lookups(
            "SELECT s.user_id, u.name \
             FROM source s INNER JOIN users u ON s.user_id = u.id",
            schema_user_id_event(),
            vec![users_lookup()],
        )
        .await
        .expect("construct transform");

        let drifted_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let drifted_batch = RecordBatch::try_new(
            drifted_schema.clone(),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1_i64, 2_i64])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();
        t.refresh_lookup("users", drifted_schema, vec![drifted_batch])
            .await
            .expect("graceful re-plan accepts a join-key type widening");

        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("post-drift transform succeeds");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    /// P3 #24: type change on a column the SELECT *does* surface
    /// changes the output schema — must still reject. Verifies the
    /// rollback leaves the OLD lookup queryable so the pipeline
    /// keeps serving until the supervisor restarts it.
    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_lookup_rejects_type_change_on_selected_column() {
        let t = DataFusionTransform::new_with_lookups(
            "SELECT s.user_id, u.name \
             FROM source s INNER JOIN users u ON s.user_id = u.id",
            schema_user_id_event(),
            vec![users_lookup()],
        )
        .await
        .expect("construct transform");

        // Original `name` is Utf8; the new shape stores it as a
        // numeric. Output column `u.name` would change from Utf8 to
        // Int64, breaking downstream targets reflected against the
        // original.
        let drifted_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Int64, false),
        ]));
        let drifted_batch = RecordBatch::try_new(
            drifted_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(arrow_array::Int64Array::from(vec![100_i64, 200_i64])),
            ],
        )
        .unwrap();
        let err = t
            .refresh_lookup("users", drifted_schema, vec![drifted_batch])
            .await
            .expect_err("output-changing type drift rejected");
        assert!(err.to_string().contains("drift"));

        // Rollback verified: pipeline keeps serving the original SQL
        // shape against the OLD lookup contents.
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("post-rollback transform still succeeds");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_reserved_source_name_errors() {
        let t = DataFusionTransform::new_with_lookups(
            "SELECT 1 FROM source",
            schema_user_id_event(),
            vec![users_lookup()],
        )
        .await
        .expect("construct transform");
        let err = t
            .refresh_lookup("source", schema_user_id_event(), Vec::new())
            .await
            .expect_err("reserved");
        assert!(err.to_string().contains("source"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_sql_transform_refresh_before_init_errors() {
        let t = LazySqlTransform::new_with_lookups(
            "SELECT s.user_id, u.name FROM source s INNER JOIN users u ON s.user_id = u.id",
            vec![users_lookup()],
        );
        // No transform() call yet → inner not built.
        let users_schema = users_lookup().schema;
        let err = t
            .refresh_lookup("users", users_schema, Vec::new())
            .await
            .expect_err("not initialized");
        assert!(
            err.to_string().to_lowercase().contains("initialized")
                || err.to_string().to_lowercase().contains("first batch"),
            "unexpected: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_sql_transform_refresh_after_init_delegates() {
        let t = LazySqlTransform::new_with_lookups(
            "SELECT s.user_id, u.name FROM source s INNER JOIN users u ON s.user_id = u.id",
            vec![users_lookup()],
        );
        // Initialize inner.
        let _ = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("first batch");
        // Now refresh works.
        let new_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let new_batch = RecordBatch::try_new(
            new_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![99])),
                Arc::new(StringArray::from(vec!["only_carol"])),
            ],
        )
        .unwrap();
        t.refresh_lookup("users", new_schema, vec![new_batch])
            .await
            .expect("refresh");
        // Now only id=99 matches.
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("apply");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "post-refresh, only id=99 in users");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_sql_transform_carries_lookups_through_to_inner() {
        let t = LazySqlTransform::new_with_lookups(
            "SELECT s.user_id, u.name \
             FROM source s INNER JOIN users u ON s.user_id = u.id",
            vec![users_lookup()],
        );
        assert!(!t.is_initialized());
        let out = t
            .transform(batch_events(), &BatchContext::default())
            .await
            .expect("apply");
        assert!(t.is_initialized());
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "lookup is registered on first build");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_sql_transform_builds_on_first_batch() {
        let t = LazySqlTransform::new("SELECT id FROM source WHERE id = 2");
        assert!(!t.is_initialized(), "not built before first call");
        let out = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("apply");
        assert!(t.is_initialized(), "built after first call");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "WHERE id=2 → one row out of two");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_sql_transform_reuses_inner_on_subsequent_calls() {
        let t = LazySqlTransform::new("SELECT id, name FROM source");
        let _ = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("first");
        // Second call must succeed against the same captured schema.
        let out = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("second");
        assert_eq!(out[0].num_rows(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_sql_transform_errors_on_schema_drift() {
        let t = LazySqlTransform::new("SELECT id FROM source");
        let _ = t
            .transform(batch_two_rows(), &BatchContext::default())
            .await
            .expect("first batch sets schema");
        // Different schema on the 2nd batch — the cached inner
        // captures the first schema and rejects mismatches.
        let other_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let other =
            RecordBatch::try_new(other_schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let err = t
            .transform(other, &BatchContext::default())
            .await
            .expect_err("schema drift");
        assert!(err.to_string().contains("schema mismatch"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invalid_sql_errors_at_construction() {
        let err = DataFusionTransform::new("NOT A QUERY", schema_id_name())
            .await
            .expect_err("invalid SQL");
        // Don't pin the exact message — just check the error
        // points at the SQL parse step.
        assert!(err.to_string().to_lowercase().contains("sql"));
    }

    // ----------------------------------------------------------
    // Coverage backfill round 5 — small pure helpers in
    // transform.rs that the existing transform-execution tests
    // don't drive specific arms of.
    // ----------------------------------------------------------

    /// `schemas_equivalent` returns false on every difference
    /// (length / name / data type / nullability) and true on
    /// equality. The drift-check path uses this to decide
    /// whether to surface a schema-mismatch error.
    #[test]
    fn schemas_equivalent_rejects_every_diff() {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        let base = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        // Equal copy → true.
        let equal = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        assert!(schemas_equivalent(&base, &equal));

        // Different field count → false.
        let shorter = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
        assert!(!schemas_equivalent(&base, &shorter));

        // Renamed column → false.
        let renamed = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user_name", DataType::Utf8, true),
        ]);
        assert!(!schemas_equivalent(&base, &renamed));

        // Different data type → false.
        let retyped = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        assert!(!schemas_equivalent(&base, &retyped));

        // Different nullability → false.
        let nullable_id = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        assert!(!schemas_equivalent(&base, &nullable_id));

        // Schema metadata differences are deliberately ignored
        // (lookup-load paths often don't preserve the metadata).
        let mut with_md = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        with_md = with_md.with_metadata(
            [("provenance".to_string(), "lookup_load".to_string())]
                .into_iter()
                .collect(),
        );
        assert!(
            schemas_equivalent(&base, &with_md),
            "schema-metadata diffs are benign for equivalence"
        );
    }

    /// `Debug` on `DataFusionTransform` renders the user-meaningful
    /// fields by hand because `SessionContext` doesn't impl Debug.
    /// Locks the rendered shape so refactors don't accidentally
    /// drop a field.
    #[tokio::test(flavor = "multi_thread")]
    async fn data_fusion_transform_debug_renders_user_fields() {
        let t = DataFusionTransform::new("SELECT id FROM source", schema_id_name())
            .await
            .expect("construct transform");
        let s = format!("{t:?}");
        assert!(s.contains("DataFusionTransform"), "got: {s}");
        assert!(s.contains("sql"), "Debug must include the SQL text");
        assert!(
            s.contains("trivial"),
            "Debug must surface the trivial-projection flag"
        );
    }

    /// `Debug` on `LazySqlTransform` includes the SQL + the
    /// initialized flag + lookup names.
    #[test]
    fn lazy_sql_transform_debug_includes_initialized_flag() {
        let t = LazySqlTransform::new("SELECT id FROM source");
        let s = format!("{t:?}");
        assert!(s.contains("LazySqlTransform"), "got: {s}");
        assert!(s.contains("initialized"));
        // Pre-build, the inner transform isn't constructed — so
        // the rendered "initialized" must be `false`.
        assert!(s.contains("false"), "uninitialized lazy transform: got {s}");
    }

    /// The trivial-projection detector rejects projections whose
    /// inner plan has filters, a LIMIT, or a non-`source` table.
    /// Each disqualifying condition routes through a separate
    /// branch; this test trips each one.
    #[tokio::test(flavor = "multi_thread")]
    async fn trivial_projection_rejects_filtered_or_limited_plans() {
        // Filter clause disqualifies the trivial fast path.
        let with_filter =
            DataFusionTransform::new("SELECT id, name FROM source WHERE id > 0", schema_id_name())
                .await
                .expect("construct");
        assert!(
            !with_filter.is_trivial(),
            "WHERE clause should disqualify the trivial bypass"
        );

        // LIMIT disqualifies.
        let with_limit =
            DataFusionTransform::new("SELECT id, name FROM source LIMIT 5", schema_id_name())
                .await
                .expect("construct");
        assert!(
            !with_limit.is_trivial(),
            "LIMIT should disqualify the trivial bypass"
        );

        // A computed column disqualifies.
        let with_expr = DataFusionTransform::new(
            "SELECT id, name, id + 1 AS id_plus FROM source",
            schema_id_name(),
        )
        .await
        .expect("construct");
        assert!(
            !with_expr.is_trivial(),
            "computed columns should disqualify the trivial bypass"
        );
    }
}
