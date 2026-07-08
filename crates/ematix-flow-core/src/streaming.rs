//! Phase 36g: Long-running streaming pipeline + graceful shutdown.
//!
//! The runtime primitive that ties the framework's "consume from a
//! streaming source, write to any target" pattern into a single
//! supervised loop:
//!
//! ```text
//!   loop:
//!     batches = source.read_arrow_stream(query)   # bounded by source's batch config
//!     if batches non-empty:
//!         target.write_arrow_stream(batches, Append)
//!         source.commit_offsets()                  # at-least-once
//!     else:
//!         brief idle pause
//!     check shutdown signal between iterations
//! ```
//!
//! Cancellation is **between batches only**. Mid-poll cancellation
//! would discard a batch we've already read from the broker; the
//! offsets aren't committed yet so it'd be re-delivered on the next
//! session, but we'd also lose the in-flight rows. Between-batch
//! cancellation is cleaner: any read batch is fully written +
//! committed before we exit.
//!
//! The actual `flow consume <pipeline>` CLI binary is a thin wrapper
//! around `StreamingPipeline::run` (lands in a CLI sub-phase). The
//! supervisor (process pool, exponential-backoff restart) wraps this
//! at an even higher layer and is also CLI-side.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use futures_util::TryStreamExt;
use prometheus::{GaugeVec, IntCounter, Registry};
use tokio::sync::watch;

use crate::backend::{ArrowBatchStream, Backend, BackendError, TargetTable, WriteMode};
use crate::dlq::{
    DEFAULT_MAX_ATTEMPTS, DeadLetterStore, DlqError, DlqMeta, DlqRecord, DlqRecordId, DlqSelection,
    DlqStage, DlqStoreMode, KafkaTopicDlq, ReplayOptions, ReplayReport, TableDlq, truncate_error,
};
use crate::kafka_backend::KafkaBackend;
use crate::state_store::{RecoveredState, StateStore};
use crate::transform::{BatchContext, BatchTransform};

/// Phase 39.4 PR 1: per-pipeline watermark configuration.
///
/// `None` on `StreamingPipelineConfig::watermark` (the historical
/// default) disables watermark machinery entirely — the pipeline
/// neither extracts `_event_ts` nor tracks per-source state, so
/// non-windowed pipelines pay zero overhead.
///
/// `Some(WatermarkConfig { ... })` opts in. The pipeline scans every
/// batch's `_event_ts` column, advances per-source watermarks, and
/// computes a global watermark as `min` over non-idle sources.
/// `BatchContext::global_wm` carries that value to the transform.
#[derive(Debug, Clone)]
pub struct WatermarkConfig {
    /// Per-source watermark slack: `wm_i = max(_event_ts_i) − lateness_ms`.
    /// Set to 0 for "I trust event-time order"; raise for sources that
    /// produce out-of-order data.
    pub lateness_ms: u64,
    /// A source is excluded from the multi-source `min(wm_i)` after
    /// going this long without producing a batch. Default: 60 s.
    pub source_idleness_ms: u64,
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            lateness_ms: 0,
            source_idleness_ms: 60_000,
        }
    }
}

/// Per-iteration mutable state for watermark tracking. Index aligns
/// with `StreamingPipeline::sources`.
#[derive(Debug)]
struct WatermarkState {
    /// Max `_event_ts` observed per source (microseconds since
    /// Unix epoch, UTC). `None` until the source produces its first
    /// non-empty batch carrying `_event_ts`.
    max_event_ts: Vec<Option<i64>>,
    /// Wall-clock timestamp of the last non-empty batch from each
    /// source. Drives idleness detection. `None` until first batch.
    last_arrival: Vec<Option<Instant>>,
}

impl WatermarkState {
    fn new(n_sources: usize) -> Self {
        Self {
            max_event_ts: vec![None; n_sources],
            last_arrival: vec![None; n_sources],
        }
    }
}

/// Extract the maximum value from a batch's `_event_ts` column.
/// Phase 39.5a P1.8: tokio task that wakes every `interval` and
/// commits any pending session/join state diff plus current source
/// offsets to `store`. Exits cleanly on shutdown.
///
/// This is the floor that bounds replay-on-restart for pipelines
/// that hold dirty session state without emitting (long retention
/// budget on a sparse source). Active pipelines see commits via
/// the per-emit path in `StreamingPipeline::run`'s
/// `finalize_iteration`; the ticker is additive.
async fn checkpoint_loop(
    store: Arc<dyn StateStore>,
    pipeline_name: String,
    transform: Option<Arc<dyn BatchTransform>>,
    sources: Vec<(Arc<dyn Backend>, String)>,
    interval: Duration,
    shutdown: ShutdownSignal,
) {
    let mut ticker = tokio::time::interval(interval);
    // Skip the immediate first tick — we just started.
    ticker.tick().await;
    // If the pipeline falls behind for any reason, MissedTickBehavior
    // = Delay drops missed ticks rather than firing them in a burst.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(e) = checkpoint_once(
                    &store,
                    &pipeline_name,
                    transform.as_ref(),
                    &sources,
                ).await {
                    // Don't propagate — the per-emit commits are
                    // still happening in the main run loop, so a
                    // ticker miss isn't fatal. Log + continue.
                    tracing::warn!(
                        pipeline = %pipeline_name,
                        error = %e,
                        "periodic checkpoint commit failed; main loop continues"
                    );
                }
            }
            _ = shutdown.wait() => break,
        }
    }
}

/// One iteration of the checkpoint loop: drain transform diff +
/// snapshot offsets + commit. No-op when nothing changed.
async fn checkpoint_once(
    store: &Arc<dyn StateStore>,
    pipeline_name: &str,
    transform: Option<&Arc<dyn BatchTransform>>,
    sources: &[(Arc<dyn Backend>, String)],
) -> Result<(), BackendError> {
    use crate::state_store::CommitSnapshot;
    let (state_upserts, state_deletes) = match transform {
        Some(t) => t.take_state_commit().await?,
        None => (Vec::new(), Vec::new()),
    };
    let mut offsets: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    for (backend, query) in sources {
        if let Some(bytes) = backend.offset_snapshot().await? {
            offsets.insert(query.clone(), bytes);
        }
    }
    if state_upserts.is_empty() && state_deletes.is_empty() && offsets.is_empty() {
        return Ok(());
    }
    store
        .commit(
            pipeline_name,
            CommitSnapshot {
                state_upserts,
                state_deletes,
                offsets,
                state_version: crate::session_blob::STATE_BLOB_VERSION,
            },
        )
        .await
}

/// Returns `None` if the column is missing, has the wrong type, or
/// is entirely null. Tolerant of non-`Microsecond` timestamps in
/// PR 1 (silently skipped); PR 2's `WindowedAggregateTransform`
/// will validate strictly.
fn batch_max_event_ts(batch: &RecordBatch) -> Option<i64> {
    let idx = batch.schema().index_of("_event_ts").ok()?;
    let arr = batch.column(idx);
    if !matches!(
        arr.data_type(),
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _)
    ) {
        return None;
    }
    let ts = arr.as_primitive_opt::<TimestampMicrosecondType>()?;
    let mut max_v: Option<i64> = None;
    for i in 0..ts.len() {
        if !ts.is_null(i) {
            let v = ts.value(i);
            max_v = Some(max_v.map_or(v, |m| m.max(v)));
        }
    }
    max_v
}

/// Prometheus counters for a single streaming pipeline. The
/// counters live on a private Registry so multiple pipelines in one
/// process can have independent label namespaces; the supervisor
/// can serve them through the same `/metrics` endpoint by federating
/// or by selecting one pipeline at a time.
#[derive(Debug, Clone)]
pub struct StreamingPipelineMetricsCounters {
    pub registry: Registry,
    pub rows_consumed: IntCounter,
    pub rows_written: IntCounter,
    pub batches: IntCounter,
    pub errors: IntCounter,
    pub dlq_writes: IntCounter,
    pub idle_iterations: IntCounter,
    /// Phase 39.4 PR 1: per-source and global watermark, in seconds
    /// since the Unix epoch. The `source` label is each source's
    /// query string (Kafka topic / Pub/Sub subscription / etc.);
    /// the special label value `"_global"` carries the
    /// `min(wm_i over non-idle)` aggregate.
    pub watermark_seconds: GaugeVec,
    /// Phase Δ PR 5.5: CDC apply path counters. Zero on pipelines
    /// without `[transform.cdc]` configured.
    pub cdc_creates: IntCounter,
    pub cdc_updates: IntCounter,
    pub cdc_deletes: IntCounter,
    /// Tombstones + envelope-parse failures.
    pub cdc_skipped: IntCounter,
    /// Events rejected by the per-PK last-seen-ts gate (PR 4) —
    /// i.e. Kafka redeliveries that the executor already applied.
    pub cdc_idempotent_skipped: IntCounter,
}

impl StreamingPipelineMetricsCounters {
    /// Build a fresh registry + counter set. `pipeline_name`
    /// becomes a label on every metric, letting downstream scrapers
    /// distinguish multiple pipelines that share a `/metrics`
    /// endpoint.
    pub fn new(pipeline_name: &str) -> Self {
        use prometheus::Opts;
        let registry = Registry::new();
        let mk = |name: &str, help: &str| -> IntCounter {
            let counter =
                IntCounter::with_opts(Opts::new(name, help).const_label("pipeline", pipeline_name))
                    .expect("IntCounter creation");
            registry
                .register(Box::new(counter.clone()))
                .expect("counter register");
            counter
        };
        let watermark_seconds = GaugeVec::new(
            Opts::new(
                "ematix_streaming_watermark_seconds",
                "Per-source and global watermark, in seconds since Unix epoch (UTC).",
            )
            .const_label("pipeline", pipeline_name),
            &["source"],
        )
        .expect("GaugeVec creation");
        registry
            .register(Box::new(watermark_seconds.clone()))
            .expect("watermark_seconds register");
        Self {
            rows_consumed: mk(
                "ematix_streaming_rows_consumed_total",
                "Total Arrow rows consumed from the source.",
            ),
            rows_written: mk(
                "ematix_streaming_rows_written_total",
                "Total Arrow rows written to the target.",
            ),
            batches: mk(
                "ematix_streaming_batches_total",
                "Total non-empty read→write batches completed.",
            ),
            errors: mk(
                "ematix_streaming_errors_total",
                "Total irrecoverable pipeline errors observed.",
            ),
            dlq_writes: mk(
                "ematix_streaming_dlq_writes_total",
                "Total rows appended to the dead-letter store (topic or table) \
                 after a transform/write failure or late-data eviction.",
            ),
            idle_iterations: mk(
                "ematix_streaming_idle_iterations_total",
                "Total idle-batch iterations (source returned zero rows).",
            ),
            cdc_creates: mk(
                "ematix_streaming_cdc_creates_total",
                "Phase Δ: CDC INSERT/UPSERT applies (Create + Read ops).",
            ),
            cdc_updates: mk(
                "ematix_streaming_cdc_updates_total",
                "Phase Δ: CDC UPDATE applies.",
            ),
            cdc_deletes: mk(
                "ematix_streaming_cdc_deletes_total",
                "Phase Δ: CDC DELETE / soft-delete applies.",
            ),
            cdc_skipped: mk(
                "ematix_streaming_cdc_skipped_total",
                "Phase Δ: CDC tombstones + envelope-parse failures.",
            ),
            cdc_idempotent_skipped: mk(
                "ematix_streaming_cdc_idempotent_skipped_total",
                "Phase Δ: CDC events filtered by the per-PK last-seen-ts gate (Kafka redeliveries).",
            ),
            watermark_seconds,
            registry,
        }
    }

    /// Render the registry as Prometheus text exposition format.
    /// Suitable for the body of a `GET /metrics` response.
    pub fn render(&self) -> Result<String, BackendError> {
        use prometheus::Encoder;
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder
            .encode(&metric_families, &mut buf)
            .map_err(|e| BackendError::Other(format!("prometheus encode: {e}")))?;
        String::from_utf8(buf).map_err(|e| BackendError::Other(format!("prometheus utf8: {e}")))
    }
}

/// Configuration for a streaming pipeline run.
#[derive(Debug, Clone)]
pub struct StreamingPipelineConfig {
    /// Query handed to `source.read_arrow_stream`. For Kafka this is
    /// the topic name. For other streaming sources (Phase 37: Pub/Sub,
    /// Kinesis) it's the subscription / stream name.
    pub source_query: String,
    /// Target table to write each batch into.
    pub target: TargetTable,
    /// Write mode. Almost always `Append` for streaming —
    /// `Truncate` per batch would be pathological. Kept on the
    /// config so users can override for unusual setups.
    pub mode: WriteMode,
    /// Sleep this long when the source returns an empty batch
    /// (idle topic). Keeps the loop from hot-spinning.
    pub idle_pause_ms: u64,
    /// Pipeline name — used for logs / metrics labels.
    pub pipeline_name: String,
    /// When `Some`, failed-batch rows get routed to this Kafka
    /// topic (via [`crate::dlq::KafkaTopicDlq`] — payload format
    /// preserved, failure metadata in `emat-dlq-*` headers) instead
    /// of bubbling the error up. Requires a Kafka source; with a
    /// non-Kafka source the DLQ resolution (DLQ Phase 1) falls back
    /// to the table store instead of silently dropping.
    ///
    /// At-least-once: source offsets are committed *after* the DLQ
    /// append ack lands, so a crash mid-DLQ-write means the
    /// original messages are re-delivered, not lost.
    pub dead_letter_topic: Option<String>,
    /// DLQ Phase 1: explicit dead-letter store override. When set it
    /// wins over every other resolution rule (topic / state-store
    /// family / in-memory fallback) — see
    /// [`StreamingPipeline::resolve_dlq_store`]. Zero happy-path
    /// cost: the store is only touched on error paths.
    pub dead_letter_store: Option<Arc<dyn DeadLetterStore>>,
    /// Π.4b-1: optional per-batch SQL transform applied between
    /// `source.read_arrow_stream` and `target.write_arrow_stream`.
    /// `None` is the historical fast path — the pipeline forwards
    /// batches unchanged (zero overhead). `Some(t)` runs every
    /// non-empty input batch through `t.transform`; each call may
    /// produce 0..N output batches that are then forwarded to the
    /// target(s).
    pub transform: Option<Arc<dyn BatchTransform>>,
    /// Phase 39.4 PR 1: opt-in watermark machinery. `None` (default)
    /// disables `_event_ts` extraction and per-source tracking
    /// entirely — non-windowed pipelines pay zero overhead. `Some`
    /// enables the per-iteration scan and feeds the resulting
    /// `global_wm` into [`crate::transform::BatchContext::global_wm`].
    pub watermark: Option<WatermarkConfig>,
    /// Phase 39.5a PR 3: optional `StateStore` for durable per-emit
    /// session state + offset commits. When set, the pipeline:
    /// - Calls [`StreamingPipeline::load_state`] at startup to
    ///   restore committed offsets and rehydrate the session
    ///   transform's per-key state map.
    /// - After each non-empty target write, drains the transform's
    ///   pending state diff plus every source's offset snapshot
    ///   and commits them in a single [`StateStore::commit`] call.
    ///   The atomic commit replaces source-side
    ///   [`Backend::commit_offsets`] (which becomes advisory and is
    ///   skipped for sources whose offsets are now in the store).
    pub state_store: Option<Arc<dyn StateStore>>,
    /// Phase 39.5a P2.15: how to handle errors that bubble out of
    /// `transform.transform(batch, ctx)`. Defaults to `Fail` —
    /// preserves the historical "propagate to supervisor on
    /// transform error" behavior.
    pub transform_on_error: TransformErrorPolicy,
    /// Phase 39.5a P1.8: periodic dirty-only checkpoint cadence.
    /// When set, [`StreamingPipeline::run`] spawns a tokio ticker
    /// that calls [`StreamingPipeline::commit_state`] every
    /// `Duration::from_millis(N)` regardless of emit activity. Bounds
    /// replay-on-restart for idle-but-dirty pipelines (long
    /// retention windows on a sparse source). `None` disables the
    /// floor; per-emit commits still drive durability for active
    /// pipelines.
    pub checkpoint_interval_ms: Option<u64>,
    /// Phase Δ PR 5.5: when `Some`, every batch is interpreted as
    /// CDC envelopes (per [`crate::cdc::CdcConfig`]) and applied
    /// via [`Backend::run_cdc`] instead of the universal
    /// `write_arrow_stream` path. Mutually exclusive with
    /// `transform` (CDC is itself a *transformation* of input
    /// shape — windowing or SQL-projecting the envelope before
    /// applying it would break the per-event semantics).
    pub cdc: Option<crate::cdc::CdcConfig>,
    /// Δ.X1.2: per-target primary-key column lists. Indexed in
    /// parallel with the pipeline's `targets` vec — `target_primary_keys[i]`
    /// declares the PK columns for `targets[i]`. When the entry
    /// is non-empty, the streaming runtime augments
    /// [`Backend::reflect_table_spec`]'s output by marking those
    /// columns as PK on the spec it hands to
    /// [`Backend::run_cdc`]. When the entry is empty, the
    /// reflected spec is used verbatim (Postgres path —
    /// `information_schema` already surfaces PK constraints).
    ///
    /// Required for backends that can't reflect PK info natively
    /// (Delta tables don't carry PK constraints; the Δ.X1 PR 1
    /// reflect impl reports `primary_key = false` on every
    /// column). The user supplies it via the
    /// `[target.table].primary_key` TOML field, the
    /// `@ematix.table(primary_key=...)` decorator, or
    /// `StreamingPipelineConfig::with_target_primary_keys`.
    pub target_primary_keys: Vec<Vec<String>>,
    /// DLQ Phase 4: operator-selected dead-letter store family.
    /// `Auto` (default) keeps the Phase 1 resolution order; `Topic`
    /// / `Table` demand one family and hard-error when the pipeline
    /// can't satisfy it. See [`DlqStoreMode`].
    pub dlq_store: DlqStoreMode,
    /// DLQ Phase 4: default replay attempt budget for this
    /// pipeline. Seeds [`ReplayOptions::max_attempts`] when the
    /// operator surface (HTTP API / CLI) doesn't override per run.
    pub dlq_max_attempts: u32,
}

/// Phase 39.5a P2.15: per-batch transform-error handling.
///
/// `transform()` errors propagate from DataFusion (CAST failures,
/// division by zero, etc.) at batch granularity — DataFusion
/// executes per-batch, so a single bad row fails the whole batch.
/// Operators choose what happens to the batch under error:
///
/// - `Fail` (default) — propagate the error. The supervisor (when
///   `--restart-on-error` is set) restarts the pipeline; otherwise
///   the run exits non-zero. Historical behavior; preserves data
///   integrity strictly.
/// - `Drop` — log the error, increment `ematix_streaming_errors_total`,
///   skip the batch. Source offsets advance as if the batch had
///   produced no rows. Use when transient bad data is expected
///   (best-effort transform layer).
/// - `Dlq` — log + counter + route the original (pre-transform)
///   input batch to the configured `dead_letter_topic`. Requires
///   `dead_letter_topic` set + a Kafka source (same constraint as
///   the per-batch-write-failure DLQ path). Operators get
///   visibility into bad rows without losing them.
///
/// Stateful transforms (windows / sessions / joins) may leave
/// partially-mutated state on transform error. `Drop` and `Dlq`
/// don't roll that back — use `Fail` + supervisor restart for
/// strict integrity in stateful pipelines.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransformErrorPolicy {
    #[default]
    Fail,
    Drop,
    Dlq,
}

impl StreamingPipelineConfig {
    /// Sensible defaults for production: 500ms idle pause, Append
    /// mode. Caller still supplies source_query, target, and
    /// pipeline_name.
    pub fn new(
        source_query: impl Into<String>,
        target: TargetTable,
        pipeline_name: impl Into<String>,
    ) -> Self {
        Self {
            source_query: source_query.into(),
            target,
            mode: WriteMode::Append,
            idle_pause_ms: 500,
            pipeline_name: pipeline_name.into(),
            dead_letter_topic: None,
            dead_letter_store: None,
            transform: None,
            watermark: None,
            state_store: None,
            checkpoint_interval_ms: None,
            transform_on_error: TransformErrorPolicy::Fail,
            cdc: None,
            target_primary_keys: Vec::new(),
            dlq_store: DlqStoreMode::default(),
            dlq_max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }

    /// Builder-style: opt the pipeline into CDC apply mode. Each
    /// batch is parsed as `cdc.envelope`-shaped change events and
    /// applied to every target via [`Backend::run_cdc`]. Phase Δ
    /// PR 5.5.
    pub fn with_cdc(mut self, cdc: crate::cdc::CdcConfig) -> Self {
        self.cdc = Some(cdc);
        self
    }

    /// Builder-style (Δ.X1.2): per-target user-declared primary
    /// keys. The streaming runtime applies these atop the
    /// reflected spec before calling
    /// [`Backend::run_cdc`] — required for Delta and any other
    /// backend that can't surface PK info via reflection.
    /// Indexed in parallel with the pipeline's `targets`.
    pub fn with_target_primary_keys(mut self, pks: Vec<Vec<String>>) -> Self {
        self.target_primary_keys = pks;
        self
    }

    /// Builder-style: install a transform-error policy. Defaults
    /// to `Fail`.
    pub fn with_transform_on_error(mut self, policy: TransformErrorPolicy) -> Self {
        self.transform_on_error = policy;
        self
    }

    /// Builder-style: install a `StateStore` for durable per-emit
    /// session state + offset commits (Phase 39.5a PR 3).
    pub fn with_state_store(mut self, store: Arc<dyn StateStore>) -> Self {
        self.state_store = Some(store);
        self
    }

    /// Builder-style: enable the periodic dirty-only checkpoint
    /// ticker at `interval_ms` cadence. Pipelines with active
    /// per-emit commits don't need this; the ticker matters only
    /// for sessions / joins that hold dirty state without
    /// emitting (long retention budget on a sparse source).
    pub fn with_checkpoint_interval_ms(mut self, interval_ms: u64) -> Self {
        self.checkpoint_interval_ms = Some(interval_ms);
        self
    }

    /// Builder-style: opt into DLQ routing on target write failure.
    pub fn with_dead_letter_topic(mut self, topic: impl Into<String>) -> Self {
        self.dead_letter_topic = Some(topic.into());
        self
    }

    /// Builder-style (DLQ Phase 1): install an explicit
    /// [`DeadLetterStore`]. Overrides the automatic resolution
    /// (Kafka topic / state-store family / in-memory fallback).
    pub fn with_dead_letter_store(mut self, store: Arc<dyn DeadLetterStore>) -> Self {
        self.dead_letter_store = Some(store);
        self
    }

    /// Builder-style (DLQ Phase 4): select the dead-letter store
    /// family. See [`DlqStoreMode`] for the resolution semantics.
    pub fn with_dlq_store(mut self, mode: DlqStoreMode) -> Self {
        self.dlq_store = mode;
        self
    }

    /// Builder-style (DLQ Phase 4): per-pipeline default replay
    /// attempt budget (poison records park past it).
    pub fn with_dlq_max_attempts(mut self, max_attempts: u32) -> Self {
        self.dlq_max_attempts = max_attempts;
        self
    }

    /// Builder-style: install a per-batch transform.
    pub fn with_transform(mut self, transform: Arc<dyn BatchTransform>) -> Self {
        self.transform = Some(transform);
        self
    }

    /// Builder-style: enable watermark machinery with the given
    /// configuration. Required for Phase 39.4 windowed transforms;
    /// no-op (and zero-overhead) for filter / project / cast.
    pub fn with_watermark(mut self, watermark: WatermarkConfig) -> Self {
        self.watermark = Some(watermark);
        self
    }
}

/// Aggregated metrics returned when `StreamingPipeline::run` exits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamingPipelineMetrics {
    /// Number of source rows that ended up written to the target.
    pub total_rows: u64,
    /// Number of read→write→commit iterations completed (non-empty
    /// batches only — empty-batch iterations are not counted).
    pub iterations: u64,
    /// Did we exit because shutdown was triggered (vs the source
    /// returning an irrecoverable error)?
    pub shutdown_triggered: bool,
}

/// Cooperative shutdown signal. Cheap to clone — internally a tokio
/// watch channel. Use `trigger()` from the controlling task; the
/// pipeline's `run()` checks `is_triggered()` between iterations and
/// awaits `wait()` during idle pauses to exit promptly.
#[derive(Debug, Clone)]
pub struct ShutdownSignal {
    rx: watch::Receiver<bool>,
}

/// Sender side of a `ShutdownSignal`. Trigger once to ask the
/// pipeline to drain and exit; subsequent triggers are idempotent.
#[derive(Debug)]
pub struct ShutdownTrigger {
    tx: watch::Sender<bool>,
}

impl ShutdownTrigger {
    pub fn trigger(&self) {
        let _ = self.tx.send(true);
    }
}

impl ShutdownSignal {
    /// Construct a fresh (signal, trigger) pair. The signal can be
    /// cloned and handed to multiple consumers; the trigger is held
    /// by the controlling task (or the SIGTERM handler).
    pub fn new() -> (Self, ShutdownTrigger) {
        let (tx, rx) = watch::channel(false);
        (Self { rx }, ShutdownTrigger { tx })
    }

    /// Has shutdown been triggered? Cheap, sync.
    pub fn is_triggered(&self) -> bool {
        *self.rx.borrow()
    }

    /// Resolves when shutdown is triggered. Use inside `tokio::select!`
    /// to break out of an idle pause early.
    pub async fn wait(&self) {
        let mut rx = self.rx.clone();
        // Loop until we observe `true` (the channel may yield the
        // initial `false` once before we see a real change).
        loop {
            if *rx.borrow() {
                return;
            }
            if rx.changed().await.is_err() {
                // Sender dropped → treat as triggered (caller is gone).
                return;
            }
        }
    }
}

/// A long-running source→target pipeline runner. Holds Arcs so
/// `run()` can be `&self` and the runner can be shared safely
/// across threads (e.g. the CLI's supervisor lives in one task,
/// the SIGTERM handler in another).
pub struct StreamingPipeline {
    /// Π.4b-2: each pipeline iteration reads concurrently from
    /// every `(backend, query)` pair here, concatenates the
    /// resulting batches (UNION ALL), then forwards them through
    /// the optional transform and to every target. A 1-element
    /// Vec is the historical single-source shape.
    pub sources: Vec<(Arc<dyn Backend>, String)>,
    /// Π.4a: each pipeline iteration fans the source's batches out
    /// to **every** entry here. A 1-element Vec is the historical
    /// single-target shape; >1 entry is multi-target fan-out.
    pub targets: Vec<(Arc<dyn Backend>, TargetTable)>,
    pub config: StreamingPipelineConfig,
    pub metrics: StreamingPipelineMetricsCounters,
    /// Phase 39.4 PR 1: per-iteration watermark state. Allocated
    /// to `sources.len()` slots regardless of `config.watermark`
    /// so the field is always indexable; updates are skipped when
    /// `config.watermark.is_none()`.
    watermark_state: Mutex<WatermarkState>,
    /// Phase Δ PR 5.5: cached reflected `TableSpec` per target,
    /// indexed parallel to `targets`. Populated lazily on the
    /// first non-empty CDC batch via [`Backend::reflect_table_spec`]
    /// so non-CDC pipelines never pay the reflection round-trip.
    /// Empty when `config.cdc.is_none()`.
    cdc_target_specs: tokio::sync::OnceCell<Vec<crate::types::TableSpec>>,
    /// DLQ Phase 1: lazily-resolved dead-letter store. Populated on
    /// the FIRST dead-letter emission (never on the happy path — a
    /// pipeline where nothing fails performs zero DLQ work) via
    /// [`Self::resolve_dlq_store`].
    dlq_store: tokio::sync::OnceCell<Arc<dyn DeadLetterStore>>,
    /// DLQ Phase 2: lazily-resolved TABLE store poison records park
    /// into when the pipeline's own store cannot express `park` (a
    /// Kafka topic) and [`ReplayOptions::park_store`] wasn't given.
    /// Cached so successive replays park into the SAME store (the
    /// in-memory fallback would otherwise drop parked records
    /// between calls). See [`Self::resolve_park_fallback_store`].
    park_fallback_store: tokio::sync::OnceCell<Arc<dyn DeadLetterStore>>,
}

impl StreamingPipeline {
    pub fn new(
        source: Arc<dyn Backend>,
        targets: Vec<(Arc<dyn Backend>, TargetTable)>,
        config: StreamingPipelineConfig,
    ) -> Self {
        let metrics = StreamingPipelineMetricsCounters::new(&config.pipeline_name);
        let sources = vec![(source, config.source_query.clone())];
        let watermark_state = Mutex::new(WatermarkState::new(sources.len()));
        Self {
            sources,
            targets,
            config,
            metrics,
            watermark_state,
            cdc_target_specs: tokio::sync::OnceCell::new(),
            dlq_store: tokio::sync::OnceCell::new(),
            park_fallback_store: tokio::sync::OnceCell::new(),
        }
    }

    /// Single-target convenience: pulls the table from
    /// `config.target` and wraps it in a one-element targets Vec.
    /// Lets v0.1-shaped callers keep their `(source, target,
    /// config)` shape without manually building the Vec.
    pub fn new_single(
        source: Arc<dyn Backend>,
        target: Arc<dyn Backend>,
        config: StreamingPipelineConfig,
    ) -> Self {
        let table = config.target.clone();
        Self::new(source, vec![(target, table)], config)
    }

    /// Π.4b-2: multi-source fan-in. Each `(backend, query)` pair is
    /// drained per iteration; their batches are concatenated
    /// (UNION ALL semantics — schemas must be compatible at the
    /// target boundary) before transform + target fan-out. The
    /// `config.source_query` field is unused on this path; per-
    /// source queries come from the `sources` vec.
    ///
    /// **DLQ caveat:** the existing DLQ path uses the first source
    /// backend to produce dead-letter messages — only meaningful
    /// when source 0 is Kafka. Cross-source DLQ routing would need
    /// a separate per-source policy and isn't built yet.
    pub fn new_multi_source(
        sources: Vec<(Arc<dyn Backend>, String)>,
        targets: Vec<(Arc<dyn Backend>, TargetTable)>,
        config: StreamingPipelineConfig,
    ) -> Self {
        let metrics = StreamingPipelineMetricsCounters::new(&config.pipeline_name);
        Self::new_multi_source_with_metrics(sources, targets, config, metrics)
    }

    /// Phase 39.4 PR 2b: like [`Self::new_multi_source`] but accepts
    /// a pre-built [`StreamingPipelineMetricsCounters`]. Use this
    /// when the caller (typically the CLI) needs to register
    /// additional metrics — e.g. [`crate::windowed::WindowedMetrics`]
    /// for a windowed transform — into the pipeline's existing
    /// Prometheus registry *before* the pipeline starts running.
    pub fn new_multi_source_with_metrics(
        sources: Vec<(Arc<dyn Backend>, String)>,
        targets: Vec<(Arc<dyn Backend>, TargetTable)>,
        config: StreamingPipelineConfig,
        metrics: StreamingPipelineMetricsCounters,
    ) -> Self {
        let watermark_state = Mutex::new(WatermarkState::new(sources.len()));
        Self {
            sources,
            targets,
            config,
            metrics,
            watermark_state,
            cdc_target_specs: tokio::sync::OnceCell::new(),
            dlq_store: tokio::sync::OnceCell::new(),
            park_fallback_store: tokio::sync::OnceCell::new(),
        }
    }

    /// Borrow the Prometheus registry — for serving via a
    /// `/metrics` HTTP endpoint or federating into a process-wide
    /// scraper.
    pub fn metrics_registry(&self) -> &Registry {
        &self.metrics.registry
    }

    /// Phase Δ PR 5.5: lazy reflect each target's column set
    /// once. Called from the CDC dispatch branch on the first
    /// non-empty batch; subsequent batches reuse the cached
    /// `Vec<TableSpec>`. Order matches `self.targets`.
    ///
    /// Δ.X1.2: after reflection, augment each spec with the
    /// user-declared `target_primary_keys[i]` (when set) — Delta
    /// and other backends that can't surface PK info natively
    /// rely on this path so `Backend::run_cdc` sees a usable
    /// spec.
    async fn ensure_cdc_target_specs(&self) -> Result<&Vec<crate::types::TableSpec>, BackendError> {
        self.cdc_target_specs
            .get_or_try_init(|| async {
                let mut out = Vec::with_capacity(self.targets.len());
                for (i, (backend, table)) in self.targets.iter().enumerate() {
                    let mut spec = backend.reflect_table_spec(table).await?;
                    if let Some(declared) = self.config.target_primary_keys.get(i)
                        && !declared.is_empty()
                    {
                        // Validate every declared PK column exists
                        // in the reflected schema before mutating —
                        // a typo'd column name should fail loud,
                        // not silently devolve to "no PK detected"
                        // at the run_cdc level.
                        for pk in declared {
                            if !spec.columns.iter().any(|c| &c.name == pk) {
                                return Err(BackendError::Other(format!(
                                    "target_primary_keys declares column '{pk}' \
                                     for {schema}.{name}, but reflect_table_spec \
                                     returned no such column. Check the \
                                     `[target.table].primary_key` TOML field or \
                                     `@ematix.table(primary_key=...)` decorator \
                                     against the live target schema.",
                                    schema = table.schema,
                                    name = table.name,
                                )));
                            }
                        }
                        // Apply: any column whose name is in the
                        // declared list flips primary_key=true,
                        // overriding whatever reflection produced.
                        for col in spec.columns.iter_mut() {
                            if declared.iter().any(|n| n == &col.name) {
                                col.primary_key = true;
                            }
                        }
                    }
                    out.push(spec);
                }
                Ok::<_, BackendError>(out)
            })
            .await
    }

    /// Phase 39.5a: load committed state for `config.pipeline_name`
    /// from `store` and apply per-source [`Backend::seek_to`]. Call
    /// once before [`Self::run`].
    ///
    /// Returns the loaded [`RecoveredState`] so the caller can also
    /// thread `state_by_key` into the windowed transform's session
    /// recovery (PR 2/3) — that wiring is not part of PR 1.
    ///
    /// ## Source identity
    ///
    /// Each source's `query` field is its `SourceId`. Stable across
    /// restarts as long as the user doesn't change the topic /
    /// subscription / stream name. Two sources with the same query
    /// in one pipeline collide on offsets — left as a config-load
    /// check for PR 3.
    ///
    /// ## Source backends without `seek_to`
    ///
    /// Sources that override `supports_seek_to() = false` skip the
    /// seek call silently, matching the design-doc rule that
    /// state-persistent pipelines are rejected at config-load when
    /// any source backend lacks `seek_to` (validation lives one
    /// layer up — PR 3 in the CLI).
    pub async fn load_state(&self, store: &dyn StateStore) -> Result<RecoveredState, BackendError> {
        let recovered = store.load(&self.config.pipeline_name).await?;
        for (backend, query) in &self.sources {
            if let Some(bytes) = recovered.offsets.get(query.as_str()) {
                if !backend.supports_seek_to() {
                    return Err(BackendError::Other(format!(
                        "pipeline `{}` has committed state for source `{}` but \
                         that source backend does not support seek_to — refusing \
                         to silently drop the recovered offset",
                        self.config.pipeline_name, query
                    )));
                }
                backend.seek_to(bytes).await?;
            }
        }
        // PR 3: rehydrate the transform's per-key session state
        // from the recovered blobs. No-op for non-stateful
        // transforms (default impl returns Ok(())).
        if let Some(t) = &self.config.transform {
            t.recover_state(&recovered.state_by_key).await?;
        }
        Ok(recovered)
    }

    /// Phase 39.5a PR 3: build a [`CommitSnapshot`] from the
    /// transform's pending session state diff plus every source's
    /// current offset, and commit it atomically to `store`. Called
    /// by [`Self::run`] after each successful target write when
    /// `config.state_store` is set.
    ///
    /// Returns the snapshot's `(state_upserts, state_deletes,
    /// offsets)` count so callers / tests can assert on commit
    /// shape; the transform's dirty/evicted sets are drained even
    /// on success-with-zero-diffs commits (which become no-ops).
    pub async fn commit_state(
        &self,
        store: &dyn StateStore,
    ) -> Result<(usize, usize, usize), BackendError> {
        use crate::state_store::CommitSnapshot;
        let (state_upserts, state_deletes) = match &self.config.transform {
            Some(t) => t.take_state_commit().await?,
            None => (Vec::new(), Vec::new()),
        };

        // Collect each source's current offset bytes. Sources that
        // don't support `offset_snapshot` (default impl returns
        // Ok(None)) drop out — they must use side-channel commits.
        let mut offsets: std::collections::HashMap<String, Vec<u8>> =
            std::collections::HashMap::new();
        for (backend, query) in &self.sources {
            if let Some(bytes) = backend.offset_snapshot().await? {
                offsets.insert(query.clone(), bytes);
            }
        }

        let n_upserts = state_upserts.len();
        let n_deletes = state_deletes.len();
        let n_offsets = offsets.len();
        let snapshot = CommitSnapshot {
            state_upserts,
            state_deletes,
            offsets,
            // PR 3 ships the v=1 layout. PR 2's session state is
            // stable — no migrations registered yet.
            state_version: crate::session_blob::STATE_BLOB_VERSION,
        };
        store.commit(&self.config.pipeline_name, snapshot).await?;
        Ok((n_upserts, n_deletes, n_offsets))
    }

    /// Phase 39.5a PR 3 + P1.7a: post-iteration durability sweep.
    ///
    /// Two responsibilities:
    /// 1. **`commit_offsets()` per source** — always runs.
    ///    Pub/Sub + RabbitMQ ack messages here (the broker is the
    ///    offset source of truth). Kafka commits to the local
    ///    consumer group; redundant under `state_store` (the store
    ///    has the same offsets) but safe + cheap.
    /// 2. **`commit_state()`** — runs when `state_store` is
    ///    configured. Bundles the transform's pending session diff
    ///    along with offsets from any source whose
    ///    `offset_snapshot()` returns Some bytes (Kafka today;
    ///    Kinesis and object stores in P1.7b).
    ///
    /// Order: commit_state first so a crash between (1) and (2)
    /// leaves the broker uncommitted (rows re-deliver, idempotent
    /// target absorbs). The reverse order would risk acked-but-
    /// uncommitted state on crash → data loss.
    async fn finalize_iteration(&self) -> Result<(), BackendError> {
        if let Some(store) = &self.config.state_store {
            let _ = self.commit_state(store.as_ref()).await?;
        }
        for (backend, _query) in &self.sources {
            backend.commit_offsets().await?;
        }
        Ok(())
    }

    /// Run the pipeline loop until `shutdown` is triggered or the
    /// source returns an error we can't continue past.
    ///
    /// Returns aggregated metrics covering every iteration. Errors
    /// from `read_arrow_stream` / `write_arrow_stream` /
    /// `commit_offsets` propagate immediately — the supervisor
    /// (process-level layer above this) is responsible for restart
    /// policy. That separation keeps the pipeline runner itself
    /// stateless w.r.t. crash recovery: the next invocation re-reads
    /// any uncommitted offsets from the broker.
    pub async fn run(
        &self,
        shutdown: ShutdownSignal,
    ) -> Result<StreamingPipelineMetrics, BackendError> {
        let mut summary = StreamingPipelineMetrics::default();

        // Phase 39.5a P1.8: spawn the periodic dirty-only checkpoint
        // ticker if the pipeline has both a `state_store` and a
        // configured `checkpoint_interval_ms`. The ticker runs
        // independently of the main read/transform/write loop and
        // exits cleanly on shutdown.
        let _checkpoint_handle: Option<tokio::task::JoinHandle<()>> = match (
            self.config.state_store.clone(),
            self.config.checkpoint_interval_ms,
        ) {
            (Some(store), Some(interval_ms)) if interval_ms > 0 => {
                let pipeline_name = self.config.pipeline_name.clone();
                let transform = self.config.transform.clone();
                let sources: Vec<(Arc<dyn Backend>, String)> = self.sources.clone();
                let shutdown = shutdown.clone();
                Some(tokio::spawn(async move {
                    checkpoint_loop(
                        store,
                        pipeline_name,
                        transform,
                        sources,
                        Duration::from_millis(interval_ms),
                        shutdown,
                    )
                    .await;
                }))
            }
            _ => None,
        };

        loop {
            if shutdown.is_triggered() {
                summary.shutdown_triggered = true;
                break;
            }

            // Π.4b-2: read concurrently from every source. join_all
            // (not try_join_all) lets us see all errors — but for
            // simplicity we surface the first one. Sources commit
            // independently below only on the all-success path.
            let reads = self.sources.iter().map(|(backend, query)| async move {
                let stream: ArrowBatchStream = backend.read_arrow_stream(query).await?;
                let batches: Vec<RecordBatch> = stream.try_collect().await?;
                Ok::<_, BackendError>(batches)
            });
            let read_results: Vec<Result<Vec<RecordBatch>, BackendError>> =
                futures_util::future::join_all(reads).await;

            // First failure short-circuits the iteration. Source
            // offsets aren't committed → the failed read's data
            // (and any sibling source's data) is re-delivered on
            // the next iteration.
            //
            // Phase 39.4 PR 1: while we have per-source visibility,
            // also fold each source's batches into per-source
            // `max_event_ts` + `last_arrival` state when watermarks
            // are enabled. The per-source loop is the only place
            // source identity survives — once we extend `batches`
            // with the union, it's lost.
            // Phase 39.5b: keep per-source identity through to the
            // transform call so stream-stream joins can route via
            // `BatchContext::source_id`. `per_source_batches[i] =
            // (source_query, batches)` mirrors `self.sources` order.
            let mut per_source_batches: Vec<(String, Vec<RecordBatch>)> =
                Vec::with_capacity(self.sources.len());
            for (source_idx, r) in read_results.into_iter().enumerate() {
                match r {
                    Ok(source_batches) => {
                        if !source_batches.is_empty() && self.config.watermark.is_some() {
                            let max_ts: Option<i64> =
                                source_batches.iter().filter_map(batch_max_event_ts).max();
                            let mut state = self
                                .watermark_state
                                .lock()
                                .expect("watermark_state mutex poisoned");
                            state.last_arrival[source_idx] = Some(Instant::now());
                            if let Some(ts) = max_ts {
                                state.max_event_ts[source_idx] = Some(
                                    state.max_event_ts[source_idx].map_or(ts, |prev| prev.max(ts)),
                                );
                            }
                        }
                        let query = self.sources[source_idx].1.clone();
                        per_source_batches.push((query, source_batches));
                    }
                    Err(e) => {
                        self.metrics.errors.inc();
                        return Err(e);
                    }
                }
            }
            // Flat view used by the empty-check, row-count, and
            // pre-transform paths that don't need source identity.
            let batches: Vec<RecordBatch> = per_source_batches
                .iter()
                .flat_map(|(_, bs)| bs.iter().cloned())
                .collect();

            // Compute the global watermark + update Prometheus
            // gauges. Cheap when watermark is disabled (early
            // return inside the helper).
            let global_wm = self.compute_and_publish_watermark();

            if batches.is_empty() {
                self.metrics.idle_iterations.inc();

                // Phase 39.4 PR 2: give the transform a chance to
                // emit time-driven output (windowed aggregator
                // flushing windows whose end <= global_wm). Stateless
                // transforms inherit the default no-op and this is
                // a cheap async call.
                if let Some(t) = &self.config.transform {
                    let ctx = BatchContext {
                        global_wm,
                        source_id: None,
                    };
                    let idle_emits = t.on_idle_tick(&ctx).await.inspect_err(|_| {
                        self.metrics.errors.inc();
                    })?;
                    if !idle_emits.is_empty() {
                        let n = self.write_emits_and_commit(idle_emits).await?;
                        summary.total_rows += n;
                        summary.iterations += 1;
                        self.metrics.batches.inc();
                    }
                    // Phase 39.5a P1.6: an idle tick may evict
                    // late rows past their drop deadline; route
                    // any captured ones to DLQ.
                    self.drain_late_data_dlq(t.as_ref()).await?;
                }

                // Idle — sleep, but wake immediately if shutdown
                // fires. Without the select! we'd block for the
                // full pause before noticing the trigger.
                tokio::select! {
                    _ = shutdown.wait() => {
                        summary.shutdown_triggered = true;
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(self.config.idle_pause_ms)) => {}
                }
                continue;
            }

            let n_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            self.metrics.rows_consumed.inc_by(n_rows);

            // Π.4b-1: apply the configured per-batch transform.
            // The `None` arm is bit-identical to today's path —
            // zero-transform pipelines pay no overhead. On error
            // we count it and bubble — the source offsets have
            // not been committed yet, so a crash here just
            // re-delivers the same batch on restart.
            let batches: Vec<RecordBatch> = match &self.config.transform {
                None => batches,
                Some(t) => {
                    let mut out: Vec<RecordBatch> = Vec::with_capacity(batches.len());
                    // Phase 39.5b: dispatch per-source so the
                    // transform's `BatchContext::source_id` carries
                    // the source's `query`. Non-join transforms
                    // ignore the field; joins use it to route to
                    // left/right buffers.
                    for (source_query, src_batches) in &per_source_batches {
                        let ctx = BatchContext {
                            global_wm,
                            source_id: Some(source_query.clone()),
                        };
                        for b in src_batches.iter().cloned() {
                            // Clone before passing — needed for the
                            // Dlq error branch below.
                            let b_clone = b.clone();
                            match t.transform(b, &ctx).await {
                                Ok(produced) => out.extend(produced),
                                Err(e) => {
                                    self.metrics.errors.inc();
                                    match self.config.transform_on_error {
                                        TransformErrorPolicy::Fail => return Err(e),
                                        TransformErrorPolicy::Drop => {
                                            tracing::warn!(
                                                pipeline = %self.config.pipeline_name,
                                                source = %source_query,
                                                rows = b_clone.num_rows(),
                                                error = %e,
                                                "transform error — batch dropped (on_error = drop)"
                                            );
                                        }
                                        TransformErrorPolicy::Dlq => {
                                            tracing::warn!(
                                                pipeline = %self.config.pipeline_name,
                                                source = %source_query,
                                                rows = b_clone.num_rows(),
                                                error = %e,
                                                "transform error — batch routed to DLQ (on_error = dlq)"
                                            );
                                            // DLQ Phase 1: on_error = "dlq" IS
                                            // the opt-in; resolution picks the
                                            // topic / table / in-memory store.
                                            // Pre-Phase-1 this silently
                                            // discarded when no topic was set.
                                            let count = self
                                                .route_batches_to_dlq(
                                                    DlqStage::Transform,
                                                    &e.to_string(),
                                                    source_query,
                                                    vec![b_clone],
                                                )
                                                .await?;
                                            self.metrics.dlq_writes.inc_by(count);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // Phase 39.5a P1.6: drain any DLQ rows the
                    // windowed transform stashed under
                    // `LateDataPolicy::Dlq` and route via the
                    // existing app-level DLQ producer (Kafka source
                    // only — same constraint as the per-batch-
                    // failure DLQ path).
                    self.drain_late_data_dlq(t.as_ref()).await?;
                    out
                }
            };

            // Post-transform row count drives the write-side
            // metrics + summary; a WHERE clause that drops rows
            // shows up here as `rows_consumed > rows_written`.
            let n_rows_out: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

            if n_rows_out == 0 {
                // Phase 39.4 PR 2: post-transform empty (e.g. all
                // rows filtered, or windowed transform isn't ready
                // to emit yet). Skip target write — empty writes
                // are a network round-trip for 0 rows in production
                // and noise in tests. Still finalize the iteration
                // (commit source offsets, or commit state+offsets
                // to the state store under PR 3).
                self.finalize_iteration().await?;
                summary.iterations += 1;
                continue;
            }

            // Phase Δ PR 5.5: when `[transform.cdc]` is configured
            // we route each batch through `Backend::run_cdc`
            // (per-event CDC apply) instead of the universal
            // `write_arrow_stream` append path. Reflects target
            // schemas once on the first non-empty batch — non-CDC
            // pipelines never pay the reflection round-trip.
            let first_err: Option<BackendError> = if let Some(cdc) = self.config.cdc.as_ref() {
                let specs = match self.ensure_cdc_target_specs().await {
                    Ok(specs) => specs,
                    Err(e) => {
                        self.metrics.errors.inc();
                        return Err(e);
                    }
                };
                let runs =
                    self.targets
                        .iter()
                        .zip(specs.iter())
                        .map(|((backend, _table), spec)| {
                            let batches_for_target: Vec<RecordBatch> = batches.clone();
                            let pipeline_name = self.config.pipeline_name.clone();
                            async move {
                                let mut total = crate::backend::CdcRunResult::default();
                                for batch in batches_for_target {
                                    let r =
                                        backend.run_cdc(spec, batch, cdc, &pipeline_name).await?;
                                    total.creates += r.creates;
                                    total.updates += r.updates;
                                    total.deletes += r.deletes;
                                    total.skipped += r.skipped;
                                    total.idempotent_skipped += r.idempotent_skipped;
                                }
                                Ok::<_, BackendError>(total)
                            }
                        });
                let results: Vec<Result<crate::backend::CdcRunResult, BackendError>> =
                    futures_util::future::join_all(runs).await;
                let mut first_err: Option<BackendError> = None;
                for r in results {
                    match r {
                        Ok(t) => {
                            self.metrics.cdc_creates.inc_by(t.creates as u64);
                            self.metrics.cdc_updates.inc_by(t.updates as u64);
                            self.metrics.cdc_deletes.inc_by(t.deletes as u64);
                            self.metrics.cdc_skipped.inc_by(t.skipped as u64);
                            self.metrics
                                .cdc_idempotent_skipped
                                .inc_by(t.idempotent_skipped as u64);
                        }
                        Err(e) if first_err.is_none() => first_err = Some(e),
                        Err(_) => {}
                    }
                }
                first_err
            } else {
                // Π.4a historical path: fan out the batches through
                // the universal Arrow append. Each target gets its
                // own RecordBatch stream cloned from the source's
                // batches. `join_all` (not try_join_all) runs every
                // write to completion so we have full visibility
                // into which targets succeeded — needed for the
                // DLQ branch + for diagnostics on failure.
                let writes = self.targets.iter().map(|(backend, table)| {
                    let batches_for_target: Vec<RecordBatch> = batches.clone();
                    let target_stream: ArrowBatchStream = Box::pin(futures_util::stream::iter(
                        batches_for_target.into_iter().map(Ok),
                    ));
                    let mode = self.config.mode;
                    async move { backend.write_arrow_stream(table, target_stream, mode).await }
                });
                let results: Vec<Result<u64, BackendError>> =
                    futures_util::future::join_all(writes).await;
                results.into_iter().find_map(|r| r.err())
            };

            match first_err {
                None => {
                    // CDC counters above already record per-op
                    // counts; for the universal path this is total
                    // rows written. The two paths share
                    // `rows_written` for the legacy "anything
                    // landed at the target?" gauge.
                    self.metrics.rows_written.inc_by(n_rows_out);
                }
                Some(e) => {
                    self.metrics.errors.inc();
                    // DLQ routing: if a topic or explicit store is
                    // configured, append each row's source-encoded
                    // payload (plus failure metadata) to the
                    // dead-letter store and continue. With
                    // multi-target the DLQ runs when *any* target
                    // failed; targets that already succeeded keep
                    // their writes (data is in those sinks plus the
                    // DLQ — partial-success is the accepted trade
                    // for at-least-once across N sinks). No DLQ
                    // opt-in ⇒ surface the first failure and skip
                    // the offset commit — byte-identical to the
                    // pre-Phase-1 behavior.
                    if self.config.dead_letter_topic.is_some()
                        || self.config.dead_letter_store.is_some()
                    {
                        let source_id = self.sources.first().map(|(_, q)| q.as_str()).unwrap_or("");
                        let dlq_count = self
                            .route_batches_to_dlq(
                                DlqStage::Write,
                                &e.to_string(),
                                source_id,
                                batches,
                            )
                            .await?;
                        self.metrics.dlq_writes.inc_by(dlq_count);
                    } else {
                        return Err(e);
                    }
                }
            }

            // At-least-once commit: only after the target ack lands
            // (or after the DLQ ack on the failure path). PR 3 with
            // `state_store` configured replaces source-side
            // `commit_offsets` with an atomic state+offsets commit
            // to the store; otherwise the historical per-source
            // commit loop applies.
            self.finalize_iteration().await?;

            self.metrics.batches.inc();
            summary.total_rows += n_rows_out;
            summary.iterations += 1;
        }
        Ok(summary)
    }

    /// Phase 39.4 PR 2: fan-out windowed-emit batches to every
    /// target, then commit source offsets. Used by the idle path
    /// when `on_idle_tick` returns time-driven emits. Window emits
    /// don't go to DLQ on target-write failure (per design Q6.2):
    /// failure bubbles, supervisor restart re-replays the source,
    /// the at-least-once + idempotent-target invariant absorbs
    /// duplicates.
    async fn write_emits_and_commit(&self, emits: Vec<RecordBatch>) -> Result<u64, BackendError> {
        let n_rows: u64 = emits.iter().map(|b| b.num_rows() as u64).sum();
        if n_rows == 0 {
            return Ok(0);
        }
        let writes = self.targets.iter().map(|(backend, table)| {
            let emits_for_target: Vec<RecordBatch> = emits.clone();
            let target_stream: ArrowBatchStream = Box::pin(futures_util::stream::iter(
                emits_for_target.into_iter().map(Ok),
            ));
            let mode = self.config.mode;
            async move { backend.write_arrow_stream(table, target_stream, mode).await }
        });
        let results: Vec<Result<u64, BackendError>> = futures_util::future::join_all(writes).await;
        if let Some(e) = results.into_iter().find_map(|r| r.err()) {
            self.metrics.errors.inc();
            return Err(e);
        }
        self.metrics.rows_written.inc_by(n_rows);
        self.finalize_iteration().await?;
        Ok(n_rows)
    }

    /// Phase 39.4 PR 1: compute the global watermark + publish per-
    /// source and global gauges. Returns `Some(micros)` when at
    /// least one non-idle source has produced an `_event_ts`-bearing
    /// batch since the pipeline started; otherwise `None`. Returns
    /// `None` immediately when watermarks are disabled.
    ///
    /// The `min`-with-idle-fallback rule: a source is excluded from
    /// the `min` once it has been idle for `source_idleness_ms`. If
    /// every source is idle (or none has produced `_event_ts` yet),
    /// the global watermark stays `None`.
    fn compute_and_publish_watermark(&self) -> Option<i64> {
        let wm_cfg = self.config.watermark.as_ref()?;
        let lateness_micros = (wm_cfg.lateness_ms as i64).saturating_mul(1_000);
        let idleness = Duration::from_millis(wm_cfg.source_idleness_ms);

        let state = self
            .watermark_state
            .lock()
            .expect("watermark_state mutex poisoned");

        let mut global_wm: Option<i64> = None;
        for (source_idx, (_backend, query)) in self.sources.iter().enumerate() {
            let last_arrival = state.last_arrival[source_idx];
            let is_idle = match last_arrival {
                None => true,
                Some(t) => t.elapsed() > idleness,
            };
            let max_ts = state.max_event_ts[source_idx];
            if let Some(ts) = max_ts {
                let wm_i = ts.saturating_sub(lateness_micros);
                self.metrics
                    .watermark_seconds
                    .with_label_values(&[query.as_str()])
                    .set(wm_i as f64 / 1_000_000.0);
                if !is_idle {
                    global_wm = Some(global_wm.map_or(wm_i, |g| g.min(wm_i)));
                }
            }
        }

        if let Some(g) = global_wm {
            self.metrics
                .watermark_seconds
                .with_label_values(&["_global"])
                .set(g as f64 / 1_000_000.0);
        }

        global_wm
    }

    /// Phase 39.5a P1.6: drain the windowed transform's
    /// `take_dlq_rows()` buffer and route any captured late rows
    /// to the configured `dead_letter_topic`. No-op when:
    /// - The transform isn't a `WindowedAggregateTransform` (default
    ///   trait impl returns empty).
    /// - The window's `late_data` policy isn't `Dlq`.
    /// - No `dead_letter_topic` is configured (rows are then
    ///   silently consumed and a counter increments — operators
    ///   should configure a topic when using the policy).
    async fn drain_late_data_dlq(
        &self,
        transform: &dyn BatchTransform,
    ) -> Result<(), BackendError> {
        let dlq_rows = transform.take_dlq_rows().await?;
        if dlq_rows.is_empty() {
            return Ok(());
        }
        let n_rows: u64 = dlq_rows.iter().map(|b| b.num_rows() as u64).sum();
        if self.config.dead_letter_topic.is_none() && self.config.dead_letter_store.is_none() {
            // Policy is Dlq but no DLQ opt-in configured. Bump the
            // dropped counter so this is visible in metrics rather
            // than silent. (Unchanged from pre-Phase-1; the full
            // late_data DLQ write path is PRD-reserved.)
            self.metrics.dlq_writes.inc_by(0);
            tracing::warn!(
                pipeline = %self.config.pipeline_name,
                rows = n_rows,
                "late_data = \"dlq\" set but no dead_letter_topic configured — \
                 late rows discarded"
            );
            return Ok(());
        }
        let source_id = self.sources.first().map(|(_, q)| q.as_str()).unwrap_or("");
        let count = self
            .route_batches_to_dlq(
                DlqStage::LateData,
                "late data evicted past its lateness deadline (late_data = \"dlq\")",
                source_id,
                dlq_rows,
            )
            .await?;
        self.metrics.dlq_writes.inc_by(count);
        Ok(())
    }

    /// DLQ Phase 1: resolve the pipeline's [`DeadLetterStore`] —
    /// once, lazily, and ONLY from an error path (zero happy-path
    /// cost; a pipeline where nothing fails never calls this).
    ///
    /// Resolution order:
    /// 1. Explicit [`StreamingPipelineConfig::dead_letter_store`].
    /// 2. `dead_letter_topic` + Kafka source →
    ///    [`KafkaTopicDlq`] (the historical behavior, upgraded with
    ///    `emat-dlq-*` metadata headers; payload format preserved).
    /// 3. The configured state store's family via
    ///    [`StateStore::dead_letter_store`] (Postgres state store →
    ///    Postgres `ematix_dlq_records` table).
    /// 4. Fallback: in-process SQLite [`TableDlq`] (`:memory:`) with
    ///    a LOUD once-per-pipeline warning — dead-lettered records
    ///    are lost on process exit, mirroring the
    ///    `InMemoryStateStore` convention.
    ///
    /// Public since DLQ Phase 2: the Python/HTTP layer (Phase 4)
    /// resolves the same store to serve depth / browse / park /
    /// purge without re-implementing the resolution rules.
    pub async fn resolve_dlq_store(&self) -> Result<&Arc<dyn DeadLetterStore>, BackendError> {
        self.dlq_store
            .get_or_try_init(|| async {
                // 1. Explicit store wins (programmatic override —
                //    the `dlq_store` mode knob below is the
                //    config-file surface and never outranks a
                //    handed-in store).
                if let Some(store) = &self.config.dead_letter_store {
                    return Ok(Arc::clone(store));
                }
                // DLQ Phase 4: `dlq_store = "topic"` is a demand,
                // not a preference — an unsatisfiable demand is a
                // hard error, never a silent fallback.
                if self.config.dlq_store == DlqStoreMode::Topic {
                    let Some(topic) = &self.config.dead_letter_topic else {
                        return Err(BackendError::Other(format!(
                            "pipeline `{}`: dlq_store = \"topic\" requires a \
                             dead_letter_topic to be configured",
                            self.config.pipeline_name
                        )));
                    };
                    let Some((backend, _query)) = self.sources.first() else {
                        return Err(BackendError::Other(
                            "DLQ requested but no sources configured".into(),
                        ));
                    };
                    if backend.as_kafka().is_none() {
                        return Err(BackendError::Other(format!(
                            "pipeline `{}`: dlq_store = \"topic\" requires a Kafka \
                             primary source (topic DLQs ride the source's producer)",
                            self.config.pipeline_name
                        )));
                    }
                    let store = KafkaTopicDlq::new(
                        Arc::clone(backend),
                        topic.clone(),
                        self.config.pipeline_name.clone(),
                    )?;
                    return Ok(Arc::new(store) as Arc<dyn DeadLetterStore>);
                }
                // 2. Historical (Auto only — `dlq_store = "table"`
                //    skips straight to the table family): explicit
                //    topic + Kafka source. Uses the primary (first)
                //    source — multi-source DLQ routing would need
                //    per-source policy and isn't built (same
                //    limitation as before Phase 1).
                if self.config.dlq_store == DlqStoreMode::Auto
                    && let Some(topic) = &self.config.dead_letter_topic
                    && let Some((backend, _query)) = self.sources.first()
                    && backend.as_kafka().is_some()
                {
                    let store = KafkaTopicDlq::new(
                        Arc::clone(backend),
                        topic.clone(),
                        self.config.pipeline_name.clone(),
                    )?;
                    return Ok(Arc::new(store) as Arc<dyn DeadLetterStore>);
                }
                // 3. The state store family (portable table DLQ).
                if let Some(state_store) = &self.config.state_store
                    && let Some(store) = state_store.dead_letter_store().await?
                {
                    if self.config.dead_letter_topic.is_some() {
                        tracing::warn!(
                            pipeline = %self.config.pipeline_name,
                            "dead_letter_topic is set but the source is not Kafka —                              routing dead letters to the state store's table DLQ                              instead"
                        );
                    }
                    return Ok(store);
                }
                // 4. LOUD fallback. Matches the InMemoryStateStore
                //    convention: works, but loses data on exit.
                tracing::warn!(
                    pipeline = %self.config.pipeline_name,
                    "DLQ requested but no dead_letter_topic (with a Kafka source),                      no state store with a SQL family, and no explicit                      dead_letter_store are configured — falling back to an                      IN-MEMORY SQLite dead-letter store. Dead-lettered records                      WILL BE LOST when this process exits. Configure a                      dead_letter_topic or a Postgres state store for durability."
                );
                let store = TableDlq::open_sqlite(":memory:").await?;
                Ok(Arc::new(store) as Arc<dyn DeadLetterStore>)
            })
            .await
    }

    /// Route failed batches to the resolved [`DeadLetterStore`] with
    /// full [`DlqMeta`]. Returns the number of dead-lettered records
    /// (one per row — feeds the `dlq_writes` counter).
    ///
    /// Format preservation: with a Kafka primary source, rows are
    /// encoded through the source's own `payload_format` machinery
    /// (`encode_batch_payloads`) — byte-identical to the pre-Phase-1
    /// `write_arrow_stream`-based DLQ path. Non-Kafka sources encode
    /// JSONL (`payload_format = "json"`).
    ///
    /// At-least-once: callers invoke this BEFORE
    /// [`Self::finalize_iteration`], so the store's append ack lands
    /// before source offsets commit.
    async fn route_batches_to_dlq(
        &self,
        stage: DlqStage,
        error: &str,
        source_id: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<u64, BackendError> {
        let store = self.resolve_dlq_store().await?;

        // Failure metadata gathered at the emission boundary (the
        // stores themselves never read clocks — house convention).
        let failed_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let event_ts: Option<i64> = batches.iter().filter_map(batch_max_event_ts).max();

        let primary = self
            .sources
            .first()
            .ok_or_else(|| BackendError::Other("DLQ requested but no sources configured".into()))?;
        // Offset snapshot is advisory metadata — a snapshot error
        // must not turn a recoverable dead-letter into a pipeline
        // failure.
        let offset_bytes: Option<Vec<u8>> = primary.0.offset_snapshot().await.unwrap_or(None);

        // Encode rows in the source's wire format.
        let (payloads, payload_format) = match primary.0.as_kafka() {
            Some(kafka) => {
                // Avro/Protobuf schema-subject convention keys off
                // the DLQ topic when one is configured (identical to
                // the historical produce path); otherwise the source
                // id stands in.
                let subject_topic = self
                    .config
                    .dead_letter_topic
                    .clone()
                    .unwrap_or_else(|| source_id.to_string());
                let mut payloads: Vec<Vec<u8>> = Vec::new();
                for batch in &batches {
                    payloads.extend(kafka.encode_batch_payloads(&subject_topic, batch).await?);
                }
                (payloads, kafka.payload_format().as_str().to_string())
            }
            None => {
                let mut payloads: Vec<Vec<u8>> = Vec::new();
                for batch in &batches {
                    payloads.extend(crate::kafka_backend::encode_batch_as_jsonl_lines(batch)?);
                }
                (payloads, "json".to_string())
            }
        };

        let records: Vec<DlqRecord> = payloads
            .into_iter()
            .map(|payload| DlqRecord {
                id: DlqRecordId(uuid::Uuid::new_v4().to_string()),
                meta: DlqMeta {
                    pipeline: self.config.pipeline_name.clone(),
                    stage,
                    error: truncate_error(error),
                    source_id: source_id.to_string(),
                    offset_bytes: offset_bytes.clone(),
                    event_ts,
                    failed_at,
                    attempt: 1,
                    payload_format: payload_format.clone(),
                },
                payload,
            })
            .collect();
        let n = records.len() as u64;
        store.append(records).await?;
        Ok(n)
    }

    /// DLQ Phase 2: replay (redrive) the pipeline's dead-lettered
    /// records **through the pipeline's own transform + targets**.
    ///
    /// Bounded, single-pass: exactly one `take_for_replay` leases
    /// the `selection`; every leased record is then resolved —
    ///
    /// - **success** (decode → transform → every target write OK):
    ///   acked (removed) from the store;
    /// - **failure** (decode, transform, or any target write):
    ///   re-dead-lettered as a NEW record with `attempt + 1`, the
    ///   fresh error string, and `failed_at` = this run's clock —
    ///   then the original is acked. Redriven records are NOT
    ///   re-taken by the same run (no hot retry loop against a
    ///   still-broken sink);
    /// - **poison** (`attempt + 1` would exceed
    ///   `options.max_attempts`, or the record arrived already past
    ///   the budget): parked. Table stores park in place (the
    ///   record is neither acked nor retried). Stores that answer
    ///   `park` with a typed `Unsupported` (a Kafka topic) park a
    ///   copy into [`ReplayOptions::park_store`] — or the
    ///   pipeline's table fallback
    ///   ([`Self::resolve_park_fallback_store`]) — and the original
    ///   IS acked so the topic cursor advances.
    ///
    /// ## Ordering / at-least-once
    ///
    /// Redrive appends and fallback parks land durably BEFORE any
    /// original is acked, and acks are issued in take order (which
    /// is per-partition offset order on a Kafka store — the only
    /// order its contiguous group cursor can express). A crash
    /// mid-run leaves originals leased; the lease expires and a
    /// later replay re-takes them. Replay is therefore
    /// at-least-once into the targets, same as the pipeline itself
    /// (idempotent-target assumption unchanged, PRD non-goal:
    /// exactly-once).
    ///
    /// ## Serialization
    ///
    /// When the resolved store's lease is process-local
    /// ([`DeadLetterStore::replay_requires_serialization`], i.e.
    /// `KafkaTopicDlq`), same-pipeline replays in this process are
    /// serialized behind an in-process mutex. Cross-process
    /// concurrency is NOT guarded (single-operator assumption; see
    /// `dlq/replay.rs` module docs).
    ///
    /// ## Scope notes (Phase 2)
    ///
    /// - CDC-mode pipelines (`config.cdc`) are typed-rejected:
    ///   replay re-applies through `write_arrow_stream`, which
    ///   would break per-event CDC semantics.
    /// - Stateful (windowed/session) transforms absorb replayed
    ///   rows into their CURRENT state — replay does not
    ///   reconstruct the original event-time context.
    /// - Records are reprocessed one at a time (exact per-record
    ///   attribution for the report + per-partition ack order);
    ///   batching successive same-format records is a perf
    ///   follow-up if DLQ drains ever get large.
    ///
    /// RunHistory registration (`kind=replay`) is Phase 4's job —
    /// the returned [`ReplayReport`] (counts + wall-clock window)
    /// is the hook it builds on.
    pub async fn run_dlq_replay(
        &self,
        selection: DlqSelection,
        options: ReplayOptions,
    ) -> Result<ReplayReport, BackendError> {
        if self.config.cdc.is_some() {
            return Err(BackendError::Other(format!(
                "DLQ replay is not supported for CDC-mode pipelines yet \
                 (pipeline '{}'): replayed batches would re-apply through \
                 write_arrow_stream, bypassing run_cdc's per-event semantics",
                self.config.pipeline_name
            )));
        }
        let store = Arc::clone(self.resolve_dlq_store().await?);

        // Serialize same-pipeline replays when the store's lease
        // can't (process-local; see method docs).
        let _guard = if store.replay_requires_serialization() {
            Some(
                crate::dlq::replay::replay_lock(&self.config.pipeline_name)
                    .lock_owned()
                    .await,
            )
        } else {
            None
        };

        // The engine is the emission boundary — it reads the clock;
        // stores never do (house convention, same as
        // `route_batches_to_dlq`).
        let now_ms = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        };
        let started_at_ms = now_ms();
        let mut report = ReplayReport {
            started_at_ms,
            ..Default::default()
        };

        let pipeline = self.config.pipeline_name.as_str();
        let mut source = crate::dlq::DlqReplaySource::new(
            Arc::clone(&store),
            pipeline,
            selection,
            options.lease,
        );
        let taken = source
            .take(started_at_ms)
            .await
            .map_err(BackendError::from)?;
        report.taken = taken.len() as u64;
        if taken.is_empty() {
            report.finished_at_ms = now_ms();
            return Ok(report);
        }

        // Kafka handle for Avro/Protobuf decode (schema-registry
        // machinery lives on the source backend).
        let kafka = self.sources.first().and_then(|(b, _)| b.as_kafka());

        /// Per-record resolution, index-aligned with `taken`.
        enum Outcome {
            /// Reprocessed OK → ack.
            Succeeded,
            /// Re-dead-lettered (new record appended) → ack original.
            Redriven,
            /// Poison → park (in place, or fallback + ack).
            Parked,
        }

        let mut outcomes: Vec<Outcome> = Vec::with_capacity(taken.len());
        let mut redrive: Vec<DlqRecord> = Vec::new();
        for record in &taken {
            // Already past the budget (e.g. a previous run used a
            // higher max_attempts): park without burning a retry.
            if record.meta.attempt > options.max_attempts {
                outcomes.push(Outcome::Parked);
                continue;
            }
            match self.replay_dlq_record(record, kafka).await {
                Ok(()) => outcomes.push(Outcome::Succeeded),
                Err(e) => {
                    let next_attempt = record.meta.attempt.saturating_add(1);
                    if next_attempt > options.max_attempts {
                        tracing::warn!(
                            pipeline,
                            record_id = %record.id,
                            attempt = record.meta.attempt,
                            max_attempts = options.max_attempts,
                            error = %e,
                            "DLQ replay: attempt budget exhausted — parking poison record"
                        );
                        outcomes.push(Outcome::Parked);
                    } else {
                        tracing::warn!(
                            pipeline,
                            record_id = %record.id,
                            next_attempt,
                            error = %e,
                            "DLQ replay: record failed again — re-dead-lettering"
                        );
                        redrive.push(DlqRecord {
                            id: DlqRecordId(uuid::Uuid::new_v4().to_string()),
                            meta: DlqMeta {
                                error: truncate_error(&e.to_string()),
                                failed_at: now_ms(),
                                attempt: next_attempt,
                                ..record.meta.clone()
                            },
                            payload: record.payload.clone(),
                        });
                        outcomes.push(Outcome::Redriven);
                    }
                }
            }
        }

        // Durability order: redrives + parks land BEFORE any
        // original is acked (a crash in between re-delivers, never
        // loses).
        if !redrive.is_empty() {
            let n = redrive.len() as u64;
            store.append(redrive).await?;
            self.metrics.dlq_writes.inc_by(n);
        }

        let parked_ids: Vec<DlqRecordId> = taken
            .iter()
            .zip(outcomes.iter())
            .filter(|(_, o)| matches!(o, Outcome::Parked))
            .map(|(r, _)| r.id.clone())
            .collect();
        // Whether parked originals must ALSO be acked (only when
        // the park landed in a fallback store, so the topic cursor
        // can advance past them).
        let mut ack_parked = false;
        if !parked_ids.is_empty() {
            match store.park(pipeline, &parked_ids).await {
                Ok(()) => {}
                Err(DlqError::Unsupported(_)) => {
                    let fallback = match &options.park_store {
                        Some(s) => Arc::clone(s),
                        None => Arc::clone(self.resolve_park_fallback_store().await?),
                    };
                    let park_records: Vec<DlqRecord> = taken
                        .iter()
                        .zip(outcomes.iter())
                        .filter(|(_, o)| matches!(o, Outcome::Parked))
                        .map(|(r, _)| r.clone())
                        .collect();
                    fallback.append(park_records).await?;
                    fallback.park(pipeline, &parked_ids).await?;
                    ack_parked = true;
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Ack in take order — per-partition offset order on a Kafka
        // store, whose group cursor is contiguous.
        let ack_ids: Vec<DlqRecordId> = taken
            .iter()
            .zip(outcomes.iter())
            .filter(|(_, o)| match o {
                Outcome::Succeeded | Outcome::Redriven => true,
                Outcome::Parked => ack_parked,
            })
            .map(|(r, _)| r.id.clone())
            .collect();
        if !ack_ids.is_empty() {
            store.ack_replayed(pipeline, &ack_ids).await?;
        }

        for o in &outcomes {
            match o {
                Outcome::Succeeded => report.succeeded += 1,
                Outcome::Redriven => report.redeadlettered += 1,
                Outcome::Parked => report.parked += 1,
            }
        }
        report.finished_at_ms = now_ms();
        tracing::info!(
            pipeline,
            taken = report.taken,
            succeeded = report.succeeded,
            redeadlettered = report.redeadlettered,
            parked = report.parked,
            "DLQ replay run finished"
        );
        Ok(report)
    }

    /// Reprocess ONE dead-lettered record through the pipeline's
    /// own transform + targets. Ok(()) iff the decoded batches
    /// cleared the transform and every target write succeeded. A
    /// transform that emits zero rows (filtered / absorbed into
    /// windowed state) counts as success — the record was consumed.
    async fn replay_dlq_record(
        &self,
        record: &DlqRecord,
        kafka: Option<&KafkaBackend>,
    ) -> Result<(), BackendError> {
        let batches = crate::dlq::DlqReplaySource::decode(record, kafka).await?;

        let batches: Vec<RecordBatch> = match &self.config.transform {
            None => batches,
            Some(t) => {
                let ctx = BatchContext {
                    global_wm: None,
                    source_id: Some(record.meta.source_id.clone()),
                };
                let mut out = Vec::new();
                for b in batches {
                    out.extend(t.transform(b, &ctx).await?);
                }
                out
            }
        };

        let n_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        if n_rows == 0 {
            return Ok(());
        }

        let writes = self.targets.iter().map(|(backend, table)| {
            let batches_for_target: Vec<RecordBatch> = batches.clone();
            let target_stream: ArrowBatchStream = Box::pin(futures_util::stream::iter(
                batches_for_target.into_iter().map(Ok),
            ));
            let mode = self.config.mode;
            async move { backend.write_arrow_stream(table, target_stream, mode).await }
        });
        let results: Vec<Result<u64, BackendError>> = futures_util::future::join_all(writes).await;
        if let Some(e) = results.into_iter().find_map(|r| r.err()) {
            return Err(e);
        }
        self.metrics.rows_written.inc_by(n_rows);
        Ok(())
    }

    /// DLQ Phase 2: the TABLE store poison records park into when
    /// the pipeline's own store cannot express `park` and no
    /// explicit [`ReplayOptions::park_store`] was provided.
    /// Resolution mirrors rules 3–4 of [`Self::resolve_dlq_store`]:
    /// the configured state store's SQL family, else an in-process
    /// SQLite `:memory:` store with a LOUD warning (parked records
    /// are then lost on process exit). Cached per pipeline instance
    /// so successive replays park into the same store.
    pub async fn resolve_park_fallback_store(
        &self,
    ) -> Result<&Arc<dyn DeadLetterStore>, BackendError> {
        self.park_fallback_store
            .get_or_try_init(|| async {
                if let Some(state_store) = &self.config.state_store
                    && let Some(store) = state_store.dead_letter_store().await?
                {
                    return Ok(store);
                }
                tracing::warn!(
                    pipeline = %self.config.pipeline_name,
                    "DLQ replay must park poison records but the pipeline's \
                     dead-letter store cannot express park and no park_store / \
                     state-store SQL family is configured — falling back to an \
                     IN-MEMORY SQLite store. Parked records WILL BE LOST when \
                     this process exits. Configure a Postgres state store or \
                     pass ReplayOptions::park_store for durability."
                );
                let store = TableDlq::open_sqlite(":memory:").await?;
                Ok(Arc::new(store) as Arc<dyn DeadLetterStore>)
            })
            .await
    }

    /// DLQ Phase 3: rewind the pipeline's read position to `to`,
    /// atomically resetting durable state for stateful pipelines.
    ///
    /// Orchestration (the pipeline loop must be paused — i.e. not
    /// currently inside [`Self::run`] — for the duration; the
    /// caller stops the loop before and resumes it after, which is
    /// what the Phase 4 API layer does):
    ///
    /// 1. **Gate** — a stateful pipeline (`transform.is_stateful()`)
    ///    hard-errors unless `confirm_state_reset` is `true`:
    ///    rewinding it destroys accumulated window/session state
    ///    (it MUST be destroyed, or re-consumed rows would
    ///    double-count) and the operator has to say so explicitly.
    /// 2. **Resolve** — [`RewindTarget::Timestamp`] resolves to
    ///    per-source offset bytes via each backend's
    ///    [`Backend::offsets_for_timestamp`] (Kafka:
    ///    `offsets_for_times`; backends without a timestamp index
    ///    return their typed error). [`RewindTarget::Offset`]
    ///    carries backend-opaque offset bytes directly and is
    ///    single-source-only (bytes are per-backend; there is no
    ///    honest way to apply one blob to N sources).
    /// 3. **Reset** — when a `state_store` is configured, its
    ///    [`StateStore::reset`] clears every state blob and
    ///    REPLACES the committed offsets in ONE transaction.
    ///    Then the in-memory transform state is cleared
    ///    ([`BatchTransform::clear_state`]) and the pipeline's
    ///    watermark bookkeeping is zeroed so the rewound run
    ///    re-derives watermarks from the re-consumed rows.
    /// 4. **Seek** — every source backend's [`Backend::seek_to`]
    ///    is applied with its resolved bytes. Sources that don't
    ///    support seeking fail the whole rewind up-front (step 2
    ///    validates before anything mutates).
    pub async fn rewind(
        &self,
        to: RewindTarget,
        confirm_state_reset: bool,
    ) -> Result<RewindReport, BackendError> {
        let pipeline = self.config.pipeline_name.as_str();

        // 1. Confirmation gate. Stateful = the transform says so —
        //    the state store alone doesn't make a pipeline stateful
        //    (a stateless pipeline checkpoints only offsets).
        let stateful = self
            .config
            .transform
            .as_ref()
            .is_some_and(|t| t.is_stateful());
        if stateful && !confirm_state_reset {
            return Err(BackendError::Other(format!(
                "pipeline `{pipeline}` has a stateful (windowed/session) transform — \
                 rewinding it clears ALL accumulated window state so re-consumed rows \
                 don't double-count. Pass confirm_state_reset = true to proceed."
            )));
        }

        // 2. Resolve per-source offset bytes and validate
        //    seekability BEFORE anything mutates, so any failure
        //    here leaves the pipeline untouched. Resolution runs
        //    first: its typed error ("no timestamp index") is more
        //    actionable than the generic seek complaint.
        let mut resolved: Vec<(String, Vec<u8>)> = Vec::with_capacity(self.sources.len());
        match &to {
            RewindTarget::Offset(bytes) => {
                // Offset bytes are backend-opaque; one blob cannot
                // honestly address N sources.
                if self.sources.len() != 1 {
                    return Err(BackendError::Other(format!(
                        "pipeline `{pipeline}` has {} sources — offset-bytes rewind is \
                         single-source only; rewind by timestamp instead",
                        self.sources.len()
                    )));
                }
                resolved.push((self.sources[0].1.clone(), bytes.clone()));
            }
            RewindTarget::Timestamp(ts_ms) => {
                for (backend, query) in &self.sources {
                    let bytes = backend.offsets_for_timestamp(query, *ts_ms).await?;
                    resolved.push((query.clone(), bytes));
                }
            }
        }
        for (backend, query) in &self.sources {
            if !backend.supports_seek_to() {
                return Err(BackendError::Other(format!(
                    "pipeline `{pipeline}`: source `{query}` does not support seek_to — \
                     it cannot be rewound"
                )));
            }
        }

        // 3. Durable reset FIRST: state-clear + offset-write in one
        //    state-store transaction, so a crash mid-rewind can
        //    never pair cleared state with stale offsets (or vice
        //    versa). Stateless pipelines with a store still go
        //    through reset — their committed offsets must move too,
        //    or the next restart would seek right back.
        if let Some(store) = &self.config.state_store {
            let offsets: std::collections::HashMap<String, Vec<u8>> =
                resolved.iter().cloned().collect();
            store.reset(pipeline, offsets).await?;
        }

        // 4. In-memory state: transform accumulators + the
        //    pipeline's watermark bookkeeping. The rewound run must
        //    re-derive watermarks from the re-consumed rows — a
        //    stale high watermark would mis-classify them as late.
        if stateful {
            if let Some(t) = &self.config.transform {
                t.clear_state().await?;
            }
        }
        {
            let mut wm = self
                .watermark_state
                .lock()
                .expect("watermark_state mutex poisoned");
            *wm = WatermarkState::new(self.sources.len());
        }

        // 5. Seek every source to its resolved position.
        for ((backend, _query), (_id, bytes)) in self.sources.iter().zip(resolved.iter()) {
            backend.seek_to(bytes).await?;
        }

        tracing::info!(
            pipeline,
            sources = resolved.len(),
            state_cleared = stateful,
            "rewind applied"
        );
        Ok(RewindReport {
            sources: resolved,
            state_cleared: stateful,
        })
    }
}

/// DLQ Phase 3: where a rewind seeks the pipeline's sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewindTarget {
    /// Backend-opaque offset bytes (the same encoding
    /// [`Backend::offset_snapshot`] / [`Backend::seek_to`] round-
    /// trip). Single-source pipelines only.
    Offset(Vec<u8>),
    /// Wall-clock timestamp, milliseconds since the Unix epoch,
    /// passed in by the caller (house convention — the library
    /// never reads a clock). Resolved per source via
    /// [`Backend::offsets_for_timestamp`].
    Timestamp(i64),
}

/// DLQ Phase 3: what a completed rewind actually did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RewindReport {
    /// `(source query, resolved offset bytes)` per source, in
    /// `sources` order — the positions the pipeline will resume
    /// from.
    pub sources: Vec<(String, Vec<u8>)>,
    /// Whether transform + store state was cleared (stateful
    /// pipelines only).
    pub state_cleared: bool,
}

/// Phase 36j.2: Kafka→Kafka exactly-once pipeline. Bundles the
/// source consumer's offset commit into the target producer's
/// transaction via `send_offsets_to_transaction`, so a partial
/// failure mid-batch aborts both the produces *and* the offset
/// advance — re-delivery on restart, no duplicates.
///
/// Differences vs the trait-object `StreamingPipeline`:
///   - typed `Arc<KafkaBackend>` references on both sides (the
///     coordination logic isn't on the `Backend` trait surface; it
///     uses Kafka-specific methods like
///     `pending_offsets_topic_partition_list` and
///     `consumer_group_metadata`).
///   - Target must have `KafkaDeliverySemantics::ExactlyOnce`
///     configured; constructor errors out otherwise.
///   - Source offsets are advanced as a side-effect of
///     `commit_transaction`; the pipeline calls
///     `source.clear_pending_offsets()` after each successful
///     commit to drop the stale in-memory pending set.
///
/// DLQ + Prometheus metrics from the trait-object pipeline aren't
/// wired here yet — the EOS coordinator focuses on correctness;
/// observability follow-ups can layer in the same counter struct.
pub struct KafkaToKafkaEosPipeline {
    pub source: Arc<KafkaBackend>,
    pub target: Arc<KafkaBackend>,
    pub config: StreamingPipelineConfig,
    pub metrics: StreamingPipelineMetricsCounters,
}

impl KafkaToKafkaEosPipeline {
    /// Construct an EOS pipeline. Validates the target backend has
    /// transactions configured.
    pub fn new(
        source: Arc<KafkaBackend>,
        target: Arc<KafkaBackend>,
        config: StreamingPipelineConfig,
    ) -> Result<Self, BackendError> {
        if !matches!(
            target.delivery_semantics(),
            crate::kafka_backend::KafkaDeliverySemantics::ExactlyOnce { .. }
        ) {
            return Err(BackendError::Other(
                "KafkaToKafkaEosPipeline: target must be configured with \
                 KafkaDeliverySemantics::ExactlyOnce { transactional_id: ... }"
                    .into(),
            ));
        }
        let metrics = StreamingPipelineMetricsCounters::new(&config.pipeline_name);
        Ok(Self {
            source,
            target,
            config,
            metrics,
        })
    }

    /// Run the EOS loop until shutdown. Same shape as
    /// `StreamingPipeline::run` but every batch is bracketed by a
    /// single Kafka transaction that atomically produces *and*
    /// advances the source consumer's offsets.
    pub async fn run(
        &self,
        shutdown: ShutdownSignal,
    ) -> Result<StreamingPipelineMetrics, BackendError> {
        let mut summary = StreamingPipelineMetrics::default();
        loop {
            if shutdown.is_triggered() {
                summary.shutdown_triggered = true;
                break;
            }

            // Use the trait method via Backend so the consumer
            // session caching machinery still applies.
            let stream: ArrowBatchStream = (self.source.as_ref() as &dyn Backend)
                .read_arrow_stream(&self.config.source_query)
                .await
                .inspect_err(|_| {
                    self.metrics.errors.inc();
                })?;
            let batches: Vec<RecordBatch> = stream.try_collect().await.inspect_err(|_| {
                self.metrics.errors.inc();
            })?;

            if batches.is_empty() {
                self.metrics.idle_iterations.inc();
                tokio::select! {
                    _ = shutdown.wait() => {
                        summary.shutdown_triggered = true;
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(self.config.idle_pause_ms)) => {}
                }
                continue;
            }

            let n_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            self.metrics.rows_consumed.inc_by(n_rows);

            let target_stream: ArrowBatchStream =
                Box::pin(futures_util::stream::iter(batches.into_iter().map(Ok)));
            // Coordinated produce: begin_txn, produce, send_offsets,
            // commit. On error → abort_txn (already handled inside
            // write_arrow_stream_eos) → bubble up.
            let produced = self
                .target
                .write_arrow_stream_eos(&self.config.target, target_stream, &self.source)
                .await
                .inspect_err(|_| {
                    self.metrics.errors.inc();
                })?;
            self.metrics.rows_written.inc_by(produced);

            // Source offsets advanced atomically as part of the
            // commit_transaction; drop the pending set so the next
            // iteration's send_offsets_to_transaction doesn't
            // re-send the same offsets.
            self.source.clear_pending_offsets()?;

            self.metrics.batches.inc();
            summary.total_rows += produced;
            summary.iterations += 1;
        }
        Ok(summary)
    }
}

/// Convenience: install OS signal handlers (SIGTERM / SIGINT on
/// Unix) that fire the returned `ShutdownTrigger`. On non-Unix
/// platforms only Ctrl-C is caught.
///
/// Returns the signal-side handle for the pipeline, plus a join
/// handle for the signal-watcher task — the caller can `.abort()`
/// it on clean exit if desired.
#[cfg(unix)]
pub fn install_shutdown_handler() -> (ShutdownSignal, tokio::task::JoinHandle<()>) {
    use tokio::signal::unix::{SignalKind, signal};

    let (sig, trigger) = ShutdownSignal::new();
    let handle = tokio::spawn(async move {
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => return, // can't install — bail
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(_) => return,
        };
        // First signal triggers shutdown. Subsequent signals are
        // ignored here (the supervisor / OS may force-kill on a
        // second one if we hang).
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
        trigger.trigger();
    });
    (sig, handle)
}

#[cfg(not(unix))]
pub fn install_shutdown_handler() -> (ShutdownSignal, tokio::task::JoinHandle<()>) {
    let (sig, trigger) = ShutdownSignal::new();
    let handle = tokio::spawn(async move {
        // Best-effort Ctrl-C catch on non-Unix.
        let _ = tokio::signal::ctrl_c().await;
        trigger.trigger();
    });
    (sig, handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_signal_starts_clear() {
        let (sig, _trigger) = ShutdownSignal::new();
        assert!(!sig.is_triggered());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_trigger_flips_signal() {
        let (sig, trigger) = ShutdownSignal::new();
        trigger.trigger();
        assert!(sig.is_triggered());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_wait_resolves_after_trigger() {
        let (sig, trigger) = ShutdownSignal::new();
        let handle = tokio::spawn(async move {
            sig.wait().await;
        });
        // Trigger from the parent task; the spawned task should
        // observe it and complete.
        trigger.trigger();
        // Bound on test latency so a regression doesn't hang CI.
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("wait() did not resolve within 5s")
            .expect("spawned task panicked");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_wait_resolves_when_trigger_dropped() {
        // Sender dropped without triggering → wait() should still
        // resolve (the watcher returns Err on `changed()` and we
        // treat that as "controller gone").
        let (sig, trigger) = ShutdownSignal::new();
        drop(trigger);
        tokio::time::timeout(Duration::from_secs(5), sig.wait())
            .await
            .expect("wait() did not resolve when trigger dropped");
    }

    // --- Phase 39.5a slice 1.6: pipeline state-load on startup -----------

    /// `load_state` against an empty `InMemoryStateStore` must
    /// succeed and produce an empty `RecoveredState`.
    #[tokio::test(flavor = "multi_thread")]
    async fn load_state_with_empty_store_returns_empty() {
        use crate::SQLiteBackend;
        use crate::kafka_backend::KafkaBackend;
        use crate::state_store::InMemoryStateStore;

        let source: Arc<dyn Backend> =
            Arc::new(KafkaBackend::open("localhost:9092", Some("g")).unwrap());
        let target: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(":memory:").unwrap());
        let target_table = TargetTable {
            schema: "".into(),
            name: "out".into(),
        };
        let pipeline = StreamingPipeline::new(
            source,
            vec![(target, target_table.clone())],
            StreamingPipelineConfig::new("topic", target_table, "fresh"),
        );
        let store = InMemoryStateStore::new();
        let recovered = pipeline.load_state(&store).await.unwrap();
        assert!(recovered.offsets.is_empty());
        assert!(recovered.state_by_key.is_empty());
        assert_eq!(recovered.state_version, 0);
    }

    /// Recovered offsets get applied via `seek_to` on the matching
    /// source. Verified by checking that the Kafka backend's
    /// `pending_seek` field is populated post-`load_state`.
    #[tokio::test(flavor = "multi_thread")]
    async fn load_state_applies_seek_to_on_matching_source() {
        use crate::SQLiteBackend;
        use crate::kafka_backend::KafkaBackend;
        use crate::state_store::{CommitSnapshot, InMemoryStateStore};

        let kafka = Arc::new(KafkaBackend::open("localhost:9092", Some("g")).unwrap());
        let target: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(":memory:").unwrap());
        let target_table = TargetTable {
            schema: "".into(),
            name: "out".into(),
        };
        let pipeline = StreamingPipeline::new(
            Arc::clone(&kafka) as Arc<dyn Backend>,
            vec![(target, target_table.clone())],
            StreamingPipelineConfig::new("topic", target_table, "p1"),
        );

        // Pre-populate the store with an offset for our source's
        // query string ("topic"). Format must match the Kafka
        // backend's wire shape — v=1 JSON.
        let mut offsets = std::collections::HashMap::new();
        offsets.insert(
            "topic".to_string(),
            br#"{"v":1,"partitions":{"0":42}}"#.to_vec(),
        );
        let store = InMemoryStateStore::new();
        store
            .commit(
                "p1",
                CommitSnapshot {
                    offsets,
                    state_version: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let recovered = pipeline.load_state(&store).await.unwrap();
        assert_eq!(recovered.offsets.len(), 1);

        // Verify the seek landed on the Kafka backend by checking
        // the shared `seek_map` populated by `seek_to`. P4 #26
        // moved this from a per-session `pending_seek` Option to a
        // backend-level `Arc<Mutex<HashMap>>` shared with every
        // `EmatixKafkaContext` for `post_rebalance` consumption.
        // Debug renders the map, so we check for the offset itself.
        let dbg_after = format!("{kafka:?}");
        assert!(
            dbg_after.contains("seek_map: Mutex { data: {0: 42}"),
            "after load_state, Kafka backend should have populated \
             seek_map; got debug: {dbg_after}"
        );
    }

    /// Recovered offset for a source whose backend doesn't support
    /// `seek_to` is a hard error — silently dropping committed
    /// state would be a recipe for double-counting.
    #[tokio::test(flavor = "multi_thread")]
    async fn load_state_errors_when_source_lacks_seek_to() {
        use crate::SQLiteBackend;
        use crate::state_store::{CommitSnapshot, InMemoryStateStore};

        // SQLite has no seek_to override → supports_seek_to() = false.
        let source: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(":memory:").unwrap());
        let target: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(":memory:").unwrap());
        let target_table = TargetTable {
            schema: "".into(),
            name: "out".into(),
        };
        let pipeline = StreamingPipeline::new(
            source,
            vec![(target, target_table.clone())],
            StreamingPipelineConfig::new("SELECT 1", target_table, "p-bad"),
        );

        let mut offsets = std::collections::HashMap::new();
        offsets.insert("SELECT 1".to_string(), b"some-bytes".to_vec());
        let store = InMemoryStateStore::new();
        store
            .commit(
                "p-bad",
                CommitSnapshot {
                    offsets,
                    state_version: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let err = pipeline.load_state(&store).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("does not support seek_to") || msg.contains("seek_to"),
            "expected seek_to-not-supported error, got: {msg}"
        );
    }

    // --- Phase 39.5a P1.8: periodic checkpoint ticker --------------------

    /// `checkpoint_loop` directly: with a session transform that
    /// has a dirty key and an in-memory store, the ticker should
    /// produce one commit within ~200ms when configured at 50ms.
    #[tokio::test(flavor = "multi_thread")]
    async fn checkpoint_loop_commits_dirty_state() {
        use crate::state_store::{InMemoryStateStore, StateStore};
        use crate::transform::BatchTransform;
        use crate::windowed::{
            AggKind, AggregationSpec, LateDataPolicy, WindowConfig, WindowKind,
            WindowedAggregateTransform,
        };
        use arrow_array::{Int64Array, RecordBatch as RB, TimestampMicrosecondArray};
        use arrow_schema::{DataType, Field, Schema, TimeUnit};

        // Minimal session config — gap=10ms, max_dur=1s. Single
        // user_id ingested → one dirty key after first batch.
        let cfg = WindowConfig {
            kind: WindowKind::Session,
            duration_ms: 0,
            hop_ms: 0,
            gap_ms: Some(10),
            max_session_duration_ms: Some(1_000),
            event_time_column: "_event_ts".into(),
            group_by: vec!["user_id".into()],
            aggregations: vec![AggregationSpec::new(AggKind::CountStar, None, "n")],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
        };
        let transform: Arc<dyn BatchTransform> =
            Arc::new(WindowedAggregateTransform::new(cfg, None).unwrap());

        // Ingest one row so the transform has a dirty key.
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new(
                "_event_ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]));
        let batch = RB::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(TimestampMicrosecondArray::from(vec![10_i64]).with_timezone("UTC")),
            ],
        )
        .unwrap();
        transform
            .transform(
                batch,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Sanity-check: the transform really has dirty state at this
        // point. (If the ingest somehow failed silently the test
        // would otherwise blame the ticker.)
        let (probe_upserts, _probe_deletes) = transform.take_state_commit().await.unwrap();
        assert!(
            !probe_upserts.is_empty(),
            "transform should have one dirty key after ingest"
        );
        // Re-dirty by ingesting another row, since we just drained.
        let batch2 = RB::try_new(
            transform.input_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(TimestampMicrosecondArray::from(vec![15_i64]).with_timezone("UTC")),
            ],
        )
        .unwrap();
        transform
            .transform(
                batch2,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Spawn the checkpoint loop with a 50ms tick.
        let store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new());
        let (shutdown, trigger) = ShutdownSignal::new();
        let store_clone = Arc::clone(&store);
        let transform_clone = Some(Arc::clone(&transform));
        let handle = tokio::spawn(async move {
            super::checkpoint_loop(
                store_clone,
                "p-test".into(),
                transform_clone,
                Vec::new(), // no sources — offset_snapshot path is empty
                Duration::from_millis(50),
                shutdown,
            )
            .await;
        });

        // Wait long enough for at least one tick to fire + commit.
        tokio::time::sleep(Duration::from_millis(200)).await;
        trigger.trigger();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        // Verify the store has the upsert.
        let recovered = store.load("p-test").await.unwrap();
        assert!(
            !recovered.state_by_key.is_empty(),
            "ticker should have committed at least one upsert"
        );
    }

    /// Ticker is a no-op when interval is 0 / not set — neither task
    /// gets spawned and `run` doesn't hang.
    #[tokio::test(flavor = "multi_thread")]
    async fn checkpoint_loop_skipped_without_interval() {
        // A 1ms interval ticker with no transform + no sources still
        // exits cleanly on shutdown.
        use crate::state_store::InMemoryStateStore;
        let store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new());
        let (shutdown, trigger) = ShutdownSignal::new();
        let handle = tokio::spawn(async move {
            super::checkpoint_loop(
                store,
                "p".into(),
                None,
                Vec::new(),
                Duration::from_millis(1),
                shutdown,
            )
            .await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        trigger.trigger();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("checkpoint_loop should exit on shutdown")
            .expect("task panicked");
    }

    // --- Phase 39.5a P2.15: transform_on_error policy --------------------

    #[test]
    fn transform_error_policy_default_is_fail() {
        use crate::backend::TargetTable;
        let cfg = StreamingPipelineConfig::new(
            "topic",
            TargetTable {
                schema: "".into(),
                name: "out".into(),
            },
            "p",
        );
        assert_eq!(cfg.transform_on_error, TransformErrorPolicy::Fail);
    }

    #[test]
    fn transform_error_policy_builder_overrides_default() {
        use crate::backend::TargetTable;
        let cfg = StreamingPipelineConfig::new(
            "topic",
            TargetTable {
                schema: "".into(),
                name: "out".into(),
            },
            "p",
        )
        .with_transform_on_error(TransformErrorPolicy::Drop);
        assert_eq!(cfg.transform_on_error, TransformErrorPolicy::Drop);
    }

    // --- Phase 36i: metrics + DLQ -----------------------------------------

    #[test]
    fn metrics_render_initial_text_format() {
        let counters = StreamingPipelineMetricsCounters::new("p");
        let body = counters.render().unwrap();
        // Prometheus text exposition format includes # HELP and
        // # TYPE comments per metric. Spot-check a couple — the
        // exact bytes are unstable across prometheus crate versions
        // but the counter names and label are part of the contract.
        assert!(body.contains("ematix_streaming_rows_consumed_total"));
        assert!(body.contains("ematix_streaming_rows_written_total"));
        assert!(body.contains("ematix_streaming_dlq_writes_total"));
        assert!(body.contains(r#"pipeline="p""#));
    }

    #[test]
    fn metrics_render_reflects_increments() {
        let counters = StreamingPipelineMetricsCounters::new("p");
        counters.rows_consumed.inc_by(7);
        counters.batches.inc();
        let body = counters.render().unwrap();
        assert!(
            body.lines()
                .any(|l| l.starts_with("ematix_streaming_rows_consumed_total")
                    && l.ends_with(" 7")),
            "rows_consumed=7 line missing in:\n{body}"
        );
        assert!(
            body.lines()
                .any(|l| l.starts_with("ematix_streaming_batches_total") && l.ends_with(" 1")),
            "batches=1 line missing in:\n{body}"
        );
    }

    // --- Phase 36j.2: KafkaToKafkaEosPipeline ----------------------------

    #[test]
    fn eos_pipeline_rejects_target_without_exactly_once() {
        use crate::kafka_backend::KafkaBackend;

        let source = Arc::new(KafkaBackend::open("localhost:9092", Some("eos-src-grp")).unwrap());
        // Target with default (AtLeastOnce) delivery semantics should
        // be rejected by KafkaToKafkaEosPipeline::new.
        let target = Arc::new(KafkaBackend::open("localhost:9092", None).unwrap());
        let cfg = StreamingPipelineConfig::new(
            "topic",
            TargetTable {
                schema: "".into(),
                name: "out".into(),
            },
            "p",
        );
        let err = match KafkaToKafkaEosPipeline::new(source, target, cfg) {
            Ok(_) => panic!("expected target-not-EOS rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("ExactlyOnce"), "got: {msg}");
    }

    #[test]
    fn config_with_dead_letter_topic_sets_field() {
        let cfg = StreamingPipelineConfig::new(
            "topic-x",
            TargetTable {
                schema: "raw".into(),
                name: "events".into(),
            },
            "p",
        )
        .with_dead_letter_topic("dlq-events");
        assert_eq!(cfg.dead_letter_topic.as_deref(), Some("dlq-events"));
    }

    // --- Π.4a-3: multi-target fan-out -------------------------------------

    mod multi_target {
        use super::*;
        use crate::backend::{
            ArrowBatchStream, Backend, BackendError, Dialect, StrategyRunResult, TargetTable,
            WriteMode,
        };
        use crate::pg::ConnectionInfo;
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use async_trait::async_trait;
        use std::sync::Mutex;

        /// Test backend: records writes for assertions; emits a
        /// scripted sequence of read batches; can be configured to
        /// fail its next write. Connection-info / strategy methods
        /// panic — they shouldn't be invoked by `StreamingPipeline`.
        struct TestBackend {
            label: String,
            // Source side: pop a batch list per `read_arrow_stream`
            // call; empty when the script is exhausted.
            scripted_reads: Mutex<Vec<Vec<RecordBatch>>>,
            // Target side: records (table.name, n_rows) per write.
            writes: Mutex<Vec<(String, usize)>>,
            // When true, the next `write_arrow_stream` returns Err.
            fail_next_write: Mutex<bool>,
            // Increments on every commit_offsets call.
            commits: Mutex<u32>,
            // Phase Δ PR 5.5: each CDC dispatch records the per-call
            // (table.name, batch row count) here; non-CDC pipelines
            // never write to it. Lets the dispatch wiring be
            // verified without spinning up testcontainers.
            cdc_calls: Mutex<Vec<(String, usize)>>,
            // Synthetic per-call result the stub returns. Defaults
            // to a zero-row "all skipped" result; tests that care
            // about counter propagation override via `set_cdc_result`.
            cdc_result: Mutex<crate::backend::CdcRunResult>,
            // Synthetic schema returned by `reflect_table_spec`.
            // Tests that exercise the CDC dispatch path supply a
            // spec via `set_table_spec`; otherwise the trait
            // default ("not implemented") fires.
            table_spec: Mutex<Option<crate::types::TableSpec>>,
            // Δ.X1.2: PK-column names from the spec each run_cdc
            // call received. Empty inner Vec = no PK on the spec
            // (the case Δ.X1.2 routes around for Delta-style
            // backends that can't reflect PKs).
            cdc_call_pk_names: Mutex<Vec<Vec<String>>>,
        }

        impl TestBackend {
            fn new(label: &str) -> Self {
                Self {
                    label: label.into(),
                    scripted_reads: Mutex::new(Vec::new()),
                    writes: Mutex::new(Vec::new()),
                    fail_next_write: Mutex::new(false),
                    commits: Mutex::new(0),
                    cdc_calls: Mutex::new(Vec::new()),
                    cdc_result: Mutex::new(crate::backend::CdcRunResult::default()),
                    table_spec: Mutex::new(None),
                    cdc_call_pk_names: Mutex::new(Vec::new()),
                }
            }

            fn enqueue_read(&self, batches: Vec<RecordBatch>) {
                self.scripted_reads.lock().unwrap().push(batches);
            }

            fn fail_next(&self) {
                *self.fail_next_write.lock().unwrap() = true;
            }

            fn writes(&self) -> Vec<(String, usize)> {
                self.writes.lock().unwrap().clone()
            }

            fn commit_count(&self) -> u32 {
                *self.commits.lock().unwrap()
            }

            fn cdc_calls(&self) -> Vec<(String, usize)> {
                self.cdc_calls.lock().unwrap().clone()
            }

            fn set_cdc_result(&self, result: crate::backend::CdcRunResult) {
                *self.cdc_result.lock().unwrap() = result;
            }

            fn set_table_spec(&self, spec: crate::types::TableSpec) {
                *self.table_spec.lock().unwrap() = Some(spec);
            }

            fn cdc_call_pk_names(&self) -> Vec<Vec<String>> {
                self.cdc_call_pk_names.lock().unwrap().clone()
            }
        }

        #[async_trait]
        impl Backend for TestBackend {
            fn dialect(&self) -> Dialect {
                Dialect::Postgres
            }
            fn connection_info(&self) -> ConnectionInfo {
                ConnectionInfo {
                    host: self.label.clone(),
                    port: 0,
                    dbname: self.label.clone(),
                    user: "test".into(),
                }
            }
            fn dsn(&self) -> Option<String> {
                None
            }
            async fn ping(&self) -> Result<(), BackendError> {
                Ok(())
            }
            async fn execute(&self, _statement: &str) -> Result<u64, BackendError> {
                Ok(0)
            }
            async fn read_arrow_stream(
                &self,
                _query: &str,
            ) -> Result<ArrowBatchStream, BackendError> {
                let next = if self.scripted_reads.lock().unwrap().is_empty() {
                    Vec::new()
                } else {
                    self.scripted_reads.lock().unwrap().remove(0)
                };
                Ok(Box::pin(futures_util::stream::iter(
                    next.into_iter().map(Ok),
                )))
            }
            async fn write_arrow_stream(
                &self,
                target: &TargetTable,
                stream: ArrowBatchStream,
                _mode: WriteMode,
            ) -> Result<u64, BackendError> {
                let batches: Vec<RecordBatch> = stream.try_collect().await?;
                let n: usize = batches.iter().map(|b| b.num_rows()).sum();
                if std::mem::replace(&mut *self.fail_next_write.lock().unwrap(), false) {
                    return Err(BackendError::Other(format!(
                        "{}: scripted write failure",
                        self.label
                    )));
                }
                self.writes.lock().unwrap().push((target.name.clone(), n));
                Ok(n as u64)
            }
            async fn run_append(
                &self,
                _spec: &crate::types::TableSpec,
                _source_query: &str,
                _pipeline_name: &str,
                _source_backend: Option<&dyn Backend>,
                _incremental_column: Option<&str>,
                _last_value_literal: Option<&str>,
                _dry_run: bool,
            ) -> Result<StrategyRunResult, BackendError> {
                unreachable!("TestBackend::run_append")
            }
            async fn run_truncate(
                &self,
                _spec: &crate::types::TableSpec,
                _source_query: &str,
                _pipeline_name: &str,
                _source_backend: Option<&dyn Backend>,
                _dry_run: bool,
            ) -> Result<StrategyRunResult, BackendError> {
                unreachable!("TestBackend::run_truncate")
            }
            async fn run_merge(
                &self,
                _spec: &crate::types::TableSpec,
                _source_query: &str,
                _keys: &[String],
                _update_columns: &[String],
                _pipeline_name: &str,
                _mode_label: &str,
                _source_backend: Option<&dyn Backend>,
                _delete_handling: Option<crate::backend::DeleteHandling>,
                _dry_run: bool,
            ) -> Result<StrategyRunResult, BackendError> {
                unreachable!("TestBackend::run_merge")
            }
            async fn run_scd2(
                &self,
                _spec: &crate::types::TableSpec,
                _source_query: &str,
                _keys: &[String],
                _compare_columns: &[String],
                _pipeline_name: &str,
                _source_backend: Option<&dyn Backend>,
                _delete_handling: Option<crate::backend::DeleteHandling>,
                _event_timestamp_column: Option<&str>,
                _ttl_seconds: Option<i64>,
                _dry_run: bool,
            ) -> Result<StrategyRunResult, BackendError> {
                unreachable!("TestBackend::run_scd2")
            }
            async fn commit_offsets(&self) -> Result<(), BackendError> {
                *self.commits.lock().unwrap() += 1;
                Ok(())
            }

            async fn run_cdc(
                &self,
                spec: &crate::types::TableSpec,
                batch: RecordBatch,
                _cdc_config: &crate::cdc::CdcConfig,
                _pipeline_name: &str,
            ) -> Result<crate::backend::CdcRunResult, BackendError> {
                self.cdc_calls
                    .lock()
                    .unwrap()
                    .push((self.label.clone(), batch.num_rows()));
                let pks: Vec<String> = spec
                    .columns
                    .iter()
                    .filter(|c| c.primary_key)
                    .map(|c| c.name.clone())
                    .collect();
                self.cdc_call_pk_names.lock().unwrap().push(pks);
                Ok(self.cdc_result.lock().unwrap().clone())
            }

            async fn reflect_table_spec(
                &self,
                target: &TargetTable,
            ) -> Result<crate::types::TableSpec, BackendError> {
                match self.table_spec.lock().unwrap().clone() {
                    Some(spec) => Ok(spec),
                    None => Err(BackendError::Other(format!(
                        "TestBackend({}): no table_spec set for target {}.{}",
                        self.label, target.schema, target.name
                    ))),
                }
            }
        }

        fn one_row_batch(n: i32) -> RecordBatch {
            let schema = Schema::new(vec![Field::new("v", DataType::Int32, false)]);
            let arr = Int32Array::from(vec![n]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arr)]).unwrap()
        }

        /// Phase 39.4 PR 1: a batch with the `_event_ts` column the
        /// pipeline scans when watermarks are enabled.
        fn batch_with_event_ts(values: Vec<i32>, ts_micros: Vec<i64>) -> RecordBatch {
            use arrow_array::TimestampMicrosecondArray;
            let schema = Schema::new(vec![
                Field::new("v", DataType::Int32, false),
                Field::new(
                    "_event_ts",
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
            ]);
            let v_arr = Int32Array::from(values);
            let ts_arr = TimestampMicrosecondArray::from(ts_micros).with_timezone("UTC");
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(v_arr), Arc::new(ts_arr)]).unwrap()
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn fan_out_writes_each_batch_to_every_target() {
            let source = Arc::new(TestBackend::new("src"));
            // Source emits one non-empty batch on the first read,
            // then empty (idle) on subsequent reads.
            source.enqueue_read(vec![one_row_batch(1), one_row_batch(2)]);

            let target_a = Arc::new(TestBackend::new("a"));
            let target_b = Arc::new(TestBackend::new("b"));
            let table_a = TargetTable {
                schema: "".into(),
                name: "warehouse_events".into(),
            };
            let table_b = TargetTable {
                schema: "".into(),
                name: "lake_events".into(),
            };

            let cfg = StreamingPipelineConfig::new("topic", table_a.clone(), "p");
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![
                    (target_a.clone() as Arc<dyn Backend>, table_a),
                    (target_b.clone() as Arc<dyn Backend>, table_b),
                ],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            // Trigger shutdown after a brief delay so the loop runs
            // at least once but exits promptly.
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            let summary = pipeline.run(sig).await.expect("pipeline.run");

            assert_eq!(target_a.writes(), vec![("warehouse_events".into(), 2)]);
            assert_eq!(target_b.writes(), vec![("lake_events".into(), 2)]);
            assert!(summary.shutdown_triggered);
            assert_eq!(summary.total_rows, 2);
            assert_eq!(source.commit_count(), 1, "exactly one offset commit");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn target_failure_skips_offset_commit_when_no_dlq() {
            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![one_row_batch(1)]);

            let target_a = Arc::new(TestBackend::new("a"));
            let target_b = Arc::new(TestBackend::new("b"));
            target_b.fail_next();

            let table_a = TargetTable {
                schema: "".into(),
                name: "warehouse_events".into(),
            };
            let table_b = TargetTable {
                schema: "".into(),
                name: "lake_events".into(),
            };

            let cfg = StreamingPipelineConfig::new("topic", table_a.clone(), "p");
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![
                    (target_a.clone() as Arc<dyn Backend>, table_a),
                    (target_b.clone() as Arc<dyn Backend>, table_b),
                ],
                cfg,
            );

            let (sig, _trigger) = ShutdownSignal::new();
            let err = pipeline
                .run(sig)
                .await
                .expect_err("expected pipeline.run to surface target B's error");
            let msg = err.to_string();
            assert!(
                msg.contains("scripted write failure"),
                "unexpected error: {msg}"
            );
            assert_eq!(
                source.commit_count(),
                0,
                "offsets must NOT advance on target failure when no DLQ configured"
            );
        }

        // --- Π.4b-2: multi-source fan-in (UNION ALL) -------------------------

        #[tokio::test(flavor = "multi_thread")]
        async fn multi_source_concatenates_batches_per_iteration() {
            let source_a = Arc::new(TestBackend::new("src-a"));
            let source_b = Arc::new(TestBackend::new("src-b"));
            // Each source emits one batch on the first iteration.
            source_a.enqueue_read(vec![one_row_batch(1)]);
            source_b.enqueue_read(vec![one_row_batch(2), one_row_batch(3)]);

            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("ignored", table.clone(), "p");
            let pipeline = StreamingPipeline::new_multi_source(
                vec![
                    (source_a.clone() as Arc<dyn Backend>, "topic-a".into()),
                    (source_b.clone() as Arc<dyn Backend>, "topic-b".into()),
                ],
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            let summary = pipeline.run(sig).await.expect("pipeline.run");

            // Three rows total (1 from A + 2 from B) in one
            // iteration, written to the single target.
            assert_eq!(summary.total_rows, 3);
            let total_target_rows: usize = target.writes().iter().map(|(_, n)| *n).sum();
            assert_eq!(total_target_rows, 3);
            // Both sources had their offsets committed.
            assert_eq!(source_a.commit_count(), 1);
            assert_eq!(source_b.commit_count(), 1);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn multi_source_target_failure_skips_all_commits() {
            let source_a = Arc::new(TestBackend::new("src-a"));
            let source_b = Arc::new(TestBackend::new("src-b"));
            source_a.enqueue_read(vec![one_row_batch(1)]);
            source_b.enqueue_read(vec![one_row_batch(2)]);

            let target = Arc::new(TestBackend::new("t"));
            target.fail_next();
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("ignored", table.clone(), "p");
            let pipeline = StreamingPipeline::new_multi_source(
                vec![
                    (source_a.clone() as Arc<dyn Backend>, "topic-a".into()),
                    (source_b.clone() as Arc<dyn Backend>, "topic-b".into()),
                ],
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, _trigger) = ShutdownSignal::new();
            let err = pipeline.run(sig).await.expect_err("target failure bubbles");
            assert!(err.to_string().contains("scripted write failure"));
            // Neither source's offsets advance — both will re-deliver
            // the same batch on restart (at-least-once across all
            // sources).
            assert_eq!(source_a.commit_count(), 0);
            assert_eq!(source_b.commit_count(), 0);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn multi_source_partial_idle_still_processes_non_empty_source() {
            // Source A has data; source B is idle. The iteration
            // should still process A's rows and commit both sources.
            let source_a = Arc::new(TestBackend::new("src-a"));
            let source_b = Arc::new(TestBackend::new("src-b"));
            source_a.enqueue_read(vec![one_row_batch(1)]);
            // source_b: nothing enqueued → empty.

            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("ignored", table.clone(), "p");
            let pipeline = StreamingPipeline::new_multi_source(
                vec![
                    (source_a.clone() as Arc<dyn Backend>, "topic-a".into()),
                    (source_b.clone() as Arc<dyn Backend>, "topic-b".into()),
                ],
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            let summary = pipeline.run(sig).await.expect("pipeline.run");

            assert_eq!(summary.total_rows, 1);
            assert_eq!(source_a.commit_count(), 1);
            assert_eq!(source_b.commit_count(), 1);
        }

        // --- Π.4b-1: SQL transform integration -------------------------------

        #[tokio::test(flavor = "multi_thread")]
        async fn transform_filters_rows_before_target_write() {
            use crate::transform::LazySqlTransform;

            let source = Arc::new(TestBackend::new("src"));
            // Three input rows; transform keeps `v >= 2` → 2 rows
            // arrive at the target.
            source.enqueue_read(vec![one_row_batch(1), one_row_batch(2), one_row_batch(3)]);

            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };

            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "p").with_transform(
                Arc::new(LazySqlTransform::new("SELECT v FROM source WHERE v >= 2")),
            );
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            let summary = pipeline.run(sig).await.expect("pipeline.run");

            // Only the rows that survived the WHERE clause hit the
            // target. The DataFusion path may consolidate per-batch
            // outputs into one or more batches — sum across them.
            let total_target_rows: usize = target.writes().iter().map(|(_, n)| *n).sum();
            assert_eq!(total_target_rows, 2, "WHERE v >= 2 keeps 2 of 3 rows");
            assert_eq!(summary.total_rows, 2, "summary counts post-transform rows");
            assert_eq!(source.commit_count(), 1);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn transform_none_path_unchanged() {
            // Belt-and-suspenders: pipeline with `transform = None`
            // must behave exactly like today's path (the existing
            // fan-out test already covers this, but pinning it here
            // makes the regression test obvious if the match arm
            // shifts).
            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![one_row_batch(1), one_row_batch(2)]);

            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };

            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "p");
            assert!(cfg.transform.is_none());

            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );
            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            let summary = pipeline.run(sig).await.expect("pipeline.run");
            assert_eq!(summary.total_rows, 2);
            assert_eq!(target.writes(), vec![("events".into(), 2)]);
        }

        // --- Phase 39.4 PR 1: watermark machinery ----------------------------

        #[test]
        fn watermark_disabled_by_default() {
            let cfg = StreamingPipelineConfig::new(
                "topic",
                TargetTable {
                    schema: "".into(),
                    name: "events".into(),
                },
                "p",
            );
            assert!(cfg.watermark.is_none(), "default config has no watermark");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn watermark_extracted_from_event_ts_when_enabled() {
            // Single source emits one batch with _event_ts microseconds
            // 100 and 200. With lateness=0 the per-source watermark
            // is 200µs = 0.0002s; that's the global wm too.
            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![batch_with_event_ts(vec![1, 2], vec![100, 200])]);

            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "p")
                .with_watermark(WatermarkConfig::default());
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            let _ = pipeline.run(sig).await.expect("pipeline.run");

            let body = pipeline.metrics.render().unwrap();
            assert!(
                body.contains(r#"source="topic""#),
                "per-source gauge missing in:\n{body}"
            );
            assert!(
                body.contains(r#"source="_global""#),
                "global gauge missing in:\n{body}"
            );
            assert!(
                body.lines()
                    .any(|l| { l.contains(r#"source="_global""#) && l.contains("0.0002") }),
                "global wm should be 0.0002s in:\n{body}"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn multi_source_watermark_takes_min_over_non_idle() {
            // Two sources; source A emits ts=1000, source B emits ts=500.
            // Both still considered active (idleness window is 60s).
            // Global wm = min(1000, 500) = 500µs = 0.0005s.
            let src_a = Arc::new(TestBackend::new("a"));
            let src_b = Arc::new(TestBackend::new("b"));
            src_a.enqueue_read(vec![batch_with_event_ts(vec![1], vec![1000])]);
            src_b.enqueue_read(vec![batch_with_event_ts(vec![2], vec![500])]);

            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("ignored", table.clone(), "p")
                .with_watermark(WatermarkConfig::default());
            let pipeline = StreamingPipeline::new_multi_source(
                vec![
                    (src_a.clone() as Arc<dyn Backend>, "topic-a".into()),
                    (src_b.clone() as Arc<dyn Backend>, "topic-b".into()),
                ],
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            let _ = pipeline.run(sig).await.expect("pipeline.run");

            let body = pipeline.metrics.render().unwrap();
            assert!(
                body.lines()
                    .any(|l| { l.contains(r#"source="_global""#) && l.contains("0.0005") }),
                "global wm = min(1000, 500) µs = 0.0005s; got:\n{body}"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn watermark_subtracts_lateness_ms() {
            // event_ts = 1_000_000 µs (1 second). lateness = 250 ms.
            // wm = 1_000_000 − 250_000 = 750_000 µs = 0.75 s.
            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![batch_with_event_ts(vec![1], vec![1_000_000])]);

            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "p").with_watermark(
                WatermarkConfig {
                    lateness_ms: 250,
                    source_idleness_ms: 60_000,
                },
            );
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            let _ = pipeline.run(sig).await.expect("pipeline.run");

            let body = pipeline.metrics.render().unwrap();
            assert!(
                body.lines()
                    .any(|l| { l.contains(r#"source="_global""#) && l.contains("0.75") }),
                "global wm with lateness 250ms should be 0.75s; got:\n{body}"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn end_to_end_windowed_aggregation() {
            // End-to-end through StreamingPipeline::run:
            //   - Source emits two batches in window [0, 60s) and a
            //     third in window [60s, 120s). The third batch's
            //     _event_ts advances the watermark past 60s, which
            //     triggers emit of window [0, 60s) inside transform().
            //   - Target should record exactly one write of size 1
            //     (one aggregated row for the single user_id=42).
            use crate::transform::BatchTransform;
            use crate::windowed::{
                AggKind, AggregationSpec, LateDataPolicy, WindowConfig, WindowKind,
                WindowedAggregateTransform,
            };

            let source = Arc::new(TestBackend::new("src"));
            // All three batches: user_id=42, amount=10/20/30, ts in
            // window [0, 60s) for the first two, ts past 60s for the third.
            source.enqueue_read(vec![batch_with_user_amount_ts(
                vec![42],
                vec![Some(10)],
                vec![1_000_000],
            )]);
            source.enqueue_read(vec![batch_with_user_amount_ts(
                vec![42],
                vec![Some(20)],
                vec![2_000_000],
            )]);
            source.enqueue_read(vec![batch_with_user_amount_ts(
                vec![42],
                vec![Some(30)],
                vec![70_000_000], // > 60s → triggers emit of [0, 60s)
            )]);

            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };

            let window_cfg = WindowConfig {
                kind: WindowKind::Tumbling,
                duration_ms: 60_000,
                hop_ms: 60_000,
                event_time_column: "_event_ts".into(),
                group_by: vec!["user_id".into()],
                aggregations: vec![
                    AggregationSpec::new(AggKind::CountStar, None, "n"),
                    AggregationSpec::new(AggKind::Sum, Some("amount".into()), "amount_sum"),
                ],
                late_data: LateDataPolicy::Drop,
                max_groups_per_window: 100,
                window_start_column: "window_start".into(),
                window_end_column: "window_end".into(),
                session_id_column: "session_id".into(),
                gap_ms: None,
                max_session_duration_ms: None,
            };
            let windowed: Arc<dyn BatchTransform> =
                Arc::new(WindowedAggregateTransform::new(window_cfg, None).unwrap());

            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "p")
                .with_transform(windowed)
                .with_watermark(WatermarkConfig::default());
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                trigger.trigger();
            });
            let summary = pipeline.run(sig).await.expect("pipeline.run");

            // The first window [0, 60s) emitted as part of the
            // third batch's transform() call. The third batch's row
            // contributed to window [60s, 120s) which doesn't emit
            // until watermark crosses 120s — never happens in this
            // test (source goes idle, no further wm advance).
            let writes = target.writes();
            assert_eq!(
                writes.len(),
                1,
                "exactly one window emit expected; got {writes:?}"
            );
            let (table_name, n_rows) = &writes[0];
            assert_eq!(table_name, "events");
            assert_eq!(*n_rows, 1, "one aggregated row (user_id=42)");
            assert_eq!(summary.total_rows, 1);
        }

        /// Helper: 3-column batch shaped (user_id, amount, _event_ts)
        /// matching the windowed integration test's expectations.
        fn batch_with_user_amount_ts(
            user_ids: Vec<i64>,
            amounts: Vec<Option<i64>>,
            ts_micros: Vec<i64>,
        ) -> RecordBatch {
            use arrow_array::{Int64Array, TimestampMicrosecondArray};
            let schema = Schema::new(vec![
                Field::new("user_id", DataType::Int64, false),
                Field::new("amount", DataType::Int64, true),
                Field::new(
                    "_event_ts",
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
            ]);
            RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(Int64Array::from(user_ids)),
                    Arc::new(Int64Array::from(amounts)),
                    Arc::new(TimestampMicrosecondArray::from(ts_micros).with_timezone("UTC")),
                ],
            )
            .unwrap()
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn watermark_disabled_does_not_publish_gauges() {
            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![batch_with_event_ts(vec![1, 2], vec![100, 200])]);

            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            // No `.with_watermark(...)` — watermark is disabled.
            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "p");
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            let _ = pipeline.run(sig).await.expect("pipeline.run");

            let body = pipeline.metrics.render().unwrap();
            // The gauge is registered (so the metric family exists in
            // the registry's HELP/TYPE lines) but no values get
            // emitted because the publish path early-returns. Look
            // for the absence of any actual gauge sample line.
            let any_sample = body
                .lines()
                .any(|l| l.starts_with("ematix_streaming_watermark_seconds{"));
            assert!(
                !any_sample,
                "no watermark samples should be emitted when watermark is disabled; got:\n{body}"
            );
        }

        /// Phase Δ PR 5.5: when `[transform.cdc]` is configured, the
        /// streaming pipeline routes each batch through
        /// `Backend::run_cdc` instead of `write_arrow_stream`. This
        /// test asserts the dispatch wiring + the per-counter metric
        /// fold-in. Real Postgres execution of `run_cdc` is covered
        /// by `tests/integration_pg.rs`.
        #[tokio::test(flavor = "multi_thread")]
        async fn cdc_dispatch_routes_to_run_cdc() {
            use crate::backend::CdcRunResult;
            use crate::cdc::{CdcConfig, EnvelopeKind};
            use crate::types::{ColumnSpec, ColumnType, TableSpec};

            let source = Arc::new(TestBackend::new("src"));
            // Two scripted batches → two CDC dispatches expected.
            source.enqueue_read(vec![one_row_batch(1)]);
            source.enqueue_read(vec![one_row_batch(2)]);

            let target = Arc::new(TestBackend::new("mirror"));
            target.set_table_spec(TableSpec {
                schema: "public".into(),
                name: "customers".into(),
                columns: vec![ColumnSpec {
                    name: "id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                }],
                unique_constraints: Vec::new(),
                fingerprint: String::new(),
            });
            // Per-call result the stub returns. Mix of every counter
            // so we can verify each one folds into the pipeline's
            // metrics.
            target.set_cdc_result(CdcRunResult {
                run_id: "fake".into(),
                creates: 3,
                updates: 2,
                deletes: 1,
                skipped: 4,
                idempotent_skipped: 5,
            });

            let table = TargetTable {
                schema: "public".into(),
                name: "customers".into(),
            };
            let cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "p").with_cdc(cdc);
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            let summary = pipeline.run(sig).await.expect("pipeline.run");

            // Two scripted batches → two run_cdc invocations on the
            // target. write_arrow_stream must not have been called.
            assert_eq!(
                target.cdc_calls(),
                vec![("mirror".into(), 1), ("mirror".into(), 1)],
                "every CDC batch routed through run_cdc"
            );
            assert!(
                target.writes().is_empty(),
                "CDC pipeline must not fall through to write_arrow_stream"
            );
            assert!(summary.shutdown_triggered);

            // Counters fold across both dispatches: each call
            // returned the same (3, 2, 1, 4, 5) shape, two calls →
            // doubled.
            let body = pipeline.metrics.render().unwrap();
            assert!(
                body.contains("ematix_streaming_cdc_creates_total") && body.contains(" 6"),
                "creates counter not visible in metrics body:\n{body}"
            );
            assert!(
                body.contains("ematix_streaming_cdc_updates_total") && body.contains(" 4"),
                "updates counter not visible:\n{body}"
            );
            assert!(
                body.contains("ematix_streaming_cdc_deletes_total") && body.contains(" 2"),
                "deletes counter not visible:\n{body}"
            );
            assert!(
                body.contains("ematix_streaming_cdc_idempotent_skipped_total")
                    && body.contains(" 10"),
                "idempotent_skipped counter not visible:\n{body}"
            );
            assert!(
                body.contains("ematix_streaming_cdc_skipped_total") && body.contains(" 8"),
                "skipped counter not visible:\n{body}"
            );
        }

        /// Phase Δ PR 5.5: a CDC pipeline whose target backend
        /// can't reflect its schema (default trait impl error)
        /// must surface that as a pipeline error, not silently
        /// fall through to the universal write path.
        #[tokio::test(flavor = "multi_thread")]
        async fn cdc_dispatch_errors_when_target_cant_reflect() {
            use crate::cdc::{CdcConfig, EnvelopeKind};

            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![one_row_batch(1)]);

            // Target deliberately has no table_spec configured →
            // `reflect_table_spec` returns Err.
            let target = Arc::new(TestBackend::new("mirror"));

            let table = TargetTable {
                schema: "public".into(),
                name: "customers".into(),
            };
            let cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "p").with_cdc(cdc);
            let pipeline = StreamingPipeline::new(
                source as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, _trigger) = ShutdownSignal::new();
            let err = pipeline
                .run(sig)
                .await
                .expect_err("reflection failure must surface");
            let msg = err.to_string();
            assert!(
                msg.contains("no table_spec set"),
                "expected reflection-failure message, got: {msg}"
            );
            assert!(
                target.cdc_calls().is_empty(),
                "no CDC apply attempted when reflection fails"
            );
        }

        /// Δ.X1.2: when a target's `reflect_table_spec` returns
        /// columns with `primary_key = false` (the Delta case —
        /// Delta tables don't carry PK constraints), the user's
        /// declared `target_primary_keys` augments the reflected
        /// spec before it's handed to `run_cdc`. Without this
        /// path, Delta-style backends are unusable from the
        /// streaming runtime.
        #[tokio::test(flavor = "multi_thread")]
        async fn cdc_dispatch_augments_pk_from_user_declaration() {
            use crate::backend::CdcRunResult;
            use crate::cdc::{CdcConfig, EnvelopeKind};
            use crate::types::{ColumnSpec, ColumnType, TableSpec};

            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![one_row_batch(1)]);

            let target = Arc::new(TestBackend::new("mirror"));
            // Delta-style reflection: every column reports
            // primary_key = false because the backend can't see
            // PK constraints natively.
            target.set_table_spec(TableSpec {
                schema: "public".into(),
                name: "customers".into(),
                columns: vec![
                    ColumnSpec {
                        name: "id".into(),
                        ty: ColumnType::BigInt,
                        nullable: false,
                        primary_key: false,
                    },
                    ColumnSpec {
                        name: "email".into(),
                        ty: ColumnType::Text,
                        nullable: true,
                        primary_key: false,
                    },
                ],
                unique_constraints: Vec::new(),
                fingerprint: String::new(),
            });
            target.set_cdc_result(CdcRunResult {
                run_id: "fake".into(),
                creates: 1,
                ..Default::default()
            });

            let table = TargetTable {
                schema: "public".into(),
                name: "customers".into(),
            };
            let cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "p")
                .with_cdc(cdc)
                .with_target_primary_keys(vec![vec!["id".to_string()]]);
            let pipeline = StreamingPipeline::new(
                source as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            pipeline.run(sig).await.expect("pipeline.run");

            let pk_names = target.cdc_call_pk_names();
            assert_eq!(
                pk_names,
                vec![vec!["id".to_string()]],
                "user-declared PK must be applied to the spec before run_cdc \
                 even though reflect_table_spec returned no PK columns"
            );
        }

        /// Δ.X1.2: a user-declared PK column that doesn't match
        /// any reflected column is a config error — fail loud,
        /// not silent. Otherwise typos hide as "no PK detected".
        #[tokio::test(flavor = "multi_thread")]
        async fn cdc_dispatch_errors_when_declared_pk_missing_from_schema() {
            use crate::cdc::{CdcConfig, EnvelopeKind};
            use crate::types::{ColumnSpec, ColumnType, TableSpec};

            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![one_row_batch(1)]);

            let target = Arc::new(TestBackend::new("mirror"));
            target.set_table_spec(TableSpec {
                schema: "public".into(),
                name: "customers".into(),
                columns: vec![ColumnSpec {
                    name: "id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: false,
                }],
                unique_constraints: Vec::new(),
                fingerprint: String::new(),
            });

            let table = TargetTable {
                schema: "public".into(),
                name: "customers".into(),
            };
            let cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "p")
                .with_cdc(cdc)
                .with_target_primary_keys(vec![vec!["customer_id".to_string()]]);
            let pipeline = StreamingPipeline::new(
                source as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, _trigger) = ShutdownSignal::new();
            let err = pipeline
                .run(sig)
                .await
                .expect_err("typo'd PK declaration must error");
            let msg = err.to_string();
            assert!(
                msg.contains("customer_id"),
                "error must name the missing PK column, got: {msg}"
            );
            assert!(
                target.cdc_calls().is_empty(),
                "no CDC apply attempted when PK declaration is invalid"
            );
        }

        // --- DLQ Phase 1: emission rewire through DeadLetterStore ---------

        use crate::dlq::{DeadLetterStore, DlqSelection, DlqStage, TableDlq};

        /// Transform that always errors — drives the
        /// `transform_on_error = Dlq` emission site.
        #[derive(Debug)]
        struct FailingTransform;

        #[async_trait]
        impl crate::transform::BatchTransform for FailingTransform {
            fn input_schema(&self) -> arrow_schema::SchemaRef {
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
            }
            fn output_schema(&self) -> arrow_schema::SchemaRef {
                self.input_schema()
            }
            async fn transform(
                &self,
                _input: RecordBatch,
                _ctx: &crate::transform::BatchContext,
            ) -> Result<Vec<RecordBatch>, BackendError> {
                Err(BackendError::Other("scripted transform failure".into()))
            }
        }

        /// Write failure with an explicit dead-letter store: the
        /// batch lands in the store with full `DlqMeta`, offsets
        /// still commit (after the append), and the pipeline
        /// continues instead of erroring.
        #[tokio::test(flavor = "multi_thread")]
        async fn write_failure_routes_to_explicit_store_with_meta() {
            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![one_row_batch(7), one_row_batch(8)]);

            let target = Arc::new(TestBackend::new("t"));
            target.fail_next();
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };

            let dlq: Arc<dyn DeadLetterStore> =
                Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
            let cfg = StreamingPipelineConfig::new("orders-topic", table.clone(), "dlq-p")
                .with_dead_letter_store(Arc::clone(&dlq));
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            pipeline.run(sig).await.expect("DLQ absorbs the failure");

            assert_eq!(
                dlq.depth("dlq-p").await.unwrap().pending,
                2,
                "both failed rows dead-lettered (one record per row)"
            );
            let records = dlq.browse("dlq-p", 0, 10, None).await.unwrap();
            assert_eq!(records.len(), 2);
            for r in &records {
                assert_eq!(r.meta.pipeline, "dlq-p");
                assert_eq!(r.meta.stage, DlqStage::Write);
                assert!(
                    r.meta.error.contains("scripted write failure"),
                    "error carries the target failure: {}",
                    r.meta.error
                );
                assert_eq!(r.meta.source_id, "orders-topic");
                assert_eq!(r.meta.attempt, 1);
                assert_eq!(r.meta.payload_format, "json");
                assert!(r.meta.failed_at > 0, "failed_at populated");
            }
            // Format preservation (non-Kafka source ⇒ JSONL): the
            // original row round-trips as its JSON wire form.
            let payloads: Vec<String> = records
                .iter()
                .map(|r| String::from_utf8(r.payload.clone()).unwrap())
                .collect();
            assert!(payloads.contains(&"{\"v\":7}".to_string()), "{payloads:?}");
            assert!(payloads.contains(&"{\"v\":8}".to_string()), "{payloads:?}");

            assert_eq!(
                source.commit_count(),
                1,
                "offsets commit AFTER the DLQ append ack (at-least-once preserved)"
            );
            assert_eq!(pipeline.metrics.dlq_writes.get(), 2, "dlq_writes counted");
        }

        /// Transform error under `on_error = "dlq"` routes the
        /// ORIGINAL (pre-transform) batch with stage = Transform.
        #[tokio::test(flavor = "multi_thread")]
        async fn transform_error_dlq_policy_routes_original_batch() {
            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![one_row_batch(42)]);

            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };

            let dlq: Arc<dyn DeadLetterStore> =
                Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
            let cfg = StreamingPipelineConfig::new("orders-topic", table.clone(), "tf-p")
                .with_transform(Arc::new(FailingTransform))
                .with_transform_on_error(TransformErrorPolicy::Dlq)
                .with_dead_letter_store(Arc::clone(&dlq));
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![(target.clone() as Arc<dyn Backend>, table)],
                cfg,
            );

            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            pipeline.run(sig).await.expect("DLQ absorbs the failure");

            let records = dlq.browse("tf-p", 0, 10, None).await.unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].meta.stage, DlqStage::Transform);
            assert!(
                records[0].meta.error.contains("scripted transform failure"),
                "{}",
                records[0].meta.error
            );
            assert_eq!(
                String::from_utf8(records[0].payload.clone()).unwrap(),
                "{\"v\":42}",
                "the PRE-transform input batch is what dead-letters"
            );
            assert!(
                target.writes().is_empty(),
                "nothing reached the target for the failed batch"
            );
        }

        /// The records a store hands back are takeable — the Phase 2
        /// replay engine sees exactly what the pipeline emitted.
        #[tokio::test(flavor = "multi_thread")]
        async fn emitted_records_are_takeable_for_replay() {
            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![one_row_batch(1)]);
            let target = Arc::new(TestBackend::new("t"));
            target.fail_next();
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let dlq: Arc<dyn DeadLetterStore> =
                Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
            let cfg = StreamingPipelineConfig::new("q", table.clone(), "rp")
                .with_dead_letter_store(Arc::clone(&dlq));
            let pipeline = StreamingPipeline::new(
                source as Arc<dyn Backend>,
                vec![(target as Arc<dyn Backend>, table)],
                cfg,
            );
            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            pipeline.run(sig).await.unwrap();

            let taken = dlq
                .take_for_replay("rp", DlqSelection::All, Duration::from_secs(60), 1)
                .await
                .unwrap();
            assert_eq!(taken.len(), 1);
        }

        /// Behavior pin: with NO DLQ configured (no topic, no store)
        /// a write failure still propagates and offsets do not
        /// commit — byte-identical to pre-Phase-1 (see also
        /// `target_failure_skips_offset_commit_when_no_dlq` above,
        /// which this complements for the single-target shape).
        #[tokio::test(flavor = "multi_thread")]
        async fn no_dlq_config_write_failure_still_propagates() {
            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![one_row_batch(1)]);
            let target = Arc::new(TestBackend::new("t"));
            target.fail_next();
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("q", table.clone(), "plain");
            let pipeline = StreamingPipeline::new(
                source.clone() as Arc<dyn Backend>,
                vec![(target as Arc<dyn Backend>, table)],
                cfg,
            );
            let (sig, _trigger) = ShutdownSignal::new();
            let err = pipeline.run(sig).await.expect_err("must propagate");
            assert!(err.to_string().contains("scripted write failure"));
            assert_eq!(source.commit_count(), 0);
        }

        /// Resolution rule 2: explicit topic + Kafka source →
        /// KafkaTopicDlq (constructing a KafkaBackend does not
        /// contact the broker, so this is a pure unit test).
        #[tokio::test(flavor = "multi_thread")]
        async fn resolution_prefers_kafka_topic_store() {
            let kafka: Arc<dyn Backend> =
                Arc::new(KafkaBackend::open("localhost:9092", Some("g")).unwrap());
            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "kp")
                .with_dead_letter_topic("dlq-topic");
            let pipeline =
                StreamingPipeline::new(kafka, vec![(target as Arc<dyn Backend>, table)], cfg);
            let store = pipeline.resolve_dlq_store().await.unwrap();
            let debug = format!("{store:?}");
            assert!(debug.contains("KafkaTopicDlq"), "resolved: {debug}");
            assert!(debug.contains("dlq-topic"), "resolved: {debug}");
        }

        /// Resolution rule 3: a state store that provides a DLQ on
        /// its family wins over the in-memory fallback.
        #[tokio::test(flavor = "multi_thread")]
        async fn resolution_uses_state_store_family() {
            #[derive(Debug)]
            struct DlqProvidingStateStore(Arc<dyn DeadLetterStore>);
            #[async_trait]
            impl crate::state_store::StateStore for DlqProvidingStateStore {
                async fn load(
                    &self,
                    _pipeline: &str,
                ) -> Result<crate::state_store::RecoveredState, BackendError> {
                    Ok(Default::default())
                }
                async fn commit(
                    &self,
                    _pipeline: &str,
                    _snapshot: crate::state_store::CommitSnapshot,
                ) -> Result<(), BackendError> {
                    Ok(())
                }
                async fn dead_letter_store(
                    &self,
                ) -> Result<Option<Arc<dyn DeadLetterStore>>, BackendError> {
                    Ok(Some(Arc::clone(&self.0)))
                }
            }

            let family_dlq: Arc<dyn DeadLetterStore> =
                Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
            let source = Arc::new(TestBackend::new("src"));
            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("q", table.clone(), "fam")
                .with_state_store(Arc::new(DlqProvidingStateStore(Arc::clone(&family_dlq))));
            let pipeline = StreamingPipeline::new(
                source as Arc<dyn Backend>,
                vec![(target as Arc<dyn Backend>, table)],
                cfg,
            );
            let resolved = pipeline.resolve_dlq_store().await.unwrap();
            assert!(
                Arc::ptr_eq(resolved, &family_dlq),
                "the state store's family DLQ must be used verbatim"
            );
        }

        /// Resolution rule 4: nothing configured → LOUD in-memory
        /// SQLite fallback (the `on_error = dlq` batch is preserved
        /// in-process instead of silently discarded as pre-Phase-1).
        #[tokio::test(flavor = "multi_thread")]
        async fn resolution_falls_back_to_in_memory_table() {
            let source = Arc::new(TestBackend::new("src"));
            source.enqueue_read(vec![one_row_batch(5)]);
            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("q", table.clone(), "fb")
                .with_transform(Arc::new(FailingTransform))
                .with_transform_on_error(TransformErrorPolicy::Dlq);
            let pipeline = StreamingPipeline::new(
                source as Arc<dyn Backend>,
                vec![(target as Arc<dyn Backend>, table)],
                cfg,
            );
            let (sig, trigger) = ShutdownSignal::new();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                trigger.trigger();
            });
            pipeline.run(sig).await.expect("fallback DLQ absorbs");
            assert_eq!(
                pipeline.metrics.dlq_writes.get(),
                1,
                "the failed batch dead-lettered into the in-memory fallback"
            );
            let store = pipeline.resolve_dlq_store().await.unwrap();
            assert_eq!(store.depth("fb").await.unwrap().pending, 1);
        }

        // --- DLQ Phase 4: dlq_store mode (auto | topic | table) ------------
        //
        // TDD note: written FIRST, red, before `DlqStoreMode` had any
        // effect on `resolve_dlq_store` — same discipline as the
        // earlier phases.

        /// `dlq_store = "table"` skips rule 2 even when a Kafka
        /// source + dead_letter_topic are configured — the operator
        /// asked for the browsable/leasable table family.
        #[tokio::test(flavor = "multi_thread")]
        async fn dlq_store_mode_table_overrides_topic_resolution() {
            let kafka: Arc<dyn Backend> =
                Arc::new(KafkaBackend::open("localhost:9092", Some("g")).unwrap());
            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "mode-table")
                .with_dead_letter_topic("dlq-topic")
                .with_dlq_store(crate::dlq::DlqStoreMode::Table);
            let pipeline =
                StreamingPipeline::new(kafka, vec![(target as Arc<dyn Backend>, table)], cfg);
            let store = pipeline.resolve_dlq_store().await.unwrap();
            let debug = format!("{store:?}");
            assert!(
                !debug.contains("KafkaTopicDlq"),
                "table mode must not resolve the topic store: {debug}"
            );
            assert!(debug.contains("TableDlq"), "resolved: {debug}");
        }

        /// `dlq_store = "topic"` is an explicit demand — a non-Kafka
        /// source can't satisfy it, so resolution hard-errors
        /// instead of silently falling back to the table family.
        #[tokio::test(flavor = "multi_thread")]
        async fn dlq_store_mode_topic_requires_kafka_source() {
            let source = Arc::new(TestBackend::new("src"));
            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("q", table.clone(), "mode-topic")
                .with_dead_letter_topic("dlq-topic")
                .with_dlq_store(crate::dlq::DlqStoreMode::Topic);
            let pipeline = StreamingPipeline::new(
                source as Arc<dyn Backend>,
                vec![(target as Arc<dyn Backend>, table)],
                cfg,
            );
            let err = pipeline.resolve_dlq_store().await.unwrap_err();
            assert!(
                err.to_string().contains("Kafka"),
                "typed error names the Kafka requirement: {err}"
            );
        }

        /// `dlq_store = "topic"` without a `dead_letter_topic` is a
        /// config contradiction — hard error, not a fallback.
        #[tokio::test(flavor = "multi_thread")]
        async fn dlq_store_mode_topic_requires_topic_config() {
            let kafka: Arc<dyn Backend> =
                Arc::new(KafkaBackend::open("localhost:9092", Some("g")).unwrap());
            let target = Arc::new(TestBackend::new("t"));
            let table = TargetTable {
                schema: "".into(),
                name: "events".into(),
            };
            let cfg = StreamingPipelineConfig::new("topic", table.clone(), "mode-topic-2")
                .with_dlq_store(crate::dlq::DlqStoreMode::Topic);
            let pipeline =
                StreamingPipeline::new(kafka, vec![(target as Arc<dyn Backend>, table)], cfg);
            let err = pipeline.resolve_dlq_store().await.unwrap_err();
            assert!(
                err.to_string().contains("dead_letter_topic"),
                "typed error names the missing topic: {err}"
            );
        }

        /// Config defaults pin: mode = Auto (historical resolution),
        /// max attempts = the Phase 2 replay default.
        #[test]
        fn dlq_config_defaults() {
            let cfg = StreamingPipelineConfig::new(
                "q",
                TargetTable {
                    schema: "".into(),
                    name: "events".into(),
                },
                "p",
            );
            assert_eq!(cfg.dlq_store, crate::dlq::DlqStoreMode::Auto);
            assert_eq!(cfg.dlq_max_attempts, crate::dlq::DEFAULT_MAX_ATTEMPTS);
            let cfg = cfg.with_dlq_max_attempts(5);
            assert_eq!(cfg.dlq_max_attempts, 5);
        }

        /// Mode string round-trip for the TOML/Python plumbing.
        #[test]
        fn dlq_store_mode_round_trips() {
            use crate::dlq::DlqStoreMode;
            for m in [DlqStoreMode::Auto, DlqStoreMode::Topic, DlqStoreMode::Table] {
                assert_eq!(DlqStoreMode::parse(m.as_str()), Some(m));
            }
            assert_eq!(DlqStoreMode::parse("bogus"), None);
        }

        // --- DLQ Phase 2: replay engine (redrive through the pipeline) ----
        //
        // TDD note: this suite was committed FIRST, red, against a
        // `run_dlq_replay` stub that returns an "unimplemented"
        // error — same discipline as Phase 1's contract suite.

        mod dlq_replay {
            use super::*;
            use crate::dlq::{DlqDepth, DlqError, DlqRecordStatus, DlqSelection, ReplayOptions};
            use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

            /// A record shaped exactly like the emission path writes
            /// for a non-Kafka source: JSONL payload,
            /// `payload_format = "json"`, attempt 1.
            fn seeded_json_record(pipeline: &str, n: u32) -> DlqRecord {
                DlqRecord {
                    id: DlqRecordId(format!("{pipeline}-seed-{n:04}")),
                    meta: DlqMeta {
                        pipeline: pipeline.to_string(),
                        stage: DlqStage::Write,
                        error: "seeded failure".into(),
                        source_id: "seed-src".into(),
                        offset_bytes: None,
                        event_ts: None,
                        failed_at: 1_700_000_000_000,
                        attempt: 1,
                        payload_format: "json".into(),
                    },
                    payload: format!("{{\"v\": {n}}}").into_bytes(),
                }
            }

            /// (target, pipeline) with an explicit DLQ store and an
            /// optional transform. The target's writes are the
            /// replay's observable output.
            fn mk_pipeline(
                pipeline: &str,
                dlq: Arc<dyn DeadLetterStore>,
                transform: Option<Arc<dyn crate::transform::BatchTransform>>,
            ) -> (Arc<TestBackend>, StreamingPipeline) {
                let source = Arc::new(TestBackend::new("src"));
                let target = Arc::new(TestBackend::new("tgt"));
                let table = TargetTable {
                    schema: "".into(),
                    name: "events".into(),
                };
                let mut cfg = StreamingPipelineConfig::new("seed-src", table.clone(), pipeline)
                    .with_dead_letter_store(dlq);
                if let Some(t) = transform {
                    cfg = cfg.with_transform(t);
                }
                let p = StreamingPipeline::new(
                    source as Arc<dyn Backend>,
                    vec![(Arc::clone(&target) as Arc<dyn Backend>, table)],
                    cfg,
                );
                (target, p)
            }

            /// The PRD round trip on the table family, end to end
            /// through the real emission path: sink fails (one-shot)
            /// → rows dead-letter → sink is fixed → replay All →
            /// rows present at the target, DLQ drained, report
            /// counts exact.
            #[tokio::test(flavor = "multi_thread")]
            async fn replay_round_trip_after_sink_fix() {
                let source = Arc::new(TestBackend::new("src"));
                source.enqueue_read(vec![one_row_batch(7), one_row_batch(8)]);
                let target = Arc::new(TestBackend::new("tgt"));
                target.fail_next();
                let table = TargetTable {
                    schema: "".into(),
                    name: "events".into(),
                };
                let dlq: Arc<dyn DeadLetterStore> =
                    Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
                let cfg = StreamingPipelineConfig::new("orders-topic", table.clone(), "rt-p")
                    .with_dead_letter_store(Arc::clone(&dlq));
                let pipeline = StreamingPipeline::new(
                    source.clone() as Arc<dyn Backend>,
                    vec![(target.clone() as Arc<dyn Backend>, table)],
                    cfg,
                );

                // 1. Sink fails → both rows dead-letter.
                let (sig, trigger) = ShutdownSignal::new();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    trigger.trigger();
                });
                pipeline.run(sig).await.unwrap();
                assert_eq!(dlq.depth("rt-p").await.unwrap().pending, 2);
                assert!(target.writes().is_empty());

                // 2. Sink is "fixed" (fail_next is one-shot). Replay.
                let report = pipeline
                    .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
                    .await
                    .unwrap();
                assert_eq!(
                    (
                        report.taken,
                        report.succeeded,
                        report.redeadlettered,
                        report.parked
                    ),
                    (2, 2, 0, 0),
                    "exact report counts"
                );
                assert!(report.started_at_ms > 0);
                assert!(report.finished_at_ms >= report.started_at_ms);

                // 3. Rows present at the target; DLQ drained (acked).
                let rows: usize = target.writes().iter().map(|(_, n)| n).sum();
                assert_eq!(rows, 2, "both rows reprocessed into the target");
                assert_eq!(dlq.depth("rt-p").await.unwrap(), DlqDepth::default());
                assert!(
                    dlq.take_for_replay(
                        "rt-p",
                        DlqSelection::All,
                        Duration::from_secs(60),
                        i64::MAX - 1
                    )
                    .await
                    .unwrap()
                    .is_empty(),
                    "acked records never come back"
                );
            }

            /// Poison guard: a record whose replay keeps failing has
            /// its attempt bumped on every re-dead-letter and parks
            /// once the budget (max_attempts, default 3) is
            /// exhausted — never loops, and parked records are
            /// excluded from later takes.
            #[tokio::test(flavor = "multi_thread")]
            async fn replay_poison_parks_at_max_attempts() {
                let dlq: Arc<dyn DeadLetterStore> =
                    Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
                dlq.append(vec![seeded_json_record("poison-p", 1)])
                    .await
                    .unwrap();
                let (target, pipeline) = mk_pipeline(
                    "poison-p",
                    Arc::clone(&dlq),
                    Some(Arc::new(FailingTransform)),
                );

                // Replay #1: fails again → re-dead-lettered, attempt 2.
                let r1 = pipeline
                    .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
                    .await
                    .unwrap();
                assert_eq!(
                    (r1.taken, r1.succeeded, r1.redeadlettered, r1.parked),
                    (1, 0, 1, 0)
                );
                let after1 = dlq.browse("poison-p", 0, 10, None).await.unwrap();
                assert_eq!(after1.len(), 1);
                assert_eq!(after1[0].meta.attempt, 2, "attempt incremented");
                assert_eq!(
                    after1[0].payload,
                    seeded_json_record("poison-p", 1).payload,
                    "payload preserved across the redrive"
                );
                assert!(
                    after1[0].meta.error.contains("scripted transform failure"),
                    "error refreshed to the replay failure: {}",
                    after1[0].meta.error
                );

                // Replay #2: attempt 3.
                let r2 = pipeline
                    .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
                    .await
                    .unwrap();
                assert_eq!(
                    (r2.taken, r2.succeeded, r2.redeadlettered, r2.parked),
                    (1, 0, 1, 0)
                );
                let after2 = dlq.browse("poison-p", 0, 10, None).await.unwrap();
                assert_eq!(after2[0].meta.attempt, 3);

                // Replay #3: budget exhausted → parked, not redriven.
                let r3 = pipeline
                    .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
                    .await
                    .unwrap();
                assert_eq!(
                    (r3.taken, r3.succeeded, r3.redeadlettered, r3.parked),
                    (1, 0, 0, 1)
                );
                assert_eq!(
                    dlq.depth("poison-p").await.unwrap(),
                    DlqDepth {
                        pending: 0,
                        parked: 1
                    }
                );
                let parked = dlq
                    .browse("poison-p", 0, 10, Some(DlqRecordStatus::Parked))
                    .await
                    .unwrap();
                assert_eq!(parked.len(), 1);
                assert_eq!(parked[0].meta.attempt, 3, "parked at max_attempts");

                // Replay #4: parked records are excluded — nothing taken.
                let r4 = pipeline
                    .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
                    .await
                    .unwrap();
                assert_eq!(
                    (r4.taken, r4.succeeded, r4.redeadlettered, r4.parked),
                    (0, 0, 0, 0)
                );
                assert!(
                    target.writes().is_empty(),
                    "the poison record never reached the target"
                );
            }

            /// Fails only for odd `v` values — drives exact report
            /// accounting under partial failure.
            #[derive(Debug)]
            struct FailOddTransform;

            #[async_trait]
            impl crate::transform::BatchTransform for FailOddTransform {
                fn input_schema(&self) -> arrow_schema::SchemaRef {
                    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
                }
                fn output_schema(&self) -> arrow_schema::SchemaRef {
                    self.input_schema()
                }
                async fn transform(
                    &self,
                    input: RecordBatch,
                    _ctx: &crate::transform::BatchContext,
                ) -> Result<Vec<RecordBatch>, BackendError> {
                    let v = input
                        .column_by_name("v")
                        .expect("v column present")
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .expect("JSON decode infers Int64");
                    if (0..v.len()).any(|i| v.value(i) % 2 == 1) {
                        return Err(BackendError::Other("odd row rejected".into()));
                    }
                    Ok(vec![input])
                }
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn replay_report_exact_under_partial_failure() {
                let dlq: Arc<dyn DeadLetterStore> =
                    Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
                dlq.append(
                    (1..=4)
                        .map(|n| seeded_json_record("partial-p", n))
                        .collect(),
                )
                .await
                .unwrap();
                let (target, pipeline) = mk_pipeline(
                    "partial-p",
                    Arc::clone(&dlq),
                    Some(Arc::new(FailOddTransform)),
                );

                let r = pipeline
                    .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
                    .await
                    .unwrap();
                assert_eq!(
                    (r.taken, r.succeeded, r.redeadlettered, r.parked),
                    (4, 2, 2, 0),
                    "v=2,4 succeed; v=1,3 re-dead-letter"
                );
                assert_eq!(
                    r.taken,
                    r.succeeded + r.redeadlettered + r.parked,
                    "report invariant"
                );
                let rows: usize = target.writes().iter().map(|(_, n)| n).sum();
                assert_eq!(rows, 2, "only the even rows reached the target");
                let pending = dlq.browse("partial-p", 0, 10, None).await.unwrap();
                assert_eq!(pending.len(), 2);
                assert!(
                    pending.iter().all(|rec| rec.meta.attempt == 2),
                    "redriven records carry attempt 2"
                );
            }

            /// Records the exact batches the transform receives.
            #[derive(Debug, Default)]
            struct CaptureTransform {
                seen: Mutex<Vec<RecordBatch>>,
            }

            #[async_trait]
            impl crate::transform::BatchTransform for CaptureTransform {
                fn input_schema(&self) -> arrow_schema::SchemaRef {
                    Arc::new(Schema::empty())
                }
                fn output_schema(&self) -> arrow_schema::SchemaRef {
                    self.input_schema()
                }
                async fn transform(
                    &self,
                    input: RecordBatch,
                    _ctx: &crate::transform::BatchContext,
                ) -> Result<Vec<RecordBatch>, BackendError> {
                    self.seen.lock().unwrap().push(input.clone());
                    Ok(vec![input])
                }
            }

            /// Format fidelity (JSON): the batch the transform sees
            /// on replay is the one produced by the SAME JSONL
            /// decode path the Kafka source uses on live reads.
            #[tokio::test(flavor = "multi_thread")]
            async fn replay_json_decodes_through_source_path() {
                let dlq: Arc<dyn DeadLetterStore> =
                    Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
                let record = seeded_json_record("fmt-json-p", 41);
                dlq.append(vec![record.clone()]).await.unwrap();

                let capture = Arc::new(CaptureTransform::default());
                let (_target, pipeline) = mk_pipeline(
                    "fmt-json-p",
                    Arc::clone(&dlq),
                    Some(Arc::clone(&capture) as Arc<dyn crate::transform::BatchTransform>),
                );
                let r = pipeline
                    .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
                    .await
                    .unwrap();
                assert_eq!((r.taken, r.succeeded), (1, 1));

                let seen = capture.seen.lock().unwrap().clone();
                assert_eq!(seen.len(), 1);
                let expected =
                    crate::kafka_backend::decode_payloads_as_jsonl(vec![record.payload.clone()])
                        .unwrap();
                assert_eq!(
                    seen, expected,
                    "replayed batch is identical to the source-side JSONL decode"
                );
            }

            /// Format fidelity (RawBytes): the opaque payload
            /// reaches the transform byte-identically as the single
            /// Binary column the RawBytes source format decodes to.
            #[tokio::test(flavor = "multi_thread")]
            async fn replay_raw_bytes_byte_identical_into_transform() {
                let dlq: Arc<dyn DeadLetterStore> =
                    Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
                let mut record = seeded_json_record("fmt-raw-p", 1);
                record.meta.payload_format = "raw_bytes".into();
                record.payload = vec![0x00, 0xff, 0x01, 0x80, 0x7f]; // non-UTF8
                dlq.append(vec![record.clone()]).await.unwrap();

                let capture = Arc::new(CaptureTransform::default());
                let (_target, pipeline) = mk_pipeline(
                    "fmt-raw-p",
                    Arc::clone(&dlq),
                    Some(Arc::clone(&capture) as Arc<dyn crate::transform::BatchTransform>),
                );
                let r = pipeline
                    .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
                    .await
                    .unwrap();
                assert_eq!((r.taken, r.succeeded), (1, 1));

                let seen = capture.seen.lock().unwrap().clone();
                assert_eq!(seen.len(), 1);
                assert_eq!(seen[0].num_columns(), 1);
                assert_eq!(seen[0].num_rows(), 1);
                let bin = seen[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .expect("RawBytes decodes to a single Binary column");
                assert_eq!(
                    bin.value(0),
                    record.payload.as_slice(),
                    "payload replays byte-identically"
                );
            }

            /// FirstN leases the oldest n; Ids replays exactly the
            /// named records.
            #[tokio::test(flavor = "multi_thread")]
            async fn replay_first_n_and_ids_selections() {
                let dlq: Arc<dyn DeadLetterStore> =
                    Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
                dlq.append((1..=3).map(|n| seeded_json_record("sel-p", n)).collect())
                    .await
                    .unwrap();
                let (target, pipeline) = mk_pipeline("sel-p", Arc::clone(&dlq), None);

                let r1 = pipeline
                    .run_dlq_replay(DlqSelection::FirstN(2), ReplayOptions::default())
                    .await
                    .unwrap();
                assert_eq!((r1.taken, r1.succeeded), (2, 2), "oldest two replayed");
                assert_eq!(dlq.depth("sel-p").await.unwrap().pending, 1);

                let left = dlq.browse("sel-p", 0, 10, None).await.unwrap();
                assert_eq!(left.len(), 1);
                assert_eq!(left[0].id, seeded_json_record("sel-p", 3).id);

                let r2 = pipeline
                    .run_dlq_replay(
                        DlqSelection::Ids(vec![left[0].id.clone()]),
                        ReplayOptions::default(),
                    )
                    .await
                    .unwrap();
                assert_eq!((r2.taken, r2.succeeded), (1, 1));
                assert_eq!(dlq.depth("sel-p").await.unwrap(), DlqDepth::default());
                let rows: usize = target.writes().iter().map(|(_, n)| n).sum();
                assert_eq!(rows, 3);
            }

            /// Two overlapping replays over one table store must not
            /// double-process: leases are exclusive, so every record
            /// is written to exactly one replay's target.
            #[tokio::test(flavor = "multi_thread")]
            async fn concurrent_replays_over_table_store_do_not_double_process() {
                let dlq: Arc<dyn DeadLetterStore> =
                    Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
                dlq.append((1..=10).map(|n| seeded_json_record("conc-p", n)).collect())
                    .await
                    .unwrap();
                let (target_a, pa) = mk_pipeline("conc-p", Arc::clone(&dlq), None);
                let (target_b, pb) = mk_pipeline("conc-p", Arc::clone(&dlq), None);

                let (ra, rb) = tokio::join!(
                    pa.run_dlq_replay(DlqSelection::All, ReplayOptions::default()),
                    pb.run_dlq_replay(DlqSelection::All, ReplayOptions::default()),
                );
                let (ra, rb) = (ra.unwrap(), rb.unwrap());

                assert_eq!(ra.taken + rb.taken, 10, "leases are exclusive");
                assert_eq!(ra.succeeded + rb.succeeded, 10);
                let rows: usize = target_a
                    .writes()
                    .iter()
                    .chain(target_b.writes().iter())
                    .map(|(_, n)| n)
                    .sum();
                assert_eq!(rows, 10, "every record written exactly once");
                assert_eq!(dlq.depth("conc-p").await.unwrap(), DlqDepth::default());
            }

            /// Mimics KafkaTopicDlq's process-local lease: flags any
            /// overlap between one replay's take and its ack.
            #[derive(Debug)]
            struct SerializedStubStore {
                active: AtomicUsize,
                overlap: AtomicBool,
                served: AtomicU32,
            }

            #[async_trait]
            impl DeadLetterStore for SerializedStubStore {
                async fn append(&self, _records: Vec<DlqRecord>) -> Result<(), DlqError> {
                    Ok(())
                }
                async fn depth(&self, _pipeline: &str) -> Result<DlqDepth, DlqError> {
                    Ok(DlqDepth::default())
                }
                async fn browse(
                    &self,
                    _pipeline: &str,
                    _page: u64,
                    _page_size: u64,
                    _status_filter: Option<DlqRecordStatus>,
                ) -> Result<Vec<DlqRecord>, DlqError> {
                    Ok(Vec::new())
                }
                async fn take_for_replay(
                    &self,
                    pipeline: &str,
                    _selection: DlqSelection,
                    _lease: Duration,
                    _now_ms: i64,
                ) -> Result<Vec<DlqRecord>, DlqError> {
                    if self.active.fetch_add(1, Ordering::SeqCst) > 0 {
                        self.overlap.store(true, Ordering::SeqCst);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let n = self.served.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![seeded_json_record(pipeline, n + 100)])
                }
                async fn ack_replayed(
                    &self,
                    _pipeline: &str,
                    _ids: &[DlqRecordId],
                ) -> Result<(), DlqError> {
                    self.active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
                async fn park(
                    &self,
                    _pipeline: &str,
                    _ids: &[DlqRecordId],
                ) -> Result<(), DlqError> {
                    Ok(())
                }
                async fn purge(
                    &self,
                    _pipeline: &str,
                    _selection: DlqSelection,
                ) -> Result<u64, DlqError> {
                    Ok(0)
                }
                fn replay_requires_serialization(&self) -> bool {
                    true
                }
            }

            /// Stores whose lease is process-local (KafkaTopicDlq)
            /// declare `replay_requires_serialization` — the engine
            /// then serializes same-pipeline replays behind an
            /// in-process mutex.
            #[tokio::test(flavor = "multi_thread")]
            async fn replay_serialized_for_stores_requiring_it() {
                let store = Arc::new(SerializedStubStore {
                    active: AtomicUsize::new(0),
                    overlap: AtomicBool::new(false),
                    served: AtomicU32::new(0),
                });
                let (_ta, pa) =
                    mk_pipeline("ser-p", store.clone() as Arc<dyn DeadLetterStore>, None);
                let (_tb, pb) =
                    mk_pipeline("ser-p", store.clone() as Arc<dyn DeadLetterStore>, None);

                let (ra, rb) = tokio::join!(
                    pa.run_dlq_replay(DlqSelection::All, ReplayOptions::default()),
                    pb.run_dlq_replay(DlqSelection::All, ReplayOptions::default()),
                );
                ra.unwrap();
                rb.unwrap();
                assert!(
                    !store.overlap.load(Ordering::SeqCst),
                    "same-pipeline replays over a topic-style store must serialize"
                );
            }

            /// A store whose `park` is inexpressible (a Kafka
            /// topic): parking must fall back to the provided table
            /// store, and the original must still be acked so the
            /// topic cursor advances.
            #[derive(Debug)]
            struct ParkUnsupportedStore {
                served: AtomicBool,
                acked: Mutex<Vec<String>>,
            }

            #[async_trait]
            impl DeadLetterStore for ParkUnsupportedStore {
                async fn append(&self, _records: Vec<DlqRecord>) -> Result<(), DlqError> {
                    Ok(())
                }
                async fn depth(&self, _pipeline: &str) -> Result<DlqDepth, DlqError> {
                    Ok(DlqDepth::default())
                }
                async fn browse(
                    &self,
                    _pipeline: &str,
                    _page: u64,
                    _page_size: u64,
                    _status_filter: Option<DlqRecordStatus>,
                ) -> Result<Vec<DlqRecord>, DlqError> {
                    Ok(Vec::new())
                }
                async fn take_for_replay(
                    &self,
                    pipeline: &str,
                    _selection: DlqSelection,
                    _lease: Duration,
                    _now_ms: i64,
                ) -> Result<Vec<DlqRecord>, DlqError> {
                    if !self.served.swap(true, Ordering::SeqCst) {
                        let mut r = seeded_json_record(pipeline, 1);
                        r.meta.attempt = 99; // way past any budget
                        Ok(vec![r])
                    } else {
                        Ok(Vec::new())
                    }
                }
                async fn ack_replayed(
                    &self,
                    _pipeline: &str,
                    ids: &[DlqRecordId],
                ) -> Result<(), DlqError> {
                    self.acked
                        .lock()
                        .unwrap()
                        .extend(ids.iter().map(|i| i.0.clone()));
                    Ok(())
                }
                async fn park(
                    &self,
                    _pipeline: &str,
                    _ids: &[DlqRecordId],
                ) -> Result<(), DlqError> {
                    Err(DlqError::Unsupported("no park on a topic".into()))
                }
                async fn purge(
                    &self,
                    _pipeline: &str,
                    _selection: DlqSelection,
                ) -> Result<u64, DlqError> {
                    Ok(0)
                }
                fn replay_requires_serialization(&self) -> bool {
                    true
                }
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn park_falls_back_to_park_store_when_unsupported() {
                let store = Arc::new(ParkUnsupportedStore {
                    served: AtomicBool::new(false),
                    acked: Mutex::new(Vec::new()),
                });
                let fallback: Arc<dyn DeadLetterStore> =
                    Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
                let (target, pipeline) =
                    mk_pipeline("parkfb-p", store.clone() as Arc<dyn DeadLetterStore>, None);

                let opts = ReplayOptions {
                    park_store: Some(Arc::clone(&fallback)),
                    ..Default::default()
                };
                let r = pipeline
                    .run_dlq_replay(DlqSelection::All, opts)
                    .await
                    .unwrap();
                assert_eq!(
                    (r.taken, r.succeeded, r.redeadlettered, r.parked),
                    (1, 0, 0, 1),
                    "attempt (99) past the budget parks without reprocessing"
                );

                // Parked into the fallback table store.
                assert_eq!(
                    fallback.depth("parkfb-p").await.unwrap(),
                    DlqDepth {
                        pending: 0,
                        parked: 1
                    }
                );
                let parked = fallback
                    .browse("parkfb-p", 0, 10, Some(DlqRecordStatus::Parked))
                    .await
                    .unwrap();
                assert_eq!(parked.len(), 1);
                assert_eq!(parked[0].meta.attempt, 99);

                // The original was acked on the topic-style store
                // (cursor advanced past it) — only AFTER the park
                // landed durably in the fallback.
                assert_eq!(
                    store.acked.lock().unwrap().clone(),
                    vec![parked[0].id.0.clone()]
                );
                assert!(
                    target.writes().is_empty(),
                    "budget exhausted — never reprocessed"
                );
            }

            /// CDC-mode pipelines are typed-rejected for now: replay
            /// re-applies through `write_arrow_stream`, which would
            /// break per-event CDC semantics.
            #[tokio::test(flavor = "multi_thread")]
            async fn replay_rejects_cdc_pipelines() {
                use crate::cdc::{CdcConfig, EnvelopeKind};
                let dlq: Arc<dyn DeadLetterStore> =
                    Arc::new(TableDlq::open_sqlite(":memory:").await.unwrap());
                let source = Arc::new(TestBackend::new("src"));
                let target = Arc::new(TestBackend::new("tgt"));
                let table = TargetTable {
                    schema: "".into(),
                    name: "events".into(),
                };
                let cfg = StreamingPipelineConfig::new("q", table.clone(), "cdc-p")
                    .with_dead_letter_store(dlq)
                    .with_cdc(CdcConfig::for_envelope(EnvelopeKind::Debezium));
                let pipeline = StreamingPipeline::new(
                    source as Arc<dyn Backend>,
                    vec![(target as Arc<dyn Backend>, table)],
                    cfg,
                );
                let err = pipeline
                    .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
                    .await
                    .unwrap_err();
                assert!(
                    err.to_string().to_lowercase().contains("cdc"),
                    "typed rejection names CDC: {err}"
                );
            }

            /// The serialization flag as landed on the real stores:
            /// topic store requires it, table store doesn't.
            #[tokio::test(flavor = "multi_thread")]
            async fn topic_store_requires_serialization_table_does_not() {
                let table = TableDlq::open_sqlite(":memory:").await.unwrap();
                assert!(!table.replay_requires_serialization());

                let kafka: Arc<dyn Backend> =
                    Arc::new(KafkaBackend::open("localhost:9092", Some("g")).unwrap());
                let topic = KafkaTopicDlq::new(kafka, "t", "p").unwrap();
                assert!(
                    topic.replay_requires_serialization(),
                    "process-local group-offset lease → replays must serialize"
                );
            }
        }

        // --- DLQ Phase 3: rewind (offset/timestamp, atomic state reset) ---
        //
        // TDD note: this suite was committed FIRST, red, against a
        // `rewind` stub that returns an "unimplemented" error — same
        // discipline as Phases 1–2.

        mod rewind {
            use super::*;
            use crate::state_store::{InMemoryStateStore, StateStore};
            use crate::transform::BatchTransform;
            use crate::windowed::{
                AggKind, AggregationSpec, LateDataPolicy, WindowConfig, WindowKind,
                WindowedAggregateTransform,
            };
            use arrow_array::types::{Int64Type, TimestampMicrosecondType};
            use arrow_array::{Int64Array, TimestampMicrosecondArray};
            use arrow_schema::TimeUnit;

            /// A seekable scripted backend for rewind tests: source
            /// side replays `data[cursor..limit]` one batch per
            /// read; target side captures written batches whole.
            /// Offset bytes are the JSON-encoded cursor index —
            /// backend-opaque, per the `offset_snapshot`/`seek_to`
            /// contract.
            struct SeekableBackend {
                label: String,
                data: Vec<RecordBatch>,
                cursor: Mutex<usize>,
                /// Reads past this index report "no data" — lets a
                /// test end a run mid-stream (open windows) and
                /// release more data for the rewound run.
                limit: Mutex<usize>,
                captured: Mutex<Vec<RecordBatch>>,
            }

            impl SeekableBackend {
                fn new(label: &str, data: Vec<RecordBatch>) -> Self {
                    let limit = data.len();
                    Self {
                        label: label.into(),
                        data,
                        cursor: Mutex::new(0),
                        limit: Mutex::new(limit),
                        captured: Mutex::new(Vec::new()),
                    }
                }

                fn target(label: &str) -> Self {
                    Self::new(label, Vec::new())
                }

                fn set_limit(&self, limit: usize) {
                    *self.limit.lock().unwrap() = limit;
                }

                fn cursor(&self) -> usize {
                    *self.cursor.lock().unwrap()
                }

                fn captured(&self) -> Vec<RecordBatch> {
                    self.captured.lock().unwrap().clone()
                }

                fn encode(idx: usize) -> Vec<u8> {
                    serde_json::to_vec(&idx).unwrap()
                }

                fn decode(bytes: &[u8]) -> usize {
                    serde_json::from_slice(bytes).unwrap()
                }
            }

            #[async_trait]
            impl Backend for SeekableBackend {
                fn dialect(&self) -> Dialect {
                    Dialect::Postgres
                }
                fn connection_info(&self) -> ConnectionInfo {
                    ConnectionInfo {
                        host: self.label.clone(),
                        port: 0,
                        dbname: self.label.clone(),
                        user: "test".into(),
                    }
                }
                fn dsn(&self) -> Option<String> {
                    None
                }
                async fn ping(&self) -> Result<(), BackendError> {
                    Ok(())
                }
                async fn execute(&self, _statement: &str) -> Result<u64, BackendError> {
                    Ok(0)
                }
                async fn read_arrow_stream(
                    &self,
                    _query: &str,
                ) -> Result<ArrowBatchStream, BackendError> {
                    let limit = *self.limit.lock().unwrap();
                    let mut cursor = self.cursor.lock().unwrap();
                    let next: Vec<RecordBatch> = if *cursor < limit.min(self.data.len()) {
                        let b = self.data[*cursor].clone();
                        *cursor += 1;
                        vec![b]
                    } else {
                        Vec::new()
                    };
                    Ok(Box::pin(futures_util::stream::iter(
                        next.into_iter().map(Ok),
                    )))
                }
                async fn write_arrow_stream(
                    &self,
                    _target: &TargetTable,
                    stream: ArrowBatchStream,
                    _mode: WriteMode,
                ) -> Result<u64, BackendError> {
                    let batches: Vec<RecordBatch> = stream.try_collect().await?;
                    let n: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                    self.captured.lock().unwrap().extend(batches);
                    Ok(n)
                }
                async fn run_append(
                    &self,
                    _spec: &crate::types::TableSpec,
                    _source_query: &str,
                    _pipeline_name: &str,
                    _source_backend: Option<&dyn Backend>,
                    _incremental_column: Option<&str>,
                    _last_value_literal: Option<&str>,
                    _dry_run: bool,
                ) -> Result<StrategyRunResult, BackendError> {
                    unreachable!("SeekableBackend::run_append")
                }
                async fn run_truncate(
                    &self,
                    _spec: &crate::types::TableSpec,
                    _source_query: &str,
                    _pipeline_name: &str,
                    _source_backend: Option<&dyn Backend>,
                    _dry_run: bool,
                ) -> Result<StrategyRunResult, BackendError> {
                    unreachable!("SeekableBackend::run_truncate")
                }
                async fn run_merge(
                    &self,
                    _spec: &crate::types::TableSpec,
                    _source_query: &str,
                    _keys: &[String],
                    _update_columns: &[String],
                    _pipeline_name: &str,
                    _mode_label: &str,
                    _source_backend: Option<&dyn Backend>,
                    _delete_handling: Option<crate::backend::DeleteHandling>,
                    _dry_run: bool,
                ) -> Result<StrategyRunResult, BackendError> {
                    unreachable!("SeekableBackend::run_merge")
                }
                async fn run_scd2(
                    &self,
                    _spec: &crate::types::TableSpec,
                    _source_query: &str,
                    _keys: &[String],
                    _compare_columns: &[String],
                    _pipeline_name: &str,
                    _source_backend: Option<&dyn Backend>,
                    _delete_handling: Option<crate::backend::DeleteHandling>,
                    _event_timestamp_column: Option<&str>,
                    _ttl_seconds: Option<i64>,
                    _dry_run: bool,
                ) -> Result<StrategyRunResult, BackendError> {
                    unreachable!("SeekableBackend::run_scd2")
                }
                fn supports_seek_to(&self) -> bool {
                    true
                }
                async fn seek_to(&self, offset_bytes: &[u8]) -> Result<(), BackendError> {
                    *self.cursor.lock().unwrap() = Self::decode(offset_bytes);
                    Ok(())
                }
                async fn offset_snapshot(&self) -> Result<Option<Vec<u8>>, BackendError> {
                    Ok(Some(Self::encode(*self.cursor.lock().unwrap())))
                }
                /// First batch whose max `_event_ts` is ≥ `ts_ms` —
                /// the same "earliest offset at-or-after t" contract
                /// Kafka's `offsets_for_times` implements.
                async fn offsets_for_timestamp(
                    &self,
                    _query: &str,
                    ts_ms: i64,
                ) -> Result<Vec<u8>, BackendError> {
                    let ts_us = ts_ms * 1_000;
                    let idx = self
                        .data
                        .iter()
                        .position(|b| batch_max_event_ts(b).is_some_and(|t| t >= ts_us))
                        .unwrap_or(self.data.len());
                    Ok(Self::encode(idx))
                }
            }

            /// One-row batch carrying (g, v, _event_ts).
            fn event_batch(g: i64, v: i64, ts_us: i64) -> RecordBatch {
                let schema = Schema::new(vec![
                    Field::new("g", DataType::Int64, false),
                    Field::new("v", DataType::Int64, true),
                    Field::new(
                        "_event_ts",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                        false,
                    ),
                ]);
                RecordBatch::try_new(
                    Arc::new(schema),
                    vec![
                        Arc::new(Int64Array::from(vec![g])),
                        Arc::new(Int64Array::from(vec![Some(v)])),
                        Arc::new(TimestampMicrosecondArray::from(vec![ts_us]).with_timezone("UTC")),
                    ],
                )
                .unwrap()
            }

            /// Tumbling 1s window over `g`, summing `v` as `total`.
            fn tumbling_config() -> WindowConfig {
                WindowConfig {
                    kind: WindowKind::Tumbling,
                    duration_ms: 1_000,
                    hop_ms: 1_000,
                    gap_ms: None,
                    max_session_duration_ms: None,
                    event_time_column: "_event_ts".into(),
                    group_by: vec!["g".into()],
                    aggregations: vec![AggregationSpec::new(
                        AggKind::Sum,
                        Some("v".into()),
                        "total",
                    )],
                    late_data: LateDataPolicy::Drop,
                    max_groups_per_window: 100,
                    window_start_column: "window_start".into(),
                    window_end_column: "window_end".into(),
                    session_id_column: "session_id".into(),
                }
            }

            /// Extract (window_start_us, total) pairs from captured
            /// windowed-output batches, sorted for comparison.
            fn window_rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
                let mut rows = Vec::new();
                for b in batches {
                    let ws = b
                        .column_by_name("window_start")
                        .expect("window_start column")
                        .as_primitive::<TimestampMicrosecondType>();
                    let total = b
                        .column_by_name("total")
                        .expect("total column")
                        .as_primitive::<Int64Type>();
                    for i in 0..b.num_rows() {
                        rows.push((ws.value(i), total.value(i)));
                    }
                }
                rows.sort_unstable();
                rows
            }

            /// Run `pipeline` until its source drains + idles, then
            /// shut down.
            async fn run_until_idle(pipeline: &StreamingPipeline) {
                let (sig, trigger) = ShutdownSignal::new();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    trigger.trigger();
                });
                pipeline.run(sig).await.expect("pipeline run");
            }

            /// The Phase 3 gate: a stateful (windowed) pipeline
            /// refuses to rewind without `confirm_state_reset` —
            /// hard error, nothing mutated.
            #[tokio::test(flavor = "multi_thread")]
            async fn stateful_rewind_requires_confirm_state_reset() {
                let source = Arc::new(SeekableBackend::new(
                    "src",
                    vec![event_batch(1, 1, 500_000)],
                ));
                let target = Arc::new(SeekableBackend::target("tgt"));
                let table = TargetTable {
                    schema: "".into(),
                    name: "out".into(),
                };
                let transform: Arc<dyn BatchTransform> =
                    Arc::new(WindowedAggregateTransform::new(tumbling_config(), None).unwrap());
                let cfg = StreamingPipelineConfig::new("q", table.clone(), "rw-gate")
                    .with_transform(transform)
                    .with_watermark(WatermarkConfig::default());
                let pipeline = StreamingPipeline::new(
                    Arc::clone(&source) as Arc<dyn Backend>,
                    vec![(target as Arc<dyn Backend>, table)],
                    cfg,
                );

                let err = pipeline
                    .rewind(RewindTarget::Offset(SeekableBackend::encode(0)), false)
                    .await
                    .expect_err("stateful rewind without confirmation must hard-error");
                assert!(
                    err.to_string().contains("confirm_state_reset"),
                    "error must name the missing confirmation flag: {err}"
                );
                assert_eq!(source.cursor(), 0, "nothing seeked on the refused rewind");
            }

            /// Stateless rewind (plan exit criterion): seek lands,
            /// the state store's offsets are REPLACED, and the
            /// rewound run re-consumes from the target offset.
            #[tokio::test(flavor = "multi_thread")]
            async fn stateless_rewind_replays_from_offset() {
                let data = vec![
                    event_batch(1, 10, 500_000),
                    event_batch(1, 20, 1_500_000),
                    event_batch(1, 30, 2_500_000),
                ];
                let source = Arc::new(SeekableBackend::new("src", data));
                let target = Arc::new(SeekableBackend::target("tgt"));
                let table = TargetTable {
                    schema: "".into(),
                    name: "out".into(),
                };
                let store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new());
                let cfg = StreamingPipelineConfig::new("q", table.clone(), "rw-stateless")
                    .with_state_store(Arc::clone(&store));
                let pipeline = StreamingPipeline::new(
                    Arc::clone(&source) as Arc<dyn Backend>,
                    vec![(Arc::clone(&target) as Arc<dyn Backend>, table)],
                    cfg,
                );

                run_until_idle(&pipeline).await;
                assert_eq!(source.cursor(), 3, "first run drained the script");
                assert_eq!(target.captured().len(), 3);

                // Rewind to batch 1. Stateless → no confirmation needed.
                let report = pipeline
                    .rewind(RewindTarget::Offset(SeekableBackend::encode(1)), false)
                    .await
                    .expect("stateless rewind");
                assert_eq!(
                    report.sources,
                    vec![("q".to_string(), SeekableBackend::encode(1))]
                );
                assert!(!report.state_cleared, "no stateful transform to clear");
                assert_eq!(source.cursor(), 1, "seek applied");

                // The durable offsets were replaced in the store —
                // a restart would also resume from the rewound spot.
                let recovered = store.load("rw-stateless").await.unwrap();
                assert_eq!(
                    recovered.offsets.get("q").map(|v| v.as_slice()),
                    Some(SeekableBackend::encode(1).as_slice()),
                    "state store offsets replaced by the rewind"
                );

                // Rewound run re-delivers batches 1..3.
                run_until_idle(&pipeline).await;
                assert_eq!(
                    target.captured().len(),
                    5,
                    "two batches re-consumed after the rewind"
                );
            }

            /// Rewind-equivalence (plan exit criterion): on a
            /// windowed pipeline, rewind-to-T then run ≡ a fresh
            /// pipeline started at T. The first run is cut
            /// mid-stream so an open window holds accumulator state
            /// — if rewind failed to clear it, the re-consumed rows
            /// would double-count and the outputs would diverge.
            #[tokio::test(flavor = "multi_thread")]
            async fn rewind_equivalence_windowed() {
                // b0..b3 in 1s windows w0..w3; the flush batch (1h
                // later) closes every open window deterministically.
                let data = vec![
                    event_batch(1, 10, 500_000),
                    event_batch(1, 20, 1_500_000),
                    event_batch(1, 30, 2_500_000),
                    event_batch(1, 40, 3_500_000),
                    event_batch(1, 0, 3_600_000_000),
                ];

                fn build(
                    name: &str,
                    data: Vec<RecordBatch>,
                ) -> (
                    Arc<SeekableBackend>,
                    Arc<SeekableBackend>,
                    StreamingPipeline,
                ) {
                    let source = Arc::new(SeekableBackend::new("q", data));
                    let target = Arc::new(SeekableBackend::target("tgt"));
                    let table = TargetTable {
                        schema: "".into(),
                        name: "out".into(),
                    };
                    let transform: Arc<dyn crate::transform::BatchTransform> =
                        Arc::new(WindowedAggregateTransform::new(tumbling_config(), None).unwrap());
                    let store: Arc<dyn crate::state_store::StateStore> =
                        Arc::new(crate::state_store::InMemoryStateStore::new());
                    let cfg = StreamingPipelineConfig::new("q", table.clone(), name)
                        .with_transform(transform)
                        .with_watermark(WatermarkConfig::default())
                        .with_state_store(store);
                    let p = StreamingPipeline::new(
                        Arc::clone(&source) as Arc<dyn Backend>,
                        vec![(Arc::clone(&target) as Arc<dyn Backend>, table)],
                        cfg,
                    );
                    (source, target, p)
                }

                // P1: consume b0..b3 only (flush withheld) so w3
                // stays open with v=40 accumulated in memory.
                let (src1, tgt1, p1) = build("rw-eq", data.clone());
                src1.set_limit(4);
                run_until_idle(&p1).await;
                let first_run_rows = window_rows(&tgt1.captured());
                assert_eq!(
                    first_run_rows,
                    vec![(0, 10), (1_000_000, 20), (2_000_000, 30)],
                    "w0..w2 emitted; w3 still open when the run stopped"
                );

                // Rewind P1 to T = 2000ms → resolves to b2 (first
                // event ≥ T). Stateful → confirmation required.
                src1.set_limit(5);
                let report = p1
                    .rewind(RewindTarget::Timestamp(2_000), true)
                    .await
                    .expect("stateful rewind with confirmation");
                assert!(report.state_cleared);
                assert_eq!(src1.cursor(), 2, "seeked to first event ≥ T");
                run_until_idle(&p1).await;
                let rewound_rows_all = window_rows(&tgt1.captured());
                let rewound_rows: Vec<(i64, i64)> =
                    rewound_rows_all[first_run_rows.len()..].to_vec();

                // P2: fresh pipeline, fresh transform + store, source
                // seeked straight to b2 — a genuine start-at-T.
                let (src2, tgt2, p2) = build("rw-eq-fresh", data);
                (src2.as_ref() as &dyn Backend)
                    .seek_to(&SeekableBackend::encode(2))
                    .await
                    .unwrap();
                run_until_idle(&p2).await;
                let fresh_rows = window_rows(&tgt2.captured());

                assert_eq!(
                    rewound_rows, fresh_rows,
                    "rewind-to-T output must equal fresh-start-at-T output; \
                     a divergence means leftover window state double-counted"
                );
                // And the discriminating value: w3 = 40 exactly once.
                assert_eq!(
                    fresh_rows,
                    vec![(2_000_000, 30), (3_000_000, 40)],
                    "w2 + w3 emit exactly their own rows"
                );
            }

            /// Timestamp rewind resolves per source — each backend
            /// maps T through its own event timeline.
            #[tokio::test(flavor = "multi_thread")]
            async fn rewind_timestamp_resolves_per_source() {
                let a = Arc::new(SeekableBackend::new(
                    "a",
                    vec![
                        event_batch(1, 1, 500_000),
                        event_batch(1, 2, 1_500_000),
                        event_batch(1, 3, 2_500_000),
                    ],
                ));
                let b = Arc::new(SeekableBackend::new(
                    "b",
                    vec![event_batch(2, 1, 2_200_000), event_batch(2, 2, 2_900_000)],
                ));
                let target = Arc::new(SeekableBackend::target("tgt"));
                let table = TargetTable {
                    schema: "".into(),
                    name: "out".into(),
                };
                let cfg = StreamingPipelineConfig::new("unused", table.clone(), "rw-multi");
                let pipeline = StreamingPipeline::new_multi_source(
                    vec![
                        (Arc::clone(&a) as Arc<dyn Backend>, "qa".into()),
                        (Arc::clone(&b) as Arc<dyn Backend>, "qb".into()),
                    ],
                    vec![(target as Arc<dyn Backend>, table)],
                    cfg,
                );
                // Pretend both sources were fully consumed.
                (a.as_ref() as &dyn Backend)
                    .seek_to(&SeekableBackend::encode(3))
                    .await
                    .unwrap();
                (b.as_ref() as &dyn Backend)
                    .seek_to(&SeekableBackend::encode(2))
                    .await
                    .unwrap();

                let report = pipeline
                    .rewind(RewindTarget::Timestamp(2_000), false)
                    .await
                    .expect("timestamp rewind");
                assert_eq!(a.cursor(), 2, "source a: first event ≥ 2000ms is index 2");
                assert_eq!(b.cursor(), 0, "source b: first event ≥ 2000ms is index 0");
                assert_eq!(report.sources.len(), 2);
                assert_eq!(report.sources[0].0, "qa");
                assert_eq!(report.sources[1].0, "qb");
            }

            /// Offset bytes are backend-opaque — applying one blob
            /// to N sources would be dishonest. Typed rejection.
            #[tokio::test(flavor = "multi_thread")]
            async fn rewind_offset_bytes_rejected_for_multi_source() {
                let a = Arc::new(SeekableBackend::new("a", vec![event_batch(1, 1, 1)]));
                let b = Arc::new(SeekableBackend::new("b", vec![event_batch(2, 1, 1)]));
                let target = Arc::new(SeekableBackend::target("tgt"));
                let table = TargetTable {
                    schema: "".into(),
                    name: "out".into(),
                };
                let cfg = StreamingPipelineConfig::new("unused", table.clone(), "rw-multi-off");
                let pipeline = StreamingPipeline::new_multi_source(
                    vec![
                        (a as Arc<dyn Backend>, "qa".into()),
                        (b as Arc<dyn Backend>, "qb".into()),
                    ],
                    vec![(target as Arc<dyn Backend>, table)],
                    cfg,
                );
                let err = pipeline
                    .rewind(RewindTarget::Offset(SeekableBackend::encode(0)), false)
                    .await
                    .expect_err("offset rewind on a multi-source pipeline");
                assert!(
                    err.to_string().contains("single-source"),
                    "typed error names the single-source constraint: {err}"
                );
            }

            /// A source that can't seek fails the rewind up-front —
            /// silently skipping it would leave the pipeline
            /// straddling two positions.
            #[tokio::test(flavor = "multi_thread")]
            async fn rewind_errors_when_source_lacks_seek_to() {
                let source = Arc::new(TestBackend::new("src"));
                let target = Arc::new(TestBackend::new("tgt"));
                let table = TargetTable {
                    schema: "".into(),
                    name: "out".into(),
                };
                let cfg = StreamingPipelineConfig::new("q", table.clone(), "rw-noseek");
                let pipeline = StreamingPipeline::new(
                    source as Arc<dyn Backend>,
                    vec![(target as Arc<dyn Backend>, table)],
                    cfg,
                );
                let err = pipeline
                    .rewind(RewindTarget::Offset(vec![0]), false)
                    .await
                    .expect_err("non-seekable source must fail the rewind");
                assert!(
                    err.to_string().contains("seek_to"),
                    "error names the missing capability: {err}"
                );
            }

            /// Timestamp rewind on a backend without a timestamp
            /// index surfaces the typed default error (Kinesis/SQL
            /// precedent: typed-Unsupported, not a silent fallback).
            #[tokio::test(flavor = "multi_thread")]
            async fn rewind_timestamp_typed_error_on_unsupported_backend() {
                let source = Arc::new(TestBackend::new("src"));
                let target = Arc::new(TestBackend::new("tgt"));
                let table = TargetTable {
                    schema: "".into(),
                    name: "out".into(),
                };
                let cfg = StreamingPipelineConfig::new("q", table.clone(), "rw-nots");
                let pipeline = StreamingPipeline::new(
                    source as Arc<dyn Backend>,
                    vec![(target as Arc<dyn Backend>, table)],
                    cfg,
                );
                let err = pipeline
                    .rewind(RewindTarget::Timestamp(1_000), false)
                    .await
                    .expect_err("timestamp resolution unsupported");
                assert!(
                    err.to_string().contains("timestamp"),
                    "typed error mentions timestamp resolution: {err}"
                );
            }
        }
    }
}
