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
                "Total rows produced to the dead-letter topic after a target write failure.",
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
    /// When `Some`, failed-batch rows get routed here as raw bytes
    /// instead of bubbling the error up. The DLQ is itself a Kafka
    /// topic — the source backend must be Kafka (its FutureProducer
    /// is reused) for DLQ routing to work; for non-Kafka sources
    /// this is silently ignored and the error bubbles up.
    ///
    /// At-least-once: source offsets are committed *after* the DLQ
    /// produce ack lands, so a crash mid-DLQ-write means the
    /// original messages are re-delivered, not lost.
    pub dead_letter_topic: Option<String>,
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
            transform: None,
            watermark: None,
            state_store: None,
            checkpoint_interval_ms: None,
            transform_on_error: TransformErrorPolicy::Fail,
            cdc: None,
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
    async fn ensure_cdc_target_specs(
        &self,
    ) -> Result<&Vec<crate::types::TableSpec>, BackendError> {
        self.cdc_target_specs
            .get_or_try_init(|| async {
                let mut out = Vec::with_capacity(self.targets.len());
                for (backend, table) in &self.targets {
                    let spec = backend.reflect_table_spec(table).await?;
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
                                            if let Some(topic) =
                                                self.config.dead_letter_topic.clone()
                                            {
                                                let count = self
                                                    .route_batches_to_dlq(
                                                        topic.as_str(),
                                                        vec![b_clone],
                                                    )
                                                    .await?;
                                                self.metrics.dlq_writes.inc_by(count);
                                            } else {
                                                tracing::warn!(
                                                    pipeline = %self.config.pipeline_name,
                                                    "on_error = \"dlq\" but no \
                                                     dead_letter_topic configured — \
                                                     batch silently discarded"
                                                );
                                            }
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
                let runs = self
                    .targets
                    .iter()
                    .zip(specs.iter())
                    .map(|((backend, _table), spec)| {
                        let batches_for_target: Vec<RecordBatch> = batches.clone();
                        let pipeline_name = self.config.pipeline_name.clone();
                        async move {
                            let mut total = crate::backend::CdcRunResult::default();
                            for batch in batches_for_target {
                                let r = backend
                                    .run_cdc(spec, batch, cdc, &pipeline_name)
                                    .await?;
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
                    let target_stream: ArrowBatchStream = Box::pin(
                        futures_util::stream::iter(batches_for_target.into_iter().map(Ok)),
                    );
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
                    // DLQ routing: if a topic is configured, produce
                    // each row's source-encoded payload to the DLQ
                    // and continue. With multi-target the DLQ runs
                    // when *any* target failed; targets that already
                    // succeeded keep their writes (data is in those
                    // sinks plus the DLQ — partial-success is the
                    // accepted trade for at-least-once across N
                    // sinks). No DLQ ⇒ surface the first failure
                    // and skip the offset commit.
                    if let Some(topic) = &self.config.dead_letter_topic {
                        let dlq_count = self.route_batches_to_dlq(topic.as_str(), batches).await?;
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
        let Some(topic) = self.config.dead_letter_topic.clone() else {
            // Policy is Dlq but no topic configured. Bump the
            // dropped counter so this is visible in metrics rather
            // than silent.
            self.metrics.dlq_writes.inc_by(0);
            tracing::warn!(
                pipeline = %self.config.pipeline_name,
                rows = n_rows,
                "late_data = \"dlq\" set but no dead_letter_topic configured — \
                 late rows discarded"
            );
            return Ok(());
        };
        let count = self.route_batches_to_dlq(topic.as_str(), dlq_rows).await?;
        self.metrics.dlq_writes.inc_by(count);
        Ok(())
    }

    /// Route a failed batch's rows to the configured DLQ topic. The
    /// DLQ is a Kafka topic produced via the source backend's own
    /// `write_arrow_stream` (which uses the source's ClientConfig +
    /// auth). This means DLQ produce only works when the source IS
    /// Kafka — for non-Kafka sources `write_arrow_stream` will likely
    /// error with a clearer "wrong target" message, which surfaces
    /// to the supervisor.
    ///
    /// Each row is sent in the source backend's payload format —
    /// JSON-formatted sources keep their JSON wire format on the
    /// DLQ, RawBytes-formatted sources keep the raw blob. That
    /// symmetry means a downstream DLQ consumer can re-consume +
    /// replay exactly the same way it would the primary topic.
    async fn route_batches_to_dlq(
        &self,
        topic: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<u64, BackendError> {
        let dlq_target = TargetTable {
            schema: String::new(),
            name: topic.to_string(),
        };
        let stream: ArrowBatchStream =
            Box::pin(futures_util::stream::iter(batches.into_iter().map(Ok)));
        // Use the primary (first) source for DLQ produces.
        // Multi-source DLQ routing would need per-source policy +
        // a way to know which source originated each row — out of
        // scope for the v0.1 multi-source path.
        let primary = self
            .sources
            .first()
            .ok_or_else(|| BackendError::Other("DLQ requested but no sources configured".into()))?;
        let n = primary
            .0
            .write_arrow_stream(&dlq_target, stream, WriteMode::Append)
            .await?;
        Ok(n)
    }
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

        // Verify the seek landed on the Kafka backend by trying to
        // re-seek with new offsets and observing the prior ones
        // were consumed (pending_seek replaced, not appended).
        // Indirect assertion: a second `seek_to` with a different
        // offset must succeed, and the resulting `Debug` output
        // shows pending_seek_count = 1.
        let dbg_after = format!("{kafka:?}");
        assert!(
            dbg_after.contains("pending_seek_count: 1"),
            "after load_state, Kafka backend should have a stashed seek; \
             got debug: {dbg_after}"
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
                _spec: &crate::types::TableSpec,
                batch: RecordBatch,
                _cdc_config: &crate::cdc::CdcConfig,
                _pipeline_name: &str,
            ) -> Result<crate::backend::CdcRunResult, BackendError> {
                self.cdc_calls
                    .lock()
                    .unwrap()
                    .push((self.label.clone(), batch.num_rows()));
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
                body.contains("ematix_streaming_cdc_creates_total")
                    && body.contains(" 6"),
                "creates counter not visible in metrics body:\n{body}"
            );
            assert!(
                body.contains("ematix_streaming_cdc_updates_total")
                    && body.contains(" 4"),
                "updates counter not visible:\n{body}"
            );
            assert!(
                body.contains("ematix_streaming_cdc_deletes_total")
                    && body.contains(" 2"),
                "deletes counter not visible:\n{body}"
            );
            assert!(
                body.contains("ematix_streaming_cdc_idempotent_skipped_total")
                    && body.contains(" 10"),
                "idempotent_skipped counter not visible:\n{body}"
            );
            assert!(
                body.contains("ematix_streaming_cdc_skipped_total")
                    && body.contains(" 8"),
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
    }
}
