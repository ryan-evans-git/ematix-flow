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

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use futures_util::TryStreamExt;
use prometheus::{IntCounter, Registry};
use tokio::sync::watch;

use crate::backend::{ArrowBatchStream, Backend, BackendError, TargetTable, WriteMode};
use crate::kafka_backend::KafkaBackend;

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
        }
    }

    /// Builder-style: opt into DLQ routing on target write failure.
    pub fn with_dead_letter_topic(mut self, topic: impl Into<String>) -> Self {
        self.dead_letter_topic = Some(topic.into());
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
    pub source: Arc<dyn Backend>,
    pub target: Arc<dyn Backend>,
    pub config: StreamingPipelineConfig,
    pub metrics: StreamingPipelineMetricsCounters,
}

impl StreamingPipeline {
    pub fn new(
        source: Arc<dyn Backend>,
        target: Arc<dyn Backend>,
        config: StreamingPipelineConfig,
    ) -> Self {
        let metrics = StreamingPipelineMetricsCounters::new(&config.pipeline_name);
        Self {
            source,
            target,
            config,
            metrics,
        }
    }

    /// Borrow the Prometheus registry — for serving via a
    /// `/metrics` HTTP endpoint or federating into a process-wide
    /// scraper.
    pub fn metrics_registry(&self) -> &Registry {
        &self.metrics.registry
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
        loop {
            if shutdown.is_triggered() {
                summary.shutdown_triggered = true;
                break;
            }

            let stream: ArrowBatchStream = self
                .source
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

            // Save a clone of the batches *before* handing them to
            // the target — needed for DLQ-on-failure routing.
            let dlq_batches = if self.config.dead_letter_topic.is_some() {
                Some(batches.clone())
            } else {
                None
            };
            let target_stream: ArrowBatchStream =
                Box::pin(futures_util::stream::iter(batches.into_iter().map(Ok)));
            let write_result = self
                .target
                .write_arrow_stream(&self.config.target, target_stream, self.config.mode)
                .await;

            match write_result {
                Ok(_) => {
                    self.metrics.rows_written.inc_by(n_rows);
                }
                Err(e) => {
                    self.metrics.errors.inc();
                    // DLQ routing: if a topic is configured, produce
                    // each row's source-encoded payload to the DLQ
                    // and continue. If no DLQ is configured, surface
                    // the error so the supervisor decides on
                    // restart.
                    if let (Some(topic), Some(batches)) =
                        (&self.config.dead_letter_topic, dlq_batches)
                    {
                        let dlq_count = self.route_batches_to_dlq(topic.as_str(), batches).await?;
                        self.metrics.dlq_writes.inc_by(dlq_count);
                    } else {
                        return Err(e);
                    }
                }
            }

            // At-least-once commit: only after the target ack lands
            // (or after the DLQ ack on the failure path). Non-Kafka
            // sources implement the trait's no-op default, so this
            // is free for them.
            self.source.commit_offsets().await?;

            self.metrics.batches.inc();
            summary.total_rows += n_rows;
            summary.iterations += 1;
        }
        Ok(summary)
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
        let n = self
            .source
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
}
