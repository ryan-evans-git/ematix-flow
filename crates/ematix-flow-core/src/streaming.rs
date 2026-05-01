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
use tokio::sync::watch;

use crate::backend::{ArrowBatchStream, Backend, BackendError, TargetTable, WriteMode};

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
        }
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
}

impl StreamingPipeline {
    pub fn new(
        source: Arc<dyn Backend>,
        target: Arc<dyn Backend>,
        config: StreamingPipelineConfig,
    ) -> Self {
        Self {
            source,
            target,
            config,
        }
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
        let mut metrics = StreamingPipelineMetrics::default();
        loop {
            if shutdown.is_triggered() {
                metrics.shutdown_triggered = true;
                break;
            }

            let stream: ArrowBatchStream = self
                .source
                .read_arrow_stream(&self.config.source_query)
                .await?;
            let batches: Vec<RecordBatch> = stream.try_collect().await?;

            if batches.is_empty() {
                // Idle — sleep, but wake immediately if shutdown
                // fires. Without the select! we'd block for the
                // full pause before noticing the trigger.
                tokio::select! {
                    _ = shutdown.wait() => {
                        metrics.shutdown_triggered = true;
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(self.config.idle_pause_ms)) => {}
                }
                continue;
            }

            let n_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            // Wrap the collected Vec back into an ArrowBatchStream so
            // the target's existing write_arrow_stream signature
            // applies.
            let target_stream: ArrowBatchStream =
                Box::pin(futures_util::stream::iter(batches.into_iter().map(Ok)));
            self.target
                .write_arrow_stream(&self.config.target, target_stream, self.config.mode)
                .await?;

            // At-least-once commit: only after the target ack lands.
            // Non-Kafka sources implement the trait's no-op default,
            // so this is free for them.
            self.source.commit_offsets().await?;

            metrics.total_rows += n_rows;
            metrics.iterations += 1;
        }
        Ok(metrics)
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
}
