//! CLI.3: process-level supervisor with restart-on-crash and
//! exponential backoff.
//!
//! Wraps a single-run pipeline closure in a retry loop so the
//! `flow` binary can act as its own supervisor when the host
//! environment doesn't already provide one (Kubernetes /
//! systemd / Nomad). Use `--restart-on-error` to opt in.
//!
//! ## Backoff schedule
//! Backoff doubles after each error, capped at
//! [`RestartPolicy::max_backoff_ms`]. After a run that lasted
//! at least [`RestartPolicy::reset_after_secs`], the backoff
//! resets to [`RestartPolicy::initial_backoff_ms`] — a long-
//! running success earns a fresh retry budget.
//!
//! ## What counts as "exit cleanly"?
//! Any `Ok(_)` from the runner. `StreamingPipelineMetrics`
//! includes a `shutdown_triggered` flag so callers can
//! distinguish SIGTERM-initiated drains from runs that exhausted
//! their source (rare for streams, but possible).
//!
//! ## What counts as a "restart"?
//! Each `Err(_)` that the supervisor handles by sleeping and
//! invoking the runner again. The supervisor exits when:
//!   - the runner returns `Ok(_)` (clean exit; not a restart)
//!   - `max_restarts` is exhausted (last error is surfaced)
//!   - the supervisor's own shutdown signal triggers mid-backoff
//!     (clean exit; metrics show the partial run).

use std::time::{Duration, Instant};

use ematix_flow_core::streaming::{ShutdownSignal, StreamingPipelineMetrics};
use tracing::{info, warn};

use crate::CliError;

/// Restart policy used by the supervisor. `initial_backoff_ms` is
/// the first sleep after a failure; subsequent failures double the
/// sleep up to `max_backoff_ms`.
#[derive(Debug, Clone)]
pub struct RestartPolicy {
    /// Master switch. When `false`, the supervisor calls the
    /// runner once and surfaces whatever it returns.
    pub enabled: bool,
    /// First sleep after an error, in ms. Doubles each subsequent
    /// failure.
    pub initial_backoff_ms: u64,
    /// Maximum sleep between retries, in ms.
    pub max_backoff_ms: u64,
    /// Optional cap on consecutive restarts. `None` = unlimited.
    /// Resets to zero after a run that lasted
    /// `reset_after_secs` or longer.
    pub max_restarts: Option<u32>,
    /// If the runner ran at least this long before erroring,
    /// reset the consecutive-restart counter and the backoff.
    pub reset_after_secs: u64,
}

impl Default for RestartPolicy {
    /// Sensible defaults: disabled (caller must opt in via the
    /// CLI flag); 1s initial backoff; 60s cap; unlimited
    /// restarts; reset after 60s of clean running.
    fn default() -> Self {
        Self {
            enabled: false,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 60_000,
            max_restarts: None,
            reset_after_secs: 60,
        }
    }
}

/// Aggregated outcome of a supervised run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SupervisorSummary {
    /// Total times the runner closure was invoked.
    pub total_runs: u32,
    /// Number of `Err(_)` returns the supervisor handled by
    /// sleeping + retrying. (Excludes the final error if
    /// `max_restarts` was hit.)
    pub error_restarts: u32,
    /// Sleep durations applied between retries. Mostly for
    /// observability + tests; production users won't typically
    /// inspect this.
    pub backoff_history_ms: Vec<u64>,
    /// Metrics from the final run (clean or error). `None` if
    /// the runner closure never returned (e.g. supervisor
    /// shutdown fired before the first run completed).
    pub final_metrics: Option<StreamingPipelineMetrics>,
}

/// Run a closure under the supervisor, retrying on error per
/// `policy`. `shutdown` lets the caller cancel a backoff sleep
/// and exit the loop early (e.g. when SIGTERM fires).
///
/// Generic over the runner so tests can pass a controlled
/// mock; the binary calls [`run_supervised_consume`] which wraps
/// `run_consume_with`.
pub async fn run_supervised_with_runner<F, Fut>(
    policy: RestartPolicy,
    shutdown: ShutdownSignal,
    runner: F,
) -> Result<SupervisorSummary, CliError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<StreamingPipelineMetrics, CliError>>,
{
    let mut summary = SupervisorSummary::default();
    let mut backoff_ms = policy.initial_backoff_ms;
    let mut consecutive_failures: u32 = 0;

    loop {
        if shutdown.is_triggered() {
            return Ok(summary);
        }

        let started = Instant::now();
        summary.total_runs += 1;
        match runner().await {
            Ok(metrics) => {
                summary.final_metrics = Some(metrics);
                return Ok(summary);
            }
            Err(e) => {
                let ran_for = started.elapsed();
                warn!(
                    error = %e,
                    ran_for_secs = ran_for.as_secs(),
                    consecutive_failures,
                    "pipeline run errored"
                );
                if !policy.enabled {
                    // Restart not opted into → surface error.
                    return Err(e);
                }
                if ran_for.as_secs() >= policy.reset_after_secs {
                    info!(
                        ran_for_secs = ran_for.as_secs(),
                        reset_after_secs = policy.reset_after_secs,
                        "long-running run before failure; resetting backoff"
                    );
                    consecutive_failures = 0;
                    backoff_ms = policy.initial_backoff_ms;
                }
                consecutive_failures += 1;
                if let Some(cap) = policy.max_restarts
                    && consecutive_failures > cap
                {
                    warn!(
                        consecutive_failures,
                        max_restarts = cap,
                        "max_restarts exhausted; surfacing final error"
                    );
                    return Err(e);
                }

                // Sleep with shutdown awareness.
                summary.backoff_history_ms.push(backoff_ms);
                summary.error_restarts += 1;
                let sleep = Duration::from_millis(backoff_ms);
                info!(
                    sleep_ms = backoff_ms,
                    consecutive_failures, "supervisor backing off before restart"
                );
                tokio::select! {
                    _ = shutdown.wait() => {
                        return Ok(summary);
                    }
                    _ = tokio::time::sleep(sleep) => {}
                }
                backoff_ms = (backoff_ms.saturating_mul(2)).min(policy.max_backoff_ms);
            }
        }
    }
}

/// Run [`crate::run_consume_with`] under the supervisor with
/// `policy`. Each iteration constructs a fresh pipeline (and
/// fresh metrics counters) — restart resets the in-process
/// metric state, which matches the at-least-once contract: the
/// committed source-side checkpoint is the source of truth.
pub async fn run_supervised_consume(
    config: crate::PipelineCliConfig,
    options: crate::ConsumeOptions,
    policy: RestartPolicy,
    shutdown: ShutdownSignal,
) -> Result<SupervisorSummary, CliError> {
    run_supervised_with_runner(policy, shutdown, move || {
        // Clone per iteration so the closure stays `Fn` (callable
        // many times) and each future owns the data it needs.
        let cfg = config.clone();
        let opts = options.clone();
        async move { crate::run_consume_with(cfg, opts).await }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use ematix_flow_core::streaming::ShutdownSignal;
    use std::sync::Mutex;

    fn metric_with_rows(rows: u64) -> StreamingPipelineMetrics {
        StreamingPipelineMetrics {
            total_rows: rows,
            iterations: 1,
            shutdown_triggered: true,
        }
    }

    /// Helper: build a runner that pops from a queue of
    /// `Result<metrics, error>` per call. Pre-supplied results
    /// drive the deterministic test paths.
    fn scripted_runner(
        results: Vec<Result<StreamingPipelineMetrics, CliError>>,
    ) -> impl Fn() -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<StreamingPipelineMetrics, CliError>> + Send>,
    > {
        let queue = std::sync::Arc::new(Mutex::new(results.into_iter().collect::<Vec<_>>()));
        move || {
            let queue = queue.clone();
            Box::pin(async move {
                let mut q = queue.lock().unwrap();
                if q.is_empty() {
                    return Err(CliError::Runtime(
                        "test scripted_runner: queue empty".into(),
                    ));
                }
                q.remove(0)
            })
        }
    }

    #[test]
    fn restart_policy_default_is_disabled() {
        let p = RestartPolicy::default();
        assert!(!p.enabled);
        assert!(p.initial_backoff_ms >= 100);
        assert!(p.max_backoff_ms >= p.initial_backoff_ms);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ok_first_call_returns_clean_exit() {
        let (signal, _trigger) = ShutdownSignal::new();
        let runner = scripted_runner(vec![Ok(metric_with_rows(7))]);
        let summary = run_supervised_with_runner(RestartPolicy::default(), signal, runner)
            .await
            .unwrap();
        assert_eq!(summary.total_runs, 1);
        assert_eq!(summary.error_restarts, 0);
        assert_eq!(summary.final_metrics.as_ref().unwrap().total_rows, 7);
        assert!(summary.backoff_history_ms.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn err_with_restart_disabled_surfaces_error() {
        let (signal, _trigger) = ShutdownSignal::new();
        let runner = scripted_runner(vec![Err(CliError::Runtime("boom".into()))]);
        let policy = RestartPolicy {
            enabled: false,
            ..Default::default()
        };
        let err = run_supervised_with_runner(policy, signal, runner)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("boom"), "got: {err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn err_then_ok_with_restart_enabled_succeeds_after_one_restart() {
        let (signal, _trigger) = ShutdownSignal::new();
        let runner = scripted_runner(vec![
            Err(CliError::Runtime("transient".into())),
            Ok(metric_with_rows(11)),
        ]);
        let policy = RestartPolicy {
            enabled: true,
            initial_backoff_ms: 5,
            max_backoff_ms: 50,
            max_restarts: None,
            reset_after_secs: 60,
        };
        let summary = run_supervised_with_runner(policy, signal, runner)
            .await
            .unwrap();
        assert_eq!(summary.total_runs, 2);
        assert_eq!(summary.error_restarts, 1);
        assert_eq!(summary.backoff_history_ms, vec![5]);
        assert_eq!(summary.final_metrics.as_ref().unwrap().total_rows, 11);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backoff_doubles_until_max() {
        let (signal, _trigger) = ShutdownSignal::new();
        let runner = scripted_runner(vec![
            Err(CliError::Runtime("e1".into())),
            Err(CliError::Runtime("e2".into())),
            Err(CliError::Runtime("e3".into())),
            Err(CliError::Runtime("e4".into())),
            Ok(metric_with_rows(1)),
        ]);
        let policy = RestartPolicy {
            enabled: true,
            initial_backoff_ms: 5,
            max_backoff_ms: 15, // saturates after 5 → 10 → 15
            max_restarts: None,
            reset_after_secs: 60,
        };
        let summary = run_supervised_with_runner(policy, signal, runner)
            .await
            .unwrap();
        assert_eq!(summary.total_runs, 5);
        assert_eq!(summary.error_restarts, 4);
        // After each retry the *next* sleep doubles & saturates.
        // Sequence pushed: 5 (after first err), 10, 15, 15.
        assert_eq!(summary.backoff_history_ms, vec![5, 10, 15, 15]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn max_restarts_caps_consecutive_failures() {
        let (signal, _trigger) = ShutdownSignal::new();
        let runner = scripted_runner(vec![
            Err(CliError::Runtime("e1".into())),
            Err(CliError::Runtime("e2".into())),
            Err(CliError::Runtime("final".into())),
        ]);
        let policy = RestartPolicy {
            enabled: true,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
            max_restarts: Some(2),
            reset_after_secs: 60,
        };
        let err = run_supervised_with_runner(policy, signal, runner)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("final"), "got: {err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_during_backoff_exits_cleanly() {
        let (signal, trigger) = ShutdownSignal::new();
        // First error triggers a (long) backoff sleep; shutdown
        // fires during the sleep; supervisor should return Ok.
        let runner = scripted_runner(vec![Err(CliError::Runtime("e1".into()))]);
        let policy = RestartPolicy {
            enabled: true,
            initial_backoff_ms: 60_000, // long enough that the test triggers shutdown first
            max_backoff_ms: 60_000,
            max_restarts: None,
            reset_after_secs: 60,
        };
        // Trigger shutdown after 50ms.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            trigger.trigger();
        });
        let summary = run_supervised_with_runner(policy, signal, runner)
            .await
            .unwrap();
        assert_eq!(summary.total_runs, 1);
        // We did push the backoff onto history *before* the
        // shutdown-aware sleep — the supervisor accounts for the
        // intent to back off even if the sleep was cut short.
        assert_eq!(summary.backoff_history_ms.len(), 1);
        assert!(summary.final_metrics.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_before_first_run_exits_zero_runs() {
        let (signal, trigger) = ShutdownSignal::new();
        trigger.trigger();
        let runner = scripted_runner(vec![Err(CliError::Runtime("never invoked".into()))]);
        let summary = run_supervised_with_runner(RestartPolicy::default(), signal, runner)
            .await
            .unwrap();
        assert_eq!(summary.total_runs, 0);
        assert_eq!(summary.error_restarts, 0);
        assert!(summary.final_metrics.is_none());
    }
}
