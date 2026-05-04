//! `flow` CLI entry point.
//!
//! Thin clap wrapper around [`ematix_flow_cli::run_consume`]. The
//! testable logic lives in the lib (config parsing, backend
//! factory, runner glue); `main.rs` just parses args and routes.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use ematix_flow_cli::supervisor::{RestartPolicy, run_supervised_consume};
use ematix_flow_cli::{CliError, ConsumeOptions, PipelineCliConfig, run_consume_with};
use ematix_flow_core::streaming::install_shutdown_handler;
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(
    name = "flow",
    version,
    about = "ematix-flow streaming pipeline runner"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run a long-running source → target streaming pipeline from
    /// a TOML config file. Honors SIGTERM / SIGINT for graceful
    /// shutdown (drains the in-flight batch + commits source
    /// offsets before exiting).
    Consume {
        /// Path to the pipeline TOML config.
        #[arg(value_name = "CONFIG", default_value = "pipeline.toml")]
        config: PathBuf,
        /// If set, expose Prometheus metrics on `127.0.0.1:<PORT>` at
        /// `/metrics`. The server shares the pipeline's shutdown
        /// signal so both stop together.
        #[arg(long, value_name = "PORT")]
        metrics_port: Option<u16>,
        /// Restart the pipeline on error with exponential backoff.
        /// Off by default (CI / Kubernetes / systemd typically
        /// supervise externally). When on, see the tuning flags
        /// below for backoff schedule + restart cap.
        #[arg(long)]
        restart_on_error: bool,
        /// First backoff sleep (ms) after an error. Doubles each
        /// subsequent failure up to `--max-backoff-ms`.
        #[arg(long, value_name = "MS", default_value_t = 1_000)]
        initial_backoff_ms: u64,
        /// Maximum backoff sleep (ms) between retries.
        #[arg(long, value_name = "MS", default_value_t = 60_000)]
        max_backoff_ms: u64,
        /// Cap on consecutive restarts. Resets to zero after a run
        /// that lasted at least `--reset-backoff-after-secs`. Omit
        /// for unlimited.
        #[arg(long, value_name = "N")]
        max_restarts: Option<u32>,
        /// If a run lasted at least this long before erroring,
        /// reset the consecutive-restart counter and the backoff.
        #[arg(long, value_name = "SECS", default_value_t = 60)]
        reset_backoff_after_secs: u64,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Commands::Consume {
            config,
            metrics_port,
            restart_on_error,
            initial_backoff_ms,
            max_backoff_ms,
            max_restarts,
            reset_backoff_after_secs,
        } => {
            let policy = RestartPolicy {
                enabled: restart_on_error,
                initial_backoff_ms,
                max_backoff_ms,
                max_restarts,
                reset_after_secs: reset_backoff_after_secs,
            };
            match run_consume_cmd(&config, metrics_port, policy).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    error!(error = %e, "flow consume failed");
                    ExitCode::from(1)
                }
            }
        }
    }
}

async fn run_consume_cmd(
    path: &std::path::Path,
    metrics_port: Option<u16>,
    policy: RestartPolicy,
) -> Result<(), CliError> {
    let cfg = PipelineCliConfig::from_path(path)?;
    emit_inline_credential_deprecations(&cfg);
    info!(
        pipeline_name = cfg.pipeline_name.as_str(),
        source_query = cfg.source_query.as_str(),
        idle_pause_ms = cfg.idle_pause_ms,
        metrics_port = ?metrics_port,
        restart_on_error = policy.enabled,
        "starting pipeline"
    );
    let options = ConsumeOptions {
        metrics_port,
        // Binary path: install_shutdown_handler manages SIGTERM /
        // SIGINT internally.
        shutdown_signal: None,
    };
    if policy.enabled {
        // Single SIGTERM/SIGINT signal shared by the supervisor
        // (to break out of backoff sleeps early) and by the
        // wrapped pipeline (so an in-flight run also drains).
        let (shutdown, _shutdown_handle) = install_shutdown_handler();
        let summary = run_supervised_consume(cfg, options, policy, shutdown).await?;
        info!(
            total_runs = summary.total_runs,
            error_restarts = summary.error_restarts,
            backoff_history_ms = ?summary.backoff_history_ms,
            final_total_rows = summary.final_metrics.as_ref().map(|m| m.total_rows),
            "supervisor exited cleanly"
        );
    } else {
        let metrics = run_consume_with(cfg, options).await?;
        info!(
            total_rows = metrics.total_rows,
            iterations = metrics.iterations,
            shutdown_triggered = metrics.shutdown_triggered,
            "pipeline exited cleanly"
        );
    }
    Ok(())
}

/// Π.5: surface inline-credential findings as `tracing::warn!`
/// lines so users see the migration pointer toward the connection
/// registry. Silenced by `EMATIX_FLOW_NO_DEPRECATION=1` so CI runs
/// that intentionally use the legacy TOML form aren't noisy.
fn emit_inline_credential_deprecations(cfg: &PipelineCliConfig) {
    if std::env::var_os("EMATIX_FLOW_NO_DEPRECATION").is_some() {
        return;
    }
    let findings = cfg.inline_credential_findings();
    if findings.is_empty() {
        return;
    }
    warn!(
        pipeline_name = cfg.pipeline_name.as_str(),
        count = findings.len(),
        "DEPRECATION: this TOML config carries inline credentials. \
         The inline-credentials TOML loader is scheduled for removal one \
         minor release after the warning lands. Migrate to \
         `flow consume --module my_pipelines <name>` + the connection \
         registry; see docs/UNIFIED_PIPELINE_API.md. \
         Set EMATIX_FLOW_NO_DEPRECATION=1 to silence this warning."
    );
    for finding in &findings {
        warn!(detail = finding.as_str(), "inline credential");
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,ematix_flow=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}
