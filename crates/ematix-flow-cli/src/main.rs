//! `flow` CLI entry point.
//!
//! Thin clap wrapper around [`ematix_flow_cli::run_consume`]. The
//! testable logic lives in the lib (config parsing, backend
//! factory, runner glue); `main.rs` just parses args and routes.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use ematix_flow_cli::{CliError, PipelineCliConfig, run_consume};
use tracing::{error, info};

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
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Commands::Consume { config } => match run_consume_cmd(&config).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                error!(error = %e, "flow consume failed");
                ExitCode::from(1)
            }
        },
    }
}

async fn run_consume_cmd(path: &std::path::Path) -> Result<(), CliError> {
    let cfg = PipelineCliConfig::from_path(path)?;
    info!(
        pipeline_name = cfg.pipeline_name.as_str(),
        source_query = cfg.source_query.as_str(),
        idle_pause_ms = cfg.idle_pause_ms,
        "starting pipeline"
    );
    let metrics = run_consume(cfg).await?;
    info!(
        total_rows = metrics.total_rows,
        iterations = metrics.iterations,
        shutdown_triggered = metrics.shutdown_triggered,
        "pipeline exited cleanly"
    );
    Ok(())
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
