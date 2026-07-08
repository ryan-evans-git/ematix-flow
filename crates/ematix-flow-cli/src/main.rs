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

/// Run the engine on mimalloc, matching the benchmark harness. The shipped
/// product otherwise defaults to the system allocator and is ~10% slower on
/// join/alloc-heavy queries (verified on the engine path: Q08 195→176ms).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(test)]
mod alloc_guard {
    // Production-allocator parity: the shipped `flow` binary must run on
    // mimalloc, not the system allocator. Measured ~10% on join/alloc-heavy
    // TPC-H queries (e.g. Q08 195ms system → 176ms mimalloc on the engine path);
    // the benchmarks already use mimalloc, so without this the product ships
    // slower than its own published numbers. A heap allocation must land in
    // mimalloc's managed region — `mi_is_in_heap_region` returns false for a
    // system-malloc pointer, so this test is RED until the `#[global_allocator]`
    // is wired in this crate.
    #[test]
    fn global_allocator_is_mimalloc() {
        let buf: Vec<u8> = vec![0xA5u8; 64 * 1024];
        let p = buf.as_ptr() as *const core::ffi::c_void;
        let in_mimalloc = unsafe { libmimalloc_sys::mi_is_in_heap_region(p) };
        core::hint::black_box(&buf);
        assert!(
            in_mimalloc,
            "global allocator is NOT mimalloc — `flow` would ship on the system allocator (~10% slower on join-heavy queries)"
        );
    }
}

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
    /// Run a single distributed-query shard from a WorkUnit JSON
    /// spec. Phase Z: one-shot ephemeral worker pattern. Reads the
    /// spec from a file (`--work-unit-file`) or stdin
    /// (`--work-unit-stdin`), executes the query against the
    /// configured input, writes the partial result to the spec's
    /// output URI, and exits.
    ///
    /// Exit codes:
    ///   0 — success, result written to output URI
    ///   2 — config error (bad spec, unreachable input)
    ///   3 — execution error (query failed mid-run)
    ///   4 — output error (write failed)
    ///
    /// Stdout: a single JSON line conforming to
    /// `ematix_flow_distributed::work_unit::WorkUnitMetrics`.
    /// Stderr: tracing logs. The coordinator parses stdout cleanly.
    ///
    /// Today this is a skeleton — parses + validates + emits the
    /// metrics envelope but does NOT execute the query. Query
    /// execution lands in a follow-up commit.
    RunShard {
        /// Path to the WorkUnit JSON spec.
        #[arg(long, value_name = "PATH", conflicts_with = "work_unit_stdin")]
        work_unit_file: Option<PathBuf>,
        /// Read the WorkUnit JSON spec from stdin.
        #[arg(long, conflicts_with = "work_unit_file")]
        work_unit_stdin: bool,
    },
    /// Create and manage sidecar indexes (`.parquet.idx`) on existing
    /// Parquet files — Postgres-style indexing without rewriting the source.
    Index {
        #[command(subcommand)]
        action: IndexAction,
    },
}

#[derive(Debug, Subcommand)]
enum IndexAction {
    /// Build a sorted sidecar index on a column of an existing Parquet file,
    /// writing `<source>.parquet.idx` beside it (or `--out`). The source
    /// Parquet is never modified — the sidecar is a separate file that a
    /// reader consults to skip whole row groups and masked-decode only the
    /// matching rows.
    ///
    /// The column's physical type selects the writer: INT64 / INT32 /
    /// BYTE_ARRAY are supported (eq + range). Exit code 2 on a bad
    /// column/type, 3 on a write failure.
    Build {
        /// Path to the source `.parquet` file.
        #[arg(value_name = "PARQUET")]
        parquet: PathBuf,
        /// Column to index: a leaf name (e.g. `l_orderkey`) or a numeric
        /// ordinal (e.g. `0`).
        #[arg(long, value_name = "COL")]
        column: String,
        /// Logical index name the reader addresses (e.g. `idx_orderkey`).
        #[arg(long, value_name = "NAME")]
        name: String,
        /// Sidecar output path. Defaults to `<source>.parquet.idx`.
        #[arg(long, value_name = "PATH")]
        out: Option<PathBuf>,
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
        Commands::RunShard {
            work_unit_file,
            work_unit_stdin,
        } => run_shard_cmd(work_unit_file.as_deref(), work_unit_stdin).await,
        Commands::Index { action } => match action {
            IndexAction::Build {
                parquet,
                column,
                name,
                out,
            } => index_build_cmd(&parquet, &column, &name, out),
        },
    }
}

/// `flow index build` — backfill a sorted sidecar index onto an existing
/// Parquet file. Thin shell over
/// [`ematix_flow_core::sidecar_build::build_sorted_sidecar`]; the source is
/// never modified. Exit codes: 2 = bad column / unsupported type / unreadable
/// source, 3 = sidecar write failed.
fn index_build_cmd(
    parquet: &std::path::Path,
    column: &str,
    name: &str,
    out: Option<PathBuf>,
) -> ExitCode {
    use ematix_flow_core::sidecar_build::build_sorted_sidecar;

    match build_sorted_sidecar(parquet, name, column, out) {
        Ok(path) => {
            info!(
                sidecar = %path.display(),
                source = %parquet.display(),
                index = name,
                column,
                "sidecar index built"
            );
            // The written path on stdout so scripts can capture it.
            println!("{}", path.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            // Classify by message: a bad column reference / unsupported type /
            // unreadable source is a user error (2); anything else is treated
            // as a write failure (3).
            let msg = format!("{e}");
            let is_user_error = msg.contains("no leaf column named")
                || msg.contains("out of range")
                || msg.contains("physical type") // unsupported-type NotImplemented
                || msg.contains("index build: open ")
                || msg.contains("read metadata");
            let code = if is_user_error { 2 } else { 3 };
            error!(error = %e, exit_code = code, "flow index build failed");
            ExitCode::from(code)
        }
    }
}

/// `flow run-shard` entry point. Phase Z ephemeral worker —
/// parses + validates the WorkUnit, executes the query against
/// `EmatixFastParquetTableProvider`, writes Arrow IPC to the
/// output URI, and emits the metrics JSON on stdout.
async fn run_shard_cmd(file: Option<&std::path::Path>, from_stdin: bool) -> ExitCode {
    use ematix_flow_cli::run_shard::execute_work_unit;
    use ematix_flow_distributed::work_unit::WorkUnit;
    use std::io::Read;

    // ---- Read the WorkUnit JSON ----
    let raw = match (file, from_stdin) {
        (Some(p), false) => match std::fs::read_to_string(p) {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, path = %p.display(), "cannot read work-unit file");
                return ExitCode::from(2);
            }
        },
        (None, true) => {
            let mut s = String::new();
            if let Err(e) = std::io::stdin().read_to_string(&mut s) {
                error!(error = %e, "cannot read work-unit from stdin");
                return ExitCode::from(2);
            }
            s
        }
        _ => {
            error!("must pass exactly one of --work-unit-file or --work-unit-stdin");
            return ExitCode::from(2);
        }
    };

    // ---- Parse + validate ----
    let wu: WorkUnit = match serde_json::from_str(&raw) {
        Ok(wu) => wu,
        Err(e) => {
            error!(error = %e, "work-unit JSON parse failed");
            return ExitCode::from(2);
        }
    };
    if let Err(e) = wu.validate_schema() {
        error!(reason = %e, "work-unit schema not supported");
        return ExitCode::from(2);
    }
    info!(
        work_unit_id = wu.id.as_str(),
        query = ?wu.query,
        "work-unit parsed; starting execution"
    );

    // ---- Execute ----
    let metrics = match execute_work_unit(&wu).await {
        Ok(m) => m,
        Err(e) => {
            let code = e.exit_code();
            error!(error = %e, exit_code = code, "execute_work_unit failed");
            return ExitCode::from(code);
        }
    };

    // ---- Emit metrics JSON on stdout (one line) ----
    match serde_json::to_string(&metrics) {
        Ok(line) => {
            println!("{}", line);
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!(error = %e, "failed to serialise WorkUnitMetrics");
            ExitCode::from(4)
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
        // No UDFs from the CLI binary path — only the Python
        // facade (`run_streaming_pipeline(udfs=...)`) registers
        // UDFs today. TOML can't carry callables. Aggregate UDFs
        // share the same Python-only constraint.
        udfs: Vec::new(),
        aggregate_udfs: Vec::new(),
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
    // Logs go to STDERR so stdout stays clean for machine-readable output:
    // `run-shard`'s WorkUnitMetrics JSON and `index build`'s written sidecar
    // path. (`run-shard`'s doc already specifies "Stderr: tracing logs".)
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init();
}
