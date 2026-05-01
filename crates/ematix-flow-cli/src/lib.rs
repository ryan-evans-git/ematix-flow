//! CLI for ematix-flow.
//!
//! Houses the testable surfaces of the `flow` binary: config
//! parsing, backend instantiation from config, and the pipeline-run
//! glue. The binary entry point in `main.rs` is a thin wrapper
//! that calls into [`run_consume`].
//!
//! ## Phases
//!   - **CLI.1** — Scaffolding. `flow consume <toml>` parses a
//!     config, builds source + target backends, and runs a
//!     [`StreamingPipeline`] under SIGTERM/SIGINT shutdown.
//!     Backends supported: Kafka / RabbitMQ / Pub/Sub / Kinesis
//!     as sources; Postgres / MySQL / SQLite / DuckDB / Kafka /
//!     RabbitMQ / Pub/Sub / Kinesis / Delta (local + S3) /
//!     ObjectStore (local + S3, parquet / csv / orc / jsonl) as
//!     targets.
//!   - **CLI.2** — `/metrics` HTTP endpoint exposing the pipeline's
//!     Prometheus registry. Opt in with `--metrics-port <PORT>`.
//!     Server shares the pipeline's shutdown signal so both stop
//!     together.
//!   - **CLI.3 (this commit)** — Process-level supervisor:
//!     restart-on-crash with exponential backoff. Opt in with
//!     `--restart-on-error` plus tuning flags. See
//!     [`crate::supervisor`].
//!
//! ## TOML config shape
//! ```toml
//! pipeline_name = "events-to-pg"
//! source_query = "events"
//! idle_pause_ms = 500
//!
//! [source]
//! kind = "kafka"
//! bootstrap_servers = "localhost:9092"
//! group_id = "ematix-flow"
//!
//! [target]
//! kind = "postgres"
//! url = "postgres://localhost/mydb"
//!
//! [target.table]
//! schema = "public"
//! name = "events"
//! ```

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use ematix_flow_core::backend::{Backend, BackendError, ObjectFormat, TargetTable, WriteMode};
use ematix_flow_core::pg::PgPool;
use ematix_flow_core::streaming::{
    ShutdownSignal, StreamingPipeline, StreamingPipelineConfig, StreamingPipelineMetrics,
    install_shutdown_handler,
};

pub mod metrics_server;
pub mod supervisor;
use ematix_flow_core::{
    DeltaBackend, DuckDBBackend, KafkaBackend, KinesisBackend, MySQLBackend, ObjectStoreBackend,
    PostgresBackend, PubSubBackend, RabbitMQBackend, SQLiteBackend,
};
use serde::Deserialize;

/// Strip the password segment of a `scheme://user:password@host`
/// URL for display. Returns the URL unchanged if no userinfo or
/// no password segment is present.
///
/// Used by the redacting `Debug` impls on `SourceConfig` /
/// `TargetConfig` so logging a `PipelineCliConfig` (directly or
/// transitively via tracing's `?value` field) doesn't leak
/// credentials to log aggregators.
fn redact_db_url(url: &str) -> String {
    // Find scheme separator.
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let (scheme, rest) = url.split_at(scheme_end + 3); // include "://"
    let (authority, tail) = match rest.split_once('/') {
        Some((a, t)) => (a, format!("/{t}")),
        None => (rest, String::new()),
    };
    let (userinfo, host) = match authority.split_once('@') {
        Some((u, h)) => (u, h),
        None => return url.to_string(),
    };
    let user = userinfo.split(':').next().unwrap_or(userinfo);
    if user.is_empty() {
        format!("{scheme}<redacted>@{host}{tail}")
    } else {
        format!("{scheme}{user}:<redacted>@{host}{tail}")
    }
}

/// Top-level pipeline config loaded from a TOML file.
#[derive(Clone, Deserialize)]
pub struct PipelineCliConfig {
    /// Used for log lines + Prometheus labels.
    pub pipeline_name: String,
    /// Argument passed to `source.read_arrow_stream`. For Kafka /
    /// Kinesis: stream/topic name. For Pub/Sub: subscription. For
    /// RabbitMQ: queue name.
    pub source_query: String,
    /// Sleep duration (ms) when the source returns an empty batch.
    /// Defaults to 500 ms.
    #[serde(default = "default_idle_pause_ms")]
    pub idle_pause_ms: u64,
    /// Optional dead-letter topic. Same Kafka-only constraint as
    /// `StreamingPipelineConfig::dead_letter_topic`.
    pub dead_letter_topic: Option<String>,
    /// Source backend.
    pub source: SourceConfig,
    /// Target backend.
    pub target: TargetConfig,
}

fn default_idle_pause_ms() -> u64 {
    500
}

/// Source backend variants. Tagged on `kind` so TOML reads
/// naturally with `[source]` + `kind = "..."`.
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceConfig {
    Kafka {
        bootstrap_servers: String,
        group_id: Option<String>,
    },
    Rabbitmq {
        amqp_url: String,
    },
    Pubsub {
        project_id: String,
        endpoint: Option<String>,
        #[serde(default)]
        anonymous_auth: bool,
    },
    Kinesis {
        stream_name: String,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    },
}

/// Target backend variants. Includes both the OLTP/OLAP / Delta /
/// ObjectStore destinations (the typical streaming sink) and the
/// streaming backends themselves (for stream→stream pipelines).
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TargetConfig {
    Postgres {
        url: String,
        table: TableSpecConfig,
    },
    Mysql {
        url: String,
        table: TableSpecConfig,
    },
    Sqlite {
        path: String,
        table: TableSpecConfig,
    },
    Duckdb {
        path: String,
        table: TableSpecConfig,
    },
    Kafka {
        bootstrap_servers: String,
        group_id: Option<String>,
        topic: String,
        /// Phase 40.2: per-row Kafka message-key column. When set,
        /// each batch row's value in this column becomes the
        /// produced message's Kafka key. Column must be Utf8,
        /// LargeUtf8, or Binary. Default (omitted): round-robin.
        #[serde(default)]
        message_key_column: Option<String>,
    },
    Rabbitmq {
        amqp_url: String,
        queue: String,
    },
    Pubsub {
        project_id: String,
        endpoint: Option<String>,
        #[serde(default)]
        anonymous_auth: bool,
        topic: String,
    },
    Kinesis {
        stream_name: String,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        partition_key_prefix: String,
    },
    /// Local-filesystem-backed Delta table.
    DeltaLocal {
        path: String,
        table: TableSpecConfig,
        /// Phase 40.1: column names used for partitioned table
        /// creation on first write. Empty (default) =
        /// unpartitioned. Pre-existing tables retain their
        /// layout; deltalake-rs validates + rejects mismatches.
        #[serde(default)]
        partition_by: Vec<String>,
    },
    /// S3-backed Delta table. Production deployments with
    /// concurrent writers should configure DynamoDB locking
    /// separately — the `s3` feature flag in deltalake-aws
    /// registers the basic store; the framework doesn't expose
    /// per-table lock provider config yet.
    DeltaS3 {
        endpoint: String,
        bucket: String,
        /// Prefix under the bucket. Empty string = bucket root.
        #[serde(default)]
        prefix: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
        table: TableSpecConfig,
        /// Phase 40.1: column names used for partitioned table
        /// creation on first write. See DeltaLocal for details.
        #[serde(default)]
        partition_by: Vec<String>,
    },
    /// Local-filesystem object store (parquet / csv / orc / jsonl).
    /// `path` is the root directory; `prefix` is the per-table
    /// prefix (mapped to `TargetTable.name` so the framework's
    /// table-prefix builder produces `<root>/<prefix>/...`).
    ObjectStoreLocal {
        path: String,
        format: ObjectFormatConfig,
        prefix: String,
    },
    /// S3-backed object store (parquet / csv / orc / jsonl).
    ObjectStoreS3 {
        endpoint: String,
        bucket: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
        format: ObjectFormatConfig,
        prefix: String,
    },
}

// ---- Redacting Debug impls (credential safety) -----------------
//
// SourceConfig / TargetConfig / PipelineCliConfig deliberately do
// NOT derive Debug. The decoded TOML carries `url`, `amqp_url`,
// `access_key_id`, and `secret_access_key` fields — printing any
// of them through tracing's `?value` field would leak the
// credential into log aggregators. The hand-written impls below
// redact every secret-bearing field while keeping the
// non-secret bits visible for debuggability.

impl std::fmt::Debug for PipelineCliConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineCliConfig")
            .field("pipeline_name", &self.pipeline_name)
            .field("source_query", &self.source_query)
            .field("idle_pause_ms", &self.idle_pause_ms)
            .field("dead_letter_topic", &self.dead_letter_topic)
            .field("source", &self.source)
            .field("target", &self.target)
            .finish()
    }
}

impl std::fmt::Debug for SourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceConfig::Kafka {
                bootstrap_servers,
                group_id,
            } => f
                .debug_struct("Kafka")
                .field("bootstrap_servers", bootstrap_servers)
                .field("group_id", group_id)
                .finish(),
            SourceConfig::Rabbitmq { amqp_url } => f
                .debug_struct("Rabbitmq")
                .field("amqp_url", &redact_db_url(amqp_url))
                .finish(),
            SourceConfig::Pubsub {
                project_id,
                endpoint,
                anonymous_auth,
            } => f
                .debug_struct("Pubsub")
                .field("project_id", project_id)
                .field("endpoint", endpoint)
                .field("anonymous_auth", anonymous_auth)
                .finish(),
            SourceConfig::Kinesis {
                stream_name,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => f
                .debug_struct("Kinesis")
                .field("stream_name", stream_name)
                .field("region", region)
                .field("endpoint", endpoint)
                .field(
                    "access_key_id",
                    &access_key_id.as_ref().map(|_| "<redacted>"),
                )
                .field(
                    "secret_access_key",
                    &secret_access_key.as_ref().map(|_| "<redacted>"),
                )
                .finish(),
        }
    }
}

impl std::fmt::Debug for TargetConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetConfig::Postgres { url, table } => f
                .debug_struct("Postgres")
                .field("url", &redact_db_url(url))
                .field("table", table)
                .finish(),
            TargetConfig::Mysql { url, table } => f
                .debug_struct("Mysql")
                .field("url", &redact_db_url(url))
                .field("table", table)
                .finish(),
            TargetConfig::Sqlite { path, table } => f
                .debug_struct("Sqlite")
                .field("path", path)
                .field("table", table)
                .finish(),
            TargetConfig::Duckdb { path, table } => f
                .debug_struct("Duckdb")
                .field("path", path)
                .field("table", table)
                .finish(),
            TargetConfig::Kafka {
                bootstrap_servers,
                group_id,
                topic,
                message_key_column,
            } => f
                .debug_struct("Kafka")
                .field("bootstrap_servers", bootstrap_servers)
                .field("group_id", group_id)
                .field("topic", topic)
                .field("message_key_column", message_key_column)
                .finish(),
            TargetConfig::Rabbitmq { amqp_url, queue } => f
                .debug_struct("Rabbitmq")
                .field("amqp_url", &redact_db_url(amqp_url))
                .field("queue", queue)
                .finish(),
            TargetConfig::Pubsub {
                project_id,
                endpoint,
                anonymous_auth,
                topic,
            } => f
                .debug_struct("Pubsub")
                .field("project_id", project_id)
                .field("endpoint", endpoint)
                .field("anonymous_auth", anonymous_auth)
                .field("topic", topic)
                .finish(),
            TargetConfig::Kinesis {
                stream_name,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                partition_key_prefix,
            } => f
                .debug_struct("Kinesis")
                .field("stream_name", stream_name)
                .field("region", region)
                .field("endpoint", endpoint)
                .field(
                    "access_key_id",
                    &access_key_id.as_ref().map(|_| "<redacted>"),
                )
                .field(
                    "secret_access_key",
                    &secret_access_key.as_ref().map(|_| "<redacted>"),
                )
                .field("partition_key_prefix", partition_key_prefix)
                .finish(),
            TargetConfig::DeltaLocal {
                path,
                table,
                partition_by,
            } => f
                .debug_struct("DeltaLocal")
                .field("path", path)
                .field("table", table)
                .field("partition_by", partition_by)
                .finish(),
            TargetConfig::DeltaS3 {
                endpoint,
                bucket,
                prefix,
                region,
                access_key_id: _,
                secret_access_key: _,
                table,
                partition_by,
            } => f
                .debug_struct("DeltaS3")
                .field("endpoint", endpoint)
                .field("bucket", bucket)
                .field("prefix", prefix)
                .field("region", region)
                .field("access_key_id", &"<redacted>")
                .field("secret_access_key", &"<redacted>")
                .field("table", table)
                .field("partition_by", partition_by)
                .finish(),
            TargetConfig::ObjectStoreLocal {
                path,
                format,
                prefix,
            } => f
                .debug_struct("ObjectStoreLocal")
                .field("path", path)
                .field("format", format)
                .field("prefix", prefix)
                .finish(),
            TargetConfig::ObjectStoreS3 {
                endpoint,
                bucket,
                region,
                access_key_id: _,
                secret_access_key: _,
                format,
                prefix,
            } => f
                .debug_struct("ObjectStoreS3")
                .field("endpoint", endpoint)
                .field("bucket", bucket)
                .field("region", region)
                .field("access_key_id", &"<redacted>")
                .field("secret_access_key", &"<redacted>")
                .field("format", format)
                .field("prefix", prefix)
                .finish(),
        }
    }
}

/// CLI-side mirror of [`ObjectFormat`]. Defined separately so we
/// can derive `Deserialize` without touching core (where the
/// enum has no serde derives yet).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectFormatConfig {
    Parquet,
    Csv,
    Orc,
    /// `jsonl` (newline-delimited JSON), matching the framework's
    /// existing file-suffix convention.
    #[serde(alias = "jsonl")]
    JsonLines,
}

impl From<ObjectFormatConfig> for ObjectFormat {
    fn from(f: ObjectFormatConfig) -> Self {
        match f {
            ObjectFormatConfig::Parquet => ObjectFormat::Parquet,
            ObjectFormatConfig::Csv => ObjectFormat::Csv,
            ObjectFormatConfig::Orc => ObjectFormat::Orc,
            ObjectFormatConfig::JsonLines => ObjectFormat::JsonLines,
        }
    }
}

/// `(schema, name)` pair used by DB targets. Maps onto
/// [`TargetTable`].
#[derive(Debug, Clone, Deserialize)]
pub struct TableSpecConfig {
    #[serde(default)]
    pub schema: String,
    pub name: String,
}

impl PipelineCliConfig {
    /// Parse a TOML config from a string.
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        toml::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))
    }

    /// Read + parse a TOML config from a file path.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let s = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(format!("read {}: {e}", path.display())))?;
        Self::from_toml_str(&s)
    }

    /// Convert the source variant to a concrete `Arc<dyn Backend>`.
    pub fn build_source(&self) -> Result<Arc<dyn Backend>, BackendError> {
        match &self.source {
            SourceConfig::Kafka {
                bootstrap_servers,
                group_id,
            } => {
                let b = KafkaBackend::open(bootstrap_servers, group_id.as_deref())?;
                Ok(Arc::new(b))
            }
            SourceConfig::Rabbitmq { amqp_url } => {
                let b = RabbitMQBackend::open(amqp_url)?;
                Ok(Arc::new(b))
            }
            SourceConfig::Pubsub {
                project_id,
                endpoint,
                anonymous_auth,
            } => {
                let mut b = PubSubBackend::open(project_id)?;
                if let Some(ep) = endpoint {
                    b = b.with_endpoint(ep);
                }
                if *anonymous_auth {
                    b = b.with_anonymous_auth();
                }
                Ok(Arc::new(b))
            }
            SourceConfig::Kinesis {
                stream_name,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => Ok(Arc::new(build_kinesis_backend(
                stream_name,
                region.as_deref(),
                endpoint.as_deref(),
                access_key_id.as_deref(),
                secret_access_key.as_deref(),
            )?)),
        }
    }

    /// Convert the target variant to a concrete `Arc<dyn Backend>`
    /// + the [`TargetTable`] used by the pipeline.
    ///
    /// Streaming-sink targets (Kafka / Pub/Sub / Kinesis / RabbitMQ)
    /// project their per-kind name field onto `TargetTable.name`.
    pub async fn build_target(&self) -> Result<(Arc<dyn Backend>, TargetTable), BackendError> {
        match &self.target {
            TargetConfig::Postgres { url, table } => {
                let pool = PgPool::connect(url).await?;
                let b = PostgresBackend::new(Arc::new(pool), url.clone());
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::Mysql { url, table } => {
                let b = MySQLBackend::open(url)?;
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::Sqlite { path, table } => {
                let b = SQLiteBackend::open(path)?;
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::Duckdb { path, table } => {
                let b = DuckDBBackend::open(path)?;
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::Kafka {
                bootstrap_servers,
                group_id,
                topic,
                message_key_column,
            } => {
                let mut b = KafkaBackend::open(bootstrap_servers, group_id.as_deref())?;
                if let Some(col) = message_key_column {
                    b = b.with_message_key_column(col);
                }
                Ok((
                    Arc::new(b),
                    TargetTable {
                        schema: String::new(),
                        name: topic.clone(),
                    },
                ))
            }
            TargetConfig::Rabbitmq { amqp_url, queue } => {
                let b = RabbitMQBackend::open(amqp_url)?;
                Ok((
                    Arc::new(b),
                    TargetTable {
                        schema: String::new(),
                        name: queue.clone(),
                    },
                ))
            }
            TargetConfig::Pubsub {
                project_id,
                endpoint,
                anonymous_auth,
                topic,
            } => {
                let mut b = PubSubBackend::open(project_id)?;
                if let Some(ep) = endpoint {
                    b = b.with_endpoint(ep);
                }
                if *anonymous_auth {
                    b = b.with_anonymous_auth();
                }
                Ok((
                    Arc::new(b),
                    TargetTable {
                        schema: String::new(),
                        name: topic.clone(),
                    },
                ))
            }
            TargetConfig::Kinesis {
                stream_name,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                partition_key_prefix,
            } => Ok((
                Arc::new(build_kinesis_backend(
                    stream_name,
                    region.as_deref(),
                    endpoint.as_deref(),
                    access_key_id.as_deref(),
                    secret_access_key.as_deref(),
                )?),
                TargetTable {
                    schema: String::new(),
                    name: partition_key_prefix.clone(),
                },
            )),
            TargetConfig::DeltaLocal {
                path,
                table,
                partition_by,
            } => {
                let mut b = DeltaBackend::open_local(path)?;
                if !partition_by.is_empty() {
                    b = b.with_partition_columns(partition_by.clone());
                }
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::DeltaS3 {
                endpoint,
                bucket,
                prefix,
                region,
                access_key_id,
                secret_access_key,
                table,
                partition_by,
            } => {
                let mut b = DeltaBackend::open_s3(
                    endpoint,
                    bucket,
                    prefix,
                    region,
                    access_key_id,
                    secret_access_key,
                )?;
                if !partition_by.is_empty() {
                    b = b.with_partition_columns(partition_by.clone());
                }
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::ObjectStoreLocal {
                path,
                format,
                prefix,
            } => {
                let b = ObjectStoreBackend::open_local(path, (*format).into())?;
                Ok((
                    Arc::new(b),
                    TargetTable {
                        schema: String::new(),
                        name: prefix.clone(),
                    },
                ))
            }
            TargetConfig::ObjectStoreS3 {
                endpoint,
                bucket,
                region,
                access_key_id,
                secret_access_key,
                format,
                prefix,
            } => {
                let b = ObjectStoreBackend::open_s3(
                    endpoint,
                    bucket,
                    region,
                    access_key_id,
                    secret_access_key,
                    (*format).into(),
                )?;
                Ok((
                    Arc::new(b),
                    TargetTable {
                        schema: String::new(),
                        name: prefix.clone(),
                    },
                ))
            }
        }
    }

    /// Build the [`StreamingPipelineConfig`] used by the runner.
    pub fn streaming_config(&self, target: TargetTable) -> StreamingPipelineConfig {
        let mut cfg = StreamingPipelineConfig::new(
            self.source_query.clone(),
            target,
            self.pipeline_name.clone(),
        );
        cfg.idle_pause_ms = self.idle_pause_ms;
        cfg.mode = WriteMode::Append;
        if let Some(dlt) = &self.dead_letter_topic {
            cfg = cfg.with_dead_letter_topic(dlt.clone());
        }
        cfg
    }
}

impl From<&TableSpecConfig> for TargetTable {
    fn from(t: &TableSpecConfig) -> Self {
        TargetTable {
            schema: t.schema.clone(),
            name: t.name.clone(),
        }
    }
}

fn build_kinesis_backend(
    stream_name: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
) -> Result<KinesisBackend, BackendError> {
    let mut b = KinesisBackend::open(stream_name)?;
    if let Some(r) = region {
        b = b.with_region(r);
    }
    if let Some(ep) = endpoint {
        b = b.with_endpoint(ep);
    }
    if let (Some(ak), Some(sk)) = (access_key_id, secret_access_key) {
        b = b.with_static_credentials(ak, sk);
    }
    Ok(b)
}

/// Errors surfaced by config parsing / file IO.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config IO error: {0}")]
    Io(String),
    #[error("config parse error: {0}")]
    Parse(String),
}

/// Errors from the top-level `flow consume` flow.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("{0}")]
    Config(#[from] ConfigError),
    #[error("backend error: {0}")]
    Backend(#[from] BackendError),
    #[error("runtime error: {0}")]
    Runtime(String),
}

/// Optional runtime knobs for [`run_consume`]. CLI flags map onto
/// this; library callers can pass `Default::default()` for the
/// pipeline-only path.
#[derive(Debug, Clone, Default)]
pub struct ConsumeOptions {
    /// When `Some`, spawn a metrics HTTP server bound to
    /// `127.0.0.1:<port>` exposing `/metrics` from the pipeline's
    /// Prometheus registry. The server shares the pipeline's
    /// shutdown signal so both stop together.
    pub metrics_port: Option<u16>,
    /// Externally-provided shutdown signal. When `None`,
    /// [`run_consume_with`] installs the default SIGTERM/SIGINT
    /// handler. Tests + library callers that want to drive
    /// shutdown programmatically (without sending real signals)
    /// pass `Some(signal)`. Use [`ShutdownSignal::new`] to create
    /// the pair and keep the trigger end alive for the duration
    /// of the run.
    pub shutdown_signal: Option<ShutdownSignal>,
}

/// Run a single pipeline to completion (until shutdown). Used by
/// the `flow consume` subcommand and by integration tests.
///
/// Convenience wrapper around [`run_consume_with`] for callers
/// that don't need the runtime knobs.
pub async fn run_consume(config: PipelineCliConfig) -> Result<StreamingPipelineMetrics, CliError> {
    run_consume_with(config, ConsumeOptions::default()).await
}

/// Run a single pipeline with explicit [`ConsumeOptions`]. Spawns
/// the metrics server (if configured) before the pipeline starts,
/// then awaits the pipeline. The server is asked to shut down
/// when the pipeline exits, regardless of how it exited.
pub async fn run_consume_with(
    config: PipelineCliConfig,
    options: ConsumeOptions,
) -> Result<StreamingPipelineMetrics, CliError> {
    let source = config.build_source()?;
    let (target, target_table) = config.build_target().await?;
    let pipeline_cfg = config.streaming_config(target_table);
    let pipeline = StreamingPipeline::new(source, target, pipeline_cfg);

    // Pick a shutdown source. Tests + programmatic callers can
    // pass an external signal via ConsumeOptions; otherwise we
    // install the default SIGTERM/SIGINT handler. The signal-
    // listening task's JoinHandle (when we install) drops at the
    // end of this function — fine, the runtime tears it down.
    let (shutdown, _shutdown_handle) = match options.shutdown_signal.clone() {
        Some(signal) => (signal, None),
        None => {
            let (s, h) = install_shutdown_handler();
            (s, Some(h))
        }
    };

    // Optional metrics server. We give it its own ShutdownSignal
    // pair so the pipeline's exit (success OR error) can stop the
    // server before this function returns.
    let (metrics_signal, metrics_trigger) = ShutdownSignal::new();
    let metrics_handle = if let Some(port) = options.metrics_port {
        let addr: SocketAddr =
            format!("127.0.0.1:{port}")
                .parse()
                .map_err(|e: std::net::AddrParseError| {
                    CliError::Runtime(format!("metrics addr parse: {e}"))
                })?;
        let registry = Arc::new(pipeline.metrics_registry().clone());
        let (_bound, handle) = metrics_server::spawn_metrics_server(addr, registry, metrics_signal)
            .await
            .map_err(|e| CliError::Runtime(format!("metrics bind {addr}: {e}")))?;
        Some(handle)
    } else {
        // No server requested — drop the signal pair on the floor.
        drop(metrics_signal);
        None
    };

    let result = pipeline
        .run(shutdown)
        .await
        .map_err(|e| CliError::Runtime(e.to_string()));

    // Stop the metrics server (best effort).
    metrics_trigger.trigger();
    if let Some(handle) = metrics_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kafka_to_pg_toml() -> &'static str {
        r#"
            pipeline_name = "events-to-pg"
            source_query = "events"
            idle_pause_ms = 250

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            group_id = "ematix-flow"

            [target]
            kind = "postgres"
            url = "postgres://localhost/mydb"

            [target.table]
            schema = "public"
            name = "events"
        "#
    }

    #[test]
    fn parses_kafka_to_postgres_config() {
        let cfg = PipelineCliConfig::from_toml_str(kafka_to_pg_toml()).unwrap();
        assert_eq!(cfg.pipeline_name, "events-to-pg");
        assert_eq!(cfg.source_query, "events");
        assert_eq!(cfg.idle_pause_ms, 250);
        match &cfg.source {
            SourceConfig::Kafka {
                bootstrap_servers,
                group_id,
            } => {
                assert_eq!(bootstrap_servers, "localhost:9092");
                assert_eq!(group_id.as_deref(), Some("ematix-flow"));
            }
            other => panic!("expected Kafka source, got {other:?}"),
        }
        match &cfg.target {
            TargetConfig::Postgres { url, table } => {
                assert_eq!(url, "postgres://localhost/mydb");
                assert_eq!(table.schema, "public");
                assert_eq!(table.name, "events");
            }
            other => panic!("expected Postgres target, got {other:?}"),
        }
    }

    #[test]
    fn idle_pause_ms_defaults_when_omitted() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "q"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        assert_eq!(cfg.idle_pause_ms, 500);
    }

    #[test]
    fn parses_pubsub_with_anonymous_auth() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "my-sub"

            [source]
            kind = "pubsub"
            project_id = "my-project"
            endpoint = "http://localhost:8085"
            anonymous_auth = true

            [target]
            kind = "duckdb"
            path = ":memory:"

            [target.table]
            schema = "main"
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match &cfg.source {
            SourceConfig::Pubsub {
                project_id,
                endpoint,
                anonymous_auth,
            } => {
                assert_eq!(project_id, "my-project");
                assert_eq!(endpoint.as_deref(), Some("http://localhost:8085"));
                assert!(*anonymous_auth);
            }
            other => panic!("expected Pubsub source, got {other:?}"),
        }
    }

    #[test]
    fn parses_rabbitmq_to_kafka_stream_to_stream() {
        let toml = r#"
            pipeline_name = "rb-to-kafka"
            source_query = "in-queue"

            [source]
            kind = "rabbitmq"
            amqp_url = "amqp://localhost:5672"

            [target]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            topic = "out-topic"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match (&cfg.source, &cfg.target) {
            (
                SourceConfig::Rabbitmq { amqp_url },
                TargetConfig::Kafka {
                    bootstrap_servers,
                    topic,
                    ..
                },
            ) => {
                assert_eq!(amqp_url, "amqp://localhost:5672");
                assert_eq!(bootstrap_servers, "localhost:9092");
                assert_eq!(topic, "out-topic");
            }
            other => panic!("unexpected pair: {other:?}"),
        }
    }

    #[test]
    fn parses_kinesis_with_static_creds() {
        let toml = r#"
            pipeline_name = "kn"
            source_query = "any"

            [source]
            kind = "kinesis"
            stream_name = "events"
            region = "us-east-1"
            endpoint = "http://localhost:4566"
            access_key_id = "fake"
            secret_access_key = "fake"

            [target]
            kind = "delta_local"
            path = "/tmp/delta-table"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match &cfg.source {
            SourceConfig::Kinesis {
                stream_name,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => {
                assert_eq!(stream_name, "events");
                assert_eq!(region.as_deref(), Some("us-east-1"));
                assert_eq!(endpoint.as_deref(), Some("http://localhost:4566"));
                assert_eq!(access_key_id.as_deref(), Some("fake"));
                assert_eq!(secret_access_key.as_deref(), Some("fake"));
            }
            other => panic!("expected Kinesis source, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_source_kind() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "q"

            [source]
            kind = "bogus"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown variant") || msg.contains("bogus"),
            "got: {msg}"
        );
    }

    #[test]
    fn streaming_config_passes_through_dead_letter_topic() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "q"
            dead_letter_topic = "p-dlq"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let table = TargetTable {
            schema: "".into(),
            name: "t".into(),
        };
        let sc = cfg.streaming_config(table);
        assert_eq!(sc.dead_letter_topic.as_deref(), Some("p-dlq"));
        assert_eq!(sc.idle_pause_ms, 500);
        assert_eq!(sc.pipeline_name, "p");
    }

    #[test]
    fn build_source_constructs_kafka_backend() {
        let cfg = PipelineCliConfig::from_toml_str(kafka_to_pg_toml()).unwrap();
        let _src = cfg.build_source().expect("build_source");
        // The Backend trait doesn't expose a direct dialect comparison here
        // without an active connection, but unwrapping the Arc returns the
        // KafkaBackend handle.
    }

    #[test]
    fn parses_delta_s3_target() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "delta_s3"
            endpoint = "http://localhost:9000"
            bucket = "events"
            prefix = "raw"
            region = "us-east-1"
            access_key_id = "minioadmin"
            secret_access_key = "minioadmin"

            [target.table]
            schema = "default"
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match &cfg.target {
            TargetConfig::DeltaS3 {
                endpoint,
                bucket,
                prefix,
                region,
                access_key_id,
                secret_access_key,
                table,
                partition_by,
            } => {
                assert_eq!(endpoint, "http://localhost:9000");
                assert_eq!(bucket, "events");
                assert_eq!(prefix, "raw");
                assert_eq!(region, "us-east-1");
                assert_eq!(access_key_id, "minioadmin");
                assert_eq!(secret_access_key, "minioadmin");
                assert_eq!(table.name, "events");
                assert!(partition_by.is_empty(), "default partition_by is empty");
            }
            other => panic!("expected DeltaS3, got {other:?}"),
        }
    }

    /// Phase 40.1: TOML `partition_by` field carries through to
    /// `TargetConfig::DeltaLocal`.
    /// Credential safety: Debug on `PipelineCliConfig` must redact
    /// every secret field. This is what `info!(?cfg, ...)` /
    /// `error!(?cfg, ...)` would render through tracing — those
    /// outputs go to log aggregators and must not include
    /// passwords, access keys, etc.
    #[test]
    fn debug_redacts_postgres_url_password() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "postgres"
            url = "postgres://alice:DO_NOT_LEAK@host/db"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let s = format!("{cfg:?}");
        assert!(!s.contains("DO_NOT_LEAK"), "Debug leaked password: {s}");
        assert!(s.contains("<redacted>"), "expected <redacted> marker: {s}");
        assert!(s.contains("alice"), "username should remain: {s}");
        assert!(s.contains("host"), "host should remain: {s}");
    }

    #[test]
    fn debug_redacts_rabbitmq_amqp_url_password() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "q"

            [source]
            kind = "rabbitmq"
            amqp_url = "amqp://guest:DO_NOT_LEAK@broker.local/vh"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let s = format!("{cfg:?}");
        assert!(
            !s.contains("DO_NOT_LEAK"),
            "Debug leaked AMQP password: {s}"
        );
        assert!(s.contains("guest"), "username should remain: {s}");
    }

    #[test]
    fn debug_redacts_kinesis_static_credentials() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "any"

            [source]
            kind = "kinesis"
            stream_name = "events"
            region = "us-east-1"
            access_key_id = "AKIA_DO_NOT_LEAK"
            secret_access_key = "SECRET_DO_NOT_LEAK"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let s = format!("{cfg:?}");
        assert!(!s.contains("AKIA_DO_NOT_LEAK"), "Debug leaked AK: {s}");
        assert!(!s.contains("SECRET_DO_NOT_LEAK"), "Debug leaked SK: {s}");
        assert!(
            s.contains("us-east-1"),
            "non-secret region should remain: {s}"
        );
    }

    #[test]
    fn debug_redacts_delta_s3_credentials() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "delta_s3"
            endpoint = "http://localhost:9000"
            bucket = "events"
            region = "us-east-1"
            access_key_id = "AKIA_DELTA_DO_NOT_LEAK"
            secret_access_key = "SECRET_DELTA_DO_NOT_LEAK"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let s = format!("{cfg:?}");
        assert!(
            !s.contains("AKIA_DELTA_DO_NOT_LEAK"),
            "Debug leaked Delta S3 AK: {s}"
        );
        assert!(
            !s.contains("SECRET_DELTA_DO_NOT_LEAK"),
            "Debug leaked Delta S3 SK: {s}"
        );
        assert!(s.contains("events"), "bucket should remain: {s}");
    }

    #[test]
    fn redact_db_url_handles_common_shapes() {
        // No userinfo → unchanged.
        assert_eq!(
            redact_db_url("postgres://localhost/mydb"),
            "postgres://localhost/mydb"
        );
        // With password → redacted.
        assert_eq!(
            redact_db_url("postgres://alice:s3cret@host:5432/db"),
            "postgres://alice:<redacted>@host:5432/db"
        );
        // mysql:// works the same way.
        assert_eq!(
            redact_db_url("mysql://u:p@host:3306/db"),
            "mysql://u:<redacted>@host:3306/db"
        );
        // Username-only userinfo.
        assert_eq!(
            redact_db_url("postgres://justuser@host/db"),
            "postgres://justuser:<redacted>@host/db"
        );
        // Schemeless string passes through.
        assert_eq!(redact_db_url("nothing"), "nothing");
    }

    #[test]
    fn parses_delta_local_with_partition_by() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "delta_local"
            path = "/tmp/events"
            partition_by = ["year", "month"]

            [target.table]
            schema = "default"
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match &cfg.target {
            TargetConfig::DeltaLocal { partition_by, .. } => {
                assert_eq!(partition_by, &vec!["year".to_string(), "month".to_string()]);
            }
            other => panic!("expected DeltaLocal, got {other:?}"),
        }
    }

    /// Phase 40.1: omitted `partition_by` defaults to empty.
    #[test]
    fn delta_local_partition_by_defaults_to_empty() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "delta_local"
            path = "/tmp/events"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match &cfg.target {
            TargetConfig::DeltaLocal { partition_by, .. } => assert!(partition_by.is_empty()),
            other => panic!("expected DeltaLocal, got {other:?}"),
        }
    }

    /// Phase 40.2: TOML `message_key_column` field on Kafka target.
    #[test]
    fn parses_kafka_target_with_message_key_column() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "src"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            topic = "events"
            message_key_column = "user_id"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match &cfg.target {
            TargetConfig::Kafka {
                message_key_column, ..
            } => {
                assert_eq!(message_key_column.as_deref(), Some("user_id"));
            }
            other => panic!("expected Kafka, got {other:?}"),
        }
    }

    /// Phase 40.2: omitted `message_key_column` defaults to None
    /// (round-robin, pre-40.2 behavior).
    #[test]
    fn kafka_target_message_key_column_defaults_to_none() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "src"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            topic = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match &cfg.target {
            TargetConfig::Kafka {
                message_key_column, ..
            } => {
                assert!(message_key_column.is_none());
            }
            other => panic!("expected Kafka, got {other:?}"),
        }
    }

    #[test]
    fn parses_object_store_local_target_with_each_format() {
        for (format_str, want) in [
            ("parquet", ObjectFormatConfig::Parquet),
            ("csv", ObjectFormatConfig::Csv),
            ("orc", ObjectFormatConfig::Orc),
            ("json_lines", ObjectFormatConfig::JsonLines),
            ("jsonl", ObjectFormatConfig::JsonLines), // alias
        ] {
            let toml = format!(
                r#"
                    pipeline_name = "p"
                    source_query = "topic"

                    [source]
                    kind = "kafka"
                    bootstrap_servers = "localhost:9092"

                    [target]
                    kind = "object_store_local"
                    path = "/var/data"
                    format = "{format_str}"
                    prefix = "events"
                "#
            );
            let cfg = PipelineCliConfig::from_toml_str(&toml).unwrap();
            match &cfg.target {
                TargetConfig::ObjectStoreLocal {
                    path,
                    format,
                    prefix,
                } => {
                    assert_eq!(path, "/var/data");
                    assert_eq!(prefix, "events");
                    let parsed: ObjectFormat = (*format).into();
                    let expected: ObjectFormat = want.into();
                    assert_eq!(parsed, expected, "format mismatch for {format_str}");
                }
                other => panic!("expected ObjectStoreLocal, got {other:?}"),
            }
        }
    }

    #[test]
    fn parses_object_store_s3_target() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "object_store_s3"
            endpoint = "http://localhost:9000"
            bucket = "events"
            region = "us-east-1"
            access_key_id = "minioadmin"
            secret_access_key = "minioadmin"
            format = "parquet"
            prefix = "raw"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match &cfg.target {
            TargetConfig::ObjectStoreS3 {
                endpoint,
                bucket,
                region,
                access_key_id,
                secret_access_key,
                format,
                prefix,
            } => {
                assert_eq!(endpoint, "http://localhost:9000");
                assert_eq!(bucket, "events");
                assert_eq!(region, "us-east-1");
                assert_eq!(access_key_id, "minioadmin");
                assert_eq!(secret_access_key, "minioadmin");
                assert_eq!(prefix, "raw");
                let parsed: ObjectFormat = (*format).into();
                assert_eq!(parsed, ObjectFormat::Parquet);
            }
            other => panic!("expected ObjectStoreS3, got {other:?}"),
        }
    }

    #[test]
    fn delta_s3_prefix_defaults_to_empty_when_omitted() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "delta_s3"
            endpoint = "http://localhost:9000"
            bucket = "events"
            region = "us-east-1"
            access_key_id = "minioadmin"
            secret_access_key = "minioadmin"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match &cfg.target {
            TargetConfig::DeltaS3 { prefix, .. } => assert_eq!(prefix, ""),
            other => panic!("expected DeltaS3, got {other:?}"),
        }
    }

    #[test]
    fn from_path_reads_config_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, kafka_to_pg_toml()).unwrap();
        let cfg = PipelineCliConfig::from_path(&path).unwrap();
        assert_eq!(cfg.pipeline_name, "events-to-pg");
    }
}
