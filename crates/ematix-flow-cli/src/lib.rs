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
//!     Backends in CLI.1: Kafka / RabbitMQ / Pub/Sub / Kinesis as
//!     sources; Postgres / MySQL / SQLite / DuckDB / Kafka /
//!     RabbitMQ / Pub/Sub / Kinesis / Delta-local as targets.
//!     S3-backed Delta + the `ObjectStore` formats are deferred to a
//!     follow-up sub-phase.
//!   - **CLI.2 (this commit)** — `/metrics` HTTP endpoint exposing
//!     the pipeline's Prometheus registry. Opt in with
//!     `--metrics-port <PORT>`. Server shares the pipeline's
//!     shutdown signal so both stop together.
//!   - **CLI.3** — Process-level supervisor: restart-on-crash with
//!     exponential backoff, multi-pipeline concurrency.
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

use ematix_flow_core::backend::{Backend, BackendError, TargetTable, WriteMode};
use ematix_flow_core::pg::PgPool;
use ematix_flow_core::streaming::{
    ShutdownSignal, StreamingPipeline, StreamingPipelineConfig, StreamingPipelineMetrics,
    install_shutdown_handler,
};

pub mod metrics_server;
use ematix_flow_core::{
    DeltaBackend, DuckDBBackend, KafkaBackend, KinesisBackend, MySQLBackend, PostgresBackend,
    PubSubBackend, RabbitMQBackend, SQLiteBackend,
};
use serde::Deserialize;

/// Top-level pipeline config loaded from a TOML file.
#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, Deserialize)]
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
    /// Local-filesystem-backed Delta table. S3-backed Delta + the
    /// `ObjectStore` formats land in a follow-up CLI sub-phase
    /// (their format / region / credentials surface doesn't fit
    /// cleanly into one tagged enum yet).
    DeltaLocal {
        path: String,
        table: TableSpecConfig,
    },
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
            } => {
                let b = KafkaBackend::open(bootstrap_servers, group_id.as_deref())?;
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
            TargetConfig::DeltaLocal { path, table } => {
                let b = DeltaBackend::open_local(path)?;
                Ok((Arc::new(b), table.into()))
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

    // install_shutdown_handler returns (signal, JoinHandle); the
    // handle drops at the end of this function — fine, the signal
    // task lives until SIGTERM/SIGINT or the runtime tears down.
    let (shutdown, _shutdown_handle) = install_shutdown_handler();

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
    fn from_path_reads_config_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, kafka_to_pg_toml()).unwrap();
        let cfg = PipelineCliConfig::from_path(&path).unwrap();
        assert_eq!(cfg.pipeline_name, "events-to-pg");
    }
}
