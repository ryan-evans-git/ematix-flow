//! Phase 30a: backend abstraction scaffolding.
//!
//! This module introduces the `Backend` trait and supporting types
//! that future backends (MySQL, SQLite, DuckDB, object storage,
//! Iceberg/Delta, Kafka, etc.) will implement. The existing Postgres
//! code in `pg.rs` remains the source of truth in this commit; a
//! `PostgresBackend` wrapper is provided as the first impl, delegating
//! to `PgPool`.
//!
//! Subsequent Phase 30 sub-commits will:
//!   - 30b: add Arrow streaming I/O methods (`read_arrow_stream` /
//!     `write_arrow_stream`) to the trait + the Postgres impl.
//!   - 30c: route `pipeline.sync` through the trait so cross-backend
//!     dispatch becomes a real code path.
//!   - 30d: migrate strategy executors (`run_append`, `run_merge`,
//!     `run_scd2`, ...) onto the trait so they're dialect-aware.
//!
//! Public API stays unchanged in 30a — existing PyO3 bindings still
//! call into `PgPool` directly.
//!
//! See `docs/MULTI_BACKEND_PLAN.md` §3 for the full design.

use std::pin::Pin;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use futures_util::Stream;
use thiserror::Error;
use tokio_postgres::types::Type as PgType;

use crate::meta::WatermarkConfig;
use crate::pg::{ConnectionInfo, PgError, PgPool};

/// Backend kind. Used by the planner / dispatcher to pick a same-backend
/// fast path vs. an Arrow streaming bridge for cross-backend syncs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dialect {
    Postgres,
    MySQL,
    SQLite,
    DuckDB,
    Iceberg,
    Delta,
    ObjectStore { format: ObjectFormat },
    Streaming { kind: StreamingKind },
}

/// File format for raw object-storage targets (Phase 34).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectFormat {
    Parquet,
    Csv,
    Orc,
    JsonLines,
}

/// Per-format write-time options. Defaults match the historical
/// pre-Π.1.4 behavior: Parquet uncompressed, CSV with comma delimiter
/// + header row. Pass via [`ObjectStoreBackend::with_write_options`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ObjectWriteOptions {
    /// Compression codec for Parquet writes. `None` keeps the
    /// historical default (UNCOMPRESSED). Only consulted when the
    /// backend's [`ObjectFormat`] is `Parquet`.
    pub parquet_compression: Option<ParquetCompression>,
    /// CSV column delimiter (single byte). `None` = comma. Used only
    /// when the backend's format is `Csv`.
    pub csv_delimiter: Option<u8>,
    /// Whether to write a header row in CSV output. `None` = true.
    /// Used only when the backend's format is `Csv`.
    pub csv_header: Option<bool>,
}

/// Parquet compression codec. Subset of the parquet crate's
/// `Compression` enum exposed at our backend boundary — kept narrow
/// because the wider set (LZO, BROTLI, LZ4, LZ4_RAW) is rarely
/// asked for in production. Each variant maps to a single
/// `parquet::basic::Compression` value at write time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    /// No compression. Same as today's default.
    Uncompressed,
    /// Hadoop-ecosystem default. Fast encode/decode, ~30% ratio for
    /// typical analytics data.
    Snappy,
    /// Universal compatibility. Slower than Snappy, similar ratio.
    Gzip,
    /// Modern best-in-class. ZstdLevel(3) — the parquet crate's
    /// default level, balanced encode speed vs ratio.
    Zstd,
}

/// Streaming source/sink kind (Phase 36–37).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamingKind {
    Kafka,
    Kinesis,
    PubSub,
    RabbitMQ,
}

impl Dialect {
    /// Whether two dialects can use a same-backend fast path. Identical
    /// dialects always agree; ObjectStore variants agree only when the
    /// file format also matches.
    pub fn matches(&self, other: &Dialect) -> bool {
        self == other
    }
}

/// Backend-agnostic error type. Each backend wraps its own native errors.
#[derive(Debug, Error)]
pub enum BackendError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("query error: {0}")]
    Query(String),
    #[error("type mapping error: {0}")]
    TypeMapping(String),
    #[error("backend error: {0}")]
    Other(String),
}

impl From<PgError> for BackendError {
    fn from(err: PgError) -> Self {
        match err {
            PgError::Url(s) => BackendError::Connection(s),
            PgError::Pool(s) => BackendError::Connection(s),
            // Reuse the PG error's already-formatted DB message.
            PgError::Postgres(_) => BackendError::Query(err.to_string()),
        }
    }
}

/// Target-table reference passed to `write_arrow_stream`. Concrete enough
/// to address a row destination in any backend (DB schema + name, S3
/// prefix, Kafka topic) without coupling the trait to dialect-specific
/// type catalogues.
#[derive(Debug, Clone)]
pub struct TargetTable {
    pub schema: String,
    pub name: String,
}

/// Write semantics for `write_arrow_stream`. Limited to the modes the
/// universal Arrow path can serve directly. Merge/SCD2 still go through
/// the dialect-specific strategy executors (Phase 30d).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Append,
    Truncate,
}

/// Stream of Arrow `RecordBatch`es. The universal IO contract for
/// cross-backend pipelines (Phase 30b → onwards).
pub type ArrowBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, BackendError>> + Send>>;

/// Phase 30d: dialect-agnostic result of running a strategy. Each row-
/// count field is `Option<i64>` because not every strategy meaningfully
/// reports every count (an append doesn't update or close anything).
#[derive(Debug, Clone)]
pub struct StrategyRunResult {
    pub run_id: String,
    pub rows_inserted: i64,
    pub rows_updated: Option<i64>,
    pub rows_unchanged: Option<i64>,
    pub rows_closed: Option<i64>,
    pub status: String,
    pub path: String,
}

impl From<crate::pg::AppendRunResult> for StrategyRunResult {
    fn from(r: crate::pg::AppendRunResult) -> Self {
        Self {
            run_id: r.run_id,
            rows_inserted: r.rows_inserted,
            rows_updated: None,
            rows_unchanged: None,
            rows_closed: None,
            status: r.status,
            path: r.path,
        }
    }
}

impl From<crate::pg::MergeRunResult> for StrategyRunResult {
    fn from(r: crate::pg::MergeRunResult) -> Self {
        Self {
            run_id: r.run_id,
            rows_inserted: r.rows_inserted,
            rows_updated: Some(r.rows_updated),
            rows_unchanged: Some(r.rows_unchanged),
            rows_closed: None,
            status: r.status,
            path: r.path,
        }
    }
}

impl From<crate::pg::Scd2RunResult> for StrategyRunResult {
    fn from(r: crate::pg::Scd2RunResult) -> Self {
        Self {
            run_id: r.run_id,
            rows_inserted: r.rows_inserted,
            rows_updated: None,
            rows_unchanged: None,
            rows_closed: Some(r.rows_closed),
            status: r.status,
            path: r.path,
        }
    }
}

/// The unified backend interface. In 30a only the connection-level
/// surface (`ping`, `execute`, `dialect`, `connection_info`, `dsn`) is
/// defined; subsequent sub-commits add schema management, strategy
/// executors, and Arrow I/O.
///
/// Backends are typically held behind `Arc<dyn Backend>` so the
/// pipeline executor can dispatch over a heterogeneous set of source +
/// target backends at runtime.
#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait Backend: Send + Sync + 'static {
    fn dialect(&self) -> Dialect;

    /// Σ.B PR 1: serializable form of this backend's constructor
    /// args + builder kwargs. Round-trips through JSON. Must NOT
    /// contain live connections, file descriptors, or runtime-
    /// dependent state — those reconstruct lazily on the executor
    /// side.
    ///
    /// Default impl panics; each backend overrides as it migrates
    /// (Σ.B PR 1 commits b/c/d). Once all in-tree backends override,
    /// the default goes away.
    ///
    /// See `docs/PHASE_SIGMA_B_TRAIT_SPIKE.md` for the rationale.
    fn config(&self) -> BackendConfig {
        unimplemented!(
            "Backend::config() not yet implemented for {:?} — Σ.B PR 1 \
             commits b/c/d migrate each backend kind",
            self.dialect()
        )
    }

    /// Σ.D-ready: can this backend partition reads of the given
    /// `query` by some key? `None` means single-stream only.
    /// Default returns `None`; Σ.D defines the override schema and
    /// individual backends opt in.
    fn partitioning_hint(&self, _query: &str) -> Option<KeyPartitioning> {
        None
    }

    /// `(host, port, dbname, user)` for DB-shaped backends; backend-
    /// specific identifying info for others (bucket name, broker list,
    /// etc.). Used for same-DB short-circuit detection and for human
    /// labels in `preview()` / logs.
    fn connection_info(&self) -> ConnectionInfo;

    /// Original connection string (DSN, S3 URI, etc.) when the backend
    /// has one. None for backends constructed from structured config
    /// without a stringified form. Carries credentials — keep within the
    /// trust boundary of the user code that constructed the backend.
    fn dsn(&self) -> Option<String>;

    /// Liveness check. For DB backends this issues `SELECT 1`; for
    /// streaming/object-store backends it verifies the underlying client
    /// is connectable.
    async fn ping(&self) -> Result<(), BackendError>;

    /// Phase 30d: optional escape hatch for backends that need to peek
    /// at a same-dialect peer's underlying Postgres pool (used by the
    /// existing PG ↔ PG cross-DB COPY BINARY path). Returning `None` is
    /// fine for backends that don't need this; callers must always
    /// handle that case.
    fn as_postgres(&self) -> Option<&PgPool> {
        None
    }

    /// Execute a side-effecting statement. SQL for DB backends; backend-
    /// specific commands for others (e.g., `DELETE` against an object
    /// prefix). Returns the affected row count where meaningful, 0
    /// otherwise.
    async fn execute(&self, statement: &str) -> Result<u64, BackendError>;

    /// Phase 30b: read source data as a stream of Arrow `RecordBatch`es.
    /// `query` is dialect-specific (SQL for DBs; topic/path/etc. for
    /// streaming or object-store backends — Phases 34/36 will refine
    /// the parameter shape). Implementations are free to chunk batches
    /// however they like; consumers should treat the stream as opaque.
    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError>;

    /// Phase 30b: write a stream of Arrow `RecordBatch`es to `target`
    /// using the requested write semantics. Returns the number of rows
    /// written. Merge / SCD2 still flow through dialect-specific
    /// strategy executors (Phase 30d).
    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError>;

    /// Phase 30d: run an append-only load. Implementations route to
    /// their dialect-native fast path (Postgres: COPY BINARY for cross-
    /// DB; INSERT…SELECT for same-DB). `source_backend` is `None` for
    /// the same-DB path; `Some(b)` triggers the cross-DB / Arrow path.
    async fn run_append(
        &self,
        spec: &crate::types::TableSpec,
        source_query: &str,
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        incremental_column: Option<&str>,
        last_value_literal: Option<&str>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError>;

    /// Phase 30d: TRUNCATE-then-load.
    async fn run_truncate(
        &self,
        spec: &crate::types::TableSpec,
        source_query: &str,
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError>;

    /// Phase 30d: merge / scd1 upsert.
    #[allow(clippy::too_many_arguments)]
    async fn run_merge(
        &self,
        spec: &crate::types::TableSpec,
        source_query: &str,
        keys: &[String],
        update_columns: &[String],
        pipeline_name: &str,
        mode_label: &str,
        source_backend: Option<&dyn Backend>,
        delete_handling: Option<DeleteHandling>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError>;

    /// Phase 30d: SCD2 versioned load.
    #[allow(clippy::too_many_arguments)]
    async fn run_scd2(
        &self,
        spec: &crate::types::TableSpec,
        source_query: &str,
        keys: &[String],
        compare_columns: &[String],
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        delete_handling: Option<DeleteHandling>,
        event_timestamp_column: Option<&str>,
        ttl_seconds: Option<i64>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError>;

    /// Phase 36e/g: commit any source-side checkpoint state
    /// accumulated by prior `read_arrow_stream` calls. Default
    /// implementation is a no-op — most backends have no
    /// "committed-offset" notion. Kafka overrides to commit consumer
    /// offsets after the target backend has durably written, which
    /// is the at-least-once primitive used by `StreamingPipeline`.
    async fn commit_offsets(&self) -> Result<(), BackendError> {
        Ok(())
    }

    /// Phase 39.5a: does this backend round-trip an offset blob
    /// through [`seek_to`] / [`offset_snapshot`]?
    ///
    /// Pipelines configured with sessions or stream-stream joins
    /// fail at config-load if any source backend reports `false`,
    /// because a `StateStore`-backed pipeline must be able to
    /// resume from its committed offset on restart.
    ///
    /// [`seek_to`]: Backend::seek_to
    /// [`offset_snapshot`]: Backend::offset_snapshot
    fn supports_seek_to(&self) -> bool {
        false
    }

    /// Phase 39.5a: rewind the read position to a previously
    /// committed offset. Called once at pipeline startup, before
    /// the first `read_arrow_stream`, with the bytes loaded from
    /// `StateStore`.
    ///
    /// `offset_bytes` is opaque to the rest of the framework — the
    /// backend that wrote them via [`offset_snapshot`] is the only
    /// thing that knows the format. For Kafka it's a JSON
    /// partition→offset map; other backends pick their own.
    ///
    /// Default impl returns an error so config-load can probe and
    /// reject session/join pipelines on backends that haven't
    /// opted in.
    ///
    /// [`offset_snapshot`]: Backend::offset_snapshot
    async fn seek_to(&self, _offset_bytes: &[u8]) -> Result<(), BackendError> {
        Err(BackendError::Other(format!(
            "backend dialect {:?} does not support seek_to (state persistence)",
            self.dialect()
        )))
    }

    /// Phase 39.5a: capture the current read position as opaque
    /// bytes suitable for handing to `StateStore.commit`. Returns
    /// `None` if no offset has advanced since the last commit (or
    /// since startup) — callers skip the source from the snapshot.
    ///
    /// Default impl returns `None`, matching the default `seek_to`
    /// "not supported": a non-streaming backend has nothing to
    /// snapshot.
    async fn offset_snapshot(&self) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(None)
    }
}

/// Re-export for trait method signatures.
pub use crate::meta::DeleteHandling;

// ============================================================
// Σ.B PR 1: BackendConfig scaffold + reverse-direction stub.
// ============================================================
//
// Per `docs/PHASE_SIGMA_B_TRAIT_SPIKE.md`'s locked decisions:
//   - Wire format: JSON
//   - Closed registry (match-on-tag in `backend_from_config`)
//   - Per-backend `<Backend>Config` payloads land in commits b/c/d
//
// PR 1 ships the surface; payloads are empty unit variants so the
// enum compiles, serializes, and discriminates uniquely. Each
// migration commit replaces a unit variant with its config struct.

/// Serializable backend configuration. Tagged enum; `serde` emits
/// `{"kind": "postgres", ...payload-fields}` so the discriminator
/// is human-readable (matches today's TOML config shape; JSON is a
/// strict superset).
///
/// Migration status per variant (Σ.B PR 1):
///   - DB backends (Postgres / MySql / Sqlite / DuckDb): commit b
///     populated; payload structs carry DSN / location
///   - Object store + Delta: commit c populated; payload covers
///     local + S3 with sub-tagged location enums
///   - Streaming (Kafka / Kinesis / Pub/Sub / RabbitMq): commit d
///     (still unit placeholders)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BackendConfig {
    Postgres(PostgresConfig),
    MySql(MySqlConfig),
    Sqlite(SqliteConfig),
    DuckDb(DuckDbConfig),
    Kafka(KafkaConfig),
    Kinesis(KinesisConfig),
    PubSub(PubSubConfig),
    RabbitMq(RabbitMqConfig),
    Delta(DeltaConfig),
    ObjectStore(ObjectStoreConfig),
}

/// Σ.B PR 1 commit b: serializable Postgres config. The DSN carries
/// credentials + host + port + dbname + sslmode in a single string,
/// matching the existing `PgPool::connect` surface. Inline format
/// per the spike's locked decision (env-var indirection lands later
/// as a non-breaking addition).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PostgresConfig {
    pub dsn: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MySqlConfig {
    /// `mysql://user:pass@host:port/db` URL.
    pub dsn: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SqliteConfig {
    /// `:memory:` for an ephemeral DB; an absolute path otherwise.
    pub location: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DuckDbConfig {
    /// `:memory:` for an ephemeral DB; an absolute path otherwise.
    pub location: String,
}

/// Σ.B PR 1 commit c: object-store config covering both local-FS
/// and S3-compatible roots. The `location` discriminator lives
/// inside this struct rather than on the parent `BackendConfig`
/// because both `Local` and `S3` are still kind=`object_store` from
/// the outer perspective — the local-vs-S3 distinction is
/// implementation-internal to the object-store backend.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ObjectStoreConfig {
    pub location: ObjectStoreLocation,
    pub format: ObjectFormat,
    /// Per-format write options (Parquet compression, CSV delimiter,
    /// CSV header). Default = pre-Π.1.4 behavior (Parquet
    /// uncompressed, CSV comma + header).
    #[serde(default)]
    pub write_options: ObjectWriteOptions,
}

/// Local-FS root vs. S3-compatible root for [`ObjectStoreConfig`].
/// Inline credentials per the spike's locked decision.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ObjectStoreLocation {
    Local {
        /// Filesystem root path; created if missing on `open_local`.
        root_dir: String,
    },
    S3 {
        /// Full URL including scheme (`http://localhost:9000` for
        /// MinIO, `https://s3.amazonaws.com` for AWS).
        endpoint: String,
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
    },
}

/// Σ.B PR 1 commit c: Delta-Lake config. Same Local/S3 discriminator
/// as [`ObjectStoreConfig`] but lives in its own struct because
/// Delta carries `partition_columns` (a Delta-only knob) and lacks
/// `format` (Delta tables are inherently Parquet under the hood).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeltaConfig {
    pub location: DeltaLocation,
    /// Phase 40.1: optional per-target partition columns. Only
    /// applied on first-write table creation; preexisting tables
    /// retain their declared layout.
    #[serde(default)]
    pub partition_columns: Vec<String>,
}

/// Local-FS root vs. S3-compatible root for [`DeltaConfig`]. The
/// S3 variant adds an optional `prefix` (key prefix inside the
/// bucket) that the [`ObjectStoreConfig::S3`] variant doesn't —
/// Delta encodes the prefix in the URL it threads through
/// `DeltaTableBuilder`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeltaLocation {
    Local {
        root_dir: String,
    },
    S3 {
        endpoint: String,
        bucket: String,
        /// Optional key prefix inside the bucket. `""` for bucket-
        /// root tables.
        #[serde(default)]
        prefix: String,
        region: String,
        access_key: String,
        secret_key: String,
    },
}

// Σ.B PR 1 commit d: streaming backend configs.
//
// Scope is constructor-args-only: each `<Backend>Config` carries
// the parameters passed to `open()`. Builder-state (auth, payload
// format, SR config, batch tuning, partition keys) is *not* yet
// captured in the config — that lands in a Σ.B PR 2 follow-up
// once we know which knobs Ballista users actually need to ship.
//
// To prevent silent data loss, `Backend::config()` for these
// backends panics if any non-default builder state has been set on
// the live instance, with a pointer at the follow-up.

/// Σ.B PR 1 commit d: Kafka constructor args.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KafkaConfig {
    /// `host1:9092,host2:9092` form expected by librdkafka.
    pub bootstrap_servers: String,
    /// Consumer group_id. `None` = producer-only backend (Kafka as
    /// a target).
    #[serde(default)]
    pub group_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KinesisConfig {
    pub stream_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PubSubConfig {
    pub project_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RabbitMqConfig {
    /// Full AMQP URL — `amqp://user:pass@host:port/vhost` or its
    /// `amqps://` TLS counterpart.
    pub amqp_url: String,
}

/// Σ.D-ready partitioning-hint placeholder. Return type for
/// [`Backend::partitioning_hint`]; concrete shape (range / hash /
/// per-source-partition) gets locked in Σ.D once the dominant
/// pattern is known. PR 1 ships an empty struct so the trait
/// method signature is stable.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct KeyPartitioning {
    /// Reserved for Σ.D — explicit unit field instead of a unit
    /// struct so the type can grow new fields without breaking
    /// callers that bind via field-init syntax.
    #[doc(hidden)]
    pub _reserved: (),
}

/// Reconstruct a backend from its serialized config. Closed
/// match-on-tag dispatch; out-of-tree backends fork (per the
/// spike's locked decision).
///
/// Migration status:
///   - Postgres / MySql / Sqlite / DuckDb: wired in commit b
///   - Object store / Delta: commit c — returns NotImplementedYet
///   - Streaming (Kafka / Kinesis / Pub/Sub / RabbitMq): commit d
///     — returns NotImplementedYet
pub async fn backend_from_config(
    cfg: BackendConfig,
) -> Result<std::sync::Arc<dyn Backend>, BackendError> {
    match cfg {
        BackendConfig::Postgres(c) => {
            let pool = std::sync::Arc::new(PgPool::connect(&c.dsn).await?);
            Ok(std::sync::Arc::new(PostgresBackend::new(pool, c.dsn)))
        }
        BackendConfig::MySql(c) => Ok(std::sync::Arc::new(
            crate::mysql_backend::MySQLBackend::open(c.dsn)?,
        )),
        BackendConfig::Sqlite(c) => Ok(std::sync::Arc::new(
            crate::sqlite_backend::SQLiteBackend::open(c.location)?,
        )),
        BackendConfig::DuckDb(c) => Ok(std::sync::Arc::new(
            crate::duckdb_backend::DuckDBBackend::open(c.location)?,
        )),
        BackendConfig::ObjectStore(c) => {
            let backend = match c.location {
                ObjectStoreLocation::Local { root_dir } => {
                    crate::objectstore_backend::ObjectStoreBackend::open_local(
                        std::path::PathBuf::from(root_dir),
                        c.format,
                    )?
                }
                ObjectStoreLocation::S3 {
                    endpoint,
                    bucket,
                    region,
                    access_key,
                    secret_key,
                } => crate::objectstore_backend::ObjectStoreBackend::open_s3(
                    &endpoint,
                    &bucket,
                    &region,
                    &access_key,
                    &secret_key,
                    c.format,
                )?,
            }
            .with_write_options(c.write_options);
            Ok(std::sync::Arc::new(backend))
        }
        BackendConfig::Delta(c) => {
            let mut backend = match c.location {
                DeltaLocation::Local { root_dir } => {
                    crate::delta_backend::DeltaBackend::open_local(std::path::PathBuf::from(
                        root_dir,
                    ))?
                }
                DeltaLocation::S3 {
                    endpoint,
                    bucket,
                    prefix,
                    region,
                    access_key,
                    secret_key,
                } => crate::delta_backend::DeltaBackend::open_s3(
                    &endpoint,
                    &bucket,
                    &prefix,
                    &region,
                    &access_key,
                    &secret_key,
                )?,
            };
            if !c.partition_columns.is_empty() {
                backend = backend.with_partition_columns(c.partition_columns);
            }
            Ok(std::sync::Arc::new(backend))
        }
        BackendConfig::Kafka(c) => Ok(std::sync::Arc::new(
            crate::kafka_backend::KafkaBackend::open(c.bootstrap_servers, c.group_id.as_deref())?,
        )),
        BackendConfig::Kinesis(c) => Ok(std::sync::Arc::new(
            crate::kinesis_backend::KinesisBackend::open(c.stream_name)?,
        )),
        BackendConfig::PubSub(c) => Ok(std::sync::Arc::new(
            crate::pubsub_backend::PubSubBackend::open(c.project_id)?,
        )),
        BackendConfig::RabbitMq(c) => Ok(std::sync::Arc::new(
            crate::rabbitmq_backend::RabbitMQBackend::open(c.amqp_url)?,
        )),
    }
}

/// Postgres backend — wraps an existing `PgPool`. The first impl of the
/// trait; in 30a it's a thin delegation. Subsequent sub-commits move
/// more functionality onto the trait surface.
pub struct PostgresBackend {
    pool: Arc<PgPool>,
    dsn: String,
}

impl PostgresBackend {
    pub fn new(pool: Arc<PgPool>, dsn: String) -> Self {
        Self { pool, dsn }
    }

    pub fn pool(&self) -> &Arc<PgPool> {
        &self.pool
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    fn connection_info(&self) -> ConnectionInfo {
        self.pool.info().clone()
    }

    fn dsn(&self) -> Option<String> {
        Some(self.dsn.clone())
    }

    fn config(&self) -> BackendConfig {
        BackendConfig::Postgres(PostgresConfig {
            dsn: self.dsn.clone(),
        })
    }

    fn as_postgres(&self) -> Option<&PgPool> {
        Some(&self.pool)
    }

    async fn ping(&self) -> Result<(), BackendError> {
        self.pool.ping().await?;
        Ok(())
    }

    async fn execute(&self, statement: &str) -> Result<u64, BackendError> {
        Ok(self.pool.execute(statement).await?)
    }

    async fn run_append(
        &self,
        spec: &crate::types::TableSpec,
        source_query: &str,
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        incremental_column: Option<&str>,
        last_value_literal: Option<&str>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        let watermark = incremental_column.map(|c| WatermarkConfig {
            column: c.to_string(),
            last_value_literal: last_value_literal.map(|s| s.to_string()),
        });
        match source_backend {
            None => {
                let outcome = self
                    .pool
                    .run_append_same_db(
                        spec,
                        source_query,
                        pipeline_name,
                        watermark.as_ref(),
                        dry_run,
                    )
                    .await?;
                Ok(outcome.into())
            }
            Some(src) => {
                let src_pool = src.as_postgres().ok_or_else(|| {
                    BackendError::Other(
                        "Phase 30d: Postgres run_append cross-backend currently \
                         requires both endpoints to be Postgres; for cross-dialect \
                         use cross_backend_arrow_sync (Phase 30c)"
                            .into(),
                    )
                })?;
                let outcome = self
                    .pool
                    .run_append_cross_db(
                        src_pool,
                        spec,
                        source_query,
                        pipeline_name,
                        watermark.as_ref(),
                    )
                    .await?;
                Ok(outcome.into())
            }
        }
    }

    async fn run_truncate(
        &self,
        spec: &crate::types::TableSpec,
        source_query: &str,
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        match source_backend {
            None => {
                let outcome = self
                    .pool
                    .run_truncate_same_db(spec, source_query, pipeline_name, dry_run)
                    .await?;
                Ok(outcome.into())
            }
            Some(src) => {
                let src_pool = src.as_postgres().ok_or_else(|| {
                    BackendError::Other(
                        "Phase 30d: Postgres run_truncate cross-backend requires \
                         both endpoints to be Postgres"
                            .into(),
                    )
                })?;
                let outcome = self
                    .pool
                    .run_truncate_cross_db(src_pool, spec, source_query, pipeline_name)
                    .await?;
                Ok(outcome.into())
            }
        }
    }

    async fn run_merge(
        &self,
        spec: &crate::types::TableSpec,
        source_query: &str,
        keys: &[String],
        update_columns: &[String],
        pipeline_name: &str,
        mode_label: &str,
        source_backend: Option<&dyn Backend>,
        delete_handling: Option<DeleteHandling>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        match source_backend {
            None => {
                let outcome = self
                    .pool
                    .run_merge_same_db(
                        spec,
                        source_query,
                        keys,
                        update_columns,
                        pipeline_name,
                        mode_label,
                        delete_handling,
                        dry_run,
                    )
                    .await?;
                Ok(outcome.into())
            }
            Some(src) => {
                let src_pool = src.as_postgres().ok_or_else(|| {
                    BackendError::Other(
                        "Phase 30d: Postgres run_merge cross-backend requires \
                         both endpoints to be Postgres"
                            .into(),
                    )
                })?;
                let outcome = self
                    .pool
                    .run_merge_cross_db(
                        src_pool,
                        spec,
                        source_query,
                        keys,
                        update_columns,
                        pipeline_name,
                        mode_label,
                        delete_handling,
                    )
                    .await?;
                Ok(outcome.into())
            }
        }
    }

    async fn run_scd2(
        &self,
        spec: &crate::types::TableSpec,
        source_query: &str,
        keys: &[String],
        compare_columns: &[String],
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        delete_handling: Option<DeleteHandling>,
        event_timestamp_column: Option<&str>,
        ttl_seconds: Option<i64>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        match source_backend {
            None => {
                let outcome = self
                    .pool
                    .run_scd2_same_db(
                        spec,
                        source_query,
                        keys,
                        compare_columns,
                        pipeline_name,
                        delete_handling,
                        event_timestamp_column,
                        ttl_seconds,
                        dry_run,
                    )
                    .await?;
                Ok(outcome.into())
            }
            Some(src) => {
                let src_pool = src.as_postgres().ok_or_else(|| {
                    BackendError::Other(
                        "Phase 30d: Postgres run_scd2 cross-backend requires \
                         both endpoints to be Postgres"
                            .into(),
                    )
                })?;
                let outcome = self
                    .pool
                    .run_scd2_cross_db(
                        src_pool,
                        spec,
                        source_query,
                        keys,
                        compare_columns,
                        pipeline_name,
                        delete_handling,
                        event_timestamp_column,
                        ttl_seconds,
                    )
                    .await?;
                Ok(outcome.into())
            }
        }
    }

    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        // Phase 30b minimum-viable: run the query, materialize all rows,
        // emit one RecordBatch. Streaming chunked output is a future
        // optimization (use COPY BINARY → arrow encoders).
        let client = self
            .pool
            .raw_pool()
            .get()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        let stmt = client
            .prepare(query)
            .await
            .map_err(PgError::Postgres)
            .map_err(BackendError::from)?;
        let rows = client
            .query(&stmt, &[])
            .await
            .map_err(PgError::Postgres)
            .map_err(BackendError::from)?;
        let batch = pg_rows_to_record_batch(stmt.columns(), &rows)?;
        let stream = futures_util::stream::once(async move { Ok::<_, BackendError>(batch) });
        Ok(Box::pin(stream))
    }

    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError> {
        use futures_util::StreamExt;

        let mut client = self
            .pool
            .raw_pool()
            .get()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        let tx = client
            .transaction()
            .await
            .map_err(PgError::Postgres)
            .map_err(BackendError::from)?;
        if mode == WriteMode::Truncate {
            tx.batch_execute(&format!(
                "TRUNCATE TABLE {}.{}",
                quote_ident(&target.schema),
                quote_ident(&target.name)
            ))
            .await
            .map_err(PgError::Postgres)
            .map_err(BackendError::from)?;
        }
        let mut total: u64 = 0;
        let mut s = stream;
        while let Some(batch) = s.next().await {
            let batch = batch?;
            total += insert_record_batch(&tx, &target.schema, &target.name, &batch).await?;
        }
        tx.commit()
            .await
            .map_err(PgError::Postgres)
            .map_err(BackendError::from)?;
        Ok(total)
    }
}

// --- Postgres ↔ Arrow conversion (Phase 30b minimum-viable type set) -------

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn pg_type_to_arrow(ty: &PgType) -> Result<DataType, BackendError> {
    Ok(match ty.oid() {
        21 => DataType::Int16,                                           // INT2
        23 => DataType::Int32,                                           // INT4
        20 => DataType::Int64,                                           // INT8
        700 => DataType::Float32,                                        // FLOAT4 (REAL)
        701 => DataType::Float64,                                        // FLOAT8 (DOUBLE)
        16 => DataType::Boolean,                                         // BOOL
        25 | 1043 | 1042 => DataType::Utf8,                              // TEXT/VARCHAR/BPCHAR
        17 => DataType::Binary,                                          // BYTEA
        2950 => DataType::Utf8,                                          // UUID (carry as text)
        114 | 3802 => DataType::Utf8,                                    // JSON / JSONB (text rep)
        1184 | 1114 => DataType::Timestamp(TimeUnit::Microsecond, None), // TIMESTAMPTZ/TIMESTAMP
        _ => {
            return Err(BackendError::TypeMapping(format!(
                "Postgres → Arrow: unsupported type {} (oid={}); \
                 add a builder in pg_type_to_arrow / pg_rows_to_record_batch",
                ty.name(),
                ty.oid()
            )));
        }
    })
}

fn pg_rows_to_record_batch(
    columns: &[tokio_postgres::Column],
    rows: &[tokio_postgres::Row],
) -> Result<RecordBatch, BackendError> {
    use std::sync::Arc as StdArc;

    let mut fields = Vec::with_capacity(columns.len());
    for col in columns {
        let dt = pg_type_to_arrow(col.type_())?;
        // All columns are nullable for now — column nullability isn't
        // exposed by tokio-postgres directly; the trait can be tightened
        // when we wire the planner through (Phase 30c).
        fields.push(Field::new(col.name(), dt, true));
    }
    let schema = StdArc::new(Schema::new(fields.clone()));

    let mut arrays: Vec<StdArc<dyn Array>> = Vec::with_capacity(columns.len());
    for (idx, col) in columns.iter().enumerate() {
        let pg_oid = col.type_().oid();
        let dt = pg_type_to_arrow(col.type_())?;
        let cap = rows.len();
        let array: StdArc<dyn Array> = match dt {
            DataType::Int16 => {
                let mut b = Int16Builder::with_capacity(cap);
                for row in rows {
                    let v: Option<i16> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!("row[{idx}] {} → i16: {e}", col.name()))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Int32 => {
                let mut b = Int32Builder::with_capacity(cap);
                for row in rows {
                    let v: Option<i32> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!("row[{idx}] {} → i32: {e}", col.name()))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(cap);
                for row in rows {
                    let v: Option<i64> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!("row[{idx}] {} → i64: {e}", col.name()))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Float32 => {
                let mut b = Float32Builder::with_capacity(cap);
                for row in rows {
                    let v: Option<f32> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!("row[{idx}] {} → f32: {e}", col.name()))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(cap);
                for row in rows {
                    let v: Option<f64> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!("row[{idx}] {} → f64: {e}", col.name()))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(cap);
                for row in rows {
                    let v: Option<bool> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!("row[{idx}] {} → bool: {e}", col.name()))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(cap, cap * 16);
                // For UUID and JSON/JSONB the value type isn't &str —
                // tokio-postgres returns a uuid::Uuid / serde_json::Value.
                // We extract via the typed Rust accessor and stringify.
                for row in rows {
                    match pg_oid {
                        2950 => {
                            let v: Option<uuid::Uuid> = row.try_get(idx).map_err(|e| {
                                BackendError::TypeMapping(format!(
                                    "row[{idx}] {} → uuid: {e}",
                                    col.name()
                                ))
                            })?;
                            b.append_option(v.map(|u| u.to_string()));
                        }
                        114 | 3802 => {
                            // JSON / JSONB → arrow Utf8 via the
                            // canonical JSON text form. Requires the
                            // `with-serde_json-1` feature on
                            // tokio-postgres.
                            let v: Option<serde_json::Value> = row.try_get(idx).map_err(|e| {
                                BackendError::TypeMapping(format!(
                                    "row[{idx}] {} → json: {e}",
                                    col.name()
                                ))
                            })?;
                            b.append_option(v.map(|j| j.to_string()));
                        }
                        _ => {
                            let v: Option<&str> = row.try_get(idx).map_err(|e| {
                                BackendError::TypeMapping(format!(
                                    "row[{idx}] {} → text: {e}",
                                    col.name()
                                ))
                            })?;
                            b.append_option(v);
                        }
                    }
                }
                StdArc::new(b.finish())
            }
            DataType::Binary => {
                let mut b = BinaryBuilder::with_capacity(cap, cap * 16);
                for row in rows {
                    let v: Option<&[u8]> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!("row[{idx}] {} → bytea: {e}", col.name()))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                use std::time::SystemTime;
                let mut b = TimestampMicrosecondBuilder::with_capacity(cap);
                for row in rows {
                    let v: Option<SystemTime> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!(
                            "row[{idx}] {} → timestamp: {e}",
                            col.name()
                        ))
                    })?;
                    let micros = v.map(system_time_to_micros);
                    b.append_option(micros);
                }
                StdArc::new(b.finish())
            }
            other => {
                return Err(BackendError::TypeMapping(format!(
                    "no Arrow builder for {other:?}"
                )));
            }
        };
        arrays.push(array);
    }
    RecordBatch::try_new(schema, arrays).map_err(|e| BackendError::TypeMapping(e.to_string()))
}

fn system_time_to_micros(t: std::time::SystemTime) -> i64 {
    use std::time::UNIX_EPOCH;
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_micros() as i64,
        Err(e) => -(e.duration().as_micros() as i64),
    }
}

async fn insert_record_batch(
    tx: &deadpool_postgres::Transaction<'_>,
    schema: &str,
    table: &str,
    batch: &RecordBatch,
) -> Result<u64, BackendError> {
    use arrow_array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        StringArray, TimestampMicrosecondArray,
    };

    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let arrow_schema = batch.schema();
    let cols: Vec<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("${i}")).collect();
    let sql = format!(
        "INSERT INTO {}.{} ({}) VALUES ({})",
        quote_ident(schema),
        quote_ident(table),
        cols.iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders.join(", ")
    );
    let stmt = tx
        .prepare(&sql)
        .await
        .map_err(PgError::Postgres)
        .map_err(BackendError::from)?;

    // Resolve each prepared-statement parameter's PG type. tokio_postgres
    // exposes them via `Statement::params()`. We use them to decide how
    // to bind Utf8 columns: as &str (TEXT/JSON/JSONB) or as uuid::Uuid
    // (UUID), since the binary protocol won't auto-cast text → uuid.
    let param_types: Vec<PgType> = stmt.params().to_vec();

    let mut rows_written: u64 = 0;
    for row_idx in 0..batch.num_rows() {
        let n = batch.num_columns();
        let mut owned_strs: Vec<Option<String>> = vec![None; n];
        let mut owned_i64: Vec<Option<i64>> = vec![None; n];
        let mut owned_i32: Vec<Option<i32>> = vec![None; n];
        let mut owned_i16: Vec<Option<i16>> = vec![None; n];
        let mut owned_f32: Vec<Option<f32>> = vec![None; n];
        let mut owned_f64: Vec<Option<f64>> = vec![None; n];
        let mut owned_bool: Vec<Option<bool>> = vec![None; n];
        let mut owned_ts: Vec<Option<std::time::SystemTime>> = vec![None; n];
        let mut owned_bytes: Vec<Option<Vec<u8>>> = vec![None; n];
        let mut owned_uuid: Vec<Option<uuid::Uuid>> = vec![None; n];
        let mut owned_json: Vec<Option<serde_json::Value>> = vec![None; n];

        for (col_idx, field) in arrow_schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            if col.is_null(row_idx) {
                continue;
            }
            match field.data_type() {
                DataType::Int16 => {
                    let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                    owned_i16[col_idx] = Some(arr.value(row_idx));
                }
                DataType::Int32 => {
                    let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                    owned_i32[col_idx] = Some(arr.value(row_idx));
                }
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    owned_i64[col_idx] = Some(arr.value(row_idx));
                }
                DataType::Float32 => {
                    let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                    owned_f32[col_idx] = Some(arr.value(row_idx));
                }
                DataType::Float64 => {
                    let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                    owned_f64[col_idx] = Some(arr.value(row_idx));
                }
                DataType::Boolean => {
                    let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                    owned_bool[col_idx] = Some(arr.value(row_idx));
                }
                DataType::Utf8 => {
                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                    let s = arr.value(row_idx);
                    // Bind by destination PG type — UUID needs a
                    // uuid::Uuid; JSON/JSONB need a serde_json::Value;
                    // everything else binds as &str.
                    let dest_oid = param_types.get(col_idx).map(|t| t.oid());
                    match dest_oid {
                        Some(2950) => {
                            let parsed = uuid::Uuid::parse_str(s).map_err(|e| {
                                BackendError::TypeMapping(format!(
                                    "row[{row_idx}] {} → uuid parse: {e}",
                                    field.name()
                                ))
                            })?;
                            owned_uuid[col_idx] = Some(parsed);
                        }
                        Some(114) | Some(3802) => {
                            let parsed: serde_json::Value =
                                serde_json::from_str(s).map_err(|e| {
                                    BackendError::TypeMapping(format!(
                                        "row[{row_idx}] {} → json parse: {e}",
                                        field.name()
                                    ))
                                })?;
                            owned_json[col_idx] = Some(parsed);
                        }
                        _ => {
                            owned_strs[col_idx] = Some(s.to_string());
                        }
                    }
                }
                DataType::Binary => {
                    let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                    owned_bytes[col_idx] = Some(arr.value(row_idx).to_vec());
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let micros = arr.value(row_idx);
                    let dur = std::time::Duration::from_micros(micros as u64);
                    owned_ts[col_idx] = Some(std::time::UNIX_EPOCH + dur);
                }
                other => {
                    return Err(BackendError::TypeMapping(format!(
                        "Arrow → Postgres: unsupported {other:?} for column {}",
                        field.name()
                    )));
                }
            }
        }

        type ToSqlRef<'a> = &'a (dyn tokio_postgres::types::ToSql + Sync);
        let mut params: Vec<ToSqlRef<'_>> = Vec::with_capacity(batch.num_columns());
        for (col_idx, field) in arrow_schema.fields().iter().enumerate() {
            match field.data_type() {
                DataType::Int16 => params.push(&owned_i16[col_idx] as ToSqlRef<'_>),
                DataType::Int32 => params.push(&owned_i32[col_idx] as ToSqlRef<'_>),
                DataType::Int64 => params.push(&owned_i64[col_idx] as ToSqlRef<'_>),
                DataType::Float32 => params.push(&owned_f32[col_idx] as ToSqlRef<'_>),
                DataType::Float64 => params.push(&owned_f64[col_idx] as ToSqlRef<'_>),
                DataType::Boolean => params.push(&owned_bool[col_idx] as ToSqlRef<'_>),
                DataType::Utf8 => match param_types.get(col_idx).map(|t| t.oid()) {
                    Some(2950) => params.push(&owned_uuid[col_idx] as ToSqlRef<'_>),
                    Some(114) | Some(3802) => params.push(&owned_json[col_idx] as ToSqlRef<'_>),
                    _ => params.push(&owned_strs[col_idx] as ToSqlRef<'_>),
                },
                DataType::Binary => params.push(&owned_bytes[col_idx] as ToSqlRef<'_>),
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    params.push(&owned_ts[col_idx] as ToSqlRef<'_>)
                }
                _ => unreachable!(),
            }
        }
        rows_written += tx
            .execute(&stmt, &params)
            .await
            .map_err(PgError::Postgres)
            .map_err(BackendError::from)?;
    }
    Ok(rows_written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect_matches_is_strict_equality() {
        assert!(Dialect::Postgres.matches(&Dialect::Postgres));
        assert!(!Dialect::Postgres.matches(&Dialect::MySQL));
        assert!(
            Dialect::ObjectStore {
                format: ObjectFormat::Parquet
            }
            .matches(&Dialect::ObjectStore {
                format: ObjectFormat::Parquet
            })
        );
        assert!(
            !Dialect::ObjectStore {
                format: ObjectFormat::Parquet
            }
            .matches(&Dialect::ObjectStore {
                format: ObjectFormat::Csv
            })
        );
        assert!(
            Dialect::Streaming {
                kind: StreamingKind::Kafka
            }
            .matches(&Dialect::Streaming {
                kind: StreamingKind::Kafka
            })
        );
    }

    #[test]
    fn pg_error_to_backend_error_preserves_kind() {
        let url_err: BackendError = PgError::Url("missing dbname".into()).into();
        assert!(matches!(url_err, BackendError::Connection(_)));
        let pool_err: BackendError = PgError::Pool("timeout".into()).into();
        assert!(matches!(pool_err, BackendError::Connection(_)));
    }

    #[test]
    fn dialect_variants_all_distinct() {
        // Sanity: the variants we'll dispatch over are all distinct.
        let variants = [
            Dialect::Postgres,
            Dialect::MySQL,
            Dialect::SQLite,
            Dialect::DuckDB,
            Dialect::Iceberg,
            Dialect::Delta,
            Dialect::ObjectStore {
                format: ObjectFormat::Parquet,
            },
            Dialect::Streaming {
                kind: StreamingKind::Kafka,
            },
        ];
        for (i, a) in variants.iter().enumerate() {
            for (j, b) in variants.iter().enumerate() {
                assert_eq!(a == b, i == j, "{a:?} vs {b:?}");
            }
        }
    }
}
