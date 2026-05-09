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
            PgError::Other(s) => BackendError::Other(s),
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

/// Phase Δ PR 3 + PR 4: result of one CDC batch's apply.
///
/// `creates` covers `c` and `r` (snapshot Read) ops — both UPSERT-
/// shaped so they share a counter. `skipped` counts tombstones +
/// rows that failed envelope-parse (the latter still log a
/// warning so they aren't silent). `idempotent_skipped` (PR 4)
/// counts events rejected by the per-PK last-seen-ts gate — i.e.
/// Kafka redeliveries that the executor has already applied. The
/// metrics path mirrors [`StrategyRunResult`] so the streaming
/// pipeline can fold both into the same observability surface.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CdcRunResult {
    pub run_id: String,
    /// Successful INSERT/UPSERT applications (`Create` + `Read`).
    pub creates: i64,
    /// Successful UPDATE applications.
    pub updates: i64,
    /// Successful DELETE / soft-delete applications.
    pub deletes: i64,
    /// Tombstones + parse failures. Always counted; never errors
    /// the run.
    pub skipped: i64,
    /// Events whose `(pipeline, pk)` already had a last-seen ts
    /// at or beyond `event.ts_ms` in `ematix_flow.cdc_idempotency`.
    /// Indicates an at-least-once redelivery the gate suppressed.
    pub idempotent_skipped: i64,
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

    /// **Internal escape hatch — not a stable extension point.**
    ///
    /// Returns the underlying Postgres pool when (and only when) the
    /// implementing backend is `PostgresBackend`. The single caller
    /// is `PostgresBackend`'s own strategy executors, which use this
    /// to take the COPY BINARY fast path on PG → PG cross-DB
    /// transfers (`run_append_cross_db` / `run_truncate_cross_db` /
    /// `run_merge_cross_db` / `run_scd2_cross_db` in `pg.rs`).
    ///
    /// Out-of-tree implementors must not override this — it lives on
    /// the public trait only because PostgresBackend's executors
    /// receive their source as `&dyn Backend` and need a way to
    /// recognise a same-dialect peer. A future refactor (Σ.B
    /// follow-up `docs/PHASE_SIGMA_PLAN.md`) will move the dispatch
    /// behind a `BackendConfig`-discriminant check + private
    /// downcast so this method can leave the trait entirely; until
    /// then, treat this as `pub(crate)` even though Rust visibility
    /// can't enforce that on a trait method.
    ///
    /// Σ.B follow-up: doc-hidden so the method doesn't appear in
    /// the rustdoc-rendered public API. The `#[allow(...)]` on the
    /// PostgresBackend impl override silences the corresponding
    /// missing-docs lint without re-exposing the method.
    #[doc(hidden)]
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

    /// Phase Δ PR 3: apply one batch of CDC events to the target
    /// table.
    ///
    /// The streaming pipeline reads `RecordBatch`es from a Kafka
    /// source, hands each batch to this method, and the
    /// implementation:
    ///
    /// 1. Converts each row to JSON via [`crate::cdc::batch_to_json_rows`].
    /// 2. Resolves each JSON row to a [`crate::cdc::CdcEvent`] via
    ///    [`crate::cdc::parse_event`] (PR 2). Tombstones return
    ///    `Ok(None)` and bump `skipped`; parse errors are logged
    ///    + counted as `skipped` (the streaming pipeline's
    ///    `transform_on_error` policy decides whether to fail
    ///    the batch instead).
    /// 3. Groups by op + applies each per-op group transactionally
    ///    via the backend's native UPSERT / UPDATE / DELETE.
    /// 4. Returns a [`CdcRunResult`] with per-op counts.
    ///
    /// Default implementation: returns a `BackendError::Other`
    /// pointing at the relevant phase doc. Postgres implements
    /// this concretely; other targets will land in PR 6 +
    /// follow-ups per `docs/PHASE_DELTA_CDC_PLAN.md`.
    async fn run_cdc(
        &self,
        _spec: &crate::types::TableSpec,
        _batch: RecordBatch,
        _cdc_config: &crate::cdc::CdcConfig,
        _pipeline_name: &str,
    ) -> Result<CdcRunResult, BackendError> {
        Err(BackendError::Other(format!(
            "run_cdc is not yet implemented for backend dialect = {:?}. \
             Phase Δ PR 3 lands the Postgres impl; other targets in \
             PR 6 + follow-ups (docs/PHASE_DELTA_CDC_PLAN.md).",
            self.dialect()
        )))
    }

    /// Phase Δ PR 5.5: reflect a target table's column set + PK
    /// from the live database. The streaming-pipeline runtime calls
    /// this once per CDC target at startup so the per-batch
    /// `run_cdc` dispatch has a real `TableSpec` (with columns) to
    /// hand the executor. Schema-evolution detection (PR 5)
    /// compares incoming `after`-payload keys against the columns
    /// returned here.
    ///
    /// Default impl errors with a backend-dialect-specific
    /// message; only Postgres ships an implementation today.
    async fn reflect_table_spec(
        &self,
        _target: &TargetTable,
    ) -> Result<crate::types::TableSpec, BackendError> {
        Err(BackendError::Other(format!(
            "reflect_table_spec is not implemented for backend dialect = {:?}. \
             CDC apply mode requires a target backend that can introspect its \
             table schema; today only Postgres qualifies (Phase Δ PR 5.5).",
            self.dialect()
        )))
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
    /// Σ.B PR 2: distributed-execution backend
    /// (`crates/ematix-flow-distributed`). The variant carries the
    /// peer-URL list; backend reconstruction happens via that
    /// crate's `DistributedBackend::open(cfg)` rather than via
    /// `backend_from_config` — `ematix-flow-core` deliberately
    /// doesn't depend on `ematix-flow-distributed` (would be a
    /// circular dep), so this arm errors with a clear pointer at
    /// the right constructor.
    Distributed(DistributedConfig),
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

/// Σ.B PR 1 commit d + follow-up: Kafka backend config.
///
/// Constructor args (`bootstrap_servers`, `group_id`) ship in PR 1
/// commit d. The Σ.B follow-up extends this struct with the rest
/// of the builder-set state (auth, payload format, schema registry,
/// delivery semantics, message-key column, batch tuning) so a
/// fully-configured `KafkaBackend` round-trips through serde JSON
/// + `backend_from_config`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KafkaConfig {
    /// `host1:9092,host2:9092` form expected by librdkafka.
    pub bootstrap_servers: String,
    /// Consumer group_id. `None` = producer-only backend (Kafka as
    /// a target).
    #[serde(default)]
    pub group_id: Option<String>,

    /// Authentication mode. `None` = `KafkaAuthConfig::None`
    /// (PLAINTEXT). Inline credentials per the spike's locked
    /// decision; same trust boundary as Postgres DSN passwords.
    #[serde(default)]
    pub auth: Option<KafkaAuthConfig>,

    /// Wire format for Kafka message payloads. `None` =
    /// `KafkaPayloadFormat::Json` (the default).
    #[serde(default)]
    pub payload_format: Option<crate::kafka_backend::KafkaPayloadFormat>,

    /// Producer-side delivery semantics. `None` =
    /// `KafkaDeliverySemantics::AtLeastOnce` (the default).
    #[serde(default)]
    pub delivery_semantics: Option<crate::kafka_backend::KafkaDeliverySemantics>,

    /// Confluent Schema Registry URL. Required for Avro / Protobuf
    /// payload formats; ignored for JSON / RawBytes.
    #[serde(default)]
    pub schema_registry_url: Option<String>,

    /// SR basic-auth credentials. Confluent Cloud uses an API key
    /// (username) + API secret (password) pair here.
    #[serde(default)]
    pub schema_registry_basic_auth: Option<crate::kafka_backend::SrBasicAuth>,

    /// Per-row Kafka message-key column. `None` = round-robin
    /// (default sticky partitioner).
    #[serde(default)]
    pub message_key_column: Option<String>,

    /// Per-call drain limits for `read_arrow_stream`. `None` =
    /// `KafkaBatchConfig::default()`.
    #[serde(default)]
    pub batch_config: Option<crate::kafka_backend::KafkaBatchConfig>,
}

/// Σ.B follow-up: serializable mirror of the private
/// `kafka_backend::AuthMode`. `KafkaBackend`'s with_sasl_plain /
/// with_sasl_scram / with_tls / with_msk_iam builder methods
/// take the same arguments these variants carry; `backend_from_
/// config` invokes the right one based on the variant.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KafkaAuthConfig {
    /// No SASL — broker speaks PLAINTEXT.
    None,
    /// SASL/PLAIN over SSL. Confluent Cloud's primary auth mode.
    SaslPlain { username: String, password: String },
    /// SASL/SCRAM over SSL.
    SaslScram {
        mechanism: crate::kafka_backend::ScramMechanism,
        username: String,
        password: String,
    },
    /// mTLS — broker authenticates the client by certificate.
    Tls(crate::kafka_backend::TlsAuth),
    /// AWS MSK IAM. The Rust-side OAUTHBEARER refresh callback
    /// mints SigV4 tokens for the named region.
    MskIam { region: String },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KinesisConfig {
    pub stream_name: String,

    /// Σ.B follow-up: pin the AWS region (overrides credential-
    /// chain resolution). `None` = use AWS SDK's default
    /// resolution (env, IMDS, shared config).
    #[serde(default)]
    pub region: Option<String>,

    /// Override the regional endpoint. Required for LocalStack
    /// (`http://localhost:4566`); leave `None` for real AWS.
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Static AWS credentials. `None` falls back to the AWS
    /// credential chain (env, IMDS, shared config). Inline
    /// credentials per the spike's locked decision; same trust
    /// boundary as Postgres DSN passwords.
    #[serde(default)]
    pub static_credentials: Option<KinesisStaticCredentials>,

    /// Per-call drain limits for `read_arrow_stream`. `None` =
    /// `KinesisBatchConfig::default()` (1000 records / 16 MiB /
    /// 1 empty-poll budget / 250ms idle sleep).
    #[serde(default)]
    pub batch_config: Option<crate::kinesis_backend::KinesisBatchConfig>,
}

/// Σ.B follow-up: serializable mirror of the private
/// `kinesis_backend::StaticAwsCredentials`. Public so it can ride
/// inside [`KinesisConfig`]; the inner `kinesis_backend` keeps its
/// own private struct for the live state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KinesisStaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PubSubConfig {
    pub project_id: String,

    /// Σ.B follow-up: gRPC endpoint override. `None` = default
    /// `https://pubsub.googleapis.com`. Set to
    /// `http://localhost:8085` for the gcloud emulator.
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Use anonymous credentials (no auth headers). Required for
    /// the emulator path. Default `false`.
    #[serde(default)]
    pub anonymous_auth: bool,

    /// Per-call drain limits for `read_arrow_stream`. `None` =
    /// `PubSubBatchConfig::default()`.
    #[serde(default)]
    pub batch_config: Option<crate::pubsub_backend::PubSubBatchConfig>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RabbitMqConfig {
    /// Full AMQP URL — `amqp://user:pass@host:port/vhost` or its
    /// `amqps://` TLS counterpart.
    pub amqp_url: String,

    /// Σ.B follow-up: stable consumer-tag prefix used by
    /// `basic_consume`. `None` = `"ematix-flow-consumer"`. Mostly
    /// informational for management-UI displays.
    #[serde(default)]
    pub consumer_tag: Option<String>,

    /// Per-call drain limits for `read_arrow_stream`. `None` =
    /// `RabbitBatchConfig::default()`.
    #[serde(default)]
    pub batch_config: Option<crate::rabbitmq_backend::RabbitBatchConfig>,
}

/// Σ.B PR 2: distributed-execution backend config. Carries the
/// peer-worker URL list; an empty list is the degenerate single-
/// worker cluster (handy for tests).
///
/// The struct lives in `ematix-flow-core` so the
/// [`BackendConfig::Distributed`] variant can carry it without
/// `ematix-flow-core` depending on `ematix-flow-distributed` (which
/// would create a circular dep — the distributed crate depends on
/// core for the trait shape). Construction lives in the distributed
/// crate via `DistributedBackend::open(cfg)`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DistributedConfig {
    /// Peer worker URLs (e.g. `http://flow-01.cluster.local:50051`).
    /// Empty = single-worker degenerate cluster.
    pub peers: Vec<String>,
    /// Σ.B follow-up: TLS settings for coordinator → worker
    /// connections. `None` (default) keeps the historical plain-HTTP
    /// behaviour. When set, the distributed crate's
    /// `TlsChannelResolver` is wired into the SessionContext so all
    /// outbound peer dials are TLS-encrypted. Server-side TLS for
    /// the worker is configured separately via `flow-worker`'s
    /// `--tls-cert` / `--tls-key` flags — both ends must agree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<DistributedTlsConfig>,
}

/// Σ.B follow-up: client-side TLS knobs for talking to peer workers.
///
/// Carries paths only — no PEM bytes. Loaded lazily when the
/// distributed crate constructs the channel resolver, so a stale
/// path surfaces at first peer dial rather than at config-load (the
/// files might not exist yet during dry-run validation).
///
/// Lives in core so the `BackendConfig::Distributed` round-trip is
/// pure-data; tonic's `ClientTlsConfig` (which would force a tonic
/// dep on core) is built in the distributed crate from these
/// fields.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DistributedTlsConfig {
    /// PEM file containing the CA bundle that signed the workers'
    /// server certificates. Required for any TLS configuration —
    /// without it, peer-cert verification has no anchor.
    pub ca_cert_pem_path: String,
    /// Optional client identity for mutual TLS (mTLS). When set,
    /// the coordinator presents this cert/key pair to workers; the
    /// workers must have been launched with `--tls-client-ca` for
    /// the verification to succeed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_identity: Option<DistributedClientIdentityConfig>,
    /// Optional override for the SNI / hostname used during peer
    /// cert verification. Defaults to the host portion of each peer
    /// URL when `None` — the override is only needed when peer URLs
    /// reference a load balancer or IP that doesn't match the
    /// certificate's CN/SAN.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_name_override: Option<String>,
}

/// Σ.B follow-up: client identity for mutual TLS to peer workers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DistributedClientIdentityConfig {
    /// Path to the coordinator's PEM-encoded client certificate.
    pub cert_pem_path: String,
    /// Path to the coordinator's PEM-encoded private key.
    pub key_pem_path: String,
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
        BackendConfig::Kafka(c) => {
            // Σ.B follow-up: full builder-state round-trip. Apply
            // each optional knob in turn via the existing public
            // with_* methods.
            let mut backend = crate::kafka_backend::KafkaBackend::open(
                c.bootstrap_servers,
                c.group_id.as_deref(),
            )?;
            if let Some(auth) = c.auth {
                backend = match auth {
                    KafkaAuthConfig::None => backend,
                    KafkaAuthConfig::SaslPlain { username, password } => {
                        backend.with_sasl_plain(username, password)
                    }
                    KafkaAuthConfig::SaslScram {
                        mechanism,
                        username,
                        password,
                    } => backend.with_sasl_scram(mechanism, username, password),
                    KafkaAuthConfig::Tls(tls) => backend.with_tls(tls),
                    KafkaAuthConfig::MskIam { region } => backend.with_msk_iam(region),
                };
            }
            if let Some(fmt) = c.payload_format {
                backend = backend.with_payload_format(fmt);
            }
            if let Some(sem) = c.delivery_semantics {
                backend = backend.with_delivery_semantics(sem);
            }
            if let Some(url) = c.schema_registry_url {
                backend = backend.with_schema_registry_url(url);
            }
            if let Some(sr_auth) = c.schema_registry_basic_auth {
                backend =
                    backend.with_schema_registry_basic_auth(sr_auth.username, sr_auth.password);
            }
            if let Some(col) = c.message_key_column {
                backend = backend.with_message_key_column(col);
            }
            if let Some(batch) = c.batch_config {
                backend = backend.with_batch_config(batch);
            }
            Ok(std::sync::Arc::new(backend))
        }
        BackendConfig::Kinesis(c) => {
            // Σ.B follow-up: apply the optional builder-state knobs
            // before handing back the backend. Each is a no-op if
            // the corresponding field is `None`/empty.
            let mut backend = crate::kinesis_backend::KinesisBackend::open(c.stream_name)?;
            if let Some(region) = c.region {
                backend = backend.with_region(region);
            }
            if let Some(endpoint) = c.endpoint {
                backend = backend.with_endpoint(endpoint);
            }
            if let Some(creds) = c.static_credentials {
                backend =
                    backend.with_static_credentials(creds.access_key_id, creds.secret_access_key);
            }
            if let Some(batch) = c.batch_config {
                backend = backend.with_batch_config(batch);
            }
            Ok(std::sync::Arc::new(backend))
        }
        BackendConfig::PubSub(c) => {
            let mut backend = crate::pubsub_backend::PubSubBackend::open(c.project_id)?;
            if let Some(endpoint) = c.endpoint {
                backend = backend.with_endpoint(endpoint);
            }
            if c.anonymous_auth {
                backend = backend.with_anonymous_auth();
            }
            if let Some(batch) = c.batch_config {
                backend = backend.with_batch_config(batch);
            }
            Ok(std::sync::Arc::new(backend))
        }
        BackendConfig::RabbitMq(c) => {
            let mut backend = crate::rabbitmq_backend::RabbitMQBackend::open(c.amqp_url)?;
            if let Some(tag) = c.consumer_tag {
                backend = backend.with_consumer_tag(tag);
            }
            if let Some(batch) = c.batch_config {
                backend = backend.with_batch_config(batch);
            }
            Ok(std::sync::Arc::new(backend))
        }
        BackendConfig::Distributed(_) => Err(BackendError::Other(
            "backend_from_config(Distributed) is intentionally a no-op in core: \
             ematix-flow-core doesn't depend on ematix-flow-distributed \
             (would be a circular dep). Construct via \
             `ematix_flow_distributed::DistributedBackend::open(cfg)` directly. \
             See docs/PHASE_SIGMA_B_TRAIT_SPIKE.md."
                .into(),
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

    // Σ.B follow-up: see the trait-method docs — this is the one
    // legitimate override. PostgresBackend's strategy executors
    // pattern-match on this to recognise a same-dialect peer source
    // and take the COPY BINARY fast path.
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
                        "Postgres run_append cross-backend requires both \
                         endpoints to be Postgres (uses COPY BINARY fast \
                         path). For cross-dialect transfers, route through \
                         the source's read_arrow_stream + target's \
                         write_arrow_stream directly."
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
                        "Postgres run_truncate cross-backend requires both \
                         endpoints to be Postgres (uses COPY BINARY fast \
                         path). For cross-dialect transfers, route through \
                         the source's read_arrow_stream + target's \
                         write_arrow_stream directly."
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
                        "Postgres run_merge cross-backend requires both \
                         endpoints to be Postgres (uses COPY BINARY fast \
                         path). For cross-dialect transfers, route through \
                         the source's read_arrow_stream + target's \
                         write_arrow_stream directly."
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
                        "Postgres run_scd2 cross-backend requires both \
                         endpoints to be Postgres (uses COPY BINARY fast \
                         path). For cross-dialect transfers, route through \
                         the source's read_arrow_stream + target's \
                         write_arrow_stream directly."
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

    /// Phase Δ PR 3: Postgres-target CDC apply. Decodes the
    /// RecordBatch via [`crate::cdc::events_from_batch`], routes
    /// each parsed event to [`crate::pg::PgPool::run_cdc`] for
    /// per-op transactional dispatch, and surfaces the resulting
    /// counts. Tombstones + parse errors are counted as `skipped`
    /// here so the executor itself stays purely about target-side
    /// SQL execution.
    async fn run_cdc(
        &self,
        spec: &crate::types::TableSpec,
        batch: RecordBatch,
        cdc_config: &crate::cdc::CdcConfig,
        pipeline_name: &str,
    ) -> Result<CdcRunResult, BackendError> {
        use crate::cdc::ParsedRow;

        let parsed = crate::cdc::events_from_batch(&batch, cdc_config)?;
        let mut events = Vec::with_capacity(parsed.len());
        let mut skipped: i64 = 0;
        for row in parsed {
            match row {
                ParsedRow::Event(e) => events.push(e),
                ParsedRow::Tombstone => skipped += 1,
                ParsedRow::ParseError(e) => {
                    // Soft-fail per row. The streaming pipeline's
                    // `transform_on_error` policy can promote this
                    // to a hard failure if the user wants strict
                    // mode; for now the parse-failed row is
                    // surfaced via the `skipped` counter and a
                    // warn line so operators can see it.
                    tracing::warn!(
                        target: "ematix_flow::cdc",
                        error = %e,
                        pipeline = pipeline_name,
                        "CDC envelope parse failed; row skipped",
                    );
                    skipped += 1;
                }
            }
        }
        self.pool
            .run_cdc(spec, events, cdc_config, pipeline_name, skipped)
            .await
            .map_err(|e| BackendError::Other(e.to_string()))
    }

    /// Phase Δ PR 5.5: hand the streaming runtime a real
    /// [`TableSpec`] for the target. Reads `information_schema`
    /// via the existing reflection helper + carries the column
    /// set, types, nullability, and PK flags through.
    async fn reflect_table_spec(
        &self,
        target: &TargetTable,
    ) -> Result<crate::types::TableSpec, BackendError> {
        let reflected = self
            .pool
            .read_existing_columns(&target.schema, &target.name)
            .await
            .map_err(|e| BackendError::Other(e.to_string()))?;
        if reflected.is_empty() {
            return Err(BackendError::Other(format!(
                "reflect_table_spec: target {}.{} has no columns — \
                 either the table doesn't exist or the connecting role \
                 lacks information_schema.columns visibility",
                target.schema, target.name
            )));
        }
        let columns: Vec<crate::types::ColumnSpec> = reflected
            .into_iter()
            .map(|c| crate::types::ColumnSpec {
                name: c.name,
                ty: c.ty,
                nullable: c.nullable,
                primary_key: c.primary_key,
            })
            .collect();
        Ok(crate::types::TableSpec {
            schema: target.schema.clone(),
            name: target.name.clone(),
            columns,
            // CDC apply doesn't consult uniques; PR 5.5 leaves them
            // empty. Schema-evolution / drift compare uses the
            // declared column set only.
            unique_constraints: Vec::new(),
            fingerprint: String::new(),
        })
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

/// Recursive Arrow → `serde_json::Value` for the Postgres JSONB write
/// path. Handles the realistic shape of "GROUP BY + array_agg(named_struct(...))"
/// pivots — Arrow `List<Struct<...>>` becomes a JSON array of objects.
///
/// Coverage: primitives (Int*/Float*/Bool/Utf8/Binary as base64),
/// `Timestamp(Microsecond, _)` → ISO-8601 string, `List<T>` → array,
/// `Struct<...>` → object. Nested combinations work via recursion.
/// Unsupported types (e.g. Decimal128, FixedSizeBinary) return an
/// error rather than silently emitting bad JSON.
fn arrow_to_json_value(
    array: &dyn arrow_array::Array,
    row_idx: usize,
) -> Result<serde_json::Value, BackendError> {
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, ListArray, StringArray, StructArray, TimestampMicrosecondArray,
    };
    use serde_json::Value;

    if array.is_null(row_idx) {
        return Ok(Value::Null);
    }
    match array.data_type() {
        DataType::Int16 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row_idx),
        )),
        DataType::Int32 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row_idx),
        )),
        DataType::Int64 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row_idx),
        )),
        DataType::Float32 => {
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row_idx);
            // serde_json rejects NaN/Inf for Number; emit null so the
            // JSONB column accepts the row instead of erroring.
            Ok(serde_json::Number::from_f64(v as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row_idx);
            Ok(serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        DataType::Boolean => Ok(Value::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row_idx),
        )),
        DataType::Utf8 => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row_idx)
                .to_string(),
        )),
        DataType::Binary => {
            // JSON has no binary type; emit lowercase hex with no prefix.
            // Postgres' `to_jsonb(bytea)` defaults to a Base16-style
            // string representation — same shape, no base64 dep.
            let bytes = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row_idx);
            let mut out = String::with_capacity(bytes.len() * 2);
            for b in bytes {
                out.push_str(&format!("{b:02x}"));
            }
            Ok(Value::String(out))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = arr.value(row_idx);
            // ISO-8601 with microsecond precision; matches what the
            // Postgres JSONB driver emits for Timestamp values.
            let secs = micros.div_euclid(1_000_000);
            let nanos = (micros.rem_euclid(1_000_000) * 1_000) as u32;
            let iso = format_iso8601_utc(secs, nanos);
            Ok(Value::String(iso))
        }
        DataType::List(_) => {
            let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
            let inner = arr.value(row_idx);
            let mut out: Vec<Value> = Vec::with_capacity(inner.len());
            for i in 0..inner.len() {
                out.push(arrow_to_json_value(inner.as_ref(), i)?);
            }
            Ok(Value::Array(out))
        }
        DataType::Struct(fields) => {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut obj = serde_json::Map::with_capacity(fields.len());
            for (idx, field) in fields.iter().enumerate() {
                let child = arr.column(idx);
                obj.insert(
                    field.name().clone(),
                    arrow_to_json_value(child.as_ref(), row_idx)?,
                );
            }
            Ok(Value::Object(obj))
        }
        other => Err(BackendError::TypeMapping(format!(
            "Arrow → Postgres JSONB: unsupported child type {other:?}"
        ))),
    }
}

/// Format `secs` (Unix seconds) + `nanos` (sub-second nanoseconds) as
/// ISO-8601 UTC. Standalone helper so [`arrow_to_json_value`] doesn't
/// pull in a chrono dep just for one format call. Year arithmetic
/// uses the same Howard-Hinnant civil_from_days routine that
/// `chrono_compat_iso8601_now` in `objectstore_backend.rs` uses.
fn format_iso8601_utc(secs: i64, nanos: u32) -> String {
    let days = secs.div_euclid(86_400);
    let time_secs = secs.rem_euclid(86_400);
    let h = time_secs / 3600;
    let m = (time_secs % 3600) / 60;
    let s = time_secs % 60;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    let micros = nanos / 1_000;
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{micros:06}Z")
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
            // Pivot-to-JSONB shortcut: when the destination column is
            // JSON / JSONB (oid 114 / 3802) AND the source carries a
            // List or Struct, recursively serialize via
            // `arrow_to_json_value`. This is the path that makes
            // `array_agg(named_struct(...))` from a `GROUP BY`
            // transform write directly into a JSONB column without a
            // staging table — see docs/USER_GUIDE.md "Aggregating
            // many source rows into one JSON-shaped target row".
            let dest_oid = param_types.get(col_idx).map(|t| t.oid());
            if matches!(dest_oid, Some(114) | Some(3802))
                && matches!(field.data_type(), DataType::List(_) | DataType::Struct(_))
            {
                owned_json[col_idx] = Some(arrow_to_json_value(col.as_ref(), row_idx)?);
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
                // List / Struct column targeting a JSON / JSONB Postgres
                // column — bound via the serde_json::Value the
                // pre-pass in this loop already populated. Targeting
                // any non-JSONB column with List/Struct is a config
                // error that the dest-oid match below surfaces.
                DataType::List(_) | DataType::Struct(_) => {
                    match param_types.get(col_idx).map(|t| t.oid()) {
                        Some(114) | Some(3802) => params.push(&owned_json[col_idx] as ToSqlRef<'_>),
                        _ => {
                            return Err(BackendError::TypeMapping(format!(
                                "Arrow → Postgres: column {} is {:?}, but the destination \
                             Postgres type isn't JSON or JSONB. List/Struct columns are \
                             only writable into JSONB targets — change the destination \
                             column type or drop this column from the SELECT.",
                                field.name(),
                                field.data_type(),
                            )));
                        }
                    }
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

    /// `arrow_to_json_value` collapses Arrow primitives into JSON
    /// primitives. Smoke-only — the recursive cases below depend on
    /// these working.
    #[test]
    fn arrow_to_json_value_handles_primitives() {
        use arrow_array::{
            BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
        };
        let i = Int64Array::from(vec![Some(7_i64), None]);
        assert_eq!(arrow_to_json_value(&i, 0).unwrap(), serde_json::json!(7));
        assert_eq!(arrow_to_json_value(&i, 1).unwrap(), serde_json::Value::Null);

        let f = Float64Array::from(vec![Some(1.5), Some(f64::NAN)]);
        assert_eq!(arrow_to_json_value(&f, 0).unwrap(), serde_json::json!(1.5));
        // NaN is not representable in JSON; emit null rather than
        // erroring, so a single bad row doesn't tank a 10k-row batch.
        assert_eq!(arrow_to_json_value(&f, 1).unwrap(), serde_json::Value::Null);

        let b = BooleanArray::from(vec![Some(true)]);
        assert_eq!(arrow_to_json_value(&b, 0).unwrap(), serde_json::json!(true));

        let s = StringArray::from(vec![Some("hi")]);
        assert_eq!(arrow_to_json_value(&s, 0).unwrap(), serde_json::json!("hi"));

        // ISO 8601 round-trip — assert the format shape, not a
        // specific calendar date (the year arithmetic is a hot
        // path in its own right and exercised by other backends).
        let ts = TimestampMicrosecondArray::from(vec![Some(1_778_438_400_000_000_i64)]);
        let v = arrow_to_json_value(&ts, 0).unwrap();
        let s = v.as_str().expect("timestamp serializes as string");
        assert_eq!(s.len(), 27, "ISO 8601 with microsecond precision");
        assert!(s.ends_with("Z"), "UTC suffix");
        assert!(s.contains('T'), "date/time separator");
        assert!(s.contains('.'), "fractional-second separator");
    }

    /// The realistic path: `array_agg(named_struct(...))` produces
    /// `List<Struct<...>>`. Each row of the output List must serialize
    /// to a JSON array of objects — that's what gets bound into a
    /// JSONB column. This is the `option_chain_snapshots.strikes_json`
    /// shape from real user feedback (60 contracts/min → one minute
    /// row with a JSON list of contracts).
    #[test]
    fn arrow_to_json_value_serializes_list_of_struct_as_json_array() {
        use arrow_array::builder::{Float64Builder, Int64Builder, ListBuilder, StructBuilder};
        use arrow_schema::{Field as ArrowField, Fields};

        let fields: Fields = vec![
            ArrowField::new("strike", DataType::Int64, false),
            ArrowField::new("bid", DataType::Float64, false),
        ]
        .into();
        let struct_builder = StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(Int64Builder::new()),
                Box::new(Float64Builder::new()),
            ],
        );
        let mut list_builder = ListBuilder::new(struct_builder);

        // Row 0: two contracts (strike=4500 bid=1.25, strike=4505 bid=0.75).
        let sb = list_builder.values();
        sb.field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(4500);
        sb.field_builder::<Float64Builder>(1)
            .unwrap()
            .append_value(1.25);
        sb.append(true);
        sb.field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(4505);
        sb.field_builder::<Float64Builder>(1)
            .unwrap()
            .append_value(0.75);
        sb.append(true);
        list_builder.append(true);

        // Row 1: empty list (a quiet minute).
        list_builder.append(true);

        let arr = list_builder.finish();
        assert_eq!(
            arrow_to_json_value(&arr, 0).unwrap(),
            serde_json::json!([
                {"strike": 4500, "bid": 1.25},
                {"strike": 4505, "bid": 0.75},
            ])
        );
        assert_eq!(arrow_to_json_value(&arr, 1).unwrap(), serde_json::json!([]));
    }

    /// `Struct<...>` (no surrounding list) becomes a JSON object —
    /// useful for "pivot one row's columns into a JSONB blob" without
    /// the GROUP-BY shape.
    #[test]
    fn arrow_to_json_value_serializes_struct_as_json_object() {
        use arrow_array::builder::{Int64Builder, StringBuilder, StructBuilder};
        use arrow_schema::{Field as ArrowField, Fields};

        let fields: Fields = vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("note", DataType::Utf8, true),
        ]
        .into();
        let mut sb = StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(Int64Builder::new()),
                Box::new(StringBuilder::new()),
            ],
        );
        sb.field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(42);
        sb.field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("hi");
        sb.append(true);
        let arr = sb.finish();
        assert_eq!(
            arrow_to_json_value(&arr, 0).unwrap(),
            serde_json::json!({"id": 42, "note": "hi"})
        );
    }

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

    /// Coverage backfill: locks the remaining `From<PgError>` arms
    /// — `Other` flows to `BackendError::Other` (the catch-all
    /// kind), preserving the message verbatim.
    #[test]
    fn pg_error_other_maps_to_backend_other() {
        let other_err: BackendError = PgError::Other("CDC target has no PK".into()).into();
        match other_err {
            BackendError::Other(msg) => {
                assert!(msg.contains("CDC target has no PK"));
            }
            _ => panic!("PgError::Other should map to BackendError::Other"),
        }
    }

    /// `From<AppendRunResult>` projects fields through with the
    /// strategy-result shape: rows_inserted carries through;
    /// rows_updated / rows_closed are `None` for an append.
    #[test]
    fn append_run_result_projects_to_strategy_run_result() {
        let appended = crate::pg::AppendRunResult {
            run_id: "r1".into(),
            rows_inserted: 42,
            status: "success".into(),
            path: "same_db".into(),
        };
        let s: StrategyRunResult = appended.into();
        assert_eq!(s.run_id, "r1");
        assert_eq!(s.rows_inserted, 42);
        assert_eq!(s.rows_updated, None);
        assert_eq!(s.rows_unchanged, None);
        assert_eq!(s.rows_closed, None);
        assert_eq!(s.status, "success");
        assert_eq!(s.path, "same_db");
    }

    /// `From<MergeRunResult>` projects insert + update + unchanged
    /// counts (rows_closed stays `None`).
    #[test]
    fn merge_run_result_projects_to_strategy_run_result() {
        let merged = crate::pg::MergeRunResult {
            run_id: "r2".into(),
            rows_inserted: 5,
            rows_updated: 3,
            rows_unchanged: 2,
            status: "success".into(),
            path: "cross_db".into(),
        };
        let s: StrategyRunResult = merged.into();
        assert_eq!(s.rows_inserted, 5);
        assert_eq!(s.rows_updated, Some(3));
        assert_eq!(s.rows_unchanged, Some(2));
        assert_eq!(s.rows_closed, None);
    }

    /// `From<Scd2RunResult>` projects insert + closed counts.
    #[test]
    fn scd2_run_result_projects_to_strategy_run_result() {
        let scd2 = crate::pg::Scd2RunResult {
            run_id: "r3".into(),
            rows_inserted: 11,
            rows_closed: 7,
            status: "success".into(),
            path: "same_db".into(),
        };
        let s: StrategyRunResult = scd2.into();
        assert_eq!(s.rows_inserted, 11);
        assert_eq!(s.rows_closed, Some(7));
        // SCD2 doesn't report row-by-row updates separately —
        // the projection leaves them None.
        assert_eq!(s.rows_updated, None);
        assert_eq!(s.rows_unchanged, None);
    }

    /// Default `Backend::run_cdc` impl errors with a clear
    /// "not implemented" message that names the dialect — picked
    /// up by the streaming runtime when an unsupported target is
    /// configured. Verified against ObjectStore (local file://),
    /// which doesn't override the default: object stores have no
    /// row-level CDC story by design and so are the natural
    /// long-term home for this contract test as more SQL backends
    /// light up native `run_cdc`.
    #[tokio::test]
    async fn default_run_cdc_errors_with_dialect_name() {
        use crate::ObjectStoreBackend;
        use crate::backend::ObjectFormat;
        use crate::types::{ColumnSpec, ColumnType, TableSpec};

        let tmp = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(tmp.path(), ObjectFormat::Parquet).unwrap();
        let spec = TableSpec {
            schema: "main".into(),
            name: "t".into(),
            columns: vec![ColumnSpec {
                name: "id".into(),
                ty: ColumnType::BigInt,
                nullable: false,
                primary_key: true,
            }],
            unique_constraints: vec![],
            fingerprint: String::new(),
        };
        let cdc_cfg = crate::cdc::CdcConfig::for_envelope(crate::cdc::EnvelopeKind::Debezium);
        // Build a 0-row RecordBatch — the default impl returns
        // before parsing, so the batch shape is irrelevant.
        let arrow_schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "op",
                arrow_schema::DataType::Utf8,
                true,
            )]));
        let batch = RecordBatch::new_empty(arrow_schema);
        let err = backend
            .run_cdc(&spec, batch, &cdc_cfg, "p")
            .await
            .expect_err("default run_cdc must error");
        let msg = err.to_string();
        assert!(
            msg.contains("ObjectStore"),
            "must name the dialect, got: {msg}"
        );
        assert!(msg.contains("not yet implemented"), "got: {msg}");
    }

    /// Default `Backend::reflect_table_spec` impl errors with a
    /// "not implemented for backend dialect" message that names
    /// the dialect.
    #[tokio::test]
    async fn default_reflect_table_spec_errors_with_dialect_name() {
        use crate::DuckDBBackend;

        let backend = DuckDBBackend::open(":memory:").unwrap();
        let target = TargetTable {
            schema: "main".into(),
            name: "t".into(),
        };
        let err = backend
            .reflect_table_spec(&target)
            .await
            .expect_err("default reflect_table_spec must error");
        let msg = err.to_string();
        assert!(msg.contains("DuckDB"), "must name the dialect, got: {msg}");
        assert!(msg.contains("not implemented"), "got: {msg}");
    }

    /// Default `Backend::seek_to` errors with a "does not support
    /// seek_to" message — the config-load probe relies on this so
    /// session/join pipelines fail loud against unsupported sources.
    #[tokio::test]
    async fn default_seek_to_errors_with_clear_message() {
        use crate::DuckDBBackend;

        let backend = DuckDBBackend::open(":memory:").unwrap();
        let err = backend
            .seek_to(&[1, 2, 3])
            .await
            .expect_err("default seek_to must error");
        let msg = err.to_string();
        assert!(
            msg.contains("seek_to"),
            "error must mention seek_to, got: {msg}"
        );
        assert!(msg.contains("DuckDB"), "must name the dialect, got: {msg}");
    }

    /// Default `Backend::offset_snapshot` returns `Ok(None)` —
    /// non-streaming backends have nothing to snapshot.
    #[tokio::test]
    async fn default_offset_snapshot_returns_none() {
        use crate::DuckDBBackend;

        let backend = DuckDBBackend::open(":memory:").unwrap();
        let snap = backend.offset_snapshot().await.expect("must not error");
        assert!(snap.is_none());
    }

    /// Default `Backend::commit_offsets` is a no-op `Ok(())`.
    /// Most backends have no committed-offset notion; only Kafka
    /// overrides.
    #[tokio::test]
    async fn default_commit_offsets_is_noop() {
        use crate::DuckDBBackend;

        let backend = DuckDBBackend::open(":memory:").unwrap();
        backend.commit_offsets().await.expect("default no-op");
    }

    /// Default `Backend::supports_seek_to` returns `false`. The
    /// streaming runtime's pipeline-config validator probes this
    /// to reject session/join configs against unsupported sources.
    #[test]
    fn default_supports_seek_to_returns_false() {
        // SQLite as the example backend that doesn't override.
        // Construction is `:memory:` so no infrastructure deps.
        let backend = crate::SQLiteBackend::open(":memory:").unwrap();
        assert!(!backend.supports_seek_to());
    }

    // ---- Pure helpers: quote_ident, pg_type_to_arrow, system_time_to_micros
    //
    // These three free functions in `backend.rs` are reachable only
    // through `read_arrow_stream` / `write_arrow_stream` integration
    // paths. The integration tests exercise the happy path; this
    // section closes the per-arm match coverage and the error path.

    #[test]
    fn quote_ident_doubles_embedded_quotes() {
        // pg_catalog.quote_ident() semantics: wrap in double-quotes,
        // escape any embedded `"` by doubling.
        assert_eq!(quote_ident("foo"), "\"foo\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_ident("\"\"\""), "\"\"\"\"\"\"\"\""); // 3 quotes → 6 inside the wrapper
        // Empty + space-containing names round-trip too.
        assert_eq!(quote_ident(""), "\"\"");
        assert_eq!(quote_ident("with space"), "\"with space\"");
    }

    #[test]
    fn pg_type_to_arrow_maps_every_supported_oid() {
        // Each match arm of `pg_type_to_arrow` corresponds to a
        // tokio_postgres `Type` constant. Walk all supported OIDs
        // explicitly so a future remap accidentally narrowing the
        // type set surfaces here.
        use tokio_postgres::types::Type as T;
        assert!(matches!(
            pg_type_to_arrow(&T::INT2).unwrap(),
            DataType::Int16
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::INT4).unwrap(),
            DataType::Int32
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::INT8).unwrap(),
            DataType::Int64
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::FLOAT4).unwrap(),
            DataType::Float32
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::FLOAT8).unwrap(),
            DataType::Float64
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::BOOL).unwrap(),
            DataType::Boolean
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::TEXT).unwrap(),
            DataType::Utf8
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::VARCHAR).unwrap(),
            DataType::Utf8
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::BPCHAR).unwrap(),
            DataType::Utf8
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::BYTEA).unwrap(),
            DataType::Binary
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::UUID).unwrap(),
            DataType::Utf8
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::JSON).unwrap(),
            DataType::Utf8
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::JSONB).unwrap(),
            DataType::Utf8
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::TIMESTAMP).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        ));
        assert!(matches!(
            pg_type_to_arrow(&T::TIMESTAMPTZ).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        ));
    }

    #[test]
    fn pg_type_to_arrow_errors_on_unsupported_oid() {
        // The `_` arm returns BackendError::TypeMapping with a
        // human-readable message that names the type. Use INTERVAL
        // (oid 1186), which is in the tokio_postgres registry but
        // not in our type map.
        use tokio_postgres::types::Type as T;
        let err = pg_type_to_arrow(&T::INTERVAL).expect_err("INTERVAL must be unsupported");
        match err {
            BackendError::TypeMapping(msg) => {
                assert!(
                    msg.contains("interval") || msg.contains("Interval"),
                    "error must name the unsupported type, got: {msg}"
                );
                assert!(
                    msg.contains("oid="),
                    "error must include the OID for diagnostics, got: {msg}"
                );
                assert!(
                    msg.contains("pg_type_to_arrow"),
                    "error must point to the function that needs widening, got: {msg}"
                );
            }
            other => panic!("expected TypeMapping, got {other:?}"),
        }
    }

    #[test]
    fn system_time_to_micros_handles_unix_epoch_anchor() {
        use std::time::{Duration, UNIX_EPOCH};
        // UNIX_EPOCH itself → 0 microseconds.
        assert_eq!(system_time_to_micros(UNIX_EPOCH), 0);
        // 1 second after epoch → 1_000_000 µs.
        assert_eq!(
            system_time_to_micros(UNIX_EPOCH + Duration::from_secs(1)),
            1_000_000
        );
        // Sub-second precision preserved.
        assert_eq!(
            system_time_to_micros(UNIX_EPOCH + Duration::from_micros(123_456)),
            123_456
        );
        // A "modern" timestamp round-trips: 2026-05-08T00:00:00Z =
        // 1778198400 unix seconds = 1_778_198_400_000_000 µs.
        let modern = UNIX_EPOCH + Duration::from_secs(1_778_198_400);
        assert_eq!(system_time_to_micros(modern), 1_778_198_400_000_000);
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
