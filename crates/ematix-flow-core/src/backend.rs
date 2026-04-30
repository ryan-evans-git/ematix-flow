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
    BooleanBuilder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectFormat {
    Parquet,
    Csv,
    Orc,
    JsonLines,
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
pub type ArrowBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, BackendError>> + Send>>;

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
pub trait Backend: Send + Sync {
    fn dialect(&self) -> Dialect;

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
    async fn read_arrow_stream(&self, query: &str)
        -> Result<ArrowBatchStream, BackendError>;

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
}

/// Re-export for trait method signatures.
pub use crate::meta::DeleteHandling;

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

    async fn read_arrow_stream(
        &self,
        query: &str,
    ) -> Result<ArrowBatchStream, BackendError> {
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
        20 => DataType::Int64,                                       // INT8
        23 => DataType::Int32,                                       // INT4
        25 | 1043 | 1042 => DataType::Utf8,                          // TEXT/VARCHAR/BPCHAR
        16 => DataType::Boolean,                                     // BOOL
        1184 | 1114 => DataType::Timestamp(TimeUnit::Microsecond, None), // TIMESTAMPTZ/TIMESTAMP
        _ => {
            return Err(BackendError::TypeMapping(format!(
                "Phase 30b Postgres → Arrow: unsupported type {} (oid={})",
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
        let dt = pg_type_to_arrow(col.type_())?;
        let array: StdArc<dyn Array> = match dt {
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    let v: Option<i64> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!(
                            "row[{idx}] {} → i64: {e}",
                            col.name()
                        ))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Int32 => {
                let mut b = Int32Builder::with_capacity(rows.len());
                for row in rows {
                    let v: Option<i32> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!(
                            "row[{idx}] {} → i32: {e}",
                            col.name()
                        ))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    let v: Option<&str> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!(
                            "row[{idx}] {} → text: {e}",
                            col.name()
                        ))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(rows.len());
                for row in rows {
                    let v: Option<bool> = row.try_get(idx).map_err(|e| {
                        BackendError::TypeMapping(format!(
                            "row[{idx}] {} → bool: {e}",
                            col.name()
                        ))
                    })?;
                    b.append_option(v);
                }
                StdArc::new(b.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                use std::time::SystemTime;
                let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
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
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| BackendError::TypeMapping(e.to_string()))
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
        BooleanArray, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };

    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let arrow_schema = batch.schema();
    let cols: Vec<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
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

    let mut rows_written: u64 = 0;
    for row_idx in 0..batch.num_rows() {
        // Build the parameter values borrowed for this row's lifetime.
        // For Phase 30b we only support a starter type set; richer
        // coverage comes with Phase 30d's strategy-executor migration.
        let mut owned_strs: Vec<Option<String>> = vec![None; batch.num_columns()];
        let mut owned_i64: Vec<Option<i64>> = vec![None; batch.num_columns()];
        let mut owned_i32: Vec<Option<i32>> = vec![None; batch.num_columns()];
        let mut owned_bool: Vec<Option<bool>> = vec![None; batch.num_columns()];
        let mut owned_ts: Vec<Option<std::time::SystemTime>> =
            vec![None; batch.num_columns()];

        for (col_idx, field) in arrow_schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            if col.is_null(row_idx) {
                continue;
            }
            match field.data_type() {
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    owned_i64[col_idx] = Some(arr.value(row_idx));
                }
                DataType::Int32 => {
                    let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                    owned_i32[col_idx] = Some(arr.value(row_idx));
                }
                DataType::Utf8 => {
                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                    owned_strs[col_idx] = Some(arr.value(row_idx).to_string());
                }
                DataType::Boolean => {
                    let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                    owned_bool[col_idx] = Some(arr.value(row_idx));
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    let arr =
                        col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
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
                DataType::Int64 => params.push(&owned_i64[col_idx] as ToSqlRef<'_>),
                DataType::Int32 => params.push(&owned_i32[col_idx] as ToSqlRef<'_>),
                DataType::Utf8 => params.push(&owned_strs[col_idx] as ToSqlRef<'_>),
                DataType::Boolean => params.push(&owned_bool[col_idx] as ToSqlRef<'_>),
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
