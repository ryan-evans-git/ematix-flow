//! Phase 32a: SQLite backend skeleton.
//!
//! Implements the connection-level + Arrow IO surface of the `Backend`
//! trait. Strategy executors are stubbed for 32a and land in 32b–d.
//!
//! Threading model: matches DuckDB's. `rusqlite::Connection` is
//! `Send + !Sync`, so we wrap it in `Arc<Mutex<…>>` and bridge sync
//! into our async trait via `tokio::task::spawn_blocking`.
//!
//! ## Schema model (MVP)
//! SQLite has no first-class schemas — it has *attached databases*,
//! and `<dbname>.<table>` only resolves when the named database has
//! been ATTACHed. For Phase 32a we require `target.schema = "main"`
//! (or empty) and reject other values with a clear error. Multi-schema
//! support via auto-ATTACH is a future enhancement.
//!
//! ## Type model
//! SQLite's storage classes (INTEGER / REAL / TEXT / BLOB / NULL) are
//! coerced into Arrow as follows:
//!   - INTEGER → Int64Array
//!   - REAL    → Float64Array
//!   - TEXT    → StringArray
//!   - BLOB    → BinaryArray
//!   - NULL    → null bit in whichever Array the column resolves to.
//!
//! The framework's column types map outbound as:
//!   - BigInt / Boolean → INTEGER
//!   - Double / Float    → REAL
//!   - Text / String / VARCHAR / Uuid / TimestampTz → TEXT (ISO-8601
//!     for timestamps, hyphenated for UUID)
//!   - Bytes             → BLOB
//!
//! See `docs/MULTI_BACKEND_PLAN.md` Phase 32 for the full design.

use std::sync::{Arc, Mutex};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use futures_util::stream;
use rusqlite::Connection as SqliteConn;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    TargetTable, WriteMode,
};
use crate::meta::{WatermarkConfig, build_hard_delete_sql, wrap_with_watermark_filter};
use crate::pg::ConnectionInfo;
use crate::strategy::append::{BATCH_ID_COL, LOADED_AT_COL, plan_same_db_append};
use crate::strategy::truncate::plan_truncate_replace;
use crate::types::TableSpec;
use uuid::Uuid;

const META_RUN_HISTORY: &str = "ematix_flow_run_history";
const META_WATERMARKS: &str = "ematix_flow_watermarks";

/// SQL that lazy-creates the meta tables on SQLite. SQLite has no
/// first-class schemas, so we live in `main` with a `ematix_flow_`
/// prefix. UUID columns become TEXT (stored as canonical hyphenated
/// hex); TIMESTAMPTZ becomes TEXT (ISO-8601 with `'Z'` suffix).
fn ensure_meta_schema_sql() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {META_RUN_HISTORY} (\
            run_id TEXT PRIMARY KEY, \
            parent_run_id TEXT, \
            pipeline_name TEXT NOT NULL, \
            step_name TEXT, \
            target_schema TEXT NOT NULL, \
            target_table TEXT NOT NULL, \
            mode TEXT NOT NULL, \
            path TEXT NOT NULL, \
            started_at TEXT NOT NULL, \
            finished_at TEXT, \
            status TEXT NOT NULL, \
            rows_inserted INTEGER, \
            rows_updated INTEGER, \
            rows_unchanged INTEGER, \
            error_message TEXT, \
            metrics_json TEXT\
         ); \
         CREATE TABLE IF NOT EXISTS {META_WATERMARKS} (\
            pipeline_name TEXT PRIMARY KEY, \
            column_name TEXT NOT NULL, \
            last_value TEXT NOT NULL, \
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))\
         )"
    )
}

/// SQLite-specific substitutions. The PG-shaped planners emit three
/// constructs SQLite rejects: `now()` (function), `$1::uuid` (cast +
/// non-existent UUID type), and `TRUNCATE TABLE …` (no such
/// statement). Each is replaced inline before execution. UUIDs are
/// stored as TEXT (canonical hyphenated hex).
fn sqlite_substitute(sql: &str, batch_id: &Uuid) -> String {
    let trimmed = sql.trim_start();
    if let Some(rest) = trimmed.strip_prefix("TRUNCATE TABLE ") {
        return format!("DELETE FROM {rest}");
    }
    sql.replace("now()", "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')")
        .replace("$1::uuid", &format!("'{batch_id}'"))
}

fn is_metadata_col(name: &str) -> bool {
    name == LOADED_AT_COL || name == BATCH_ID_COL
}

/// SQLite-native merge SQL builder. SQLite supports `INSERT … ON
/// CONFLICT (cols) DO UPDATE SET col = excluded.col` since 3.24, but
/// uses lowercase `excluded` (PG/DuckDB use uppercase `EXCLUDED`).
/// Otherwise the shape mirrors `duckdb_merge_sql`. UUID + timestamp
/// emission go as plain TEXT, matching the rest of the SQLite layer.
fn sqlite_merge_sql(
    target: &TableSpec,
    source_query: &str,
    keys: &[String],
    update_columns: &[String],
    batch_id: &Uuid,
) -> String {
    let user_columns: Vec<String> = target
        .columns
        .iter()
        .filter(|c| !is_metadata_col(&c.name))
        .map(|c| c.name.clone())
        .collect();
    let has_metadata = target.columns.iter().any(|c| is_metadata_col(&c.name));
    let mut insert_cols: Vec<String> = user_columns.clone();
    let mut select_exprs: Vec<String> = user_columns.clone();
    if has_metadata {
        insert_cols.push(LOADED_AT_COL.into());
        insert_cols.push(BATCH_ID_COL.into());
        select_exprs.push("strftime('%Y-%m-%dT%H:%M:%fZ', 'now')".into());
        select_exprs.push(format!("'{batch_id}'"));
    }
    let on_conflict = if update_columns.is_empty() {
        format!("ON CONFLICT ({}) DO NOTHING", keys.join(", "))
    } else {
        let mut sets: Vec<String> = update_columns
            .iter()
            .map(|c| format!("{c} = excluded.{c}"))
            .collect();
        if has_metadata {
            sets.push(format!("{LOADED_AT_COL} = excluded.{LOADED_AT_COL}"));
            sets.push(format!("{BATCH_ID_COL} = excluded.{BATCH_ID_COL}"));
        }
        format!(
            "ON CONFLICT ({}) DO UPDATE SET {}",
            keys.join(", "),
            sets.join(", ")
        )
    };
    // SQLite's UPSERT grammar is ambiguous when the SELECT has a FROM
    // clause: `… FROM x ON CONFLICT …` could parse as a join. Disambig
    // with a trailing `WHERE true` (per the SQLite docs' UPSERT note).
    format!(
        "INSERT INTO {schema}.{table} ({insert_cols}) \
         SELECT {select_exprs} FROM ({source}) src_inner WHERE true \
         {on_conflict}",
        schema = if target.schema.is_empty() {
            "main"
        } else {
            &target.schema
        },
        table = target.name,
        insert_cols = insert_cols.join(", "),
        select_exprs = select_exprs.join(", "),
        source = source_query,
    )
}

/// SQLite-backed implementation of `Backend`.
///
/// `SQLiteBackend::open(":memory:")` for a fresh in-memory DB;
/// `SQLiteBackend::open("/path/to/foo.sqlite")` for a file-backed
/// one (auto-created if missing).
pub struct SQLiteBackend {
    conn: Arc<Mutex<SqliteConn>>,
    location: String,
}

impl SQLiteBackend {
    pub fn open(location: impl Into<String>) -> Result<Self, BackendError> {
        let location = location.into();
        let conn = if location == ":memory:" {
            SqliteConn::open_in_memory().map_err(|e| BackendError::Connection(e.to_string()))?
        } else {
            SqliteConn::open(&location).map_err(|e| BackendError::Connection(e.to_string()))?
        };
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            location,
        })
    }

    pub fn location(&self) -> &str {
        &self.location
    }

    async fn with_conn_blocking<F, R>(&self, f: F) -> Result<R, BackendError>
    where
        F: FnOnce(&SqliteConn) -> Result<R, BackendError> + Send + 'static,
        R: Send + 'static,
    {
        let arc = self.conn.clone();
        let join = tokio::task::spawn_blocking(move || {
            let guard = arc
                .lock()
                .map_err(|e| BackendError::Other(format!("sqlite mutex poisoned: {e}")))?;
            f(&guard)
        });
        match join.await {
            Ok(r) => r,
            Err(e) => Err(BackendError::Other(format!("sqlite task join: {e}"))),
        }
    }

    async fn ensure_meta_schema(&self) -> Result<(), BackendError> {
        self.with_conn_blocking(|c| {
            c.execute_batch(&ensure_meta_schema_sql())
                .map_err(|e| BackendError::Query(e.to_string()))
        })
        .await
    }

    async fn record_run_start(
        &self,
        run_id: Uuid,
        pipeline_name: &str,
        target_schema: &str,
        target_table: &str,
        mode: &str,
        path: &str,
    ) -> Result<(), BackendError> {
        let pipeline_name = pipeline_name.to_string();
        let target_schema = target_schema.to_string();
        let target_table = target_table.to_string();
        let mode = mode.to_string();
        let path = path.to_string();
        self.with_conn_blocking(move |c| {
            c.execute(
                &format!(
                    "INSERT INTO {META_RUN_HISTORY} \
                     (run_id, pipeline_name, target_schema, target_table, mode, path, \
                      started_at, status) \
                     VALUES (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), \
                             'running')"
                ),
                rusqlite::params![
                    run_id.to_string(),
                    pipeline_name,
                    target_schema,
                    target_table,
                    mode,
                    path,
                ],
            )
            .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn record_run_success(
        &self,
        run_id: Uuid,
        rows_inserted: i64,
    ) -> Result<(), BackendError> {
        self.with_conn_blocking(move |c| {
            c.execute(
                &format!(
                    "UPDATE {META_RUN_HISTORY} \
                     SET status='success', rows_inserted=?, \
                         finished_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE run_id=?"
                ),
                rusqlite::params![rows_inserted, run_id.to_string()],
            )
            .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(())
        })
        .await
    }

    #[allow(dead_code)] // wired in 32c
    async fn record_run_success_merge(
        &self,
        run_id: Uuid,
        rows_inserted: i64,
        rows_updated: i64,
        rows_unchanged: i64,
    ) -> Result<(), BackendError> {
        self.with_conn_blocking(move |c| {
            c.execute(
                &format!(
                    "UPDATE {META_RUN_HISTORY} \
                     SET status='success', rows_inserted=?, rows_updated=?, \
                         rows_unchanged=?, \
                         finished_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE run_id=?"
                ),
                rusqlite::params![
                    rows_inserted,
                    rows_updated,
                    rows_unchanged,
                    run_id.to_string()
                ],
            )
            .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn record_run_failure(
        &self,
        run_id: Uuid,
        error_message: &str,
    ) -> Result<(), BackendError> {
        let error_message = error_message.to_string();
        self.with_conn_blocking(move |c| {
            c.execute(
                &format!(
                    "UPDATE {META_RUN_HISTORY} \
                     SET status='failed', error_message=?, \
                         finished_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE run_id=?"
                ),
                rusqlite::params![error_message, run_id.to_string()],
            )
            .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(())
        })
        .await
    }

    /// Compute MAX(<column>) over rows just inserted in this batch and
    /// UPSERT into the watermarks table. SQLite supports
    /// `INSERT … ON CONFLICT … DO UPDATE` since 3.24, and the bundled
    /// rusqlite SQLite is well past that. NULL MAX is a no-op so a
    /// stale watermark survives a zero-row run.
    async fn advance_watermark(
        &self,
        target: &TableSpec,
        batch_id: Uuid,
        pipeline_name: &str,
        watermark: &WatermarkConfig,
    ) -> Result<(), BackendError> {
        let target_schema = target.schema.clone();
        let target_table = target.name.clone();
        let pipeline_name = pipeline_name.to_string();
        let column = watermark.column.clone();
        self.with_conn_blocking(move |c| {
            // Schema is always 'main' or empty — qualifying with 'main.'
            // works either way. Cast the watermark value to TEXT so
            // every type round-trips through last_value.
            let max_sql = format!(
                "SELECT CAST(MAX({col}) AS TEXT) FROM {schema}.{table} \
                 WHERE {batch_id_col} = ?",
                col = column,
                schema = if target_schema.is_empty() {
                    "main"
                } else {
                    &target_schema
                },
                table = target_table,
                batch_id_col = BATCH_ID_COL,
            );
            let mut stmt = c
                .prepare(&max_sql)
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let max_value: Option<String> = stmt
                .query_row(rusqlite::params![batch_id.to_string()], |r| r.get(0))
                .map_err(|e| BackendError::Query(e.to_string()))?;
            drop(stmt);

            let Some(value) = max_value else {
                return Ok(());
            };
            c.execute(
                &format!(
                    "INSERT INTO {META_WATERMARKS} \
                     (pipeline_name, column_name, last_value, updated_at) \
                     VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
                     ON CONFLICT (pipeline_name) DO UPDATE SET \
                       column_name = excluded.column_name, \
                       last_value = excluded.last_value, \
                       updated_at = excluded.updated_at"
                ),
                rusqlite::params![pipeline_name, column, value],
            )
            .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(())
        })
        .await
    }
}

/// Validate SQLite's MVP schema constraint: only `main` (or empty) for
/// 32a. Returns the bare table name to use in SQL.
fn require_main_schema(target_schema: &str) -> Result<(), BackendError> {
    if target_schema.is_empty() || target_schema == "main" {
        Ok(())
    } else {
        Err(BackendError::Other(format!(
            "SQLite backend (Phase 32) does not yet support schema '{target_schema}'; \
             use schema='main' or empty. Multi-schema support via ATTACH DATABASE \
             is a future enhancement."
        )))
    }
}

/// SQLite's dynamic typing means a column's declared type is a hint,
/// not a guarantee, and computed columns (`count(*)`, constants) carry
/// no declared type at all. To map cleanly into Arrow we collect each
/// column's values as `rusqlite::types::Value` first, then pick a
/// concrete Arrow data type per column from what was actually present.
/// The hint from `column_decltype` is used only as a tiebreaker when
/// every value is NULL.
///
/// Rule: if any row in a column produced a non-NULL value, the column
/// inherits that value's storage class (Integer → Int64, Real →
/// Float64, Blob → Binary, Text → Utf8). Mixed columns fall back to
/// Utf8 with stringified values. All-NULL columns use the decltype
/// hint, defaulting to Utf8.
fn sqlite_query_to_record_batch(
    conn: &SqliteConn,
    query: &str,
) -> Result<RecordBatch, BackendError> {
    use rusqlite::types::Value;

    let mut stmt = conn
        .prepare(query)
        .map_err(|e| BackendError::Query(e.to_string()))?;
    let column_meta: Vec<(String, String)> = stmt
        .columns()
        .iter()
        .map(|c| {
            (
                c.name().to_string(),
                c.decl_type().unwrap_or("").to_uppercase(),
            )
        })
        .collect();
    let n_cols = column_meta.len();
    let mut buf: Vec<Vec<Value>> = (0..n_cols).map(|_| Vec::new()).collect();
    let mut rows = stmt
        .query([])
        .map_err(|e| BackendError::Query(e.to_string()))?;
    while let Some(row) = rows
        .next()
        .map_err(|e| BackendError::Query(e.to_string()))?
    {
        for (i, col_buf) in buf.iter_mut().enumerate().take(n_cols) {
            let v: Value = row.get(i).map_err(|e| BackendError::Query(e.to_string()))?;
            col_buf.push(v);
        }
    }

    let mut fields = Vec::with_capacity(n_cols);
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(n_cols);
    for (i, (name, decl)) in column_meta.iter().enumerate() {
        let observed = sqlite_dominant_type(&buf[i]);
        let dtype = observed.unwrap_or_else(|| decltype_to_arrow(decl));
        fields.push(Field::new(name, dtype.clone(), true));
        columns.push(build_arrow_column(&dtype, &buf[i])?);
    }
    let schema = ArrowSchema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns)
        .map_err(|e| BackendError::Other(format!("arrow batch build: {e}")))
}

fn decltype_to_arrow(decl: &str) -> DataType {
    match decl {
        "INTEGER" | "BIGINT" | "INT" | "SMALLINT" | "BOOLEAN" | "BOOL" => DataType::Int64,
        "REAL" | "DOUBLE" | "FLOAT" | "NUMERIC" | "DECIMAL" => DataType::Float64,
        "BLOB" | "BYTEA" => DataType::Binary,
        _ => DataType::Utf8,
    }
}

/// The Arrow type implied by the actual values observed for a column,
/// or None if every value is NULL (caller falls back to the decltype
/// hint). Mixed types fall back to Utf8.
fn sqlite_dominant_type(vals: &[rusqlite::types::Value]) -> Option<DataType> {
    use rusqlite::types::Value;
    let mut seen: Option<DataType> = None;
    for v in vals {
        let t = match v {
            Value::Null => continue,
            Value::Integer(_) => DataType::Int64,
            Value::Real(_) => DataType::Float64,
            Value::Text(_) => DataType::Utf8,
            Value::Blob(_) => DataType::Binary,
        };
        match &seen {
            None => seen = Some(t),
            Some(prev) if *prev == t => {}
            // Mixed types (e.g. a column with both Integer and Text in
            // different rows) — coerce all to Utf8.
            Some(_) => seen = Some(DataType::Utf8),
        }
    }
    seen
}

fn build_arrow_column(
    dtype: &DataType,
    vals: &[rusqlite::types::Value],
) -> Result<Arc<dyn Array>, BackendError> {
    use rusqlite::types::Value;
    match dtype {
        DataType::Int64 => {
            let v: Vec<Option<i64>> = vals
                .iter()
                .map(|x| match x {
                    Value::Null => None,
                    Value::Integer(i) => Some(*i),
                    Value::Real(f) => Some(*f as i64),
                    Value::Text(t) => t.parse().ok(),
                    Value::Blob(_) => None,
                })
                .collect();
            Ok(Arc::new(Int64Array::from_iter(v)))
        }
        DataType::Float64 => {
            let v: Vec<Option<f64>> = vals
                .iter()
                .map(|x| match x {
                    Value::Null => None,
                    Value::Real(f) => Some(*f),
                    Value::Integer(i) => Some(*i as f64),
                    Value::Text(t) => t.parse().ok(),
                    Value::Blob(_) => None,
                })
                .collect();
            Ok(Arc::new(Float64Array::from_iter(v)))
        }
        DataType::Binary => {
            let v: Vec<Option<Vec<u8>>> = vals
                .iter()
                .map(|x| match x {
                    Value::Null => None,
                    Value::Blob(b) => Some(b.clone()),
                    Value::Text(t) => Some(t.clone().into_bytes()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(BinaryArray::from_iter(v)))
        }
        DataType::Utf8 => {
            let v: Vec<Option<String>> = vals
                .iter()
                .map(|x| match x {
                    Value::Null => None,
                    Value::Text(t) => Some(t.clone()),
                    Value::Integer(i) => Some(i.to_string()),
                    Value::Real(f) => Some(f.to_string()),
                    Value::Blob(_) => None,
                })
                .collect();
            Ok(Arc::new(StringArray::from_iter(v)))
        }
        other => Err(BackendError::Other(format!(
            "SQLite read_arrow_stream: unsupported Arrow type {other:?}"
        ))),
    }
}

/// Insert one Arrow RecordBatch into a SQLite table, row-by-row. The
/// table is expected to exist with column names matching the batch's
/// field names. A new prepared statement is built once per batch and
/// reused across rows. Conversion mirrors `sqlite_query_to_record_batch`
/// in reverse: each Arrow column is read by data type and bound as
/// the matching rusqlite native type.
fn insert_record_batch(
    conn: &SqliteConn,
    target: &TargetTable,
    batch: &RecordBatch,
) -> Result<u64, BackendError> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let schema = batch.schema();
    let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let placeholders = (1..=cols.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let table_ref = format!("\"{}\"", target.name.replace('"', "\"\""));
    let sql = format!(
        "INSERT INTO {table} ({cols}) VALUES ({values})",
        table = table_ref,
        cols = cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", "),
        values = placeholders,
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| BackendError::Query(e.to_string()))?;

    let n_cols = batch.num_columns();
    let n_rows = batch.num_rows();
    for r in 0..n_rows {
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(n_cols);
        for c in 0..n_cols {
            let col = batch.column(c);
            // Each branch inserts the appropriate Rust value (or NULL).
            params.push(arrow_value_to_sqlite_param(col.as_ref(), r)?);
        }
        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        stmt.execute(rusqlite::params_from_iter(param_refs.iter()))
            .map_err(|e| BackendError::Query(e.to_string()))?;
    }
    Ok(n_rows as u64)
}

fn arrow_value_to_sqlite_param(
    col: &dyn Array,
    row: usize,
) -> Result<Box<dyn rusqlite::ToSql>, BackendError> {
    if col.is_null(row) {
        return Ok(Box::new(rusqlite::types::Null));
    }
    match col.data_type() {
        DataType::Int64 => {
            let a = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                BackendError::Other("Arrow column type mismatch: expected Int64Array".into())
            })?;
            Ok(Box::new(a.value(row)))
        }
        DataType::Float64 => {
            let a = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                BackendError::Other("Arrow column type mismatch: expected Float64Array".into())
            })?;
            Ok(Box::new(a.value(row)))
        }
        DataType::Boolean => {
            let a = col.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                BackendError::Other("Arrow column type mismatch: expected BooleanArray".into())
            })?;
            // SQLite has no Boolean type; store as INTEGER 0/1.
            Ok(Box::new(a.value(row) as i64))
        }
        DataType::Utf8 => {
            let a = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                BackendError::Other("Arrow column type mismatch: expected StringArray".into())
            })?;
            Ok(Box::new(a.value(row).to_string()))
        }
        DataType::Binary => {
            let a = col.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                BackendError::Other("Arrow column type mismatch: expected BinaryArray".into())
            })?;
            Ok(Box::new(a.value(row).to_vec()))
        }
        other => Err(BackendError::Other(format!(
            "SQLite write_arrow_stream: unsupported Arrow type {other:?} (Phase 32a covers \
             Int64/Float64/Boolean/Utf8/Binary; richer types land in 32b+)"
        ))),
    }
}

#[async_trait]
impl Backend for SQLiteBackend {
    fn dialect(&self) -> Dialect {
        Dialect::SQLite
    }

    fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            host: "sqlite".into(),
            port: 0,
            dbname: self.location.clone(),
            user: "local".into(),
        }
    }

    fn dsn(&self) -> Option<String> {
        Some(format!("sqlite://{}", self.location))
    }

    async fn ping(&self) -> Result<(), BackendError> {
        self.with_conn_blocking(|c| {
            c.execute_batch("SELECT 1")
                .map_err(|e| BackendError::Query(e.to_string()))
        })
        .await
    }

    async fn execute(&self, statement: &str) -> Result<u64, BackendError> {
        let stmt = statement.to_string();
        self.with_conn_blocking(move |c| {
            // execute_batch handles multi-statement SQL but doesn't
            // return an affected-row count. Approximate with 0; users
            // who need the number can run a SELECT count(*) follow-up.
            c.execute_batch(&stmt)
                .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(0)
        })
        .await
    }

    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        let q = query.to_string();
        let batch = self
            .with_conn_blocking(move |c| sqlite_query_to_record_batch(c, &q))
            .await?;
        let s = stream::once(async move { Ok::<_, BackendError>(batch) });
        Ok(Box::pin(s))
    }

    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError> {
        use futures_util::StreamExt;

        require_main_schema(&target.schema)?;
        let mut s = stream;
        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(b) = s.next().await {
            batches.push(b?);
        }
        let target = target.clone();
        self.with_conn_blocking(move |c| {
            let table_ref = format!("\"{}\"", target.name.replace('"', "\"\""));
            if mode == WriteMode::Truncate {
                c.execute_batch(&format!("DELETE FROM {table_ref}"))
                    .map_err(|e| BackendError::Query(e.to_string()))?;
            }
            // Wrap inserts in a single transaction so the per-row
            // INSERTs aren't flushed individually — orders of magnitude
            // faster on SQLite.
            c.execute_batch("BEGIN")
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let mut total: u64 = 0;
            for batch in &batches {
                total += insert_record_batch(c, &target, batch)?;
            }
            c.execute_batch("COMMIT")
                .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(total)
        })
        .await
    }

    async fn run_append(
        &self,
        spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        incremental_column: Option<&str>,
        last_value_literal: Option<&str>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        if source_backend.is_some() {
            return Err(BackendError::Other(
                "SQLite cross-backend run_append goes through the Arrow streaming bridge \
                 (cross_backend_arrow_sync); same-backend only here"
                    .into(),
            ));
        }
        require_main_schema(&spec.schema)?;
        let watermark = incremental_column.map(|c| WatermarkConfig {
            column: c.to_string(),
            last_value_literal: last_value_literal.map(|s| s.to_string()),
        });
        let filtered_source = wrap_with_watermark_filter(source_query, watermark.as_ref());
        let plan = plan_same_db_append(spec, &filtered_source);
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let sql = sqlite_substitute(&plan.sql, &batch_id);

        self.ensure_meta_schema().await?;
        self.record_run_start(
            run_id,
            pipeline_name,
            &spec.schema,
            &spec.name,
            "append",
            "same_db",
        )
        .await?;

        let inserted_result: Result<u64, BackendError> = self
            .with_conn_blocking(move |c| {
                if dry_run {
                    c.execute_batch("BEGIN")
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    let n = c
                        .execute(&sql, [])
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    c.execute_batch("ROLLBACK")
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    Ok::<u64, BackendError>(n as u64)
                } else {
                    let n = c
                        .execute(&sql, [])
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    Ok(n as u64)
                }
            })
            .await;

        match &inserted_result {
            Ok(n) => {
                self.record_run_success(run_id, *n as i64).await?;
                if !dry_run && let Some(wc) = watermark.as_ref() {
                    self.advance_watermark(spec, batch_id, pipeline_name, wc)
                        .await?;
                }
            }
            Err(e) => {
                let _ = self.record_run_failure(run_id, &e.to_string()).await;
            }
        }
        let inserted = inserted_result?;

        Ok(StrategyRunResult {
            run_id: run_id.to_string(),
            rows_inserted: inserted as i64,
            rows_updated: None,
            rows_unchanged: None,
            rows_closed: None,
            status: if dry_run { "dry_run" } else { "success" }.into(),
            path: "same_db".into(),
        })
    }

    async fn run_truncate(
        &self,
        spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        if source_backend.is_some() {
            return Err(BackendError::Other(
                "SQLite cross-backend run_truncate goes through the Arrow bridge".into(),
            ));
        }
        require_main_schema(&spec.schema)?;
        let plan = plan_truncate_replace(spec, source_query);
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let stmts: Vec<String> = plan
            .statements
            .iter()
            .map(|s| sqlite_substitute(s, &batch_id))
            .collect();

        self.ensure_meta_schema().await?;
        self.record_run_start(
            run_id,
            pipeline_name,
            &spec.schema,
            &spec.name,
            "truncate",
            "same_db",
        )
        .await?;

        let inserted_result: Result<u64, BackendError> = self
            .with_conn_blocking(move |c| {
                c.execute_batch("BEGIN")
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                let mut last_n: u64 = 0;
                for s in &stmts {
                    let n = c
                        .execute(s, [])
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    last_n = n as u64;
                }
                if dry_run {
                    c.execute_batch("ROLLBACK")
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                } else {
                    c.execute_batch("COMMIT")
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                }
                Ok::<u64, BackendError>(last_n)
            })
            .await;

        match &inserted_result {
            Ok(n) => self.record_run_success(run_id, *n as i64).await?,
            Err(e) => {
                let _ = self.record_run_failure(run_id, &e.to_string()).await;
            }
        }
        let inserted = inserted_result?;

        Ok(StrategyRunResult {
            run_id: run_id.to_string(),
            rows_inserted: inserted as i64,
            rows_updated: None,
            rows_unchanged: None,
            rows_closed: None,
            status: if dry_run { "dry_run" } else { "success" }.into(),
            path: "same_db".into(),
        })
    }

    async fn run_merge(
        &self,
        spec: &TableSpec,
        source_query: &str,
        keys: &[String],
        update_columns: &[String],
        pipeline_name: &str,
        mode_label: &str,
        source_backend: Option<&dyn Backend>,
        delete_handling: Option<DeleteHandling>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        if source_backend.is_some() {
            return Err(BackendError::Other(
                "SQLite cross-backend run_merge goes through the Arrow bridge".into(),
            ));
        }
        require_main_schema(&spec.schema)?;
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let merge_sql = sqlite_merge_sql(spec, source_query, keys, update_columns, &batch_id);
        let insert_count_sql = format!(
            "SELECT count(*) FROM ({src}) src_count \
             WHERE ({key_tuple}) NOT IN (SELECT {key_tuple} FROM {schema}.{table})",
            src = source_query,
            key_tuple = keys.join(", "),
            schema = if spec.schema.is_empty() {
                "main"
            } else {
                &spec.schema
            },
            table = spec.name,
        );
        let delete_sql = if matches!(delete_handling, Some(DeleteHandling::Hard)) {
            Some(build_hard_delete_sql(
                if spec.schema.is_empty() {
                    "main"
                } else {
                    &spec.schema
                },
                &spec.name,
                keys,
                source_query,
            ))
        } else {
            None
        };

        self.ensure_meta_schema().await?;
        self.record_run_start(
            run_id,
            pipeline_name,
            &spec.schema,
            &spec.name,
            mode_label,
            "same_db",
        )
        .await?;

        let merge_result: Result<(i64, i64), BackendError> = self
            .with_conn_blocking(move |c| {
                c.execute_batch("BEGIN")
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                let inserts: i64 = {
                    let mut stmt = c
                        .prepare(&insert_count_sql)
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    stmt.query_row([], |r| r.get(0))
                        .map_err(|e| BackendError::Query(e.to_string()))?
                };
                let total = c
                    .execute(&merge_sql, [])
                    .map_err(|e| BackendError::Query(e.to_string()))?
                    as i64;
                if let Some(dsql) = &delete_sql {
                    c.execute(dsql, [])
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                }
                if dry_run {
                    c.execute_batch("ROLLBACK")
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                } else {
                    c.execute_batch("COMMIT")
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                }
                let updated = (total - inserts).max(0);
                Ok((inserts, updated))
            })
            .await;

        match &merge_result {
            Ok((inserted, updated)) => {
                self.record_run_success_merge(run_id, *inserted, *updated, 0)
                    .await?
            }
            Err(e) => {
                let _ = self.record_run_failure(run_id, &e.to_string()).await;
            }
        }
        let (inserted, updated) = merge_result?;

        Ok(StrategyRunResult {
            run_id: run_id.to_string(),
            rows_inserted: inserted,
            rows_updated: Some(updated),
            rows_unchanged: Some(0),
            rows_closed: None,
            status: if dry_run { "dry_run" } else { "success" }.into(),
            path: "same_db".into(),
        })
    }

    async fn run_scd2(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _keys: &[String],
        _compare_columns: &[String],
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _delete_handling: Option<DeleteHandling>,
        _event_timestamp_column: Option<&str>,
        _ttl_seconds: Option<i64>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "SQLite run_scd2: not implemented in Phase 32a (lands in 32d)".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_in_memory_ping() {
        let b = SQLiteBackend::open(":memory:").unwrap();
        assert_eq!(b.dialect(), Dialect::SQLite);
        assert_eq!(b.dsn().as_deref(), Some("sqlite://:memory:"));
        b.ping().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_in_memory_execute_creates_table() {
        let b = SQLiteBackend::open(":memory:").unwrap();
        b.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        b.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_arrow_round_trip_in_memory() {
        let b = SQLiteBackend::open(":memory:").unwrap();
        b.execute("CREATE TABLE src (id INTEGER, name TEXT, score REAL, payload BLOB)")
            .await
            .unwrap();
        b.execute(
            "INSERT INTO src VALUES \
             (1, 'a', 1.5, x'01ff'), \
             (2, NULL, 2.5, x'02fe'), \
             (3, 'c', NULL, NULL)",
        )
        .await
        .unwrap();
        b.execute("CREATE TABLE dst (id INTEGER, name TEXT, score REAL, payload BLOB)")
            .await
            .unwrap();

        let s = b
            .read_arrow_stream("SELECT id, name, score, payload FROM src ORDER BY id")
            .await
            .unwrap();
        let target = TargetTable {
            schema: "main".into(),
            name: "dst".into(),
        };
        let written = b
            .write_arrow_stream(&target, s, WriteMode::Append)
            .await
            .unwrap();
        assert_eq!(written, 3);

        // Read it back from `dst` and confirm row-count + a value spot
        // check.
        use futures_util::TryStreamExt;
        let s2 = b
            .read_arrow_stream("SELECT id, name, score FROM dst ORDER BY id")
            .await
            .unwrap();
        let batches: Vec<_> = s2.try_collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 3);
        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(2), 3);
        let name_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "a");
        assert!(name_col.is_null(1));
        let score_col = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(score_col.value(0), 1.5);
        assert!(score_col.is_null(2));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_truncate_replaces_existing() {
        let b = SQLiteBackend::open(":memory:").unwrap();
        b.execute("CREATE TABLE src (id INTEGER, name TEXT)")
            .await
            .unwrap();
        b.execute("INSERT INTO src VALUES (1, 'new')")
            .await
            .unwrap();
        b.execute("CREATE TABLE dst (id INTEGER, name TEXT)")
            .await
            .unwrap();
        b.execute("INSERT INTO dst VALUES (99, 'old')")
            .await
            .unwrap();

        let s = b
            .read_arrow_stream("SELECT id, name FROM src")
            .await
            .unwrap();
        let target = TargetTable {
            schema: "main".into(),
            name: "dst".into(),
        };
        let n = b
            .write_arrow_stream(&target, s, WriteMode::Truncate)
            .await
            .unwrap();
        assert_eq!(n, 1);

        use futures_util::TryStreamExt;
        let s2 = b
            .read_arrow_stream("SELECT count(*) AS n, max(id) AS max_id FROM dst")
            .await
            .unwrap();
        let batches: Vec<_> = s2.try_collect().await.unwrap();
        let n_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let max_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(n_col.value(0), 1);
        assert_eq!(max_col.value(0), 1, "old row 99 was truncated");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_rejects_non_main_schema_in_write() {
        let b = SQLiteBackend::open(":memory:").unwrap();
        b.execute("CREATE TABLE foo (id INTEGER)").await.unwrap();
        let s = b.read_arrow_stream("SELECT 1 AS id").await.unwrap();
        let target = TargetTable {
            schema: "wh".into(),
            name: "foo".into(),
        };
        let err = b
            .write_arrow_stream(&target, s, WriteMode::Append)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("does not yet support schema"),
            "expected helpful error, got: {err}"
        );
    }

    // --- Phase 32b: run_append + run_truncate + run_history --------------

    use crate::strategy::append::augment_with_metadata;
    use crate::types::{ColumnSpec, ColumnType, TableSpec};
    use std::sync::Arc as StdArc;

    fn sqlite_event_log_spec() -> TableSpec {
        augment_with_metadata(&TableSpec {
            schema: "main".into(),
            name: "event_log".into(),
            columns: vec![
                ColumnSpec {
                    name: "event_id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "name".into(),
                    ty: ColumnType::Text,
                    nullable: true,
                    primary_key: false,
                },
            ],
            unique_constraints: Vec::new(),
            fingerprint: String::new(),
        })
    }

    async fn make_backend_with_event_log() -> StdArc<dyn Backend> {
        let b: StdArc<dyn Backend> = StdArc::new(SQLiteBackend::open(":memory:").unwrap());
        b.execute("CREATE TABLE src_events (event_id INTEGER, name TEXT)")
            .await
            .unwrap();
        b.execute(
            "CREATE TABLE event_log (\
              event_id INTEGER, name TEXT, _loaded_at TEXT, _batch_id TEXT\
            )",
        )
        .await
        .unwrap();
        b
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_append_inserts_rows_and_records_history() {
        let b = make_backend_with_event_log().await;
        b.execute("INSERT INTO src_events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .unwrap();
        let target = sqlite_event_log_spec();
        let result = b
            .run_append(
                &target,
                "SELECT event_id, name FROM src_events",
                "sqlite_append_test",
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.rows_inserted, 3);
        assert_eq!(result.path, "same_db");
        assert_eq!(result.status, "success");

        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream(
                "SELECT count(*), \
                        sum(CASE WHEN status='success' THEN 1 ELSE 0 END), \
                        max(rows_inserted) \
                 FROM ematix_flow_run_history \
                 WHERE pipeline_name='sqlite_append_test'",
            )
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let success = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let rows_inserted = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 1);
        assert_eq!(success, 1);
        assert_eq!(rows_inserted, 3);

        // Target row count.
        let s = b
            .read_arrow_stream("SELECT count(*) FROM event_log")
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_truncate_replaces_target() {
        let b = make_backend_with_event_log().await;
        b.execute(
            "INSERT INTO event_log VALUES \
             (99, 'old', '2024-01-01T00:00:00Z', '00000000-0000-0000-0000-000000000000')",
        )
        .await
        .unwrap();
        b.execute("INSERT INTO src_events VALUES (1, 'a')")
            .await
            .unwrap();
        let target = sqlite_event_log_spec();
        let r = b
            .run_truncate(
                &target,
                "SELECT event_id, name FROM src_events",
                "sqlite_truncate_test",
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(r.rows_inserted, 1);
        assert_eq!(r.status, "success");

        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream("SELECT count(*), max(event_id) FROM event_log")
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let max_id = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 1, "old row 99 was truncated");
        assert_eq!(max_id, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_append_records_failure_when_target_missing() {
        let b: StdArc<dyn Backend> = StdArc::new(SQLiteBackend::open(":memory:").unwrap());
        b.execute("CREATE TABLE src_events (event_id INTEGER)")
            .await
            .unwrap();
        b.execute("INSERT INTO src_events VALUES (1)")
            .await
            .unwrap();
        // event_log table missing on purpose.
        let target = augment_with_metadata(&TableSpec {
            schema: "main".into(),
            name: "missing_table".into(),
            columns: vec![ColumnSpec {
                name: "event_id".into(),
                ty: ColumnType::BigInt,
                nullable: false,
                primary_key: true,
            }],
            unique_constraints: Vec::new(),
            fingerprint: String::new(),
        });
        let result = b
            .run_append(
                &target,
                "SELECT event_id FROM src_events",
                "sqlite_failure_test",
                None,
                None,
                None,
                false,
            )
            .await;
        assert!(result.is_err());

        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream(
                "SELECT count(*), sum(CASE WHEN status='failed' THEN 1 ELSE 0 END) \
                 FROM ematix_flow_run_history \
                 WHERE pipeline_name='sqlite_failure_test'",
            )
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let failed = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 1);
        assert_eq!(failed, 1, "failure recorded");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_append_incremental_filters_and_advances_watermark() {
        let b: StdArc<dyn Backend> = StdArc::new(SQLiteBackend::open(":memory:").unwrap());
        b.execute("CREATE TABLE src_events (event_id INTEGER, ts INTEGER)")
            .await
            .unwrap();
        b.execute(
            "CREATE TABLE event_log (\
              event_id INTEGER, ts INTEGER, _loaded_at TEXT, _batch_id TEXT\
            )",
        )
        .await
        .unwrap();
        b.execute("INSERT INTO src_events VALUES (1, 100), (2, 200), (3, 300)")
            .await
            .unwrap();
        let target = augment_with_metadata(&TableSpec {
            schema: "main".into(),
            name: "event_log".into(),
            columns: vec![
                ColumnSpec {
                    name: "event_id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "ts".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: false,
                },
            ],
            unique_constraints: Vec::new(),
            fingerprint: String::new(),
        });

        // Cold start.
        let r1 = b
            .run_append(
                &target,
                "SELECT event_id, ts FROM src_events",
                "sqlite_incr",
                None,
                Some("ts"),
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(r1.rows_inserted, 3);

        // Watermark advanced to 300.
        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream(
                "SELECT last_value FROM ematix_flow_watermarks WHERE pipeline_name='sqlite_incr'",
            )
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let last = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_eq!(last, "300");

        // Hot start with literal: only ts > 300 passes.
        b.execute("INSERT INTO src_events VALUES (4, 400), (5, 500), (6, 150)")
            .await
            .unwrap();
        let r2 = b
            .run_append(
                &target,
                "SELECT event_id, ts FROM src_events",
                "sqlite_incr",
                None,
                Some("ts"),
                Some("300"),
                false,
            )
            .await
            .unwrap();
        assert_eq!(r2.rows_inserted, 2);

        let s = b
            .read_arrow_stream("SELECT count(*) FROM event_log")
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 5);
    }

    // --- Phase 32c: run_merge ---------------------------------------------

    fn sqlite_merge_target_spec() -> TableSpec {
        augment_with_metadata(&TableSpec {
            schema: "main".into(),
            name: "event_log".into(),
            columns: vec![
                ColumnSpec {
                    name: "event_id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "name".into(),
                    ty: ColumnType::Text,
                    nullable: true,
                    primary_key: false,
                },
            ],
            unique_constraints: Vec::new(),
            fingerprint: String::new(),
        })
    }

    async fn sqlite_merge_setup() -> StdArc<dyn Backend> {
        let b: StdArc<dyn Backend> = StdArc::new(SQLiteBackend::open(":memory:").unwrap());
        b.execute("CREATE TABLE src_events (event_id INTEGER, name TEXT)")
            .await
            .unwrap();
        b.execute(
            "CREATE TABLE event_log (\
              event_id INTEGER PRIMARY KEY, name TEXT, _loaded_at TEXT, _batch_id TEXT\
            )",
        )
        .await
        .unwrap();
        b
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_merge_splits_inserts_from_updates() {
        let b = sqlite_merge_setup().await;
        let target = sqlite_merge_target_spec();
        b.execute("INSERT INTO src_events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .unwrap();

        let r1 = b
            .run_merge(
                &target,
                "SELECT event_id, name FROM src_events",
                &["event_id".into()],
                &["name".into()],
                "sqlite_merge_split",
                "merge",
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(r1.rows_inserted, 3);
        assert_eq!(r1.rows_updated, Some(0));

        b.execute("DELETE FROM src_events").await.unwrap();
        b.execute("INSERT INTO src_events VALUES (1, 'a-new'), (2, 'b'), (4, 'd')")
            .await
            .unwrap();
        let r2 = b
            .run_merge(
                &target,
                "SELECT event_id, name FROM src_events",
                &["event_id".into()],
                &["name".into()],
                "sqlite_merge_split",
                "merge",
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(r2.rows_inserted, 1, "only event_id=4 is new");
        assert_eq!(r2.rows_updated, Some(2));

        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream("SELECT count(*) FROM event_log")
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 4, "target = {{1,2,3,4}}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_merge_handle_deletes_removes_missing_keys() {
        let b = sqlite_merge_setup().await;
        let target = sqlite_merge_target_spec();
        b.execute("INSERT INTO src_events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .unwrap();
        b.run_merge(
            &target,
            "SELECT event_id, name FROM src_events",
            &["event_id".into()],
            &["name".into()],
            "sqlite_delete",
            "merge",
            None,
            None,
            false,
        )
        .await
        .unwrap();

        b.execute("DELETE FROM src_events").await.unwrap();
        b.execute("INSERT INTO src_events VALUES (1, 'a'), (2, 'b-new')")
            .await
            .unwrap();
        b.run_merge(
            &target,
            "SELECT event_id, name FROM src_events",
            &["event_id".into()],
            &["name".into()],
            "sqlite_delete",
            "merge",
            None,
            Some(DeleteHandling::Hard),
            false,
        )
        .await
        .unwrap();

        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream(
                "SELECT count(*), \
                        sum(CASE WHEN event_id=3 THEN 1 ELSE 0 END) \
                 FROM event_log",
            )
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let row3 = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 2);
        assert_eq!(row3, 0, "row 3 hard-deleted");
    }
}
