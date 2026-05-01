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
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

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
        _spec: &TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _incremental_column: Option<&str>,
        _last_value_literal: Option<&str>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "SQLite run_append: not implemented in Phase 32a (lands in 32b)".into(),
        ))
    }

    async fn run_truncate(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "SQLite run_truncate: not implemented in Phase 32a (lands in 32b)".into(),
        ))
    }

    async fn run_merge(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _keys: &[String],
        _update_columns: &[String],
        _pipeline_name: &str,
        _mode_label: &str,
        _source_backend: Option<&dyn Backend>,
        _delete_handling: Option<DeleteHandling>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "SQLite run_merge: not implemented in Phase 32a (lands in 32c)".into(),
        ))
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
}
