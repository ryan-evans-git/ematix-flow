//! Phase 33a: MySQL backend skeleton.
//!
//! Implements the connection-level + Arrow IO surface of the `Backend`
//! trait. Strategy executors are stubbed for 33a and land in 33b–d.
//!
//! Threading model: `mysql_async` is natively async (Tokio), so we hold
//! a `mysql_async::Pool` directly — no `spawn_blocking` indirection
//! (unlike DuckDB/SQLite which wrap sync handles).
//!
//! ## Schema model
//! MySQL uses *databases* as schemas. We use `target.schema` as the
//! database name and address tables with backtick-quoted
//! `` `schema`.`table` ``. The connection's default schema (from the
//! URL) is unused at the planner level — every statement is qualified.
//!
//! ## Type model
//! MySQL → Arrow (read path):
//!   - TINYINT(1)            → Boolean
//!   - TINYINT/SMALLINT/INT  → Int64 (widened for uniformity)
//!   - BIGINT                → Int64
//!   - FLOAT/DOUBLE/DECIMAL  → Float64 (DECIMAL widened lossily)
//!   - CHAR/VARCHAR/TEXT/JSON/UUID-as-text → Utf8
//!   - BINARY/VARBINARY/BLOB → Binary
//!   - DATE/DATETIME/TIMESTAMP → Timestamp(Microsecond, None)
//!
//! Arrow → MySQL (write path) inverts the same map.
//!
//! See `docs/MULTI_BACKEND_PLAN.md` Phase 33 for the full design.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use async_trait::async_trait;
use futures_util::stream;
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, Pool};

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

/// MySQL-backed implementation of `Backend`.
///
/// `MySQLBackend::open("mysql://user:pass@host:3306/db")` constructs
/// a pool. The pool stays open for the lifetime of the backend.
pub struct MySQLBackend {
    pool: Pool,
    dsn: String,
    info: ConnectionInfo,
}

impl MySQLBackend {
    /// Construct a MySQL backend from a `mysql://` URL.
    pub fn open(url: impl Into<String>) -> Result<Self, BackendError> {
        let dsn = url.into();
        let opts = Opts::from_url(&dsn)
            .map_err(|e| BackendError::Connection(format!("invalid mysql url: {e}")))?;
        let info = ConnectionInfo {
            host: opts.ip_or_hostname().to_string(),
            port: opts.tcp_port(),
            dbname: opts.db_name().unwrap_or("").to_string(),
            user: opts.user().unwrap_or("").to_string(),
        };
        let pool = Pool::new(opts);
        Ok(Self { pool, dsn, info })
    }

    /// Borrow the underlying `mysql_async::Pool`. Used by the strategy
    /// executors in 33b+.
    #[allow(dead_code)]
    pub(crate) fn pool(&self) -> &Pool {
        &self.pool
    }
}

fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn qualified(schema: &str, table: &str) -> String {
    if schema.is_empty() {
        quote_ident(table)
    } else {
        format!("{}.{}", quote_ident(schema), quote_ident(table))
    }
}

/// Translate a MySQL column type into the Arrow data type we'll
/// materialize. `column_length` is required to distinguish boolean
/// (TINYINT(1)) from a true tiny integer.
fn mysql_column_to_arrow(col: &mysql_async::Column) -> DataType {
    use mysql_async::consts::ColumnType as Ct;
    match col.column_type() {
        Ct::MYSQL_TYPE_TINY => {
            // TINYINT(1) is the conventional MySQL boolean. The
            // protocol exposes the *display width* via `column_length`;
            // 1 → boolean, anything else → small integer (widened to
            // i64 for type-uniformity across the framework).
            if col.column_length() == 1 {
                DataType::Boolean
            } else {
                DataType::Int64
            }
        }
        Ct::MYSQL_TYPE_SHORT
        | Ct::MYSQL_TYPE_INT24
        | Ct::MYSQL_TYPE_LONG
        | Ct::MYSQL_TYPE_LONGLONG
        | Ct::MYSQL_TYPE_YEAR => DataType::Int64,
        Ct::MYSQL_TYPE_FLOAT
        | Ct::MYSQL_TYPE_DOUBLE
        | Ct::MYSQL_TYPE_DECIMAL
        | Ct::MYSQL_TYPE_NEWDECIMAL => DataType::Float64,
        Ct::MYSQL_TYPE_TIMESTAMP
        | Ct::MYSQL_TYPE_DATETIME
        | Ct::MYSQL_TYPE_TIMESTAMP2
        | Ct::MYSQL_TYPE_DATETIME2
        | Ct::MYSQL_TYPE_DATE
        | Ct::MYSQL_TYPE_NEWDATE => DataType::Timestamp(TimeUnit::Microsecond, None),
        Ct::MYSQL_TYPE_TINY_BLOB
        | Ct::MYSQL_TYPE_MEDIUM_BLOB
        | Ct::MYSQL_TYPE_LONG_BLOB
        | Ct::MYSQL_TYPE_BLOB
            // BINARY / VARBINARY share blob types but with binary charset.
            if col.character_set() == 63 =>
        {
            DataType::Binary
        }
        // String-shaped (VARCHAR/TEXT/CHAR/JSON/SET/ENUM/STRING) and any
        // text-charset BLOB. Anything we didn't classify above falls
        // through to Utf8 — safer than a hard error for forward compat.
        _ => DataType::Utf8,
    }
}

fn mysql_value_is_null(v: &mysql_async::Value) -> bool {
    matches!(v, mysql_async::Value::NULL)
}

fn mysql_to_i64(v: &mysql_async::Value) -> Result<i64, BackendError> {
    use mysql_async::Value::*;
    match v {
        Int(i) => Ok(*i),
        UInt(u) => Ok(*u as i64),
        Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| BackendError::TypeMapping("mysql i64: non-numeric bytes".into())),
        other => Err(BackendError::TypeMapping(format!(
            "mysql i64: unexpected variant {other:?}"
        ))),
    }
}

fn mysql_to_f64(v: &mysql_async::Value) -> Result<f64, BackendError> {
    use mysql_async::Value::*;
    match v {
        Float(f) => Ok(*f as f64),
        Double(f) => Ok(*f),
        Int(i) => Ok(*i as f64),
        UInt(u) => Ok(*u as f64),
        Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| BackendError::TypeMapping("mysql f64: non-numeric bytes".into())),
        other => Err(BackendError::TypeMapping(format!(
            "mysql f64: unexpected variant {other:?}"
        ))),
    }
}

fn mysql_to_bool(v: &mysql_async::Value) -> Result<bool, BackendError> {
    use mysql_async::Value::*;
    match v {
        Int(i) => Ok(*i != 0),
        UInt(u) => Ok(*u != 0),
        Bytes(b) => Ok(b != b"0" && !b.is_empty()),
        other => Err(BackendError::TypeMapping(format!(
            "mysql bool: unexpected variant {other:?}"
        ))),
    }
}

fn mysql_to_string(v: &mysql_async::Value) -> Result<String, BackendError> {
    use mysql_async::Value::*;
    match v {
        Bytes(b) => String::from_utf8(b.clone())
            .map_err(|e| BackendError::TypeMapping(format!("mysql utf8: {e}"))),
        Int(i) => Ok(i.to_string()),
        UInt(u) => Ok(u.to_string()),
        Float(f) => Ok(f.to_string()),
        Double(f) => Ok(f.to_string()),
        Date(y, m, d, h, mi, s, us) => Ok(format!(
            "{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{us:06}"
        )),
        other => Err(BackendError::TypeMapping(format!(
            "mysql string: unexpected variant {other:?}"
        ))),
    }
}

fn mysql_to_binary(v: &mysql_async::Value) -> Result<Vec<u8>, BackendError> {
    use mysql_async::Value::*;
    match v {
        Bytes(b) => Ok(b.clone()),
        other => Err(BackendError::TypeMapping(format!(
            "mysql binary: unexpected variant {other:?}"
        ))),
    }
}

/// Convert a MySQL Date/Datetime/Timestamp value to microseconds since
/// the Unix epoch. Treats the value as UTC since MySQL DATETIME is
/// timezone-naive and TIMESTAMP is normalized to UTC by the server.
fn mysql_to_timestamp_us(v: &mysql_async::Value) -> Result<i64, BackendError> {
    use chrono_compat::days_from_civil;
    use mysql_async::Value::*;
    match v {
        Date(y, mo, d, h, mi, s, us) => {
            let days = days_from_civil(*y as i32, *mo as i32, *d as i32);
            let secs = days * 86_400 + (*h as i64) * 3600 + (*mi as i64) * 60 + (*s as i64);
            Ok(secs * 1_000_000 + (*us as i64))
        }
        Bytes(b) => {
            let s = std::str::from_utf8(b)
                .map_err(|e| BackendError::TypeMapping(format!("ts utf8: {e}")))?;
            // Accept both "YYYY-MM-DD HH:MM:SS[.fff]" and the "T"
            // ISO-8601 form. Falls back to error rather than silently
            // returning 0.
            parse_naive_iso8601_us(s)
                .ok_or_else(|| BackendError::TypeMapping(format!("mysql ts: cannot parse {s:?}")))
        }
        other => Err(BackendError::TypeMapping(format!(
            "mysql timestamp: unexpected variant {other:?}"
        ))),
    }
}

/// Minimal ISO-8601-ish parser for "YYYY-MM-DD[ T]HH:MM:SS[.us]". We
/// avoid pulling in chrono just for this — the framework only ever sees
/// MySQL-shaped timestamps here.
fn parse_naive_iso8601_us(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, time) = if let Some(t) = s.find(['T', ' ']) {
        (&s[..t], &s[t + 1..])
    } else {
        return None;
    };
    let mut date_parts = date.split('-');
    let y: i32 = date_parts.next()?.parse().ok()?;
    let mo: i32 = date_parts.next()?.parse().ok()?;
    let d: i32 = date_parts.next()?.parse().ok()?;
    let (hms, frac) = match time.split_once('.') {
        Some((a, b)) => (a, b),
        None => (time, ""),
    };
    let mut t = hms.split(':');
    let h: i64 = t.next()?.parse().ok()?;
    let mi: i64 = t.next()?.parse().ok()?;
    let s: i64 = t.next().unwrap_or("0").parse().ok()?;
    let us: i64 = if frac.is_empty() {
        0
    } else {
        let mut buf = String::with_capacity(6);
        for c in frac.chars().take(6) {
            buf.push(c);
        }
        while buf.len() < 6 {
            buf.push('0');
        }
        buf.parse().ok()?
    };
    let days = chrono_compat::days_from_civil(y, mo, d);
    Some(days * 86_400_000_000 + h * 3_600_000_000 + mi * 60_000_000 + s * 1_000_000 + us)
}

/// Inline implementation of Howard Hinnant's chrono::civil_from_days
/// inverse. Avoids pulling in chrono; only used for MySQL DATETIME →
/// micros-since-epoch conversion.
mod chrono_compat {
    /// Days from 1970-01-01 to the given proleptic Gregorian date.
    pub fn days_from_civil(y: i32, m: i32, d: i32) -> i64 {
        let y = if m <= 2 { y - 1 } else { y };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as i64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) as i64 / 5 + d as i64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era as i64 * 146097 + doe - 719_468
    }
}

fn rows_to_record_batch(
    columns: &[mysql_async::Column],
    rows: &[mysql_async::Row],
) -> Result<RecordBatch, BackendError> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut col_dtypes = Vec::with_capacity(columns.len());
    for col in columns {
        let dt = mysql_column_to_arrow(col);
        fields.push(Field::new(col.name_str().as_ref(), dt.clone(), true));
        col_dtypes.push(dt);
    }
    let schema = Arc::new(ArrowSchema::new(fields));

    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(columns.len());
    let cap = rows.len();
    for (idx, dt) in col_dtypes.iter().enumerate() {
        let array: Arc<dyn Array> = match dt {
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(cap);
                for row in rows {
                    let v = row.as_ref(idx).ok_or_else(|| {
                        BackendError::TypeMapping(format!("missing column {idx}"))
                    })?;
                    if mysql_value_is_null(v) {
                        b.append_null();
                    } else {
                        b.append_value(mysql_to_i64(v)?);
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(cap);
                for row in rows {
                    let v = row.as_ref(idx).ok_or_else(|| {
                        BackendError::TypeMapping(format!("missing column {idx}"))
                    })?;
                    if mysql_value_is_null(v) {
                        b.append_null();
                    } else {
                        b.append_value(mysql_to_f64(v)?);
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(cap);
                for row in rows {
                    let v = row.as_ref(idx).ok_or_else(|| {
                        BackendError::TypeMapping(format!("missing column {idx}"))
                    })?;
                    if mysql_value_is_null(v) {
                        b.append_null();
                    } else {
                        b.append_value(mysql_to_bool(v)?);
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(cap, cap * 16);
                for row in rows {
                    let v = row.as_ref(idx).ok_or_else(|| {
                        BackendError::TypeMapping(format!("missing column {idx}"))
                    })?;
                    if mysql_value_is_null(v) {
                        b.append_null();
                    } else {
                        b.append_value(mysql_to_string(v)?);
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Binary => {
                let mut b = BinaryBuilder::with_capacity(cap, cap * 16);
                for row in rows {
                    let v = row.as_ref(idx).ok_or_else(|| {
                        BackendError::TypeMapping(format!("missing column {idx}"))
                    })?;
                    if mysql_value_is_null(v) {
                        b.append_null();
                    } else {
                        b.append_value(mysql_to_binary(v)?);
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(cap);
                for row in rows {
                    let v = row.as_ref(idx).ok_or_else(|| {
                        BackendError::TypeMapping(format!("missing column {idx}"))
                    })?;
                    if mysql_value_is_null(v) {
                        b.append_null();
                    } else {
                        b.append_value(mysql_to_timestamp_us(v)?);
                    }
                }
                Arc::new(b.finish())
            }
            other => {
                return Err(BackendError::TypeMapping(format!(
                    "MySQL → Arrow: no builder for {other:?}"
                )));
            }
        };
        arrays.push(array);
    }
    RecordBatch::try_new(schema, arrays).map_err(|e| BackendError::TypeMapping(e.to_string()))
}

/// Build the Arrow → MySQL parameters for one row of a record batch.
/// MySQL uses positional `?` placeholders; we bind via
/// `mysql_async::Params::Positional`.
fn arrow_row_to_mysql_params(
    batch: &RecordBatch,
    row_idx: usize,
) -> Result<Vec<mysql_async::Value>, BackendError> {
    use arrow_array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        StringArray, TimestampMicrosecondArray,
    };

    let n = batch.num_columns();
    let mut params = Vec::with_capacity(n);
    let schema = batch.schema();
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        if col.is_null(row_idx) {
            params.push(mysql_async::Value::NULL);
            continue;
        }
        let v = match field.data_type() {
            DataType::Boolean => {
                let a = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                mysql_async::Value::Int(a.value(row_idx) as i64)
            }
            DataType::Int16 => {
                let a = col.as_any().downcast_ref::<Int16Array>().unwrap();
                mysql_async::Value::Int(a.value(row_idx) as i64)
            }
            DataType::Int32 => {
                let a = col.as_any().downcast_ref::<Int32Array>().unwrap();
                mysql_async::Value::Int(a.value(row_idx) as i64)
            }
            DataType::Int64 => {
                let a = col.as_any().downcast_ref::<Int64Array>().unwrap();
                mysql_async::Value::Int(a.value(row_idx))
            }
            DataType::Float32 => {
                let a = col.as_any().downcast_ref::<Float32Array>().unwrap();
                mysql_async::Value::Float(a.value(row_idx))
            }
            DataType::Float64 => {
                let a = col.as_any().downcast_ref::<Float64Array>().unwrap();
                mysql_async::Value::Double(a.value(row_idx))
            }
            DataType::Utf8 => {
                let a = col.as_any().downcast_ref::<StringArray>().unwrap();
                mysql_async::Value::Bytes(a.value(row_idx).as_bytes().to_vec())
            }
            DataType::Binary => {
                let a = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                mysql_async::Value::Bytes(a.value(row_idx).to_vec())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let a = col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let micros = a.value(row_idx);
                let s = format_micros_as_mysql_datetime(micros);
                mysql_async::Value::Bytes(s.into_bytes())
            }
            other => {
                return Err(BackendError::TypeMapping(format!(
                    "Arrow → MySQL: unsupported {other:?} for column {}",
                    field.name()
                )));
            }
        };
        params.push(v);
    }
    Ok(params)
}

/// Format microseconds-since-epoch as MySQL-friendly
/// "YYYY-MM-DD HH:MM:SS.uuuuuu". MySQL parses this for both DATETIME
/// and TIMESTAMP columns.
fn format_micros_as_mysql_datetime(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let us = micros.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let time_secs = secs.rem_euclid(86_400);
    let h = time_secs / 3600;
    let mi = (time_secs % 3600) / 60;
    let s = time_secs % 60;
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}")
}

/// Howard Hinnant's chrono::civil_from_days. Inverse of
/// `chrono_compat::days_from_civil`.
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

#[async_trait]
impl Backend for MySQLBackend {
    fn dialect(&self) -> Dialect {
        Dialect::MySQL
    }

    fn connection_info(&self) -> ConnectionInfo {
        self.info.clone()
    }

    fn dsn(&self) -> Option<String> {
        Some(self.dsn.clone())
    }

    async fn ping(&self) -> Result<(), BackendError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        conn.ping()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn execute(&self, statement: &str) -> Result<u64, BackendError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        // `query_drop` accepts arbitrary SQL (DDL or DML) without a row
        // result. mysql_async exposes `affected_rows()` on the QueryResult
        // returned by `query_iter`, which is one line longer but lets us
        // surface the row count for DML statements like the strategy
        // executors will use in 33b+.
        let result: mysql_async::QueryResult<'_, '_, mysql_async::TextProtocol> = conn
            .query_iter(statement)
            .await
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let affected = result.affected_rows();
        // Drain the result so the connection is reusable.
        result
            .drop_result()
            .await
            .map_err(|e| BackendError::Query(e.to_string()))?;
        Ok(affected)
    }

    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        let mut result = conn
            .query_iter(query)
            .await
            .map_err(|e| BackendError::Query(e.to_string()))?;
        // Capture the column metadata before draining the stream — once
        // collected the result is consumed.
        let columns: Vec<mysql_async::Column> = result.columns_ref().to_vec();
        let rows: Vec<mysql_async::Row> = result
            .collect::<mysql_async::Row>()
            .await
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let batch = rows_to_record_batch(&columns, &rows)?;
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

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        if mode == WriteMode::Truncate {
            conn.query_drop(format!(
                "TRUNCATE TABLE {}",
                qualified(&target.schema, &target.name)
            ))
            .await
            .map_err(|e| BackendError::Query(e.to_string()))?;
        }
        let mut s = stream;
        let mut total: u64 = 0;
        while let Some(batch) = s.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let schema = batch.schema();
            let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            let placeholders = vec!["?"; cols.len()].join(", ");
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                qualified(&target.schema, &target.name),
                cols.iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", "),
                placeholders
            );
            // Bind once per row. mysql_async will internally reuse a
            // prepared statement when given the same SQL repeatedly, so
            // this is cheaper than it looks; richer bulk-load (LOAD DATA
            // LOCAL INFILE) is a future-phase optimization.
            for row_idx in 0..batch.num_rows() {
                let params = arrow_row_to_mysql_params(&batch, row_idx)?;
                conn.exec_drop(&sql, params)
                    .await
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                total += 1;
            }
        }
        Ok(total)
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
            "MySQL run_append lands in Phase 33b".into(),
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
            "MySQL run_truncate lands in Phase 33b".into(),
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
            "MySQL run_merge lands in Phase 33c".into(),
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
            "MySQL run_scd2 lands in Phase 33d".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_escapes_backticks() {
        assert_eq!(quote_ident("foo"), "`foo`");
        assert_eq!(quote_ident("a`b"), "`a``b`");
    }

    #[test]
    fn qualified_omits_empty_schema() {
        assert_eq!(qualified("", "t"), "`t`");
        assert_eq!(qualified("s", "t"), "`s`.`t`");
    }

    #[test]
    fn civil_from_days_round_trips() {
        for &(y, m, d) in &[
            (1970, 1, 1),
            (2000, 2, 29),
            (2024, 12, 31),
            (1999, 7, 4),
            (1, 1, 1),
        ] {
            let days = chrono_compat::days_from_civil(y, m, d);
            let (yy, mm, dd) = civil_from_days(days);
            assert_eq!((yy, mm, dd), (y, m as u32, d as u32));
        }
    }

    #[test]
    fn parse_naive_iso8601_us_handles_t_and_space() {
        let a = parse_naive_iso8601_us("1970-01-01T00:00:00.000000").unwrap();
        let b = parse_naive_iso8601_us("1970-01-01 00:00:00").unwrap();
        assert_eq!(a, 0);
        assert_eq!(b, 0);
    }

    #[test]
    fn parse_naive_iso8601_us_microseconds() {
        let v = parse_naive_iso8601_us("2024-06-15 12:34:56.789012").unwrap();
        // 2024-06-15 12:34:56.789012 UTC == 1718454896789012 micros
        assert_eq!(v, 1_718_455_096_789_012 - 200_000_000);
    }

    #[test]
    fn format_micros_round_trip() {
        let s = format_micros_as_mysql_datetime(0);
        assert_eq!(s, "1970-01-01 00:00:00.000000");
        let v = parse_naive_iso8601_us(&s).unwrap();
        assert_eq!(v, 0);

        let s = format_micros_as_mysql_datetime(1_718_454_896_789_012);
        let v = parse_naive_iso8601_us(&s).unwrap();
        assert_eq!(v, 1_718_454_896_789_012);
    }
}
