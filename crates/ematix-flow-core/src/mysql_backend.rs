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
use crate::meta::{WatermarkConfig, build_hard_delete_sql, wrap_with_watermark_filter};
use crate::pg::ConnectionInfo;
use crate::strategy::append::{BATCH_ID_COL, LOADED_AT_COL, plan_same_db_append};
use crate::strategy::truncate::plan_truncate_replace;
use crate::types::TableSpec;
use uuid::Uuid;

fn is_metadata_col(name: &str) -> bool {
    name == LOADED_AT_COL || name == BATCH_ID_COL
}

/// MySQL-native merge SQL builder.
///
/// PG uses `WITH … MATERIALIZED` CTEs + `RETURNING (xmax = 0)` to split
/// inserts/updates; DuckDB and SQLite use `INSERT … ON CONFLICT (…) DO
/// UPDATE`. MySQL's equivalent is `INSERT … ON DUPLICATE KEY UPDATE
/// col = VALUES(col)`. The target table must have a PRIMARY KEY or
/// UNIQUE index on the merge keys for the conflict to fire.
///
/// `_loaded_at` is set to `NOW(6)` and `_batch_id` to the framework
/// batch UUID literal, so the target carries provenance for both
/// inserted and updated rows.
fn mysql_merge_sql(
    target: &TableSpec,
    source_query: &str,
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
        select_exprs.push("NOW(6)".into());
        select_exprs.push(format!("'{batch_id}'"));
    }
    // Quote metadata column names — both `_loaded_at` and `_batch_id`
    // start with an underscore but are valid identifiers; user columns
    // are emitted unquoted, mirroring DuckDB / PG.
    let on_dup = if update_columns.is_empty() {
        // No updatable columns? `ON DUPLICATE KEY UPDATE` requires at
        // least one assignment; collapse to a no-op self-assignment on
        // the first key column. (MySQL has no `DO NOTHING` syntax for
        // upserts; this is the conventional workaround.)
        format!(
            "ON DUPLICATE KEY UPDATE {col} = {col}",
            col = user_columns[0]
        )
    } else {
        let mut sets: Vec<String> = update_columns
            .iter()
            .map(|c| format!("{c} = VALUES({c})"))
            .collect();
        if has_metadata {
            sets.push(format!("{LOADED_AT_COL} = VALUES({LOADED_AT_COL})"));
            sets.push(format!("{BATCH_ID_COL} = VALUES({BATCH_ID_COL})"));
        }
        format!("ON DUPLICATE KEY UPDATE {}", sets.join(", "))
    };
    format!(
        "INSERT INTO {qualified} ({insert_cols}) \
         SELECT {select_exprs} FROM ({source}) src_inner \
         {on_dup}",
        qualified = qualified(&target.schema, &target.name),
        insert_cols = insert_cols.join(", "),
        select_exprs = select_exprs.join(", "),
        source = source_query,
    )
}

const META_SCHEMA: &str = "ematix_flow";
const RUN_HISTORY_TABLE: &str = "run_history";
const WATERMARKS_TABLE: &str = "watermarks";

/// SQL DDL that lazy-creates the `ematix_flow` database and the
/// `run_history` + `watermarks` tables on MySQL. Returned as a list so
/// each statement can be issued individually — `mysql_async` doesn't
/// enable multi-statement protocol by default.
///
/// Type choices (vs PG/DuckDB):
///   - `run_id` is `CHAR(36)` (canonical hyphenated UUID text) — MySQL
///     has no native UUID type before 8.4 and we want compat with 8.0+.
///   - Timestamps are `DATETIME(6)` (microsecond precision, no TZ
///     conversion). MySQL's TIMESTAMP type is timezone-converted by
///     the server and limited to 1970–2038, which would drift logs
///     across timezone changes — DATETIME stays stable.
///   - InnoDB explicitly so primary keys, FKs, and transactions all
///     work the same way regardless of server-default storage engine.
fn ensure_meta_schema_sql() -> Vec<String> {
    vec![
        format!("CREATE DATABASE IF NOT EXISTS `{META_SCHEMA}`"),
        format!(
            "CREATE TABLE IF NOT EXISTS `{META_SCHEMA}`.`{RUN_HISTORY_TABLE}` (\
                run_id CHAR(36) NOT NULL PRIMARY KEY, \
                parent_run_id CHAR(36) NULL, \
                pipeline_name VARCHAR(255) NOT NULL, \
                step_name VARCHAR(255) NULL, \
                target_schema VARCHAR(255) NOT NULL, \
                target_table VARCHAR(255) NOT NULL, \
                mode VARCHAR(32) NOT NULL, \
                path VARCHAR(64) NOT NULL, \
                started_at DATETIME(6) NOT NULL, \
                finished_at DATETIME(6) NULL, \
                status VARCHAR(32) NOT NULL, \
                rows_inserted BIGINT NULL, \
                rows_updated BIGINT NULL, \
                rows_unchanged BIGINT NULL, \
                error_message TEXT NULL, \
                metrics_json TEXT NULL\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        ),
        // `last_value` is reserved in MySQL 8 (window function); the
        // identifier is backtick-quoted everywhere it appears.
        format!(
            "CREATE TABLE IF NOT EXISTS `{META_SCHEMA}`.`{WATERMARKS_TABLE}` (\
                pipeline_name VARCHAR(255) NOT NULL PRIMARY KEY, \
                column_name VARCHAR(255) NOT NULL, \
                `last_value` VARCHAR(1024) NOT NULL, \
                updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        ),
    ]
}

/// MySQL-specific SQL substitutions on the PG-shaped strategy planner
/// output. The planners emit:
///   - `now()` — MySQL accepts this but only at second precision; we
///     want microseconds for run-history correlation, so rewrite to
///     `NOW(6)`.
///   - `$1::uuid` — MySQL has no `$N` placeholders inside `query_drop`
///     and no UUID cast; replace with a literal `'<uuid>'` string.
///   - `TRUNCATE TABLE x` — TRUNCATE auto-commits in MySQL, which
///     would break the TruncateReplace transaction semantics; rewrite
///     to `DELETE FROM x` so the empty-then-load pair is one atomic
///     transaction.
fn mysql_substitute(sql: &str, batch_id: &Uuid) -> String {
    let trimmed = sql.trim_start();
    if let Some(rest) = trimmed.strip_prefix("TRUNCATE TABLE ") {
        return format!("DELETE FROM {rest}");
    }
    sql.replace("now()", "NOW(6)")
        .replace("$1::uuid", &format!("'{batch_id}'"))
}

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

    /// Lazy-create `ematix_flow.run_history` + `ematix_flow.watermarks`.
    /// Idempotent: each statement uses `IF NOT EXISTS`.
    async fn ensure_meta_schema(&self) -> Result<(), BackendError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        for stmt in ensure_meta_schema_sql() {
            conn.query_drop(&stmt)
                .await
                .map_err(|e| BackendError::Query(e.to_string()))?;
        }
        Ok(())
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
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        let sql = format!(
            "INSERT INTO `{META_SCHEMA}`.`{RUN_HISTORY_TABLE}` \
             (run_id, pipeline_name, target_schema, target_table, \
              mode, path, started_at, status) \
             VALUES (?, ?, ?, ?, ?, ?, NOW(6), 'running')"
        );
        conn.exec_drop(
            &sql,
            (
                run_id.to_string(),
                pipeline_name.to_string(),
                target_schema.to_string(),
                target_table.to_string(),
                mode.to_string(),
                path.to_string(),
            ),
        )
        .await
        .map_err(|e| BackendError::Query(e.to_string()))?;
        Ok(())
    }

    async fn record_run_success(
        &self,
        run_id: Uuid,
        rows_inserted: i64,
    ) -> Result<(), BackendError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        let sql = format!(
            "UPDATE `{META_SCHEMA}`.`{RUN_HISTORY_TABLE}` \
             SET status='success', rows_inserted=?, finished_at=NOW(6) \
             WHERE run_id=?"
        );
        conn.exec_drop(&sql, (rows_inserted, run_id.to_string()))
            .await
            .map_err(|e| BackendError::Query(e.to_string()))?;
        Ok(())
    }

    #[allow(dead_code)] // wired in 33c
    async fn record_run_success_merge(
        &self,
        run_id: Uuid,
        rows_inserted: i64,
        rows_updated: i64,
        rows_unchanged: i64,
    ) -> Result<(), BackendError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        let sql = format!(
            "UPDATE `{META_SCHEMA}`.`{RUN_HISTORY_TABLE}` \
             SET status='success', rows_inserted=?, rows_updated=?, \
                 rows_unchanged=?, finished_at=NOW(6) \
             WHERE run_id=?"
        );
        conn.exec_drop(
            &sql,
            (
                rows_inserted,
                rows_updated,
                rows_unchanged,
                run_id.to_string(),
            ),
        )
        .await
        .map_err(|e| BackendError::Query(e.to_string()))?;
        Ok(())
    }

    async fn record_run_failure(
        &self,
        run_id: Uuid,
        error_message: &str,
    ) -> Result<(), BackendError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        let sql = format!(
            "UPDATE `{META_SCHEMA}`.`{RUN_HISTORY_TABLE}` \
             SET status='failed', error_message=?, finished_at=NOW(6) \
             WHERE run_id=?"
        );
        conn.exec_drop(&sql, (error_message.to_string(), run_id.to_string()))
            .await
            .map_err(|e| BackendError::Query(e.to_string()))?;
        Ok(())
    }

    /// Compute `MAX(<column>)` over the rows just inserted by this batch
    /// (matched on `_batch_id`) and UPSERT into the watermarks table.
    /// NULL `MAX` (zero rows inserted, or column itself NULL) is a
    /// no-op so a stale `last_value` survives a zero-row run. Mirrors
    /// `pg::advance_watermark` and `duckdb_backend::advance_watermark`.
    async fn advance_watermark(
        &self,
        target: &TableSpec,
        batch_id: Uuid,
        pipeline_name: &str,
        watermark: &WatermarkConfig,
    ) -> Result<(), BackendError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;
        // Cast MAX to CHAR so every type round-trips through last_value
        // as text — matches DuckDB's `::VARCHAR` cast and SQLite's
        // `CAST(... AS TEXT)`.
        let max_sql = format!(
            "SELECT CAST(MAX({col}) AS CHAR) FROM {qualified} \
             WHERE {batch_id_col} = ?",
            col = watermark.column,
            qualified = qualified(&target.schema, &target.name),
            batch_id_col = BATCH_ID_COL,
        );
        let max_value: Option<Option<String>> = conn
            .exec_first(&max_sql, (batch_id.to_string(),))
            .await
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let value = match max_value.flatten() {
            Some(v) => v,
            None => return Ok(()),
        };
        // MySQL UPSERT via `INSERT ... ON DUPLICATE KEY UPDATE`. The
        // legacy `VALUES(col)` form is deprecated in 8.0.20+ but still
        // works through 8.x; the row-alias form (`INSERT ... AS new
        // ON DUPLICATE KEY UPDATE col = new.col`) requires 8.0.19+.
        // We use the legacy form for broader compat with managed MySQL
        // 5.7 deployments.
        let upsert_sql = format!(
            "INSERT INTO `{META_SCHEMA}`.`{WATERMARKS_TABLE}` \
             (pipeline_name, column_name, `last_value`, updated_at) \
             VALUES (?, ?, ?, NOW(6)) \
             ON DUPLICATE KEY UPDATE \
               column_name = VALUES(column_name), \
               `last_value` = VALUES(`last_value`), \
               updated_at = VALUES(updated_at)"
        );
        conn.exec_drop(
            &upsert_sql,
            (pipeline_name.to_string(), watermark.column.clone(), value),
        )
        .await
        .map_err(|e| BackendError::Query(e.to_string()))?;
        Ok(())
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
                "MySQL cross-backend run_append goes through the Arrow streaming \
                 bridge (cross_backend_arrow_sync); same-backend only here"
                    .into(),
            ));
        }
        let watermark = incremental_column.map(|c| WatermarkConfig {
            column: c.to_string(),
            last_value_literal: last_value_literal.map(|s| s.to_string()),
        });
        let filtered_source = wrap_with_watermark_filter(source_query, watermark.as_ref());
        let plan = plan_same_db_append(spec, &filtered_source);
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let sql = mysql_substitute(&plan.sql, &batch_id);

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

        // Wrap in a transaction so dry_run can rollback. MySQL DDL
        // (CREATE TABLE etc.) auto-commits, but our INSERT … SELECT is
        // pure DML and is fully transactional under InnoDB.
        let inserted_result: Result<u64, BackendError> = async {
            let mut conn = self
                .pool
                .get_conn()
                .await
                .map_err(|e| BackendError::Connection(e.to_string()))?;
            conn.query_drop("START TRANSACTION")
                .await
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let n_result = conn.query_iter(&sql).await;
            let n = match n_result {
                Ok(result) => {
                    let n = result.affected_rows();
                    result
                        .drop_result()
                        .await
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    n
                }
                Err(e) => {
                    let _ = conn.query_drop("ROLLBACK").await;
                    return Err(BackendError::Query(e.to_string()));
                }
            };
            if dry_run {
                conn.query_drop("ROLLBACK")
                    .await
                    .map_err(|e| BackendError::Query(e.to_string()))?;
            } else {
                conn.query_drop("COMMIT")
                    .await
                    .map_err(|e| BackendError::Query(e.to_string()))?;
            }
            Ok(n)
        }
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
                "MySQL cross-backend run_truncate goes through the Arrow bridge".into(),
            ));
        }
        let plan = plan_truncate_replace(spec, source_query);
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        // mysql_substitute also rewrites `TRUNCATE TABLE x` → `DELETE
        // FROM x` so the empty-then-load pair stays inside one
        // transaction (TRUNCATE auto-commits in MySQL).
        let stmts: Vec<String> = plan
            .statements
            .iter()
            .map(|s| mysql_substitute(s, &batch_id))
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

        let inserted_result: Result<u64, BackendError> = async {
            let mut conn = self
                .pool
                .get_conn()
                .await
                .map_err(|e| BackendError::Connection(e.to_string()))?;
            conn.query_drop("START TRANSACTION")
                .await
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let mut last_n: u64 = 0;
            for s in &stmts {
                let result = match conn.query_iter(s).await {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = conn.query_drop("ROLLBACK").await;
                        return Err(BackendError::Query(e.to_string()));
                    }
                };
                last_n = result.affected_rows();
                result
                    .drop_result()
                    .await
                    .map_err(|e| BackendError::Query(e.to_string()))?;
            }
            if dry_run {
                conn.query_drop("ROLLBACK")
                    .await
                    .map_err(|e| BackendError::Query(e.to_string()))?;
            } else {
                conn.query_drop("COMMIT")
                    .await
                    .map_err(|e| BackendError::Query(e.to_string()))?;
            }
            Ok(last_n)
        }
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
                "MySQL cross-backend run_merge goes through the Arrow bridge".into(),
            ));
        }
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let merge_sql = mysql_merge_sql(spec, source_query, update_columns, &batch_id);

        // MySQL `affected_rows()` for ON DUPLICATE KEY UPDATE counts
        // each updated row as 2 (insert-attempted + update-applied),
        // and unchanged matched rows as 0 — so we can't subtract
        // inserts from total to get updates the way SQLite/DuckDB do.
        // Compute both counts independently from the source query and
        // the target's existing keys, then derive updates from the
        // difference. `rows_unchanged` stays 0 (no MySQL-native way to
        // distinguish unchanged from updated without a row-by-row hash
        // compare; same trade-off as DuckDB / SQLite).
        let qualified_target = qualified(&spec.schema, &spec.name);
        let key_tuple = keys.join(", ");
        let source_count_sql = format!("SELECT count(*) FROM ({source_query}) src_total");
        let insert_count_sql = format!(
            "SELECT count(*) FROM ({source_query}) src_count \
             WHERE ({key_tuple}) NOT IN (SELECT {key_tuple} FROM {qualified_target})"
        );
        let delete_sql = if matches!(delete_handling, Some(DeleteHandling::Hard)) {
            Some(build_hard_delete_sql(
                &spec.schema,
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

        let merge_result: Result<(i64, i64), BackendError> = async {
            let mut conn = self
                .pool
                .get_conn()
                .await
                .map_err(|e| BackendError::Connection(e.to_string()))?;
            conn.query_drop("START TRANSACTION")
                .await
                .map_err(|e| BackendError::Query(e.to_string()))?;
            // Anti-join count of source rows whose keys aren't in the
            // target — these are the inserts.
            let inserts: i64 = match conn.query_first(&insert_count_sql).await {
                Ok(Some(n)) => n,
                Ok(None) => 0,
                Err(e) => {
                    let _ = conn.query_drop("ROLLBACK").await;
                    return Err(BackendError::Query(e.to_string()));
                }
            };
            let source_total: i64 = match conn.query_first(&source_count_sql).await {
                Ok(Some(n)) => n,
                Ok(None) => 0,
                Err(e) => {
                    let _ = conn.query_drop("ROLLBACK").await;
                    return Err(BackendError::Query(e.to_string()));
                }
            };
            // Run the upsert. We deliberately ignore the affected-row
            // count returned here; counts come from the COUNT(*) queries.
            if let Err(e) = conn.query_drop(&merge_sql).await {
                let _ = conn.query_drop("ROLLBACK").await;
                return Err(BackendError::Query(e.to_string()));
            }
            if let Some(dsql) = &delete_sql
                && let Err(e) = conn.query_drop(dsql).await
            {
                let _ = conn.query_drop("ROLLBACK").await;
                return Err(BackendError::Query(e.to_string()));
            }
            if dry_run {
                conn.query_drop("ROLLBACK")
                    .await
                    .map_err(|e| BackendError::Query(e.to_string()))?;
            } else {
                conn.query_drop("COMMIT")
                    .await
                    .map_err(|e| BackendError::Query(e.to_string()))?;
            }
            let updated = (source_total - inserts).max(0);
            Ok((inserts, updated))
        }
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
