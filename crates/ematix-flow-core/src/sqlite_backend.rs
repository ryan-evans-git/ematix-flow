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
use crate::strategy::scd2::{IS_CURRENT_COL, ROW_HASH_COL, VALID_FROM_COL, VALID_TO_COL};
use crate::strategy::truncate::plan_truncate_replace;
use crate::types::TableSpec;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const META_RUN_HISTORY: &str = "ematix_flow_run_history";
const META_WATERMARKS: &str = "ematix_flow_watermarks";
/// Δ.X2: per-pipeline CDC idempotency table. Mirrors the PG /
/// DuckDB layout but flattened into `main` since SQLite has no
/// schemas (the prefix `ematix_flow_` keeps the meta surface
/// distinct from user tables in the same database).
const META_CDC_IDEMPOTENCY: &str = "ematix_flow_cdc_idempotency";

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
         ); \
         CREATE TABLE IF NOT EXISTS {META_CDC_IDEMPOTENCY} (\
            pipeline_name TEXT NOT NULL, \
            pk_json TEXT NOT NULL, \
            last_seen_ts_ms INTEGER NOT NULL, \
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), \
            PRIMARY KEY (pipeline_name, pk_json)\
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

const SQLITE_NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

/// SQLite equivalent of `pg::hash::postgres_digest_expression` and
/// `duckdb_digest_expression`. SQLite has no native sha256, so we
/// invoke the `ematix_sha256` UDF registered at backend open time.
/// Returns BLOB so row_hash storage is consistent across backends.
fn sqlite_digest_expression(columns: &[String], prefix: &str) -> String {
    let parts: Vec<String> = columns
        .iter()
        .map(|c| {
            format!(
                "coalesce(CAST({prefix}{c} AS TEXT), char(0) || 'NULL' || char(0))",
                prefix = prefix,
                c = c,
            )
        })
        .collect();
    format!("ematix_sha256({})", parts.join(" || char(1) || "))
}

/// SQLite-specific SCD2 statement builder. Mirrors the structure of
/// `duckdb_scd2_statements` but emits SQL SQLite accepts:
///   - schema is always `main` (validated by caller via require_main_schema).
///   - `is_current` is INTEGER 0/1 (SQLite has no native BOOLEAN).
///   - `now()` → `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`.
///   - `digest(…, 'sha256')` → `ematix_sha256(…)`.
///   - `DISTINCT ON (keys) … ORDER BY ts DESC` → ROW_NUMBER subquery,
///     since SQLite doesn't have DISTINCT ON.
///   - Temp table dropped explicitly at the end (no ON COMMIT DROP).
///   - $1::uuid placeholder substituted to a literal up-front.
fn sqlite_scd2_statements(
    target: &TableSpec,
    source_query: &str,
    keys: &[String],
    compare_columns: &[String],
    run_token: &str,
    event_ts_column: Option<&str>,
    batch_id: &Uuid,
) -> Vec<String> {
    let user_columns: Vec<String> = target
        .columns
        .iter()
        .filter(|c| {
            let n = c.name.as_str();
            n != VALID_FROM_COL
                && n != VALID_TO_COL
                && n != IS_CURRENT_COL
                && n != ROW_HASH_COL
                && n != LOADED_AT_COL
                && n != BATCH_ID_COL
        })
        .map(|c| c.name.clone())
        .collect();
    let has_metadata = target
        .columns
        .iter()
        .any(|c| c.name == LOADED_AT_COL || c.name == BATCH_ID_COL);

    let stage = format!("_scd2_changed_{run_token}");
    let digest_expr = sqlite_digest_expression(compare_columns, "");
    let key_tuple: String = keys.join(", ");
    let t_tuple: String = keys
        .iter()
        .map(|k| format!("t.{k}"))
        .collect::<Vec<_>>()
        .join(", ");
    let src_tuple: String = keys
        .iter()
        .map(|k| format!("src.{k}"))
        .collect::<Vec<_>>()
        .join(", ");
    let join_clause = format!("({t_tuple}) = ({src_tuple})");
    let table_ref = format!("main.{}", target.name);

    let src_select = match event_ts_column {
        Some(ets) => format!(
            "SELECT * FROM (\
               SELECT {user_cols}, {ets} AS _event_ts, \
                      {digest_expr} AS _row_hash, \
                      ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {ets} DESC) AS _rn \
               FROM ({source}) q \
             ) WHERE _rn = 1",
            user_cols = user_columns.join(", "),
            ets = ets,
            digest_expr = digest_expr,
            keys = key_tuple,
            source = source_query,
        ),
        None => format!(
            "SELECT {user_cols}, {digest_expr} AS _row_hash FROM ({source}) q",
            user_cols = user_columns.join(", "),
            digest_expr = digest_expr,
            source = source_query,
        ),
    };

    let create_temp = format!(
        "CREATE TEMP TABLE {stage} AS \
         WITH src AS ( {src_select} ) \
         SELECT src.* FROM src \
         LEFT JOIN {table_ref} t \
             ON {join_clause} AND t.{is_current_col} = 1 \
         WHERE t.{row_hash_col} IS NOT src._row_hash",
        stage = stage,
        src_select = src_select,
        table_ref = table_ref,
        join_clause = join_clause,
        is_current_col = IS_CURRENT_COL,
        row_hash_col = ROW_HASH_COL,
    );

    let close_out = match event_ts_column {
        Some(_) => {
            // Correlated UPDATE picks each row's valid_to from the matching
            // changed-stage row's event_ts. SQLite's UPDATE … FROM landed
            // in 3.33; the bundled SQLite is well past that.
            let join: String = keys
                .iter()
                .map(|k| format!("t.{k} = c.{k}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!(
                "UPDATE {table_ref} AS t \
                 SET {valid_to} = c._event_ts, {is_current} = 0 \
                 FROM {stage} c \
                 WHERE t.{is_current} = 1 AND {join}",
                table_ref = table_ref,
                valid_to = VALID_TO_COL,
                is_current = IS_CURRENT_COL,
                stage = stage,
                join = join,
            )
        }
        None => format!(
            "UPDATE {table_ref} \
             SET {valid_to} = {now}, {is_current} = 0 \
             WHERE {is_current} = 1 AND ({keys}) IN (SELECT {keys} FROM {stage})",
            table_ref = table_ref,
            valid_to = VALID_TO_COL,
            is_current = IS_CURRENT_COL,
            keys = key_tuple,
            stage = stage,
            now = SQLITE_NOW,
        ),
    };

    let mut insert_cols: Vec<String> = user_columns.clone();
    insert_cols.push(VALID_FROM_COL.into());
    insert_cols.push(VALID_TO_COL.into());
    insert_cols.push(IS_CURRENT_COL.into());
    insert_cols.push(ROW_HASH_COL.into());
    let mut select_exprs: Vec<String> = user_columns.clone();
    let valid_from_expr: String = match event_ts_column {
        Some(_) => "_event_ts".into(),
        None => SQLITE_NOW.into(),
    };
    select_exprs.push(valid_from_expr);
    select_exprs.push("NULL".into());
    select_exprs.push("1".into());
    select_exprs.push("_row_hash".into());
    if has_metadata {
        insert_cols.push(LOADED_AT_COL.into());
        insert_cols.push(BATCH_ID_COL.into());
        select_exprs.push(SQLITE_NOW.into());
        select_exprs.push(format!("'{batch_id}'"));
    }
    let insert_new = format!(
        "INSERT INTO {table_ref} ({insert_cols}) \
         SELECT {select_exprs} FROM {stage}",
        table_ref = table_ref,
        insert_cols = insert_cols.join(", "),
        select_exprs = select_exprs.join(", "),
        stage = stage,
    );

    let drop_temp = format!("DROP TABLE {stage}");

    vec![create_temp, close_out, insert_new, drop_temp]
}

/// SQLite-specific TTL expiry. PG/DuckDB compute timestamp arithmetic
/// directly; SQLite stores TIMESTAMPTZ as TEXT (ISO-8601), so we
/// compare epoch seconds via strftime('%s', …).
fn sqlite_scd2_ttl_expire_sql(target_table: &str, ttl_seconds: i64) -> String {
    format!(
        "UPDATE main.{table} \
         SET valid_to = {now}, is_current = 0 \
         WHERE is_current = 1 \
           AND CAST(strftime('%s', valid_from) AS INTEGER) < \
               CAST(strftime('%s', 'now') AS INTEGER) - {ttl_seconds}",
        table = target_table,
        now = SQLITE_NOW,
        ttl_seconds = ttl_seconds,
    )
}

/// SQLite-specific soft-delete (close-missing). Mirrors
/// `meta::build_scd2_close_missing_sql` but with SQLite literals.
fn sqlite_scd2_close_missing_sql(
    target_table: &str,
    keys: &[String],
    source_keys_query: &str,
) -> String {
    let key_tuple = keys.join(", ");
    format!(
        "UPDATE main.{table} \
         SET valid_to = {now}, is_current = 0 \
         WHERE is_current = 1 \
           AND ({key_tuple}) NOT IN (SELECT {key_tuple} FROM ({source}) _del)",
        table = target_table,
        now = SQLITE_NOW,
        key_tuple = key_tuple,
        source = source_keys_query,
    )
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
        // Register the SCD2 hash UDF on every connection. SQLite has no
        // built-in sha256() (PG has pgcrypto.digest, DuckDB has
        // sha256()/unhex()) so we plug Rust's sha2 in as a scalar
        // function. Returns BLOB so it round-trips with the row_hash
        // column type.
        conn.create_scalar_function(
            "ematix_sha256",
            1,
            rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC
                | rusqlite::functions::FunctionFlags::SQLITE_UTF8,
            |ctx| {
                let s = ctx.get::<String>(0)?;
                let mut h = Sha256::new();
                h.update(s.as_bytes());
                let bytes: [u8; 32] = h.finalize().into();
                Ok(bytes.to_vec())
            },
        )
        .map_err(|e| BackendError::Connection(e.to_string()))?;
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

impl SQLiteBackend {
    /// Δ.X2: SQLite CDC executor — applies a parsed batch of
    /// `CdcEvent`s against `target_spec` inside a single
    /// transaction.
    ///
    /// Lifecycle:
    ///   1. Lazy-bootstrap the meta schema (so the idempotency
    ///      table exists on first call).
    ///   2. Schema-evolution `Fail` policy: pre-flight `after`
    ///      payload keys against the spec; abort the whole batch
    ///      on first unknown.
    ///   3. Per-event in one transaction: hit the idempotency
    ///      gate, then dispatch on `event.op` to UPSERT / DELETE
    ///      / soft-delete via prepared statements that re-bind
    ///      the JSON parameter per call.
    ///
    /// Differs from PG in that SQLite has no row-typed JSON
    /// populate. Each upsert/delete uses `json_extract(?1, '$.col')`
    /// per column; the JSON payload is bound once via the
    /// `?1` indexed-parameter form.
    pub async fn run_cdc_inner(
        &self,
        target_spec: &crate::types::TableSpec,
        events: Vec<crate::cdc::CdcEvent>,
        cdc_config: &crate::cdc::CdcConfig,
        pipeline_name: &str,
        skipped: i64,
    ) -> Result<crate::backend::CdcRunResult, BackendError> {
        use crate::cdc::{CdcOp, DeleteMode, SchemaEvolutionPolicy};

        require_main_schema(&target_spec.schema)?;
        self.ensure_meta_schema().await?;

        // Single-PK only for the first cut (matches PG PR 3 scope).
        let pk_col = target_spec
            .columns
            .iter()
            .find(|c| c.primary_key)
            .ok_or_else(|| {
                BackendError::Other(format!(
                    "SQLite run_cdc: target {}.{} has no primary-key column \
                     (Δ.X2 v1 supports single-PK targets; declare PK on \
                     @ematix.table or [target.table].primary_key)",
                    target_spec.schema, target_spec.name
                ))
            })?;
        let pk_name = pk_col.name.clone();

        // Schema-evolution `Fail` policy: pre-flight check against
        // the spec's columns. `Skip` quietly drops unknown columns
        // at the lowering boundary (rest of the row applies).
        if matches!(cdc_config.schema_evolution, SchemaEvolutionPolicy::Fail) {
            let valid_cols: std::collections::HashSet<&str> = target_spec
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            for event in &events {
                if let Some(after) = event.after.as_ref() {
                    for k in after.keys() {
                        if !valid_cols.contains(k.as_str()) {
                            return Err(BackendError::Other(format!(
                                "schema evolution: unknown column '{k}' in `after` \
                                 payload for pipeline '{pipeline_name}' on SQLite target \
                                 {schema}.{name} (SchemaEvolutionPolicy::Fail) — \
                                 add the column to the target table or switch to Skip",
                                schema = target_spec.schema,
                                name = target_spec.name,
                            )));
                        }
                    }
                }
            }
        }

        let qualified = format!(
            "\"{}\".\"{}\"",
            target_spec.schema.replace('"', "\"\""),
            target_spec.name.replace('"', "\"\""),
        );
        let user_cols: Vec<String> = target_spec.columns.iter().map(|c| c.name.clone()).collect();
        let non_pk_cols: Vec<&str> = target_spec
            .columns
            .iter()
            .filter(|c| !c.primary_key)
            .map(|c| c.name.as_str())
            .collect();

        // Pre-build SQL templates. Each upsert/delete is bound
        // with a single parameter (the JSON payload) at index ?1;
        // SQLite's `?N` indexed form lets one bind serve many
        // `json_extract` calls without re-marshalling the string.
        let cols_csv = user_cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let select_exprs = user_cols
            .iter()
            .map(|c| {
                // SQLite's json_extract path uses '$.col'; column
                // names with a literal `.` aren't supported here
                // (consistent with PG/DuckDB CDC scope).
                format!("json_extract(?1, '$.{}')", c.replace('\'', "''"))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let upsert_sql = if non_pk_cols.is_empty() {
            format!(
                "INSERT INTO {qualified} ({cols_csv}) SELECT {select_exprs} \
                 ON CONFLICT(\"{pk}\") DO NOTHING",
                pk = pk_name.replace('"', "\"\""),
            )
        } else {
            let set_clause = non_pk_cols
                .iter()
                .map(|c| {
                    let q = c.replace('"', "\"\"");
                    format!("\"{q}\" = excluded.\"{q}\"")
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "INSERT INTO {qualified} ({cols_csv}) SELECT {select_exprs} \
                 ON CONFLICT(\"{pk}\") DO UPDATE SET {set_clause}",
                pk = pk_name.replace('"', "\"\""),
            )
        };

        // DELETE / soft-delete: pull the PK out of the same JSON
        // payload via json_extract so the type coercion is
        // identical to the upsert path.
        let pk_json_path = format!("json_extract(?1, '$.{}')", pk_name.replace('\'', "''"));
        let delete_sql = match &cdc_config.delete_mode {
            DeleteMode::Hard => format!(
                "DELETE FROM {qualified} WHERE \"{pk}\" = {pk_json_path}",
                pk = pk_name.replace('"', "\"\""),
            ),
            DeleteMode::Soft { column } => format!(
                "UPDATE {qualified} SET \"{col}\" = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                 WHERE \"{pk}\" = {pk_json_path}",
                pk = pk_name.replace('"', "\"\""),
                col = column.replace('"', "\"\""),
            ),
        };

        // Idempotency gate. Same shape as PG / DuckDB: INSERT …
        // ON CONFLICT DO UPDATE WHERE existing.last_seen_ts_ms <
        // EXCLUDED.last_seen_ts_ms RETURNING 1. SQLite's
        // RETURNING (3.35+) makes the empty-result-means-skip
        // pattern reusable here.
        let gate_sql = format!(
            "INSERT INTO {META_CDC_IDEMPOTENCY} \
                 (pipeline_name, pk_json, last_seen_ts_ms, updated_at) \
             VALUES (?1, ?2, ?3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
             ON CONFLICT(pipeline_name, pk_json) DO UPDATE \
                 SET last_seen_ts_ms = excluded.last_seen_ts_ms, \
                     updated_at = excluded.updated_at \
                 WHERE {META_CDC_IDEMPOTENCY}.last_seen_ts_ms < excluded.last_seen_ts_ms \
             RETURNING 1"
        );

        // Move owned values into the blocking task.
        let pipeline_name_owned = pipeline_name.to_string();
        let pk_name_owned = pk_name;
        let events_owned = events;
        let is_soft_delete = matches!(cdc_config.delete_mode, DeleteMode::Soft { .. });

        let outcome: Result<(i64, i64, i64, i64), BackendError> = self
            .with_conn_blocking(move |c| {
                c.execute_batch("BEGIN")
                    .map_err(|e| BackendError::Query(e.to_string()))?;

                let result: Result<(i64, i64, i64, i64), BackendError> = (|| {
                    let mut creates = 0i64;
                    let mut updates = 0i64;
                    let mut deletes = 0i64;
                    let mut idempotent_skipped = 0i64;

                    // In-batch gate cache: multiple events for the
                    // same PK in this batch only hit the gate once.
                    let mut gate_cache: std::collections::HashMap<String, i64> =
                        std::collections::HashMap::new();

                    let mut upsert_stmt = c
                        .prepare(&upsert_sql)
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    let mut delete_stmt = c
                        .prepare(&delete_sql)
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    let mut gate_stmt = c
                        .prepare(&gate_sql)
                        .map_err(|e| BackendError::Query(e.to_string()))?;

                    for event in &events_owned {
                        let payload_json: String = match event.op {
                            CdcOp::Delete => {
                                let mut m = serde_json::Map::new();
                                m.insert(pk_name_owned.clone(), event.key.clone());
                                serde_json::to_string(&m).unwrap()
                            }
                            _ => match event.after.as_ref() {
                                Some(map) => serde_json::to_string(map).unwrap(),
                                None => continue,
                            },
                        };

                        if let Some(ts_ms) = event.ts_ms {
                            let pk_canon = serde_json::to_string(&event.key).unwrap();
                            let cache_hit = gate_cache.get(&pk_canon).copied();
                            let admit = match cache_hit {
                                Some(cached_ts) => {
                                    if ts_ms > cached_ts {
                                        gate_cache.insert(pk_canon.clone(), ts_ms);
                                        true
                                    } else {
                                        false
                                    }
                                }
                                None => {
                                    let mut rows = gate_stmt
                                        .query(rusqlite::params![
                                            &pipeline_name_owned,
                                            &pk_canon,
                                            ts_ms,
                                        ])
                                        .map_err(|e| {
                                            BackendError::Query(format!("sqlite cdc gate: {e}"))
                                        })?;
                                    let admitted = rows
                                        .next()
                                        .map_err(|e| {
                                            BackendError::Query(format!(
                                                "sqlite cdc gate read: {e}"
                                            ))
                                        })?
                                        .is_some();
                                    if admitted {
                                        gate_cache.insert(pk_canon, ts_ms);
                                    }
                                    admitted
                                }
                            };
                            if !admit {
                                idempotent_skipped += 1;
                                continue;
                            }
                        }

                        match event.op {
                            CdcOp::Create | CdcOp::Read => {
                                let n = upsert_stmt
                                    .execute(rusqlite::params![&payload_json])
                                    .map_err(|e| {
                                        BackendError::Query(format!("sqlite cdc upsert: {e}"))
                                    })?;
                                creates += n as i64;
                            }
                            CdcOp::Update => {
                                let n = upsert_stmt
                                    .execute(rusqlite::params![&payload_json])
                                    .map_err(|e| {
                                        BackendError::Query(format!(
                                            "sqlite cdc update-as-upsert: {e}"
                                        ))
                                    })?;
                                updates += n as i64;
                            }
                            CdcOp::Delete => {
                                let n = delete_stmt
                                    .execute(rusqlite::params![&payload_json])
                                    .map_err(|e| {
                                        BackendError::Query(format!("sqlite cdc delete: {e}"))
                                    })?;
                                // Soft-delete is wired through an
                                // UPDATE, so the affected rows go
                                // under `updates` — only hard-delete
                                // bumps `deletes`.
                                if is_soft_delete {
                                    updates += n as i64;
                                } else {
                                    deletes += n as i64;
                                }
                            }
                        }
                    }

                    Ok((creates, updates, deletes, idempotent_skipped))
                })();

                match result {
                    Ok(counts) => {
                        c.execute_batch("COMMIT")
                            .map_err(|e| BackendError::Query(e.to_string()))?;
                        Ok(counts)
                    }
                    Err(e) => {
                        let _ = c.execute_batch("ROLLBACK");
                        Err(e)
                    }
                }
            })
            .await;

        let (creates, updates, deletes, idempotent_skipped) = outcome?;
        let run_id = Uuid::new_v4();
        Ok(crate::backend::CdcRunResult {
            run_id: run_id.to_string(),
            creates,
            updates,
            deletes,
            skipped,
            idempotent_skipped,
        })
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

    fn config(&self) -> crate::backend::BackendConfig {
        crate::backend::BackendConfig::Sqlite(crate::backend::SqliteConfig {
            location: self.location.clone(),
        })
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
            // faster on SQLite. On ANY failure the transaction is
            // rolled back explicitly: the connection is pooled, and a
            // dangling BEGIN would poison every later write with
            // "cannot start a transaction within a transaction"
            // (found by the DLQ Phase 2 fail-at-sink → fix → replay
            // round trip).
            c.execute_batch("BEGIN")
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let mut total: u64 = 0;
            for batch in &batches {
                match insert_record_batch(c, &target, batch) {
                    Ok(n) => total += n,
                    Err(e) => {
                        let _ = c.execute_batch("ROLLBACK");
                        return Err(e);
                    }
                }
            }
            if let Err(e) = c.execute_batch("COMMIT") {
                let _ = c.execute_batch("ROLLBACK");
                return Err(BackendError::Query(e.to_string()));
            }
            Ok(total)
        })
        .await
    }

    // Σ.XO (2026-07-12): batch insert + `_ematix_offsets` upsert in
    // ONE transaction. The lazy CREATE TABLE IF NOT EXISTS rides the
    // same transaction, so a crash at any point leaves either
    // everything (rows + offsets + table) or nothing.
    async fn write_arrow_stream_with_offsets(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
        pipeline_id: &str,
        offsets: &[(String, String)],
    ) -> Result<Option<u64>, BackendError> {
        use futures_util::StreamExt;

        require_main_schema(&target.schema)?;
        let mut s = stream;
        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(b) = s.next().await {
            batches.push(b?);
        }
        let target = target.clone();
        let pipeline_id = pipeline_id.to_string();
        let offsets = offsets.to_vec();
        self.with_conn_blocking(move |c| {
            c.execute_batch("BEGIN")
                .map_err(|e| BackendError::Query(e.to_string()))?;
            // Same rollback-on-any-failure discipline as
            // `write_arrow_stream`: the connection is shared, and a
            // dangling BEGIN poisons every later write.
            let result: Result<u64, BackendError> = (|| {
                let table_ref = format!("\"{}\"", target.name.replace('"', "\"\""));
                if mode == WriteMode::Truncate {
                    c.execute_batch(&format!("DELETE FROM {table_ref}"))
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                }
                let mut total: u64 = 0;
                for batch in &batches {
                    total += insert_record_batch(c, &target, batch)?;
                }
                // Empty offsets = plain transactional write; don't
                // create a meta table the pipeline will never read.
                if !offsets.is_empty() {
                    c.execute_batch(
                        "CREATE TABLE IF NOT EXISTS _ematix_offsets (\
                             pipeline_id TEXT NOT NULL, \
                             source_id TEXT NOT NULL, \
                             offsets_json TEXT NOT NULL, \
                             updated_at TEXT NOT NULL, \
                             PRIMARY KEY (pipeline_id, source_id))",
                    )
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                    let mut stmt = c
                        .prepare(
                            "INSERT INTO _ematix_offsets \
                                 (pipeline_id, source_id, offsets_json, updated_at) \
                             VALUES (?1, ?2, ?3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
                             ON CONFLICT (pipeline_id, source_id) DO UPDATE SET \
                                 offsets_json = excluded.offsets_json, \
                                 updated_at = excluded.updated_at",
                        )
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    for (source_id, offsets_json) in &offsets {
                        stmt.execute(rusqlite::params![&pipeline_id, source_id, offsets_json])
                            .map_err(|e| BackendError::Query(e.to_string()))?;
                    }
                }
                Ok(total)
            })();
            match result {
                Ok(total) => {
                    if let Err(e) = c.execute_batch("COMMIT") {
                        let _ = c.execute_batch("ROLLBACK");
                        return Err(BackendError::Query(e.to_string()));
                    }
                    Ok(Some(total))
                }
                Err(e) => {
                    let _ = c.execute_batch("ROLLBACK");
                    Err(e)
                }
            }
        })
        .await
    }

    // Σ.XO (2026-07-12): a missing `_ematix_offsets` table is
    // "supported, nothing committed yet" — the write side creates it
    // lazily, so a fresh target must not read as unsupported.
    async fn load_committed_offsets(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<Vec<(String, String)>>, BackendError> {
        let pipeline_id = pipeline_id.to_string();
        self.with_conn_blocking(move |c| {
            let table_exists: i64 = c
                .query_row(
                    "SELECT count(*) FROM sqlite_master \
                     WHERE type = 'table' AND name = '_ematix_offsets'",
                    [],
                    |r| r.get(0),
                )
                .map_err(|e| BackendError::Query(e.to_string()))?;
            if table_exists == 0 {
                return Ok(Some(Vec::new()));
            }
            let mut stmt = c
                .prepare(
                    "SELECT source_id, offsets_json FROM _ematix_offsets \
                     WHERE pipeline_id = ?1 ORDER BY source_id",
                )
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let rows = stmt
                .query_map(rusqlite::params![&pipeline_id], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(|e| BackendError::Query(e.to_string()))?
                .collect::<Result<Vec<(String, String)>, _>>()
                .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(Some(rows))
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
                 (route via source.read_arrow_stream + target.write_arrow_stream); \
                 same-backend only here"
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
        spec: &TableSpec,
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
        if source_backend.is_some() {
            return Err(BackendError::Other(
                "SQLite cross-backend run_scd2 goes through the Arrow bridge".into(),
            ));
        }
        if let Some(dh) = delete_handling
            && !matches!(dh, DeleteHandling::Soft)
        {
            return Err(BackendError::Other(format!(
                "SQLite run_scd2: only DeleteHandling::Soft is supported (got {dh:?}); \
                 Hard is for merge"
            )));
        }
        require_main_schema(&spec.schema)?;
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let run_token = run_id.simple().to_string();
        let mut stmts = sqlite_scd2_statements(
            spec,
            source_query,
            keys,
            compare_columns,
            &run_token,
            event_timestamp_column,
            &batch_id,
        );
        // Append soft-delete + TTL between INSERT and the final DROP, so
        // newly-inserted current rows aren't accidentally tombstoned by
        // their own absence in the join.
        let drop_temp = stmts.pop().expect("plan ends with DROP TABLE");
        if matches!(delete_handling, Some(DeleteHandling::Soft)) {
            stmts.push(sqlite_scd2_close_missing_sql(
                &spec.name,
                keys,
                source_query,
            ));
        }
        if let Some(ttl) = ttl_seconds {
            stmts.push(sqlite_scd2_ttl_expire_sql(&spec.name, ttl));
        }
        stmts.push(drop_temp);

        self.ensure_meta_schema().await?;
        self.record_run_start(
            run_id,
            pipeline_name,
            &spec.schema,
            &spec.name,
            "scd2",
            "same_db",
        )
        .await?;

        let inserted_result: Result<u64, BackendError> = self
            .with_conn_blocking(move |c| {
                c.execute_batch("BEGIN")
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                let mut inserted: u64 = 0;
                for (idx, sql) in stmts.iter().enumerate() {
                    let n = c.execute(sql, []).map_err(|e| {
                        let _ = c.execute_batch("ROLLBACK");
                        BackendError::Query(format!("scd2 stmt {idx} failed: {e}; sql={sql}"))
                    })?;
                    if idx == 2 {
                        inserted = n as u64;
                    }
                }
                if dry_run {
                    c.execute_batch("ROLLBACK")
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                } else {
                    c.execute_batch("COMMIT")
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                }
                Ok::<u64, BackendError>(inserted)
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

    /// Δ.X2: parse the incoming RecordBatch into CDC events, then
    /// dispatch to `run_cdc_inner`. Same shape as the Postgres /
    /// DuckDB trait-method wrappers — keeps the parse logic
    /// identical so a soft-fail row (parse error) is reported in
    /// the `skipped` counter rather than failing the whole batch.
    async fn run_cdc(
        &self,
        spec: &crate::types::TableSpec,
        batch: RecordBatch,
        cdc_config: &crate::cdc::CdcConfig,
        pipeline_name: &str,
    ) -> Result<crate::backend::CdcRunResult, BackendError> {
        use crate::cdc::ParsedRow;

        let parsed = crate::cdc::events_from_batch(&batch, cdc_config)?;
        let mut events = Vec::with_capacity(parsed.len());
        let mut skipped: i64 = 0;
        for row in parsed {
            match row {
                ParsedRow::Event(e) => events.push(e),
                ParsedRow::Tombstone => skipped += 1,
                ParsedRow::ParseError(e) => {
                    tracing::warn!(
                        target: "ematix_flow::cdc",
                        error = %e,
                        pipeline = pipeline_name,
                        "SQLite CDC envelope parse failed; row skipped",
                    );
                    skipped += 1;
                }
            }
        }
        self.run_cdc_inner(spec, events, cdc_config, pipeline_name, skipped)
            .await
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

    /// DLQ Phase 2 regression: a FAILED `write_arrow_stream` must
    /// roll its transaction back — otherwise the pooled connection
    /// is left inside an open `BEGIN` and every subsequent write
    /// fails with "cannot start a transaction within a
    /// transaction". This is exactly the fail-at-sink → fix →
    /// replay lifecycle.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_write_rolls_back_and_connection_stays_usable() {
        use arrow_array::{Int64Array as I64, RecordBatch as RB};
        use arrow_schema::{DataType as Dt, Field as F, Schema as S};

        let b = SQLiteBackend::open(":memory:").unwrap();

        let mk_stream = || {
            let schema = std::sync::Arc::new(S::new(vec![F::new("id", Dt::Int64, false)]));
            let batch = RB::try_new(
                schema,
                vec![std::sync::Arc::new(I64::from(vec![1_i64, 2_i64]))],
            )
            .unwrap();
            let s: crate::backend::ArrowBatchStream =
                Box::pin(futures_util::stream::iter(vec![Ok(batch)]));
            s
        };
        let target = TargetTable {
            schema: "main".into(),
            name: "events".into(),
        };

        // 1. Sink broken: no `events` table → the write fails.
        let err = b
            .write_arrow_stream(&target, mk_stream(), WriteMode::Append)
            .await
            .expect_err("write into a missing table must fail");
        assert!(
            err.to_string().contains("no such table"),
            "expected the missing-table error, got: {err}"
        );

        // 2. Fix the sink; the SAME backend must be able to write —
        //    i.e. the failed write rolled back instead of leaving
        //    the connection mid-transaction.
        b.execute("CREATE TABLE events (id INTEGER)").await.unwrap();
        let written = b
            .write_arrow_stream(&target, mk_stream(), WriteMode::Append)
            .await
            .expect("connection must be usable after a failed write");
        assert_eq!(written, 2);
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

    // --- Phase 32d: run_scd2 ----------------------------------------------

    use crate::strategy::scd2::augment_with_scd2;

    fn sqlite_scd2_dim_spec() -> TableSpec {
        augment_with_scd2(&TableSpec {
            schema: "main".into(),
            name: "customer_dim".into(),
            columns: vec![
                ColumnSpec {
                    name: "customer_id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "email".into(),
                    ty: ColumnType::Text,
                    nullable: false,
                    primary_key: false,
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

    async fn sqlite_scd2_setup() -> StdArc<dyn Backend> {
        let b: StdArc<dyn Backend> = StdArc::new(SQLiteBackend::open(":memory:").unwrap());
        b.execute("CREATE TABLE src_customers (customer_id INTEGER, email TEXT, name TEXT)")
            .await
            .unwrap();
        b.execute(
            "CREATE TABLE customer_dim (\
              customer_id INTEGER, \
              email TEXT, \
              name TEXT, \
              valid_from TEXT NOT NULL, \
              valid_to TEXT, \
              is_current INTEGER NOT NULL, \
              row_hash BLOB NOT NULL, \
              _loaded_at TEXT NOT NULL, \
              _batch_id TEXT NOT NULL, \
              PRIMARY KEY (customer_id, valid_from)\
            )",
        )
        .await
        .unwrap();
        b
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_scd2_first_load_inserts_all_current() {
        let b = sqlite_scd2_setup().await;
        b.execute(
            "INSERT INTO src_customers VALUES \
             (1, 'a@x.com', 'alice'), (2, 'b@x.com', 'bob'), (3, 'c@x.com', NULL)",
        )
        .await
        .unwrap();
        let target = sqlite_scd2_dim_spec();
        b.run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src_customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "sqlite_scd2_first",
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();

        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream(
                "SELECT count(*), \
                        sum(CASE WHEN is_current=1 THEN 1 ELSE 0 END), \
                        sum(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END) \
                 FROM customer_dim",
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
        let currents = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let null_valid_to = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 3);
        assert_eq!(currents, 3);
        assert_eq!(null_valid_to, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_scd2_second_load_closes_changed_row() {
        let b = sqlite_scd2_setup().await;
        b.execute(
            "INSERT INTO src_customers VALUES (1, 'a@x.com', 'alice'), (2, 'b@x.com', 'bob')",
        )
        .await
        .unwrap();
        let target = sqlite_scd2_dim_spec();
        b.run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src_customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "sqlite_scd2_a",
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();

        b.execute("UPDATE src_customers SET email = 'b2@x.com' WHERE customer_id = 2")
            .await
            .unwrap();
        // SQLite's `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')` is
        // millisecond-precision. On a fast runner the two SCD2
        // loads can land in the same ms — the second tries to
        // insert (customer_id=2, valid_from=T) when the first
        // already inserted (customer_id=2, valid_from=T) and the
        // (customer_id, valid_from) PK fires. Production has
        // natural gaps between SCD2 batches; tests don't, so a
        // 2 ms sleep guarantees T_b > T_a.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        b.run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src_customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "sqlite_scd2_b",
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();

        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream(
                "SELECT count(*), \
                        sum(CASE WHEN is_current=1 THEN 1 ELSE 0 END), \
                        sum(CASE WHEN customer_id=2 AND is_current=1 THEN 1 ELSE 0 END), \
                        sum(CASE WHEN customer_id=2 AND is_current=0 THEN 1 ELSE 0 END) \
                 FROM customer_dim",
            )
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let col = |i: usize| {
            batches[0]
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(col(0), 3);
        assert_eq!(col(1), 2);
        assert_eq!(col(2), 1, "exactly one current bob");
        assert_eq!(col(3), 1, "exactly one closed bob");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_scd2_idempotent_when_no_changes() {
        let b = sqlite_scd2_setup().await;
        b.execute("INSERT INTO src_customers VALUES (1, 'a@x.com', 'alice')")
            .await
            .unwrap();
        let target = sqlite_scd2_dim_spec();
        for tag in ["sqlite_scd2_idem_1", "sqlite_scd2_idem_2"] {
            b.run_scd2(
                &target,
                "SELECT customer_id, email, name FROM src_customers",
                &["customer_id".into()],
                &["email".into(), "name".into()],
                tag,
                None,
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();
        }
        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream("SELECT count(*) FROM customer_dim")
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 1, "no second version when nothing changed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_scd2_soft_delete_closes_missing_keys() {
        let b = sqlite_scd2_setup().await;
        b.execute(
            "INSERT INTO src_customers VALUES (1, 'a@x.com', 'alice'), (2, 'b@x.com', 'bob')",
        )
        .await
        .unwrap();
        let target = sqlite_scd2_dim_spec();
        b.run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src_customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "sqlite_scd2_softdel_a",
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();

        b.execute("DELETE FROM src_customers WHERE customer_id = 2")
            .await
            .unwrap();
        b.run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src_customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "sqlite_scd2_softdel_b",
            None,
            Some(DeleteHandling::Soft),
            None,
            None,
            false,
        )
        .await
        .unwrap();

        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream(
                "SELECT \
                  sum(CASE WHEN customer_id=2 AND is_current=1 THEN 1 ELSE 0 END), \
                  sum(CASE WHEN customer_id=2 AND is_current=0 AND valid_to IS NOT NULL \
                           THEN 1 ELSE 0 END), \
                  sum(CASE WHEN customer_id=1 AND is_current=1 THEN 1 ELSE 0 END) \
                 FROM customer_dim",
            )
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let col = |i: usize| {
            batches[0]
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(col(0), 0, "bob's current closed");
        assert_eq!(col(1), 1, "exactly one closed bob");
        assert_eq!(col(2), 1, "alice still current");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_run_scd2_ttl_expires_stale_current() {
        let b = sqlite_scd2_setup().await;
        b.execute("INSERT INTO src_customers VALUES (1, 'a@x.com', 'alice')")
            .await
            .unwrap();
        let target = sqlite_scd2_dim_spec();
        b.run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src_customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "sqlite_scd2_ttl_a",
            None,
            None,
            None,
            Some(600),
            false,
        )
        .await
        .unwrap();

        // Force-age alice's valid_from beyond the TTL.
        b.execute(
            "UPDATE customer_dim SET valid_from = '2020-01-01T00:00:00.000Z' \
             WHERE customer_id = 1",
        )
        .await
        .unwrap();
        b.run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src_customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "sqlite_scd2_ttl_b",
            None,
            None,
            None,
            Some(600),
            false,
        )
        .await
        .unwrap();

        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream(
                "SELECT sum(CASE WHEN is_current=1 THEN 1 ELSE 0 END), \
                        sum(CASE WHEN is_current=0 AND valid_to IS NOT NULL THEN 1 ELSE 0 END) \
                 FROM customer_dim",
            )
            .await
            .unwrap();
        let batches: Vec<_> = s.try_collect().await.unwrap();
        let currents = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let closed = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(currents, 0, "TTL-expired");
        assert_eq!(closed, 1);
    }

    // ------------------------------------------------------------
    // Phase Δ.X2 — CDC executor tests for SQLite. Mirror the
    // DuckDB / Postgres / Delta CDC test matrix: c+u+d across two
    // batches, redelivery dropped at the idempotency gate, Fail
    // policy aborting on schema drift, and soft-delete column-flip.
    // SQLite is in-memory so no testcontainer needed.
    // ------------------------------------------------------------

    fn sqlite_cdc_customers_spec() -> TableSpec {
        use crate::types::{ColumnSpec, ColumnType};
        TableSpec {
            // SQLite has no first-class schemas — `main` is the
            // only writable namespace without an ATTACH.
            schema: "main".into(),
            name: "customers".into(),
            columns: vec![
                ColumnSpec {
                    name: "id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "email".into(),
                    ty: ColumnType::Text,
                    nullable: true,
                    primary_key: false,
                },
                ColumnSpec {
                    name: "name".into(),
                    ty: ColumnType::Text,
                    nullable: true,
                    primary_key: false,
                },
            ],
            unique_constraints: vec![],
            fingerprint: String::new(),
        }
    }

    /// Encode a slice of CDC envelope JSON rows as a
    /// schema-inferred RecordBatch — same shape as DuckDB's helper.
    fn sqlite_cdc_record_batch_from_json(
        rows: &[serde_json::Map<String, serde_json::Value>],
    ) -> RecordBatch {
        use std::io::Cursor;
        use std::sync::Arc as StdArc;
        let mut buf = Vec::new();
        for row in rows {
            buf.extend(serde_json::to_string(row).unwrap().bytes());
            buf.push(b'\n');
        }
        let mut sniff = Cursor::new(&buf);
        let (schema, _) =
            arrow_json::reader::infer_json_schema_from_seekable(&mut sniff, None).expect("schema");
        let cursor = Cursor::new(&buf);
        let mut reader = arrow_json::ReaderBuilder::new(StdArc::new(schema))
            .build(cursor)
            .expect("reader");
        reader
            .next()
            .expect("at least one batch")
            .expect("decode ok")
    }

    async fn create_sqlite_cdc_target(backend: &std::sync::Arc<dyn Backend>) {
        backend
            .execute(
                "CREATE TABLE customers (\
                    id INTEGER PRIMARY KEY, \
                    email TEXT, \
                    name TEXT)",
            )
            .await
            .unwrap();
    }

    /// Δ.X2 (SQLite): c / u / d each get a turn so the per-op
    /// counters are observable independently.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_cdc_inserts_updates_deletes_across_batches() {
        use crate::cdc::{CdcConfig, EnvelopeKind};
        use serde_json::json;
        use std::sync::Arc as StdArc;

        let backend: StdArc<dyn Backend> = StdArc::new(SQLiteBackend::open(":memory:").unwrap());
        create_sqlite_cdc_target(&backend).await;

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        let spec = sqlite_cdc_customers_spec();

        let r1 = backend
            .run_cdc(
                &spec,
                sqlite_cdc_record_batch_from_json(&[
                    json!({
                        "before": null,
                        "after": {"id": 1, "email": "alice@x", "name": "Alice"},
                        "source": {"ts_ms": 100_i64},
                        "op": "c",
                        "ts_ms": 100_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                    json!({
                        "before": null,
                        "after": {"id": 2, "email": "bob@x", "name": "Bob"},
                        "source": {"ts_ms": 200_i64},
                        "op": "c",
                        "ts_ms": 200_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ]),
                &cdc,
                "sqlite_cdc_test",
            )
            .await
            .expect("batch 1");
        assert_eq!(r1.creates, 2, "two inserts in batch 1");
        assert_eq!(r1.updates, 0);
        assert_eq!(r1.deletes, 0);

        let r2 = backend
            .run_cdc(
                &spec,
                sqlite_cdc_record_batch_from_json(&[
                    json!({
                        "before": {"id": 1, "email": "alice@x", "name": "Alice"},
                        "after":  {"id": 1, "email": "alice@x", "name": "Alice Smith"},
                        "source": {"ts_ms": 300_i64},
                        "op": "u",
                        "ts_ms": 300_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                    json!({
                        "before": {"id": 2, "email": "bob@x", "name": "Bob"},
                        "after":  null,
                        "source": {"ts_ms": 400_i64},
                        "op": "d",
                        "ts_ms": 400_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ]),
                &cdc,
                "sqlite_cdc_test",
            )
            .await
            .expect("batch 2");
        assert_eq!(r2.updates, 1, "one update");
        assert_eq!(r2.deletes, 1, "one delete");

        use futures_util::TryStreamExt;
        let stream = backend
            .read_arrow_stream("SELECT id, name FROM customers ORDER BY id")
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "exactly id=1 should remain");

        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column is utf8");
        assert_eq!(names.value(0), "Alice Smith");
    }

    /// Δ.X2 (SQLite): older-redelivery is dropped at the
    /// idempotency gate. Same shape as the PG / DuckDB / Delta
    /// idempotency tests — different dialect, same semantics.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_cdc_redelivery_is_idempotent() {
        use crate::cdc::{CdcConfig, EnvelopeKind};
        use serde_json::json;
        use std::sync::Arc as StdArc;

        let backend: StdArc<dyn Backend> = StdArc::new(SQLiteBackend::open(":memory:").unwrap());
        create_sqlite_cdc_target(&backend).await;

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        let spec = sqlite_cdc_customers_spec();

        let _r1 = backend
            .run_cdc(
                &spec,
                sqlite_cdc_record_batch_from_json(&[json!({
                    "before": null,
                    "after": {"id": 1, "email": "a@x", "name": "Alice v2"},
                    "source": {"ts_ms": 200_i64},
                    "op": "c",
                    "ts_ms": 200_i64,
                })
                .as_object()
                .unwrap()
                .clone()]),
                &cdc,
                "sqlite_cdc_idem",
            )
            .await
            .expect("batch 1");

        let r2 = backend
            .run_cdc(
                &spec,
                sqlite_cdc_record_batch_from_json(&[json!({
                    "before": null,
                    "after": {"id": 1, "email": "a@x", "name": "Alice v1"},
                    "source": {"ts_ms": 100_i64},
                    "op": "c",
                    "ts_ms": 100_i64,
                })
                .as_object()
                .unwrap()
                .clone()]),
                &cdc,
                "sqlite_cdc_idem",
            )
            .await
            .expect("batch 2");
        assert_eq!(r2.creates, 0, "older redelivery must not apply");
        assert_eq!(r2.updates, 0);
        assert_eq!(
            r2.idempotent_skipped, 1,
            "idempotency gate must report the skip"
        );

        use futures_util::TryStreamExt;
        let stream = backend
            .read_arrow_stream("SELECT name FROM customers WHERE id = 1")
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "Alice v2");
    }

    /// Δ.X2 (SQLite): SchemaEvolutionPolicy::Fail must abort the
    /// batch when an `after` payload carries a column missing on
    /// the target spec.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_cdc_schema_fail_aborts() {
        use crate::cdc::{CdcConfig, EnvelopeKind, SchemaEvolutionPolicy};
        use serde_json::json;
        use std::sync::Arc as StdArc;

        let backend: StdArc<dyn Backend> = StdArc::new(SQLiteBackend::open(":memory:").unwrap());
        create_sqlite_cdc_target(&backend).await;
        let spec = sqlite_cdc_customers_spec();

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        cdc.schema_evolution = SchemaEvolutionPolicy::Fail;

        let err = backend
            .run_cdc(
                &spec,
                sqlite_cdc_record_batch_from_json(&[json!({
                    "before": null,
                    "after": {"id": 1, "email": "a@x", "name": "A", "phone": "+1"},
                    "op": "c",
                    "ts_ms": 1_i64,
                })
                .as_object()
                .unwrap()
                .clone()]),
                &cdc,
                "sqlite_cdc_fail",
            )
            .await
            .expect_err("Fail policy must error on unknown column");
        assert!(
            err.to_string().contains("phone"),
            "error must name the offending column"
        );
    }

    /// Δ.X2 (SQLite): soft-delete flips the configured column
    /// instead of removing the row, and the affected rows are
    /// counted as updates rather than deletes.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_cdc_soft_delete_flips_column() {
        use crate::cdc::{CdcConfig, DeleteMode, EnvelopeKind};
        use serde_json::json;
        use std::sync::Arc as StdArc;

        let backend: StdArc<dyn Backend> = StdArc::new(SQLiteBackend::open(":memory:").unwrap());
        backend
            .execute(
                "CREATE TABLE customers (\
                    id INTEGER PRIMARY KEY, \
                    email TEXT, \
                    name TEXT, \
                    deleted_at TEXT)",
            )
            .await
            .unwrap();

        let mut spec = sqlite_cdc_customers_spec();
        spec.columns.push(crate::types::ColumnSpec {
            name: "deleted_at".into(),
            ty: crate::types::ColumnType::TimestampTz,
            nullable: true,
            primary_key: false,
        });

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        cdc.delete_mode = DeleteMode::Soft {
            column: "deleted_at".into(),
        };

        let _r1 = backend
            .run_cdc(
                &spec,
                sqlite_cdc_record_batch_from_json(&[json!({
                    "before": null,
                    "after": {"id": 1, "email": "a@x", "name": "A", "deleted_at": null},
                    "op": "c",
                    "ts_ms": 100_i64,
                })
                .as_object()
                .unwrap()
                .clone()]),
                &cdc,
                "sqlite_cdc_soft",
            )
            .await
            .unwrap();

        let r2 = backend
            .run_cdc(
                &spec,
                sqlite_cdc_record_batch_from_json(&[json!({
                    "before": {"id": 1, "email": "a@x", "name": "A", "deleted_at": null},
                    "after":  null,
                    "op": "d",
                    "ts_ms": 200_i64,
                })
                .as_object()
                .unwrap()
                .clone()]),
                &cdc,
                "sqlite_cdc_soft",
            )
            .await
            .unwrap();
        assert_eq!(r2.deletes, 0, "soft-delete must not increment deletes");
        assert!(r2.updates >= 1, "soft-delete reports as an update");

        use futures_util::TryStreamExt;
        let stream = backend
            .read_arrow_stream("SELECT id FROM customers WHERE id = 1")
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "soft-delete preserves the row");

        let stream = backend
            .read_arrow_stream(
                "SELECT count(*) FROM customers WHERE id = 1 AND deleted_at IS NOT NULL",
            )
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 1, "deleted_at must be populated");
    }

    // --- Σ.XO: atomic batch + offsets (exactly-once primitive) -----------

    /// Column names deliberately avoid the `*key` suffix (downcast
    /// trap — house rule).
    fn xo_batch(idents: Vec<i64>) -> RecordBatch {
        use arrow_schema::{DataType as Dt, Field as F, Schema as S};
        let schema = std::sync::Arc::new(S::new(vec![F::new("ident", Dt::Int64, false)]));
        RecordBatch::try_new(schema, vec![std::sync::Arc::new(Int64Array::from(idents))]).unwrap()
    }

    fn xo_stream(batches: Vec<RecordBatch>) -> crate::backend::ArrowBatchStream {
        Box::pin(futures_util::stream::iter(batches.into_iter().map(Ok)))
    }

    /// Fresh target: capability probe answers "supported, nothing
    /// committed" even though `_ematix_offsets` doesn't exist yet —
    /// the write side creates it lazily.
    #[tokio::test(flavor = "multi_thread")]
    async fn xo_load_committed_offsets_without_table_is_supported_empty() {
        let b = SQLiteBackend::open(":memory:").unwrap();
        let loaded = b.load_committed_offsets("pipe-x").await.unwrap();
        assert_eq!(loaded, Some(Vec::new()));
    }

    /// Round-trip: the offsets stamped by the atomic write come back
    /// verbatim from `load_committed_offsets`, scoped by pipeline_id;
    /// a second write for the same (pipeline, source) upserts.
    #[tokio::test(flavor = "multi_thread")]
    async fn xo_write_with_offsets_round_trip_and_upsert() {
        let b = SQLiteBackend::open(":memory:").unwrap();
        b.execute("CREATE TABLE xo_sink (ident INTEGER)")
            .await
            .unwrap();
        let target = TargetTable {
            schema: "main".into(),
            name: "xo_sink".into(),
        };

        let written = b
            .write_arrow_stream_with_offsets(
                &target,
                xo_stream(vec![xo_batch(vec![1, 2])]),
                WriteMode::Append,
                "pipe-a",
                &[("src-1".into(), r#"{"0":5}"#.into())],
            )
            .await
            .unwrap();
        assert_eq!(written, Some(2));

        let loaded = b.load_committed_offsets("pipe-a").await.unwrap().unwrap();
        assert_eq!(
            loaded,
            vec![("src-1".to_string(), r#"{"0":5}"#.to_string())]
        );
        // Other pipelines see nothing.
        assert_eq!(
            b.load_committed_offsets("pipe-b").await.unwrap(),
            Some(Vec::new())
        );

        // Same (pipeline, source) again → the offsets row is
        // REPLACED, not duplicated; absent source ids are untouched.
        let written = b
            .write_arrow_stream_with_offsets(
                &target,
                xo_stream(vec![xo_batch(vec![3])]),
                WriteMode::Append,
                "pipe-a",
                &[
                    ("src-1".into(), r#"{"0":9}"#.into()),
                    ("src-2".into(), r#"{"1":4}"#.into()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(written, Some(1));
        let mut loaded = b.load_committed_offsets("pipe-a").await.unwrap().unwrap();
        loaded.sort();
        assert_eq!(
            loaded,
            vec![
                ("src-1".to_string(), r#"{"0":9}"#.to_string()),
                ("src-2".to_string(), r#"{"1":4}"#.to_string()),
            ]
        );
    }

    /// The load-bearing atomicity property: when the batch insert
    /// fails, the offsets upsert rolls back WITH it — a crash can
    /// never pair written rows with un-advanced offsets or vice
    /// versa. (Int32 is an unsupported Arrow type on the SQLite
    /// write path, so the second batch fails mid-transaction.)
    #[tokio::test(flavor = "multi_thread")]
    async fn xo_failed_write_rolls_back_rows_and_offsets_together() {
        use arrow_schema::{DataType as Dt, Field as F, Schema as S};

        let b = SQLiteBackend::open(":memory:").unwrap();
        b.execute("CREATE TABLE xo_sink (ident INTEGER)")
            .await
            .unwrap();
        let target = TargetTable {
            schema: "main".into(),
            name: "xo_sink".into(),
        };

        // Seed one committed write so we can verify failure leaves
        // the PREVIOUS offsets intact (not just "no offsets").
        b.write_arrow_stream_with_offsets(
            &target,
            xo_stream(vec![xo_batch(vec![1])]),
            WriteMode::Append,
            "pipe-a",
            &[("src-1".into(), r#"{"0":1}"#.into())],
        )
        .await
        .unwrap();

        let bad_schema = std::sync::Arc::new(S::new(vec![F::new("ident", Dt::Int32, false)]));
        let bad_batch = RecordBatch::try_new(
            bad_schema,
            vec![std::sync::Arc::new(arrow_array::Int32Array::from(vec![7]))],
        )
        .unwrap();
        let err = b
            .write_arrow_stream_with_offsets(
                &target,
                xo_stream(vec![xo_batch(vec![2]), bad_batch]),
                WriteMode::Append,
                "pipe-a",
                &[("src-1".into(), r#"{"0":2}"#.into())],
            )
            .await
            .expect_err("unsupported Arrow type must fail the write");
        assert!(
            err.to_string().contains("unsupported Arrow type"),
            "got: {err}"
        );

        // Neither the rows nor the offsets from the failed call are
        // visible; the connection stays usable (no dangling BEGIN).
        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream("SELECT count(*) FROM xo_sink")
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = s.try_collect().await.unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 1, "only the seed row survives the rollback");
        let loaded = b.load_committed_offsets("pipe-a").await.unwrap().unwrap();
        assert_eq!(
            loaded,
            vec![("src-1".to_string(), r#"{"0":1}"#.to_string())],
            "offsets must still be the seed write's"
        );
    }

    /// Empty offsets slice = plain transactional write; the meta
    /// table is not created (no surprise tables in targets whose
    /// sources can't snapshot offsets).
    #[tokio::test(flavor = "multi_thread")]
    async fn xo_empty_offsets_skips_meta_table() {
        let b = SQLiteBackend::open(":memory:").unwrap();
        b.execute("CREATE TABLE xo_sink (ident INTEGER)")
            .await
            .unwrap();
        let target = TargetTable {
            schema: "main".into(),
            name: "xo_sink".into(),
        };
        let written = b
            .write_arrow_stream_with_offsets(
                &target,
                xo_stream(vec![xo_batch(vec![1])]),
                WriteMode::Append,
                "pipe-a",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(written, Some(1));
        use futures_util::TryStreamExt;
        let s = b
            .read_arrow_stream(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='_ematix_offsets'",
            )
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = s.try_collect().await.unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            n, 0,
            "_ematix_offsets must not exist after an empty-offsets write"
        );
    }
}
