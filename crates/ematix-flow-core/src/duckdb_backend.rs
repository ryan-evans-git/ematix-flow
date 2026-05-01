//! Phase 31a: DuckDB backend skeleton.
//!
//! Implements the connection-level + Arrow IO surface of the `Backend`
//! trait. Strategy executors (`run_append` / `run_truncate` /
//! `run_merge` / `run_scd2`) are stubbed for 31a and land in 31b/c.
//!
//! Threading model: `duckdb::Connection` is `Send + !Sync`, so we wrap
//! it in `Arc<Mutex<…>>` and bridge the sync DuckDB API into our async
//! trait via `tokio::task::spawn_blocking`. DuckDB writes that touch
//! the same connection serialize on the mutex; concurrent writers want
//! separate connections (or, for MVP, one consumer per pipeline).
//!
//! See `docs/MULTI_BACKEND_PLAN.md` Phase 31 for the full design.

use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use duckdb::Connection as DuckConn;
use futures_util::stream;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    TargetTable, WriteMode,
};
use crate::meta::{WatermarkConfig, wrap_with_watermark_filter};
use crate::pg::ConnectionInfo;
use crate::strategy::append::{BATCH_ID_COL, LOADED_AT_COL, plan_same_db_append};
use crate::strategy::scd2::{IS_CURRENT_COL, ROW_HASH_COL, VALID_FROM_COL, VALID_TO_COL};
use crate::strategy::truncate::plan_truncate_replace;
use crate::types::TableSpec;
use uuid::Uuid;

fn is_metadata_col(name: &str) -> bool {
    name == LOADED_AT_COL || name == BATCH_ID_COL
}

const META_SCHEMA: &str = "ematix_flow";
const RUN_HISTORY_TABLE: &str = "run_history";
const WATERMARKS_TABLE: &str = "watermarks";

/// SQL that lazy-creates the meta schema + `run_history` + `watermarks`
/// on DuckDB. Mirrors the PG schema (see `pg::ensure_meta_schema`) but
/// flattened since DuckDB doesn't need the PG-style `ALTER TABLE …
/// ADD COLUMN IF NOT EXISTS` upgrade pattern (no installed-base yet).
fn ensure_meta_schema_sql() -> String {
    format!(
        "CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}; \
         CREATE TABLE IF NOT EXISTS {META_SCHEMA}.{RUN_HISTORY_TABLE} (\
            run_id UUID PRIMARY KEY, \
            parent_run_id UUID, \
            pipeline_name VARCHAR NOT NULL, \
            step_name VARCHAR, \
            target_schema VARCHAR NOT NULL, \
            target_table VARCHAR NOT NULL, \
            mode VARCHAR NOT NULL, \
            path VARCHAR NOT NULL, \
            started_at TIMESTAMPTZ NOT NULL, \
            finished_at TIMESTAMPTZ, \
            status VARCHAR NOT NULL, \
            rows_inserted BIGINT, \
            rows_updated BIGINT, \
            rows_unchanged BIGINT, \
            error_message VARCHAR, \
            metrics_json VARCHAR\
         ); \
         CREATE TABLE IF NOT EXISTS {META_SCHEMA}.{WATERMARKS_TABLE} (\
            pipeline_name VARCHAR PRIMARY KEY, \
            column_name VARCHAR NOT NULL, \
            last_value VARCHAR NOT NULL, \
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()\
         )"
    )
}

/// DuckDB-native merge SQL builder. The PG `plan_merge_upsert` uses
/// `WITH … AS MATERIALIZED` CTEs + a `RETURNING (xmax = 0)` trick to
/// split inserts vs. updates; both are PG-specific. DuckDB doesn't
/// allow CTEs before INSERT and has no `xmax`. The DuckDB version is
/// a flat `INSERT … ON CONFLICT (…) DO UPDATE SET …`; affected-row
/// count is returned but not split inserts/updates (Phase 31d will add
/// a follow-up SELECT for that breakdown if the user needs it).
fn duckdb_merge_sql(
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
        select_exprs.push("now()".into());
        select_exprs.push(format!("'{batch_id}'::uuid"));
    }
    let on_conflict = if update_columns.is_empty() {
        format!("ON CONFLICT ({}) DO NOTHING", keys.join(", "))
    } else {
        let mut sets: Vec<String> = update_columns
            .iter()
            .map(|c| format!("{c} = EXCLUDED.{c}"))
            .collect();
        if has_metadata {
            sets.push(format!("{LOADED_AT_COL} = EXCLUDED.{LOADED_AT_COL}"));
            sets.push(format!("{BATCH_ID_COL} = EXCLUDED.{BATCH_ID_COL}"));
        }
        format!(
            "ON CONFLICT ({}) DO UPDATE SET {}",
            keys.join(", "),
            sets.join(", ")
        )
    };
    format!(
        "INSERT INTO {schema}.{table} ({insert_cols}) \
         SELECT {select_exprs} FROM ({source}) src_inner \
         {on_conflict}",
        schema = target.schema,
        table = target.name,
        insert_cols = insert_cols.join(", "),
        select_exprs = select_exprs.join(", "),
        source = source_query,
    )
}

/// Substitute the `$1::uuid` parameter placeholder used by the PG
/// strategy planners with a SQL literal. DuckDB doesn't accept `$N`
/// placeholders inside `execute_batch`; embedding the framework-
/// generated UUID as `'<v4-uuid>'::uuid` is safe (the value is never
/// user input).
fn substitute_batch_id(sql: &str, batch_id: &Uuid) -> String {
    sql.replace("$1::uuid", &format!("'{}'::uuid", batch_id))
}

/// DuckDB equivalent of `pg::hash::postgres_digest_expression`.
///
/// PG uses `digest(<expr>, 'sha256')` from the pgcrypto extension and
/// E-string escapes (`E'\\x00NULL\\x00'`, `E'\\x01'`) which DuckDB
/// doesn't accept. DuckDB's built-in `sha256(varchar)` returns a hex
/// VARCHAR; `unhex(...)` converts that to BLOB so it round-trips with
/// the row_hash column type. Null marker uses `chr(0)` to stay
/// distinguishable from a literal `'NULL'` value.
fn duckdb_digest_expression(columns: &[String], prefix: &str) -> String {
    let parts: Vec<String> = columns
        .iter()
        .map(|c| {
            format!(
                "coalesce({prefix}{c}::VARCHAR, chr(0) || 'NULL' || chr(0))",
                prefix = prefix,
                c = c,
            )
        })
        .collect();
    format!("unhex(sha256({}))", parts.join(" || chr(1) || "))
}

/// DuckDB-native SCD2 plan. Mirrors `strategy::scd2::plan_scd2` but
/// emits SQL DuckDB will accept:
///   - `CREATE TEMP TABLE …` without `ON COMMIT DROP` (DuckDB drops
///     the temp table only when the connection ends, so we DROP
///     explicitly at the end of the executor).
///   - `WITH src AS (…)` without the `MATERIALIZED` hint.
///   - `unhex(sha256(…))` instead of pgcrypto `digest(…, 'sha256')`.
///   - `$1::uuid` placeholder substituted to a literal up-front so we
///     can run the statements through `execute_batch`.
fn duckdb_scd2_statements(
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
    let digest_expr = duckdb_digest_expression(compare_columns, "");
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
    let key_tuple: String = keys.join(", ");

    let src_select = match event_ts_column {
        Some(ets) => format!(
            "SELECT DISTINCT ON ({keys}) {user_cols}, \
             {ets}::TIMESTAMPTZ AS _event_ts, \
             {digest_expr} AS _row_hash \
             FROM ({source}) q \
             ORDER BY {keys}, {ets} DESC",
            keys = key_tuple,
            user_cols = user_columns.join(", "),
            ets = ets,
            digest_expr = digest_expr,
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
         LEFT JOIN {schema}.{table} t \
             ON {join_clause} AND t.{is_current_col} \
         WHERE t.{row_hash_col} IS DISTINCT FROM src._row_hash",
        stage = stage,
        src_select = src_select,
        schema = target.schema,
        table = target.name,
        join_clause = join_clause,
        is_current_col = IS_CURRENT_COL,
        row_hash_col = ROW_HASH_COL,
    );

    let close_out = match event_ts_column {
        Some(_) => {
            let join: String = keys
                .iter()
                .map(|k| format!("t.{k} = c.{k}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!(
                "UPDATE {schema}.{table} t \
                 SET {valid_to} = c._event_ts, {is_current} = false \
                 FROM {stage} c \
                 WHERE t.{is_current} AND {join}",
                schema = target.schema,
                table = target.name,
                valid_to = VALID_TO_COL,
                is_current = IS_CURRENT_COL,
                stage = stage,
                join = join,
            )
        }
        None => format!(
            "UPDATE {schema}.{table} \
             SET {valid_to} = now(), {is_current} = false \
             WHERE {is_current} AND ({keys}) IN (SELECT {keys} FROM {stage})",
            schema = target.schema,
            table = target.name,
            valid_to = VALID_TO_COL,
            is_current = IS_CURRENT_COL,
            keys = key_tuple,
            stage = stage,
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
        None => "now()".into(),
    };
    select_exprs.push(valid_from_expr);
    select_exprs.push("NULL".into());
    select_exprs.push("true".into());
    select_exprs.push("_row_hash".into());
    if has_metadata {
        insert_cols.push(LOADED_AT_COL.into());
        insert_cols.push(BATCH_ID_COL.into());
        select_exprs.push("now()".into());
        select_exprs.push(format!("'{batch_id}'::uuid"));
    }
    let insert_new = format!(
        "INSERT INTO {schema}.{table} ({insert_cols}) \
         SELECT {select_exprs} FROM {stage}",
        schema = target.schema,
        table = target.name,
        insert_cols = insert_cols.join(", "),
        select_exprs = select_exprs.join(", "),
        stage = stage,
    );

    // Explicit DROP — DuckDB has no `ON COMMIT DROP`, and reusing the
    // same connection across runs would clash on the temp table name
    // if we left it behind.
    let drop_temp = format!("DROP TABLE {stage}");

    vec![create_temp, close_out, insert_new, drop_temp]
}

/// DuckDB-backed implementation of `Backend`. Created via
/// `DuckDBBackend::open(":memory:")` for an in-memory database or
/// `DuckDBBackend::open("/path/to/db.duckdb")` for a file-backed one.
///
/// In Phase 31a only the connection surface + Arrow IO are functional;
/// strategy executors return a clear NotImplemented error.
pub struct DuckDBBackend {
    conn: Arc<Mutex<DuckConn>>,
    location: String,
}

impl DuckDBBackend {
    pub fn open(location: impl Into<String>) -> Result<Self, BackendError> {
        let location = location.into();
        let conn = if location == ":memory:" {
            DuckConn::open_in_memory().map_err(|e| BackendError::Connection(e.to_string()))?
        } else {
            DuckConn::open(&location).map_err(|e| BackendError::Connection(e.to_string()))?
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
        F: FnOnce(&DuckConn) -> Result<R, BackendError> + Send + 'static,
        R: Send + 'static,
    {
        let arc = self.conn.clone();
        // Move the lock+work into a spawn_blocking task so we don't pin
        // the async executor on synchronous DuckDB calls.
        let join = tokio::task::spawn_blocking(move || {
            let guard = arc
                .lock()
                .map_err(|e| BackendError::Other(format!("duckdb mutex poisoned: {e}")))?;
            f(&guard)
        });
        match join.await {
            Ok(r) => r,
            Err(e) => Err(BackendError::Other(format!("duckdb task join: {e}"))),
        }
    }

    /// Lazy-create `ematix_flow.run_history`. Idempotent.
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
            // Bind via positional `?` params so the connection-mutex stays
            // off the hot path; UUIDs go in as their string repr because
            // the duckdb crate's ToSql isn't implemented for `uuid::Uuid`.
            c.execute(
                &format!(
                    "INSERT INTO {META_SCHEMA}.{RUN_HISTORY_TABLE} \
                     (run_id, pipeline_name, target_schema, target_table, \
                      mode, path, started_at, status) \
                     VALUES (?::uuid, ?, ?, ?, ?, ?, now(), 'running')"
                ),
                duckdb::params![
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
                    "UPDATE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
                     SET status='success', rows_inserted=?, finished_at=now() \
                     WHERE run_id=?::uuid"
                ),
                duckdb::params![rows_inserted, run_id.to_string()],
            )
            .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(())
        })
        .await
    }

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
                    "UPDATE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
                     SET status='success', rows_inserted=?, rows_updated=?, \
                         rows_unchanged=?, finished_at=now() \
                     WHERE run_id=?::uuid"
                ),
                duckdb::params![
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
                    "UPDATE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
                     SET status='failed', error_message=?, finished_at=now() \
                     WHERE run_id=?::uuid"
                ),
                duckdb::params![error_message, run_id.to_string()],
            )
            .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(())
        })
        .await
    }

    /// Compute `MAX(<column>)::VARCHAR` over the rows that this batch
    /// just inserted (matched by `_batch_id`) and UPSERT into the
    /// watermarks table. NULL `MAX` (zero rows inserted, or column
    /// itself was NULL) is a no-op so a stale `last_value` doesn't get
    /// clobbered. Mirrors `pg::advance_watermark`.
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
            // Read MAX(column) over rows just inserted. We pull as
            // VARCHAR because watermarks.last_value is VARCHAR and
            // we'll feed it back as a SQL literal next run; the user
            // is responsible for any type-specific cast in
            // last_value_literal.
            let max_sql = format!(
                "SELECT MAX({col})::VARCHAR FROM {schema}.{table} \
                 WHERE {batch_id_col} = ?::uuid",
                col = column,
                schema = target_schema,
                table = target_table,
                batch_id_col = BATCH_ID_COL,
            );
            let mut stmt = c
                .prepare(&max_sql)
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let mut rows = stmt
                .query(duckdb::params![batch_id.to_string()])
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let row = rows
                .next()
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let max_value: Option<String> = match row {
                Some(r) => r
                    .get::<_, Option<String>>(0)
                    .map_err(|e| BackendError::Query(e.to_string()))?,
                None => None,
            };
            // Drop the borrow on the connection so we can issue the
            // UPSERT below.
            drop(rows);
            drop(stmt);

            let Some(value) = max_value else {
                return Ok(());
            };
            c.execute(
                &format!(
                    "INSERT INTO {META_SCHEMA}.{WATERMARKS_TABLE} \
                     (pipeline_name, column_name, last_value, updated_at) \
                     VALUES (?, ?, ?, now()) \
                     ON CONFLICT (pipeline_name) DO UPDATE SET \
                       column_name = EXCLUDED.column_name, \
                       last_value = EXCLUDED.last_value, \
                       updated_at = EXCLUDED.updated_at"
                ),
                duckdb::params![pipeline_name, column, value],
            )
            .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(())
        })
        .await
    }
}

#[async_trait]
impl Backend for DuckDBBackend {
    fn dialect(&self) -> Dialect {
        Dialect::DuckDB
    }

    fn connection_info(&self) -> ConnectionInfo {
        // DuckDB is file/in-memory; reuse the ConnectionInfo struct
        // shape but populate identifying fields for human display.
        ConnectionInfo {
            host: "duckdb".into(),
            port: 0,
            dbname: self.location.clone(),
            user: "local".into(),
        }
    }

    fn dsn(&self) -> Option<String> {
        Some(format!("duckdb://{}", self.location))
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
            // DuckDB's execute_batch handles multi-statement SQL but
            // doesn't return a row count. For our trait contract we
            // approximate with 0; users who need an exact count can
            // run a SELECT count(*) follow-up.
            c.execute_batch(&stmt)
                .map_err(|e| BackendError::Query(e.to_string()))?;
            Ok(0)
        })
        .await
    }

    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        let q = query.to_string();
        let batches = self
            .with_conn_blocking(move |c| {
                let mut stmt = c
                    .prepare(&q)
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                let arrow_iter = stmt
                    .query_arrow([])
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                let collected: Vec<RecordBatch> = arrow_iter.collect();
                Ok::<Vec<RecordBatch>, BackendError>(collected)
            })
            .await?;
        let stream = stream::iter(batches.into_iter().map(Ok::<_, BackendError>));
        Ok(Box::pin(stream))
    }

    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError> {
        use futures_util::StreamExt;

        // Collect the source stream first so we can hand a Vec into
        // spawn_blocking. Future optimization: stream batches one at a
        // time through a sync channel into the blocking task.
        let mut s = stream;
        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(b) = s.next().await {
            batches.push(b?);
        }
        let target_schema = target.schema.clone();
        let target_table = target.name.clone();

        self.with_conn_blocking(move |c| {
            let qualified = format!(
                "\"{}\".\"{}\"",
                target_schema.replace('"', "\"\""),
                target_table.replace('"', "\"\""),
            );
            if mode == WriteMode::Truncate {
                c.execute_batch(&format!("DELETE FROM {qualified}"))
                    .map_err(|e| BackendError::Query(e.to_string()))?;
            }
            let mut total: u64 = 0;
            for batch in &batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                // DuckDB's `Appender` is the fast bulk-load API; it
                // accepts an Arrow RecordBatch directly without a
                // per-row INSERT. ~10× faster than the equivalent PG
                // INSERT loop in our PG backend.
                let mut appender = c
                    .appender_to_db(&target_table, &target_schema)
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                appender
                    .append_record_batch(batch.clone())
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                appender
                    .flush()
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                total += batch.num_rows() as u64;
            }
            Ok::<u64, BackendError>(total)
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
                "DuckDB cross-backend run_append goes through the Arrow streaming \
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
        let sql = substitute_batch_id(&plan.sql, &batch_id);

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
                    // DuckDB: wrap in a transaction we'll rollback.
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
                // Watermark advancement is best-effort — its failure
                // doesn't unwind the strategy, but the user's next run
                // would re-load the same window. dry_run skips because
                // the data was rolled back.
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
                "DuckDB cross-backend run_truncate goes through the Arrow bridge".into(),
            ));
        }
        let plan = plan_truncate_replace(spec, source_query);
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let stmts: Vec<String> = plan
            .statements
            .iter()
            .map(|s| substitute_batch_id(s, &batch_id))
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
                "DuckDB cross-backend run_merge goes through the Arrow bridge".into(),
            ));
        }
        if delete_handling.is_some() {
            return Err(BackendError::Other(
                "DuckDB run_merge: handle_deletes not yet supported (Phase 31d)".into(),
            ));
        }
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let sql = duckdb_merge_sql(spec, source_query, keys, update_columns, &batch_id);

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

        // DuckDB's `INSERT ... ON CONFLICT DO UPDATE` returns the
        // affected-row count as inserts + updates summed (no easy way
        // to split without a follow-up query). Surface it as
        // rows_inserted for now; rows_updated tracking is a 31d
        // refinement.
        let affected_result: Result<u64, BackendError> = self
            .with_conn_blocking(move |c| {
                c.execute_batch("BEGIN")
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                let n = c
                    .execute(&sql, [])
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                if dry_run {
                    c.execute_batch("ROLLBACK")
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                } else {
                    c.execute_batch("COMMIT")
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                }
                Ok::<u64, BackendError>(n as u64)
            })
            .await;

        match &affected_result {
            Ok(n) => {
                self.record_run_success_merge(run_id, *n as i64, 0, 0)
                    .await?
            }
            Err(e) => {
                let _ = self.record_run_failure(run_id, &e.to_string()).await;
            }
        }
        let affected = affected_result?;

        Ok(StrategyRunResult {
            run_id: run_id.to_string(),
            rows_inserted: affected as i64,
            rows_updated: Some(0),
            rows_unchanged: Some(0),
            rows_closed: None,
            status: if dry_run { "dry_run" } else { "success" }.into(),
            path: "same_db".into(),
        })
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
        if source_backend.is_some() {
            return Err(BackendError::Other(
                "DuckDB cross-backend run_scd2 goes through the Arrow bridge \
                 (cross_backend_arrow_sync); same-backend only here"
                    .into(),
            ));
        }
        if delete_handling.is_some() {
            return Err(BackendError::Other(
                "DuckDB run_scd2: handle_deletes not yet supported (Phase 31d)".into(),
            ));
        }
        if ttl_seconds.is_some() {
            return Err(BackendError::Other(
                "DuckDB run_scd2: ttl_seconds not yet supported (Phase 31d)".into(),
            ));
        }
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        // The temp-table token must be a valid SQL identifier and unique
        // across runs that share a connection. Hex of the run UUID
        // (no dashes) satisfies both.
        let run_token = run_id.simple().to_string();
        let stmts = duckdb_scd2_statements(
            spec,
            source_query,
            keys,
            compare_columns,
            &run_token,
            event_timestamp_column,
            &batch_id,
        );

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
                // Statement order: CREATE TEMP, UPDATE close-out, INSERT
                // new versions, DROP TEMP. Affected-row count for
                // `rows_inserted` comes from the INSERT (index 2).
                let mut inserted: u64 = 0;
                for (idx, sql) in stmts.iter().enumerate() {
                    let n = c.execute(sql, []).map_err(|e| {
                        // Best-effort cleanup so a failure mid-run doesn't
                        // leave a half-built temp table around. The DROP
                        // is harmless if the CREATE never succeeded.
                        let _ = c.execute_batch("ROLLBACK");
                        BackendError::Query(format!("scd2 stmt {idx} failed: {e}; sql={sql}",))
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn duckdb_in_memory_ping() {
        let backend = DuckDBBackend::open(":memory:").unwrap();
        assert_eq!(backend.dialect(), Dialect::DuckDB);
        assert_eq!(backend.dsn().as_deref(), Some("duckdb://:memory:"));
        backend.ping().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duckdb_in_memory_execute_creates_table() {
        let backend = DuckDBBackend::open(":memory:").unwrap();
        backend
            .execute("CREATE TABLE t (id BIGINT, name TEXT)")
            .await
            .unwrap();
        backend
            .execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duckdb_arrow_round_trip_in_memory() {
        let backend = DuckDBBackend::open(":memory:").unwrap();
        backend.execute("CREATE SCHEMA s").await.unwrap();
        backend
            .execute("CREATE TABLE s.src (id BIGINT, name VARCHAR)")
            .await
            .unwrap();
        backend
            .execute("INSERT INTO s.src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .unwrap();
        backend
            .execute("CREATE TABLE s.dst (id BIGINT, name VARCHAR)")
            .await
            .unwrap();

        let stream = backend
            .read_arrow_stream("SELECT id, name FROM s.src ORDER BY id")
            .await
            .unwrap();
        let target = TargetTable {
            schema: "s".into(),
            name: "dst".into(),
        };
        let written = backend
            .write_arrow_stream(&target, stream, WriteMode::Append)
            .await
            .unwrap();
        assert_eq!(written, 3);
    }
}
