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
use crate::meta::{
    WatermarkConfig, build_hard_delete_sql, build_scd2_close_missing_sql,
    wrap_with_watermark_filter,
};
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

/// Δ.X2: per-PK admission gate, mirror of the PG
/// `cdc_idempotency` table. Same `(pipeline, pk_json)` →
/// `last_seen_ts_ms` schema; the SQL the run_cdc executor emits
/// is identical bar dialect quirks.
const CDC_IDEMPOTENCY_TABLE: &str = "cdc_idempotency";

/// SQL that lazy-creates the meta schema + `run_history` +
/// `watermarks` + `cdc_idempotency` on DuckDB. Mirrors the PG
/// schema (see `pg::ensure_meta_schema`) but flattened since
/// DuckDB doesn't need the PG-style `ALTER TABLE … ADD COLUMN IF
/// NOT EXISTS` upgrade pattern (no installed-base yet).
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
         ); \
         CREATE TABLE IF NOT EXISTS {META_SCHEMA}.{CDC_IDEMPOTENCY_TABLE} (\
            pipeline_name VARCHAR NOT NULL, \
            pk_json VARCHAR NOT NULL, \
            last_seen_ts_ms BIGINT NOT NULL, \
            updated_at TIMESTAMPTZ NOT NULL, \
            PRIMARY KEY (pipeline_name, pk_json)\
         )"
    )
}

/// Δ.X2: DuckDB SQL type for the `from_json` structure spec
/// (DuckDB's analog to Postgres's `jsonb_populate_record`). The
/// per-target run_cdc executor builds a struct-spec string from
/// the target's TableSpec and embeds it once; per-event JSON
/// payloads then feed `from_json('{...}', '<struct-spec>')`.
fn column_type_to_duckdb_sql(ct: &crate::types::ColumnType) -> String {
    use crate::types::ColumnType as CT;
    match ct {
        CT::SmallInt => "SMALLINT".into(),
        CT::Integer => "INTEGER".into(),
        CT::BigInt => "BIGINT".into(),
        CT::Float => "REAL".into(),
        CT::Double => "DOUBLE".into(),
        CT::Boolean => "BOOLEAN".into(),
        // DuckDB has both VARCHAR and a dedicated JSON type. Carry
        // text-shaped columns as VARCHAR — the same shape Delta's
        // executor uses, and what `from_json` produces for `"VARCHAR"`
        // structure entries.
        CT::Text | CT::Json | CT::Jsonb => "VARCHAR".into(),
        CT::String { length } => format!("VARCHAR({length})"),
        CT::Uuid => "UUID".into(),
        CT::Bytes => "BLOB".into(),
        CT::Date => "DATE".into(),
        CT::Timestamp => "TIMESTAMP".into(),
        CT::TimestampTz => "TIMESTAMPTZ".into(),
        CT::Numeric { precision, scale } => format!("DECIMAL({precision},{scale})"),
    }
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

/// DuckDB equivalent of `meta::build_scd2_ttl_expire_sql`. PG uses
/// `make_interval(secs => N)` and TIMESTAMPTZ - INTERVAL arithmetic;
/// DuckDB's `(TIMESTAMP WITH TIME ZONE, INTERVAL) -> ?` overload
/// doesn't exist, so we compare epoch seconds instead.
fn duckdb_scd2_ttl_expire_sql(target_schema: &str, target_table: &str, ttl_seconds: i64) -> String {
    format!(
        "UPDATE {schema}.{table} \
         SET valid_to = now(), is_current = false \
         WHERE is_current \
           AND epoch(valid_from) < epoch(now()) - {ttl_seconds}",
        schema = target_schema,
        table = target_table,
        ttl_seconds = ttl_seconds,
    )
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

    /// Δ.X2: per-event apply loop for DuckDB-target CDC.
    ///
    /// Same architectural shape as `pg::PgPool::run_cdc`:
    ///   1. Lazy-create the meta schema (idempotency table lives
    ///      under `ematix_flow.cdc_idempotency`).
    ///   2. Schema-evolution `Fail` policy: pre-flight `after`
    ///      payload keys against the spec; abort the whole batch
    ///      on first unknown.
    ///   3. Per-event in one transaction: hit the idempotency
    ///      gate, then dispatch on `event.op` to UPSERT / DELETE
    ///      / soft-delete via prepared statements that re-bind
    ///      JSON parameters per call.
    ///
    /// Differs from PG in that DuckDB has no `jsonb_populate_record`.
    /// We use DuckDB's `from_json('<json>', '<struct-spec>')` —
    /// the struct-spec string is built once per batch from
    /// `target_spec.columns` (`column_type_to_duckdb_sql`).
    /// Per-event, the JSON payload is bound as a parameter.
    pub async fn run_cdc_inner(
        &self,
        target_spec: &crate::types::TableSpec,
        events: Vec<crate::cdc::CdcEvent>,
        cdc_config: &crate::cdc::CdcConfig,
        pipeline_name: &str,
        skipped: i64,
    ) -> Result<crate::backend::CdcRunResult, BackendError> {
        use crate::cdc::{CdcOp, DeleteMode, SchemaEvolutionPolicy};

        self.ensure_meta_schema().await?;

        // Single-PK only for the first cut (matches PG PR 3 scope).
        let pk_col = target_spec
            .columns
            .iter()
            .find(|c| c.primary_key)
            .ok_or_else(|| {
                BackendError::Other(format!(
                    "DuckDB run_cdc: target {}.{} has no primary-key column \
                     (Δ.X2 v1 supports single-PK targets; declare PK on \
                     @ematix.table or [target.table].primary_key)",
                    target_spec.schema, target_spec.name
                ))
            })?;
        let pk_name = pk_col.name.clone();

        // Schema-evolution Fail policy: pre-flight check against
        // the spec's columns. Skip relies on DuckDB's auto-add
        // behavior on INSERT (matches PG PR 5's policy semantics).
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
                                 payload for pipeline '{pipeline_name}' on DuckDB target \
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

        // Build the from_json struct-spec once. DuckDB's
        // `from_json(text, '{"col": "TYPE", ...}')` parses + types
        // every column to its target type, identically to how
        // PG's `jsonb_populate_record(NULL::table, $1::jsonb)`
        // round-trips through the table type.
        let struct_spec = struct_spec_from_columns(target_spec);
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

        // Pre-build SQL templates. Each is parameterised on a
        // single `?` (the JSON payload string); per-event we
        // re-bind via the prepared statement.
        // Alias the from_json struct then `.*` it through a
        // subquery: DuckDB's parser doesn't accept
        // `(from_json(...)).*` directly, but does accept
        // `SELECT s.* FROM (SELECT from_json(...) AS s)`.
        let select_struct = format!("SELECT s.* FROM (SELECT from_json(?, '{struct_spec}') AS s)");
        let upsert_sql = if non_pk_cols.is_empty() {
            // PK-only target: nothing to update on conflict.
            format!(
                "INSERT INTO {qualified} ({cols}) {select_struct} \
                 ON CONFLICT (\"{pk}\") DO NOTHING",
                cols = user_cols
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", "),
                pk = pk_name.replace('"', "\"\""),
            )
        } else {
            let set_clause = non_pk_cols
                .iter()
                .map(|c| format!("\"{c}\" = EXCLUDED.\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "INSERT INTO {qualified} ({cols}) {select_struct} \
                 ON CONFLICT (\"{pk}\") DO UPDATE SET {set_clause}",
                cols = user_cols
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", "),
                pk = pk_name.replace('"', "\"\""),
            )
        };

        // DELETE — hard or soft per cdc_config.delete_mode. Pulls
        // the PK out of the same `from_json` struct so the type
        // coercion is identical to the upsert path.
        let delete_sql = match &cdc_config.delete_mode {
            DeleteMode::Hard => format!(
                "DELETE FROM {qualified} WHERE \"{pk}\" = \
                 (SELECT s.\"{pk}\" FROM (SELECT from_json(?, '{struct_spec}') AS s))",
                pk = pk_name.replace('"', "\"\""),
            ),
            DeleteMode::Soft { column } => format!(
                "UPDATE {qualified} SET \"{col}\" = current_timestamp \
                 WHERE \"{pk}\" = \
                 (SELECT s.\"{pk}\" FROM (SELECT from_json(?, '{struct_spec}') AS s))",
                pk = pk_name.replace('"', "\"\""),
                col = column.replace('"', "\"\""),
            ),
        };

        // Idempotency gate. Same shape as PG: INSERT … ON
        // CONFLICT DO UPDATE … WHERE existing.last_seen_ts_ms <
        // EXCLUDED.last_seen_ts_ms RETURNING 1. Empty RETURNING
        // = redelivery; the data write is skipped.
        let gate_sql = format!(
            "INSERT INTO {META_SCHEMA}.{CDC_IDEMPOTENCY_TABLE} \
                 (pipeline_name, pk_json, last_seen_ts_ms, updated_at) \
             VALUES (?, ?, ?, current_timestamp) \
             ON CONFLICT (pipeline_name, pk_json) DO UPDATE \
                 SET last_seen_ts_ms = EXCLUDED.last_seen_ts_ms, \
                     updated_at = EXCLUDED.updated_at \
                 WHERE {META_SCHEMA}.{CDC_IDEMPOTENCY_TABLE}.last_seen_ts_ms \
                       < EXCLUDED.last_seen_ts_ms \
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
                        // Build the JSON payload for the data write.
                        // For c/r/u: serialize `after`.
                        // For d: synthesize { pk: event.key }.
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

                        // Idempotency gate. ts_ms-less events bypass
                        // (matches PG semantics — the user accepts
                        // duplicate-suppression-off in that case).
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
                                        .query(duckdb::params![
                                            &pipeline_name_owned,
                                            &pk_canon,
                                            ts_ms,
                                        ])
                                        .map_err(|e| {
                                            BackendError::Query(format!("duckdb cdc gate: {e}"))
                                        })?;
                                    let admitted = rows
                                        .next()
                                        .map_err(|e| {
                                            BackendError::Query(format!(
                                                "duckdb cdc gate read: {e}"
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
                                    .execute(duckdb::params![&payload_json])
                                    .map_err(|e| {
                                        BackendError::Query(format!("duckdb cdc upsert: {e}"))
                                    })?;
                                creates += n as i64;
                            }
                            CdcOp::Update => {
                                let n = upsert_stmt
                                    .execute(duckdb::params![&payload_json])
                                    .map_err(|e| {
                                        BackendError::Query(format!(
                                            "duckdb cdc update-as-upsert: {e}"
                                        ))
                                    })?;
                                updates += n as i64;
                            }
                            CdcOp::Delete => {
                                let n = delete_stmt
                                    .execute(duckdb::params![&payload_json])
                                    .map_err(|e| {
                                        BackendError::Query(format!("duckdb cdc delete: {e}"))
                                    })?;
                                // Soft-delete is wired through an
                                // UPDATE, so the affected rows
                                // belong under `updates` — only
                                // hard-delete increments `deletes`.
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

/// Δ.X2: build the `from_json` structure-specifier for a target's
/// columns. Output is the JSON-shaped `{"col": "TYPE", ...}` that
/// DuckDB's `from_json` consumes; types come from
/// `column_type_to_duckdb_sql`. Keeps the per-event SQL templates
/// hot — the spec is built once per batch and embedded as a SQL
/// literal, while only the JSON payload changes per event.
fn struct_spec_from_columns(spec: &crate::types::TableSpec) -> String {
    let mut buf = String::from("{");
    let mut first = true;
    for col in &spec.columns {
        if !first {
            buf.push_str(", ");
        }
        first = false;
        buf.push_str(&format!(
            "\"{}\": \"{}\"",
            col.name.replace('"', "\\\""),
            column_type_to_duckdb_sql(&col.ty)
        ));
    }
    buf.push('}');
    buf
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

    fn config(&self) -> crate::backend::BackendConfig {
        crate::backend::BackendConfig::DuckDb(crate::backend::DuckDbConfig {
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
                 bridge (route via source.read_arrow_stream + \
                 target.write_arrow_stream); same-backend only here"
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
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let merge_sql = duckdb_merge_sql(spec, source_query, keys, update_columns, &batch_id);

        // Pre-merge insert count via anti-join: rows in source whose
        // keys aren't already in target. Done inside the same
        // transaction as the merge so source-side data churn doesn't
        // race with us. DuckDB's `INSERT … ON CONFLICT DO UPDATE`
        // returns the inserts+updates total; subtracting yields the
        // update count. PG's xmax trick splits unchanged from updated
        // — DuckDB has no equivalent, so rows_unchanged stays 0 here.
        let insert_count_sql = format!(
            "SELECT count(*)::BIGINT FROM ({src}) src_count \
             WHERE ({key_tuple}) NOT IN (SELECT {key_tuple} FROM {schema}.{table})",
            src = source_query,
            key_tuple = keys.join(", "),
            schema = spec.schema,
            table = spec.name,
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

        let merge_result: Result<(i64, i64), BackendError> = self
            .with_conn_blocking(move |c| {
                c.execute_batch("BEGIN")
                    .map_err(|e| BackendError::Query(e.to_string()))?;
                // Anti-join count first.
                let inserts: i64 = {
                    let mut stmt = c
                        .prepare(&insert_count_sql)
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    let mut rows = stmt
                        .query([])
                        .map_err(|e| BackendError::Query(e.to_string()))?;
                    let row = rows
                        .next()
                        .map_err(|e| BackendError::Query(e.to_string()))?
                        .ok_or_else(|| {
                            BackendError::Query("anti-join count returned no row".into())
                        })?;
                    row.get(0).map_err(|e| BackendError::Query(e.to_string()))?
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
                 (route via source.read_arrow_stream + target.write_arrow_stream); \
                 same-backend only here"
                    .into(),
            ));
        }
        if let Some(dh) = delete_handling
            && !matches!(dh, DeleteHandling::Soft)
        {
            return Err(BackendError::Other(format!(
                "DuckDB run_scd2: only DeleteHandling::Soft is supported \
                 (got {dh:?}); Hard is for merge"
            )));
        }
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        // The temp-table token must be a valid SQL identifier and unique
        // across runs that share a connection. Hex of the run UUID
        // (no dashes) satisfies both.
        let run_token = run_id.simple().to_string();
        let mut stmts = duckdb_scd2_statements(
            spec,
            source_query,
            keys,
            compare_columns,
            &run_token,
            event_timestamp_column,
            &batch_id,
        );
        // Soft-delete: close out current versions whose key is missing
        // from the new source. Append to the statement list so it runs
        // inside the same transaction as the SCD2 plan but after the
        // INSERT (so newly added current rows don't get accidentally
        // closed by their own absence in the join). The drop of the
        // temp table stays last.
        let drop_temp = stmts.pop().expect("plan ends with DROP TABLE");
        if matches!(delete_handling, Some(DeleteHandling::Soft)) {
            stmts.push(build_scd2_close_missing_sql(
                &spec.schema,
                &spec.name,
                keys,
                source_query,
            ));
        }
        // TTL expiry: tombstone any current row whose valid_from has
        // aged past the freshness window. Runs whether or not the
        // user supplied soft-delete.
        if let Some(ttl) = ttl_seconds {
            stmts.push(duckdb_scd2_ttl_expire_sql(&spec.schema, &spec.name, ttl));
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

    /// Δ.X2: parse the incoming RecordBatch into CDC events, then
    /// dispatch to `run_cdc_inner`. Same shape as the Postgres
    /// trait-method wrapper — keeps the parse logic identical so a
    /// soft-fail row (parse error) is reported in the `skipped`
    /// counter rather than failing the whole batch.
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
                        "DuckDB CDC envelope parse failed; row skipped",
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

    // ---- DuckDB pure helpers ------------------------------------------
    //
    // Coverage backfill: `is_metadata_col`, `ensure_meta_schema_sql`,
    // `substitute_batch_id`, `duckdb_scd2_ttl_expire_sql`, and
    // `duckdb_digest_expression` were exercised only indirectly through
    // the existing in-memory + same-DB integration tests. Direct unit
    // tests close the per-helper branches without per-test backend setup.

    #[test]
    fn is_metadata_col_classifies_only_loaded_at_and_batch_id() {
        // Mirrors the MySQL `is_metadata_col`: append-only metadata
        // surface, NOT the SCD2 columns.
        assert!(is_metadata_col(LOADED_AT_COL));
        assert!(is_metadata_col(BATCH_ID_COL));
        // SCD2 metadata is not classified as metadata here — that's
        // the SCD2-specific predicate's job.
        assert!(!is_metadata_col("valid_from"));
        assert!(!is_metadata_col("valid_to"));
        assert!(!is_metadata_col("is_current"));
        assert!(!is_metadata_col("row_hash"));
        // User columns.
        assert!(!is_metadata_col("customer_id"));
        assert!(!is_metadata_col("name"));
    }

    #[test]
    fn ensure_meta_schema_sql_includes_run_history_and_watermarks() {
        let sql = ensure_meta_schema_sql();
        assert!(
            sql.contains("CREATE SCHEMA IF NOT EXISTS ematix_flow"),
            "must lazy-create ematix_flow schema, got: {sql}"
        );
        assert!(
            sql.contains("ematix_flow.run_history"),
            "must declare run_history, got: {sql}"
        );
        assert!(
            sql.contains("ematix_flow.watermarks"),
            "must declare watermarks, got: {sql}"
        );
        // Δ.X2: cdc_idempotency is part of the same lazy bootstrap.
        assert!(
            sql.contains("ematix_flow.cdc_idempotency"),
            "must declare cdc_idempotency, got: {sql}"
        );
        // Idempotent — re-running the bootstrap must not error on
        // an existing install. CREATE TABLE IF NOT EXISTS for all
        // three (run_history, watermarks, cdc_idempotency).
        let if_not_exists_count = sql.matches("CREATE TABLE IF NOT EXISTS").count();
        assert_eq!(if_not_exists_count, 3);
    }

    #[test]
    fn substitute_batch_id_replaces_pg_uuid_placeholder() {
        let batch = Uuid::new_v4();
        let sql = "INSERT INTO t (id, _batch_id) VALUES (1, $1::uuid)";
        let out = substitute_batch_id(sql, &batch);
        assert!(
            !out.contains("$1::uuid"),
            "placeholder must be substituted, got: {out}"
        );
        assert!(
            out.contains(&format!("'{batch}'::uuid")),
            "batch UUID literal must appear, got: {out}"
        );
    }

    #[test]
    fn substitute_batch_id_leaves_unrelated_sql_alone() {
        let batch = Uuid::new_v4();
        let sql = "SELECT 1";
        assert_eq!(substitute_batch_id(sql, &batch), "SELECT 1");
    }

    #[test]
    fn duckdb_scd2_ttl_expire_sql_includes_target_and_ttl_window() {
        let sql = duckdb_scd2_ttl_expire_sql("wh", "customer_dim", 86400);
        assert!(
            sql.contains("UPDATE wh.customer_dim"),
            "TTL expire must target wh.customer_dim, got: {sql}"
        );
        assert!(
            sql.contains("is_current = false"),
            "TTL expire must close the row (is_current = false), got: {sql}"
        );
        assert!(
            sql.contains("86400"),
            "TTL seconds must appear in the predicate, got: {sql}"
        );
        // DuckDB-specific epoch-based comparison (PG uses INTERVAL
        // arithmetic; DuckDB doesn't have the matching overload).
        assert!(
            sql.contains("epoch(valid_from)") && sql.contains("epoch(now())"),
            "TTL expire must use epoch-second comparison, got: {sql}"
        );
    }

    #[test]
    fn duckdb_digest_expression_uses_sha256_with_null_markers() {
        let cols = vec!["email".to_string(), "name".to_string()];
        let expr = duckdb_digest_expression(&cols, "src.");
        // Hashing the row uses SHA-256 wrapped in unhex() so the result
        // round-trips with the BLOB row_hash column type.
        assert!(
            expr.contains("unhex(sha256("),
            "must use unhex(sha256(...)), got: {expr}"
        );
        // Per-column NULL marker uses chr(0) NULL chr(0) so a literal
        // 'NULL' value stays distinguishable from a real null.
        assert!(
            expr.contains("chr(0) || 'NULL' || chr(0)"),
            "must mark nulls with chr(0)-bracketed NULL token, got: {expr}"
        );
        // Inter-column separator is chr(1) so values can't collide
        // across column boundaries.
        assert!(
            expr.contains("chr(1)"),
            "must separate columns with chr(1), got: {expr}"
        );
        // Source-prefix applied to each column.
        assert!(expr.contains("src.email::VARCHAR"));
        assert!(expr.contains("src.name::VARCHAR"));
    }

    #[test]
    fn duckdb_digest_expression_handles_empty_column_list() {
        // Edge case: when a SCD2 spec declares no compare columns, the
        // hash should still produce something deterministic (sha256 of
        // an empty input string), not panic.
        let expr = duckdb_digest_expression(&[], "src.");
        assert!(expr.contains("sha256("));
        assert!(expr.contains("unhex("));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duckdb_arrow_round_trip_wide_type_coverage() {
        // Mirrors the Postgres wide-type round-trip that closed the
        // backend.rs match arms (PR #8). DuckDB has its own column-
        // type-to-Arrow mapping in this file's read_arrow_stream
        // implementation; this test exercises every type variant the
        // mapping handles.
        use arrow_array::cast::AsArray;
        use arrow_array::{BinaryArray, BooleanArray, RecordBatch, StringArray};
        use arrow_schema::DataType;

        let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());

        backend.execute("CREATE SCHEMA wide").await.unwrap();
        // DuckDB supports a wider native type set than Postgres'
        // wire-protocol. Use the types that map cleanly to Arrow per
        // the existing read_arrow_stream implementation.
        backend
            .execute(
                "CREATE TABLE wide.src (\
                    c_smallint  SMALLINT, \
                    c_int       INTEGER, \
                    c_bigint    BIGINT, \
                    c_float     REAL, \
                    c_double    DOUBLE, \
                    c_bool      BOOLEAN, \
                    c_text      VARCHAR, \
                    c_blob      BLOB, \
                    c_ts        TIMESTAMP, \
                    c_uuid      UUID)",
            )
            .await
            .unwrap();
        backend
            .execute(
                "INSERT INTO wide.src VALUES \
                 (32767, 2147483647, 9223372036854775807, \
                  1.5, 3.5, true, 'hello', \
                  '\\x00deadbeef'::BLOB, \
                  TIMESTAMP '2026-05-08 12:34:56', \
                  '12345678-1234-5678-1234-567812345678'::UUID), \
                 (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            )
            .await
            .unwrap();

        let stream = backend
            .read_arrow_stream("SELECT * FROM wide.src ORDER BY c_bigint NULLS LAST")
            .await
            .unwrap();
        use futures_util::TryStreamExt;
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let b = &batches[0];
        let schema = b.schema();
        // Spot-check key DataType mappings + a few values.
        let types: Vec<&DataType> = schema.fields().iter().map(|f| f.data_type()).collect();
        // SMALLINT → Int16 (DuckDB), INTEGER → Int32, BIGINT → Int64.
        assert!(matches!(types[0], DataType::Int16));
        assert!(matches!(types[1], DataType::Int32));
        assert!(matches!(types[2], DataType::Int64));
        assert!(matches!(types[5], DataType::Boolean));
        let texts = b.column(6).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(texts.value(0), "hello");
        let bools = b.column(5).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.value(0));

        // Spot-check the BLOB column. DuckDB's blob-literal escape
        // semantics differ from Postgres' bytea (`'\x...'` is a
        // *string*, not binary); it's enough here to assert that the
        // BLOB column round-trips as non-null Binary with the expected
        // shape — exact byte content is exercised in the simpler
        // existing test above.
        if let Some(bin) = b.column(7).as_any().downcast_ref::<BinaryArray>() {
            assert!(!bin.value(0).is_empty(), "blob row 0 must be non-empty");
        }

        let smallints = b.column(0).as_primitive::<arrow_array::types::Int16Type>();
        assert_eq!(smallints.value(0), 32767);
        let bigints = b.column(2).as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(bigints.value(0), 9223372036854775807);

        // Null row preserved.
        for c in 0..b.num_columns() {
            assert!(b.column(c).is_null(1), "col {c} row 1 must be null");
        }

        // Round-trip back through write_arrow_stream into a clone
        // table — exercises the writer's per-DataType arm + DuckDB's
        // appender bind path.
        backend
            .execute(
                "CREATE TABLE wide.dst (\
                    c_smallint  SMALLINT, \
                    c_int       INTEGER, \
                    c_bigint    BIGINT, \
                    c_float     REAL, \
                    c_double    DOUBLE, \
                    c_bool      BOOLEAN, \
                    c_text      VARCHAR, \
                    c_blob      BLOB, \
                    c_ts        TIMESTAMP, \
                    c_uuid      UUID)",
            )
            .await
            .unwrap();
        let stream2 = backend
            .read_arrow_stream("SELECT * FROM wide.src")
            .await
            .unwrap();
        let target = TargetTable {
            schema: "wide".into(),
            name: "dst".into(),
        };
        let written = backend
            .write_arrow_stream(&target, stream2, WriteMode::Append)
            .await
            .unwrap();
        assert_eq!(written, 2);
    }

    // ------------------------------------------------------------
    // Δ.X2: CDC executor for DuckDB targets. End-to-end coverage —
    // c/u/d ops with idempotency gate + schema evolution + soft-
    // delete. In-memory DuckDB so no testcontainer needed.
    // ------------------------------------------------------------

    fn duckdb_cdc_customers_spec() -> TableSpec {
        use crate::types::{ColumnSpec, ColumnType};
        TableSpec {
            schema: "mirror".into(),
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

    /// Helper: encode a slice of CDC envelope JSON rows as a
    /// schema-inferred RecordBatch — the shape Kafka source's
    /// JSON decoder produces. The CDC executor calls
    /// `events_from_batch` on whatever shape arrives.
    fn duckdb_cdc_record_batch_from_json(
        rows: &[serde_json::Map<String, serde_json::Value>],
    ) -> RecordBatch {
        use std::io::Cursor;
        let mut buf = Vec::new();
        for row in rows {
            buf.extend(serde_json::to_string(row).unwrap().bytes());
            buf.push(b'\n');
        }
        let mut sniff = Cursor::new(&buf);
        let (schema, _) =
            arrow_json::reader::infer_json_schema_from_seekable(&mut sniff, None).expect("schema");
        let cursor = Cursor::new(&buf);
        let mut reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
            .build(cursor)
            .expect("reader");
        reader
            .next()
            .expect("at least one batch")
            .expect("decode ok")
    }

    async fn create_duckdb_cdc_target(backend: &Arc<dyn Backend>) {
        backend.execute("CREATE SCHEMA mirror").await.unwrap();
        backend
            .execute(
                "CREATE TABLE mirror.customers (\
                    id BIGINT PRIMARY KEY, \
                    email VARCHAR, \
                    name VARCHAR)",
            )
            .await
            .unwrap();
    }

    /// Δ.X2: c / u / d each get a turn in their own batch so the
    /// per-op counters are observable independently.
    #[tokio::test(flavor = "multi_thread")]
    async fn duckdb_cdc_inserts_updates_deletes_across_batches() {
        use crate::cdc::{CdcConfig, EnvelopeKind};
        use serde_json::json;

        let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
        create_duckdb_cdc_target(&backend).await;

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        let spec = duckdb_cdc_customers_spec();

        // Batch 1: two INSERTs.
        let r1 = backend
            .run_cdc(
                &spec,
                duckdb_cdc_record_batch_from_json(&[
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
                "duckdb_cdc_test",
            )
            .await
            .expect("batch 1");
        assert_eq!(r1.creates, 2, "two inserts in batch 1");
        assert_eq!(r1.updates, 0);
        assert_eq!(r1.deletes, 0);

        // Batch 2: UPDATE id=1, DELETE id=2.
        let r2 = backend
            .run_cdc(
                &spec,
                duckdb_cdc_record_batch_from_json(&[
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
                "duckdb_cdc_test",
            )
            .await
            .expect("batch 2");
        assert_eq!(r2.updates, 1, "one update");
        assert_eq!(r2.deletes, 1, "one delete");

        // Verify final state.
        let stream = backend
            .read_arrow_stream("SELECT id, name FROM mirror.customers ORDER BY id")
            .await
            .unwrap();
        use futures_util::TryStreamExt;
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "exactly id=1 should remain");

        let b = &batches[0];
        let names = b.column(1).as_any();
        let names = names
            .downcast_ref::<arrow_array::StringArray>()
            .expect("name column is utf8");
        assert_eq!(names.value(0), "Alice Smith");
    }

    /// Δ.X2: an OLDER redelivered event in a LATER batch is
    /// dropped at the idempotency gate. Lifted from the PG PR 4
    /// idempotency test pattern — same gate, same SQL, different
    /// dialect.
    #[tokio::test(flavor = "multi_thread")]
    async fn duckdb_cdc_redelivery_is_idempotent() {
        use crate::cdc::{CdcConfig, EnvelopeKind};
        use serde_json::json;

        let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
        create_duckdb_cdc_target(&backend).await;

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        let spec = duckdb_cdc_customers_spec();

        // Batch 1: insert id=1 at ts=200 with the "newer" name.
        let _r1 = backend
            .run_cdc(
                &spec,
                duckdb_cdc_record_batch_from_json(&[json!({
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
                "duckdb_cdc_idem",
            )
            .await
            .expect("batch 1");

        // Batch 2: redeliver an OLDER version (ts=100). Gate must
        // reject it; target stays at "Alice v2".
        let r2 = backend
            .run_cdc(
                &spec,
                duckdb_cdc_record_batch_from_json(&[json!({
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
                "duckdb_cdc_idem",
            )
            .await
            .expect("batch 2");
        assert_eq!(r2.creates, 0, "older redelivery must not apply");
        assert_eq!(r2.updates, 0);
        assert_eq!(
            r2.idempotent_skipped, 1,
            "idempotency gate must report the skip"
        );

        // Confirm the target row still says v2.
        let stream = backend
            .read_arrow_stream("SELECT name FROM mirror.customers WHERE id = 1")
            .await
            .unwrap();
        use futures_util::TryStreamExt;
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "Alice v2");
    }

    /// Δ.X2: SchemaEvolutionPolicy::Fail must abort the batch
    /// when an `after` payload carries a column missing on the
    /// target spec.
    #[tokio::test(flavor = "multi_thread")]
    async fn duckdb_cdc_schema_fail_aborts() {
        use crate::cdc::{CdcConfig, EnvelopeKind, SchemaEvolutionPolicy};
        use serde_json::json;

        let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
        create_duckdb_cdc_target(&backend).await;
        let spec = duckdb_cdc_customers_spec();

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        cdc.schema_evolution = SchemaEvolutionPolicy::Fail;

        let err = backend
            .run_cdc(
                &spec,
                duckdb_cdc_record_batch_from_json(&[json!({
                    "before": null,
                    "after": {"id": 1, "email": "a@x", "name": "A", "phone": "+1"},
                    "op": "c",
                    "ts_ms": 1_i64,
                })
                .as_object()
                .unwrap()
                .clone()]),
                &cdc,
                "duckdb_cdc_fail",
            )
            .await
            .expect_err("Fail policy must error on unknown column");
        assert!(
            err.to_string().contains("phone"),
            "error must name the offending column"
        );
    }

    /// Δ.X2: soft-delete flips the configured column instead of
    /// removing the row. Mirrors the PG / Delta soft-delete tests.
    #[tokio::test(flavor = "multi_thread")]
    async fn duckdb_cdc_soft_delete_flips_column() {
        use crate::cdc::{CdcConfig, DeleteMode, EnvelopeKind};
        use serde_json::json;

        let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
        backend.execute("CREATE SCHEMA mirror").await.unwrap();
        backend
            .execute(
                "CREATE TABLE mirror.customers (\
                    id BIGINT PRIMARY KEY, \
                    email VARCHAR, \
                    name VARCHAR, \
                    deleted_at TIMESTAMPTZ)",
            )
            .await
            .unwrap();

        let mut spec = duckdb_cdc_customers_spec();
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

        // Insert + soft-delete in two batches. The second batch's
        // delete event should flip `deleted_at`, not remove the row.
        let _r1 = backend
            .run_cdc(
                &spec,
                duckdb_cdc_record_batch_from_json(&[json!({
                    "before": null,
                    "after": {"id": 1, "email": "a@x", "name": "A", "deleted_at": null},
                    "op": "c",
                    "ts_ms": 100_i64,
                })
                .as_object()
                .unwrap()
                .clone()]),
                &cdc,
                "duckdb_cdc_soft",
            )
            .await
            .unwrap();

        let r2 = backend
            .run_cdc(
                &spec,
                duckdb_cdc_record_batch_from_json(&[json!({
                    "before": {"id": 1, "email": "a@x", "name": "A", "deleted_at": null},
                    "after":  null,
                    "op": "d",
                    "ts_ms": 200_i64,
                })
                .as_object()
                .unwrap()
                .clone()]),
                &cdc,
                "duckdb_cdc_soft",
            )
            .await
            .unwrap();
        assert_eq!(r2.deletes, 0, "soft-delete must not increment deletes");
        // Soft-delete is wired through UPDATE, so the counter
        // shows up under updates rather than deletes.
        assert!(r2.updates >= 1, "soft-delete reports as an update");

        // Row still present — but deleted_at populated.
        let stream = backend
            .read_arrow_stream("SELECT id FROM mirror.customers WHERE id = 1")
            .await
            .unwrap();
        use futures_util::TryStreamExt;
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "soft-delete preserves the row");

        let stream = backend
            .read_arrow_stream(
                "SELECT count(*) FROM mirror.customers WHERE id = 1 AND deleted_at IS NOT NULL",
            )
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 1, "deleted_at must be populated");
    }
}
