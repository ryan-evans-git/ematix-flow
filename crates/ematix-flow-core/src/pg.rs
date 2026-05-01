//! Phase 3: Postgres adapter — connection-string parsing, pool, and
//! same-database detection.

use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::{SinkExt, StreamExt, pin_mut};
use thiserror::Error;
use tokio_postgres::{Config as PgConfig, NoTls, config::Host};
use uuid::Uuid;

use crate::ddl::{
    DriftResult, ReflectedColumn, canonicalize_reflected_type, compare_table_with_uniques,
    create_table_sql,
};
use crate::meta::{
    DeleteHandling, WatermarkConfig, build_hard_delete_sql, build_scd2_close_missing_sql,
    build_scd2_ttl_expire_sql, wrap_with_watermark_filter,
};
use crate::strategy::append::{BATCH_ID_COL, LOADED_AT_COL, plan_same_db_append};
use crate::strategy::merge::plan_merge_upsert;
use crate::strategy::scd2::{build_out_of_order_check_sql, plan_scd2};
use crate::strategy::truncate::plan_truncate_replace;
use crate::types::TableSpec;

const DEFAULT_PORT: u16 = 5432;

#[derive(Debug, Error)]
pub enum PgError {
    #[error("invalid connection URL: {0}")]
    Url(String),
    #[error("postgres error: {}", format_pg_error(.0))]
    Postgres(#[from] tokio_postgres::Error),
    #[error("pool error: {0}")]
    Pool(String),
}

/// `tokio_postgres::Error`'s default Display strips the DB error message
/// down to "db error". Pull the underlying `DbError` so callers see the
/// real Postgres message ("column foo does not exist", etc.).
fn format_pg_error(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        let mut out = format!("{}: {}", db.severity(), db.message());
        if let Some(detail) = db.detail() {
            out.push_str(&format!(" ({detail})"));
        }
        if let Some(hint) = db.hint() {
            out.push_str(&format!(" [hint: {hint}]"));
        }
        out
    } else {
        e.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
}

pub fn parse_url(url: &str) -> Result<ConnectionInfo, PgError> {
    let cfg: PgConfig = url
        .parse()
        .map_err(|e: tokio_postgres::Error| PgError::Url(e.to_string()))?;
    let host = cfg
        .get_hosts()
        .iter()
        .find_map(|h| match h {
            Host::Tcp(s) => Some(s.clone()),
            #[allow(unreachable_patterns)]
            _ => None,
        })
        .ok_or_else(|| PgError::Url("missing host".into()))?;
    let port = cfg.get_ports().first().copied().unwrap_or(DEFAULT_PORT);
    let dbname = cfg
        .get_dbname()
        .ok_or_else(|| PgError::Url("missing dbname".into()))?
        .to_string();
    let user = cfg
        .get_user()
        .ok_or_else(|| PgError::Url("missing user".into()))?
        .to_string();
    Ok(ConnectionInfo {
        host,
        port,
        dbname,
        user,
    })
}

pub fn same_database(a: &str, b: &str) -> Result<bool, PgError> {
    Ok(parse_url(a)? == parse_url(b)?)
}

#[derive(Clone)]
pub struct PgPool {
    pool: Pool,
    info: ConnectionInfo,
}

#[derive(Debug, Clone)]
pub struct AppendRunResult {
    pub run_id: String,
    pub rows_inserted: i64,
    pub status: String,
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct MergeRunResult {
    pub run_id: String,
    pub rows_inserted: i64,
    pub rows_updated: i64,
    pub rows_unchanged: i64,
    pub status: String,
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct Scd2RunResult {
    pub run_id: String,
    /// Number of new current versions inserted (new keys + changed keys).
    pub rows_inserted: i64,
    /// Number of previous current versions closed out (changed keys only).
    pub rows_closed: i64,
    pub status: String,
    pub path: String,
}

const META_SCHEMA: &str = "ematix_flow";
const RUN_HISTORY_TABLE: &str = "run_history";
const WATERMARKS_TABLE: &str = "watermarks";

#[derive(Debug, Clone)]
pub struct WatermarkRow {
    pub pipeline_name: String,
    pub column_name: String,
    pub last_value: String,
}

impl PgPool {
    pub fn info(&self) -> &ConnectionInfo {
        &self.info
    }

    /// Crate-internal accessor for the underlying pool. Used by
    /// `crate::backend::PostgresBackend` to acquire clients without
    /// exposing deadpool internals on the public API.
    pub(crate) fn raw_pool(&self) -> &Pool {
        &self.pool
    }

    pub async fn connect(url: &str) -> Result<Self, PgError> {
        let info = parse_url(url)?;
        let pg_cfg: PgConfig = url
            .parse()
            .map_err(|e: tokio_postgres::Error| PgError::Url(e.to_string()))?;
        let mgr_cfg = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(pg_cfg, NoTls, mgr_cfg);
        let pool = Pool::builder(mgr)
            .max_size(8)
            .build()
            .map_err(|e| PgError::Pool(e.to_string()))?;
        // Eagerly validate the connection so connect() fails fast on a bad
        // URL/credentials/host rather than deferring the error to first use.
        let client = pool.get().await.map_err(|e| PgError::Pool(e.to_string()))?;
        let _: i32 = client.query_one("SELECT 1", &[]).await?.get(0);
        drop(client);
        Ok(Self { pool, info })
    }

    pub async fn ping(&self) -> Result<i32, PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        let row = client.query_one("SELECT 1", &[]).await?;
        Ok(row.get(0))
    }

    pub async fn execute(&self, sql: &str) -> Result<u64, PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        let n = client.execute(sql, &[]).await?;
        Ok(n)
    }

    pub async fn fetch_scalar_int(&self, sql: &str) -> Result<i32, PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        let row = client.query_one(sql, &[]).await?;
        Ok(row.get(0))
    }

    pub async fn execute_in_transaction(&self, sqls: &[String]) -> Result<(), PgError> {
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        let tx = client.transaction().await?;
        for sql in sqls {
            tx.batch_execute(sql).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn table_exists(&self, schema: &str, table: &str) -> Result<bool, PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        let row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )",
                &[&schema, &table],
            )
            .await?;
        Ok(row.get::<_, bool>(0))
    }

    pub async fn read_existing_columns(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ReflectedColumn>, PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        let rows = client
            .query(
                "SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable = 'YES' AS nullable,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    EXISTS (
                        SELECT 1
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                        WHERE tc.table_schema = c.table_schema
                          AND tc.table_name = c.table_name
                          AND tc.constraint_type = 'PRIMARY KEY'
                          AND kcu.column_name = c.column_name
                    ) AS is_primary_key
                FROM information_schema.columns c
                WHERE c.table_schema = $1 AND c.table_name = $2
                ORDER BY c.ordinal_position",
                &[&schema, &table],
            )
            .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row.get(0);
            let data_type: String = row.get(1);
            let nullable: bool = row.get(2);
            let char_max: Option<i32> = row.get(3);
            let num_precision: Option<i32> = row.get(4);
            let num_scale: Option<i32> = row.get(5);
            let primary_key: bool = row.get(6);
            let ty = canonicalize_reflected_type(&data_type, char_max, num_precision, num_scale)
                .ok_or_else(|| {
                    PgError::Pool(format!(
                        "unsupported reflected type for column `{name}`: {data_type}"
                    ))
                })?;
            out.push(ReflectedColumn {
                name,
                ty,
                nullable,
                primary_key,
            });
        }
        Ok(out)
    }

    /// Phase 22: read UNIQUE constraints from `information_schema`. Returns
    /// each constraint's columns ordered by `ordinal_position`. Excludes
    /// PRIMARY KEY constraints (`tc.constraint_type = 'UNIQUE'` already
    /// filters them out).
    pub async fn read_existing_unique_constraints(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<Vec<String>>, PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        let rows = client
            .query(
                // information_schema columns use the `name` type; cast to
                // text so tokio-postgres can deserialize the resulting
                // text[] into Vec<String>.
                "SELECT tc.constraint_name::text,
                        array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position) AS cols
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage kcu
                   ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                 WHERE tc.table_schema = $1
                   AND tc.table_name = $2
                   AND tc.constraint_type = 'UNIQUE'
                 GROUP BY tc.constraint_name
                 ORDER BY tc.constraint_name",
                &[&schema, &table],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<_, Vec<String>>(1))
            .collect())
    }

    /// Create the table if missing, or compare against the live schema.
    /// Caller decides what to do with `EnsureOutcome::Drift`.
    pub async fn ensure_table(&self, spec: &TableSpec) -> Result<EnsureOutcome, PgError> {
        if !self.table_exists(&spec.schema, &spec.name).await? {
            let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", spec.schema);
            let create_table = create_table_sql(spec);
            self.execute_in_transaction(&[create_schema, create_table])
                .await?;
            return Ok(EnsureOutcome::Created);
        }
        let reflected = self.read_existing_columns(&spec.schema, &spec.name).await?;
        let reflected_uniques = self
            .read_existing_unique_constraints(&spec.schema, &spec.name)
            .await?;
        match compare_table_with_uniques(spec, &reflected, &reflected_uniques) {
            DriftResult::Match => Ok(EnsureOutcome::Matched),
            DriftResult::Drift(diffs) => Ok(EnsureOutcome::Drift(diffs)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum EnsureOutcome {
    Created,
    Matched,
    Drift(Vec<crate::ddl::Difference>),
}

/// Inside the load transaction, fetch `max(col)::text` from the rows just
/// inserted (matched by `_batch_id`) and UPSERT the watermark row. NULL
/// max (e.g. when no rows were inserted) is treated as "no advance".
async fn advance_watermark(
    tx: &deadpool_postgres::tokio_postgres::Transaction<'_>,
    target_spec: &TableSpec,
    batch_id: &Uuid,
    pipeline_name: &str,
    watermark: &WatermarkConfig,
) -> Result<(), PgError> {
    let max_sql = format!(
        "SELECT max({col})::text FROM {schema}.{table} WHERE {batch} = $1",
        col = watermark.column,
        schema = target_spec.schema,
        table = target_spec.name,
        batch = BATCH_ID_COL,
    );
    let row = tx.query_opt(&max_sql, &[batch_id]).await?;
    let new_max: Option<String> = row.and_then(|r| r.get::<_, Option<String>>(0));
    if let Some(value) = new_max {
        tx.execute(
            &format!(
                "INSERT INTO {META_SCHEMA}.{WATERMARKS_TABLE} \
                 (pipeline_name, column_name, last_value, updated_at) \
                 VALUES ($1, $2, $3, now()) \
                 ON CONFLICT (pipeline_name) DO UPDATE \
                 SET column_name = EXCLUDED.column_name, \
                     last_value = EXCLUDED.last_value, \
                     updated_at = now()"
            ),
            &[&pipeline_name, &watermark.column, &value],
        )
        .await?;
    }
    Ok(())
}

impl PgPool {
    /// Lazy-create the `ematix_flow.run_history` table. Uses ALTER TABLE
    /// IF NOT EXISTS for columns added in later phases so existing
    /// installations get upgraded transparently.
    pub async fn ensure_meta_schema(&self) -> Result<(), PgError> {
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}");
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {META_SCHEMA}.{RUN_HISTORY_TABLE} (
                run_id UUID PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                target_schema TEXT NOT NULL,
                target_table TEXT NOT NULL,
                mode TEXT NOT NULL,
                path TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                finished_at TIMESTAMPTZ,
                status TEXT NOT NULL,
                rows_inserted BIGINT,
                error_message TEXT
            )"
        );
        let alter_updated = format!(
            "ALTER TABLE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
             ADD COLUMN IF NOT EXISTS rows_updated BIGINT"
        );
        let alter_unchanged = format!(
            "ALTER TABLE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
             ADD COLUMN IF NOT EXISTS rows_unchanged BIGINT"
        );
        let alter_parent_run_id = format!(
            "ALTER TABLE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
             ADD COLUMN IF NOT EXISTS parent_run_id UUID"
        );
        let alter_step_name = format!(
            "ALTER TABLE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
             ADD COLUMN IF NOT EXISTS step_name TEXT"
        );
        let alter_metrics_json = format!(
            "ALTER TABLE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
             ADD COLUMN IF NOT EXISTS metrics_json TEXT"
        );
        let create_watermarks = format!(
            "CREATE TABLE IF NOT EXISTS {META_SCHEMA}.{WATERMARKS_TABLE} (
                pipeline_name TEXT PRIMARY KEY,
                column_name TEXT NOT NULL,
                last_value TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        );
        self.execute_in_transaction(&[
            create_schema,
            create_table,
            alter_updated,
            alter_unchanged,
            alter_parent_run_id,
            alter_step_name,
            alter_metrics_json,
            create_watermarks,
        ])
        .await
    }

    /// Phase 27a: record a `transforms_post` step's outcome. Each step
    /// gets its own run_history row linked to the parent's run_id via
    /// `parent_run_id`. `metrics_json` is the optional dict the callable
    /// returned (Phase 27 §4.2 / Q3.1) — None for SQL-string steps.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_transform_history(
        &self,
        parent_run_id: &str,
        pipeline_name: &str,
        step_name: &str,
        status: &str,
        target_schema: &str,
        target_table: &str,
        error_message: Option<&str>,
        metrics_json: Option<&str>,
    ) -> Result<(), PgError> {
        self.ensure_meta_schema().await?;
        let parent_uuid = Uuid::parse_str(parent_run_id)
            .map_err(|e| PgError::Pool(format!("invalid parent_run_id: {e}")))?;
        let row_id = Uuid::new_v4();
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        client
            .execute(
                &format!(
                    "INSERT INTO {META_SCHEMA}.{RUN_HISTORY_TABLE} \
                     (run_id, parent_run_id, pipeline_name, target_schema, target_table, \
                      mode, path, started_at, finished_at, status, step_name, \
                      error_message, metrics_json) \
                     VALUES ($1, $2, $3, $4, $5, 'transform', 'post', now(), now(), \
                             $6, $7, $8, $9)"
                ),
                &[
                    &row_id,
                    &parent_uuid,
                    &pipeline_name,
                    &target_schema,
                    &target_table,
                    &status,
                    &step_name,
                    &error_message,
                    &metrics_json,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn read_watermark(
        &self,
        pipeline_name: &str,
    ) -> Result<Option<WatermarkRow>, PgError> {
        self.ensure_meta_schema().await?;
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT pipeline_name, column_name, last_value \
                     FROM {META_SCHEMA}.{WATERMARKS_TABLE} \
                     WHERE pipeline_name = $1"
                ),
                &[&pipeline_name],
            )
            .await?;
        Ok(row.map(|r| WatermarkRow {
            pipeline_name: r.get(0),
            column_name: r.get(1),
            last_value: r.get(2),
        }))
    }

    async fn insert_history_start(
        &self,
        run_id: Uuid,
        pipeline_name: &str,
        target_spec: &TableSpec,
        mode: &str,
        path: &str,
    ) -> Result<(), PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        client
            .execute(
                &format!(
                    "INSERT INTO {META_SCHEMA}.{RUN_HISTORY_TABLE} \
                     (run_id, pipeline_name, target_schema, target_table, \
                      mode, path, started_at, status) \
                     VALUES ($1, $2, $3, $4, $5, $6, now(), 'running')"
                ),
                &[
                    &run_id,
                    &pipeline_name,
                    &target_spec.schema,
                    &target_spec.name,
                    &mode,
                    &path,
                ],
            )
            .await?;
        Ok(())
    }

    async fn finish_history_success(
        &self,
        run_id: Uuid,
        rows_inserted: i64,
    ) -> Result<(), PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        client
            .execute(
                &format!(
                    "UPDATE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
                     SET status='success', rows_inserted=$2, finished_at=now() \
                     WHERE run_id=$1"
                ),
                &[&run_id, &rows_inserted],
            )
            .await?;
        Ok(())
    }

    async fn finish_history_success_merge(
        &self,
        run_id: Uuid,
        inserted: i64,
        updated: i64,
        unchanged: i64,
    ) -> Result<(), PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        client
            .execute(
                &format!(
                    "UPDATE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
                     SET status='success', rows_inserted=$2, rows_updated=$3, \
                         rows_unchanged=$4, finished_at=now() \
                     WHERE run_id=$1"
                ),
                &[&run_id, &inserted, &updated, &unchanged],
            )
            .await?;
        Ok(())
    }

    async fn finish_history_failure(&self, run_id: Uuid, message: &str) -> Result<(), PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        client
            .execute(
                &format!(
                    "UPDATE {META_SCHEMA}.{RUN_HISTORY_TABLE} \
                     SET status='failed', error_message=$2, finished_at=now() \
                     WHERE run_id=$1"
                ),
                &[&run_id, &message],
            )
            .await?;
        Ok(())
    }

    /// Same-DB AppendOnly executor: target spec already augmented with
    /// metadata columns and ensured to exist. `watermark` (if any) filters
    /// the source and gets advanced atomically inside the load transaction
    /// after a successful INSERT. With `dry_run = true`, the transaction
    /// is rolled back at the end and run_history side effects are skipped;
    /// row counts are still returned.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_append_same_db(
        &self,
        target_spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
        watermark: Option<&WatermarkConfig>,
        dry_run: bool,
    ) -> Result<AppendRunResult, PgError> {
        if !dry_run {
            self.ensure_meta_schema().await?;
        }
        let run_id = Uuid::new_v4();
        let batch_id = run_id;
        if !dry_run {
            self.insert_history_start(run_id, pipeline_name, target_spec, "append", "same_db")
                .await?;
        }

        let filtered_source = wrap_with_watermark_filter(source_query, watermark);
        let plan = plan_same_db_append(target_spec, &filtered_source);
        let result: Result<i64, PgError> = async {
            let mut client = self
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let tx = client.transaction().await?;
            let rows = if plan.has_metadata {
                tx.execute(&plan.sql, &[&batch_id]).await?
            } else {
                tx.execute(&plan.sql, &[]).await?
            };
            if let Some(wc) = watermark {
                advance_watermark(&tx, target_spec, &batch_id, pipeline_name, wc).await?;
            }
            if dry_run {
                tx.rollback().await?;
            } else {
                tx.commit().await?;
            }
            Ok(rows as i64)
        }
        .await;

        match result {
            Ok(rows_inserted) => {
                if !dry_run {
                    self.finish_history_success(run_id, rows_inserted).await?;
                }
                Ok(AppendRunResult {
                    run_id: run_id.to_string(),
                    rows_inserted,
                    status: if dry_run { "dry_run" } else { "success" }.into(),
                    path: "same_db".into(),
                })
            }
            Err(err) => {
                if !dry_run {
                    let _ = self.finish_history_failure(run_id, &err.to_string()).await;
                }
                Err(err)
            }
        }
    }

    /// Cross-DB AppendOnly executor: COPY (binary) from source into a
    /// target-side temp staging table, then INSERT INTO target SELECT FROM
    /// staging. Source-side and target-side connections are distinct pools.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_append_cross_db(
        &self,
        source_pool: &PgPool,
        target_spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
        watermark: Option<&WatermarkConfig>,
    ) -> Result<AppendRunResult, PgError> {
        self.ensure_meta_schema().await?;
        let run_id = Uuid::new_v4();
        let batch_id = run_id;
        self.insert_history_start(run_id, pipeline_name, target_spec, "append", "cross_db")
            .await?;

        // Determine user columns (target columns minus metadata) and the
        // staging table's column definitions.
        let user_columns: Vec<&str> = target_spec
            .columns
            .iter()
            .filter(|c| c.name != LOADED_AT_COL && c.name != BATCH_ID_COL)
            .map(|c| c.name.as_str())
            .collect();
        let staging_def: String = target_spec
            .columns
            .iter()
            .filter(|c| c.name != LOADED_AT_COL && c.name != BATCH_ID_COL)
            .map(|c| format!("{} {}", c.name, c.ty.to_postgres_sql()))
            .collect::<Vec<_>>()
            .join(", ");
        let staging = format!("_ematix_stage_{}", run_id.simple());
        // Push the watermark filter down to the source side so we don't COPY
        // rows we'll discard. The plain (un-watermarked) projected_source is
        // what feeds the COPY OUT.
        let filtered_source_for_copy = wrap_with_watermark_filter(source_query, watermark);
        let projected_source = format!(
            "SELECT {cols} FROM ({source}) src",
            cols = user_columns.join(", "),
            source = filtered_source_for_copy,
        );

        let plan = plan_same_db_append(target_spec, &format!("SELECT * FROM {staging}"));

        let result: Result<i64, PgError> = async {
            // Hold a target client across the transaction (temp table needs
            // the same session).
            let mut target_client = self
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let target_tx = target_client.transaction().await?;
            target_tx
                .batch_execute(&format!("CREATE TEMP TABLE {staging} ({staging_def})"))
                .await?;

            // Source-side COPY OUT.
            let source_client = source_pool
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let copy_out_sql = format!("COPY ({projected_source}) TO STDOUT (FORMAT binary)");
            let stream = source_client.copy_out(&copy_out_sql).await?;

            // Target-side COPY IN.
            let copy_in_sql = format!("COPY {staging} FROM STDIN (FORMAT binary)");
            let sink = target_tx.copy_in::<_, Bytes>(&copy_in_sql).await?;

            pin_mut!(stream);
            pin_mut!(sink);
            while let Some(chunk) = stream.next().await {
                let bytes = chunk?;
                sink.send(bytes).await?;
            }
            let _bytes_copied = sink.finish().await?;

            // INSERT INTO target SELECT FROM staging (with metadata literals).
            let rows = if plan.has_metadata {
                target_tx.execute(&plan.sql, &[&batch_id]).await?
            } else {
                target_tx.execute(&plan.sql, &[]).await?
            };
            if let Some(wc) = watermark {
                advance_watermark(&target_tx, target_spec, &batch_id, pipeline_name, wc).await?;
            }
            target_tx.commit().await?;
            Ok(rows as i64)
        }
        .await;

        match result {
            Ok(rows_inserted) => {
                self.finish_history_success(run_id, rows_inserted).await?;
                Ok(AppendRunResult {
                    run_id: run_id.to_string(),
                    rows_inserted,
                    status: "success".into(),
                    path: "cross_db".into(),
                })
            }
            Err(err) => {
                let _ = self.finish_history_failure(run_id, &err.to_string()).await;
                Err(err)
            }
        }
    }

    /// Same-DB TruncateReplace executor: TRUNCATE then INSERT...SELECT in
    /// a single transaction so the target's pre-load contents survive on
    /// failure. With `dry_run = true`, ROLLBACK at end and skip
    /// run_history side effects.
    pub async fn run_truncate_same_db(
        &self,
        target_spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
        dry_run: bool,
    ) -> Result<AppendRunResult, PgError> {
        if !dry_run {
            self.ensure_meta_schema().await?;
        }
        let run_id = Uuid::new_v4();
        let batch_id = run_id;
        if !dry_run {
            self.insert_history_start(run_id, pipeline_name, target_spec, "truncate", "same_db")
                .await?;
        }

        let plan = plan_truncate_replace(target_spec, source_query);
        let result: Result<i64, PgError> = async {
            let mut client = self
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let tx = client.transaction().await?;
            // Statement 0: TRUNCATE. Statement 1: INSERT...SELECT.
            tx.batch_execute(&plan.statements[0]).await?;
            let rows = if plan.has_metadata {
                tx.execute(&plan.statements[1], &[&batch_id]).await?
            } else {
                tx.execute(&plan.statements[1], &[]).await?
            };
            if dry_run {
                tx.rollback().await?;
            } else {
                tx.commit().await?;
            }
            Ok(rows as i64)
        }
        .await;

        match result {
            Ok(rows_inserted) => {
                if !dry_run {
                    self.finish_history_success(run_id, rows_inserted).await?;
                }
                Ok(AppendRunResult {
                    run_id: run_id.to_string(),
                    rows_inserted,
                    status: if dry_run { "dry_run" } else { "success" }.into(),
                    path: "same_db".into(),
                })
            }
            Err(err) => {
                if !dry_run {
                    let _ = self.finish_history_failure(run_id, &err.to_string()).await;
                }
                Err(err)
            }
        }
    }

    /// Cross-DB TruncateReplace: COPY source rows into a target-side temp
    /// staging table, then TRUNCATE target + INSERT FROM stage in one
    /// transaction.
    pub async fn run_truncate_cross_db(
        &self,
        source_pool: &PgPool,
        target_spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
    ) -> Result<AppendRunResult, PgError> {
        self.ensure_meta_schema().await?;
        let run_id = Uuid::new_v4();
        let batch_id = run_id;
        self.insert_history_start(run_id, pipeline_name, target_spec, "truncate", "cross_db")
            .await?;

        let user_columns: Vec<&str> = target_spec
            .columns
            .iter()
            .filter(|c| c.name != LOADED_AT_COL && c.name != BATCH_ID_COL)
            .map(|c| c.name.as_str())
            .collect();
        let staging_def: String = target_spec
            .columns
            .iter()
            .filter(|c| c.name != LOADED_AT_COL && c.name != BATCH_ID_COL)
            .map(|c| format!("{} {}", c.name, c.ty.to_postgres_sql()))
            .collect::<Vec<_>>()
            .join(", ");
        let staging = format!("_ematix_stage_{}", run_id.simple());
        let projected_source = format!(
            "SELECT {cols} FROM ({source_query}) src",
            cols = user_columns.join(", "),
        );
        let plan = plan_truncate_replace(target_spec, &format!("SELECT * FROM {staging}"));

        let result: Result<i64, PgError> = async {
            let mut target_client = self
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let target_tx = target_client.transaction().await?;
            target_tx
                .batch_execute(&format!("CREATE TEMP TABLE {staging} ({staging_def})"))
                .await?;

            let source_client = source_pool
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let copy_out_sql = format!("COPY ({projected_source}) TO STDOUT (FORMAT binary)");
            let stream = source_client.copy_out(&copy_out_sql).await?;

            let copy_in_sql = format!("COPY {staging} FROM STDIN (FORMAT binary)");
            let sink = target_tx.copy_in::<_, Bytes>(&copy_in_sql).await?;

            pin_mut!(stream);
            pin_mut!(sink);
            while let Some(chunk) = stream.next().await {
                let bytes = chunk?;
                sink.send(bytes).await?;
            }
            let _bytes_copied = sink.finish().await?;

            // TRUNCATE then INSERT FROM stage — both inside the same tx so
            // staging-load errors leave target untouched.
            target_tx.batch_execute(&plan.statements[0]).await?;
            let rows = if plan.has_metadata {
                target_tx.execute(&plan.statements[1], &[&batch_id]).await?
            } else {
                target_tx.execute(&plan.statements[1], &[]).await?
            };
            target_tx.commit().await?;
            Ok(rows as i64)
        }
        .await;

        match result {
            Ok(rows_inserted) => {
                self.finish_history_success(run_id, rows_inserted).await?;
                Ok(AppendRunResult {
                    run_id: run_id.to_string(),
                    rows_inserted,
                    status: "success".into(),
                    path: "cross_db".into(),
                })
            }
            Err(err) => {
                let _ = self.finish_history_failure(run_id, &err.to_string()).await;
                Err(err)
            }
        }
    }

    /// Same-DB MergeUpsert / SCD1 executor. `mode_label` is whatever the
    /// user requested ("merge" or "scd1") and is recorded in run_history.
    /// With `delete_handling = Some(Hard)`, a DELETE post-step removes
    /// target rows whose keys are missing from the source — atomically
    /// with the upsert. With `dry_run = true`, ROLLBACK at end and skip
    /// run_history side effects.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_merge_same_db(
        &self,
        target_spec: &TableSpec,
        source_query: &str,
        keys: &[String],
        update_columns: &[String],
        pipeline_name: &str,
        mode_label: &str,
        delete_handling: Option<DeleteHandling>,
        dry_run: bool,
    ) -> Result<MergeRunResult, PgError> {
        if !dry_run {
            self.ensure_meta_schema().await?;
        }
        let run_id = Uuid::new_v4();
        let batch_id = run_id;
        if !dry_run {
            self.insert_history_start(run_id, pipeline_name, target_spec, mode_label, "same_db")
                .await?;
        }

        let plan = plan_merge_upsert(target_spec, source_query, keys, update_columns);
        let result: Result<(i64, i64, i64), PgError> = async {
            let mut client = self
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let tx = client.transaction().await?;
            let row = if plan.has_metadata {
                tx.query_one(&plan.sql, &[&batch_id]).await?
            } else {
                tx.query_one(&plan.sql, &[]).await?
            };
            let inserted: i64 = row.get(0);
            let updated: i64 = row.get(1);
            let total: i64 = row.get(2);
            if matches!(delete_handling, Some(DeleteHandling::Hard)) {
                let delete_sql = build_hard_delete_sql(
                    &target_spec.schema,
                    &target_spec.name,
                    keys,
                    source_query,
                );
                tx.batch_execute(&delete_sql).await?;
            }
            if dry_run {
                tx.rollback().await?;
            } else {
                tx.commit().await?;
            }
            let unchanged = total - inserted - updated;
            Ok((inserted, updated, unchanged))
        }
        .await;

        match result {
            Ok((inserted, updated, unchanged)) => {
                if !dry_run {
                    self.finish_history_success_merge(run_id, inserted, updated, unchanged)
                        .await?;
                }
                Ok(MergeRunResult {
                    run_id: run_id.to_string(),
                    rows_inserted: inserted,
                    rows_updated: updated,
                    rows_unchanged: unchanged,
                    status: if dry_run { "dry_run" } else { "success" }.into(),
                    path: "same_db".into(),
                })
            }
            Err(err) => {
                if !dry_run {
                    let _ = self.finish_history_failure(run_id, &err.to_string()).await;
                }
                Err(err)
            }
        }
    }

    /// Lazy-create the pgcrypto extension. SCD2 needs `digest()`. On
    /// managed services where the user lacks CREATE EXTENSION rights this
    /// errors with a clear message; future phases can fall back to md5.
    async fn ensure_pgcrypto(&self) -> Result<(), PgError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| PgError::Pool(e.to_string()))?;
        client
            .batch_execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
            .await?;
        Ok(())
    }

    /// Same-DB SCD2 executor. Three statements in one transaction:
    /// 1. Create temp `_scd2_changed_<id>` from a hashed source.
    /// 2. Close out previous current versions for changed keys.
    /// 3. Insert new current versions.
    ///
    /// With `delete_handling = Some(Soft)`, a fourth statement closes out
    /// current versions whose natural key is missing from the source.
    /// With `event_ts_column = Some(col)`, switches to event-time SCD2:
    /// `valid_from` is taken from that source column instead of `now()`,
    /// the close-out's `valid_to` chains to the new event_ts, and a guard
    /// query rejects out-of-order arrivals before close-out runs.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_scd2_same_db(
        &self,
        target_spec: &TableSpec,
        source_query: &str,
        keys: &[String],
        compare_columns: &[String],
        pipeline_name: &str,
        delete_handling: Option<DeleteHandling>,
        event_ts_column: Option<&str>,
        ttl_seconds: Option<i64>,
        dry_run: bool,
    ) -> Result<Scd2RunResult, PgError> {
        if !dry_run {
            self.ensure_meta_schema().await?;
        }
        self.ensure_pgcrypto().await?;
        let run_id = Uuid::new_v4();
        let batch_id = run_id;
        let run_token = run_id.simple().to_string();
        if !dry_run {
            self.insert_history_start(run_id, pipeline_name, target_spec, "scd2", "same_db")
                .await?;
        }

        let plan = plan_scd2(
            target_spec,
            source_query,
            keys,
            compare_columns,
            &run_token,
            event_ts_column,
        );

        let result: Result<(i64, i64), PgError> = async {
            let mut client = self
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let tx = client.transaction().await?;
            tx.batch_execute(&plan.statements[0]).await?;
            // Phase 15: detect event_ts arriving older than the existing
            // current version's valid_from before mutating anything.
            if event_ts_column.is_some() {
                let check_sql = build_out_of_order_check_sql(target_spec, keys, &run_token);
                let row = tx.query_one(&check_sql, &[]).await?;
                let bad: i64 = row.get(0);
                if bad > 0 {
                    return Err(PgError::Pool(format!(
                        "event_ts out-of-order: {bad} row(s) carry an event_ts older \
                         than the existing current version's valid_from"
                    )));
                }
            }
            let closed_changed = tx.execute(&plan.statements[1], &[]).await? as i64;
            let inserted = if plan.has_metadata {
                tx.execute(&plan.statements[2], &[&batch_id]).await? as i64
            } else {
                tx.execute(&plan.statements[2], &[]).await? as i64
            };
            let closed_missing = if matches!(delete_handling, Some(DeleteHandling::Soft)) {
                let close_sql = build_scd2_close_missing_sql(
                    &target_spec.schema,
                    &target_spec.name,
                    keys,
                    source_query,
                );
                tx.execute(&close_sql, &[]).await? as i64
            } else {
                0
            };
            // Phase 16: TTL expiry. Closes out current versions whose
            // valid_from is older than now() - ttl. Same-tx atomicity.
            let closed_ttl = if let Some(ttl_secs) = ttl_seconds {
                let ttl_sql =
                    build_scd2_ttl_expire_sql(&target_spec.schema, &target_spec.name, ttl_secs);
                tx.execute(&ttl_sql, &[]).await? as i64
            } else {
                0
            };
            if dry_run {
                tx.rollback().await?;
            } else {
                tx.commit().await?;
            }
            Ok((inserted, closed_changed + closed_missing + closed_ttl))
        }
        .await;

        match result {
            Ok((inserted, closed)) => {
                // rows_unchanged not meaningful here without a separate
                // count; record updated=closed for run_history symmetry.
                if !dry_run {
                    self.finish_history_success_merge(run_id, inserted, closed, 0)
                        .await?;
                }
                Ok(Scd2RunResult {
                    run_id: run_id.to_string(),
                    rows_inserted: inserted,
                    rows_closed: closed,
                    status: if dry_run { "dry_run" } else { "success" }.into(),
                    path: "same_db".into(),
                })
            }
            Err(err) => {
                if !dry_run {
                    let _ = self.finish_history_failure(run_id, &err.to_string()).await;
                }
                Err(err)
            }
        }
    }

    /// Cross-DB SCD2 executor. COPY user columns into a target-side temp
    /// staging table, then run the SCD2 plan against that staging table.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_scd2_cross_db(
        &self,
        source_pool: &PgPool,
        target_spec: &TableSpec,
        source_query: &str,
        keys: &[String],
        compare_columns: &[String],
        pipeline_name: &str,
        delete_handling: Option<DeleteHandling>,
        event_ts_column: Option<&str>,
        ttl_seconds: Option<i64>,
    ) -> Result<Scd2RunResult, PgError> {
        self.ensure_meta_schema().await?;
        self.ensure_pgcrypto().await?;
        let run_id = Uuid::new_v4();
        let batch_id = run_id;
        let run_token = run_id.simple().to_string();
        self.insert_history_start(run_id, pipeline_name, target_spec, "scd2", "cross_db")
            .await?;

        // user columns = target columns minus all metadata (scd2 + run).
        let user_columns: Vec<&str> = target_spec
            .columns
            .iter()
            .filter(|c| {
                c.name != LOADED_AT_COL
                    && c.name != BATCH_ID_COL
                    && !crate::strategy::scd2::is_scd2_metadata(&c.name)
            })
            .map(|c| c.name.as_str())
            .collect();
        let mut staging_columns: Vec<String> = target_spec
            .columns
            .iter()
            .filter(|c| {
                c.name != LOADED_AT_COL
                    && c.name != BATCH_ID_COL
                    && !crate::strategy::scd2::is_scd2_metadata(&c.name)
            })
            .map(|c| format!("{} {}", c.name, c.ty.to_postgres_sql()))
            .collect();
        // Phase 15: when running event-time SCD2 cross-DB, the source-side
        // event_ts column must travel through staging too — it isn't a
        // target column but the SCD2 plan against the staging table needs it.
        let mut projected_select: Vec<String> =
            user_columns.iter().map(|c| (*c).to_string()).collect();
        if let Some(ets) = event_ts_column {
            staging_columns.push(format!("{ets} TIMESTAMPTZ"));
            projected_select.push(ets.to_string());
        }
        let staging_def: String = staging_columns.join(", ");
        let staging = format!("_ematix_stage_{}", run_token);
        let projected_source = format!(
            "SELECT {cols} FROM ({source_query}) src",
            cols = projected_select.join(", "),
        );
        let plan = plan_scd2(
            target_spec,
            &format!("SELECT * FROM {staging}"),
            keys,
            compare_columns,
            &run_token,
            event_ts_column,
        );

        let result: Result<(i64, i64), PgError> = async {
            let mut target_client = self
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let target_tx = target_client.transaction().await?;
            target_tx
                .batch_execute(&format!("CREATE TEMP TABLE {staging} ({staging_def})"))
                .await?;

            let source_client = source_pool
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let copy_out_sql = format!("COPY ({projected_source}) TO STDOUT (FORMAT binary)");
            let stream = source_client.copy_out(&copy_out_sql).await?;
            let copy_in_sql = format!("COPY {staging} FROM STDIN (FORMAT binary)");
            let sink = target_tx.copy_in::<_, Bytes>(&copy_in_sql).await?;
            pin_mut!(stream);
            pin_mut!(sink);
            while let Some(chunk) = stream.next().await {
                let bytes = chunk?;
                sink.send(bytes).await?;
            }
            let _ = sink.finish().await?;

            target_tx.batch_execute(&plan.statements[0]).await?;
            if event_ts_column.is_some() {
                let check_sql = build_out_of_order_check_sql(target_spec, keys, &run_token);
                let row = target_tx.query_one(&check_sql, &[]).await?;
                let bad: i64 = row.get(0);
                if bad > 0 {
                    return Err(PgError::Pool(format!(
                        "event_ts out-of-order: {bad} row(s) carry an event_ts older \
                         than the existing current version's valid_from"
                    )));
                }
            }
            let closed_changed = target_tx.execute(&plan.statements[1], &[]).await? as i64;
            let inserted = if plan.has_metadata {
                target_tx.execute(&plan.statements[2], &[&batch_id]).await? as i64
            } else {
                target_tx.execute(&plan.statements[2], &[]).await? as i64
            };
            let closed_missing = if matches!(delete_handling, Some(DeleteHandling::Soft)) {
                let close_sql = build_scd2_close_missing_sql(
                    &target_spec.schema,
                    &target_spec.name,
                    keys,
                    &format!("SELECT * FROM {staging}"),
                );
                target_tx.execute(&close_sql, &[]).await? as i64
            } else {
                0
            };
            // Phase 16: TTL expiry runs in the same transaction.
            let closed_ttl = if let Some(ttl_secs) = ttl_seconds {
                let ttl_sql =
                    build_scd2_ttl_expire_sql(&target_spec.schema, &target_spec.name, ttl_secs);
                target_tx.execute(&ttl_sql, &[]).await? as i64
            } else {
                0
            };
            target_tx.commit().await?;
            Ok((inserted, closed_changed + closed_missing + closed_ttl))
        }
        .await;

        match result {
            Ok((inserted, closed)) => {
                self.finish_history_success_merge(run_id, inserted, closed, 0)
                    .await?;
                Ok(Scd2RunResult {
                    run_id: run_id.to_string(),
                    rows_inserted: inserted,
                    rows_closed: closed,
                    status: "success".into(),
                    path: "cross_db".into(),
                })
            }
            Err(err) => {
                let _ = self.finish_history_failure(run_id, &err.to_string()).await;
                Err(err)
            }
        }
    }

    /// Cross-DB MergeUpsert: COPY source rows into a target-side temp
    /// staging table, then run the merge upsert with the staging table as
    /// the source.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_merge_cross_db(
        &self,
        source_pool: &PgPool,
        target_spec: &TableSpec,
        source_query: &str,
        keys: &[String],
        update_columns: &[String],
        pipeline_name: &str,
        mode_label: &str,
        delete_handling: Option<DeleteHandling>,
    ) -> Result<MergeRunResult, PgError> {
        self.ensure_meta_schema().await?;
        let run_id = Uuid::new_v4();
        let batch_id = run_id;
        self.insert_history_start(run_id, pipeline_name, target_spec, mode_label, "cross_db")
            .await?;

        let user_columns: Vec<&str> = target_spec
            .columns
            .iter()
            .filter(|c| c.name != LOADED_AT_COL && c.name != BATCH_ID_COL)
            .map(|c| c.name.as_str())
            .collect();
        let staging_def: String = target_spec
            .columns
            .iter()
            .filter(|c| c.name != LOADED_AT_COL && c.name != BATCH_ID_COL)
            .map(|c| format!("{} {}", c.name, c.ty.to_postgres_sql()))
            .collect::<Vec<_>>()
            .join(", ");
        let staging = format!("_ematix_stage_{}", run_id.simple());
        let projected_source = format!(
            "SELECT {cols} FROM ({source_query}) src",
            cols = user_columns.join(", "),
        );
        let plan = plan_merge_upsert(
            target_spec,
            &format!("SELECT * FROM {staging}"),
            keys,
            update_columns,
        );

        let result: Result<(i64, i64, i64), PgError> = async {
            let mut target_client = self
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let target_tx = target_client.transaction().await?;
            target_tx
                .batch_execute(&format!("CREATE TEMP TABLE {staging} ({staging_def})"))
                .await?;

            let source_client = source_pool
                .pool
                .get()
                .await
                .map_err(|e| PgError::Pool(e.to_string()))?;
            let copy_out_sql = format!("COPY ({projected_source}) TO STDOUT (FORMAT binary)");
            let stream = source_client.copy_out(&copy_out_sql).await?;

            let copy_in_sql = format!("COPY {staging} FROM STDIN (FORMAT binary)");
            let sink = target_tx.copy_in::<_, Bytes>(&copy_in_sql).await?;

            pin_mut!(stream);
            pin_mut!(sink);
            while let Some(chunk) = stream.next().await {
                let bytes = chunk?;
                sink.send(bytes).await?;
            }
            let _ = sink.finish().await?;

            let row = if plan.has_metadata {
                target_tx.query_one(&plan.sql, &[&batch_id]).await?
            } else {
                target_tx.query_one(&plan.sql, &[]).await?
            };
            let inserted: i64 = row.get(0);
            let updated: i64 = row.get(1);
            let total: i64 = row.get(2);
            if matches!(delete_handling, Some(DeleteHandling::Hard)) {
                let delete_sql = build_hard_delete_sql(
                    &target_spec.schema,
                    &target_spec.name,
                    keys,
                    &format!("SELECT * FROM {staging}"),
                );
                target_tx.batch_execute(&delete_sql).await?;
            }
            target_tx.commit().await?;
            let unchanged = total - inserted - updated;
            Ok((inserted, updated, unchanged))
        }
        .await;

        match result {
            Ok((inserted, updated, unchanged)) => {
                self.finish_history_success_merge(run_id, inserted, updated, unchanged)
                    .await?;
                Ok(MergeRunResult {
                    run_id: run_id.to_string(),
                    rows_inserted: inserted,
                    rows_updated: updated,
                    rows_unchanged: unchanged,
                    status: "success".into(),
                    path: "cross_db".into(),
                })
            }
            Err(err) => {
                let _ = self.finish_history_failure(run_id, &err.to_string()).await;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pg::{ConnectionInfo, parse_url, same_database};

    #[test]
    fn parses_full_url() {
        let info = parse_url("postgres://app_user:secret@db.example.com:5433/warehouse").unwrap();
        assert_eq!(info.host, "db.example.com");
        assert_eq!(info.port, 5433);
        assert_eq!(info.dbname, "warehouse");
        assert_eq!(info.user, "app_user");
    }

    #[test]
    fn defaults_port_to_5432() {
        let info = parse_url("postgres://u@h/d").unwrap();
        assert_eq!(info.port, 5432);
    }

    #[test]
    fn accepts_postgresql_scheme() {
        let info = parse_url("postgresql://u@h/d").unwrap();
        assert_eq!(info.host, "h");
    }

    #[test]
    fn missing_dbname_is_error() {
        assert!(parse_url("postgres://u@h").is_err());
    }

    #[test]
    fn missing_user_is_error() {
        assert!(parse_url("postgres://h/d").is_err());
    }

    #[test]
    fn invalid_url_is_error() {
        assert!(parse_url("not a url").is_err());
    }

    #[test]
    fn same_database_normalizes_default_port() {
        // explicit 5432 vs implicit default — same database
        assert!(same_database("postgres://u@h:5432/d", "postgres://u@h/d",).unwrap());
    }

    #[test]
    fn same_database_distinguishes_host() {
        assert!(!same_database("postgres://u@h1/d", "postgres://u@h2/d",).unwrap());
    }

    #[test]
    fn same_database_distinguishes_port() {
        assert!(!same_database("postgres://u@h:5432/d", "postgres://u@h:5433/d",).unwrap());
    }

    #[test]
    fn same_database_distinguishes_dbname() {
        assert!(!same_database("postgres://u@h/d1", "postgres://u@h/d2",).unwrap());
    }

    #[test]
    fn same_database_distinguishes_user() {
        assert!(!same_database("postgres://u1@h/d", "postgres://u2@h/d",).unwrap());
    }

    #[test]
    fn connection_info_ignores_password_and_query() {
        // password and ?sslmode=... should not affect the four-tuple
        let a = parse_url("postgres://u:p1@h/d?sslmode=disable").unwrap();
        let b = parse_url("postgres://u:p2@h/d?sslmode=require").unwrap();
        assert_eq!(a, b);
        let _ = ConnectionInfo {
            host: a.host.clone(),
            port: a.port,
            dbname: a.dbname.clone(),
            user: a.user.clone(),
        };
    }
}
