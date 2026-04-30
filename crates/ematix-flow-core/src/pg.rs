//! Phase 3: Postgres adapter — connection-string parsing, pool, and
//! same-database detection.

use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::{SinkExt, StreamExt, pin_mut};
use thiserror::Error;
use tokio_postgres::{Config as PgConfig, NoTls, config::Host};
use uuid::Uuid;

use crate::ddl::{
    DriftResult, ReflectedColumn, canonicalize_reflected_type, compare_table, create_table_sql,
};
use crate::strategy::append::{BATCH_ID_COL, LOADED_AT_COL, plan_same_db_append};
use crate::types::TableSpec;

const DEFAULT_PORT: u16 = 5432;

#[derive(Debug, Error)]
pub enum PgError {
    #[error("invalid connection URL: {0}")]
    Url(String),
    #[error("postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("pool error: {0}")]
    Pool(String),
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

const META_SCHEMA: &str = "ematix_flow";
const RUN_HISTORY_TABLE: &str = "run_history";

impl PgPool {
    pub fn info(&self) -> &ConnectionInfo {
        &self.info
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
        match compare_table(spec, &reflected) {
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

impl PgPool {
    /// Lazy-create the `ematix_flow.run_history` table.
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
        self.execute_in_transaction(&[create_schema, create_table])
            .await
    }

    async fn insert_history_start(
        &self,
        run_id: Uuid,
        pipeline_name: &str,
        target_spec: &TableSpec,
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
                     VALUES ($1, $2, $3, $4, 'append', $5, now(), 'running')"
                ),
                &[
                    &run_id,
                    &pipeline_name,
                    &target_spec.schema,
                    &target_spec.name,
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
    /// metadata columns and ensured to exist.
    pub async fn run_append_same_db(
        &self,
        target_spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
    ) -> Result<AppendRunResult, PgError> {
        self.ensure_meta_schema().await?;
        let run_id = Uuid::new_v4();
        let batch_id = run_id;
        self.insert_history_start(run_id, pipeline_name, target_spec, "same_db")
            .await?;

        let plan = plan_same_db_append(target_spec, source_query);
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
            tx.commit().await?;
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
                    path: "same_db".into(),
                })
            }
            Err(err) => {
                let _ = self.finish_history_failure(run_id, &err.to_string()).await;
                Err(err)
            }
        }
    }

    /// Cross-DB AppendOnly executor: COPY (binary) from source into a
    /// target-side temp staging table, then INSERT INTO target SELECT FROM
    /// staging. Source-side and target-side connections are distinct pools.
    pub async fn run_append_cross_db(
        &self,
        source_pool: &PgPool,
        target_spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
    ) -> Result<AppendRunResult, PgError> {
        self.ensure_meta_schema().await?;
        let run_id = Uuid::new_v4();
        let batch_id = run_id;
        self.insert_history_start(run_id, pipeline_name, target_spec, "cross_db")
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
        let projected_source = format!(
            "SELECT {cols} FROM ({source_query}) src",
            cols = user_columns.join(", "),
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
