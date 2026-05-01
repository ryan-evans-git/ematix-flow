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
    ArrowBatchStream, Backend, BackendError, Dialect, DeleteHandling, StrategyRunResult,
    TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::strategy::append::{BATCH_ID_COL, LOADED_AT_COL, plan_same_db_append};
use crate::strategy::truncate::plan_truncate_replace;
use crate::types::TableSpec;
use uuid::Uuid;

fn is_metadata_col(name: &str) -> bool {
    name == LOADED_AT_COL || name == BATCH_ID_COL
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
            DuckConn::open_in_memory()
                .map_err(|e| BackendError::Connection(e.to_string()))?
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

    async fn read_arrow_stream(
        &self,
        query: &str,
    ) -> Result<ArrowBatchStream, BackendError> {
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
        _pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        incremental_column: Option<&str>,
        _last_value_literal: Option<&str>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        if source_backend.is_some() {
            return Err(BackendError::Other(
                "DuckDB cross-backend run_append goes through the Arrow streaming \
                 bridge (cross_backend_arrow_sync); same-backend only here"
                    .into(),
            ));
        }
        if incremental_column.is_some() {
            return Err(BackendError::Other(
                "DuckDB run_append: incremental_column not yet supported (Phase 31d)"
                    .into(),
            ));
        }
        let plan = plan_same_db_append(spec, source_query);
        let batch_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let sql = substitute_batch_id(&plan.sql, &batch_id);

        let inserted = self
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
            .await?;

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
        _pipeline_name: &str,
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

        let inserted = self
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
            .await?;

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
        _pipeline_name: &str,
        _mode_label: &str,
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

        // DuckDB's `INSERT ... ON CONFLICT DO UPDATE` returns the
        // affected-row count as inserts + updates summed (no easy way
        // to split without a follow-up query). Surface it as
        // rows_inserted for now; rows_updated tracking is a 31d
        // refinement.
        let affected = self
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
            .await?;

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
        _spec: &crate::types::TableSpec,
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
            "DuckDB run_scd2: not implemented in Phase 31a (lands in 31c)".into(),
        ))
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
