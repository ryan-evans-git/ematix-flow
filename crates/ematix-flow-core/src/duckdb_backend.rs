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
        _spec: &crate::types::TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _incremental_column: Option<&str>,
        _last_value_literal: Option<&str>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "DuckDB run_append: not implemented in Phase 31a (lands in 31b)".into(),
        ))
    }

    async fn run_truncate(
        &self,
        _spec: &crate::types::TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "DuckDB run_truncate: not implemented in Phase 31a (lands in 31b)".into(),
        ))
    }

    async fn run_merge(
        &self,
        _spec: &crate::types::TableSpec,
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
            "DuckDB run_merge: not implemented in Phase 31a (lands in 31b)".into(),
        ))
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
