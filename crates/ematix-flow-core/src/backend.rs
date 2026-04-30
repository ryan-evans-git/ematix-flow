//! Phase 30a: backend abstraction scaffolding.
//!
//! This module introduces the `Backend` trait and supporting types
//! that future backends (MySQL, SQLite, DuckDB, object storage,
//! Iceberg/Delta, Kafka, etc.) will implement. The existing Postgres
//! code in `pg.rs` remains the source of truth in this commit; a
//! `PostgresBackend` wrapper is provided as the first impl, delegating
//! to `PgPool`.
//!
//! Subsequent Phase 30 sub-commits will:
//!   - 30b: add Arrow streaming I/O methods (`read_arrow_stream` /
//!     `write_arrow_stream`) to the trait + the Postgres impl.
//!   - 30c: route `pipeline.sync` through the trait so cross-backend
//!     dispatch becomes a real code path.
//!   - 30d: migrate strategy executors (`run_append`, `run_merge`,
//!     `run_scd2`, ...) onto the trait so they're dialect-aware.
//!
//! Public API stays unchanged in 30a — existing PyO3 bindings still
//! call into `PgPool` directly.
//!
//! See `docs/MULTI_BACKEND_PLAN.md` §3 for the full design.

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::pg::{ConnectionInfo, PgError, PgPool};

/// Backend kind. Used by the planner / dispatcher to pick a same-backend
/// fast path vs. an Arrow streaming bridge for cross-backend syncs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dialect {
    Postgres,
    MySQL,
    SQLite,
    DuckDB,
    Iceberg,
    Delta,
    ObjectStore { format: ObjectFormat },
    Streaming { kind: StreamingKind },
}

/// File format for raw object-storage targets (Phase 34).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectFormat {
    Parquet,
    Csv,
    Orc,
    JsonLines,
}

/// Streaming source/sink kind (Phase 36–37).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamingKind {
    Kafka,
    Kinesis,
    PubSub,
    RabbitMQ,
}

impl Dialect {
    /// Whether two dialects can use a same-backend fast path. Identical
    /// dialects always agree; ObjectStore variants agree only when the
    /// file format also matches.
    pub fn matches(&self, other: &Dialect) -> bool {
        self == other
    }
}

/// Backend-agnostic error type. Each backend wraps its own native errors.
#[derive(Debug, Error)]
pub enum BackendError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("query error: {0}")]
    Query(String),
    #[error("type mapping error: {0}")]
    TypeMapping(String),
    #[error("backend error: {0}")]
    Other(String),
}

impl From<PgError> for BackendError {
    fn from(err: PgError) -> Self {
        match err {
            PgError::Url(s) => BackendError::Connection(s),
            PgError::Pool(s) => BackendError::Connection(s),
            // Reuse the PG error's already-formatted DB message.
            PgError::Postgres(_) => BackendError::Query(err.to_string()),
        }
    }
}

/// The unified backend interface. In 30a only the connection-level
/// surface (`ping`, `execute`, `dialect`, `connection_info`, `dsn`) is
/// defined; subsequent sub-commits add schema management, strategy
/// executors, and Arrow I/O.
///
/// Backends are typically held behind `Arc<dyn Backend>` so the
/// pipeline executor can dispatch over a heterogeneous set of source +
/// target backends at runtime.
#[async_trait]
pub trait Backend: Send + Sync {
    fn dialect(&self) -> Dialect;

    /// `(host, port, dbname, user)` for DB-shaped backends; backend-
    /// specific identifying info for others (bucket name, broker list,
    /// etc.). Used for same-DB short-circuit detection and for human
    /// labels in `preview()` / logs.
    fn connection_info(&self) -> ConnectionInfo;

    /// Original connection string (DSN, S3 URI, etc.) when the backend
    /// has one. None for backends constructed from structured config
    /// without a stringified form. Carries credentials — keep within the
    /// trust boundary of the user code that constructed the backend.
    fn dsn(&self) -> Option<String>;

    /// Liveness check. For DB backends this issues `SELECT 1`; for
    /// streaming/object-store backends it verifies the underlying client
    /// is connectable.
    async fn ping(&self) -> Result<(), BackendError>;

    /// Execute a side-effecting statement. SQL for DB backends; backend-
    /// specific commands for others (e.g., `DELETE` against an object
    /// prefix). Returns the affected row count where meaningful, 0
    /// otherwise.
    async fn execute(&self, statement: &str) -> Result<u64, BackendError>;
}

/// Postgres backend — wraps an existing `PgPool`. The first impl of the
/// trait; in 30a it's a thin delegation. Subsequent sub-commits move
/// more functionality onto the trait surface.
pub struct PostgresBackend {
    pool: Arc<PgPool>,
    dsn: String,
}

impl PostgresBackend {
    pub fn new(pool: Arc<PgPool>, dsn: String) -> Self {
        Self { pool, dsn }
    }

    pub fn pool(&self) -> &Arc<PgPool> {
        &self.pool
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    fn connection_info(&self) -> ConnectionInfo {
        self.pool.info().clone()
    }

    fn dsn(&self) -> Option<String> {
        Some(self.dsn.clone())
    }

    async fn ping(&self) -> Result<(), BackendError> {
        self.pool.ping().await?;
        Ok(())
    }

    async fn execute(&self, statement: &str) -> Result<u64, BackendError> {
        Ok(self.pool.execute(statement).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect_matches_is_strict_equality() {
        assert!(Dialect::Postgres.matches(&Dialect::Postgres));
        assert!(!Dialect::Postgres.matches(&Dialect::MySQL));
        assert!(
            Dialect::ObjectStore {
                format: ObjectFormat::Parquet
            }
            .matches(&Dialect::ObjectStore {
                format: ObjectFormat::Parquet
            })
        );
        assert!(
            !Dialect::ObjectStore {
                format: ObjectFormat::Parquet
            }
            .matches(&Dialect::ObjectStore {
                format: ObjectFormat::Csv
            })
        );
        assert!(
            Dialect::Streaming {
                kind: StreamingKind::Kafka
            }
            .matches(&Dialect::Streaming {
                kind: StreamingKind::Kafka
            })
        );
    }

    #[test]
    fn pg_error_to_backend_error_preserves_kind() {
        let url_err: BackendError = PgError::Url("missing dbname".into()).into();
        assert!(matches!(url_err, BackendError::Connection(_)));
        let pool_err: BackendError = PgError::Pool("timeout".into()).into();
        assert!(matches!(pool_err, BackendError::Connection(_)));
    }

    #[test]
    fn dialect_variants_all_distinct() {
        // Sanity: the variants we'll dispatch over are all distinct.
        let variants = [
            Dialect::Postgres,
            Dialect::MySQL,
            Dialect::SQLite,
            Dialect::DuckDB,
            Dialect::Iceberg,
            Dialect::Delta,
            Dialect::ObjectStore {
                format: ObjectFormat::Parquet,
            },
            Dialect::Streaming {
                kind: StreamingKind::Kafka,
            },
        ];
        for (i, a) in variants.iter().enumerate() {
            for (j, b) in variants.iter().enumerate() {
                assert_eq!(a == b, i == j, "{a:?} vs {b:?}");
            }
        }
    }
}
