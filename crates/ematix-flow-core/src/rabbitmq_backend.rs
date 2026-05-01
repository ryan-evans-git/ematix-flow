//! Phase 37a: RabbitMQ backend skeleton.
//!
//! Wraps `lapin` (pure-rust AMQP 0.9.1 client) as a `Backend` so the
//! same trait surface drives RabbitMQ pipelines that already drives
//! Kafka, the DB / object-store / Delta backends. 37a covers the
//! connection-level surface only — `read_arrow_stream` /
//! `write_arrow_stream` and the strategy executors are stubbed and
//! land in 37a.x.
//!
//! ## What 37a ships
//!   - `RabbitMQBackend::open(amqp_url)` — wraps an AMQP URI of the
//!     form `amqp://user:pass@host:port/vhost`.
//!   - `dialect()` → `Dialect::Streaming { kind: RabbitMQ }`.
//!   - `connection_info()` reports host:port + the user component.
//!   - `dsn()` returns the AMQP URL.
//!   - `ping()` opens a connection, declares a channel, closes both,
//!     all under a short timeout.
//!   - `execute()` rejects: AMQP has no SQL surface.
//!   - All five strategy executors stub with phase-marker errors,
//!     mirroring the Kafka skeleton's rejection patterns.
//!
//! ## What lands in 37a.x
//!   - 37a.2 — Arrow IO (consume/produce against a queue/exchange).
//!   - 37a.3 — manual ack with at-least-once delivery semantics.
//!   - 37a.4 — DLQ via dead-letter exchange.
//!   - 37a.5 — auth providers (TLS, SASL EXTERNAL, plain login —
//!     `lapin` already supports them through the URI scheme; expose
//!     the same builder-method surface as Kafka for consistency).
//!
//! ## Why a single AMQP-URL constructor
//! AMQP collapses host / port / vhost / credentials into one URI, so
//! a single `open(amqp_url)` covers what Kafka splits across
//! `bootstrap_servers` + `group_id` + auth builders. `lapin`
//! validates the URI on connect, so `open` itself only stores the
//! string and does not allocate any networking resources.

use std::time::Duration;

use async_trait::async_trait;
use lapin::{Connection, ConnectionProperties};

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    StreamingKind, TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

/// RabbitMQ-backed implementation of `Backend`.
///
/// Holds the AMQP URL rather than a long-lived connection — `lapin`
/// builds connections on demand and channels are cheap. Matches the
/// "config holder" pattern every other Backend in the framework
/// uses.
#[derive(Debug)]
pub struct RabbitMQBackend {
    /// Full AMQP URI (`amqp://user:pass@host:port/vhost`).
    amqp_url: String,
}

impl RabbitMQBackend {
    /// Construct a RabbitMQ backend from an AMQP URI.
    ///
    /// `amqp_url` should be the standard `amqp://` (or `amqps://` for
    /// TLS) form. The URL is validated on `ping()` / first IO; this
    /// constructor only checks for a non-empty string and a known
    /// scheme prefix so misconfiguration surfaces early.
    pub fn open(amqp_url: impl Into<String>) -> Result<Self, BackendError> {
        let amqp_url = amqp_url.into();
        if amqp_url.is_empty() {
            return Err(BackendError::Connection(
                "rabbitmq backend: amqp_url cannot be empty".into(),
            ));
        }
        if !amqp_url.starts_with("amqp://") && !amqp_url.starts_with("amqps://") {
            return Err(BackendError::Connection(format!(
                "rabbitmq backend: amqp_url must start with `amqp://` or `amqps://`; got: {amqp_url}"
            )));
        }
        Ok(Self { amqp_url })
    }

    /// Borrow the configured AMQP URL.
    pub fn amqp_url(&self) -> &str {
        &self.amqp_url
    }
}

#[async_trait]
impl Backend for RabbitMQBackend {
    fn dialect(&self) -> Dialect {
        Dialect::Streaming {
            kind: StreamingKind::RabbitMQ,
        }
    }

    fn connection_info(&self) -> ConnectionInfo {
        // The framework's ConnectionInfo struct shape is DB-shaped
        // (host/port/dbname/user). For RabbitMQ we project as:
        //   host  = parsed host
        //   port  = parsed port (defaults: 5672 for amqp, 5671 for amqps)
        //   dbname = full amqp_url (for logs)
        //   user  = parsed user, or "anonymous" if no credentials
        let parsed = parse_amqp_url(&self.amqp_url);
        ConnectionInfo {
            host: parsed.host,
            port: parsed.port,
            dbname: self.amqp_url.clone(),
            user: parsed.user,
        }
    }

    fn dsn(&self) -> Option<String> {
        Some(self.amqp_url.clone())
    }

    /// Connect, open a channel, close — all under a short timeout.
    /// `lapin` validates the URI here for the first time; a malformed
    /// URI surfaces as `BackendError::Connection`.
    async fn ping(&self) -> Result<(), BackendError> {
        let url = self.amqp_url.clone();
        let fut = async move {
            let conn = Connection::connect(&url, ConnectionProperties::default())
                .await
                .map_err(|e| BackendError::Connection(format!("rabbitmq connect: {e}")))?;
            let channel = conn
                .create_channel()
                .await
                .map_err(|e| BackendError::Connection(format!("rabbitmq channel: {e}")))?;
            // Best-effort close; ignore failures — the connection
            // will tear down when `conn` is dropped at the end of
            // this scope.
            let _ = channel.close(0, "ping ok").await;
            let _ = conn.close(0, "ping ok").await;
            Ok::<_, BackendError>(())
        };
        tokio::time::timeout(Duration::from_secs(5), fut)
            .await
            .map_err(|_| BackendError::Connection("rabbitmq ping: timed out after 5s".into()))?
    }

    /// RabbitMQ has no SQL surface — reject explicitly.
    async fn execute(&self, _statement: &str) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "RabbitMQ backend has no execute() surface — \
             use read_arrow_stream / write_arrow_stream (37a.2) or \
             run_append (37a.2+); merge / scd2 are not meaningful for \
             a streaming source"
                .into(),
        ))
    }

    /// Arrow consume — stub for 37a.2.
    async fn read_arrow_stream(&self, _query: &str) -> Result<ArrowBatchStream, BackendError> {
        Err(BackendError::Other(
            "RabbitMQ read_arrow_stream: lands in Phase 37a.2 (queue → \
             Arrow batches via lapin's basic_consume + arrow-json)"
                .into(),
        ))
    }

    /// Arrow produce — stub for 37a.2.
    async fn write_arrow_stream(
        &self,
        _target: &TargetTable,
        _stream: ArrowBatchStream,
        _mode: WriteMode,
    ) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "RabbitMQ write_arrow_stream: lands in Phase 37a.2 \
             (Arrow batches → exchange via lapin's basic_publish)"
                .into(),
        ))
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
            "RabbitMQ run_append: lands in Phase 37a.2+ once \
             write_arrow_stream is implemented"
                .into(),
        ))
    }

    /// `Truncate` on a queue/exchange isn't a producer-side
    /// operation — it's an admin/management API call. Reject with a
    /// clear pointer.
    async fn run_truncate(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "RabbitMQ run_truncate: queues are append-style streams; \
             to start fresh, purge or delete the queue via the \
             RabbitMQ management API or admin tools"
                .into(),
        ))
    }

    /// Merge / SCD2 are not meaningful on a streaming sink. Match
    /// Kafka's pattern pointer.
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
            "RabbitMQ run_merge: merge has no natural meaning for an \
             AMQP exchange (every publish is independent). To merge \
             upstream RabbitMQ data into a transactional store, use \
             RabbitMQ as the source and a DB / Delta backend as the \
             target"
                .into(),
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
            "RabbitMQ run_scd2: SCD2 versioning has no natural meaning \
             for an AMQP exchange. Use RabbitMQ as a source and a DB / \
             Delta backend as the SCD2 target"
                .into(),
        ))
    }
}

/// Best-effort parse of `amqp://user:pass@host:port/vhost` into the
/// shape of `ConnectionInfo`. Robust to missing port (defaults
/// per-scheme) and missing credentials (user becomes "anonymous").
struct ParsedAmqpUrl {
    host: String,
    port: u16,
    user: String,
}

fn parse_amqp_url(url: &str) -> ParsedAmqpUrl {
    let (default_port, rest) = if let Some(rest) = url.strip_prefix("amqp://") {
        (5672_u16, rest)
    } else if let Some(rest) = url.strip_prefix("amqps://") {
        (5671_u16, rest)
    } else {
        return ParsedAmqpUrl {
            host: "unknown".into(),
            port: 0,
            user: "anonymous".into(),
        };
    };
    // Split on first '/' to drop the vhost. Then split the
    // userinfo@authority portion on '@'.
    let authority = rest.split('/').next().unwrap_or(rest);
    let (userinfo, host_port) = match authority.split_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let user = userinfo
        .map(|u| u.split(':').next().unwrap_or(u).to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "anonymous".into());
    let (host, port) = match host_port.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().unwrap_or(default_port)),
        None => (host_port.to_string(), default_port),
    };
    let host = if host.is_empty() {
        "localhost".into()
    } else {
        host
    };
    ParsedAmqpUrl { host, port, user }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Dialect;

    #[test]
    fn open_rejects_empty_url() {
        let err = RabbitMQBackend::open("").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot be empty"), "got: {msg}");
    }

    #[test]
    fn open_rejects_unknown_scheme() {
        let err = RabbitMQBackend::open("http://localhost:5672").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("amqp://") || msg.contains("amqps://"),
            "got: {msg}"
        );
    }

    #[test]
    fn open_accepts_amqp_url() {
        let b = RabbitMQBackend::open("amqp://guest:guest@localhost:5672/%2f").unwrap();
        assert_eq!(b.amqp_url(), "amqp://guest:guest@localhost:5672/%2f");
    }

    #[test]
    fn open_accepts_amqps_url() {
        let b = RabbitMQBackend::open("amqps://broker.example.com").unwrap();
        assert!(b.amqp_url().starts_with("amqps://"));
    }

    #[test]
    fn dialect_is_streaming_rabbitmq() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        assert_eq!(
            b.dialect(),
            Dialect::Streaming {
                kind: StreamingKind::RabbitMQ
            }
        );
    }

    #[test]
    fn connection_info_parses_default_port_for_amqp() {
        let b = RabbitMQBackend::open("amqp://guest:guest@broker.local/%2f").unwrap();
        let info = b.connection_info();
        assert_eq!(info.host, "broker.local");
        assert_eq!(info.port, 5672, "amqp default port");
        assert_eq!(info.user, "guest");
    }

    #[test]
    fn connection_info_parses_default_port_for_amqps() {
        let b = RabbitMQBackend::open("amqps://broker.local").unwrap();
        let info = b.connection_info();
        assert_eq!(info.host, "broker.local");
        assert_eq!(info.port, 5671, "amqps default port");
    }

    #[test]
    fn connection_info_parses_explicit_port_and_credentials() {
        let b = RabbitMQBackend::open("amqp://alice:s3cret@10.0.0.1:5673/").unwrap();
        let info = b.connection_info();
        assert_eq!(info.host, "10.0.0.1");
        assert_eq!(info.port, 5673);
        assert_eq!(info.user, "alice");
    }

    #[test]
    fn connection_info_anonymous_user_when_no_credentials() {
        let b = RabbitMQBackend::open("amqp://localhost:5672/").unwrap();
        let info = b.connection_info();
        assert_eq!(info.user, "anonymous");
    }

    #[test]
    fn dsn_returns_amqp_url() {
        let b = RabbitMQBackend::open("amqp://localhost:5672/%2f").unwrap();
        assert_eq!(b.dsn().as_deref(), Some("amqp://localhost:5672/%2f"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_rejects_with_pointer() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        let err = b.execute("anything").await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no execute() surface"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_arrow_stream_rejects_with_pointer_to_37a_2() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        let err = match b.read_arrow_stream("any-queue").await {
            Ok(_) => panic!("expected stub rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("Phase 37a.2"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_arrow_stream_rejects_with_pointer_to_37a_2() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        let target = TargetTable {
            schema: "".into(),
            name: "any".into(),
        };
        let stream = Box::pin(futures_util::stream::empty());
        let err = b
            .write_arrow_stream(&target, stream, WriteMode::Append)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Phase 37a.2"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_rejects_with_admin_pointer() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        let spec = TableSpec {
            schema: "s".into(),
            name: "t".into(),
            columns: vec![],
            unique_constraints: vec![],
            fingerprint: String::new(),
        };
        let err = b
            .run_truncate(&spec, "x", "p", None, false)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("admin tools") || msg.contains("management API"),
            "got: {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn merge_rejects_with_canonical_pattern_pointer() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        let spec = TableSpec {
            schema: "s".into(),
            name: "t".into(),
            columns: vec![],
            unique_constraints: vec![],
            fingerprint: String::new(),
        };
        let err = b
            .run_merge(
                &spec,
                "x",
                &["k".into()],
                &["c".into()],
                "p",
                "merge",
                None,
                None,
                false,
            )
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("DB / Delta backend as the"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scd2_rejects_with_pattern_pointer() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        let spec = TableSpec {
            schema: "s".into(),
            name: "t".into(),
            columns: vec![],
            unique_constraints: vec![],
            fingerprint: String::new(),
        };
        let err = b
            .run_scd2(
                &spec,
                "x",
                &["k".into()],
                &["c".into()],
                "p",
                None,
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("SCD2 target"), "got: {msg}");
    }
}
