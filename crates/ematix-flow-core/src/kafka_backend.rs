//! Phase 36a: Kafka backend skeleton.
//!
//! Wraps `rdkafka` (FFI to librdkafka) as a `Backend` so the same
//! trait surface drives streaming pipelines that already drives the
//! DB / object-store / Delta backends. 36a covers the connection-level
//! surface only — `read_arrow_stream` / `write_arrow_stream` and the
//! strategy executors are stubbed and land in 36b–j.
//!
//! ## What 36a ships
//!   - `KafkaBackend::open(bootstrap_servers, group_id)` — a producer-
//!     only handle if `group_id` is `None`, or a consumer handle if
//!     `Some(...)`.
//!   - `dialect()` → `Dialect::Streaming { kind: Kafka }`.
//!   - `connection_info()` reports broker host:port + group_id-as-user.
//!   - `dsn()` returns the bootstrap-servers string.
//!   - `ping()` issues a metadata fetch with a short timeout.
//!   - `execute()` rejects: Kafka has no SQL surface.
//!   - All five strategy executors stub with phase-marker errors.
//!
//! ## What lands in 36b+
//!   - 36b/c — Arrow IO (consume/produce).
//!   - 36d   — batching (size / window / bytes).
//!   - 36e   — manual offset commits + at-least-once.
//!   - 36f   — Auth providers (SASL/PLAIN, SASL/SCRAM-SHA-256/512,
//!     mTLS, MSK IAM via `oauthbearer_token_refresh_cb` proactive
//!     renewal at ~80% TTL). Each lands as a builder method on
//!     `KafkaBackend` so the simple `open(bootstrap_servers,
//!     group_id)` constructor stays unchanged for unauthenticated
//!     dev clusters.
//!   - 36g   — long-running supervised consumer process + graceful
//!     SIGTERM (lives at the CLI layer, not here).
//!   - 36h   — source format dispatch (JSON / Avro / Protobuf / raw).
//!   - 36i   — DLQ + Prometheus lag metrics.
//!   - 36j   — exactly-once delivery via Kafka transactions.
//!
//! ## Why a producer-only path
//! The framework supports both consume and produce roles for Kafka.
//! Producer-only (`group_id = None`) is the simpler path and the one
//! every pipeline targeting Kafka uses. Consumer mode requires a
//! `group_id` for offset tracking and rebalancing.

use std::time::Duration;

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    StreamingKind, TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

/// Kafka-backed implementation of `Backend`.
///
/// Holds a librdkafka `ClientConfig` rather than a long-lived
/// connection — librdkafka is lazy: producers, consumers, and admin
/// clients are constructed on demand from the same config. This
/// matches how every other Backend in the framework holds a "config"
/// and constructs a fresh handle per operation.
#[derive(Debug)]
pub struct KafkaBackend {
    /// Comma-separated `host:port` list. Cloned into every per-op
    /// client config we build.
    bootstrap_servers: String,
    /// Consumer group_id. `None` means this backend is producer-only.
    /// Populated for `read_arrow_stream` callers in 36b.
    group_id: Option<String>,
}

impl KafkaBackend {
    /// Construct a Kafka backend.
    ///
    /// `bootstrap_servers` accepts the standard
    /// `host1:9092,host2:9092` form. `group_id` is required for
    /// consumer pipelines and ignored for producer-only ones.
    pub fn open(
        bootstrap_servers: impl Into<String>,
        group_id: Option<&str>,
    ) -> Result<Self, BackendError> {
        let bootstrap_servers = bootstrap_servers.into();
        if bootstrap_servers.is_empty() {
            return Err(BackendError::Connection(
                "kafka backend: bootstrap_servers cannot be empty".into(),
            ));
        }
        Ok(Self {
            bootstrap_servers,
            group_id: group_id.map(|s| s.to_string()),
        })
    }

    /// Build a fresh `ClientConfig` populated with this backend's
    /// bootstrap servers + optional group_id. 36f will layer auth
    /// settings (SASL/PLAIN, SASL/SCRAM, mTLS, MSK IAM
    /// `oauthbearer_token_refresh_cb`) on top through builder
    /// methods on `KafkaBackend` so the framework works against
    /// Confluent Cloud, self-hosted Kafka with SASL, AWS MSK, and
    /// other cloud-managed Kafka services without privileging any
    /// specific deployment.
    pub(crate) fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap_servers);
        if let Some(gid) = &self.group_id {
            config.set("group.id", gid);
            // Phase 36e: explicit manual commits. Set here so even
            // throwaway clients (ping / metadata fetches) carry the
            // safe default.
            config.set("enable.auto.commit", "false");
        }
        config
    }

    /// Borrow the configured bootstrap servers for tests.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Borrow the consumer group id (None for producer-only).
    pub fn group_id(&self) -> Option<&str> {
        self.group_id.as_deref()
    }
}

#[async_trait]
impl Backend for KafkaBackend {
    fn dialect(&self) -> Dialect {
        Dialect::Streaming {
            kind: StreamingKind::Kafka,
        }
    }

    fn connection_info(&self) -> ConnectionInfo {
        // The framework's ConnectionInfo struct shape is DB-shaped
        // (host/port/dbname/user). For Kafka we project as:
        //   host  = first broker hostname (rough but identifying)
        //   port  = first broker port
        //   dbname = bootstrap_servers (full string for logs)
        //   user  = group_id if set, otherwise "producer"
        let (host, port) = parse_first_broker(&self.bootstrap_servers);
        ConnectionInfo {
            host,
            port,
            dbname: self.bootstrap_servers.clone(),
            user: self
                .group_id
                .clone()
                .unwrap_or_else(|| "producer".to_string()),
        }
    }

    fn dsn(&self) -> Option<String> {
        Some(self.bootstrap_servers.clone())
    }

    /// Liveness check via metadata fetch on an `AdminClient`. A
    /// successful metadata response means: brokers reachable, auth
    /// (if any) negotiated, cluster ID retrievable.
    async fn ping(&self) -> Result<(), BackendError> {
        // librdkafka's `fetch_metadata` is synchronous and blocks the
        // current thread up to its timeout. We construct the
        // AdminClient + run the fetch inside `spawn_blocking` so we
        // don't stall the tokio runtime. The client is dropped at the
        // end of the closure (no leak / lifetime trickery needed).
        let config = self.client_config();
        tokio::task::spawn_blocking(move || {
            let client: AdminClient<DefaultClientContext> = config
                .create()
                .map_err(|e| BackendError::Connection(format!("kafka admin client: {e}")))?;
            client
                .inner()
                .fetch_metadata(None, Duration::from_secs(5))
                .map_err(|e| BackendError::Connection(format!("kafka metadata: {e}")))
                .map(|_| ())
        })
        .await
        .map_err(|e| BackendError::Connection(format!("kafka metadata join: {e}")))?
    }

    /// Kafka has no SQL surface — reject explicitly.
    async fn execute(&self, _statement: &str) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "Kafka backend has no execute() surface — \
             use read_arrow_stream / write_arrow_stream (36b/c) or \
             run_append (36c+); merge / scd2 are not meaningful for a \
             streaming source"
                .into(),
        ))
    }

    async fn read_arrow_stream(&self, _query: &str) -> Result<ArrowBatchStream, BackendError> {
        Err(BackendError::Other(
            "Kafka read_arrow_stream lands in Phase 36b".into(),
        ))
    }

    async fn write_arrow_stream(
        &self,
        _target: &TargetTable,
        _stream: ArrowBatchStream,
        _mode: WriteMode,
    ) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "Kafka write_arrow_stream lands in Phase 36c".into(),
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
            "Kafka run_append lands in Phase 36c".into(),
        ))
    }

    async fn run_truncate(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "Kafka: truncate is not meaningful for a topic — \
             use mode='append' to produce, or delete the topic via \
             admin tools and recreate it"
                .into(),
        ))
    }

    /// Kafka topics are append-only logs; merge / SCD2 don't have a
    /// natural meaning. The user pattern here is: stream Kafka into a
    /// DB / Delta target and run merge / SCD2 there. Reject with a
    /// pointer.
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
            "Kafka: merge is not supported on a streaming target. \
             To merge upstream Kafka data into a transactional store, \
             use Kafka as the source and a DB / Delta backend as the \
             target — that's the canonical streaming-into-warehouse \
             pattern. (Log-compacted upserts are a future-phase \
             follow-up — see Phase 36 §streaming sinks.)"
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
            "Kafka: scd2 is not supported on a streaming target. \
             Use Kafka as a source and a DB / Delta backend as the \
             target."
                .into(),
        ))
    }
}

/// Best-effort parse of "host:port,host:port,..." → (first_host, first_port).
/// Used only by `connection_info()` for a human-readable label.
fn parse_first_broker(bootstrap_servers: &str) -> (String, u16) {
    let first = bootstrap_servers.split(',').next().unwrap_or("");
    if let Some((h, p)) = first.rsplit_once(':')
        && let Ok(port) = p.parse::<u16>()
    {
        return (h.to_string(), port);
    }
    (first.to_string(), 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_first_broker_extracts_host_port() {
        assert_eq!(
            parse_first_broker("kafka.example.com:9092"),
            ("kafka.example.com".into(), 9092)
        );
        assert_eq!(
            parse_first_broker("a.example.com:9094,b.example.com:9094"),
            ("a.example.com".into(), 9094)
        );
        assert_eq!(parse_first_broker(""), ("".into(), 0));
    }

    #[test]
    fn open_rejects_empty_bootstrap_servers() {
        let err = KafkaBackend::open("", None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("bootstrap_servers"), "got: {msg}");
    }

    #[test]
    fn open_succeeds_and_carries_dialect() {
        let b = KafkaBackend::open("localhost:9092", Some("g1")).unwrap();
        assert!(matches!(
            b.dialect(),
            Dialect::Streaming {
                kind: StreamingKind::Kafka
            }
        ));
        assert_eq!(b.bootstrap_servers(), "localhost:9092");
        assert_eq!(b.group_id(), Some("g1"));
        assert_eq!(b.dsn().as_deref(), Some("localhost:9092"));
    }

    #[test]
    fn connection_info_uses_first_broker() {
        let b = KafkaBackend::open("a:9092,b:9092", Some("g1")).unwrap();
        let info = b.connection_info();
        assert_eq!(info.host, "a");
        assert_eq!(info.port, 9092);
        assert_eq!(info.user, "g1");
    }

    #[test]
    fn connection_info_producer_user_label() {
        let b = KafkaBackend::open("a:9092", None).unwrap();
        let info = b.connection_info();
        assert_eq!(info.user, "producer");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_rejects_with_pointer() {
        let b = KafkaBackend::open("localhost:9092", None).unwrap();
        let err = b.execute("anything").await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no execute() surface"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn merge_rejects_with_canonical_pattern_pointer() {
        let b = KafkaBackend::open("localhost:9092", None).unwrap();
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
    async fn truncate_rejects_with_topic_admin_pointer() {
        let b = KafkaBackend::open("localhost:9092", None).unwrap();
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
        assert!(msg.contains("admin tools"), "got: {msg}");
    }
}
