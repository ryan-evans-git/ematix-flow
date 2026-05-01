//! Phase 37b: GCP Pub/Sub backend skeleton.
//!
//! Wraps `google-cloud-pubsub` (gRPC, googleapis/google-cloud-rust)
//! as a `Backend`. 37b covers the connection-level surface only —
//! `read_arrow_stream` / `write_arrow_stream` and the strategy
//! executors are stubbed and land in 37b.x.
//!
//! ## What 37b ships
//!   - `PubSubBackend::open(project_id)` — minimal constructor.
//!   - `with_endpoint(url)` builder — overrides the default
//!     `https://pubsub.googleapis.com` endpoint. Required for the
//!     gcloud Pub/Sub emulator (e.g. `http://localhost:8085`).
//!   - `with_anonymous_auth()` builder — opts into anonymous
//!     credentials. The emulator doesn't require auth, so production
//!     code uses Application Default Credentials (ADC) and tests use
//!     this opt-out.
//!   - `dialect()` → `Dialect::Streaming { kind: PubSub }`.
//!   - `connection_info()` reports endpoint host/port + the
//!     project_id-as-user.
//!   - `dsn()` returns `pubsub://<project_id>`.
//!   - `ping()` constructs a `TopicAdmin` client and lists topics in
//!     the project — under a 5s timeout. Validates auth + endpoint
//!     reachability without requiring any pre-existing topic.
//!   - `execute()` rejects: Pub/Sub has no SQL surface.
//!   - All five strategy executors stub with phase-marker errors,
//!     mirroring the Kafka / RabbitMQ skeletons.
//!
//! ## What lands in 37b.x
//!   - 37b.2 — Arrow IO: `read_arrow_stream(subscription)` via
//!     pull-based subscriber, `write_arrow_stream(topic, ...)` via
//!     publisher with batching.
//!   - 37b.3 — manual ack with at-least-once delivery semantics
//!     (tracks ack_ids; `commit_offsets()` issues `acknowledge` RPC).
//!   - 37b.4 — DLQ via the Pub/Sub-native dead-letter-policy
//!     attached at subscription declaration time + `nack` on
//!     pull deliveries.

use std::time::Duration;

use async_trait::async_trait;
use google_cloud_auth::credentials::Credentials;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousAuthBuilder;
use google_cloud_pubsub::client::TopicAdmin;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    StreamingKind, TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

/// GCP Pub/Sub-backed implementation of `Backend`.
///
/// Holds the project_id + optional endpoint + optional anonymous
/// auth flag. Each per-op gRPC client is built lazily from these
/// — matches the "config holder" pattern every other Backend in
/// the framework uses.
#[derive(Debug, Clone)]
pub struct PubSubBackend {
    /// GCP project id (the bare id, not the fully-qualified
    /// `projects/<id>` form). The framework constructs the
    /// fully-qualified name internally as needed.
    project_id: String,
    /// Optional gRPC endpoint override. `None` uses the default
    /// `https://pubsub.googleapis.com`. Set to
    /// `http://localhost:8085` for the gcloud emulator.
    endpoint: Option<String>,
    /// When true, ping/IO use anonymous credentials (no auth
    /// headers). Required for the emulator path.
    anonymous_auth: bool,
}

impl PubSubBackend {
    /// Construct a Pub/Sub backend bound to `project_id`. Validates
    /// non-emptiness; auth + endpoint reachability are checked on
    /// `ping()` / first IO.
    pub fn open(project_id: impl Into<String>) -> Result<Self, BackendError> {
        let project_id = project_id.into();
        if project_id.trim().is_empty() {
            return Err(BackendError::Connection(
                "pubsub backend: project_id cannot be empty".into(),
            ));
        }
        Ok(Self {
            project_id,
            endpoint: None,
            anonymous_auth: false,
        })
    }

    /// Override the gRPC endpoint. Use `http://<host>:<port>` for
    /// the gcloud Pub/Sub emulator; production code uses the
    /// default `https://pubsub.googleapis.com`.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Opt into anonymous credentials. Required for the emulator
    /// path — production deployments leave this off and
    /// Application Default Credentials are used.
    pub fn with_anonymous_auth(mut self) -> Self {
        self.anonymous_auth = true;
        self
    }

    /// Borrow the project id.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Borrow the configured endpoint (if any).
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// Whether anonymous auth is enabled.
    pub fn is_anonymous_auth(&self) -> bool {
        self.anonymous_auth
    }

    /// Build a `TopicAdmin` client matching this backend's config.
    /// Used by `ping` and (in 37b.x+) by run-control / topic
    /// existence checks. Per-op clients are cheap; the underlying
    /// gRPC channel is built per call.
    async fn topic_admin_client(&self) -> Result<TopicAdmin, BackendError> {
        let mut builder = TopicAdmin::builder();
        if let Some(ep) = &self.endpoint {
            builder = builder.with_endpoint(ep.clone());
        }
        if self.anonymous_auth {
            let creds: Credentials = AnonymousAuthBuilder::new().build();
            builder = builder.with_credentials(creds);
        }
        builder
            .build()
            .await
            .map_err(|e| BackendError::Connection(format!("pubsub TopicAdmin build: {e}")))
    }
}

#[async_trait]
impl Backend for PubSubBackend {
    fn dialect(&self) -> Dialect {
        Dialect::Streaming {
            kind: StreamingKind::PubSub,
        }
    }

    fn connection_info(&self) -> ConnectionInfo {
        // The framework's ConnectionInfo struct shape is DB-shaped
        // (host/port/dbname/user). For Pub/Sub we project as:
        //   host  = endpoint host (default pubsub.googleapis.com)
        //   port  = endpoint port (default 443; emulator uses
        //           whatever PUBSUB_EMULATOR_HOST advertises)
        //   dbname = full endpoint URL
        //   user  = project_id
        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| "https://pubsub.googleapis.com".into());
        let parsed = parse_endpoint(&endpoint);
        ConnectionInfo {
            host: parsed.host,
            port: parsed.port,
            dbname: endpoint,
            user: self.project_id.clone(),
        }
    }

    fn dsn(&self) -> Option<String> {
        Some(format!("pubsub://{}", self.project_id))
    }

    /// Construct a `TopicAdmin` client and list topics in the
    /// project — under a 5s timeout. Validates that the auth chain
    /// resolves and the endpoint is reachable, without requiring
    /// any pre-existing topic. An empty topic list is success.
    async fn ping(&self) -> Result<(), BackendError> {
        let project_path = format!("projects/{}", self.project_id);
        let fut = async {
            let client = self.topic_admin_client().await?;
            client
                .list_topics()
                .set_project(project_path)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| BackendError::Connection(format!("pubsub list_topics: {e}")))
        };
        tokio::time::timeout(Duration::from_secs(5), fut)
            .await
            .map_err(|_| BackendError::Connection("pubsub ping: timed out after 5s".into()))?
    }

    /// Pub/Sub has no SQL surface — reject explicitly.
    async fn execute(&self, _statement: &str) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "Pub/Sub backend has no execute() surface — \
             use read_arrow_stream / write_arrow_stream (37b.2) or \
             run_append (37b.2+); merge / scd2 are not meaningful for a \
             streaming source"
                .into(),
        ))
    }

    /// Arrow consume — stub for 37b.2.
    async fn read_arrow_stream(&self, _query: &str) -> Result<ArrowBatchStream, BackendError> {
        Err(BackendError::Other(
            "Pub/Sub read_arrow_stream: lands in Phase 37b.2 \
             (subscription pull → Arrow batches via google-cloud-pubsub \
             Subscriber + arrow-json)"
                .into(),
        ))
    }

    /// Arrow produce — stub for 37b.2.
    async fn write_arrow_stream(
        &self,
        _target: &TargetTable,
        _stream: ArrowBatchStream,
        _mode: WriteMode,
    ) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "Pub/Sub write_arrow_stream: lands in Phase 37b.2 \
             (Arrow batches → topic via google-cloud-pubsub Publisher)"
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
            "Pub/Sub run_append: lands in Phase 37b.2+ once \
             write_arrow_stream is implemented"
                .into(),
        ))
    }

    /// `Truncate` on a topic isn't a producer-side operation — it's
    /// an admin/management API operation. Reject with a clear pointer.
    async fn run_truncate(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "Pub/Sub run_truncate: topics are append-only streams; to start \
             fresh, delete and recreate the topic via gcloud / the Pub/Sub \
             admin API"
                .into(),
        ))
    }

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
            "Pub/Sub run_merge: merge has no natural meaning for a Pub/Sub \
             topic (every publish is independent). To merge upstream \
             Pub/Sub data into a transactional store, use Pub/Sub as the \
             source and a DB / Delta backend as the target"
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
            "Pub/Sub run_scd2: SCD2 versioning has no natural meaning for a \
             Pub/Sub topic. Use Pub/Sub as a source and a DB / Delta backend \
             as the SCD2 target"
                .into(),
        ))
    }
}

/// Best-effort parse of a Pub/Sub endpoint URL into the
/// `ConnectionInfo` shape. Strips the scheme, splits on `:` for the
/// port, and applies sane defaults (443 for https, 80 for http,
/// 8085 for the emulator's typical port).
struct ParsedEndpoint {
    host: String,
    port: u16,
}

fn parse_endpoint(url: &str) -> ParsedEndpoint {
    let (default_port, rest) = if let Some(rest) = url.strip_prefix("https://") {
        (443_u16, rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (80_u16, rest)
    } else {
        return ParsedEndpoint {
            host: url.to_string(),
            port: 0,
        };
    };
    let host_port = rest.split('/').next().unwrap_or(rest);
    match host_port.rsplit_once(':') {
        Some((h, p)) => ParsedEndpoint {
            host: h.to_string(),
            port: p.parse::<u16>().unwrap_or(default_port),
        },
        None => ParsedEndpoint {
            host: host_port.to_string(),
            port: default_port,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_rejects_empty_project_id() {
        let err = PubSubBackend::open("").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot be empty"), "got: {msg}");
    }

    #[test]
    fn open_rejects_whitespace_project_id() {
        let err = PubSubBackend::open("   ").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot be empty"), "got: {msg}");
    }

    #[test]
    fn open_accepts_simple_project_id() {
        let b = PubSubBackend::open("my-project").unwrap();
        assert_eq!(b.project_id(), "my-project");
        assert!(b.endpoint().is_none());
        assert!(!b.is_anonymous_auth());
    }

    #[test]
    fn dialect_is_streaming_pubsub() {
        let b = PubSubBackend::open("p").unwrap();
        assert_eq!(
            b.dialect(),
            Dialect::Streaming {
                kind: StreamingKind::PubSub
            }
        );
    }

    #[test]
    fn dsn_returns_pubsub_uri() {
        let b = PubSubBackend::open("my-project").unwrap();
        assert_eq!(b.dsn().as_deref(), Some("pubsub://my-project"));
    }

    #[test]
    fn connection_info_uses_default_endpoint() {
        let b = PubSubBackend::open("my-project").unwrap();
        let info = b.connection_info();
        assert_eq!(info.host, "pubsub.googleapis.com");
        assert_eq!(info.port, 443);
        assert_eq!(info.user, "my-project");
    }

    #[test]
    fn connection_info_uses_custom_endpoint() {
        let b = PubSubBackend::open("p")
            .unwrap()
            .with_endpoint("http://localhost:8085");
        let info = b.connection_info();
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, 8085);
        assert_eq!(info.dbname, "http://localhost:8085");
    }

    #[test]
    fn with_endpoint_overrides_default() {
        let b = PubSubBackend::open("p")
            .unwrap()
            .with_endpoint("http://localhost:8085");
        assert_eq!(b.endpoint(), Some("http://localhost:8085"));
    }

    #[test]
    fn with_anonymous_auth_flips_flag() {
        let b = PubSubBackend::open("p").unwrap().with_anonymous_auth();
        assert!(b.is_anonymous_auth());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_rejects_with_pointer() {
        let b = PubSubBackend::open("p").unwrap();
        let err = b.execute("anything").await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no execute() surface"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_arrow_stream_rejects_with_pointer_to_37b_2() {
        let b = PubSubBackend::open("p").unwrap();
        let err = match b.read_arrow_stream("any-sub").await {
            Ok(_) => panic!("expected stub rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("Phase 37b.2"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_arrow_stream_rejects_with_pointer_to_37b_2() {
        let b = PubSubBackend::open("p").unwrap();
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
        assert!(msg.contains("Phase 37b.2"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_rejects_with_admin_pointer() {
        let b = PubSubBackend::open("p").unwrap();
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
        assert!(msg.contains("admin API"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn merge_rejects_with_canonical_pattern_pointer() {
        let b = PubSubBackend::open("p").unwrap();
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
        let b = PubSubBackend::open("p").unwrap();
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
