//! Phase 37c: AWS Kinesis backend skeleton.
//!
//! Wraps `aws-sdk-kinesis` (smithy/runtime) as a `Backend`. 37c
//! covers the connection-level surface only — `read_arrow_stream` /
//! `write_arrow_stream` and the strategy executors are stubbed and
//! land in 37c.x.
//!
//! ## What 37c ships
//!   - `KinesisBackend::open(stream_name)` — minimal constructor.
//!     Region resolves from env/config the way every aws-sdk-rust
//!     client resolves it.
//!   - `with_region(region)` builder — pin a specific region.
//!   - `with_endpoint(url)` builder — overrides the default
//!     regional endpoint. Required for LocalStack
//!     (`http://localhost:4566`).
//!   - `with_static_credentials(access_key, secret_key)` builder —
//!     bypasses the AWS credential chain. Required for LocalStack
//!     and for tests that don't have AWS_PROFILE set.
//!   - `dialect()` → `Dialect::Streaming { kind: Kinesis }`.
//!   - `connection_info()` reports endpoint host:port + the
//!     stream-name-as-user.
//!   - `dsn()` returns `kinesis://<stream_name>`.
//!   - `ping()` calls `list_streams()` under a 5s timeout. Validates
//!     credentials + endpoint reachability.
//!   - `execute()` rejects: Kinesis has no SQL surface.
//!   - All five strategy executors stub with phase-marker errors,
//!     mirroring the Kafka / RabbitMQ / Pub/Sub skeletons.
//!
//! ## What lands in 37c.x
//!   - 37c.2 — Arrow IO. Read: `GetShardIterator` + `GetRecords` per
//!     shard, fan out across shards, decode JSONL. Write: batch rows
//!     into `PutRecords` calls (max 500 records, 5 MiB / call).
//!   - 37c.3 — sequence-number checkpoints (manual ack equivalent).
//!     Track the highest sequence-number per shard; `commit_offsets`
//!     persists them via DynamoDB or an in-process store.
//!   - 37c.4 — DLQ via the streaming pipeline's app-level pattern
//!     (the existing `write_arrow_stream` already supports it; just
//!     point `dlq_target` at a separate Kinesis stream).
//!
//! ## Why a stream-bound constructor
//! Unlike Pub/Sub (project-bound) or Kafka (broker-bound), a
//! Kinesis client is per-stream in practice — every read/write API
//! takes a stream name. We bind it at construction time so
//! `read_arrow_stream(query)` / `write_arrow_stream(target)` don't
//! need to re-thread the name through every call. Cross-stream
//! pipelines instantiate one `KinesisBackend` per stream.

use std::time::Duration;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_kinesis::Client;
use aws_sdk_kinesis::config::Region;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    StreamingKind, TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

/// Static AWS credential pair. Used by tests + LocalStack to bypass
/// the AWS credential chain. Production callers leave this `None`
/// and rely on the SDK's default chain (env, IMDS, shared config).
#[derive(Debug, Clone)]
struct StaticAwsCredentials {
    access_key_id: String,
    secret_access_key: String,
}

/// AWS Kinesis-backed implementation of `Backend`.
///
/// Holds the stream name + optional region/endpoint/static-creds.
/// Each per-op SDK client is built lazily from these — same lazy
/// "config holder" pattern every other Backend in the framework
/// uses.
#[derive(Debug, Clone)]
pub struct KinesisBackend {
    /// Kinesis stream name. Bound at construction time and used by
    /// every `read_arrow_stream` / `write_arrow_stream` call.
    stream_name: String,
    /// Optional AWS region override. `None` means resolve from the
    /// AWS credential chain (env, shared config, IMDS).
    region: Option<String>,
    /// Optional endpoint override. `None` uses the default regional
    /// endpoint. Set to `http://localhost:4566` for LocalStack.
    endpoint: Option<String>,
    /// Optional static credentials. `None` falls back to the AWS
    /// credential chain.
    static_credentials: Option<StaticAwsCredentials>,
}

impl KinesisBackend {
    /// Construct a Kinesis backend bound to `stream_name`. Validates
    /// non-emptiness; region / endpoint / credentials checked on
    /// `ping()` / first IO.
    pub fn open(stream_name: impl Into<String>) -> Result<Self, BackendError> {
        let stream_name = stream_name.into();
        if stream_name.trim().is_empty() {
            return Err(BackendError::Connection(
                "kinesis backend: stream_name cannot be empty".into(),
            ));
        }
        Ok(Self {
            stream_name,
            region: None,
            endpoint: None,
            static_credentials: None,
        })
    }

    /// Pin the AWS region (overrides chain resolution).
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Override the SDK endpoint. Use `http://<host>:<port>` for
    /// LocalStack; production code uses the default regional
    /// endpoint.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set static AWS credentials, bypassing the AWS credential
    /// chain. Required for LocalStack + tests; production code
    /// leaves this off.
    pub fn with_static_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.static_credentials = Some(StaticAwsCredentials {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        });
        self
    }

    /// Borrow the stream name.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Borrow the configured region (if any).
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    /// Borrow the configured endpoint (if any).
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// Whether static credentials are configured.
    pub fn has_static_credentials(&self) -> bool {
        self.static_credentials.is_some()
    }

    /// Build a Kinesis SDK `Client` matching this backend's config.
    /// Used by `ping` and (in 37c.x+) by every per-op call.
    async fn client(&self) -> Result<Client, BackendError> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = &self.region {
            loader = loader.region(Region::new(region.clone()));
        }
        if let Some(ep) = &self.endpoint {
            loader = loader.endpoint_url(ep);
        }
        if let Some(creds) = &self.static_credentials {
            loader = loader.credentials_provider(Credentials::new(
                creds.access_key_id.clone(),
                creds.secret_access_key.clone(),
                None,
                None,
                "ematix-flow-static",
            ));
        }
        let cfg = loader.load().await;
        Ok(Client::new(&cfg))
    }
}

#[async_trait]
impl Backend for KinesisBackend {
    fn dialect(&self) -> Dialect {
        Dialect::Streaming {
            kind: StreamingKind::Kinesis,
        }
    }

    fn connection_info(&self) -> ConnectionInfo {
        // Kinesis endpoints follow `kinesis.<region>.amazonaws.com`
        // by default. We expose either the configured override or a
        // synthesized default-region label.
        let endpoint = self.endpoint.clone().unwrap_or_else(|| {
            format!(
                "kinesis.{}.amazonaws.com",
                self.region.as_deref().unwrap_or("<default>")
            )
        });
        let parsed = parse_endpoint(&endpoint);
        ConnectionInfo {
            host: parsed.host,
            port: parsed.port,
            dbname: endpoint,
            user: self.stream_name.clone(),
        }
    }

    fn dsn(&self) -> Option<String> {
        Some(format!("kinesis://{}", self.stream_name))
    }

    /// Build a Kinesis client and call `list_streams()` under a 5s
    /// timeout. Validates credentials + endpoint reachability
    /// without requiring the bound stream to exist.
    async fn ping(&self) -> Result<(), BackendError> {
        let fut = async {
            let client = self.client().await?;
            client
                .list_streams()
                .send()
                .await
                .map(|_| ())
                .map_err(|e| BackendError::Connection(format!("kinesis list_streams: {e}")))
        };
        tokio::time::timeout(Duration::from_secs(5), fut)
            .await
            .map_err(|_| BackendError::Connection("kinesis ping: timed out after 5s".into()))?
    }

    /// Kinesis has no SQL surface — reject explicitly.
    async fn execute(&self, _statement: &str) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "Kinesis backend has no execute() surface — \
             use read_arrow_stream / write_arrow_stream (37c.2) or \
             run_append (37c.2+); merge / scd2 are not meaningful for a \
             streaming source"
                .into(),
        ))
    }

    /// Arrow consume — stub for 37c.2.
    async fn read_arrow_stream(&self, _query: &str) -> Result<ArrowBatchStream, BackendError> {
        Err(BackendError::Other(
            "Kinesis read_arrow_stream: lands in Phase 37c.2 \
             (GetShardIterator + GetRecords per shard → Arrow batches)"
                .into(),
        ))
    }

    /// Arrow produce — stub for 37c.2.
    async fn write_arrow_stream(
        &self,
        _target: &TargetTable,
        _stream: ArrowBatchStream,
        _mode: WriteMode,
    ) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "Kinesis write_arrow_stream: lands in Phase 37c.2 \
             (Arrow batches → PutRecords with batching, max 500 records / 5 MiB)"
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
            "Kinesis run_append: lands in Phase 37c.2+ once \
             write_arrow_stream is implemented"
                .into(),
        ))
    }

    /// `Truncate` on a stream isn't a producer-side operation — it's
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
            "Kinesis run_truncate: streams are append-only by design; to \
             start fresh, delete and recreate the stream via the AWS \
             admin API or `aws kinesis` CLI"
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
            "Kinesis run_merge: merge has no natural meaning for a Kinesis \
             stream (every PutRecords call is independent). To merge upstream \
             Kinesis data into a transactional store, use Kinesis as the \
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
            "Kinesis run_scd2: SCD2 versioning has no natural meaning for a \
             Kinesis stream. Use Kinesis as a source and a DB / Delta backend \
             as the SCD2 target"
                .into(),
        ))
    }
}

/// Best-effort parse of an endpoint URL into the `ConnectionInfo`
/// shape. Strips the scheme (default https → 443, http → 80, no
/// scheme → port 0).
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
    fn open_rejects_empty_stream_name() {
        let err = KinesisBackend::open("").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot be empty"), "got: {msg}");
    }

    #[test]
    fn open_rejects_whitespace_stream_name() {
        let err = KinesisBackend::open("   ").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot be empty"), "got: {msg}");
    }

    #[test]
    fn open_accepts_simple_stream_name() {
        let b = KinesisBackend::open("my-stream").unwrap();
        assert_eq!(b.stream_name(), "my-stream");
        assert!(b.region().is_none());
        assert!(b.endpoint().is_none());
        assert!(!b.has_static_credentials());
    }

    #[test]
    fn dialect_is_streaming_kinesis() {
        let b = KinesisBackend::open("s").unwrap();
        assert_eq!(
            b.dialect(),
            Dialect::Streaming {
                kind: StreamingKind::Kinesis
            }
        );
    }

    #[test]
    fn dsn_returns_kinesis_uri() {
        let b = KinesisBackend::open("my-stream").unwrap();
        assert_eq!(b.dsn().as_deref(), Some("kinesis://my-stream"));
    }

    #[test]
    fn connection_info_uses_default_endpoint_when_none() {
        let b = KinesisBackend::open("s").unwrap().with_region("us-east-1");
        let info = b.connection_info();
        assert_eq!(info.host, "kinesis.us-east-1.amazonaws.com");
        assert_eq!(info.user, "s");
    }

    #[test]
    fn connection_info_uses_custom_endpoint() {
        let b = KinesisBackend::open("s")
            .unwrap()
            .with_endpoint("http://localhost:4566");
        let info = b.connection_info();
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, 4566);
    }

    #[test]
    fn with_region_records_value() {
        let b = KinesisBackend::open("s").unwrap().with_region("us-west-2");
        assert_eq!(b.region(), Some("us-west-2"));
    }

    #[test]
    fn with_endpoint_records_value() {
        let b = KinesisBackend::open("s")
            .unwrap()
            .with_endpoint("http://localhost:4566");
        assert_eq!(b.endpoint(), Some("http://localhost:4566"));
    }

    #[test]
    fn with_static_credentials_flips_flag() {
        let b = KinesisBackend::open("s")
            .unwrap()
            .with_static_credentials("ak", "sk");
        assert!(b.has_static_credentials());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_rejects_with_pointer() {
        let b = KinesisBackend::open("s").unwrap();
        let err = b.execute("anything").await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no execute() surface"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_arrow_stream_rejects_with_pointer_to_37c_2() {
        let b = KinesisBackend::open("s").unwrap();
        let err = match b.read_arrow_stream("any").await {
            Ok(_) => panic!("expected stub rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("Phase 37c.2"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_arrow_stream_rejects_with_pointer_to_37c_2() {
        let b = KinesisBackend::open("s").unwrap();
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
        assert!(msg.contains("Phase 37c.2"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_rejects_with_admin_pointer() {
        let b = KinesisBackend::open("s").unwrap();
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
            msg.contains("admin API") || msg.contains("aws kinesis"),
            "got: {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn merge_rejects_with_canonical_pattern_pointer() {
        let b = KinesisBackend::open("s").unwrap();
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
        let b = KinesisBackend::open("s").unwrap();
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
