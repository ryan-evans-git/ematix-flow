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
//! ## What 37c.2 adds (Arrow IO)
//!   - `read_arrow_stream(query)` — drains the bound stream's first
//!     shard. The session caches the shard iterator across calls
//!     and auto-advances via the `NextShardIterator` returned by
//!     each `GetRecords` response. First call starts at
//!     `TRIM_HORIZON` (oldest record); the iterator marches forward
//!     from there.
//!   - `query` is ignored beyond a non-empty check — the bound
//!     stream name is the consumption surface (Kinesis equivalent
//!     of "subscribe to a topic").
//!   - `write_arrow_stream(target, ...)` — encodes each row as
//!     JSONL, batches into `PutRecords` calls (max 500 records per
//!     call to match the AWS limit). `target.name` is treated as
//!     the partition key prefix; rows use `<target.name>-<row-idx>`
//!     so they hash across shards. If `target.name` is empty,
//!     defaults to `default-<row-idx>`.
//!   - `WriteMode::Truncate` is rejected — Kinesis streams are
//!     append-only; purging is an admin-API operation.
//!
//! ### Single-shard limit in 37c.2
//! 37c.2 reads only the **first shard** returned by `ListShards`.
//! Multi-shard fanout (parallel-drain across shards, merge into
//! one stream) lands in 37c.3 alongside per-shard sequence-number
//! checkpointing. Most LocalStack/dev streams are single-shard so
//! this restriction doesn't bite the integration test; production
//! callers that need multi-shard wait for 37c.3.
//!
//! ## What lands in 37c.x
//!   - 37c.3 — multi-shard fanout + sequence-number checkpoints
//!     (manual ack equivalent). Track the highest sequence-number
//!     per shard; `commit_offsets` advances a durable position so
//!     restart resumes at the committed checkpoint instead of
//!     `TRIM_HORIZON`.
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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_kinesis::Client;
use aws_sdk_kinesis::config::Region;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::{PutRecordsRequestEntry, ShardIteratorType};
use futures_util::StreamExt;
use tokio::sync::Mutex;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    StreamingKind, TargetTable, WriteMode,
};
use crate::kafka_backend::{decode_payloads_as_jsonl, encode_batch_as_jsonl_lines};
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

/// Per-call drain limits for `read_arrow_stream`. First trigger
/// flushes — the consumer returns whatever it has accumulated and
/// callers can call `read_arrow_stream` again to continue.
///
/// The defaults are tuned for AWS Kinesis: `batch_size` is capped
/// at 10_000 by the `GetRecords` API, but in practice 1000 is
/// plenty per call and lets the framework fan out batches.
#[derive(Debug, Clone)]
pub struct KinesisBatchConfig {
    /// Max records per call. Clamped to 10_000 (the AWS limit).
    pub batch_size: usize,
    /// Max bytes accumulated per call.
    pub batch_bytes: usize,
    /// Max number of consecutive empty `GetRecords` responses
    /// before we give up and return what we have. Each empty poll
    /// sleeps `idle_poll_ms` before retrying.
    pub max_empty_polls: u32,
    /// Sleep between empty `GetRecords` polls.
    pub idle_poll_ms: u64,
}

impl Default for KinesisBatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_bytes: 16 * 1024 * 1024, // 16 MiB
            max_empty_polls: 1,            // typical "did the broker have data" check
            idle_poll_ms: 250,
        }
    }
}

/// Persistent consumer session. Holds the current shard iterator so
/// successive `read_arrow_stream` calls advance through the stream
/// rather than re-reading from `TRIM_HORIZON` each time.
struct KinesisConsumerSession {
    /// Shard the session is bound to. 37c.2 binds to the first shard
    /// in `ListShards`; multi-shard fanout lands in 37c.3.
    shard_id: String,
    /// Current iterator. `None` means the shard has been read to
    /// the end (rare for long-lived streams). Updated after each
    /// `GetRecords` response.
    next_shard_iterator: Option<String>,
}

impl std::fmt::Debug for KinesisConsumerSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisConsumerSession")
            .field("shard_id", &self.shard_id)
            .field("has_iterator", &self.next_shard_iterator.is_some())
            .finish_non_exhaustive()
    }
}

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
/// Holds the stream name + optional region/endpoint/static-creds +
/// a lazily-initialized consumer session. Each per-op SDK client
/// is built lazily — same "config holder" pattern every other
/// Backend in the framework uses; the session adds the across-call
/// shard iterator state needed to march forward through the stream.
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
    /// Per-call consumer drain limits. Builder-set via
    /// `with_batch_config`.
    batch_config: KinesisBatchConfig,
    /// Lazy consumer session (Phase 37c.2). Populated on first
    /// `read_arrow_stream`; reused on subsequent calls so the
    /// shard iterator advances through the stream.
    consumer_session: Arc<Mutex<Option<KinesisConsumerSession>>>,
}

impl std::fmt::Debug for KinesisBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisBackend")
            .field("stream_name", &self.stream_name)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("has_static_credentials", &self.static_credentials.is_some())
            .field("batch_config", &self.batch_config)
            .finish_non_exhaustive()
    }
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
            batch_config: KinesisBatchConfig::default(),
            consumer_session: Arc::new(Mutex::new(None)),
        })
    }

    /// Override the per-call drain limits used by
    /// `read_arrow_stream`.
    pub fn with_batch_config(mut self, cfg: KinesisBatchConfig) -> Self {
        self.batch_config = cfg;
        self
    }

    /// Borrow the active batch config.
    pub fn batch_config(&self) -> &KinesisBatchConfig {
        &self.batch_config
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

    /// Open the persistent consumer session: list the stream's
    /// shards, pick the first one, and request a `TRIM_HORIZON`
    /// shard iterator for it. The iterator will be auto-advanced
    /// across `read_arrow_stream` calls.
    async fn open_consumer_session(
        &self,
        client: &Client,
    ) -> Result<KinesisConsumerSession, BackendError> {
        let shards_resp = client
            .list_shards()
            .stream_name(&self.stream_name)
            .send()
            .await
            .map_err(|e| BackendError::Connection(format!("kinesis list_shards: {e}")))?;
        let shards = shards_resp.shards.unwrap_or_default();
        let shard = shards.into_iter().next().ok_or_else(|| {
            BackendError::Other(format!(
                "kinesis read_arrow_stream: stream `{}` has no shards",
                self.stream_name
            ))
        })?;
        let shard_id = shard.shard_id().to_string();
        let iter_resp = client
            .get_shard_iterator()
            .stream_name(&self.stream_name)
            .shard_id(&shard_id)
            .shard_iterator_type(ShardIteratorType::TrimHorizon)
            .send()
            .await
            .map_err(|e| BackendError::Connection(format!("kinesis get_shard_iterator: {e}")))?;
        Ok(KinesisConsumerSession {
            shard_id,
            next_shard_iterator: iter_resp.shard_iterator,
        })
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

    /// Drain the bound stream's first shard via repeated
    /// `GetRecords`. The session caches the shard iterator across
    /// calls so subsequent invocations advance through the stream
    /// rather than re-reading from `TRIM_HORIZON`. Empty polls
    /// retry up to `batch_config.max_empty_polls` times before
    /// giving up.
    ///
    /// `query` is required to be non-empty but otherwise ignored —
    /// the bound `stream_name` is the consumption surface.
    ///
    /// Limits in 37c.2 — folded out in 37c.3:
    ///   - First shard only. Multi-shard fanout is a follow-up.
    ///   - Auto-advance: each call advances the iterator
    ///     irreversibly. Manual sequence-number commits via
    ///     `commit_offsets` land in 37c.3.
    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        let q = query.trim();
        if q.is_empty() {
            return Err(BackendError::Other(
                "Kinesis read_arrow_stream: query argument must be non-empty \
                 (the bound stream name is the consumption surface; pass any \
                 non-empty placeholder)"
                    .into(),
            ));
        }

        let cfg = self.batch_config.clone();
        let client = self.client().await?;

        let mut session_lock = self.consumer_session.lock().await;
        if session_lock.is_none() {
            *session_lock = Some(self.open_consumer_session(&client).await?);
        }
        let session = session_lock.as_mut().expect("session populated above");

        let mut payloads: Vec<Vec<u8>> = Vec::new();
        let mut bytes_total: usize = 0;
        let mut empty_polls = 0_u32;
        let limit = cfg.batch_size.min(10_000) as i32;

        loop {
            let Some(iterator) = session.next_shard_iterator.clone() else {
                // Iterator exhausted (rare). Drop the session so a
                // future call re-binds at TRIM_HORIZON; for 37c.2
                // this matches the "auto-advance" contract.
                break;
            };
            let resp = client
                .get_records()
                .shard_iterator(iterator)
                .limit(limit)
                .send()
                .await
                .map_err(|e| BackendError::Query(format!("kinesis get_records: {e}")))?;
            // Always advance to the new iterator, even on empty
            // responses — the AWS doc explicitly recommends this.
            session.next_shard_iterator = resp.next_shard_iterator;

            let records = resp.records;
            if records.is_empty() {
                empty_polls += 1;
                if empty_polls > cfg.max_empty_polls {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(cfg.idle_poll_ms)).await;
                continue;
            }
            for rec in records {
                let data = rec.data.into_inner();
                bytes_total += data.len();
                payloads.push(data);
                if payloads.len() >= cfg.batch_size || bytes_total >= cfg.batch_bytes {
                    break;
                }
            }
            if payloads.len() >= cfg.batch_size || bytes_total >= cfg.batch_bytes {
                break;
            }
        }
        // Drop the lock before decoding so concurrent callers don't
        // wait on JSON parsing.
        drop(session_lock);

        if payloads.is_empty() {
            let stream = futures_util::stream::empty();
            return Ok(Box::pin(stream));
        }
        let batches = decode_payloads_as_jsonl(payloads)?;
        let stream = futures_util::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }

    /// Produce each Arrow row as a Kinesis record to the bound
    /// stream. Each batch is encoded as JSONL and split row-wise;
    /// rows are batched into `PutRecords` calls of at most 500
    /// records (the AWS limit). `WriteMode::Truncate` is rejected.
    ///
    /// `target.name` is used as the partition-key prefix:
    /// `<target.name>-<row-idx>` for each row, so producers can
    /// fan rows across shards. An empty `target.name` yields
    /// `default-<row-idx>`. (The bound `stream_name` is the
    /// destination; `target.name` doesn't override it.)
    ///
    /// Limits in 37c.2:
    ///   - JSON-only payload format.
    ///   - Per-row partition key — no support yet for per-row
    ///     custom keys via the Arrow batch (would need a
    ///     `with_partition_key_column` builder).
    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError> {
        if mode == WriteMode::Truncate {
            return Err(BackendError::Other(
                "Kinesis write_arrow_stream: Truncate is not supported on a \
                 stream. Streams are append-only by design; to start fresh, \
                 delete and recreate the stream via the AWS admin API."
                    .into(),
            ));
        }
        let key_prefix = if target.name.trim().is_empty() {
            "default".to_string()
        } else {
            target.name.trim().to_string()
        };

        let client = self.client().await?;
        const MAX_RECORDS_PER_PUT: usize = 500;

        let mut s = stream;
        let mut total: u64 = 0;
        let mut row_counter: u64 = 0;
        while let Some(batch) = s.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let payloads = encode_batch_as_jsonl_lines(&batch)?;
            for chunk in payloads.chunks(MAX_RECORDS_PER_PUT) {
                let mut entries: Vec<PutRecordsRequestEntry> = Vec::with_capacity(chunk.len());
                for payload in chunk {
                    let pk = format!("{key_prefix}-{row_counter}");
                    row_counter += 1;
                    let entry = PutRecordsRequestEntry::builder()
                        .data(Blob::new(payload.clone()))
                        .partition_key(pk)
                        .build()
                        .map_err(|e| {
                            BackendError::Query(format!(
                                "kinesis PutRecordsRequestEntry build: {e}"
                            ))
                        })?;
                    entries.push(entry);
                }
                let entries_len = entries.len();
                let resp = client
                    .put_records()
                    .stream_name(&self.stream_name)
                    .set_records(Some(entries))
                    .send()
                    .await
                    .map_err(|e| BackendError::Query(format!("kinesis put_records: {e}")))?;
                let failed = resp.failed_record_count.unwrap_or(0);
                if failed > 0 {
                    return Err(BackendError::Query(format!(
                        "kinesis put_records: {failed} of {} records rejected",
                        entries_len
                    )));
                }
                total += entries_len as u64;
            }
        }
        Ok(total)
    }

    /// Cross-backend produce-side run_append: read source rows via
    /// the source's `read_arrow_stream`, produce them to the bound
    /// Kinesis stream via `write_arrow_stream`. Mirrors the Kafka /
    /// RabbitMQ / Pub/Sub run_append shape.
    async fn run_append(
        &self,
        spec: &TableSpec,
        source_query: &str,
        _pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        _incremental_column: Option<&str>,
        _last_value_literal: Option<&str>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        let source = source_backend.ok_or_else(|| {
            BackendError::Other(
                "Kinesis run_append: source_backend is required (cross-backend produce)".into(),
            )
        })?;
        if dry_run {
            return Ok(StrategyRunResult {
                run_id: String::new(),
                rows_inserted: 0,
                rows_updated: None,
                rows_unchanged: None,
                rows_closed: None,
                status: "dry-run".into(),
                path: format!("kinesis:{}", self.stream_name),
            });
        }
        let target = TargetTable {
            schema: spec.schema.clone(),
            name: spec.name.clone(),
        };
        let stream = source.read_arrow_stream(source_query).await?;
        let n = self
            .write_arrow_stream(&target, stream, WriteMode::Append)
            .await?;
        Ok(StrategyRunResult {
            run_id: String::new(),
            rows_inserted: n as i64,
            rows_updated: None,
            rows_unchanged: None,
            rows_closed: None,
            status: "ok".into(),
            path: format!("kinesis:{}", self.stream_name),
        })
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
    async fn read_arrow_stream_rejects_empty_query() {
        let b = KinesisBackend::open("s").unwrap();
        let err = match b.read_arrow_stream("   ").await {
            Ok(_) => panic!("expected empty-query rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("non-empty"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_arrow_stream_truncate_rejects() {
        let b = KinesisBackend::open("s").unwrap();
        let target = TargetTable {
            schema: "".into(),
            name: "any".into(),
        };
        let stream = Box::pin(futures_util::stream::empty());
        let err = b
            .write_arrow_stream(&target, stream, WriteMode::Truncate)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Truncate is not supported"), "got: {msg}");
    }

    #[test]
    fn batch_config_defaults_are_reasonable() {
        let cfg = KinesisBatchConfig::default();
        assert!(cfg.batch_size >= 100);
        assert!(cfg.batch_bytes >= 1 << 20);
        assert!(cfg.max_empty_polls >= 1);
        assert!(cfg.idle_poll_ms >= 100);
    }

    #[test]
    fn with_batch_config_overrides_default() {
        let b = KinesisBackend::open("s")
            .unwrap()
            .with_batch_config(KinesisBatchConfig {
                batch_size: 7,
                batch_bytes: 99,
                max_empty_polls: 2,
                idle_poll_ms: 11,
            });
        assert_eq!(b.batch_config().batch_size, 7);
        assert_eq!(b.batch_config().batch_bytes, 99);
        assert_eq!(b.batch_config().max_empty_polls, 2);
        assert_eq!(b.batch_config().idle_poll_ms, 11);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_append_requires_source_backend() {
        let b = KinesisBackend::open("s").unwrap();
        let spec = TableSpec {
            schema: "".into(),
            name: "t".into(),
            columns: vec![],
            unique_constraints: vec![],
            fingerprint: String::new(),
        };
        let err = b
            .run_append(&spec, "x", "p", None, None, None, false)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("source_backend is required"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_append_dry_run_returns_zero_inserts_without_io() {
        let b = KinesisBackend::open("my-stream").unwrap();
        let spec = TableSpec {
            schema: "".into(),
            name: "t".into(),
            columns: vec![],
            unique_constraints: vec![],
            fingerprint: String::new(),
        };
        let other = KinesisBackend::open("my-stream").unwrap();
        let res = b
            .run_append(&spec, "x", "p", Some(&other), None, None, true)
            .await
            .unwrap();
        assert_eq!(res.rows_inserted, 0);
        assert_eq!(res.status, "dry-run");
        assert_eq!(res.path, "kinesis:my-stream");
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
