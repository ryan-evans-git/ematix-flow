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
//! ## What 37c.3 adds (multi-shard + checkpoints)
//!   - Multi-shard fanout: `read_arrow_stream` drains **every**
//!     shard returned by `ListShards`, not just the first. Each
//!     shard gets its own `ShardCursor` (current iterator + pending
//!     and committed sequence numbers).
//!   - Per-shard sequence-number tracking. As records are pulled
//!     the cursor's `pending_sequence_number` updates to the
//!     highest seen.
//!   - `commit_offsets()` (the `Backend`-trait hook used by the
//!     streaming pipeline) advances each shard's
//!     `committed_sequence_number = pending_sequence_number`. No
//!     broker round-trip — Kinesis has no native checkpoint API,
//!     so the framework manages it.
//!   - `reset_to_committed_offsets()` invalidates the in-memory
//!     iterators so the next `read_arrow_stream` rebuilds each
//!     iterator via `AFTER_SEQUENCE_NUMBER` from the committed
//!     position. This is the analog of "process restart from the
//!     last commit" within a single backend lifetime.
//!
//! ### Manual-ack semantics (mirrors RabbitMQ 37a.3 / Pub/Sub 37b.3)
//!
//! Producer perspective:
//!
//! 1. read_arrow_stream → returns records, advances pending
//! 2. process records (write to target)
//! 3. On success: commit_offsets → pending → committed
//! 4. On failure: reset_to_committed_offsets → next read re-bootstraps
//!    from committed and re-delivers.
//!
//! ### What's still open
//! Checkpoint state is **in-memory only**. If the backend drops
//! without commit, the next process starts from `TRIM_HORIZON`
//! (or wherever a fresh `KinesisBackend::open` would). Durable
//! checkpoint storage (DynamoDB / file) is a documented follow-up;
//! the streaming-pipeline-level contract still holds because the
//! pipeline calls `commit_offsets` after every successful target
//! write, and the framework's at-least-once guarantee is "no
//! commit before durable target write".
//!
//! ## What lands in 37c.4
//!   - DLQ via the streaming pipeline's app-level pattern (the
//!     existing `write_arrow_stream` already supports it; just
//!     point `dlq_target` at a separate Kinesis stream).
//!
//! ## Why a stream-bound constructor
//! Unlike Pub/Sub (project-bound) or Kafka (broker-bound), a
//! Kinesis client is per-stream in practice — every read/write API
//! takes a stream name. We bind it at construction time so
//! `read_arrow_stream(query)` / `write_arrow_stream(target)` don't
//! need to re-thread the name through every call. Cross-stream
//! pipelines instantiate one `KinesisBackend` per stream.

use std::collections::BTreeMap;
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

/// Per-shard cursor state held in the consumer session.
///
/// Tracks three sequence-number-related slots:
///   - `next_iterator` — the SDK's opaque iterator for the next
///     `GetRecords` call. `None` means "rebuild from
///     `committed_sequence_number` on the next read".
///   - `pending_sequence_number` — highest sequence number observed
///     since the last commit. Advances on every `GetRecords` that
///     returns records.
///   - `committed_sequence_number` — last committed checkpoint.
///     Updated only by `commit_offsets`. Used to bootstrap a fresh
///     iterator (via `AFTER_SEQUENCE_NUMBER`) when `next_iterator`
///     is `None`.
#[derive(Debug, Clone, Default)]
struct ShardCursor {
    next_iterator: Option<String>,
    pending_sequence_number: Option<String>,
    committed_sequence_number: Option<String>,
}

/// Persistent consumer session. Holds one cursor per shard so
/// successive `read_arrow_stream` calls drain the whole stream and
/// `commit_offsets` advances per-shard checkpoints.
struct KinesisConsumerSession {
    /// `BTreeMap` for deterministic iteration order across shards
    /// (lets the test assertions stay stable regardless of which
    /// shard a record landed on).
    cursors: BTreeMap<String, ShardCursor>,
}

impl std::fmt::Debug for KinesisConsumerSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisConsumerSession")
            .field("shard_count", &self.cursors.len())
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
    /// Phase 37c.3: number of shards with pending (uncommitted)
    /// sequence numbers. Observability hook for tests + the
    /// streaming pipeline. Returns 0 when no session has been
    /// opened or every shard has been fully committed.
    pub async fn pending_sequence_count(&self) -> usize {
        self.consumer_session
            .lock()
            .await
            .as_ref()
            .map(|s| {
                s.cursors
                    .values()
                    .filter(|c| c.pending_sequence_number.is_some())
                    .count()
            })
            .unwrap_or(0)
    }

    /// Phase 37c.3: invalidate every shard's in-memory iterator and
    /// drop pending (uncommitted) sequence numbers. The next
    /// `read_arrow_stream` will rebuild iterators from the
    /// committed checkpoint via `AFTER_SEQUENCE_NUMBER` (or
    /// `TRIM_HORIZON` if the shard has never been committed).
    ///
    /// This is the analog of "process restart from the last commit"
    /// within a single backend lifetime — handy for retry loops in
    /// the streaming pipeline when the target write fails.
    /// No-op if no session has been opened.
    pub async fn reset_to_committed_offsets(&self) -> Result<(), BackendError> {
        let mut session_lock = self.consumer_session.lock().await;
        let Some(session) = session_lock.as_mut() else {
            return Ok(());
        };
        for cursor in session.cursors.values_mut() {
            cursor.next_iterator = None;
            cursor.pending_sequence_number = None;
        }
        Ok(())
    }

    /// Open the persistent consumer session: list every shard in
    /// the stream and create an empty `ShardCursor` for each (no
    /// iterator yet — bootstrapped lazily on first read).
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
        if shards.is_empty() {
            return Err(BackendError::Other(format!(
                "kinesis read_arrow_stream: stream `{}` has no shards",
                self.stream_name
            )));
        }
        let mut cursors = BTreeMap::new();
        for shard in shards {
            cursors.insert(shard.shard_id().to_string(), ShardCursor::default());
        }
        Ok(KinesisConsumerSession { cursors })
    }

    /// Bootstrap a shard iterator from the cursor's committed
    /// sequence number, falling back to `TRIM_HORIZON` for shards
    /// that have never been committed.
    async fn build_shard_iterator(
        &self,
        client: &Client,
        shard_id: &str,
        committed: Option<&str>,
    ) -> Result<Option<String>, BackendError> {
        let mut req = client
            .get_shard_iterator()
            .stream_name(&self.stream_name)
            .shard_id(shard_id);
        req = match committed {
            Some(seq) => req
                .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                .starting_sequence_number(seq.to_string()),
            None => req.shard_iterator_type(ShardIteratorType::TrimHorizon),
        };
        let resp = req
            .send()
            .await
            .map_err(|e| BackendError::Connection(format!("kinesis get_shard_iterator: {e}")))?;
        Ok(resp.shard_iterator)
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

    fn config(&self) -> crate::backend::BackendConfig {
        // Σ.B PR 1 commit d: constructor-args only (region /
        // endpoint / static_credentials / batch_config land in
        // Σ.B PR 2). Panic if any builder-set state is non-default.
        if self.region.is_some() || self.endpoint.is_some() || self.static_credentials.is_some() {
            panic!(
                "KinesisBackend::config() called on an instance with non-default builder \
                 state (region / endpoint / static credentials). Full round-trip ships \
                 in Σ.B PR 2 — see docs/PHASE_SIGMA_B_TRAIT_SPIKE.md."
            );
        }
        crate::backend::BackendConfig::Kinesis(crate::backend::KinesisConfig {
            stream_name: self.stream_name.clone(),
        })
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
        let limit = cfg.batch_size.min(10_000) as i32;

        // Drain each shard in turn (sequential fanout). Per shard:
        // bootstrap iterator if needed, then loop GetRecords until
        // we hit limits or max_empty_polls.
        let shard_ids: Vec<String> = session.cursors.keys().cloned().collect();
        for shard_id in shard_ids {
            // Bootstrap iterator from committed checkpoint if the
            // cursor doesn't already have one.
            if session
                .cursors
                .get(&shard_id)
                .and_then(|c| c.next_iterator.as_ref())
                .is_none()
            {
                let committed = session
                    .cursors
                    .get(&shard_id)
                    .and_then(|c| c.committed_sequence_number.clone());
                let iter = self
                    .build_shard_iterator(&client, &shard_id, committed.as_deref())
                    .await?;
                if let Some(c) = session.cursors.get_mut(&shard_id) {
                    c.next_iterator = iter;
                }
            }

            let mut empty_polls = 0_u32;
            while let Some(iterator) = session
                .cursors
                .get(&shard_id)
                .and_then(|c| c.next_iterator.clone())
            {
                let resp = client
                    .get_records()
                    .shard_iterator(iterator)
                    .limit(limit)
                    .send()
                    .await
                    .map_err(|e| BackendError::Query(format!("kinesis get_records: {e}")))?;
                // Always advance the in-memory iterator (AWS docs
                // explicitly recommend this even on empty responses).
                if let Some(c) = session.cursors.get_mut(&shard_id) {
                    c.next_iterator = resp.next_shard_iterator;
                }

                let records = resp.records;
                if records.is_empty() {
                    empty_polls += 1;
                    if empty_polls > cfg.max_empty_polls {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(cfg.idle_poll_ms)).await;
                    continue;
                }

                let mut last_seq: Option<String> = None;
                for rec in records {
                    last_seq = Some(rec.sequence_number().to_string());
                    let data = rec.data.into_inner();
                    bytes_total += data.len();
                    payloads.push(data);
                    if payloads.len() >= cfg.batch_size || bytes_total >= cfg.batch_bytes {
                        break;
                    }
                }
                if let (Some(c), Some(seq)) = (session.cursors.get_mut(&shard_id), last_seq) {
                    c.pending_sequence_number = Some(seq);
                }

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

    /// Advance each shard's `committed_sequence_number` to the
    /// pending value. Mirrors Kafka 36e / RabbitMQ 37a.3 / Pub/Sub
    /// 37b.3 — the framework's at-least-once primitive. No-op if
    /// no consumer session has been opened or no shard has any
    /// pending sequence number.
    ///
    /// Note: the committed checkpoint is in-memory only. Durable
    /// checkpoint storage (DynamoDB / file) is a documented
    /// follow-up; the streaming-pipeline contract still holds
    /// in-process because the pipeline calls `commit_offsets`
    /// after every successful target write.
    async fn commit_offsets(&self) -> Result<(), BackendError> {
        let mut session_lock = self.consumer_session.lock().await;
        let Some(session) = session_lock.as_mut() else {
            return Ok(());
        };
        for cursor in session.cursors.values_mut() {
            if let Some(seq) = cursor.pending_sequence_number.take() {
                cursor.committed_sequence_number = Some(seq);
            }
        }
        Ok(())
    }

    /// Phase 39.5a P1.7b: Kinesis exposes seek-to via per-shard
    /// sequence numbers. The next `read_arrow_stream` call rebuilds
    /// shard iterators using `ShardIteratorType=AFTER_SEQUENCE_NUMBER`
    /// + the recovered checkpoint, resuming consumption from that
    /// point. Stateful (session/join) pipelines now accept Kinesis
    /// as a source.
    fn supports_seek_to(&self) -> bool {
        true
    }

    /// Decode the wire format `{"v": 1, "shards": {"shard_id":
    /// "seq_num"}}` and stash the per-shard committed checkpoints
    /// in the consumer session. The session may not yet exist (the
    /// pipeline calls `seek_to` before the first `read_arrow_stream`
    /// in the recovery path); create it lazily here so the cursor
    /// state survives for the next consumer-open call.
    async fn seek_to(&self, offset_bytes: &[u8]) -> Result<(), BackendError> {
        let parsed = decode_kinesis_offsets(offset_bytes)?;
        let mut session_lock = self.consumer_session.lock().await;
        let session = session_lock.get_or_insert_with(|| KinesisConsumerSession {
            cursors: BTreeMap::new(),
        });
        for (shard_id, seq) in parsed {
            // For each recovered shard: install the committed
            // sequence + clear next_iterator + pending. The
            // consumer-open path below sees committed_sequence_number
            // is Some and bootstraps via AFTER_SEQUENCE_NUMBER.
            let cursor = session.cursors.entry(shard_id).or_default();
            cursor.committed_sequence_number = Some(seq);
            cursor.next_iterator = None;
            cursor.pending_sequence_number = None;
        }
        Ok(())
    }

    /// Capture per-shard committed sequence numbers as opaque
    /// bytes for `StateStore.commit`. Returns `None` when no
    /// consumer session is active or no cursor has been
    /// committed yet — caller skips this source from the snapshot.
    async fn offset_snapshot(&self) -> Result<Option<Vec<u8>>, BackendError> {
        let session_lock = self.consumer_session.lock().await;
        let Some(session) = session_lock.as_ref() else {
            return Ok(None);
        };
        let mut shards: BTreeMap<String, String> = BTreeMap::new();
        for (shard_id, cursor) in &session.cursors {
            // Prefer pending (most recent) but fall back to
            // committed so a snapshot taken between commits still
            // captures the in-flight position. This matches the
            // Kafka backend's pending_offsets-as-source-of-truth.
            if let Some(seq) = cursor
                .pending_sequence_number
                .as_ref()
                .or(cursor.committed_sequence_number.as_ref())
            {
                shards.insert(shard_id.clone(), seq.clone());
            }
        }
        if shards.is_empty() {
            return Ok(None);
        }
        Ok(Some(encode_kinesis_offsets(&shards)?))
    }
}

// =====================================================================
// Phase 39.5a P1.7b: opaque per-shard sequence-number wire format.
// =====================================================================

#[derive(serde::Serialize, serde::Deserialize)]
struct KinesisOffsetSnapshotV1 {
    v: u32,
    shards: BTreeMap<String, String>,
}

pub(crate) fn encode_kinesis_offsets(
    shards: &BTreeMap<String, String>,
) -> Result<Vec<u8>, BackendError> {
    let snap = KinesisOffsetSnapshotV1 {
        v: 1,
        shards: shards.clone(),
    };
    serde_json::to_vec(&snap)
        .map_err(|e| BackendError::Other(format!("kinesis offset encode: {e}")))
}

pub(crate) fn decode_kinesis_offsets(
    bytes: &[u8],
) -> Result<BTreeMap<String, String>, BackendError> {
    let snap: KinesisOffsetSnapshotV1 = serde_json::from_slice(bytes)
        .map_err(|e| BackendError::Other(format!("kinesis offset decode: {e}")))?;
    if snap.v != 1 {
        return Err(BackendError::Other(format!(
            "kinesis offset payload v={} not supported (this build understands v=1)",
            snap.v
        )));
    }
    Ok(snap.shards)
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

    /// 37c.3: commit_offsets is a no-op before any consumer
    /// session has been opened. Lets pipelines call it
    /// unconditionally.
    #[tokio::test(flavor = "multi_thread")]
    async fn commit_offsets_noop_without_consumer_session() {
        let b = KinesisBackend::open("s").unwrap();
        b.commit_offsets().await.unwrap();
        assert_eq!(b.pending_sequence_count().await, 0);
    }

    /// 37c.3: reset_to_committed_offsets is a no-op before any
    /// session has been opened.
    #[tokio::test(flavor = "multi_thread")]
    async fn reset_to_committed_offsets_noop_without_consumer_session() {
        let b = KinesisBackend::open("s").unwrap();
        b.reset_to_committed_offsets().await.unwrap();
        assert_eq!(b.pending_sequence_count().await, 0);
    }

    // ----- P1.7b Kinesis seek_to + offset_snapshot -----

    #[test]
    fn kinesis_offsets_roundtrip_through_codec() {
        let mut shards: BTreeMap<String, String> = BTreeMap::new();
        shards.insert("shardId-000000000000".into(), "49600000000000000000".into());
        shards.insert("shardId-000000000001".into(), "49600000000000000001".into());
        let bytes = encode_kinesis_offsets(&shards).unwrap();
        let back = decode_kinesis_offsets(&bytes).unwrap();
        assert_eq!(back, shards);
    }

    #[test]
    fn kinesis_decode_rejects_unknown_payload_version() {
        let payload = br#"{"v":99,"shards":{}}"#;
        let err = decode_kinesis_offsets(payload).unwrap_err();
        assert!(err.to_string().contains("v=99"), "got: {err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kinesis_supports_seek_to_reports_true() {
        let b = KinesisBackend::open("s").unwrap();
        assert!(b.supports_seek_to());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kinesis_offset_snapshot_returns_none_before_session() {
        let b = KinesisBackend::open("s").unwrap();
        assert!(b.offset_snapshot().await.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kinesis_seek_to_stashes_committed_sequence_per_shard() {
        let b = KinesisBackend::open("s").unwrap();
        let mut shards: BTreeMap<String, String> = BTreeMap::new();
        shards.insert("shardId-000000000000".into(), "49600000000000000000".into());
        shards.insert("shardId-000000000001".into(), "49600000000000000042".into());
        let payload = encode_kinesis_offsets(&shards).unwrap();
        b.seek_to(&payload).await.unwrap();

        // Inspect the stashed cursor map directly — same module so
        // private fields are reachable.
        let session = b.consumer_session.lock().await;
        let session = session.as_ref().expect("seek_to must create the session");
        assert_eq!(session.cursors.len(), 2);
        let c0 = session.cursors.get("shardId-000000000000").unwrap();
        assert_eq!(
            c0.committed_sequence_number.as_deref(),
            Some("49600000000000000000")
        );
        // next_iterator cleared so next read rebuilds via
        // AFTER_SEQUENCE_NUMBER.
        assert!(c0.next_iterator.is_none());
        assert!(c0.pending_sequence_number.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kinesis_offset_snapshot_after_seek_roundtrips() {
        // Round-trip: seek with payload P → snapshot returns
        // bytes that decode to the same shard map.
        let b = KinesisBackend::open("s").unwrap();
        let mut shards: BTreeMap<String, String> = BTreeMap::new();
        shards.insert("shard-0".into(), "seq-100".into());
        let payload = encode_kinesis_offsets(&shards).unwrap();
        b.seek_to(&payload).await.unwrap();
        let snap = b.offset_snapshot().await.unwrap().unwrap();
        let back = decode_kinesis_offsets(&snap).unwrap();
        assert_eq!(back, shards);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kinesis_seek_to_with_garbage_bytes_returns_error() {
        let b = KinesisBackend::open("s").unwrap();
        let err = b.seek_to(b"not-json").await.unwrap_err();
        assert!(err.to_string().contains("decode"));
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
