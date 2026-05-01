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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::admin::AdminClient;
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::{
    CommitMode, Consumer, ConsumerContext, ConsumerGroupMetadata, StreamConsumer,
};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    StreamingKind, TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

/// Per-batch limits for `read_arrow_stream`. The drain loop closes
/// the current batch and returns as soon as **any** of these
/// triggers fires:
///   - `batch_size` messages received,
///   - `batch_bytes` of payload accumulated (sum of message lengths),
///   - `batch_window_ms` elapsed since the first message in the
///     batch arrived (latency cap — useful for low-volume topics),
///   - `idle_timeout_ms` elapsed without a new message (drain done).
///
/// `idle_timeout_ms` is the only trigger that can fire on an empty
/// batch — that's how a `read_arrow_stream` against a quiet topic
/// returns an empty stream rather than blocking forever.
///
/// Defaults are tuned for one-shot `read_arrow_stream` calls (rather
/// than the long-running consumer in 36g): generous size and bytes
/// caps, a 5s window, a 5s idle timeout. The first-message wait is
/// still bumped to 15s internally so a fresh broker has time to
/// rebalance — that's a separate clock from these batch limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KafkaBatchConfig {
    pub batch_size: usize,
    pub batch_bytes: usize,
    pub batch_window_ms: u64,
    pub idle_timeout_ms: u64,
}

impl Default for KafkaBatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 100_000,
            batch_bytes: 16 * 1024 * 1024, // 16 MiB
            batch_window_ms: 5_000,
            idle_timeout_ms: 5_000,
        }
    }
}

/// Wire format for Kafka message payloads. Selected via
/// `KafkaBackend::with_payload_format`. JSON and RawBytes are
/// fully implemented in 36h; Avro and Protobuf are reserved
/// surface — every encode/decode path returns a clear
/// "lands in Phase 36h.X" error until those sub-phases land
/// (36h.3 Avro decode, 36h.4 Avro encode, 36h.5 Protobuf decode,
/// 36h.6 Protobuf encode).
///
/// Each direction is its own sub-phase because the encode and
/// decode pipelines are largely independent. Decode strips the
/// 5-byte Confluent magic-byte prefix, fetches the schema from
/// Schema Registry by ID, decodes into the target's Arrow
/// dialect; encode does the symmetric reverse + schema
/// registration on first produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaPayloadFormat {
    /// One JSON object per message. Decodes to N rows with the
    /// inferred schema; encodes each Arrow row as a JSON object.
    /// This is the default — matches what most pipeline builders
    /// reach for first.
    #[default]
    Json,
    /// Each message body is a single opaque blob. On read, every
    /// message becomes one row with a single `payload` column of
    /// type Binary. On write, the source must produce exactly one
    /// Binary-typed column; each row's value is sent as one
    /// message payload (column name doesn't matter — only its
    /// type).
    RawBytes,
    /// Confluent-Schema-Registry-framed Avro. Surface reserved in
    /// 36h.2; the decode path lands in 36h.3 and the encode path
    /// in 36h.4.
    Avro,
    /// Confluent-Schema-Registry-framed Protobuf. Decode lands in
    /// 36h.5, encode in 36h.6.
    Protobuf,
}

/// Column name used by the RawBytes decoder for the single Binary
/// column it emits.
const RAW_BYTES_COLUMN: &str = "payload";

/// Producer-side delivery semantics for `write_arrow_stream`.
///
///   - `AtLeastOnce` (default): the existing per-row awaited produce
///     path. A partial-batch failure surfaces with rows already on
///     the topic; downstream consumers may see them.
///   - `ExactlyOnce`: each `write_arrow_stream` batch is wrapped in
///     a Kafka transaction (`begin_transaction` →
///     produce all rows → `commit_transaction`, or
///     `abort_transaction` on failure). Requires a unique
///     `transactional_id`. Adds an extra round-trip per batch but
///     gives all-or-nothing produce semantics.
///
/// Consumer-coordinated EOS via `send_offsets_to_transaction` (the
/// full Kafka→Kafka read-process-write exactly-once flow) is a
/// follow-up — see Phase 36j.2.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum KafkaDeliverySemantics {
    #[default]
    AtLeastOnce,
    ExactlyOnce {
        /// Unique-per-process transactional id. The broker fences
        /// any prior producer using the same id, so reusing this
        /// across multiple processes within the same time window
        /// produces an `OperationNotPermitted` error from the
        /// later-starting one.
        transactional_id: String,
    },
}

/// Cached producer + transactional-init state. Producers used in
/// transactional mode must be reused across `write_arrow_stream`
/// calls because:
///   1. `init_transactions` is once-per-producer-lifetime, not
///      once-per-transaction.
///   2. Two producers with the same `transactional.id` are mutually
///      exclusive at the broker level (the broker fences the older
///      one), so a fresh handle per call would self-fence on the
///      second call.
///
/// At-least-once mode also benefits from caching (avoids the cost
/// of building a librdkafka client every batch) but functionally
/// works either way.
#[derive(Default)]
struct ProducerSession {
    producer: Option<Arc<FutureProducer<EmatixKafkaContext>>>,
    /// Tracks whether init_transactions has been called on the
    /// cached producer. Set true on first transactional write.
    transactions_initialized: bool,
}

impl std::fmt::Debug for ProducerSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerSession")
            .field("has_producer", &self.producer.is_some())
            .field("transactions_initialized", &self.transactions_initialized)
            .finish()
    }
}

/// SCRAM mechanisms supported by librdkafka. Most cloud providers
/// run SHA-512 by default; some self-hosted deployments still use
/// SHA-256.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

impl ScramMechanism {
    fn as_kafka_str(&self) -> &'static str {
        match self {
            ScramMechanism::Sha256 => "SCRAM-SHA-256",
            ScramMechanism::Sha512 => "SCRAM-SHA-512",
        }
    }
}

/// mTLS / cert-based auth config. All three paths are required:
/// `ca_location` points at the broker's trust root (usually a CA
/// bundle); `cert_location` and `key_location` are the client cert
/// and its private key. `key_password` is for password-protected
/// keys.
#[derive(Debug, Clone)]
pub struct TlsAuth {
    pub ca_location: String,
    pub cert_location: String,
    pub key_location: String,
    pub key_password: Option<String>,
}

/// Auth-provider state on `KafkaBackend`. Each variant maps to a
/// well-known librdkafka security setup (`security.protocol` +
/// `sasl.mechanism` + the corresponding credential keys). The
/// builder methods on `KafkaBackend` populate the right variant;
/// `client_config()` reads it and applies keys.
///
/// The framework treats Confluent Cloud, self-hosted SASL, AWS MSK,
/// and mTLS-enabled deployments as sibling first-class auth modes —
/// none privileged in the API.
#[derive(Debug, Clone, Default)]
enum AuthMode {
    /// No auth. SASL_PLAINTEXT or PLAINTEXT depending on whether
    /// the broker speaks TLS — but for `None` we leave
    /// `security.protocol` unset so librdkafka picks PLAINTEXT.
    #[default]
    None,
    /// SASL/PLAIN over SSL. Confluent Cloud's primary auth mode.
    SaslPlain { username: String, password: String },
    /// SASL/SCRAM over SSL. Common for self-hosted Kafka with
    /// SCRAM-SHA-256/512 user accounts.
    SaslScram {
        mechanism: ScramMechanism,
        username: String,
        password: String,
    },
    /// mTLS — broker authenticates the client by certificate.
    /// `security.protocol = SSL`.
    Tls(TlsAuth),
    /// AWS MSK IAM. Sets `security.protocol = SASL_SSL`,
    /// `sasl.mechanism = OAUTHBEARER`, and (in the follow-up that
    /// wires the custom `ClientContext`) registers a
    /// `generate_oauth_token` callback that mints MSK IAM tokens
    /// via SigV4 and refreshes them at ~80% of TTL.
    MskIam { region: String },
}

/// Custom librdkafka client context that overrides
/// `generate_oauth_token` to mint AWS MSK IAM tokens via SigV4 (the
/// signing happens inside `aws-msk-iam-sasl-signer`). librdkafka
/// calls this proactively at ~80% of the previous token's TTL —
/// we don't have to schedule refresh manually; setting
/// `ENABLE_REFRESH_OAUTH_TOKEN = true` is the whole opt-in.
///
/// For non-MSK auth modes (PLAIN / SCRAM / TLS / no-auth), the
/// callback is wired but never fires because librdkafka only
/// invokes it when `sasl.mechanism = OAUTHBEARER`.
///
/// We wrap an optional MSK region + an optional tokio runtime
/// handle. The handle is captured at `with_msk_iam` time
/// (assumed-async context); the callback uses
/// `runtime.block_on(...)` to bridge sync librdkafka into the
/// async signer.
#[derive(Clone, Default)]
pub(crate) struct EmatixKafkaContext {
    msk_region: Option<String>,
    runtime: Option<tokio::runtime::Handle>,
}

impl ClientContext for EmatixKafkaContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        let region_str = self.msk_region.as_ref().ok_or_else(|| -> Box<dyn std::error::Error> {
            "OAUTHBEARER token requested but no MSK region configured on KafkaBackend (call `with_msk_iam`)".into()
        })?;
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| -> Box<dyn std::error::Error> {
                "MSK IAM token: no tokio runtime handle was captured. \
             Call `KafkaBackend::with_msk_iam(...)` from within an async context \
             (e.g. inside #[tokio::main] or a runtime block_on)."
                    .into()
            })?;
        let region = aws_types::region::Region::new(region_str.clone());
        // librdkafka calls this from its background poll thread, not
        // a tokio runtime worker. `block_on` on the captured handle
        // bridges that into our async signer call.
        let principal = format!("kafka/{region_str}");
        let result =
            runtime.block_on(async { aws_msk_iam_sasl_signer::generate_auth_token(region).await });
        let (token, lifetime_ms) = result.map_err(|e| -> Box<dyn std::error::Error> {
            format!("MSK IAM token generation failed: {e}").into()
        })?;
        Ok(OAuthToken {
            token,
            principal_name: principal,
            lifetime_ms,
        })
    }
}

impl ConsumerContext for EmatixKafkaContext {}

/// Lazy-initialized consumer session that persists across
/// `read_arrow_stream` calls so that:
///   - the same group rebalance / offset state is reused (avoids
///     re-paying the rebalance cost every read),
///   - offsets accumulated during read can be committed *after* the
///     downstream backend has durably written, via
///     `KafkaBackend::commit_offsets`.
///
/// Only one topic is supported per backend handle; subscribing to a
/// different topic drops and recreates the consumer (and discards
/// any pending offsets — uncommitted reads on the old topic will be
/// re-delivered).
#[derive(Default)]
struct ConsumerSession {
    consumer: Option<Arc<StreamConsumer<EmatixKafkaContext>>>,
    subscribed_topic: Option<String>,
    /// `partition → offset_to_commit`. The commit position is the
    /// offset of the *next* message to consume (i.e. last consumed
    /// offset + 1) — that's what the Kafka commit protocol wants.
    pending_offsets: HashMap<i32, i64>,
}

/// Kafka-backed implementation of `Backend`.
///
/// Holds a librdkafka `ClientConfig` rather than a long-lived
/// connection — librdkafka is lazy: producers, consumers, and admin
/// clients are constructed on demand from the same config. This
/// matches how every other Backend in the framework holds a "config"
/// and constructs a fresh handle per operation.
///
/// Consumer-side state (the StreamConsumer + pending offsets) is held
/// behind a `Mutex` so multiple `read_arrow_stream` + `commit_offsets`
/// calls can share the same consumer session (Phase 36e).
#[derive(Debug)]
pub struct KafkaBackend {
    /// Comma-separated `host:port` list. Cloned into every per-op
    /// client config we build.
    bootstrap_servers: String,
    /// Consumer group_id. `None` means this backend is producer-only.
    /// Populated for `read_arrow_stream` callers in 36b.
    group_id: Option<String>,
    /// Batch limits applied by `read_arrow_stream`. Builder-set via
    /// `with_batch_config`; defaults match `KafkaBatchConfig::default`.
    batch_config: KafkaBatchConfig,
    /// Lazy consumer session — populated on first `read_arrow_stream`,
    /// reused on subsequent calls within the same backend instance,
    /// committed by `commit_offsets`.
    consumer_session: Arc<Mutex<ConsumerSession>>,
    /// Auth provider — `AuthMode::None` for unauthenticated clusters;
    /// populated by the `with_sasl_plain` / `with_sasl_scram` /
    /// `with_tls` / `with_msk_iam` builder methods.
    auth: AuthMode,
    /// Payload wire format applied by `read_arrow_stream` /
    /// `write_arrow_stream`. Builder-set via `with_payload_format`;
    /// defaults to JSON.
    payload_format: KafkaPayloadFormat,
    /// Producer-side delivery semantics. Builder-set via
    /// `with_delivery_semantics`; defaults to `AtLeastOnce`.
    delivery_semantics: KafkaDeliverySemantics,
    /// Cached transactional producer state. Required for
    /// `ExactlyOnce` because init_transactions is once-per-lifetime
    /// and same-`transactional.id` producers are mutually exclusive
    /// at the broker.
    producer_session: Arc<Mutex<ProducerSession>>,
}

impl std::fmt::Debug for ConsumerSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `StreamConsumer` doesn't implement Debug; redact it.
        f.debug_struct("ConsumerSession")
            .field("subscribed_topic", &self.subscribed_topic)
            .field("pending_offsets_count", &self.pending_offsets.len())
            .finish()
    }
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
            batch_config: KafkaBatchConfig::default(),
            consumer_session: Arc::new(Mutex::new(ConsumerSession::default())),
            auth: AuthMode::None,
            payload_format: KafkaPayloadFormat::default(),
            delivery_semantics: KafkaDeliverySemantics::default(),
            producer_session: Arc::new(Mutex::new(ProducerSession::default())),
        })
    }

    /// Override producer-side delivery semantics. Defaults to
    /// `AtLeastOnce`. `ExactlyOnce { transactional_id }` wraps each
    /// `write_arrow_stream` batch in a Kafka transaction.
    pub fn with_delivery_semantics(mut self, semantics: KafkaDeliverySemantics) -> Self {
        self.delivery_semantics = semantics;
        self
    }

    /// Borrow the active delivery semantics (tests + introspection).
    pub fn delivery_semantics(&self) -> &KafkaDeliverySemantics {
        &self.delivery_semantics
    }

    /// Override the wire format for message payloads. JSON is the
    /// default; `RawBytes` is the other 36h-supported variant. Avro
    /// and Protobuf are placeholders today and rejected at runtime
    /// with a pointer to 36h.2.
    pub fn with_payload_format(mut self, format: KafkaPayloadFormat) -> Self {
        self.payload_format = format;
        self
    }

    /// Borrow the active payload format (tests + introspection).
    pub fn payload_format(&self) -> KafkaPayloadFormat {
        self.payload_format
    }

    /// SASL/PLAIN over TLS — Confluent Cloud's primary auth mode and
    /// a common self-hosted setup. Sets:
    ///   - `security.protocol = SASL_SSL`
    ///   - `sasl.mechanism = PLAIN`
    ///   - `sasl.username` / `sasl.password`
    pub fn with_sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = AuthMode::SaslPlain {
            username: username.into(),
            password: password.into(),
        };
        self
    }

    /// SASL/SCRAM-SHA-{256,512} over TLS — common for self-hosted
    /// Kafka with SCRAM user accounts. Sets:
    ///   - `security.protocol = SASL_SSL`
    ///   - `sasl.mechanism = SCRAM-SHA-256` or `SCRAM-SHA-512`
    ///   - `sasl.username` / `sasl.password`
    pub fn with_sasl_scram(
        mut self,
        mechanism: ScramMechanism,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = AuthMode::SaslScram {
            mechanism,
            username: username.into(),
            password: password.into(),
        };
        self
    }

    /// mTLS / cert-based authentication. Sets:
    ///   - `security.protocol = SSL`
    ///   - `ssl.ca.location`
    ///   - `ssl.certificate.location` / `ssl.key.location`
    ///   - `ssl.key.password` (when set)
    pub fn with_tls(mut self, tls: TlsAuth) -> Self {
        self.auth = AuthMode::Tls(tls);
        self
    }

    /// AWS MSK IAM. Sets:
    ///   - `security.protocol = SASL_SSL`
    ///   - `sasl.mechanism = OAUTHBEARER`
    ///
    /// Producer / consumer creation in this builder doesn't yet
    /// register the `generate_oauth_token` callback that mints MSK
    /// IAM tokens — that needs a custom `ClientContext` (which
    /// requires parameterizing every `StreamConsumer` /
    /// `FutureProducer` / `AdminClient` construction site through a
    /// new `Self::ClientCtx` associated type). Tracked as the 36f
    /// follow-up; the dep on `aws-msk-iam-sasl-signer` is in place
    /// so the token-generation side is ready.
    ///
    /// In the meantime, calling `ping` / `read_arrow_stream` /
    /// produce on an MSK-IAM-configured backend will fail at the
    /// librdkafka layer with a missing-token error — that's the
    /// honest error to surface until the callback wiring lands.
    pub fn with_msk_iam(mut self, region: impl Into<String>) -> Self {
        self.auth = AuthMode::MskIam {
            region: region.into(),
        };
        self
    }

    /// Commit any offsets accumulated by prior `read_arrow_stream`
    /// calls on this backend. The commit fires synchronously against
    /// the broker (`CommitMode::Sync`); on success, the committed
    /// offsets are cleared from the in-memory pending set.
    ///
    /// This is the at-least-once primitive: the framework's
    /// pipeline executor (a future phase) calls it **after** the
    /// target backend has durably written the rows. A crash between
    /// read and commit means the same messages get re-delivered on
    /// the next consumer session — that's the at-least-once
    /// guarantee. `enable.auto.commit=false` is set in
    /// `client_config` so librdkafka never commits behind our back.
    ///
    /// No-ops if no consumer has been opened yet, or if no offsets
    /// are pending — safe to call after a zero-message read.
    pub async fn commit_offsets(&self) -> Result<(), BackendError> {
        let snapshot = {
            let session = self
                .consumer_session
                .lock()
                .map_err(|e| BackendError::Other(format!("kafka consumer lock: {e}")))?;
            match (
                session.consumer.as_ref(),
                session.subscribed_topic.as_deref(),
                session.pending_offsets.is_empty(),
            ) {
                (Some(consumer), Some(topic), false) => Some((
                    Arc::clone(consumer),
                    topic.to_string(),
                    session.pending_offsets.clone(),
                )),
                _ => None,
            }
        };
        let Some((consumer, topic, offsets)) = snapshot else {
            return Ok(());
        };
        // Build the TopicPartitionList we want to commit. The Kafka
        // commit protocol expects "offset of the next message to
        // consume" per partition — we already store offsets in that
        // form (last consumed + 1), so it's a direct copy.
        let mut tpl = TopicPartitionList::new();
        for (partition, offset) in &offsets {
            tpl.add_partition_offset(&topic, *partition, Offset::Offset(*offset))
                .map_err(|e| {
                    BackendError::Query(format!("kafka commit tpl partition={partition}: {e}"))
                })?;
        }
        // Commit is synchronous (FFI) — wrap in spawn_blocking.
        tokio::task::spawn_blocking(move || {
            consumer
                .commit(&tpl, CommitMode::Sync)
                .map_err(|e| BackendError::Query(format!("kafka commit: {e}")))
        })
        .await
        .map_err(|e| BackendError::Other(format!("kafka commit join: {e}")))??;

        // Clear pending offsets — only after the broker ack lands.
        let mut session = self
            .consumer_session
            .lock()
            .map_err(|e| BackendError::Other(format!("kafka consumer lock: {e}")))?;
        session.pending_offsets.clear();
        Ok(())
    }

    /// Number of pending (uncommitted) offsets across all assigned
    /// partitions. Tests + introspection.
    pub fn pending_offset_count(&self) -> usize {
        self.consumer_session
            .lock()
            .map(|s| s.pending_offsets.len())
            .unwrap_or(0)
    }

    /// Snapshot pending offsets as a `TopicPartitionList` suitable
    /// for handing to a producer's `send_offsets_to_transaction`.
    /// Returns `None` if no consumer session is active or no
    /// offsets are pending. Used by `KafkaToKafkaEosPipeline` (Phase
    /// 36j.2) to bundle consumer-side offset advancement into the
    /// producer's transaction.
    pub fn pending_offsets_topic_partition_list(&self) -> Option<TopicPartitionList> {
        let session = self.consumer_session.lock().ok()?;
        let topic = session.subscribed_topic.as_deref()?;
        if session.pending_offsets.is_empty() {
            return None;
        }
        let mut tpl = TopicPartitionList::new();
        for (partition, offset) in &session.pending_offsets {
            tpl.add_partition_offset(topic, *partition, Offset::Offset(*offset))
                .ok()?;
        }
        Some(tpl)
    }

    /// Snapshot the source consumer's group metadata. Required by
    /// `send_offsets_to_transaction` to attribute the offset commit
    /// to the right consumer group. Returns `None` if no consumer
    /// session is active.
    pub fn consumer_group_metadata(&self) -> Option<ConsumerGroupMetadata> {
        let session = self.consumer_session.lock().ok()?;
        session.consumer.as_ref()?.group_metadata()
    }

    /// Drop pending consumer offsets *without* committing to the
    /// broker. Used by `KafkaToKafkaEosPipeline` after the producer's
    /// `commit_transaction` has atomically advanced offsets via
    /// `send_offsets_to_transaction` — the in-memory pending set is
    /// now stale.
    pub fn clear_pending_offsets(&self) -> Result<(), BackendError> {
        let mut session = self
            .consumer_session
            .lock()
            .map_err(|e| BackendError::Other(format!("kafka consumer lock: {e}")))?;
        session.pending_offsets.clear();
        Ok(())
    }

    /// Coordinated transactional produce: bundles the source
    /// consumer's pending offsets into the producer transaction so
    /// the entire read-process-write cycle becomes atomic. Phase
    /// 36j.2 — full Kafka→Kafka exactly-once.
    ///
    /// Sequence per call:
    ///   1. begin_transaction
    ///   2. produce all rows from `stream` to `target.name`
    ///   3. send_offsets_to_transaction(source.pending_offsets,
    ///      source.consumer_group_metadata) — attaches the source
    ///      consumer's offset commit to the in-flight transaction
    ///   4. commit_transaction (or abort_transaction on any earlier
    ///      failure)
    ///
    /// After commit, the source's offsets have been advanced as
    /// part of the transaction; the caller must
    /// `source.clear_pending_offsets()` to drop the in-memory
    /// pending set.
    ///
    /// Requires `self.delivery_semantics` to be `ExactlyOnce` —
    /// errors out otherwise. The source must have a consumer
    /// session with pending offsets (i.e. `read_arrow_stream` has
    /// been called at least once on it).
    pub async fn write_arrow_stream_eos(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        source: &KafkaBackend,
    ) -> Result<u64, BackendError> {
        if !matches!(
            self.delivery_semantics,
            KafkaDeliverySemantics::ExactlyOnce { .. }
        ) {
            return Err(BackendError::Other(
                "Kafka write_arrow_stream_eos: target must be configured with \
                 KafkaDeliverySemantics::ExactlyOnce { transactional_id: ... }"
                    .into(),
            ));
        }
        let topic = target.name.trim();
        if topic.is_empty() {
            return Err(BackendError::Other(
                "Kafka write_arrow_stream_eos: target.name (topic) must be non-empty".into(),
            ));
        }
        match self.payload_format {
            KafkaPayloadFormat::Json | KafkaPayloadFormat::RawBytes => {}
            KafkaPayloadFormat::Avro => {
                return Err(BackendError::Other(
                    "Kafka write_arrow_stream_eos Avro: lands in Phase 36h.4".into(),
                ));
            }
            KafkaPayloadFormat::Protobuf => {
                return Err(BackendError::Other(
                    "Kafka write_arrow_stream_eos Protobuf: lands in Phase 36h.6".into(),
                ));
            }
        }
        let producer = self.acquire_producer().await?;
        // Snapshot source offsets / metadata BEFORE the txn begins
        // so we don't capture offsets that the source consumer
        // advances mid-flight (it shouldn't here, but defensive
        // against future async-consumer churn).
        let source_offsets = source.pending_offsets_topic_partition_list();
        let source_cgm = source.consumer_group_metadata();

        // Buffer the stream so the txn-bracketed produce can run
        // synchronously through `produce_payloads_to_topic`.
        let mut s = stream;
        let mut all_payloads: Vec<Vec<u8>> = Vec::new();
        while let Some(batch) = futures_util::StreamExt::next(&mut s).await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let mut payloads = match self.payload_format {
                KafkaPayloadFormat::Json => encode_batch_as_jsonl_lines(&batch)?,
                KafkaPayloadFormat::RawBytes => encode_batch_as_raw_bytes(&batch)?,
                KafkaPayloadFormat::Avro | KafkaPayloadFormat::Protobuf => {
                    unreachable!("rejected at entry")
                }
            };
            all_payloads.append(&mut payloads);
        }
        if all_payloads.is_empty() {
            return Ok(0);
        }

        // Begin transaction.
        let p = Arc::clone(&producer);
        tokio::task::spawn_blocking(move || p.begin_transaction())
            .await
            .map_err(|e| BackendError::Other(format!("kafka begin_transaction join: {e}")))?
            .map_err(|e| BackendError::Query(format!("kafka begin_transaction: {e}")))?;

        // Produce-then-coordinate-then-commit. On any failure,
        // abort_transaction (best effort) and surface the error.
        let result = async {
            let total = produce_payloads_to_topic(&producer, topic, &all_payloads).await?;
            // send_offsets_to_transaction only fires if the source
            // has both offsets and group metadata. A source without
            // a consumer session (e.g. a producer-only KafkaBackend
            // accidentally passed in) gets no offset coordination —
            // arguably an error, but easier to surface as "produced
            // OK but didn't advance source offsets" which the
            // caller can detect via source.pending_offset_count.
            if let (Some(offsets), Some(cgm)) = (source_offsets, source_cgm) {
                let p = Arc::clone(&producer);
                tokio::task::spawn_blocking(move || {
                    p.send_offsets_to_transaction(
                        &offsets,
                        &cgm,
                        Timeout::After(Duration::from_secs(30)),
                    )
                })
                .await
                .map_err(|e| {
                    BackendError::Other(format!("kafka send_offsets_to_transaction join: {e}"))
                })?
                .map_err(|e| {
                    BackendError::Query(format!("kafka send_offsets_to_transaction: {e}"))
                })?;
            }
            Ok::<u64, BackendError>(total)
        }
        .await;

        match result {
            Ok(total) => {
                let p = Arc::clone(&producer);
                tokio::task::spawn_blocking(move || {
                    p.commit_transaction(Timeout::After(Duration::from_secs(30)))
                })
                .await
                .map_err(|e| BackendError::Other(format!("kafka commit_transaction join: {e}")))?
                .map_err(|e| BackendError::Query(format!("kafka commit_transaction: {e}")))?;
                Ok(total)
            }
            Err(e) => {
                let p = Arc::clone(&producer);
                let _ = tokio::task::spawn_blocking(move || {
                    p.abort_transaction(Timeout::After(Duration::from_secs(30)))
                })
                .await;
                Err(e)
            }
        }
    }

    /// Get (or lazily-create) the FutureProducer for this backend.
    /// In `ExactlyOnce` mode the producer must be reused across
    /// `write_arrow_stream` calls because (a) `init_transactions`
    /// is once-per-producer-lifetime, and (b) two producers with
    /// the same `transactional.id` are mutually fenced at the
    /// broker. `AtLeastOnce` mode also caches for cheaper repeated
    /// writes.
    async fn acquire_producer(
        &self,
    ) -> Result<Arc<FutureProducer<EmatixKafkaContext>>, BackendError> {
        // Fast path: producer exists and (for ExactlyOnce)
        // transactions are already initialized.
        let needs_setup = {
            let session = self
                .producer_session
                .lock()
                .map_err(|e| BackendError::Other(format!("kafka producer lock: {e}")))?;
            match &session.producer {
                Some(p) => {
                    let need_init = matches!(
                        &self.delivery_semantics,
                        KafkaDeliverySemantics::ExactlyOnce { .. }
                    ) && !session.transactions_initialized;
                    if !need_init {
                        return Ok(Arc::clone(p));
                    }
                    // Reuse existing handle but still need to run
                    // init_transactions on it.
                    Some(Arc::clone(p))
                }
                None => None,
            }
        };

        let producer = match needs_setup {
            Some(p) => p,
            None => {
                let producer: FutureProducer<EmatixKafkaContext> = self
                    .client_config()
                    .create_with_context(self.build_context())
                    .map_err(|e| BackendError::Connection(format!("kafka producer create: {e}")))?;
                Arc::new(producer)
            }
        };

        // Run init_transactions for ExactlyOnce mode. Sync FFI →
        // spawn_blocking. 30s is the librdkafka-recommended timeout
        // for transactional bootstrap (broker registers the
        // transactional.id, fences any prior producer using it).
        if matches!(
            &self.delivery_semantics,
            KafkaDeliverySemantics::ExactlyOnce { .. }
        ) {
            let p = Arc::clone(&producer);
            tokio::task::spawn_blocking(move || {
                p.init_transactions(Timeout::After(Duration::from_secs(30)))
            })
            .await
            .map_err(|e| BackendError::Other(format!("kafka init_transactions join: {e}")))?
            .map_err(|e| BackendError::Query(format!("kafka init_transactions: {e}")))?;
        }

        let mut session = self
            .producer_session
            .lock()
            .map_err(|e| BackendError::Other(format!("kafka producer lock: {e}")))?;
        session.producer = Some(Arc::clone(&producer));
        if matches!(
            &self.delivery_semantics,
            KafkaDeliverySemantics::ExactlyOnce { .. }
        ) {
            session.transactions_initialized = true;
        }
        Ok(producer)
    }

    /// Get (or lazily-create) the StreamConsumer for `topic`. If the
    /// session is already subscribed to a different topic, drop the
    /// old consumer (and pending offsets — uncommitted reads on the
    /// old topic will be re-delivered on the next subscribe to it).
    fn acquire_consumer_for(
        &self,
        topic: &str,
    ) -> Result<Arc<StreamConsumer<EmatixKafkaContext>>, BackendError> {
        let mut session = self
            .consumer_session
            .lock()
            .map_err(|e| BackendError::Other(format!("kafka consumer lock: {e}")))?;
        let need_new = !matches!(
            (&session.consumer, &session.subscribed_topic),
            (Some(_), Some(t)) if t == topic
        );
        if need_new {
            // Default to `earliest` so a fresh consumer with no
            // committed offsets starts from the beginning. Long-
            // running consumers (36g) may want `latest`; they can
            // override at the ClientConfig layer in 36f+.
            let mut config = self.client_config();
            config.set("auto.offset.reset", "earliest");
            let context = self.build_context();
            let consumer: StreamConsumer<EmatixKafkaContext> = config
                .create_with_context(context)
                .map_err(|e| BackendError::Connection(format!("kafka consumer create: {e}")))?;
            consumer
                .subscribe(&[topic])
                .map_err(|e| BackendError::Connection(format!("kafka subscribe {topic}: {e}")))?;
            session.consumer = Some(Arc::new(consumer));
            session.subscribed_topic = Some(topic.to_string());
            session.pending_offsets.clear();
        }
        Ok(Arc::clone(session.consumer.as_ref().expect("just set")))
    }

    /// Override the batch limits applied by `read_arrow_stream`.
    /// Builder-style, so call sites can do
    /// `KafkaBackend::open(...)?.with_batch_config(KafkaBatchConfig {
    ///     batch_size: 1024, batch_window_ms: 200, ..Default::default()
    /// })`.
    pub fn with_batch_config(mut self, config: KafkaBatchConfig) -> Self {
        self.batch_config = config;
        self
    }

    /// Borrow the active batch config (tests + 36g supervisor read it).
    pub fn batch_config(&self) -> KafkaBatchConfig {
        self.batch_config
    }

    /// Build a fresh `ClientConfig` populated with this backend's
    /// bootstrap servers + optional group_id. 36f will layer auth
    /// settings (SASL/PLAIN, SASL/SCRAM, mTLS, MSK IAM
    /// `oauthbearer_token_refresh_cb`) on top through builder
    /// methods on `KafkaBackend` so the framework works against
    /// Confluent Cloud, self-hosted Kafka with SASL, AWS MSK, and
    /// other cloud-managed Kafka services without privileging any
    /// specific deployment.
    /// Build a fresh `EmatixKafkaContext` suitable for handing to a
    /// new producer / consumer / admin client. Captures
    /// `tokio::runtime::Handle::try_current()` lazily so the MSK IAM
    /// callback can bridge sync librdkafka into the async signer.
    /// For non-MSK auth modes the context is essentially inert
    /// (the OAUTHBEARER override never fires).
    fn build_context(&self) -> EmatixKafkaContext {
        let msk_region = match &self.auth {
            AuthMode::MskIam { region } => Some(region.clone()),
            _ => None,
        };
        let runtime = if msk_region.is_some() {
            tokio::runtime::Handle::try_current().ok()
        } else {
            None
        };
        EmatixKafkaContext {
            msk_region,
            runtime,
        }
    }

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
        // Phase 36f: layer auth-provider config keys.
        match &self.auth {
            AuthMode::None => {}
            AuthMode::SaslPlain { username, password } => {
                config.set("security.protocol", "SASL_SSL");
                config.set("sasl.mechanism", "PLAIN");
                config.set("sasl.username", username);
                config.set("sasl.password", password);
            }
            AuthMode::SaslScram {
                mechanism,
                username,
                password,
            } => {
                config.set("security.protocol", "SASL_SSL");
                config.set("sasl.mechanism", mechanism.as_kafka_str());
                config.set("sasl.username", username);
                config.set("sasl.password", password);
            }
            AuthMode::Tls(tls) => {
                config.set("security.protocol", "SSL");
                config.set("ssl.ca.location", &tls.ca_location);
                config.set("ssl.certificate.location", &tls.cert_location);
                config.set("ssl.key.location", &tls.key_location);
                if let Some(pw) = &tls.key_password {
                    config.set("ssl.key.password", pw);
                }
            }
            AuthMode::MskIam { region: _region } => {
                // Surface the OAUTHBEARER mechanism so librdkafka
                // knows it should ask us for tokens. The actual
                // generate_oauth_token callback wiring (custom
                // ClientContext + aws-msk-iam-sasl-signer) is the
                // 36f follow-up; until then this fails fast with a
                // clear "missing token" error from librdkafka, which
                // is more useful than letting traffic flow on plain
                // creds.
                config.set("security.protocol", "SASL_SSL");
                config.set("sasl.mechanism", "OAUTHBEARER");
            }
        }
        // Phase 36j: producer-side transactional config. ExactlyOnce
        // requires transactional.id + idempotence + acks=all (the
        // broker enforces these prerequisites).
        if let KafkaDeliverySemantics::ExactlyOnce { transactional_id } = &self.delivery_semantics {
            config.set("transactional.id", transactional_id);
            config.set("enable.idempotence", "true");
            config.set("acks", "all");
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
        let context = self.build_context();
        tokio::task::spawn_blocking(move || {
            let client: AdminClient<EmatixKafkaContext> = config
                .create_with_context(context)
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

    /// Subscribe to `query` (a topic name) and read messages until
    /// the consumer goes idle for `READ_IDLE_TIMEOUT_SECS`. Each
    /// message payload is decoded as a single JSON object and rows
    /// are concatenated into one Arrow `RecordBatch`. Schema is
    /// inferred from the first 1024 messages (arrow-json default).
    ///
    /// Returns an empty stream if the topic has no messages.
    ///
    /// Limits in 36b — folded out in later sub-phases:
    ///   - JSON-only payload decode (raw bytes / Avro / Protobuf land
    ///     in 36h via a `with_format(...)` builder).
    ///   - Bounded read: stops after `READ_IDLE_TIMEOUT_SECS` of no
    ///     new messages. The long-running streaming-consumer model
    ///     (36g) holds the topic open indefinitely and supervises a
    ///     consumer process; this `read_arrow_stream` is the
    ///     batch-read complement that fits the framework's existing
    ///     "read source → write target" call shape.
    ///   - Auto-commit stays disabled (set in `client_config`); 36e
    ///     wires the manual-commit path through the strategy
    ///     executors, where commits fire only after a durable write.
    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        if self.group_id.is_none() {
            return Err(BackendError::Other(
                "Kafka read_arrow_stream: group_id is required for the consumer; \
                 construct with KafkaBackend::open(bootstrap, Some(group_id))"
                    .into(),
            ));
        }
        let topic = query.trim();
        if topic.is_empty() {
            return Err(BackendError::Other(
                "Kafka read_arrow_stream: query argument must be a non-empty topic name".into(),
            ));
        }
        // Reject unsupported payload formats *before* opening the
        // consumer. Saves a broker round-trip + makes the error
        // testable without Docker.
        match self.payload_format {
            KafkaPayloadFormat::Json | KafkaPayloadFormat::RawBytes => {}
            KafkaPayloadFormat::Avro => {
                return Err(BackendError::Other(
                    "Kafka read_arrow_stream Avro: surface reserved in 36h.2; \
                     decode lands in Phase 36h.3 (Confluent Schema Registry \
                     client + magic-byte framing + apache_avro::Value → Arrow)"
                        .into(),
                ));
            }
            KafkaPayloadFormat::Protobuf => {
                return Err(BackendError::Other(
                    "Kafka read_arrow_stream Protobuf: surface reserved in 36h.2; \
                     decode lands in Phase 36h.5 (Schema Registry + prost-reflect)"
                        .into(),
                ));
            }
        }

        // Acquire / lazily-create the persistent consumer session.
        // Reuses the StreamConsumer across calls so group rebalance +
        // offset state survives between read_arrow_stream invocations
        // and `commit_offsets` can target the same broker session.
        let consumer = self.acquire_consumer_for(topic)?;

        // Drain the consumer, capturing per-partition max offset+1
        // so commit_offsets can advance the broker's view atomically.
        let (payloads, offsets) = drain_consumer(&consumer, &self.batch_config).await?;
        if !offsets.is_empty() {
            let mut session = self
                .consumer_session
                .lock()
                .map_err(|e| BackendError::Other(format!("kafka consumer lock: {e}")))?;
            // Merge: keep the highest commit position per partition.
            // (Multiple read_arrow_stream calls before a commit will
            // accumulate offsets across all of them.)
            for (partition, next_offset) in offsets {
                let entry = session
                    .pending_offsets
                    .entry(partition)
                    .or_insert(next_offset);
                if next_offset > *entry {
                    *entry = next_offset;
                }
            }
        }
        if payloads.is_empty() {
            let stream = futures_util::stream::empty();
            return Ok(Box::pin(stream));
        }
        let batches = match self.payload_format {
            KafkaPayloadFormat::Json => decode_payloads_as_jsonl(payloads)?,
            KafkaPayloadFormat::RawBytes => decode_payloads_as_raw_bytes(payloads)?,
            // Avro / Protobuf are rejected up-front (above), so this
            // arm is unreachable in practice. The compiler enforces
            // exhaustiveness; we route through unreachable!() to
            // keep the format-dispatch readable without dragging
            // duplicated error strings into two places.
            KafkaPayloadFormat::Avro | KafkaPayloadFormat::Protobuf => {
                unreachable!("Avro/Protobuf format rejected at read entry")
            }
        };
        let stream = futures_util::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }

    /// Produce each Arrow row as a Kafka message to `target.name`
    /// (the topic). Each batch is encoded as JSONL via arrow-json's
    /// `LineDelimitedWriter`; lines are split and produced as
    /// individual messages. `WriteMode::Truncate` is rejected — Kafka
    /// topics aren't truncatable from a producer (delete-and-recreate
    /// is an admin-tool operation).
    ///
    /// Limits in 36c — folded out in later sub-phases:
    ///   - JSON-only payload encode (raw bytes / Avro / Protobuf land
    ///     in 36h).
    ///   - At-least-once delivery: messages are awaited individually,
    ///     so a batch of N rows results in N awaited produces. Phase
    ///     36j wires Kafka transactions for exactly-once semantics.
    ///   - No partition keying yet — every message uses round-robin
    ///     partitioning. A `with_message_key(...)` builder lands in
    ///     36c+ once the keying contract is settled.
    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError> {
        if mode == WriteMode::Truncate {
            return Err(BackendError::Other(
                "Kafka write_arrow_stream: Truncate is not supported on a topic. \
                 Topics are append-only logs; to start fresh, delete and recreate \
                 the topic via admin tools."
                    .into(),
            ));
        }
        let topic = target.name.trim();
        if topic.is_empty() {
            return Err(BackendError::Other(
                "Kafka write_arrow_stream: target.name (topic) must be non-empty".into(),
            ));
        }
        // Reject unsupported payload formats *before* opening the
        // producer / running init_transactions. Saves a broker
        // round-trip + makes the error testable without Docker.
        match self.payload_format {
            KafkaPayloadFormat::Json | KafkaPayloadFormat::RawBytes => {}
            KafkaPayloadFormat::Avro => {
                return Err(BackendError::Other(
                    "Kafka write_arrow_stream Avro: surface reserved in 36h.2; \
                     encode lands in Phase 36h.4 (Arrow → apache_avro::Value + \
                     Schema Registry register/fetch + magic-byte framing)"
                        .into(),
                ));
            }
            KafkaPayloadFormat::Protobuf => {
                return Err(BackendError::Other(
                    "Kafka write_arrow_stream Protobuf: surface reserved in \
                     36h.2; encode lands in Phase 36h.6"
                        .into(),
                ));
            }
        }
        let producer = self.acquire_producer().await?;
        let transactional = matches!(
            &self.delivery_semantics,
            KafkaDeliverySemantics::ExactlyOnce { .. }
        );

        let mut s = stream;
        let mut total: u64 = 0;
        while let Some(batch) = futures_util::StreamExt::next(&mut s).await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let payloads = match self.payload_format {
                KafkaPayloadFormat::Json => encode_batch_as_jsonl_lines(&batch)?,
                KafkaPayloadFormat::RawBytes => encode_batch_as_raw_bytes(&batch)?,
                // Avro / Protobuf are rejected up-front (above), so
                // this arm is unreachable in practice.
                KafkaPayloadFormat::Avro | KafkaPayloadFormat::Protobuf => {
                    unreachable!("Avro/Protobuf format rejected at write entry")
                }
            };
            // ExactlyOnce: wrap the per-batch produce in a Kafka
            // transaction so a partial-failure mid-batch aborts and
            // no rows leak. The transaction is begun + committed
            // synchronously via `spawn_blocking` (the underlying
            // librdkafka calls block).
            if transactional {
                let p = Arc::clone(&producer);
                tokio::task::spawn_blocking(move || p.begin_transaction())
                    .await
                    .map_err(|e| BackendError::Other(format!("kafka begin_transaction join: {e}")))?
                    .map_err(|e| BackendError::Query(format!("kafka begin_transaction: {e}")))?;
            }
            let produce_result = produce_payloads_to_topic(&producer, topic, &payloads).await;
            match produce_result {
                Ok(n) => {
                    if transactional {
                        let p = Arc::clone(&producer);
                        tokio::task::spawn_blocking(move || {
                            p.commit_transaction(Timeout::After(Duration::from_secs(30)))
                        })
                        .await
                        .map_err(|e| {
                            BackendError::Other(format!("kafka commit_transaction join: {e}"))
                        })?
                        .map_err(|e| {
                            BackendError::Query(format!("kafka commit_transaction: {e}"))
                        })?;
                    }
                    total += n;
                }
                Err(e) => {
                    if transactional {
                        // Best-effort abort; if the abort itself
                        // fails (broker unreachable etc.) the
                        // transaction is force-aborted on broker
                        // timeout regardless. We surface the
                        // original produce error.
                        let p = Arc::clone(&producer);
                        let _ = tokio::task::spawn_blocking(move || {
                            p.abort_transaction(Timeout::After(Duration::from_secs(30)))
                        })
                        .await;
                    }
                    return Err(e);
                }
            }
        }
        Ok(total)
    }

    /// Produce-side run_append: read source rows via the source's
    /// `read_arrow_stream`, encode each row as a JSON message,
    /// produce to `spec.name` (the topic). Cross-backend by design —
    /// `source_backend` is required.
    ///
    /// run_history is intentionally *not* persisted by this backend.
    /// Kafka has no native sidecar for it (unlike PG / DuckDB /
    /// SQLite / MySQL with their `ematix_flow.run_history` tables, or
    /// ObjectStore / Delta with their JSONL sidecar files). Callers
    /// who need durable run audit should layer a separate
    /// observability backend; the StrategyRunResult is still returned
    /// for in-process observability.
    async fn run_append(
        &self,
        spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        incremental_column: Option<&str>,
        last_value_literal: Option<&str>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        let source = source_backend.ok_or_else(|| {
            BackendError::Other(
                "Kafka run_append: source_backend is required \
                 (Kafka is a target only — there is no same-backend path)"
                    .into(),
            )
        })?;
        // Watermark filter wraps the source SQL in the source's
        // dialect. Kafka itself doesn't track watermarks (no
        // queryable surface); users running incremental loads to
        // Kafka must persist `last_value_literal` externally.
        let watermark = incremental_column.map(|c| crate::meta::WatermarkConfig {
            column: c.to_string(),
            last_value_literal: last_value_literal.map(|s| s.to_string()),
        });
        let filtered_source =
            crate::meta::wrap_with_watermark_filter(source_query, watermark.as_ref());

        let run_id = uuid::Uuid::now_v7();
        let target = TargetTable {
            schema: spec.schema.clone(),
            name: spec.name.clone(),
        };
        let inserted: u64 = if dry_run {
            // Probe the source so a missing query / bad credentials
            // surfaces; do not produce anything.
            let _ = source.read_arrow_stream(&filtered_source).await?;
            0
        } else {
            let stream = source.read_arrow_stream(&filtered_source).await?;
            self.write_arrow_stream(&target, stream, WriteMode::Append)
                .await?
        };
        Ok(StrategyRunResult {
            run_id: run_id.to_string(),
            rows_inserted: inserted as i64,
            rows_updated: None,
            rows_unchanged: None,
            rows_closed: None,
            status: if dry_run { "dry_run" } else { "success" }.into(),
            // pipeline_name is logged here so per-pipeline routing is
            // visible at the StrategyRunResult level even though we
            // don't persist run_history rows for Kafka.
            path: format!(
                "cross_backend → kafka topic '{topic}' (pipeline={pipeline_name})",
                topic = spec.name,
            ),
        })
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

    /// Override the trait's default-noop `commit_offsets` to fire
    /// the inherent `KafkaBackend::commit_offsets`. This way the
    /// `StreamingPipeline` runner can call `source.commit_offsets()`
    /// through `&dyn Backend` and have the right thing happen for
    /// Kafka sources without downcasting.
    async fn commit_offsets(&self) -> Result<(), BackendError> {
        // Fully-qualified path to avoid recursing into this trait
        // method. The inherent method has the same name (the existing
        // 36e API surface) and that's intentional.
        Self::commit_offsets(self).await
    }
}

/// How long to wait for the first message after subscribing. The
/// consumer's initial group join + partition assignment + offset
/// fetch can take several seconds, so the first-message budget is
/// larger than the subsequent-message one.
const READ_FIRST_MESSAGE_TIMEOUT_SECS: u64 = 15;

/// Drain `consumer` into a `Vec<Vec<u8>>` of payloads, honoring the
/// batch limits in `cfg`. Returns when **any** of these fires:
///   - `cfg.batch_size` messages received,
///   - `cfg.batch_bytes` total payload bytes accumulated,
///   - `cfg.batch_window_ms` elapsed since the first message arrived
///     (latency cap — independent of the idle timer),
///   - `cfg.idle_timeout_ms` elapsed without a new message.
///
/// Messages with no payload (Kafka tombstones) are skipped — they're
/// meaningful for compacted topics but don't carry a row to decode,
/// and they don't tick `batch_size` or `batch_bytes`.
///
/// First-message wait is independently bumped to
/// `READ_FIRST_MESSAGE_TIMEOUT_SECS` so a fresh broker has time to
/// rebalance and assign partitions. The user-provided
/// `batch_window_ms` clock starts on the first received message,
/// not on subscribe.
async fn drain_consumer(
    consumer: &StreamConsumer<EmatixKafkaContext>,
    cfg: &KafkaBatchConfig,
) -> Result<(Vec<Vec<u8>>, HashMap<i32, i64>), BackendError> {
    let mut payloads: Vec<Vec<u8>> = Vec::new();
    let mut total_bytes: usize = 0;
    let mut window_start: Option<std::time::Instant> = None;
    // For commit_offsets: remember the highest (offset + 1) per
    // partition we've consumed in this drain.
    let mut max_offsets: HashMap<i32, i64> = HashMap::new();

    while payloads.len() < cfg.batch_size {
        // Compute the per-recv timeout. Three clocks are in play:
        //   1. First-message wait: generous, swallows rebalance.
        //   2. Idle timeout: tight, stops drain when topic is quiet.
        //   3. Batch window: caps total time-from-first-message, so
        //      a low-rate topic still flushes a small batch promptly.
        let recv_timeout = if payloads.is_empty() {
            Duration::from_secs(READ_FIRST_MESSAGE_TIMEOUT_SECS)
        } else {
            // Take the smaller of (idle_timeout, remaining window).
            let idle = Duration::from_millis(cfg.idle_timeout_ms);
            match window_start {
                Some(start) => {
                    let elapsed = start.elapsed();
                    let total = Duration::from_millis(cfg.batch_window_ms);
                    let remaining = total.saturating_sub(elapsed);
                    if remaining.is_zero() {
                        break; // window already exhausted
                    }
                    idle.min(remaining)
                }
                None => idle,
            }
        };
        let recv_fut = consumer.recv();
        let msg = tokio::time::timeout(recv_timeout, recv_fut).await;
        match msg {
            Ok(Ok(borrowed)) => {
                let partition = borrowed.partition();
                let offset = borrowed.offset();
                if let Some(payload) = borrowed.payload() {
                    if window_start.is_none() {
                        window_start = Some(std::time::Instant::now());
                    }
                    total_bytes += payload.len();
                    payloads.push(payload.to_vec());
                    // Record commit position for this partition: the
                    // offset of the next message to consume.
                    let next = offset + 1;
                    let entry = max_offsets.entry(partition).or_insert(next);
                    if next > *entry {
                        *entry = next;
                    }
                    if total_bytes >= cfg.batch_bytes {
                        break;
                    }
                }
            }
            Ok(Err(e)) => {
                // UnknownTopicOrPartition is the broker's way of
                // saying "this topic doesn't exist". For a batch-read
                // that's logically equivalent to an empty stream —
                // return what we have rather than escalating.
                let s = e.to_string();
                if s.contains("UnknownTopicOrPartition") || s.contains("Unknown topic or partition")
                {
                    break;
                }
                return Err(BackendError::Query(format!("kafka recv: {e}")));
            }
            Err(_elapsed) => break, // idle / window timeout → flush
        }
    }
    Ok((payloads, max_offsets))
}

/// Decode a Vec of message payloads as JSONL. Concatenates payloads
/// with `\n` separators so arrow-json's
/// `infer_json_schema_from_seekable` can run a single pass. Returns
/// the full RecordBatch list — typically one batch for a moderate
/// drain; arrow-json may chunk if the buffer is large.
fn decode_payloads_as_jsonl(payloads: Vec<Vec<u8>>) -> Result<Vec<RecordBatch>, BackendError> {
    use arrow_json::ReaderBuilder;
    use arrow_json::reader::infer_json_schema_from_seekable;

    let mut buf: Vec<u8> = Vec::with_capacity(payloads.iter().map(|p| p.len() + 1).sum());
    for p in &payloads {
        buf.extend_from_slice(p);
        // arrow-json's line-delimited reader splits on `\n`; ensure
        // every payload ends with one, regardless of whether the
        // producer included a trailing newline.
        if !p.ends_with(b"\n") {
            buf.push(b'\n');
        }
    }
    let mut cursor = std::io::Cursor::new(buf);
    let (schema, _records_inferred) = infer_json_schema_from_seekable(&mut cursor, Some(1024))
        .map_err(|e| BackendError::Query(format!("kafka json infer: {e}")))?;
    let reader = ReaderBuilder::new(Arc::new(schema))
        .build(std::io::BufReader::new(cursor))
        .map_err(|e| BackendError::Query(format!("kafka json reader: {e}")))?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for b in reader {
        batches.push(b.map_err(|e| BackendError::Query(format!("kafka json batch: {e}")))?);
    }
    Ok(batches)
}

/// Decode a Vec of message payloads under the `RawBytes` format:
/// one row per message, one column ("payload", Binary) carrying the
/// opaque blob. Tombstones (zero-byte payloads on the read path are
/// already filtered upstream as None) reach this function as
/// already-collected `Vec<u8>`s.
fn decode_payloads_as_raw_bytes(payloads: Vec<Vec<u8>>) -> Result<Vec<RecordBatch>, BackendError> {
    use arrow_array::BinaryArray;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    if payloads.is_empty() {
        return Ok(Vec::new());
    }
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        RAW_BYTES_COLUMN,
        DataType::Binary,
        false,
    )]));
    let payload_refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
    let array = BinaryArray::from_vec(payload_refs);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])
        .map_err(|e| BackendError::Query(format!("kafka raw_bytes record_batch: {e}")))?;
    Ok(vec![batch])
}

/// Encode a `RecordBatch` for the `RawBytes` format. The batch must
/// have **exactly one** column of type Binary (the column name is
/// not significant). Each row's value is one outgoing payload —
/// nulls become empty payloads (matching Kafka tombstone semantics
/// on the produce side).
fn encode_batch_as_raw_bytes(batch: &RecordBatch) -> Result<Vec<Vec<u8>>, BackendError> {
    use arrow_array::Array;
    use arrow_array::BinaryArray;
    use arrow_schema::DataType;

    if batch.num_columns() != 1 {
        return Err(BackendError::Query(format!(
            "kafka write_arrow_stream RawBytes: expected 1 column, got {}",
            batch.num_columns()
        )));
    }
    let column = batch.column(0);
    if !matches!(column.data_type(), DataType::Binary) {
        return Err(BackendError::Query(format!(
            "kafka write_arrow_stream RawBytes: expected single column of \
             type Binary, got {:?}",
            column.data_type()
        )));
    }
    let bin = column
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            BackendError::Query("kafka raw_bytes: BinaryArray downcast failed".into())
        })?;
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(batch.num_rows());
    for i in 0..bin.len() {
        if bin.is_null(i) {
            out.push(Vec::new());
        } else {
            out.push(bin.value(i).to_vec());
        }
    }
    Ok(out)
}

/// Produce each `payload` to `topic` via `producer`, awaiting the
/// broker ack per message. Returns the number of messages
/// successfully produced. Used by both the `AtLeastOnce` (no
/// surrounding transaction) and `ExactlyOnce` (surrounding txn)
/// produce paths in `write_arrow_stream`.
async fn produce_payloads_to_topic(
    producer: &FutureProducer<EmatixKafkaContext>,
    topic: &str,
    payloads: &[Vec<u8>],
) -> Result<u64, BackendError> {
    let mut total: u64 = 0;
    for payload in payloads {
        producer
            .send(
                FutureRecord::<(), [u8]>::to(topic).payload(payload.as_slice()),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .map_err(|(e, _msg)| BackendError::Query(format!("kafka produce {topic}: {e}")))?;
        total += 1;
    }
    Ok(total)
}

/// Encode a `RecordBatch` as JSONL bytes, then split on newlines so
/// each row becomes its own payload for produce. The `Vec<Vec<u8>>`
/// has one entry per row (matching `batch.num_rows()`).
fn encode_batch_as_jsonl_lines(batch: &RecordBatch) -> Result<Vec<Vec<u8>>, BackendError> {
    use arrow_json::LineDelimitedWriter;

    let mut buf: Vec<u8> = Vec::with_capacity(batch.num_rows() * 64);
    let mut writer = LineDelimitedWriter::new(&mut buf);
    writer
        .write(batch)
        .map_err(|e| BackendError::Query(format!("kafka produce json encode: {e}")))?;
    writer
        .finish()
        .map_err(|e| BackendError::Query(format!("kafka produce json finish: {e}")))?;
    drop(writer);
    // arrow-json terminates each row with `\n`; split-by-newline
    // produces one trailing empty entry which we filter out.
    let lines: Vec<Vec<u8>> = buf
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| l.to_vec())
        .collect();
    Ok(lines)
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
    fn batch_config_default_is_reasonable() {
        let cfg = KafkaBatchConfig::default();
        assert_eq!(cfg.batch_size, 100_000);
        assert_eq!(cfg.batch_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.batch_window_ms, 5_000);
        assert_eq!(cfg.idle_timeout_ms, 5_000);
    }

    #[test]
    fn with_batch_config_overrides_defaults() {
        let custom = KafkaBatchConfig {
            batch_size: 50,
            batch_bytes: 1024,
            batch_window_ms: 250,
            idle_timeout_ms: 1_000,
        };
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_batch_config(custom);
        assert_eq!(b.batch_config(), custom);
    }

    /// `ClientConfig`'s `Debug` strips sensitive keys (passwords),
    /// so we use `.get(key)` for direct assertions instead.
    fn config_get(b: &KafkaBackend, key: &str) -> Option<String> {
        b.client_config().get(key).map(|s| s.to_string())
    }

    #[test]
    fn with_sasl_plain_sets_kafka_keys() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_sasl_plain("alice", "s3cret");
        assert_eq!(
            config_get(&b, "security.protocol").as_deref(),
            Some("SASL_SSL")
        );
        assert_eq!(config_get(&b, "sasl.mechanism").as_deref(), Some("PLAIN"));
        assert_eq!(config_get(&b, "sasl.username").as_deref(), Some("alice"));
        assert_eq!(config_get(&b, "sasl.password").as_deref(), Some("s3cret"));
    }

    #[test]
    fn with_sasl_scram_sets_kafka_keys_per_mechanism() {
        let b256 = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_sasl_scram(ScramMechanism::Sha256, "alice", "s3cret");
        assert_eq!(
            config_get(&b256, "sasl.mechanism").as_deref(),
            Some("SCRAM-SHA-256")
        );
        let b512 = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_sasl_scram(ScramMechanism::Sha512, "bob", "s3cret");
        assert_eq!(
            config_get(&b512, "sasl.mechanism").as_deref(),
            Some("SCRAM-SHA-512")
        );
        assert_eq!(config_get(&b512, "sasl.username").as_deref(), Some("bob"));
    }

    #[test]
    fn with_tls_sets_kafka_keys() {
        let tls = TlsAuth {
            ca_location: "/tmp/ca.pem".into(),
            cert_location: "/tmp/cert.pem".into(),
            key_location: "/tmp/key.pem".into(),
            key_password: Some("kp".into()),
        };
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_tls(tls);
        assert_eq!(config_get(&b, "security.protocol").as_deref(), Some("SSL"));
        assert_eq!(
            config_get(&b, "ssl.ca.location").as_deref(),
            Some("/tmp/ca.pem")
        );
        assert_eq!(
            config_get(&b, "ssl.certificate.location").as_deref(),
            Some("/tmp/cert.pem")
        );
        assert_eq!(
            config_get(&b, "ssl.key.location").as_deref(),
            Some("/tmp/key.pem")
        );
        assert_eq!(config_get(&b, "ssl.key.password").as_deref(), Some("kp"));
    }

    #[test]
    fn with_msk_iam_sets_oauthbearer_mechanism() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_msk_iam("us-east-1");
        assert_eq!(
            config_get(&b, "security.protocol").as_deref(),
            Some("SASL_SSL")
        );
        assert_eq!(
            config_get(&b, "sasl.mechanism").as_deref(),
            Some("OAUTHBEARER")
        );
    }

    #[test]
    fn open_with_no_auth_leaves_security_protocol_unset() {
        let b = KafkaBackend::open("localhost:9092", Some("g1")).unwrap();
        assert_eq!(config_get(&b, "security.protocol"), None);
        assert_eq!(config_get(&b, "sasl.mechanism"), None);
    }

    // --- Phase 36h: payload format dispatch -------------------------------

    #[test]
    fn payload_format_default_is_json() {
        let b = KafkaBackend::open("localhost:9092", Some("g1")).unwrap();
        assert_eq!(b.payload_format(), KafkaPayloadFormat::Json);
    }

    #[test]
    fn with_payload_format_overrides_default() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_payload_format(KafkaPayloadFormat::RawBytes);
        assert_eq!(b.payload_format(), KafkaPayloadFormat::RawBytes);
    }

    #[test]
    fn raw_bytes_decode_emits_one_binary_column_per_message() {
        let payloads = vec![b"a".to_vec(), b"bb".to_vec(), b"".to_vec()];
        let batches = decode_payloads_as_raw_bytes(payloads).unwrap();
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.num_columns(), 1);
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        assert_eq!(arr.value(0), b"a");
        assert_eq!(arr.value(1), b"bb");
        assert_eq!(arr.value(2), b"");
    }

    #[test]
    fn raw_bytes_encode_rejects_multi_column_batch() {
        use arrow_array::{BinaryArray, Int64Array};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("k", DataType::Binary, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BinaryArray::from_vec(vec![b"a"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let err = encode_batch_as_raw_bytes(&batch).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("expected 1 column"), "got: {msg}");
    }

    #[test]
    fn raw_bytes_encode_rejects_non_binary_column() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let err = encode_batch_as_raw_bytes(&batch).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("type Binary"), "got: {msg}");
    }

    // --- Phase 36h.2: Avro / Protobuf surface reserved --------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn avro_read_rejects_with_pointer_to_36h_3() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_payload_format(KafkaPayloadFormat::Avro);
        let err = match b.read_arrow_stream("any-topic").await {
            Ok(_) => panic!("expected Avro read rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("Phase 36h.3"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn avro_write_rejects_with_pointer_to_36h_4() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        let b = KafkaBackend::open("localhost:9092", None)
            .unwrap()
            .with_payload_format(KafkaPayloadFormat::Avro);
        // Build a non-empty stream so dispatch reaches the format
        // match (an empty stream would early-return before encoding).
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch =
            arrow_array::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])
                .unwrap();
        let stream: ArrowBatchStream = Box::pin(futures_util::stream::once(async move {
            Ok::<_, BackendError>(batch)
        }));
        let target = TargetTable {
            schema: "".into(),
            name: "any".into(),
        };
        let err = b
            .write_arrow_stream(&target, stream, WriteMode::Append)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Phase 36h.4"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn protobuf_read_rejects_with_pointer_to_36h_5() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_payload_format(KafkaPayloadFormat::Protobuf);
        let err = match b.read_arrow_stream("any-topic").await {
            Ok(_) => panic!("expected Protobuf read rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("Phase 36h.5"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn protobuf_write_rejects_with_pointer_to_36h_6() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        let b = KafkaBackend::open("localhost:9092", None)
            .unwrap()
            .with_payload_format(KafkaPayloadFormat::Protobuf);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch =
            arrow_array::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])
                .unwrap();
        let stream: ArrowBatchStream = Box::pin(futures_util::stream::once(async move {
            Ok::<_, BackendError>(batch)
        }));
        let target = TargetTable {
            schema: "".into(),
            name: "any".into(),
        };
        let err = b
            .write_arrow_stream(&target, stream, WriteMode::Append)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Phase 36h.6"), "got: {msg}");
    }

    // --- Phase 36j: delivery semantics ------------------------------------

    #[test]
    fn delivery_semantics_default_is_at_least_once() {
        let b = KafkaBackend::open("localhost:9092", Some("g1")).unwrap();
        assert_eq!(*b.delivery_semantics(), KafkaDeliverySemantics::AtLeastOnce);
    }

    #[test]
    fn with_delivery_semantics_exactly_once_sets_kafka_keys() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_delivery_semantics(KafkaDeliverySemantics::ExactlyOnce {
                transactional_id: "p-tx-1".into(),
            });
        // transactional.id + idempotence + acks=all are all
        // prerequisites the broker enforces for transactions.
        assert_eq!(
            config_get(&b, "transactional.id").as_deref(),
            Some("p-tx-1")
        );
        assert_eq!(
            config_get(&b, "enable.idempotence").as_deref(),
            Some("true")
        );
        assert_eq!(config_get(&b, "acks").as_deref(), Some("all"));
    }

    #[test]
    fn with_delivery_semantics_at_least_once_leaves_keys_unset() {
        let b = KafkaBackend::open("localhost:9092", Some("g1")).unwrap();
        assert_eq!(config_get(&b, "transactional.id"), None);
        assert_eq!(config_get(&b, "enable.idempotence"), None);
    }

    #[test]
    fn raw_bytes_encode_round_trips_payloads() {
        use arrow_array::BinaryArray;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "data",
            DataType::Binary,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from_iter([
                Some(b"alpha".as_slice()),
                None,
                Some(b"gamma".as_slice()),
            ]))],
        )
        .unwrap();
        let payloads = encode_batch_as_raw_bytes(&batch).unwrap();
        // null becomes empty payload.
        assert_eq!(
            payloads,
            vec![b"alpha".to_vec(), Vec::<u8>::new(), b"gamma".to_vec()]
        );
    }

    /// Phase 36f.2: build_context() captures the MSK region for the
    /// OAUTHBEARER callback. Runtime handle is captured only when an
    /// async runtime is present — this test runs sync, so we assert
    /// region is set and runtime is None (the callback would error
    /// helpfully at first OAUTHBEARER fire).
    #[test]
    fn build_context_captures_msk_region_without_runtime_in_sync() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_msk_iam("us-east-1");
        let ctx = b.build_context();
        assert_eq!(ctx.msk_region.as_deref(), Some("us-east-1"));
        // No tokio runtime in this sync #[test] — handle is None.
        assert!(ctx.runtime.is_none());
    }

    /// build_context() captures `Handle::current()` when called from
    /// async context (the typical caller path for KafkaBackend usage).
    #[tokio::test(flavor = "multi_thread")]
    async fn build_context_captures_runtime_handle_in_async() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_msk_iam("us-east-1");
        let ctx = b.build_context();
        assert_eq!(ctx.msk_region.as_deref(), Some("us-east-1"));
        assert!(
            ctx.runtime.is_some(),
            "runtime handle must be captured for the OAUTHBEARER callback"
        );
    }

    /// Non-MSK auth modes don't capture a runtime — the callback
    /// never fires because librdkafka invokes it only when
    /// sasl.mechanism = OAUTHBEARER.
    #[tokio::test(flavor = "multi_thread")]
    async fn build_context_no_runtime_for_non_msk_auth() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_sasl_plain("alice", "s3cret");
        let ctx = b.build_context();
        assert!(ctx.msk_region.is_none());
        assert!(ctx.runtime.is_none());
    }

    /// generate_oauth_token returns a clear "no region configured"
    /// error when called on a context with msk_region=None. The
    /// real callback path only fires for MSK-configured backends,
    /// but the safety net matters in case of misconfigured tests.
    #[test]
    fn generate_oauth_token_errors_without_msk_region() {
        let ctx = EmatixKafkaContext::default();
        let err = match ctx.generate_oauth_token(None) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("no MSK region"), "got: {msg}");
    }

    /// generate_oauth_token errors with a clear message when called
    /// on a context that has the MSK region but no runtime handle —
    /// this is the "user called with_msk_iam from outside async
    /// context" case.
    #[test]
    fn generate_oauth_token_errors_without_runtime() {
        let ctx = EmatixKafkaContext {
            msk_region: Some("us-east-1".into()),
            runtime: None,
        };
        let err = match ctx.generate_oauth_token(None) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("tokio runtime"), "got: {msg}");
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
