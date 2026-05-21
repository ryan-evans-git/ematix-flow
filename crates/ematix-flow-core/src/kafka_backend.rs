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
use std::sync::{Arc, Mutex, RwLock};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct TlsAuth {
    pub ca_location: String,
    pub cert_location: String,
    pub key_location: String,
    pub key_password: Option<String>,
}

/// Hand-written `Debug` for `TlsAuth` that redacts `key_password`
/// (a private-key passphrase). The cert/key/ca *paths* are
/// non-secret and remain visible.
impl std::fmt::Debug for TlsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsAuth")
            .field("ca_location", &self.ca_location)
            .field("cert_location", &self.cert_location)
            .field("key_location", &self.key_location)
            .field(
                "key_password",
                &if self.key_password.is_some() {
                    "<redacted>"
                } else {
                    "None"
                },
            )
            .finish()
    }
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
// Σ.B follow-up: `AuthMode` itself stays private (the live state
// is internal to KafkaBackend), but the `From<&AuthMode> for
// KafkaAuthConfig` impl below maps it to the public, serializable
// `crate::backend::KafkaAuthConfig` so `Backend::config()` can
// round-trip the auth surface. The reverse direction
// (`KafkaAuthConfig` → builder calls) lives in `backend_from_config`
// where it dispatches via the existing public with_* methods.
#[derive(Clone, Default)]
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

impl From<&AuthMode> for crate::backend::KafkaAuthConfig {
    fn from(am: &AuthMode) -> Self {
        match am {
            AuthMode::None => crate::backend::KafkaAuthConfig::None,
            AuthMode::SaslPlain { username, password } => {
                crate::backend::KafkaAuthConfig::SaslPlain {
                    username: username.clone(),
                    password: password.clone(),
                }
            }
            AuthMode::SaslScram {
                mechanism,
                username,
                password,
            } => crate::backend::KafkaAuthConfig::SaslScram {
                mechanism: *mechanism,
                username: username.clone(),
                password: password.clone(),
            },
            AuthMode::Tls(tls) => crate::backend::KafkaAuthConfig::Tls(tls.clone()),
            AuthMode::MskIam { region } => crate::backend::KafkaAuthConfig::MskIam {
                region: region.clone(),
            },
        }
    }
}

/// Hand-written `Debug` for `AuthMode` that redacts every secret-
/// bearing field. Logging an `AuthMode` (directly or via the
/// owning `KafkaBackend`) won't leak passwords, key passwords, or
/// any other confidential bits — just the auth-mode shape +
/// non-secret labels (username, mechanism, region).
impl std::fmt::Debug for AuthMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthMode::None => f.write_str("None"),
            AuthMode::SaslPlain { username, .. } => f
                .debug_struct("SaslPlain")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            AuthMode::SaslScram {
                mechanism,
                username,
                ..
            } => f
                .debug_struct("SaslScram")
                .field("mechanism", mechanism)
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            AuthMode::Tls(tls) => f
                .debug_struct("Tls")
                .field("ca_location", &tls.ca_location)
                .field("cert_location", &tls.cert_location)
                .field("key_location", &tls.key_location)
                .field(
                    "key_password",
                    &if tls.key_password.is_some() {
                        "<redacted>"
                    } else {
                        "None"
                    },
                )
                .finish(),
            AuthMode::MskIam { region } => {
                f.debug_struct("MskIam").field("region", region).finish()
            }
        }
    }
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
    /// P4 #26: per-partition recovered offsets to apply on the next
    /// `Rebalance::Assign(tpl)`. Populated by `KafkaBackend::seek_to`,
    /// shared with the backend so a `seek_to` call after a consumer
    /// is built still reaches the active context. Entries are
    /// removed from the map as they're consumed by `post_rebalance`,
    /// so a later rebalance with broker-stored committed offsets
    /// doesn't get clobbered by the original recovered offsets.
    seek_map: Arc<std::sync::Mutex<HashMap<i32, i64>>>,
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

impl ConsumerContext for EmatixKafkaContext {
    /// P4 #26: bridge `seek_to`-recovered offsets into the
    /// consumer-group rebalance protocol. Replaces the prior
    /// "manual `assign + Offset::Offset(...)` at acquire time"
    /// path with `subscribe()` + this callback — the result is
    /// equivalent for single-worker pipelines (initial assign-all
    /// rebalance triggers the seek), and correctly handles
    /// partition reassignment when a future multi-worker setup
    /// (Σ.D) joins or leaves the group mid-stream.
    ///
    /// Each entry is consumed (removed) the first time the
    /// callback applies it, so a subsequent rebalance reads
    /// broker-stored committed offsets instead of re-applying
    /// the now-stale recovery point.
    fn post_rebalance(
        &self,
        base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        let rdkafka::consumer::Rebalance::Assign(tpl) = rebalance else {
            return;
        };
        let Ok(mut map) = self.seek_map.lock() else {
            return;
        };
        if map.is_empty() {
            return;
        }
        for elem in tpl.elements() {
            let partition = elem.partition();
            let Some(offset) = map.remove(&partition) else {
                continue;
            };
            // Best-effort: a failed seek surfaces on the next
            // poll as a Kafka error and the supervisor handles
            // it. We don't want to panic from inside the
            // librdkafka rebalance callback.
            if let Err(e) = base_consumer.seek(
                elem.topic(),
                partition,
                Offset::Offset(offset),
                std::time::Duration::from_secs(5),
            ) {
                tracing::warn!(
                    topic = %elem.topic(),
                    partition,
                    offset,
                    error = %e,
                    "kafka post_rebalance seek failed; partition will use \
                     broker-stored committed offset (or auto.offset.reset)"
                );
            }
        }
    }
}

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
    /// P4 #26: shared per-partition seek map populated by
    /// `seek_to(...)` and consumed by `EmatixKafkaContext::post_rebalance`.
    /// Cloned (Arc) into every consumer/producer/admin context built
    /// via `build_context()` — only the consumer's rebalance callback
    /// reads it; producer/admin contexts hold an inert clone.
    seek_map: Arc<std::sync::Mutex<HashMap<i32, i64>>>,
    /// Auth provider — `AuthMode::None` for unauthenticated clusters;
    /// populated by the `with_sasl_plain` / `with_sasl_scram` /
    /// `with_tls` / `with_msk_iam` builder methods.
    auth: AuthMode,
    /// Payload wire format applied by `read_arrow_stream` /
    /// `write_arrow_stream`. Builder-set via `with_payload_format`;
    /// defaults to JSON.
    payload_format: KafkaPayloadFormat,
    /// Consumer-side `auto.offset.reset` override. `Some("earliest")`
    /// (default behavior) reads from the start of the topic when
    /// there's no committed offset; `Some("latest")` only sees events
    /// produced AFTER the consumer subscribes. None ⇒ rdkafka library
    /// default (`latest`). Builder-set via `with_auto_offset_reset`.
    auto_offset_reset: Option<String>,
    /// Producer-side delivery semantics. Builder-set via
    /// `with_delivery_semantics`; defaults to `AtLeastOnce`.
    delivery_semantics: KafkaDeliverySemantics,
    /// Cached transactional producer state. Required for
    /// `ExactlyOnce` because init_transactions is once-per-lifetime
    /// and same-`transactional.id` producers are mutually exclusive
    /// at the broker.
    producer_session: Arc<Mutex<ProducerSession>>,
    /// Confluent Schema Registry URL (e.g. `http://localhost:8081`).
    /// Required for `KafkaPayloadFormat::Avro` (Phase 36h.3) and
    /// `Protobuf` (Phase 36h.5); ignored for JSON / RawBytes.
    /// Builder-set via `with_schema_registry_url`.
    schema_registry_url: Option<String>,
    /// Optional Schema Registry basic-auth credentials. Set via
    /// `with_schema_registry_basic_auth(user, password)`. Confluent
    /// Cloud uses an API key (username) + API secret (password)
    /// pair here. Wrapper type carries a custom Debug impl so the
    /// password redacts when KafkaBackend is `{:?}`-printed.
    schema_registry_basic_auth: Option<SrBasicAuth>,
    /// Task #556: schema-registry wire-format selector. `Confluent`
    /// (default) uses the 0x00+4-byte-BE-id framing handled by the
    /// existing `decode_payloads_as_avro` path. `Glue { ... }` swaps
    /// in the AWS Glue framing (0x03+16-byte-UUID+1-byte-codec) and
    /// resolves schemas via the named Python callback (typically
    /// `ematix_flow.glue_schema_registry.fetch_schema_by_uuid`).
    schema_registry_kind: SchemaRegistryKind,
    /// Task #556: per-backend cache of UUID→parsed-Avro-schema
    /// mappings for the Glue *consumer* dispatch path. Populated
    /// lazily on first sight of each schema_uuid via a callback into
    /// Python's boto3 ``GetSchemaVersion`` call.
    glue_schema_cache: Arc<RwLock<HashMap<uuid::Uuid, Arc<apache_avro::Schema>>>>,
    /// Task #556 producer slice: per-backend cache of
    /// schema-name→(UUID, parsed Avro schema) for the Glue producer
    /// path. Populated lazily on first send via a callback into
    /// Python's boto3 ``GetSchemaVersion(LatestVersion=True)`` call.
    /// Separate from ``glue_schema_cache`` because the key shape and
    /// payload differ (producers also need the UUID to embed in the
    /// wire frame).
    glue_producer_schema_cache:
        Arc<RwLock<HashMap<String, Arc<(uuid::Uuid, apache_avro::Schema)>>>>,
    /// Phase 40.2: name of the Arrow column to use as the per-row
    /// Kafka message key. `None` means round-robin (default sticky
    /// partitioner) — matches pre-40.2 behavior. When set, the
    /// column must exist in every produced batch and be a string-
    /// compatible type (`Utf8`, `LargeUtf8`, or `Binary`); other
    /// types raise a clear error at produce time.
    message_key_column: Option<String>,
}

/// Task #556: Kafka schema-registry wire-format selector.
///
/// `Confluent` (default) preserves the pre-existing behavior — the
/// 0x00+4-byte-BE-id framing handled by the
/// [`decode_payloads_as_avro`] path via the `schema_registry_converter`
/// crate. `Glue` swaps in AWS Glue's 0x03+16-byte-UUID+1-byte-codec
/// framing; schema definitions are resolved at decode time via the
/// [`crate::py_callbacks`] registry, with the boto3-backed lookup
/// living in Python (`ematix_flow.glue_schema_registry`).
///
/// IAM credential refresh is handled by boto3 inside the Python
/// callback — the Rust side stays IAM-free. The Glue variant carries
/// the region and registry name so the callback has enough context
/// to call `GetSchemaVersion`; the schema UUID comes from the wire
/// frame on each message.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaRegistryKind {
    /// Confluent / Apicurio wire format (0x00 + 4-byte BE schema id).
    /// The schema fetch is handled by `schema_registry_converter`.
    Confluent,
    /// AWS Glue Schema Registry wire format (0x03 + 16-byte UUID +
    /// 1-byte codec). Schema fetch routes through named callbacks,
    /// which are expected to call boto3's Glue API.
    Glue {
        /// AWS region the registry lives in. Passed verbatim to the
        /// lookup callbacks so they target the right Glue endpoint.
        region: String,
        /// The Glue registry name. The wire format only carries the
        /// UUID; this is supplied to the callbacks so they have
        /// enough context to call ``GetSchemaVersion`` /
        /// ``ListSchemaVersions``.
        registry_name: String,
        /// Consumer-side callback: takes a schema UUID, returns the
        /// schema text + format. Wraps boto3's ``GetSchemaVersion``.
        /// Defaults to
        /// ``ematix_flow.glue_schema_registry.fetch_schema_by_uuid``.
        schema_lookup_callback: String,
        /// Producer-side callback: takes a schema name, returns
        /// the latest UUID + schema text. Wraps boto3's
        /// ``GetSchemaVersion`` with
        /// ``SchemaVersionNumber={"LatestVersion": True}``. Defaults
        /// to
        /// ``ematix_flow.glue_schema_registry.fetch_schema_by_name``.
        /// Empty string means producers haven't wired up Glue —
        /// any attempt to produce with this variant will error.
        #[serde(default)]
        schema_lookup_by_name_callback: String,
    },
}

impl Default for SchemaRegistryKind {
    fn default() -> Self {
        Self::Confluent
    }
}

impl SchemaRegistryKind {
    /// True if this is the Glue variant. Used by hot-path dispatch to
    /// skip a `match` step on the JSON / RawBytes producer/consumer
    /// paths where the kind doesn't matter.
    pub fn is_glue(&self) -> bool {
        matches!(self, Self::Glue { .. })
    }
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
            seek_map: Arc::new(std::sync::Mutex::new(HashMap::new())),
            auth: AuthMode::None,
            payload_format: KafkaPayloadFormat::default(),
            // Default "earliest" preserves the prior hard-coded
            // behavior — a fresh consumer with no committed offsets
            // sees pre-existing topic content.
            auto_offset_reset: Some("earliest".to_string()),
            delivery_semantics: KafkaDeliverySemantics::default(),
            producer_session: Arc::new(Mutex::new(ProducerSession::default())),
            schema_registry_url: None,
            schema_registry_basic_auth: None,
            schema_registry_kind: SchemaRegistryKind::Confluent,
            glue_schema_cache: Arc::new(RwLock::new(HashMap::new())),
            glue_producer_schema_cache: Arc::new(RwLock::new(HashMap::new())),
            message_key_column: None,
        })
    }

    /// Task #556: configure the Schema Registry wire-format
    /// dispatch. Defaults to [`SchemaRegistryKind::Confluent`]; pass
    /// [`SchemaRegistryKind::Glue`] when reading from a topic that
    /// uses AWS Glue Schema Registry framing.
    ///
    /// Has no effect for `KafkaPayloadFormat::Json` /
    /// `KafkaPayloadFormat::RawBytes` — those formats don't carry an
    /// SR wire frame.
    pub fn with_schema_registry_kind(mut self, kind: SchemaRegistryKind) -> Self {
        self.schema_registry_kind = kind;
        self
    }

    /// Borrow the active schema-registry kind. Returns
    /// [`SchemaRegistryKind::Confluent`] when unset.
    pub fn schema_registry_kind(&self) -> &SchemaRegistryKind {
        &self.schema_registry_kind
    }

    /// Phase 40.2: configure a per-row message-key column. When
    /// set, `write_arrow_stream` extracts that column from each
    /// `RecordBatch` and uses each value as the Kafka message
    /// key — letting the broker route messages predictably to
    /// partitions (downstream partition affinity, ordering by
    /// key, log compaction).
    ///
    /// Supported column types: `Utf8`, `LargeUtf8`, `Binary`.
    /// Other types raise at produce time.
    pub fn with_message_key_column(mut self, column: impl Into<String>) -> Self {
        self.message_key_column = Some(column.into());
        self
    }

    /// Borrow the configured message-key column name (if any).
    pub fn message_key_column(&self) -> Option<&str> {
        self.message_key_column.as_deref()
    }

    /// Configure the Confluent Schema Registry URL used for Avro
    /// (36h.3) and Protobuf (36h.5) payload formats. The URL is the
    /// SR REST endpoint, e.g. `http://localhost:8081` or
    /// `https://psrc-xxxxx.us-east-2.aws.confluent.cloud`. Pair with
    /// [`Self::with_schema_registry_basic_auth`] for SRs that
    /// require authentication (Confluent Cloud, etc.).
    pub fn with_schema_registry_url(mut self, url: impl Into<String>) -> Self {
        self.schema_registry_url = Some(url.into());
        self
    }

    /// Borrow the configured Schema Registry URL.
    pub fn schema_registry_url(&self) -> Option<&str> {
        self.schema_registry_url.as_deref()
    }

    /// Π.1 follow-up: configure HTTP Basic auth on the Schema
    /// Registry client. Confluent Cloud's API-key / API-secret pair
    /// goes here. The credentials are passed to
    /// `SrSettings::set_basic_authorization` whenever the framework
    /// constructs an SR client (Avro / Protobuf encode + decode
    /// paths).
    pub fn with_schema_registry_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.schema_registry_basic_auth = Some(SrBasicAuth {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    /// Borrow the configured SR basic-auth credentials. Tests +
    /// introspection only; production code path goes through
    /// `Self::sr_auth()`.
    pub fn schema_registry_basic_auth(&self) -> Option<(&str, &str)> {
        self.schema_registry_basic_auth
            .as_ref()
            .map(|a| (a.username.as_str(), a.password.as_str()))
    }

    /// Build the `SrAuth` value the helper functions consume. Errors
    /// when called without a configured SR URL — callers should
    /// only invoke once they've validated the payload format
    /// requires SR.
    pub(crate) fn sr_auth(&self) -> Result<SrAuth, BackendError> {
        let url = self.schema_registry_url.as_ref().ok_or_else(|| {
            BackendError::Other(
                "internal: sr_auth() called without a configured \
                 schema_registry_url"
                    .into(),
            )
        })?;
        Ok(SrAuth {
            url: url.clone(),
            basic_auth: self
                .schema_registry_basic_auth
                .as_ref()
                .map(|a| (a.username.clone(), a.password.clone())),
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

    /// Override the consumer-side `auto.offset.reset` knob.
    /// Accepts `"earliest"` / `"latest"` per rdkafka semantics.
    pub fn with_auto_offset_reset(mut self, reset: impl Into<String>) -> Self {
        self.auto_offset_reset = Some(reset.into());
        self
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
                if self.schema_registry_url.is_none() {
                    return Err(BackendError::Other(
                        "Kafka write_arrow_stream_eos Avro: schema_registry_url is \
                         required (call `with_schema_registry_url(...)` on the \
                         backend before writing)"
                            .into(),
                    ));
                }
            }
            KafkaPayloadFormat::Protobuf => {
                if self.schema_registry_url.is_none() {
                    return Err(BackendError::Other(
                        "Kafka write_arrow_stream_eos Protobuf: schema_registry_url is \
                         required (call `with_schema_registry_url(...)` on the \
                         backend before writing)"
                            .into(),
                    ));
                }
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
        let mut all_keys: Vec<Vec<u8>> = Vec::new();
        let key_col = self.message_key_column.clone();
        while let Some(batch) = futures_util::StreamExt::next(&mut s).await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            // Phase 40.2: pull keys before encoding so we error out
            // on a missing/wrong-typed column without having paid
            // the encode cost.
            if let Some(col) = &key_col {
                let mut keys = extract_message_keys(&batch, col)?;
                all_keys.append(&mut keys);
            }
            let mut payloads = match self.payload_format {
                KafkaPayloadFormat::Json => encode_batch_as_jsonl_lines(&batch)?,
                KafkaPayloadFormat::RawBytes => encode_batch_as_raw_bytes(&batch)?,
                KafkaPayloadFormat::Avro => {
                    encode_batch_as_avro(&batch, topic, &self.sr_auth()?).await?
                }
                KafkaPayloadFormat::Protobuf => {
                    encode_batch_as_protobuf(&batch, topic, &self.sr_auth()?).await?
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
        let keys_slice = if key_col.is_some() {
            Some(all_keys.as_slice())
        } else {
            None
        };
        let result = async {
            let total =
                produce_payloads_to_topic(&producer, topic, &all_payloads, keys_slice).await?;
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
            // `auto.offset.reset` honors the builder override (Π.W
            // demo-09 ergonomics); defaults to `"earliest"` so a
            // fresh consumer with no committed offsets starts from
            // the beginning. Producers that need post-subscribe-only
            // semantics set `"latest"` via `with_auto_offset_reset`.
            let mut config = self.client_config();
            if let Some(ref reset) = self.auto_offset_reset {
                config.set("auto.offset.reset", reset.as_str());
            }
            let context = self.build_context();
            let consumer: StreamConsumer<EmatixKafkaContext> = config
                .create_with_context(context)
                .map_err(|e| BackendError::Connection(format!("kafka consumer create: {e}")))?;
            // P4 #26: always `subscribe()` — when `seek_to` has
            // populated `self.seek_map`, the initial assign-all
            // rebalance fires `EmatixKafkaContext::post_rebalance`
            // which seeks each partition to its recovered offset
            // before the consumer's first poll. New partitions not
            // in the seek_map (e.g. broker added a partition) fall
            // through to `auto.offset.reset = earliest`.
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
            seek_map: Arc::clone(&self.seek_map),
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

    fn config(&self) -> crate::backend::BackendConfig {
        // Σ.B follow-up: full builder-state round-trip. Every
        // configurable knob lands in the BackendConfig — auth,
        // payload format, SR config, delivery semantics, message-
        // key column, batch tuning. The internal AuthMode enum is
        // mapped to the public KafkaAuthConfig mirror via the
        // From impl below.
        crate::backend::BackendConfig::Kafka(crate::backend::KafkaConfig {
            bootstrap_servers: self.bootstrap_servers.clone(),
            group_id: self.group_id.clone(),
            auth: Some((&self.auth).into()),
            payload_format: Some(self.payload_format),
            delivery_semantics: Some(self.delivery_semantics.clone()),
            schema_registry_url: self.schema_registry_url.clone(),
            schema_registry_basic_auth: self.schema_registry_basic_auth.clone(),
            schema_registry_kind: match &self.schema_registry_kind {
                SchemaRegistryKind::Confluent => None,
                glue => Some(glue.clone()),
            },
            message_key_column: self.message_key_column.clone(),
            batch_config: Some(self.batch_config),
        })
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
        // Validate format-specific prerequisites *before* opening
        // the consumer. Saves a broker round-trip + makes the
        // rejection paths testable without Docker.
        match self.payload_format {
            KafkaPayloadFormat::Json | KafkaPayloadFormat::RawBytes => {}
            KafkaPayloadFormat::Avro => {
                // Avro needs *some* SR — either a Confluent URL or a
                // Glue registry. The Glue variant carries its own
                // dispatch state on `schema_registry_kind`; the URL
                // is the Confluent prerequisite.
                if !self.schema_registry_kind.is_glue() && self.schema_registry_url.is_none() {
                    return Err(BackendError::Other(
                        "Kafka read_arrow_stream Avro: schema_registry_url is \
                         required (call `with_schema_registry_url(...)` on the \
                         backend before reading, or pass \
                         `with_schema_registry_kind(SchemaRegistryKind::Glue { .. })`)"
                            .into(),
                    ));
                }
            }
            KafkaPayloadFormat::Protobuf => {
                if self.schema_registry_url.is_none() {
                    return Err(BackendError::Other(
                        "Kafka read_arrow_stream Protobuf: schema_registry_url \
                         is required (call `with_schema_registry_url(...)` on \
                         the backend before reading)"
                            .into(),
                    ));
                }
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
            KafkaPayloadFormat::Avro => match &self.schema_registry_kind {
                SchemaRegistryKind::Confluent => {
                    // schema_registry_url presence already validated above.
                    decode_payloads_as_avro(payloads, &self.sr_auth()?).await?
                }
                SchemaRegistryKind::Glue {
                    region,
                    registry_name,
                    schema_lookup_callback,
                    ..
                } => {
                    // Glue path: schema fetched via the named callback
                    // (typically the Python boto3 wrapper). Cache
                    // lives on the backend so a hot topic only pays
                    // the network round-trip once per UUID.
                    decode_payloads_as_glue_avro(
                        payloads,
                        region,
                        registry_name,
                        schema_lookup_callback,
                        &self.glue_schema_cache,
                    )?
                }
            },
            KafkaPayloadFormat::Protobuf => {
                // schema_registry_url presence already validated above.
                decode_payloads_as_protobuf(payloads, &self.sr_auth()?).await?
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
                // Same gate as the consumer side: either Confluent SR
                // URL or a Glue variant with a producer callback set.
                match &self.schema_registry_kind {
                    SchemaRegistryKind::Confluent => {
                        if self.schema_registry_url.is_none() {
                            return Err(BackendError::Other(
                                "Kafka write_arrow_stream Avro: schema_registry_url is \
                                 required (call `with_schema_registry_url(...)` on the \
                                 backend before writing, or use \
                                 `with_schema_registry_kind(SchemaRegistryKind::Glue { .. })`)"
                                    .into(),
                            ));
                        }
                    }
                    SchemaRegistryKind::Glue {
                        schema_lookup_by_name_callback,
                        ..
                    } => {
                        if schema_lookup_by_name_callback.is_empty() {
                            return Err(BackendError::Other(
                                "Kafka write_arrow_stream Avro: Glue producer path \
                                 needs schema_lookup_by_name_callback set on the \
                                 SchemaRegistryKind::Glue variant"
                                    .into(),
                            ));
                        }
                    }
                }
            }
            KafkaPayloadFormat::Protobuf => {
                if self.schema_registry_url.is_none() {
                    return Err(BackendError::Other(
                        "Kafka write_arrow_stream Protobuf: schema_registry_url is \
                         required (call `with_schema_registry_url(...)` on the \
                         backend before writing)"
                            .into(),
                    ));
                }
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
            // Phase 40.2: extract per-row message keys before
            // encoding so a missing column errors out cheaply.
            let keys: Option<Vec<Vec<u8>>> = match &self.message_key_column {
                Some(col) => Some(extract_message_keys(&batch, col)?),
                None => None,
            };
            let payloads = match self.payload_format {
                KafkaPayloadFormat::Json => encode_batch_as_jsonl_lines(&batch)?,
                KafkaPayloadFormat::RawBytes => encode_batch_as_raw_bytes(&batch)?,
                KafkaPayloadFormat::Avro => match &self.schema_registry_kind {
                    SchemaRegistryKind::Confluent => {
                        encode_batch_as_avro(&batch, topic, &self.sr_auth()?).await?
                    }
                    SchemaRegistryKind::Glue {
                        region,
                        registry_name,
                        schema_lookup_by_name_callback,
                        ..
                    } => {
                        // Topic name doubles as the Glue schema name —
                        // matches the Confluent convention of
                        // "<topic>-value" subject. Users wanting a
                        // different schema name today can manage it
                        // outside this surface; a per-pipeline
                        // override field is a future enhancement.
                        encode_batch_as_glue_avro(
                            &batch,
                            region,
                            registry_name,
                            topic,
                            schema_lookup_by_name_callback,
                            &self.glue_producer_schema_cache,
                        )?
                    }
                },
                KafkaPayloadFormat::Protobuf => {
                    // schema_registry_url presence already validated above.
                    encode_batch_as_protobuf(&batch, topic, &self.sr_auth()?).await?
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
            let produce_result =
                produce_payloads_to_topic(&producer, topic, &payloads, keys.as_deref()).await;
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

    fn supports_seek_to(&self) -> bool {
        true
    }

    /// Phase 39.5a: stash a per-partition seek map decoded from
    /// `offset_bytes`. The next `read_arrow_stream` call (which
    /// drives `acquire_consumer_for`) sees this and uses
    /// `assign + seek` instead of `subscribe`, restoring exact
    /// per-partition positions from a `StateStore`-recovered offset.
    ///
    /// Calling `seek_to` on an already-active consumer drops the
    /// consumer so the next read re-creates it with the new
    /// positions.
    async fn seek_to(&self, offset_bytes: &[u8]) -> Result<(), BackendError> {
        let parsed = decode_kafka_offsets(offset_bytes)?;
        // P4 #26: write the recovered offsets into the shared
        // `seek_map` that every `EmatixKafkaContext` we build
        // references. The next consumer's initial post_rebalance
        // (or any subsequent rebalance — Σ.D multi-worker) reads
        // from this map and seeks the assigned partitions.
        {
            let mut map = self
                .seek_map
                .lock()
                .map_err(|e| BackendError::Other(format!("kafka seek_map lock: {e}")))?;
            map.clear();
            map.extend(parsed);
        }
        // Drop any active consumer — its assignment + offset state
        // is stale relative to the recovered offsets. The next
        // `acquire_consumer_for` rebuilds with a context that
        // references the just-populated map; the broker-driven
        // initial rebalance triggers post_rebalance and the seek.
        let mut session = self
            .consumer_session
            .lock()
            .map_err(|e| BackendError::Other(format!("kafka consumer lock: {e}")))?;
        session.consumer = None;
        session.subscribed_topic = None;
        session.pending_offsets.clear();
        Ok(())
    }

    /// Phase 39.5a: hand the current per-partition pending offsets
    /// back as JSON-encoded bytes for `StateStore.commit`. Returns
    /// `None` when no offsets have advanced since startup or the
    /// last commit — caller skips this source from the snapshot.
    async fn offset_snapshot(&self) -> Result<Option<Vec<u8>>, BackendError> {
        let session = self
            .consumer_session
            .lock()
            .map_err(|e| BackendError::Other(format!("kafka consumer lock: {e}")))?;
        if session.pending_offsets.is_empty() {
            return Ok(None);
        }
        let bytes = encode_kafka_offsets(&session.pending_offsets)?;
        Ok(Some(bytes))
    }
}

// =====================================================================
// Phase 39.5a: opaque offset encoding for `StateStore`.
//
// State-store consumers see `Vec<u8>`; the format below is internal
// to this backend. JSON keeps the bytes debuggable in the database
// (e.g., `bytea_to_text` in psql) at negligible size cost — a typical
// session pipeline tracks O(10) partitions.
// =====================================================================

#[derive(serde::Serialize, serde::Deserialize)]
struct KafkaOffsetSnapshotV1 {
    /// Schema discriminator. Kept distinct from `state_store`'s
    /// `state_version` because the offset payload is owned by the
    /// source backend, not the windowed transform.
    v: u32,
    /// `partition → next-offset-to-consume`. Same convention as
    /// Kafka's commit protocol (last-consumed + 1).
    partitions: std::collections::BTreeMap<i32, i64>,
}

pub(crate) fn encode_kafka_offsets(offsets: &HashMap<i32, i64>) -> Result<Vec<u8>, BackendError> {
    let snap = KafkaOffsetSnapshotV1 {
        v: 1,
        partitions: offsets.iter().map(|(p, o)| (*p, *o)).collect(),
    };
    serde_json::to_vec(&snap).map_err(|e| BackendError::Other(format!("kafka offset encode: {e}")))
}

pub(crate) fn decode_kafka_offsets(bytes: &[u8]) -> Result<HashMap<i32, i64>, BackendError> {
    let snap: KafkaOffsetSnapshotV1 = serde_json::from_slice(bytes)
        .map_err(|e| BackendError::Other(format!("kafka offset decode: {e}")))?;
    if snap.v != 1 {
        return Err(BackendError::Other(format!(
            "kafka offset payload v={} not supported (this build understands v=1)",
            snap.v
        )));
    }
    Ok(snap.partitions.into_iter().collect())
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
pub fn decode_payloads_as_jsonl(payloads: Vec<Vec<u8>>) -> Result<Vec<RecordBatch>, BackendError> {
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

/// Decode a Vec of message payloads under `KafkaPayloadFormat::Avro`.
/// Confluent wire format: `0x00` magic byte + 4-byte BE schema id +
/// Avro single-object body. The schema is fetched from the Schema
/// Registry by id (the SR client caches per-id internally so a hot
/// topic only pays the lookup once).
///
/// Decoded `apache_avro::Value`s are converted to `serde_json::Value`
/// and concatenated as JSONL bytes; we then route through the
/// existing `decode_payloads_as_jsonl` path so Arrow schema
/// inference + RecordBatch construction is shared with the JSON
/// payload path. The trade-off: Avro `long` → Arrow Int64 (matches
/// JSON path), but Avro Decimal / Fixed types lose strict typing
/// (round-trip via stringification). Strict-typed Avro→Arrow with
/// schema-driven Arrow builders is a follow-up.
/// Π.1 follow-up: SR basic-auth credentials with a Debug impl that
/// redacts the password. Wrapper around `(username, password)` so
/// the redaction is automatic anywhere this lands inside a
/// `#[derive(Debug)]` struct.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SrBasicAuth {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for SrBasicAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SrBasicAuth")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Π.1 follow-up: bundle the SR URL with optional HTTP Basic auth
/// so the four SR helper functions take a single value rather than
/// growing twin `Option<&str>` parameters at every signature.
/// Construct via `KafkaBackend::sr_auth()`.
#[derive(Debug, Clone)]
pub struct SrAuth {
    /// SR REST endpoint, e.g. `http://localhost:8081`.
    pub url: String,
    /// Optional `(username, password)`; Confluent Cloud uses
    /// (API-key, API-secret).
    pub basic_auth: Option<(String, String)>,
}

impl SrAuth {
    /// Convenience for tests + non-Backend callers.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            basic_auth: None,
        }
    }

    /// Build the `SrSettings` consumed by the schema_registry_converter
    /// crate. Falls back to `SrSettings::new(url)` when no basic auth
    /// is set so the unauthenticated path stays a single allocation.
    pub(crate) fn build_sr_settings(
        &self,
    ) -> Result<schema_registry_converter::async_impl::schema_registry::SrSettings, BackendError>
    {
        use schema_registry_converter::async_impl::schema_registry::SrSettings;
        if let Some((user, password)) = &self.basic_auth {
            SrSettings::new_builder(self.url.clone())
                .set_basic_authorization(user, Some(password))
                .build()
                .map_err(|e| BackendError::Other(format!("sr settings build: {e}")))
        } else {
            Ok(SrSettings::new(self.url.clone()))
        }
    }
}

pub async fn decode_payloads_as_avro(
    payloads: Vec<Vec<u8>>,
    sr: &SrAuth,
) -> Result<Vec<RecordBatch>, BackendError> {
    use schema_registry_converter::async_impl::easy_avro::EasyAvroDecoder;

    let sr_settings = sr.build_sr_settings()?;
    let decoder = EasyAvroDecoder::new(sr_settings);

    let mut json_payloads: Vec<Vec<u8>> = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let decoded = decoder
            .decode(Some(&payload))
            .await
            .map_err(|e| BackendError::Query(format!("kafka avro decode: {e}")))?;
        let json = avro_value_to_json(&decoded.value);
        let bytes = serde_json::to_vec(&json)
            .map_err(|e| BackendError::Other(format!("avro→json serialize: {e}")))?;
        json_payloads.push(bytes);
    }
    decode_payloads_as_jsonl(json_payloads)
}

/// Producer-side counterpart to [`decode_payloads_as_glue_avro`].
/// Renders an Arrow batch as JSONL, parses each line, and for each
/// row encodes an Avro single-object body, then wraps in the Glue
/// frame (header byte + UUID + codec).
///
/// The schema UUID + Avro text come from the named ``by-name``
/// callback (typically ``ematix_flow.glue_schema_registry.fetch_schema_by_name``).
/// The first send pays a network round-trip; subsequent sends use
/// the same cached schema.
///
/// Compression is always ``GlueCodec::None`` on the produce side —
/// the wire frame's codec byte is informational on read but
/// re-compressing every payload would burn producer CPU for marginal
/// network savings. Users who care can set it on the AWS SDK side.
pub fn encode_batch_as_glue_avro(
    batch: &arrow_array::RecordBatch,
    region: &str,
    registry_name: &str,
    schema_name: &str,
    callback_name: &str,
    cache: &Arc<RwLock<HashMap<String, Arc<(uuid::Uuid, apache_avro::Schema)>>>>,
) -> Result<Vec<Vec<u8>>, BackendError> {
    use crate::glue_schema_registry::{GlueCodec, build_glue_frame};

    // ---- Resolve schema (cache-first) -------------------------------
    let schema_arc: Arc<(uuid::Uuid, apache_avro::Schema)> = {
        let cache_r = cache.read().map_err(|_| {
            BackendError::Other("glue producer schema cache read lock poisoned".into())
        })?;
        cache_r.get(schema_name).cloned()
    }
    .ok_or(())
    .or_else(
        |_| -> Result<Arc<(uuid::Uuid, apache_avro::Schema)>, BackendError> {
            #[derive(serde::Serialize)]
            struct ByNameRequest<'a> {
                schema_name: &'a str,
                registry_name: &'a str,
                region: &'a str,
            }
            let req = ByNameRequest {
                schema_name,
                registry_name,
                region,
            };
            let req_bytes = serde_json::to_vec(&req)
                .map_err(|e| BackendError::Other(format!("glue by-name request serialize: {e}")))?;
            let resp_bytes = crate::py_callbacks::global()
                .invoke(callback_name, &req_bytes)
                .map_err(|e| BackendError::Query(format!("glue by-name schema lookup: {e}")))?;
            let resp: GlueSchemaResponse = serde_json::from_slice(&resp_bytes)
                .map_err(|e| BackendError::Other(format!("glue by-name response parse: {e}")))?;
            if resp.data_format != "AVRO" {
                return Err(BackendError::Query(format!(
                    "glue schema {:?} has data_format={:?}; only AVRO is wired \
                 on the producer path today",
                    schema_name, resp.data_format,
                )));
            }
            let uuid = uuid::Uuid::parse_str(&resp.schema_uuid).map_err(|e| {
                BackendError::Other(format!(
                    "glue by-name response: invalid SchemaVersionId UUID {:?}: {e}",
                    resp.schema_uuid,
                ))
            })?;
            let parsed = apache_avro::Schema::parse_str(&resp.schema_definition).map_err(|e| {
                BackendError::Query(format!("glue avro schema parse for {:?}: {e}", schema_name,))
            })?;
            let arc = Arc::new((uuid, parsed));
            let mut cache_w = cache.write().map_err(|_| {
                BackendError::Other("glue producer schema cache write lock poisoned".into())
            })?;
            cache_w.insert(schema_name.to_string(), arc.clone());
            Ok(arc)
        },
    )?;
    let (schema_uuid, schema) = schema_arc.as_ref();

    // ---- Render batch as JSONL → serde_json::Value rows -------------
    let mut buf: Vec<u8> = Vec::with_capacity(batch.num_rows() * 64);
    {
        let mut writer = arrow_json::LineDelimitedWriter::new(&mut buf);
        writer.write(batch).map_err(|e| {
            BackendError::Query(format!("kafka glue avro encode (json render): {e}"))
        })?;
        writer.finish().map_err(|e| {
            BackendError::Query(format!("kafka glue avro encode (json finish): {e}"))
        })?;
    }

    let mut out: Vec<Vec<u8>> = Vec::with_capacity(batch.num_rows());
    for line in buf.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
        let json: serde_json::Value = serde_json::from_slice(line).map_err(|e| {
            BackendError::Query(format!("kafka glue avro encode (json parse): {e}"))
        })?;
        // Convert serde_json::Value → apache_avro::types::Value with
        // the schema driving the conversion. apache_avro's generic
        // to_value produces a Map for a JSON object, which doesn't
        // satisfy a Record schema; we walk the schema field-by-field
        // instead. Errors here surface as "expected X, got Y" — much
        // more actionable than the downstream "Value does not match
        // schema" the datum encoder would produce.
        let avro_value = json_to_avro_value(&json, schema).map_err(|e| {
            BackendError::Query(format!("kafka glue avro encode (json→avro value): {e}",))
        })?;
        let avro_bytes = apache_avro::to_avro_datum(schema, avro_value)
            .map_err(|e| BackendError::Query(format!("kafka glue avro encode (datum): {e}")))?;
        out.push(build_glue_frame(*schema_uuid, GlueCodec::None, &avro_bytes));
    }
    Ok(out)
}

/// Convert a ``serde_json::Value`` into an
/// ``apache_avro::types::Value`` driven by an Avro schema. The
/// schema picks the concrete Avro type for each piece (Int vs Long,
/// String vs Bytes, etc.); ``apache_avro::to_value`` alone produces
/// a generic ``Map`` for objects which doesn't satisfy a Record
/// schema at datum-encode time.
///
/// Coverage today:
/// * Primitives: null, bool, int/long (from JSON Number), float/double,
///   string (also for date/timestamp logical types — Avro encodes the
///   epoch count).
/// * Records: walks fields in schema order, missing fields → Null.
/// * Unions: picks the first variant the value satisfies (handles
///   the common ``union { null, X }`` nullable idiom).
/// * Arrays: maps element-by-element using the item schema.
///
/// Not yet supported: maps, enums, fixed, decimal, named-type
/// references. Surfaces a clear ``"unsupported schema"`` error so
/// the caller knows which field is the blocker.
fn json_to_avro_value(
    json: &serde_json::Value,
    schema: &apache_avro::Schema,
) -> Result<apache_avro::types::Value, String> {
    use apache_avro::Schema as S;
    use apache_avro::types::Value as V;
    use serde_json::Value as J;

    // Unions: try variants in declared order. The standard ``[null, X]``
    // idiom requires that we accept a JSON null against the Null
    // branch even though apache_avro::types::Value::Union expects an
    // index + inner value.
    if let S::Union(union_schema) = schema {
        for (idx, variant) in union_schema.variants().iter().enumerate() {
            if matches!(variant, S::Null) && matches!(json, J::Null) {
                return Ok(V::Union(idx as u32, Box::new(V::Null)));
            }
            if !matches!(variant, S::Null) && !matches!(json, J::Null) {
                if let Ok(inner) = json_to_avro_value(json, variant) {
                    return Ok(V::Union(idx as u32, Box::new(inner)));
                }
            }
        }
        return Err(format!("no matching union variant for value {json:?}",));
    }

    match (schema, json) {
        (S::Null, J::Null) => Ok(V::Null),
        (S::Boolean, J::Bool(b)) => Ok(V::Boolean(*b)),
        (S::Int, J::Number(n)) => {
            let i = n
                .as_i64()
                .ok_or_else(|| format!("Avro Int field got non-integer JSON number: {n}"))?;
            Ok(V::Int(i as i32))
        }
        (S::Long, J::Number(n)) => {
            let i = n
                .as_i64()
                .ok_or_else(|| format!("Avro Long field got non-integer JSON number: {n}"))?;
            Ok(V::Long(i))
        }
        (S::Float, J::Number(n)) => {
            let f = n
                .as_f64()
                .ok_or_else(|| format!("Avro Float field got non-numeric JSON: {n}"))?;
            Ok(V::Float(f as f32))
        }
        (S::Double, J::Number(n)) => {
            let f = n
                .as_f64()
                .ok_or_else(|| format!("Avro Double field got non-numeric JSON: {n}"))?;
            Ok(V::Double(f))
        }
        (S::String, J::String(s)) => Ok(V::String(s.clone())),
        (S::Bytes, J::String(s)) => Ok(V::Bytes(s.as_bytes().to_vec())),
        (S::Array(item_schema), J::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(json_to_avro_value(item, &item_schema.items)?);
            }
            Ok(V::Array(out))
        }
        (S::Record(record_schema), J::Object(obj)) => {
            let mut fields = Vec::with_capacity(record_schema.fields.len());
            for field in &record_schema.fields {
                let json_val = obj.get(&field.name).unwrap_or(&J::Null);
                let avro_val = json_to_avro_value(json_val, &field.schema)?;
                fields.push((field.name.clone(), avro_val));
            }
            Ok(V::Record(fields))
        }
        _ => Err(format!(
            "unsupported (schema, json) shape during Glue producer \
             encode: schema={schema:?}, json={json:?}",
        )),
    }
}

/// Decompress a zlib-compressed Glue payload. Matches the AWS Glue
/// SerDe's ``compression_type=ZLIB`` setting (codec byte 0x05 in the
/// wire frame). Uses ``flate2::read::ZlibDecoder`` so the dep is
/// shared with parquet's codec path rather than introducing a new
/// transitive crate.
fn decompress_glue_zlib(payload: &[u8]) -> Result<Vec<u8>, BackendError> {
    use std::io::Read;
    let mut decoder = flate2::read::ZlibDecoder::new(payload);
    // Avro single-object bodies are typically 2-4x larger than their
    // zlib form; size the buffer accordingly to avoid a re-alloc
    // sweep on the hot path.
    let mut out = Vec::with_capacity(payload.len() * 3);
    decoder
        .read_to_end(&mut out)
        .map_err(|e| BackendError::Query(format!("kafka glue avro: zlib decode failed: {e}",)))?;
    Ok(out)
}

/// Convert an `apache_avro::types::Value` into a `serde_json::Value`.
/// Lossy on logical types: Decimal / Duration / Fixed become
/// strings; date/time variants become numeric epoch counts; Records
/// become JSON objects, Arrays become JSON arrays, Unions unwrap to
/// their inner value (so a `union { null, X }` becomes null or X —
/// the standard Avro nullable-field idiom). Strict logical-type
/// preservation is a future-phase enhancement (would build
/// per-type Arrow arrays directly rather than going through JSON).
fn avro_value_to_json(value: &apache_avro::types::Value) -> serde_json::Value {
    use apache_avro::types::Value as Av;
    use serde_json::Value as Js;
    use serde_json::json;

    match value {
        Av::Null => Js::Null,
        Av::Boolean(b) => Js::Bool(*b),
        Av::Int(i) => json!(*i),
        Av::Long(i) => json!(*i),
        Av::Float(f) => json!(*f),
        Av::Double(f) => json!(*f),
        Av::Bytes(b) | Av::Fixed(_, b) => {
            // Lowercase hex — keeps the dep surface tight (no extra
            // base64 crate). Users wanting canonical Avro JSON can
            // post-process; the data is still recoverable.
            Js::String(b.iter().map(|byte| format!("{:02x}", byte)).collect())
        }
        Av::String(s) | Av::Enum(_, s) => Js::String(s.clone()),
        Av::Union(_idx, inner) => avro_value_to_json(inner),
        Av::Array(items) => Js::Array(items.iter().map(avro_value_to_json).collect()),
        Av::Map(m) => Js::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect(),
        ),
        Av::Record(fields) => Js::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect(),
        ),
        Av::Date(d) => json!(*d),
        Av::Decimal(d) => Js::String(format!("{:?}", d)),
        Av::TimeMillis(t) => json!(*t),
        Av::TimeMicros(t) => json!(*t),
        Av::TimestampMillis(t)
        | Av::TimestampMicros(t)
        | Av::TimestampNanos(t)
        | Av::LocalTimestampMillis(t)
        | Av::LocalTimestampMicros(t)
        | Av::LocalTimestampNanos(t) => json!(*t),
        Av::Duration(d) => Js::String(format!("{:?}", d)),
        Av::Uuid(u) => Js::String(u.to_string()),
        other => Js::String(format!("{:?}", other)),
    }
}

/// Decode a Vec of message payloads under `KafkaPayloadFormat::Protobuf`.
/// Confluent wire format: `0x00` magic byte + 4-byte BE schema id +
/// message-index varint(s) + Protobuf body. The schema (.proto file
/// content) is fetched from the Schema Registry by id; the SR
/// converter caches per-id internally so a hot topic only pays the
/// lookup once.
///
/// Task #556: schema-fetch request payload sent from Rust to the
/// Python callback that wraps boto3's `GetSchemaVersion`. JSON-
/// encoded so it round-trips through the opaque `&[u8]` callback
/// boundary in [`crate::py_callbacks`].
///
/// The response shape ([`GlueSchemaResponse`]) carries the schema
/// text + data format the Python side returned.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GlueSchemaRequest {
    /// Schema-version UUID parsed from the Kafka message frame.
    pub schema_uuid: String,
    /// AWS region the Glue registry lives in. Mirrored from the
    /// connection so the callback can target the right endpoint.
    pub region: String,
    /// Glue registry name. The lookup keys off `schema_uuid` but the
    /// callback can record the registry name for audit / metrics.
    pub registry_name: String,
}

/// Response shape from the Python Glue schema lookup. The Rust side
/// converts `schema_definition` into a parsed
/// [`apache_avro::Schema`] (for Avro payloads); future Protobuf
/// support would similarly parse `schema_definition` as a `.proto`
/// source via `protofish`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GlueSchemaResponse {
    /// "AVRO", "PROTOBUF", or "JSON". Mirrors the boto3
    /// `DataFormat` field.
    pub data_format: String,
    /// The schema text — Avro JSON for AVRO, `.proto` source for
    /// PROTOBUF, JSON Schema for JSON.
    pub schema_definition: String,
    /// Echoed back from the request so the cache key matches.
    pub schema_uuid: String,
}

/// Decode a Vec of message payloads using AWS Glue Schema Registry
/// framing.
///
/// Wire format: `0x03` + 16-byte UUID + 1-byte codec + payload.
/// The schema corresponding to the UUID is fetched once per UUID
/// via the [`crate::py_callbacks`] registry; subsequent messages
/// using the same UUID are served out of `cache`.
///
/// The codec byte chooses how to decompress the payload before
/// handing it to `apache_avro::from_avro_datum`:
/// - [`GlueCodec::None`] — bytes are uncompressed Avro single-object.
/// - [`GlueCodec::Zlib`] — Glue's optional `Z` (zlib) compression.
///
/// Returns the same Arrow shape as
/// [`decode_payloads_as_avro`]: Avro values are converted to
/// `serde_json::Value`, batched as JSONL, and run through the
/// existing Arrow schema-inference path. Strict-typed Avro→Arrow
/// builders are a future enhancement (matches the Confluent path's
/// trade-off).
pub fn decode_payloads_as_glue_avro(
    payloads: Vec<Vec<u8>>,
    region: &str,
    registry_name: &str,
    callback_name: &str,
    cache: &Arc<RwLock<HashMap<uuid::Uuid, Arc<apache_avro::Schema>>>>,
) -> Result<Vec<RecordBatch>, BackendError> {
    use crate::glue_schema_registry::{GlueCodec, parse_glue_frame};

    let mut json_payloads: Vec<Vec<u8>> = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let frame = parse_glue_frame(&payload)
            .map_err(|e| BackendError::Query(format!("kafka glue frame parse: {e}")))?;

        // Decompress the payload if the codec byte says so. The
        // None (0x00) case is what messages produced by the AWS SDK
        // with default settings emit. Zlib (0x05) is what users get
        // when they pass ``compression_type=ZLIB`` to the AWS Glue
        // SerDe; we decode via flate2 so both producer settings
        // round-trip transparently.
        let bytes: Vec<u8> = match frame.codec {
            GlueCodec::None => frame.payload.to_vec(),
            GlueCodec::Zlib => decompress_glue_zlib(frame.payload)?,
        };

        // Cache lookup. The schema cache lives on the KafkaBackend
        // so a hot topic only pays the boto3 round-trip once per
        // schema version (not per message).
        let schema_arc: Arc<apache_avro::Schema> = {
            let cache_r = cache
                .read()
                .map_err(|_| BackendError::Other("glue schema cache read lock poisoned".into()))?;
            cache_r.get(&frame.schema_uuid).cloned()
        }
        .ok_or(())
        .or_else(|_| -> Result<Arc<apache_avro::Schema>, BackendError> {
            // Cache miss — call into Python to resolve.
            let req = GlueSchemaRequest {
                schema_uuid: frame.schema_uuid.to_string(),
                region: region.to_string(),
                registry_name: registry_name.to_string(),
            };
            let req_bytes = serde_json::to_vec(&req)
                .map_err(|e| BackendError::Other(format!("glue lookup request serialize: {e}")))?;
            let resp_bytes = crate::py_callbacks::global()
                .invoke(callback_name, &req_bytes)
                .map_err(|e| BackendError::Query(format!("glue schema lookup: {e}")))?;
            let resp: GlueSchemaResponse = serde_json::from_slice(&resp_bytes)
                .map_err(|e| BackendError::Other(format!("glue lookup response parse: {e}")))?;
            if resp.data_format != "AVRO" {
                return Err(BackendError::Query(format!(
                    "glue schema UUID {} has data_format={:?}; only AVRO is wired \
                     today (Protobuf / JSON Schema are future work)",
                    frame.schema_uuid, resp.data_format,
                )));
            }
            let parsed = apache_avro::Schema::parse_str(&resp.schema_definition).map_err(|e| {
                BackendError::Query(format!(
                    "glue avro schema parse for UUID {}: {e}",
                    frame.schema_uuid,
                ))
            })?;
            let arc = Arc::new(parsed);
            let mut cache_w = cache
                .write()
                .map_err(|_| BackendError::Other("glue schema cache write lock poisoned".into()))?;
            cache_w.insert(frame.schema_uuid, arc.clone());
            Ok(arc)
        })?;

        // Decode the Avro single-object body against the resolved
        // writer schema. We pass the writer schema as both writer and
        // reader — schema-evolution-aware decoding is a future
        // enhancement (would track per-consumer reader schemas).
        let mut cursor = std::io::Cursor::new(&bytes);
        let value = apache_avro::from_avro_datum(&schema_arc, &mut cursor, None)
            .map_err(|e| BackendError::Query(format!("kafka glue avro decode: {e}")))?;
        let json = avro_value_to_json(&value);
        let json_bytes = serde_json::to_vec(&json)
            .map_err(|e| BackendError::Other(format!("glue avro→json serialize: {e}")))?;
        json_payloads.push(json_bytes);
    }
    decode_payloads_as_jsonl(json_payloads)
}

/// Decoded `protofish::decode::MessageValue`s are converted to
/// `serde_json::Value`s using the schema-resolved field names, then
/// concatenated as JSONL bytes; we then route through the existing
/// `decode_payloads_as_jsonl` path so Arrow schema inference +
/// RecordBatch construction is shared with the JSON / Avro decode
/// paths. The trade-off matches the Avro path: Protobuf int32/64 →
/// Arrow Int32/Int64 via JSON numbers, but Protobuf bytes are
/// rendered as lowercase hex strings (no canonical Avro JSON
/// equivalent for proto bytes — pick whichever round-trips cleanly).
/// Strict-typed Protobuf→Arrow is a future follow-up.
pub async fn decode_payloads_as_protobuf(
    payloads: Vec<Vec<u8>>,
    sr: &SrAuth,
) -> Result<Vec<RecordBatch>, BackendError> {
    use schema_registry_converter::async_impl::easy_proto_decoder::EasyProtoDecoder;

    let sr_settings = sr.build_sr_settings()?;
    let decoder = EasyProtoDecoder::new(sr_settings);

    let mut json_payloads: Vec<Vec<u8>> = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let decoded = decoder
            .decode_with_context(Some(&payload))
            .await
            .map_err(|e| BackendError::Query(format!("kafka protobuf decode: {e}")))?;
        let Some(decoded) = decoded else {
            // Null / empty payload — skip.
            continue;
        };
        let json = protofish_message_to_json(&decoded.value, &decoded.context.context);
        let bytes = serde_json::to_vec(&json)
            .map_err(|e| BackendError::Other(format!("protobuf→json serialize: {e}")))?;
        json_payloads.push(bytes);
    }
    if json_payloads.is_empty() {
        return Ok(Vec::new());
    }
    decode_payloads_as_jsonl(json_payloads)
}

/// Convert a `protofish::decode::MessageValue` into a
/// `serde_json::Value::Object`, using the protofish `Context` to
/// resolve field numbers → field names. Repeated fields collapse
/// to JSON arrays (preserving order); singular fields take the last
/// observed value (proto3 last-wins). Lossy on bytes (rendered as
/// lowercase hex) — same trade-off as the Avro path.
fn protofish_message_to_json(
    msg: &protofish::decode::MessageValue,
    context: &protofish::context::Context,
) -> serde_json::Value {
    use protofish::context::Multiplicity;
    use serde_json::{Map, Value as Js};

    let info = context.resolve_message(msg.msg_ref);

    // Group field values by field number (preserves order of first
    // appearance for the iteration below; later we rely on the
    // schema's field declaration order).
    let mut by_number: std::collections::BTreeMap<u64, Vec<&protofish::decode::Value>> =
        std::collections::BTreeMap::new();
    for fv in &msg.fields {
        by_number.entry(fv.number).or_default().push(&fv.value);
    }

    let mut obj: Map<String, Js> = Map::new();
    for field in info.iter_fields() {
        let Some(values) = by_number.get(&field.number) else {
            continue;
        };
        let is_repeated = matches!(
            field.multiplicity,
            Multiplicity::Repeated | Multiplicity::RepeatedPacked
        );
        let json_value = if is_repeated {
            // Flatten any Packed arrays so each scalar is its own
            // JSON entry.
            let mut arr: Vec<Js> = Vec::new();
            for v in values {
                match v {
                    protofish::decode::Value::Packed(packed) => {
                        for item in packed_array_to_json(packed) {
                            arr.push(item);
                        }
                    }
                    other => arr.push(protofish_value_to_json(other, context)),
                }
            }
            Js::Array(arr)
        } else {
            // Singular: proto3 last-wins. Packed inside a singular
            // field shouldn't happen, but if it does we take the
            // first scalar.
            let last = values.last().expect("non-empty by construction");
            match last {
                protofish::decode::Value::Packed(packed) => packed_array_to_json(packed)
                    .into_iter()
                    .next()
                    .unwrap_or(Js::Null),
                other => protofish_value_to_json(other, context),
            }
        };
        obj.insert(field.name.clone(), json_value);
    }
    Js::Object(obj)
}

/// Convert a `protofish::decode::Value` into a `serde_json::Value`.
/// Recurses into nested messages via the supplied `Context`.
fn protofish_value_to_json(
    value: &protofish::decode::Value,
    context: &protofish::context::Context,
) -> serde_json::Value {
    use protofish::decode::Value as Pv;
    use serde_json::{Value as Js, json};

    match value {
        Pv::Double(f) => json!(*f),
        Pv::Float(f) => json!(*f),
        Pv::Int32(i) | Pv::SInt32(i) | Pv::SFixed32(i) => json!(*i),
        Pv::Int64(i) | Pv::SInt64(i) | Pv::SFixed64(i) => json!(*i),
        Pv::UInt32(u) | Pv::Fixed32(u) => json!(*u),
        Pv::UInt64(u) | Pv::Fixed64(u) => json!(*u),
        Pv::Bool(b) => Js::Bool(*b),
        Pv::String(s) => Js::String(s.clone()),
        Pv::Bytes(b) => Js::String(b.iter().map(|byte| format!("{:02x}", byte)).collect()),
        Pv::Message(boxed) => protofish_message_to_json(boxed, context),
        Pv::Enum(e) => {
            // Resolve enum name via context; if not available, fall
            // back to the raw integer value.
            let enum_info = context.resolve_enum(e.enum_ref);
            match enum_info.get_field_by_value(e.value) {
                Some(f) => Js::String(f.name.clone()),
                None => json!(e.value),
            }
        }
        Pv::Packed(p) => Js::Array(packed_array_to_json(p)),
        // Incomplete / Unknown fall through to a debug string —
        // these only appear when the schema is mismatched against
        // the payload, which is an outright error condition that
        // SR's id-based resolution should prevent in practice.
        other => Js::String(format!("{:?}", other)),
    }
}

/// Expand a `protofish::decode::PackedArray` into a Vec of
/// `serde_json::Value` scalars.
fn packed_array_to_json(packed: &protofish::decode::PackedArray) -> Vec<serde_json::Value> {
    use protofish::decode::PackedArray as Pa;
    use serde_json::{Value as Js, json};

    match packed {
        Pa::Double(v) => v.iter().map(|x| json!(*x)).collect(),
        Pa::Float(v) => v.iter().map(|x| json!(*x)).collect(),
        Pa::Int32(v) | Pa::SInt32(v) | Pa::SFixed32(v) => v.iter().map(|x| json!(*x)).collect(),
        Pa::Int64(v) | Pa::SInt64(v) | Pa::SFixed64(v) => v.iter().map(|x| json!(*x)).collect(),
        Pa::UInt32(v) | Pa::Fixed32(v) => v.iter().map(|x| json!(*x)).collect(),
        Pa::UInt64(v) | Pa::Fixed64(v) => v.iter().map(|x| json!(*x)).collect(),
        Pa::Bool(v) => v.iter().map(|x| Js::Bool(*x)).collect(),
    }
}

/// Encode a `RecordBatch` as Confluent-Schema-Registry-framed
/// Protobuf payloads, one payload per row. The .proto schema is
/// fetched from the Schema Registry by subject `<topic>-value`; the
/// subject must already be registered.
///
/// Conversion path: RecordBatch → JSONL → `serde_json::Value` rows →
/// `protofish::decode::MessageValue` (built field-by-field via
/// schema-driven coercion) → `MessageValue::encode(&context)` →
/// frame via `EasyProtoRawEncoder::encode` (which prepends the
/// magic byte + schema id + message-index varint).
///
/// Limits in 36h.6 (intentional, documented):
///   - Single top-level message per schema (Confluent's most common
///     setup). The first top-level message in the .proto file is
///     used; multi-message schemas with non-default message indexes
///     are a follow-up.
///   - Enum fields must be JSON integers (not symbolic names).
///     Round-trip with the decode path requires manually mapping
///     names → integers in the producer pipeline.
///   - Logical types (google.protobuf.Timestamp, Duration, Any) lose
///     the JSON-string hint of the decode path; users must produce
///     them as nested message objects.
pub async fn encode_batch_as_protobuf(
    batch: &RecordBatch,
    topic: &str,
    sr: &SrAuth,
) -> Result<Vec<Vec<u8>>, BackendError> {
    use schema_registry_converter::async_impl::easy_proto_raw::EasyProtoRawEncoder;
    use schema_registry_converter::async_impl::schema_registry::get_schema_by_subject;
    use schema_registry_converter::proto_resolver::MessageResolver;
    use schema_registry_converter::schema_registry_common::SubjectNameStrategy;

    // Render to JSONL once, parse each line into a serde_json::Value.
    let mut buf: Vec<u8> = Vec::with_capacity(batch.num_rows() * 64);
    {
        let mut writer = arrow_json::LineDelimitedWriter::new(&mut buf);
        writer.write(batch).map_err(|e| {
            BackendError::Query(format!("kafka protobuf encode (json render): {e}"))
        })?;
        writer.finish().map_err(|e| {
            BackendError::Query(format!("kafka protobuf encode (json finish): {e}"))
        })?;
    }
    let mut rows: Vec<serde_json::Value> = Vec::with_capacity(batch.num_rows());
    for line in buf.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
        let v: serde_json::Value = serde_json::from_slice(line)
            .map_err(|e| BackendError::Query(format!("kafka protobuf encode (json parse): {e}")))?;
        rows.push(v);
    }

    let sr_settings = sr.build_sr_settings()?;
    let strategy = SubjectNameStrategy::TopicNameStrategy(topic.to_string(), false);

    // Fetch the .proto schema source so we can build MessageValues.
    let registered = get_schema_by_subject(&sr_settings, &strategy)
        .await
        .map_err(|e| BackendError::Query(format!("kafka protobuf schema fetch: {e}")))?;
    let proto_src = registered.schema;

    // Resolve the primary message's full name (index [0] = first
    // top-level message in the .proto file).
    let resolver = MessageResolver::new(&proto_src);
    let full_name = resolver
        .find_name(&[0])
        .ok_or_else(|| {
            BackendError::Query(
                "kafka protobuf encode: schema has no top-level message at index [0]".into(),
            )
        })?
        .as_str()
        .to_string();

    // Parse the schema for protofish encoding.
    let context = protofish::context::Context::parse([&proto_src])
        .map_err(|e| BackendError::Query(format!("kafka protobuf encode (parse schema): {e:?}")))?;
    let msg_info = context.get_message(&full_name).ok_or_else(|| {
        BackendError::Query(format!(
            "kafka protobuf encode: message {full_name} not found in parsed context"
        ))
    })?;

    let encoder = EasyProtoRawEncoder::new(sr_settings);

    let mut out: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
    for row in rows {
        let msg_value = json_to_protofish_message(&row, msg_info, &context)?;
        let proto_bytes = msg_value.encode(&context).to_vec();
        let framed = encoder
            .encode(
                &proto_bytes,
                &full_name,
                SubjectNameStrategy::TopicNameStrategy(topic.to_string(), false),
            )
            .await
            .map_err(|e| BackendError::Query(format!("kafka protobuf frame: {e}")))?;
        out.push(framed);
    }
    Ok(out)
}

/// Build a `protofish::decode::MessageValue` from a JSON object,
/// using `MessageInfo` for field-name → field-number resolution and
/// type coercion. Recurses into nested messages.
fn json_to_protofish_message(
    json: &serde_json::Value,
    msg_info: &protofish::context::MessageInfo,
    ctx: &protofish::context::Context,
) -> Result<protofish::decode::MessageValue, BackendError> {
    use protofish::context::Multiplicity;
    use protofish::decode::{FieldValue, MessageValue};

    let obj = json.as_object().ok_or_else(|| {
        BackendError::Query(format!(
            "kafka protobuf encode: expected JSON object for message {}, got: {json}",
            msg_info.full_name
        ))
    })?;
    let mut fields: Vec<FieldValue> = Vec::new();
    for field in msg_info.iter_fields() {
        let Some(jv) = obj.get(&field.name) else {
            continue;
        };
        if jv.is_null() {
            continue;
        }
        let is_repeated = matches!(
            field.multiplicity,
            Multiplicity::Repeated | Multiplicity::RepeatedPacked
        );
        if is_repeated {
            let arr = jv.as_array().ok_or_else(|| {
                BackendError::Query(format!(
                    "kafka protobuf encode: field {} is repeated; expected JSON array, got: {jv}",
                    field.name
                ))
            })?;
            for elem in arr {
                let v = json_to_protofish_value(elem, &field.field_type, ctx)?;
                fields.push(FieldValue {
                    number: field.number,
                    value: v,
                });
            }
        } else {
            let v = json_to_protofish_value(jv, &field.field_type, ctx)?;
            fields.push(FieldValue {
                number: field.number,
                value: v,
            });
        }
    }
    Ok(MessageValue {
        msg_ref: msg_info.self_ref,
        fields,
        garbage: None,
    })
}

/// Convert a JSON value to a `protofish::decode::Value` matching the
/// supplied `ValueType`. Numeric narrowing (i64 → i32 etc.) is
/// silent — the caller's data is trusted to fit the schema. Hex
/// strings round-trip back into byte fields. Enums must be JSON
/// integers (see helper docs on `encode_batch_as_protobuf` for the
/// reasoning).
fn json_to_protofish_value(
    json: &serde_json::Value,
    vtype: &protofish::context::ValueType,
    ctx: &protofish::context::Context,
) -> Result<protofish::decode::Value, BackendError> {
    use protofish::context::ValueType as Vt;
    use protofish::decode::{EnumValue, Value as Pv};

    let mismatch = |expected: &str| -> BackendError {
        BackendError::Query(format!(
            "kafka protobuf encode: expected {expected}, got JSON: {json}"
        ))
    };

    match vtype {
        Vt::Double => json
            .as_f64()
            .map(Pv::Double)
            .ok_or_else(|| mismatch("number")),
        Vt::Float => json
            .as_f64()
            .map(|f| Pv::Float(f as f32))
            .ok_or_else(|| mismatch("number")),
        Vt::Int32 => json
            .as_i64()
            .map(|i| Pv::Int32(i as i32))
            .ok_or_else(|| mismatch("integer")),
        Vt::Int64 => json
            .as_i64()
            .map(Pv::Int64)
            .ok_or_else(|| mismatch("integer")),
        Vt::UInt32 => json
            .as_u64()
            .map(|u| Pv::UInt32(u as u32))
            .ok_or_else(|| mismatch("unsigned integer")),
        Vt::UInt64 => json
            .as_u64()
            .map(Pv::UInt64)
            .ok_or_else(|| mismatch("unsigned integer")),
        Vt::SInt32 => json
            .as_i64()
            .map(|i| Pv::SInt32(i as i32))
            .ok_or_else(|| mismatch("integer")),
        Vt::SInt64 => json
            .as_i64()
            .map(Pv::SInt64)
            .ok_or_else(|| mismatch("integer")),
        Vt::Fixed32 => json
            .as_u64()
            .map(|u| Pv::Fixed32(u as u32))
            .ok_or_else(|| mismatch("unsigned integer")),
        Vt::Fixed64 => json
            .as_u64()
            .map(Pv::Fixed64)
            .ok_or_else(|| mismatch("unsigned integer")),
        Vt::SFixed32 => json
            .as_i64()
            .map(|i| Pv::SFixed32(i as i32))
            .ok_or_else(|| mismatch("integer")),
        Vt::SFixed64 => json
            .as_i64()
            .map(Pv::SFixed64)
            .ok_or_else(|| mismatch("integer")),
        Vt::Bool => json.as_bool().map(Pv::Bool).ok_or_else(|| mismatch("bool")),
        Vt::String => json
            .as_str()
            .map(|s| Pv::String(s.to_string()))
            .ok_or_else(|| mismatch("string")),
        Vt::Bytes => {
            // Reverse of the decode path: lowercase hex → bytes.
            let s = json.as_str().ok_or_else(|| mismatch("hex string"))?;
            let raw = hex_decode(s).map_err(|e| {
                BackendError::Query(format!("kafka protobuf encode (hex bytes): {e}"))
            })?;
            Ok(Pv::Bytes(bytes::Bytes::from(raw)))
        }
        Vt::Message(mref) => {
            let nested = ctx.resolve_message(*mref);
            let nested_msg = json_to_protofish_message(json, nested, ctx)?;
            Ok(Pv::Message(Box::new(nested_msg)))
        }
        Vt::Enum(eref) => {
            let v = json.as_i64().ok_or_else(|| {
                BackendError::Query(format!(
                    "kafka protobuf encode: enum field requires JSON integer (symbolic \
                     names not yet supported in 36h.6); got: {json}"
                ))
            })?;
            Ok(Pv::Enum(EnumValue {
                enum_ref: *eref,
                value: v,
            }))
        }
    }
}

/// Decode a lowercase-hex string into bytes. Returns Err if the
/// string has odd length or non-hex characters.
fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err(format!("hex string has odd length ({})", s.len()));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(c: u8) -> Result<u8, String> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(format!("non-hex character: {:?}", c as char)),
    }
}

/// Produce each `payload` to `topic` via `producer`, awaiting the
/// broker ack per message. Returns the number of messages
/// successfully produced. Used by both the `AtLeastOnce` (no
/// surrounding transaction) and `ExactlyOnce` (surrounding txn)
/// produce paths in `write_arrow_stream`.
///
/// `keys`: when `Some`, must have one entry per `payloads` entry
/// — each is used as the Kafka message key (Phase 40.2). When
/// `None`, no key is set on the `FutureRecord` and the broker's
/// default sticky partitioner picks a partition.
async fn produce_payloads_to_topic(
    producer: &FutureProducer<EmatixKafkaContext>,
    topic: &str,
    payloads: &[Vec<u8>],
    keys: Option<&[Vec<u8>]>,
) -> Result<u64, BackendError> {
    if let Some(ks) = keys
        && ks.len() != payloads.len()
    {
        return Err(BackendError::Other(format!(
            "kafka produce {topic}: keys.len()={} != payloads.len()={}; \
             internal invariant violation",
            ks.len(),
            payloads.len()
        )));
    }
    let mut total: u64 = 0;
    for (i, payload) in payloads.iter().enumerate() {
        let timeout = Timeout::After(Duration::from_secs(5));
        let send_result = match keys {
            Some(ks) => {
                producer
                    .send(
                        FutureRecord::<[u8], [u8]>::to(topic)
                            .payload(payload.as_slice())
                            .key(ks[i].as_slice()),
                        timeout,
                    )
                    .await
            }
            None => {
                producer
                    .send(
                        FutureRecord::<(), [u8]>::to(topic).payload(payload.as_slice()),
                        timeout,
                    )
                    .await
            }
        };
        send_result
            .map_err(|(e, _msg)| BackendError::Query(format!("kafka produce {topic}: {e}")))?;
        total += 1;
    }
    Ok(total)
}

/// Phase 40.2: extract per-row Kafka message keys from a
/// `RecordBatch` column. Returns one `Vec<u8>` per row, with
/// nulls becoming empty byte arrays (consistent with arrow-json
/// null handling).
///
/// Supported column types: `Utf8`, `LargeUtf8`, `Binary`. Other
/// types raise a `BackendError::Query` with a clear pointer.
fn extract_message_keys(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<Vec<Vec<u8>>, BackendError> {
    use arrow_array::{Array, BinaryArray, LargeStringArray, StringArray};

    let column = batch.column_by_name(column_name).ok_or_else(|| {
        BackendError::Query(format!(
            "kafka produce: message_key_column `{column_name}` not present in batch \
             (batch columns: {:?})",
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        ))
    })?;
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    Vec::new()
                } else {
                    arr.value(i).as_bytes().to_vec()
                }
            })
            .collect());
    }
    if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    Vec::new()
                } else {
                    arr.value(i).as_bytes().to_vec()
                }
            })
            .collect());
    }
    if let Some(arr) = column.as_any().downcast_ref::<BinaryArray>() {
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    Vec::new()
                } else {
                    arr.value(i).to_vec()
                }
            })
            .collect());
    }
    Err(BackendError::Query(format!(
        "kafka produce: message_key_column `{column_name}` has unsupported type \
         {:?}; expected Utf8, LargeUtf8, or Binary",
        column.data_type()
    )))
}

/// Encode a `RecordBatch` as JSONL bytes, then split on newlines so
/// each row becomes its own payload for produce. The `Vec<Vec<u8>>`
/// has one entry per row (matching `batch.num_rows()`).
pub fn encode_batch_as_jsonl_lines(batch: &RecordBatch) -> Result<Vec<Vec<u8>>, BackendError> {
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

/// Encode a `RecordBatch` as Confluent-Schema-Registry-framed Avro
/// payloads, one payload per row. The schema is fetched (and cached)
/// from the Schema Registry by subject `<topic>-value`; the subject
/// must already be registered.
///
/// Conversion path: RecordBatch → JSONL → `serde_json::Value` rows →
/// `EasyAvroEncoder::encode_struct(...)` (which uses apache-avro's
/// serde to coerce the JSON into an Avro `Value` matching the
/// fetched schema, then frames with magic byte + schema id). The
/// JSONL intermediary keeps this path symmetric with the decode path
/// (36h.3) — both share the JSON ↔ Avro lossy round-trip on logical
/// types (Decimal / Duration / Fixed lose strict typing). Strict-
/// typed Arrow→Avro builders (no JSON intermediary) is a future
/// follow-up; the win there is logical-type fidelity, not throughput.
pub async fn encode_batch_as_avro(
    batch: &RecordBatch,
    topic: &str,
    sr: &SrAuth,
) -> Result<Vec<Vec<u8>>, BackendError> {
    use schema_registry_converter::async_impl::easy_avro::EasyAvroEncoder;
    use schema_registry_converter::schema_registry_common::SubjectNameStrategy;

    // Render to JSONL once, parse each line into a serde_json::Value.
    let mut buf: Vec<u8> = Vec::with_capacity(batch.num_rows() * 64);
    {
        let mut writer = arrow_json::LineDelimitedWriter::new(&mut buf);
        writer
            .write(batch)
            .map_err(|e| BackendError::Query(format!("kafka avro encode (json render): {e}")))?;
        writer
            .finish()
            .map_err(|e| BackendError::Query(format!("kafka avro encode (json finish): {e}")))?;
    }
    let mut rows: Vec<serde_json::Value> = Vec::with_capacity(batch.num_rows());
    for line in buf.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
        let v: serde_json::Value = serde_json::from_slice(line)
            .map_err(|e| BackendError::Query(format!("kafka avro encode (json parse): {e}")))?;
        rows.push(v);
    }

    let sr_settings = sr.build_sr_settings()?;
    let encoder = EasyAvroEncoder::new(sr_settings);
    // Subject "<topic>-value" — the standard Confluent default. Key
    // subjects ("<topic>-key") aren't relevant: we don't produce
    // keys yet (round-robin partitioning, see 36c+ doc comment on
    // `with_message_key`).
    let strategy = SubjectNameStrategy::TopicNameStrategy(topic.to_string(), false);

    let mut out: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
    for row in rows {
        let bytes = encoder
            .encode_struct(row, &strategy)
            .await
            .map_err(|e| BackendError::Query(format!("kafka avro encode: {e}")))?;
        out.push(bytes);
    }
    Ok(out)
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

    // ----- Phase 39.5a slice 1.5: seek_to + offset codec -----

    #[test]
    fn kafka_offsets_roundtrip_through_codec() {
        let mut original = HashMap::new();
        original.insert(0_i32, 100_i64);
        original.insert(1, 50);
        original.insert(7, 99_999);

        let bytes = encode_kafka_offsets(&original).unwrap();
        let decoded = decode_kafka_offsets(&bytes).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn decode_rejects_unknown_payload_version() {
        // Hand-roll a v=2 payload — the decoder only knows v=1.
        let payload = br#"{"v":2,"partitions":{"0":1}}"#;
        let err = decode_kafka_offsets(payload).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("v=2"),
            "should mention the unsupported version: {msg}"
        );
    }

    #[test]
    fn decode_rejects_garbage_bytes() {
        let err = decode_kafka_offsets(b"not json").unwrap_err();
        assert!(err.to_string().contains("decode"));
    }

    #[test]
    fn kafka_backend_reports_supports_seek_to() {
        let b = KafkaBackend::open("localhost:9092", Some("g")).unwrap();
        assert!(b.supports_seek_to(), "Kafka must opt in to seek_to");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn offset_snapshot_returns_none_before_any_read() {
        let b = KafkaBackend::open("localhost:9092", Some("g")).unwrap();
        assert!(b.offset_snapshot().await.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn seek_to_populates_shared_seek_map_for_post_rebalance() {
        // No real broker is contacted here — `seek_to` just decodes
        // the payload and writes to the shared `seek_map` Arc that
        // every `EmatixKafkaContext` references. The actual seek
        // happens later from inside `post_rebalance` once a consumer
        // is built and its initial rebalance fires.
        let b = KafkaBackend::open("localhost:9092", Some("g")).unwrap();
        let mut offsets = HashMap::new();
        offsets.insert(0_i32, 42_i64);
        offsets.insert(1, 7);
        let payload = encode_kafka_offsets(&offsets).unwrap();
        b.seek_to(&payload).await.unwrap();

        let map = b.seek_map.lock().unwrap();
        assert_eq!(map.get(&0_i32), Some(&42_i64));
        assert_eq!(map.get(&1_i32), Some(&7_i64));
    }

    /// P4 #26: a follow-up `seek_to` overwrites the previous map
    /// rather than merging. The contract is "treat each call as the
    /// authoritative recovery point" — partial overrides would
    /// silently leave stale offsets for partitions the new payload
    /// dropped.
    #[tokio::test(flavor = "multi_thread")]
    async fn seek_to_replaces_prior_seek_map() {
        let b = KafkaBackend::open("localhost:9092", Some("g")).unwrap();
        let mut first = HashMap::new();
        first.insert(0_i32, 42_i64);
        first.insert(1, 7);
        b.seek_to(&encode_kafka_offsets(&first).unwrap())
            .await
            .unwrap();

        let mut second = HashMap::new();
        second.insert(2_i32, 99_i64);
        b.seek_to(&encode_kafka_offsets(&second).unwrap())
            .await
            .unwrap();

        let map = b.seek_map.lock().unwrap();
        assert_eq!(map.len(), 1, "second seek_to replaces, doesn't merge");
        assert_eq!(map.get(&2_i32), Some(&99_i64));
    }

    /// P4 #26: every context built by `KafkaBackend::build_context`
    /// shares the same `seek_map` Arc. Verifies that a `seek_to`
    /// after a context is built still reaches it (the rebalance
    /// callback reads the latest map on the next assignment, even
    /// if the context predates the call).
    #[tokio::test(flavor = "multi_thread")]
    async fn build_context_clones_share_seek_map_with_backend() {
        let b = KafkaBackend::open("localhost:9092", Some("g")).unwrap();
        let ctx_before = b.build_context();
        let mut offsets = HashMap::new();
        offsets.insert(3_i32, 100_i64);
        b.seek_to(&encode_kafka_offsets(&offsets).unwrap())
            .await
            .unwrap();
        // The pre-`seek_to` context's seek_map sees the update via
        // the shared Arc.
        let m = ctx_before.seek_map.lock().unwrap();
        assert_eq!(m.get(&3_i32), Some(&100_i64));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn seek_to_with_garbage_bytes_returns_error() {
        let b = KafkaBackend::open("localhost:9092", Some("g")).unwrap();
        let err = b.seek_to(b"not-json").await.unwrap_err();
        assert!(err.to_string().contains("decode"));
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

    /// Π.1 follow-up: SR basic auth round-trips through the builder
    /// and `sr_auth()` produces an `SrAuth` that carries it.
    #[test]
    fn with_schema_registry_basic_auth_round_trips() {
        let backend = KafkaBackend::open("localhost:9092", None)
            .unwrap()
            .with_schema_registry_url("http://sr.example.com")
            .with_schema_registry_basic_auth("api-key", "api-secret");

        // Public accessor exposes the (user, password) pair as
        // borrowed strings.
        let (u, p) = backend.schema_registry_basic_auth().expect("set");
        assert_eq!(u, "api-key");
        assert_eq!(p, "api-secret");

        // sr_auth() bundles URL + auth.
        let auth = backend.sr_auth().expect("URL set");
        assert_eq!(auth.url, "http://sr.example.com");
        assert_eq!(
            auth.basic_auth,
            Some(("api-key".to_string(), "api-secret".to_string()))
        );
    }

    /// Without `with_schema_registry_basic_auth`, `sr_auth()` carries
    /// `None` for the credentials and `build_sr_settings()` falls
    /// through to the unauthenticated `SrSettings::new` path.
    #[test]
    fn sr_auth_without_basic_auth_is_unauthenticated() {
        let backend = KafkaBackend::open("localhost:9092", None)
            .unwrap()
            .with_schema_registry_url("http://sr:8081");
        let auth = backend.sr_auth().expect("URL set");
        assert!(auth.basic_auth.is_none());
        // Build call should succeed (no auth → SrSettings::new).
        auth.build_sr_settings().expect("settings build");
    }

    #[test]
    fn debug_redacts_schema_registry_basic_auth_password() {
        let backend = KafkaBackend::open("localhost:9092", None)
            .unwrap()
            .with_schema_registry_url("http://sr:8081")
            .with_schema_registry_basic_auth("api-key", "secret-do-not-leak");
        let dbg = format!("{backend:?}");
        assert!(
            !dbg.contains("secret-do-not-leak"),
            "Debug leaked SR password: {dbg}"
        );
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
    async fn avro_read_without_sr_url_rejects_with_pointer() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_payload_format(KafkaPayloadFormat::Avro);
        let err = match b.read_arrow_stream("any-topic").await {
            Ok(_) => panic!("expected schema_registry_url rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("schema_registry_url"),
            "expected SR URL rejection; got: {msg}"
        );
    }

    #[test]
    fn with_schema_registry_url_records_the_url() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_payload_format(KafkaPayloadFormat::Avro)
            .with_schema_registry_url("http://localhost:8081");
        assert_eq!(b.schema_registry_url(), Some("http://localhost:8081"));
        assert_eq!(b.payload_format(), KafkaPayloadFormat::Avro);
    }

    #[test]
    fn avro_value_to_json_unwraps_nullable_union() {
        // Avro union { null, string } → JSON null or string.
        let null_union =
            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null));
        assert_eq!(avro_value_to_json(&null_union), serde_json::Value::Null);
        let string_union = apache_avro::types::Value::Union(
            1,
            Box::new(apache_avro::types::Value::String("hi".into())),
        );
        assert_eq!(
            avro_value_to_json(&string_union),
            serde_json::Value::String("hi".into())
        );
    }

    #[test]
    fn avro_value_to_json_record_becomes_object() {
        let record = apache_avro::types::Value::Record(vec![
            ("id".into(), apache_avro::types::Value::Long(7)),
            (
                "name".into(),
                apache_avro::types::Value::String("alice".into()),
            ),
        ]);
        let js = avro_value_to_json(&record);
        let obj = js.as_object().expect("record → object");
        assert_eq!(obj.get("id"), Some(&serde_json::json!(7)));
        assert_eq!(
            obj.get("name"),
            Some(&serde_json::Value::String("alice".into()))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn avro_write_without_sr_url_rejects_with_pointer() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        let b = KafkaBackend::open("localhost:9092", None)
            .unwrap()
            .with_payload_format(KafkaPayloadFormat::Avro);
        // Non-empty stream so dispatch reaches the format check.
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
        assert!(
            msg.contains("schema_registry_url"),
            "expected SR URL rejection on write; got: {msg}"
        );
    }

    /// 36h.4: encode_batch_as_avro is invoked when SR URL is set;
    /// we test that it surfaces a clean error against an unreachable
    /// SR URL (rather than panicking). Doesn't require a live SR.
    #[tokio::test(flavor = "multi_thread")]
    async fn avro_encode_unreachable_sr_returns_clean_error() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "beat",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))])
                .unwrap();
        // Port 1 is "tcpmux"; effectively guaranteed to refuse.
        let err = encode_batch_as_avro(&batch, "heartbeat", &SrAuth::new("http://127.0.0.1:1"))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("kafka avro encode"),
            "expected clean encode error; got: {msg}"
        );
    }

    /// 36h.5: protofish → JSON conversion preserves field names
    /// and primitive types for a simple message.
    #[test]
    fn protofish_message_to_json_simple_record() {
        use protofish::context::Context;
        use protofish::decode::{FieldValue, MessageValue, Value as Pv};

        let proto_src = r#"
            syntax = "proto3";
            package demo;
            message Heartbeat {
                int64 beat = 1;
                string label = 2;
            }
        "#;
        let ctx = Context::parse([proto_src]).expect("parse proto");
        let msg_info = ctx.get_message("demo.Heartbeat").expect("Heartbeat exists");
        let msg = MessageValue {
            msg_ref: msg_info.self_ref,
            fields: vec![
                FieldValue {
                    number: 1,
                    value: Pv::Int64(7),
                },
                FieldValue {
                    number: 2,
                    value: Pv::String("alice".into()),
                },
            ],
            garbage: None,
        };
        let js = protofish_message_to_json(&msg, &ctx);
        let obj = js.as_object().expect("record → object");
        assert_eq!(obj.get("beat"), Some(&serde_json::json!(7)));
        assert_eq!(
            obj.get("label"),
            Some(&serde_json::Value::String("alice".into()))
        );
    }

    /// 36h.5: repeated proto fields collapse to JSON arrays in the
    /// declared field order.
    #[test]
    fn protofish_message_to_json_repeated_field_becomes_array() {
        use protofish::context::Context;
        use protofish::decode::{FieldValue, MessageValue, Value as Pv};

        let proto_src = r#"
            syntax = "proto3";
            package demo;
            message Tags {
                repeated string name = 1;
            }
        "#;
        let ctx = Context::parse([proto_src]).expect("parse proto");
        let msg_info = ctx.get_message("demo.Tags").expect("Tags exists");
        let msg = MessageValue {
            msg_ref: msg_info.self_ref,
            fields: vec![
                FieldValue {
                    number: 1,
                    value: Pv::String("a".into()),
                },
                FieldValue {
                    number: 1,
                    value: Pv::String("b".into()),
                },
            ],
            garbage: None,
        };
        let js = protofish_message_to_json(&msg, &ctx);
        let arr = js
            .as_object()
            .and_then(|o| o.get("name"))
            .and_then(|v| v.as_array())
            .expect("name is array");
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], serde_json::Value::String("a".into()));
        assert_eq!(arr[1], serde_json::Value::String("b".into()));
    }

    /// 36h.5: read path surfaces a clean error when SR URL is set
    /// but unreachable. Doesn't require a live SR.
    #[tokio::test(flavor = "multi_thread")]
    async fn protobuf_decode_unreachable_sr_returns_clean_error() {
        let payloads = vec![vec![0x00, 0x00, 0x00, 0x00, 0x01, 0x08, 0x65]];
        let err = decode_payloads_as_protobuf(payloads, &SrAuth::new("http://127.0.0.1:1"))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("kafka protobuf decode"),
            "expected clean decode error; got: {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn protobuf_read_without_sr_url_rejects_with_pointer() {
        let b = KafkaBackend::open("localhost:9092", Some("g1"))
            .unwrap()
            .with_payload_format(KafkaPayloadFormat::Protobuf);
        let err = match b.read_arrow_stream("any-topic").await {
            Ok(_) => panic!("expected Protobuf SR URL rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("schema_registry_url"),
            "expected SR URL rejection on protobuf read; got: {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn protobuf_write_without_sr_url_rejects_with_pointer() {
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
        assert!(
            msg.contains("schema_registry_url"),
            "expected SR URL rejection on protobuf write; got: {msg}"
        );
    }

    /// 36h.6: JSON → MessageValue conversion populates fields by
    /// schema-resolved name and primitive type. Round-trips through
    /// protofish's encode/decode within a single in-process Context
    /// (no SR involved).
    #[test]
    fn json_to_protofish_message_round_trips_primitives() {
        use protofish::context::Context;
        let proto_src = r#"
            syntax = "proto3";
            package demo;
            message Heartbeat {
                int64 beat = 1;
                string label = 2;
                bool ok = 3;
            }
        "#;
        let ctx = Context::parse([proto_src]).expect("parse proto");
        let msg_info = ctx.get_message("demo.Heartbeat").expect("Heartbeat exists");

        let json = serde_json::json!({
            "beat": 42,
            "label": "alice",
            "ok": true,
        });
        let mv = json_to_protofish_message(&json, msg_info, &ctx).expect("convert");
        let bytes = mv.encode(&ctx);
        let decoded = msg_info.decode(&bytes, &ctx);
        // Re-render to JSON and check field presence.
        let round = protofish_message_to_json(&decoded, &ctx);
        let obj = round.as_object().unwrap();
        assert_eq!(obj.get("beat"), Some(&serde_json::json!(42)));
        assert_eq!(
            obj.get("label"),
            Some(&serde_json::Value::String("alice".into()))
        );
        assert_eq!(obj.get("ok"), Some(&serde_json::Value::Bool(true)));
    }

    /// 36h.6: repeated JSON arrays become repeated proto fields.
    #[test]
    fn json_to_protofish_message_repeated_round_trip() {
        use protofish::context::Context;
        let proto_src = r#"
            syntax = "proto3";
            package demo;
            message Tags {
                repeated string name = 1;
            }
        "#;
        let ctx = Context::parse([proto_src]).expect("parse proto");
        let msg_info = ctx.get_message("demo.Tags").expect("Tags exists");
        let json = serde_json::json!({ "name": ["a", "b", "c"] });
        let mv = json_to_protofish_message(&json, msg_info, &ctx).expect("convert");
        // Three FieldValues with number=1.
        assert_eq!(mv.fields.len(), 3);
        assert!(
            mv.fields
                .iter()
                .all(|f| f.number == 1 && matches!(&f.value, protofish::decode::Value::String(_)))
        );
    }

    /// 36h.6: encode path surfaces a clean error when SR URL is set
    /// but unreachable. Doesn't require a live SR.
    #[tokio::test(flavor = "multi_thread")]
    async fn protobuf_encode_unreachable_sr_returns_clean_error() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "beat",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2]))]).unwrap();
        let err = encode_batch_as_protobuf(&batch, "heartbeat", &SrAuth::new("http://127.0.0.1:1"))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("kafka protobuf schema fetch"),
            "expected clean schema-fetch error; got: {msg}"
        );
    }

    /// hex_decode round-trips lowercase hex strings.
    #[test]
    fn hex_decode_round_trips_simple_bytes() {
        let raw = vec![0x00, 0x10, 0xff, 0xab];
        let s: String = raw.iter().map(|b| format!("{:02x}", b)).collect();
        let decoded = hex_decode(&s).expect("decode");
        assert_eq!(decoded, raw);
    }

    #[test]
    fn hex_decode_rejects_odd_length() {
        let err = hex_decode("abc").unwrap_err();
        assert!(err.contains("odd length"), "got: {err}");
    }

    #[test]
    fn hex_decode_rejects_non_hex() {
        let err = hex_decode("zz").unwrap_err();
        assert!(err.contains("non-hex"), "got: {err}");
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

    /// Credential-safety: SASL/PLAIN password must not appear in
    /// Debug output. The framework relies on this when callers
    /// log `info!(?backend, ...)` or `error!(?cfg, ...)` for
    /// observability.
    #[test]
    fn debug_redacts_sasl_plain_password() {
        let b = KafkaBackend::open("localhost:9092", None)
            .unwrap()
            .with_sasl_plain("alice", "s3cret-password-do-not-leak");
        let s = format!("{b:?}");
        assert!(
            !s.contains("s3cret-password-do-not-leak"),
            "Debug leaks SASL/PLAIN password: {s}"
        );
        assert!(s.contains("<redacted>"), "expected <redacted> marker: {s}");
        assert!(s.contains("alice"), "username should remain visible: {s}");
    }

    /// Same credential-safety check for SASL/SCRAM.
    #[test]
    fn debug_redacts_sasl_scram_password() {
        let b = KafkaBackend::open("localhost:9092", None)
            .unwrap()
            .with_sasl_scram(ScramMechanism::Sha512, "alice", "scram-secret-do-not-leak");
        let s = format!("{b:?}");
        assert!(
            !s.contains("scram-secret-do-not-leak"),
            "Debug leaks SASL/SCRAM password: {s}"
        );
        assert!(s.contains("Sha512"), "mechanism should remain visible: {s}");
    }

    /// TlsAuth's `key_password` is a private-key passphrase —
    /// also redacted; cert/key/ca paths stay visible.
    #[test]
    fn debug_redacts_tls_key_password() {
        let tls = TlsAuth {
            ca_location: "/etc/ca.pem".into(),
            cert_location: "/etc/client.crt".into(),
            key_location: "/etc/client.key".into(),
            key_password: Some("private-key-passphrase".into()),
        };
        let s = format!("{tls:?}");
        assert!(
            !s.contains("private-key-passphrase"),
            "TlsAuth Debug leaks key_password: {s}"
        );
        assert!(s.contains("/etc/ca.pem"), "ca path should be visible: {s}");
    }

    /// Phase 40.2: with_message_key_column records the column name.
    #[test]
    fn with_message_key_column_records_value() {
        let b = KafkaBackend::open("localhost:9092", None)
            .unwrap()
            .with_message_key_column("user_id");
        assert_eq!(b.message_key_column(), Some("user_id"));
    }

    /// Phase 40.2: by default no key column → round-robin.
    #[test]
    fn message_key_column_default_is_none() {
        let b = KafkaBackend::open("localhost:9092", None).unwrap();
        assert!(b.message_key_column().is_none());
    }

    /// Phase 40.2: extract_message_keys handles a Utf8 column,
    /// rendering nulls as empty bytes.
    #[test]
    fn extract_message_keys_utf8_column_with_nulls() {
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), None, Some("carol")])),
            ],
        )
        .unwrap();
        let keys = extract_message_keys(&batch, "user").unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"alice".to_vec());
        assert_eq!(keys[1], Vec::<u8>::new());
        assert_eq!(keys[2], b"carol".to_vec());
    }

    /// Phase 40.2: missing column raises a clear error.
    #[test]
    fn extract_message_keys_missing_column_errors() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        let err = extract_message_keys(&batch, "user").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not present in batch") && msg.contains("user"),
            "got: {msg}"
        );
    }

    /// Phase 40.2: unsupported column type raises a clear error.
    #[test]
    fn extract_message_keys_unsupported_type_errors() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "user_id",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
                .unwrap();
        let err = extract_message_keys(&batch, "user_id").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unsupported type") && msg.contains("Utf8"),
            "got: {msg}"
        );
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
            seek_map: Default::default(),
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

    // -----------------------------------------------------------------
    // Task #556 — Glue Schema Registry dispatch tests
    // -----------------------------------------------------------------

    #[test]
    fn schema_registry_kind_defaults_to_confluent() {
        let b = KafkaBackend::open("localhost:9092", None).unwrap();
        assert_eq!(b.schema_registry_kind(), &SchemaRegistryKind::Confluent);
        assert!(!b.schema_registry_kind().is_glue());
    }

    #[test]
    fn with_schema_registry_kind_sets_glue() {
        let b = KafkaBackend::open("localhost:9092", None)
            .unwrap()
            .with_schema_registry_kind(SchemaRegistryKind::Glue {
                region: "us-east-1".into(),
                registry_name: "my-registry".into(),
                schema_lookup_callback: "test_cb".into(),
                schema_lookup_by_name_callback: String::new(),
            });
        assert!(b.schema_registry_kind().is_glue());
    }

    #[test]
    fn glue_decode_calls_callback_and_caches_schema() {
        use crate::glue_schema_registry::{GlueCodec, build_glue_frame};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use uuid::Uuid;

        // Build a Glue-framed message with a known schema UUID +
        // tiny Avro record (one int field, value=42).
        let schema_text = r#"{"type":"record","name":"X","fields":[{"name":"i","type":"int"}]}"#;
        let parsed = apache_avro::Schema::parse_str(schema_text).unwrap();
        let mut record = apache_avro::types::Record::new(&parsed).unwrap();
        record.put("i", 42i32);
        let avro_bytes = apache_avro::to_avro_datum(&parsed, record).unwrap();

        let uuid = Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap();
        let framed = build_glue_frame(uuid, GlueCodec::None, &avro_bytes);

        // Register a callback that returns the schema text for that UUID.
        // Count invocations so the cache assertion is meaningful.
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = calls.clone();
        let cb: crate::py_callbacks::CallbackFn = Arc::new(move |req_bytes: &[u8]| {
            calls_inner.fetch_add(1, Ordering::SeqCst);
            let req: GlueSchemaRequest = serde_json::from_slice(req_bytes).unwrap();
            assert_eq!(req.schema_uuid, "12345678-1234-5678-1234-567812345678");
            assert_eq!(req.region, "us-east-1");
            assert_eq!(req.registry_name, "my-registry");
            let resp = GlueSchemaResponse {
                data_format: "AVRO".into(),
                schema_definition:
                    r#"{"type":"record","name":"X","fields":[{"name":"i","type":"int"}]}"#.into(),
                schema_uuid: req.schema_uuid,
            };
            Ok(serde_json::to_vec(&resp).unwrap())
        });
        // Register against the *global* registry — that's what
        // decode_payloads_as_glue_avro reads. Use a unique callback
        // name so this test doesn't collide with other tests.
        crate::py_callbacks::global().register("test::glue_lookup_caches", cb);

        let cache: Arc<RwLock<HashMap<Uuid, Arc<apache_avro::Schema>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // First call: cache miss → callback fires once.
        let batches = decode_payloads_as_glue_avro(
            vec![framed.clone()],
            "us-east-1",
            "my-registry",
            "test::glue_lookup_caches",
            &cache,
        )
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        // Second call with the same UUID: cache hit → no extra callback.
        let _ = decode_payloads_as_glue_avro(
            vec![framed.clone()],
            "us-east-1",
            "my-registry",
            "test::glue_lookup_caches",
            &cache,
        )
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Cleanup so the global registry doesn't leak to other tests.
        crate::py_callbacks::global().unregister("test::glue_lookup_caches");
    }

    #[test]
    fn glue_decode_rejects_non_avro_data_format() {
        use crate::glue_schema_registry::{GlueCodec, build_glue_frame};
        use std::sync::Arc;
        use uuid::Uuid;

        let uuid = Uuid::new_v4();
        let framed = build_glue_frame(uuid, GlueCodec::None, b"ignored");
        let cb: crate::py_callbacks::CallbackFn = Arc::new(|req_bytes: &[u8]| {
            let req: GlueSchemaRequest = serde_json::from_slice(req_bytes).unwrap();
            let resp = GlueSchemaResponse {
                data_format: "PROTOBUF".into(),
                schema_definition: "".into(),
                schema_uuid: req.schema_uuid,
            };
            Ok(serde_json::to_vec(&resp).unwrap())
        });
        crate::py_callbacks::global().register("test::glue_lookup_protobuf", cb);

        let cache: Arc<RwLock<HashMap<Uuid, Arc<apache_avro::Schema>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let err = decode_payloads_as_glue_avro(
            vec![framed],
            "us-east-1",
            "r",
            "test::glue_lookup_protobuf",
            &cache,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("PROTOBUF") || err.to_string().contains("AVRO"),
            "got: {err}",
        );

        crate::py_callbacks::global().unregister("test::glue_lookup_protobuf");
    }

    #[test]
    fn glue_decode_surfaces_missing_callback() {
        use crate::glue_schema_registry::{GlueCodec, build_glue_frame};
        use std::sync::Arc;
        use uuid::Uuid;

        let uuid = Uuid::new_v4();
        let framed = build_glue_frame(uuid, GlueCodec::None, b"x");
        let cache: Arc<RwLock<HashMap<Uuid, Arc<apache_avro::Schema>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let err = decode_payloads_as_glue_avro(
            vec![framed],
            "us-east-1",
            "r",
            "test::nonexistent_callback_for_glue_decode",
            &cache,
        )
        .unwrap_err();
        assert!(err.to_string().contains("schema lookup"), "got: {err}",);
    }

    #[test]
    fn glue_decode_round_trips_zlib_compressed_payload() {
        use crate::glue_schema_registry::{GlueCodec, build_glue_frame};
        use std::io::Write;
        use std::sync::Arc;
        use uuid::Uuid;

        // Build a real Avro datum, zlib-compress it, then wrap in
        // the Glue frame with codec=Zlib. The decode path should
        // decompress + decode transparently and produce the same
        // RecordBatch the uncompressed path produces.
        let schema_text = r#"{"type":"record","name":"X","fields":[{"name":"i","type":"int"}]}"#;
        let parsed = apache_avro::Schema::parse_str(schema_text).unwrap();
        let mut record = apache_avro::types::Record::new(&parsed).unwrap();
        record.put("i", 7i32);
        let avro_bytes = apache_avro::to_avro_datum(&parsed, record).unwrap();

        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&avro_bytes).unwrap();
        let zlib_payload = encoder.finish().unwrap();
        // Sanity: the compressed form is byte-different from raw.
        assert_ne!(zlib_payload, avro_bytes);

        let uuid = Uuid::parse_str("aabbccdd-eeff-0011-2233-445566778899").unwrap();
        let framed = build_glue_frame(uuid, GlueCodec::Zlib, &zlib_payload);

        let cb: crate::py_callbacks::CallbackFn = Arc::new(move |_req| {
            let resp = GlueSchemaResponse {
                data_format: "AVRO".into(),
                schema_definition:
                    r#"{"type":"record","name":"X","fields":[{"name":"i","type":"int"}]}"#.into(),
                schema_uuid: "aabbccdd-eeff-0011-2233-445566778899".into(),
            };
            Ok(serde_json::to_vec(&resp).unwrap())
        });
        crate::py_callbacks::global().register("test::glue_zlib_roundtrip", cb);

        let cache: Arc<RwLock<HashMap<Uuid, Arc<apache_avro::Schema>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let batches = decode_payloads_as_glue_avro(
            vec![framed],
            "us-east-1",
            "r",
            "test::glue_zlib_roundtrip",
            &cache,
        )
        .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        crate::py_callbacks::global().unregister("test::glue_zlib_roundtrip");
    }

    #[test]
    fn glue_decode_zlib_decode_failure_surfaces_clearly() {
        use crate::glue_schema_registry::{GlueCodec, build_glue_frame};
        use std::sync::Arc;
        use uuid::Uuid;

        // Truncated / corrupted zlib bytes — the decoder must
        // surface a clear "zlib decode failed" error, not a generic
        // panic or a downstream Avro parse error that hides the
        // root cause.
        let uuid = Uuid::new_v4();
        let framed = build_glue_frame(uuid, GlueCodec::Zlib, b"not-actually-zlib");
        let cache: Arc<RwLock<HashMap<Uuid, Arc<apache_avro::Schema>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let err = decode_payloads_as_glue_avro(vec![framed], "us-east-1", "r", "any-name", &cache)
            .unwrap_err();
        assert!(err.to_string().contains("zlib decode failed"), "got: {err}",);
    }

    #[test]
    fn glue_producer_encode_round_trips_through_consumer() {
        // End-to-end-ish: encode an Arrow batch with the producer
        // helper, then decode the resulting bytes through the
        // consumer helper. Same backing schema, same UUID.
        use crate::glue_schema_registry::{GLUE_HEADER_BYTE, parse_glue_frame};
        use arrow_array::{Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;
        use uuid::Uuid;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Int32Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["a", "b", "c"]);
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(ids), Arc::new(names)]).unwrap();

        let avro_text = r#"{"type":"record","name":"X","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;
        let known_uuid = Uuid::parse_str("11223344-5566-7788-9900-aabbccddeeff").unwrap();

        let cb: crate::py_callbacks::CallbackFn = Arc::new(move |_req| {
            let resp = GlueSchemaResponse {
                data_format: "AVRO".into(),
                schema_definition: r#"{"type":"record","name":"X","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#.into(),
                schema_uuid: "11223344-5566-7788-9900-aabbccddeeff".into(),
            };
            Ok(serde_json::to_vec(&resp).unwrap())
        });
        crate::py_callbacks::global().register("test::glue_prod_by_name", cb);

        let producer_cache: Arc<RwLock<HashMap<String, Arc<(Uuid, apache_avro::Schema)>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let encoded = encode_batch_as_glue_avro(
            &batch,
            "us-east-1",
            "test-registry",
            "test-topic",
            "test::glue_prod_by_name",
            &producer_cache,
        )
        .unwrap();
        assert_eq!(encoded.len(), 3);
        for framed in &encoded {
            assert_eq!(framed[0], GLUE_HEADER_BYTE);
            let frame = parse_glue_frame(framed).unwrap();
            assert_eq!(frame.schema_uuid, known_uuid);
        }

        let cb2: crate::py_callbacks::CallbackFn = Arc::new(move |_req| {
            let resp = GlueSchemaResponse {
                data_format: "AVRO".into(),
                schema_definition: avro_text.into(),
                schema_uuid: "11223344-5566-7788-9900-aabbccddeeff".into(),
            };
            Ok(serde_json::to_vec(&resp).unwrap())
        });
        crate::py_callbacks::global().register("test::glue_prod_by_uuid", cb2);

        let consumer_cache: Arc<RwLock<HashMap<Uuid, Arc<apache_avro::Schema>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let batches = decode_payloads_as_glue_avro(
            encoded,
            "us-east-1",
            "test-registry",
            "test::glue_prod_by_uuid",
            &consumer_cache,
        )
        .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        crate::py_callbacks::global().unregister("test::glue_prod_by_name");
        crate::py_callbacks::global().unregister("test::glue_prod_by_uuid");
    }

    #[test]
    fn glue_producer_caches_schema_across_batches() {
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use uuid::Uuid;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "i",
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int32Array::from(vec![42]))]).unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = calls.clone();
        let cb: crate::py_callbacks::CallbackFn = Arc::new(move |_req| {
            calls_inner.fetch_add(1, Ordering::SeqCst);
            let resp = GlueSchemaResponse {
                data_format: "AVRO".into(),
                schema_definition:
                    r#"{"type":"record","name":"X","fields":[{"name":"i","type":"int"}]}"#.into(),
                schema_uuid: "deadbeef-0000-1111-2222-333344445555".into(),
            };
            Ok(serde_json::to_vec(&resp).unwrap())
        });
        crate::py_callbacks::global().register("test::glue_prod_cache", cb);

        let cache: Arc<RwLock<HashMap<String, Arc<(Uuid, apache_avro::Schema)>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        for _ in 0..5 {
            let _ = encode_batch_as_glue_avro(
                &batch,
                "us-east-1",
                "r",
                "topic-x",
                "test::glue_prod_cache",
                &cache,
            )
            .unwrap();
        }
        // Callback fires exactly once across 5 batches.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        crate::py_callbacks::global().unregister("test::glue_prod_cache");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn avro_read_skips_sr_url_check_when_glue_kind_set() {
        // Schema-registry-URL check must be bypassed for the Glue
        // path — the URL is meaningless when using Glue's IAM-backed
        // GetSchemaVersion. We can't actually drain a broker here,
        // but the validation block fires before broker IO, so a
        // dispatch error means we made it past that gate.
        let b = KafkaBackend::open("localhost:9092", Some("g"))
            .unwrap()
            .with_payload_format(KafkaPayloadFormat::Avro)
            .with_schema_registry_kind(SchemaRegistryKind::Glue {
                region: "us-east-1".into(),
                registry_name: "r".into(),
                schema_lookup_callback: "cb".into(),
                schema_lookup_by_name_callback: String::new(),
            });
        // No `with_schema_registry_url` — Confluent path would reject.
        // We expect either a successful drain attempt (in which case
        // the broker connection will fail with a different error) or
        // no SR-URL gate complaint. The test just confirms the gate
        // path didn't emit its specific error string.
        let result = b.read_arrow_stream("test-topic").await;
        if let Err(e) = result {
            assert!(
                !e.to_string().contains("schema_registry_url is"),
                "Glue path should not require schema_registry_url; got: {e}",
            );
        }
    }
}
