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
//! ## What 37b.2 adds (Arrow IO)
//!   - `read_arrow_stream(subscription)` — opens a streaming pull,
//!     drains messages until idle for `batch_config.idle_timeout_ms`
//!     or one of the size/byte limits is hit. Each payload is decoded
//!     as a single JSON object and rows are concatenated into one
//!     Arrow `RecordBatch` via arrow-json. `query` accepts either a
//!     bare subscription name (will be qualified with the backend's
//!     project_id) or a fully-qualified `projects/X/subscriptions/Y`.
//!   - `write_arrow_stream(target, ...)` — encodes each row as JSONL
//!     and publishes to `target.name` (bare topic name auto-qualified
//!     with the backend's project_id, or pass `projects/X/topics/Y`
//!     directly).
//!   - `WriteMode::Truncate` is rejected — Pub/Sub topics are
//!     append-only; purging is an admin-API operation.
//!
//! ## What 37b.3 adds (manual ack + at-least-once)
//!   - Persistent consumer session: `Subscriber` + `MessageStream` +
//!     `Vec<Handler>` are held in a Mutex on the backend and reused
//!     across `read_arrow_stream` calls. The `Handler` type returned
//!     by the SDK nacks-on-drop, so retaining handlers is the
//!     mechanism by which deliveries stay leased to us until we ack.
//!     Dropping the `MessageStream` also tears down the lease loop
//!     that re-extends ack-deadlines, so the session keeps it alive.
//!   - `read_arrow_stream` no longer auto-acks. It pushes each
//!     delivery's `Handler` onto the session's pending list.
//!   - `commit_offsets()` drains the held handlers and calls
//!     `Handler::ack()` on each — fire-and-forget against the SDK's
//!     internal ack channel; the lease loop flushes them
//!     server-side. Mirrors Kafka 36e / RabbitMQ 37a.3.
//!   - Single-subscription-per-backend constraint: the first
//!     `read_arrow_stream` call binds the session; subsequent calls
//!     must target the same subscription. Multi-subscription fanout
//!     would need separate `PubSubBackend` instances.
//!
//! ## What 37b.4 adds (DLQ)
//!   - `nack_pending()` — drain every retained handler and drop
//!     them. The SDK's `Handler::Drop` impl nacks the underlying
//!     ack_id, so the broker schedules immediate redelivery (vs
//!     waiting for the ~10s ack-deadline to expire).
//!   - The streaming-pipeline-level DLQ pattern (write a failed
//!     batch to a DLQ topic via `source.write_arrow_stream`)
//!     already works for Pub/Sub via the `write_arrow_stream` we
//!     ship in 37b.2 — `dlq_target.name` is the topic name; auto-
//!     qualification with the project_id lets bare names route.
//!
//! ### Two DLQ modes for Pub/Sub
//! | mode | what it does | when to use |
//! |--|--|--|
//! | App-level | StreamingPipeline writes the failed batch's rows to a separate topic via the source backend's `write_arrow_stream`, then commits. | Cross-pipeline DLQ replay: re-consume the DLQ topic identically to the primary. |
//! | Broker-level | `nack_pending()` triggers redelivery. With the subscription declared with `dead_letter_policy.dead_letter_topic = projects/.../topics/<dlt>` and `max_delivery_attempts = N`, after N nacks the broker auto-routes the message to the DLT and acks it from the original subscription. | Native Pub/Sub DLQ; observability via Pub/Sub admin tooling. |

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use google_cloud_auth::credentials::Credentials;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousAuthBuilder;
use google_cloud_pubsub::client::{Publisher, Subscriber, TopicAdmin};
use google_cloud_pubsub::model::Message;
use google_cloud_pubsub::subscriber::MessageStream;
use google_cloud_pubsub::subscriber::handler::Handler;
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
/// callers can call `read_arrow_stream` again to continue. Mirrors
/// `RabbitBatchConfig`.
#[derive(Debug, Clone)]
pub struct PubSubBatchConfig {
    /// Max number of messages per call.
    pub batch_size: usize,
    /// Max bytes accumulated per call.
    pub batch_bytes: usize,
    /// Idle timeout (ms): if no message arrives within this window,
    /// the call returns whatever it has (possibly empty).
    pub idle_timeout_ms: u64,
}

impl Default for PubSubBatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_bytes: 16 * 1024 * 1024, // 16 MiB
            idle_timeout_ms: 2_000,
        }
    }
}

/// Persistent consumer session held across `read_arrow_stream`
/// calls.
///
/// Three things must stay alive together for manual ack to work:
///   - the `MessageStream` (its lease loop re-extends ack-deadlines
///     on the messages we haven't acked yet),
///   - the `Vec<Handler>` (each `Handler` nacks on drop, so dropping
///     them all without acking would force re-delivery),
///   - the bound subscription name (so subsequent calls can be
///     validated against the original binding).
struct PubSubConsumerSession {
    /// The streaming-pull message stream. Held to keep the lease
    /// loop alive between drains.
    stream: MessageStream,
    /// Handlers for delivered-but-not-acked messages. Drained by
    /// `commit_offsets()` (each `Handler::ack()` consumes the handle).
    pending_handlers: Vec<Handler>,
    /// Subscription name (the fully-qualified
    /// `projects/.../subscriptions/...`) bound on first call.
    subscription: String,
}

impl std::fmt::Debug for PubSubConsumerSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubConsumerSession")
            .field("subscription", &self.subscription)
            .field("pending_handlers", &self.pending_handlers.len())
            .finish_non_exhaustive()
    }
}

/// GCP Pub/Sub-backed implementation of `Backend`.
///
/// Holds the project_id + optional endpoint + optional anonymous
/// auth flag, plus a lazily-initialized consumer session. The
/// session keeps the AMQP-style ack discipline working: handlers
/// are retained until `commit_offsets()` drains them.
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
    /// Per-call drain limits for `read_arrow_stream`. Builder-set
    /// via `with_batch_config`.
    batch_config: PubSubBatchConfig,
    /// Lazy consumer session. Populated on first
    /// `read_arrow_stream`; reused on subsequent calls and
    /// `commit_offsets`.
    consumer_session: Arc<Mutex<Option<PubSubConsumerSession>>>,
}

impl std::fmt::Debug for PubSubBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubBackend")
            .field("project_id", &self.project_id)
            .field("endpoint", &self.endpoint)
            .field("anonymous_auth", &self.anonymous_auth)
            .field("batch_config", &self.batch_config)
            .finish_non_exhaustive()
    }
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
            batch_config: PubSubBatchConfig::default(),
            consumer_session: Arc::new(Mutex::new(None)),
        })
    }

    /// Test/observability hook: number of unacked deliveries
    /// retained in the consumer session. Returns 0 when no session
    /// has been opened.
    pub async fn pending_handler_count(&self) -> usize {
        self.consumer_session
            .lock()
            .await
            .as_ref()
            .map(|s| s.pending_handlers.len())
            .unwrap_or(0)
    }

    /// Phase 37b.4: drain every retained handler and drop them.
    /// The SDK's `Handler::Drop` impl nacks the underlying ack_id,
    /// so the broker schedules immediate redelivery. Combined with
    /// a subscription declared with `dead_letter_policy`
    /// (configured at subscription-create time, not by the
    /// framework), after `max_delivery_attempts` nacks the broker
    /// auto-routes the message to the DLT topic and acks it from
    /// the original subscription.
    ///
    /// No-op if no consumer session has been opened or no handlers
    /// are pending.
    pub async fn nack_pending(&self) -> Result<(), BackendError> {
        let mut session_lock = self.consumer_session.lock().await;
        let Some(session) = session_lock.as_mut() else {
            return Ok(());
        };
        let drained = std::mem::take(&mut session.pending_handlers);
        // Each Handler nacks on drop; explicit drop here so the
        // intent is unambiguous in code review.
        drop(drained);
        Ok(())
    }

    /// Override the per-call drain limits used by `read_arrow_stream`.
    pub fn with_batch_config(mut self, cfg: PubSubBatchConfig) -> Self {
        self.batch_config = cfg;
        self
    }

    /// Borrow the active batch config.
    pub fn batch_config(&self) -> &PubSubBatchConfig {
        &self.batch_config
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

    /// Build a `Subscriber` client matching this backend's config.
    /// Phase 37b.2 builds one per `read_arrow_stream` call; 37b.3
    /// will keep it alive across calls in a session struct so
    /// handlers can be retained between drain and ack.
    async fn subscriber_client(&self) -> Result<Subscriber, BackendError> {
        let mut builder = Subscriber::builder();
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
            .map_err(|e| BackendError::Connection(format!("pubsub Subscriber build: {e}")))
    }

    /// Build a per-topic `Publisher` client matching this backend's
    /// config. The Publisher batches publishes internally; for
    /// one-shot drains we just construct a fresh one per
    /// `write_arrow_stream` call.
    async fn publisher_client(&self, topic: &str) -> Result<Publisher, BackendError> {
        let mut builder = Publisher::builder(topic.to_string());
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
            .map_err(|e| BackendError::Connection(format!("pubsub Publisher build: {e}")))
    }

    /// Open the persistent consumer session: build a `Subscriber`,
    /// start a streaming pull on `subscription_name`, and wrap them
    /// in a `PubSubConsumerSession` ready to retain handlers across
    /// drains. Called lazily on first `read_arrow_stream`.
    async fn open_consumer_session(
        &self,
        subscription_name: &str,
    ) -> Result<PubSubConsumerSession, BackendError> {
        let client = self.subscriber_client().await?;
        let stream = client.subscribe(subscription_name.to_string()).build();
        Ok(PubSubConsumerSession {
            stream,
            pending_handlers: Vec::new(),
            subscription: subscription_name.to_string(),
        })
    }

    /// Qualify a bare subscription name with this backend's
    /// project_id. If the input already starts with `projects/`,
    /// pass it through unchanged.
    fn qualify_subscription(&self, name: &str) -> String {
        if name.starts_with("projects/") {
            name.to_string()
        } else {
            format!("projects/{}/subscriptions/{}", self.project_id, name)
        }
    }

    /// Qualify a bare topic name with this backend's project_id.
    /// Same passthrough behavior as `qualify_subscription`.
    fn qualify_topic(&self, name: &str) -> String {
        if name.starts_with("projects/") {
            name.to_string()
        } else {
            format!("projects/{}/topics/{}", self.project_id, name)
        }
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

    fn config(&self) -> crate::backend::BackendConfig {
        // Σ.B PR 1 commit d: constructor args (project_id) only.
        // Endpoint + anonymous_auth + batch_config round-trip ships
        // in Σ.B PR 2.
        if self.endpoint.is_some() || self.anonymous_auth {
            panic!(
                "PubSubBackend::config() called with non-default builder state \
                 (endpoint / anonymous_auth). Full round-trip ships in Σ.B PR 2 — \
                 see docs/PHASE_SIGMA_B_TRAIT_SPIKE.md."
            );
        }
        crate::backend::BackendConfig::PubSub(crate::backend::PubSubConfig {
            project_id: self.project_id.clone(),
        })
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

    /// Subscribe to `query` (subscription name; bare or
    /// fully-qualified) and drain messages until idle for
    /// `batch_config.idle_timeout_ms` or one of the size/byte
    /// limits hits. Each payload is decoded as a single JSON
    /// object and rows are concatenated into one Arrow `RecordBatch`.
    /// Schema is inferred from the first 1024 messages (arrow-json
    /// default).
    ///
    /// Returns an empty stream if the subscription has no pending
    /// messages (idle timeout fires immediately).
    ///
    /// Acks are deferred. Each delivery's `Handler` is retained on
    /// the persistent consumer session and acked when
    /// `commit_offsets()` fires — which the streaming pipeline
    /// triggers after the target backend has durably written. This
    /// is the at-least-once primitive that mirrors Kafka 36e and
    /// RabbitMQ 37a.3.
    ///
    /// Limits in 37b.3 — folded out in later sub-phases:
    ///   - JSON-only payload decode.
    ///   - Single subscription per backend instance: the first call
    ///     wins the subscription binding; subsequent calls must
    ///     target the same name.
    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        let subscription = query.trim();
        if subscription.is_empty() {
            return Err(BackendError::Other(
                "Pub/Sub read_arrow_stream: query argument must be a non-empty \
                 subscription name"
                    .into(),
            ));
        }
        let subscription_name = self.qualify_subscription(subscription);

        let cfg = self.batch_config.clone();
        let mut session_lock = self.consumer_session.lock().await;
        if session_lock.is_none() {
            *session_lock = Some(self.open_consumer_session(&subscription_name).await?);
        }
        let session = session_lock.as_mut().expect("session populated above");
        if session.subscription != subscription_name {
            return Err(BackendError::Other(format!(
                "Pub/Sub read_arrow_stream: this backend instance is already \
                 bound to subscription `{}`; multi-subscription fanout would \
                 need separate PubSubBackend instances",
                session.subscription
            )));
        }

        let idle = Duration::from_millis(cfg.idle_timeout_ms);
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        let mut bytes_total: usize = 0;
        loop {
            match tokio::time::timeout(idle, session.stream.next()).await {
                Ok(Some(Ok((message, handler)))) => {
                    bytes_total += message.data.len();
                    payloads.push(message.data.to_vec());
                    // Retain the handler so the message stays leased
                    // until commit_offsets() fires (or the backend
                    // drops, in which case Handler::Drop nacks).
                    session.pending_handlers.push(handler);
                    if payloads.len() >= cfg.batch_size || bytes_total >= cfg.batch_bytes {
                        break;
                    }
                }
                Ok(Some(Err(e))) => {
                    return Err(BackendError::Query(format!("pubsub stream error: {e}")));
                }
                // None = end-of-stream (the SDK keeps the stream
                // open in practice). Err = idle timeout — flush.
                Ok(None) | Err(_) => break,
            }
        }
        // Drop the lock before decoding so a concurrent
        // commit_offsets call doesn't wait on JSON parsing.
        drop(session_lock);

        if payloads.is_empty() {
            let stream = futures_util::stream::empty();
            return Ok(Box::pin(stream));
        }
        let batches = decode_payloads_as_jsonl(payloads)?;
        let stream = futures_util::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }

    /// Produce each Arrow row as a Pub/Sub message to `target.name`
    /// (topic; bare or fully-qualified). Each batch is encoded as
    /// JSONL and split row-wise; one `publish` per row. The
    /// returned `PublishFuture` is awaited so the message_id is
    /// confirmed before we count the row. `WriteMode::Truncate` is
    /// rejected — Pub/Sub topics aren't truncatable from a producer.
    ///
    /// Limits in 37b.2 — folded out in later sub-phases:
    ///   - JSON-only payload encode.
    ///   - One `Publisher` per call (the SDK's internal batching
    ///     is per-Publisher, so per-call clients miss some
    ///     batching opportunity. 37b.3 / pipeline-level reuse
    ///     can amortize that.)
    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError> {
        if mode == WriteMode::Truncate {
            return Err(BackendError::Other(
                "Pub/Sub write_arrow_stream: Truncate is not supported on a topic. \
                 Topics are append-only; to start fresh, delete and recreate the \
                 topic via gcloud / the Pub/Sub admin API."
                    .into(),
            ));
        }
        let topic = target.name.trim();
        if topic.is_empty() {
            return Err(BackendError::Other(
                "Pub/Sub write_arrow_stream: target.name (topic) must be non-empty".into(),
            ));
        }
        let topic_name = self.qualify_topic(topic);
        let publisher = self.publisher_client(&topic_name).await?;

        let mut s = stream;
        let mut total: u64 = 0;
        while let Some(batch) = s.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let payloads = encode_batch_as_jsonl_lines(&batch)?;
            // Publish all rows of the batch concurrently and await
            // the message-ids — the SDK returns a Future per
            // publish that resolves after the broker ack.
            let mut futures = Vec::with_capacity(payloads.len());
            for payload in payloads {
                let msg = Message::new().set_data(payload);
                futures.push(publisher.publish(msg));
            }
            for fut in futures {
                fut.await
                    .map_err(|e| BackendError::Query(format!("pubsub publish: {e}")))?;
                total += 1;
            }
        }
        Ok(total)
    }

    /// Cross-backend produce-side run_append: read source rows via
    /// the source's `read_arrow_stream`, produce them to
    /// `spec.name` (the topic) via `write_arrow_stream`. Mirrors
    /// the Kafka / RabbitMQ run_append shape.
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
                "Pub/Sub run_append: source_backend is required (cross-backend produce)".into(),
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
                path: format!("pubsub:{}", self.qualify_topic(&spec.name)),
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
            path: format!("pubsub:{}", self.qualify_topic(&spec.name)),
        })
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

    /// Drain the consumer session's retained handlers, calling
    /// `Handler::ack()` on each. The SDK queues each ack on the
    /// lease loop's internal channel — fire-and-forget; the lease
    /// loop flushes them server-side. No-op if no consumer session
    /// has been opened or no handlers are pending.
    async fn commit_offsets(&self) -> Result<(), BackendError> {
        let mut session_lock = self.consumer_session.lock().await;
        let Some(session) = session_lock.as_mut() else {
            return Ok(());
        };
        let drained = std::mem::take(&mut session.pending_handlers);
        for h in drained {
            h.ack();
        }
        Ok(())
    }

    /// Phase 39.5a P1.7a: Pub/Sub uses broker-tracked offsets via
    /// the per-message ack stream. There's no client-side offset to
    /// stash — the subscription's ack horizon is the source of
    /// truth. Reporting `true` lets stateful (session/join)
    /// pipelines accept Pub/Sub as a source; `seek_to` is a no-op
    /// because the broker already knows where we are.
    fn supports_seek_to(&self) -> bool {
        true
    }

    async fn seek_to(&self, _offset_bytes: &[u8]) -> Result<(), BackendError> {
        // No-op. Subscription `seek` exists in the Pub/Sub API but
        // is destructive (rewinds the ack horizon for every
        // consumer of the subscription); we never call it. On
        // restart unacked messages re-deliver automatically.
        Ok(())
    }

    // `offset_snapshot()` keeps the trait default `Ok(None)` —
    // there's nothing client-side to commit to the StateStore.
    // The subscription's broker-tracked ack horizon advances when
    // `commit_offsets()` fires.
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
    async fn read_arrow_stream_rejects_empty_subscription_name() {
        let b = PubSubBackend::open("p").unwrap();
        let err = match b.read_arrow_stream("   ").await {
            Ok(_) => panic!("expected empty-subscription rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("non-empty subscription name"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_arrow_stream_truncate_rejects() {
        let b = PubSubBackend::open("p").unwrap();
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

    #[tokio::test(flavor = "multi_thread")]
    async fn write_arrow_stream_rejects_empty_topic_name() {
        let b = PubSubBackend::open("p").unwrap();
        let target = TargetTable {
            schema: "".into(),
            name: "   ".into(),
        };
        let stream = Box::pin(futures_util::stream::empty());
        let err = b
            .write_arrow_stream(&target, stream, WriteMode::Append)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must be non-empty"), "got: {msg}");
    }

    #[test]
    fn batch_config_defaults_are_reasonable() {
        let cfg = PubSubBatchConfig::default();
        assert!(cfg.batch_size >= 100);
        assert!(cfg.batch_bytes >= 1 << 20);
        assert!(cfg.idle_timeout_ms >= 500);
    }

    #[test]
    fn with_batch_config_overrides_default() {
        let b = PubSubBackend::open("p")
            .unwrap()
            .with_batch_config(PubSubBatchConfig {
                batch_size: 7,
                batch_bytes: 99,
                idle_timeout_ms: 11,
            });
        assert_eq!(b.batch_config().batch_size, 7);
        assert_eq!(b.batch_config().batch_bytes, 99);
        assert_eq!(b.batch_config().idle_timeout_ms, 11);
    }

    #[test]
    fn qualify_subscription_prefixes_bare_name() {
        let b = PubSubBackend::open("my-project").unwrap();
        assert_eq!(
            b.qualify_subscription("my-sub"),
            "projects/my-project/subscriptions/my-sub"
        );
    }

    #[test]
    fn qualify_subscription_passes_through_fully_qualified() {
        let b = PubSubBackend::open("my-project").unwrap();
        let fq = "projects/other-project/subscriptions/my-sub";
        assert_eq!(b.qualify_subscription(fq), fq);
    }

    #[test]
    fn qualify_topic_prefixes_bare_name() {
        let b = PubSubBackend::open("my-project").unwrap();
        assert_eq!(
            b.qualify_topic("my-topic"),
            "projects/my-project/topics/my-topic"
        );
    }

    #[test]
    fn qualify_topic_passes_through_fully_qualified() {
        let b = PubSubBackend::open("my-project").unwrap();
        let fq = "projects/other-project/topics/my-topic";
        assert_eq!(b.qualify_topic(fq), fq);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_append_requires_source_backend() {
        let b = PubSubBackend::open("p").unwrap();
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

    /// 37b.3: commit_offsets is a no-op before any consumer
    /// session has been opened. Lets pipelines call it
    /// unconditionally.
    #[tokio::test(flavor = "multi_thread")]
    async fn commit_offsets_noop_without_consumer_session() {
        let b = PubSubBackend::open("p").unwrap();
        b.commit_offsets().await.unwrap();
        assert_eq!(b.pending_handler_count().await, 0);
    }

    /// 37b.4: nack_pending is a no-op before any consumer session
    /// has been opened, and is idempotent across repeated calls.
    #[tokio::test(flavor = "multi_thread")]
    async fn nack_pending_noop_without_consumer_session() {
        let b = PubSubBackend::open("p").unwrap();
        b.nack_pending().await.unwrap();
        b.nack_pending().await.unwrap();
        assert_eq!(b.pending_handler_count().await, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_append_dry_run_returns_zero_inserts_without_io() {
        let b = PubSubBackend::open("my-project").unwrap();
        let spec = TableSpec {
            schema: "".into(),
            name: "t".into(),
            columns: vec![],
            unique_constraints: vec![],
            fingerprint: String::new(),
        };
        // Construct a noop "source" via the pubsub backend itself —
        // dry_run skips IO before it actually calls the source.
        let other = PubSubBackend::open("my-project").unwrap();
        let res = b
            .run_append(&spec, "x", "p", Some(&other), None, None, true)
            .await
            .unwrap();
        assert_eq!(res.rows_inserted, 0);
        assert_eq!(res.status, "dry-run");
        assert_eq!(res.path, "pubsub:projects/my-project/topics/t");
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
