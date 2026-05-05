//! Phase 37a: RabbitMQ backend.
//!
//! Wraps `lapin` (pure-rust AMQP 0.9.1 client) as a `Backend` so the
//! same trait surface drives RabbitMQ pipelines that already drives
//! Kafka, the DB / object-store / Delta backends.
//!
//! ## What 37a ships (skeleton + ping)
//!   - `RabbitMQBackend::open(amqp_url)` — wraps an AMQP URI of the
//!     form `amqp://user:pass@host:port/vhost`.
//!   - `dialect()` → `Dialect::Streaming { kind: RabbitMQ }`.
//!   - `connection_info()` reports host:port + the user component.
//!   - `dsn()` returns the AMQP URL.
//!   - `ping()` opens a connection, declares a channel, closes both,
//!     all under a short timeout.
//!   - `execute()` rejects: AMQP has no SQL surface.
//!
//! ## What 37a.2 adds (Arrow IO)
//!   - `read_arrow_stream(queue)` — drains the queue via
//!     `basic_consume` until idle (no message for `idle_timeout_ms`)
//!     or the size/byte/window limits in `RabbitBatchConfig` fire.
//!     Decodes each payload as JSON and concatenates rows into one
//!     `RecordBatch` via arrow-json.
//!   - `write_arrow_stream(target, ...)` — encodes each row as JSONL
//!     and produces via `basic_publish` to the default exchange with
//!     `routing_key = target.name`. Default-exchange routing means
//!     `target.name` is the destination queue. Custom exchanges land
//!     in 37a.x once the routing-key contract is settled.
//!   - `WriteMode::Truncate` is rejected — queues are append-style
//!     streams; purging is an admin operation.
//!
//! ## What 37a.3 adds (manual ack + at-least-once)
//!   - Persistent consumer session: `Connection` + `Channel` +
//!     `Consumer` are held in a Mutex on the backend and reused
//!     across `read_arrow_stream` calls. Closing the channel between
//!     calls would requeue all unacked deliveries (AMQP semantics);
//!     the session keeps that from happening.
//!   - `basic_consume` switches to `no_ack=false`. Each delivery's
//!     `delivery_tag` accumulates as a "pending highest tag"; the
//!     backend never acks on its own.
//!   - `commit_offsets()` (the `Backend`-trait hook used by the
//!     streaming pipeline) calls `basic_ack(highest_tag, multiple=true)`
//!     to ack every accumulated delivery in one round-trip, then
//!     clears the pending state. Mirrors Kafka 36e.
//!   - Channel-level `basic_qos` is set to `batch_size` so the broker
//!     never delivers more unacked messages than the consumer is
//!     prepared to process in one drain.
//!
//! ## What 37a.4 adds (DLQ)
//!   - `nack_pending(requeue)` — batch-nack every accumulated
//!     delivery tag in one round-trip via `basic_nack(tag,
//!     multiple=true, requeue)`. With `requeue=false`, the broker
//!     either drops the messages or routes them to the configured
//!     `x-dead-letter-exchange` (DLX) — that's standard AMQP DLX
//!     behavior, controlled at queue declaration time, not by the
//!     framework.
//!   - The streaming-pipeline-level DLQ pattern (write a failed
//!     batch to a DLQ queue via `source.write_arrow_stream`) already
//!     works for RabbitMQ via the `write_arrow_stream` we ship in
//!     37a.2 — `dlq_target.name` is the queue name; the default
//!     exchange routes by queue name.
//!
//! ### Two DLQ modes for RabbitMQ
//! | mode | what it does | when to use |
//! |--|--|--|
//! | App-level | StreamingPipeline writes the failed batch's rows to a separate queue via the source backend's `write_arrow_stream`, then commits the source. | Cross-pipeline DLQ replay (re-consume the DLQ queue identically to the primary). |
//! | Broker-level | `nack_pending(requeue=false)` lets RabbitMQ's DLX feature route the dropped delivery to a configured dead-letter exchange. | Native AMQP DLQ, separate retry/inspection queues, observability via the management plugin. |
//!
//! ## What lands in 37a.x
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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Consumer};
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RabbitBatchConfig {
    /// Max number of messages per call.
    pub batch_size: usize,
    /// Max bytes accumulated per call.
    pub batch_bytes: usize,
    /// Idle timeout (ms): if no message arrives within this window,
    /// the call returns whatever it has (possibly empty).
    pub idle_timeout_ms: u64,
}

impl Default for RabbitBatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_bytes: 16 * 1024 * 1024, // 16 MiB
            idle_timeout_ms: 2_000,
        }
    }
}

/// Persistent consumer session held across `read_arrow_stream` calls.
///
/// Closing the channel between calls would return every unacked
/// delivery to the queue — that's standard AMQP semantics, not a bug.
/// Keeping the connection / channel / consumer alive lets us defer
/// the ack until `commit_offsets()` (i.e., until the target backend
/// has durably written), which is the at-least-once primitive used
/// by `StreamingPipeline`.
struct RabbitConsumerSession {
    /// Held to keep the connection from closing on drop. Never
    /// cloned — `lapin::Connection` is `Send + Sync` already.
    _connection: Connection,
    channel: Channel,
    consumer: Consumer,
    /// Queue name the consumer is bound to. Used to validate that
    /// subsequent `read_arrow_stream` calls target the same queue
    /// (we don't yet support session-switching mid-pipeline).
    queue: String,
    /// Highest delivery tag we've seen but not yet acked. AMQP's
    /// `basic_ack(tag, multiple=true)` acks everything up to and
    /// including this tag in a single round-trip.
    pending_max_delivery_tag: Option<u64>,
}

/// RabbitMQ-backed implementation of `Backend`.
///
/// Holds the AMQP URL plus a lazily-initialized consumer session.
/// The session holds the AMQP `Connection` + `Channel` + `Consumer`
/// across `read_arrow_stream` calls so manual ack via
/// `commit_offsets()` works — closing the channel would otherwise
/// requeue the unacked deliveries.
pub struct RabbitMQBackend {
    /// Full AMQP URI (`amqp://user:pass@host:port/vhost`).
    amqp_url: String,
    /// Per-call consumer drain limits. Builder-set via
    /// `with_batch_config`.
    batch_config: RabbitBatchConfig,
    /// Stable consumer tag prefix for `basic_consume`. Defaults to
    /// "ematix-flow-consumer". Mostly informational for management
    /// UIs.
    consumer_tag: String,
    /// Lazy consumer session. Populated on first
    /// `read_arrow_stream`; reused on subsequent calls and
    /// `commit_offsets`.
    consumer_session: Arc<Mutex<Option<RabbitConsumerSession>>>,
}

impl std::fmt::Debug for RabbitMQBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitMQBackend")
            // The AMQP URL has the form
            // `amqp://user:password@host:port/vhost`. Redact the
            // password segment so logging the backend doesn't leak
            // the credential. Username + host + vhost stay visible
            // for debuggability.
            .field("amqp_url", &redact_amqp_url(&self.amqp_url))
            .field("batch_config", &self.batch_config)
            .field("consumer_tag", &self.consumer_tag)
            .finish_non_exhaustive()
    }
}

/// Strip the password segment of an AMQP URL for display.
/// `amqp://user:pw@host/vhost` → `amqp://user:<redacted>@host/vhost`.
/// URLs without credentials are returned unchanged. Keeps the rest
/// of the URL intact for debuggability.
fn redact_amqp_url(url: &str) -> String {
    let (scheme, rest) = if let Some(r) = url.strip_prefix("amqp://") {
        ("amqp://", r)
    } else if let Some(r) = url.strip_prefix("amqps://") {
        ("amqps://", r)
    } else {
        return url.to_string();
    };
    let (authority, tail) = match rest.split_once('/') {
        Some((a, t)) => (a, format!("/{t}")),
        None => (rest, String::new()),
    };
    let (userinfo, host) = match authority.split_once('@') {
        Some((u, h)) => (Some(u), h),
        None => return url.to_string(),
    };
    let user = match userinfo {
        Some(u) => u.split(':').next().unwrap_or(u),
        None => "",
    };
    if user.is_empty() {
        format!("{scheme}<redacted>@{host}{tail}")
    } else {
        format!("{scheme}{user}:<redacted>@{host}{tail}")
    }
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
        Ok(Self {
            amqp_url,
            batch_config: RabbitBatchConfig::default(),
            consumer_tag: "ematix-flow-consumer".to_string(),
            consumer_session: Arc::new(Mutex::new(None)),
        })
    }

    /// Override the per-call drain limits used by `read_arrow_stream`.
    pub fn with_batch_config(mut self, cfg: RabbitBatchConfig) -> Self {
        self.batch_config = cfg;
        self
    }

    /// Override the consumer tag prefix used by `basic_consume`.
    pub fn with_consumer_tag(mut self, tag: impl Into<String>) -> Self {
        self.consumer_tag = tag.into();
        self
    }

    /// Borrow the configured AMQP URL.
    pub fn amqp_url(&self) -> &str {
        &self.amqp_url
    }

    /// Borrow the active batch config.
    pub fn batch_config(&self) -> &RabbitBatchConfig {
        &self.batch_config
    }

    /// Borrow the consumer tag prefix.
    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    /// Test/observability hook: number of unacked deliveries
    /// accumulated since the last `commit_offsets`. Returns 0 when
    /// no session has been opened or no pending tag has accumulated.
    pub async fn pending_delivery_count(&self) -> u64 {
        // basic_ack(multiple=true) acks tag and everything below it
        // on the channel, so the highest pending tag is a coarse
        // upper bound on outstanding delivery count.
        self.consumer_session
            .lock()
            .await
            .as_ref()
            .and_then(|s| s.pending_max_delivery_tag)
            .unwrap_or_default()
    }

    /// Phase 37a.4: batch-nack every accumulated delivery tag in one
    /// round-trip via `basic_nack(highest_tag, multiple=true, requeue)`.
    /// Clears the pending state on success.
    ///
    ///   - `requeue=true`  — return the deliveries to the front of
    ///     the queue. Useful for transient errors where retry is
    ///     appropriate.
    ///   - `requeue=false` — drop the deliveries. If the source
    ///     queue was declared with an `x-dead-letter-exchange`
    ///     argument, the broker routes them to the DLX; otherwise
    ///     they're discarded silently.
    ///
    /// No-ops if no consumer session has been opened or no tag has
    /// accumulated.
    pub async fn nack_pending(&self, requeue: bool) -> Result<(), BackendError> {
        let mut session_lock = self.consumer_session.lock().await;
        let Some(session) = session_lock.as_mut() else {
            return Ok(());
        };
        let Some(tag) = session.pending_max_delivery_tag.take() else {
            return Ok(());
        };
        session
            .channel
            .basic_nack(
                tag,
                BasicNackOptions {
                    multiple: true,
                    requeue,
                },
            )
            .await
            .map_err(|e| BackendError::Query(format!("rabbitmq basic_nack: {e}")))?;
        Ok(())
    }

    /// Open the persistent consumer session: connect, create channel,
    /// set per-channel prefetch via `basic_qos`, and start a
    /// `basic_consume` with manual ack. The session lives until the
    /// backend drops.
    async fn open_consumer_session(
        &self,
        queue: &str,
    ) -> Result<RabbitConsumerSession, BackendError> {
        let connection = Connection::connect(&self.amqp_url, ConnectionProperties::default())
            .await
            .map_err(|e| BackendError::Connection(format!("rabbitmq connect: {e}")))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| BackendError::Connection(format!("rabbitmq channel: {e}")))?;
        // basic_qos prefetch_count is u16 — clamp at u16::MAX.
        let prefetch = self.batch_config.batch_size.min(u16::MAX as usize) as u16;
        channel
            .basic_qos(prefetch, BasicQosOptions::default())
            .await
            .map_err(|e| BackendError::Query(format!("rabbitmq basic_qos: {e}")))?;
        let opts = BasicConsumeOptions::default(); // no_ack defaults to false → manual ack.
        let consumer = channel
            .basic_consume(queue, &self.consumer_tag, opts, FieldTable::default())
            .await
            .map_err(|e| BackendError::Query(format!("rabbitmq basic_consume {queue}: {e}")))?;
        Ok(RabbitConsumerSession {
            _connection: connection,
            channel,
            consumer,
            queue: queue.to_string(),
            pending_max_delivery_tag: None,
        })
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

    fn config(&self) -> crate::backend::BackendConfig {
        // Σ.B follow-up: full builder-state round-trip. The default
        // `consumer_tag` "ematix-flow-consumer" rides through as a
        // None so the JSON stays minimal in the common case;
        // populated only when the operator overrides it.
        let consumer_tag_default = self.consumer_tag == "ematix-flow-consumer";
        crate::backend::BackendConfig::RabbitMq(crate::backend::RabbitMqConfig {
            amqp_url: self.amqp_url.clone(),
            consumer_tag: if consumer_tag_default {
                None
            } else {
                Some(self.consumer_tag.clone())
            },
            batch_config: Some(self.batch_config.clone()),
        })
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

    /// Subscribe to `query` (the queue name) via `basic_consume`,
    /// drain messages until idle for `batch_config.idle_timeout_ms`
    /// or one of the batch_size / batch_bytes limits is hit. Each
    /// payload is decoded as a single JSON object and rows are
    /// concatenated into one Arrow `RecordBatch`. Schema is inferred
    /// from the first 1024 messages (arrow-json default).
    ///
    /// Returns an empty stream if the queue is empty (idle timeout
    /// fires immediately).
    ///
    /// Acks are deferred. The accumulated highest delivery tag is
    /// remembered on the persistent consumer session and acked in
    /// one round-trip when `commit_offsets()` fires — which the
    /// `StreamingPipeline` triggers after the target backend has
    /// durably written. This is the at-least-once primitive that
    /// mirrors Kafka 36e.
    ///
    /// Limits in 37a.3 — folded out in later sub-phases:
    ///   - JSON-only payload decode (raw bytes / Avro / Protobuf
    ///     land later, mirroring Kafka's 36h trajectory).
    ///   - Single queue per backend instance: the first call wins
    ///     the queue binding; subsequent calls must target the same
    ///     queue. Multi-queue fanout would need one
    ///     `RabbitMQBackend` per queue.
    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        let queue = query.trim();
        if queue.is_empty() {
            return Err(BackendError::Other(
                "RabbitMQ read_arrow_stream: query argument must be a non-empty queue name".into(),
            ));
        }

        let cfg = self.batch_config.clone();
        let mut session_lock = self.consumer_session.lock().await;
        if session_lock.is_none() {
            *session_lock = Some(self.open_consumer_session(queue).await?);
        }
        let session = session_lock.as_mut().expect("session populated above");
        if session.queue != queue {
            return Err(BackendError::Other(format!(
                "RabbitMQ read_arrow_stream: this backend instance is already \
                 bound to queue `{}`; multi-queue fanout would need separate \
                 RabbitMQBackend instances",
                session.queue
            )));
        }

        let idle = Duration::from_millis(cfg.idle_timeout_ms);
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        let mut bytes_total: usize = 0;
        let mut max_tag = session.pending_max_delivery_tag;
        loop {
            match tokio::time::timeout(idle, session.consumer.next()).await {
                Ok(Some(Ok(delivery))) => {
                    bytes_total += delivery.data.len();
                    let tag = delivery.delivery_tag;
                    payloads.push(delivery.data);
                    max_tag = Some(match max_tag {
                        Some(m) if m > tag => m,
                        _ => tag,
                    });
                    if payloads.len() >= cfg.batch_size || bytes_total >= cfg.batch_bytes {
                        break;
                    }
                }
                Ok(Some(Err(e))) => {
                    return Err(BackendError::Query(format!("rabbitmq consume: {e}")));
                }
                // Stream ended (channel closed) or idle timeout — flush.
                Ok(None) | Err(_) => break,
            }
        }
        session.pending_max_delivery_tag = max_tag;
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

    /// Produce each Arrow row as an AMQP message to the default
    /// exchange with `routing_key = target.name` — i.e., publish
    /// directly to a queue with that name. Each batch is encoded as
    /// JSONL and split row-wise; one `basic_publish` per row.
    /// `WriteMode::Truncate` is rejected — queues aren't truncatable
    /// from a producer.
    ///
    /// Limits in 37a.2 — folded out in later sub-phases:
    ///   - JSON-only payload encode (mirrors Kafka 36h).
    ///   - Default exchange only — `target.name` is the queue name.
    ///     Custom exchange + routing-key support lands in a 37a.x
    ///     follow-up once the contract is settled.
    ///   - No publisher confirms — basic_publish returns once the
    ///     local send completes. 37a.3 wires confirms + manual-ack
    ///     coordination on the consume side.
    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError> {
        if mode == WriteMode::Truncate {
            return Err(BackendError::Other(
                "RabbitMQ write_arrow_stream: Truncate is not supported on a queue. \
                 Queues are append-style streams; to start fresh, purge or delete \
                 the queue via the RabbitMQ management API or admin tools."
                    .into(),
            ));
        }
        let queue = target.name.trim();
        if queue.is_empty() {
            return Err(BackendError::Other(
                "RabbitMQ write_arrow_stream: target.name (queue) must be non-empty".into(),
            ));
        }

        let conn = Connection::connect(&self.amqp_url, ConnectionProperties::default())
            .await
            .map_err(|e| BackendError::Connection(format!("rabbitmq connect: {e}")))?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| BackendError::Connection(format!("rabbitmq channel: {e}")))?;

        let mut s = stream;
        let mut total: u64 = 0;
        while let Some(batch) = s.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let payloads = encode_batch_as_jsonl_lines(&batch)?;
            for payload in &payloads {
                channel
                    .basic_publish(
                        "", // default exchange
                        queue,
                        BasicPublishOptions::default(),
                        payload,
                        BasicProperties::default(),
                    )
                    .await
                    .map_err(|e| {
                        BackendError::Query(format!("rabbitmq basic_publish {queue}: {e}"))
                    })?;
                total += 1;
            }
        }
        let _ = channel.close(0, "ematix-flow write done").await;
        let _ = conn.close(0, "ematix-flow write done").await;
        Ok(total)
    }

    /// Cross-backend produce-side run_append: read source rows via
    /// the source's `read_arrow_stream`, produce them to `spec.name`
    /// (the queue) via `write_arrow_stream`. Mirrors the Kafka
    /// produce-side run_append shape (cross-backend by design;
    /// `source_backend` is required).
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
                "RabbitMQ run_append: source_backend is required (cross-backend produce)".into(),
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
                path: format!("rabbitmq:{}", spec.name),
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
            path: format!("rabbitmq:{}", spec.name),
        })
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

    /// Manually ack everything we've drained since the last commit.
    /// Uses `basic_ack(highest_tag, multiple=true)` so all
    /// accumulated deliveries are acked in one round-trip. No-ops
    /// if there are no pending tags or no consumer session has been
    /// opened.
    async fn commit_offsets(&self) -> Result<(), BackendError> {
        let mut session_lock = self.consumer_session.lock().await;
        let Some(session) = session_lock.as_mut() else {
            return Ok(());
        };
        let Some(tag) = session.pending_max_delivery_tag.take() else {
            return Ok(());
        };
        session
            .channel
            .basic_ack(tag, BasicAckOptions { multiple: true })
            .await
            .map_err(|e| BackendError::Query(format!("rabbitmq basic_ack: {e}")))?;
        Ok(())
    }

    /// Phase 39.5a P1.7a: RabbitMQ uses broker-tracked offsets via
    /// `basic_ack` / `basic_nack`. Acked messages are removed; on
    /// restart the unacked tail is automatically re-delivered.
    /// There's no seek primitive — and no need for one. Reporting
    /// `true` lets stateful pipelines accept RabbitMQ as a source.
    fn supports_seek_to(&self) -> bool {
        true
    }

    async fn seek_to(&self, _offset_bytes: &[u8]) -> Result<(), BackendError> {
        Ok(())
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
    async fn read_arrow_stream_rejects_empty_queue_name() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        let err = match b.read_arrow_stream("   ").await {
            Ok(_) => panic!("expected empty-queue rejection"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("non-empty queue name"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_arrow_stream_truncate_rejects() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
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
    async fn write_arrow_stream_rejects_empty_queue_name() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
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
        let cfg = RabbitBatchConfig::default();
        assert!(cfg.batch_size >= 100);
        assert!(cfg.batch_bytes >= 1 << 20);
        assert!(cfg.idle_timeout_ms >= 500);
    }

    #[test]
    fn with_batch_config_overrides_default() {
        let b = RabbitMQBackend::open("amqp://localhost")
            .unwrap()
            .with_batch_config(RabbitBatchConfig {
                batch_size: 7,
                batch_bytes: 99,
                idle_timeout_ms: 11,
            });
        assert_eq!(b.batch_config().batch_size, 7);
        assert_eq!(b.batch_config().batch_bytes, 99);
        assert_eq!(b.batch_config().idle_timeout_ms, 11);
    }

    #[test]
    fn with_consumer_tag_overrides_default() {
        let b = RabbitMQBackend::open("amqp://localhost")
            .unwrap()
            .with_consumer_tag("my-tag");
        assert_eq!(b.consumer_tag(), "my-tag");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_append_requires_source_backend() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        let spec = TableSpec {
            schema: "".into(),
            name: "q".into(),
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

    /// Credential-safety: Debug must redact the password segment
    /// of the amqp_url so logs don't leak it. Username + host stay
    /// visible.
    #[test]
    fn debug_redacts_amqp_password() {
        let b = RabbitMQBackend::open("amqp://alice:s3cret@broker.local:5672/%2f").unwrap();
        let s = format!("{b:?}");
        assert!(!s.contains("s3cret"), "Debug leaks password: {s}");
        assert!(s.contains("alice"), "Debug should keep username: {s}");
        assert!(s.contains("broker.local"), "Debug should keep host: {s}");
        assert!(s.contains("<redacted>"), "expected <redacted> marker: {s}");
    }

    #[test]
    fn redact_amqp_url_handles_common_shapes() {
        // No credentials → unchanged.
        assert_eq!(
            redact_amqp_url("amqp://localhost:5672/"),
            "amqp://localhost:5672/"
        );
        // With credentials → password redacted.
        assert_eq!(
            redact_amqp_url("amqp://alice:s3cret@host:5672/vh"),
            "amqp://alice:<redacted>@host:5672/vh"
        );
        // amqps stays amqps.
        assert_eq!(
            redact_amqp_url("amqps://u:p@host"),
            "amqps://u:<redacted>@host"
        );
        // Username-only userinfo.
        assert_eq!(
            redact_amqp_url("amqp://justuser@host/v"),
            "amqp://justuser:<redacted>@host/v"
        );
        // Non-AMQP scheme passes through (defensive).
        assert_eq!(redact_amqp_url("http://x"), "http://x");
    }

    /// 37a.3: commit_offsets is a no-op before any consumer session
    /// has been opened. Lets pipelines call it unconditionally.
    #[tokio::test(flavor = "multi_thread")]
    async fn commit_offsets_noop_without_consumer_session() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        b.commit_offsets().await.unwrap();
        assert_eq!(b.pending_delivery_count().await, 0);
    }

    /// 37a.4: nack_pending is a no-op before any consumer session has
    /// been opened, and is idempotent across both requeue=true and
    /// requeue=false.
    #[tokio::test(flavor = "multi_thread")]
    async fn nack_pending_noop_without_consumer_session() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        b.nack_pending(true).await.unwrap();
        b.nack_pending(false).await.unwrap();
        assert_eq!(b.pending_delivery_count().await, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_append_dry_run_returns_zero_inserts_without_io() {
        let b = RabbitMQBackend::open("amqp://localhost").unwrap();
        let spec = TableSpec {
            schema: "".into(),
            name: "q".into(),
            columns: vec![],
            unique_constraints: vec![],
            fingerprint: String::new(),
        };
        // Construct a noop "source" via the rabbit backend itself —
        // dry_run skips IO before it actually calls the source.
        let other = RabbitMQBackend::open("amqp://localhost").unwrap();
        let res = b
            .run_append(&spec, "x", "p", Some(&other), None, None, true)
            .await
            .unwrap();
        assert_eq!(res.rows_inserted, 0);
        assert_eq!(res.status, "dry-run");
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
