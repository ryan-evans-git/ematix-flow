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

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

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

        // Default to `earliest` so a fresh consumer reads the topic
        // from the start. Long-running consumers in 36g may want
        // `latest` instead and can override the config; the batch
        // `read_arrow_stream` path matches the "drain the topic"
        // expectation users have when they hand a SQL backend a
        // `SELECT * FROM …` query.
        let mut config = self.client_config();
        config.set("auto.offset.reset", "earliest");
        let consumer: StreamConsumer = config
            .create()
            .map_err(|e| BackendError::Connection(format!("kafka consumer create: {e}")))?;
        consumer
            .subscribe(&[topic])
            .map_err(|e| BackendError::Connection(format!("kafka subscribe {topic}: {e}")))?;

        // Drain the consumer into a Vec<Vec<u8>> of payloads.
        let payloads = drain_consumer(&consumer).await?;
        if payloads.is_empty() {
            let stream = futures_util::stream::empty();
            return Ok(Box::pin(stream));
        }
        let batches = decode_payloads_as_jsonl(payloads)?;
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
        let producer: FutureProducer = self
            .client_config()
            .create()
            .map_err(|e| BackendError::Connection(format!("kafka producer create: {e}")))?;

        let mut s = stream;
        let mut total: u64 = 0;
        while let Some(batch) = futures_util::StreamExt::next(&mut s).await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let payloads = encode_batch_as_jsonl_lines(&batch)?;
            for payload in &payloads {
                // The 5s timeout caps how long each produce can wait
                // for broker ack. For real-AWS-MSK / Confluent Cloud
                // this is generous; for slow networks the user
                // should set `message.timeout.ms` on the underlying
                // ClientConfig (future builder method).
                producer
                    .send(
                        FutureRecord::<(), [u8]>::to(topic).payload(payload.as_slice()),
                        Timeout::After(Duration::from_secs(5)),
                    )
                    .await
                    .map_err(|(e, _msg)| {
                        BackendError::Query(format!("kafka produce {topic}: {e}"))
                    })?;
                total += 1;
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
}

/// How long to wait for the first message after subscribing. The
/// consumer's initial group join + partition assignment + offset
/// fetch can take several seconds, so the first-message budget is
/// larger than the subsequent-message one.
const READ_FIRST_MESSAGE_TIMEOUT_SECS: u64 = 15;

/// How long to wait without a new message before declaring the
/// batch-read drained, after the first message has arrived.
/// `recv()` blocks until a message arrives, so we wrap it in a
/// tokio timeout. A long-running streaming consumer (36g) doesn't
/// use this path.
const READ_IDLE_TIMEOUT_SECS: u64 = 5;

/// Maximum messages collected per `read_arrow_stream` call. Cap
/// keeps the in-memory accumulator bounded for unbounded topics.
/// Larger reads should drive the streaming-consumer path (36g)
/// where backpressure + checkpointing are first-class.
const READ_MAX_MESSAGES: usize = 100_000;

/// Drain `consumer` into a `Vec<Vec<u8>>` of payloads. Stops at
/// `READ_IDLE_TIMEOUT_SECS` of silence, or after `READ_MAX_MESSAGES`
/// messages, whichever comes first. Messages with no payload (Kafka
/// tombstones) are skipped — they're meaningful for compacted topics
/// but don't carry a row to decode.
async fn drain_consumer(consumer: &StreamConsumer) -> Result<Vec<Vec<u8>>, BackendError> {
    let mut payloads: Vec<Vec<u8>> = Vec::new();
    while payloads.len() < READ_MAX_MESSAGES {
        // First-message timeout is generous (group rebalance + offset
        // fetch can take several seconds on a fresh broker);
        // subsequent timeouts are tight because we're "draining" a
        // known-active stream.
        let timeout_secs = if payloads.is_empty() {
            READ_FIRST_MESSAGE_TIMEOUT_SECS
        } else {
            READ_IDLE_TIMEOUT_SECS
        };
        let recv_fut = consumer.recv();
        let msg = tokio::time::timeout(Duration::from_secs(timeout_secs), recv_fut).await;
        match msg {
            Ok(Ok(borrowed)) => {
                if let Some(payload) = borrowed.payload() {
                    payloads.push(payload.to_vec());
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
            Err(_elapsed) => break, // idle timeout → stop
        }
    }
    Ok(payloads)
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
