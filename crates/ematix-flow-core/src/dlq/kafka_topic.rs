//! `KafkaTopicDlq` — the Kafka-topic `DeadLetterStore`.
//!
//! Today's format-preserving DLQ topic emission, upgraded: the
//! payload is produced **as-is** (byte-identical to the pre-Phase-1
//! behavior) and every [`DlqMeta`] field travels in Kafka HEADERS
//! under `emat-dlq-*` keys. A pre-existing DLQ consumer keeps
//! working unchanged; a metadata-aware consumer (the Phase 2 replay
//! engine, the UI) reads the headers.
//!
//! ## Semantics mapping (nearest honest per topic mechanics)
//!
//! A Kafka topic is an append-only log with per-group offsets — it
//! cannot express every table-store operation. This impl documents
//! the mapping explicitly and returns typed
//! [`DlqError::Unsupported`] for the inexpressible rather than
//! faking semantics:
//!
//! | trait op            | topic mapping |
//! |---------------------|---------------|
//! | `append`            | produce payload + `emat-dlq-*` headers, await broker ack per message |
//! | `depth.pending`     | Σ per-partition (end offset − replay-group committed) |
//! | `depth.parked`      | always 0 — parking is inexpressible |
//! | `browse`            | bounded peek from the replay group's committed offsets with a **non-joining** assigned consumer, no commits; `status_filter` `Leased`/`Parked` → `Unsupported` |
//! | `take_for_replay`   | consume forward from committed offsets; `Ids` selection → `Unsupported` |
//! | `ack_replayed`      | commit per-partition offsets covering the acked records to the replay group |
//! | `park`              | `Unsupported` |
//! | `purge(All)`        | seek-to-end: commit end offsets for the replay group (records stay on the topic per retention — Kafka has no per-record delete) |
//! | `purge(FirstN)`     | consume + commit the first n pending |
//! | `purge(Ids)`        | `Unsupported` |
//!
//! ## Lease semantics (IMPORTANT for Phase 2)
//!
//! The lease is **process-local + group-offset based**, weaker than
//! `TableDlq`'s row lease:
//!
//! - Within one process, taken-but-unacked offsets are remembered
//!   (per partition) and excluded from further takes until the
//!   caller-provided `now_ms` passes the lease deadline — matching
//!   the contract's exclusivity + expiry behavior.
//! - **Across processes** the only durable cursor is the replay
//!   group's committed offset: a second process could re-take
//!   messages a first process has taken but not yet acked. That is
//!   at-least-once, not exclusive. Concurrent replays of the same
//!   pipeline from different processes must be serialized one layer
//!   up (Phase 2 concern; the table store needs no such care).
//! - `ack_replayed` commits the **highest** acked offset + 1 per
//!   partition: acking record X implicitly acks any lower-offset
//!   taken record in the same partition. The replay engine must ack
//!   in-order per partition or accept implicit acks (a warning is
//!   logged when this collapses unacked ids).
//!
//! ## Cross-partition ordering
//!
//! `browse`/`take` order records by (broker timestamp, partition,
//! offset) — exact append order within a partition, approximate
//! across partitions. Single-partition DLQ topics (the common case)
//! get exact ordering.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{BorrowedMessage, Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;

use crate::backend::{Backend, BackendError};
use crate::kafka_backend::{EmatixKafkaContext, KafkaBackend};

use super::{
    DeadLetterStore, DlqDepth, DlqError, DlqMeta, DlqRecord, DlqRecordId, DlqRecordStatus,
    DlqSelection, DlqStage, truncate_error,
};

/// Header keys. Every `DlqMeta` field rides one `emat-dlq-*`
/// header; absent headers on legacy (pre-Phase-1) messages fall
/// back to documented defaults in `parse_message`.
const H_ID: &str = "emat-dlq-id";
const H_PIPELINE: &str = "emat-dlq-pipeline";
const H_STAGE: &str = "emat-dlq-stage";
const H_ERROR: &str = "emat-dlq-error";
const H_SOURCE_ID: &str = "emat-dlq-source-id";
const H_OFFSET_BYTES: &str = "emat-dlq-offset-bytes";
const H_EVENT_TS: &str = "emat-dlq-event-ts";
const H_FAILED_AT: &str = "emat-dlq-failed-at";
const H_ATTEMPT: &str = "emat-dlq-attempt";
const H_PAYLOAD_FORMAT: &str = "emat-dlq-payload-format";

/// Broker-facing timeouts. Metadata/watermark/commit calls get the
/// OP timeout; per-message poll uses POLL; a browse/take gives up
/// filling its budget after IDLE without a message.
const OP_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_TIMEOUT: Duration = Duration::from_millis(250);
const IDLE_TIMEOUT: Duration = Duration::from_secs(3);

/// Group-coordinator ops (`committed_offsets` / `commit`) fail with
/// transient errors while a broker (re-)elects the coordinator —
/// e.g. `NotCoordinator` / `CoordinatorLoadInProgress` on a freshly
/// started single-broker cluster. Retry with a bounded backoff
/// before surfacing; only called from `spawn_blocking` contexts so
/// the thread sleep is fine.
fn retry_group_op<T>(
    what: &str,
    mut op: impl FnMut() -> Result<T, rdkafka::error::KafkaError>,
) -> Result<T, BackendError> {
    const ATTEMPTS: u32 = 20;
    const BACKOFF: Duration = Duration::from_millis(500);
    let mut last_err: Option<rdkafka::error::KafkaError> = None;
    for attempt in 0..ATTEMPTS {
        match op() {
            Ok(v) => return Ok(v),
            Err(e) => {
                tracing::debug!(
                    what,
                    attempt,
                    error = %e,
                    "kafka dlq group op failed; retrying"
                );
                last_err = Some(e);
                std::thread::sleep(BACKOFF);
            }
        }
    }
    Err(BackendError::Query(format!(
        "kafka dlq {what}: {e} (after {ATTEMPTS} attempts)",
        e = last_err.expect("at least one attempt ran"),
    )))
}

/// In-process lease bookkeeping (see module docs — process-local by
/// construction, offset-committed across processes).
#[derive(Debug, Default)]
struct LeaseState {
    /// partition → next offset a future take may read from (i.e.
    /// one past the highest currently-leased offset).
    next: HashMap<i32, i64>,
    /// Lease deadline (ms since epoch) after which `next` resets to
    /// the committed offsets. One deadline for the whole in-flight
    /// take set — takes extend it.
    until: i64,
    /// Taken-but-unacked record ids → (partition, offset).
    ids: HashMap<String, (i32, i64)>,
}

/// Kafka-topic [`DeadLetterStore`]. Bound to exactly one pipeline's
/// DLQ topic; produces via the SOURCE backend's own producer (same
/// bootstrap + auth — identical to the historical DLQ path).
pub struct KafkaTopicDlq {
    /// The pipeline's (first) source backend. Must be Kafka —
    /// validated at construction via [`Backend::as_kafka`].
    source: Arc<dyn Backend>,
    topic: String,
    pipeline: String,
    /// Consumer group holding the replay cursor. Deterministic per
    /// pipeline so depth/take/ack agree across restarts.
    replay_group: String,
    lease: Mutex<LeaseState>,
}

impl std::fmt::Debug for KafkaTopicDlq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaTopicDlq")
            .field("topic", &self.topic)
            .field("pipeline", &self.pipeline)
            .field("replay_group", &self.replay_group)
            .finish()
    }
}

impl KafkaTopicDlq {
    /// Wrap `source` (must be a Kafka backend) as the DLQ store for
    /// `pipeline`, writing to `topic`.
    pub fn new(
        source: Arc<dyn Backend>,
        topic: impl Into<String>,
        pipeline: impl Into<String>,
    ) -> Result<Self, BackendError> {
        if source.as_kafka().is_none() {
            return Err(BackendError::Other(
                "KafkaTopicDlq requires a Kafka source backend (the DLQ topic is \
                 produced with the source's own client config + auth)"
                    .into(),
            ));
        }
        let pipeline = pipeline.into();
        Ok(Self {
            source,
            topic: topic.into(),
            replay_group: format!("emat-dlq-replay-{pipeline}"),
            pipeline,
            lease: Mutex::new(LeaseState::default()),
        })
    }

    /// The replay consumer-group id (test + Phase 2 visibility).
    pub fn replay_group(&self) -> &str {
        &self.replay_group
    }

    fn kafka(&self) -> &KafkaBackend {
        self.source
            .as_kafka()
            .expect("validated at construction: source is Kafka")
    }

    /// Per-partition `(low, high, committed_start)` for the DLQ
    /// topic under the replay group. `committed_start` is where the
    /// pending window begins: `max(committed, low)`, or `low` when
    /// the group has never committed.
    fn partition_windows(
        consumer: &BaseConsumer<EmatixKafkaContext>,
        topic: &str,
    ) -> Result<Vec<(i32, i64, i64, i64)>, BackendError> {
        let metadata = consumer
            .fetch_metadata(Some(topic), OP_TIMEOUT)
            .map_err(|e| BackendError::Query(format!("kafka dlq metadata {topic}: {e}")))?;
        let Some(t) = metadata.topics().iter().find(|t| t.name() == topic) else {
            // Topic doesn't exist yet (nothing ever dead-lettered):
            // an empty window set, not an error.
            return Ok(Vec::new());
        };
        let partitions: Vec<i32> = t.partitions().iter().map(|p| p.id()).collect();
        if partitions.is_empty() {
            return Ok(Vec::new());
        }

        let committed = retry_group_op(&format!("committed {topic}"), || {
            let mut tpl = TopicPartitionList::new();
            for p in &partitions {
                tpl.add_partition(topic, *p);
            }
            consumer.committed_offsets(tpl, OP_TIMEOUT)
        })?;

        let mut out = Vec::with_capacity(partitions.len());
        for p in partitions {
            let (low, high) = consumer
                .fetch_watermarks(topic, p, OP_TIMEOUT)
                .map_err(|e| {
                    BackendError::Query(format!("kafka dlq watermarks {topic}/{p}: {e}"))
                })?;
            // Never-committed partitions come back as
            // Offset::Invalid; anything non-concrete starts at the
            // low watermark.
            let start = match committed.find_partition(topic, p).map(|e| e.offset()) {
                Some(rdkafka::Offset::Offset(o)) => o.max(low),
                _ => low,
            };
            out.push((p, low, high, start));
        }
        Ok(out)
    }

    /// Blocking bounded read: assign `starts` and poll until
    /// `budget` messages are collected, the per-partition highs are
    /// exhausted, or the idle timeout fires. Returns raw
    /// `(partition, offset, timestamp_ms, record)` tuples sorted by
    /// (timestamp, partition, offset).
    fn read_window(
        consumer: &BaseConsumer<EmatixKafkaContext>,
        topic: &str,
        pipeline: &str,
        starts: &[(i32, i64, i64)], // (partition, start, high)
        budget: usize,
    ) -> Result<Vec<(i32, i64, i64, DlqRecord)>, BackendError> {
        let available: u64 = starts
            .iter()
            .map(|(_, start, high)| (high - start).max(0) as u64)
            .sum();
        let want = (budget as u64).min(available) as usize;
        if want == 0 {
            return Ok(Vec::new());
        }

        let mut tpl = TopicPartitionList::new();
        for (p, start, high) in starts {
            if start < high {
                tpl.add_partition_offset(topic, *p, rdkafka::Offset::Offset(*start))
                    .map_err(|e| BackendError::Other(format!("kafka dlq tpl: {e}")))?;
            }
        }
        consumer
            .assign(&tpl)
            .map_err(|e| BackendError::Query(format!("kafka dlq assign {topic}: {e}")))?;

        let mut collected: Vec<(i32, i64, i64, DlqRecord)> = Vec::with_capacity(want);
        let mut idle_since = std::time::Instant::now();
        while collected.len() < want {
            match consumer.poll(POLL_TIMEOUT) {
                Some(Ok(msg)) => {
                    idle_since = std::time::Instant::now();
                    let ts = msg.timestamp().to_millis().unwrap_or(0);
                    let record = parse_message(&msg, pipeline);
                    collected.push((msg.partition(), msg.offset(), ts, record));
                }
                Some(Err(e)) => {
                    return Err(BackendError::Query(format!("kafka dlq poll {topic}: {e}")));
                }
                None => {
                    if idle_since.elapsed() >= IDLE_TIMEOUT {
                        break;
                    }
                }
            }
        }
        consumer.unassign().ok();
        collected.sort_by_key(|(p, o, ts, _)| (*ts, *p, *o));
        Ok(collected)
    }
}

/// Build the `emat-dlq-*` header set for one record. The error is
/// (re-)truncated to 8KB — Kafka header values count against the
/// broker's max message size.
fn build_headers(record: &DlqRecord) -> OwnedHeaders {
    let meta = &record.meta;
    let error = truncate_error(&meta.error);
    let mut headers = OwnedHeaders::new()
        .insert(Header {
            key: H_ID,
            value: Some(record.id.0.as_bytes()),
        })
        .insert(Header {
            key: H_PIPELINE,
            value: Some(meta.pipeline.as_bytes()),
        })
        .insert(Header {
            key: H_STAGE,
            value: Some(meta.stage.as_str().as_bytes()),
        })
        .insert(Header {
            key: H_ERROR,
            value: Some(error.as_bytes()),
        })
        .insert(Header {
            key: H_SOURCE_ID,
            value: Some(meta.source_id.as_bytes()),
        })
        .insert(Header {
            key: H_FAILED_AT,
            value: Some(meta.failed_at.to_string().as_bytes()),
        })
        .insert(Header {
            key: H_ATTEMPT,
            value: Some(meta.attempt.to_string().as_bytes()),
        })
        .insert(Header {
            key: H_PAYLOAD_FORMAT,
            value: Some(meta.payload_format.as_bytes()),
        });
    if let Some(offset_bytes) = &meta.offset_bytes {
        headers = headers.insert(Header {
            key: H_OFFSET_BYTES,
            value: Some(offset_bytes.as_slice()),
        });
    }
    if let Some(event_ts) = meta.event_ts {
        headers = headers.insert(Header {
            key: H_EVENT_TS,
            value: Some(event_ts.to_string().as_bytes()),
        });
    }
    headers
}

/// Decode one consumed message into a `DlqRecord`. Legacy messages
/// (produced before Phase 1, no headers) surface with the payload
/// intact and documented meta defaults: id = `"{partition}:{offset}"`,
/// stage = `Write` (the historical DLQ path), empty error/source,
/// `failed_at` = broker timestamp, attempt = 1.
fn parse_message(msg: &BorrowedMessage<'_>, pipeline: &str) -> DlqRecord {
    let mut header_map: HashMap<&str, Vec<u8>> = HashMap::new();
    if let Some(headers) = msg.headers() {
        for h in headers.iter() {
            if let Some(v) = h.value {
                header_map.insert(h.key, v.to_vec());
            }
        }
    }
    let text = |key: &str| -> Option<String> {
        header_map
            .get(key)
            .map(|v| String::from_utf8_lossy(v).into_owned())
    };
    let int = |key: &str| -> Option<i64> { text(key).and_then(|s| s.parse().ok()) };

    let id = text(H_ID).unwrap_or_else(|| format!("{}:{}", msg.partition(), msg.offset()));
    DlqRecord {
        id: DlqRecordId(id),
        meta: DlqMeta {
            pipeline: text(H_PIPELINE).unwrap_or_else(|| pipeline.to_string()),
            stage: text(H_STAGE)
                .and_then(|s| DlqStage::parse(&s))
                .unwrap_or(DlqStage::Write),
            error: text(H_ERROR).unwrap_or_default(),
            source_id: text(H_SOURCE_ID).unwrap_or_default(),
            offset_bytes: header_map.get(H_OFFSET_BYTES).cloned(),
            event_ts: int(H_EVENT_TS),
            failed_at: int(H_FAILED_AT).unwrap_or_else(|| msg.timestamp().to_millis().unwrap_or(0)),
            attempt: int(H_ATTEMPT).map(|a| a.max(1) as u32).unwrap_or(1),
            payload_format: text(H_PAYLOAD_FORMAT).unwrap_or_else(|| "json".to_string()),
        },
        payload: msg.payload().map(|p| p.to_vec()).unwrap_or_default(),
    }
}

#[async_trait::async_trait]
impl DeadLetterStore for KafkaTopicDlq {
    /// Produce payload-as-is + metadata headers, awaiting the
    /// broker ack per message (the pipeline commits source offsets
    /// only after this resolves — at-least-once preserved).
    async fn append(&self, records: Vec<DlqRecord>) -> Result<(), DlqError> {
        if records.is_empty() {
            return Ok(());
        }
        let producer = self.kafka().acquire_producer().await?;
        for record in &records {
            let headers = build_headers(record);
            producer
                .send(
                    FutureRecord::<(), [u8]>::to(&self.topic)
                        .payload(record.payload.as_slice())
                        .headers(headers),
                    Timeout::After(Duration::from_secs(5)),
                )
                .await
                .map_err(|(e, _msg)| {
                    BackendError::Query(format!("kafka dlq produce {}: {e}", self.topic))
                })?;
        }
        Ok(())
    }

    /// pending = Σ (end offset − replay-group committed);
    /// parked = 0 always (inexpressible on a topic — see module
    /// docs).
    async fn depth(&self, _pipeline: &str) -> Result<DlqDepth, DlqError> {
        let this = self.clone_handles();
        let pending = tokio::task::spawn_blocking(move || -> Result<u64, BackendError> {
            let consumer = this.replay_consumer()?;
            let windows = Self::partition_windows(&consumer, &this.topic)?;
            Ok(windows
                .iter()
                .map(|(_, _, high, start)| (high - start).max(0) as u64)
                .sum())
        })
        .await
        .map_err(|e| BackendError::Other(format!("kafka dlq depth join: {e}")))??;
        Ok(DlqDepth { pending, parked: 0 })
    }

    /// Bounded peek from the replay group's committed offsets with
    /// a non-joining assigned consumer; nothing is committed.
    /// `status_filter`: `None`/`Some(Pending)` browse the pending
    /// window; `Leased`/`Parked` are inexpressible → `Unsupported`.
    async fn browse(
        &self,
        _pipeline: &str,
        page: u64,
        page_size: u64,
        status_filter: Option<DlqRecordStatus>,
    ) -> Result<Vec<DlqRecord>, DlqError> {
        match status_filter {
            None | Some(DlqRecordStatus::Pending) => {}
            Some(s) => {
                return Err(DlqError::Unsupported(format!(
                    "KafkaTopicDlq cannot browse by status '{}': a topic only \
                     exposes the pending window (committed offset → end). Use the \
                     table store for lease/park visibility.",
                    s.as_str()
                )));
            }
        }
        let this = self.clone_handles();
        let pipeline = self.pipeline.clone();
        let budget = page
            .saturating_add(1)
            .saturating_mul(page_size)
            .min(usize::MAX as u64) as usize;
        let skip = page.saturating_mul(page_size) as usize;
        let records =
            tokio::task::spawn_blocking(move || -> Result<Vec<DlqRecord>, BackendError> {
                let consumer = this.replay_consumer()?;
                let windows = Self::partition_windows(&consumer, &this.topic)?;
                let starts: Vec<(i32, i64, i64)> = windows
                    .iter()
                    .map(|(p, _, high, start)| (*p, *start, *high))
                    .collect();
                let collected =
                    Self::read_window(&consumer, &this.topic, &pipeline, &starts, budget)?;
                Ok(collected
                    .into_iter()
                    .skip(skip)
                    .map(|(_, _, _, r)| r)
                    .collect())
            })
            .await
            .map_err(|e| BackendError::Other(format!("kafka dlq browse join: {e}")))??;
        Ok(records)
    }

    /// Consume forward from the replay group's committed offsets
    /// (skipping in-process leases still inside their window).
    /// `Ids` selection is inexpressible → `Unsupported`. See module
    /// docs for the (weaker) cross-process lease semantics.
    async fn take_for_replay(
        &self,
        _pipeline: &str,
        selection: DlqSelection,
        lease: Duration,
        now_ms: i64,
    ) -> Result<Vec<DlqRecord>, DlqError> {
        let budget: usize = match &selection {
            DlqSelection::All => usize::MAX,
            DlqSelection::FirstN(n) => usize::try_from(*n).unwrap_or(usize::MAX),
            DlqSelection::Ids(_) => {
                return Err(DlqError::Unsupported(
                    "KafkaTopicDlq cannot take records by id: a topic is consumed \
                     in offset order, not addressed by key. Use FirstN/All, or the \
                     table store for id-addressed replay."
                        .into(),
                ));
            }
        };

        // Lease gate: start each partition at max(committed,
        // in-process leased-next) unless the previous lease
        // expired (then committed wins and unacked takes become
        // re-takeable — the contract's expiry behavior).
        let lease_next: HashMap<i32, i64> = {
            let state = self
                .lease
                .lock()
                .expect("KafkaTopicDlq lease mutex poisoned");
            if state.until > now_ms {
                state.next.clone()
            } else {
                HashMap::new()
            }
        };

        let this = self.clone_handles();
        let pipeline = self.pipeline.clone();
        let taken = tokio::task::spawn_blocking(
            move || -> Result<Vec<(i32, i64, i64, DlqRecord)>, BackendError> {
                let consumer = this.replay_consumer()?;
                let windows = Self::partition_windows(&consumer, &this.topic)?;
                let starts: Vec<(i32, i64, i64)> = windows
                    .iter()
                    .map(|(p, _, high, start)| {
                        let gated = lease_next.get(p).copied().unwrap_or(*start).max(*start);
                        (*p, gated, *high)
                    })
                    .collect();
                Self::read_window(&consumer, &this.topic, &pipeline, &starts, budget)
            },
        )
        .await
        .map_err(|e| BackendError::Other(format!("kafka dlq take join: {e}")))??;

        // Record the new lease window.
        if !taken.is_empty() {
            let mut state = self
                .lease
                .lock()
                .expect("KafkaTopicDlq lease mutex poisoned");
            if state.until <= now_ms {
                // Previous lease expired — drop its bookkeeping.
                state.next.clear();
                state.ids.clear();
            }
            for (p, o, _, r) in &taken {
                let next = state.next.entry(*p).or_insert(0);
                *next = (*next).max(o + 1);
                state.ids.insert(r.id.0.clone(), (*p, *o));
            }
            state.until =
                now_ms.saturating_add(i64::try_from(lease.as_millis()).unwrap_or(i64::MAX));
        }
        Ok(taken.into_iter().map(|(_, _, _, r)| r).collect())
    }

    /// Commit per-partition offsets covering the acked records to
    /// the replay group. Kafka can only commit a contiguous cursor:
    /// acking record X implicitly acks lower-offset taken records
    /// in the same partition (warned when that collapses ids the
    /// caller didn't name). Unknown ids are ignored.
    async fn ack_replayed(&self, _pipeline: &str, ids: &[DlqRecordId]) -> Result<(), DlqError> {
        if ids.is_empty() {
            return Ok(());
        }
        // Resolve ids → per-partition commit cursors.
        let commits: HashMap<i32, i64> = {
            let mut state = self
                .lease
                .lock()
                .expect("KafkaTopicDlq lease mutex poisoned");
            let mut commits: HashMap<i32, i64> = HashMap::new();
            for id in ids {
                if let Some((p, o)) = state.ids.get(&id.0) {
                    let cur = commits.entry(*p).or_insert(*o + 1);
                    *cur = (*cur).max(o + 1);
                }
            }
            // Drop acked ids + warn about implicit acks.
            let mut implicit: Vec<String> = Vec::new();
            state.ids.retain(|id, (p, o)| {
                let acked_explicitly = ids.iter().any(|i| i.0 == *id);
                let acked_implicitly =
                    !acked_explicitly && commits.get(p).map(|c| *o < *c).unwrap_or(false);
                if acked_implicitly {
                    implicit.push(id.clone());
                }
                !(acked_explicitly || acked_implicitly)
            });
            if !implicit.is_empty() {
                tracing::warn!(
                    pipeline = %self.pipeline,
                    topic = %self.topic,
                    implicit_count = implicit.len(),
                    "KafkaTopicDlq ack_replayed: committing a contiguous offset \
                     cursor implicitly acked lower-offset taken records that were \
                     not in the ack set (topic stores cannot ack out of order)"
                );
            }
            commits
        };
        if commits.is_empty() {
            return Ok(());
        }

        let this = self.clone_handles();
        tokio::task::spawn_blocking(move || -> Result<(), BackendError> {
            let consumer = this.replay_consumer()?;
            let mut tpl = TopicPartitionList::new();
            for (p, next) in &commits {
                tpl.add_partition_offset(&this.topic, *p, rdkafka::Offset::Offset(*next))
                    .map_err(|e| BackendError::Other(format!("kafka dlq ack tpl: {e}")))?;
            }
            retry_group_op("ack commit", || {
                consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            })?;
            Ok(())
        })
        .await
        .map_err(|e| BackendError::Other(format!("kafka dlq ack join: {e}")))??;
        Ok(())
    }

    /// Inexpressible on a topic — records cannot change state in
    /// place. The DLQ Phase 2 replay engine catches this typed
    /// `Unsupported` and parks poison records into the pipeline's
    /// table fallback store instead (see
    /// `StreamingPipeline::run_dlq_replay`).
    async fn park(&self, _pipeline: &str, _ids: &[DlqRecordId]) -> Result<(), DlqError> {
        Err(DlqError::Unsupported(
            "KafkaTopicDlq cannot park records: a topic is an immutable log with \
             no per-record state. Use the table store for park semantics."
                .into(),
        ))
    }

    /// The lease is process-local + group-offset based (see module
    /// docs) — overlapping replays of the same pipeline must be
    /// serialized one layer up. The replay engine honors this flag
    /// with an in-process per-pipeline mutex.
    fn replay_requires_serialization(&self) -> bool {
        true
    }

    /// `All` → seek-to-end (commit end offsets for the replay
    /// group; messages remain on the topic until broker retention —
    /// Kafka has no per-record delete on a shared topic).
    /// `FirstN(n)` → consume + commit the first n pending.
    /// `Ids` → `Unsupported`.
    async fn purge(&self, _pipeline: &str, selection: DlqSelection) -> Result<u64, DlqError> {
        match selection {
            DlqSelection::Ids(_) => Err(DlqError::Unsupported(
                "KafkaTopicDlq cannot purge records by id: a topic only supports \
                 advancing the replay cursor (All/FirstN). Use the table store for \
                 id-addressed purge."
                    .into(),
            )),
            DlqSelection::All => {
                let this = self.clone_handles();
                let purged = tokio::task::spawn_blocking(move || -> Result<u64, BackendError> {
                    let consumer = this.replay_consumer()?;
                    let windows = Self::partition_windows(&consumer, &this.topic)?;
                    let pending: u64 = windows
                        .iter()
                        .map(|(_, _, high, start)| (high - start).max(0) as u64)
                        .sum();
                    if pending == 0 {
                        return Ok(0);
                    }
                    let mut tpl = TopicPartitionList::new();
                    for (p, _, high, _) in &windows {
                        tpl.add_partition_offset(&this.topic, *p, rdkafka::Offset::Offset(*high))
                            .map_err(|e| {
                                BackendError::Other(format!("kafka dlq purge tpl: {e}"))
                            })?;
                    }
                    retry_group_op("purge commit", || {
                        consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)
                    })?;
                    Ok(pending)
                })
                .await
                .map_err(|e| BackendError::Other(format!("kafka dlq purge join: {e}")))??;
                // The purge invalidates any in-process lease state.
                let mut state = self
                    .lease
                    .lock()
                    .expect("KafkaTopicDlq lease mutex poisoned");
                *state = LeaseState::default();
                Ok(purged)
            }
            DlqSelection::FirstN(n) => {
                let this = self.clone_handles();
                let pipeline = self.pipeline.clone();
                let budget = usize::try_from(n).unwrap_or(usize::MAX);
                let purged = tokio::task::spawn_blocking(move || -> Result<u64, BackendError> {
                    let consumer = this.replay_consumer()?;
                    let windows = Self::partition_windows(&consumer, &this.topic)?;
                    let starts: Vec<(i32, i64, i64)> = windows
                        .iter()
                        .map(|(p, _, high, start)| (*p, *start, *high))
                        .collect();
                    let collected =
                        Self::read_window(&consumer, &this.topic, &pipeline, &starts, budget)?;
                    if collected.is_empty() {
                        return Ok(0);
                    }
                    let mut commits: HashMap<i32, i64> = HashMap::new();
                    for (p, o, _, _) in &collected {
                        let cur = commits.entry(*p).or_insert(*o + 1);
                        *cur = (*cur).max(o + 1);
                    }
                    let mut tpl = TopicPartitionList::new();
                    for (p, next) in &commits {
                        tpl.add_partition_offset(&this.topic, *p, rdkafka::Offset::Offset(*next))
                            .map_err(|e| {
                                BackendError::Other(format!("kafka dlq purge tpl: {e}"))
                            })?;
                    }
                    retry_group_op("purge commit", || {
                        consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)
                    })?;
                    Ok(collected.len() as u64)
                })
                .await
                .map_err(|e| BackendError::Other(format!("kafka dlq purge join: {e}")))??;
                Ok(purged)
            }
        }
    }
}

impl KafkaTopicDlq {
    /// Cheap clonable view for `spawn_blocking` closures (the
    /// consumer construction needs `&KafkaBackend` which lives
    /// behind `Arc<dyn Backend>`).
    fn clone_handles(&self) -> KafkaTopicDlqHandles {
        KafkaTopicDlqHandles {
            source: Arc::clone(&self.source),
            topic: self.topic.clone(),
            replay_group: self.replay_group.clone(),
        }
    }
}

/// The subset of `KafkaTopicDlq` that blocking closures need.
struct KafkaTopicDlqHandles {
    source: Arc<dyn Backend>,
    topic: String,
    replay_group: String,
}

impl KafkaTopicDlqHandles {
    fn replay_consumer(&self) -> Result<BaseConsumer<EmatixKafkaContext>, BackendError> {
        let kafka = self
            .source
            .as_kafka()
            .expect("validated at construction: source is Kafka");
        let mut config = kafka.client_config();
        config.set("group.id", &self.replay_group);
        config.set("enable.auto.commit", "false");
        config.set("auto.offset.reset", "earliest");
        config
            .create_with_context(kafka.build_context())
            .map_err(|e| BackendError::Connection(format!("kafka dlq consumer create: {e}")))
    }
}
