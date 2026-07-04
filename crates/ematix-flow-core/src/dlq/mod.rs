//! DLQ Phase 1: the `DeadLetterStore` abstraction.
//!
//! Streaming pipelines can already *emit* failed batches to a Kafka
//! dead-letter topic, but that store is write-only and Kafka-only
//! (see `docs/prds/2026-07-04-dlq-replay.md`). `DeadLetterStore` is
//! the uniform surface the pipeline emits through and the replay
//! engine (Phase 2) + web UI (Phases 4/5) consume from:
//!
//! - [`table::TableDlq`] — the universal default. One
//!   `ematix_dlq_records` table on the state-store SQL family
//!   (SQLite for dev, Postgres for prod). Payload bytes + metadata
//!   columns; lease via a `leased_until` column checked against a
//!   caller-provided now.
//! - [`kafka_topic::KafkaTopicDlq`] — today's format-preserving
//!   topic emission upgraded with metadata in Kafka HEADERS
//!   (payload untouched). Browse = bounded peek consumer; depth =
//!   end offsets − replay-group committed.
//!
//! ## Format preservation
//!
//! Payloads stay in their source wire format (JSON↔JSON,
//! RawBytes↔RawBytes) so replay is symmetric. Failure metadata
//! travels OUT OF BAND of the payload (Kafka headers / table
//! columns), never by envelope-wrapping.
//!
//! ## Timestamps are passed IN
//!
//! No `SystemTime::now()` in store logic — `DlqMeta::failed_at` is
//! supplied by the caller and [`DeadLetterStore::take_for_replay`]
//! takes an explicit `now_ms` the lease is checked against. This is
//! the house testability convention (lease expiry is unit-testable
//! without sleeping).
//!
//! ## At-least-once contract
//!
//! [`DeadLetterStore::append`] must not resolve until the records
//! are durably accepted by the store — the pipeline commits source
//! offsets only AFTER the append future resolves `Ok`.

use std::time::Duration;

use crate::backend::BackendError;

/// Error strings persisted in `DlqMeta` are truncated to this many
/// bytes. Guards against Kafka header size limits (and unbounded
/// TEXT columns) when a transform error carries a giant plan dump.
pub const MAX_ERROR_BYTES: usize = 8 * 1024;

/// Truncate `error` to [`MAX_ERROR_BYTES`], respecting UTF-8 char
/// boundaries so the result is still a valid `String`.
pub fn truncate_error(error: &str) -> String {
    if error.len() <= MAX_ERROR_BYTES {
        return error.to_string();
    }
    let mut end = MAX_ERROR_BYTES;
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    error[..end].to_string()
}

/// Which pipeline stage dead-lettered the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DlqStage {
    /// `transform.transform(batch)` errored under
    /// `transform_on_error = "dlq"`.
    Transform,
    /// A target `write_arrow_stream` failed with a
    /// `dead_letter_topic` / dead-letter store configured.
    Write,
    /// A windowed transform evicted rows past their lateness
    /// deadline under `late_data = "dlq"`.
    LateData,
}

impl DlqStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            DlqStage::Transform => "transform",
            DlqStage::Write => "write",
            DlqStage::LateData => "late_data",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "transform" => Some(DlqStage::Transform),
            "write" => Some(DlqStage::Write),
            "late_data" => Some(DlqStage::LateData),
            _ => None,
        }
    }
}

/// Opaque record identity. Table stores mint UUIDs at emission time;
/// the Kafka store round-trips the same id through an
/// `emat-dlq-id` header (falling back to `"{partition}:{offset}"`
/// for legacy header-less messages).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DlqRecordId(pub String);

impl std::fmt::Display for DlqRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Lifecycle status of a dead-lettered record.
///
/// - `Pending` — awaiting replay (the default after `append`).
/// - `Leased` — handed out by [`DeadLetterStore::take_for_replay`]
///   and not yet acked/parked; invisible to further takes until the
///   lease expires.
/// - `Parked` — poison-message terminal state (max replay attempts
///   exhausted, or operator action). Excluded from takes; visible
///   via `browse` + `depth.parked`; removed only by `purge`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DlqRecordStatus {
    Pending,
    Leased,
    Parked,
}

impl DlqRecordStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            DlqRecordStatus::Pending => "pending",
            DlqRecordStatus::Leased => "leased",
            DlqRecordStatus::Parked => "parked",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(DlqRecordStatus::Pending),
            "leased" => Some(DlqRecordStatus::Leased),
            "parked" => Some(DlqRecordStatus::Parked),
            _ => None,
        }
    }
}

/// Failure metadata carried out-of-band alongside the (untouched)
/// payload. Every field is populated by the emitting pipeline at
/// failure time — stores round-trip it with full fidelity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqMeta {
    /// Owning pipeline name. A DLQ record belongs to exactly one
    /// pipeline (no cross-pipeline routing — PRD non-goal).
    pub pipeline: String,
    /// Which stage failed.
    pub stage: DlqStage,
    /// Failure reason, truncated to [`MAX_ERROR_BYTES`] (callers
    /// should pass through [`truncate_error`]; stores re-truncate
    /// defensively).
    pub error: String,
    /// The source the failed rows were read from (the source's
    /// query string — Kafka topic / subscription / stream name).
    pub source_id: String,
    /// Opaque source-offset snapshot at failure time (same encoding
    /// as [`crate::backend::Backend::offset_snapshot`]). `None` for
    /// sources without offset support.
    pub offset_bytes: Option<Vec<u8>>,
    /// Max `_event_ts` of the failed batch (microseconds since the
    /// Unix epoch), when the batch carried one.
    pub event_ts: Option<i64>,
    /// Wall-clock failure time, milliseconds since the Unix epoch.
    /// Passed in by the caller — never computed inside a store.
    pub failed_at: i64,
    /// Replay attempt count. `1` on first dead-lettering; the
    /// replay engine (Phase 2) bumps it on re-dead-letter and
    /// parks at `max_attempts`.
    pub attempt: u32,
    /// Wire format of `payload` (`"json"`, `"raw_bytes"`, `"avro"`,
    /// `"protobuf"`). Lets the replay source decode symmetrically.
    pub payload_format: String,
}

/// One dead-lettered message: identity + metadata + the payload in
/// its original source wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqRecord {
    pub id: DlqRecordId,
    pub meta: DlqMeta,
    pub payload: Vec<u8>,
}

/// Depth summary for one pipeline's DLQ.
///
/// `pending` counts records awaiting resolution — both `Pending`
/// and currently-`Leased` records (a lease is transient custody,
/// not resolution). `parked` counts poison-parked records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DlqDepth {
    pub pending: u64,
    pub parked: u64,
}

/// Record selection for [`DeadLetterStore::take_for_replay`] and
/// [`DeadLetterStore::purge`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DlqSelection {
    /// Every eligible record.
    All,
    /// The oldest `n` eligible records (append order).
    FirstN(u64),
    /// Exactly these records (unknown / ineligible ids are skipped
    /// silently — the operation is idempotent, not all-or-nothing).
    Ids(Vec<DlqRecordId>),
}

/// Errors from a `DeadLetterStore`.
///
/// `Unsupported` is a *typed* signal that the backing medium cannot
/// express the operation (e.g. purge-by-id on a Kafka topic) —
/// callers surface it to the operator instead of retrying.
#[derive(Debug, thiserror::Error)]
pub enum DlqError {
    #[error("operation not supported by this DeadLetterStore: {0}")]
    Unsupported(String),
    #[error(transparent)]
    Backend(#[from] BackendError),
}

impl From<DlqError> for BackendError {
    fn from(e: DlqError) -> Self {
        match e {
            DlqError::Backend(b) => b,
            DlqError::Unsupported(msg) => {
                BackendError::Other(format!("dead-letter store: unsupported operation: {msg}"))
            }
        }
    }
}

/// Uniform dead-letter store surface. Async + object-safe so the
/// pipeline holds `Arc<dyn DeadLetterStore>` resolved once per run.
///
/// ## Semantics contract (pinned by `tests/dlq_helpers/mod.rs`)
///
/// - `append` is durable on `Ok` (source offsets commit after it).
/// - `browse` pages oldest-first in append order; `page` is
///   0-based; a page past the end returns an empty vec.
/// - `take_for_replay` atomically leases eligible records
///   (`Pending`, or `Leased` with `leased_until <= now_ms`) and
///   returns them oldest-first. Leased records are invisible to
///   concurrent takes until the lease expires.
/// - `ack_replayed` removes records regardless of current status;
///   unknown ids are ignored (idempotent).
/// - `park` transitions records to `Parked` (excluded from takes,
///   counted in `depth.parked`); unknown ids are ignored.
/// - `purge` deletes matching records of ANY status and returns
///   the deleted count.
#[async_trait::async_trait]
pub trait DeadLetterStore: Send + Sync + std::fmt::Debug {
    /// Durably append dead-lettered records. Must not resolve `Ok`
    /// before the records are accepted by the backing medium — the
    /// pipeline's at-least-once ordering (append ack BEFORE source
    /// offset commit) depends on it.
    async fn append(&self, records: Vec<DlqRecord>) -> Result<(), DlqError>;

    /// Pending + parked counts for `pipeline`.
    async fn depth(&self, pipeline: &str) -> Result<DlqDepth, DlqError>;

    /// Page through records oldest-first. `status_filter = None`
    /// returns every status a backing medium can enumerate.
    async fn browse(
        &self,
        pipeline: &str,
        page: u64,
        page_size: u64,
        status_filter: Option<DlqRecordStatus>,
    ) -> Result<Vec<DlqRecord>, DlqError>;

    /// Atomically lease eligible records for replay. `now_ms` is
    /// the caller's clock (milliseconds since the Unix epoch); a
    /// record whose `leased_until <= now_ms` is re-eligible. The
    /// new lease expires at `now_ms + lease`.
    async fn take_for_replay(
        &self,
        pipeline: &str,
        selection: DlqSelection,
        lease: Duration,
        now_ms: i64,
    ) -> Result<Vec<DlqRecord>, DlqError>;

    /// Remove successfully-replayed records.
    async fn ack_replayed(&self, pipeline: &str, ids: &[DlqRecordId]) -> Result<(), DlqError>;

    /// Park poison records (terminal until purged).
    async fn park(&self, pipeline: &str, ids: &[DlqRecordId]) -> Result<(), DlqError>;

    /// Delete matching records of any status; returns the count.
    async fn purge(&self, pipeline: &str, selection: DlqSelection) -> Result<u64, DlqError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_error_short_string_unchanged() {
        assert_eq!(truncate_error("boom"), "boom");
    }

    #[test]
    fn truncate_error_caps_at_8kb() {
        let long = "x".repeat(MAX_ERROR_BYTES + 100);
        let t = truncate_error(&long);
        assert_eq!(t.len(), MAX_ERROR_BYTES);
    }

    #[test]
    fn truncate_error_respects_char_boundaries() {
        // 'é' is 2 bytes; build a string whose 8192nd byte falls
        // mid-char so a naive slice would panic.
        let long = "é".repeat(MAX_ERROR_BYTES / 2 + 50);
        let t = truncate_error(&long);
        assert!(t.len() <= MAX_ERROR_BYTES);
        assert!(t.chars().all(|c| c == 'é'));
    }

    #[test]
    fn stage_round_trips() {
        for s in [DlqStage::Transform, DlqStage::Write, DlqStage::LateData] {
            assert_eq!(DlqStage::parse(s.as_str()), Some(s));
        }
        assert_eq!(DlqStage::parse("bogus"), None);
    }

    #[test]
    fn status_round_trips() {
        for s in [
            DlqRecordStatus::Pending,
            DlqRecordStatus::Leased,
            DlqRecordStatus::Parked,
        ] {
            assert_eq!(DlqRecordStatus::parse(s.as_str()), Some(s));
        }
        assert_eq!(DlqRecordStatus::parse("bogus"), None);
    }
}
