//! DLQ Phase 2: the replay engine's store-facing primitives.
//!
//! Redrive = **reprocess through the pipeline** (PRD decision):
//! [`DlqReplaySource`] leases records out of a [`DeadLetterStore`]
//! via `take_for_replay` and decodes them back to `RecordBatch`es
//! through the SAME decode paths the streaming sources use; the
//! execution loop lives on
//! [`crate::streaming::StreamingPipeline::run_dlq_replay`], which
//! feeds the pipeline's OWN transform + targets.
//!
//! ## Bounded, single-pass semantics
//!
//! A replay run performs exactly ONE `take_for_replay` and resolves
//! every leased record (ack / re-dead-letter with `attempt + 1` /
//! park at `max_attempts`). Records the run re-dead-letters are new
//! pending records — they are *not* re-taken by the same run, so a
//! sink that is still broken never spins a hot retry loop; the next
//! (manual) replay picks them up with the incremented attempt.
//! Termination is structural: the selection is drained in one pass.
//!
//! ## Replay serialization for topic stores
//!
//! `TableDlq` leases are safe under concurrency (`SKIP LOCKED` on
//! Postgres, connection-serialized transactions on SQLite).
//! `KafkaTopicDlq`'s lease is process-local + group-offset based —
//! overlapping replays of the same pipeline must be serialized.
//! [`replay_lock`] keys an in-process `tokio::sync::Mutex` by
//! pipeline name; `run_dlq_replay` takes it whenever the resolved
//! store reports
//! [`DeadLetterStore::replay_requires_serialization`].
//!
//! **Cross-process caveat:** the mutex is process-local. Two
//! *processes* replaying the same pipeline's Kafka DLQ concurrently
//! can double-take (at-least-once, never lost) — acceptable for
//! Phase 2's single-operator model (the web UI is single-user; the
//! Python API layer runs replays in one process). A shared advisory
//! lock is a Phase 4+ follow-up if multi-operator arrives.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::Duration;

use arrow_array::RecordBatch;

use crate::backend::BackendError;
use crate::kafka_backend::KafkaBackend;

use super::{DeadLetterStore, DlqError, DlqRecord, DlqSelection};

/// Default poison guard: a record is parked once it has been
/// dead-lettered `DEFAULT_MAX_ATTEMPTS` times (original emission is
/// attempt 1; each failed replay re-dead-letters at `attempt + 1`).
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Default lease handed to `take_for_replay` — long enough to
/// decode + transform + write a drained selection; expired leases
/// make crashed replays' records re-takeable.
pub const DEFAULT_REPLAY_LEASE: Duration = Duration::from_secs(60);

/// Tuning knobs for one replay run.
#[derive(Debug, Clone)]
pub struct ReplayOptions {
    /// Park a record instead of re-dead-lettering once its attempt
    /// count would exceed this (see [`DEFAULT_MAX_ATTEMPTS`]).
    pub max_attempts: u32,
    /// Lease duration for `take_for_replay`.
    pub lease: Duration,
    /// Where poison records park when the pipeline's own store
    /// cannot express `park` (a Kafka topic). `None` resolves the
    /// pipeline's table fallback (state-store family, else a LOUD
    /// in-memory SQLite store).
    pub park_store: Option<Arc<dyn DeadLetterStore>>,
}

impl Default for ReplayOptions {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            lease: DEFAULT_REPLAY_LEASE,
            park_store: None,
        }
    }
}

/// Outcome counts for one replay run. Invariant:
/// `taken == succeeded + redeadlettered + parked`.
///
/// Phase 4 registers replay executions in RunHistory
/// (`kind=replay`) from this struct — counts plus the wall-clock
/// window are everything the Runs surface needs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayReport {
    /// Records leased out of the store by this run.
    pub taken: u64,
    /// Records that reprocessed through transform + targets and
    /// were acked (removed) from the store.
    pub succeeded: u64,
    /// Records that failed again and returned to the DLQ with
    /// `attempt + 1`.
    pub redeadlettered: u64,
    /// Records parked as poison (attempt budget exhausted).
    pub parked: u64,
    /// Wall-clock window of the run (ms since the Unix epoch).
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
}

/// A bounded, one-shot source over `take_for_replay` leases.
///
/// `take` leases the selection exactly once; subsequent calls
/// return empty (the single-pass semantics documented on the
/// module). Decoding is a separate, per-record step ([`Self::decode`])
/// so the engine can attribute decode failures to individual
/// records (a corrupt payload re-dead-letters / parks that record
/// alone, not the whole run).
pub struct DlqReplaySource {
    store: Arc<dyn DeadLetterStore>,
    pipeline: String,
    selection: DlqSelection,
    lease: Duration,
    drained: bool,
}

impl DlqReplaySource {
    pub fn new(
        store: Arc<dyn DeadLetterStore>,
        pipeline: impl Into<String>,
        selection: DlqSelection,
        lease: Duration,
    ) -> Self {
        Self {
            store,
            pipeline: pipeline.into(),
            selection,
            lease,
            drained: false,
        }
    }

    /// Lease the selection (once). `now_ms` is the caller's clock,
    /// passed through to the store's lease check (house
    /// timestamps-passed-in convention).
    pub async fn take(&mut self, now_ms: i64) -> Result<Vec<DlqRecord>, DlqError> {
        if self.drained {
            return Ok(Vec::new());
        }
        self.drained = true;
        self.store
            .take_for_replay(&self.pipeline, self.selection.clone(), self.lease, now_ms)
            .await
    }

    /// Decode one DLQ record back to `RecordBatch`es, respecting
    /// `meta.payload_format` and reusing the SAME decode paths the
    /// streaming read side uses (`decode_payloads_as_jsonl` /
    /// `decode_payloads_as_raw_bytes`; Avro/Protobuf route through
    /// the primary Kafka source's own schema-registry machinery
    /// when `kafka` is supplied and its configured format matches).
    pub async fn decode(
        record: &DlqRecord,
        kafka: Option<&KafkaBackend>,
    ) -> Result<Vec<RecordBatch>, BackendError> {
        let payloads = vec![record.payload.clone()];
        match record.meta.payload_format.as_str() {
            "json" => crate::kafka_backend::decode_payloads_as_jsonl(payloads),
            "raw_bytes" => crate::kafka_backend::decode_payloads_as_raw_bytes(payloads),
            other => match kafka {
                Some(k) if k.payload_format().as_str() == other => {
                    k.decode_payloads(payloads).await
                }
                _ => Err(BackendError::Other(format!(
                    "DLQ replay: cannot decode payload_format '{other}' for record \
                     {id}: it needs the source backend's schema-registry machinery \
                     and the pipeline's primary source is not a Kafka backend \
                     configured for that format",
                    id = record.id
                ))),
            },
        }
    }
}

/// Process-wide replay serialization locks, keyed by pipeline name.
/// See the module docs for scope + the cross-process caveat.
static REPLAY_LOCKS: LazyLock<StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// The in-process replay lock for `pipeline`. Callers clone the
/// `Arc` and `lock_owned()` it for the duration of a replay run.
pub(crate) fn replay_lock(pipeline: &str) -> Arc<tokio::sync::Mutex<()>> {
    REPLAY_LOCKS
        .lock()
        .expect("replay lock registry mutex poisoned")
        .entry(pipeline.to_string())
        .or_default()
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_match_prd() {
        let o = ReplayOptions::default();
        assert_eq!(o.max_attempts, 3, "PRD: max_attempts defaults to 3");
        assert_eq!(o.lease, Duration::from_secs(60));
        assert!(o.park_store.is_none());
    }

    #[test]
    fn replay_lock_is_per_pipeline() {
        let a1 = replay_lock("pipe-a");
        let a2 = replay_lock("pipe-a");
        let b = replay_lock("pipe-b");
        assert!(Arc::ptr_eq(&a1, &a2), "same pipeline → same lock");
        assert!(
            !Arc::ptr_eq(&a1, &b),
            "different pipelines → independent locks"
        );
    }
}
