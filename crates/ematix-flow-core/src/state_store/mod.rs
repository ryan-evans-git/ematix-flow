//! Phase 39.5a: durable per-pipeline state persistence.
//!
//! Sessions and (39.5b) stream-stream joins can hold accumulator state
//! for hours — replay distance becomes unbounded if state is reconstructed
//! from source on every restart. `StateStore` is the durable layer that
//! lets a pipeline reload its in-flight state plus the source offsets
//! that produced it, atomically committed together.
//!
//! See `docs/PHASE_39_5_SESSIONS.md` for the design (sections "StateStore"
//! and "Cadence").
//!
//! ## Trait shape
//!
//! Single-shot atomic commit: the caller assembles a [`CommitSnapshot`]
//! of every key that changed since the previous commit plus the new
//! source offsets, hands it to [`StateStore::commit`], and the backend
//! opens a transaction internally. The pipeline never sees a transaction
//! handle. [`StateStore::load`] is called once at pipeline start.
//!
//! ## Keys are opaque bytes
//!
//! The trait does not know about `GroupKey` — it takes pre-serialized
//! `Vec<u8>` keys. This keeps the durable layer decoupled from the
//! windowed transform's internal representation. The session/join
//! machinery in PR 2 / 39.5b is responsible for encoding its own keys.
//!
//! ## Implementations
//!
//! - [`InMemoryStateStore`] — for unit tests and as a documented
//!   "no-op" production option (loud config-load warning).
//! - `PostgresStateStore` — postcard-serialized blobs in two tables,
//!   per-emit single transaction. Lands in a follow-up slice.

use std::collections::HashMap;
use std::sync::Arc;

use crate::backend::BackendError;
use crate::dlq::DeadLetterStore;

pub mod in_memory;
pub mod migrations;
pub mod postgres;

pub use in_memory::InMemoryStateStore;
pub use postgres::PostgresStateStore;

/// Opaque per-key state-blob key. The session/join machinery serializes
/// its `GroupKey` (or join key) into this byte vector before handing it
/// to the store.
pub type StateKey = Vec<u8>;

/// Identifies one of the pipeline's `[[sources]]`. Stable across
/// restarts (typically the source's configured name).
pub type SourceId = String;

/// Snapshot returned by [`StateStore::load`]. An unknown pipeline name
/// returns an empty `RecoveredState` with `state_version = 0` — never
/// a "not found" error.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveredState {
    /// Per-key state blobs. The transform decodes these with `postcard`.
    pub state_by_key: HashMap<StateKey, Vec<u8>>,
    /// Per-source committed offset bytes. The source backend decodes
    /// these in `seek_to`.
    pub offsets: HashMap<SourceId, Vec<u8>>,
    /// Schema version of the loaded `state_blob`s. Migration runs in
    /// the transform layer; the store reports what it has.
    pub state_version: u32,
}

/// Snapshot passed to [`StateStore::commit`].
///
/// The store applies upserts, then deletes, then offsets in a single
/// transaction. Calling code only sends rows that actually changed —
/// dirty-only — so a steady-state pipeline emits small commits.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CommitSnapshot {
    /// Keys that were created or modified since the previous commit.
    pub state_upserts: Vec<(StateKey, Vec<u8>)>,
    /// Keys whose state was finalized + evicted since the previous
    /// commit (window emit + retention deadline passed). Ordering
    /// vs. `state_upserts` doesn't matter — duplicates across the two
    /// fields are not legal and not defended against.
    pub state_deletes: Vec<StateKey>,
    /// Replaces every entry; absent source ids are left unchanged in
    /// the store. Pass an empty map to commit only state changes.
    pub offsets: HashMap<SourceId, Vec<u8>>,
    /// Version stamp written alongside this snapshot's rows. Bumped
    /// only when the on-disk shape genuinely changes; minor field
    /// additions ride on `postcard`'s append-only rules.
    pub state_version: u32,
}

/// Durable per-pipeline state persistence.
///
/// `Send + Sync + Debug` so the runtime can hold this behind
/// `Arc<dyn StateStore>` across the watermark loop and the periodic
/// checkpoint ticker.
#[async_trait::async_trait]
pub trait StateStore: Send + Sync + std::fmt::Debug {
    /// Load every committed key + offset for `pipeline`. Returns an
    /// empty `RecoveredState` for an unknown pipeline.
    async fn load(&self, pipeline: &str) -> Result<RecoveredState, BackendError>;

    /// Apply `snapshot` atomically. On error nothing is visible to a
    /// subsequent `load`.
    async fn commit(&self, pipeline: &str, snapshot: CommitSnapshot) -> Result<(), BackendError>;

    /// DLQ Phase 1: hand back a [`DeadLetterStore`] riding this
    /// state store's connection family, if it has one. The streaming
    /// pipeline resolves its dead-letter store through this hook so
    /// a Postgres-checkpointed pipeline gets a Postgres table DLQ
    /// with zero extra configuration.
    ///
    /// Default `Ok(None)` — stores without a durable SQL surface
    /// (e.g. [`InMemoryStateStore`]) opt out, and the pipeline falls
    /// back to an in-process SQLite `TableDlq` with a loud warning.
    async fn dead_letter_store(&self) -> Result<Option<Arc<dyn DeadLetterStore>>, BackendError> {
        Ok(None)
    }

    /// DLQ Phase 3: atomically clear EVERY state blob for
    /// `pipeline` and replace its committed offsets with `offsets`
    /// — the rewind primitive. Both mutations happen in ONE
    /// transaction: a crash in between must never leave cleared
    /// state paired with the OLD offsets (double-count) or old
    /// state paired with the NEW offsets (stale windows absorb the
    /// re-consumed rows).
    ///
    /// Unlike [`commit`], `offsets` REPLACES the stored set —
    /// source ids absent from the map are deleted, so a rewound
    /// pipeline can't resume a stale sibling source cursor.
    ///
    /// Default is a typed error so stores that haven't opted in
    /// fail a stateful rewind loudly instead of half-applying it.
    ///
    /// [`commit`]: StateStore::commit
    async fn reset(
        &self,
        pipeline: &str,
        _offsets: HashMap<SourceId, Vec<u8>>,
    ) -> Result<(), BackendError> {
        Err(BackendError::Other(format!(
            "this StateStore does not support reset (rewind) — pipeline `{pipeline}` \
             cannot atomically clear state + rewrite offsets on this store"
        )))
    }
}
