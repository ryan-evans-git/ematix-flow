//! In-memory `StateStore` for unit tests and the documented
//! production "no-op" option.
//!
//! Backed by a `Mutex<HashMap>` per pipeline; cheap, lossy on process
//! exit. Production deployments using this opt out of crash recovery —
//! the config loader emits a loud warning at startup (PR 3).

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::backend::BackendError;

use super::{CommitSnapshot, RecoveredState, SourceId, StateKey, StateStore};

/// Per-pipeline state held in memory. Survives across `load` /
/// `commit` calls within the same process; never written to disk.
#[derive(Debug, Default)]
pub struct InMemoryStateStore {
    inner: Mutex<HashMap<String, PipelineState>>,
}

#[derive(Debug, Default, Clone)]
struct PipelineState {
    state_by_key: HashMap<StateKey, Vec<u8>>,
    offsets: HashMap<SourceId, Vec<u8>>,
    state_version: u32,
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StateStore for InMemoryStateStore {
    async fn load(&self, pipeline: &str) -> Result<RecoveredState, BackendError> {
        let guard = self
            .inner
            .lock()
            .expect("InMemoryStateStore mutex poisoned");
        let recovered = match guard.get(pipeline) {
            Some(p) => RecoveredState {
                state_by_key: p.state_by_key.clone(),
                offsets: p.offsets.clone(),
                state_version: p.state_version,
            },
            None => RecoveredState::default(),
        };
        Ok(recovered)
    }

    async fn commit(&self, pipeline: &str, snapshot: CommitSnapshot) -> Result<(), BackendError> {
        let mut guard = self
            .inner
            .lock()
            .expect("InMemoryStateStore mutex poisoned");
        let p = guard.entry(pipeline.to_string()).or_default();

        // Apply upserts, then deletes. Deletes-of-unknown are silent
        // by contract — see `delete_of_unknown_key_is_ok` in
        // `tests/state_store_contract.rs`.
        for (key, blob) in snapshot.state_upserts {
            p.state_by_key.insert(key, blob);
        }
        for key in snapshot.state_deletes {
            p.state_by_key.remove(&key);
        }

        // Per-source offset merge: absent ids retain their prior
        // value so a per-source ticker can commit just its own.
        for (source_id, bytes) in snapshot.offsets {
            p.offsets.insert(source_id, bytes);
        }

        p.state_version = snapshot.state_version;
        Ok(())
    }
}
