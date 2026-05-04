//! Forward-only state-blob migration chain.
//!
//! `StateStore` rounds-trips opaque bytes; the *shape* of those bytes
//! is the session/join layer's concern. When that layer bumps its
//! `state_version` (genuine layout change, not just an appended
//! field), it registers a migrator from the previous version to the
//! new one. On `load`, [`MigrationChain::migrate`] walks each blob
//! forward through every step it's behind.
//!
//! See `docs/PHASE_39_5_SESSIONS.md` § "Migrations".
//!
//! ## Forward-only
//!
//! Reverting an upgraded process to a prior binary is out of scope —
//! a v3 commit cannot be deserialized as v2 because new fields aren't
//! part of v2's schema. Backward migration would require shadow
//! double-writes and is not worth the complexity for this framework.
//! Trying to `migrate(.., from=5, to=3)` returns
//! [`MigrationError::Backward`].
//!
//! ## Failure modes
//!
//! - [`MigrationError::Backward`] — caller asked for `to < from`.
//! - [`MigrationError::Unsupported`] — no migrator registered for
//!   step `at -> at+1`. Distinct from `Failed` so the operator can
//!   tell "you forgot to register a step" from "the data couldn't
//!   be migrated."
//! - [`MigrationError::Failed`] — a registered migrator returned
//!   `Err(reason)`. Carries the step index so logs say which hop
//!   broke.

use thiserror::Error;

/// One forward step. Pure function; the chain calls it with the
/// blob at `from` and expects the blob at `from + 1`.
type StepFn = Box<dyn Fn(&[u8]) -> Result<Vec<u8>, String> + Send + Sync + 'static>;

/// Registry of forward-only migration steps, indexed by source
/// version. Build once at process start; clone on demand.
#[derive(Default)]
pub struct MigrationChain {
    /// `steps[i] = Some(f)` means there's a migrator from version
    /// `i` to version `i + 1`. We use a `Vec<Option<_>>` rather
    /// than a `HashMap<u32, _>` because we expect a small dense
    /// range of versions and the indexing is O(1) and obvious.
    steps: Vec<Option<StepFn>>,
}

impl std::fmt::Debug for MigrationChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let registered: Vec<usize> = self
            .steps
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_ref().map(|_| i))
            .collect();
        f.debug_struct("MigrationChain")
            .field("registered_steps", &registered)
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum MigrationError {
    /// Caller requested a downgrade. Forward-only by design.
    #[error("backward migration is not supported (from {from} to {to})")]
    Backward { from: u32, to: u32 },

    /// No migrator registered for step `at -> at + 1`. Programming
    /// error: the layer that defines `state_version` forgot to add
    /// a step when it bumped the version.
    #[error("no migrator registered for state_version {at} -> {next}", next = at + 1)]
    Unsupported { at: u32 },

    /// A registered migrator returned `Err`. Genuinely undecidable
    /// shape change — operator must intervene (typically: drop the
    /// state and let the pipeline rebuild from the source's earliest
    /// retained offset).
    #[error("migration step {at} failed: {reason}")]
    Failed { at: u32, reason: String },
}

impl MigrationChain {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Register a migrator from `from_version` to `from_version + 1`.
    ///
    /// Panics if a migrator is already registered for the same step —
    /// it's a programming error and we want it to surface in tests,
    /// not silently shadow-replace at runtime.
    pub fn add<F>(mut self, from_version: u32, step: F) -> Self
    where
        F: Fn(&[u8]) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    {
        let idx = from_version as usize;
        if self.steps.len() <= idx {
            self.steps.resize_with(idx + 1, || None);
        }
        if self.steps[idx].is_some() {
            panic!(
                "duplicate migration step registered for state_version {from_version} \
                 -> {next}",
                next = from_version + 1,
            );
        }
        self.steps[idx] = Some(Box::new(step));
        self
    }

    /// Walk `blob` from `from` to `to`, applying each registered step.
    pub fn migrate(&self, blob: &[u8], from: u32, to: u32) -> Result<Vec<u8>, MigrationError> {
        if to < from {
            return Err(MigrationError::Backward { from, to });
        }
        if to == from {
            return Ok(blob.to_vec());
        }

        let mut current = blob.to_vec();
        for v in from..to {
            let idx = v as usize;
            let step = self
                .steps
                .get(idx)
                .and_then(|opt| opt.as_ref())
                .ok_or(MigrationError::Unsupported { at: v })?;
            current = step(&current).map_err(|reason| MigrationError::Failed { at: v, reason })?;
        }
        Ok(current)
    }
}
