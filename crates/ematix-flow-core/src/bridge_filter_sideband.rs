//! Σ.Q.L9 — runtime sideband for `EmatixFastParquetExec`.
//!
//! Mid-query plan adaptation channel. A `BridgeFilterSideband` is an
//! `Arc<RwLock<Option<Vec<ColumnPredicate>>>>` shared between an
//! upstream producer (typically a `BuildSideBloomEmitterExec` wrapping
//! a HashJoinExec build side) and a downstream consumer (the probe-
//! side `EmatixFastParquetExec`).
//!
//! Lifecycle:
//!
//! 1. **Planner**: the L9 rule creates one `BridgeFilterSideband` per
//!    eligible HashJoin equi-key, plumbs it into both the build-side
//!    wrapper and the probe-side scan via `with_runtime_sideband`.
//! 2. **Execute (build phase)**: the wrapper streams build batches
//!    through, accumulating join-key values into a `BloomFilter`.
//!    When the stream ends, it publishes a `ColumnPredicate::I64InBloom`
//!    into the sideband (via `publish`).
//! 3. **Execute (probe phase)**: the probe-scan's `execute()` reads
//!    the sideband AFTER the build stream is consumed (HashJoinExec
//!    blocks on build before probe). The sideband contents are
//!    merged into the BridgeFilter just before masked-decode.
//!
//! Empty sideband = no-op (the probe scan reads it, sees `None` /
//! empty, runs unmodified).
//!
//! ## Why a sideband and not a planner-time predicate
//!
//! [[sigma-q-l4-prime]] (slice 4) showed that pre-executing build
//! subtrees just to extract a bloom doubles their cost on TPC-H.
//! Capturing the bloom as a side-effect of the regular HashJoin
//! build phase avoids that — the build runs once, and the bloom is
//! "free" data falling out of it.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::ematix_fast_parquet::ColumnPredicate;

/// Σ.Q.L9 — shared per-query slot for runtime predicates. Cheap to
/// clone (it's an Arc<RwLock<_>>). Default-empty.
///
/// Σ.Q.L16: holds a `tokio::sync::Notify` alongside the predicate slot
/// so the probe-side scan can await publication with a short timeout.
/// Pre-L16, the peek raced with publish on 12 of 14 lineitem
/// partitions in Q17 (small `filtered_part`⋈lineitem build finishes
/// in ~6 ms; probe partitions started before the bloom was published
/// and read None). With the timed wait, the probe blocks briefly for
/// the bloom in the hot, small-build cases.
#[derive(Debug, Clone, Default)]
pub struct BridgeFilterSideband {
    inner: Arc<RwLock<Option<Vec<ColumnPredicate>>>>,
    notify: Arc<tokio::sync::Notify>,
}

impl BridgeFilterSideband {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish a set of predicates into the sideband. Replaces any
    /// previously-published set. Typical usage: the build-side
    /// wrapper calls this once when its input stream is fully drained.
    pub fn publish(&self, preds: Vec<ColumnPredicate>) {
        *self.inner.write().unwrap() = Some(preds);
        // Σ.Q.L16 — wake any probe-side scans that are blocked in
        // `wait_for_publish`. `notify_waiters` is no-op when there
        // are no waiters and only wakes the currently-parked ones,
        // which is exactly the semantics we need (each probe waits
        // at most once before its first poll).
        self.notify.notify_waiters();
    }

    /// Consume the published predicates (if any), leaving the
    /// sideband empty. Called by the probe-side scan at execute()
    /// time. Returns `None` if nothing was published (either because
    /// the build hasn't finished, or because there's no producer
    /// attached).
    pub fn take(&self) -> Option<Vec<ColumnPredicate>> {
        self.inner.write().unwrap().take()
    }

    /// Read without consuming. Useful for tests and diagnostics.
    pub fn peek(&self) -> Option<Vec<ColumnPredicate>> {
        self.inner.read().unwrap().clone()
    }

    /// Has a producer published yet?
    pub fn is_ready(&self) -> bool {
        self.inner.read().unwrap().is_some()
    }

    /// Σ.Q.L16: await publication, or `timeout`, whichever comes first.
    /// Returns true if a publish happened (already or during the wait),
    /// false on timeout. Safe to call multiple times — it short-circuits
    /// if already published.
    pub async fn wait_for_publish(&self, timeout: Duration) -> bool {
        if self.is_ready() {
            return true;
        }
        // Register interest BEFORE re-checking is_ready, to avoid
        // missing a publish that races between the check and the wait.
        let notified = self.notify.notified();
        if self.is_ready() {
            return true;
        }
        tokio::select! {
            _ = notified => self.is_ready(),
            _ = tokio::time::sleep(timeout) => self.is_ready(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_then_take_roundtrips() {
        let sb = BridgeFilterSideband::new();
        assert!(!sb.is_ready());
        assert!(sb.peek().is_none());
        sb.publish(vec![ColumnPredicate::I32In {
            col_idx: 0,
            values: vec![1, 2, 3],
        }]);
        assert!(sb.is_ready());
        let got = sb.take().expect("expected published predicates");
        assert_eq!(got.len(), 1);
        // Take is destructive.
        assert!(!sb.is_ready());
        assert!(sb.take().is_none());
    }

    #[test]
    fn clones_share_state() {
        let sb1 = BridgeFilterSideband::new();
        let sb2 = sb1.clone();
        sb1.publish(vec![ColumnPredicate::I32In {
            col_idx: 7,
            values: vec![99],
        }]);
        assert!(sb2.is_ready());
        let got = sb2.take().unwrap();
        assert_eq!(got.len(), 1);
        // Both sides see the empty state.
        assert!(!sb1.is_ready());
    }
}
