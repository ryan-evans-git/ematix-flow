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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::ematix_fast_parquet::ColumnPredicate;

/// L9.ADAPT Guard 2 — runtime per-row-group disarm for tight-admitted
/// probe predicates. A tight-admitted wrap's payoff is unproven at plan
/// time (the default estimator rejected it); if at runtime the published
/// set/bloom turns out not to prune (FK-contained keys, high overlap),
/// every further per-row probe is pure waste — above the stash threshold
/// the bitmap is discarded anyway and the scan emits dense. The reader
/// records each row group's marginal probe outcome (rows that survived
/// the static predicates vs rows that also survived the probes); once
/// enough rows are seen and the cumulative probe pass-rate exceeds the
/// stash threshold, `disarmed()` flips and remaining row groups skip the
/// probe entirely (statics keep running — they're cheap and still route
/// stash-vs-dense).
///
/// Shared across partition streams via `Arc`; relaxed atomics — the
/// counters are advisory (a few extra probed row groups around the flip
/// are harmless, correctness never depends on the filter: the join
/// re-applies every inexact-by-design predicate).
#[derive(Debug, Default)]
pub struct ProbeDisarm {
    seen: AtomicUsize,
    passed: AtomicUsize,
}

impl ProbeDisarm {
    /// Don't judge before this many statics-surviving rows have been
    /// probed — early row groups can be unrepresentative (clustered
    /// keys), and on small scans the probe cost is bounded anyway.
    pub const MIN_ROWS: usize = 2_000_000;

    /// Record one row group's marginal probe outcome: `seen` rows
    /// entered the probe (post-statics survivors), `passed` rows left it.
    pub fn record(&self, seen: usize, passed: usize) {
        self.seen.fetch_add(seen, Ordering::Relaxed);
        self.passed.fetch_add(passed, Ordering::Relaxed);
    }

    /// True once ≥ `MIN_ROWS` have been probed and the cumulative
    /// pass-rate exceeds `stash_threshold` (the same masked→dense
    /// routing threshold: above it the bitmap is discarded, so the
    /// probe buys nothing).
    pub fn disarmed(&self, stash_threshold: f64) -> bool {
        let seen = self.seen.load(Ordering::Relaxed);
        if seen < Self::MIN_ROWS {
            return false;
        }
        let passed = self.passed.load(Ordering::Relaxed);
        (passed as f64) > (seen as f64) * stash_threshold
    }
}

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
    /// L9.ADAPT — set when the wrap that owns this sideband was admitted
    /// ONLY by the tight (NDV) cardinality estimate (the default
    /// estimator's ratio gate would have rejected it). The probe-side
    /// peek applies a row-scaled wait budget to these wraps instead of
    /// the flat timeout: a tight-admitted wrap's payoff is unproven, so
    /// the scan must not stall behind a slow build longer than its own
    /// decode work justifies (Q02's 1-key wrap behind a ~15 ms
    /// MIN-subquery build stalled a 19 ms query by +81%). Set once at
    /// plan time before the sideband fans out — plain field, clones
    /// carry it.
    tight_admitted: bool,
    /// L9.DIMSEL.RT (2026-06-24) — set when this wrap was admitted by the
    /// L9.DIMSEL rescue (a filtered-dim→fact bloom) AND carries the runtime
    /// build-selectivity gate. Such wraps must NOT late-arm: a dim→fact probe
    /// scan that is itself eager-polled (on a build side upstream) races past
    /// the build, peeks None, and — if it late-armed — would route to the eager
    /// whole-RG reader to wait for a publish that may turn out empty (the Q3
    /// `customer⋈orders` +50ms). For these wraps a missed peek just falls back
    /// to the base path (inline reader), forgoing the bloom that pass — correct
    /// and cheap. When the probe IS lazy-polled (Q9 lineitem) the build has
    /// already published by first peek, so this never costs the win.
    dimsel_gated: bool,
    /// Σ.Q05.CHAIN (2026-07-02) — set on the INTERMEDIATE links of an
    /// L9 cascade chain. The scan's `execute()` translates it into
    /// `BridgeFilter::set_apply_when_dense`, so the reader's masked→
    /// dense pass-rate routing stashes (applies) the bitmap instead of
    /// discarding it — a chain link's whole value is the rows it
    /// removes from the NEXT link's build sample, and intermediate
    /// targets are dim-sized scans where the per-batch filter is
    /// negligible. Default false: every other wrap keeps the REV.23
    /// discard behavior.
    chain_intermediate: bool,
    /// L9.ADAPT Guard 2 — shared runtime probe-outcome counters for
    /// this wrap. The scan threads a handle into the extended
    /// `BridgeFilter` iff the wrap is tight-admitted; the reader
    /// records per-row-group marginal probe outcomes and skips the
    /// probe once `disarmed()`.
    probe_disarm: Arc<ProbeDisarm>,
}

impl BridgeFilterSideband {
    pub fn new() -> Self {
        Self::default()
    }

    /// L9.ADAPT — mark this sideband's wrap as tight-admitted (builder
    /// style; call before cloning into emitter + scan).
    pub fn mark_tight_admitted(mut self) -> Self {
        self.tight_admitted = true;
        self
    }

    /// L9.ADAPT — was this wrap admitted only by the tight estimator?
    pub fn tight_admitted(&self) -> bool {
        self.tight_admitted
    }

    /// L9.DIMSEL.RT — mark this sideband's wrap as DIMSEL-gated (builder
    /// style; call before cloning into emitter + scan). See the field docs.
    pub fn mark_dimsel_gated(mut self) -> Self {
        self.dimsel_gated = true;
        self
    }

    /// L9.DIMSEL.RT — is this a DIMSEL-gated wrap (no late-arm)?
    pub fn dimsel_gated(&self) -> bool {
        self.dimsel_gated
    }

    /// Σ.Q05.CHAIN — mark this sideband's wrap as a cascade-chain
    /// INTERMEDIATE link (builder style; call before cloning into
    /// emitter + scan). See the field docs.
    pub fn mark_chain_intermediate(mut self) -> Self {
        self.chain_intermediate = true;
        self
    }

    /// Σ.Q05.CHAIN — is this a chain-intermediate wrap (bitmap must
    /// apply even when the pass-rate routing picks dense)?
    pub fn chain_intermediate(&self) -> bool {
        self.chain_intermediate
    }

    /// L9.ADAPT Guard 2 — handle to this wrap's shared probe-disarm
    /// counters (all clones of this sideband return the same handle).
    pub fn probe_disarm_handle(&self) -> Arc<ProbeDisarm> {
        Arc::clone(&self.probe_disarm)
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

    /// Σ.AJ.1: identity check via inner Arc pointer.
    ///
    /// Two cloned `BridgeFilterSideband` instances share the same
    /// inner storage; `ptr_eq` returns true iff both clones trace back
    /// to the same `BridgeFilterSideband::new()` allocation. Used by
    /// the broadcast-siblings rule to find the scan that holds a
    /// given emitter's primary sideband.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
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

    /// L9.ADAPT — wraps admitted only by the tight (NDV) estimator carry
    /// a marker so the probe-side peek applies the stricter wait budget.
    /// Default false (existing wraps keep today's behavior); set before
    /// the sideband fans out to emitter + scan, so clones carry it.
    #[test]
    fn tight_admitted_marker_roundtrip() {
        let sb = BridgeFilterSideband::new();
        assert!(!sb.tight_admitted());
        let sb = sb.mark_tight_admitted();
        assert!(sb.tight_admitted());
        let clone = sb.clone();
        assert!(clone.tight_admitted());
        assert!(sb.ptr_eq(&clone));
    }

    /// L9.ADAPT Guard 2 — disarm truth table: never before MIN_ROWS;
    /// above it, disarm exactly when cumulative pass-rate exceeds the
    /// stash threshold. Winners (Q08 ~0.7% pass) stay armed; FK-pass
    /// shapes (~100%) disarm as soon as the floor is met.
    #[test]
    fn probe_disarm_threshold_and_floor() {
        let d = ProbeDisarm::default();
        // 100% pass but below the row floor → stay armed.
        d.record(ProbeDisarm::MIN_ROWS - 1, ProbeDisarm::MIN_ROWS - 1);
        assert!(!d.disarmed(0.10));
        // Cross the floor with one more all-pass row group → disarm.
        d.record(1_000, 1_000);
        assert!(d.disarmed(0.10));

        // Winner shape: plenty of rows, 0.7% pass → armed forever.
        let w = ProbeDisarm::default();
        w.record(10_000_000, 70_000);
        assert!(!w.disarmed(0.10));

        // Boundary: pass-rate exactly AT the threshold keeps the probe
        // (strictly-greater disarms; at-threshold rows still stash).
        let b = ProbeDisarm::default();
        b.record(10_000_000, 1_000_000);
        assert!(!b.disarmed(0.10));
        b.record(0, 1); // nudge above
        assert!(b.disarmed(0.10));
    }

    /// Guard 2 — sideband clones share one disarm: a record through the
    /// scan-side clone is visible through the emitter-side clone.
    #[test]
    fn probe_disarm_shared_across_clones() {
        let sb = BridgeFilterSideband::new().mark_tight_admitted();
        let clone = sb.clone();
        sb.probe_disarm_handle()
            .record(ProbeDisarm::MIN_ROWS, ProbeDisarm::MIN_ROWS);
        assert!(clone.probe_disarm_handle().disarmed(0.10));
        assert!(Arc::ptr_eq(
            &sb.probe_disarm_handle(),
            &clone.probe_disarm_handle()
        ));
    }
}
