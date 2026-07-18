//! The parallel work source: a shared, lock-free morsel dispenser.
//!
//! The scan's parallel driver used to assign row groups to workers by a
//! static stride (`rg % nthreads`). That ties each worker to a fixed set
//! of morsels up front, so a worker dealt the fat/slow row groups — or one
//! that a noisy neighbour on the box slows down — becomes the straggler
//! that sets the wall while the others sit idle.
//!
//! [`MorselQueue`] replaces that: every worker pulls the next morsel by a
//! single atomic fetch-add, so a worker that finishes early immediately
//! absorbs the next available morsel instead of idling. Under skewed
//! morsel cost (SF-10 lineitem's row groups vary ~5× in row count) or a
//! straggler thread, the healthy workers drain the queue and the slow one
//! simply does fewer morsels — the load balances itself.
//!
//! This is the *work-sharing* form of a morsel scheduler (one shared
//! queue, as in DuckDB's morsel-driven model). For a flat, statically
//! known list of scan morsels it balances identically to per-worker
//! Chase-Lev deques with far less machinery; true work-*stealing* deques
//! only start to pay once operators recursively spawn nested morsels (a
//! later phase), at which point this type grows a per-worker tier behind
//! the same `next()` contract.

use std::sync::atomic::{AtomicUsize, Ordering};

/// A shared dispenser over morsel indices `0..total`. Constructed once per
/// parallel scan and shared read-only across every worker thread; `next()`
/// is the only mutating call and is lock-free.
pub struct MorselQueue {
    /// The next morsel index to hand out. Monotonic; once it reaches
    /// `total` the queue is drained and every further `next()` is `None`.
    next: AtomicUsize,
    total: usize,
}

impl MorselQueue {
    /// A queue that will hand out indices `0, 1, …, total-1` exactly once.
    pub fn new(total: usize) -> Self {
        Self {
            next: AtomicUsize::new(0),
            total,
        }
    }

    /// Claim the next morsel index, or `None` once the queue is drained.
    /// Lock-free and safe from any number of workers: each caller gets a
    /// distinct index (the atomic fetch-add serialises claims), so no
    /// morsel is ever decoded twice or dropped.
    ///
    /// `Relaxed` ordering suffices — the counter carries no data, only the
    /// claim; the morsel's payload (the row group) is read independently by
    /// whoever wins the index, and the caller joins the workers (an
    /// `Acquire`/`Release` barrier) before reading their results.
    #[inline]
    pub fn next(&self) -> Option<usize> {
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        if i < self.total { Some(i) } else { None }
    }

    /// Total morsels this queue dispenses over its lifetime.
    #[inline]
    pub fn total(&self) -> usize {
        self.total
    }
}
