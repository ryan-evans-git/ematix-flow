//! Σ.T V5 L13 Story 2.1 — Robin Hood hash-join table for i64 keys.
//!
//! See `lib.rs` for the rationale + OQ-L13-A resolution. This module
//! ships the bare data structure: build phase + probe phase, no
//! operator integration.
//!
//! ## Layout
//!
//! ```text
//!     buckets[]          chain_nodes[]
//!     ┌──────────┐       ┌─────────────────────┐
//!     │ key=42   │──┐    │ build_row_idx=  0   │
//!     │ head ●───┼──┘──> │ next ●──────────────┼──┐
//!     │ psl=0    │       └─────────────────────┘  │
//!     ├──────────┤       ┌─────────────────────┐  │
//!     │ key=7    │──┐    │ build_row_idx=  4   │<─┘  (next match for key=42)
//!     │ head ●───┼──┐    │ next = NULL_CHAIN   │
//!     │ psl=2    │  │    └─────────────────────┘
//!     ├──────────┤  │    ┌─────────────────────┐
//!     │ empty    │  └──> │ build_row_idx=  9   │
//!     └──────────┘       │ next = NULL_CHAIN   │
//!                        └─────────────────────┘
//! ```
//!
//! `buckets` is open-addressed Robin Hood (probe distance
//! monotonically non-decreasing along a chain). `chain_nodes` is
//! a side-table linked list — one node per inserted (key, row_idx)
//! pair. Build duplicates → multiple chain nodes per bucket.
//!
//! Why a linked list for duplicates instead of `SmallVec` /
//! `Vec<u32>` per bucket: the chain_nodes pool is one contiguous
//! `Vec` allocation, so push-the-row in the build hot loop is one
//! `chain_nodes[new_idx] = ChainNode { ... }` write + one
//! `buckets[slot].head = new_idx` write. A `SmallVec<[u32; 4]>`
//! inline branches on inline-vs-spilled and adds a length field;
//! a `Vec<u32>` per bucket is N allocations for N distinct keys.
//! The chain layout keeps the build phase to one allocation
//! upfront + bumping the chain index.

use std::iter::Iterator;

const INITIAL_CAPACITY: usize = 64;
const MAX_LOAD_FACTOR_NUMERATOR: usize = 7;
const MAX_LOAD_FACTOR_DENOMINATOR: usize = 10; // 70%

const EMPTY_PSL: u32 = u32::MAX;
const NULL_CHAIN: u32 = u32::MAX;

#[derive(Debug, Clone, Copy)]
struct Bucket {
    key: i64,
    /// Index into `chain_nodes`. `NULL_CHAIN` = vacant bucket.
    head: u32,
    /// Probe distance from ideal slot. `EMPTY_PSL` = vacant.
    psl: u32,
}

impl Bucket {
    const fn empty() -> Self {
        Self {
            key: 0,
            head: NULL_CHAIN,
            psl: EMPTY_PSL,
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.psl == EMPTY_PSL
    }
}

#[derive(Debug, Clone, Copy)]
struct ChainNode {
    build_row_idx: u32,
    /// Index into `chain_nodes` of the next match for the same key;
    /// `NULL_CHAIN` marks the end of the chain.
    next: u32,
}

/// Output pair from a probe. `probe_row_idx` is the row in the
/// caller's probe-side batch (offset by the `probe_row_idx_base`
/// arg); `build_row_idx` is the absolute build-side row index that
/// was stored at insert time (offset by `row_idx_base`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProbeMatch {
    pub probe_row_idx: u32,
    pub build_row_idx: u32,
}

/// Robin Hood hash-join table specialised for `i64` keys + `u32`
/// build-side row indices. Inner-join semantics: duplicate keys on
/// either side emit the cross product of matches; NULL keys never
/// match (caller passes per-row validity).
pub struct RobinHoodHashJoinI64Table {
    buckets: Vec<Bucket>,
    /// Power-of-two `buckets.len() - 1` for cheap modulo via mask.
    mask: usize,
    /// Distinct keys in the table.
    n_keys: usize,
    /// Total `(key, build_row_idx)` pairs inserted.
    n_rows: usize,

    /// Linked-list pool of build-side payload row indices. Indexed
    /// from `Bucket::head` and walked via `ChainNode::next`.
    chain_nodes: Vec<ChainNode>,
}

impl Default for RobinHoodHashJoinI64Table {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinHoodHashJoinI64Table {
    /// New table with the minimum capacity (64 buckets). Use
    /// [`with_capacity`] when the build-side row count is known.
    ///
    /// [`with_capacity`]: Self::with_capacity
    pub fn new() -> Self {
        Self::with_capacity(INITIAL_CAPACITY)
    }

    /// New table sized for `n_build_rows_expected` distinct-key
    /// inserts at 70% load. Capacity rounds up to the next power
    /// of two ≥ `INITIAL_CAPACITY`.
    ///
    /// **Critical perf knob.** Per Σ.N.f.1, sizing the table from
    /// the caller's distinct-key estimate beats lazy-grow by 2-4×
    /// at 10K-200K keys. Callers should pass the build-side row
    /// count (an upper bound — actual distinct keys is <= rows).
    pub fn with_capacity(n_build_rows_expected: usize) -> Self {
        let min_cap = (n_build_rows_expected * MAX_LOAD_FACTOR_DENOMINATOR)
            .div_ceil(MAX_LOAD_FACTOR_NUMERATOR)
            .max(INITIAL_CAPACITY)
            .next_power_of_two();
        Self {
            buckets: vec![Bucket::empty(); min_cap],
            mask: min_cap - 1,
            n_keys: 0,
            n_rows: 0,
            chain_nodes: Vec::with_capacity(n_build_rows_expected),
        }
    }

    /// Distinct keys.
    pub fn len(&self) -> usize {
        self.n_keys
    }

    /// Total inserted rows (= sum of chain lengths).
    pub fn n_rows(&self) -> usize {
        self.n_rows
    }

    pub fn is_empty(&self) -> bool {
        self.n_keys == 0
    }

    pub fn capacity(&self) -> usize {
        self.buckets.len()
    }

    /// Ingest a batch of build-side keys.
    ///
    /// - `keys`: the i64 keys for this batch.
    /// - `nulls`: `Some(mask)` if any keys may be NULL; `mask[i]
    ///   == true` means the row is valid (not null). NULL rows
    ///   are silently skipped (Inner-join semantics: NULL never
    ///   matches anything).
    /// - `row_idx_base`: absolute row index of `keys[0]` in the
    ///   caller's build relation. Multi-batch builds pass
    ///   increasing bases so the emitted `build_row_idx` stays
    ///   consistent with the caller's row numbering.
    pub fn insert_batch(&mut self, keys: &[i64], nulls: Option<&[bool]>, row_idx_base: u32) {
        for (i, &key) in keys.iter().enumerate() {
            let valid = nulls.map(|m| m[i]).unwrap_or(true);
            if !valid {
                continue;
            }
            self.insert(key, row_idx_base + i as u32);
        }
    }

    /// Insert a single `(key, build_row_idx)` pair. Allows
    /// duplicates: subsequent inserts for the same key push onto
    /// the front of that key's chain.
    pub fn insert(&mut self, key: i64, build_row_idx: u32) {
        if self.needs_grow() {
            self.grow();
        }

        let new_node_idx = self.chain_nodes.len() as u32;
        // We push the chain node first; if the key already exists,
        // the new node's `next` points at the existing chain's
        // head (front-insertion onto the chain).
        let h = hash_i64(key);
        let mut slot = h & self.mask;
        let mut incoming = Bucket {
            key,
            head: new_node_idx,
            psl: 0,
        };
        // Walk Robin Hood until we find either an existing bucket
        // for this key (chain prepend) or an empty slot (new key).
        loop {
            let b = self.buckets[slot];
            if b.is_empty() {
                self.chain_nodes.push(ChainNode {
                    build_row_idx,
                    next: NULL_CHAIN,
                });
                self.buckets[slot] = incoming;
                self.n_keys += 1;
                self.n_rows += 1;
                return;
            }
            if b.key == incoming.key {
                // Key already in the table — chain-prepend.
                self.chain_nodes.push(ChainNode {
                    build_row_idx,
                    next: b.head,
                });
                self.buckets[slot].head = new_node_idx;
                self.n_rows += 1;
                return;
            }
            if incoming.psl > b.psl {
                // Robin Hood — incoming displaces the incumbent.
                // Critical: incoming carries the original key all
                // the way through, even after displacement, so the
                // "already in the table" check above stays correct
                // for every loop iteration.
                self.buckets[slot] = incoming;
                incoming = b;
            }
            slot = (slot + 1) & self.mask;
            incoming.psl = incoming.psl.saturating_add(1);
        }
    }

    /// Probe with a batch of `(key, probe_row_idx)` pairs.
    ///
    /// For each matching key in the build side, emit one
    /// `ProbeMatch` per build-side row that shares the key
    /// (Inner-join cross-product semantics for duplicate keys).
    ///
    /// - `probe_keys`: the i64 probe keys.
    /// - `nulls`: `Some(mask)` if any probe keys may be NULL;
    ///   `mask[i] == true` means valid. NULL probe rows emit
    ///   no matches.
    /// - `probe_row_idx_base`: absolute row index of
    ///   `probe_keys[0]` in the caller's probe relation.
    /// - `out`: matches are pushed onto this Vec; existing entries
    ///   are preserved. Callers should pre-size with
    ///   `out.reserve(probe_keys.len())` for a tight inner loop
    ///   when most probes are expected to match.
    pub fn probe_batch(
        &self,
        probe_keys: &[i64],
        nulls: Option<&[bool]>,
        probe_row_idx_base: u32,
        out: &mut Vec<ProbeMatch>,
    ) {
        if self.buckets.is_empty() || self.n_rows == 0 {
            return;
        }
        for (i, &key) in probe_keys.iter().enumerate() {
            let valid = nulls.map(|m| m[i]).unwrap_or(true);
            if !valid {
                continue;
            }
            if let Some(head) = self.lookup_chain_head(key) {
                let probe_row_idx = probe_row_idx_base + i as u32;
                let mut node_idx = head;
                while node_idx != NULL_CHAIN {
                    let node = self.chain_nodes[node_idx as usize];
                    out.push(ProbeMatch {
                        probe_row_idx,
                        build_row_idx: node.build_row_idx,
                    });
                    node_idx = node.next;
                }
            }
        }
    }

    /// Iterate every `(key, count)` pair in the table. `count` is
    /// the number of `(key, build_row_idx)` entries inserted for
    /// that key (== chain length).
    ///
    /// Iteration order is bucket order, not insertion order.
    /// O(n_buckets + n_rows) — walks every bucket and every chain
    /// node. Cheap enough for end-of-build skew analysis; do not
    /// call inside the probe hot loop.
    pub fn iter_key_counts(&self) -> impl Iterator<Item = (i64, usize)> + '_ {
        self.buckets.iter().filter(|b| !b.is_empty()).map(|b| {
            let mut count: usize = 0;
            let mut node_idx = b.head;
            while node_idx != NULL_CHAIN {
                count += 1;
                node_idx = self.chain_nodes[node_idx as usize].next;
            }
            (b.key, count)
        })
    }

    /// Returns every `build_row_idx` inserted for `key`, in
    /// insertion-reverse order (front-prepended chain).
    ///
    /// Allocates a `Vec` — intended for one-off lookup
    /// (skew-partition extraction, debugging), not the hot probe
    /// path.
    pub fn build_rows_for_key(&self, key: i64) -> Vec<u32> {
        let mut out = Vec::new();
        if let Some(head) = self.lookup_chain_head(key) {
            let mut node_idx = head;
            while node_idx != NULL_CHAIN {
                let node = self.chain_nodes[node_idx as usize];
                out.push(node.build_row_idx);
                node_idx = node.next;
            }
        }
        out
    }

    /// Read-only lookup. Returns the head chain-node index for
    /// `key`, or `None` if not present.
    #[inline]
    fn lookup_chain_head(&self, key: i64) -> Option<u32> {
        let h = hash_i64(key);
        let mut slot = h & self.mask;
        let mut psl: u32 = 0;
        loop {
            let b = self.buckets[slot];
            if b.is_empty() {
                return None;
            }
            if b.key == key {
                return Some(b.head);
            }
            // Robin Hood invariant: if our probe distance exceeds
            // the incumbent's, the key cannot be in the table —
            // if it were, it would have displaced this bucket on
            // insert.
            if psl > b.psl {
                return None;
            }
            slot = (slot + 1) & self.mask;
            psl = psl.saturating_add(1);
            // Defensive bound — table is power-of-two sized and
            // grow() maintains load < 70%, so this should never
            // trip, but bail rather than infinite-loop if the
            // invariant is somehow violated.
            if psl as usize >= self.buckets.len() {
                return None;
            }
        }
    }

    fn needs_grow(&self) -> bool {
        // load >= 70%
        self.n_keys * MAX_LOAD_FACTOR_DENOMINATOR >= self.buckets.len() * MAX_LOAD_FACTOR_NUMERATOR
    }

    fn grow(&mut self) {
        let new_cap = self.buckets.len() * 2;
        let old = std::mem::replace(&mut self.buckets, vec![Bucket::empty(); new_cap]);
        self.mask = new_cap - 1;
        let saved_keys = self.n_keys;
        self.n_keys = 0;
        // Re-insert each bucket head; the chain_nodes side-table
        // doesn't move so head pointers stay valid.
        for b in old {
            if !b.is_empty() {
                self.reinsert_bucket(b);
            }
        }
        debug_assert_eq!(self.n_keys, saved_keys, "grow lost distinct-key count");
    }

    /// During grow: walk Robin Hood to place an existing bucket
    /// (key, head, psl reset). Does NOT touch chain_nodes or
    /// n_rows — those are stable across grow.
    fn reinsert_bucket(&mut self, b: Bucket) {
        let h = hash_i64(b.key);
        let mut slot = h & self.mask;
        let mut incoming = Bucket {
            key: b.key,
            head: b.head,
            psl: 0,
        };
        loop {
            let cur = self.buckets[slot];
            if cur.is_empty() {
                self.buckets[slot] = incoming;
                self.n_keys += 1;
                return;
            }
            if incoming.psl > cur.psl {
                self.buckets[slot] = incoming;
                incoming = cur;
            }
            slot = (slot + 1) & self.mask;
            incoming.psl = incoming.psl.saturating_add(1);
        }
    }
}

/// Splitmix64 — fast, deterministic; matches
/// `crates/ematix-flow-core/src/robin_hood_agg.rs::hash_i64`.
#[inline]
fn hash_i64(v: i64) -> usize {
    let mut x = v as u64;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (x ^ (x >> 31)) as usize
}
