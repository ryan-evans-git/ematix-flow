//! L9.HashSet (Q17 lever) — open-addressing i64 exact-membership set.
//!
//! Used by the L9 runtime bloom sideband when the build side is small
//! enough that an exact set is more efficient than the probabilistic
//! [`BloomFilter`](crate::bloom::BloomFilter).
//!
//! ## Why this exists
//!
//! The Σ.Q.L9 runtime bloom sideband ([[sigma-l1-speculative]]) ships
//! a `ColumnPredicate::I64InBloom` from the HashJoinExec build side
//! to the probe-side `EmatixFastParquetExec`. The probe scan calls
//! `BloomFilter::might_contain_i64` once per row to decide whether to
//! keep it in the row bitmap.
//!
//! Q17 SF=10 fresh profile (2026-05-24) measured the per-row bloom
//! check at **17.2 ns/probe** — 8 multiply-shift hash iterations with
//! early-out. The same Q17-shape A/B against a tiny open-addressing
//! i64 table comes in at **1.3 ns/probe** (13× faster) and a std
//! `HashSet<i64>` at 5.1 ns/probe (3.4× faster). The exact set also
//! eliminates the bloom's 1.13% false-positive rate that costs
//! downstream join work.
//!
//! ## Design
//!
//! - Open addressing on a power-of-two `Vec<i64>` with linear probe.
//! - `i64::MIN` is the empty sentinel. Inserting `i64::MIN` is
//!   rejected (returns false) — TPC-H never uses that value.
//! - Hash: splitmix-style multiply-shift (`v.wrapping_mul(0x9e37...)
//!   >> 32`) — same hash family as the bloom, deterministic, no
//!   external dep.
//! - Load factor cap: 50% (grow when `len * 2 >= cap`). Higher load
//!   pushes probe lengths up; at small N (a few thousand entries
//!   for the L9 use case) the cap headroom is cheap.
//! - No removal — the set is build-once, query-many.
//!
//! ## When to use
//!
//! `I64Set` should be picked over `BloomFilter` when:
//! 1. Total key count is small enough to fit in L2 cache for the
//!    probing thread (`< ~256 KB` data → < ~32K keys at 8 B each).
//! 2. False positives have real downstream cost (e.g. the probe-
//!    filter rejection saves a hash-join probe).
//!
//! For larger key sets (≥ 32K), the per-key memory cost of an exact
//! set starts to compete with the bloom's fixed bit budget, and the
//! cache-miss cost on probe lookups erodes the speed advantage.
//! Callers should fall back to `BloomFilter` past the threshold.

use std::sync::Arc;

/// Empty-slot sentinel. Real TPC-H i64 keys never collide with this
/// (l_orderkey, l_partkey, etc. are positive); shift in a check if
/// some downstream caller could feasibly insert it.
const EMPTY: i64 = i64::MIN;

/// Multiply-shift hash constant (splitmix64 mixer first stage).
const MULT: u64 = 0x9e37_79b9_7f4a_7c15;

/// Default initial capacity. Power of two; large enough to skip the
/// first grow chain for the typical L9-small-build use case (a few
/// thousand keys).
const INITIAL_CAPACITY: usize = 64;

/// Open-addressing i64 set. Build-once, query-many.
#[derive(Clone)]
pub struct I64Set {
    buckets: Vec<i64>,
    mask: usize,
    len: usize,
}

impl Default for I64Set {
    fn default() -> Self {
        Self::new()
    }
}

impl I64Set {
    pub fn new() -> Self {
        Self::with_capacity(INITIAL_CAPACITY)
    }

    /// Construct with at least `cap` buckets (rounded up to a power
    /// of two and at least `INITIAL_CAPACITY`).
    pub fn with_capacity(cap: usize) -> Self {
        let cap = cap.max(INITIAL_CAPACITY).next_power_of_two();
        Self {
            buckets: vec![EMPTY; cap],
            mask: cap - 1,
            len: 0,
        }
    }

    /// Pre-sized to hold `n` keys at 50% load — skips the grow chain
    /// when the caller already knows the build size.
    pub fn with_keys(n: usize) -> Self {
        let cap = (n * 2).max(INITIAL_CAPACITY).next_power_of_two();
        Self {
            buckets: vec![EMPTY; cap],
            mask: cap - 1,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.buckets.len()
    }

    /// Heap bytes held by the bucket array. Used by callers picking
    /// between `I64Set` and `BloomFilter` at publish time.
    pub fn byte_size(&self) -> usize {
        self.buckets.len() * std::mem::size_of::<i64>()
    }

    #[inline(always)]
    fn slot(&self, v: i64) -> usize {
        ((v as u64).wrapping_mul(MULT) >> 32) as usize & self.mask
    }

    /// Insert `v` into the set. Returns `true` if `v` was new,
    /// `false` if it was already present or if `v == i64::MIN`
    /// (the empty sentinel).
    pub fn insert(&mut self, v: i64) -> bool {
        if v == EMPTY {
            return false;
        }
        if self.len * 2 >= self.buckets.len() {
            self.grow();
        }
        let mut s = self.slot(v);
        loop {
            let b = self.buckets[s];
            if b == EMPTY {
                self.buckets[s] = v;
                self.len += 1;
                return true;
            }
            if b == v {
                return false;
            }
            s = (s + 1) & self.mask;
        }
    }

    /// Exact membership test. ~1.3 ns/probe at Q17-shape on M3 Pro.
    #[inline(always)]
    pub fn contains(&self, v: i64) -> bool {
        if v == EMPTY {
            return false;
        }
        let mut s = self.slot(v);
        loop {
            let b = self.buckets[s];
            if b == v {
                return true;
            }
            if b == EMPTY {
                return false;
            }
            s = (s + 1) & self.mask;
        }
    }

    /// Bulk insert from a slice. Pre-reserves capacity for the full
    /// slice up-front so the inner loop sees zero grows.
    pub fn extend_from_slice(&mut self, vs: &[i64]) {
        let needed = (self.len + vs.len()) * 2;
        while self.buckets.len() < needed.next_power_of_two().max(INITIAL_CAPACITY) {
            self.grow();
        }
        for &v in vs {
            self.insert(v);
        }
    }

    /// Union another `I64Set` into self.
    pub fn extend(&mut self, other: &I64Set) {
        let needed = (self.len + other.len) * 2;
        while self.buckets.len() < needed.next_power_of_two().max(INITIAL_CAPACITY) {
            self.grow();
        }
        for &b in &other.buckets {
            if b != EMPTY {
                self.insert(b);
            }
        }
    }

    /// Iterate over the keys in bucket order (NOT insertion order).
    pub fn iter(&self) -> impl Iterator<Item = i64> + '_ {
        self.buckets.iter().copied().filter(|&b| b != EMPTY)
    }

    fn grow(&mut self) {
        let new_cap = (self.buckets.len() * 2).max(INITIAL_CAPACITY);
        let old = std::mem::replace(&mut self.buckets, vec![EMPTY; new_cap]);
        self.mask = new_cap - 1;
        self.len = 0;
        for b in old {
            if b != EMPTY {
                // Re-insert; cannot already be present in the fresh
                // bucket array.
                let mut s = self.slot(b);
                loop {
                    if self.buckets[s] == EMPTY {
                        self.buckets[s] = b;
                        self.len += 1;
                        break;
                    }
                    s = (s + 1) & self.mask;
                }
            }
        }
    }
}

impl std::fmt::Debug for I64Set {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "I64Set {{ len: {}, capacity: {} }}",
            self.len,
            self.buckets.len()
        )
    }
}

/// Convenience constructor: build an `Arc<I64Set>` from an iterator
/// of keys. Pre-sizes to skip grows.
pub fn arc_from_iter<I: ExactSizeIterator<Item = i64>>(keys: I) -> Arc<I64Set> {
    let n = keys.len();
    let mut s = I64Set::with_keys(n);
    for k in keys {
        s.insert(k);
    }
    Arc::new(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_set_is_empty() {
        let s = I64Set::new();
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
        assert!(!s.contains(0));
        assert!(!s.contains(42));
    }

    #[test]
    fn insert_and_contains_match() {
        let mut s = I64Set::new();
        for k in &[1i64, 42, 100, -5, 1_000_000] {
            assert!(s.insert(*k), "first insert of {k} should return true");
            assert!(s.contains(*k));
        }
        assert_eq!(s.len(), 5);
        assert!(!s.contains(2));
        assert!(!s.contains(7));
    }

    #[test]
    fn duplicate_insert_returns_false_no_double_count() {
        let mut s = I64Set::new();
        assert!(s.insert(42));
        assert!(!s.insert(42));
        assert_eq!(s.len(), 1);
        assert!(s.contains(42));
    }

    #[test]
    fn empty_sentinel_is_rejected() {
        let mut s = I64Set::new();
        assert!(!s.insert(i64::MIN));
        assert!(!s.contains(i64::MIN));
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn grow_preserves_existing_keys() {
        let mut s = I64Set::with_capacity(INITIAL_CAPACITY);
        let n = 1024;
        for i in 0..n as i64 {
            s.insert(i * 37 + 1);
        }
        assert_eq!(s.len(), n);
        for i in 0..n as i64 {
            assert!(
                s.contains(i * 37 + 1),
                "missing key after grow: {}",
                i * 37 + 1
            );
        }
        assert!(!s.contains(0));
        assert!(!s.contains(2));
    }

    #[test]
    fn extend_from_slice_matches_individual_inserts() {
        let keys: Vec<i64> = (0..100).map(|i| i * 7 + 11).collect();
        let mut a = I64Set::new();
        for &k in &keys {
            a.insert(k);
        }
        let mut b = I64Set::new();
        b.extend_from_slice(&keys);
        for &k in &keys {
            assert!(a.contains(k));
            assert!(b.contains(k));
        }
        assert_eq!(a.len(), b.len());
    }

    #[test]
    fn extend_unions_another_set() {
        let mut a = I64Set::new();
        for k in &[1i64, 2, 3] {
            a.insert(*k);
        }
        let mut b = I64Set::new();
        for k in &[3i64, 4, 5] {
            b.insert(*k);
        }
        a.extend(&b);
        assert_eq!(a.len(), 5);
        for k in &[1i64, 2, 3, 4, 5] {
            assert!(a.contains(*k));
        }
    }

    #[test]
    fn iter_yields_all_inserted_keys() {
        let mut s = I64Set::new();
        let mut expected: Vec<i64> = (0..50).map(|i| i * 13 + 1).collect();
        for &k in &expected {
            s.insert(k);
        }
        let mut got: Vec<i64> = s.iter().collect();
        got.sort_unstable();
        expected.sort_unstable();
        assert_eq!(got, expected);
    }

    #[test]
    fn with_keys_pre_sizes_to_skip_grows() {
        let s = I64Set::with_keys(1000);
        // 1000 keys @ 50% load → ≥ 2000 buckets, rounded up to 2048
        assert!(s.capacity() >= 2000);
        assert!(s.capacity().is_power_of_two());
    }

    #[test]
    fn arc_from_iter_builds_populated_set() {
        let keys: Vec<i64> = vec![10, 20, 30, 40];
        let s = arc_from_iter(keys.iter().copied());
        for &k in &keys {
            assert!(s.contains(k));
        }
        assert_eq!(s.len(), 4);
    }

    #[test]
    fn q17_shape_membership_matches_naive_hashset() {
        use std::collections::HashSet;
        let n_keys = 2_044i64;
        let keys: Vec<i64> = (0..n_keys).map(|i| (i * 977) % 2_000_000 + 1).collect();

        let mut ours = I64Set::with_keys(n_keys as usize);
        for k in &keys {
            ours.insert(*k);
        }
        let theirs: HashSet<i64> = keys.iter().copied().collect();

        for v in 0..3_000_000i64 {
            assert_eq!(
                ours.contains(v),
                theirs.contains(&v),
                "disagreement at {v}: ours={}, theirs={}",
                ours.contains(v),
                theirs.contains(&v),
            );
        }
    }
}
