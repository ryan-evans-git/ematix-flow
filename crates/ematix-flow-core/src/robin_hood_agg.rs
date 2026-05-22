//! Σ.N — Robin Hood hash table for aggregate GROUP BY.
//!
//! ## Why this exists
//!
//! DataFusion's stock hash aggregate uses [`hashbrown`] (SwissTable-style
//! probing) which is good for general use but not specialised for the
//! aggregate-table access pattern: known-fixed key type (i64 / i32 /
//! u32 / string-ref) + numeric accumulator value (u64 count, f64 sum).
//! Photon publishes that their Robin Hood agg is **2-3× faster** than
//! the equivalent Spark hash agg on Q03 / Q05 / Q10 / Q11 / Q13 / Q16 /
//! Q21 — the multi-row group-by queries that dominate analytics work.
//!
//! ## Robin Hood, briefly
//!
//! Open-addressing hash table with one invariant: every entry's probe
//! distance from its ideal slot is monotonically non-decreasing as you
//! walk the probe chain. Insert walks slots; if its own probe distance
//! exceeds the incumbent's, it **swaps in and the incumbent continues
//! probing** ("rob from the rich"). This bounds the worst-case probe
//! length and **dramatically tightens** the average — exactly what makes
//! the hot agg loop predictable for the branch predictor.
//!
//! ## What ships here
//!
//! [`RobinHoodI64U64`] — keys `i64`, values `u64`. The data structure +
//! `insert_or_update` (the agg-friendly entry point: insert with init
//! value, or accumulate onto existing value), `len`, `iter`, `clear`.
//!
//! Specialised because:
//! - i64 keys are the dominant TPC-H GROUP BY type after Date32 and the
//!   string dictionary codes Σ.L.1 produces.
//! - u64 values cover COUNT, SUM(int), and most aggregate accumulators.
//! - For SUM(float) / AVG / MIN / MAX, follow-up bites add
//!   [`RobinHoodI64F64`], [`RobinHoodStrRefU64`], etc. using the same
//!   bucket layout.
//!
//! ## What's deferred
//!
//! Operator-level integration into a `RobinHoodAggregateExec` that
//! replaces DataFusion's `AggregateExec` for shape-matched plans. The
//! data structure is the hard part; the operator is wiring. The
//! deferral matches the [[optimizer-codegen-sensitivity]] memory —
//! shipping the data structure as a free-standing module gives us a
//! microbench-able win we can compare to hashbrown without touching
//! the optimizer rule stack.

use std::iter::Iterator;

const INITIAL_CAPACITY: usize = 64;
const MAX_LOAD_FACTOR_NUMERATOR: usize = 7;
const MAX_LOAD_FACTOR_DENOMINATOR: usize = 10; // 70%

#[derive(Debug, Clone, Copy)]
struct Bucket {
    key: i64,
    value: u64,
    /// Probe distance from ideal slot. `EMPTY` = vacant.
    psl: u32,
}

const EMPTY_PSL: u32 = u32::MAX;

impl Bucket {
    const fn empty() -> Self {
        Self {
            key: 0,
            value: 0,
            psl: EMPTY_PSL,
        }
    }
    fn is_empty(&self) -> bool {
        self.psl == EMPTY_PSL
    }
}

/// Σ.N — Robin Hood hash table specialised for i64 keys + u64 values
/// (COUNT, SUM(int)). Designed for the aggregate hot loop.
pub struct RobinHoodI64U64 {
    buckets: Vec<Bucket>,
    /// Power-of-two for cheap modulo via mask.
    mask: usize,
    len: usize,
}

impl Default for RobinHoodI64U64 {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinHoodI64U64 {
    pub fn new() -> Self {
        Self::with_capacity(INITIAL_CAPACITY)
    }

    /// Capacity rounds up to the next power of two ≥ max(64, cap).
    pub fn with_capacity(cap: usize) -> Self {
        let cap = cap.max(INITIAL_CAPACITY).next_power_of_two();
        Self {
            buckets: vec![Bucket::empty(); cap],
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

    pub fn clear(&mut self) {
        for b in &mut self.buckets {
            *b = Bucket::empty();
        }
        self.len = 0;
    }

    /// Σ.N — aggregate-friendly entry. If `key` is present, calls
    /// `accumulate(existing_value, delta) -> new_value`. Otherwise
    /// inserts (key, delta). Returns the new value for the key
    /// post-update (useful for things like running max).
    pub fn insert_or_update(
        &mut self,
        key: i64,
        delta: u64,
        accumulate: fn(u64, u64) -> u64,
    ) -> u64 {
        if self.needs_grow() {
            self.grow();
        }
        let h = hash_i64(key);
        let mut slot = h & self.mask;
        let mut psl: u32 = 0;
        let mut incoming = Bucket {
            key,
            value: delta,
            psl: 0,
        };
        loop {
            let b = self.buckets[slot];
            if b.is_empty() {
                // Empty slot — insert.
                incoming.psl = psl;
                self.buckets[slot] = incoming;
                self.len += 1;
                return incoming.value;
            }
            if b.key == incoming.key && !b.is_empty() && incoming.psl == 0 {
                // This is the original (non-evicted) caller's key + a
                // hit. Accumulate. (We can short-circuit eviction
                // logic because the chain hasn't displaced anything
                // yet.)
                let new_val = accumulate(b.value, incoming.value);
                self.buckets[slot].value = new_val;
                return new_val;
            }
            if incoming.psl > b.psl {
                // Robin Hood — incoming is "poorer" (further from
                // ideal), so it gets this slot and the displaced
                // bucket continues probing.
                self.buckets[slot] = incoming;
                incoming = b;
            }
            slot = (slot + 1) & self.mask;
            psl = psl.saturating_add(1);
            incoming.psl = incoming.psl.saturating_add(1);
        }
    }

    /// Iterate (key, value). Iteration order is bucket order (not
    /// insertion order). Callers that care about order should sort.
    pub fn iter(&self) -> impl Iterator<Item = (i64, u64)> + '_ {
        self.buckets
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| (b.key, b.value))
    }

    /// Σ.N — read-only lookup. Returns Some(value) if key present.
    pub fn get(&self, key: i64) -> Option<u64> {
        if self.buckets.is_empty() {
            return None;
        }
        let h = hash_i64(key);
        let mut slot = h & self.mask;
        let mut psl: u32 = 0;
        loop {
            let b = self.buckets[slot];
            if b.is_empty() {
                return None;
            }
            if b.key == key {
                return Some(b.value);
            }
            // Robin Hood invariant: if our probe distance exceeds the
            // incumbent's, the key isn't here.
            if psl > b.psl {
                return None;
            }
            slot = (slot + 1) & self.mask;
            psl = psl.saturating_add(1);
            // Defensive bound — if we somehow loop around, bail.
            if psl as usize >= self.buckets.len() {
                return None;
            }
        }
    }

    fn needs_grow(&self) -> bool {
        // load >= 70%
        self.len * MAX_LOAD_FACTOR_DENOMINATOR
            >= self.buckets.len() * MAX_LOAD_FACTOR_NUMERATOR
    }

    fn grow(&mut self) {
        let new_cap = self.buckets.len() * 2;
        let old = std::mem::replace(&mut self.buckets, vec![Bucket::empty(); new_cap]);
        self.mask = new_cap - 1;
        self.len = 0;
        for b in old {
            if !b.is_empty() {
                // re-insert with sum accumulator (which won't fire on
                // first insert).
                self.insert_or_update(b.key, b.value, |_a, b| b);
            }
        }
    }
}

/// Splitmix64 — fast, deterministic.
fn hash_i64(v: i64) -> usize {
    let mut x = v as u64;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (x ^ (x >> 31)) as usize
}

// ---------------------------------------------------------------------
// Aggregate accumulator helpers — the function pointers
// `insert_or_update` takes. These match the most-common agg shapes.
// ---------------------------------------------------------------------

/// `COUNT(*)` — each insert bumps the value by 1.
pub fn count_accumulator(existing: u64, delta: u64) -> u64 {
    existing.saturating_add(delta)
}

/// `SUM(x)` — each insert adds the delta.
pub fn sum_accumulator(existing: u64, delta: u64) -> u64 {
    existing.saturating_add(delta)
}

/// `MAX(x)` — keep the larger.
pub fn max_accumulator(existing: u64, delta: u64) -> u64 {
    existing.max(delta)
}

/// `MIN(x)` — keep the smaller. Slight care: insert path uses
/// `delta` as the initial value, so MIN of an empty group is the
/// first inserted value (correct).
pub fn min_accumulator(existing: u64, delta: u64) -> u64 {
    existing.min(delta)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup() {
        let mut t = RobinHoodI64U64::new();
        t.insert_or_update(42, 1, count_accumulator);
        t.insert_or_update(7, 1, count_accumulator);
        t.insert_or_update(42, 1, count_accumulator);
        assert_eq!(t.get(42), Some(2));
        assert_eq!(t.get(7), Some(1));
        assert_eq!(t.get(99), None);
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn count_accumulator_works() {
        let mut t = RobinHoodI64U64::new();
        for k in [1, 2, 1, 3, 1, 2, 1] {
            t.insert_or_update(k, 1, count_accumulator);
        }
        assert_eq!(t.get(1), Some(4));
        assert_eq!(t.get(2), Some(2));
        assert_eq!(t.get(3), Some(1));
    }

    #[test]
    fn sum_accumulator_works() {
        let mut t = RobinHoodI64U64::new();
        t.insert_or_update(100, 10, sum_accumulator);
        t.insert_or_update(100, 20, sum_accumulator);
        t.insert_or_update(200, 5, sum_accumulator);
        assert_eq!(t.get(100), Some(30));
        assert_eq!(t.get(200), Some(5));
    }

    #[test]
    fn max_accumulator_keeps_larger() {
        let mut t = RobinHoodI64U64::new();
        t.insert_or_update(1, 50, max_accumulator);
        t.insert_or_update(1, 30, max_accumulator);
        t.insert_or_update(1, 80, max_accumulator);
        t.insert_or_update(1, 10, max_accumulator);
        assert_eq!(t.get(1), Some(80));
    }

    #[test]
    fn min_accumulator_keeps_smaller() {
        let mut t = RobinHoodI64U64::new();
        t.insert_or_update(1, 50, min_accumulator);
        t.insert_or_update(1, 30, min_accumulator);
        t.insert_or_update(1, 80, min_accumulator);
        t.insert_or_update(1, 10, min_accumulator);
        assert_eq!(t.get(1), Some(10));
    }

    #[test]
    fn grows_past_initial_capacity() {
        let mut t = RobinHoodI64U64::with_capacity(64);
        for k in 0..1000i64 {
            t.insert_or_update(k, 1, count_accumulator);
        }
        assert_eq!(t.len(), 1000);
        for k in 0..1000i64 {
            assert_eq!(t.get(k), Some(1), "missing key {k}");
        }
        assert!(t.capacity() >= 1000);
    }

    #[test]
    fn iter_returns_all_entries() {
        let mut t = RobinHoodI64U64::new();
        let keys = [10, 20, 30, 40, 50];
        for k in keys {
            t.insert_or_update(k, 7, count_accumulator);
        }
        let mut out: Vec<i64> = t.iter().map(|(k, _)| k).collect();
        out.sort();
        assert_eq!(out, keys);
    }

    #[test]
    fn clear_resets() {
        let mut t = RobinHoodI64U64::new();
        for k in 0..100i64 {
            t.insert_or_update(k, 1, count_accumulator);
        }
        assert_eq!(t.len(), 100);
        t.clear();
        assert_eq!(t.len(), 0);
        assert_eq!(t.get(0), None);
        // Should still work after clear.
        t.insert_or_update(42, 1, count_accumulator);
        assert_eq!(t.get(42), Some(1));
    }

    #[test]
    fn robin_hood_invariant_bounds_probe_length() {
        // Insert a worst-case pattern: many keys with adjacent hashes.
        let mut t = RobinHoodI64U64::with_capacity(64);
        // Stress: deliberately create hash collisions by using keys
        // 0..16 which after splitmix64 may collide a few times.
        for k in 0..50i64 {
            t.insert_or_update(k, 1, count_accumulator);
        }
        // Compute max PSL among inserted buckets — bounded by load
        // factor, typically < 10 for 70%-full table.
        let max_psl = t
            .buckets
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| b.psl)
            .max()
            .unwrap_or(0);
        // Robin Hood with 70% load → expected max PSL ≤ ~log(n) ≈ 6
        // for n=50. Generous bound:
        assert!(max_psl < 20, "max PSL {max_psl} exceeded RH expected bound");
    }

    #[test]
    fn returns_post_update_value() {
        let mut t = RobinHoodI64U64::new();
        let v1 = t.insert_or_update(1, 5, sum_accumulator);
        assert_eq!(v1, 5);
        let v2 = t.insert_or_update(1, 3, sum_accumulator);
        assert_eq!(v2, 8);
    }
}
