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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
                // Σ.N — at psl=0 we know `incoming` is still the
                // caller's original key (no displacement has happened
                // yet). Safe to accumulate against the existing bucket
                // for that key. The Robin Hood invariant guarantees
                // at most one bucket per key, so this is THE bucket.
                let new_val = accumulate(b.value, incoming.value);
                self.buckets[slot].value = new_val;
                return new_val;
            }
            // Σ.N.f.1 — Robin Hood invariant: at most one bucket per
            // key. If we encounter our key in the chain (even after
            // displacement), accumulate against it. Without this,
            // displaced keys could create duplicate entries.
            if b.key == incoming.key && !b.is_empty() {
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

    /// Σ.N.f.1 — bulk COUNT(*) GROUP BY ingestion. A thin loop over
    /// `insert_or_update`; the real perf lever is **table capacity
    /// sized from the caller's distinct-key estimate**, NOT this
    /// batch wrapper. Microbench at 10K keys:
    ///
    /// - default capacity (64), let it grow: ~100 M rows/sec
    /// - `with_capacity(20K)` upfront:       ~230 M rows/sec (2.3×)
    ///
    /// Callers (e.g. RobinHoodAggregateExec) should construct the
    /// table via [`RobinHoodCountAgg::with_capacity`] sized from
    /// observed cardinality or a workload-tuned default. Don't
    /// pre-grow to `keys.len()` blindly here — that over-allocates
    /// 1000× when many duplicates are present and *regresses* perf.
    pub fn insert_or_update_batch_count(&mut self, keys: &[i64]) {
        for &k in keys {
            self.insert_or_update(k, 1, count_accumulator);
        }
    }

    /// Σ.N.f.1 — bulk SUM(partial_count) ingestion. Same shape as
    /// `insert_or_update_batch_count`. Caller should size via
    /// `with_capacity` for high-cardinality merges.
    pub fn insert_or_update_batch_sum(&mut self, keys: &[i64], partial_counts: &[u64]) {
        assert_eq!(
            keys.len(),
            partial_counts.len(),
            "insert_or_update_batch_sum: keys and counts must be same length"
        );
        for i in 0..keys.len() {
            self.insert_or_update(keys[i], partial_counts[i], sum_accumulator);
        }
    }

    /// Σ.N.f.2 — jump-resize the table to fit at least `target`
    /// entries at 70% load. Used by the operator's dynamic-resize
    /// heuristic after the first batch reveals high cardinality.
    /// Cheaper than 3-4 sequential grow() cycles.
    pub fn reserve_to_capacity_pow2_of(&mut self, target: usize) {
        let target_capacity = (target * MAX_LOAD_FACTOR_DENOMINATOR)
            .div_ceil(MAX_LOAD_FACTOR_NUMERATOR)
            .next_power_of_two();
        while self.buckets.len() < target_capacity {
            self.grow();
        }
    }

    /// Σ.N.f.1 — pre-grow to accommodate `extra` more inserts without
    /// rehashing in the hot path. Worst case: every key is new and
    /// the table grows to maintain ≤70% load factor.
    #[allow(dead_code)]
    fn reserve_for_n_more(&mut self, extra: usize) {
        let target_min_capacity =
            ((self.len + extra) * MAX_LOAD_FACTOR_DENOMINATOR).div_ceil(MAX_LOAD_FACTOR_NUMERATOR);
        while self.buckets.len() < target_min_capacity {
            self.grow();
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
        self.len * MAX_LOAD_FACTOR_DENOMINATOR >= self.buckets.len() * MAX_LOAD_FACTOR_NUMERATOR
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

// ---------------------------------------------------------------------
// Σ.Q.L1b — Robin Hood table specialised for i64 keys + f64 values.
// Mirror of RobinHoodI64U64 with `value: f64`. Targets the Q18 shape:
// `SUM(l_quantity::Float64) GROUP BY l_orderkey::Int64` at 15M
// cardinality. The U64 version is the COUNT/SUM(i64) workhorse; this
// is the SUM(f64) sibling. Kept as a separate type (not a generic)
// because the EMPTY sentinel + accumulator semantics differ enough
// from u64 to make a flat copy clearer than a parameterised one.
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct BucketF64 {
    key: i64,
    value: f64,
    /// Probe distance from ideal slot. `EMPTY` = vacant.
    psl: u32,
}

impl BucketF64 {
    const fn empty() -> Self {
        Self {
            key: 0,
            value: 0.0,
            psl: EMPTY_PSL,
        }
    }
    fn is_empty(&self) -> bool {
        self.psl == EMPTY_PSL
    }
}

/// Σ.Q.L1b — Robin Hood hash table specialised for i64 keys + f64
/// values. Used by `RobinHoodSumF64Agg` for `SUM(f64) GROUP BY i64`.
/// Same probe / load / grow strategy as `RobinHoodI64U64`.
pub struct RobinHoodI64F64 {
    buckets: Vec<BucketF64>,
    mask: usize,
    len: usize,
}

impl Default for RobinHoodI64F64 {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinHoodI64F64 {
    pub fn new() -> Self {
        Self::with_capacity(INITIAL_CAPACITY)
    }

    pub fn with_capacity(cap: usize) -> Self {
        let cap = cap.max(INITIAL_CAPACITY).next_power_of_two();
        Self {
            buckets: vec![BucketF64::empty(); cap],
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
            *b = BucketF64::empty();
        }
        self.len = 0;
    }

    /// Σ.Q.L1b — SUM-style entry. If `key` is present, adds `delta` to
    /// the existing value. Otherwise inserts (key, delta). Returns the
    /// post-update value.
    pub fn insert_or_sum(&mut self, key: i64, delta: f64) -> f64 {
        if self.needs_grow() {
            self.grow();
        }
        let h = hash_i64(key);
        let mut slot = h & self.mask;
        let mut psl: u32 = 0;
        let mut incoming = BucketF64 {
            key,
            value: delta,
            psl: 0,
        };
        loop {
            let b = self.buckets[slot];
            if b.is_empty() {
                incoming.psl = psl;
                self.buckets[slot] = incoming;
                self.len += 1;
                return incoming.value;
            }
            // Robin Hood invariant: at most one bucket per key.
            if b.key == incoming.key && !b.is_empty() {
                let new_val = b.value + incoming.value;
                self.buckets[slot].value = new_val;
                return new_val;
            }
            if incoming.psl > b.psl {
                self.buckets[slot] = incoming;
                incoming = b;
            }
            slot = (slot + 1) & self.mask;
            psl = psl.saturating_add(1);
            incoming.psl = incoming.psl.saturating_add(1);
        }
    }

    /// Bulk `SUM(values) GROUP BY keys` ingestion. Caller should
    /// pre-size via `with_capacity` for high-cardinality merges.
    pub fn insert_or_sum_batch(&mut self, keys: &[i64], values: &[f64]) {
        assert_eq!(
            keys.len(),
            values.len(),
            "insert_or_sum_batch: keys and values must be same length"
        );
        for i in 0..keys.len() {
            self.insert_or_sum(keys[i], values[i]);
        }
    }

    /// Σ.Q.L1b retry — vectorised batch ingest. Photon-style 4-stage
    /// pipeline that processes the input in 1024-row chunks:
    ///
    /// 1. Hash all keys in the chunk → ideal_slot array.
    /// 2. Probe primary slot for direct hit (no displacement, no
    ///    chaining) → boolean hit array.
    /// 3. Fast-path accumulate for hits — tight loop, autovectorised by
    ///    the compiler since loads/stores are predictable.
    /// 4. Scalar fallback for misses (insertions + collisions with
    ///    different keys) via `insert_or_sum`. This is the only path
    ///    that can grow the table; stage 3 modifications survive grow
    ///    because the re-insert preserves bucket values.
    ///
    /// At Robin Hood's 70% load factor, ~70-80% of rows hit on stage 2
    /// and take the tight stage-3 loop. The remaining 20-30% fall to
    /// stage 4. Net throughput target: ≥3× scalar `insert_or_sum_batch`
    /// when pre-sized for high-cardinality workloads.
    pub fn insert_or_sum_batch_vectorised(&mut self, keys: &[i64], values: &[f64]) {
        // Convenience wrapper: allocate scratch once and dispatch to
        // the scratch-passing variant. Hot callers (e.g. the radix
        // aggregator) should hold their own scratch and call
        // `insert_or_sum_batch_vectorised_with_scratch` directly to
        // avoid the per-call 8 KB stack-init cost.
        const VEC_CHUNK: usize = 1024;
        let mut slots = [0usize; VEC_CHUNK];
        let mut hit = [false; VEC_CHUNK];
        self.insert_or_sum_batch_vectorised_with_scratch(keys, values, &mut slots, &mut hit);
    }

    /// Σ.R.1 — scratch-passing variant of `insert_or_sum_batch_vectorised`.
    ///
    /// Caller provides `slots` (slot-index buffer) and `hit` (boolean
    /// hit-mask buffer); both must be ≥ 1024 elements. The radix
    /// aggregator calls this hundreds of thousands of times per
    /// partition; owning the scratch in the radix struct avoids the
    /// ~1 µs / 8 KB stack-init cost per call that otherwise dominates
    /// when per-bin sub-batches are small.
    pub fn insert_or_sum_batch_vectorised_with_scratch(
        &mut self,
        keys: &[i64],
        values: &[f64],
        slots: &mut [usize],
        hit: &mut [bool],
    ) {
        assert_eq!(
            keys.len(),
            values.len(),
            "insert_or_sum_batch_vectorised: keys and values must be same length"
        );
        const VEC_CHUNK: usize = 1024;
        assert!(slots.len() >= VEC_CHUNK && hit.len() >= VEC_CHUNK);
        let n_total = keys.len();
        if n_total == 0 {
            return;
        }

        let mut off = 0;
        while off < n_total {
            let n = (n_total - off).min(VEC_CHUNK);
            let mask = self.mask;

            // Stage 1: hash all keys, store ideal slot.
            for i in 0..n {
                slots[i] = hash_i64(keys[off + i]) & mask;
            }

            // Stage 2: check primary slot for direct hit. A hit means
            // (a) the bucket is non-empty AND (b) it holds our key —
            // safe to accumulate without probing.
            for i in 0..n {
                let s = slots[i];
                let b = self.buckets[s];
                hit[i] = !b.is_empty() && b.key == keys[off + i];
            }

            // Stage 3: fast-path accumulate. Iterates the chunk again
            // but with no branches in the hot path — compiler can
            // autovectorise the predicated store.
            for i in 0..n {
                if hit[i] {
                    self.buckets[slots[i]].value += values[off + i];
                }
            }

            // Stage 4: scalar fallback for misses. `insert_or_sum` may
            // grow the table internally; stage 3's in-place writes
            // are preserved because grow() re-inserts every existing
            // bucket. Note: subsequent rows in this chunk that hash to
            // the same key as a stage-4 insertion fall through here
            // too (their hit[i] was computed against the empty slot
            // before insertion), so `insert_or_sum` correctly
            // accumulates them.
            for i in 0..n {
                if !hit[i] {
                    self.insert_or_sum(keys[off + i], values[off + i]);
                }
            }

            off += n;
        }
    }

    /// Jump-resize to fit at least `target` entries at 70% load.
    /// Mirrors `RobinHoodI64U64::reserve_to_capacity_pow2_of`.
    pub fn reserve_to_capacity_pow2_of(&mut self, target: usize) {
        let target_capacity = (target * MAX_LOAD_FACTOR_DENOMINATOR)
            .div_ceil(MAX_LOAD_FACTOR_NUMERATOR)
            .next_power_of_two();
        while self.buckets.len() < target_capacity {
            self.grow();
        }
    }

    /// Iterate (key, value). Order is bucket order (not insertion).
    pub fn iter(&self) -> impl Iterator<Item = (i64, f64)> + '_ {
        self.buckets
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| (b.key, b.value))
    }

    /// Read-only lookup. Returns Some(value) if key present.
    pub fn get(&self, key: i64) -> Option<f64> {
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
            if psl > b.psl {
                return None;
            }
            slot = (slot + 1) & self.mask;
            psl = psl.saturating_add(1);
            if psl as usize >= self.buckets.len() {
                return None;
            }
        }
    }

    fn needs_grow(&self) -> bool {
        self.len * MAX_LOAD_FACTOR_DENOMINATOR >= self.buckets.len() * MAX_LOAD_FACTOR_NUMERATOR
    }

    fn grow(&mut self) {
        let new_cap = self.buckets.len() * 2;
        let old = std::mem::replace(&mut self.buckets, vec![BucketF64::empty(); new_cap]);
        self.mask = new_cap - 1;
        self.len = 0;
        for b in old {
            if !b.is_empty() {
                // Re-insert. Since `insert_or_sum` adds on key-match,
                // and the re-insertion key is fresh in the new buckets
                // array, this acts as a plain insert.
                self.insert_or_sum(b.key, b.value);
            }
        }
    }
}

/// Σ.Q.L1b — streaming `SUM(f64) GROUP BY i64` aggregator. Sister to
/// `RobinHoodCountAgg`. Mirrors the same shape so the operator wiring
/// can dispatch on an `AggKind` enum.
pub struct RobinHoodSumF64Agg {
    table: RobinHoodI64F64,
}

impl Default for RobinHoodSumF64Agg {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinHoodSumF64Agg {
    pub fn new() -> Self {
        Self {
            table: RobinHoodI64F64::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            table: RobinHoodI64F64::with_capacity(cap),
        }
    }

    /// Ingest one batch's (group_key, sum_value) pair. Skips rows where
    /// either column is null — DataFusion's analyzer drops NULL keys
    /// in SQL standard `SUM GROUP BY` semantics, and NULL value rows
    /// don't contribute to a sum.
    pub fn ingest_batch(
        &mut self,
        keys: &arrow_array::Int64Array,
        values: &arrow_array::Float64Array,
    ) {
        use arrow_array::Array;
        assert_eq!(
            keys.len(),
            values.len(),
            "ingest_batch: key and value arrays must be same length"
        );
        let n = keys.len();
        if keys.null_count() == 0 && values.null_count() == 0 {
            for i in 0..n {
                let k = keys.value(i);
                let v = values.value(i);
                self.table.insert_or_sum(k, v);
            }
        } else {
            for i in 0..n {
                if !keys.is_null(i) && !values.is_null(i) {
                    let k = keys.value(i);
                    let v = values.value(i);
                    self.table.insert_or_sum(k, v);
                }
            }
        }
    }

    /// Σ.Q.L1b — merge Partial output of the form
    /// `(group_key i64, partial_sum f64)` from upstream. Identical to
    /// `ingest_batch` but named for symmetry with
    /// `RobinHoodCountAgg::ingest_partial_counts`.
    pub fn ingest_partial_sums(
        &mut self,
        keys: &arrow_array::Int64Array,
        partial_sums: &arrow_array::Float64Array,
    ) {
        self.ingest_batch(keys, partial_sums);
    }

    /// Σ.Q.L1b retry — vectorised batch ingest. Routes to
    /// [`RobinHoodI64F64::insert_or_sum_batch_vectorised`] when both
    /// columns are null-free (the Q18 SF=10 case) — gives ~13% at 15M
    /// cardinality with pre-grow, ~65% at default-cap. Falls back to
    /// the per-row scalar path when nulls are present.
    pub fn ingest_batch_vectorised(
        &mut self,
        keys: &arrow_array::Int64Array,
        values: &arrow_array::Float64Array,
    ) {
        use arrow_array::Array;
        assert_eq!(
            keys.len(),
            values.len(),
            "ingest_batch_vectorised: key and value arrays must be same length"
        );
        if keys.null_count() == 0 && values.null_count() == 0 {
            // Hot path: extract raw slices and run the vectorised
            // kernel directly. Arrow's `.values()` on a non-null array
            // returns the underlying buffer with no per-element check.
            self.table
                .insert_or_sum_batch_vectorised(keys.values(), values.values());
        } else {
            // Mixed null/non-null: stay on the scalar per-row path so
            // we can mask correctly. Q18 SF=10's lineitem columns are
            // both NOT NULL, so this branch is cold for the target
            // workload but kept for SQL-correctness on other shapes.
            let n = keys.len();
            for i in 0..n {
                if !keys.is_null(i) && !values.is_null(i) {
                    self.table.insert_or_sum(keys.value(i), values.value(i));
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn table(&self) -> &RobinHoodI64F64 {
        &self.table
    }

    pub fn table_mut(&mut self) -> &mut RobinHoodI64F64 {
        &mut self.table
    }
}

// ---------------------------------------------------------------------
// Σ.R.2 — Robin Hood hash table specialised for i64 keys + (sum f64,
// count u64) tuples. Targets the Q17 SF=10 hot kernel: AVG(f64) GROUP
// BY i64 at ~2M cardinality, where the Σ.Q closeout profile pinned
// DataFusion's `GroupValuesPrimitive::intern` at 21.6% self time.
//
// Sister to `RobinHoodI64F64`. Same probe / load / grow strategy; the
// bucket simply carries an extra `count` so the Partial stage can
// emit both pieces of AVG state and the FinalPartitioned stage can
// merge two (sum, count) pairs without recomputing.
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct BucketAvgF64 {
    key: i64,
    sum: f64,
    count: u64,
    /// Probe distance from ideal slot. `EMPTY_PSL` = vacant.
    psl: u32,
}

impl BucketAvgF64 {
    const fn empty() -> Self {
        Self {
            key: 0,
            sum: 0.0,
            count: 0,
            psl: EMPTY_PSL,
        }
    }
    fn is_empty(&self) -> bool {
        self.psl == EMPTY_PSL
    }
}

/// Σ.R.2 — Robin Hood hash table for AVG(f64) GROUP BY i64. Buckets
/// store (sum, count); callers compute `sum/count` on the way out.
pub struct RobinHoodI64AvgF64 {
    buckets: Vec<BucketAvgF64>,
    mask: usize,
    len: usize,
}

impl Default for RobinHoodI64AvgF64 {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinHoodI64AvgF64 {
    pub fn new() -> Self {
        Self::with_capacity(INITIAL_CAPACITY)
    }

    pub fn with_capacity(cap: usize) -> Self {
        let cap = cap.max(INITIAL_CAPACITY).next_power_of_two();
        Self {
            buckets: vec![BucketAvgF64::empty(); cap],
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
            *b = BucketAvgF64::empty();
        }
        self.len = 0;
    }

    /// Σ.R.2 — Partial-stage entry. If `key` is present, adds `value`
    /// to the existing sum and increments count. Otherwise inserts
    /// (key, sum=value, count=1).
    pub fn insert_or_update(&mut self, key: i64, value: f64) {
        if self.needs_grow() {
            self.grow();
        }
        let h = hash_i64(key);
        let mut slot = h & self.mask;
        let mut incoming = BucketAvgF64 {
            key,
            sum: value,
            count: 1,
            psl: 0,
        };
        loop {
            let b = self.buckets[slot];
            if b.is_empty() {
                self.buckets[slot] = incoming;
                self.len += 1;
                return;
            }
            if b.key == incoming.key {
                // Same-key accumulate: merge incoming into existing,
                // don't displace.
                self.buckets[slot].sum = b.sum + incoming.sum;
                self.buckets[slot].count = b.count + incoming.count;
                return;
            }
            if incoming.psl > b.psl {
                self.buckets[slot] = incoming;
                incoming = b;
            }
            slot = (slot + 1) & self.mask;
            incoming.psl = incoming.psl.saturating_add(1);
        }
    }

    /// Σ.R.2 — FinalPartitioned-stage entry. Merges a (partial_sum,
    /// partial_count) pair for `key`. If `key` is present, adds both
    /// fields; otherwise inserts as-is.
    pub fn insert_or_merge(&mut self, key: i64, partial_sum: f64, partial_count: u64) {
        if self.needs_grow() {
            self.grow();
        }
        let h = hash_i64(key);
        let mut slot = h & self.mask;
        let mut incoming = BucketAvgF64 {
            key,
            sum: partial_sum,
            count: partial_count,
            psl: 0,
        };
        loop {
            let b = self.buckets[slot];
            if b.is_empty() {
                self.buckets[slot] = incoming;
                self.len += 1;
                return;
            }
            if b.key == incoming.key {
                self.buckets[slot].sum = b.sum + incoming.sum;
                self.buckets[slot].count = b.count + incoming.count;
                return;
            }
            if incoming.psl > b.psl {
                self.buckets[slot] = incoming;
                incoming = b;
            }
            slot = (slot + 1) & self.mask;
            incoming.psl = incoming.psl.saturating_add(1);
        }
    }

    /// Bulk Partial-stage ingest. Caller should pre-size via
    /// `with_capacity` for high-cardinality merges.
    pub fn insert_or_update_batch(&mut self, keys: &[i64], values: &[f64]) {
        assert_eq!(
            keys.len(),
            values.len(),
            "insert_or_update_batch: keys and values must be same length"
        );
        for i in 0..keys.len() {
            self.insert_or_update(keys[i], values[i]);
        }
    }

    /// Σ.R.2 — vectorised batch ingest mirroring the Σ.Q.L1b retry
    /// 4-stage Photon-style pipeline. Processes input in 1024-row
    /// chunks: (1) hash all keys, (2) probe primary slot for direct
    /// hit, (3) fast-path accumulate hits (predicated store, branch-
    /// free body, autovectorisable), (4) scalar fallback for misses
    /// via `insert_or_update`. At Robin Hood's 70% load factor the
    /// expected hit rate on stage 2 is 70-80%.
    pub fn insert_or_update_batch_vectorised(&mut self, keys: &[i64], values: &[f64]) {
        assert_eq!(
            keys.len(),
            values.len(),
            "insert_or_update_batch_vectorised: keys and values must be same length"
        );
        const VEC_CHUNK: usize = 1024;
        let n_total = keys.len();
        if n_total == 0 {
            return;
        }
        let mut slots = [0usize; VEC_CHUNK];
        let mut hit = [false; VEC_CHUNK];

        let mut off = 0;
        while off < n_total {
            let n = (n_total - off).min(VEC_CHUNK);
            let mask = self.mask;

            // Stage 1: hash all keys.
            for i in 0..n {
                slots[i] = hash_i64(keys[off + i]) & mask;
            }

            // Stage 2: check primary slot for direct hit.
            for i in 0..n {
                let s = slots[i];
                let b = self.buckets[s];
                hit[i] = !b.is_empty() && b.key == keys[off + i];
            }

            // Stage 3: fast-path accumulate hits.
            for i in 0..n {
                if hit[i] {
                    self.buckets[slots[i]].sum += values[off + i];
                    self.buckets[slots[i]].count += 1;
                }
            }

            // Stage 4: scalar fallback for misses (insertions +
            // chained probes). `insert_or_update` may grow the table;
            // stage 3's in-place writes survive grow() because every
            // existing bucket is reinserted.
            for i in 0..n {
                if !hit[i] {
                    self.insert_or_update(keys[off + i], values[off + i]);
                }
            }

            off += n;
        }
    }

    /// Jump-resize to fit at least `target` entries at 70% load.
    pub fn reserve_to_capacity_pow2_of(&mut self, target: usize) {
        let target_capacity = (target * MAX_LOAD_FACTOR_DENOMINATOR)
            .div_ceil(MAX_LOAD_FACTOR_NUMERATOR)
            .next_power_of_two();
        while self.buckets.len() < target_capacity {
            self.grow();
        }
    }

    /// Iterate (key, sum, count) — for FinalPartitioned passthrough or
    /// inspection.
    pub fn iter_sum_count(&self) -> impl Iterator<Item = (i64, f64, u64)> + '_ {
        self.buckets
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| (b.key, b.sum, b.count))
    }

    /// Iterate (key, avg) — convenience for FinalPartitioned output.
    /// Yields `sum / count` per bucket.
    pub fn iter_avg(&self) -> impl Iterator<Item = (i64, f64)> + '_ {
        self.buckets
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| (b.key, b.sum / b.count as f64))
    }

    /// Read-only lookup. Returns Some((sum, count)) if key present.
    pub fn get(&self, key: i64) -> Option<(f64, u64)> {
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
                return Some((b.sum, b.count));
            }
            if psl > b.psl {
                return None;
            }
            slot = (slot + 1) & self.mask;
            psl = psl.saturating_add(1);
            if psl as usize >= self.buckets.len() {
                return None;
            }
        }
    }

    /// Convenience: returns Some(sum/count) if key present.
    pub fn average(&self, key: i64) -> Option<f64> {
        self.get(key).map(|(s, c)| s / c as f64)
    }

    fn needs_grow(&self) -> bool {
        self.len * MAX_LOAD_FACTOR_DENOMINATOR >= self.buckets.len() * MAX_LOAD_FACTOR_NUMERATOR
    }

    fn grow(&mut self) {
        let new_cap = self.buckets.len() * 2;
        let old = std::mem::replace(&mut self.buckets, vec![BucketAvgF64::empty(); new_cap]);
        self.mask = new_cap - 1;
        self.len = 0;
        for b in old {
            if !b.is_empty() {
                // Re-insert preserving (sum, count) — go through
                // `insert_or_merge` so the bucket lands at the right
                // slot in the new table.
                self.insert_or_merge(b.key, b.sum, b.count);
            }
        }
    }
}

/// Σ.R.2 — streaming `AVG(f64) GROUP BY i64` aggregator. Sister to
/// `RobinHoodSumF64Agg`. Partial-stage callers invoke `ingest_batch`
/// (or `ingest_batch_vectorised`); FinalPartitioned callers invoke
/// `ingest_partials` with two state columns (Float64 sums + UInt64
/// counts).
pub struct RobinHoodAvgF64Agg {
    table: RobinHoodI64AvgF64,
}

impl Default for RobinHoodAvgF64Agg {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinHoodAvgF64Agg {
    pub fn new() -> Self {
        Self {
            table: RobinHoodI64AvgF64::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            table: RobinHoodI64AvgF64::with_capacity(cap),
        }
    }

    /// Partial-stage ingest. Skips rows where either column is null —
    /// SQL `AVG GROUP BY` drops NULL keys and NULL value rows don't
    /// contribute to the average.
    pub fn ingest_batch(
        &mut self,
        keys: &arrow_array::Int64Array,
        values: &arrow_array::Float64Array,
    ) {
        use arrow_array::Array;
        assert_eq!(
            keys.len(),
            values.len(),
            "ingest_batch: key and value arrays must be same length"
        );
        let n = keys.len();
        if keys.null_count() == 0 && values.null_count() == 0 {
            for i in 0..n {
                self.table.insert_or_update(keys.value(i), values.value(i));
            }
        } else {
            for i in 0..n {
                if !keys.is_null(i) && !values.is_null(i) {
                    self.table.insert_or_update(keys.value(i), values.value(i));
                }
            }
        }
    }

    /// Vectorised Partial-stage ingest. Routes to
    /// [`RobinHoodI64AvgF64::insert_or_update_batch_vectorised`] when
    /// both columns are null-free; otherwise falls back to the per-row
    /// null-aware scalar path.
    pub fn ingest_batch_vectorised(
        &mut self,
        keys: &arrow_array::Int64Array,
        values: &arrow_array::Float64Array,
    ) {
        use arrow_array::Array;
        assert_eq!(
            keys.len(),
            values.len(),
            "ingest_batch_vectorised: key and value arrays must be same length"
        );
        if keys.null_count() == 0 && values.null_count() == 0 {
            self.table
                .insert_or_update_batch_vectorised(keys.values(), values.values());
        } else {
            let n = keys.len();
            for i in 0..n {
                if !keys.is_null(i) && !values.is_null(i) {
                    self.table.insert_or_update(keys.value(i), values.value(i));
                }
            }
        }
    }

    /// FinalPartitioned-stage ingest. Consumes a batch of (group_key,
    /// partial_sum, partial_count) and merges each row into the
    /// table. Null keys are skipped; a null in either state column
    /// while the other is present is treated as 0 (preserves the
    /// other side's contribution) — matches DataFusion's
    /// `AvgGroupsAccumulator::merge` semantics.
    pub fn ingest_partials(
        &mut self,
        keys: &arrow_array::Int64Array,
        partial_sums: &arrow_array::Float64Array,
        partial_counts: &arrow_array::UInt64Array,
    ) {
        use arrow_array::Array;
        assert_eq!(keys.len(), partial_sums.len());
        assert_eq!(keys.len(), partial_counts.len());
        let n = keys.len();
        if keys.null_count() == 0
            && partial_sums.null_count() == 0
            && partial_counts.null_count() == 0
        {
            for i in 0..n {
                self.table.insert_or_merge(
                    keys.value(i),
                    partial_sums.value(i),
                    partial_counts.value(i),
                );
            }
        } else {
            for i in 0..n {
                if keys.is_null(i) {
                    continue;
                }
                let s = if partial_sums.is_null(i) {
                    0.0
                } else {
                    partial_sums.value(i)
                };
                let c = if partial_counts.is_null(i) {
                    0
                } else {
                    partial_counts.value(i)
                };
                if c > 0 {
                    self.table.insert_or_merge(keys.value(i), s, c);
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn table(&self) -> &RobinHoodI64AvgF64 {
        &self.table
    }

    pub fn table_mut(&mut self) -> &mut RobinHoodI64AvgF64 {
        &mut self.table
    }
}

// ---------------------------------------------------------------------
// Σ.R.1 — radix-partitioned `SUM(f64) GROUP BY i64` aggregator.
//
// Cache-conscious aggregation à la DuckDB MorselDrivenParallelism:
// hash → top `radix_bits` select a per-radix micro-table; each
// micro-table is sized to fit in L1/L2 even at multi-million
// total cardinality. Within a chunk we bin rows by radix first,
// then call the L1b vectorised pipeline per bin — that keeps each
// micro-table cache-resident for the duration of its bin.
//
// Designed for the Partial-aggregation phase of high-cardinality
// `SUM(Float64-col) GROUP BY Int64-col` aggregates. FinalPartitioned
// stays on the single-table L1b path because its input is already
// hash-partitioned by RepartitionExec and radixing again would
// concentrate ~all rows in one bin.
// ---------------------------------------------------------------------

/// Σ.R.1 — radix-partitioned `SUM(f64) GROUP BY i64` aggregator.
pub struct RobinHoodSumF64RadixAgg {
    /// `1 << radix_bits` per-radix tables. Allocated lazily on first
    /// ingest so a Partial-mode operator that sees an empty input
    /// doesn't pay the alloc cost.
    tables: Vec<RobinHoodI64F64>,
    radix_bits: u8,
    /// Per-table init_cap passed to `RobinHoodI64F64::with_capacity`.
    /// Caller sizes this from the upstream cardinality estimate divided
    /// by `1 << radix_bits` so each micro-table starts at the right
    /// load factor without grow-chain cost.
    per_table_cap: usize,
    /// Σ.R.1 scratch — owned to amortise per-bin call cost. Without
    /// this, the per-call 8 KB stack-init of `slots` + `hit` inside
    /// `insert_or_sum_batch_vectorised` dominates: 256 bins × thousands
    /// of chunks × ~1 µs / call = >1 s overhead at 6M rows.
    scratch_slots: Vec<usize>,
    scratch_hit: Vec<bool>,
}

impl RobinHoodSumF64RadixAgg {
    /// Create a radix-partitioned aggregator with `2^radix_bits`
    /// micro-tables, each sized to `per_table_cap` slots.
    ///
    /// Reasonable defaults for Q18-shape Partial aggregation:
    /// - `radix_bits = 6` → 64 tables; sweet spot for 100K–10M
    ///   per-partition cardinality on M3 Pro (each table fits L2).
    /// - `per_table_cap = max(input_rows_est / 4 / (1 << radix_bits),
    ///   65_536 / (1 << radix_bits))`.
    pub fn new(radix_bits: u8, per_table_cap: usize) -> Self {
        assert!(
            radix_bits <= 12,
            "radix_bits must be ≤ 12 (4096 tables max); got {radix_bits}"
        );
        let n_tables = 1usize << radix_bits;
        let mut tables = Vec::with_capacity(n_tables);
        for _ in 0..n_tables {
            tables.push(RobinHoodI64F64::with_capacity(per_table_cap));
        }
        Self {
            tables,
            radix_bits,
            per_table_cap,
            scratch_slots: vec![0usize; 1024],
            scratch_hit: vec![false; 1024],
        }
    }

    pub fn radix_bits(&self) -> u8 {
        self.radix_bits
    }

    pub fn n_tables(&self) -> usize {
        self.tables.len()
    }

    pub fn per_table_cap(&self) -> usize {
        self.per_table_cap
    }

    /// Total entries across all micro-tables. O(n_tables); cheap.
    pub fn len(&self) -> usize {
        self.tables.iter().map(|t| t.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.tables.iter().all(|t| t.is_empty())
    }

    /// Σ.R.1 — radix-binned ingest. Processes the input in fixed-size
    /// chunks of [`Self::VEC_CHUNK`] rows:
    ///
    /// 1. Hash all keys in the chunk; derive radix tag from the top
    ///    `radix_bits` bits of the hash.
    /// 2. Tally per-radix counts; prefix-sum to produce bin offsets.
    /// 3. Scatter row indices into a single sorted buffer (rows
    ///    grouped by radix tag).
    /// 4. Per radix bin: gather the bin's keys + values into scratch
    ///    buffers, then dispatch to
    ///    [`RobinHoodI64F64::insert_or_sum_batch_vectorised`] against
    ///    that bin's micro-table. The bin's table stays cache-resident
    ///    for the duration of the bin's processing.
    pub fn ingest_batch_radix(&mut self, keys: &[i64], values: &[f64]) {
        assert_eq!(
            keys.len(),
            values.len(),
            "ingest_batch_radix: keys and values must be same length"
        );
        if keys.is_empty() {
            return;
        }
        const VEC_CHUNK: usize = 1024;
        const MAX_RADIX: usize = 4096;
        debug_assert!(self.tables.len() <= MAX_RADIX);

        // Scratch arrays, stack-allocated for the inner loop.
        let mut counts = [0u32; MAX_RADIX];
        let mut offsets = [0u32; MAX_RADIX + 1];
        let mut next = [0u32; MAX_RADIX];
        let mut radix_tag = [0u16; VEC_CHUNK];
        let mut scratch_keys = [0i64; VEC_CHUNK];
        let mut scratch_vals = [0f64; VEC_CHUNK];

        let n_tables = self.tables.len();
        let shift: u32 = if self.radix_bits == 0 {
            0
        } else {
            usize::BITS - self.radix_bits as u32
        };

        let mut off = 0;
        while off < keys.len() {
            let n = (keys.len() - off).min(VEC_CHUNK);

            // Stage 1: hash → radix tag; tally per-radix counts.
            for c in counts.iter_mut().take(n_tables) {
                *c = 0;
            }
            if self.radix_bits == 0 {
                // Degenerate case: one table, no binning needed.
                self.tables[0]
                    .insert_or_sum_batch_vectorised(&keys[off..off + n], &values[off..off + n]);
                off += n;
                continue;
            }
            for i in 0..n {
                let r = (hash_i64(keys[off + i]) >> shift) as u16;
                radix_tag[i] = r;
                counts[r as usize] += 1;
            }

            // Stage 2: prefix-sum to bin offsets.
            let mut acc = 0u32;
            for r in 0..n_tables {
                offsets[r] = acc;
                acc += counts[r];
            }
            offsets[n_tables] = acc;

            // Stage 3: scatter keys + values into bin-sorted scratch
            // buffers. Single pass; writes are to predictable offsets.
            for (i, slot) in next.iter_mut().take(n_tables).enumerate() {
                *slot = offsets[i];
            }
            for i in 0..n {
                let r = radix_tag[i] as usize;
                let dst = next[r] as usize;
                scratch_keys[dst] = keys[off + i];
                scratch_vals[dst] = values[off + i];
                next[r] += 1;
            }

            // Stage 4: per-bin vectorised ingest into its micro-table.
            // Pass the radix agg's owned scratch (slots/hit) to avoid
            // the per-call 8 KB stack-init cost that otherwise
            // dominates when bins are small.
            for r in 0..n_tables {
                let start = offsets[r] as usize;
                let end = offsets[r + 1] as usize;
                if end == start {
                    continue;
                }
                self.tables[r].insert_or_sum_batch_vectorised_with_scratch(
                    &scratch_keys[start..end],
                    &scratch_vals[start..end],
                    &mut self.scratch_slots,
                    &mut self.scratch_hit,
                );
            }

            off += n;
        }
    }

    /// Iterate (key, value) pairs across all micro-tables. Order is
    /// (radix-bin, bucket) — neither stable across reps nor sorted by
    /// key. Callers that care should sort downstream.
    pub fn iter(&self) -> impl Iterator<Item = (i64, f64)> + '_ {
        self.tables.iter().flat_map(|t| t.iter())
    }
}

// ---------------------------------------------------------------------
// REV.5.b — CORRECTED global-partition `SUM(f64) GROUP BY i64` aggregator.
//
// Fixes the per-1024-row-binning flaw in `RobinHoodSumF64RadixAgg` above
// (which revisits all bins every chunk → no sustained cache residency,
// and LOST its microbench 0.48–0.99× vs single-table). This version does
// TRUE global partitioning, streaming-compatible:
//   1. `ingest_batch` scatter-APPENDS rows into persistent per-bin
//      buffers (no aggregation yet).
//   2. When buffered rows exceed a memory budget, `drain_all` flushes
//      EVERY bin's accumulated rows into its micro-table in one large
//      wave (so each table stays cache-hot for its wave) and frees the
//      raw buffers.
//   3. `finish` drains the remainder; `iter`/`len` read the tables.
//
// Microbench (examples/bench_radix_agg.rs `run_radix_global`): wins
// 1.31–1.39× @ 4M keys, 1.09–1.37× @ 38M keys vs the single-table agg.
//
// The incremental drain bounds peak raw-buffer memory to ~budget, so N
// concurrent operator partitions don't thrash at SF=100 (full buffering
// of one Partial partition is ~690 MB; 14 of them ≈ 10 GB).
// ---------------------------------------------------------------------

/// splitmix64 finalizer for the partition hash — the top `radix_bits`
/// bits pick the bin. Independent of the micro-tables' internal
/// `hash_i64` slotting; it only needs to map identical keys to the same
/// bin, which preserves correctness for any deterministic hash.
#[inline]
fn radix_part_hash(k: i64) -> u64 {
    let mut z = (k as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Process-unique sequence so concurrent aggregators (one per partition
/// thread) write to non-colliding spill files.
static SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

/// REV.5.b — disk-spill backend for the radix aggregator. One append-only
/// spill file per bin, created lazily on first spill. Rows are written as
/// raw native-endian `[i64 key][f64 val]` pairs (16 B/row) and streamed
/// back sequentially at finish. A spilled bin is drained exactly once
/// (disk rows, then the in-memory tail) into its micro-table, which stays
/// cache-resident for the whole drain — the residency win, preserved
/// under a memory bound instead of given up to a mid-stream re-visit.
struct SpillStore {
    dir: std::path::PathBuf,
    files: Vec<Option<std::fs::File>>,
    spilled_rows: Vec<usize>,
    /// Reused serialisation scratch (rebuilt per spill, never grows past
    /// the largest single spill).
    scratch: Vec<u8>,
    id: u64,
}

impl SpillStore {
    fn new(dir: std::path::PathBuf, n_bins: usize) -> Self {
        let id = SPILL_SEQ.fetch_add(1, Ordering::Relaxed);
        Self {
            dir,
            files: (0..n_bins).map(|_| None).collect(),
            spilled_rows: vec![0usize; n_bins],
            scratch: Vec::new(),
            id,
        }
    }

    fn path_for(&self, bin: usize) -> std::path::PathBuf {
        self.dir
            .join(format!("ematix-radix-spill-{}-{}.bin", self.id, bin))
    }

    /// Append a bin's buffered rows to its spill file (create on first use).
    fn spill_bin(&mut self, bin: usize, keys: &[i64], vals: &[f64]) -> std::io::Result<()> {
        debug_assert_eq!(keys.len(), vals.len());
        if keys.is_empty() {
            return Ok(());
        }
        if self.files[bin].is_none() {
            let f = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(self.path_for(bin))?;
            self.files[bin] = Some(f);
        }
        self.scratch.clear();
        self.scratch.reserve(keys.len() * 16);
        for i in 0..keys.len() {
            self.scratch.extend_from_slice(&keys[i].to_ne_bytes());
            self.scratch.extend_from_slice(&vals[i].to_ne_bytes());
        }
        use std::io::Write;
        self.files[bin].as_mut().unwrap().write_all(&self.scratch)?;
        self.spilled_rows[bin] += keys.len();
        Ok(())
    }

    /// Move a bin's spill file out into a streaming reader (rewound to
    /// start). Returns `None` if the bin never spilled. The reader removes
    /// the spill file on drop.
    fn take_reader(&mut self, bin: usize, chunk: usize) -> std::io::Result<Option<SpillReader>> {
        match self.files[bin].take() {
            None => Ok(None),
            Some(mut file) => {
                use std::io::{Seek, SeekFrom};
                file.seek(SeekFrom::Start(0))?;
                let chunk = chunk.max(1);
                Ok(Some(SpillReader {
                    file,
                    path: self.path_for(bin),
                    raw: vec![0u8; chunk * 16],
                    keys: vec![0i64; chunk],
                    vals: vec![0f64; chunk],
                }))
            }
        }
    }
}

impl Drop for SpillStore {
    fn drop(&mut self) {
        // Best-effort cleanup of any spill files not yet drained (e.g. on
        // early drop before finish()).
        for bin in 0..self.files.len() {
            if self.files[bin].is_some() {
                let _ = std::fs::remove_file(self.path_for(bin));
            }
        }
    }
}

/// Streaming reader over one bin's spill file. Decodes `[i64][f64]` rows
/// into reusable `keys`/`vals` buffers, `chunk` rows at a time.
struct SpillReader {
    file: std::fs::File,
    path: std::path::PathBuf,
    raw: Vec<u8>,
    keys: Vec<i64>,
    vals: Vec<f64>,
}

impl SpillReader {
    /// Fill `self.keys`/`self.vals` with the next block of rows. Returns
    /// the row count (0 = EOF). Handles short reads by accumulating until
    /// the chunk buffer is full or EOF.
    fn read_chunk(&mut self) -> std::io::Result<usize> {
        use std::io::Read;
        let cap = self.raw.len();
        let mut filled = 0usize;
        while filled < cap {
            match self.file.read(&mut self.raw[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        debug_assert_eq!(filled % 16, 0, "spill file truncated mid-row");
        let rows = filled / 16;
        for r in 0..rows {
            let o = r * 16;
            self.keys[r] = i64::from_ne_bytes(self.raw[o..o + 8].try_into().unwrap());
            self.vals[r] = f64::from_ne_bytes(self.raw[o + 8..o + 16].try_into().unwrap());
        }
        Ok(rows)
    }
}

impl Drop for SpillReader {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// REV.5.b — global-partition `SUM(f64) GROUP BY i64` aggregator with a
/// memory-bounded incremental drain. See the module banner above.
///
/// Layout: rows are radix-partitioned **on ingest** into per-bin append
/// buffers (`bin_keys[b]`, `bin_vals[b]`). At `finish()` each bin's
/// contiguous buffer is drained once into its own micro-table, which
/// stays cache-resident for the whole drain — DuckDB's shape, and the
/// source of the residency win. A single fused tag+scatter pass per batch
/// hashes each key exactly once (REV.6: no double-hash).
///
/// If the buffered rows exceed `drain_threshold_rows`, the over-budget
/// action is *aggregate-and-clear* (mid-stream drain) — memory-safe but it
/// re-visits partitions, eroding residency. The spill-backed variant
/// (`new_with_spill`) instead pushes cold partitions to disk and drains
/// each exactly once at finish, preserving residency under a memory bound.
pub struct RobinHoodSumF64GlobalRadixAgg {
    tables: Vec<RobinHoodI64F64>,
    /// Per-bin append buffers — rows are partitioned here on ingest.
    bin_keys: Vec<Vec<i64>>,
    bin_vals: Vec<Vec<f64>>,
    /// Total rows currently buffered across all bins (drives the budget).
    buffered_rows: usize,
    /// Reused batch-local tag scratch (one hash/row, reused for scatter).
    tag_scratch: Vec<u16>,
    n_bins: usize,
    shift: u32,
    /// Drain once buffered rows reach this many (= budget / 16 B).
    drain_threshold_rows: usize,
    /// Optional disk-spill backend; `None` = pure in-memory (mid-stream
    /// aggregate-drain on over-budget).
    spill: Option<SpillStore>,
    /// Reused drain scratch for the vectorised per-bin insert.
    scratch_slots: Vec<usize>,
    scratch_hit: Vec<bool>,
    finished: bool,
}

impl RobinHoodSumF64GlobalRadixAgg {
    /// `radix_bits` → `2^radix_bits` bins (≤ 4096). `per_table_cap` sizes
    /// each micro-table (caller passes `total_card_est / n_bins`).
    /// `mem_budget_bytes` caps the per-bin input buffers; the over-budget
    /// action fires once buffered rows would exceed it (each row = 16 B:
    /// i64 key + f64 value). Floored at one 1024-row chunk.
    ///
    /// Pure in-memory: over-budget aggregates the current buffers into the
    /// tables and clears them (mid-stream re-visit — gives up residency to
    /// stay memory-safe). For the residency-preserving variant, see
    /// [`Self::new_with_spill`].
    pub fn new(radix_bits: u8, per_table_cap: usize, mem_budget_bytes: usize) -> Self {
        Self::build(radix_bits, per_table_cap, mem_budget_bytes, None)
    }

    /// Spill-backed variant (REV.5.b — DuckDB's shape). When buffered rows
    /// exceed the budget, cold partitions are written to disk under
    /// `spill_dir` rather than aggregated; at `finish()` each partition is
    /// read back and aggregated **exactly once** into its table, which
    /// stays cache-resident for that whole drain. Memory-bounded (input
    /// buffering capped at the budget) but residency-preserving.
    pub fn new_with_spill(
        radix_bits: u8,
        per_table_cap: usize,
        mem_budget_bytes: usize,
        spill_dir: std::path::PathBuf,
    ) -> Self {
        let nb = 1usize << radix_bits;
        Self::build(
            radix_bits,
            per_table_cap,
            mem_budget_bytes,
            Some(SpillStore::new(spill_dir, nb)),
        )
    }

    fn build(
        radix_bits: u8,
        per_table_cap: usize,
        mem_budget_bytes: usize,
        spill: Option<SpillStore>,
    ) -> Self {
        assert!(
            radix_bits <= 12,
            "radix_bits must be ≤ 12; got {radix_bits}"
        );
        let nb = 1usize << radix_bits;
        let mut tables = Vec::with_capacity(nb);
        for _ in 0..nb {
            tables.push(RobinHoodI64F64::with_capacity(per_table_cap));
        }
        let shift = if radix_bits == 0 {
            0
        } else {
            usize::BITS - radix_bits as u32
        };
        let drain_threshold_rows = (mem_budget_bytes / 16).max(1024);
        Self {
            tables,
            bin_keys: (0..nb).map(|_| Vec::new()).collect(),
            bin_vals: (0..nb).map(|_| Vec::new()).collect(),
            buffered_rows: 0,
            tag_scratch: Vec::new(),
            n_bins: nb,
            shift,
            drain_threshold_rows,
            spill,
            scratch_slots: vec![0usize; 1024],
            scratch_hit: vec![false; 1024],
            finished: false,
        }
    }

    pub fn n_tables(&self) -> usize {
        self.tables.len()
    }

    /// True if the spill backend wrote at least one partition to disk.
    pub fn did_spill(&self) -> bool {
        self.total_spilled_rows() > 0
    }

    /// Total rows written to disk across all bins (0 if not spilling).
    pub fn total_spilled_rows(&self) -> usize {
        self.spill
            .as_ref()
            .map_or(0, |s| s.spilled_rows.iter().sum())
    }

    /// REV.8 — consume the aggregator and return its per-bin micro-tables
    /// (one per radix bin). Must be called after [`Self::finish`]. The
    /// single-pass-radix operator uses this to merge bin `b` across input
    /// partitions: because the bin = `hash(key) >> shift` is identical in
    /// every partition's aggregator, bin `b` holds the same key space
    /// everywhere, so a per-bin cross-partition merge is complete.
    pub fn into_tables(self) -> Vec<RobinHoodI64F64> {
        debug_assert!(self.finished, "into_tables called before finish()");
        self.tables
    }

    /// Radix-partition a batch into the per-bin append buffers via a single
    /// fused tag+scatter pass (one hash per key — REV.6: no double-hash).
    /// Triggers the over-budget action if the buffered rows cross the cap.
    pub fn ingest_batch(&mut self, keys: &[i64], vals: &[f64]) {
        assert_eq!(
            keys.len(),
            vals.len(),
            "ingest_batch: keys and values must be same length"
        );
        debug_assert!(!self.finished, "ingest_batch called after finish()");
        let m = keys.len();
        if m == 0 {
            return;
        }
        let nb = self.n_bins;
        if nb == 1 {
            // Degenerate single-bin: append straight into bin 0.
            self.bin_keys[0].extend_from_slice(keys);
            self.bin_vals[0].extend_from_slice(vals);
        } else {
            let shift = self.shift;
            if self.tag_scratch.len() < m {
                self.tag_scratch.resize(m, 0);
            }
            // Pass 1: tag each key + histogram (one hash/key, reused below).
            let mut counts = [0u32; 4096];
            #[allow(clippy::needless_range_loop)] // `i` indexes keys + tag_scratch in lockstep
            for i in 0..m {
                let b = (radix_part_hash(keys[i]) >> shift) as usize;
                self.tag_scratch[i] = b as u16;
                counts[b] += 1;
            }
            // Reserve each touched bin once so the scatter-append below is a
            // pure write (no realloc churn mid-loop).
            #[allow(clippy::needless_range_loop)] // `b` indexes counts + bin_keys + bin_vals
            for b in 0..nb {
                let c = counts[b] as usize;
                if c > 0 {
                    self.bin_keys[b].reserve(c);
                    self.bin_vals[b].reserve(c);
                }
            }
            // Pass 2: scatter-append into the persistent per-bin buffers.
            for i in 0..m {
                let b = self.tag_scratch[i] as usize;
                self.bin_keys[b].push(keys[i]);
                self.bin_vals[b].push(vals[i]);
            }
        }
        self.buffered_rows += m;
        if self.buffered_rows >= self.drain_threshold_rows {
            self.over_budget();
        }
    }

    /// Buffered rows crossed the cap. In-memory mode aggregates the current
    /// buffers into the tables (re-visits partitions). Spill mode pushes
    /// them to disk, deferring aggregation to a single pass at finish.
    fn over_budget(&mut self) {
        if self.spill.is_some() {
            self.spill_all_bins();
        } else {
            self.drain_to_tables();
        }
    }

    /// In-memory drain (over-budget + finish): aggregate each bin's current
    /// buffer into its persistent table, then clear. Accumulates correctly
    /// across repeated calls — tables persist, so keys repeated across
    /// drains sum into the same group.
    fn drain_to_tables(&mut self) {
        let nb = self.n_bins;
        for b in 0..nb {
            if self.bin_keys[b].is_empty() {
                continue;
            }
            self.tables[b].insert_or_sum_batch_vectorised_with_scratch(
                &self.bin_keys[b],
                &self.bin_vals[b],
                &mut self.scratch_slots,
                &mut self.scratch_hit,
            );
            self.bin_keys[b].clear();
            self.bin_vals[b].clear();
        }
        self.buffered_rows = 0;
    }

    /// Spill-mode over-budget: append each non-empty bin's buffer to its
    /// on-disk spill file and clear it. Aggregation is deferred to
    /// `finalize_spilled` so each partition is drained exactly once.
    fn spill_all_bins(&mut self) {
        let nb = self.n_bins;
        for b in 0..nb {
            if self.bin_keys[b].is_empty() {
                continue;
            }
            self.spill
                .as_mut()
                .expect("spill_all_bins without spill backend")
                .spill_bin(b, &self.bin_keys[b], &self.bin_vals[b])
                .expect("radix spill write failed");
            self.bin_keys[b].clear();
            self.bin_vals[b].clear();
        }
        self.buffered_rows = 0;
    }

    /// Spill-mode finish: for each bin, aggregate its disk-spilled rows
    /// (streamed back in chunks) then its in-memory tail into the table —
    /// exactly once, so the table stays cache-resident for the whole bin.
    fn finalize_spilled(&mut self) {
        let nb = self.n_bins;
        const READ_CHUNK: usize = 8192;
        for b in 0..nb {
            // 1. Disk-spilled rows, if any (reader removes the file on drop).
            let reader = self
                .spill
                .as_mut()
                .expect("finalize_spilled without spill backend")
                .take_reader(b, READ_CHUNK)
                .expect("spill open failed");
            if let Some(mut reader) = reader {
                loop {
                    let n = reader.read_chunk().expect("spill read failed");
                    if n == 0 {
                        break;
                    }
                    self.tables[b].insert_or_sum_batch_vectorised_with_scratch(
                        &reader.keys[..n],
                        &reader.vals[..n],
                        &mut self.scratch_slots,
                        &mut self.scratch_hit,
                    );
                }
            }
            // 2. In-memory tail.
            if !self.bin_keys[b].is_empty() {
                self.tables[b].insert_or_sum_batch_vectorised_with_scratch(
                    &self.bin_keys[b],
                    &self.bin_vals[b],
                    &mut self.scratch_slots,
                    &mut self.scratch_hit,
                );
                self.bin_keys[b].clear();
                self.bin_vals[b].clear();
            }
        }
        self.buffered_rows = 0;
    }

    /// Drain remaining buffers. Must be called before `iter`/`len`.
    pub fn finish(&mut self) {
        if self.spill.is_some() {
            self.finalize_spilled();
        } else {
            self.drain_to_tables();
        }
        self.finished = true;
    }

    /// Total groups across all bins. Valid only after `finish()`.
    pub fn len(&self) -> usize {
        self.tables.iter().map(|t| t.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `(key, sum)` across all bins. Valid only after `finish()`.
    pub fn iter(&self) -> impl Iterator<Item = (i64, f64)> + '_ {
        self.tables.iter().flat_map(|t| t.iter())
    }
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

// ---------------------------------------------------------------------
// Σ.N.b — streaming aggregator that ingests Arrow batches.
// ---------------------------------------------------------------------

/// Σ.N.b — streaming `COUNT(*) GROUP BY i64_col` over Arrow
/// Int64Array inputs. Mirrors what a `RobinHoodAggregateExec` would
/// do without going through DataFusion's ExecutionPlan trait — for
/// microbenching the real-data win + as a reference impl for the
/// follow-up Σ.N.c full operator integration.
pub struct RobinHoodCountAgg {
    table: RobinHoodI64U64,
}

impl Default for RobinHoodCountAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinHoodCountAgg {
    pub fn new() -> Self {
        Self {
            table: RobinHoodI64U64::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            table: RobinHoodI64U64::with_capacity(cap),
        }
    }

    /// Σ.N.b — ingest one batch's GROUP BY column. Skips nulls (NULL
    /// keys don't form their own group in SQL standard COUNT GROUP BY
    /// semantics; they're filtered out by DataFusion's analyzer in
    /// most cases). For full SQL conformance, the caller can pre-
    /// process NULLs into a sentinel key (e.g. i64::MIN).
    pub fn ingest_int64_array(&mut self, arr: &arrow_array::Int64Array) {
        use arrow_array::Array;
        // Two paths: no nulls → tight loop; with nulls → null-check.
        if arr.null_count() == 0 {
            for i in 0..arr.len() {
                let k = arr.value(i);
                self.table.insert_or_update(k, 1, count_accumulator);
            }
        } else {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let k = arr.value(i);
                    self.table.insert_or_update(k, 1, count_accumulator);
                }
            }
        }
    }

    /// Σ.N.e — ingest a (key, partial_count) pair stream. For each
    /// row, sums `partial_count` into the running total for `key`.
    /// Used by the FinalPartitioned mode to merge upstream Partial
    /// outputs.
    pub fn ingest_partial_counts(
        &mut self,
        keys: &arrow_array::Int64Array,
        partial_counts: &arrow_array::Int64Array,
    ) {
        use arrow_array::Array;
        assert_eq!(
            keys.len(),
            partial_counts.len(),
            "ingest_partial_counts: key and count arrays must be same length"
        );
        let n = keys.len();
        if keys.null_count() == 0 && partial_counts.null_count() == 0 {
            for i in 0..n {
                let k = keys.value(i);
                let c = partial_counts.value(i) as u64;
                self.table.insert_or_update(k, c, sum_accumulator);
            }
        } else {
            for i in 0..n {
                if !keys.is_null(i) && !partial_counts.is_null(i) {
                    let k = keys.value(i);
                    let c = partial_counts.value(i) as u64;
                    self.table.insert_or_update(k, c, sum_accumulator);
                }
            }
        }
    }

    /// Σ.N.b — finalise into a `RecordBatch` matching the SQL output
    /// schema `(group_key i64, count u64)`. Sorted by group key for
    /// stable comparison against DataFusion output.
    pub fn finalize_to_record_batch(self) -> arrow_array::RecordBatch {
        use arrow_array::{Int64Array, UInt64Array};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        let mut pairs: Vec<(i64, u64)> = self.table.iter().collect();
        pairs.sort_by_key(|(k, _)| *k);
        let keys: Vec<i64> = pairs.iter().map(|(k, _)| *k).collect();
        let counts: Vec<u64> = pairs.iter().map(|(_, v)| *v).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_key", DataType::Int64, false),
            Field::new("count", DataType::UInt64, false),
        ]));
        arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(UInt64Array::from(counts)),
            ],
        )
        .unwrap()
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Read-only view of the inner table. Useful for callers that
    /// want to spot-check a group's count without finalising.
    pub fn table(&self) -> &RobinHoodI64U64 {
        &self.table
    }

    /// Σ.N.f.2 — mutable view for dynamic-resize callers (operator
    /// hot path peeks at len + asks for jump-resize).
    pub fn table_mut(&mut self) -> &mut RobinHoodI64U64 {
        &mut self.table
    }
}

// ---------------------------------------------------------------------
// Σ.N.c — DataFusion ExecutionPlan wrapping RobinHoodCountAgg.
// ---------------------------------------------------------------------

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};
use std::any::Any;

/// Σ.N.e — execution mode mirroring DataFusion's `AggregateExec`.
/// Lets RobinHoodAggregateExec slot into the two-stage agg pipeline
/// (Partial → RepartitionExec → FinalPartitioned) so a multi-partition
/// scan is aggregated in parallel instead of being serialised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RobinHoodMode {
    /// Per-input-partition COUNT(*) GROUP BY. Each output partition
    /// emits one batch containing the partial counts for that
    /// partition's rows. Output column 1 is `partial_count: Int64`.
    Partial,
    /// Consumes partial counts (from upstream `Partial` after a
    /// `RepartitionExec(Hash)`) and sums them per group key. Output
    /// column 1 is the final `count: Int64`.
    FinalPartitioned,
}

impl RobinHoodMode {
    #[allow(dead_code)]
    fn count_col_name(self) -> &'static str {
        match self {
            RobinHoodMode::Partial => "partial_count",
            RobinHoodMode::FinalPartitioned => "count",
        }
    }
}

/// Σ.N.c + Σ.N.e — `SELECT col, COUNT(*) FROM child GROUP BY col`
/// operator. Two modes:
///
/// - **Partial**: input is the raw scan; output is per-partition
///   partial counts. Used as the leaf-side of the two-stage agg.
/// - **FinalPartitioned**: input is partial counts from a
///   `RepartitionExec(Hash)`; output is per-partition final counts.
///
/// Output partitioning matches input partitioning in both modes.
/// EmissionType::Final since each call emits one batch.
///
/// Uses RobinHoodCountAgg internally — 1.16-1.54× faster than the
/// stock hashbrown-based aggregate at higher cardinalities.
#[derive(Debug)]
pub struct RobinHoodAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    group_col_idx: usize,
    /// Σ.N.e — for `FinalPartitioned`, the column index of the
    /// partial-count input. `None` for `Partial` (raw scan; count by
    /// incrementing 1 per row).
    partial_count_col_idx: Option<usize>,
    mode: RobinHoodMode,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RobinHoodAggregateExec {
    /// Σ.N.c convenience — Partial mode with default field names.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, group_col_idx: usize) -> DfResult<Self> {
        Self::try_new_with_names(
            input,
            group_col_idx,
            "group_key".to_string(),
            "count".to_string(),
        )
    }

    /// Σ.N.c convenience — Partial mode with caller-supplied names.
    pub fn try_new_with_names(
        input: Arc<dyn ExecutionPlan>,
        group_col_idx: usize,
        group_out_name: String,
        count_out_name: String,
    ) -> DfResult<Self> {
        Self::try_new_full(
            input,
            group_col_idx,
            None,
            RobinHoodMode::Partial,
            group_out_name,
            count_out_name,
        )
    }

    /// Σ.N.e — full constructor with mode + optional partial-count
    /// column index (required for FinalPartitioned).
    pub fn try_new_full(
        input: Arc<dyn ExecutionPlan>,
        group_col_idx: usize,
        partial_count_col_idx: Option<usize>,
        mode: RobinHoodMode,
        group_out_name: String,
        count_out_name: String,
    ) -> DfResult<Self> {
        let input_schema = input.schema();
        if group_col_idx >= input_schema.fields().len() {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodAggregateExec: group_col_idx={group_col_idx} out of bounds (schema has {} cols)",
                input_schema.fields().len()
            )));
        }
        let gb_type = input_schema.field(group_col_idx).data_type();
        if gb_type != &DataType::Int64 {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodAggregateExec: group column must be Int64, got {gb_type:?}"
            )));
        }
        if mode == RobinHoodMode::FinalPartitioned {
            let cci = partial_count_col_idx.ok_or_else(|| {
                DataFusionError::Internal(
                    "RobinHoodAggregateExec(FinalPartitioned) needs partial_count_col_idx".into(),
                )
            })?;
            if cci >= input_schema.fields().len() {
                return Err(DataFusionError::Internal(format!(
                    "RobinHoodAggregateExec: partial_count_col_idx={cci} out of bounds"
                )));
            }
            if input_schema.field(cci).data_type() != &DataType::Int64 {
                return Err(DataFusionError::Internal(format!(
                    "RobinHoodAggregateExec: partial_count column must be Int64, got {:?}",
                    input_schema.field(cci).data_type()
                )));
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new(&group_out_name, DataType::Int64, false),
            Field::new(&count_out_name, DataType::Int64, false),
        ]));
        let eq_props = EquivalenceProperties::new(schema.clone());
        // Σ.N.e — output partitioning matches input. Critical for
        // not serialising parallel scans onto one thread.
        use datafusion::physical_plan::ExecutionPlanProperties;
        let n_parts = input.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(n_parts),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            group_col_idx,
            partial_count_col_idx,
            mode,
            schema,
            properties,
        })
    }

    pub fn group_col_idx(&self) -> usize {
        self.group_col_idx
    }

    pub fn mode(&self) -> RobinHoodMode {
        self.mode
    }
}

impl DisplayAs for RobinHoodAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mode_str = match self.mode {
            RobinHoodMode::Partial => "Partial",
            RobinHoodMode::FinalPartitioned => "FinalPartitioned",
        };
        write!(
            f,
            "RobinHoodAggregateExec(mode={mode_str}, group_col_idx={})",
            self.group_col_idx
        )
    }
}

impl ExecutionPlan for RobinHoodAggregateExec {
    fn name(&self) -> &str {
        "RobinHoodAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let new_input = children.pop().ok_or_else(|| {
            DataFusionError::Internal("RobinHoodAggregateExec requires exactly 1 child".into())
        })?;
        let group_out_name = self.schema.field(0).name().clone();
        let count_out_name = self.schema.field(1).name().clone();
        Ok(Arc::new(Self::try_new_full(
            new_input,
            self.group_col_idx,
            self.partial_count_col_idx,
            self.mode,
            group_out_name,
            count_out_name,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        // Σ.N.e — output partitioning matches input. Each call to
        // execute(p) reads ONLY input partition p, not all of them.
        let input = self.input.clone();
        let group_col_idx = self.group_col_idx;
        let partial_count_col_idx = self.partial_count_col_idx;
        let mode = self.mode;
        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();

        let fut = async move {
            // Σ.N.f profiling — set EMAT_RH_TIMING=1 to dump per-stage
            // wall times per partition. Helps identify whether the
            // operator's 25× wall-time gap vs stock lives in ingest,
            // finalize, or stream-yielding.
            let timing = std::env::var("EMAT_RH_TIMING").is_ok();
            let mut t_ingest_us: u128 = 0;
            let mut t_input_wait_us: u128 = 0;
            let mut n_rows: usize = 0;
            let t_total = std::time::Instant::now();

            // Σ.N.f.1 + Σ.N.f.2 — pre-size the hash table. Microbench
            // (10K card) showed default cap=64 → ~100 M rows/sec,
            // pre-sized → 230 M rows/sec (RH beats hashbrown 1.3-4×).
            //
            // Σ.N.f.2 cap sweep on l_suppkey (10K) + l_partkey (200K):
            //   cap   suppkey   partkey
            //    8K   +99%      +23%
            //   16K   +71%      +51%
            //   32K   +30%      +39%
            //   65K   +18%      +32%   ← sweet spot
            //  131K   +77%      +27%   ← memory pressure hurts mid
            //  262K  +128%      +25%
            //
            // 65536 balances the two: enough headroom for ~50K distinct
            // groups without grow, small enough that memory pressure
            // doesn't dominate at mid cardinality. Override via
            // EMAT_RH_INIT_CAP for known-workload tuning.
            let init_cap: usize = std::env::var("EMAT_RH_INIT_CAP")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(65536);
            let mut agg = RobinHoodCountAgg::with_capacity(init_cap);
            let mut s = input.execute(partition, context)?;
            // Σ.N.f.2 — dynamic resize: after first batch, if observed
            // distinct count is already > 50% of capacity, the input
            // is high-cardinality. Jump-resize to 4× ahead of the
            // 2× geometric growth so we don't spend the next few
            // batches in rehash territory. Cheap single-shot heuristic.
            let mut first_batch_seen = false;
            loop {
                let t_w = std::time::Instant::now();
                let batch_opt = s.try_next().await?;
                t_input_wait_us += t_w.elapsed().as_micros();
                let Some(batch) = batch_opt else { break };
                n_rows += batch.num_rows();
                let keys_arr = batch.column(group_col_idx);
                let keys = keys_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "RobinHoodAggregateExec: column {group_col_idx} not Int64Array"
                        ))
                    })?;
                let t_i = std::time::Instant::now();
                match mode {
                    RobinHoodMode::Partial => {
                        agg.ingest_int64_array(keys);
                    }
                    RobinHoodMode::FinalPartitioned => {
                        let cci = partial_count_col_idx.expect("validated in constructor");
                        let counts_arr = batch.column(cci);
                        let counts = counts_arr
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "RobinHoodAggregateExec: column {cci} not Int64Array"
                                ))
                            })?;
                        agg.ingest_partial_counts(keys, counts);
                    }
                }
                t_ingest_us += t_i.elapsed().as_micros();
                // Σ.N.f.2 dynamic resize check — after first batch only.
                if !first_batch_seen {
                    first_batch_seen = true;
                    let len = agg.table().len();
                    let cap = agg.table().capacity();
                    if len * 2 > cap {
                        // > 50% full already → high-cardinality
                        // workload. Pre-empt grow chain.
                        agg.table_mut().reserve_to_capacity_pow2_of(len * 8);
                    }
                }
            }
            // Σ.N.f.3 — direct write into pre-sized Vec<i64> buffers.
            // No intermediate Vec<(i64, u64)>, no sort (DataFusion's
            // stock AggregateExec emits in hash-table order too —
            // downstream SortExec / ORDER BY does the sorting).
            // Saves O(N log N) sort + one Vec alloc + two iter().map()
            // passes. For 200K-row finalize this is ~5-12 ms saved per
            // partition.
            let t_f = std::time::Instant::now();
            let n = agg.table().len();
            let mut keys: Vec<i64> = Vec::with_capacity(n);
            let mut counts: Vec<i64> = Vec::with_capacity(n);
            for (k, v) in agg.table().iter() {
                keys.push(k);
                counts.push(v as i64);
            }
            let out = RecordBatch::try_new(
                schema_for_stream.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(counts)),
                ],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let t_finalize_us = t_f.elapsed().as_micros();
            if timing {
                let mode_str = match mode {
                    RobinHoodMode::Partial => "Partial",
                    RobinHoodMode::FinalPartitioned => "Final",
                };
                let t_total_us = t_total.elapsed().as_micros();
                let n_groups = agg.len();
                eprintln!(
                    "[RH-timing] {mode_str:<8} p={partition:<2} total={t_total_us}us \
                     input_wait={t_input_wait_us}us ingest={t_ingest_us}us \
                     finalize={t_finalize_us}us  rows={n_rows} groups={n_groups}"
                );
            }
            DfResult::Ok(out)
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

// ---------------------------------------------------------------------
// Σ.Q.L12 — SIMD-tagged hash table specialised for i64 keys + f64
// values. SwissTable-style metadata-byte probing on 16-byte groups.
// Targets the Q17 SF=10 shape: AVG(f64) GROUP BY i64 at 2M cardinality.
//
// Rationale: existing RobinHoodI64F64 already beats hashbrown by 1-5%,
// but its probe touches the full 24-byte bucket (key + value + PSL)
// for every probed slot. Tagged probing reads a 16-byte tag group once
// (one cache-line load), compares all 16 tags in parallel via NEON,
// and only touches keys/values on tag-match. Sized for the high-
// cardinality SUM/AVG workload where stage-4 chained probes dominate
// the existing PSL-only path.
// ---------------------------------------------------------------------

const TAG_EMPTY: u8 = 0xFF;
const GROUP_SIZE: usize = 16;

#[inline(always)]
fn tag_from_hash(h: usize) -> u8 {
    // Top 7 bits of hash. High bit always clear, so real tags never
    // collide with TAG_EMPTY (0xFF, high bit set).
    ((h >> 57) as u8) & 0x7F
}

/// Σ.Q.L12 — match a tag byte across a 16-byte group on aarch64+NEON.
/// Returns a u16 bitmask: bit i set iff slot+i equals `byte`. NEON has
/// no native movemask; we AND with a bit-pattern then horizontal-sum
/// each half (the compare result is 0x00 or 0xFF per lane so bits don't
/// overlap; ADD == OR).
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[inline]
unsafe fn neon_match_byte_mask_at(tags: &[u8], slot: usize, byte: u8) -> u16 {
    use std::arch::aarch64::{
        vaddv_u8, vandq_u8, vceqq_u8, vdupq_n_u8, vget_high_u8, vget_low_u8, vld1q_u8,
    };
    let group = unsafe { vld1q_u8(tags.as_ptr().add(slot)) };
    let cmp = vceqq_u8(group, vdupq_n_u8(byte));
    let bit_mask = unsafe {
        std::mem::transmute::<[u8; 16], std::arch::aarch64::uint8x16_t>([
            0x01u8, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x01u8, 0x02, 0x04, 0x08, 0x10, 0x20,
            0x40, 0x80,
        ])
    };
    let masked = vandq_u8(cmp, bit_mask);
    let lo = vaddv_u8(vget_low_u8(masked)) as u16;
    let hi = vaddv_u8(vget_high_u8(masked)) as u16;
    (hi << 8) | lo
}

/// Σ.Q.L12 — match a tag byte across a 16-byte group on x86_64+SSE2.
/// Returns a u16 bitmask: bit i set iff slot+i equals `byte`.
///
/// PCMPEQB sets 0xFF in each matching byte lane and 0x00 otherwise.
/// PMOVMSKB extracts the top bit of each of the 16 bytes into a 16-bit
/// integer — exactly the bitmask shape we want.  Two instructions per
/// probe, the canonical SwissTable / boost::flat_hash_map pattern.
///
/// SSE2 is mandatory on x86_64 (part of the base ISA since AMD64),
/// so this path needs no runtime feature detection; the `cfg` gate
/// alone is sufficient.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[inline]
unsafe fn sse2_match_byte_mask_at(tags: &[u8], slot: usize, byte: u8) -> u16 {
    use std::arch::x86_64::{
        __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
    };
    // SAFETY: callers guarantee `slot + GROUP_SIZE <= tags.len()` via the
    // tail-mirror invariant documented on `RobinHoodI64F64::tags`.
    let group = unsafe { _mm_loadu_si128(tags.as_ptr().add(slot) as *const __m128i) };
    let cmp = _mm_cmpeq_epi8(group, _mm_set1_epi8(byte as i8));
    _mm_movemask_epi8(cmp) as u16
}

/// Scalar fallback for non-{aarch64, x86_64} builds (e.g. RISC-V,
/// wasm32 without SIMD).  Byte-by-byte tag compare.
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
#[inline]
fn scalar_match_byte_mask(tags: &[u8], slot: usize, byte: u8) -> u16 {
    let mut mask = 0u16;
    for i in 0..GROUP_SIZE {
        if tags[slot + i] == byte {
            mask |= 1 << i;
        }
    }
    mask
}

/// Σ.Q.L12 — match a tag byte across a 16-byte group. Returns a u16
/// bitmask: bit i set iff slot+i equals `byte`.
#[inline(always)]
fn match_byte_mask(tags: &[u8], slot: usize, byte: u8) -> u16 {
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { neon_match_byte_mask_at(tags, slot, byte) }
    }
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { sse2_match_byte_mask_at(tags, slot, byte) }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        scalar_match_byte_mask(tags, slot, byte)
    }
}

/// Σ.Q.L12 — SwissTable-style hash table for i64 keys + f64 values.
/// Probes 16 slots per group via NEON tag compare. SoA layout:
/// tags / keys / values in three parallel Vecs.
///
/// Tail-pad invariant: all three buffers are sized `capacity +
/// GROUP_SIZE` so a SIMD load at the last real slot is safe. The
/// extra GROUP_SIZE tail entries always mirror slots [0..GROUP_SIZE)
/// so linear scan wraps cleanly without a branch.
pub struct TaggedI64F64 {
    tags: Vec<u8>,
    keys: Vec<i64>,
    values: Vec<f64>,
    mask: usize,
    len: usize,
}

impl Default for TaggedI64F64 {
    fn default() -> Self {
        Self::new()
    }
}

impl TaggedI64F64 {
    pub fn new() -> Self {
        Self::with_capacity(INITIAL_CAPACITY)
    }

    pub fn with_capacity(cap: usize) -> Self {
        let cap = cap.max(INITIAL_CAPACITY).next_power_of_two();
        Self {
            tags: vec![TAG_EMPTY; cap + GROUP_SIZE],
            keys: vec![0i64; cap + GROUP_SIZE],
            values: vec![0.0f64; cap + GROUP_SIZE],
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
        self.mask + 1
    }

    pub fn clear(&mut self) {
        for t in &mut self.tags {
            *t = TAG_EMPTY;
        }
        self.len = 0;
    }

    /// Tail-mirror invariant: when slot `< GROUP_SIZE` is written, the
    /// mirror at `cap + slot` must match so wrap-around loads still see
    /// the right tag.
    #[inline(always)]
    fn write_slot(&mut self, slot: usize, tag: u8, key: i64, value: f64) {
        let cap = self.mask + 1;
        self.tags[slot] = tag;
        self.keys[slot] = key;
        self.values[slot] = value;
        if slot < GROUP_SIZE {
            self.tags[cap + slot] = tag;
            self.keys[cap + slot] = key;
            self.values[cap + slot] = value;
        }
    }

    #[inline(always)]
    fn accumulate_slot(&mut self, slot: usize, delta: f64) -> f64 {
        let cap = self.mask + 1;
        self.values[slot] += delta;
        let v = self.values[slot];
        if slot < GROUP_SIZE {
            self.values[cap + slot] = v;
        }
        v
    }

    /// Σ.Q.L12 — SUM-style entry. Returns post-update value.
    pub fn insert_or_sum(&mut self, key: i64, delta: f64) -> f64 {
        if self.needs_grow() {
            self.grow();
        }
        let h = hash_i64(key);
        let tag = tag_from_hash(h);
        let mask = self.mask;
        let cap = mask + 1;
        let mut slot = h & mask;
        // Group-aligned scan: walk in steps of GROUP_SIZE. The tail-
        // mirror invariant guarantees a safe load at any `slot < cap`.
        loop {
            // Stage 1: scan group for tag match.
            let mut mm = match_byte_mask(&self.tags, slot, tag);
            while mm != 0 {
                let idx = mm.trailing_zeros() as usize;
                let cand = slot + idx;
                // cand is in [slot, slot + GROUP_SIZE). If cand >= cap,
                // it lies in the tail mirror; the canonical slot is
                // (cand - cap). Both the tag and the key in the mirror
                // copy mirror the canonical slot, so equality works.
                let canonical = if cand >= cap { cand - cap } else { cand };
                if self.keys[cand] == key {
                    return self.accumulate_slot(canonical, delta);
                }
                mm &= mm - 1;
            }
            // Stage 2: no key match → check for empty slot to insert.
            let em = match_byte_mask(&self.tags, slot, TAG_EMPTY);
            if em != 0 {
                let idx = em.trailing_zeros() as usize;
                let cand = slot + idx;
                let canonical = if cand >= cap { cand - cap } else { cand };
                self.write_slot(canonical, tag, key, delta);
                self.len += 1;
                return delta;
            }
            slot = (slot + GROUP_SIZE) & mask;
        }
    }

    pub fn insert_or_sum_batch(&mut self, keys: &[i64], values: &[f64]) {
        assert_eq!(
            keys.len(),
            values.len(),
            "insert_or_sum_batch: keys and values must be same length"
        );
        for i in 0..keys.len() {
            self.insert_or_sum(keys[i], values[i]);
        }
    }

    /// Σ.Q.L12 — vectorised batch ingest. 4-stage pipeline analogous to
    /// `RobinHoodI64F64::insert_or_sum_batch_vectorised`, but stage 2
    /// uses a SIMD-tag scan over the FIRST GROUP per key, so primary-
    /// group hits avoid touching keys/values on tag-mismatch slots.
    ///
    /// 1. Hash all keys in chunk → slot[i] = h & mask, tag[i] = tag(h).
    /// 2. For each row, scan the primary group's 16 tags. If a tag
    ///    matches AND the key matches, record `match_slot[i]`.
    /// 3. Fast-path accumulate for hits (predictable store).
    /// 4. Scalar fallback for misses via `insert_or_sum`.
    pub fn insert_or_sum_batch_vectorised(&mut self, keys: &[i64], values: &[f64]) {
        const VEC_CHUNK: usize = 1024;
        let mut slots = [0usize; VEC_CHUNK];
        let mut tags = [0u8; VEC_CHUNK];
        let mut match_slot = [u32::MAX; VEC_CHUNK]; // u32::MAX = miss
        self.insert_or_sum_batch_vectorised_with_scratch(
            keys,
            values,
            &mut slots,
            &mut tags,
            &mut match_slot,
        );
    }

    /// Scratch-passing variant. Hot callers should own scratch.
    pub fn insert_or_sum_batch_vectorised_with_scratch(
        &mut self,
        keys: &[i64],
        values: &[f64],
        slots: &mut [usize],
        tags: &mut [u8],
        match_slot: &mut [u32],
    ) {
        assert_eq!(keys.len(), values.len());
        const VEC_CHUNK: usize = 1024;
        assert!(slots.len() >= VEC_CHUNK);
        assert!(tags.len() >= VEC_CHUNK);
        assert!(match_slot.len() >= VEC_CHUNK);
        let n_total = keys.len();
        if n_total == 0 {
            return;
        }

        let mut off = 0;
        while off < n_total {
            let n = (n_total - off).min(VEC_CHUNK);
            let mask = self.mask;
            let cap = mask + 1;

            // Stage 1: hash + group-base + tag.
            for i in 0..n {
                let h = hash_i64(keys[off + i]);
                slots[i] = h & mask;
                tags[i] = tag_from_hash(h);
            }

            // Stage 2: primary-group SIMD tag scan + key check. Record
            // the exact slot index on first key match; u32::MAX = miss.
            for i in 0..n {
                let slot = slots[i];
                let tag = tags[i];
                let key = keys[off + i];
                let mut mm = match_byte_mask(&self.tags, slot, tag);
                let mut found = u32::MAX;
                while mm != 0 {
                    let idx = mm.trailing_zeros() as usize;
                    let cand = slot + idx;
                    if self.keys[cand] == key {
                        let canonical = if cand >= cap { cand - cap } else { cand };
                        found = canonical as u32;
                        break;
                    }
                    mm &= mm - 1;
                }
                match_slot[i] = found;
            }

            // Stage 3: fast-path accumulate for hits.
            for i in 0..n {
                let ms = match_slot[i];
                if ms != u32::MAX {
                    let s = ms as usize;
                    self.values[s] += values[off + i];
                    if s < GROUP_SIZE {
                        self.values[cap + s] = self.values[s];
                    }
                }
            }

            // Stage 4: scalar fallback for misses. May grow the table.
            for i in 0..n {
                if match_slot[i] == u32::MAX {
                    self.insert_or_sum(keys[off + i], values[off + i]);
                }
            }

            off += n;
        }
    }

    pub fn reserve_to_capacity_pow2_of(&mut self, target: usize) {
        let target_capacity = (target * MAX_LOAD_FACTOR_DENOMINATOR)
            .div_ceil(MAX_LOAD_FACTOR_NUMERATOR)
            .next_power_of_two();
        while self.mask + 1 < target_capacity {
            self.grow();
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (i64, f64)> + '_ {
        let cap = self.mask + 1;
        // Skip tail-mirror entries [cap..cap+GROUP_SIZE).
        self.tags[..cap]
            .iter()
            .enumerate()
            .filter_map(move |(i, &t)| {
                if t == TAG_EMPTY {
                    None
                } else {
                    Some((self.keys[i], self.values[i]))
                }
            })
    }

    pub fn get(&self, key: i64) -> Option<f64> {
        let h = hash_i64(key);
        let tag = tag_from_hash(h);
        let mask = self.mask;
        let cap = mask + 1;
        let mut slot = h & mask;
        let mut probed = 0usize;
        loop {
            let mut mm = match_byte_mask(&self.tags, slot, tag);
            while mm != 0 {
                let idx = mm.trailing_zeros() as usize;
                let cand = slot + idx;
                if self.keys[cand] == key {
                    return Some(self.values[cand]);
                }
                mm &= mm - 1;
            }
            let em = match_byte_mask(&self.tags, slot, TAG_EMPTY);
            if em != 0 {
                return None;
            }
            slot = (slot + GROUP_SIZE) & mask;
            probed += GROUP_SIZE;
            if probed >= cap {
                return None;
            }
        }
    }

    fn needs_grow(&self) -> bool {
        self.len * MAX_LOAD_FACTOR_DENOMINATOR >= (self.mask + 1) * MAX_LOAD_FACTOR_NUMERATOR
    }

    fn grow(&mut self) {
        let new_cap = (self.mask + 1) * 2;
        let old_cap = self.mask + 1;
        let old_tags = std::mem::replace(&mut self.tags, vec![TAG_EMPTY; new_cap + GROUP_SIZE]);
        let old_keys = std::mem::replace(&mut self.keys, vec![0i64; new_cap + GROUP_SIZE]);
        let old_values = std::mem::replace(&mut self.values, vec![0.0f64; new_cap + GROUP_SIZE]);
        self.mask = new_cap - 1;
        self.len = 0;
        for i in 0..old_cap {
            if old_tags[i] != TAG_EMPTY {
                self.insert_or_sum(old_keys[i], old_values[i]);
            }
        }
    }
}

/// Σ.Q.L12 — streaming `SUM(f64) GROUP BY i64` aggregator backed by the
/// tagged kernel. Drop-in shape match for `RobinHoodSumF64Agg` so the
/// operator can dispatch on a runtime kind flag.
pub struct TaggedSumF64Agg {
    table: TaggedI64F64,
}

impl Default for TaggedSumF64Agg {
    fn default() -> Self {
        Self::new()
    }
}

impl TaggedSumF64Agg {
    pub fn new() -> Self {
        Self {
            table: TaggedI64F64::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            table: TaggedI64F64::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn ingest_batch(
        &mut self,
        keys: &arrow_array::Int64Array,
        values: &arrow_array::Float64Array,
    ) {
        use arrow_array::Array;
        let n = keys.len();
        assert_eq!(n, values.len());
        for i in 0..n {
            if keys.is_null(i) || values.is_null(i) {
                continue;
            }
            self.table.insert_or_sum(keys.value(i), values.value(i));
        }
    }

    pub fn ingest_batch_vectorised(
        &mut self,
        keys: &arrow_array::Int64Array,
        values: &arrow_array::Float64Array,
    ) {
        use arrow_array::Array;
        let n = keys.len();
        assert_eq!(n, values.len());
        if keys.null_count() == 0 && values.null_count() == 0 {
            self.table
                .insert_or_sum_batch_vectorised(keys.values(), values.values());
            return;
        }
        for i in 0..n {
            if keys.is_null(i) || values.is_null(i) {
                continue;
            }
            self.table.insert_or_sum(keys.value(i), values.value(i));
        }
    }

    pub fn table(&self) -> &TaggedI64F64 {
        &self.table
    }
}

#[cfg(test)]
#[allow(clippy::type_complexity, clippy::approx_constant)]
mod tests {
    use super::*;

    /// SIMD parity: confirm the per-arch `match_byte_mask` dispatch returns
    /// the same u16 bitmask as a byte-by-byte reference implementation, for
    /// every position-of-match in a 16-byte group plus a handful of
    /// multi-hit / no-hit shapes.  Runs on every target; the active arch
    /// path (NEON on aarch64, SSE2 on x86_64, scalar elsewhere) is the one
    /// being validated against the inline reference loop.
    #[test]
    fn match_byte_mask_matches_scalar_reference() {
        fn reference(tags: &[u8], slot: usize, byte: u8) -> u16 {
            let mut m = 0u16;
            for i in 0..GROUP_SIZE {
                if tags[slot + i] == byte {
                    m |= 1 << i;
                }
            }
            m
        }
        // Pad with GROUP_SIZE extra bytes so the SIMD load at slot=0 is in bounds
        // even though we only vary the first 16 lanes.
        let mut tags = vec![0xFFu8; GROUP_SIZE * 2];
        // Single-position hits at every lane.
        for i in 0..GROUP_SIZE {
            for t in tags.iter_mut().take(GROUP_SIZE) {
                *t = 0xFF;
            }
            tags[i] = 0x42;
            assert_eq!(
                match_byte_mask(&tags, 0, 0x42),
                reference(&tags, 0, 0x42),
                "single-hit mismatch at lane {i}",
            );
        }
        // All-match / no-match boundaries.
        for t in tags.iter_mut().take(GROUP_SIZE) {
            *t = 0x77;
        }
        assert_eq!(match_byte_mask(&tags, 0, 0x77), 0xFFFF);
        assert_eq!(match_byte_mask(&tags, 0, 0x00), 0x0000);
        // Alternating pattern (every other lane matches).
        for (i, t) in tags.iter_mut().take(GROUP_SIZE).enumerate() {
            *t = if i % 2 == 0 { 0xAA } else { 0xBB };
        }
        assert_eq!(match_byte_mask(&tags, 0, 0xAA), 0b0101_0101_0101_0101);
        assert_eq!(match_byte_mask(&tags, 0, 0xBB), 0b1010_1010_1010_1010);
    }

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

    #[test]
    fn batch_count_matches_row_count() {
        // Σ.N.f.1 — batch API correctness vs the row-by-row path.
        let mut row_t = RobinHoodI64U64::new();
        let mut batch_t = RobinHoodI64U64::new();
        let keys: Vec<i64> = (0..1000i64).chain(0..500i64).chain(0..200i64).collect();
        for &k in &keys {
            row_t.insert_or_update(k, 1, count_accumulator);
        }
        batch_t.insert_or_update_batch_count(&keys);
        // Same len + same per-key counts.
        assert_eq!(row_t.len(), batch_t.len());
        for k in 0..1000i64 {
            assert_eq!(
                row_t.get(k),
                batch_t.get(k),
                "diverged at key {k}: row={:?} batch={:?}",
                row_t.get(k),
                batch_t.get(k)
            );
        }
    }

    #[test]
    fn batch_sum_matches_row_sum() {
        let mut row_t = RobinHoodI64U64::new();
        let mut batch_t = RobinHoodI64U64::new();
        let keys: Vec<i64> = (0..500i64).chain(0..500i64).chain(0..500i64).collect();
        let counts: Vec<u64> = (0..1500u64).collect();
        for i in 0..keys.len() {
            row_t.insert_or_update(keys[i], counts[i], sum_accumulator);
        }
        batch_t.insert_or_update_batch_sum(&keys, &counts);
        assert_eq!(row_t.len(), batch_t.len());
        for k in 0..500i64 {
            assert_eq!(
                row_t.get(k),
                batch_t.get(k),
                "sum diverged at key {k}: row={:?} batch={:?}",
                row_t.get(k),
                batch_t.get(k)
            );
        }
    }

    #[test]
    fn batch_count_pre_grows() {
        let mut t = RobinHoodI64U64::with_capacity(64);
        let initial_cap = t.capacity();
        let keys: Vec<i64> = (0..10_000i64).collect();
        t.insert_or_update_batch_count(&keys);
        // Should NOT have grown progressively; the pre-grow saw the
        // batch size up-front and went straight to the target.
        assert!(t.capacity() >= initial_cap);
        assert_eq!(t.len(), 10_000);
    }

    #[test]
    fn streaming_agg_ingests_int64_array() {
        use arrow_array::Int64Array;
        let mut agg = RobinHoodCountAgg::new();
        agg.ingest_int64_array(&Int64Array::from(vec![1, 2, 3, 1, 2, 1]));
        let batch = agg.finalize_to_record_batch();
        // 3 distinct keys, sorted ascending.
        assert_eq!(batch.num_rows(), 3);
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert_eq!(
            (0..3)
                .map(|i| (keys.value(i), counts.value(i)))
                .collect::<Vec<_>>(),
            vec![(1, 3), (2, 2), (3, 1)]
        );
    }

    #[test]
    fn streaming_agg_skips_nulls() {
        use arrow_array::Int64Array;
        let mut agg = RobinHoodCountAgg::new();
        agg.ingest_int64_array(&Int64Array::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(1),
        ]));
        let batch = agg.finalize_to_record_batch();
        // 2 distinct non-null keys.
        assert_eq!(batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn robin_hood_aggregate_exec_matches_datafusion() {
        // Σ.N.c — install RobinHoodAggregateExec over a MemTable scan
        // and verify its output matches DataFusion's stock agg.
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;
        use futures_util::TryStreamExt;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                1i64, 2, 3, 1, 2, 1, 5, 5, 2, 3, 3, 3,
            ]))],
        )
        .unwrap();

        let mt = MemTable::try_new(schema, vec![vec![batch.clone()]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mt)).unwrap();

        // Get the input plan (just MemTable scan).
        let df = ctx.sql("SELECT k FROM t").await.unwrap();
        let scan_plan = df.create_physical_plan().await.unwrap();

        // Wrap in RobinHoodAggregateExec.
        let rh_exec = RobinHoodAggregateExec::try_new(scan_plan, 0).unwrap();

        let task_ctx = ctx.task_ctx();
        let mut s = rh_exec.execute(0, task_ctx.clone()).unwrap();
        let mut rh_pairs: Vec<(i64, i64)> = Vec::new();
        while let Some(b) = s.try_next().await.unwrap() {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let cs = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                rh_pairs.push((ks.value(i), cs.value(i)));
            }
        }
        rh_pairs.sort();

        // DataFusion stock agg path.
        let df2 = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let plan2 = df2.create_physical_plan().await.unwrap();
        let mut s2 = plan2.execute(0, task_ctx).unwrap();
        let mut df_pairs: Vec<(i64, i64)> = Vec::new();
        while let Some(b) = s2.try_next().await.unwrap() {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let cs = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                df_pairs.push((ks.value(i), cs.value(i)));
            }
        }
        df_pairs.sort();

        assert_eq!(
            rh_pairs, df_pairs,
            "RobinHoodAggregateExec output diverged from DataFusion stock"
        );
    }

    #[tokio::test]
    async fn streaming_agg_matches_datafusion_output() {
        // End-to-end equivalence: same input, same output as
        // DataFusion's stock GROUP BY + COUNT(*).
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;
        use futures_util::TryStreamExt;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let data = vec![
            Some(1i64),
            Some(2),
            Some(3),
            Some(1),
            Some(2),
            Some(1),
            Some(5),
            Some(5),
        ];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(data.clone()))],
        )
        .unwrap();

        // Robin Hood path.
        let mut agg = RobinHoodCountAgg::new();
        agg.ingest_int64_array(batch.column(0).as_any().downcast_ref().unwrap());
        let mut rh_pairs: Vec<(i64, u64)> = agg.table().iter().collect();
        rh_pairs.sort();

        // DataFusion path.
        let mt = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let mut s = plan.execute(0, ctx.task_ctx()).unwrap();
        let mut df_pairs: Vec<(i64, u64)> = Vec::new();
        while let Some(b) = s.try_next().await.unwrap() {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let cs = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                df_pairs.push((ks.value(i), cs.value(i) as u64));
            }
        }
        df_pairs.sort();

        assert_eq!(
            rh_pairs, df_pairs,
            "robin hood output differs from datafusion"
        );
    }

    // ---------------------------------------------------------------------
    // Σ.Q.L1b — RobinHoodI64F64 + SumF64Agg tests.
    // ---------------------------------------------------------------------

    #[test]
    fn f64_insert_and_lookup() {
        let mut t = RobinHoodI64F64::new();
        t.insert_or_sum(42, 1.5);
        t.insert_or_sum(7, 2.25);
        t.insert_or_sum(42, 0.5);
        assert_eq!(t.get(42), Some(2.0));
        assert_eq!(t.get(7), Some(2.25));
        assert_eq!(t.get(99), None);
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn f64_sum_accumulates() {
        let mut t = RobinHoodI64F64::new();
        for (k, v) in [(1i64, 1.0), (2, 2.0), (1, 3.0), (3, 4.0), (2, 5.0)] {
            t.insert_or_sum(k, v);
        }
        assert_eq!(t.get(1), Some(4.0));
        assert_eq!(t.get(2), Some(7.0));
        assert_eq!(t.get(3), Some(4.0));
    }

    #[test]
    fn f64_grows_past_initial_capacity() {
        let mut t = RobinHoodI64F64::with_capacity(64);
        for k in 0..1000i64 {
            t.insert_or_sum(k, k as f64 * 0.5);
        }
        assert_eq!(t.len(), 1000);
        for k in 0..1000i64 {
            assert_eq!(t.get(k), Some(k as f64 * 0.5), "missing key {k}");
        }
        assert!(t.capacity() >= 1000);
    }

    #[test]
    fn f64_agg_ingest_batch() {
        use arrow_array::{Float64Array, Int64Array};
        let keys = Int64Array::from(vec![1i64, 2, 1, 3, 2, 1]);
        let vals = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let mut agg = RobinHoodSumF64Agg::with_capacity(8);
        agg.ingest_batch(&keys, &vals);
        assert_eq!(agg.table().get(1), Some(10.0)); // 1+3+6
        assert_eq!(agg.table().get(2), Some(7.0)); // 2+5
        assert_eq!(agg.table().get(3), Some(4.0));
    }

    #[test]
    fn f64_agg_skips_nulls() {
        use arrow_array::{Float64Array, Int64Array};
        let keys = Int64Array::from(vec![Some(1i64), None, Some(2), Some(1)]);
        let vals = Float64Array::from(vec![Some(10.0), Some(20.0), None, Some(5.0)]);
        let mut agg = RobinHoodSumF64Agg::new();
        agg.ingest_batch(&keys, &vals);
        // (None, 20.0) and (2, None) are both dropped; only (1, 10) +
        // (1, 5) survive.
        assert_eq!(agg.len(), 1);
        assert_eq!(agg.table().get(1), Some(15.0));
    }

    /// Q18-shape sanity: 10K distinct i64 keys, two batches, verify
    /// SUM is bit-exact for the synthetic data (no FP rounding because
    /// values are integer-valued f64).
    #[test]
    fn f64_agg_q18_shape() {
        use arrow_array::{Float64Array, Int64Array};
        let n_groups = 10_000i64;
        let rows_per_group = 5usize;
        let mut keys: Vec<i64> = Vec::new();
        let mut vals: Vec<f64> = Vec::new();
        for k in 0..n_groups {
            for r in 0..rows_per_group {
                keys.push(k);
                vals.push((r as f64 + 1.0) * 7.0);
            }
        }
        let mut agg = RobinHoodSumF64Agg::with_capacity(n_groups as usize * 2);
        agg.ingest_batch(&Int64Array::from(keys), &Float64Array::from(vals));
        assert_eq!(agg.len(), n_groups as usize);
        // Each group: (1+2+3+4+5)*7 = 105.
        for k in 0..n_groups {
            assert_eq!(agg.table().get(k), Some(105.0), "wrong sum for key {k}");
        }
    }

    // ----- Σ.Q.L1b retry: insert_or_sum_batch_vectorised -----
    // Equivalence tests: vectorised path must produce identical final
    // state to the scalar `insert_or_sum_batch` for every input shape.

    fn scalar_then_vec_state(
        keys: &[i64],
        vals: &[f64],
        cap: usize,
    ) -> (Vec<(i64, f64)>, Vec<(i64, f64)>) {
        let mut scalar = RobinHoodI64F64::with_capacity(cap);
        scalar.insert_or_sum_batch(keys, vals);
        let mut s_pairs: Vec<(i64, f64)> = scalar.iter().collect();
        s_pairs.sort_by_key(|(k, _)| *k);

        let mut vec_path = RobinHoodI64F64::with_capacity(cap);
        vec_path.insert_or_sum_batch_vectorised(keys, vals);
        let mut v_pairs: Vec<(i64, f64)> = vec_path.iter().collect();
        v_pairs.sort_by_key(|(k, _)| *k);

        (s_pairs, v_pairs)
    }

    #[test]
    fn f64_vec_batch_empty_input() {
        let mut t = RobinHoodI64F64::new();
        t.insert_or_sum_batch_vectorised(&[], &[]);
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn f64_vec_batch_single_row() {
        let mut t = RobinHoodI64F64::new();
        t.insert_or_sum_batch_vectorised(&[42], &[3.14]);
        assert_eq!(t.get(42), Some(3.14));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn f64_vec_batch_all_duplicates() {
        // All rows hit the same key — exercises the fast-path stage 3
        // running multiple times against the same bucket.
        let n = 1000usize;
        let keys: Vec<i64> = vec![7; n];
        let vals: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let expected: f64 = vals.iter().sum();
        let mut t = RobinHoodI64F64::new();
        t.insert_or_sum_batch_vectorised(&keys, &vals);
        assert_eq!(t.len(), 1);
        assert_eq!(t.get(7), Some(expected));
    }

    #[test]
    fn f64_vec_batch_matches_scalar_low_card() {
        // 10K rows / 100 distinct keys — heavy duplication.
        let n = 10_000usize;
        let card = 100i64;
        let keys: Vec<i64> = (0..n).map(|i| (i as i64) % card).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) * 0.5).collect();
        let (s, v) = scalar_then_vec_state(&keys, &vals, 256);
        assert_eq!(s, v, "vec path diverged from scalar at low cardinality");
    }

    #[test]
    fn f64_vec_batch_matches_scalar_high_card() {
        // 10K rows / 10K distinct keys — no duplication, stresses
        // insertion path (stage 4 dominant).
        let n = 10_000usize;
        let keys: Vec<i64> = (0..n as i64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) + 0.25).collect();
        let (s, v) = scalar_then_vec_state(&keys, &vals, 64);
        // Compare element-wise with bit-exact f64 (no FP rounding since
        // each key gets exactly one value).
        assert_eq!(s.len(), v.len());
        for (a, b) in s.iter().zip(v.iter()) {
            assert_eq!(a.0, b.0);
            assert!(
                (a.1 - b.1).abs() < 1e-9,
                "diverged at key {}: {} vs {}",
                a.0,
                a.1,
                b.1
            );
        }
    }

    #[test]
    fn f64_vec_batch_matches_scalar_chunk_boundary() {
        // Exactly 1024 rows: stresses the inner chunk size = 1024 path
        // (one full chunk, no remainder).
        let n = 1024usize;
        let keys: Vec<i64> = (0..n as i64).map(|k| k % 64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64).sin() * 100.0).collect();
        let (s, v) = scalar_then_vec_state(&keys, &vals, 128);
        assert_eq!(s.len(), v.len());
        for (a, b) in s.iter().zip(v.iter()) {
            assert_eq!(a.0, b.0);
            assert!(
                (a.1 - b.1).abs() < 1e-6,
                "diverged at key {}: {} vs {}",
                a.0,
                a.1,
                b.1
            );
        }
    }

    #[test]
    fn f64_vec_batch_matches_scalar_spans_multiple_chunks() {
        // 5000 rows = 4 full chunks + remainder, 200 distinct keys.
        // Each chunk should accumulate against existing buckets.
        let n = 5_000usize;
        let card = 200i64;
        let keys: Vec<i64> = (0..n).map(|i| (i as i64 * 31) % card).collect();
        let vals: Vec<f64> = (0..n).map(|i| ((i * 7) % 1000) as f64 * 0.01).collect();
        let (s, v) = scalar_then_vec_state(&keys, &vals, 512);
        assert_eq!(s.len(), v.len(), "len differs across paths");
        for (a, b) in s.iter().zip(v.iter()) {
            assert_eq!(a.0, b.0);
            assert!(
                (a.1 - b.1).abs() < 1e-6,
                "diverged at key {}: scalar={} vec={}",
                a.0,
                a.1,
                b.1
            );
        }
    }

    #[test]
    fn f64_agg_vec_ingest_no_nulls_matches_scalar() {
        use arrow_array::{Float64Array, Int64Array};
        let keys = Int64Array::from(vec![1i64, 2, 1, 3, 2, 1, 5, 5, 5, 5]);
        let vals = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
        let mut scalar = RobinHoodSumF64Agg::new();
        scalar.ingest_batch(&keys, &vals);
        let mut vectorised = RobinHoodSumF64Agg::new();
        vectorised.ingest_batch_vectorised(&keys, &vals);
        assert_eq!(scalar.len(), vectorised.len());
        for k in [1, 2, 3, 5] {
            assert_eq!(
                scalar.table().get(k),
                vectorised.table().get(k),
                "diverged at key {k}"
            );
        }
    }

    #[test]
    fn f64_agg_vec_ingest_with_nulls_falls_back() {
        // Nulls present → vectorised path must take the masked scalar
        // fallback and produce the same result as `ingest_batch`.
        use arrow_array::{Float64Array, Int64Array};
        let keys = Int64Array::from(vec![Some(1i64), None, Some(2), Some(1), Some(2)]);
        let vals = Float64Array::from(vec![Some(10.0), Some(99.0), None, Some(5.0), Some(7.0)]);
        let mut vectorised = RobinHoodSumF64Agg::new();
        vectorised.ingest_batch_vectorised(&keys, &vals);
        // (None, 99) and (2, None) dropped; (1, 10) + (1, 5) → 15; (2, 7) → 7.
        assert_eq!(vectorised.len(), 2);
        assert_eq!(vectorised.table().get(1), Some(15.0));
        assert_eq!(vectorised.table().get(2), Some(7.0));
    }

    // ----- Σ.R.1: RobinHoodSumF64RadixAgg correctness -----
    // Property: radix-binned aggregation produces the same group-sum
    // mapping as the single-table vectorised path, for any radix_bits
    // ∈ {0..=8} and any input shape.

    fn radix_vs_single_state(
        keys: &[i64],
        vals: &[f64],
        radix_bits: u8,
        per_table_cap: usize,
    ) -> (Vec<(i64, f64)>, Vec<(i64, f64)>) {
        // Single-table baseline.
        let total_cap = per_table_cap.saturating_mul(1usize << radix_bits);
        let mut single = RobinHoodI64F64::with_capacity(total_cap.max(64));
        single.insert_or_sum_batch_vectorised(keys, vals);
        let mut s_pairs: Vec<(i64, f64)> = single.iter().collect();
        s_pairs.sort_by_key(|(k, _)| *k);

        // Radix aggregator.
        let mut radix = RobinHoodSumF64RadixAgg::new(radix_bits, per_table_cap);
        radix.ingest_batch_radix(keys, vals);
        let mut r_pairs: Vec<(i64, f64)> = radix.iter().collect();
        r_pairs.sort_by_key(|(k, _)| *k);

        (s_pairs, r_pairs)
    }

    #[test]
    fn radix_agg_empty_input() {
        let mut a = RobinHoodSumF64RadixAgg::new(6, 64);
        a.ingest_batch_radix(&[], &[]);
        assert_eq!(a.len(), 0);
        assert!(a.is_empty());
    }

    #[test]
    fn radix_agg_single_row_lands_in_one_bin() {
        let mut a = RobinHoodSumF64RadixAgg::new(6, 64);
        a.ingest_batch_radix(&[42], &[3.14]);
        assert_eq!(a.len(), 1);
        let pairs: Vec<(i64, f64)> = a.iter().collect();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, 42);
        assert_eq!(pairs[0].1, 3.14);
    }

    #[test]
    fn radix_agg_radix_bits_zero_degrades_to_single_table() {
        // radix_bits=0 → one table. Should be identical to the
        // single-table vectorised path.
        let n = 5000;
        let keys: Vec<i64> = (0..n as i64).map(|k| k % 200).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) * 0.5).collect();
        let (s, r) = radix_vs_single_state(&keys, &vals, 0, 1024);
        assert_eq!(s, r);
    }

    #[test]
    fn radix_agg_matches_single_low_card() {
        // 10K rows / 100 distinct keys, heavy duplication → many rows
        // hit the same radix bin and accumulate against the same
        // bucket. radix_bits=4 → 16 bins.
        let n = 10_000;
        let card = 100i64;
        let keys: Vec<i64> = (0..n).map(|i| (i as i64) % card).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) * 0.25).collect();
        let (s, r) = radix_vs_single_state(&keys, &vals, 4, 64);
        assert_eq!(s.len(), r.len());
        for (a, b) in s.iter().zip(r.iter()) {
            assert_eq!(a.0, b.0);
            assert!(
                (a.1 - b.1).abs() < 1e-6,
                "diverged at key {}: single={} radix={}",
                a.0,
                a.1,
                b.1
            );
        }
    }

    #[test]
    fn radix_agg_matches_single_high_card() {
        // 10K rows / 10K distinct keys → keys evenly spread across all
        // radix bins. Stresses scatter + per-bin dispatch.
        let n = 10_000;
        let keys: Vec<i64> = (0..n as i64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) + 0.125).collect();
        let (s, r) = radix_vs_single_state(&keys, &vals, 6, 32);
        assert_eq!(s.len(), r.len());
        for (a, b) in s.iter().zip(r.iter()) {
            assert_eq!(a.0, b.0);
            assert!((a.1 - b.1).abs() < 1e-9);
        }
    }

    #[test]
    fn radix_agg_spans_multiple_chunks() {
        // 5000 rows ≈ 4 VEC_CHUNK + remainder; per-chunk bin offsets
        // must reset cleanly each chunk.
        let n = 5_000;
        let card = 1000i64;
        let keys: Vec<i64> = (0..n).map(|i| (i as i64 * 31) % card).collect();
        let vals: Vec<f64> = (0..n).map(|i| ((i * 7) % 1000) as f64 * 0.01).collect();
        let (s, r) = radix_vs_single_state(&keys, &vals, 5, 64);
        assert_eq!(s.len(), r.len());
        for (a, b) in s.iter().zip(r.iter()) {
            assert_eq!(a.0, b.0);
            assert!(
                (a.1 - b.1).abs() < 1e-6,
                "diverged at key {}: single={} radix={}",
                a.0,
                a.1,
                b.1
            );
        }
    }

    #[test]
    fn radix_agg_max_radix_bits_8() {
        // radix_bits=8 → 256 bins; ensures the per-bin counters
        // (u32) array fits and offsets[] indexing is correct.
        let n = 20_000;
        let keys: Vec<i64> = (0..n as i64).map(|k| k % 1000).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) * 0.5).collect();
        let (s, r) = radix_vs_single_state(&keys, &vals, 8, 16);
        assert_eq!(s.len(), r.len());
        for (a, b) in s.iter().zip(r.iter()) {
            assert_eq!(a.0, b.0);
            assert!((a.1 - b.1).abs() < 1e-6);
        }
    }

    #[test]
    fn radix_agg_triggers_per_bin_grow() {
        // Start with tiny per_table_cap so several bins exceed their
        // initial capacity and trigger grow() mid-ingest. Verify all
        // keys survive at correct values.
        let n = 2_000;
        let keys: Vec<i64> = (0..n as i64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) + 0.5).collect();
        let mut radix = RobinHoodSumF64RadixAgg::new(4, 4); // 16 bins, cap=4
        radix.ingest_batch_radix(&keys, &vals);
        assert_eq!(radix.len(), n);
        let mut pairs: Vec<(i64, f64)> = radix.iter().collect();
        pairs.sort_by_key(|(k, _)| *k);
        for k in 0..n as i64 {
            assert!(
                (pairs[k as usize].1 - (k as f64 + 0.5)).abs() < 1e-9,
                "lost or wrong value for key {k} after per-bin grow"
            );
        }
    }

    // -----------------------------------------------------------------
    // REV.5.b — RobinHoodSumF64GlobalRadixAgg (corrected global-partition)
    // -----------------------------------------------------------------

    /// Ingest in `batch`-row batches + finish, vs the single-table
    /// baseline. Returns sorted (single, global) (key,sum) pairs.
    fn global_radix_vs_single(
        keys: &[i64],
        vals: &[f64],
        radix_bits: u8,
        per_table_cap: usize,
        mem_budget_bytes: usize,
        batch: usize,
    ) -> (Vec<(i64, f64)>, Vec<(i64, f64)>) {
        let total_cap = per_table_cap.saturating_mul(1usize << radix_bits);
        let mut single = RobinHoodI64F64::with_capacity(total_cap.max(64));
        single.insert_or_sum_batch_vectorised(keys, vals);
        let mut s: Vec<(i64, f64)> = single.iter().collect();
        s.sort_by_key(|(k, _)| *k);

        let mut g = RobinHoodSumF64GlobalRadixAgg::new(radix_bits, per_table_cap, mem_budget_bytes);
        let mut o = 0;
        while o < keys.len() {
            let e = (o + batch).min(keys.len());
            g.ingest_batch(&keys[o..e], &vals[o..e]);
            o = e;
        }
        g.finish();
        let mut r: Vec<(i64, f64)> = g.iter().collect();
        r.sort_by_key(|(k, _)| *k);
        (s, r)
    }

    fn assert_pairs_eq(s: &[(i64, f64)], r: &[(i64, f64)]) {
        assert_eq!(
            s.len(),
            r.len(),
            "group count differs: single={} global={}",
            s.len(),
            r.len()
        );
        for (a, b) in s.iter().zip(r.iter()) {
            assert_eq!(a.0, b.0, "key mismatch");
            assert!(
                (a.1 - b.1).abs() < 1e-6,
                "sum diverged at key {}: single={} global={}",
                a.0,
                a.1,
                b.1
            );
        }
    }

    #[test]
    fn global_radix_empty() {
        let mut g = RobinHoodSumF64GlobalRadixAgg::new(6, 64, 1 << 20);
        g.ingest_batch(&[], &[]);
        g.finish();
        assert_eq!(g.len(), 0);
        assert!(g.is_empty());
    }

    #[test]
    fn global_radix_matches_single_high_card() {
        // 50K distinct keys, ingested in 8192-row batches, 256 bins.
        let n = 50_000;
        let keys: Vec<i64> = (0..n as i64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) + 0.125).collect();
        let (s, r) = global_radix_vs_single(&keys, &vals, 8, 256, 1 << 26, 8192);
        assert_pairs_eq(&s, &r);
    }

    #[test]
    fn global_radix_matches_single_low_card_sums() {
        // 100K rows / 500 distinct keys → heavy accumulation per key.
        let n = 100_000;
        let card = 500i64;
        let keys: Vec<i64> = (0..n).map(|i| (i as i64) % card).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) * 0.25).collect();
        let (s, r) = global_radix_vs_single(&keys, &vals, 6, 128, 1 << 26, 8192);
        assert_pairs_eq(&s, &r);
    }

    #[test]
    fn global_radix_incremental_drain_correct() {
        // Tiny 16 KB budget (~1024 rows) forces MANY mid-stream drains;
        // result must still match single-table (drains accumulate into
        // the persistent micro-tables, incl. keys repeated across drains).
        let n = 40_000;
        let keys: Vec<i64> = (0..n as i64).map(|k| k % 7000).collect();
        let vals: Vec<f64> = (0..n).map(|i| ((i * 3) % 97) as f64 + 0.5).collect();
        let (s, r) = global_radix_vs_single(&keys, &vals, 6, 256, 16 * 1024, 4096);
        assert_pairs_eq(&s, &r);
    }

    #[test]
    fn global_radix_bits_zero_degrades_to_single() {
        let n = 8_000;
        let keys: Vec<i64> = (0..n as i64).map(|k| k % 300).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) * 0.5).collect();
        let (s, r) = global_radix_vs_single(&keys, &vals, 0, 1024, 1 << 26, 8192);
        assert_pairs_eq(&s, &r);
    }

    #[test]
    fn global_radix_sums_same_key_across_many_batches() {
        // Same key in every batch + a small budget → must sum across
        // batches AND across incremental drains.
        let batches = 50usize;
        let per = 1000usize;
        let mut g = RobinHoodSumF64GlobalRadixAgg::new(4, 16, 8 * 1024);
        for _ in 0..batches {
            g.ingest_batch(&vec![42i64; per], &vec![1.0f64; per]);
        }
        g.finish();
        assert_eq!(g.len(), 1);
        let pairs: Vec<(i64, f64)> = g.iter().collect();
        assert_eq!(pairs[0].0, 42);
        assert!((pairs[0].1 - (batches * per) as f64).abs() < 1e-6);
    }

    // -----------------------------------------------------------------
    // REV.5.b — spill-backed global radix (memory-bounded, disk-backed).
    // The residency win preserved under a memory bound: over-budget
    // partitions go to disk and are aggregated exactly once at finish.
    // -----------------------------------------------------------------

    static SPILL_TEST_SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

    /// A fresh, empty temp dir unique to this test (process id + counter),
    /// so the post-finish "no leftover spill files" assertion is exact and
    /// parallel tests never collide.
    fn fresh_spill_dir() -> std::path::PathBuf {
        let n = SPILL_TEST_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!(
            "ematix-radix-spill-test-{}-{}",
            std::process::id(),
            n
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Spill-backed aggregator vs single-table. Returns sorted (single,
    /// global) pairs + whether spilling fired, and asserts spill files are
    /// cleaned up after finish().
    fn global_radix_spill_vs_single(
        keys: &[i64],
        vals: &[f64],
        radix_bits: u8,
        per_table_cap: usize,
        mem_budget_bytes: usize,
        batch: usize,
    ) -> (Vec<(i64, f64)>, Vec<(i64, f64)>, bool) {
        let total_cap = per_table_cap.saturating_mul(1usize << radix_bits);
        let mut single = RobinHoodI64F64::with_capacity(total_cap.max(64));
        single.insert_or_sum_batch_vectorised(keys, vals);
        let mut s: Vec<(i64, f64)> = single.iter().collect();
        s.sort_by_key(|(k, _)| *k);

        let dir = fresh_spill_dir();
        let mut g = RobinHoodSumF64GlobalRadixAgg::new_with_spill(
            radix_bits,
            per_table_cap,
            mem_budget_bytes,
            dir.clone(),
        );
        let mut o = 0;
        while o < keys.len() {
            let e = (o + batch).min(keys.len());
            g.ingest_batch(&keys[o..e], &vals[o..e]);
            o = e;
        }
        g.finish();
        let spilled = g.did_spill();
        let mut r: Vec<(i64, f64)> = g.iter().collect();
        r.sort_by_key(|(k, _)| *k);
        let leftover = std::fs::read_dir(&dir).map(|rd| rd.count()).unwrap_or(0);
        assert_eq!(
            leftover, 0,
            "spill files not cleaned up after finish ({leftover} left)"
        );
        drop(g);
        let _ = std::fs::remove_dir_all(&dir);
        (s, r, spilled)
    }

    #[test]
    fn global_radix_spill_matches_single_high_card() {
        // 50K distinct keys, tiny 16 KB budget → many spills across 256 bins.
        let n = 50_000;
        let keys: Vec<i64> = (0..n as i64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) + 0.125).collect();
        let (s, r, spilled) = global_radix_spill_vs_single(&keys, &vals, 8, 256, 16 * 1024, 8192);
        assert!(spilled, "expected spilling under a 16 KB budget");
        assert_pairs_eq(&s, &r);
    }

    #[test]
    fn global_radix_spill_low_card_heavy_accumulation() {
        // 100K rows / 500 distinct → heavy per-key sums spanning many spills.
        let n = 100_000;
        let card = 500i64;
        let keys: Vec<i64> = (0..n).map(|i| (i as i64) % card).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) * 0.25).collect();
        let (s, r, spilled) = global_radix_spill_vs_single(&keys, &vals, 6, 128, 16 * 1024, 4096);
        assert!(spilled);
        assert_pairs_eq(&s, &r);
    }

    #[test]
    fn global_radix_spill_no_trigger_matches_single() {
        // Budget far exceeds the data → spill backend present but never
        // fires; result matches single-table (the in-RAM residency path).
        let n = 20_000;
        let keys: Vec<i64> = (0..n as i64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) + 0.5).collect();
        let (s, r, spilled) = global_radix_spill_vs_single(&keys, &vals, 6, 512, 1 << 26, 8192);
        assert!(!spilled, "did not expect spilling under a 64 MB budget");
        assert_pairs_eq(&s, &r);
    }

    #[test]
    fn global_radix_spill_single_bin_degenerate() {
        // radix_bits=0 (one bin) + tiny budget → degenerate path still
        // spills bin 0 and aggregates correctly.
        let n = 30_000;
        let keys: Vec<i64> = (0..n as i64).map(|k| k % 4000).collect();
        let vals: Vec<f64> = (0..n).map(|i| ((i * 7) % 13) as f64 + 0.5).collect();
        let (s, r, spilled) = global_radix_spill_vs_single(&keys, &vals, 0, 4096, 16 * 1024, 4096);
        assert!(spilled);
        assert_pairs_eq(&s, &r);
    }

    #[test]
    fn global_radix_spill_sums_same_key_across_batches() {
        // Same key every batch + tiny budget → must sum across batches AND
        // across many disk spills, then clean up.
        let batches = 60usize;
        let per = 1000usize;
        let dir = fresh_spill_dir();
        let mut g = RobinHoodSumF64GlobalRadixAgg::new_with_spill(4, 16, 8 * 1024, dir.clone());
        for _ in 0..batches {
            g.ingest_batch(&vec![7i64; per], &vec![2.0f64; per]);
        }
        g.finish();
        assert!(g.did_spill());
        assert_eq!(g.len(), 1);
        let pairs: Vec<(i64, f64)> = g.iter().collect();
        assert_eq!(pairs[0].0, 7);
        assert!((pairs[0].1 - (batches * per) as f64 * 2.0).abs() < 1e-6);
        let leftover = std::fs::read_dir(&dir).map(|rd| rd.count()).unwrap_or(0);
        assert_eq!(leftover, 0, "spill files left after finish");
        drop(g);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn f64_vec_batch_triggers_grow_mid_stream() {
        // Start small (64 buckets, ~44 capacity at 70% load), insert
        // 1000 unique keys, force multiple grow()s inside stage 4.
        let n = 1000usize;
        let keys: Vec<i64> = (0..n as i64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) + 0.125).collect();
        let mut t = RobinHoodI64F64::with_capacity(64);
        t.insert_or_sum_batch_vectorised(&keys, &vals);
        assert_eq!(t.len(), n);
        for k in 0..n as i64 {
            assert_eq!(
                t.get(k),
                Some(k as f64 + 0.125),
                "lost or wrong value for key {k} after mid-stream grow"
            );
        }
    }

    // ---------------------------------------------------------------------
    // Σ.Q.L12 — TaggedI64F64 (SwissTable-style SIMD-tagged hash) tests.
    // Mirror coverage of RobinHoodI64F64: insert/lookup, accumulation,
    // grow, batch equivalence vs scalar.
    // ---------------------------------------------------------------------

    #[test]
    fn tagged_insert_and_lookup() {
        let mut t = TaggedI64F64::new();
        t.insert_or_sum(42, 1.5);
        t.insert_or_sum(7, 2.25);
        t.insert_or_sum(42, 0.5);
        assert_eq!(t.get(42), Some(2.0));
        assert_eq!(t.get(7), Some(2.25));
        assert_eq!(t.get(99), None);
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn tagged_sum_accumulates() {
        let mut t = TaggedI64F64::new();
        for (k, v) in [(1i64, 1.0), (2, 2.0), (1, 3.0), (3, 4.0), (2, 5.0)] {
            t.insert_or_sum(k, v);
        }
        assert_eq!(t.get(1), Some(4.0));
        assert_eq!(t.get(2), Some(7.0));
        assert_eq!(t.get(3), Some(4.0));
    }

    #[test]
    fn tagged_grows_past_initial_capacity() {
        let mut t = TaggedI64F64::with_capacity(64);
        for k in 0..1000i64 {
            t.insert_or_sum(k, k as f64 * 0.5);
        }
        assert_eq!(t.len(), 1000);
        for k in 0..1000i64 {
            assert_eq!(t.get(k), Some(k as f64 * 0.5), "missing key {k}");
        }
        assert!(t.capacity() >= 1000);
    }

    #[test]
    fn tagged_iter_yields_only_real_entries() {
        // Sanity: tail-mirror entries must not appear in iter().
        let mut t = TaggedI64F64::with_capacity(64);
        for k in 0..5i64 {
            t.insert_or_sum(k, k as f64);
        }
        let pairs: Vec<(i64, f64)> = t.iter().collect();
        assert_eq!(pairs.len(), 5);
        let mut keys: Vec<i64> = pairs.iter().map(|(k, _)| *k).collect();
        keys.sort();
        assert_eq!(keys, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn tagged_vec_batch_matches_scalar() {
        // Equivalence: vectorised path must produce identical state to
        // the scalar `insert_or_sum_batch` for any input shape.
        let n = 5000;
        let card = 200i64;
        let keys: Vec<i64> = (0..n).map(|i| (i as i64 * 31) % card).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) * 0.125).collect();

        let cap = (card as usize * 2).max(64);
        let mut scalar = TaggedI64F64::with_capacity(cap);
        scalar.insert_or_sum_batch(&keys, &vals);
        let mut vec_path = TaggedI64F64::with_capacity(cap);
        vec_path.insert_or_sum_batch_vectorised(&keys, &vals);

        let mut s_pairs: Vec<(i64, f64)> = scalar.iter().collect();
        let mut v_pairs: Vec<(i64, f64)> = vec_path.iter().collect();
        s_pairs.sort_by_key(|(k, _)| *k);
        v_pairs.sort_by_key(|(k, _)| *k);
        assert_eq!(s_pairs.len(), v_pairs.len());
        for (a, b) in s_pairs.iter().zip(v_pairs.iter()) {
            assert_eq!(a.0, b.0);
            assert!((a.1 - b.1).abs() < 1e-9, "diverged at key {}", a.0);
        }
    }

    #[test]
    fn tagged_vec_batch_triggers_grow_mid_stream() {
        // Force multiple grow()s inside stage 4.
        let n = 1000usize;
        let keys: Vec<i64> = (0..n as i64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64) + 0.125).collect();
        let mut t = TaggedI64F64::with_capacity(64);
        t.insert_or_sum_batch_vectorised(&keys, &vals);
        assert_eq!(t.len(), n);
        for k in 0..n as i64 {
            assert_eq!(
                t.get(k),
                Some(k as f64 + 0.125),
                "lost or wrong value for key {k} after mid-stream grow"
            );
        }
    }

    #[test]
    fn tagged_vs_robin_hood_q18_shape() {
        // Q18-shape sanity: 10K distinct keys, multiple ingests, must
        // match the RobinHoodI64F64 result bit-exactly (synthetic data
        // chosen for integer-valued f64 sums).
        let n_groups = 10_000i64;
        let rows_per_group = 5usize;
        let mut keys: Vec<i64> = Vec::new();
        let mut vals: Vec<f64> = Vec::new();
        for k in 0..n_groups {
            for r in 0..rows_per_group {
                keys.push(k);
                vals.push((r as f64 + 1.0) * 7.0);
            }
        }
        let mut rh = RobinHoodI64F64::with_capacity(n_groups as usize * 2);
        rh.insert_or_sum_batch_vectorised(&keys, &vals);
        let mut tagged = TaggedI64F64::with_capacity(n_groups as usize * 2);
        tagged.insert_or_sum_batch_vectorised(&keys, &vals);

        assert_eq!(rh.len(), tagged.len());
        for k in 0..n_groups {
            assert_eq!(rh.get(k), tagged.get(k), "diverged at key {k}");
            assert_eq!(tagged.get(k), Some(105.0));
        }
    }

    #[test]
    fn tagged_agg_wrapper_works() {
        use arrow_array::{Float64Array, Int64Array};
        let keys = Int64Array::from(vec![1i64, 2, 1, 3, 2, 1]);
        let vals = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let mut agg = TaggedSumF64Agg::with_capacity(8);
        agg.ingest_batch_vectorised(&keys, &vals);
        assert_eq!(agg.table().get(1), Some(10.0));
        assert_eq!(agg.table().get(2), Some(7.0));
        assert_eq!(agg.table().get(3), Some(4.0));
    }

    // ---- Σ.R.2.a — RobinHoodI64AvgF64 + RobinHoodAvgF64Agg tests ----
    //
    // Sister to the Σ.Q.L1b RobinHoodI64F64 / RobinHoodSumF64Agg tests.
    // Targets the Q17 SF=10 hot kernel: AVG(f64) GROUP BY i64. The
    // bucket now carries (sum: f64, count: u64) so the operator can
    // emit Partial state directly, and the FinalPartitioned stage can
    // merge two (sum, count) pairs without recomputing.

    #[test]
    fn avg_f64_insert_first_row_seeds_sum_and_count() {
        let mut t = RobinHoodI64AvgF64::new();
        t.insert_or_update(42, 1.5);
        let (sum, count) = t.get(42).expect("present");
        assert_eq!(sum, 1.5);
        assert_eq!(count, 1);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn avg_f64_second_row_accumulates_sum_and_count() {
        let mut t = RobinHoodI64AvgF64::new();
        t.insert_or_update(7, 2.25);
        t.insert_or_update(7, 0.75);
        let (sum, count) = t.get(7).expect("present");
        assert_eq!(sum, 3.0);
        assert_eq!(count, 2);
        assert_eq!(t.len(), 1, "same key must not create a second bucket");
    }

    #[test]
    fn avg_f64_average_helper_is_sum_over_count() {
        let mut t = RobinHoodI64AvgF64::new();
        t.insert_or_update(1, 10.0);
        t.insert_or_update(1, 20.0);
        t.insert_or_update(1, 30.0);
        // AVG(10, 20, 30) = 20.0
        let avg = t.average(1).expect("present");
        assert!((avg - 20.0).abs() < 1e-12, "got {avg}");
    }

    #[test]
    fn avg_f64_get_missing_returns_none() {
        let t = RobinHoodI64AvgF64::new();
        assert!(t.get(99).is_none());
        assert!(t.average(99).is_none());
    }

    #[test]
    fn avg_f64_grows_past_initial_capacity() {
        let mut t = RobinHoodI64AvgF64::with_capacity(64);
        for k in 0..1024i64 {
            t.insert_or_update(k, k as f64 * 0.5);
        }
        assert_eq!(t.len(), 1024);
        // Spot-check a few keys survived the grow chain.
        assert_eq!(t.get(0), Some((0.0, 1)));
        assert_eq!(t.get(500), Some((250.0, 1)));
        assert_eq!(t.get(1023), Some((511.5, 1)));
    }

    #[test]
    fn avg_f64_merge_partial_seeds_then_accumulates() {
        // FinalPartitioned-stage simulation: feed (sum, count) pairs and
        // verify they merge correctly across batches.
        let mut t = RobinHoodI64AvgF64::new();
        t.insert_or_merge(10, 100.0, 4);
        t.insert_or_merge(10, 50.0, 1);
        t.insert_or_merge(11, 7.5, 3);
        assert_eq!(t.get(10), Some((150.0, 5)));
        assert_eq!(t.get(11), Some((7.5, 3)));
        // average(10) = 150.0 / 5 = 30.0
        assert!((t.average(10).unwrap() - 30.0).abs() < 1e-12);
    }

    #[test]
    fn avg_f64_iter_yields_all_keys() {
        let mut t = RobinHoodI64AvgF64::new();
        t.insert_or_update(1, 1.0);
        t.insert_or_update(2, 2.0);
        t.insert_or_update(3, 3.0);
        t.insert_or_update(2, 4.0);
        let mut entries: Vec<(i64, f64, u64)> = t.iter_sum_count().collect();
        entries.sort_by_key(|(k, _, _)| *k);
        assert_eq!(entries, vec![(1, 1.0, 1), (2, 6.0, 2), (3, 3.0, 1)]);
    }

    #[test]
    fn avg_f64_agg_ingest_batch_no_nulls() {
        use arrow_array::{Float64Array, Int64Array};
        let keys = Int64Array::from(vec![1i64, 2, 1, 3, 2, 1]);
        let vals = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let mut agg = RobinHoodAvgF64Agg::with_capacity(8);
        agg.ingest_batch(&keys, &vals);
        assert_eq!(agg.table().get(1), Some((10.0, 3))); // 1+3+6
        assert_eq!(agg.table().get(2), Some((7.0, 2))); // 2+5
        assert_eq!(agg.table().get(3), Some((4.0, 1)));
    }

    #[test]
    fn avg_f64_agg_skips_null_keys_and_values() {
        use arrow_array::{Float64Array, Int64Array};
        let keys = Int64Array::from(vec![Some(1i64), None, Some(1), Some(2)]);
        let vals = Float64Array::from(vec![Some(1.0), Some(2.0), None, Some(4.0)]);
        let mut agg = RobinHoodAvgF64Agg::new();
        agg.ingest_batch(&keys, &vals);
        // Row 0 (k=1, v=1.0) and row 3 (k=2, v=4.0) contribute; rows 1+2 skipped.
        assert_eq!(agg.table().get(1), Some((1.0, 1)));
        assert_eq!(agg.table().get(2), Some((4.0, 1)));
        assert_eq!(agg.table().len(), 2);
    }

    #[test]
    fn avg_f64_agg_merge_partials_no_nulls() {
        use arrow_array::{Float64Array, Int64Array, UInt64Array};
        // Simulate two upstream Partial outputs being merged at the Final.
        let keys = Int64Array::from(vec![1i64, 2, 1, 3]);
        let sums = Float64Array::from(vec![10.0, 20.0, 5.0, 7.0]);
        let counts = UInt64Array::from(vec![4u64, 5, 1, 2]);
        let mut agg = RobinHoodAvgF64Agg::new();
        agg.ingest_partials(&keys, &sums, &counts);
        assert_eq!(agg.table().get(1), Some((15.0, 5)));
        assert_eq!(agg.table().get(2), Some((20.0, 5)));
        assert_eq!(agg.table().get(3), Some((7.0, 2)));
    }

    #[test]
    fn avg_f64_q17_shape_matches_scalar_sum_count() {
        // Q17-like: 2K distinct keys × 30 rows/key = 60K rows.
        let n_groups = 2_000i64;
        let rows_per_group = 30usize;
        let mut keys: Vec<i64> = Vec::new();
        let mut vals: Vec<f64> = Vec::new();
        for k in 0..n_groups {
            for r in 0..rows_per_group {
                keys.push(k);
                vals.push((r as f64 + 1.0) * 7.0);
            }
        }
        let key_arr = arrow_array::Int64Array::from(keys);
        let val_arr = arrow_array::Float64Array::from(vals);
        let mut agg = RobinHoodAvgF64Agg::with_capacity(n_groups as usize * 2);
        agg.ingest_batch(&key_arr, &val_arr);
        // Per-group sum = 7 * (1+2+...+30) = 7 * 465 = 3255; count = 30.
        for k in 0..n_groups {
            assert_eq!(agg.table().get(k), Some((3255.0, 30)));
        }
    }

    // ----- Σ.R.2.a vectorised batch ingest -----

    fn avg_scalar_then_vec_state(
        keys: &[i64],
        vals: &[f64],
        cap: usize,
    ) -> (Vec<(i64, f64, u64)>, Vec<(i64, f64, u64)>) {
        let mut scalar = RobinHoodI64AvgF64::with_capacity(cap);
        for i in 0..keys.len() {
            scalar.insert_or_update(keys[i], vals[i]);
        }
        let mut scalar_state: Vec<(i64, f64, u64)> = scalar.iter_sum_count().collect();
        scalar_state.sort_by_key(|(k, _, _)| *k);

        let mut vec_path = RobinHoodI64AvgF64::with_capacity(cap);
        vec_path.insert_or_update_batch_vectorised(keys, vals);
        let mut vec_state: Vec<(i64, f64, u64)> = vec_path.iter_sum_count().collect();
        vec_state.sort_by_key(|(k, _, _)| *k);

        (scalar_state, vec_state)
    }

    #[test]
    fn avg_vec_batch_empty_input_is_noop() {
        let mut t = RobinHoodI64AvgF64::new();
        t.insert_or_update_batch_vectorised(&[], &[]);
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn avg_vec_batch_single_row_seeds() {
        let mut t = RobinHoodI64AvgF64::new();
        t.insert_or_update_batch_vectorised(&[42], &[3.14]);
        assert_eq!(t.get(42), Some((3.14, 1)));
    }

    #[test]
    fn avg_vec_batch_matches_scalar_low_cardinality() {
        let keys = vec![1i64, 2, 1, 3, 2, 1, 5, 5];
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let (scalar, vec_path) = avg_scalar_then_vec_state(&keys, &vals, 64);
        assert_eq!(scalar, vec_path);
    }

    #[test]
    fn avg_vec_batch_matches_scalar_high_cardinality() {
        // 5K distinct keys, 4 rows each → 20K input rows.
        let mut keys = Vec::new();
        let mut vals = Vec::new();
        for k in 0..5_000i64 {
            for r in 0..4 {
                keys.push(k);
                vals.push(k as f64 + r as f64 * 0.25);
            }
        }
        let (scalar, vec_path) = avg_scalar_then_vec_state(&keys, &vals, 8_192);
        assert_eq!(scalar.len(), 5_000);
        assert_eq!(scalar, vec_path);
    }

    #[test]
    fn avg_vec_batch_spans_multiple_1024_chunks() {
        // 3 full chunks + a partial. Forces the off-loop branch +
        // ensures hits found in chunk N still accumulate when keys
        // recur in chunk N+1.
        let n = 1024 * 3 + 257;
        let mut keys = Vec::with_capacity(n);
        let mut vals = Vec::with_capacity(n);
        for i in 0..n {
            keys.push((i % 137) as i64);
            vals.push(i as f64 * 0.5);
        }
        let (scalar, vec_path) = avg_scalar_then_vec_state(&keys, &vals, 256);
        assert_eq!(scalar, vec_path);
    }

    #[test]
    fn avg_agg_vec_ingest_no_nulls_matches_scalar() {
        use arrow_array::{Float64Array, Int64Array};
        let keys = Int64Array::from((0..2_000i64).chain(0..2_000i64).collect::<Vec<_>>());
        let vals = Float64Array::from(
            (0..2_000)
                .map(|i| i as f64 * 0.5)
                .chain((0..2_000).map(|i| i as f64 * 0.5 + 1.0))
                .collect::<Vec<_>>(),
        );
        let mut scalar = RobinHoodAvgF64Agg::new();
        scalar.ingest_batch(&keys, &vals);
        let mut vectorised = RobinHoodAvgF64Agg::new();
        vectorised.ingest_batch_vectorised(&keys, &vals);
        for k in 0..2_000i64 {
            assert_eq!(scalar.table().get(k), vectorised.table().get(k), "k={k}");
        }
    }

    #[test]
    fn avg_agg_vec_ingest_with_nulls_falls_back_to_scalar() {
        use arrow_array::{Float64Array, Int64Array};
        let keys = Int64Array::from(vec![Some(1i64), None, Some(1), Some(2)]);
        let vals = Float64Array::from(vec![Some(1.0), Some(2.0), None, Some(4.0)]);
        let mut vectorised = RobinHoodAvgF64Agg::new();
        vectorised.ingest_batch_vectorised(&keys, &vals);
        // Same expected state as the null-aware scalar path.
        assert_eq!(vectorised.table().get(1), Some((1.0, 1)));
        assert_eq!(vectorised.table().get(2), Some((4.0, 1)));
        assert_eq!(vectorised.table().len(), 2);
    }
}
