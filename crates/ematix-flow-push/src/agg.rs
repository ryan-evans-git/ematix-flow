//! PV.M.8 — fused i64-keyed f64-SUM aggregation kernel.
//!
//! ## Why this exists (the measured lever)
//!
//! The Q15 SF=10 gap to Polars is the `GROUP BY l_suppkey` SUM. The
//! `q15_morsel_agg_spike` + EXPLAIN ANALYZE de-risk (Phase-0) found:
//!   - the actual aggregation work is ~4.5ms single-thread (2.27M rows
//!     → 100K groups) and does NOT parallelize past ~2× — it's too
//!     small to spread over 14 cores;
//!   - yet DataFusion spends ~13ms wall / ~58ms CPU on it, almost all
//!     in `time_calculating_group_ids` — done TWICE (Partial + Final)
//!     across a 1.12M-row hash-`RepartitionExec` shuffle, via a
//!     `GroupValues`→group-idx→accumulator-`Vec` indirection that runs
//!     ~10× slower per row (~22ns) than a direct inline open-addressing
//!     table (~2ns).
//!
//! So the lever is NOT a parallel agg — it is to do the group-keying
//! ONCE, inline, per scan-partition (riding on the scan threads for
//! free), then **direct-combine** the per-partition tables instead of
//! shuffling 1.12M rows through the Partial→Repartition→Final exchange.
//! This kernel is that inline table + its combine. Pure — no Arrow, no
//! DataFusion (keeps the LLVM codegen tax local, per
//! `project_optimizer_codegen_sensitivity.md`).
//!
//! ## Generality (no TPC-H hardcoding)
//!
//! - Any `i64` group key — `i64::MIN` is the empty sentinel, but a real
//!   `MIN` key is handled in a dedicated slot (one well-predicted branch
//!   per row), so the kernel is total over all `i64`.
//! - Grows + rehashes on load > 0.75, so an inaccurate size hint is
//!   only a perf hint, never a correctness/capacity bug.
//! - NULL handling is the caller's responsibility: `ingest` assumes
//!   non-null key+value (the gated fast path). The flow-core operator
//!   gates on `column_has_no_nulls` and falls back to DataFusion
//!   otherwise — same discipline as PV.M.7.

/// `i64::MIN` is reserved as the empty-slot sentinel; a real `MIN` key
/// accumulates in [`I64SumF64::min_key_sum`] so the kernel stays total.
const EMPTY: i64 = i64::MIN;

#[inline(always)]
fn hash_i64(k: i64) -> u64 {
    // Fibonacci/multiplicative hash — what DuckDB/Polars effectively use
    // for integer keys; far cheaper than ahash for a single i64.
    (k as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Smallest pow2 capacity giving load < 0.5 for `distinct_hint` groups
/// (floor 2048 to avoid pathological tiny tables / frequent growth).
fn capacity_for(distinct_hint: usize) -> usize {
    let want = distinct_hint.saturating_mul(2).max(1);
    let mut cap = 2048usize;
    while cap < want {
        cap <<= 1;
    }
    cap
}

/// Open-addressing (linear-probe) `i64 -> f64` SUM table with an
/// inline-stored running sum. Pow2 capacity; grows on load > 0.75.
#[derive(Clone)]
pub struct I64SumF64 {
    keys: Vec<i64>,
    sums: Vec<f64>,
    mask: usize,
    occupied: usize,
    /// Running sum for the reserved `i64::MIN` key, if it ever appears.
    min_key_sum: Option<f64>,
}

impl Default for I64SumF64 {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl I64SumF64 {
    /// New table pre-sized for ~`distinct_hint` groups.
    pub fn with_capacity(distinct_hint: usize) -> Self {
        let cap = capacity_for(distinct_hint);
        I64SumF64 {
            keys: vec![EMPTY; cap],
            sums: vec![0.0; cap],
            mask: cap - 1,
            occupied: 0,
            min_key_sum: None,
        }
    }

    /// Accumulate `val` into the SUM for `key`.
    #[inline]
    pub fn add(&mut self, key: i64, val: f64) {
        if key == EMPTY {
            *self.min_key_sum.get_or_insert(0.0) += val;
            return;
        }
        // Keep load < 0.75 (well-predicted not-taken when sized right).
        if (self.occupied + 1) * 4 >= self.keys.len() * 3 {
            self.grow();
        }
        let mut i = (hash_i64(key) as usize) & self.mask;
        loop {
            unsafe {
                let cur = *self.keys.get_unchecked(i);
                if cur == key {
                    *self.sums.get_unchecked_mut(i) += val;
                    return;
                }
                if cur == EMPTY {
                    *self.keys.get_unchecked_mut(i) = key;
                    *self.sums.get_unchecked_mut(i) = val;
                    self.occupied += 1;
                    return;
                }
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Bulk-accumulate parallel `keys`/`vals` slices (the hot path).
    #[inline]
    pub fn ingest(&mut self, keys: &[i64], vals: &[f64]) {
        assert_eq!(keys.len(), vals.len(), "key/val length mismatch");
        for (&k, &v) in keys.iter().zip(vals) {
            self.add(k, v);
        }
    }

    /// Merge another table's groups into this one (the direct-combine
    /// that replaces RepartitionExec+Final). Float sum order is
    /// `self`-then-`other`-slot-order — deterministic for fixed inputs.
    pub fn combine(&mut self, other: &I64SumF64) {
        let mut i = 0;
        while i < other.keys.len() {
            let k = other.keys[i];
            if k != EMPTY {
                self.add(k, other.sums[i]);
            }
            i += 1;
        }
        if let Some(ms) = other.min_key_sum {
            *self.min_key_sum.get_or_insert(0.0) += ms;
        }
    }

    /// Number of distinct groups (including the `MIN` group if present).
    pub fn len(&self) -> usize {
        self.occupied + self.min_key_sum.is_some() as usize
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Visit each `(key, sum)` once, in unspecified order.
    #[inline]
    pub fn for_each(&self, mut f: impl FnMut(i64, f64)) {
        let mut i = 0;
        while i < self.keys.len() {
            let k = self.keys[i];
            if k != EMPTY {
                f(k, self.sums[i]);
            }
            i += 1;
        }
        if let Some(ms) = self.min_key_sum {
            f(EMPTY, ms);
        }
    }

    /// Append all groups to the output vectors (operator emit path).
    pub fn drain_into(&self, keys_out: &mut Vec<i64>, sums_out: &mut Vec<f64>) {
        self.for_each(|k, s| {
            keys_out.push(k);
            sums_out.push(s);
        });
    }

    fn grow(&mut self) {
        let new_cap = (self.keys.len() * 2).max(2048);
        let old_keys = std::mem::replace(&mut self.keys, vec![EMPTY; new_cap]);
        let old_sums = std::mem::replace(&mut self.sums, vec![0.0; new_cap]);
        self.sums.fill(0.0);
        self.mask = new_cap - 1;
        self.occupied = 0;
        let mut i = 0;
        while i < old_keys.len() {
            let k = old_keys[i];
            if k != EMPTY {
                self.insert_nogrow(k, old_sums[i]);
            }
            i += 1;
        }
    }

    /// Insert during rehash — capacity is guaranteed, never grows.
    #[inline]
    fn insert_nogrow(&mut self, key: i64, val: f64) {
        let mut i = (hash_i64(key) as usize) & self.mask;
        loop {
            let cur = self.keys[i];
            if cur == EMPTY {
                self.keys[i] = key;
                self.sums[i] = val;
                self.occupied += 1;
                return;
            }
            // rehash sources are distinct keys → no equal case
            i = (i + 1) & self.mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn reference(keys: &[i64], vals: &[f64]) -> HashMap<i64, f64> {
        let mut m = HashMap::new();
        for (&k, &v) in keys.iter().zip(vals) {
            *m.entry(k).or_insert(0.0) += v;
        }
        m
    }

    fn collect(t: &I64SumF64) -> HashMap<i64, f64> {
        let mut m = HashMap::new();
        t.for_each(|k, v| {
            m.insert(k, v);
        });
        m
    }

    #[test]
    fn empty_table_has_no_groups() {
        let t = I64SumF64::with_capacity(0);
        assert_eq!(t.len(), 0);
        assert!(t.is_empty());
    }

    #[test]
    fn single_key_accumulates() {
        let mut t = I64SumF64::with_capacity(4);
        t.add(7, 1.5);
        t.add(7, 2.5);
        assert_eq!(t.len(), 1);
        assert_eq!(collect(&t)[&7], 4.0);
    }

    #[test]
    fn distinct_keys_independent() {
        let mut t = I64SumF64::with_capacity(4);
        t.add(1, 10.0);
        t.add(2, 20.0);
        t.add(1, 5.0);
        let m = collect(&t);
        assert_eq!(t.len(), 2);
        assert_eq!(m[&1], 15.0);
        assert_eq!(m[&2], 20.0);
    }

    #[test]
    fn ingest_bulk_equals_per_add() {
        let keys = [3i64, 1, 3, 2, 1, 3];
        let vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let mut t = I64SumF64::with_capacity(8);
        t.ingest(&keys, &vals);
        let r = reference(&keys, &vals);
        let m = collect(&t);
        assert_eq!(m, r); // same insertion order per key → bit-identical
    }

    #[test]
    fn negative_and_zero_keys() {
        let keys = [0i64, -5, 0, -5, 9];
        let vals = [1.0, 2.0, 3.0, 4.0, 5.0];
        let mut t = I64SumF64::with_capacity(8);
        t.ingest(&keys, &vals);
        let m = collect(&t);
        assert_eq!(m[&0], 4.0);
        assert_eq!(m[&-5], 6.0);
        assert_eq!(m[&9], 5.0);
    }

    #[test]
    fn handles_min_sentinel_as_real_key() {
        // i64::MIN is the empty sentinel internally — must still work as data.
        let mut t = I64SumF64::with_capacity(4);
        t.add(i64::MIN, 1.0);
        t.add(5, 2.0);
        t.add(i64::MIN, 3.0);
        let m = collect(&t);
        assert_eq!(t.len(), 2);
        assert_eq!(m[&i64::MIN], 4.0);
        assert_eq!(m[&5], 2.0);
    }

    #[test]
    fn grows_past_initial_capacity_without_loss() {
        // Insert far more distinct keys than the initial cap forces grow().
        let n = 5000i64;
        let mut t = I64SumF64::with_capacity(2); // tiny → must grow repeatedly
        for k in 0..n {
            t.add(k, k as f64);
            t.add(k, 1.0);
        }
        assert_eq!(t.len(), n as usize);
        let m = collect(&t);
        for k in 0..n {
            assert_eq!(m[&k], k as f64 + 1.0, "key {k} lost on grow");
        }
    }

    #[test]
    fn combine_merges_disjoint_and_overlapping() {
        let mut a = I64SumF64::with_capacity(8);
        a.ingest(&[1, 2, 3], &[1.0, 2.0, 3.0]);
        let mut b = I64SumF64::with_capacity(8);
        b.ingest(&[2, 3, 4], &[20.0, 30.0, 40.0]);
        b.add(i64::MIN, 7.0); // exercise sentinel-key merge
        a.combine(&b);
        let m = collect(&a);
        assert_eq!(a.len(), 5); // {1,2,3,4,MIN}
        assert_eq!(m[&1], 1.0);
        assert_eq!(m[&2], 22.0);
        assert_eq!(m[&3], 33.0);
        assert_eq!(m[&4], 40.0);
        assert_eq!(m[&i64::MIN], 7.0);
    }

    #[test]
    fn drain_into_emits_all_groups() {
        let mut t = I64SumF64::with_capacity(8);
        t.ingest(&[10, 20, 10], &[1.0, 2.0, 3.0]);
        let (mut ks, mut vs) = (Vec::new(), Vec::new());
        t.drain_into(&mut ks, &mut vs);
        assert_eq!(ks.len(), 2);
        let m: HashMap<i64, f64> = ks.iter().copied().zip(vs.iter().copied()).collect();
        assert_eq!(m[&10], 4.0);
        assert_eq!(m[&20], 2.0);
    }

    #[test]
    fn matches_hashmap_on_pseudo_random_load() {
        // Deterministic LCG — no Date/rand; ~3k distinct keys, collisions.
        let mut keys = Vec::new();
        let mut vals = Vec::new();
        let mut s: u64 = 0x1234_5678;
        for _ in 0..50_000 {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            keys.push((s >> 40) as i64 % 3000 - 1500); // negatives too
            vals.push(((s >> 20) & 0xffff) as f64 * 0.5);
        }
        let mut t = I64SumF64::with_capacity(100);
        t.ingest(&keys, &vals);
        let r = reference(&keys, &vals);
        let m = collect(&t);
        assert_eq!(m.len(), r.len());
        for (k, v) in &r {
            // same per-key insertion order → exact match
            assert_eq!(m[k], *v, "key {k}");
        }
    }

    #[test]
    fn combine_chain_equals_single_table() {
        // Splitting input across N tables + combine == one table (values
        // may differ in last ULPs vs single-table; assert approx).
        let keys: Vec<i64> = (0..9000).map(|i| (i * 7) % 700).collect();
        let vals: Vec<f64> = (0..9000).map(|i| (i as f64) * 0.001).collect();
        let mut single = I64SumF64::with_capacity(700);
        single.ingest(&keys, &vals);

        let mut combined = I64SumF64::with_capacity(700);
        for chunk_k in keys.chunks(1000).zip(vals.chunks(1000)) {
            let mut local = I64SumF64::with_capacity(700);
            local.ingest(chunk_k.0, chunk_k.1);
            combined.combine(&local);
        }
        assert_eq!(single.len(), combined.len());
        let (ms, mc) = (collect(&single), collect(&combined));
        for (k, v) in &ms {
            assert!((mc[k] - v).abs() < 1e-6, "key {k}: {} vs {}", mc[k], v);
        }
    }
}
