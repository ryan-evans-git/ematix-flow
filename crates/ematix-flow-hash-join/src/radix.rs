//! RADIX.1 — per-partition radix/morsel hash join over [`TaggedJoinI64U32`].
//!
//! Phase-0 (`crates/ematix-flow-core/examples/radix_join_derisk.rs`) proved a
//! per-partition radix build+probe beats BOTH a single shared build AND the
//! stock 14-way `RepartitionExec` shuffle on every TPC-H join shape (−25..−51%
//! vs the shuffle, neutral-to-better on small builds, no regression). The
//! mechanism: one 90–128 MB build (or a 14-way split → ~6 MB sub-tables) misses
//! L2 on every probe; `N = 2^radix_bits` sub-tables of a few KB each stay
//! cache-resident, so the probe runs at L1/L2 latency once each partition is
//! processed as a contiguous burst.
//!
//! This kernel is the reusable substrate: it scatters both build and probe keys
//! by an INDEPENDENT hash into N partitions, builds one [`TaggedJoinI64U32`] per
//! partition, and probes each partition's keys against only its own sub-table.
//!
//! **Why an independent partition hash.** [`TaggedJoinI64U32`] derives its slot
//! from the low bits of a splitmix64 hash and its 7-bit SIMD tag from the top
//! bits of that same hash. If we partitioned by the top bits of *that* hash,
//! every key in a partition would share its tag → the SIMD tag filter would
//! never reject, collapsing to a linear scan. So [`radix_of`] uses a separate
//! murmur3 `fmix64`, statistically independent of the sub-table's slot/tag bits.
//!
//! **Unique-key only** (mirrors [`TaggedJoinI64U32`]): [`RadixTaggedJoin::try_build`]
//! returns `None` if any partition sees a duplicate, so the Arrow bridge falls
//! back to the chained [`crate::RobinHoodHashJoinI64Table`] for non-unique builds.

use crate::table::ProbeMatch;
use crate::tagged::TaggedJoinI64U32;

/// Independent partition hash (murmur3 `fmix64`). Deliberately NOT the
/// splitmix64 that [`TaggedJoinI64U32`] uses for slot/tag, so the radix
/// partition bits are statistically independent of the in-sub-table bits.
/// Returns a partition id in `[0, 2^bits)` from the top `bits` of the mix.
#[inline]
fn radix_of(key: i64, bits: u32) -> usize {
    if bits == 0 {
        return 0; // single partition; avoid the `>> 64` UB below
    }
    let mut x = key as u64;
    x = (x ^ (x >> 33)).wrapping_mul(0xff51_afd7_ed55_8ccd);
    x = (x ^ (x >> 33)).wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    (x >> (64 - bits)) as usize
}

/// `N = 2^radix_bits` cache-resident [`TaggedJoinI64U32`] sub-tables + the radix
/// scatter that routes a key to its partition. Build-partition `p` and
/// probe-partition `p` use bit-identical [`radix_of`], so a key that built into
/// sub-table `p` is probed only against sub-table `p`.
pub struct RadixTaggedJoin {
    subtables: Vec<TaggedJoinI64U32>,
    radix_bits: u32,
}

impl RadixTaggedJoin {
    /// Build `N = 2^radix_bits` sub-tables. Scatters `(key, global_row_idx)` into
    /// partitions by [`radix_of`], then builds one [`TaggedJoinI64U32`] per
    /// partition (sized to its actual occupancy — partitioning re-slices the key
    /// table, it does not duplicate it). NULL build keys are skipped (Inner
    /// semantics). Returns `None` the moment any partition sees a duplicate key
    /// (non-unique build → caller falls back to the chained table).
    pub fn try_build(keys: &[i64], nulls: Option<&[bool]>, radix_bits: u32) -> Option<Self> {
        let n_part = 1usize << radix_bits;
        let est = keys.len() / n_part + 1;
        let mut pk: Vec<Vec<i64>> = (0..n_part).map(|_| Vec::with_capacity(est)).collect();
        let mut pi: Vec<Vec<u32>> = (0..n_part).map(|_| Vec::with_capacity(est)).collect();
        for (i, &k) in keys.iter().enumerate() {
            if let Some(m) = nulls {
                if !m[i] {
                    continue; // NULL build key never matches
                }
            }
            let p = radix_of(k, radix_bits);
            pk[p].push(k);
            pi[p].push(i as u32); // GLOBAL build-row index, preserved across the scatter
        }
        let mut subtables = Vec::with_capacity(n_part);
        for p in 0..n_part {
            subtables.push(TaggedJoinI64U32::try_build_pairs(&pk[p], &pi[p])?);
        }
        Some(Self {
            subtables,
            radix_bits,
        })
    }

    /// Total distinct build keys across all sub-tables.
    pub fn len(&self) -> usize {
        self.subtables.iter().map(|t| t.len()).sum()
    }
    pub fn is_empty(&self) -> bool {
        self.subtables.iter().all(|t| t.is_empty())
    }
    /// `N` = number of radix partitions (`2^radix_bits`).
    pub fn num_partitions(&self) -> usize {
        self.subtables.len()
    }

    /// Partition id for a key (build/probe symmetric). A parallel operator uses
    /// this to scatter probe keys into per-partition buffers, then probes each
    /// buffer against [`Self::subtable`] — the cache-local, contention-free path
    /// that the single-`probe_batch` call also takes internally.
    #[inline]
    pub fn partition_of(&self, key: i64) -> usize {
        radix_of(key, self.radix_bits)
    }

    /// Borrow sub-table `p` for a per-partition probe (drives the parallel probe
    /// in the operator: thread `t` owns a disjoint set of partitions, so no two
    /// threads touch the same sub-table → no cache-line contention).
    pub fn subtable(&self, p: usize) -> &TaggedJoinI64U32 {
        &self.subtables[p]
    }

    /// Probe a batch, **B-scatter** style: scatter the probe keys (carrying each
    /// key's row index) into per-partition buffers, then probe each partition's
    /// keys against only its sub-table — one contiguous cache-resident burst per
    /// partition. Emits one [`ProbeMatch`] per hit. The match **set** is
    /// identical to a single [`TaggedJoinI64U32`] over the same build; only the
    /// emission order differs (grouped by partition), which is irrelevant for an
    /// Inner join feeding an aggregate.
    pub fn probe_batch(
        &self,
        probe_keys: &[i64],
        nulls: Option<&[bool]>,
        probe_row_idx_base: u32,
        out: &mut Vec<ProbeMatch>,
    ) {
        let n_part = self.subtables.len();
        let est = probe_keys.len() / n_part + 1;
        let mut pk: Vec<Vec<i64>> = (0..n_part).map(|_| Vec::with_capacity(est)).collect();
        let mut pi: Vec<Vec<u32>> = (0..n_part).map(|_| Vec::with_capacity(est)).collect();
        for (i, &k) in probe_keys.iter().enumerate() {
            if let Some(m) = nulls {
                if !m[i] {
                    continue;
                }
            }
            let p = radix_of(k, self.radix_bits);
            pk[p].push(k);
            pi[p].push(probe_row_idx_base + i as u32);
        }
        for p in 0..n_part {
            self.subtables[p].probe_pairs(&pk[p], &pi[p], out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn match_set(out: &[ProbeMatch]) -> HashSet<(u32, u32)> {
        out.iter()
            .map(|m| (m.probe_row_idx, m.build_row_idx))
            .collect()
    }

    /// THE core correctness gate: across a range of partition counts, the radix
    /// join's match SET must be byte-identical to a single TaggedJoinI64U32 over
    /// the same build+probe. bits=0 (1 partition) is the degenerate single-table
    /// case; bits=12 (4096 partitions, the Phase-0 winner) exercises deep radix.
    #[test]
    fn radix_match_set_equals_single_table() {
        let build: Vec<i64> = (0..5_000).map(|i| (i as i64) * 3 + 7).collect();
        let probe: Vec<i64> = (0..12_000).map(|i| (i as i64) * 2).collect();

        let single = TaggedJoinI64U32::try_build(&build, None, 0).unwrap();
        let mut single_out = Vec::new();
        single.probe_batch(&probe, None, 0, &mut single_out);
        let expect = match_set(&single_out);
        assert!(!expect.is_empty(), "test data must produce some matches");

        for bits in [0u32, 1, 4, 8, 12] {
            let r = RadixTaggedJoin::try_build(&build, None, bits).unwrap();
            assert_eq!(r.len(), single.len(), "bits={bits}: build key count");
            assert_eq!(
                r.num_partitions(),
                1usize << bits,
                "bits={bits}: N partitions"
            );
            let mut out = Vec::new();
            r.probe_batch(&probe, None, 0, &mut out);
            assert_eq!(out.len(), single_out.len(), "bits={bits}: match count");
            assert_eq!(
                match_set(&out),
                expect,
                "bits={bits}: radix match SET must equal the single-table match set"
            );
        }
    }

    /// Build-partition `p` ⟺ probe-partition `p` symmetry: every build key,
    /// probed back, must be found — proving both sides use bit-identical radix
    /// extraction. Includes negatives + extremes (radix_of operates on the raw
    /// i64 bit pattern). Keys are unique so each maps to its own build row.
    #[test]
    fn build_probe_partition_symmetry() {
        let build: Vec<i64> = vec![1, 1_000_003, -42, i64::MAX, i64::MIN, 17, 999_999_999_999];
        let r = RadixTaggedJoin::try_build(&build, None, 6).unwrap();
        let mut out = Vec::new();
        r.probe_batch(&build, None, 0, &mut out);
        assert_eq!(
            out.len(),
            build.len(),
            "every build key found via its own partition"
        );
        let s = match_set(&out);
        for i in 0..build.len() as u32 {
            assert!(
                s.contains(&(i, i)),
                "build key at row {i} round-trips to itself"
            );
        }
    }

    /// NULL keys skipped on BOTH sides, across the scatter.
    #[test]
    fn null_keys_skipped_across_scatter() {
        let bvalid = [true, false, true, true];
        let r = RadixTaggedJoin::try_build(&[5, 999, 7, 11], Some(&bvalid), 4).unwrap();
        assert_eq!(r.len(), 3, "NULL build row 1 (key 999) skipped");
        let mut out = Vec::new();
        let pvalid = [false, true, true, true]; // probe row 0 (key 5) is NULL
        r.probe_batch(&[5, 999, 7, 11], Some(&pvalid), 0, &mut out);
        let s = match_set(&out);
        assert_eq!(
            out.len(),
            2,
            "row0 NULL-skipped; 999 never built (miss); 7,11 hit"
        );
        assert!(s.contains(&(2, 2)), "probe row2 key7 → build row2");
        assert!(s.contains(&(3, 3)), "probe row3 key11 → build row3");
    }

    /// A duplicate build key (which always lands in the SAME partition) must
    /// surface as `None` so the Arrow bridge falls back to the chained table.
    #[test]
    fn duplicate_build_key_bails() {
        assert!(
            RadixTaggedJoin::try_build(&[10, 20, 10], None, 4).is_none(),
            "duplicate key 10 → not unique → None (caller falls back)"
        );
    }

    /// Scale + the cache-resident-sub-table regime: 200k unique build, 400k
    /// probe (every even probe hits), 4096 partitions.
    #[test]
    fn large_round_trip_deep_radix() {
        let build: Vec<i64> = (0..200_000).map(|i| (i as i64) * 2).collect();
        let probe: Vec<i64> = (0..400_000).map(|i| i as i64).collect();
        let r = RadixTaggedJoin::try_build(&build, None, 12).unwrap();
        assert_eq!(r.len(), 200_000);
        let mut out = Vec::new();
        r.probe_batch(&probe, None, 0, &mut out);
        assert_eq!(
            out.len(),
            200_000,
            "every even probe in [0,400k) hits exactly once"
        );
    }

    /// probe_row_idx_base offset is honored (the operator probes batch-relative
    /// indices that must be shifted to global probe-row space).
    #[test]
    fn probe_row_base_offset_applied() {
        let r = RadixTaggedJoin::try_build(&[100, 200, 300], None, 4).unwrap();
        let mut out = Vec::new();
        r.probe_batch(&[200, 100], None, 1_000, &mut out);
        let s = match_set(&out);
        assert!(
            s.contains(&(1_000, 1)),
            "probe row 1000 (key200) → build row 1"
        );
        assert!(
            s.contains(&(1_001, 0)),
            "probe row 1001 (key100) → build row 0"
        );
    }
}
