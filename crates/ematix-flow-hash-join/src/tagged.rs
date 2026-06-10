//! HJ.4 — SwissTable-style SIMD-tag probe for UNIQUE i64 build keys.
//!
//! Ported from the Σ.Q.L12 agg tag machinery (`robin_hood_agg.rs`). A 1-byte
//! tag array rejects the (Q08) ~99.3% probe misses from L1 with a 16-wide SIMD
//! compare, instead of loading the chained RobinHood bucket from L2/DRAM. The
//! de-risk (`examples/hj4_salt_derisk.rs`) measured 3.0–3.9× over the RobinHood
//! probe in the faithful batched / high-miss regime.
//!
//! **UNIQUE-KEY ONLY:** [`TaggedJoinI64U32::try_build`] returns `None` if the
//! build side has a duplicate key, so the Arrow bridge falls back to the chained
//! [`crate::RobinHoodHashJoinI64Table`] for non-unique builds (general joins).
//! Q08's `part⋈lineitem` build (`p_partkey`, a PK) is unique → Tag fires there.

use crate::table::ProbeMatch;

const TAG_EMPTY: u8 = 0xFF;
const GROUP: usize = 16;

/// splitmix64 — identical to the kernel's `table::hash_i64`.
#[inline]
fn hash_i64(v: i64) -> usize {
    let mut x = v as u64;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (x ^ (x >> 31)) as usize
}

/// Top 7 bits of the hash; high bit clear so a real tag never equals TAG_EMPTY.
#[inline(always)]
fn tag_from_hash(h: usize) -> u8 {
    ((h >> 57) as u8) & 0x7F
}

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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[inline]
unsafe fn sse2_match_byte_mask_at(tags: &[u8], slot: usize, byte: u8) -> u16 {
    use std::arch::x86_64::{
        __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
    };
    let group = unsafe { _mm_loadu_si128(tags.as_ptr().add(slot) as *const __m128i) };
    let cmp = _mm_cmpeq_epi8(group, _mm_set1_epi8(byte as i8));
    _mm_movemask_epi8(cmp) as u16
}

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
        let mut mask = 0u16;
        for i in 0..GROUP {
            if tags[slot + i] == byte {
                mask |= 1 << i;
            }
        }
        mask
    }
}

/// SwissTable-style i64-key → u32 build-row-idx table for UNIQUE keys, probed by
/// a 16-wide SIMD tag compare. SoA layout (tags / keys / idx). Tail-mirror
/// invariant: all three buffers are sized `cap + GROUP` and slots `< GROUP`
/// mirror at `cap + slot`, so a SIMD load at any slot `< cap` is in-bounds and
/// wraps cleanly without a branch.
pub struct TaggedJoinI64U32 {
    tags: Vec<u8>,
    keys: Vec<i64>,
    idx: Vec<u32>,
    mask: usize,
    len: usize,
}

impl TaggedJoinI64U32 {
    /// Build from a key slice (+ optional validity), with `row_idx_base` added
    /// to each emitted build-row index. Returns `None` the moment a duplicate
    /// non-NULL key is seen — this table is unique-key only, and the caller
    /// (Arrow bridge) then falls back to the chained RobinHood table.
    pub fn try_build(keys: &[i64], nulls: Option<&[bool]>, row_idx_base: u32) -> Option<Self> {
        let n = keys.len();
        let cap = (n.saturating_mul(10) / 7).max(64).next_power_of_two();
        let mut t = Self {
            tags: vec![TAG_EMPTY; cap + GROUP],
            keys: vec![0i64; cap + GROUP],
            idx: vec![0u32; cap + GROUP],
            mask: cap - 1,
            len: 0,
        };
        for (i, &k) in keys.iter().enumerate() {
            if let Some(m) = nulls {
                if !m[i] {
                    continue; // NULL build key never matches (Inner semantics)
                }
            }
            if !t.insert_unique(k, row_idx_base + i as u32) {
                return None; // duplicate → not unique → caller falls back
            }
        }
        Some(t)
    }

    /// Distinct build keys.
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    fn write_slot(&mut self, slot: usize, tag: u8, key: i64, ix: u32) {
        let cap = self.mask + 1;
        self.tags[slot] = tag;
        self.keys[slot] = key;
        self.idx[slot] = ix;
        if slot < GROUP {
            self.tags[cap + slot] = tag;
            self.keys[cap + slot] = key;
            self.idx[cap + slot] = ix;
        }
    }

    /// Insert a key, returning `false` if it is already present (duplicate). The
    /// group walk is identical to [`Self::probe_one`], so a key inserted here is
    /// found there.
    fn insert_unique(&mut self, key: i64, ix: u32) -> bool {
        let h = hash_i64(key);
        let tag = tag_from_hash(h);
        let cap = self.mask + 1;
        let mut slot = h & self.mask;
        loop {
            let mut mm = match_byte_mask(&self.tags, slot, tag);
            while mm != 0 {
                let cand = slot + mm.trailing_zeros() as usize;
                if self.keys[cand] == key {
                    return false; // duplicate
                }
                mm &= mm - 1;
            }
            let em = match_byte_mask(&self.tags, slot, TAG_EMPTY);
            if em != 0 {
                let cand = slot + em.trailing_zeros() as usize;
                let canonical = if cand >= cap { cand - cap } else { cand };
                self.write_slot(canonical, tag, key, ix);
                self.len += 1;
                return true;
            }
            slot = (slot + GROUP) & self.mask;
        }
    }

    #[inline(always)]
    fn probe_one(&self, key: i64) -> Option<u32> {
        let h = hash_i64(key);
        let tag = tag_from_hash(h);
        let mut slot = h & self.mask;
        loop {
            let mut mm = match_byte_mask(&self.tags, slot, tag);
            while mm != 0 {
                let cand = slot + mm.trailing_zeros() as usize;
                if self.keys[cand] == key {
                    return Some(self.idx[cand]);
                }
                mm &= mm - 1;
            }
            // An empty tag in this group → key absent (insert stops at first empty).
            if match_byte_mask(&self.tags, slot, TAG_EMPTY) != 0 {
                return None;
            }
            slot = (slot + GROUP) & self.mask;
        }
    }

    /// Probe a batch, emitting one [`ProbeMatch`] per hit (unique keys → ≤1 per
    /// probe row). Signature mirrors [`crate::RobinHoodHashJoinI64Table::probe_batch`].
    pub fn probe_batch(
        &self,
        probe_keys: &[i64],
        nulls: Option<&[bool]>,
        probe_row_idx_base: u32,
        out: &mut Vec<ProbeMatch>,
    ) {
        for (i, &k) in probe_keys.iter().enumerate() {
            if let Some(m) = nulls {
                if !m[i] {
                    continue;
                }
            }
            if let Some(bi) = self.probe_one(k) {
                out.push(ProbeMatch {
                    probe_row_idx: probe_row_idx_base + i as u32,
                    build_row_idx: bi,
                });
            }
        }
    }

    /// Build from explicit `(key, build_row_idx)` pairs. Unlike [`Self::try_build`]
    /// (which derives the build-row index as `row_idx_base + i`), this takes an
    /// arbitrary `idxs[i]` per key — needed by [`crate::radix::RadixTaggedJoin`],
    /// where a radix scatter sends keys to partitions in non-sequential global
    /// order. Callers must pre-filter NULL keys. Returns `None` on a duplicate
    /// (same unique-key contract as `try_build`).
    pub fn try_build_pairs(keys: &[i64], idxs: &[u32]) -> Option<Self> {
        debug_assert_eq!(keys.len(), idxs.len());
        let n = keys.len();
        let cap = (n.saturating_mul(10) / 7).max(64).next_power_of_two();
        let mut t = Self {
            tags: vec![TAG_EMPTY; cap + GROUP],
            keys: vec![0i64; cap + GROUP],
            idx: vec![0u32; cap + GROUP],
            mask: cap - 1,
            len: 0,
        };
        for (&k, &ix) in keys.iter().zip(idxs.iter()) {
            if !t.insert_unique(k, ix) {
                return None; // duplicate → not unique → caller falls back
            }
        }
        Some(t)
    }

    /// Probe with explicit per-key probe-row indices (the radix scatter carries
    /// each probe key's original row index alongside it). NULLs pre-filtered.
    /// One [`ProbeMatch`] per hit.
    pub fn probe_pairs(&self, keys: &[i64], probe_idxs: &[u32], out: &mut Vec<ProbeMatch>) {
        debug_assert_eq!(keys.len(), probe_idxs.len());
        for (&k, &pix) in keys.iter().zip(probe_idxs.iter()) {
            if let Some(bi) = self.probe_one(k) {
                out.push(ProbeMatch {
                    probe_row_idx: pix,
                    build_row_idx: bi,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_build_probe_matches() {
        // keys 10,20,30,40 → rows 0..3. Probe 20,40,50,10 → hits 20(r1),40(r3),10(r0).
        let t = TaggedJoinI64U32::try_build(&[10, 20, 30, 40], None, 0).expect("unique");
        assert_eq!(t.len(), 4);
        let mut out = Vec::new();
        t.probe_batch(&[20, 40, 50, 10], None, 0, &mut out);
        assert_eq!(out.len(), 3, "20,40,10 hit; 50 misses");
        // probe row 0 (key 20) → build row 1; row 1 (key 40) → build row 3; row 3 (key 10) → 0
        assert_eq!(
            out[0],
            ProbeMatch {
                probe_row_idx: 0,
                build_row_idx: 1
            }
        );
        assert_eq!(
            out[1],
            ProbeMatch {
                probe_row_idx: 1,
                build_row_idx: 3
            }
        );
        assert_eq!(
            out[2],
            ProbeMatch {
                probe_row_idx: 3,
                build_row_idx: 0
            }
        );
    }

    #[test]
    fn duplicate_build_key_bails() {
        assert!(
            TaggedJoinI64U32::try_build(&[10, 20, 10], None, 0).is_none(),
            "duplicate key 10 must make try_build return None (caller falls back)"
        );
    }

    #[test]
    fn null_build_keys_skipped() {
        // key index 1 is NULL → skipped; 5,7 inserted at rows 0,2.
        let valid = [true, false, true];
        let t = TaggedJoinI64U32::try_build(&[5, 999, 7], Some(&valid), 0).expect("unique");
        assert_eq!(t.len(), 2);
        let mut out = Vec::new();
        t.probe_batch(&[999, 7, 5], None, 0, &mut out);
        // 999 was never inserted (it was the NULL row) → miss; 7→row2, 5→row0.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].build_row_idx, 2);
        assert_eq!(out[1].build_row_idx, 0);
    }

    #[test]
    fn large_unique_round_trips() {
        let keys: Vec<i64> = (0..50_000).map(|i| (i as i64) * 2).collect();
        let t = TaggedJoinI64U32::try_build(&keys, None, 0).expect("unique");
        assert_eq!(t.len(), 50_000);
        let mut out = Vec::new();
        // even keys hit, odd keys miss
        let probes: Vec<i64> = (0..100_000).map(|i| i as i64).collect();
        t.probe_batch(&probes, None, 0, &mut out);
        assert_eq!(out.len(), 50_000, "every even probe in [0,100k) hits");
    }
}
