//! HJ.4 Phase-0 de-risk — does a SIMD-tag (salted) probe beat the current
//! RobinHood join probe in Q08's REAL regime (batched, ~99.3% miss)?
//!
//! Context: HJ.3 found the EmatixHashJoinExec probe ~parity with DuckDB and
//! its 2.36× microbench an ARTIFACT (one cache-hot contiguous 60M probe). The
//! un-tested lever was a 1-byte SIMD tag (DuckDB/SwissTable salt) to reject the
//! 99.3% misses from L1 before touching the 16-byte bucket. This de-risk ports
//! the proven Σ.Q.L12 tag machinery (robin_hood_agg.rs) to a JOIN probe and
//! measures it against the real `RobinHoodHashJoinI64Table::probe_batch` in the
//! faithful regime: 8192-row batches, 0.67% hit, unique build keys (Q08 part).
//!
//! GATE: tag probe must be meaningfully faster (≥~1.3×) on the miss-dominated
//! probe to justify wiring it into the operator. Else NO-GO (kernel ≈ parity,
//! consistent with HJ.3 — the Q08 gap is row VOLUME, not per-row probe speed).
//!
//! Usage:  cargo run --release -p ematix-flow-hash-join --example hj4_salt_derisk

use std::hint::black_box;
use std::time::Instant;

use ematix_flow_hash_join::{ProbeMatch, RobinHoodHashJoinI64Table};

// ---- proven Σ.Q.L12 tag machinery (ported; identical to robin_hood_agg.rs) ----

const TAG_EMPTY: u8 = 0xFF;
const GROUP: usize = 16;

#[inline]
fn hash_i64(v: i64) -> usize {
    // splitmix64 — identical to the kernel's hash so both tables hash the same.
    let mut x = v as u64;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (x ^ (x >> 31)) as usize
}

#[inline(always)]
fn tag_from_hash(h: usize) -> u8 {
    ((h >> 57) as u8) & 0x7F // top 7 bits; never collides with TAG_EMPTY (0xFF)
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

/// SwissTable-style i64-key → u32 build-row-idx table (unique keys), tag-probed.
/// Tail-mirror invariant: buffers sized `cap + GROUP`; slots < GROUP mirror at
/// `cap + slot` so a 16-wide load at any slot < cap is safe and wraps cleanly.
struct TaggedI64U32 {
    tags: Vec<u8>,
    keys: Vec<i64>,
    idx: Vec<u32>,
    mask: usize,
    len: usize,
}

impl TaggedI64U32 {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 10 / 7).max(64).next_power_of_two();
        Self {
            tags: vec![TAG_EMPTY; cap + GROUP],
            keys: vec![0i64; cap + GROUP],
            idx: vec![0u32; cap + GROUP],
            mask: cap - 1,
            len: 0,
        }
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

    /// Insert a UNIQUE key (de-risk build keys are distinct by construction).
    fn insert(&mut self, key: i64, ix: u32) {
        let h = hash_i64(key);
        let tag = tag_from_hash(h);
        let cap = self.mask + 1;
        let mut slot = h & self.mask;
        loop {
            let em = match_byte_mask(&self.tags, slot, TAG_EMPTY);
            if em != 0 {
                let cand = slot + em.trailing_zeros() as usize;
                let canonical = if cand >= cap { cand - cap } else { cand };
                self.write_slot(canonical, tag, key, ix);
                self.len += 1;
                return;
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
            // Empty tag in this group → key absent (insert would have stopped here).
            if match_byte_mask(&self.tags, slot, TAG_EMPTY) != 0 {
                return None;
            }
            slot = (slot + GROUP) & self.mask;
        }
    }

    fn probe_batch(&self, keys: &[i64], base: u32, out: &mut Vec<ProbeMatch>) {
        for (i, &k) in keys.iter().enumerate() {
            if let Some(bi) = self.probe_one(k) {
                out.push(ProbeMatch {
                    probe_row_idx: base + i as u32,
                    build_row_idx: bi,
                });
            }
        }
    }
}

// ---- counter-based PRNG (no rand dep; deterministic) ----
#[inline]
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

const BATCH: usize = 8192;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let n_probe: usize = std::env::var("HJ4_PROBE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40_000_000);
    let hit_rate: f64 = 0.0067; // Q08 part⋈lineitem ≈ 0.67% survive (REV.20)
    let reps = 5;

    println!(
        "HJ.4 salt de-risk — n_probe={n_probe} hit_rate={hit_rate} batch={BATCH} (single-thread ns/probe)\n"
    );

    for &n_build in &[13_450usize, 200_000, 2_000_000] {
        // Build keys: DISTINCT EVEN i64 (dedup so RH's chained table and the
        // unique-key tag table emit identical counts). Miss keys are ODD → ∉ build.
        let mut st = 0xD1CE_5EEDu64 ^ (n_build as u64);
        let mut seen = std::collections::HashSet::with_capacity(n_build);
        let mut build_keys: Vec<i64> = Vec::with_capacity(n_build);
        while build_keys.len() < n_build {
            let k = ((splitmix(&mut st) >> 1) as i64) << 1;
            if seen.insert(k) {
                build_keys.push(k);
            }
        }

        // Probe stream: hit_rate draw a real build key, else a fresh ODD miss.
        let mut probe_keys: Vec<i64> = Vec::with_capacity(n_probe);
        let mut hits_expected = 0usize;
        let mut ps = 0x1234_5678_9abc_def0u64 ^ (n_build as u64);
        for _ in 0..n_probe {
            let r = splitmix(&mut ps);
            if (r & 0xFFFF) as f64 / 65536.0 < hit_rate {
                let bi = (splitmix(&mut ps) as usize) % n_build;
                probe_keys.push(build_keys[bi]);
                hits_expected += 1;
            } else {
                probe_keys.push((((splitmix(&mut ps) >> 1) as i64) << 1) | 1); // odd → miss
            }
        }

        // Build both tables (untimed — same one-time cost for both).
        let mut rh = RobinHoodHashJoinI64Table::with_capacity(n_build);
        rh.insert_batch(&build_keys, None, 0);
        let mut tg = TaggedI64U32::with_capacity(n_build);
        for (i, &k) in build_keys.iter().enumerate() {
            tg.insert(k, i as u32);
        }
        assert_eq!(tg.len, n_build, "tagged build lost keys");

        // Correctness: both probes must emit the SAME match count.
        let mut a = Vec::with_capacity(hits_expected + BATCH);
        let mut b = Vec::with_capacity(hits_expected + BATCH);
        for (bi, chunk) in probe_keys.chunks(BATCH).enumerate() {
            rh.probe_batch(chunk, None, (bi * BATCH) as u32, &mut a);
            tg.probe_batch(chunk, (bi * BATCH) as u32, &mut b);
        }
        assert_eq!(
            a.len(),
            b.len(),
            "match-count mismatch RH={} TAG={} (n_build={n_build})",
            a.len(),
            b.len()
        );
        let matches = a.len();

        // Time each probe (median of `reps`, 2 warmups); `out` pre-reserved.
        println!(
            "  n_build={n_build:>9}  matches={matches}  (miss {:.1}%)",
            (1.0 - matches as f64 / n_probe as f64) * 100.0
        );
        let mut out = Vec::with_capacity(matches + BATCH);
        let rh_ns = run_timed(
            "RobinHood",
            |o| {
                for (bi, chunk) in probe_keys.chunks(BATCH).enumerate() {
                    rh.probe_batch(chunk, None, (bi * BATCH) as u32, o);
                }
            },
            n_probe,
            reps,
            &mut out,
        );
        let tg_ns = run_timed(
            "Tag(SwissT)",
            |o| {
                for (bi, chunk) in probe_keys.chunks(BATCH).enumerate() {
                    tg.probe_batch(chunk, (bi * BATCH) as u32, o);
                }
            },
            n_probe,
            reps,
            &mut out,
        );
        let speedup = rh_ns / tg_ns;
        let verdict = if speedup >= 1.3 {
            "GO  (≥1.3× — wire into operator)"
        } else if speedup >= 1.05 {
            "MARGINAL"
        } else {
            "NO-GO (≈ parity → HJ.3 stands)"
        };
        println!("    speedup (RH/Tag) = {speedup:.2}×   {verdict}\n");
    }
}

fn run_timed(
    label: &str,
    mut probe: impl FnMut(&mut Vec<ProbeMatch>),
    n_probe: usize,
    reps: usize,
    out: &mut Vec<ProbeMatch>,
) -> f64 {
    for _ in 0..2 {
        out.clear();
        probe(out);
        black_box(out.len());
    }
    let mut times = Vec::with_capacity(reps);
    for _ in 0..reps {
        out.clear();
        let t = Instant::now();
        probe(out);
        times.push(t.elapsed().as_secs_f64());
        black_box(out.len());
    }
    let med = median(times);
    let ns = med / n_probe as f64 * 1e9;
    println!("    {label:<14} {:.1} ms   {ns:.2} ns/probe", med * 1e3);
    ns
}
