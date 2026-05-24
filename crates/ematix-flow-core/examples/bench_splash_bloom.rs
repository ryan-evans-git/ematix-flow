//! Lever 5 kernel A/B — current sequential-multiply-shift bloom vs
//! Apache Impala "splash" 8-lane parallel bloom.
//!
//! Decision gate: splash bloom is ≥2× faster than the current bloom
//! at Q05-shape workload (mix of misses + hits) AND no slower on
//! miss-dominant workloads → proceed to wire it in.

use ematix_flow_core::bloom::BloomFilter;
use std::time::Instant;

// Apache Impala "splash" bloom layout — 256-bit block, 8 × u32 lanes,
// 8 independent salted hashes per probe.
const SPLASH_SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
];

struct SplashBloom {
    blocks: Vec<[u32; 8]>,
    mask: usize,
    seed: u64,
}

impl SplashBloom {
    fn with_keys(n_keys: usize) -> Self {
        // Same bits-per-key target as the current BloomFilter (10).
        let total_bits = (n_keys.max(1) * 10).max(256);
        let n_blocks = (total_bits.div_ceil(256)).next_power_of_two();
        Self {
            blocks: vec![[0u32; 8]; n_blocks],
            mask: n_blocks - 1,
            seed: 0xc0ffee_u64,
        }
    }

    #[inline(always)]
    fn block_idx(&self, h: u64) -> usize {
        (h as usize) & self.mask
    }

    #[inline(always)]
    fn lane_masks(h: u64) -> [u32; 8] {
        let h32 = h as u32;
        let mut m = [0u32; 8];
        for i in 0..8 {
            // (h * SALT[i]) >> 27 → 5-bit lane-bit position (0..31)
            let bit = (h32.wrapping_mul(SPLASH_SALT[i]) >> 27) & 0x1f;
            m[i] = 1u32 << bit;
        }
        m
    }

    #[inline(always)]
    fn hash_i64(&self, v: i64) -> u64 {
        // splitmix64 — same as current bloom.
        let mut x = (v as u64).wrapping_add(self.seed);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        x ^ (x >> 31)
    }

    #[inline(always)]
    fn insert_i64(&mut self, v: i64) {
        let h = self.hash_i64(v);
        let idx = self.block_idx(h);
        let block = &mut self.blocks[idx];
        let m = Self::lane_masks(h);
        for i in 0..8 {
            block[i] |= m[i];
        }
    }

    /// Splash-style probe: load block, build needed-mask, AND, compare.
    /// All 8 lanes tested in parallel; no early-out → branch-free,
    /// autovectorisable by LLVM.
    #[inline(always)]
    fn might_contain_i64(&self, v: i64) -> bool {
        let h = self.hash_i64(v);
        let block = &self.blocks[self.block_idx(h)];
        let m = Self::lane_masks(h);
        // (block & m) == m  ⟺  every needed bit is set
        // Combine across lanes via OR of "differences": if any lane
        // differs, result is non-zero → miss.
        let mut diff: u32 = 0;
        for i in 0..8 {
            diff |= m[i] & !block[i];
        }
        diff == 0
    }
}

fn time_loop(label: &str, probe: impl Fn(i64) -> bool, probes: &[i64]) -> (f64, usize) {
    // Warmup
    let mut hits = 0usize;
    for &p in probes {
        if probe(p) {
            hits += 1;
        }
    }
    // Timed
    let t0 = Instant::now();
    let mut hits_timed = 0usize;
    for &p in probes {
        if probe(p) {
            hits_timed += 1;
        }
    }
    let ns_per = t0.elapsed().as_nanos() as f64 / probes.len() as f64;
    println!(
        "  {label:<40} {ns_per:6.2} ns/probe   hits={hits_timed} (warmup={hits})"
    );
    (ns_per, hits_timed)
}

fn run_shape(label: &str, n_keys: i64, n_probes: usize, hit_rate: f64) {
    // Build keys uniformly across an i64 range.
    let keys: Vec<i64> = (0..n_keys).map(|i| (i * 977) % 2_000_000 + 1).collect();

    // Probes: mix hits and misses by hit_rate.
    let n_hits = (n_probes as f64 * hit_rate) as usize;
    let mut probes: Vec<i64> = (0..n_probes as i64)
        .map(|i| (i * 31) % 2_000_000 + 2_000_000)
        .collect();
    for i in 0..n_hits {
        probes[i] = keys[i % keys.len()];
    }
    probes.sort();

    println!(
        "\n[{label}]  keys={} probes={n_probes} hit_rate={:.0}%",
        keys.len(),
        hit_rate * 100.0
    );

    // Build current BloomFilter
    let mut bloom = BloomFilter::for_keys(keys.len());
    for k in &keys {
        bloom.insert_i64(*k);
    }

    // Build splash bloom
    let mut splash = SplashBloom::with_keys(keys.len());
    for k in &keys {
        splash.insert_i64(*k);
    }

    let (bloom_ns, bloom_hits) =
        time_loop("BloomFilter (current)", |v| bloom.might_contain_i64(v), &probes);
    let (splash_ns, splash_hits) =
        time_loop("SplashBloom (Impala-style)", |v| splash.might_contain_i64(v), &probes);

    println!(
        "  speedup: {:.2}x   FP delta: bloom={} splash={}",
        bloom_ns / splash_ns,
        bloom_hits as i64 - n_hits as i64,
        splash_hits as i64 - n_hits as i64,
    );
}

fn main() {
    println!("Lever 5 — Apache Impala splash bloom A/B");

    // Q17-shape: small build, miss-dominant
    run_shape("Q17-shape (small build, ~0.1% hit)", 2_044, 1_000_000, 0.001);

    // Q05 o⋈l-shape: medium build, ~15% hit
    run_shape("Q05-o⋈l-shape (2.3M build, ~15% hit)", 2_300_000, 1_000_000, 0.15);

    // FK-shape (Q05 c⋈o, Q18 customer⋈orders): ~100% hit
    run_shape("FK-shape (1.5M build, ~100% hit)", 1_500_000, 1_000_000, 1.0);

    // High-selectivity (Q21-like): tiny build, miss-dominant
    run_shape("Q21-shape (~500 keys, ~0.05% hit)", 500, 1_000_000, 0.0005);
}
