//! L9.HashSet — kernel-level A/B before building the integration.
//!
//! Q17 SF=10 profile shows `BloomFilter::might_contain_hash` at
//! 13.4% self-time on a workload where the build side is ~2K keys
//! and the probe side is ~60M lineitem rows. This bench measures
//! the per-call cost of:
//!   - `BloomFilter::might_contain_i64` (current L9 path)
//!   - `std::collections::HashSet<i64>::contains` (proposed L9 path
//!     when build is small)
//!   - `ahash::AHashSet<i64>::contains` (faster hasher, single hash)
//!
//! Decision rule (proceed-to-integration gate):
//! - HashSet beats Bloom by ≥3× on Q17-shape miss-heavy workload
//! - HashSet is at most 2× slower on a hit-heavy workload
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example bench_bloom_vs_hashset

use ematix_flow_core::bloom::BloomFilter;
use std::collections::HashSet;
use std::time::Instant;

fn main() {
    // Q17-shape: ~2K matching keys out of ~60M probe rows.
    let n_keys = 2_044usize;
    let n_probes = 1_000_000usize;

    // Build keys: i64 partkeys roughly in the 2K..2M range like
    // filtered Brand#23 + MED BOX parts.
    let mut keys: Vec<i64> = (0..n_keys as i64)
        .map(|i| (i * 977) % 2_000_000 + 1)
        .collect();
    keys.sort_unstable();

    // Probe set: mostly miss, ~0.1% hit (matches Q17 selectivity).
    let mut probes: Vec<i64> = (0..n_probes as i64).map(|i| (i * 31) % 2_000_000 + 1).collect();
    // Sprinkle in some real hits to keep the early-out path honest.
    for i in (0..n_probes).step_by(1000) {
        probes[i] = keys[i % n_keys];
    }

    println!(
        "Q17-shape A/B: build={n_keys} keys, probes={n_probes} rows, ~0.1% hit rate\n"
    );

    // ---- BloomFilter (current L9 path) ----
    let mut bloom = BloomFilter::for_keys(n_keys);
    for k in &keys {
        bloom.insert_i64(*k);
    }
    // Warmup
    let mut hits = 0usize;
    for &p in &probes {
        if bloom.might_contain_i64(p) {
            hits += 1;
        }
    }
    // Timed
    let t0 = Instant::now();
    let mut bloom_hits = 0usize;
    for &p in &probes {
        if bloom.might_contain_i64(p) {
            bloom_hits += 1;
        }
    }
    let bloom_ns = t0.elapsed().as_nanos() as f64 / n_probes as f64;
    println!(
        "  BloomFilter::might_contain_i64   {bloom_ns:6.1} ns/probe   hits={bloom_hits}   warmup_hits={hits}"
    );

    // ---- HashSet<i64> with std default hasher (SipHash) ----
    let mut set_std: HashSet<i64> = HashSet::with_capacity(n_keys * 2);
    for k in &keys {
        set_std.insert(*k);
    }
    // Warmup
    let mut hits = 0usize;
    for &p in &probes {
        if set_std.contains(&p) {
            hits += 1;
        }
    }
    // Timed
    let t0 = Instant::now();
    let mut set_hits = 0usize;
    for &p in &probes {
        if set_std.contains(&p) {
            set_hits += 1;
        }
    }
    let set_ns = t0.elapsed().as_nanos() as f64 / n_probes as f64;
    println!(
        "  std HashSet (SipHash)            {set_ns:6.1} ns/probe   hits={set_hits}   warmup_hits={hits}"
    );

    // ---- Tiny manual i64 hash set: bucket array, no hasher branch ----
    // Power-of-2 bucket count + ahash-style multiply-shift for i64.
    // Open addressing with linear probe. Designed for the EXACT use
    // case: small i64 set, miss-dominant probe.
    let cap = (n_keys * 2).next_power_of_two().max(64);
    let mut buckets: Vec<i64> = vec![i64::MIN; cap];
    let mask = cap - 1;
    const MULT: u64 = 0x9e37_79b9_7f4a_7c15;
    let h64 = |v: i64| -> usize { ((v as u64).wrapping_mul(MULT) >> 32) as usize & mask };
    for &k in &keys {
        let mut s = h64(k);
        loop {
            if buckets[s] == i64::MIN {
                buckets[s] = k;
                break;
            }
            if buckets[s] == k {
                break;
            }
            s = (s + 1) & mask;
        }
    }
    let contains = |v: i64| -> bool {
        let mut s = h64(v);
        loop {
            let b = buckets[s];
            if b == v {
                return true;
            }
            if b == i64::MIN {
                return false;
            }
            s = (s + 1) & mask;
        }
    };
    // Warmup
    let mut hits = 0usize;
    for &p in &probes {
        if contains(p) {
            hits += 1;
        }
    }
    // Timed
    let t0 = Instant::now();
    let mut tiny_hits = 0usize;
    for &p in &probes {
        if contains(p) {
            tiny_hits += 1;
        }
    }
    let tiny_ns = t0.elapsed().as_nanos() as f64 / n_probes as f64;
    println!(
        "  manual i64 open-addr table       {tiny_ns:6.1} ns/probe   hits={tiny_hits}   warmup_hits={hits}"
    );

    println!();
    println!(
        "  Bloom vs std HashSet   speedup = {:.2}x",
        bloom_ns / set_ns
    );
    println!(
        "  Bloom vs tiny i64 set  speedup = {:.2}x",
        bloom_ns / tiny_ns
    );

    // Sanity: every hit found by the bloom should also be found by
    // the exact sets (modulo bloom false positives going the OTHER
    // direction). std and tiny should agree exactly.
    if set_hits != tiny_hits {
        panic!("std vs tiny disagree: {set_hits} vs {tiny_hits}");
    }
    println!(
        "\n  bloom false-positive rate ≈ {:.2}%  (extra hits over exact: {})",
        100.0 * (bloom_hits - set_hits) as f64 / n_probes as f64,
        bloom_hits.saturating_sub(set_hits)
    );
}
