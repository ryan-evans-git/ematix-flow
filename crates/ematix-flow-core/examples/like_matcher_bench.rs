//! Σ.E5 (2026-05-19): bench Photon-style `LikeMatcher` against the
//! baseline `<[u8]>::windows().find()` on a TPC-H-shaped corpus.
//!
//! Patterns mirror what TPC-H queries actually use:
//!   * Q13: `%special%requests%`        — two-substring contains
//!   * Q16: `MEDIUM POLISHED%`          — prefix-anchored
//!   * Q14: `PROMO%`                    — prefix-anchored
//!
//! Generates a 1.5M-row synthetic corpus of ~30-byte ASCII strings
//! (orders.o_comment-shaped) and counts matches across the corpus.

use ematix_flow_core::like_matcher::LikeMatcher;
use std::time::Instant;

fn rng_string(seed: u64, idx: u64, target_len: usize) -> Vec<u8> {
    // xorshift-ish; ASCII printable only.
    let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(idx);
    let mut out = Vec::with_capacity(target_len);
    for _ in 0..target_len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let c = b'a' + ((state >> 24) as u8 % 26);
        out.push(c);
    }
    // Inject "special" or "requests" into ~5% of strings to give the
    // contains pattern non-trivial work.
    if idx % 20 == 0 {
        let pos = (idx as usize) % (target_len.saturating_sub(8));
        out.splice(pos..pos + 7, b"special".iter().copied());
    }
    if idx % 25 == 7 {
        let pos = (idx as usize + 5) % (target_len.saturating_sub(9));
        out.splice(pos..pos + 8, b"requests".iter().copied());
    }
    out
}

fn baseline_contains(haystack: &[u8], needles: &[&[u8]]) -> bool {
    // Multi-substring contains using std .windows().position().
    let mut cur = 0usize;
    for n in needles {
        match haystack[cur..].windows(n.len()).position(|w| w == *n) {
            Some(p) => cur += p + n.len(),
            None => return false,
        }
    }
    true
}

fn main() {
    let n_rows = 1_500_000usize;
    let avg_len = 30usize;
    println!("Generating {n_rows} rows × ~{avg_len} bytes…");
    let corpus: Vec<Vec<u8>> = (0..n_rows as u64)
        .map(|i| rng_string(42, i, avg_len))
        .collect();
    println!(
        "Total bytes: {}",
        corpus.iter().map(|v| v.len()).sum::<usize>()
    );
    println!();

    let trials = 5;

    for &pattern in &["%special%requests%", "MEDIUM POLISHED%", "PROMO%", "%lazy%"] {
        let matcher = LikeMatcher::compile(pattern).expect("compile");

        // Photon-style timing.
        let mut times = Vec::new();
        let mut hits = 0usize;
        for _ in 0..trials {
            let t = Instant::now();
            let mut h = 0usize;
            for s in &corpus {
                if matcher.matches(s) {
                    h += 1;
                }
            }
            times.push(t.elapsed().as_secs_f64() * 1000.0);
            hits = h;
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median_simd = times[times.len() / 2];

        // Baseline timing (only for substring-style patterns; prefix
        // patterns get a simpler equivalent for fairness).
        let needles: Vec<&[u8]> = match pattern {
            "%special%requests%" => vec![b"special", b"requests"],
            "%lazy%" => vec![b"lazy"],
            "MEDIUM POLISHED%" => vec![b"MEDIUM POLISHED"],
            "PROMO%" => vec![b"PROMO"],
            _ => vec![],
        };
        let mut bl_times = Vec::new();
        for _ in 0..trials {
            let t = Instant::now();
            for s in &corpus {
                let _ = if pattern.starts_with("%") && pattern.ends_with("%") {
                    baseline_contains(s, &needles)
                } else if pattern.ends_with("%") {
                    s.starts_with(needles[0])
                } else {
                    s.ends_with(needles[0])
                };
            }
            bl_times.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        bl_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median_bl = bl_times[bl_times.len() / 2];

        println!(
            "pattern={pattern:>22}  hits={hits:>7}  simd={median_simd:>6.2}ms  baseline={median_bl:>6.2}ms  speedup={:.2}×",
            median_bl / median_simd
        );
    }
}
