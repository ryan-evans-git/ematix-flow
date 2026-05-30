//! REV.8 GATE — does single-pass radix aggregation beat DataFusion's
//! two-phase (Partial → hash-shuffle → Final) at the Q18 SF=100 shape?
//!
//! This is the go/no-go for the `SinglePassRadixSumF64Exec` operator
//! (docs/PHASE_REV8_SINGLE_PASS_RADIX_AGG.md). The win is NOT self-evident
//! — DataFusion's two-phase is already a hash-shuffle — and it is a
//! *parallel-coordination* question that the single-threaded REV.5.b
//! microbench cannot answer. So we model P parallel partitions with real
//! threads:
//!
//!   two_phase   : P× local Partial hash-agg  →  hash-shuffle partial
//!                 groups into P buckets  →  P× Final hash-agg.
//!   single_pass : P× radix-scatter RAW rows into B bins (local)  →
//!                 barrier  →  parallel per-bin aggregate-once.
//!
//! Shape: R rows, R/DUP distinct contiguous-sorted i64 keys (models
//! lineitem sorted by l_orderkey, ~DUP lineitems/order → Partial reduces
//! ~1/DUP, exactly the measured reduction_factor=25%). Keys disjoint
//! across input partitions, so the Final phase combines almost nothing —
//! the wasteful re-hash REV.7 measured.
//!
//! GATE: single_pass must beat two_phase by >= 1.25x (margin for the
//! integration tax that erased every prior kernel win — REV.5/5.b, Σ.N).
//!
//! Usage:
//!   R=152000000 P=14 DUP=4 TRIALS=3 \
//!     cargo run --release -p ematix-flow-core --example bench_agg_two_phase_vs_single_pass

use std::thread;
use std::time::Instant;

use ematix_flow_core::robin_hood_agg::RobinHoodI64F64;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// splitmix64 — used for BOTH the two-phase shuffle bucket and the
/// single-pass radix bin, so the two paths route keys identically.
#[inline]
fn hash64(k: i64) -> u64 {
    let mut z = (k as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// DataFusion's two-phase shape: P parallel Partial aggs, hash-shuffle of
/// the partial groups, then P parallel Final aggs.
fn two_phase(keys: &[i64], vals: &[f64], p: usize, distinct: usize) -> (f64, usize) {
    let n = keys.len();
    let chunk = n.div_ceil(p);
    // Each input partition sees ~distinct/p groups (sorted ⇒ disjoint ranges).
    let part_cap = (distinct / p * 3 / 2).max(64);
    let t0 = Instant::now();

    // Phase Partial: aggregate each slice, then scatter partial groups into
    // P shuffle buckets by hash(key) % P.
    #[allow(clippy::type_complexity)]
    let partials: Vec<(Vec<Vec<i64>>, Vec<Vec<f64>>)> = thread::scope(|s| {
        let handles: Vec<_> = (0..p)
            .map(|t| {
                let lo = t * chunk;
                let hi = ((t + 1) * chunk).min(n);
                let ks = &keys[lo..hi];
                let vs = &vals[lo..hi];
                s.spawn(move || {
                    let mut tbl = RobinHoodI64F64::with_capacity(part_cap);
                    tbl.insert_or_sum_batch_vectorised(ks, vs);
                    let mut bk: Vec<Vec<i64>> = (0..p).map(|_| Vec::new()).collect();
                    let mut bv: Vec<Vec<f64>> = (0..p).map(|_| Vec::new()).collect();
                    let approx = tbl.len() / p + 16;
                    for b in 0..p {
                        bk[b].reserve(approx);
                        bv[b].reserve(approx);
                    }
                    for (k, v) in tbl.iter() {
                        let b = (hash64(k) % p as u64) as usize;
                        bk[b].push(k);
                        bv[b].push(v);
                    }
                    (bk, bv)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // Phase Final: bucket b is aggregated from every Partial thread's b-th
    // sub-bucket. (Near-unique sorted keys ⇒ no real combining; this is the
    // redundant re-hash REV.7 measured at 22.85s time_calculating_group_ids.)
    let final_cap = (distinct / p * 3 / 2).max(64);
    let groups: usize = thread::scope(|s| {
        let partials = &partials;
        let handles: Vec<_> = (0..p)
            .map(|b| {
                s.spawn(move || {
                    let mut tbl = RobinHoodI64F64::with_capacity(final_cap);
                    for (bk, bv) in partials.iter() {
                        tbl.insert_or_sum_batch_vectorised(&bk[b], &bv[b]);
                    }
                    tbl.len()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).sum()
    });

    (t0.elapsed().as_secs_f64() * 1e3, groups)
}

/// Single-pass radix shape (DuckDB): P parallel radix-scatters of RAW rows
/// into B bins, a barrier, then a per-bin aggregate-once (each bin combined
/// exactly once — no second hash pass).
fn single_pass(keys: &[i64], vals: &[f64], p: usize, b_bins: usize, distinct: usize) -> (f64, usize) {
    let n = keys.len();
    let chunk = n.div_ceil(p);
    let t0 = Instant::now();

    // Phase scatter: radix-partition each raw slice into B bins.
    #[allow(clippy::type_complexity)]
    let scattered: Vec<(Vec<Vec<i64>>, Vec<Vec<f64>>)> = thread::scope(|s| {
        let handles: Vec<_> = (0..p)
            .map(|t| {
                let lo = t * chunk;
                let hi = ((t + 1) * chunk).min(n);
                let ks = &keys[lo..hi];
                let vs = &vals[lo..hi];
                s.spawn(move || {
                    let mut bk: Vec<Vec<i64>> = (0..b_bins).map(|_| Vec::new()).collect();
                    let mut bv: Vec<Vec<f64>> = (0..b_bins).map(|_| Vec::new()).collect();
                    let approx = ks.len() / b_bins + 16;
                    for bin in 0..b_bins {
                        bk[bin].reserve(approx);
                        bv[bin].reserve(approx);
                    }
                    for i in 0..ks.len() {
                        let bin = (hash64(ks[i]) % b_bins as u64) as usize;
                        bk[bin].push(ks[i]);
                        bv[bin].push(vs[i]);
                    }
                    (bk, bv)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // Phase combine: spread the B bins across P threads; each bin is
    // aggregated exactly once from all partitions' scatter buffers.
    let comb_cap = (distinct / b_bins * 3 / 2).max(64);
    let bins_per = b_bins.div_ceil(p);
    let groups: usize = thread::scope(|s| {
        let scattered = &scattered;
        let handles: Vec<_> = (0..p)
            .map(|t| {
                s.spawn(move || {
                    let mut g = 0usize;
                    let lo = t * bins_per;
                    let hi = ((t + 1) * bins_per).min(b_bins);
                    for bin in lo..hi {
                        let mut tbl = RobinHoodI64F64::with_capacity(comb_cap);
                        for (bk, bv) in scattered.iter() {
                            tbl.insert_or_sum_batch_vectorised(&bk[bin], &bv[bin]);
                        }
                        g += tbl.len();
                    }
                    g
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).sum()
    });

    (t0.elapsed().as_secs_f64() * 1e3, groups)
}

fn main() {
    let r = env_usize("R", 152_000_000);
    let p = env_usize("P", 14);
    let dup = env_usize("DUP", 4).max(1);
    let trials = env_usize("TRIALS", 3);
    let distinct = r / dup;

    // Sorted contiguous keys: key[i] = i / DUP → DUP repeats each, ascending.
    // Models lineitem sorted by l_orderkey (~DUP lineitems per order).
    let keys: Vec<i64> = (0..r).map(|i| (i / dup) as i64).collect();
    let vals: Vec<f64> = (0..r).map(|i| ((i % 50) + 1) as f64).collect();

    println!(
        "== REV.8 gate: R={r} rows, distinct={distinct}, P={p} partitions, DUP={dup}, trials={trials} =="
    );

    // Warm up + correctness: both must produce `distinct` groups.
    let (_, tp_g) = two_phase(&keys, &vals, p, distinct);
    let (_, sp_g) = single_pass(&keys, &vals, p, 64, distinct);
    assert_eq!(tp_g, distinct, "two_phase groups {tp_g} != {distinct}");
    assert_eq!(sp_g, distinct, "single_pass groups {sp_g} != {distinct}");

    let mut tp = Vec::new();
    for _ in 0..trials {
        tp.push(two_phase(&keys, &vals, p, distinct).0);
    }
    let tp_ms = median(tp);
    let mrows = r as f64 / 1e6;
    println!(
        "  two_phase (Partial→shuffle→Final)   : {tp_ms:8.1} ms  ({:6.1} M rows/s)",
        mrows / (tp_ms / 1e3)
    );

    // single_pass at a few bin counts (more bins = smaller, cache-hotter
    // combine tables, but more scatter buffers).
    let mut best_ms = f64::INFINITY;
    let mut best_b = 0usize;
    for &b_bins in &[p, 64usize, 256usize, 1024usize] {
        let mut sp = Vec::new();
        for _ in 0..trials {
            sp.push(single_pass(&keys, &vals, p, b_bins, distinct).0);
        }
        let sp_ms = median(sp);
        println!(
            "  single_pass (radix→combine, B={b_bins:<4})    : {sp_ms:8.1} ms  ({:6.1} M rows/s)  {:.2}x vs two_phase",
            mrows / (sp_ms / 1e3),
            tp_ms / sp_ms
        );
        if sp_ms < best_ms {
            best_ms = sp_ms;
            best_b = b_bins;
        }
    }

    let speedup = tp_ms / best_ms;
    println!(
        "\n  BEST single_pass: {best_ms:.1} ms (B={best_b}) = {speedup:.2}x vs two_phase"
    );
    println!(
        "  GATE (>=1.25x to build the operator): {}",
        if speedup >= 1.25 {
            "PASS — build SinglePassRadixSumF64Exec"
        } else {
            "FAIL — gap is key-width/probe-speed, not the two-phase structure; pursue u32 keys instead"
        }
    );
}
