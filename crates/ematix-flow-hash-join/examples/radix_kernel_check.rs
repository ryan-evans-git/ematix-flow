//! RADIX.1.gate — does the production kernel replicate Phase-0's MULTI-THREAD win?
//!
//! Phase-0's radix win is **contention relief**, not single-thread locality: a
//! single shared ~104 MB `TaggedJoinI64U32` hammered by 14 probe threads pays a
//! ~2× per-probe cache-coherence/bandwidth penalty (~15.5 vs ~7.9 ns/probe);
//! radix gives each thread its own cache-resident sub-tables → no contention.
//! (Single-threaded, radix *loses* ~20% — pure scatter overhead, nothing to
//! relieve. That is expected and is why this check is multi-threaded.)
//!
//! Faithful A/B on Q10's cardinalities, both arms parallel, real kernel, match
//! counts asserted equal:
//!   A — one shared TaggedJoinI64U32, 14 threads probe disjoint slices (contended).
//!   B — RadixTaggedJoin: 14-thread parallel scatter into per-partition buffers,
//!       then 14 threads each own disjoint partitions (contention-free probe).
//!
//! Run: cargo run --release -p ematix-flow-hash-join --example radix_kernel_check

use ematix_flow_hash_join::{ProbeMatch, RadixTaggedJoin, TaggedJoinI64U32};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::thread;
use std::time::Instant;

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let build_n = env("BUILD_N", 5_700_000);
    let probe_n = env("PROBE_N", 80_000_000);
    let threads = env(
        "THREADS",
        thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(8),
    );

    let build: Vec<i64> = (0..build_n).map(|i| i as i64 * 2 + 1).collect(); // distinct odd
    let probe: Vec<i64> = (0..probe_n)
        .map(|i| {
            let h = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            if h % 1000 < 78 {
                build[(h as usize >> 8) % build_n]
            } else {
                (i as i64) * 2
            }
        })
        .collect();

    let tbl_mb = (build_n * 10 / 7).next_power_of_two() * 13 / 1_048_576;
    println!(
        "# RADIX.1.gate MULTI-THREAD kernel check  build={build_n} (~{tbl_mb}MB) probe={probe_n} threads={threads}"
    );

    // Arm A — one shared table, 14 threads probe disjoint slices (the contended shape).
    let shared = TaggedJoinI64U32::try_build(&build, None, 0).expect("unique build");
    let mut a_ms = f64::MAX;
    let mut a_n = 0u64;
    for _ in 0..3 {
        let total = AtomicU64::new(0);
        let chunk = probe.len().div_ceil(threads);
        let t0 = Instant::now();
        thread::scope(|s| {
            for c in probe.chunks(chunk) {
                let (shared, total) = (&shared, &total);
                s.spawn(move || {
                    let mut out: Vec<ProbeMatch> = Vec::with_capacity(c.len() / 10);
                    shared.probe_batch(c, None, 0, &mut out);
                    total.fetch_add(out.len() as u64, Relaxed);
                });
            }
        });
        a_ms = a_ms.min(t0.elapsed().as_secs_f64() * 1000.0);
        a_n = total.load(Relaxed);
    }
    println!("  [A shared, 14-thread contended]   {a_ms:>5.0}ms  matches={a_n}");

    // Arm B — radix: EFFICIENT single-array parallel scatter (histogram + prefix +
    // disjoint-range scatter, the microbench's ~0.26ns/row approach) + contention-
    // free per-partition probe. The nested-Vec scatter was 2× slower (alloc churn).
    for bits in [10u32, 12] {
        let radix = RadixTaggedJoin::try_build(&build, None, bits).expect("unique build");
        let n_part = radix.num_partitions();
        let (mut b_ms, mut b_sc, mut b_n) = (f64::MAX, f64::MAX, 0u64);
        for _ in 0..3 {
            let chunk = probe.len().div_ceil(threads);
            let nch = probe.len().div_ceil(chunk).max(1);
            let ts = Instant::now();
            // 1. parallel per-chunk histogram
            let mut hist = vec![vec![0usize; n_part]; nch];
            thread::scope(|s| {
                for (h, c) in hist.iter_mut().zip(probe.chunks(chunk)) {
                    let radix = &radix;
                    s.spawn(move || {
                        for &k in c {
                            h[radix.partition_of(k)] += 1;
                        }
                    });
                }
            });
            // 2. partition offsets + per-(chunk,partition) write starts
            let mut part_off = vec![0usize; n_part + 1];
            for p in 0..n_part {
                let s: usize = hist.iter().map(|h| h[p]).sum();
                part_off[p + 1] = part_off[p] + s;
            }
            let mut wstart = vec![vec![0usize; n_part]; nch];
            for p in 0..n_part {
                let mut run = part_off[p];
                for (c, h) in wstart.iter_mut().zip(hist.iter()) {
                    c[p] = run;
                    run += h[p];
                }
            }
            // 3. parallel scatter into single key+idx arrays (disjoint ranges → raw ptr)
            let mut okeys = vec![0i64; probe.len()];
            let mut oidx = vec![0u32; probe.len()];
            let (kp, ip) = (okeys.as_mut_ptr() as usize, oidx.as_mut_ptr() as usize);
            thread::scope(|s| {
                for (ci, c) in probe.chunks(chunk).enumerate() {
                    let (radix, ws) = (&radix, wstart[ci].clone());
                    let base = ci * chunk;
                    s.spawn(move || {
                        let (kp, ip) = (kp as *mut i64, ip as *mut u32);
                        let mut cur = ws;
                        for (i, &k) in c.iter().enumerate() {
                            let p = radix.partition_of(k);
                            unsafe {
                                *kp.add(cur[p]) = k;
                                *ip.add(cur[p]) = (base + i) as u32;
                            }
                            cur[p] += 1;
                        }
                    });
                }
            });
            let sc_ms = ts.elapsed().as_secs_f64() * 1000.0;
            // 4. parallel probe: each worker owns disjoint partitions (no shared sub-table)
            let next = AtomicUsize::new(0);
            let total = AtomicU64::new(0);
            let tp = Instant::now();
            thread::scope(|s| {
                for _ in 0..threads {
                    let (radix, okeys, oidx, part_off, next, total) =
                        (&radix, &okeys, &oidx, &part_off, &next, &total);
                    s.spawn(move || {
                        let mut out: Vec<ProbeMatch> = Vec::new();
                        let mut local = 0u64;
                        loop {
                            let p = next.fetch_add(1, Relaxed);
                            if p >= n_part {
                                break;
                            }
                            let (lo, hi) = (part_off[p], part_off[p + 1]);
                            radix
                                .subtable(p)
                                .probe_pairs(&okeys[lo..hi], &oidx[lo..hi], &mut out);
                            local += out.len() as u64;
                            out.clear();
                        }
                        total.fetch_add(local, Relaxed);
                    });
                }
            });
            let pr_ms = tp.elapsed().as_secs_f64() * 1000.0;
            b_sc = b_sc.min(sc_ms);
            b_ms = b_ms.min(sc_ms + pr_ms);
            b_n = total.load(Relaxed);
        }
        assert_eq!(b_n, a_n, "bits={bits}: match count must equal shared");
        let d = (b_ms - a_ms) / a_ms * 100.0;
        let sub_kb = (build_n / n_part * 10 / 7).next_power_of_two() * 13 / 1024;
        println!(
            "  [B radix N={n_part:>5}]  {b_ms:>5.0}ms (scatter {b_sc:.0} + probe {:.0})  Δ={d:+5.1}%  sub-tbl~{sub_kb}KB",
            b_ms - b_sc
        );
    }
    println!(
        "# Δ negative = radix relieves the shared-build contention in the real kernel → Phase-1 gate clear."
    );
}
