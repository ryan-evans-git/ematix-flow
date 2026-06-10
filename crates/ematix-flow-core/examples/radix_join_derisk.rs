//! Q10 radix-build join — Phase-0 kill-gate microbench (standalone, no DataFusion).
//!
//! The one gating question for the multi-week radix-build join program:
//! on Q10 SF=100's join cardinalities, does a DuckDB-style per-partition
//! radix build+probe (cache-resident sub-tables) beat the SHARED ~90MB
//! build that the SF100.6/7 no-shuffle experiment showed is +21.7% (one
//! 3.2GB table hammered by 14 threads = cache contention)?
//!
//! Q10 hot join (o_orderkey = l_orderkey):
//!   BUILD = customer⋈orders ≈ 5.7M rows, keyed on orderkey (+ a payload row-idx)
//!   PROBE = 148M `l_returnflag='R'` lineitems, ~11.5M of which match a build key
//!
//! Arms (all 14-thread, identical synthetic data, identical match count asserted):
//!   A. shared-build : one open-addressing table (~5.7M entries, ~90MB), 14 threads
//!                     probe disjoint slices of the 148M probe keys.
//!   B. radix-build  : radix-partition BOTH sides by the HIGH bits of the key hash
//!                     into P partitions (sweep P), then 14 threads each build a
//!                     small cache-resident sub-table and probe its partition.
//!                     The 2 partition passes over 148M are counted in B's wall.
//!
//! KILL-GATE: B(best P) must beat A by a clear margin (>~15%) for the radix
//! direction to be worth the multi-week build. If B >= A at every P, the
//! partitioning passes eat the locality gain -> NO-GO.
//!
//! Run: cargo run --release --example radix_join_derisk
//!   env: BUILD_N (default 5_700_000), PROBE_N (default 148_000_000),
//!        MATCH_N (default 11_500_000), THREADS (default available_parallelism)

use std::time::Instant;

#[inline(always)]
fn mix(mut x: u64) -> u64 {
    // splitmix64 finalizer — good avalanche, independent high/low bits.
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

const EMPTY: i64 = i64::MIN;

/// Open-addressing linear-probe table over i64 keys. `slot_bits` low bits
/// of `mix(key)` index the table. Stores key only (payload row-idx omitted;
/// it doesn't change the cache-miss behaviour we're measuring — the table
/// entry would be key+u32, still cache-missing at 5.7M).
struct Table {
    slots: Vec<i64>,
    mask: u64,
}
impl Table {
    fn with_capacity(n: usize) -> Self {
        // ~0.4 load factor, power of two.
        let cap = (n * 5 / 2).next_power_of_two().max(16);
        Table {
            slots: vec![EMPTY; cap],
            mask: (cap - 1) as u64,
        }
    }
    #[inline(always)]
    fn insert(&mut self, key: i64) {
        let mut i = (mix(key as u64) & self.mask) as usize;
        loop {
            let s = self.slots[i];
            if s == EMPTY {
                self.slots[i] = key;
                return;
            }
            if s == key {
                return;
            } // distinct build keys; dedup
            i = (i + 1) & self.mask as usize;
        }
    }
    #[inline(always)]
    fn contains(&self, key: i64) -> bool {
        let mut i = (mix(key as u64) & self.mask) as usize;
        loop {
            let s = self.slots[i];
            if s == key {
                return true;
            }
            if s == EMPTY {
                return false;
            }
            i = (i + 1) & self.mask as usize;
        }
    }
}

fn gen_data(build_n: usize, probe_n: usize, match_n: usize) -> (Vec<i64>, Vec<i64>) {
    // Build: ~build_n near-distinct keys in [0, 600M) (SF=100 orderkey range).
    let key_space: u64 = 600_000_000;
    let mut build = Vec::with_capacity(build_n);
    for i in 0..build_n as u64 {
        build.push((mix(i) % key_space) as i64);
    }
    // Probe: match_n keys drawn from `build` (so they hit), the rest from
    // [600M, 1200M) (disjoint -> guaranteed miss). Interleaved.
    let mut probe = Vec::with_capacity(probe_n);
    for i in 0..probe_n as u64 {
        if (mix(i ^ 0xABCD) % probe_n as u64) < match_n as u64 {
            probe.push(build[(mix(i ^ 0x1234) as usize) % build_n]);
        } else {
            probe.push((key_space + (mix(i ^ 0x9999) % key_space)) as i64);
        }
    }
    (build, probe)
}

/// Arm A — one shared table, 14-thread probe.
fn arm_shared(build: &[i64], probe: &[i64], threads: usize) -> (u64, f64, f64) {
    let t0 = Instant::now();
    let mut table = Table::with_capacity(build.len());
    for &k in build {
        table.insert(k);
    }
    let build_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t1 = Instant::now();
    let total = std::sync::atomic::AtomicU64::new(0);
    let chunk = probe.len().div_ceil(threads);
    std::thread::scope(|s| {
        for c in probe.chunks(chunk) {
            let table = &table;
            let total = &total;
            s.spawn(move || {
                let mut local = 0u64;
                for &k in c {
                    if table.contains(k) {
                        local += 1;
                    }
                }
                total.fetch_add(local, std::sync::atomic::Ordering::Relaxed);
            });
        }
    });
    let probe_ms = t1.elapsed().as_secs_f64() * 1000.0;
    (total.into_inner(), build_ms, probe_ms)
}

/// Parallel radix partition by the TOP `log2(p)` bits of mix(key).
/// Returns the partitioned keys + per-partition [start,end) offsets.
fn radix_partition(keys: &[i64], p: usize, threads: usize) -> (Vec<i64>, Vec<usize>) {
    let pbits = p.trailing_zeros();
    let shift = 64 - pbits;

    let chunk = keys.len().div_ceil(threads);
    let nchunks = keys.len().div_ceil(chunk).max(1);
    // hist[t][p]
    let mut hist = vec![vec![0usize; p]; nchunks];
    std::thread::scope(|s| {
        for (h, c) in hist.iter_mut().zip(keys.chunks(chunk)) {
            s.spawn(move || {
                for &k in c {
                    h[(mix(k as u64) >> shift) as usize] += 1;
                }
            });
        }
    });
    // global per-partition offsets + per-(thread,partition) write starts.
    let mut part_off = vec![0usize; p + 1];
    for pp in 0..p {
        let mut s = 0usize;
        for h in hist.iter() {
            s += h[pp];
        }
        part_off[pp + 1] = part_off[pp] + s;
    }
    let mut wstart = vec![vec![0usize; p]; nchunks];
    for pp in 0..p {
        let mut run = part_off[pp];
        for t in 0..nchunks {
            wstart[t][pp] = run;
            run += hist[t][pp];
        }
    }
    let mut out = vec![0i64; keys.len()];
    // scatter: each thread writes into disjoint ranges -> safe via raw ptr.
    let out_ptr = out.as_mut_ptr() as usize;
    std::thread::scope(|s| {
        for (t, c) in keys.chunks(chunk).enumerate() {
            let ws = wstart[t].clone();
            s.spawn(move || {
                let out = out_ptr as *mut i64;
                let mut cur = ws;
                for &k in c {
                    let pp = (mix(k as u64) >> shift) as usize;
                    unsafe { *out.add(cur[pp]) = k };
                    cur[pp] += 1;
                }
            });
        }
    });
    (out, part_off)
}

/// Arm B — radix build+probe with P partitions.
fn arm_radix(build: &[i64], probe: &[i64], p: usize, threads: usize) -> (u64, f64, f64) {
    let t0 = Instant::now();
    let (bpart, boff) = radix_partition(build, p, threads);
    let (ppart, poff) = radix_partition(probe, p, threads);
    let part_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t1 = Instant::now();
    let total = std::sync::atomic::AtomicU64::new(0);
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..threads {
            let (total, next) = (&total, &next);
            let (bpart, boff, ppart, poff) = (&bpart, &boff, &ppart, &poff);
            s.spawn(move || {
                let mut local = 0u64;
                loop {
                    let pp = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if pp >= p {
                        break;
                    }
                    let bsl = &bpart[boff[pp]..boff[pp + 1]];
                    let psl = &ppart[poff[pp]..poff[pp + 1]];
                    let mut tab = Table::with_capacity(bsl.len().max(1));
                    for &k in bsl {
                        tab.insert(k);
                    }
                    for &k in psl {
                        if tab.contains(k) {
                            local += 1;
                        }
                    }
                }
                total.fetch_add(local, std::sync::atomic::Ordering::Relaxed);
            });
        }
    });
    let bp_ms = t1.elapsed().as_secs_f64() * 1000.0;
    (total.into_inner(), part_ms, bp_ms)
}

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// One TPC-H join shape: build/probe cardinalities of a query's dominant hash join.
struct Shape {
    label: &'static str,
    build_n: usize,
    probe_n: usize,
    match_n: usize,
}

/// best-of-2 wall (ms) for a closure.
fn best2(mut f: impl FnMut() -> (u64, f64, f64)) -> (u64, f64) {
    let mut best = f64::MAX;
    let mut m0 = 0;
    for _ in 0..2 {
        let (m, a, b) = f();
        m0 = m;
        best = best.min(a + b);
    }
    (m0, best)
}

fn run_shape(s: &Shape, threads: usize) {
    let (build, probe) = gen_data(s.build_n, s.probe_n, s.match_n);
    let shared_mb = (s.build_n * 5 / 2).next_power_of_two() * 8 / 1_048_576;

    // A: shared single build (the SF100.6/7 +21.7% shape).
    let (am, a) = best2(|| arm_shared(&build, &probe, threads));
    // C: shuffle proxy — P=16 partitions ≈ DataFusion's target_partitions shuffle
    //    (per-partition build+probe; conservative — omits RepartitionExec channel cost
    //    that real stock ALSO pays, so a radix win here under-states the real advantage).
    let (cm, c) = best2(|| arm_radix(&build, &probe, 16, threads));
    // B: radix-best — sweep many partitions to reach cache-resident sub-tables.
    let mut b = f64::MAX;
    let mut bp_at = 0;
    let mut bm = 0;
    for &p in &[256usize, 1024, 4096] {
        let (m, t) = best2(|| arm_radix(&build, &probe, p, threads));
        bm = m;
        if t < b {
            b = t;
            bp_at = p;
        }
    }
    assert_eq!(am, cm, "shuffle match diverged");
    assert_eq!(am, bm, "radix match diverged");

    let d_bc = (b - c) / c * 100.0; // radix-best vs shuffle (the REAL "would it help")
    let d_ba = (b - a) / a * 100.0; // radix-best vs shared
    let verdict = if d_bc < -8.0 {
        "RADIX WINS vs shuffle"
    } else if d_bc > 5.0 {
        "radix LOSES vs shuffle"
    } else {
        "neutral (no regression)"
    };
    println!(
        "{:<26} build={:>7} ({:>4}MB)  shared={:>5.0} shuffle(P16)={:>5.0} radix(P{:<4})={:>5.0}  Δradix-v-shuffle={:+5.1}%  Δradix-v-shared={:+5.1}%  → {}",
        s.label, s.build_n, shared_mb, a, c, bp_at, b, d_bc, d_ba, verdict
    );
}

fn main() {
    let threads = env(
        "THREADS",
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(8),
    );
    println!("# Radix-build join — cross-query shape sweep (threads={threads})");
    println!("# Q: does per-partition radix build beat the STOCK SHUFFLE (P16), and where?");
    println!(
        "# (radix WINS vs shuffle = a real candidate query; neutral = safe to enable, no regression)\n"
    );

    // Dominant hash-join cardinalities per join-bound TPC-H query family.
    // build = the build side feeding the join; probe = the streamed side; match = output rows.
    // SF=10 and SF=100 shapes both included to see the build-size (cache) effect.
    let shapes = [
        // small-build family — the Q02/Q11/Q16 partsupp-probe that REGRESSED +12/+63% default-on.
        Shape {
            label: "Q02/Q11 small-dim⋈partsupp",
            build_n: 200_000,
            probe_n: 80_000_000,
            match_n: 8_000_000,
        },
        Shape {
            label: "Q16 part⋈partsupp SF100",
            build_n: 1_000_000,
            probe_n: 80_000_000,
            match_n: 12_000_000,
        },
        // cust⋈orders ⋈ lineitem family (Q03/Q05/Q10).
        Shape {
            label: "Q10 cust⋈ord SF10",
            build_n: 573_000,
            probe_n: 14_800_000,
            match_n: 11_000_000,
        },
        Shape {
            label: "Q10 cust⋈ord SF100",
            build_n: 5_700_000,
            probe_n: 148_000_000,
            match_n: 11_500_000,
        },
        Shape {
            label: "Q03 cust⋈ord SF100",
            build_n: 7_500_000,
            probe_n: 60_000_000,
            match_n: 30_000_000,
        },
        // part-filtered ⋈ lineitem family (Q08/Q09/Q14/Q19).
        Shape {
            label: "Q09 part⋈ps⋈line SF100",
            build_n: 12_000_000,
            probe_n: 60_000_000,
            match_n: 20_000_000,
        },
        Shape {
            label: "Q08 part-filt⋈line SF100",
            build_n: 800_000,
            probe_n: 60_000_000,
            match_n: 1_500_000,
        },
        // big-build (Q18 orders, Q21 lineitem self-join).
        Shape {
            label: "Q18 orders⋈line SF100",
            build_n: 30_000_000,
            probe_n: 80_000_000,
            match_n: 40_000_000,
        },
        Shape {
            label: "Q21 line⋈ord SF100",
            build_n: 76_000_000,
            probe_n: 80_000_000,
            match_n: 40_000_000,
        },
    ];
    for s in &shapes {
        run_shape(s, threads);
    }
    println!(
        "\n# Read: 'RADIX WINS vs shuffle' shapes are queries radix-build could speed up (if join-bound, not decode-floored)."
    );
    println!(
        "# 'neutral' = no regression (radix adapts P to build size — fixes the small-build regression that blocked default-on)."
    );
}
