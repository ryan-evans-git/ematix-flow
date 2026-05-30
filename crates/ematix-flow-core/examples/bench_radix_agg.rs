//! REV.5.a — de-risk microbench for the radix-partitioned SUM agg.
//!
//! Compares the single-table `RobinHoodI64F64` (what `RobinHoodSumF64Exec`
//! currently uses) against the already-built-but-unwired
//! `RobinHoodSumF64RadixAgg` at the per-partition cardinality regime of
//! Q18's `SUM(l_quantity) GROUP BY l_orderkey` subquery.
//!
//! Q18 partial-agg cardinality (one of N scan partitions):
//!   - SF=10:  ~4M distinct orderkeys / partial
//!   - SF=100: ~38M distinct orderkeys / partial
//! Keys are near-unique within a partition (lineitem is ~sorted by
//! orderkey; each partial sees a contiguous range), so we model them as
//! sequential-unique — the hash scatters them, exercising the random-
//! probe DRAM-miss behaviour that dominates at high cardinality.
//!
//! GATE: radix must beat single-table here, or there's no point wiring it
//! into the operator.
//!
//! Usage:
//!   N=4000000 RADIX_BITS=8 TRIALS=5 \
//!     cargo run --release -p ematix-flow-core --example bench_radix_agg

use std::time::Instant;

use ematix_flow_core::robin_hood_agg::{
    RobinHoodI64F64, RobinHoodSumF64GlobalRadixAgg, RobinHoodSumF64RadixAgg,
};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// splitmix64 finalizer — even bin distribution for the partition pass.
/// (The micro-tables re-hash internally with their own `hash_i64`; this
/// only needs to group identical keys into the same bin, which any
/// deterministic hash does — correctness is preserved.)
#[inline]
fn part_hash(k: i64) -> u64 {
    let mut z = (k as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// CORRECTED radix: a single GLOBAL partition pass over ALL rows
/// (scatter into per-bin contiguous buffers), then drain each partition
/// to completion into its own micro-table — so each table stays
/// cache-resident for its whole partition. This is DuckDB's shape, and
/// the fix for the library kernel's per-1024-row-binning flaw (which
/// revisits all bins every chunk → no sustained residency).
fn run_radix_global(keys: &[i64], vals: &[f64], radix_bits: u8) -> (f64, usize) {
    let n = keys.len();
    let nb = 1usize << radix_bits;
    let per = (n / nb).max(1024);
    let shift = if radix_bits == 0 { 0 } else { 64 - radix_bits as u32 };
    let t = Instant::now();

    // Pass 1: tag every row + histogram bin sizes.
    let mut tags = vec![0u16; n];
    let mut counts = vec![0u32; nb];
    for i in 0..n {
        let r = (part_hash(keys[i]) >> shift) as usize;
        tags[i] = r as u16;
        counts[r] += 1;
    }
    // Prefix-sum → bin offsets.
    let mut offsets = vec![0u32; nb + 1];
    let mut acc = 0u32;
    for r in 0..nb {
        offsets[r] = acc;
        acc += counts[r];
    }
    offsets[nb] = acc;
    // Pass 2: scatter keys+values into global bin-contiguous buffers.
    let mut pkeys = vec![0i64; n];
    let mut pvals = vec![0f64; n];
    let mut next: Vec<u32> = offsets[..nb].to_vec();
    for i in 0..n {
        let r = tags[i] as usize;
        let d = next[r] as usize;
        pkeys[d] = keys[i];
        pvals[d] = vals[i];
        next[r] += 1;
    }
    // Pass 3: drain each partition fully into its own cache-resident table.
    let mut slots = [0usize; 1024];
    let mut hit = [false; 1024];
    let mut groups = 0usize;
    for r in 0..nb {
        let s = offsets[r] as usize;
        let e = offsets[r + 1] as usize;
        if s == e {
            continue;
        }
        let mut tbl = RobinHoodI64F64::with_capacity(per);
        tbl.insert_or_sum_batch_vectorised_with_scratch(&pkeys[s..e], &pvals[s..e], &mut slots, &mut hit);
        groups += tbl.len();
    }
    (t.elapsed().as_secs_f64() * 1e3, groups)
}

fn main() {
    let n = env_usize("N", 4_000_000);
    let radix_bits = env_usize("RADIX_BITS", 8) as u8;
    let trials = env_usize("TRIALS", 5);
    const BATCH: usize = 8192;

    // Sequential-unique keys (n distinct); values a small repeating f64.
    let keys: Vec<i64> = (0..n as i64).collect();
    let vals: Vec<f64> = (0..n).map(|i| ((i % 50) + 1) as f64).collect();

    println!("== radix-agg microbench: N={n} distinct keys, BATCH={BATCH}, radix_bits={radix_bits}, trials={trials} ==");

    // --- single-table (current operator path) ---
    let run_single = || -> (f64, usize) {
        let mut agg = RobinHoodI64F64::with_capacity(n);
        let mut slots = [0usize; 1024];
        let mut hit = [false; 1024];
        let t = Instant::now();
        let mut o = 0;
        while o < n {
            let e = (o + BATCH).min(n);
            agg.insert_or_sum_batch_vectorised_with_scratch(&keys[o..e], &vals[o..e], &mut slots, &mut hit);
            o = e;
        }
        (t.elapsed().as_secs_f64() * 1e3, agg.len())
    };

    // --- radix-partitioned (candidate) ---
    let run_radix = || -> (f64, usize) {
        let per = (n / (1usize << radix_bits)).max(1024);
        let mut agg = RobinHoodSumF64RadixAgg::new(radix_bits, per);
        let t = Instant::now();
        let mut o = 0;
        while o < n {
            let e = (o + BATCH).min(n);
            agg.ingest_batch_radix(&keys[o..e], &vals[o..e]);
            o = e;
        }
        (t.elapsed().as_secs_f64() * 1e3, agg.len())
    };

    // --- streaming GLOBAL-partition (the library aggregator, bounded) ---
    // The real REV.5.b artifact: scatter-append per batch + incremental
    // drain at a memory budget. MEM_BUDGET_MB caps raw buffers.
    let mem_budget_mb = env_usize("MEM_BUDGET_MB", 128);
    let run_stream = || -> (f64, usize) {
        let per = (n / (1usize << radix_bits)).max(1024);
        let mut agg = RobinHoodSumF64GlobalRadixAgg::new(radix_bits, per, mem_budget_mb << 20);
        let t = Instant::now();
        let mut o = 0;
        while o < n {
            let e = (o + BATCH).min(n);
            agg.ingest_batch(&keys[o..e], &vals[o..e]);
            o = e;
        }
        agg.finish();
        (t.elapsed().as_secs_f64() * 1e3, agg.len())
    };

    // --- spill-backed GLOBAL-partition (REV.5.b — DuckDB's shape) ---
    // Over-budget partitions are written to disk; each is aggregated
    // exactly once at finish, preserving cache residency under the SAME
    // memory bound the in-memory STREAM path gives up via mid-stream
    // re-visits. The win minus disk-I/O cost is what this measures.
    let spill_dir = std::env::temp_dir();
    let run_spill = || -> (f64, usize, bool) {
        let per = (n / (1usize << radix_bits)).max(1024);
        let mut agg = RobinHoodSumF64GlobalRadixAgg::new_with_spill(
            radix_bits,
            per,
            mem_budget_mb << 20,
            spill_dir.clone(),
        );
        let t = Instant::now();
        let mut o = 0;
        while o < n {
            let e = (o + BATCH).min(n);
            agg.ingest_batch(&keys[o..e], &vals[o..e]);
            o = e;
        }
        agg.finish();
        (t.elapsed().as_secs_f64() * 1e3, agg.len(), agg.did_spill())
    };

    // Warm up once (page-in, branch predictor, allocator).
    let (_, single_groups) = run_single();
    let (_, radix_groups) = run_radix();
    let (_, global_groups) = run_radix_global(&keys, &vals, radix_bits);
    let (_, stream_groups) = run_stream();
    let (_, spill_groups, spill_fired) = run_spill();
    assert_eq!(single_groups, n, "single: expected {n} groups, got {single_groups}");
    assert_eq!(radix_groups, n, "radix per-chunk: expected {n} groups, got {radix_groups}");
    assert_eq!(global_groups, n, "radix global: expected {n} groups, got {global_groups}");
    assert_eq!(stream_groups, n, "stream: expected {n} groups, got {stream_groups}");
    assert_eq!(spill_groups, n, "spill: expected {n} groups, got {spill_groups}");

    let mut single_ms = Vec::new();
    let mut radix_ms = Vec::new();
    let mut global_ms = Vec::new();
    let mut stream_ms = Vec::new();
    let mut spill_ms = Vec::new();
    for _ in 0..trials {
        single_ms.push(run_single().0);
        radix_ms.push(run_radix().0);
        global_ms.push(run_radix_global(&keys, &vals, radix_bits).0);
        stream_ms.push(run_stream().0);
        spill_ms.push(run_spill().0);
    }
    let s = median(single_ms);
    let r = median(radix_ms);
    let g = median(global_ms);
    let st = median(stream_ms);
    let sp = median(spill_ms);
    let mrows = n as f64 / 1e6;
    println!("  single-table                 : {s:8.1} ms  ({:6.1} M rows/s)", mrows / (s / 1e3));
    println!(
        "  radix per-chunk(b={radix_bits})         : {r:8.1} ms  ({:6.1} M rows/s)  {:.2}x vs single",
        mrows / (r / 1e3),
        s / r
    );
    println!(
        "  radix GLOBAL unbounded(b={radix_bits})  : {g:8.1} ms  ({:6.1} M rows/s)  {:.2}x vs single",
        mrows / (g / 1e3),
        s / g
    );
    println!(
        "  radix STREAM bounded(b={radix_bits},{mem_budget_mb}MB) : {st:8.1} ms  ({:6.1} M rows/s)  {:.2}x vs single  [>1 = wins]",
        mrows / (st / 1e3),
        s / st
    );
    println!(
        "  radix SPILL  bounded(b={radix_bits},{mem_budget_mb}MB) : {sp:8.1} ms  ({:6.1} M rows/s)  {:.2}x vs single  [{}]",
        mrows / (sp / 1e3),
        s / sp,
        if spill_fired { "spilled to disk" } else { "in-RAM, no spill" }
    );
    println!("  groups: {single_groups} (all)");
}
