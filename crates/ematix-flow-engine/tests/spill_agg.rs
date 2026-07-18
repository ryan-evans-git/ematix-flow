//! P2 spill kill-gate: a hash aggregate that stays correct beyond its
//! memory budget.
//!
//! With a tiny budget that forces the vast majority of rows through
//! disk-backed partitions, the `SUM(val) GROUP BY key` result must be
//! **bit-identical** to the unbudgeted, fully in-memory result. `SUM(i64)`
//! is exact and order-independent, so the spill path's changed summation
//! order cannot alter the answer — any difference would be a lost,
//! duplicated, or mis-routed row. The gate also asserts the spill path was
//! actually exercised (not silently kept in memory).

use ematix_flow_engine::agg::SpillableSumAgg;
use ematix_flow_engine::chunk::DataChunk;
use ematix_flow_engine::vector::Vector;

const N: usize = 1_000_000;
const NGROUPS: i64 = 50_000;
const CHUNK: usize = 8_192;
const PART_BITS: u32 = 8; // 256 partitions

/// Deterministic (key, val) generator — no RNG, so the gate never flakes.
/// Each key in 0..NGROUPS recurs N/NGROUPS times; vals cycle 1..=7.
fn row(i: usize) -> (i64, i64) {
    let k = (i as i64) % NGROUPS;
    let v = (i as i64 % 7) + 1;
    (k, v)
}

/// Drive the aggregate over `N` rows in `CHUNK`-sized `DataChunk`s under
/// `budget_bytes`; return the sorted (key, sum) pairs and how many rows
/// were spilled.
fn run(budget_bytes: usize) -> (Vec<(i64, i64)>, u64) {
    let mut agg = SpillableSumAgg::new(budget_bytes, PART_BITS);
    let mut i = 0;
    while i < N {
        let end = (i + CHUNK).min(N);
        let mut keys = Vec::with_capacity(end - i);
        let mut vals = Vec::with_capacity(end - i);
        for j in i..end {
            let (k, v) = row(j);
            keys.push(k);
            vals.push(v);
        }
        let chunk = DataChunk::new(vec![Vector::i64(keys), Vector::i64(vals)]);
        agg.consume(&chunk, 0, 1).expect("consume");
        i = end;
    }
    let spilled = agg.spilled_records();
    let out = agg.finish().expect("finish");
    (out, spilled)
}

#[test]
fn spilling_aggregate_matches_in_memory_bit_identically() {
    // Reference: unbudgeted — must never touch disk.
    let (ref_out, ref_spilled) = run(usize::MAX);
    assert_eq!(ref_spilled, 0, "reference run must not spill");
    assert_eq!(
        ref_out.len(),
        NGROUPS as usize,
        "one output row per distinct key"
    );

    // Spilled: 64 KiB budget over ~16 MB of rows → almost everything spills.
    let (spill_out, spilled) = run(64 * 1024);
    eprintln!("spill_agg: {spilled} of {N} rows spilled through disk-backed partitions");
    assert!(
        spilled as usize >= N - 10_000,
        "spill path must be exercised hard: only {spilled} of {N} rows spilled"
    );

    // The core invariant: identical group set, identical sums, bit-for-bit.
    assert_eq!(
        spill_out, ref_out,
        "spilled result must equal the in-memory result exactly"
    );

    // Every input value accounted for exactly once.
    let got_total: i64 = ref_out.iter().map(|&(_, s)| s).sum();
    let expect_total: i64 = (0..N).map(|i| row(i).1).sum();
    assert_eq!(
        got_total, expect_total,
        "sum of group sums must equal the total of all input values"
    );
}
