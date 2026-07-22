//! P2 spilling hash-join kill-gate: an inner equi-join that stays correct
//! when both sides exceed the memory budget.
//!
//! GRACE co-partitioning routes a build row and a probe row with the same
//! key to the same partition, so joining partition-pairs independently is
//! the complete join. The gate forces both sides to disk under a tiny
//! budget and asserts the produced match multiset is **bit-identical** to
//! the unbudgeted in-memory join — any lost, duplicated, or mis-routed
//! match would diverge — cross-checked against an independent count.
//!
//! The data deliberately exercises the hard cases a semijoin can't:
//! **multi-match** (duplicate keys on the build side → a probe row matches
//! several build rows) and **inner-join drop** (probe keys with no build
//! row must vanish).

use ematix_flow_engine::chunk::DataChunk;
use ematix_flow_engine::hashjoin::SpillableHashJoin;
use ematix_flow_engine::vector::Vector;

const NBUILD: usize = 40_000;
const BUILD_NKEYS: i64 = 20_000; // each build key recurs NBUILD/BUILD_NKEYS = 2×
const NPROBE: usize = 200_000;
const PROBE_NKEYS: i64 = 40_000; // probe keys 0..39_999; only 0..19_999 match
const CHUNK: usize = 8_192;
const PART_BITS: u32 = 8; // 256 partitions

/// Build row: (key in 0..BUILD_NKEYS, payload = its unique index).
fn build_row(i: usize) -> (i64, i64) {
    ((i as i64) % BUILD_NKEYS, i as i64)
}
/// Probe row: (key in 0..PROBE_NKEYS, payload = its unique index).
fn probe_row(i: usize) -> (i64, i64) {
    ((i as i64) % PROBE_NKEYS, i as i64)
}

fn feed(join: &mut SpillableHashJoin, n: usize, rowf: fn(usize) -> (i64, i64), build: bool) {
    let mut i = 0;
    while i < n {
        let end = (i + CHUNK).min(n);
        let mut keys = Vec::with_capacity(end - i);
        let mut pays = Vec::with_capacity(end - i);
        for j in i..end {
            let (k, p) = rowf(j);
            keys.push(k);
            pays.push(p);
        }
        let chunk = DataChunk::new(vec![Vector::i64(keys), Vector::i64(pays)]);
        if build {
            join.consume_build(&chunk, 0, 1).expect("consume_build");
        } else {
            join.consume_probe(&chunk, 0, 1).expect("consume_probe");
        }
        i = end;
    }
}

/// Run the join under `budget_bytes`; return sorted (key, build_pay,
/// probe_pay) matches plus the build/probe spilled-row counts.
fn run(budget_bytes: usize) -> (Vec<(i64, i64, i64)>, u64, u64) {
    let mut join = SpillableHashJoin::new(budget_bytes, PART_BITS);
    feed(&mut join, NBUILD, build_row, true);
    feed(&mut join, NPROBE, probe_row, false);
    let build_spilled = join.build_spilled();
    let probe_spilled = join.probe_spilled();
    let mut out: Vec<(i64, i64, i64)> = Vec::new();
    join.run(|k, bp, pp| out.push((k, bp, pp))).expect("run");
    out.sort_unstable();
    (out, build_spilled, probe_spilled)
}

#[test]
fn spilling_join_matches_in_memory_bit_identically() {
    // Reference: unbudgeted — must never touch disk.
    let (ref_out, b0, p0) = run(usize::MAX);
    assert_eq!((b0, p0), (0, 0), "reference run must not spill");

    // Independent oracle: matching probe keys are 0..BUILD_NKEYS, each
    // recurs NPROBE/PROBE_NKEYS times and matches NBUILD/BUILD_NKEYS build
    // rows → that many (unique) pairs.
    let expected =
        (BUILD_NKEYS as usize) * (NPROBE / PROBE_NKEYS as usize) * (NBUILD / BUILD_NKEYS as usize);
    assert_eq!(ref_out.len(), expected, "matched-pair count vs formula");

    // Spilled: 256 KiB budget forces both sides through disk-backed
    // partitions (build keeps an in-memory remainder too, so the join
    // merges spilled + resident build rows).
    let (spill_out, bs, ps) = run(256 * 1024);
    eprintln!("spill_join: build spilled {bs}/{NBUILD}, probe spilled {ps}/{NPROBE}");
    assert!(
        bs > 0 && ps > 0,
        "both sides must spill: build={bs} probe={ps}"
    );
    assert_eq!(
        spill_out, ref_out,
        "spilled join must equal the in-memory join exactly"
    );
}
