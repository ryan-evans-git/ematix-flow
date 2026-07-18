//! P2 adaptive re-plan kill-gate.
//!
//! The join's physical strategy is chosen at the build breaker from the
//! **observed** build size, overriding the planner's estimate — the
//! capability a static DataFusion plan can't express (it commits before
//! seeing data). Proven in both directions, each against an independent
//! in-memory oracle:
//!
//! - a wrong "build is huge" estimate is **downgraded** to the cheap
//!   in-memory join once the build is observed to be small;
//! - a wrong "build is tiny" estimate is **upgraded** to the spilling join
//!   once the build overflows the budget — the OOM a static plan would hit;
//! - correct estimates don't re-plan;
//! - every strategy produces a bit-identical result.

use std::collections::HashMap;

use ematix_flow_engine::adaptive::{AdaptiveHashJoin, Strategy};
use ematix_flow_engine::chunk::DataChunk;
use ematix_flow_engine::vector::Vector;

const BUDGET: usize = 64 * 1024; // ≈ 4096 rows resident
const PART_BITS: u32 = 8;
const BUILD_NKEYS: i64 = 4_000;
const PROBE_NKEYS: i64 = 4_000;
const SMALL_BUILD: usize = 2_000; // 32 KB  < budget → in-memory
const LARGE_BUILD: usize = 40_000; // 640 KB > budget → partitioned/spill
const PROBE: usize = 8_000;
const CHUNK: usize = 4_096;

fn build_row(i: usize) -> (i64, i64) {
    ((i as i64) % BUILD_NKEYS, i as i64)
}
fn probe_row(i: usize) -> (i64, i64) {
    ((i as i64) % PROBE_NKEYS, i as i64)
}

fn feed(j: &mut AdaptiveHashJoin, n: usize, rowf: fn(usize) -> (i64, i64), build: bool) {
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
            j.consume_build(&chunk, 0, 1).expect("consume_build");
        } else {
            j.consume_probe(&chunk, 0, 1).expect("consume_probe");
        }
        i = end;
    }
}

struct Outcome {
    out: Vec<(i64, i64, i64)>,
    plan: Strategy,
    chosen: Strategy,
    replanned: bool,
    build_spilled: u64,
}

fn run_join(estimate_bytes: usize, build_rows: usize) -> Outcome {
    let mut j = AdaptiveHashJoin::new(BUDGET, PART_BITS, estimate_bytes);
    feed(&mut j, build_rows, build_row, true);
    feed(&mut j, PROBE, probe_row, false);
    let plan = j.plan_would_choose();
    let chosen = j.chosen();
    let replanned = j.replanned();
    let build_spilled = j.build_spilled();
    let mut out = Vec::new();
    j.run(|k, b, p| out.push((k, b, p))).expect("run");
    out.sort_unstable();
    Outcome {
        out,
        plan,
        chosen,
        replanned,
        build_spilled,
    }
}

/// Independent hand-rolled in-memory join — the oracle every strategy must
/// match for the given build size.
fn oracle(build_rows: usize) -> Vec<(i64, i64, i64)> {
    let mut ht: HashMap<i64, Vec<i64>> = HashMap::new();
    for i in 0..build_rows {
        let (k, p) = build_row(i);
        ht.entry(k).or_default().push(p);
    }
    let mut out = Vec::new();
    for i in 0..PROBE {
        let (k, pp) = probe_row(i);
        if let Some(bps) = ht.get(&k) {
            for &bp in bps {
                out.push((k, bp, pp));
            }
        }
    }
    out.sort_unstable();
    out
}

#[test]
fn overestimate_downgrades_to_in_memory() {
    // Planner says "build is 10 MB" (→ Partitioned), actual build is small.
    let o = run_join(10 * 1024 * 1024, SMALL_BUILD);
    assert_eq!(
        o.plan,
        Strategy::Partitioned,
        "estimate implies Partitioned"
    );
    assert_eq!(
        o.chosen,
        Strategy::InMemory,
        "runtime observed small → InMemory"
    );
    assert!(o.replanned, "wrong estimate must be overridden");
    assert_eq!(o.build_spilled, 0, "in-memory path never spills");
    assert_eq!(o.out, oracle(SMALL_BUILD), "result bit-identical to oracle");
}

#[test]
fn underestimate_upgrades_to_spilling() {
    // Planner says "build is 1 KB" (→ InMemory), actual build overflows.
    let o = run_join(1024, LARGE_BUILD);
    assert_eq!(o.plan, Strategy::InMemory, "estimate implies InMemory");
    assert_eq!(
        o.chosen,
        Strategy::Partitioned,
        "runtime observed overflow → Partitioned"
    );
    assert!(
        o.replanned,
        "wrong estimate must be overridden (avoids OOM)"
    );
    assert!(o.build_spilled > 0, "spilling path must reach disk");
    assert_eq!(o.out, oracle(LARGE_BUILD), "result bit-identical to oracle");
}

#[test]
fn correct_estimates_do_not_replan() {
    // Correct "large" estimate + large build → Partitioned, no re-plan.
    let big = run_join(10 * 1024 * 1024, LARGE_BUILD);
    assert_eq!(big.plan, Strategy::Partitioned);
    assert_eq!(big.chosen, Strategy::Partitioned);
    assert!(!big.replanned);
    assert!(big.build_spilled > 0);
    assert_eq!(big.out, oracle(LARGE_BUILD));

    // Correct "small" estimate + small build → InMemory, no re-plan.
    let small = run_join(1024, SMALL_BUILD);
    assert_eq!(small.plan, Strategy::InMemory);
    assert_eq!(small.chosen, Strategy::InMemory);
    assert!(!small.replanned);
    assert_eq!(small.build_spilled, 0);
    assert_eq!(small.out, oracle(SMALL_BUILD));
}

#[test]
fn oracle_is_non_trivial() {
    // Guard against "every path agrees on a degenerate result". Large build
    // fully covers the probe key domain: every probe row matches
    // LARGE_BUILD/BUILD_NKEYS build rows → 8000 × 10 = 80 000 pairs.
    assert_eq!(
        oracle(LARGE_BUILD).len(),
        PROBE * (LARGE_BUILD / BUILD_NKEYS as usize)
    );
    // Small build covers only the lower half of the probe key domain, one
    // build row per key → half the probe rows match, once each.
    assert_eq!(oracle(SMALL_BUILD).len(), PROBE / 2);
}
