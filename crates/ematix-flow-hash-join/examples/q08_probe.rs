//! Q08-shape probe microbench — de-risk gate for HJ.3/HJ.4.
//!
//! Q08's `part⋈lineitem` join (SF=10): build = 13.45K filtered p_partkeys,
//! probe = 59.99M l_partkey values, ~0.67% hit → ~403K matches. DataFusion's
//! HashJoinExec does this in 1.13s of SUMMED compute (≈18.8 ns/probe). This
//! measures the L13 RobinHoodHashJoinI64Table doing the same probe-match work
//! SINGLE-THREADED (summed-compute-equivalent), as a LOWER BOUND on a custom
//! operator (excludes the output gather of the 4 payload columns).
//!
//! Decision: if probe-match alone isn't materially under ~1.13s, the operator
//! can't move Q08 wall and we pivot to the gather kernel (ADR Option A).
//!
//!   cargo run --release -p ematix-flow-hash-join --example q08_probe
use ematix_flow_hash_join::RobinHoodHashJoinI64Table;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;

fn main() {
    // Q08 SF=10 cardinalities. l_partkey ∈ [1, 2_000_000]; 13.45K parts survive
    // the p_type filter, so a uniform probe key hits a build key ~0.67% of the
    // time → ~403K matches out of 60M.
    let n_build = 13_450usize;
    let n_probe = 60_000_000usize;
    let domain = 2_000_000i64;

    let mut rb = StdRng::seed_from_u64(0xC0FFEE);
    let build: Vec<i64> = (0..n_build).map(|_| rb.random_range(0..domain)).collect();
    let mut rp = StdRng::seed_from_u64(0xDEADBEEF);
    let probe: Vec<i64> = (0..n_probe).map(|_| rp.random_range(0..domain)).collect();

    // Build cost (one-off; report separately — it's tiny here).
    let mut build_ms = f64::MAX;
    let mut probe_best = f64::MAX;
    let mut matches = 0usize;
    let trials = 5;
    for _ in 0..trials {
        let s = Instant::now();
        let mut t = RobinHoodHashJoinI64Table::with_capacity(n_build);
        t.insert_batch(&build, None, 0);
        build_ms = build_ms.min(s.elapsed().as_secs_f64() * 1000.0);

        let mut out: Vec<ematix_flow_hash_join::ProbeMatch> = Vec::with_capacity(n_probe / 100);
        let s = Instant::now();
        t.probe_batch(&probe, None, 0, &mut out);
        let ms = s.elapsed().as_secs_f64() * 1000.0;
        probe_best = probe_best.min(ms);
        matches = out.len();
    }

    let ns_per_probe = probe_best * 1e6 / n_probe as f64;
    println!("Q08-shape probe-match (single-thread, best of {trials}):");
    println!("  build {n_build} keys: {build_ms:.2} ms");
    println!(
        "  probe {n_probe} keys → {matches} matches: {probe_best:.1} ms  ({ns_per_probe:.2} ns/probe, {:.0} M probes/s)",
        n_probe as f64 / probe_best / 1000.0
    );
    println!("  DataFusion reference (summed compute): ~1130 ms (~18.8 ns/probe)");
    println!(
        "  → kernel speedup vs DataFusion probe: {:.2}x",
        1130.0 / probe_best
    );
}
