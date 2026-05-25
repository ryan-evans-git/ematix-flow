//! Story 2.1 / 2.6 — kernel baseline bench at 1M and 15M build
//! cardinalities. This file reports numbers; Story 2.6 wires the
//! ≥1.3× vs stock HashJoinExec gate (separate harness — this one
//! is a pure kernel microbench).
//!
//! Run:
//!     cargo bench -p ematix-flow-hash-join --bench kernel_benchmark_baseline

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ematix_flow_hash_join::RobinHoodHashJoinI64Table;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

fn synth_keys(seed: u64, n: usize, distinct: i64) -> Vec<i64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n).map(|_| rng.random_range(0..distinct)).collect()
}

fn build_and_probe(
    n_build: usize,
    n_probe: usize,
    distinct_build: i64,
    distinct_probe: i64,
) -> Vec<ematix_flow_hash_join::ProbeMatch> {
    let build_keys = synth_keys(0xC0FFEE, n_build, distinct_build);
    let probe_keys = synth_keys(0xDEADBEEF, n_probe, distinct_probe);

    let mut table = RobinHoodHashJoinI64Table::with_capacity(n_build);
    table.insert_batch(&build_keys, None, 0);

    // Heuristic capacity for the output Vec: 1× probe count for
    // typical FK joins. Real numbers will vary by selectivity; this
    // is a pure-kernel measurement.
    let mut out = Vec::with_capacity(n_probe);
    table.probe_batch(&probe_keys, None, 0, &mut out);
    out
}

fn bench_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join_i64_inner");

    // Two canonical cardinalities from CURRENT.md Phase 2 acceptance
    // gate: 1M and 15M build rows. Distinct count = 1/4 build rows
    // (Photon-class realistic GROUP BY skew).
    for &(n_build, n_probe) in &[(1_000_000_usize, 1_000_000_usize), (15_000_000, 15_000_000)] {
        let distinct_build = (n_build / 4) as i64;
        let distinct_probe = distinct_build;
        group.throughput(Throughput::Elements(n_probe as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("build={n_build}/probe={n_probe}")),
            &(n_build, n_probe, distinct_build, distinct_probe),
            |b, &(nb, np, db, dp)| {
                b.iter(|| build_and_probe(nb, np, db, dp));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_join);
criterion_main!(benches);
