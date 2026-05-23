# Σ.Q — Single-node parity status

**Mission**: ematix-flow is the fastest single-node TPC-H engine across
the data-fits-in-RAM regime. Win the SF=1 geomean (already there at
~1.79× DuckDB) **and** win the SF=10 geomean (currently losing several
join-heavy queries). Product positioning: one tool that adapts as data
grows; users opt into distributed when they want extra parallelism,
but ematix is the single-node default.

**Branch**: `perf/sigma-q-single-node-parity`
**Scope start**: 2026-05-22, post-merge of #144 (Σ.P CSE + AWS harness)
**Owner-set defaults** (override at any wake):

- **Pass gate per lever**: SF=10 geomean(ematix/duckdb) drops ≥3% AND
  SF=1 geomean(ematix/duckdb) stays within ±2% of current ~0.56 lead.
- **Per-query SLO**: no query regresses >10% at either SF. Marginal
  regressions (<5%) treated as noise.
- **Tradeoff tolerance**: if a lever helps SF=10 but hurts SF=1, gate
  it via runtime shape detection ([[shape-catalog-autotune-direction]])
  rather than rejecting outright.
- **Stop conditions**: hit SF=10 parity-or-better AND maintain SF=1
  lead, OR exhaust hypotheses, OR hit a design fork that needs
  operator input.

---

## Baseline (2026-05-22, 20 trials × 5 warmups, M3 Pro)

### SF=1 (post-Σ.P, from main)

```
Q01 27.80   Q07 27.04   Q13 41.87   Q19 17.69
Q02  9.65   Q08 20.37   Q14 11.20   Q20 15.36
Q03 13.89   Q09 33.08   Q15 11.39   Q21 41.11
Q04 12.58   Q10 29.74   Q16  8.61   Q22  8.26
Q05 21.29   Q11  7.84   Q17 36.67
Q06 10.75   Q12 14.21   Q18 49.25
```

- geomean(ematix/duckdb) = **0.559** (we are 1.79× faster than DuckDB)
- geomean(ematix/polars) = **0.362** (we are 2.76× faster than Polars)
- Wins (outright): 19 / 22; beats DuckDB 21/22; beats Polars 20/22

### SF=10 (in progress as of this commit)

```
Q01 273.02   Q07 ?    Q13 ?    Q19 ?
Q02  49.77   Q08 ?    Q14 ?    Q20 139.30
Q03 159.73   Q09 ?    Q15 ?    Q21 447.36 (Polars panic'd, skipped)
Q04  81.12   Q10 ?    Q16 ?    Q22 ?
Q05 196.89   Q11 ?    Q17 307.54
Q06  ?       Q12 ?    Q18 696.82
```

(table will be filled when the in-progress bench completes; partial
values from screen-scrape during run)

### Initial known losses at SF=10 (vs DuckDB)

| Q | ematix ms | DuckDB ms | Δ ematix/duckdb | Initial hypothesis |
|---|---|---|---|---|
| Q01 | 273.02 | 236.48 | 1.15 | full-lineitem scan-bound; decode parallelism |
| Q02 | 49.77 | 43.21 | 1.15 | small joins; optimizer overhead borderline |
| Q03 | 159.73 | 142.99 | 1.12 | hash join build cost |
| Q05 | 196.89 | 141.35 | 1.39 | 6-way join shuffle |
| Q17 | 307.54 | 163.40 | 1.88 | correlated subquery shape |
| Q18 | 696.82 | 224.97 | 3.10 | huge hash table — biggest single loss |

Q18 is the most extreme; flamegraph spike (Σ.Q.0) targets it first.

---

## Lever inventory

Status legend:
- 🟢 WIN — landed + bench-validated SF=1 + SF=10
- 🟡 IN PROGRESS
- 🔵 PROPOSED — not yet tried
- 🔴 NEG — tried, regressed, reverted
- ⚫ REJECTED — explicit reason, won't try

| ID | Lever | Status | Notes |
|---|---|---|---|
| L1 | Multi-column parallel decode (task #397) | 🔵 PROPOSED | Decode-bound queries (Q01/Q03/Q06/Q14) likely biggest beneficiary at SF=10. Known parquet-rs/ematix-parquet mutex bottleneck at ~1.8× scaling cap. |
| L2 | Bloom-on-build for HashJoinExec (single-node) | 🔵 PROPOSED | Σ.J.2 already built the bloom infra for distributed. Wire it into single-node HashJoin too. Q18/Q21 should win 2-5×. |
| L3 | Custom RobinHoodHashJoin | 🔵 PROPOSED | Biggest potential, biggest risk. Defer until profile justifies. |
| L4 | Q17 correlated-subquery plan diagnosis | 🔵 PROPOSED | 1.88× loss; might be plan shape rather than execution. |
| L5 | Q05 6-way join order | 🔵 PROPOSED | DataFusion's join reorderer might be picking sub-optimal order for 6-table at SF=10. |
| L6 | (placeholders — add as profile reveals) | 🔵 PROPOSED |  |

---

## Experiment log

Each lever experiment gets a subsection: hypothesis, design, code touched,
per-query bench numbers SF=1 + SF=10, decision (commit/revert/gate).

### Σ.Q.0 — Profile spike on Q18 SF=10

**Status**: 🟡 IN PROGRESS — harness built, awaiting bench completion to run.

**Tool**: `samply record ./target/release/examples/sigma_q_profile_loop`
with `Q=18`. New example at
`crates/ematix-flow-core/examples/sigma_q_profile_loop.rs` mirrors the
triangulation bench's session (preset rules + dict-aware Emat).

**Expected findings**: (to be filled)

---

## Methodology notes

- **Bench tool**: `cargo run --release -p ematix-flow-core --features triangulation --example tpch_triangulation_bench`
- **20×5 trials** for publishable medians. 3-7-trial benches have ±15%
  swings on sub-15ms queries — see [[optimizer-codegen-sensitivity]] +
  earlier 2026-05-22 noise analysis.
- **Polars Q05 SF=10 panics** — `chunked_array/ops/chunkops.rs:152: Polars'
  maximum length reached. Consider compiling with 'bigidx' feature.`
  Run with `TPCH_QUERIES=1,2,3,4,6,…22` to skip. Real Polars limitation,
  not a bench bug.
- **Q21 polars-side at SF=10** also runs ~25× ematix's number; flagged
  but doesn't block the run.

---

## Decision log

Records design choices the operator may want to revisit.

(none yet)
