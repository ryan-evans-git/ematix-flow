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

### SF=10 (complete; 21 queries — Q05 excluded due to Polars panic)

Median ± σ across 20 trials after 5 warmups. M3 Pro, all engines
in-process. ematix is on the post-Σ.P main.

| Q   | ematix (ms) | DuckDB (ms) | Polars (ms) | ematix vs DuckDB |
|-----|-------:|-------:|-------:|:---|
| Q01 | 274.07 | 232.23 | 342.33 | **−18%** loss |
| Q02 | 48.17  | 43.53  | 428.41 | **−11%** loss |
| Q03 | 154.65 | 143.37 | 560.56 | **−8%** loss |
| Q04 | 81.71  | 86.80  | 270.26 | +6% win |
| Q05 | (skip) | (skip) | PANIC  | (excluded) |
| Q06 | 78.73  | 72.67  | 60.51  | **−8%** loss |
| Q07 | 274.59 | 138.63 | 1294.52 | **−98%** loss (1.98×) |
| Q08 | 201.68 | 173.61 | 1154.14 | **−16%** loss |
| Q09 | 294.79 | 308.69 | 436.70 | +5% win |
| Q10 | 243.35 | 409.21 | 5625.75 | **+68% win** |
| Q11 | 26.24  | 24.82  | 33.23  | −6% (all 3 = 0 rows; spec quirk) |
| Q12 | 100.33 | 105.94 | 110.50 | +6% win |
| Q13 | 134.56 | 267.76 | 409.16 | **+99% win** |
| Q14 | 88.38  | 138.74 | 93.21  | **+57% win** |
| Q15 | 79.34  | 85.82  | 66.63  | +8% win (vs DuckDB; Polars wins outright) |
| Q16 | 43.14  | 63.27  | 171.47 | **+47% win** |
| Q17 | 307.54 | 163.40 | 450.17 | **−88%** loss (1.88×) |
| Q18 | 696.82 | 224.97 | 592.65 | **−210%** loss (3.10×) ⭐ BIGGEST GAP |
| Q19 | 136.02 | 189.06 | 1193.02 | **+39% win** |
| Q20 | 139.30 | 137.49 | 267.19 | −1% tied |
| Q21 | 447.36 | 411.79 | 41009.60 | **−9%** loss |
| Q22 | 62.59  | 129.87 | 111.91 | **+107% win** |

### SF=10 geomean

| Pair | Geomean ratio | Speedup |
|---|---|---|
| ematix / DuckDB | **0.9920** | ematix is 1.008× faster (essentially tied) |
| ematix / Polars | 0.3458 | ematix is 2.89× faster |

**Outright wins**: ematix=9, DuckDB=10, Polars=2.

### Where the losses concentrate

Sorted by absolute ms gap (= what closing would shift the geomean most):

| Q | Δ ms | Ratio | Hypothesis |
|---|---:|---:|---|
| Q18 | +472 | 3.10× | scalar-subquery + giant hash join + big group-by. **#1 lever.** |
| Q17 | +144 | 1.88× | correlated subquery on lineitem — plan shape suspected |
| Q07 | +136 | 1.98× | 5-way join + nation lookups; partitioning shuffle suspected |
| Q01 | +42  | 1.18× | full-lineitem agg; decode parallelism |
| Q21 | +36  | 1.09× | 4-way join + 2 anti-joins; hash join |
| Q08 | +28  | 1.16× | 7-way join; same flavor as Q07 |
| Q03 | +11  | 1.08× | filter + 3-way join; hash join probe |
| Q06 | +6   | 1.08× | scan-bound; surprising loss (was win at SF=1) |
| Q02 | +5   | 1.11× | small joins; borderline noise |
| Q11 | +1   | 1.06× | zero-row result; spec quirk, not perf |

**If Q18 alone closes to DuckDB parity (224ms), ematix/duckdb geomean
becomes ~0.86 — a 14% lead over DuckDB.**

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
