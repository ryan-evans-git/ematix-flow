# Scheduler arc — 2026-07-03 (registry-driven pool sizing)

The residual SF=10 multi-stream gap (~10% behind DuckDB after the
partitions productization) was NOT a work-stealing/morsel-scheduler
deficit. Diagnostics showed it was two more per-process thread pools
still sized at `available_parallelism()`, undoing the registry's
reduction under multi-process load:

1. Reader column-decode fan-out (`available_parallelism()/partitions`
   = 7 scoped threads per partition at partitions=2 → still 14 decode
   threads per process).
2. Rayon global pool (intra-chunk page decode) at all cores per process.

## Diagnostics (SF=10, 10 streams, ematix only; `sched-diag-2026-07-03/`)

| Config | QPH |
|---|---:|
| partitions auto (=2), defaults | 26,882 |
| + RAYON_NUM_THREADS=2 | 27,674 |
| + reader budget 1 | **29,271** (diag-day DuckDB ref: 29,426) |
| reader budget 2 / 4 | 27,652 / 26,532 (monotonic contention) |
| partitions 3 / 4 / 7 | 25,028 / 23,279 / 16,736 — "plan wide" is strictly worse |

## Product change (`feat/registry-driven-pool-sizing`)

All three pools now size from ONE number — the registry core share
S = clamp(cores/live, 2, cores): target_partitions = S (existing),
reader budget = max(1, S/partitions), rayon pool = S (lazy, once per
process; `RAYON_NUM_THREADS` and `EMAT_RAYON_BUDGET`/`
EMAT_READER_PARALLELISM_BUDGET` escapes). Solo is bit-identical to the
old formulas by construction (pinned by tests).

## Final official re-baseline (both engines, pure auto, same session)

| Config | ematix QPH | DuckDB QPH | ratio |
|---|---:|---:|---|
| SF=10, 1 stream | **27,462** | 21,405 | 1.28× |
| SF=10, 10 streams | **28,333** | 26,463 | **1.07×** |
| SF=10, 100 streams | **26,073** | 25,824 | 1.01× (parity-to-win) |
| SF=100, 1 stream | **2,212** | 1,814 | 1.22× |
| SF=100, 10 streams | **1,581** | 1,118 | **1.41×** |

ematix leads every measured scale × concurrency configuration.
Absolute QPH drifts day-to-day (thermal/page-cache); co-measured
ratios are the verdict. SF=10 s100 is nominal-win/parity — the one
configuration without clear daylight.

Raw: `sf10-final/`, `sf100-final/`, diagnostics in
`../sched-diag-2026-07-03/`, env.json throughout.
