# Σ.AC — Final 20-trial Rebench (2026-05-26)

Closing bench for the Σ.X→Σ.AD work series. Both scale factors run at
release cadence: 20 timed trials after 3 warmups, single machine,
14 partitions, `EMAT_AGG_SEMI=1` (Σ.U Phase 1 active).
**`EMAT_DIM_PUSH=0`** (Σ.AD default-off — see its module docs for the
rationale).

## Summary

| Scale | ematix-flow wins | DuckDB wins | Polars wins | Geomean ratio ematix/DuckDB |
|------:|-----------------:|------------:|------------:|----------------------------:|
| SF=1  | **20** / 22 | 0 / 22 | 2 / 22 (Q06, Q15) | well below 1.0 |
| SF=10 | **14** / 22 |  6 / 22 | 2 / 22 (Q06, Q15) | **0.76** (hand-computed) |

All 22 queries pass row-and-value validation against DuckDB ground
truth at both scale factors (`tpch_validate` PASS, no FAIL/SKIP).

Compared to historical milestone (0.738 / 17 wins at SF=10):
- 3-win regression at SF=10 (Q03, Q05, Q07/Q08/Q17/Q18 flips).
- Geomean +3 pp worse (0.76 vs 0.738) — within typical run-to-run
  variance on a single-machine bench.
- Pattern matches the diagnosis in [docs/POST_BENCH_PERF_BACKLOG.md](POST_BENCH_PERF_BACKLOG.md):
  no plan-level fix is available for the SF=10 losses below the
  multi-day-infra threshold; the gaps are kernel/decode or magic-set
  rewriting.

## SF=1 detail (20 trials × 3 warmups)

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 26.36 ± 0.62 | 46.74 ± 1.93 | 36.96 ± 1.27 | ematix-flow |
| Q02  | 7.14 ± 0.43 | 22.21 ± 1.22 | 46.22 ± 2.02 | ematix-flow |
| Q03  | 12.85 ± 2.03 | 35.02 ± 0.82 | 44.92 ± 1.70 | ematix-flow |
| Q04  | 11.02 ± 0.25 | 23.99 ± 1.92 | 23.32 ± 0.63 | ematix-flow |
| Q05  | 19.60 ± 1.72 | 34.18 ± 1.57 | 11080.16 ± 449.73 | ematix-flow |
| Q06  | 10.93 ± 0.78 | 12.82 ± 0.32 | 10.44 ± 1.93 | Polars |
| Q07  | 30.93 ± 0.67 | 36.44 ± 5.33 | 123.25 ± 8.06 | ematix-flow |
| Q08  | 17.19 ± 3.49 | 44.41 ± 2.19 | 99.99 ± 5.41 | ematix-flow |
| Q09  | 19.16 ± 0.65 | 65.66 ± 1.63 | 48.34 ± 3.95 | ematix-flow |
| Q10  | 27.47 ± 1.04 | 75.16 ± 7.38 | 119.93 ± 23.44 | ematix-flow |
| Q11  | 5.86 ± 1.30 | 13.74 ± 3.96 | 9.92 ± 0.67 | ematix-flow |
| Q12  | 15.65 ± 0.47 | 31.02 ± 2.39 | 19.95 ± 0.96 | ematix-flow |
| Q13  | 10.87 ± 0.67 | 166.74 ± 2.55 | 121.70 ± 2.98 | ematix-flow |
| Q14  | 13.32 ± 0.80 | 26.42 ± 1.09 | 13.81 ± 0.72 | ematix-flow |
| Q15  | 13.05 ± 0.71 | 18.10 ± 2.11 | 11.70 ± 0.45 | Polars |
| Q16  | 9.89 ± 0.48 | 29.05 ± 3.77 | 22.19 ± 0.59 | ematix-flow |
| Q17  | 17.37 ± 0.64 | 35.13 ± 3.59 | 45.29 ± 3.73 | ematix-flow |
| Q18  | 42.24 ± 1.61 | 58.54 ± 2.56 | 58.48 ± 5.49 | ematix-flow |
| Q19  | 19.00 ± 0.86 | 41.06 ± 4.09 | 110.29 ± 8.17 | ematix-flow |
| Q20  | 17.25 ± 0.74 | 45.60 ± 3.45 | 23.51 ± 0.87 | ematix-flow |
| Q21  | 36.11 ± 3.63 | 98.59 ± 4.25 | 1155.62 ± 233.07 | ematix-flow |
| Q22  | 23.28 ± 13.43 | 43.59 ± 7.18 | 35.38 ± 21.58 | ematix-flow |

## SF=10 detail (20 trials × 3 warmups)

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 232.43 ± 14.60 | 238.69 ± 3.62 | 334.55 ± 18.67 | ematix-flow |
| Q02  | 30.57 ± 10.86 | 45.18 ± 6.75 | 409.45 ± 28.04 | ematix-flow |
| Q03  | 146.29 ± 7.66 | 144.02 ± 5.78 | 549.78 ± 11.28 | DuckDB |
| Q04  | 54.41 ± 17.39 | 87.01 ± 2.66 | 265.32 ± 17.14 | ematix-flow |
| Q05  | 187.02 ± 6.67 | 144.82 ± 3.55 | — | DuckDB |
| Q06  | 77.40 ± 5.40 | 70.88 ± 1.34 | 62.50 ± 5.54 | Polars |
| Q07  | 156.36 ± 6.44 | 140.55 ± 3.35 | 1348.14 ± 42.58 | DuckDB |
| Q08  | 193.92 ± 7.35 | 175.03 ± 5.12 | 1183.64 ± 30.65 | DuckDB |
| Q09  | 273.89 ± 26.51 | 311.36 ± 8.56 | 435.29 ± 18.50 | ematix-flow |
| Q10  | 236.54 ± 9.81 | 405.69 ± 12.67 | 3917.55 ± 380.30 | ematix-flow |
| Q11  | 11.83 ± 15.22 | 26.18 ± 2.82 | 32.55 ± 2.32 | ematix-flow |
| Q12  | 88.93 ± 4.60 | 103.64 ± 2.17 | 110.60 ± 3.65 | ematix-flow |
| Q13  | 98.12 ± 5.76 | 265.27 ± 4.05 | 407.50 ± 22.35 | ematix-flow |
| Q14  | 86.23 ± 3.71 | 135.59 ± 3.26 | 91.86 ± 1.72 | ematix-flow |
| Q15  | 76.91 ± 3.54 | 87.76 ± 1.91 | 66.24 ± 2.15 | Polars |
| Q16  | 51.75 ± 1.96 | 65.41 ± 3.43 | 168.38 ± 7.00 | ematix-flow |
| Q17  | 185.49 ± 13.46 | 157.35 ± 4.52 | 433.71 ± 15.97 | DuckDB |
| Q18  | 248.89 ± 9.33 | 228.99 ± 7.61 | 597.14 ± 18.75 | DuckDB |
| Q19  | 131.75 ± 4.31 | 192.37 ± 2.91 | 1206.10 ± 40.74 | ematix-flow |
| Q20  | 125.33 ± 28.55 | 140.23 ± 5.63 | 268.57 ± 11.07 | ematix-flow |
| Q21  | 295.74 ± 9.04 | 406.68 ± 6.97 | 34347.37 ± 4359.83 | ematix-flow |
| Q22  | 24.10 ± 31.46 | 127.19 ± 5.10 | 107.50 ± 6.86 | ematix-flow |

## Deferred future work

- **Π.16** (ematix-parquet sibling repo): Q06/Q15 SF=10 decode gap to Polars.
  - **Key finding** (2026-05-26 probe): Polars uses literally the same
    `snap::raw::Decoder` we do (verified at
    `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/polars-parquet-0.52.0/src/parquet/compression.rs:176-187`).
    The Snappy decode call site is functionally identical. The
    16-18 ms gap is therefore NOT in Snappy itself — it's spread
    across page-decode / filter-build / projection-decode phases.
  - Q06 SF=10 stage breakdown (our engine, from `q06_sf10_breakdown`):
    filter (3 predicates → bitmap) 46.55 ms + projection+agg 37 ms ≈
    83 ms total. Polars does the same in 64 ms.
  - Closing the gap needs samply/perf profiling to find the actual
    hotspot, then targeted SIMD or micro-opts in ematix-parquet.
    Multi-day work; deferred.
- **Σ.AE**: physical-plan follow-up to Σ.AD so dim-join pushdown
  doesn't regress Q07/Q21 SF=10 wall-time. The structurally-correct
  plan inserts a `CoalescePartitionsExec` between two CollectLeft
  joins; the new shape needs partition-aware emit OR static-IN-list
  pushdown to avoid the coalesce.
- **Magic-set rewriting** for Q05/Q07 logical layer (multi-day infra).
- **Bushy join planner** for Q08 (multi-week).
