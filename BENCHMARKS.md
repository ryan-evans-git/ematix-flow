# TPC-H triangulation — 2026-05-26 refresh (post-Σ.AG.7, plan cache default ON)

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries, 20 timed trials after 3 warmups, single-machine (M3 Pro, 14 cores).

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`. Bench reaches the milestone perf config from a **bare invocation** as of Σ.AG.7 — every winning lever (plan cache, push-semi, RG decode cache, runtime bloom sideband, Robin Hood SumF64, all-tables-Emat, etc.) defaults ON.

Each cell is **median ms ± σ** across 20 trials. "—" means the engine couldn't parse / execute the query (dialect gap) or was skipped.

---

## SF=1 (3 engines)

Data: `examples/tpch/data/sf1` (~150 MB).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 26.28 ± 0.63 | 47.40 ± 2.09 | 40.42 ± 5.17 | ematix-flow |
| Q02  | 4.75 ± 1.59 | 23.66 ± 0.95 | 55.35 ± 4.92 | ematix-flow |
| Q03  | 11.90 ± 0.42 | 41.51 ± 7.07 | 53.17 ± 3.59 | ematix-flow |
| Q04  | 12.21 ± 0.48 | 26.54 ± 2.09 | 26.28 ± 1.23 | ematix-flow |
| Q05  | 20.20 ± 1.63 | 37.10 ± 1.27 | 10404.93 ± 629.86 | ematix-flow |
| Q06  | 13.05 ± 2.18 | 12.81 ± 0.24 | 9.78 ± 0.26 | Polars |
| Q07  | 28.18 ± 0.84 | 34.35 ± 1.50 | 111.30 ± 8.07 | ematix-flow |
| Q08  | 13.33 ± 0.74 | 41.61 ± 0.66 | 90.41 ± 5.90 | ematix-flow |
| Q09  | 16.85 ± 0.48 | 63.12 ± 2.12 | 55.16 ± 21.85 | ematix-flow |
| Q10  | 25.84 ± 7.74 | 71.32 ± 4.40 | 104.67 ± 14.30 | ematix-flow |
| Q11  | 3.61 ± 0.69 | 12.36 ± 0.79 | 9.27 ± 0.75 | ematix-flow |
| Q12  | 13.74 ± 0.50 | 27.76 ± 1.60 | 18.62 ± 3.33 | ematix-flow |
| Q13  | 8.34 ± 0.59 | 146.97 ± 1.99 | 118.16 ± 2.82 | ematix-flow |
| Q14  | 10.10 ± 0.53 | 24.52 ± 2.22 | 12.24 ± 0.45 | ematix-flow |
| Q15  | 11.78 ± 0.67 | 16.43 ± 1.00 | 11.20 ± 0.30 | Polars |
| Q16  | 7.88 ± 0.46 | 26.92 ± 1.19 | 21.72 ± 1.86 | ematix-flow |
| Q17  | 22.15 ± 1.67 | 31.79 ± 2.60 | 38.59 ± 1.35 | ematix-flow |
| Q18  | 37.02 ± 2.92 | 54.07 ± 4.94 | 53.63 ± 3.76 | ematix-flow |
| Q19  | 15.30 ± 0.52 | 37.81 ± 1.24 | 99.38 ± 7.41 | ematix-flow |
| Q20  | 15.89 ± 0.78 | 41.37 ± 3.48 | 22.36 ± 0.56 | ematix-flow |
| Q21  | 34.57 ± 1.06 | 91.31 ± 4.90 | 677.87 ± 28.86 | ematix-flow |
| Q22  | 8.18 ± 0.30 | 23.25 ± 0.43 | 12.36 ± 0.48 | ematix-flow |

**Wins:** ematix-flow **20**, DuckDB 0, Polars 2 (Q06, Q15)

> **Q06 footnote.** In-battery Q06 is noisy (13.05 ± 2.18) — running it in isolation under the same 20/3 config gives 9.03 ± 0.67 ms with the plan cache on, narrowly beating Polars (9.78 ± 0.26). The in-battery measurement is contaminated by preceding queries (likely thermal / RG-cache eviction). We keep the in-battery number in the table because every other cell has the same provenance.

---

## SF=10 (2 engines — Polars skipped)

Data: `examples/tpch/data/sf10` (~10 GB).

Polars is omitted at SF=10: Q05 panics with "Polars' maximum length reached" (needs `bigidx` build), and Q21 takes ~41 s/trial — distorts the geomean and aren't apples-to-apples without a bigidx Polars install.

| Query | ematix-flow | DuckDB | Best |
|------:|------------:|-------:|:-----|
| Q01  | 235.38 ± 10.52 | 237.23 ± 4.14 | ematix-flow |
| Q02  | 29.37 ± 1.48 | 45.30 ± 2.25 | ematix-flow |
| Q03  | 145.74 ± 6.50 | 145.58 ± 4.28 | DuckDB |
| Q04  | 54.30 ± 2.27 | 89.44 ± 2.45 | ematix-flow |
| Q05  | 186.25 ± 6.92 | 148.97 ± 4.49 | DuckDB |
| Q06  | 76.08 ± 4.30 | 74.62 ± 2.31 | DuckDB |
| Q07  | 157.48 ± 5.68 | 142.25 ± 3.62 | DuckDB |
| Q08  | 188.76 ± 5.16 | 175.86 ± 4.61 | DuckDB |
| Q09  | 273.41 ± 7.76 | 313.32 ± 4.27 | ematix-flow |
| Q10  | 231.97 ± 9.02 | 408.76 ± 7.78 | ematix-flow |
| Q11  | 11.59 ± 0.64 | 30.70 ± 2.94 | ematix-flow |
| Q12  | 87.64 ± 2.95 | 115.52 ± 2.90 | ematix-flow |
| Q13  | 95.81 ± 7.10 | 273.22 ± 6.15 | ematix-flow |
| Q14  | 85.49 ± 4.87 | 137.88 ± 2.69 | ematix-flow |
| Q15  | 77.28 ± 3.80 | 95.80 ± 3.95 | ematix-flow |
| Q16  | 50.01 ± 1.53 | 68.51 ± 2.49 | ematix-flow |
| Q17  | 175.26 ± 6.99 | 165.93 ± 5.47 | DuckDB |
| Q18  | 243.70 ± 6.49 | 229.81 ± 4.58 | DuckDB |
| Q19  | 138.72 ± 13.54 | 210.30 ± 4.55 | ematix-flow |
| Q20  | 131.47 ± 4.34 | 150.94 ± 4.39 | ematix-flow |
| Q21  | 311.87 ± 13.64 | 443.53 ± 7.84 | ematix-flow |
| Q22  | 23.35 ± 2.20 | 149.95 ± 6.75 | ematix-flow |

**Wins:** ematix-flow **15**, DuckDB 7 (Q03 a tie at 145.74 vs 145.58, scored to DuckDB)

Headline ratios:
- **Q22 −84%** (23.35 vs 149.95 ms — 6.4× faster)
- **Q13 −65%** (95.81 vs 273.22)
- **Q11 −62%** (11.59 vs 30.70)
- **Q10 −43%** (231.97 vs 408.76)
- **Q14 −38%**, **Q04 −39%**, **Q19 −34%**, **Q22 −84%**

Remaining DuckDB-faster shapes: Q03 (tie), Q05/Q07/Q08 (composite-key joins), Q06 (decoder), Q17/Q18 (correlated subquery).

---

## Reproduce

```sh
# SF=1 (3-engine, ~3 min):
cargo build --release -p ematix-flow-core --example tpch_triangulation_bench --features triangulation
TPCH_DATA_DIR=examples/tpch/data/sf1 \
TPCH_TRIALS=20 TPCH_WARMUPS=3 \
./target/release/examples/tpch_triangulation_bench

# SF=10 (2-engine, ~25 min):
TPCH_DATA_DIR=examples/tpch/data/sf10 \
TPCH_TRIALS=20 TPCH_WARMUPS=3 \
TPCH_SKIP_POLARS=1 \
./target/release/examples/tpch_triangulation_bench
```

A/B opt-OUTs (set to `0` to disable any of the milestone levers): `EMAT_PLAN_CACHE`, `EMAT_PUSH_SEMI`, `EMAT_RH_SUM_F64`, `EMAT_RT_BLOOM_SIDEBAND`, `EMAT_RT_BLOOM_INNER_JOIN`, `EMAT_L9_REQUIRE_FILTERED_BUILD`, `EMAT_ALL_TABLES_EMAT`, `EMAT_RG_DECODE_CACHE`.

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Helps queries with selective filters on dict/PLAIN-decodable scalar columns; effectively no-op on aggregate-heavy queries with low filter selectivity.
- Polars's SQL frontend rejects several TPC-H canonical shapes (implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns). We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files; the bench feeds Polars the polars variant when present. Translations are semantically equivalent.
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the full milestone optimizer rule set.
- SF=10 noise band: per-query medians can shift ±5–15% across back-to-back runs from thermal effects (both engines move together). Treat 13–17 ematix wins as the steady-state SF=10 range.
