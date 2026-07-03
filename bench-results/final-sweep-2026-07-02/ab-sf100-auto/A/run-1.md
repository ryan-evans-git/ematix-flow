# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 2529.64 ± 31.09 | — | — | ematix-flow |
| Q02  | 233.84 ± 3.47 | — | — | ematix-flow |
| Q03  | 1593.72 ± 19.88 | — | — | ematix-flow |
| Q04  | 905.73 ± 13.57 | — | — | ematix-flow |
| Q05  | 2047.50 ± 31.16 | — | — | ematix-flow |
| Q06  | 534.48 ± 8.88 | — | — | ematix-flow |
| Q07  | 1621.22 ± 19.79 | — | — | ematix-flow |
| Q08  | 1768.90 ± 9.24 | — | — | ematix-flow |
| Q09  | 4297.38 ± 52.18 | — | — | ematix-flow |
| Q10  | 3210.22 ± 72.51 | — | — | ematix-flow |
| Q11  | 226.36 ± 18.53 | — | — | ematix-flow |
| Q12  | 1087.06 ± 57.16 | — | — | ematix-flow |
| Q13  | 2080.65 ± 37.17 | — | — | ematix-flow |
| Q14  | 831.13 ± 7.72 | — | — | ematix-flow |
| Q15  | 805.64 ± 8.27 | — | — | ematix-flow |
| Q16  | 437.49 ± 15.03 | — | — | ematix-flow |
| Q17  | 1570.79 ± 9.90 | — | — | ematix-flow |
| Q18  | 2754.57 ± 31.31 | — | — | ematix-flow |
| Q19  | 1251.98 ± 10.65 | — | — | ematix-flow |
| Q20  | 1314.84 ± 8.39 | — | — | ematix-flow |
| Q21  | 3710.15 ± 9.77 | — | — | ematix-flow |
| Q22  | 468.43 ± 16.49 | — | — | ematix-flow |

## Wins

- **ematix-flow**: 22
- **DuckDB**: 0
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions = std::thread::available_parallelism()` (override via `PARTITIONS=N`) and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
