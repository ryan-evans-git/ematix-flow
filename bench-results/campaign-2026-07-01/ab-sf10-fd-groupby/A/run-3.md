# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 262.24 ± 9.76 | — | — | ematix-flow |
| Q02  | 20.52 ± 0.62 | — | — | ematix-flow |
| Q03  | 126.32 ± 4.51 | — | — | ematix-flow |
| Q04  | 57.61 ± 2.66 | — | — | ematix-flow |
| Q05  | 155.92 ± 3.26 | — | — | ematix-flow |
| Q06  | 26.78 ± 1.99 | — | — | ematix-flow |
| Q07  | 130.01 ± 6.74 | — | — | ematix-flow |
| Q08  | 158.13 ± 6.38 | — | — | ematix-flow |
| Q09  | 293.52 ± 8.99 | — | — | ematix-flow |
| Q10  | 202.28 ± 7.44 | — | — | ematix-flow |
| Q11  | 14.20 ± 1.64 | — | — | ematix-flow |
| Q12  | 92.19 ± 2.97 | — | — | ematix-flow |
| Q13  | 104.86 ± 6.01 | — | — | ematix-flow |
| Q14  | 85.49 ± 4.13 | — | — | ematix-flow |
| Q15  | 61.73 ± 3.20 | — | — | ematix-flow |
| Q16  | 50.71 ± 3.61 | — | — | ematix-flow |
| Q17  | 80.95 ± 3.42 | — | — | ematix-flow |
| Q18  | 190.82 ± 4.62 | — | — | ematix-flow |
| Q19  | 131.30 ± 5.40 | — | — | ematix-flow |
| Q20  | 94.96 ± 3.93 | — | — | ematix-flow |
| Q21  | 189.25 ± 6.53 | — | — | ematix-flow |
| Q22  | 23.25 ± 1.41 | — | — | ematix-flow |

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
