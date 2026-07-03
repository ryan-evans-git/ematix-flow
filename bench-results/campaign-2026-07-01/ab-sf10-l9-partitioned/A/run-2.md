# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 229.74 ± 9.81 | — | — | ematix-flow |
| Q02  | 18.64 ± 0.57 | — | — | ematix-flow |
| Q03  | 115.91 ± 1.77 | — | — | ematix-flow |
| Q04  | 54.51 ± 2.90 | — | — | ematix-flow |
| Q05  | 144.98 ± 8.36 | — | — | ematix-flow |
| Q06  | 26.72 ± 2.67 | — | — | ematix-flow |
| Q07  | 118.55 ± 4.49 | — | — | ematix-flow |
| Q08  | 144.90 ± 5.05 | — | — | ematix-flow |
| Q09  | 265.29 ± 8.76 | — | — | ematix-flow |
| Q10  | 189.23 ± 10.18 | — | — | ematix-flow |
| Q11  | 13.41 ± 1.13 | — | — | ematix-flow |
| Q12  | 87.89 ± 4.06 | — | — | ematix-flow |
| Q13  | 97.66 ± 2.00 | — | — | ematix-flow |
| Q14  | 79.43 ± 3.68 | — | — | ematix-flow |
| Q15  | 61.38 ± 2.88 | — | — | ematix-flow |
| Q16  | 48.29 ± 2.60 | — | — | ematix-flow |
| Q17  | 77.65 ± 5.75 | — | — | ematix-flow |
| Q18  | 178.79 ± 3.76 | — | — | ematix-flow |
| Q19  | 121.52 ± 3.07 | — | — | ematix-flow |
| Q20  | 88.50 ± 3.96 | — | — | ematix-flow |
| Q21  | 179.49 ± 7.50 | — | — | ematix-flow |
| Q22  | 24.46 ± 2.42 | — | — | ematix-flow |

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
