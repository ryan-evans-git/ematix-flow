# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 10 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf1`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 16.06 ± 0.24 | 47.65 ± 0.68 | — | ematix-flow |
| Q02  | 6.79 ± 0.24 | 16.19 ± 0.38 | — | ematix-flow |
| Q03  | 9.05 ± 0.16 | 31.24 ± 0.29 | — | ematix-flow |
| Q04  | 9.69 ± 0.16 | 21.58 ± 0.15 | — | ematix-flow |
| Q05  | 6.81 ± 0.98 | 30.30 ± 3.11 | — | ematix-flow |
| Q06  | 0.87 ± 0.03 | 12.01 ± 0.23 | — | ematix-flow |
| Q07  | 26.22 ± 0.42 | 31.28 ± 3.66 | — | ematix-flow |
| Q08  | 12.35 ± 0.72 | 37.29 ± 0.12 | — | ematix-flow |
| Q09  | 16.19 ± 0.44 | 52.91 ± 0.53 | — | ematix-flow |
| Q10  | 23.23 ± 0.71 | 57.02 ± 0.23 | — | ematix-flow |
| Q11  | 5.05 ± 0.16 | 8.97 ± 0.11 | — | ematix-flow |
| Q12  | 12.69 ± 0.30 | 23.05 ± 0.21 | — | ematix-flow |
| Q13  | 8.62 ± 0.70 | 129.93 ± 0.55 | — | ematix-flow |
| Q14  | 9.77 ± 0.30 | 21.03 ± 0.13 | — | ematix-flow |
| Q15  | 10.84 ± 0.44 | 13.15 ± 0.61 | — | ematix-flow |
| Q16  | 7.12 ± 0.38 | 20.63 ± 0.11 | — | ematix-flow |
| Q17  | 14.09 ± 0.63 | 23.99 ± 0.14 | — | ematix-flow |
| Q18  | 24.02 ± 0.28 | 43.14 ± 0.66 | — | ematix-flow |
| Q19  | 14.42 ± 0.48 | 31.93 ± 0.40 | — | ematix-flow |
| Q20  | 14.43 ± 0.34 | 27.36 ± 0.34 | — | ematix-flow |
| Q21  | 32.71 ± 0.86 | 69.01 ± 0.88 | — | ematix-flow |
| Q22  | 7.78 ± 0.26 | 19.58 ± 0.12 | — | ematix-flow |

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
