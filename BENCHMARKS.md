# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf1`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 80.86 ± 0.68 | 54.63 ± 4.95 | 48.60 ± 1.41 | Polars |
| Q02  | 10.83 ± 0.25 | 19.80 ± 2.26 | 62.69 ± 24.10 | ematix-flow |
| Q03  | 27.22 ± 1.72 | 43.66 ± 4.44 | 85.35 ± 9.20 | ematix-flow |
| Q04  | 20.36 ± 2.44 | 32.33 ± 1.59 | 40.73 ± 5.79 | ematix-flow |
| Q05  | 42.07 ± 2.97 | 51.25 ± 6.58 | 14083.27 ± 324.62 | ematix-flow |
| Q06  | 17.57 ± 3.51 | 15.72 ± 2.80 | 12.04 ± 89.60 | Polars |
| Q07  | 73.59 ± 4.36 | 38.06 ± 3.41 | 205.06 ± 14.75 | DuckDB |
| Q08  | 43.65 ± 2.58 | 56.25 ± 4.20 | 153.63 ± 19.76 | ematix-flow |
| Q09  | 54.25 ± 2.79 | 72.86 ± 6.23 | 72.05 ± 2.06 | ematix-flow |
| Q10  | 44.66 ± 0.97 | 85.32 ± 3.61 | 174.49 ± 35.55 | ematix-flow |
| Q11  | 11.68 ± 1.19 | 15.93 ± 1.84 | 11.72 ± 0.27 | ematix-flow |
| Q12  | 27.61 ± 0.90 | 38.29 ± 2.65 | 26.07 ± 0.90 | Polars |
| Q13  | 49.54 ± 2.70 | 164.26 ± 6.33 | 122.99 ± 2.18 | ematix-flow |
| Q14  | 15.14 ± 0.84 | 31.49 ± 3.39 | 18.62 ± 0.73 | ematix-flow |
| Q15  | 18.73 ± 0.74 | 21.21 ± 1.51 | 13.15 ± 0.62 | Polars |
| Q16  | 13.75 ± 2.71 | 34.20 ± 1.98 | 41.89 ± 4.24 | ematix-flow |
| Q17  | 51.77 ± 1.32 | 34.55 ± 3.51 | 60.13 ± 3.58 | DuckDB |
| Q18  | 85.13 ± 8.39 | 73.86 ± 7.27 | 74.39 ± 10.47 | DuckDB |
| Q19  | 73.55 ± 6.27 | 47.54 ± 1.01 | 182.20 ± 12.33 | DuckDB |
| Q20  | 26.12 ± 1.97 | 45.00 ± 1.28 | 31.35 ± 1.01 | ematix-flow |
| Q21  | 78.92 ± 8.78 | 101.83 ± 2.13 | 1112.38 ± 44.63 | ematix-flow |
| Q22  | 12.87 ± 0.64 | 32.10 ± 0.38 | 16.68 ± 0.71 | ematix-flow |

## Wins

- **ematix-flow**: 14
- **DuckDB**: 4
- **Polars**: 4

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
