# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf1`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 79.95 ± 0.89 | 48.38 ± 0.45 | 47.80 ± 0.95 | Polars |
| Q02  | 10.69 ± 0.77 | 21.10 ± 2.48 | 65.02 ± 4.96 | ematix-flow |
| Q03  | 22.81 ± 1.54 | 36.66 ± 3.04 | 75.91 ± 25.28 | ematix-flow |
| Q04  | 16.59 ± 4.44 | 25.87 ± 1.28 | 39.59 ± 8.32 | ematix-flow |
| Q05  | 39.89 ± 2.43 | 37.84 ± 0.88 | 13961.47 ± 196.42 | DuckDB |
| Q06  | 15.64 ± 2.70 | 16.27 ± 744.42 | 11.90 ± 1.12 | Polars |
| Q07  | 77.63 ± 5.28 | 40.68 ± 2.53 | 155.09 ± 24.75 | DuckDB |
| Q08  | 34.43 ± 1.99 | 45.90 ± 2.42 | 122.58 ± 4.62 | ematix-flow |
| Q09  | 42.97 ± 1.88 | 71.21 ± 3.40 | 65.13 ± 8.17 | ematix-flow |
| Q10  | 39.67 ± 1.42 | 74.51 ± 2.55 | 147.82 ± 6.14 | ematix-flow |
| Q11  | 8.94 ± 1.26 | 10.58 ± 0.61 | 10.53 ± 0.89 | ematix-flow |
| Q12  | 24.79 ± 0.59 | 29.45 ± 1.80 | 21.49 ± 5.21 | Polars |
| Q13  | 43.39 ± 1.23 | 165.10 ± 2.10 | 125.65 ± 3.31 | ematix-flow |
| Q14  | 17.14 ± 2.09 | 30.14 ± 2.54 | 16.19 ± 0.46 | Polars |
| Q15  | 17.65 ± 1.01 | 17.75 ± 1.05 | 13.19 ± 0.71 | Polars |
| Q16  | 10.91 ± 1.43 | 29.18 ± 14.33 | 24.10 ± 4.17 | ematix-flow |
| Q17  | 46.56 ± 1.69 | 33.01 ± 1.98 | 52.99 ± 6.96 | DuckDB |
| Q18  | 73.24 ± 1.46 | 64.73 ± 6.59 | 67.22 ± 3.03 | DuckDB |
| Q19  | 68.17 ± 4.27 | 41.95 ± 2.64 | 130.62 ± 3.88 | DuckDB |
| Q20  | 21.52 ± 13.04 | 39.43 ± 4.24 | 29.21 ± 0.68 | ematix-flow |
| Q21  | 75.89 ± 4.96 | 129.50 ± 8.52 | 893.53 ± 24.71 | ematix-flow |
| Q22  | 9.59 ± 4.84 | 30.52 ± 2.44 | 15.67 ± 1.23 | ematix-flow |

## Wins

- **ematix-flow**: 12
- **DuckDB**: 5
- **Polars**: 5

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
