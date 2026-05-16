# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf1`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 78.19 ± 2.85 | 46.55 ± 1.20 | 45.95 ± 3.15 | Polars |
| Q02  | 10.32 ± 0.27 | 20.38 ± 3.28 | 56.43 ± 1.57 | ematix-flow |
| Q03  | 20.38 ± 1.22 | 33.85 ± 2.13 | 56.12 ± 3.27 | ematix-flow |
| Q04  | 15.78 ± 0.55 | 23.84 ± 2.74 | 27.57 ± 0.57 | ematix-flow |
| Q05  | 34.09 ± 0.57 | 34.29 ± 4.33 | 13959.84 ± 1638.74 | ematix-flow |
| Q06  | 11.67 ± 0.55 | 15.35 ± 2.06 | 13.95 ± 663.47 | ematix-flow |
| Q07  | 75.56 ± 15.62 | 38.19 ± 3.22 | 178.72 ± 21.10 | DuckDB |
| Q08  | 35.66 ± 0.89 | 47.58 ± 3.12 | 136.28 ± 11.88 | ematix-flow |
| Q09  | 50.16 ± 13.81 | 71.09 ± 4.96 | 71.85 ± 10.20 | ematix-flow |
| Q10  | 39.73 ± 1.07 | 77.23 ± 3.29 | 145.32 ± 9.26 | ematix-flow |
| Q11  | 9.37 ± 1.98 | 11.64 ± 0.63 | 11.00 ± 1.42 | ematix-flow |
| Q12  | 23.34 ± 1.00 | 29.06 ± 2.33 | 21.91 ± 1.40 | Polars |
| Q13  | 44.73 ± 1.07 | 165.46 ± 55.39 | 210.17 ± 31.58 | ematix-flow |
| Q14  | 19.45 ± 2.62 | 31.20 ± 10.59 | 23.71 ± 1.01 | ematix-flow |
| Q15  | 27.70 ± 2.14 | 28.40 ± 4.94 | 20.95 ± 0.93 | Polars |
| Q16  | 18.29 ± 1.34 | 52.82 ± 5.40 | 38.14 ± 1.58 | ematix-flow |
| Q17  | 80.96 ± 34.35 | 59.92 ± 2.02 | 110.50 ± 42.51 | DuckDB |
| Q18  | 157.55 ± 45.84 | 59.51 ± 18.11 | 67.81 ± 1.75 | DuckDB |
| Q19  | 99.76 ± 27.36 | 85.65 ± 3.59 | 341.46 ± 81.09 | DuckDB |
| Q20  | 44.22 ± 5.99 | 77.82 ± 4.43 | 70.68 ± 23.44 | ematix-flow |
| Q21  | 75.48 ± 2.08 | 98.97 ± 16.94 | 829.37 ± 23.82 | ematix-flow |
| Q22  | 8.90 ± 0.46 | 25.38 ± 5.38 | 14.82 ± 0.66 | ematix-flow |

## Wins

- **ematix-flow**: 15
- **DuckDB**: 4
- **Polars**: 3

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
