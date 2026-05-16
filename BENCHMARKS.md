# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf1`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 78.51 ± 0.40 | 49.20 ± 2.07 | 48.51 ± 2.52 | Polars |
| Q02  | 11.01 ± 0.30 | 20.63 ± 1.81 | 65.84 ± 3.10 | ematix-flow |
| Q03  | 21.15 ± 0.75 | 36.62 ± 1.42 | 66.62 ± 4.07 | ematix-flow |
| Q04  | 16.73 ± 0.43 | 24.79 ± 1.86 | 30.77 ± 1.78 | ematix-flow |
| Q05  | 37.91 ± 2.28 | 37.46 ± 1.11 | 13439.24 ± 138.89 | DuckDB |
| Q06  | 13.74 ± 2.88 | 13.79 ± 646.29 | 12.79 ± 1.10 | Polars |
| Q07  | 74.51 ± 13.13 | 38.08 ± 2.28 | 151.87 ± 12.38 | DuckDB |
| Q08  | 35.75 ± 0.82 | 41.04 ± 2.17 | 119.70 ± 14.38 | ematix-flow |
| Q09  | 40.78 ± 2.46 | 65.68 ± 4.32 | 66.10 ± 2.01 | ematix-flow |
| Q10  | 37.79 ± 0.65 | 81.02 ± 5.08 | 138.36 ± 3.58 | ematix-flow |
| Q11  | 9.29 ± 2.45 | 13.43 ± 3.38 | 11.59 ± 0.92 | ematix-flow |
| Q12  | 23.55 ± 0.38 | 29.20 ± 2.55 | 22.34 ± 1.49 | Polars |
| Q13  | 44.60 ± 1.24 | 164.96 ± 4.62 | 127.78 ± 3.17 | ematix-flow |
| Q14  | 15.74 ± 0.33 | 26.04 ± 2.51 | 17.18 ± 0.95 | ematix-flow |
| Q15  | 18.42 ± 1.01 | 17.44 ± 2.47 | 13.82 ± 1.07 | Polars |
| Q16  | 12.65 ± 0.70 | 28.54 ± 1.64 | 24.09 ± 3.31 | ematix-flow |
| Q17  | 46.81 ± 0.78 | 39.78 ± 4.46 | 53.82 ± 9.02 | DuckDB |
| Q18  | 70.62 ± 3.47 | 55.45 ± 5.17 | 64.22 ± 2.09 | DuckDB |
| Q19  | 60.77 ± 0.88 | 39.24 ± 6.02 | 145.63 ± 14.33 | DuckDB |
| Q20  | 23.52 ± 1.18 | 43.95 ± 3.06 | 28.35 ± 1.71 | ematix-flow |
| Q21  | 72.57 ± 10.23 | 102.35 ± 15.18 | 893.20 ± 147.35 | ematix-flow |
| Q22  | 16.19 ± 2.23 | 25.48 ± 0.61 | 16.30 ± 3.45 | ematix-flow |

## Wins

- **ematix-flow**: 13
- **DuckDB**: 5
- **Polars**: 4

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
