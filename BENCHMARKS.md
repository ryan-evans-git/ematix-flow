# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 3 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf1`.

Each cell is **median ms ± σ** across 3 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 31.97 ± 0.43 | 46.25 ± 0.23 | 42.51 ± 1.56 | ematix-flow |
| Q02  | 9.98 ± 0.16 | 22.45 ± 1.71 | 50.70 ± 1.22 | ematix-flow |
| Q03  | 14.67 ± 1.95 | 34.18 ± 0.46 | 50.14 ± 0.57 | ematix-flow |
| Q04  | 13.52 ± 0.33 | 23.28 ± 0.26 | 25.72 ± 0.83 | ematix-flow |
| Q05  | 23.73 ± 1.25 | 32.80 ± 0.78 | 15386.77 ± 1587.26 | ematix-flow |
| Q06  | 16.77 ± 0.72 | 23.39 ± 2.79 | 15.70 ± 1.24 | Polars |
| Q07  | 37.88 ± 3.59 | 39.71 ± 5.87 | 134.62 ± 25.51 | ematix-flow |
| Q08  | 25.45 ± 0.52 | 40.43 ± 2.88 | 101.68 ± 15.73 | ematix-flow |
| Q09  | 30.11 ± 1.51 | 63.95 ± 1.24 | 52.61 ± 4.43 | ematix-flow |
| Q10  | 37.26 ± 13.68 | 72.97 ± 4.01 | 128.90 ± 22.00 | ematix-flow |
| Q11  | 7.86 ± 0.34 | 10.95 ± 0.63 | 10.44 ± 0.63 | ematix-flow |
| Q12  | 15.27 ± 0.01 | 25.18 ± 0.39 | 20.67 ± 1.12 | ematix-flow |
| Q13  | 42.47 ± 0.87 | 158.95 ± 0.65 | 118.67 ± 4.51 | ematix-flow |
| Q14  | 12.21 ± 1.67 | 25.04 ± 0.63 | 13.18 ± 0.89 | ematix-flow |
| Q15  | 17.08 ± 1.13 | 15.45 ± 0.73 | 11.59 ± 0.80 | Polars |
| Q16  | 9.36 ± 0.29 | 26.09 ± 1.01 | 22.78 ± 0.70 | ematix-flow |
| Q17  | 43.96 ± 0.76 | 32.37 ± 2.49 | 48.85 ± 7.03 | DuckDB |
| Q18  | 57.97 ± 3.08 | 53.95 ± 0.12 | 72.04 ± 14.51 | DuckDB |
| Q19  | 19.42 ± 0.38 | 37.61 ± 1.24 | 113.72 ± 4.36 | ematix-flow |
| Q20  | 20.05 ± 2.00 | 35.57 ± 0.92 | 24.47 ± 0.77 | ematix-flow |
| Q21  | 44.49 ± 3.13 | 86.17 ± 8.56 | 760.58 ± 29.74 | ematix-flow |
| Q22  | 25.31 ± 7.05 | 24.25 ± 8.61 | 14.29 ± 0.72 | Polars |

## Wins

- **ematix-flow**: 17
- **DuckDB**: 2
- **Polars**: 3

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
