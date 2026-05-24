# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 20 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf1`.

Each cell is **median ms ± σ** across 20 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 28.31 ± 0.54 | 47.11 ± 0.66 | 39.32 ± 1.52 | ematix-flow |
| Q02  | 8.18 ± 1.08 | 22.25 ± 0.65 | 47.99 ± 1.48 | ematix-flow |
| Q03  | 13.42 ± 0.60 | 36.74 ± 1.12 | 47.43 ± 5.97 | ematix-flow |
| Q04  | 11.54 ± 0.41 | 24.84 ± 0.47 | 24.82 ± 0.62 | ematix-flow |
| Q05  | 20.13 ± 0.95 | 37.24 ± 1.79 | 11141.29 ± 571.59 | ematix-flow |
| Q06  | 13.65 ± 257.77 | 13.08 ± 0.35 | 10.08 ± 0.35 | Polars |
| Q07  | 30.08 ± 6.50 | 36.47 ± 0.58 | 117.64 ± 15.59 | ematix-flow |
| Q08  | 16.50 ± 3.23 | 43.08 ± 0.90 | 94.99 ± 6.20 | ematix-flow |
| Q09  | 18.46 ± 0.44 | 64.49 ± 5.55 | 48.73 ± 4.54 | ematix-flow |
| Q10  | 26.81 ± 1.24 | 75.36 ± 2.62 | 135.18 ± 25.62 | ematix-flow |
| Q11  | 5.60 ± 1.69 | 12.64 ± 1.09 | 9.34 ± 1.24 | ematix-flow |
| Q12  | 15.75 ± 0.96 | 30.21 ± 3.04 | 19.31 ± 0.91 | ematix-flow |
| Q13  | 18.67 ± 0.33 | 162.70 ± 1.99 | 121.65 ± 5.09 | ematix-flow |
| Q14  | 12.07 ± 1.00 | 26.04 ± 1.77 | 12.64 ± 0.65 | ematix-flow |
| Q15  | 12.06 ± 0.66 | 17.03 ± 1.14 | 11.57 ± 0.53 | Polars |
| Q16  | 9.82 ± 0.57 | 28.00 ± 1.42 | 22.03 ± 0.59 | ematix-flow |
| Q17  | 26.73 ± 1.33 | 33.71 ± 1.62 | 42.78 ± 5.08 | ematix-flow |
| Q18  | 39.53 ± 0.95 | 56.68 ± 2.52 | 58.61 ± 4.79 | ematix-flow |
| Q19  | 18.50 ± 0.52 | 41.01 ± 1.35 | 109.08 ± 11.38 | ematix-flow |
| Q20  | 17.43 ± 5.25 | 44.07 ± 3.91 | 23.15 ± 0.67 | ematix-flow |
| Q21  | 38.17 ± 4.77 | 99.12 ± 3.66 | 741.03 ± 24.33 | ematix-flow |
| Q22  | 8.62 ± 0.38 | 25.00 ± 2.77 | 13.01 ± 2.02 | ematix-flow |

## Wins

- **ematix-flow**: 20
- **DuckDB**: 0
- **Polars**: 2

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
