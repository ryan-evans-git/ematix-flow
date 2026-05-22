# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 20 timed trials after 5 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf1`.

Each cell is **median ms ± σ** across 20 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 27.80 ± 0.54 | 44.76 ± 0.21 | 36.46 ± 1.22 | ematix-flow |
| Q02  | 9.65 ± 0.65 | 17.64 ± 1.57 | 45.93 ± 2.97 | ematix-flow |
| Q03  | 13.89 ± 0.78 | 32.36 ± 1.96 | 44.97 ± 1.98 | ematix-flow |
| Q04  | 12.58 ± 0.42 | 22.15 ± 2.26 | 24.04 ± 1.03 | ematix-flow |
| Q05  | 21.29 ± 2.76 | 30.47 ± 3.05 | 10760.71 ± 616.70 | ematix-flow |
| Q06  | 10.75 ± 177.05 | 12.08 ± 2.40 | 9.97 ± 0.36 | Polars |
| Q07  | 27.04 ± 1.45 | 31.60 ± 4.41 | 109.74 ± 7.59 | ematix-flow |
| Q08  | 20.37 ± 5.10 | 38.77 ± 1.47 | 97.57 ± 13.89 | ematix-flow |
| Q09  | 33.08 ± 10.77 | 61.82 ± 8.29 | 47.70 ± 5.69 | ematix-flow |
| Q10  | 29.74 ± 1.74 | 66.62 ± 6.73 | 109.17 ± 5.83 | ematix-flow |
| Q11  | 7.84 ± 0.56 | 9.97 ± 3.86 | 9.17 ± 0.50 | ematix-flow |
| Q12  | 14.21 ± 0.34 | 24.26 ± 0.90 | 18.37 ± 0.55 | ematix-flow |
| Q13  | 41.87 ± 0.80 | 144.56 ± 3.29 | 116.36 ± 4.22 | ematix-flow |
| Q14  | 11.20 ± 2.08 | 24.36 ± 2.55 | 12.22 ± 0.67 | ematix-flow |
| Q15  | 11.39 ± 0.55 | 16.22 ± 0.98 | 11.27 ± 0.26 | Polars |
| Q16  | 8.61 ± 0.24 | 26.48 ± 1.92 | 20.68 ± 0.31 | ematix-flow |
| Q17  | 36.67 ± 6.85 | 30.05 ± 1.76 | 39.27 ± 1.54 | DuckDB |
| Q18  | 49.25 ± 2.72 | 53.30 ± 4.41 | 55.35 ± 6.30 | ematix-flow |
| Q19  | 17.69 ± 1.15 | 37.55 ± 3.92 | 100.97 ± 10.87 | ematix-flow |
| Q20  | 15.36 ± 0.59 | 38.52 ± 2.12 | 21.90 ± 0.57 | ematix-flow |
| Q21  | 41.11 ± 6.39 | 84.42 ± 3.21 | 691.68 ± 25.84 | ematix-flow |
| Q22  | 8.26 ± 3.33 | 23.48 ± 1.94 | 12.52 ± 0.42 | ematix-flow |

## Wins

- **ematix-flow**: 19
- **DuckDB**: 1
- **Polars**: 2

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
