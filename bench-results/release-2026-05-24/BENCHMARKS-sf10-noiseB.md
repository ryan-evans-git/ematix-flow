# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 20 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 20 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 247.18 ± 5.98 | 238.45 ± 3.80 | 342.13 ± 25.24 | DuckDB |
| Q02  | 34.78 ± 9.86 | 45.21 ± 1.31 | 418.20 ± 26.85 | ematix-flow |
| Q03  | 144.88 ± 13.75 | 145.32 ± 2.47 | 557.36 ± 9.84 | ematix-flow |
| Q04  | 55.49 ± 18.63 | 87.30 ± 2.91 | 270.06 ± 11.38 | ematix-flow |
| Q05  | 187.74 ± 10.72 | 143.71 ± 3.68 | — | DuckDB |
| Q06  | 74.88 ± 4.31 | 72.06 ± 2.22 | 63.52 ± 3.87 | Polars |
| Q07  | 158.41 ± 7.23 | 139.83 ± 5.75 | 1329.70 ± 39.31 | DuckDB |
| Q08  | 194.53 ± 6.60 | 176.80 ± 5.08 | 1179.38 ± 22.81 | DuckDB |
| Q09  | 279.41 ± 32.95 | 318.75 ± 9.13 | 428.74 ± 16.92 | ematix-flow |
| Q10  | 253.52 ± 7.55 | 406.15 ± 5.77 | 4111.21 ± 225.09 | ematix-flow |
| Q11  | 11.58 ± 3.59 | 28.28 ± 2.41 | 32.69 ± 2.99 | ematix-flow |
| Q12  | 102.69 ± 9.04 | 107.76 ± 9.78 | 112.57 ± 4.74 | ematix-flow |
| Q13  | 117.74 ± 6.38 | 265.82 ± 6.65 | 414.52 ± 17.81 | ematix-flow |
| Q14  | 90.60 ± 4.58 | 137.52 ± 3.98 | 92.62 ± 1.31 | ematix-flow |
| Q15  | 79.61 ± 4.08 | 86.92 ± 3.72 | 63.97 ± 2.32 | Polars |
| Q16  | 53.66 ± 1.34 | 65.98 ± 3.39 | 173.32 ± 7.79 | ematix-flow |
| Q17  | 182.69 ± 11.46 | 159.95 ± 5.02 | 438.21 ± 15.78 | DuckDB |
| Q18  | 246.64 ± 8.82 | 225.58 ± 6.97 | 611.28 ± 22.57 | DuckDB |
| Q19  | 144.57 ± 8.41 | 210.15 ± 5.63 | 1244.95 ± 21.61 | ematix-flow |
| Q20  | 139.08 ± 22.82 | 150.79 ± 5.90 | 281.13 ± 8.27 | ematix-flow |
| Q21  | 331.27 ± 11.35 | 436.24 ± 7.82 | 33104.91 ± 2789.95 | ematix-flow |
| Q22  | 29.69 ± 0.86 | 127.51 ± 14.23 | 107.61 ± 5.39 | ematix-flow |

## Wins

- **ematix-flow**: 14
- **DuckDB**: 6
- **Polars**: 2

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

- **Q05 / Polars**: polars join: task 23060 panicked with message "Polars' maximum length reached. Consider compiling with 'big…
