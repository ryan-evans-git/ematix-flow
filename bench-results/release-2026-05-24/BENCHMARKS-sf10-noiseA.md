# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 20 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 20 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 266.30 ± 12.55 | 248.57 ± 6.08 | 374.84 ± 23.23 | DuckDB |
| Q02  | 35.76 ± 7.41 | 49.37 ± 3.14 | 421.71 ± 33.71 | ematix-flow |
| Q03  | 155.21 ± 10.90 | 154.37 ± 3.81 | 626.94 ± 31.36 | DuckDB |
| Q04  | 58.65 ± 7.48 | 92.57 ± 4.00 | 277.20 ± 18.21 | ematix-flow |
| Q05  | 203.46 ± 13.31 | 146.40 ± 5.84 | — | DuckDB |
| Q06  | 75.38 ± 3.58 | 70.44 ± 1.36 | 59.99 ± 4.89 | Polars |
| Q07  | 159.04 ± 5.83 | 144.06 ± 5.96 | 1371.31 ± 53.88 | DuckDB |
| Q08  | 201.08 ± 13.02 | 181.79 ± 3.18 | 1314.79 ± 37.53 | DuckDB |
| Q09  | 353.78 ± 27.05 | 368.02 ± 13.67 | 491.43 ± 18.75 | ematix-flow |
| Q10  | 300.43 ± 8.98 | 451.05 ± 11.10 | 4627.00 ± 1454.30 | ematix-flow |
| Q11  | 13.53 ± 9.52 | 28.53 ± 1.96 | 41.66 ± 10.13 | ematix-flow |
| Q12  | 127.32 ± 7.22 | 129.72 ± 4.68 | 143.16 ± 8.49 | ematix-flow |
| Q13  | 150.86 ± 10.93 | 330.08 ± 12.20 | 433.27 ± 17.92 | ematix-flow |
| Q14  | 107.74 ± 6.76 | 165.27 ± 5.41 | 106.92 ± 5.90 | Polars |
| Q15  | 173.68 ± 58.27 | 259.78 ± 46.27 | 227.48 ± 97.25 | ematix-flow |
| Q16  | 75.84 ± 6.89 | 74.35 ± 4.79 | 190.91 ± 28.42 | DuckDB |
| Q17  | 243.73 ± 13.24 | 210.39 ± 13.92 | 582.62 ± 24.03 | DuckDB |
| Q18  | 346.06 ± 14.49 | 287.95 ± 9.69 | 742.70 ± 150.83 | DuckDB |
| Q19  | 179.51 ± 12.11 | 222.91 ± 8.22 | 1434.94 ± 42.47 | ematix-flow |
| Q20  | 165.87 ± 34.17 | 158.18 ± 5.04 | 283.30 ± 12.25 | DuckDB |
| Q21  | 389.47 ± 12.73 | 453.50 ± 14.39 | 35655.73 ± 3945.99 | ematix-flow |
| Q22  | 31.25 ± 39.23 | 129.05 ± 2.41 | 106.89 ± 6.01 | ematix-flow |

## Wins

- **ematix-flow**: 11
- **DuckDB**: 9
- **Polars**: 2

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

- **Q05 / Polars**: polars join: task 23060 panicked with message "Polars' maximum length reached. Consider compiling with 'big…
