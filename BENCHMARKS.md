# TPC-H SF=10 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 20 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 20 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 232.43 ± 14.60 | 238.69 ± 3.62 | 334.55 ± 18.67 | ematix-flow |
| Q02  | 30.57 ± 10.86 | 45.18 ± 6.75 | 409.45 ± 28.04 | ematix-flow |
| Q03  | 146.29 ± 7.66 | 144.02 ± 5.78 | 549.78 ± 11.28 | DuckDB |
| Q04  | 54.41 ± 17.39 | 87.01 ± 2.66 | 265.32 ± 17.14 | ematix-flow |
| Q05  | 187.02 ± 6.67 | 144.82 ± 3.55 | — | DuckDB |
| Q06  | 77.40 ± 5.40 | 70.88 ± 1.34 | 62.50 ± 5.54 | Polars |
| Q07  | 156.36 ± 6.44 | 140.55 ± 3.35 | 1348.14 ± 42.58 | DuckDB |
| Q08  | 193.92 ± 7.35 | 175.03 ± 5.12 | 1183.64 ± 30.65 | DuckDB |
| Q09  | 273.89 ± 26.51 | 311.36 ± 8.56 | 435.29 ± 18.50 | ematix-flow |
| Q10  | 236.54 ± 9.81 | 405.69 ± 12.67 | 3917.55 ± 380.30 | ematix-flow |
| Q11  | 11.83 ± 15.22 | 26.18 ± 2.82 | 32.55 ± 2.32 | ematix-flow |
| Q12  | 88.93 ± 4.60 | 103.64 ± 2.17 | 110.60 ± 3.65 | ematix-flow |
| Q13  | 98.12 ± 5.76 | 265.27 ± 4.05 | 407.50 ± 22.35 | ematix-flow |
| Q14  | 86.23 ± 3.71 | 135.59 ± 3.26 | 91.86 ± 1.72 | ematix-flow |
| Q15  | 76.91 ± 3.54 | 87.76 ± 1.91 | 66.24 ± 2.15 | Polars |
| Q16  | 51.75 ± 1.96 | 65.41 ± 3.43 | 168.38 ± 7.00 | ematix-flow |
| Q17  | 185.49 ± 13.46 | 157.35 ± 4.52 | 433.71 ± 15.97 | DuckDB |
| Q18  | 248.89 ± 9.33 | 228.99 ± 7.61 | 597.14 ± 18.75 | DuckDB |
| Q19  | 131.75 ± 4.31 | 192.37 ± 2.91 | 1206.10 ± 40.74 | ematix-flow |
| Q20  | 125.33 ± 28.55 | 140.23 ± 5.63 | 268.57 ± 11.07 | ematix-flow |
| Q21  | 295.74 ± 9.04 | 406.68 ± 6.97 | 34347.37 ± 4359.83 | ematix-flow |
| Q22  | 24.10 ± 31.46 | 127.19 ± 5.10 | 107.50 ± 6.86 | ematix-flow |

## Wins

- **ematix-flow**: 14
- **DuckDB**: 6
- **Polars**: 2

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

- **Q05 / Polars**: polars join: task 23198 panicked with message "Polars' maximum length reached. Consider compiling with 'big…
