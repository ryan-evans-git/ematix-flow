# TPC-H SF=10 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 229.87 ± 4.25 | 253.58 ± 5.63 | 342.84 ± 9.00 | ematix-flow |
| Q02  | 18.77 ± 2.74 | 41.64 ± 0.89 | 447.35 ± 25.03 | ematix-flow |
| Q03  | 83.69 ± 5.46 | 150.04 ± 3.80 | 590.46 ± 18.82 | ematix-flow |
| Q04  | 57.88 ± 6.88 | 92.26 ± 5.91 | 276.90 ± 14.53 | ematix-flow |
| Q05  | 116.22 ± 8.13 | 148.43 ± 4.11 | — | ematix-flow |
| Q06  | 37.81 ± 13.01 | 85.75 ± 1.48 | 58.61 ± 4.05 | ematix-flow |
| Q07  | 123.96 ± 5.69 | 132.82 ± 9.74 | 1314.89 ± 63.54 | ematix-flow |
| Q08  | 200.07 ± 5.85 | 181.20 ± 2.33 | 1281.61 ± 61.82 | DuckDB |
| Q09  | 269.75 ± 12.97 | 275.69 ± 10.40 | 413.91 ± 17.74 | ematix-flow |
| Q10  | 200.26 ± 9.08 | 384.61 ± 5.43 | 4153.26 ± 327.20 | ematix-flow |
| Q11  | 12.70 ± 3.64 | 24.61 ± 31.75 | 32.49 ± 6.98 | ematix-flow |
| Q12  | 96.72 ± 7.87 | 121.54 ± 1.72 | 133.89 ± 4.22 | ematix-flow |
| Q13  | 115.04 ± 7.14 | 268.25 ± 12.67 | 423.69 ± 17.80 | ematix-flow |
| Q14  | 82.43 ± 5.36 | 122.67 ± 16.23 | 83.78 ± 3.81 | ematix-flow |
| Q15  | 63.37 ± 1.74 | 91.24 ± 3.13 | 71.95 ± 1.43 | ematix-flow |
| Q16  | 33.63 ± 1.04 | 59.56 ± 10.29 | 170.52 ± 9.98 | ematix-flow |
| Q17  | 121.20 ± 8.01 | 159.35 ± 10.02 | 523.02 ± 24.02 | ematix-flow |
| Q18  | 20.64 ± 4.75 | 244.86 ± 7.13 | 623.78 ± 27.89 | ematix-flow |
| Q19  | 133.67 ± 7.57 | 185.09 ± 5.56 | 1388.65 ± 67.90 | ematix-flow |
| Q20  | 109.71 ± 78.10 | 140.20 ± 5.52 | 274.81 ± 19.01 | ematix-flow |
| Q21  | 256.88 ± 13.82 | 409.44 ± 31.35 | 34716.87 ± 3955.62 | ematix-flow |
| Q22  | 51.28 ± 41.40 | 115.30 ± 7.36 | 112.38 ± 10.78 | ematix-flow |

## Wins

- **ematix-flow**: 21
- **DuckDB**: 1
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions = std::thread::available_parallelism()` (override via `PARTITIONS=N`) and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

- **Q05 / Polars**: polars join: task 10144 panicked with message "Polars' maximum length reached. Consider compiling with 'big…
