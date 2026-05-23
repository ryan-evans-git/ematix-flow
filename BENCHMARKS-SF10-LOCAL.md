# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 20 timed trials after 5 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 20 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 274.07 ± 12.05 | 232.23 ± 4.05 | 342.33 ± 12.55 | DuckDB |
| Q02  | 48.17 ± 8.39 | 43.53 ± 1.29 | 428.41 ± 29.85 | DuckDB |
| Q03  | 154.65 ± 5.43 | 143.37 ± 3.57 | 560.56 ± 28.96 | DuckDB |
| Q04  | 81.71 ± 15.98 | 86.80 ± 17.56 | 270.26 ± 11.64 | ematix-flow |
| Q06  | 78.73 ± 4.37 | 72.67 ± 2.85 | 60.51 ± 1.47 | Polars |
| Q07  | 274.59 ± 12.81 | 138.63 ± 5.30 | 1294.52 ± 46.77 | DuckDB |
| Q08  | 201.68 ± 17.02 | 173.61 ± 4.53 | 1154.14 ± 24.15 | DuckDB |
| Q09  | 294.79 ± 11.97 | 308.69 ± 39.77 | 436.70 ± 21.14 | ematix-flow |
| Q10  | 243.35 ± 10.76 | 409.21 ± 21.44 | 5625.75 ± 248.53 | ematix-flow |
| Q11  | 26.24 ± 36.65 | 24.82 ± 3.88 | 33.23 ± 1.69 | DuckDB |
| Q12  | 100.33 ± 5.66 | 105.94 ± 15.73 | 110.50 ± 5.29 | ematix-flow |
| Q13  | 134.56 ± 5.85 | 267.76 ± 12.50 | 409.16 ± 15.38 | ematix-flow |
| Q14  | 88.38 ± 2.98 | 138.74 ± 13.38 | 93.21 ± 2.42 | ematix-flow |
| Q15  | 79.34 ± 4.63 | 85.82 ± 5.88 | 66.63 ± 4.72 | Polars |
| Q16  | 43.14 ± 2.00 | 63.27 ± 1.38 | 171.47 ± 13.75 | ematix-flow |
| Q17  | 307.54 ± 8.06 | 163.40 ± 3.89 | 450.17 ± 32.17 | DuckDB |
| Q18  | 696.82 ± 45.28 | 224.97 ± 5.28 | 592.65 ± 21.69 | DuckDB |
| Q19  | 136.02 ± 6.40 | 189.06 ± 5.14 | 1193.02 ± 28.83 | ematix-flow |
| Q20  | 139.30 ± 11.93 | 137.49 ± 4.37 | 267.19 ± 14.12 | DuckDB |
| Q21  | 447.36 ± 43.72 | 411.79 ± 37.57 | 41009.60 ± 5165.24 | DuckDB |
| Q22  | 62.59 ± 35.96 | 129.87 ± 2.61 | 111.91 ± 8.85 | ematix-flow |

## Wins

- **ematix-flow**: 9
- **DuckDB**: 10
- **Polars**: 2

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
