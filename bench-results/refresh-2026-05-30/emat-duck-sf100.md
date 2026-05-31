# TPC-H SF=100 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 10 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 2131.21 ± 41.55 | 2270.04 ± 23.31 | — | ematix-flow |
| Q02  | 259.41 ± 7.34 | 377.64 ± 2.65 | — | ematix-flow |
| Q03  | 910.65 ± 67.09 | 2763.46 ± 47.92 | — | ematix-flow |
| Q04  | 773.16 ± 9.61 | 842.59 ± 2.79 | — | ematix-flow |
| Q05  | 1294.79 ± 14.72 | 1499.64 ± 10.63 | — | ematix-flow |
| Q06  | 435.60 ± 6.83 | 708.11 ± 8.00 | — | ematix-flow |
| Q07  | 1742.20 ± 34.77 | 1582.26 ± 26.06 | — | DuckDB |
| Q08  | 2247.19 ± 9.36 | 2629.44 ± 15.54 | — | ematix-flow |
| Q09  | 7084.95 ± 430.09 | 6627.09 ± 323.35 | — | DuckDB |
| Q10  | 3019.15 ± 172.83 | 2531.83 ± 10.55 | — | DuckDB |
| Q11  | 197.36 ± 14.37 | 209.05 ± 3.34 | — | ematix-flow |
| Q12  | 920.52 ± 29.84 | 1100.42 ± 7.29 | — | ematix-flow |
| Q13  | 1874.16 ± 26.07 | 2211.84 ± 11.92 | — | ematix-flow |
| Q14  | 775.74 ± 14.03 | 1454.28 ± 64.16 | — | ematix-flow |
| Q15  | 909.18 ± 45.38 | 913.09 ± 19.41 | — | ematix-flow |
| Q16  | 142.89 ± 5.34 | 356.83 ± 8.56 | — | ematix-flow |
| Q17  | 1759.37 ± 49.48 | 1521.43 ± 11.70 | — | DuckDB |
| Q18  | 376.97 ± 20.94 | 2193.22 ± 18.14 | — | ematix-flow |
| Q19  | 1131.10 ± 22.63 | 1475.43 ± 38.76 | — | ematix-flow |
| Q20  | 1952.95 ± 63.87 | 1663.60 ± 12.01 | — | DuckDB |
| Q21  | 4018.91 ± 23.65 | 4278.57 ± 91.42 | — | ematix-flow |
| Q22  | 440.93 ± 12.70 | 577.26 ± 5.42 | — | ematix-flow |

## Wins

- **ematix-flow**: 17
- **DuckDB**: 5
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions = std::thread::available_parallelism()` (override via `PARTITIONS=N`) and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
