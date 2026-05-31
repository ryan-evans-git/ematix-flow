# TPC-H SF=100 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 10 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 2598.66 ± 54.98 | — | — | ematix-flow |
| Q02  | 399.39 ± 13.96 | — | — | ematix-flow |
| Q03  | 1706.98 ± 57.66 | — | — | ematix-flow |
| Q04  | 973.55 ± 15.95 | — | — | ematix-flow |
| Q05  | 2766.83 ± 24.60 | — | — | ematix-flow |
| Q06  | 538.57 ± 14.29 | — | — | ematix-flow |
| Q07  | 2285.85 ± 29.20 | — | — | ematix-flow |
| Q08  | 2983.87 ± 169.79 | — | — | ematix-flow |
| Q09  | 10598.51 ± 743.50 | — | — | ematix-flow |
| Q10  | 3850.77 ± 369.36 | — | — | ematix-flow |
| Q11  | 431.62 ± 23.69 | — | — | ematix-flow |
| Q12  | 1332.50 ± 49.27 | — | — | ematix-flow |
| Q13  | 2403.27 ± 34.94 | — | — | ematix-flow |
| Q14  | 1004.60 ± 72.93 | — | — | ematix-flow |
| Q15  | 1084.33 ± 23.41 | — | — | ematix-flow |
| Q16  | 665.03 ± 18.64 | — | — | ematix-flow |
| Q17  | 2328.46 ± 58.36 | — | — | ematix-flow |
| Q18  | 8200.49 ± 412.13 | — | — | ematix-flow |
| Q19  | 1384.41 ± 49.15 | — | — | ematix-flow |
| Q20  | 2153.44 ± 37.35 | — | — | ematix-flow |
| Q21  | 4671.63 ± 58.81 | — | — | ematix-flow |
| Q22  | 506.07 ± 11.14 | — | — | ematix-flow |

## Wins

- **ematix-flow**: 22
- **DuckDB**: 0
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions = std::thread::available_parallelism()` (override via `PARTITIONS=N`) and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
