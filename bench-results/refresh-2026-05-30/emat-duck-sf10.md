# TPC-H SF=10 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 214.28 ± 4.84 | 243.16 ± 3.20 | — | ematix-flow |
| Q02  | 23.84 ± 0.44 | 37.79 ± 0.27 | — | ematix-flow |
| Q03  | 78.27 ± 5.75 | 138.20 ± 3.99 | — | ematix-flow |
| Q04  | 52.35 ± 1.85 | 82.32 ± 0.91 | — | ematix-flow |
| Q05  | 101.18 ± 3.26 | 133.10 ± 1.89 | — | ematix-flow |
| Q06  | 34.76 ± 5.00 | 70.20 ± 1.72 | — | ematix-flow |
| Q07  | 145.96 ± 5.99 | 129.73 ± 1.60 | — | DuckDB |
| Q08  | 183.95 ± 7.18 | 157.64 ± 4.12 | — | DuckDB |
| Q09  | 259.71 ± 6.58 | 265.29 ± 3.22 | — | ematix-flow |
| Q10  | 196.60 ± 5.75 | 358.36 ± 2.32 | — | ematix-flow |
| Q11  | 13.06 ± 0.38 | 23.20 ± 0.30 | — | ematix-flow |
| Q12  | 81.76 ± 3.13 | 104.94 ± 1.25 | — | ematix-flow |
| Q13  | 94.39 ± 2.59 | 237.77 ± 4.49 | — | ematix-flow |
| Q14  | 79.99 ± 3.42 | 119.44 ± 1.75 | — | ematix-flow |
| Q15  | 69.77 ± 2.14 | 76.85 ± 1.07 | — | ematix-flow |
| Q16  | 27.42 ± 0.66 | 54.79 ± 2.93 | — | ematix-flow |
| Q17  | 131.69 ± 2.92 | 143.42 ± 1.59 | — | ematix-flow |
| Q18  | 197.90 ± 2.97 | 194.43 ± 1.46 | — | DuckDB |
| Q19  | 119.55 ± 3.22 | 178.25 ± 2.76 | — | ematix-flow |
| Q20  | 118.65 ± 4.40 | 129.03 ± 2.84 | — | ematix-flow |
| Q21  | 255.37 ± 5.67 | 358.20 ± 3.26 | — | ematix-flow |
| Q22  | 22.64 ± 1.26 | 114.01 ± 1.98 | — | ematix-flow |

## Wins

- **ematix-flow**: 19
- **DuckDB**: 3
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions = std::thread::available_parallelism()` (override via `PARTITIONS=N`) and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
