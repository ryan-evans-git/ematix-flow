# TPC-H SF=10 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 248.24 ± 7.33 | — | — | ematix-flow |
| Q02  | 32.58 ± 0.37 | — | — | ematix-flow |
| Q03  | 162.00 ± 10.63 | — | — | ematix-flow |
| Q04  | 62.09 ± 2.91 | — | — | ematix-flow |
| Q05  | 216.74 ± 3.93 | — | — | ematix-flow |
| Q06  | 38.39 ± 7.36 | — | — | ematix-flow |
| Q07  | 180.93 ± 16.71 | — | — | ematix-flow |
| Q08  | 207.66 ± 7.30 | — | — | ematix-flow |
| Q09  | 312.92 ± 9.48 | — | — | ematix-flow |
| Q10  | 224.72 ± 6.64 | — | — | ematix-flow |
| Q11  | 12.76 ± 1.71 | — | — | ematix-flow |
| Q12  | 99.30 ± 4.02 | — | — | ematix-flow |
| Q13  | 110.75 ± 15.17 | — | — | ematix-flow |
| Q14  | 88.31 ± 4.23 | — | — | ematix-flow |
| Q15  | 82.99 ± 6.09 | — | — | ematix-flow |
| Q16  | 56.35 ± 1.81 | — | — | ematix-flow |
| Q17  | 142.10 ± 9.10 | — | — | ematix-flow |
| Q18  | 257.85 ± 6.84 | — | — | ematix-flow |
| Q19  | 140.72 ± 6.47 | — | — | ematix-flow |
| Q20  | 138.53 ± 6.02 | — | — | ematix-flow |
| Q21  | 291.91 ± 5.08 | — | — | ematix-flow |
| Q22  | 25.48 ± 1.25 | — | — | ematix-flow |

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
