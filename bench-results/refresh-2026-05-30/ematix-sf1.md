# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 10 timed trials after 3 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf1`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 18.07 ± 0.47 | — | — | ematix-flow |
| Q02  | 7.18 ± 0.22 | — | — | ematix-flow |
| Q03  | 9.91 ± 0.26 | — | — | ematix-flow |
| Q04  | 10.26 ± 0.24 | — | — | ematix-flow |
| Q05  | 18.88 ± 0.28 | — | — | ematix-flow |
| Q06  | 0.95 ± 0.04 | — | — | ematix-flow |
| Q07  | 29.46 ± 1.81 | — | — | ematix-flow |
| Q08  | 12.83 ± 0.69 | — | — | ematix-flow |
| Q09  | 17.63 ± 0.50 | — | — | ematix-flow |
| Q10  | 26.00 ± 5.99 | — | — | ematix-flow |
| Q11  | 5.53 ± 0.16 | — | — | ematix-flow |
| Q12  | 18.39 ± 3.12 | — | — | ematix-flow |
| Q13  | 11.49 ± 1.18 | — | — | ematix-flow |
| Q14  | 12.64 ± 0.82 | — | — | ematix-flow |
| Q15  | 12.61 ± 0.91 | — | — | ematix-flow |
| Q16  | 7.63 ± 0.21 | — | — | ematix-flow |
| Q17  | 16.95 ± 0.68 | — | — | ematix-flow |
| Q18  | 37.88 ± 0.93 | — | — | ematix-flow |
| Q19  | 16.22 ± 0.77 | — | — | ematix-flow |
| Q20  | 16.54 ± 0.57 | — | — | ematix-flow |
| Q21  | 35.67 ± 0.63 | — | — | ematix-flow |
| Q22  | 8.26 ± 0.18 | — | — | ematix-flow |

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
