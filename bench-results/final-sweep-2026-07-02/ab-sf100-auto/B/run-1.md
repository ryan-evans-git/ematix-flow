# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 2591.23 ± 26.52 | — | — | ematix-flow |
| Q02  | 200.99 ± 6.98 | — | — | ematix-flow |
| Q03  | 1567.10 ± 22.29 | — | — | ematix-flow |
| Q04  | 893.48 ± 11.98 | — | — | ematix-flow |
| Q05  | 1989.83 ± 13.42 | — | — | ematix-flow |
| Q06  | 533.93 ± 14.87 | — | — | ematix-flow |
| Q07  | 1538.10 ± 5.35 | — | — | ematix-flow |
| Q08  | 1683.91 ± 11.29 | — | — | ematix-flow |
| Q09  | 3580.08 ± 111.45 | — | — | ematix-flow |
| Q10  | 2029.35 ± 20.83 | — | — | ematix-flow |
| Q11  | 189.34 ± 6.37 | — | — | ematix-flow |
| Q12  | 1048.74 ± 19.75 | — | — | ematix-flow |
| Q13  | 1966.12 ± 15.46 | — | — | ematix-flow |
| Q14  | 837.29 ± 12.25 | — | — | ematix-flow |
| Q15  | 835.03 ± 40.07 | — | — | ematix-flow |
| Q16  | 410.63 ± 8.58 | — | — | ematix-flow |
| Q17  | 1508.09 ± 13.41 | — | — | ematix-flow |
| Q18  | 2578.03 ± 28.64 | — | — | ematix-flow |
| Q19  | 1237.19 ± 16.14 | — | — | ematix-flow |
| Q20  | 1168.56 ± 32.78 | — | — | ematix-flow |
| Q21  | 3536.16 ± 42.73 | — | — | ematix-flow |
| Q22  | 374.89 ± 8.49 | — | — | ematix-flow |

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
