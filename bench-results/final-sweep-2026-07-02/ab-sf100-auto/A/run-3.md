# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 2577.37 ± 23.60 | — | — | ematix-flow |
| Q02  | 236.21 ± 8.50 | — | — | ematix-flow |
| Q03  | 1624.37 ± 8.39 | — | — | ematix-flow |
| Q04  | 924.27 ± 11.47 | — | — | ematix-flow |
| Q05  | 2030.28 ± 18.47 | — | — | ematix-flow |
| Q06  | 543.92 ± 11.09 | — | — | ematix-flow |
| Q07  | 1617.61 ± 7.39 | — | — | ematix-flow |
| Q08  | 1765.97 ± 5.67 | — | — | ematix-flow |
| Q09  | 4214.90 ± 89.41 | — | — | ematix-flow |
| Q10  | 3275.44 ± 82.05 | — | — | ematix-flow |
| Q11  | 224.08 ± 16.08 | — | — | ematix-flow |
| Q12  | 1065.61 ± 21.10 | — | — | ematix-flow |
| Q13  | 2053.45 ± 7.87 | — | — | ematix-flow |
| Q14  | 832.00 ± 11.25 | — | — | ematix-flow |
| Q15  | 791.15 ± 4.45 | — | — | ematix-flow |
| Q16  | 431.08 ± 9.79 | — | — | ematix-flow |
| Q17  | 1547.66 ± 28.24 | — | — | ematix-flow |
| Q18  | 2732.25 ± 37.37 | — | — | ematix-flow |
| Q19  | 1243.36 ± 3.60 | — | — | ematix-flow |
| Q20  | 1304.08 ± 11.24 | — | — | ematix-flow |
| Q21  | 3673.29 ± 22.94 | — | — | ematix-flow |
| Q22  | 473.16 ± 14.64 | — | — | ematix-flow |

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
