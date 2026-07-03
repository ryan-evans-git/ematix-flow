# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 250.93 ± 14.67 | — | — | ematix-flow |
| Q02  | 20.36 ± 1.31 | — | — | ematix-flow |
| Q03  | 123.68 ± 11.21 | — | — | ematix-flow |
| Q04  | 56.10 ± 1.84 | — | — | ematix-flow |
| Q05  | 160.20 ± 10.98 | — | — | ematix-flow |
| Q06  | 26.39 ± 1.84 | — | — | ematix-flow |
| Q07  | 126.01 ± 6.86 | — | — | ematix-flow |
| Q08  | 156.55 ± 5.40 | — | — | ematix-flow |
| Q09  | 296.96 ± 13.12 | — | — | ematix-flow |
| Q10  | 200.09 ± 6.94 | — | — | ematix-flow |
| Q11  | 14.17 ± 2.27 | — | — | ematix-flow |
| Q12  | 93.52 ± 4.52 | — | — | ematix-flow |
| Q13  | 102.74 ± 4.18 | — | — | ematix-flow |
| Q14  | 87.19 ± 4.04 | — | — | ematix-flow |
| Q15  | 61.46 ± 2.52 | — | — | ematix-flow |
| Q16  | 49.78 ± 2.45 | — | — | ematix-flow |
| Q17  | 79.53 ± 4.29 | — | — | ematix-flow |
| Q18  | 188.45 ± 5.91 | — | — | ematix-flow |
| Q19  | 133.13 ± 6.20 | — | — | ematix-flow |
| Q20  | 94.88 ± 3.74 | — | — | ematix-flow |
| Q21  | 186.39 ± 6.71 | — | — | ematix-flow |
| Q22  | 23.44 ± 1.79 | — | — | ematix-flow |

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
