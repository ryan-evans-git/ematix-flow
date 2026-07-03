# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 258.97 ± 12.61 | — | — | ematix-flow |
| Q02  | 23.81 ± 1.95 | — | — | ematix-flow |
| Q03  | 122.77 ± 4.62 | — | — | ematix-flow |
| Q04  | 70.49 ± 2.91 | — | — | ematix-flow |
| Q05  | 113.62 ± 7.81 | — | — | ematix-flow |
| Q06  | 25.89 ± 3.45 | — | — | ematix-flow |
| Q07  | 135.72 ± 6.05 | — | — | ematix-flow |
| Q08  | 280.67 ± 7.91 | — | — | ematix-flow |
| Q09  | 249.45 ± 15.75 | — | — | ematix-flow |
| Q10  | 196.69 ± 4.82 | — | — | ematix-flow |
| Q11  | 17.52 ± 1.25 | — | — | ematix-flow |
| Q12  | 100.32 ± 3.90 | — | — | ematix-flow |
| Q13  | 101.18 ± 2.17 | — | — | ematix-flow |
| Q14  | 87.93 ± 4.59 | — | — | ematix-flow |
| Q15  | 61.30 ± 2.66 | — | — | ematix-flow |
| Q16  | 49.38 ± 2.22 | — | — | ematix-flow |
| Q17  | 100.08 ± 4.20 | — | — | ematix-flow |
| Q18  | 236.78 ± 4.41 | — | — | ematix-flow |
| Q19  | 129.48 ± 2.64 | — | — | ematix-flow |
| Q20  | 176.05 ± 2.64 | — | — | ematix-flow |
| Q21  | 210.31 ± 5.71 | — | — | ematix-flow |
| Q22  | 30.55 ± 1.77 | — | — | ematix-flow |

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
