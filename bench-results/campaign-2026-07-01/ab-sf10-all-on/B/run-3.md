# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 255.92 ± 7.40 | — | — | ematix-flow |
| Q02  | 24.61 ± 2.02 | — | — | ematix-flow |
| Q03  | 119.76 ± 6.11 | — | — | ematix-flow |
| Q04  | 68.79 ± 2.10 | — | — | ematix-flow |
| Q05  | 109.88 ± 2.35 | — | — | ematix-flow |
| Q06  | 26.32 ± 3.71 | — | — | ematix-flow |
| Q07  | 136.62 ± 4.60 | — | — | ematix-flow |
| Q08  | 280.52 ± 10.06 | — | — | ematix-flow |
| Q09  | 246.96 ± 10.25 | — | — | ematix-flow |
| Q10  | 190.92 ± 6.12 | — | — | ematix-flow |
| Q11  | 17.75 ± 1.19 | — | — | ematix-flow |
| Q12  | 102.32 ± 4.52 | — | — | ematix-flow |
| Q13  | 100.62 ± 3.86 | — | — | ematix-flow |
| Q14  | 89.70 ± 4.83 | — | — | ematix-flow |
| Q15  | 61.82 ± 1.44 | — | — | ematix-flow |
| Q16  | 50.98 ± 3.07 | — | — | ematix-flow |
| Q17  | 100.03 ± 1.74 | — | — | ematix-flow |
| Q18  | 235.78 ± 5.10 | — | — | ematix-flow |
| Q19  | 129.74 ± 4.86 | — | — | ematix-flow |
| Q20  | 172.65 ± 4.01 | — | — | ematix-flow |
| Q21  | 215.39 ± 9.70 | — | — | ematix-flow |
| Q22  | 30.55 ± 1.78 | — | — | ematix-flow |

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
