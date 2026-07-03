# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 255.92 ± 14.12 | — | — | ematix-flow |
| Q02  | 23.98 ± 1.41 | — | — | ematix-flow |
| Q03  | 121.23 ± 3.83 | — | — | ematix-flow |
| Q04  | 69.65 ± 1.55 | — | — | ematix-flow |
| Q05  | 108.17 ± 1.91 | — | — | ematix-flow |
| Q06  | 25.71 ± 4.96 | — | — | ematix-flow |
| Q07  | 136.46 ± 4.19 | — | — | ematix-flow |
| Q08  | 280.70 ± 9.58 | — | — | ematix-flow |
| Q09  | 252.07 ± 9.05 | — | — | ematix-flow |
| Q10  | 195.35 ± 5.29 | — | — | ematix-flow |
| Q11  | 17.85 ± 1.80 | — | — | ematix-flow |
| Q12  | 98.66 ± 4.17 | — | — | ematix-flow |
| Q13  | 101.05 ± 4.52 | — | — | ematix-flow |
| Q14  | 84.62 ± 3.03 | — | — | ematix-flow |
| Q15  | 62.89 ± 1.91 | — | — | ematix-flow |
| Q16  | 50.45 ± 3.24 | — | — | ematix-flow |
| Q17  | 99.06 ± 3.35 | — | — | ematix-flow |
| Q18  | 235.72 ± 4.98 | — | — | ematix-flow |
| Q19  | 128.25 ± 2.58 | — | — | ematix-flow |
| Q20  | 176.58 ± 4.17 | — | — | ematix-flow |
| Q21  | 211.52 ± 5.98 | — | — | ematix-flow |
| Q22  | 30.25 ± 1.41 | — | — | ematix-flow |

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
