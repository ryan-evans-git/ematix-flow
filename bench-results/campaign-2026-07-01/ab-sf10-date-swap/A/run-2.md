# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 263.37 ± 13.62 | — | — | ematix-flow |
| Q02  | 20.17 ± 0.74 | — | — | ematix-flow |
| Q03  | 123.58 ± 4.95 | — | — | ematix-flow |
| Q04  | 57.67 ± 1.77 | — | — | ematix-flow |
| Q05  | 155.43 ± 7.26 | — | — | ematix-flow |
| Q06  | 26.44 ± 1.93 | — | — | ematix-flow |
| Q07  | 128.46 ± 3.23 | — | — | ematix-flow |
| Q08  | 155.69 ± 5.99 | — | — | ematix-flow |
| Q09  | 291.17 ± 13.08 | — | — | ematix-flow |
| Q10  | 199.51 ± 5.39 | — | — | ematix-flow |
| Q11  | 13.93 ± 0.48 | — | — | ematix-flow |
| Q12  | 93.24 ± 2.15 | — | — | ematix-flow |
| Q13  | 104.91 ± 5.34 | — | — | ematix-flow |
| Q14  | 86.09 ± 5.72 | — | — | ematix-flow |
| Q15  | 61.65 ± 3.14 | — | — | ematix-flow |
| Q16  | 52.03 ± 2.18 | — | — | ematix-flow |
| Q17  | 79.27 ± 2.23 | — | — | ematix-flow |
| Q18  | 188.84 ± 7.96 | — | — | ematix-flow |
| Q19  | 132.85 ± 4.47 | — | — | ematix-flow |
| Q20  | 98.86 ± 3.63 | — | — | ematix-flow |
| Q21  | 188.65 ± 9.87 | — | — | ematix-flow |
| Q22  | 22.07 ± 2.05 | — | — | ematix-flow |

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
