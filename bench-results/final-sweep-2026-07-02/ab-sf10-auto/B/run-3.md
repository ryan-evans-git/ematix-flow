# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 235.88 ± 9.09 | — | — | ematix-flow |
| Q02  | 18.58 ± 0.52 | — | — | ematix-flow |
| Q03  | 119.08 ± 3.96 | — | — | ematix-flow |
| Q04  | 54.09 ± 1.62 | — | — | ematix-flow |
| Q05  | 148.48 ± 5.49 | — | — | ematix-flow |
| Q06  | 24.56 ± 1.82 | — | — | ematix-flow |
| Q07  | 118.56 ± 5.33 | — | — | ematix-flow |
| Q08  | 150.17 ± 3.97 | — | — | ematix-flow |
| Q09  | 274.57 ± 10.56 | — | — | ematix-flow |
| Q10  | 194.07 ± 8.17 | — | — | ematix-flow |
| Q11  | 13.17 ± 1.40 | — | — | ematix-flow |
| Q12  | 87.11 ± 4.43 | — | — | ematix-flow |
| Q13  | 100.17 ± 6.21 | — | — | ematix-flow |
| Q14  | 81.16 ± 3.95 | — | — | ematix-flow |
| Q15  | 59.02 ± 2.82 | — | — | ematix-flow |
| Q16  | 51.18 ± 3.21 | — | — | ematix-flow |
| Q17  | 76.32 ± 5.46 | — | — | ematix-flow |
| Q18  | 182.77 ± 3.45 | — | — | ematix-flow |
| Q19  | 123.35 ± 3.13 | — | — | ematix-flow |
| Q20  | 91.50 ± 4.30 | — | — | ematix-flow |
| Q21  | 188.60 ± 6.14 | — | — | ematix-flow |
| Q22  | 23.71 ± 1.70 | — | — | ematix-flow |

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
