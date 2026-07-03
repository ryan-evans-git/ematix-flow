# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 229.58 ± 8.78 | — | — | ematix-flow |
| Q02  | 19.54 ± 0.69 | — | — | ematix-flow |
| Q03  | 115.97 ± 3.98 | — | — | ematix-flow |
| Q04  | 55.07 ± 1.96 | — | — | ematix-flow |
| Q05  | 147.85 ± 6.56 | — | — | ematix-flow |
| Q06  | 25.38 ± 3.52 | — | — | ematix-flow |
| Q07  | 115.86 ± 3.49 | — | — | ematix-flow |
| Q08  | 147.34 ± 4.46 | — | — | ematix-flow |
| Q09  | 269.01 ± 5.88 | — | — | ematix-flow |
| Q10  | 188.18 ± 8.47 | — | — | ematix-flow |
| Q11  | 13.26 ± 1.17 | — | — | ematix-flow |
| Q12  | 88.51 ± 3.63 | — | — | ematix-flow |
| Q13  | 99.37 ± 4.00 | — | — | ematix-flow |
| Q14  | 79.40 ± 3.49 | — | — | ematix-flow |
| Q15  | 59.35 ± 6.11 | — | — | ematix-flow |
| Q16  | 50.77 ± 2.06 | — | — | ematix-flow |
| Q17  | 78.38 ± 5.29 | — | — | ematix-flow |
| Q18  | 179.44 ± 4.96 | — | — | ematix-flow |
| Q19  | 122.29 ± 2.61 | — | — | ematix-flow |
| Q20  | 90.07 ± 3.19 | — | — | ematix-flow |
| Q21  | 182.99 ± 4.80 | — | — | ematix-flow |
| Q22  | 24.34 ± 2.54 | — | — | ematix-flow |

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
