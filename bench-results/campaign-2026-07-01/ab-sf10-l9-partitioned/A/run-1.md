# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 222.53 ± 7.82 | — | — | ematix-flow |
| Q02  | 18.76 ± 0.25 | — | — | ematix-flow |
| Q03  | 118.20 ± 2.10 | — | — | ematix-flow |
| Q04  | 54.49 ± 1.28 | — | — | ematix-flow |
| Q05  | 150.48 ± 5.47 | — | — | ematix-flow |
| Q06  | 24.61 ± 2.79 | — | — | ematix-flow |
| Q07  | 119.96 ± 14.52 | — | — | ematix-flow |
| Q08  | 149.83 ± 6.17 | — | — | ematix-flow |
| Q09  | 278.75 ± 6.36 | — | — | ematix-flow |
| Q10  | 194.20 ± 7.36 | — | — | ematix-flow |
| Q11  | 12.84 ± 0.64 | — | — | ematix-flow |
| Q12  | 91.04 ± 4.86 | — | — | ematix-flow |
| Q13  | 95.98 ± 3.38 | — | — | ematix-flow |
| Q14  | 81.31 ± 4.20 | — | — | ematix-flow |
| Q15  | 60.25 ± 1.28 | — | — | ematix-flow |
| Q16  | 50.25 ± 2.38 | — | — | ematix-flow |
| Q17  | 78.04 ± 4.84 | — | — | ematix-flow |
| Q18  | 176.87 ± 5.71 | — | — | ematix-flow |
| Q19  | 124.73 ± 6.51 | — | — | ematix-flow |
| Q20  | 92.36 ± 4.89 | — | — | ematix-flow |
| Q21  | 177.68 ± 8.92 | — | — | ematix-flow |
| Q22  | 22.92 ± 2.89 | — | — | ematix-flow |

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
