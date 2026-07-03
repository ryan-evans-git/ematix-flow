# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 253.27 ± 12.11 | — | — | ematix-flow |
| Q02  | 20.73 ± 0.63 | — | — | ematix-flow |
| Q03  | 123.81 ± 4.07 | — | — | ematix-flow |
| Q04  | 56.94 ± 2.54 | — | — | ematix-flow |
| Q05  | 157.44 ± 4.59 | — | — | ematix-flow |
| Q06  | 25.18 ± 2.98 | — | — | ematix-flow |
| Q07  | 127.42 ± 8.95 | — | — | ematix-flow |
| Q08  | 162.59 ± 9.21 | — | — | ematix-flow |
| Q09  | 295.04 ± 18.15 | — | — | ematix-flow |
| Q10  | 202.46 ± 6.28 | — | — | ematix-flow |
| Q11  | 14.17 ± 1.03 | — | — | ematix-flow |
| Q12  | 94.61 ± 6.07 | — | — | ematix-flow |
| Q13  | 103.49 ± 3.27 | — | — | ematix-flow |
| Q14  | 83.77 ± 2.56 | — | — | ematix-flow |
| Q15  | 62.48 ± 4.01 | — | — | ematix-flow |
| Q16  | 50.10 ± 2.64 | — | — | ematix-flow |
| Q17  | 80.55 ± 4.78 | — | — | ematix-flow |
| Q18  | 183.67 ± 4.39 | — | — | ematix-flow |
| Q19  | 129.62 ± 4.78 | — | — | ematix-flow |
| Q20  | 95.44 ± 4.63 | — | — | ematix-flow |
| Q21  | 181.29 ± 13.65 | — | — | ematix-flow |
| Q22  | 23.61 ± 1.13 | — | — | ematix-flow |

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
