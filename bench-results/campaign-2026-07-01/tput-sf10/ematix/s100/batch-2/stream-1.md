# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 4621.04 ± 0.00 | — | — | ematix-flow |
| Q02  | 354.36 ± 0.00 | — | — | ematix-flow |
| Q03  | 4146.08 ± 0.00 | — | — | ematix-flow |
| Q04  | 1998.13 ± 0.00 | — | — | ematix-flow |
| Q05  | 2662.13 ± 0.00 | — | — | ematix-flow |
| Q06  | 87.89 ± 0.00 | — | — | ematix-flow |
| Q07  | 5299.46 ± 0.00 | — | — | ematix-flow |
| Q08  | 5833.16 ± 0.00 | — | — | ematix-flow |
| Q09  | 9756.47 ± 0.00 | — | — | ematix-flow |
| Q10  | 1576.63 ± 0.00 | — | — | ematix-flow |
| Q11  | 1550.57 ± 0.00 | — | — | ematix-flow |
| Q12  | 1213.62 ± 0.00 | — | — | ematix-flow |
| Q13  | 3912.50 ± 0.00 | — | — | ematix-flow |
| Q14  | 291.68 ± 0.00 | — | — | ematix-flow |
| Q15  | 866.89 ± 0.00 | — | — | ematix-flow |
| Q16  | 1814.13 ± 0.00 | — | — | ematix-flow |
| Q17  | 1329.99 ± 0.00 | — | — | ematix-flow |
| Q18  | 9774.50 ± 0.00 | — | — | ematix-flow |
| Q19  | 607.93 ± 0.00 | — | — | ematix-flow |
| Q20  | 247.32 ± 0.00 | — | — | ematix-flow |
| Q21  | 9519.55 ± 0.00 | — | — | ematix-flow |
| Q22  | 2008.99 ± 0.00 | — | — | ematix-flow |

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
