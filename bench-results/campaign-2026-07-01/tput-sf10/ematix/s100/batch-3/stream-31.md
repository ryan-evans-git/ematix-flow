# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 3600.48 ± 0.00 | — | — | ematix-flow |
| Q02  | 2106.72 ± 0.00 | — | — | ematix-flow |
| Q03  | 1384.06 ± 0.00 | — | — | ematix-flow |
| Q04  | 2278.60 ± 0.00 | — | — | ematix-flow |
| Q05  | 9258.44 ± 0.00 | — | — | ematix-flow |
| Q06  | 491.16 ± 0.00 | — | — | ematix-flow |
| Q07  | 1788.30 ± 0.00 | — | — | ematix-flow |
| Q08  | 3180.32 ± 0.00 | — | — | ematix-flow |
| Q09  | 7610.10 ± 0.00 | — | — | ematix-flow |
| Q10  | 3136.89 ± 0.00 | — | — | ematix-flow |
| Q11  | 481.69 ± 0.00 | — | — | ematix-flow |
| Q12  | 535.93 ± 0.00 | — | — | ematix-flow |
| Q13  | 5211.42 ± 0.00 | — | — | ematix-flow |
| Q14  | 286.95 ± 0.00 | — | — | ematix-flow |
| Q15  | 497.81 ± 0.00 | — | — | ematix-flow |
| Q16  | 1877.25 ± 0.00 | — | — | ematix-flow |
| Q17  | 2445.26 ± 0.00 | — | — | ematix-flow |
| Q18  | 7375.56 ± 0.00 | — | — | ematix-flow |
| Q19  | 935.18 ± 0.00 | — | — | ematix-flow |
| Q20  | 1217.45 ± 0.00 | — | — | ematix-flow |
| Q21  | 11531.18 ± 0.00 | — | — | ematix-flow |
| Q22  | 378.40 ± 0.00 | — | — | ematix-flow |

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
