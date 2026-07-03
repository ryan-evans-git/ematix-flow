# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 259.54 ± 9.15 | — | — | ematix-flow |
| Q02  | 20.54 ± 0.82 | — | — | ematix-flow |
| Q03  | 124.46 ± 5.66 | — | — | ematix-flow |
| Q04  | 56.06 ± 2.26 | — | — | ematix-flow |
| Q05  | 153.43 ± 8.26 | — | — | ematix-flow |
| Q06  | 25.82 ± 1.52 | — | — | ematix-flow |
| Q07  | 127.03 ± 6.55 | — | — | ematix-flow |
| Q08  | 151.96 ± 5.20 | — | — | ematix-flow |
| Q09  | 291.21 ± 13.94 | — | — | ematix-flow |
| Q10  | 202.74 ± 5.76 | — | — | ematix-flow |
| Q11  | 13.80 ± 1.87 | — | — | ematix-flow |
| Q12  | 90.41 ± 2.07 | — | — | ematix-flow |
| Q13  | 101.72 ± 4.11 | — | — | ematix-flow |
| Q14  | 84.03 ± 4.65 | — | — | ematix-flow |
| Q15  | 62.34 ± 3.02 | — | — | ematix-flow |
| Q16  | 51.12 ± 1.55 | — | — | ematix-flow |
| Q17  | 78.78 ± 5.85 | — | — | ematix-flow |
| Q18  | 191.14 ± 9.17 | — | — | ematix-flow |
| Q19  | 132.39 ± 8.81 | — | — | ematix-flow |
| Q20  | 93.44 ± 4.34 | — | — | ematix-flow |
| Q21  | 191.75 ± 7.43 | — | — | ematix-flow |
| Q22  | 23.85 ± 1.48 | — | — | ematix-flow |

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
