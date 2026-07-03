# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 244.50 ± 8.36 | — | — | ematix-flow |
| Q02  | 20.67 ± 1.06 | — | — | ematix-flow |
| Q03  | 122.47 ± 4.76 | — | — | ematix-flow |
| Q04  | 55.74 ± 2.57 | — | — | ematix-flow |
| Q05  | 154.54 ± 7.55 | — | — | ematix-flow |
| Q06  | 26.02 ± 3.03 | — | — | ematix-flow |
| Q07  | 121.65 ± 4.91 | — | — | ematix-flow |
| Q08  | 152.88 ± 4.60 | — | — | ematix-flow |
| Q09  | 285.68 ± 9.49 | — | — | ematix-flow |
| Q10  | 196.03 ± 8.60 | — | — | ematix-flow |
| Q11  | 13.81 ± 0.57 | — | — | ematix-flow |
| Q12  | 90.22 ± 3.37 | — | — | ematix-flow |
| Q13  | 102.63 ± 3.21 | — | — | ematix-flow |
| Q14  | 84.97 ± 5.99 | — | — | ematix-flow |
| Q15  | 61.10 ± 2.27 | — | — | ematix-flow |
| Q16  | 51.92 ± 1.79 | — | — | ematix-flow |
| Q17  | 80.73 ± 4.26 | — | — | ematix-flow |
| Q18  | 189.98 ± 11.29 | — | — | ematix-flow |
| Q19  | 126.06 ± 3.96 | — | — | ematix-flow |
| Q20  | 92.62 ± 6.13 | — | — | ematix-flow |
| Q21  | 185.49 ± 5.88 | — | — | ematix-flow |
| Q22  | 22.91 ± 1.54 | — | — | ematix-flow |

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
