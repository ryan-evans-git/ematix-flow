# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 241.08 ± 8.78 | — | — | ematix-flow |
| Q02  | 19.68 ± 0.65 | — | — | ematix-flow |
| Q03  | 121.44 ± 4.53 | — | — | ematix-flow |
| Q04  | 55.73 ± 1.79 | — | — | ematix-flow |
| Q05  | 149.43 ± 5.15 | — | — | ematix-flow |
| Q06  | 25.25 ± 2.74 | — | — | ematix-flow |
| Q07  | 125.78 ± 5.45 | — | — | ematix-flow |
| Q08  | 150.30 ± 8.10 | — | — | ematix-flow |
| Q09  | 282.16 ± 11.82 | — | — | ematix-flow |
| Q10  | 196.05 ± 11.34 | — | — | ematix-flow |
| Q11  | 13.20 ± 0.41 | — | — | ematix-flow |
| Q12  | 92.05 ± 4.62 | — | — | ematix-flow |
| Q13  | 101.83 ± 2.57 | — | — | ematix-flow |
| Q14  | 84.19 ± 3.29 | — | — | ematix-flow |
| Q15  | 60.99 ± 2.42 | — | — | ematix-flow |
| Q16  | 51.10 ± 2.35 | — | — | ematix-flow |
| Q17  | 78.22 ± 4.87 | — | — | ematix-flow |
| Q18  | 185.90 ± 3.93 | — | — | ematix-flow |
| Q19  | 129.90 ± 6.65 | — | — | ematix-flow |
| Q20  | 92.35 ± 3.46 | — | — | ematix-flow |
| Q21  | 184.01 ± 7.86 | — | — | ematix-flow |
| Q22  | 23.00 ± 1.67 | — | — | ematix-flow |

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
