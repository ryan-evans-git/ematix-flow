# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 227.42 ± 6.33 | — | — | ematix-flow |
| Q02  | 18.12 ± 0.51 | — | — | ematix-flow |
| Q03  | 118.34 ± 1.90 | — | — | ematix-flow |
| Q04  | 52.61 ± 2.82 | — | — | ematix-flow |
| Q05  | 148.51 ± 3.98 | — | — | ematix-flow |
| Q06  | 24.48 ± 3.95 | — | — | ematix-flow |
| Q07  | 112.72 ± 5.01 | — | — | ematix-flow |
| Q08  | 144.72 ± 5.93 | — | — | ematix-flow |
| Q09  | 259.40 ± 5.62 | — | — | ematix-flow |
| Q10  | 188.71 ± 8.15 | — | — | ematix-flow |
| Q11  | 13.27 ± 0.21 | — | — | ematix-flow |
| Q12  | 85.40 ± 2.86 | — | — | ematix-flow |
| Q13  | 101.03 ± 4.30 | — | — | ematix-flow |
| Q14  | 79.27 ± 1.52 | — | — | ematix-flow |
| Q15  | 59.95 ± 2.81 | — | — | ematix-flow |
| Q16  | 46.58 ± 1.13 | — | — | ematix-flow |
| Q17  | 76.54 ± 4.80 | — | — | ematix-flow |
| Q18  | 172.44 ± 4.90 | — | — | ematix-flow |
| Q19  | 120.66 ± 5.67 | — | — | ematix-flow |
| Q20  | 91.81 ± 4.03 | — | — | ematix-flow |
| Q21  | 181.51 ± 8.14 | — | — | ematix-flow |
| Q22  | 23.68 ± 2.24 | — | — | ematix-flow |

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
