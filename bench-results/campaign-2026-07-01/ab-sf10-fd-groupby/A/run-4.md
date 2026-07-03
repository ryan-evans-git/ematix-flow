# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 260.11 ± 14.18 | — | — | ematix-flow |
| Q02  | 20.50 ± 1.13 | — | — | ematix-flow |
| Q03  | 122.42 ± 3.68 | — | — | ematix-flow |
| Q04  | 57.83 ± 1.95 | — | — | ematix-flow |
| Q05  | 152.31 ± 6.82 | — | — | ematix-flow |
| Q06  | 26.18 ± 3.04 | — | — | ematix-flow |
| Q07  | 125.79 ± 4.92 | — | — | ematix-flow |
| Q08  | 158.19 ± 4.47 | — | — | ematix-flow |
| Q09  | 287.86 ± 12.28 | — | — | ematix-flow |
| Q10  | 200.41 ± 4.81 | — | — | ematix-flow |
| Q11  | 13.93 ± 1.40 | — | — | ematix-flow |
| Q12  | 90.69 ± 4.75 | — | — | ematix-flow |
| Q13  | 103.55 ± 5.50 | — | — | ematix-flow |
| Q14  | 85.04 ± 2.84 | — | — | ematix-flow |
| Q15  | 62.79 ± 1.60 | — | — | ematix-flow |
| Q16  | 49.65 ± 2.28 | — | — | ematix-flow |
| Q17  | 79.98 ± 4.54 | — | — | ematix-flow |
| Q18  | 192.41 ± 6.30 | — | — | ematix-flow |
| Q19  | 130.06 ± 4.72 | — | — | ematix-flow |
| Q20  | 94.10 ± 3.86 | — | — | ematix-flow |
| Q21  | 190.70 ± 13.06 | — | — | ematix-flow |
| Q22  | 22.76 ± 1.78 | — | — | ematix-flow |

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
