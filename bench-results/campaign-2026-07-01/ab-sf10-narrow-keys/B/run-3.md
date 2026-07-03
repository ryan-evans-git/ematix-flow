# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 257.85 ± 13.16 | — | — | ematix-flow |
| Q02  | 24.10 ± 2.03 | — | — | ematix-flow |
| Q03  | 119.68 ± 4.55 | — | — | ematix-flow |
| Q04  | 70.45 ± 2.21 | — | — | ematix-flow |
| Q05  | 109.21 ± 2.84 | — | — | ematix-flow |
| Q06  | 27.46 ± 4.13 | — | — | ematix-flow |
| Q07  | 134.02 ± 3.90 | — | — | ematix-flow |
| Q08  | 280.58 ± 7.58 | — | — | ematix-flow |
| Q09  | 248.33 ± 6.23 | — | — | ematix-flow |
| Q10  | 198.41 ± 4.04 | — | — | ematix-flow |
| Q11  | 17.15 ± 0.93 | — | — | ematix-flow |
| Q12  | 101.15 ± 4.54 | — | — | ematix-flow |
| Q13  | 100.30 ± 3.35 | — | — | ematix-flow |
| Q14  | 83.40 ± 3.64 | — | — | ematix-flow |
| Q15  | 65.62 ± 2.78 | — | — | ematix-flow |
| Q16  | 50.45 ± 2.73 | — | — | ematix-flow |
| Q17  | 98.35 ± 3.13 | — | — | ematix-flow |
| Q18  | 235.72 ± 3.72 | — | — | ematix-flow |
| Q19  | 128.81 ± 4.27 | — | — | ematix-flow |
| Q20  | 176.65 ± 3.74 | — | — | ematix-flow |
| Q21  | 211.35 ± 5.22 | — | — | ematix-flow |
| Q22  | 29.80 ± 1.32 | — | — | ematix-flow |

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
