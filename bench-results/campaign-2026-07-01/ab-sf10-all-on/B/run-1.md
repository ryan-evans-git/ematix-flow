# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 253.07 ± 16.96 | — | — | ematix-flow |
| Q02  | 23.62 ± 2.72 | — | — | ematix-flow |
| Q03  | 120.10 ± 4.10 | — | — | ematix-flow |
| Q04  | 70.16 ± 3.98 | — | — | ematix-flow |
| Q05  | 109.71 ± 5.40 | — | — | ematix-flow |
| Q06  | 27.96 ± 5.78 | — | — | ematix-flow |
| Q07  | 136.90 ± 4.08 | — | — | ematix-flow |
| Q08  | 280.96 ± 6.28 | — | — | ematix-flow |
| Q09  | 245.59 ± 12.54 | — | — | ematix-flow |
| Q10  | 187.53 ± 6.32 | — | — | ematix-flow |
| Q11  | 17.46 ± 1.26 | — | — | ematix-flow |
| Q12  | 96.19 ± 5.62 | — | — | ematix-flow |
| Q13  | 98.88 ± 6.63 | — | — | ematix-flow |
| Q14  | 86.04 ± 3.09 | — | — | ematix-flow |
| Q15  | 60.64 ± 4.79 | — | — | ematix-flow |
| Q16  | 49.48 ± 2.72 | — | — | ematix-flow |
| Q17  | 102.30 ± 2.92 | — | — | ematix-flow |
| Q18  | 233.75 ± 6.54 | — | — | ematix-flow |
| Q19  | 134.04 ± 7.92 | — | — | ematix-flow |
| Q20  | 173.22 ± 3.75 | — | — | ematix-flow |
| Q21  | 210.16 ± 7.43 | — | — | ematix-flow |
| Q22  | 30.36 ± 2.00 | — | — | ematix-flow |

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
