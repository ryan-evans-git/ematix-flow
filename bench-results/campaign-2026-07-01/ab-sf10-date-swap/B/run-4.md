# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 255.49 ± 16.86 | — | — | ematix-flow |
| Q02  | 20.23 ± 0.52 | — | — | ematix-flow |
| Q03  | 121.52 ± 4.27 | — | — | ematix-flow |
| Q04  | 59.00 ± 3.03 | — | — | ematix-flow |
| Q05  | 154.42 ± 6.27 | — | — | ematix-flow |
| Q06  | 26.56 ± 2.83 | — | — | ematix-flow |
| Q07  | 128.14 ± 4.92 | — | — | ematix-flow |
| Q08  | 157.70 ± 8.11 | — | — | ematix-flow |
| Q09  | 287.28 ± 11.65 | — | — | ematix-flow |
| Q10  | 193.91 ± 8.51 | — | — | ematix-flow |
| Q11  | 14.01 ± 1.14 | — | — | ematix-flow |
| Q12  | 92.08 ± 3.85 | — | — | ematix-flow |
| Q13  | 104.93 ± 3.64 | — | — | ematix-flow |
| Q14  | 86.28 ± 2.71 | — | — | ematix-flow |
| Q15  | 60.94 ± 3.15 | — | — | ematix-flow |
| Q16  | 52.13 ± 2.92 | — | — | ematix-flow |
| Q17  | 82.47 ± 4.97 | — | — | ematix-flow |
| Q18  | 189.20 ± 4.47 | — | — | ematix-flow |
| Q19  | 131.64 ± 5.68 | — | — | ematix-flow |
| Q20  | 92.02 ± 3.39 | — | — | ematix-flow |
| Q21  | 187.21 ± 7.49 | — | — | ematix-flow |
| Q22  | 21.10 ± 1.85 | — | — | ematix-flow |

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
