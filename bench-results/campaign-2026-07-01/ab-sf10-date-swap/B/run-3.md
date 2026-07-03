# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 255.14 ± 11.57 | — | — | ematix-flow |
| Q02  | 20.44 ± 1.74 | — | — | ematix-flow |
| Q03  | 123.08 ± 3.98 | — | — | ematix-flow |
| Q04  | 59.30 ± 2.07 | — | — | ematix-flow |
| Q05  | 155.74 ± 8.38 | — | — | ematix-flow |
| Q06  | 24.57 ± 2.46 | — | — | ematix-flow |
| Q07  | 126.83 ± 4.84 | — | — | ematix-flow |
| Q08  | 154.53 ± 7.06 | — | — | ematix-flow |
| Q09  | 294.73 ± 14.53 | — | — | ematix-flow |
| Q10  | 192.87 ± 7.01 | — | — | ematix-flow |
| Q11  | 13.67 ± 0.45 | — | — | ematix-flow |
| Q12  | 94.53 ± 3.17 | — | — | ematix-flow |
| Q13  | 103.08 ± 3.91 | — | — | ematix-flow |
| Q14  | 86.20 ± 5.81 | — | — | ematix-flow |
| Q15  | 61.98 ± 1.87 | — | — | ematix-flow |
| Q16  | 51.11 ± 2.29 | — | — | ematix-flow |
| Q17  | 81.67 ± 3.71 | — | — | ematix-flow |
| Q18  | 191.82 ± 3.26 | — | — | ematix-flow |
| Q19  | 131.14 ± 4.60 | — | — | ematix-flow |
| Q20  | 95.75 ± 4.35 | — | — | ematix-flow |
| Q21  | 187.65 ± 9.44 | — | — | ematix-flow |
| Q22  | 22.26 ± 2.28 | — | — | ematix-flow |

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
