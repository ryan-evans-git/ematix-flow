# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 257.30 ± 13.69 | — | — | ematix-flow |
| Q02  | 20.83 ± 1.24 | — | — | ematix-flow |
| Q03  | 125.11 ± 3.90 | — | — | ematix-flow |
| Q04  | 57.44 ± 3.38 | — | — | ematix-flow |
| Q05  | 152.97 ± 9.29 | — | — | ematix-flow |
| Q06  | 26.43 ± 2.49 | — | — | ematix-flow |
| Q07  | 129.64 ± 3.44 | — | — | ematix-flow |
| Q08  | 158.96 ± 6.92 | — | — | ematix-flow |
| Q09  | 293.57 ± 10.52 | — | — | ematix-flow |
| Q10  | 200.62 ± 4.88 | — | — | ematix-flow |
| Q11  | 13.71 ± 0.71 | — | — | ematix-flow |
| Q12  | 92.68 ± 4.88 | — | — | ematix-flow |
| Q13  | 101.01 ± 5.06 | — | — | ematix-flow |
| Q14  | 82.07 ± 3.74 | — | — | ematix-flow |
| Q15  | 61.73 ± 2.71 | — | — | ematix-flow |
| Q16  | 52.28 ± 1.89 | — | — | ematix-flow |
| Q17  | 83.61 ± 3.73 | — | — | ematix-flow |
| Q18  | 191.01 ± 9.01 | — | — | ematix-flow |
| Q19  | 129.15 ± 7.03 | — | — | ematix-flow |
| Q20  | 99.20 ± 6.50 | — | — | ematix-flow |
| Q21  | 185.25 ± 6.36 | — | — | ematix-flow |
| Q22  | 24.08 ± 1.81 | — | — | ematix-flow |

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
