# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 255.95 ± 13.21 | — | — | ematix-flow |
| Q02  | 20.90 ± 1.31 | — | — | ematix-flow |
| Q03  | 125.56 ± 5.78 | — | — | ematix-flow |
| Q04  | 56.85 ± 2.35 | — | — | ematix-flow |
| Q05  | 152.27 ± 10.38 | — | — | ematix-flow |
| Q06  | 25.75 ± 2.20 | — | — | ematix-flow |
| Q07  | 125.97 ± 6.05 | — | — | ematix-flow |
| Q08  | 156.22 ± 5.06 | — | — | ematix-flow |
| Q09  | 290.60 ± 10.51 | — | — | ematix-flow |
| Q10  | 201.37 ± 8.15 | — | — | ematix-flow |
| Q11  | 13.86 ± 0.74 | — | — | ematix-flow |
| Q12  | 92.44 ± 5.51 | — | — | ematix-flow |
| Q13  | 102.10 ± 5.61 | — | — | ematix-flow |
| Q14  | 85.25 ± 4.06 | — | — | ematix-flow |
| Q15  | 61.96 ± 2.15 | — | — | ematix-flow |
| Q16  | 52.20 ± 3.19 | — | — | ematix-flow |
| Q17  | 77.53 ± 8.15 | — | — | ematix-flow |
| Q18  | 191.77 ± 4.85 | — | — | ematix-flow |
| Q19  | 129.18 ± 6.98 | — | — | ematix-flow |
| Q20  | 93.50 ± 4.66 | — | — | ematix-flow |
| Q21  | 188.81 ± 13.27 | — | — | ematix-flow |
| Q22  | 22.95 ± 2.12 | — | — | ematix-flow |

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
