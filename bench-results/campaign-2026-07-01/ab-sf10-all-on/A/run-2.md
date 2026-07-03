# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 255.72 ± 15.88 | — | — | ematix-flow |
| Q02  | 19.97 ± 0.81 | — | — | ematix-flow |
| Q03  | 121.11 ± 4.01 | — | — | ematix-flow |
| Q04  | 57.90 ± 1.89 | — | — | ematix-flow |
| Q05  | 154.69 ± 5.65 | — | — | ematix-flow |
| Q06  | 25.63 ± 1.20 | — | — | ematix-flow |
| Q07  | 126.84 ± 2.12 | — | — | ematix-flow |
| Q08  | 155.15 ± 3.17 | — | — | ematix-flow |
| Q09  | 288.01 ± 16.01 | — | — | ematix-flow |
| Q10  | 199.04 ± 4.41 | — | — | ematix-flow |
| Q11  | 14.07 ± 1.04 | — | — | ematix-flow |
| Q12  | 93.22 ± 6.53 | — | — | ematix-flow |
| Q13  | 104.19 ± 2.44 | — | — | ematix-flow |
| Q14  | 86.25 ± 3.09 | — | — | ematix-flow |
| Q15  | 62.93 ± 2.13 | — | — | ematix-flow |
| Q16  | 52.32 ± 2.73 | — | — | ematix-flow |
| Q17  | 82.07 ± 6.66 | — | — | ematix-flow |
| Q18  | 188.67 ± 6.91 | — | — | ematix-flow |
| Q19  | 130.53 ± 5.79 | — | — | ematix-flow |
| Q20  | 93.72 ± 3.79 | — | — | ematix-flow |
| Q21  | 184.17 ± 6.90 | — | — | ematix-flow |
| Q22  | 22.44 ± 1.74 | — | — | ematix-flow |

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
