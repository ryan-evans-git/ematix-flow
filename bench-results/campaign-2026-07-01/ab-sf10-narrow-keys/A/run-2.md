# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 255.73 ± 7.13 | — | — | ematix-flow |
| Q02  | 20.37 ± 1.53 | — | — | ematix-flow |
| Q03  | 122.69 ± 5.23 | — | — | ematix-flow |
| Q04  | 55.88 ± 1.66 | — | — | ematix-flow |
| Q05  | 154.06 ± 5.68 | — | — | ematix-flow |
| Q06  | 25.39 ± 2.82 | — | — | ematix-flow |
| Q07  | 126.75 ± 4.50 | — | — | ematix-flow |
| Q08  | 156.34 ± 5.19 | — | — | ematix-flow |
| Q09  | 289.72 ± 7.03 | — | — | ematix-flow |
| Q10  | 202.42 ± 3.84 | — | — | ematix-flow |
| Q11  | 13.93 ± 0.71 | — | — | ematix-flow |
| Q12  | 92.11 ± 3.59 | — | — | ematix-flow |
| Q13  | 101.97 ± 6.50 | — | — | ematix-flow |
| Q14  | 82.42 ± 3.59 | — | — | ematix-flow |
| Q15  | 61.49 ± 3.02 | — | — | ematix-flow |
| Q16  | 50.57 ± 2.09 | — | — | ematix-flow |
| Q17  | 80.07 ± 3.53 | — | — | ematix-flow |
| Q18  | 191.83 ± 5.16 | — | — | ematix-flow |
| Q19  | 128.42 ± 7.03 | — | — | ematix-flow |
| Q20  | 96.39 ± 4.84 | — | — | ematix-flow |
| Q21  | 189.49 ± 10.56 | — | — | ematix-flow |
| Q22  | 23.89 ± 1.98 | — | — | ematix-flow |

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
