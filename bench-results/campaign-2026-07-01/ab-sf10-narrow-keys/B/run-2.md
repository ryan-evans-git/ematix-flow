# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 255.00 ± 13.24 | — | — | ematix-flow |
| Q02  | 24.42 ± 1.40 | — | — | ematix-flow |
| Q03  | 119.22 ± 2.57 | — | — | ematix-flow |
| Q04  | 71.16 ± 3.01 | — | — | ematix-flow |
| Q05  | 111.33 ± 4.37 | — | — | ematix-flow |
| Q06  | 26.25 ± 4.36 | — | — | ematix-flow |
| Q07  | 135.69 ± 2.82 | — | — | ematix-flow |
| Q08  | 277.90 ± 6.41 | — | — | ematix-flow |
| Q09  | 275.26 ± 9.87 | — | — | ematix-flow |
| Q10  | 196.80 ± 8.24 | — | — | ematix-flow |
| Q11  | 17.22 ± 0.85 | — | — | ematix-flow |
| Q12  | 103.31 ± 6.32 | — | — | ematix-flow |
| Q13  | 101.48 ± 2.47 | — | — | ematix-flow |
| Q14  | 85.54 ± 3.00 | — | — | ematix-flow |
| Q15  | 63.81 ± 3.45 | — | — | ematix-flow |
| Q16  | 52.31 ± 2.98 | — | — | ematix-flow |
| Q17  | 98.63 ± 3.53 | — | — | ematix-flow |
| Q18  | 239.02 ± 4.93 | — | — | ematix-flow |
| Q19  | 128.86 ± 4.97 | — | — | ematix-flow |
| Q20  | 178.58 ± 5.59 | — | — | ematix-flow |
| Q21  | 210.32 ± 8.38 | — | — | ematix-flow |
| Q22  | 29.97 ± 1.43 | — | — | ematix-flow |

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
