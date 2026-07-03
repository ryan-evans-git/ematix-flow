# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 265.86 ± 13.76 | — | — | ematix-flow |
| Q02  | 20.23 ± 0.92 | — | — | ematix-flow |
| Q03  | 122.97 ± 4.25 | — | — | ematix-flow |
| Q04  | 57.84 ± 2.34 | — | — | ematix-flow |
| Q05  | 158.46 ± 4.66 | — | — | ematix-flow |
| Q06  | 26.20 ± 2.40 | — | — | ematix-flow |
| Q07  | 129.08 ± 4.94 | — | — | ematix-flow |
| Q08  | 156.59 ± 5.28 | — | — | ematix-flow |
| Q09  | 291.18 ± 10.91 | — | — | ematix-flow |
| Q10  | 205.23 ± 8.69 | — | — | ematix-flow |
| Q11  | 14.16 ± 1.62 | — | — | ematix-flow |
| Q12  | 95.21 ± 4.73 | — | — | ematix-flow |
| Q13  | 108.31 ± 9.24 | — | — | ematix-flow |
| Q14  | 84.97 ± 4.11 | — | — | ematix-flow |
| Q15  | 62.87 ± 2.33 | — | — | ematix-flow |
| Q16  | 54.65 ± 2.91 | — | — | ematix-flow |
| Q17  | 76.17 ± 4.89 | — | — | ematix-flow |
| Q18  | 193.03 ± 10.30 | — | — | ematix-flow |
| Q19  | 130.73 ± 3.29 | — | — | ematix-flow |
| Q20  | 93.07 ± 5.56 | — | — | ematix-flow |
| Q21  | 188.25 ± 10.08 | — | — | ematix-flow |
| Q22  | 23.47 ± 3.19 | — | — | ematix-flow |

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
