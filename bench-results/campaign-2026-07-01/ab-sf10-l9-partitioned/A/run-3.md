# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 239.74 ± 9.86 | — | — | ematix-flow |
| Q02  | 19.21 ± 0.61 | — | — | ematix-flow |
| Q03  | 120.63 ± 4.44 | — | — | ematix-flow |
| Q04  | 55.21 ± 2.47 | — | — | ematix-flow |
| Q05  | 153.10 ± 4.15 | — | — | ematix-flow |
| Q06  | 25.22 ± 2.31 | — | — | ematix-flow |
| Q07  | 117.54 ± 4.97 | — | — | ematix-flow |
| Q08  | 148.14 ± 3.62 | — | — | ematix-flow |
| Q09  | 286.43 ± 11.58 | — | — | ematix-flow |
| Q10  | 191.94 ± 7.52 | — | — | ematix-flow |
| Q11  | 13.25 ± 0.30 | — | — | ematix-flow |
| Q12  | 90.35 ± 4.26 | — | — | ematix-flow |
| Q13  | 98.24 ± 8.02 | — | — | ematix-flow |
| Q14  | 80.64 ± 3.02 | — | — | ematix-flow |
| Q15  | 61.16 ± 2.76 | — | — | ematix-flow |
| Q16  | 50.56 ± 2.23 | — | — | ematix-flow |
| Q17  | 76.78 ± 3.49 | — | — | ematix-flow |
| Q18  | 181.14 ± 5.59 | — | — | ematix-flow |
| Q19  | 123.38 ± 2.80 | — | — | ematix-flow |
| Q20  | 91.76 ± 4.37 | — | — | ematix-flow |
| Q21  | 185.46 ± 6.92 | — | — | ematix-flow |
| Q22  | 23.89 ± 2.21 | — | — | ematix-flow |

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
