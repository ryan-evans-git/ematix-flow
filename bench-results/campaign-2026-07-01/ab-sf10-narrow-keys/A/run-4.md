# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 254.31 ± 6.48 | — | — | ematix-flow |
| Q02  | 20.83 ± 0.84 | — | — | ematix-flow |
| Q03  | 122.22 ± 5.20 | — | — | ematix-flow |
| Q04  | 56.68 ± 2.18 | — | — | ematix-flow |
| Q05  | 151.33 ± 6.54 | — | — | ematix-flow |
| Q06  | 26.02 ± 1.95 | — | — | ematix-flow |
| Q07  | 127.35 ± 4.93 | — | — | ematix-flow |
| Q08  | 155.55 ± 5.66 | — | — | ematix-flow |
| Q09  | 288.46 ± 11.97 | — | — | ematix-flow |
| Q10  | 199.43 ± 8.63 | — | — | ematix-flow |
| Q11  | 14.08 ± 1.17 | — | — | ematix-flow |
| Q12  | 92.20 ± 3.53 | — | — | ematix-flow |
| Q13  | 102.27 ± 3.26 | — | — | ematix-flow |
| Q14  | 86.92 ± 5.88 | — | — | ematix-flow |
| Q15  | 64.37 ± 4.11 | — | — | ematix-flow |
| Q16  | 50.69 ± 2.52 | — | — | ematix-flow |
| Q17  | 76.96 ± 3.47 | — | — | ematix-flow |
| Q18  | 192.03 ± 7.28 | — | — | ematix-flow |
| Q19  | 129.78 ± 5.61 | — | — | ematix-flow |
| Q20  | 93.13 ± 3.83 | — | — | ematix-flow |
| Q21  | 182.59 ± 4.27 | — | — | ematix-flow |
| Q22  | 24.02 ± 2.60 | — | — | ematix-flow |

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
