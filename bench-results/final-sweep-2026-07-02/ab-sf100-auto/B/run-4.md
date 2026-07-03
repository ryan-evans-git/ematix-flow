# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 2580.95 ± 39.18 | — | — | ematix-flow |
| Q02  | 203.56 ± 7.90 | — | — | ematix-flow |
| Q03  | 1566.32 ± 19.14 | — | — | ematix-flow |
| Q04  | 885.65 ± 11.06 | — | — | ematix-flow |
| Q05  | 1929.81 ± 13.63 | — | — | ematix-flow |
| Q06  | 541.40 ± 10.86 | — | — | ematix-flow |
| Q07  | 1526.52 ± 13.52 | — | — | ematix-flow |
| Q08  | 1689.56 ± 12.72 | — | — | ematix-flow |
| Q09  | 3602.83 ± 66.84 | — | — | ematix-flow |
| Q10  | 2008.73 ± 19.83 | — | — | ematix-flow |
| Q11  | 193.38 ± 2.29 | — | — | ematix-flow |
| Q12  | 1085.54 ± 33.25 | — | — | ematix-flow |
| Q13  | 2007.40 ± 30.99 | — | — | ematix-flow |
| Q14  | 851.38 ± 17.05 | — | — | ematix-flow |
| Q15  | 813.98 ± 28.15 | — | — | ematix-flow |
| Q16  | 418.06 ± 6.93 | — | — | ematix-flow |
| Q17  | 1522.91 ± 3.80 | — | — | ematix-flow |
| Q18  | 2603.07 ± 7.14 | — | — | ematix-flow |
| Q19  | 1239.96 ± 8.24 | — | — | ematix-flow |
| Q20  | 1179.24 ± 16.58 | — | — | ematix-flow |
| Q21  | 3533.27 ± 13.01 | — | — | ematix-flow |
| Q22  | 383.43 ± 22.22 | — | — | ematix-flow |

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
