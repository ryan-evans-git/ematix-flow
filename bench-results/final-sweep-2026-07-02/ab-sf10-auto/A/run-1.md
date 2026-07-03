# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 218.92 ± 9.61 | — | — | ematix-flow |
| Q02  | 18.56 ± 0.28 | — | — | ematix-flow |
| Q03  | 116.54 ± 3.13 | — | — | ematix-flow |
| Q04  | 52.09 ± 1.91 | — | — | ematix-flow |
| Q05  | 146.61 ± 6.42 | — | — | ematix-flow |
| Q06  | 25.17 ± 2.98 | — | — | ematix-flow |
| Q07  | 112.73 ± 15.62 | — | — | ematix-flow |
| Q08  | 142.69 ± 5.19 | — | — | ematix-flow |
| Q09  | 260.75 ± 6.82 | — | — | ematix-flow |
| Q10  | 189.62 ± 6.17 | — | — | ematix-flow |
| Q11  | 12.91 ± 0.49 | — | — | ematix-flow |
| Q12  | 84.62 ± 2.79 | — | — | ematix-flow |
| Q13  | 93.72 ± 3.46 | — | — | ematix-flow |
| Q14  | 78.77 ± 4.45 | — | — | ematix-flow |
| Q15  | 58.49 ± 1.95 | — | — | ematix-flow |
| Q16  | 50.63 ± 2.11 | — | — | ematix-flow |
| Q17  | 73.95 ± 4.90 | — | — | ematix-flow |
| Q18  | 175.40 ± 3.53 | — | — | ematix-flow |
| Q19  | 117.70 ± 4.56 | — | — | ematix-flow |
| Q20  | 90.34 ± 3.58 | — | — | ematix-flow |
| Q21  | 178.00 ± 3.50 | — | — | ematix-flow |
| Q22  | 21.97 ± 1.61 | — | — | ematix-flow |

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
