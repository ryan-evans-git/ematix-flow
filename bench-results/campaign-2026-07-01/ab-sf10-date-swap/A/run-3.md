# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 260.98 ± 13.48 | — | — | ematix-flow |
| Q02  | 20.45 ± 0.63 | — | — | ematix-flow |
| Q03  | 123.79 ± 4.78 | — | — | ematix-flow |
| Q04  | 56.20 ± 2.53 | — | — | ematix-flow |
| Q05  | 152.56 ± 6.15 | — | — | ematix-flow |
| Q06  | 26.03 ± 3.04 | — | — | ematix-flow |
| Q07  | 128.47 ± 2.98 | — | — | ematix-flow |
| Q08  | 154.56 ± 4.35 | — | — | ematix-flow |
| Q09  | 290.44 ± 4.37 | — | — | ematix-flow |
| Q10  | 198.83 ± 6.88 | — | — | ematix-flow |
| Q11  | 13.82 ± 1.03 | — | — | ematix-flow |
| Q12  | 91.44 ± 4.76 | — | — | ematix-flow |
| Q13  | 102.20 ± 4.49 | — | — | ematix-flow |
| Q14  | 84.73 ± 3.76 | — | — | ematix-flow |
| Q15  | 62.02 ± 3.06 | — | — | ematix-flow |
| Q16  | 53.60 ± 3.08 | — | — | ematix-flow |
| Q17  | 82.03 ± 8.20 | — | — | ematix-flow |
| Q18  | 191.33 ± 5.80 | — | — | ematix-flow |
| Q19  | 130.28 ± 6.54 | — | — | ematix-flow |
| Q20  | 94.82 ± 4.87 | — | — | ematix-flow |
| Q21  | 189.19 ± 7.81 | — | — | ematix-flow |
| Q22  | 24.07 ± 1.25 | — | — | ematix-flow |

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
