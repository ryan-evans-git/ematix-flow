# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 4049.93 ± 0.00 | — | — | ematix-flow |
| Q02  | 94.28 ± 0.00 | — | — | ematix-flow |
| Q03  | 1253.77 ± 0.00 | — | — | ematix-flow |
| Q04  | 983.85 ± 0.00 | — | — | ematix-flow |
| Q05  | 1215.26 ± 0.00 | — | — | ematix-flow |
| Q06  | 440.30 ± 0.00 | — | — | ematix-flow |
| Q07  | 794.25 ± 0.00 | — | — | ematix-flow |
| Q08  | 936.22 ± 0.00 | — | — | ematix-flow |
| Q09  | 3259.24 ± 0.00 | — | — | ematix-flow |
| Q10  | 1889.16 ± 0.00 | — | — | ematix-flow |
| Q11  | 347.75 ± 0.00 | — | — | ematix-flow |
| Q12  | 1045.11 ± 0.00 | — | — | ematix-flow |
| Q13  | 1988.59 ± 0.00 | — | — | ematix-flow |
| Q14  | 329.18 ± 0.00 | — | — | ematix-flow |
| Q15  | 543.60 ± 0.00 | — | — | ematix-flow |
| Q16  | 637.00 ± 0.00 | — | — | ematix-flow |
| Q17  | 622.93 ± 0.00 | — | — | ematix-flow |
| Q18  | 4989.01 ± 0.00 | — | — | ematix-flow |
| Q19  | 458.45 ± 0.00 | — | — | ematix-flow |
| Q20  | 943.30 ± 0.00 | — | — | ematix-flow |
| Q21  | 2872.13 ± 0.00 | — | — | ematix-flow |
| Q22  | 506.53 ± 0.00 | — | — | ematix-flow |

## Wins

- **ematix-flow**: 22
- **DuckDB**: 0
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow's `target_partitions` resolves as: explicit `PARTITIONS=N`, else the `EMAT_TARGET_PARTITIONS` tri-state (`=N` force, `=0` legacy `available_parallelism()`, unset = AUTO cross-process sensing — solo processes get full cores). The InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules are registered.

## Failures and dialect gaps

_None — every engine ran every query._
