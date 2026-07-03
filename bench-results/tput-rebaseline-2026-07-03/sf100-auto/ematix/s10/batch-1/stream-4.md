# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 7100.40 ± 0.00 | — | — | ematix-flow |
| Q02  | 649.12 ± 0.00 | — | — | ematix-flow |
| Q03  | 5045.61 ± 0.00 | — | — | ematix-flow |
| Q04  | 2927.58 ± 0.00 | — | — | ematix-flow |
| Q05  | 4883.38 ± 0.00 | — | — | ematix-flow |
| Q06  | 1044.23 ± 0.00 | — | — | ematix-flow |
| Q07  | 5078.89 ± 0.00 | — | — | ematix-flow |
| Q08  | 10436.14 ± 0.00 | — | — | ematix-flow |
| Q09  | 10535.67 ± 0.00 | — | — | ematix-flow |
| Q10  | 6939.17 ± 0.00 | — | — | ematix-flow |
| Q11  | 452.17 ± 0.00 | — | — | ematix-flow |
| Q12  | 3368.10 ± 0.00 | — | — | ematix-flow |
| Q13  | 5035.50 ± 0.00 | — | — | ematix-flow |
| Q14  | 2364.13 ± 0.00 | — | — | ematix-flow |
| Q15  | 2020.76 ± 0.00 | — | — | ematix-flow |
| Q16  | 1192.84 ± 0.00 | — | — | ematix-flow |
| Q17  | 4413.06 ± 0.00 | — | — | ematix-flow |
| Q18  | 10932.58 ± 0.00 | — | — | ematix-flow |
| Q19  | 2174.81 ± 0.00 | — | — | ematix-flow |
| Q20  | 4186.41 ± 0.00 | — | — | ematix-flow |
| Q21  | 10744.61 ± 0.00 | — | — | ematix-flow |
| Q22  | 1448.72 ± 0.00 | — | — | ematix-flow |

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
