# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 1955.14 ± 0.00 | — | DuckDB |
| Q02  | — | 196.66 ± 0.00 | — | DuckDB |
| Q03  | — | 1068.57 ± 0.00 | — | DuckDB |
| Q04  | — | 805.72 ± 0.00 | — | DuckDB |
| Q05  | — | 986.52 ± 0.00 | — | DuckDB |
| Q06  | — | 418.69 ± 0.00 | — | DuckDB |
| Q07  | — | 1103.13 ± 0.00 | — | DuckDB |
| Q08  | — | 1265.74 ± 0.00 | — | DuckDB |
| Q09  | — | 2158.12 ± 0.00 | — | DuckDB |
| Q10  | — | 1758.10 ± 0.00 | — | DuckDB |
| Q11  | — | 108.81 ± 0.00 | — | DuckDB |
| Q12  | — | 682.07 ± 0.00 | — | DuckDB |
| Q13  | — | 1886.48 ± 0.00 | — | DuckDB |
| Q14  | — | 1035.56 ± 0.00 | — | DuckDB |
| Q15  | — | 345.87 ± 0.00 | — | DuckDB |
| Q16  | — | 408.64 ± 0.00 | — | DuckDB |
| Q17  | — | 1279.55 ± 0.00 | — | DuckDB |
| Q18  | — | 1497.29 ± 0.00 | — | DuckDB |
| Q19  | — | 1217.14 ± 0.00 | — | DuckDB |
| Q20  | — | 981.99 ± 0.00 | — | DuckDB |
| Q21  | — | 3021.75 ± 0.00 | — | DuckDB |
| Q22  | — | 213.80 ± 0.00 | — | DuckDB |

## Wins

- **ematix-flow**: 0
- **DuckDB**: 22
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow's `target_partitions` resolves as: explicit `PARTITIONS=N`, else the `EMAT_TARGET_PARTITIONS` tri-state (`=N` force, `=0` legacy `available_parallelism()`, unset = AUTO cross-process sensing — solo processes get full cores). The InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules are registered.

## Failures and dialect gaps

_None — every engine ran every query._
