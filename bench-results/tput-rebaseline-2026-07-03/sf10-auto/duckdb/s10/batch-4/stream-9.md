# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 2273.37 ± 0.00 | — | DuckDB |
| Q02  | — | 179.19 ± 0.00 | — | DuckDB |
| Q03  | — | 1290.16 ± 0.00 | — | DuckDB |
| Q04  | — | 658.44 ± 0.00 | — | DuckDB |
| Q05  | — | 1067.19 ± 0.00 | — | DuckDB |
| Q06  | — | 440.75 ± 0.00 | — | DuckDB |
| Q07  | — | 1050.69 ± 0.00 | — | DuckDB |
| Q08  | — | 1281.77 ± 0.00 | — | DuckDB |
| Q09  | — | 2389.12 ± 0.00 | — | DuckDB |
| Q10  | — | 1824.07 ± 0.00 | — | DuckDB |
| Q11  | — | 128.22 ± 0.00 | — | DuckDB |
| Q12  | — | 694.05 ± 0.00 | — | DuckDB |
| Q13  | — | 1948.91 ± 0.00 | — | DuckDB |
| Q14  | — | 1094.83 ± 0.00 | — | DuckDB |
| Q15  | — | 196.38 ± 0.00 | — | DuckDB |
| Q16  | — | 281.71 ± 0.00 | — | DuckDB |
| Q17  | — | 1149.32 ± 0.00 | — | DuckDB |
| Q18  | — | 1448.37 ± 0.00 | — | DuckDB |
| Q19  | — | 1430.32 ± 0.00 | — | DuckDB |
| Q20  | — | 501.86 ± 0.00 | — | DuckDB |
| Q21  | — | 2849.06 ± 0.00 | — | DuckDB |
| Q22  | — | 294.65 ± 0.00 | — | DuckDB |

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
