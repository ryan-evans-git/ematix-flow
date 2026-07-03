# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 2193.65 ± 0.00 | — | DuckDB |
| Q02  | — | 278.16 ± 0.00 | — | DuckDB |
| Q03  | — | 1107.95 ± 0.00 | — | DuckDB |
| Q04  | — | 682.20 ± 0.00 | — | DuckDB |
| Q05  | — | 809.17 ± 0.00 | — | DuckDB |
| Q06  | — | 563.23 ± 0.00 | — | DuckDB |
| Q07  | — | 1224.55 ± 0.00 | — | DuckDB |
| Q08  | — | 1350.33 ± 0.00 | — | DuckDB |
| Q09  | — | 2403.03 ± 0.00 | — | DuckDB |
| Q10  | — | 2034.85 ± 0.00 | — | DuckDB |
| Q11  | — | 112.70 ± 0.00 | — | DuckDB |
| Q12  | — | 784.47 ± 0.00 | — | DuckDB |
| Q13  | — | 1825.03 ± 0.00 | — | DuckDB |
| Q14  | — | 954.78 ± 0.00 | — | DuckDB |
| Q15  | — | 499.24 ± 0.00 | — | DuckDB |
| Q16  | — | 310.97 ± 0.00 | — | DuckDB |
| Q17  | — | 1163.67 ± 0.00 | — | DuckDB |
| Q18  | — | 1708.67 ± 0.00 | — | DuckDB |
| Q19  | — | 1701.66 ± 0.00 | — | DuckDB |
| Q20  | — | 1148.25 ± 0.00 | — | DuckDB |
| Q21  | — | 3411.30 ± 0.00 | — | DuckDB |
| Q22  | — | 445.37 ± 0.00 | — | DuckDB |

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
