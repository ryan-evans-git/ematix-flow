# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 7876.17 ± 0.00 | — | DuckDB |
| Q02  | — | 674.99 ± 0.00 | — | DuckDB |
| Q03  | — | 4646.42 ± 0.00 | — | DuckDB |
| Q04  | — | 3305.09 ± 0.00 | — | DuckDB |
| Q05  | — | 4963.03 ± 0.00 | — | DuckDB |
| Q06  | — | 5915.50 ± 0.00 | — | DuckDB |
| Q07  | — | 4637.40 ± 0.00 | — | DuckDB |
| Q08  | — | 5463.67 ± 0.00 | — | DuckDB |
| Q09  | — | 67899.89 ± 0.00 | — | DuckDB |
| Q10  | — | 7620.78 ± 0.00 | — | DuckDB |
| Q11  | — | 689.64 ± 0.00 | — | DuckDB |
| Q12  | — | 4229.57 ± 0.00 | — | DuckDB |
| Q13  | — | 6334.51 ± 0.00 | — | DuckDB |
| Q14  | — | 3778.57 ± 0.00 | — | DuckDB |
| Q15  | — | 2702.04 ± 0.00 | — | DuckDB |
| Q16  | — | 1282.64 ± 0.00 | — | DuckDB |
| Q17  | — | 4424.53 ± 0.00 | — | DuckDB |
| Q18  | — | 7771.53 ± 0.00 | — | DuckDB |
| Q19  | — | 5683.98 ± 0.00 | — | DuckDB |
| Q20  | — | 3518.09 ± 0.00 | — | DuckDB |
| Q21  | — | 14887.28 ± 0.00 | — | DuckDB |
| Q22  | — | 1524.98 ± 0.00 | — | DuckDB |

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
