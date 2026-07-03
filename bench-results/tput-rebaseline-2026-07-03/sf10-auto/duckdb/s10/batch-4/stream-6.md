# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 2145.08 ± 0.00 | — | DuckDB |
| Q02  | — | 294.61 ± 0.00 | — | DuckDB |
| Q03  | — | 607.48 ± 0.00 | — | DuckDB |
| Q04  | — | 804.71 ± 0.00 | — | DuckDB |
| Q05  | — | 1164.71 ± 0.00 | — | DuckDB |
| Q06  | — | 414.58 ± 0.00 | — | DuckDB |
| Q07  | — | 1015.27 ± 0.00 | — | DuckDB |
| Q08  | — | 1426.33 ± 0.00 | — | DuckDB |
| Q09  | — | 2366.80 ± 0.00 | — | DuckDB |
| Q10  | — | 1564.32 ± 0.00 | — | DuckDB |
| Q11  | — | 128.67 ± 0.00 | — | DuckDB |
| Q12  | — | 749.84 ± 0.00 | — | DuckDB |
| Q13  | — | 2128.33 ± 0.00 | — | DuckDB |
| Q14  | — | 796.64 ± 0.00 | — | DuckDB |
| Q15  | — | 651.22 ± 0.00 | — | DuckDB |
| Q16  | — | 511.90 ± 0.00 | — | DuckDB |
| Q17  | — | 1043.19 ± 0.00 | — | DuckDB |
| Q18  | — | 1537.15 ± 0.00 | — | DuckDB |
| Q19  | — | 566.07 ± 0.00 | — | DuckDB |
| Q20  | — | 1179.49 ± 0.00 | — | DuckDB |
| Q21  | — | 2781.59 ± 0.00 | — | DuckDB |
| Q22  | — | 340.24 ± 0.00 | — | DuckDB |

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
