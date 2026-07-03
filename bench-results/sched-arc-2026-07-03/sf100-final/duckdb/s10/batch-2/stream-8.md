# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 8961.64 ± 0.00 | — | DuckDB |
| Q02  | — | 1002.87 ± 0.00 | — | DuckDB |
| Q03  | — | 5649.77 ± 0.00 | — | DuckDB |
| Q04  | — | 3551.11 ± 0.00 | — | DuckDB |
| Q05  | — | 4865.81 ± 0.00 | — | DuckDB |
| Q06  | — | 1947.97 ± 0.00 | — | DuckDB |
| Q07  | — | 5713.23 ± 0.00 | — | DuckDB |
| Q08  | — | 18411.99 ± 0.00 | — | DuckDB |
| Q09  | — | 13376.14 ± 0.00 | — | DuckDB |
| Q10  | — | 5000.41 ± 0.00 | — | DuckDB |
| Q11  | — | 656.53 ± 0.00 | — | DuckDB |
| Q12  | — | 7329.50 ± 0.00 | — | DuckDB |
| Q13  | — | 8136.25 ± 0.00 | — | DuckDB |
| Q14  | — | 4382.90 ± 0.00 | — | DuckDB |
| Q15  | — | 3808.98 ± 0.00 | — | DuckDB |
| Q16  | — | 1192.32 ± 0.00 | — | DuckDB |
| Q17  | — | 4153.18 ± 0.00 | — | DuckDB |
| Q18  | — | 9006.03 ± 0.00 | — | DuckDB |
| Q19  | — | 5350.56 ± 0.00 | — | DuckDB |
| Q20  | — | 3814.49 ± 0.00 | — | DuckDB |
| Q21  | — | 26006.72 ± 0.00 | — | DuckDB |
| Q22  | — | 1706.83 ± 0.00 | — | DuckDB |

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
