# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 1972.30 ± 0.00 | — | DuckDB |
| Q02  | — | 357.52 ± 0.00 | — | DuckDB |
| Q03  | — | 1290.90 ± 0.00 | — | DuckDB |
| Q04  | — | 688.01 ± 0.00 | — | DuckDB |
| Q05  | — | 133.89 ± 0.00 | — | DuckDB |
| Q06  | — | 99.36 ± 0.00 | — | DuckDB |
| Q07  | — | 1249.36 ± 0.00 | — | DuckDB |
| Q08  | — | 1335.41 ± 0.00 | — | DuckDB |
| Q09  | — | 2109.26 ± 0.00 | — | DuckDB |
| Q10  | — | 1662.44 ± 0.00 | — | DuckDB |
| Q11  | — | 76.34 ± 0.00 | — | DuckDB |
| Q12  | — | 619.80 ± 0.00 | — | DuckDB |
| Q13  | — | 733.08 ± 0.00 | — | DuckDB |
| Q14  | — | 691.13 ± 0.00 | — | DuckDB |
| Q15  | — | 84.78 ± 0.00 | — | DuckDB |
| Q16  | — | 432.93 ± 0.00 | — | DuckDB |
| Q17  | — | 1124.52 ± 0.00 | — | DuckDB |
| Q18  | — | 1419.97 ± 0.00 | — | DuckDB |
| Q19  | — | 1567.70 ± 0.00 | — | DuckDB |
| Q20  | — | 1243.17 ± 0.00 | — | DuckDB |
| Q21  | — | 395.88 ± 0.00 | — | DuckDB |
| Q22  | — | 512.89 ± 0.00 | — | DuckDB |

## Wins

- **ematix-flow**: 0
- **DuckDB**: 22
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions = std::thread::available_parallelism()` (override via `PARTITIONS=N`) and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
