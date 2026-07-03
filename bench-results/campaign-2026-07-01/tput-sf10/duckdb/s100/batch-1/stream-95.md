# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 2067.75 ± 0.00 | — | DuckDB |
| Q02  | — | 222.57 ± 0.00 | — | DuckDB |
| Q03  | — | 1047.20 ± 0.00 | — | DuckDB |
| Q04  | — | 722.27 ± 0.00 | — | DuckDB |
| Q05  | — | 1104.94 ± 0.00 | — | DuckDB |
| Q06  | — | 584.05 ± 0.00 | — | DuckDB |
| Q07  | — | 1124.03 ± 0.00 | — | DuckDB |
| Q08  | — | 1264.08 ± 0.00 | — | DuckDB |
| Q09  | — | 2208.79 ± 0.00 | — | DuckDB |
| Q10  | — | 1710.80 ± 0.00 | — | DuckDB |
| Q11  | — | 266.05 ± 0.00 | — | DuckDB |
| Q12  | — | 781.64 ± 0.00 | — | DuckDB |
| Q13  | — | 2069.86 ± 0.00 | — | DuckDB |
| Q14  | — | 994.06 ± 0.00 | — | DuckDB |
| Q15  | — | 587.14 ± 0.00 | — | DuckDB |
| Q16  | — | 413.14 ± 0.00 | — | DuckDB |
| Q17  | — | 885.41 ± 0.00 | — | DuckDB |
| Q18  | — | 1213.89 ± 0.00 | — | DuckDB |
| Q19  | — | 1396.14 ± 0.00 | — | DuckDB |
| Q20  | — | 873.15 ± 0.00 | — | DuckDB |
| Q21  | — | 3012.18 ± 0.00 | — | DuckDB |
| Q22  | — | 411.24 ± 0.00 | — | DuckDB |

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
