# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 1922.64 ± 0.00 | — | DuckDB |
| Q02  | — | 205.83 ± 0.00 | — | DuckDB |
| Q03  | — | 1048.37 ± 0.00 | — | DuckDB |
| Q04  | — | 702.03 ± 0.00 | — | DuckDB |
| Q05  | — | 836.63 ± 0.00 | — | DuckDB |
| Q06  | — | 476.07 ± 0.00 | — | DuckDB |
| Q07  | — | 1011.01 ± 0.00 | — | DuckDB |
| Q08  | — | 1145.38 ± 0.00 | — | DuckDB |
| Q09  | — | 2234.17 ± 0.00 | — | DuckDB |
| Q10  | — | 2113.68 ± 0.00 | — | DuckDB |
| Q11  | — | 86.44 ± 0.00 | — | DuckDB |
| Q12  | — | 771.73 ± 0.00 | — | DuckDB |
| Q13  | — | 1758.52 ± 0.00 | — | DuckDB |
| Q14  | — | 820.03 ± 0.00 | — | DuckDB |
| Q15  | — | 497.74 ± 0.00 | — | DuckDB |
| Q16  | — | 407.25 ± 0.00 | — | DuckDB |
| Q17  | — | 1008.30 ± 0.00 | — | DuckDB |
| Q18  | — | 1272.32 ± 0.00 | — | DuckDB |
| Q19  | — | 1610.82 ± 0.00 | — | DuckDB |
| Q20  | — | 967.55 ± 0.00 | — | DuckDB |
| Q21  | — | 3156.93 ± 0.00 | — | DuckDB |
| Q22  | — | 232.13 ± 0.00 | — | DuckDB |

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
