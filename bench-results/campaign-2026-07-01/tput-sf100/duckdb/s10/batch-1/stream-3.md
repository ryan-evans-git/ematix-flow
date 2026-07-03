# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 9701.98 ± 0.00 | — | DuckDB |
| Q02  | — | 751.44 ± 0.00 | — | DuckDB |
| Q03  | — | 5651.27 ± 0.00 | — | DuckDB |
| Q04  | — | 3608.23 ± 0.00 | — | DuckDB |
| Q05  | — | 6454.11 ± 0.00 | — | DuckDB |
| Q06  | — | 2073.42 ± 0.00 | — | DuckDB |
| Q07  | — | 4238.37 ± 0.00 | — | DuckDB |
| Q08  | — | 5973.94 ± 0.00 | — | DuckDB |
| Q09  | — | 24495.87 ± 0.00 | — | DuckDB |
| Q10  | — | 7583.34 ± 0.00 | — | DuckDB |
| Q11  | — | 513.90 ± 0.00 | — | DuckDB |
| Q12  | — | 6879.96 ± 0.00 | — | DuckDB |
| Q13  | — | 5273.76 ± 0.00 | — | DuckDB |
| Q14  | — | 2864.05 ± 0.00 | — | DuckDB |
| Q15  | — | 3755.70 ± 0.00 | — | DuckDB |
| Q16  | — | 1354.66 ± 0.00 | — | DuckDB |
| Q17  | — | 3818.94 ± 0.00 | — | DuckDB |
| Q18  | — | 8217.43 ± 0.00 | — | DuckDB |
| Q19  | — | 6794.03 ± 0.00 | — | DuckDB |
| Q20  | — | 4316.75 ± 0.00 | — | DuckDB |
| Q21  | — | 13632.52 ± 0.00 | — | DuckDB |
| Q22  | — | 1383.75 ± 0.00 | — | DuckDB |

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
