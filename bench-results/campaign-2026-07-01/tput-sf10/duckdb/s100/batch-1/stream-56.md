# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | — | 1887.45 ± 0.00 | — | DuckDB |
| Q02  | — | 154.40 ± 0.00 | — | DuckDB |
| Q03  | — | 1106.56 ± 0.00 | — | DuckDB |
| Q04  | — | 778.64 ± 0.00 | — | DuckDB |
| Q05  | — | 1027.54 ± 0.00 | — | DuckDB |
| Q06  | — | 481.60 ± 0.00 | — | DuckDB |
| Q07  | — | 980.22 ± 0.00 | — | DuckDB |
| Q08  | — | 1250.29 ± 0.00 | — | DuckDB |
| Q09  | — | 2088.78 ± 0.00 | — | DuckDB |
| Q10  | — | 1897.30 ± 0.00 | — | DuckDB |
| Q11  | — | 105.72 ± 0.00 | — | DuckDB |
| Q12  | — | 922.40 ± 0.00 | — | DuckDB |
| Q13  | — | 1909.53 ± 0.00 | — | DuckDB |
| Q14  | — | 998.21 ± 0.00 | — | DuckDB |
| Q15  | — | 861.48 ± 0.00 | — | DuckDB |
| Q16  | — | 333.82 ± 0.00 | — | DuckDB |
| Q17  | — | 1233.33 ± 0.00 | — | DuckDB |
| Q18  | — | 1505.78 ± 0.00 | — | DuckDB |
| Q19  | — | 1303.71 ± 0.00 | — | DuckDB |
| Q20  | — | 1002.84 ± 0.00 | — | DuckDB |
| Q21  | — | 3113.71 ± 0.00 | — | DuckDB |
| Q22  | — | 335.38 ± 0.00 | — | DuckDB |

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
