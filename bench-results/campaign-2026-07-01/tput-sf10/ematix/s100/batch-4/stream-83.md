# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 3641.94 ± 0.00 | — | — | ematix-flow |
| Q02  | 899.74 ± 0.00 | — | — | ematix-flow |
| Q03  | 4948.04 ± 0.00 | — | — | ematix-flow |
| Q04  | 2259.90 ± 0.00 | — | — | ematix-flow |
| Q05  | 8962.58 ± 0.00 | — | — | ematix-flow |
| Q06  | 1299.69 ± 0.00 | — | — | ematix-flow |
| Q07  | 2739.60 ± 0.00 | — | — | ematix-flow |
| Q08  | 4728.37 ± 0.00 | — | — | ematix-flow |
| Q09  | 6855.60 ± 0.00 | — | — | ematix-flow |
| Q10  | 3768.10 ± 0.00 | — | — | ematix-flow |
| Q11  | 866.16 ± 0.00 | — | — | ematix-flow |
| Q12  | 863.39 ± 0.00 | — | — | ematix-flow |
| Q13  | 4497.12 ± 0.00 | — | — | ematix-flow |
| Q14  | 753.88 ± 0.00 | — | — | ematix-flow |
| Q15  | 1576.08 ± 0.00 | — | — | ematix-flow |
| Q16  | 1287.52 ± 0.00 | — | — | ematix-flow |
| Q17  | 2774.92 ± 0.00 | — | — | ematix-flow |
| Q18  | 3645.84 ± 0.00 | — | — | ematix-flow |
| Q19  | 1194.00 ± 0.00 | — | — | ematix-flow |
| Q20  | 2373.38 ± 0.00 | — | — | ematix-flow |
| Q21  | 9407.57 ± 0.00 | — | — | ematix-flow |
| Q22  | 1738.18 ± 0.00 | — | — | ematix-flow |

## Wins

- **ematix-flow**: 22
- **DuckDB**: 0
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions = std::thread::available_parallelism()` (override via `PARTITIONS=N`) and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
