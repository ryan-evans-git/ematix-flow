# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 8479.98 ± 0.00 | — | — | ematix-flow |
| Q02  | 997.24 ± 0.00 | — | — | ematix-flow |
| Q03  | 6480.00 ± 0.00 | — | — | ematix-flow |
| Q04  | 4383.21 ± 0.00 | — | — | ematix-flow |
| Q05  | 8461.34 ± 0.00 | — | — | ematix-flow |
| Q06  | 1348.32 ± 0.00 | — | — | ematix-flow |
| Q07  | 6803.28 ± 0.00 | — | — | ematix-flow |
| Q08  | 6376.68 ± 0.00 | — | — | ematix-flow |
| Q09  | 140523.33 ± 0.00 | — | — | ematix-flow |
| Q10  | 12194.91 ± 0.00 | — | — | ematix-flow |
| Q11  | 1075.87 ± 0.00 | — | — | ematix-flow |
| Q12  | 3551.17 ± 0.00 | — | — | ematix-flow |
| Q13  | 6240.58 ± 0.00 | — | — | ematix-flow |
| Q14  | 1561.46 ± 0.00 | — | — | ematix-flow |
| Q15  | 3295.57 ± 0.00 | — | — | ematix-flow |
| Q16  | 2996.78 ± 0.00 | — | — | ematix-flow |
| Q17  | 4697.73 ± 0.00 | — | — | ematix-flow |
| Q18  | 24498.60 ± 0.00 | — | — | ematix-flow |
| Q19  | 2079.56 ± 0.00 | — | — | ematix-flow |
| Q20  | 6966.44 ± 0.00 | — | — | ematix-flow |
| Q21  | 12026.09 ± 0.00 | — | — | ematix-flow |
| Q22  | 1865.36 ± 0.00 | — | — | ematix-flow |

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
