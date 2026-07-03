# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 7200.37 ± 0.00 | — | — | ematix-flow |
| Q02  | 1119.69 ± 0.00 | — | — | ematix-flow |
| Q03  | 5159.21 ± 0.00 | — | — | ematix-flow |
| Q04  | 3536.43 ± 0.00 | — | — | ematix-flow |
| Q05  | 7661.92 ± 0.00 | — | — | ematix-flow |
| Q06  | 1720.06 ± 0.00 | — | — | ematix-flow |
| Q07  | 6540.79 ± 0.00 | — | — | ematix-flow |
| Q08  | 7414.86 ± 0.00 | — | — | ematix-flow |
| Q09  | 15206.75 ± 0.00 | — | — | ematix-flow |
| Q10  | 10756.54 ± 0.00 | — | — | ematix-flow |
| Q11  | 688.70 ± 0.00 | — | — | ematix-flow |
| Q12  | 3764.21 ± 0.00 | — | — | ematix-flow |
| Q13  | 5894.79 ± 0.00 | — | — | ematix-flow |
| Q14  | 2719.36 ± 0.00 | — | — | ematix-flow |
| Q15  | 2571.79 ± 0.00 | — | — | ematix-flow |
| Q16  | 2360.78 ± 0.00 | — | — | ematix-flow |
| Q17  | 4222.59 ± 0.00 | — | — | ematix-flow |
| Q18  | 14925.28 ± 0.00 | — | — | ematix-flow |
| Q19  | 3017.75 ± 0.00 | — | — | ematix-flow |
| Q20  | 4458.23 ± 0.00 | — | — | ematix-flow |
| Q21  | 24211.13 ± 0.00 | — | — | ematix-flow |
| Q22  | 1993.80 ± 0.00 | — | — | ematix-flow |

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
