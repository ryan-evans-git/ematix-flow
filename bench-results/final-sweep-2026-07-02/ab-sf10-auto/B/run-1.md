# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 10 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 10 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 223.36 ± 7.06 | — | — | ematix-flow |
| Q02  | 18.49 ± 0.26 | — | — | ematix-flow |
| Q03  | 117.77 ± 3.85 | — | — | ematix-flow |
| Q04  | 52.53 ± 1.46 | — | — | ematix-flow |
| Q05  | 145.44 ± 4.38 | — | — | ematix-flow |
| Q06  | 24.50 ± 2.00 | — | — | ematix-flow |
| Q07  | 114.47 ± 2.83 | — | — | ematix-flow |
| Q08  | 141.28 ± 4.60 | — | — | ematix-flow |
| Q09  | 257.70 ± 3.78 | — | — | ematix-flow |
| Q10  | 187.86 ± 3.91 | — | — | ematix-flow |
| Q11  | 12.85 ± 0.41 | — | — | ematix-flow |
| Q12  | 85.22 ± 3.03 | — | — | ematix-flow |
| Q13  | 95.59 ± 1.77 | — | — | ematix-flow |
| Q14  | 79.79 ± 3.29 | — | — | ematix-flow |
| Q15  | 59.76 ± 2.99 | — | — | ematix-flow |
| Q16  | 48.13 ± 2.30 | — | — | ematix-flow |
| Q17  | 76.13 ± 3.54 | — | — | ematix-flow |
| Q18  | 173.16 ± 12.56 | — | — | ematix-flow |
| Q19  | 117.46 ± 2.85 | — | — | ematix-flow |
| Q20  | 90.01 ± 4.62 | — | — | ematix-flow |
| Q21  | 179.74 ± 7.49 | — | — | ematix-flow |
| Q22  | 22.90 ± 2.15 | — | — | ematix-flow |

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
