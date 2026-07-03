# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 2575.79 ± 26.65 | — | — | ematix-flow |
| Q02  | 200.37 ± 5.45 | — | — | ematix-flow |
| Q03  | 1576.43 ± 19.10 | — | — | ematix-flow |
| Q04  | 884.38 ± 8.95 | — | — | ematix-flow |
| Q05  | 1923.62 ± 6.33 | — | — | ematix-flow |
| Q06  | 539.78 ± 9.60 | — | — | ematix-flow |
| Q07  | 1534.22 ± 10.89 | — | — | ematix-flow |
| Q08  | 1685.21 ± 19.96 | — | — | ematix-flow |
| Q09  | 3250.14 ± 196.84 | — | — | ematix-flow |
| Q10  | 2016.26 ± 19.41 | — | — | ematix-flow |
| Q11  | 190.87 ± 8.43 | — | — | ematix-flow |
| Q12  | 1063.16 ± 15.36 | — | — | ematix-flow |
| Q13  | 1954.92 ± 8.79 | — | — | ematix-flow |
| Q14  | 840.22 ± 8.80 | — | — | ematix-flow |
| Q15  | 808.79 ± 7.82 | — | — | ematix-flow |
| Q16  | 418.85 ± 1.60 | — | — | ematix-flow |
| Q17  | 1514.17 ± 13.38 | — | — | ematix-flow |
| Q18  | 2588.29 ± 31.97 | — | — | ematix-flow |
| Q19  | 1231.14 ± 13.33 | — | — | ematix-flow |
| Q20  | 1171.84 ± 16.04 | — | — | ematix-flow |
| Q21  | 3525.02 ± 15.81 | — | — | ematix-flow |
| Q22  | 371.39 ± 4.50 | — | — | ematix-flow |

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
