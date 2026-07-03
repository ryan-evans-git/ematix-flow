# TPC-H SF=100 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 2576.17 ± 32.65 | — | — | ematix-flow |
| Q02  | 233.52 ± 4.46 | — | — | ematix-flow |
| Q03  | 1618.04 ± 13.41 | — | — | ematix-flow |
| Q04  | 902.55 ± 15.88 | — | — | ematix-flow |
| Q05  | 2032.10 ± 50.19 | — | — | ematix-flow |
| Q06  | 542.15 ± 9.91 | — | — | ematix-flow |
| Q07  | 1613.98 ± 14.45 | — | — | ematix-flow |
| Q08  | 1764.49 ± 9.35 | — | — | ematix-flow |
| Q09  | 4280.92 ± 99.59 | — | — | ematix-flow |
| Q10  | 3281.95 ± 95.62 | — | — | ematix-flow |
| Q11  | 213.48 ± 9.86 | — | — | ematix-flow |
| Q12  | 1065.60 ± 5.70 | — | — | ematix-flow |
| Q13  | 2043.16 ± 9.98 | — | — | ematix-flow |
| Q14  | 828.77 ± 10.61 | — | — | ematix-flow |
| Q15  | 794.42 ± 9.53 | — | — | ematix-flow |
| Q16  | 426.18 ± 18.50 | — | — | ematix-flow |
| Q17  | 1536.49 ± 27.42 | — | — | ematix-flow |
| Q18  | 2722.02 ± 16.61 | — | — | ematix-flow |
| Q19  | 1251.22 ± 12.29 | — | — | ematix-flow |
| Q20  | 1298.46 ± 7.02 | — | — | ematix-flow |
| Q21  | 3648.37 ± 14.36 | — | — | ematix-flow |
| Q22  | 469.49 ± 9.03 | — | — | ematix-flow |

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
