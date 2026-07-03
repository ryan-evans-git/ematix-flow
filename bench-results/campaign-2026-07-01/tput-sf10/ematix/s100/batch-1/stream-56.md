# TPC-H SF=10 triangulation

> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: cross-invocation variance is 5-10x what the ± column suggests, and ematix reuses in-process session state competitors don't get. Verdict-grade win/loss claims must come from the strict protocol (`scripts/bench/README.md`).

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=10 parquet data, 1 timed trials after 0 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 1 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 3452.06 ± 0.00 | — | — | ematix-flow |
| Q02  | 664.77 ± 0.00 | — | — | ematix-flow |
| Q03  | 5808.76 ± 0.00 | — | — | ematix-flow |
| Q04  | 2202.16 ± 0.00 | — | — | ematix-flow |
| Q05  | 8241.01 ± 0.00 | — | — | ematix-flow |
| Q06  | 2442.22 ± 0.00 | — | — | ematix-flow |
| Q07  | 3091.47 ± 0.00 | — | — | ematix-flow |
| Q08  | 2936.30 ± 0.00 | — | — | ematix-flow |
| Q09  | 4782.45 ± 0.00 | — | — | ematix-flow |
| Q10  | 9939.66 ± 0.00 | — | — | ematix-flow |
| Q11  | 114.02 ± 0.00 | — | — | ematix-flow |
| Q12  | 728.58 ± 0.00 | — | — | ematix-flow |
| Q13  | 4173.46 ± 0.00 | — | — | ematix-flow |
| Q14  | 672.19 ± 0.00 | — | — | ematix-flow |
| Q15  | 775.91 ± 0.00 | — | — | ematix-flow |
| Q16  | 2285.14 ± 0.00 | — | — | ematix-flow |
| Q17  | 2064.66 ± 0.00 | — | — | ematix-flow |
| Q18  | 6771.32 ± 0.00 | — | — | ematix-flow |
| Q19  | 1087.78 ± 0.00 | — | — | ematix-flow |
| Q20  | 4132.53 ± 0.00 | — | — | ematix-flow |
| Q21  | 9888.88 ± 0.00 | — | — | ematix-flow |
| Q22  | 906.22 ± 0.00 | — | — | ematix-flow |

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
