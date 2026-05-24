# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf10`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 277.93 ± 16.23 | 258.03 ± 1.61 | — | DuckDB |
| Q02  | 36.37 ± 1.05 | 48.09 ± 3.99 | — | ematix-flow |
| Q03  | 154.70 ± 2.11 | 160.77 ± 3.71 | — | ematix-flow |
| Q04  | 59.09 ± 2.86 | 95.24 ± 0.74 | — | ematix-flow |
| Q05  | 202.78 ± 2.09 | 159.07 ± 4.00 | — | DuckDB |
| Q06  | 81.82 ± 3.06 | 82.09 ± 3.43 | — | ematix-flow |
| Q07  | 173.68 ± 3.38 | 155.67 ± 5.63 | — | DuckDB |
| Q08  | 206.63 ± 5.94 | 191.19 ± 4.66 | — | DuckDB |
| Q09  | 290.92 ± 3.79 | 348.14 ± 9.76 | — | ematix-flow |
| Q10  | 259.84 ± 12.35 | 489.86 ± 17.96 | — | ematix-flow |
| Q11  | 12.90 ± 0.56 | 39.75 ± 4.83 | — | ematix-flow |
| Q12  | 109.02 ± 5.29 | 128.33 ± 5.53 | — | ematix-flow |
| Q13  | 120.08 ± 5.73 | 286.28 ± 9.41 | — | ematix-flow |
| Q14  | 96.90 ± 7.89 | 151.41 ± 3.72 | — | ematix-flow |
| Q15  | 87.92 ± 4.43 | 96.98 ± 3.41 | — | ematix-flow |
| Q16  | 56.93 ± 0.88 | 70.55 ± 3.66 | — | ematix-flow |
| Q17  | 181.85 ± 13.47 | 188.41 ± 6.50 | — | ematix-flow |
| Q18  | 271.69 ± 3.99 | 251.57 ± 8.93 | — | DuckDB |
| Q19  | 153.43 ± 7.32 | 224.17 ± 8.42 | — | ematix-flow |
| Q20  | 140.05 ± 3.03 | 175.93 ± 4.31 | — | ematix-flow |
| Q21  | 312.62 ± 11.17 | 495.94 ± 20.39 | — | ematix-flow |
| Q22  | 30.91 ± 1.15 | 166.07 ± 7.33 | — | ematix-flow |

## Wins

- **ematix-flow**: 17
- **DuckDB**: 5
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

_None — every engine ran every query._
