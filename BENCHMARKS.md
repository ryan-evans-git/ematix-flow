# TPC-H SF=1 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=1 parquet data, 5 timed trials after 2 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf1`.

Each cell is **median ms ± σ** across 5 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 81.82 ± 3.13 | 46.78 ± 0.50 | 48.00 ± 3.85 | DuckDB |
| Q02  | 10.86 ± 3.18 | 20.04 ± 7.38 | — | ematix-flow |
| Q03  | 19.93 ± 1.20 | 34.68 ± 4.55 | — | ematix-flow |
| Q04  | 15.76 ± 1.39 | 25.08 ± 1.26 | — | ematix-flow |
| Q05  | 36.50 ± 1.59 | 36.23 ± 1.83 | — | DuckDB |
| Q06  | 11.33 ± 1.70 | 13.65 ± 2.52 | 10.43 ± 0.32 | Polars |
| Q07  | 65.21 ± 2.83 | 36.18 ± 1.72 | — | DuckDB |
| Q08  | 31.70 ± 0.61 | 42.19 ± 0.97 | — | ematix-flow |
| Q09  | 39.80 ± 0.68 | 66.95 ± 3.33 | — | ematix-flow |
| Q10  | 39.74 ± 8.67 | 79.31 ± 3.30 | — | ematix-flow |
| Q11  | 7.81 ± 0.27 | 12.12 ± 1.64 | — | ematix-flow |
| Q12  | 22.02 ± 0.94 | 28.74 ± 1.15 | — | ematix-flow |
| Q13  | 44.23 ± 1.42 | 162.53 ± 4.99 | — | ematix-flow |
| Q14  | 15.47 ± 0.27 | 25.64 ± 2.89 | — | ematix-flow |
| Q15  | 17.62 ± 1.07 | 17.27 ± 1.93 | — | DuckDB |
| Q16  | 10.03 ± 0.67 | 28.83 ± 1.76 | — | ematix-flow |
| Q17  | 45.46 ± 1.54 | 32.00 ± 0.84 | — | DuckDB |
| Q18  | 74.13 ± 4.16 | 54.13 ± 1.87 | — | DuckDB |
| Q19  | 57.47 ± 0.33 | 43.76 ± 3.18 | — | DuckDB |
| Q20  | 20.08 ± 0.30 | 36.72 ± 3.24 | — | ematix-flow |
| Q21  | 62.09 ± 3.10 | 96.24 ± 4.68 | — | ematix-flow |
| Q22  | 10.03 ± 0.73 | 27.52 ± 0.53 | — | ematix-flow |

## Wins

- **ematix-flow**: 14
- **DuckDB**: 7
- **Polars**: 1

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend has structural gaps on the TPC-H reference shapes (implicit cross-join in FROM, non-equi join predicates, EXISTS subqueries, SUBSTRING). To get Polars numbers across the full 22 you must translate to the LazyFrame DSL; we have not done that here.
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 physical-optimizer rules registered.

## Failures and dialect gaps

- **Q02 / Polars**: polars sql: multiple tables in FROM clause are not currently supported (found 5)
- **Q03 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q04 / Polars**: polars sql: expression Exists { subquery: Query { with: None
- **Q05 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q07 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q08 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q09 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q10 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q11 / Polars**: polars sql: multiple tables in FROM clause are not currently supported (found 3)
- **Q12 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q13 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q14 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q15 / Polars**: polars sql: multiple tables in FROM clause are not currently supported (found 2)
- **Q16 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q17 / Polars**: polars sql: multiple tables in FROM clause are not currently supported (found 2)
- **Q18 / Polars**: polars sql: multiple tables in FROM clause are not currently supported (found 3)
- **Q19 / Polars**: polars sql: only equi-join constraints (on identifiers) are currently supported
- **Q20 / Polars**: polars sql: multiple tables in FROM clause are not currently supported (found 2)
- **Q21 / Polars**: polars sql: multiple tables in FROM clause are not currently supported (found 4)
- **Q22 / Polars**: polars sql: expression Substring { expr: Identifier(Ident { value: "c_phone"
