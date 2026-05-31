# TPC-H SF=100 triangulation

Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on SF=100 parquet data, 3 timed trials after 1 warmups, single-machine.

Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — feature-gated behind `--features triangulation`.

Data: `examples/tpch/data/sf100`.

Each cell is **median ms ± σ** across 3 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | Best |
|------:|------------:|-------:|-------:|:-----|
| Q01  | 2156.58 ± 23.77 | 2259.80 ± 33.27 | — | ematix-flow |
| Q02  | 333.00 ± 25.21 | 411.70 ± 15.33 | — | ematix-flow |
| Q03  | 1405.21 ± 12.25 | 1432.88 ± 11.70 | — | ematix-flow |
| Q04  | 781.15 ± 7.14 | 844.05 ± 12.54 | — | ematix-flow |
| Q05  | 2274.80 ± 18.80 | 1539.51 ± 25.53 | — | DuckDB |
| Q06  | 451.19 ± 6.03 | 769.29 ± 22.85 | — | ematix-flow |
| Q07  | 1881.34 ± 17.86 | 1994.65 ± 6.82 | — | ematix-flow |
| Q08  | 2353.01 ± 30.33 | 2706.47 ± 14.31 | — | ematix-flow |
| Q09  | 7532.44 ± 456.53 | 8696.76 ± 1325.11 | — | ematix-flow |
| Q10  | 3089.75 ± 125.78 | 2673.18 ± 22.34 | — | DuckDB |
| Q11  | 303.84 ± 30.66 | 229.72 ± 2.95 | — | DuckDB |
| Q12  | 930.21 ± 29.40 | 1093.55 ± 10.63 | — | ematix-flow |
| Q13  | 1890.23 ± 39.75 | 2356.82 ± 11.14 | — | ematix-flow |
| Q14  | 762.30 ± 29.34 | 1166.51 ± 23.04 | — | ematix-flow |
| Q15  | — | 1632.03 ± 28.48 | — | DuckDB |
| Q16  | 496.45 ± 7.48 | 419.96 ± 11.36 | — | DuckDB |
| Q17  | 1807.95 ± 10.18 | 1516.82 ± 35.85 | — | DuckDB |
| Q18  | 6953.26 ± 335.21 | 2317.39 ± 13.97 | — | DuckDB |
| Q19  | 1118.16 ± 2.18 | 1507.25 ± 19.95 | — | ematix-flow |
| Q20  | 1795.12 ± 36.55 | 1933.26 ± 22.67 | — | ematix-flow |
| Q21  | 3693.53 ± 76.86 | 4359.60 ± 196.54 | — | ematix-flow |
| Q22  | 420.11 ± 18.11 | 575.93 ± 9.10 | — | ematix-flow |

## Wins

- **ematix-flow**: 15
- **DuckDB**: 7
- **Polars**: 0

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit JOIN ON with qualified columns; scalar subqueries materialized as CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; SUBSTRING rewritten as SUBSTR(x, start, len).
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions = std::thread::available_parallelism()` (override via `PARTITIONS=N`) and the InjectFusedQ1/Q3/Q5/Q6/Q12 + EnableDictGroupCount physical-optimizer rules registered.

## Failures and dialect gaps

- **Q15 / ematix-flow**: execute: Internal error: Assertion failed: self.mode != PartitionMode::Partitioned || left_partitions == ri…
