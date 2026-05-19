# TPC-H SF=1 cross-engine

Same-machine TPC-H bench (Apple Silicon, single-node) over all 22 queries against SF=1 Parquet data. ematix-flow / DuckDB / Polars run in-process; PySpark runs in `local[*]` mode against the same files.

- ematix-flow / DuckDB / Polars: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` (feature `triangulation`), 5 timed trials after 2 warmups.
- PySpark: `scripts/bench-tpch-pyspark.py`, 3 trials after 1 warmup, Spark 4.1.1 on JDK 23.

Data: `examples/tpch/data/sf1`.

Each ematix-flow / DuckDB / Polars cell is **median ms ± σ** across 5 trials; PySpark cells are median ms across 3 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | PySpark | Best |
|------:|------------:|-------:|-------:|--------:|:-----|
| Q01  | **28.11 ± 0.87** | 45.17 ± 0.98 | 36.22 ± 1.63 | 189.8 | ematix-flow |
| Q02  | **10.51 ± 1.95** | 18.84 ± 0.16 | 45.85 ± 0.29 | 215.6 | ematix-flow |
| Q03  | **15.11 ± 1.31** | 32.36 ± 0.63 | 45.39 ± 0.81 | 293.7 | ematix-flow |
| Q04  | **12.55 ± 0.15** | 22.04 ± 0.26 | 23.30 ± 0.14 | 218.8 | ematix-flow |
| Q05  | **20.93 ± 0.62** | 30.49 ± 0.34 | 10754.97 ± 746.46 | 366.2 | ematix-flow |
| Q06  | 14.50 ± 401.99 | 11.90 ± 3.15 | **10.57 ± 0.36** | 47.9 | Polars |
| Q07  | **28.96 ± 1.79** | 31.57 ± 0.31 | 112.41 ± 14.63 | 288.7 | ematix-flow |
| Q08  | **20.76 ± 0.43** | 37.35 ± 1.22 | 93.29 ± 3.37 | 215.2 | ematix-flow |
| Q09  | **28.13 ± 1.59** | 62.42 ± 4.42 | 47.22 ± 3.17 | 453.0 | ematix-flow |
| Q10  | **28.16 ± 1.14** | 64.14 ± 2.51 | 109.27 ± 2.49 | 416.9 | ematix-flow |
| Q11  | **7.47 ± 0.38** | 10.36 ± 0.38 | 9.57 ± 2.96 | 140.0 | ematix-flow |
| Q12  | **14.72 ± 0.10** | 23.49 ± 0.82 | 19.33 ± 0.67 | 310.5 | ematix-flow |
| Q13  | **41.36 ± 1.10** | 141.92 ± 0.80 | 115.08 ± 2.20 | 699.6 | ematix-flow |
| Q14  | **11.28 ± 0.50** | 23.00 ± 0.55 | 12.38 ± 0.37 | 117.1 | ematix-flow |
| Q15  | 15.45 ± 0.59 | 14.51 ± 3.80 | **11.33 ± 0.20** | 142.0 | Polars |
| Q16  | **8.60 ± 0.26** | 24.56 ± 0.57 | 20.56 ± 2.41 | 213.4 | ematix-flow |
| Q17  | 35.71 ± 5.54 | **28.77 ± 1.74** | 40.28 ± 0.74 | 272.4 | DuckDB |
| Q18  | 52.02 ± 2.98 | **50.70 ± 8.95** | 56.38 ± 1.99 | 587.1 | DuckDB |
| Q19  | **18.81 ± 7.42** | 34.15 ± 3.00 | 100.06 ± 8.83 | 103.2 | ematix-flow |
| Q20  | **14.81 ± 0.28** | 35.00 ± 1.97 | 22.12 ± 0.46 | 154.0 | ematix-flow |
| Q21  | **38.08 ± 0.67** | 82.49 ± 0.57 | 679.78 ± 35.14 | 598.8 | ematix-flow |
| Q22  | **8.25 ± 0.79** | 23.22 ± 2.07 | 13.06 ± 0.38 | 284.3 | ematix-flow |

## Headline

- **Geomean speedup of ematix-flow:**
  - **1.69×** vs DuckDB
  - **2.71×** vs Polars
  - **12.9×** vs PySpark local[*]
- **Win counts (lowest median per query):** ematix-flow 18, DuckDB 2, Polars 2, PySpark 0.

## v0.3.0 deltas vs v0.2.1

| Query | v0.2.1 ematix-flow | v0.3.0 ematix-flow | Δ |
|------:|------------:|-----------:|----:|
| Q01 | 78.19 | **28.11** | **-64%** |
| Q03 | 20.38 | 15.11 | -26% |
| Q05 | 34.09 | 20.93 | -39% |
| Q07 | 75.56 | 28.96 | -62% |
| Q08 | 35.66 | 20.76 | -42% |
| Q09 | 50.16 | 28.13 | -44% |
| Q10 | 39.73 | 28.16 | -29% |
| Q13 | 44.73 | 41.36 | -8% |
| Q14 | 19.45 | 11.28 | -42% |
| Q16 | 18.29 | 8.60 | -53% |
| Q18 | 157.55 | 52.02 | **-67%** |
| Q19 | 99.76 | 18.81 | **-81%** |
| Q21 | 75.48 | 38.08 | -50% |

The v0.3.0 win count rose from 15→18/22.

## Caveats

- ematix-flow's late-materialization path (`read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column; on aggregate-heavy queries with low filter selectivity (Q1) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes; we ship hand-translated `q??.polars.sql` variants under `examples/tpch/queries/`. Q05 specifically still blows up Polars's planner — flagged but not fixed for this release.
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the `InjectFilterMultiAggRule` + `InjectFilterSumRule` + `EnableDictGroupCountRule` physical-optimizer rules registered.
- PySpark uses `local[*]`, `spark.sql.shuffle.partitions=8`, `spark.sql.adaptive.enabled=true`. The PySpark numbers include JVM warmup costs above what the warmup-trial discard can amortize; treat them as an order-of-magnitude comparison.

## Reproducing

```bash
# ematix-flow vs DuckDB vs Polars
cargo run --release -p ematix-flow-core \
    --example tpch_triangulation_bench --features triangulation

# PySpark (needs Java 17+; install with `brew install openjdk@23`):
JAVA_HOME=$(/usr/libexec/java_home) python scripts/bench-tpch-pyspark.py \
    --data-dir examples/tpch/data/sf1 --trials 3
```
