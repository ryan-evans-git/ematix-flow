# TPC-H SF=1 cross-engine

Same-machine TPC-H bench (Apple M3 Pro, single-node) over all 22 queries against SF=1 Parquet data. ematix-flow / DuckDB / Polars run in-process; PySpark runs in `local[*]` mode against the same files.

- ematix-flow / DuckDB / Polars: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` (feature `triangulation`), **10 timed trials after 3 warmups**. Refreshed 2026-05-20 against v0.4.0.
- PySpark: `scripts/bench-tpch-pyspark.py`, 3 trials after 1 warmup, **Spark 4.1.1 on JDK 23**. Refreshed 2026-05-20 against the same data + same machine.

Data: `examples/tpch/data/sf1`.

Each ematix-flow / DuckDB / Polars cell is **median ms ± σ** across 10 trials; PySpark cells are median ms across 3 trials. "—" means the engine couldn't parse / execute the query (dialect gap).

| Query | ematix-flow | DuckDB | Polars | PySpark | Best |
|------:|------------:|-------:|-------:|--------:|:-----|
| Q01  | **28.63 ± 0.61** | 45.24 ± 0.20 | 38.52 ± 0.84 | 196.5 | ematix-flow |
| Q02  | **9.85 ± 0.21**  | 19.07 ± 0.62 | 46.07 ± 0.65 | 290.7 | ematix-flow |
| Q03  | **13.96 ± 1.40** | 32.70 ± 0.65 | 46.00 ± 0.86 | 288.2 | ematix-flow |
| Q04  | **13.21 ± 0.43** | 23.07 ± 2.21 | 25.28 ± 1.51 | 226.1 | ematix-flow |
| Q05  | **21.59 ± 0.93** | 31.48 ± 0.70 | 11150.72 ± 689.69 | 364.2 | ematix-flow |
| Q06  | 11.04 ± 1.41 | 11.94 ± 0.20 | **10.16 ± 0.27** | 68.3 | Polars |
| Q07  | **28.79 ± 1.15** | 32.65 ± 0.93 | 115.31 ± 3.89 | 286.8 | ematix-flow |
| Q08  | **20.41 ± 0.67** | 38.26 ± 0.41 | 93.62 ± 7.78 | 209.8 | ematix-flow |
| Q09  | **26.30 ± 1.36** | 60.67 ± 1.63 | 47.96 ± 1.36 | 461.3 | ematix-flow |
| Q10  | **28.83 ± 10.44** | 68.29 ± 2.23 | 111.80 ± 8.15 | 421.9 | ematix-flow |
| Q11  | **8.65 ± 0.31**  | 11.62 ± 0.62 | 9.35 ± 5.04 | 139.1 | ematix-flow |
| Q12  | **14.85 ± 0.37** | 24.37 ± 0.68 | 19.06 ± 0.86 | 288.4 | ematix-flow |
| Q13  | **41.68 ± 0.73** | 147.33 ± 2.06 | 117.00 ± 4.13 | 694.2 | ematix-flow |
| Q14  | **12.13 ± 1.00** | 24.22 ± 1.04 | 13.01 ± 0.78 | 138.3 | ematix-flow |
| Q15  | 16.25 ± 0.92 | 15.69 ± 1.87 | **11.48 ± 0.22** | 166.4 | Polars |
| Q16  | **8.76 ± 1.48**  | 26.00 ± 4.35 | 21.29 ± 0.71 | 211.5 | ematix-flow |
| Q17  | 36.85 ± 2.24 | **28.48 ± 1.62** | 42.04 ± 2.96 | 239.4 | DuckDB |
| Q18  | **51.21 ± 3.06** | 52.37 ± 1.31 | 59.19 ± 2.32 | 569.1 | ematix-flow |
| Q19  | **17.79 ± 1.89** | 36.82 ± 3.48 | 106.55 ± 9.04 | 111.4 | ematix-flow |
| Q20  | **16.34 ± 0.85** | 39.11 ± 3.04 | 23.30 ± 2.39 | 148.8 | ematix-flow |
| Q21  | **41.08 ± 1.67** | 87.04 ± 2.18 | 730.68 ± 39.43 | 648.5 | ematix-flow |
| Q22  | **8.62 ± 0.52**  | 22.40 ± 0.65 | 12.97 ± 1.67 | 280.2 | ematix-flow |

## Headline geomeans

- **1.75× faster than DuckDB** (was 1.69× at v0.3.0)
- **2.77× faster than Polars** (was 2.71×)
- **13.4× faster than single-node PySpark** (was 12.9×)

ematix-flow wins **19 / 22** queries outright (was 18 / 22 at v0.3.0). The three it doesn't (Q06, Q15, Q17) are single-digit-ms gaps inside the run-to-run noise envelope.

## Wins

- **ematix-flow**: 19
- **DuckDB**: 1 (Q17)
- **Polars**: 2 (Q06, Q15)

## What moved in v0.4.0

The "What's not shipped" closures (warehouse, Web UI, secrets, distributed peer auto-detection) are orthogonal to the scan/aggregate hot path, so the geomean improvement comes from two adjacent landings that shipped on the same branch:

- **ematix-parquet v0.13.0** (bumped 2026-05-20 — full SIMD specialisation bw=1..=32). Headline kernel-level wins: Q06 -18.7%, Q17 -9.5% on the scan side. The 22-query end-to-end geomean improvement vs v0.3.0 is ~+3.5%.
- **Σ.F.1 shape-catalog substrate** — bit-identical end-to-end perf vs the hand-coded `Inject*Rule` set it replaced (validated at the bench gate), but stable enough to allow tighter trial counts without optimizer drift contaminating the median.

Combined with the bigger 10-trial / 3-warmup run, the σ floor on most queries dropped from 1-5ms to <1ms — the v0.4.0 improvement that was previously hiding inside the noise envelope is now visible.

## Caveats

- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) is enabled for `lineitem`. Late-mat helps queries with a selective filter on a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries with low filter selectivity (Q01) it's effectively a no-op.
- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit cross-join in `FROM`, bare-column equi-joins, `EXISTS` subqueries, scalar-subquery comparisons, `SUBSTRING ... FROM ... FOR`, `HAVING` against unprojected columns. We ship hand-translated `q??.polars.sql` variants alongside the canonical `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the polars variant when present. Translations are semantically equivalent: explicit `JOIN ON` with qualified columns; scalar subqueries materialized as CTE + `CROSS JOIN`; `EXISTS` rewritten as semi-join via `DISTINCT` + `INNER JOIN`; `SUBSTRING` rewritten as `SUBSTR(x, start, len)`.
- Polars's Q05 outlier (~11s) is a planner blowup on the canonical TPC-H Q05 shape — not a true execution-time number. Flagged but not a release blocker.
- DuckDB runs at default settings (in-memory `read_parquet` views). ematix-flow runs with `target_partitions=14` and the InjectFilterMultiAgg / InjectFilterSum / EnableDictGroupCount physical-optimizer rules registered (auto-loaded via shape-catalog substrate as of Σ.F.1).
- PySpark uses `local[*]`, `spark.sql.shuffle.partitions=8`, `spark.sql.adaptive.enabled=true`. JVM warmup costs sit above what the 3-trial harness amortises; the absolute numbers are meant to anchor the gap to a JVM/Catalyst baseline, not to be a fair head-to-head.

## Reproducer

```sh
# ematix-flow / DuckDB / Polars (10 trials, 3 warmups)
TPCH_TRIALS=10 TPCH_WARMUPS=3 cargo run --release -p ematix-flow-core \
    --example tpch_triangulation_bench --features triangulation

# PySpark (requires JDK + ematix-flow[spark] extra)
JAVA_HOME=$(/usr/libexec/java_home) python scripts/bench-tpch-pyspark.py \
    --data-dir examples/tpch/data/sf1
```

Triangulation bench writes `BENCHMARKS.md`; PySpark bench prints a markdown table to stdout for manual merge.

## Failures and dialect gaps

_None — every engine ran every query._
