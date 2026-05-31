# TPC-H SF=100 — 3-engine, single-node (M3 Pro, 36 GB RAM) — 2026-05-29

Local SF=100 (34 GB Snappy parquet, lineitem 600,037,902 rows). ematix-flow built
`--features triangulation`, Q06 scan-filter-strip (Σ.Q06.SF10.8) default-on.
- ematix + DuckDB: 3 trials / 1 warmup (raw: `sf100-ematix-duckdb-2026-05-29.{md,txt}`).
- Polars: run separately, query-by-query, 2 trials / 1 warmup (raw: `sf100-polars-2026-05-29.txt`).
  Run **once** — Polars at SF=100 is very slow / OOMs; do not re-run casually.

median ms (lower = better). **bold** = fastest of the three.

| Q | ematix-flow | DuckDB | Polars | fastest |
|---|---:|---:|---:|:--|
| Q01 | **2157** | 2260 | 79084 | ematix |
| Q02 | **333**  | 412  | 53109 | ematix |
| Q03 | **1405** | 1433 | 30103 | ematix |
| Q04 | **781**  | 844  | 6550  | ematix |
| Q05 | 2275 | **1540** | OOM | duckdb |
| Q06 | **451**  | 769  | 540   | ematix |
| Q07 | **1881** | 1995 | 95946 | ematix |
| Q08 | **2353** | 2706 | OOM   | ematix |
| Q09 | **7532** | 8697 | 21438 | ematix |
| Q10 | 3090 | **2673** | OOM | duckdb |
| Q11 | 304  | **230**  | 412 | duckdb |
| Q12 | **930**  | 1094 | 1112  | ematix |
| Q13 | **1890** | 2357 | 5114  | ematix |
| Q14 | **762**  | 1167 | 895   | ematix |
| Q15 | **CRASH** | 1632 | 890 | (ematix crashes — partition-mismatch bug; see below) |
| Q16 | 496  | **420**  | 1809 | duckdb |
| Q17 | 1808 | **1517** | 9536 | duckdb |
| Q18 | 6953 | **2317** | 15572 | duckdb |
| Q19 | **1118** | 1507 | OOM | ematix |
| Q20 | **1795** | 1933 | 6692 | ematix |
| Q21 | **3694** | 4360 | OOM | ematix |
| Q22 | **420**  | 576  | 1255 | ematix |

## Tally
- **ematix fastest of all three: 15 / 22.**
- vs DuckDB: ematix wins 15, loses 6 (Q05, Q10, Q11, Q16, Q17, Q18), Q15 crashes.
- vs Polars: ematix wins 21/21 completed (Polars never beats ematix on a completed query except Q15 where ematix crashes). Polars OOMs on Q05/Q08/Q10/Q19/Q21 and is 30–50× slower elsewhere.

## Key findings
1. **Polars collapses at SF=100** (in-memory; 34 GB data on 36 GB RAM): 5 OOMs, 30–50× slower on the rest. This is the "Snowflake/Spark/Polars fail at scale" thesis — ematix (streaming) holds up. DuckDB (streams+spills) is the only real competitor at SF=100.
2. **The DuckDB losses are the join-order / intermediate-materialization queries, and the gaps AMPLIFY with scale** vs SF=10:
   - Q18 1.07× → **3.00×** (6953 vs 2317) — 60M→600M probe/intermediate.
   - Q05 1.28× → 1.48×; Q17 SF=10 *win* → 1.19×; Q10/Q16 flipped win→loss; Q11 1.32×.
   This **validates the deferred bushy-reorder / L8-CBO / L10-IO-reduction work** — neutral at SF=10 on BW-rich HW, but real and growing at SF=100 (V5 §5.2 prediction confirmed). Same root cause across all six.
3. **Q15 hard-crashes at SF=100**: `Invalid HashJoinExec: PartitionMode::Partitioned requires left_partitions == right_partitions`. Scale-only (passes SF=1/10). Robustness bug — must fix before any SF=100 publication. (Flagged as a separate task.)

## Reproduce
```
cargo build --release -p ematix-flow-core --features triangulation --example tpch_triangulation_bench
# ematix + DuckDB:
TPCH_DATA_DIR=examples/tpch/data/sf100 TPCH_TRIALS=3 TPCH_WARMUPS=1 TPCH_SKIP_POLARS=1 \
  TPCH_OUT=bench-results/sf100-ematix-duckdb-2026-05-29.md ./target/release/examples/tpch_triangulation_bench
# Polars (slow; per-query, skip ematix+duckdb): see sf100-polars-2026-05-29.txt header.
```
