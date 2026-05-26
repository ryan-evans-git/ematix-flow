# Stage profiling methodology (2026-05)

Approach used for the Q01-Q22 SF=10 waste survey. Each query gets its own `docs/PERF_Q<n>.md`. This doc captures the shared methodology so each writeup can stay focused on findings.

## Goal

Not "is ematix winning this query" — that's `BENCHMARKS.md`. This survey asks: **for each stage in the executed physical plan, is the time spent within the realistic floor for that work?** Anything ≥2× the theoretical floor is a "waste candidate" to investigate.

## Tooling

1. **Per-stage timings** — `crates/ematix-flow-core/examples/stage_profiler.rs` runs one query at SF=10 with our milestone-default rule set, executes every output partition, walks the physical-plan tree post-execution, and dumps per-node `elapsed_compute_ms` + `output_rows` aggregated across trials.

2. **Function self-time** — macOS `sample <pid> 10`. Sample at 1kHz over a ~10s window covering ~40 trials. Filter to threads whose stacks descend into `ematix_flow_core` (skips parking/idle pool). Aggregate self-time = inclusive count − immediate children count.

3. **Wall-time comparison** — same-process `tpch_triangulation_bench` with `TPCH_QUERIES=<n>` for ematix vs DuckDB vs Polars on the same file. Tells us the wall-time gap to the leader.

4. **Codec inspection** — `inspect_lineitem_codec` (when relevant) to confirm what compression each column uses on disk.

5. **Theoretical floor** — derive per-stage lower bound from first principles:

| Stage | Floor formula | Constants (M-series arm64, 14 cores) |
|-------|---------------|---------------------------------------|
| File I/O (warm page cache) | bytes / 5 GB/s | — |
| File I/O (cold) | bytes / NVMe BW (~3 GB/s) | — |
| Snappy decompress | uncompressed_bytes / (2 GB/s × cores) | per-thread ~2 GB/s |
| LZ4_RAW decompress | uncompressed_bytes / (5 GB/s × cores) | per-thread ~5 GB/s |
| PLAIN i64/f64 unpack | rows × 1 ns/row / cores | |
| DICT decode (RLE indices + lookup) | rows × 1-2 ns/row / cores | |
| Filter (i32/i64 cmp, single predicate) | rows × 0.5 ns/row / cores | SIMD-vectorised |
| Hash aggregate (≤100 groups in L1) | rows × 1-2 ns/row / cores | |
| Hash aggregate (10k-1M groups, hits L2/L3) | rows × 5-15 ns/row / cores | |
| Hash join build (i64 keys) | rows × 5 ns/row / cores | RobinHood fits L2 |
| Hash join probe (build fits L2) | rows × 8-15 ns/row / cores | |
| Sort (n log n comparisons) | n × log₂(n) × 50-100 ns / cores | |
| Arrow-batch assembly | negligible (Arc<Buffer> ref-counts) | — |

These constants are calibrated against published Photon / DuckDB / Polars kernel benchmarks and our own kernel micro-bench results (`ematix-parquet` codec benches, `ematix-flow-hash-join` kernel bench). They are **lower bounds on hot, single-purpose kernels** — real plans rarely hit them due to memory bandwidth, cache-line collisions, and operator boundary crossings.

If actual is within 2× floor: the stage is essentially optimal, look elsewhere.
If actual is 2-5×: a worthwhile investigation but expect single-digit-percent wall-time gain.
If actual is >5×: a real waste candidate; verify with a kernel microbench before chasing.

## Per-query writeup template

Each `PERF_Q<n>.md` contains:

1. **Wall time** — median ms + σ for ematix / DuckDB / Polars on the same file.
2. **Physical plan** — text-rendered from `displayable(plan).indent(true)`.
3. **Per-stage breakdown** — operator + elapsed_compute_ms + out_rows.
4. **Function self-time** — top families by % work compute (from `sample`).
5. **Theoretical floor** — per-stage table + total + waste ratio.
6. **Waste candidates** — ranked by confidence × impact, with concrete file:line + suggested fix.
7. **Findings to capture as memories** — anything generalisable beyond this query.
8. **Next levers** — short ordered list.

## How to run it

```bash
# 1. Build profiler + bench:
cargo build --release -p ematix-flow-core --example stage_profiler
cargo build --release -p ematix-flow-core --example tpch_triangulation_bench --features triangulation

# (RG decode cache + RH sum-f64 default ON — no env prefix needed.)

# 2. Wall-time comparison (3 engines, same file):
TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_QUERIES=<N> \
  TPCH_TRIALS=5 TPCH_WARMUPS=2 \
  ./target/release/examples/tpch_triangulation_bench

# Restore BENCHMARKS.md (the bench overwrites it for single-query runs):
git checkout BENCHMARKS.md

# 3. Per-operator metrics:
TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_QUERY=<N> \
  TPCH_TRIALS=5 TPCH_WARMUPS=2 \
  ./target/release/examples/stage_profiler

# 4. Function self-time (macOS sample on a long-running profile):
TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_QUERY=<N> \
  TPCH_TRIALS=40 TPCH_WARMUPS=2 \
  ./target/release/examples/stage_profiler > /tmp/qNN_stage.log 2>&1 &
BPID=$!
sleep 2
sample $BPID 10 -file /tmp/qNN_sample.txt
wait $BPID
```

Then aggregate using the Python parser at `docs/PERF_Q01.md` (the call-graph walker — copy from prior queries' workups).

## Important: scope discipline

The point of the survey is to **identify** waste, not fix everything inline. A "waste candidate" entry should land in the PERF_Q<n>.md doc — actually fixing it is a separate decision. The previous V5 / L13 cycle stalled because we kept jumping ahead to implementation before completing the survey. Resist that here.

---

## 2026-05-26 audit (Phase A.1)

Re-validates the §"Theoretical floor" constants against current kernel benches post-Σ.AG.7 / ematix-parquet 0.16.1. Hardware: Apple M3 Pro, 14 cores. Source data for each row in the "Source" column.

| # | Constant | Original | Measured 2026-05-26 | Status | Source |
|---|---|---|---|---|---|
| 1 | File I/O warm page cache | 5 GB/s | (accept as published) | UNVERIFIED | OS-level; no kernel bench in workspace |
| 2 | File I/O cold NVMe | 3 GB/s | (accept as published) | UNVERIFIED | OS-level; no kernel bench in workspace |
| 3 | Snappy decompress (compressible) | 2 GB/s/thread | 1.61 GB/s/thread (l_extendedprice ratio 0.73) | VERIFIED | `snappy_decompress_probe` (19% under, within ±30%) |
| 3a | Snappy decompress (incompressible) | — | 10.19 GB/s/thread (l_quantity ratio 1.00) | INFO | floor is dominated by compressible payload; near-memcpy when ratio≈1 |
| 4 | LZ4_RAW decompress (compressible) | 5 GB/s/thread | 4.23 GB/s/thread (l_extendedprice ratio 0.71) | VERIFIED | `ematix-parquet-codec/examples/bench_lz4_decompress` (15% under, within ±30%) |
| 4a | LZ4_RAW decompress (incompressible) | — | 61 GB/s/thread (l_shipdate/l_quantity ratio≈1.00) | INFO | LZ4 hits memcpy speed for incompressible pages |
| 5 | PLAIN i64/f64 unpack | 1 ns/row | ~0.6 ns/row (l_orderkey 3.51 ms ÷ 1.048M rows ≈ 3.35 ns/row mixed; pure i64 dict path 0.6 ns/row) | VERIFIED | `bench_decode` l_suppkey 0.62 ms _into / 1.048M = 0.59 ns/row — within floor |
| 6 | DICT decode (RLE indices + lookup) | 1-2 ns/row | 0.59 ns/row (l_suppkey i64), 0.65 ns/row (l_shipdate i32), 1.70 ns/row (gather 100% sel) | VERIFIED | `bench_decode` + `bench_dict_gather_prefetch` |
| 6a | DICT bit-unpack (u32 indices) | — | 0.05-0.11 ns/value (scalar), 0.05 ns/value (NEON bw=12-18) | INFO | `bench_unpack`; an order of magnitude under the DICT floor — already at L1 bandwidth |
| 7 | Filter (i32/i64 single predicate) | 0.5 ns/row | 0.62 ns/row (lane-parallel Q06 5-pred) | VERIFIED | `bench_lane_filter_sum`; 5-predicate fused = 0.62 ns/row, single-pred extrapolates well under 0.5 |
| 8 | Hash agg ≤100 groups (L1) | 1-2 ns/row | 2.17 ns/row (RobinHood card=100, 6M rows) | VERIFIED | `robin_hood_vs_hashbrown_bench` — at upper edge but in band; hashbrown is 4.49 ns/row (2.07× slower) |
| 9 | Hash agg 10K-1M groups | 5-15 ns/row | **3.0 ns/row** (RobinHood card=10K, 6M rows) | **STALE (low)** | `robin_hood_vs_hashbrown_bench` — kernel floor has moved DOWN since Σ.N.f.3 / pre-grow + dynamic resize. New floor: 3-7 ns/row at 10K cardinality; legacy 5-15 was a hashbrown-era number |
| 10 | Hash join build i64 keys | 5 ns/row | (accept as published) | UNVERIFIED | Bench source removed; criterion artefacts in `target/criterion/hash_join_i64_inner_emat_vs_stock/` are stale, no current runnable bench in workspace |
| 11 | Hash join probe (build in L2) | 8-15 ns/row | (accept as published) | UNVERIFIED | Same — no runnable kernel bench in workspace |
| 12 | Sort n × log₂(n) × 50-100 ns | published | (accept as published) | UNVERIFIED | No kernel bench in workspace; the literature constant stands |

### Notes

**Tally:** VERIFIED=6, STALE=1 (#9 Hash agg 10K-1M), UNVERIFIED=5.

**Material moves (>30% change):**

1. **#9 Hash agg 10K-1M groups** — kernel measured 3.0 ns/row vs published lower bound 5 ns/row, a 40% drop. Σ.N.f arc (Σ.N.f.1 pre-grow + Σ.N.f.2 dynamic resize + Σ.N.f.3 direct MutableBuffer finalize, per memories) moved the floor below the literature number. **Recommended new constant: `rows × 3-7 ns/row / cores` for 10K-1M groups.** Downstream Phase B floor tables that use the 5-15 number will *understate* waste for queries dominated by mid-cardinality aggs (Q09, Q10, Q20). Worth re-noting in the per-query writeups.

2. **No other constant moved by >30%.** Snappy is 19% under (1.61 vs 2 GB/s); LZ4 is 15% under (4.23 vs 5 GB/s) — both within band. The bit-unpack kernel numbers (0.05 ns/val) are an order of magnitude under the DICT floor, but that's because they measure a sub-stage (just the unpack) while the DICT floor covers the full RLE + lookup pipeline; they aren't directly comparable.

**A.2 verdict:** **SKIP.** Only 1 STALE constant (<3 threshold from the plan). Note the #9 revision in this appendix; do not rewrite the body table. Phase B writeups that touch mid-cardinality aggs should reference this appendix and use `3-7 ns/row / cores` as the floor.

**UNVERIFIED carry-forward:** hash join build/probe (rows 10/11) had a criterion bench (`hash_join_i64_inner_emat_vs_stock`) whose source was removed from the workspace. If Phase B finds a query whose dominant stage is hash join *and* the rough-floor math suggests near-floor or way-over-floor, run a fresh kernel bench (Σ.T V5 L13 work is in the archived plan — the artefacts may be re-instantiable from the `feat/l13-bloom-emitter` branch). For Phase B's purposes, accept 5/8-15 ns/row as published.

**Probe scripts (re-runnable):**
- `ematix-parquet/crates/ematix-parquet-codec/examples/bench_unpack` — bit-unpack scalar + NEON
- `ematix-parquet/crates/ematix-parquet-codec/examples/bench_decode` — end-to-end column decode vs parquet-rs vs polars
- `ematix-parquet/crates/ematix-parquet-codec/examples/bench_lz4_decompress` — **new** Phase A.1 probe; reads `lineitem_lz4.parquet` RG0 column N (default extprice), times `decompress_lz4_raw_into_sized` per page
- `ematix-parquet/crates/ematix-parquet-codec/examples/bench_dict_gather_prefetch` — dict gather across selectivity sweeps
- `ematix-flow-core/examples/snappy_decompress_probe` — Snappy on real lineitem columns
- `ematix-flow-core/examples/bench_lane_filter_sum` — lane-parallel Q06-shape filter+sum
- `ematix-flow-core/examples/robin_hood_vs_hashbrown_bench` — single-thread RobinHood vs hashbrown across 3 cardinalities
