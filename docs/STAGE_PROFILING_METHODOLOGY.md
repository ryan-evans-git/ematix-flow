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
