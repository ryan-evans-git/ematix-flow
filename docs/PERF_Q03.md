# PERF_Q03 — Q03 SF=10 stage profile

Status: profiled 2026-05-25 (post StringView `new_unchecked` fix).
Data: `examples/tpch/data/sf10/*.parquet`, Snappy.

## Wall time (median of 5 trials, 2 warmups)

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 151.44 | 1.51 | 114,003 |
| DuckDB | 161.59 | 10.87 | 114,003 |
| Polars | 557.36 | 9.84 | 114,003 |

ematix is **6% ahead** of DuckDB. Tight win.

## Physical plan

3-way join: customer (filter c_mktsegment='BUILDING') ⋈ orders (filter o_orderdate<1995-03-15) ⋈ lineitem (filter l_shipdate>1995-03-15), grouped by (l_orderkey, o_orderdate, o_shippriority), sum revenue, sort top by revenue.

```
SortPreservingMergeExec [revenue DESC, o_orderdate ASC]
  SortExec
    AggregateExec SinglePartitioned gby=[l_orderkey, o_orderdate, o_shippriority] sum(l_extendedprice * (1 - l_discount))
      HashJoinExec Partitioned Inner (o_orderkey, l_orderkey)
        HashJoinExec Partitioned Inner (c_custkey, o_custkey)
          customer (filter c_mktsegment='BUILDING')                -- 30k rows
          orders (filter o_orderdate<1995-03-15)                   -- 7.3M rows
        lineitem (filter l_shipdate>1995-03-15)                    -- 32M rows (filtered from 60M)
```

## Per-stage breakdown (top 12, 5 trials)

| Rank | Operator | Depth | Median ms | Min | Max | Out rows |
|-----:|:---------|------:|----------:|----:|----:|---------:|
| 1 | EmatixFastParquetExec (lineitem) | 7 | 345.07 | 296.48 | 385.75 | 59,986,052 |
| 2 | HashJoinExec (cust+orders ⋈ lineitem) | 4 | 302.65 | 282.15 | 319.36 | 302,114 |
| 3 | FilterExec (l_shipdate > 1995-03-15) | 6 | 104.76 | 98.00 | 126.43 | 32,334,250 |
| 4 | HashJoinExec (cust ⋈ orders) | 6 | 72.43 | 68.40 | 83.20 | 1,461,923 |
| 5 | RepartitionExec (Hash(l_orderkey), 14p) | 5 | 64.42 | 52.84 | 137.85 | 32,334,250 |
| 6 | EmatixFastParquetExec (customer) | 10 | 37.05 | 4.28 | 43.95 | 300,276 |
| 7 | FilterExec (o_orderdate filter) | 8 | 22.57 | 20.03 | 27.12 | 7,289,442 |
| 8 | EmatixFastParquetExec (orders) | 9 | 15.97 | 2.54 | 62.74 | 15,000,000 |
| 9 | RepartitionExec | 7 | 14.85 | 14.22 | 15.91 | 7,289,442 |
| 10 | AggregateExec SinglePartitioned | 3 | 7.45 | 7.16 | 11.60 | 114,003 |
| 11 | RepartitionExec | 5 | 4.86 | 4.18 | 5.38 | 1,461,923 |
| 12 | SortPreservingMergeExec | 0 | 4.56 | 4.45 | 4.70 | 114,003 |

Σ median compute: 997 ms. Wall median 148 ms. Parallel speedup ≈ 6.73× of 14 cores.

## Theoretical floor

| Stage | Floor formula | ms |
|-------|---------------|---:|
| lineitem scan + decompress (4 cols × 60M rows, Snappy) | ~340 MB uncompressed / (2 GB/s × 14) | 12 |
| orders scan + decompress (4 cols × 15M rows) | ~120 MB / (2 GB/s × 14) | 4 |
| customer scan (2 cols × 1.5M rows) | trivial | 1 |
| Filter l_shipdate > date (60M rows i32 cmp) | 60M × 0.5 ns / 14 | 2 |
| Filter o_orderdate < date (15M rows) | 15M × 0.5 ns / 14 | 1 |
| Filter c_mktsegment = string (1.5M rows) | string-cmp ~5 ns/row | 1 |
| HashJoin build cust ⋈ orders (30k × 7.3M) → 1.46M rows | build 30k + probe 7.3M × 8 ns / 14 | 5 |
| HashJoin probe (cust+orders) ⋈ lineitem (1.46M build, 32M probe) | 32M × 12 ns / 14 | 27 |
| Repartition 32M rows | memcpy-bound, 32M × 30 B / 14 / 30 GB/s | 2 |
| Hash agg (~114k groups, sum f64) | 32M × 6 ns / 14 | 14 |
| Sort 114k rows | trivial | <1 |
| **Floor** | | **~70 ms** |
| **Actual** | | **148 ms** |
| **Waste ratio** | | **2.1×** |

## Waste candidates

### 1. l_shipdate filter NOT pushed into scan — 28M rows decoded then discarded

The plan shows `FilterExec(l_shipdate > 1995-03-15)` as a **separate node** above `EmatixFastParquetExec`. The scan outputs 60M rows; the filter passes 32M (54% pass rate). That means the bridge-filter / late-mat path is NOT applied here.

Memory [[sigma-e5-streaming-late-mat-landed]] notes the masked-decode infrastructure exists but is dormant pending dict-preserved Utf8View. l_shipdate is `date32` (i32) — should be eligible. Memory [[sigma-q-l13-landed]] reports `EMAT_FORCE_PARALLEL_BITMAP=1` as the opt-in for this.

Expected impact: if the 28M-row decode skip succeeds, lineitem decode + filter drops from ~50 ms wall to ~25 ms = **wall 148 → ~120 ms** (~19% improvement).

Risk: this exact lever was tried in [[sigma-l3c-reverted]] (parallel masked-AND default-on) and regressed 22q geomean +16.8 pp — but that finding was for the `with_adaptive_reordering` arm. The basic single-predicate l_shipdate path may be safer. Needs A/B before defaulting on.

### 2. HashJoinExec (cust+orders ⋈ lineitem) at 303 ms compute = 30 ms wall

302k output rows from a 32M-row probe against a 1.46M-row build. Per-row probe cost is 303 ms × 14 / 32M = ~132 ns/row probe, which is 10× the floor of ~12 ns/row.

The build (1.46M rows) is too large for L2 (256-512 KB). Each probe pays L3 access. This is the same memory-bandwidth-bound regime as [[sigma-r2-rejected]] hit on Q17.

Likely lever: **Bloom-filter pushdown from the (cust ⋈ orders) build side into the lineitem scan** before the join. Memory [[sigma-j2b-vii-landed]] has the build-side bloom emitter at `emit_build_side_blooms_local`. Need to verify L9 fired here — the scan output is 60M (full table), suggesting the bloom didn't pre-filter lineitem at scan time. If L9 had fired, lineitem scan would emit only orderkeys present in the 1.46M build, dropping the probe side dramatically.

Q03 should be a textbook L9 case. Check why the bloom isn't pushing in.

### 3. Lineitem scan 345 ms parallel compute on 60M rows = 5.7 ns/row decode

Floor for snappy-compressed 60M × 4-col scan is ~12 ms parallel. We're 28× the floor (345 ms). That's 24 ms of wall time over the parallel-amortised floor. Profile the scan to see where the time goes — likely:
- Snappy decompress (per [[q06-sf10-polars-gap]] Snappy is ~1.7 GB/s, slower than my 2 GB/s assumption)
- PLAIN i64/f64 unpack for the 3 numeric cols
- Some scan-side overhead (RG decode cache miss on cold trial, page metadata reads)

Worth a sample-profile pass to confirm.

## Findings to capture as memories

- Q03 SF=10 candidate: **L9 build-side bloom not firing on (cust+orders)→lineitem**. Check why — the L9 rule fires on small-build / large-probe / FK shapes, and Q03 is exactly that shape. May be a rule guard miss-fire.
- Q03 SF=10 candidate: **l_shipdate predicate pushdown into scan masked-decode**. Eligible by type; blocked by past noise band on parallel-masked default.

## Next levers from Q03

1. **Check L9 fired** (or didn't) on Q03 — `EMAT_RT_BLOOM_SIDEBAND=1` is default; verify in the EXPLAIN ANALYZE output whether `BuildSideBloomEmitterExec` is in the plan. If it's there but the scan doesn't drop rows, the bloom is being installed but the in-scan filter isn't consuming it.
2. **Profile the lineitem scan with sample** to confirm Snappy is the dominant cost.
