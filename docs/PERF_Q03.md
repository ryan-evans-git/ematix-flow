# PERF_Q03 — Q03 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH Phase B.3). Originally profiled 2026-05-25.
Data: `examples/tpch/data/sf10/*.parquet`, Snappy.

## Wall time

### 2026-05-26 refresh (20 trials × 3 warmups, post-Σ.AG.7 bare invocation — canonical)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **145.74** | 6.50 |
| DuckDB | 145.58 | 4.28 |

**Statistical tie** (ematix 0.1% slower). Was −6% ahead at the 2026-05-25 profile; DuckDB closed the gap (was 161.59 → now 145.58 — DuckDB got faster too, not just us).

Stage-profile 5-trial run: 147.69 ms (consistent).

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

## Per-stage breakdown (2026-05-26, top 12 by median elapsed_compute_ms)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | HashJoinExec (cust+orders ⋈ lineitem) | 4 | **332.84** | 302,114 |
| 2 | EmatixFastParquetExec (lineitem) | 7 | **269.26** | 59,986,052 |
| 3 | FilterExec (l_shipdate > 1995-03-15) | 6 | **110.57** | 32,334,250 |
| 4 | RepartitionExec (Hash(l_orderkey), 14p) | 5 | 77.16 | 32,334,250 |
| 5 | HashJoinExec (cust ⋈ orders) | 6 | 68.62 | 1,461,923 |
| 6 | EmatixFastParquetExec (customer, **2 partitions**!) | 10 | 41.13 | 300,276 |
| 7 | FilterExec (o_orderdate < 1995-03-15) | 8 | 22.43 | 7,289,442 |
| 8 | RepartitionExec (Hash(o_custkey)) | 7 | 15.40 | 7,289,442 |
| 9 | AggregateExec SinglePartitioned (3-col gby) | 3 | 13.21 | 114,003 |
| 10 | EmatixFastParquetExec (orders, 14 partitions) | 9 | 4.94 | 15,000,000 |
| 11 | RepartitionExec | 5 | 4.83 | 1,461,923 |
| 12 | SortPreservingMergeExec | 0 | 4.56 | 114,003 |

Σ median compute: **965.46 ms**. Wall median: 147.69 ms. **Effective parallelism: 6.54× = 47% of 14 cores.**

## Theoretical floor (per-stage, Phase A.1 audit constants)

Per-stage floor in **summed CPU ms** (units match `elapsed_compute_ms`). Floor formula: rows × per-row-floor (in ns) / 1e6 = ms.

| Stage | Floor formula | Floor (sum ms) | Actual (sum ms) | Over floor | Per-row ratio |
|-------|---------------|---------------:|----------------:|-----------:|---------------|
| FilterExec l_shipdate (60M i32 cmp) | 60M × 0.62 ns | 37 | 110.57 | **+74 (3×)** | 1.85 ns/row vs 0.62 floor |
| FilterExec o_orderdate (15M i32 cmp) | 15M × 0.62 ns | 9 | 22.43 | +13 (2.5×) | 1.49 ns/row |
| HashJoinExec cust ⋈ orders probe (7.3M) | 7.3M × 12 ns | 88 | 68.62 | **−19 (0.78×)** | 9.4 ns/row — at-floor ✓ |
| HashJoinExec (cust+orders) ⋈ lineitem probe (32M) | 32M × 12 ns (L2) to 30 ns (L3) | 384–960 | 332.84 | **−51** | 10.4 ns/row — at L2-floor (build 1.46M = 47 MB just barely fits L2 cluster) |
| RepartitionExec 32M rows (memcpy + Hash) | 32M × ~3 ns | 96 | 77.16 | −19 | near-floor |
| Hash agg SinglePartitioned (32M → 114k groups, f64 sum) | 32M × 3 ns (revised audit) | 96 | 13.21 | **−83** | already optimal (SinglePartitioned, no Partial+Final waste) ✓ |
| EmatixFastParquetExec lineitem (60M × 4 cols, mixed Snappy) | mix-weighted: ~3 GB/s × 14 = 42 GB/s; 1.4 GB / 42 = 33 ms wall × 14 = 462 ms sum | 462 | 269.26 | −193 | **at-floor (decode-bound, async overlap helps)** ✓ |
| EmatixFastParquetExec customer (1.5M × 2 cols, **2 partitions only**) | small | <10 | 41.13 | **+30** | 2-partition limit serialises early pipeline |
| EmatixFastParquetExec orders (15M × 4 cols, 14 partitions) | small | <30 | 4.94 | −25 | at-floor ✓ |
| SortPreservingMergeExec / Sort / Project | <10 | <10 | <5 | 0 | trivial |
| **Σ floor sum** | | **~830 ms** | **965 ms** | **+135** | |

**Effective-parallelism-adjusted floor:** 830 ms sum / 6.54 effective parallelism = **127 ms wall floor**. Observed: 147 ms. **Q03 is 16% over its realistic-parallelism floor** — already close to its ceiling.

The 2026-05-25 "70 ms" floor was the perfect-parallelism (14×) idealisation; on a real plan with 47% effective parallelism, the achievable floor is closer to 127 ms.

## Where the gap actually goes

| Decomposition | ms |
|---------------|---:|
| Wall observed | 148 |
| Realistic floor (sum / 6.54 effective parallelism) | 127 |
| **Gap** | **+21 ms** |

Where the 21 ms gap goes:
1. **+74 ms summed** on FilterExec l_shipdate over 0.62 ns/row floor → **+11 ms wall**. Batch-boundary overhead (BooleanBuffer materialisation, post-filter row-projection). 3× kernel floor.
2. **+30 ms summed** on customer scan over-floor → **+5 ms wall**. The 2-partition customer scan limits early pipeline parallelism. Customer.parquet has only 2 row groups at SF=10 → natural 2-way fan-out → must round-robin to 14 before downstream parallelism kicks in.
3. **+13 ms summed** on FilterExec o_orderdate → +2 ms wall (same batch-boundary tax).
4. Remaining ~3 ms wall = noise / orchestration tail.

**Parallelism gate:** 47% effective vs 14 cores theoretical. Pushing this to 60% would shrink the realistic-floor wall by 127 × (1 − 0.47/0.60) = **~28 ms wall savings**. But effective parallelism is structurally limited by the pipeline shape (build→probe→agg sequence + customer's 2-RG limit) — not all reachable.

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

## Σ.AH waste candidate ranking (current, per-stage anchored)

| Rank | Candidate | Wall savings | Confidence | Notes |
|-----:|-----------|-------------:|:----------:|-------|
| 1 | **FilterExec batch-boundary overhead** (3× kernel floor) | ~11 ms | medium | Q03 + Q02 both show ~3× kernel-floor on FilterExec (1.85 vs 0.62 ns/row). Cross-query lever: fuse FilterExec into scan or downstream operator to eliminate post-filter row-projection. |
| 2 | **L9 bloom pushdown** (cust+orders)→lineitem at scan time | ~15-25 ms | medium-low | Pre-2026-05-25 candidate. Check whether L9 already fires on Q03 — if it does, this is closed; if it doesn't, scan output drops 60M → ~302k. Memory note flagged this as a candidate but it was never verified. |
| 3 | **Customer scan 2-partition bottleneck** | ~5 ms | medium | customer.parquet has only 2 RGs at SF=10 — only 2 threads at scan time. Re-emit with more RGs (cheap fix, customer is small). |
| 4 | **Parallelism imbalance (47% → 60%)** | ~28 ms | low | Same lever as Q01/Q02. Structural limit from pipeline shape. Hard to push without rebalancing RG assignment. |
| 5 | **L9 + late-mat fusion** — push l_shipdate filter into lineitem scan | ~10 ms | low | `EMAT_FORCE_PARALLEL_BITMAP=1` and friends; previously rejected as default-on but may be safe for single-predicate i32 filters. |

## Findings to capture as memories

- **Q03 SF=10 is at 1.16× realistic-parallelism floor** — already close to ceiling. Only ~21 ms of wall headroom on a 148 ms query.
- **The 2026-05-25 floor of 70 ms (perfect-parallelism) was over-optimistic.** Realistic floor accounting for 47% effective parallelism is ~127 ms. The achievable wall improvement on Q03 alone is bounded ~15-20 ms (~12%).
- **AggregateExec SinglePartitioned at 13 ms on 32M rows in / 114k groups out is near-optimal** — the SinglePartitioned mode (vs Partial+Final) already elides the Q02-style orchestration waste. The optimizer picked the right pattern here because the upstream HashJoinExec output is already hash-partitioned on l_orderkey, which is a superset of the group-by keys.
- **FilterExec batch-boundary overhead is a Q01-Q03 cross-query pattern** at 2-3× kernel floor. Likely fixable by fusing FilterExec into the producing operator (HashJoinExec output projection or scan post-filter), saving 1-3 ns/row per filter.
- **Customer.parquet 2-RG layout is a structural bottleneck** at SF=10. Cheap fix: re-emit with 14+ RGs.

## Next levers from Q03

1. **Verify L9 firing on Q03** — fast diagnostic. Add Q03 to a probe that dumps whether `BuildSideBloomEmitterExec` is in the optimized plan. If it's there, check if lineitem scan actually drops 60M → ~302k. If not, the L9-pushdown candidate is the highest-value remaining lever on Q03.
2. **Customer re-emit** — trivial; bench-gate via Q03 wall delta. Estimate +5 ms wall.
3. **Defer FilterExec fusion** — pattern-confirm across more queries first (Q06/Q14/Q19/Q12 all have similar Filter-after-scan patterns).

---

## Verify pass — 2026-05-26 (Σ.AH B.3)

**What changed since 2026-05-25:**
- Wall: 151.44 → 145.74 ms canonical (−4%). Stage profile 147.69 ms.
- vs DuckDB: was −6% ahead, now **tied** (DuckDB also got faster, 161.59 → 145.58).
- Plan structure: unchanged.
- **Floor methodology fixed.** 2026-05-25 floor of 70 ms (perfect parallelism) was over-optimistic. Realistic floor at 47% effective parallelism is ~127 ms; we're 16% over.
- **New: SinglePartitioned agg confirmed near-optimal** (13 ms, 1.7× kernel floor on a 32M-row → 114k-group agg). Q02-style Partial+Final overhead is NOT present here because the planner correctly aligned RepartitionExec with the agg group keys upstream.
- **AggregateExec on l_orderkey/o_orderdate/o_shippriority is SinglePartitioned** because the prior HashJoinExec output is hash-partitioned on l_orderkey (a subset of the group-by columns). Cross-query: any time the optimizer can route hash-partitioned input directly to SinglePartitioned agg, the Q02 Partial+Final waste vanishes — Q03 demonstrates this works in practice.

**Top remaining cross-query candidates from Q03:**
1. **L9 bloom firing check** — if it's not firing on this shape, this is potentially a 15-25 ms win.
2. **FilterExec batch-boundary overhead** (now confirmed on 2 queries; promote to top Phase C candidate).
3. **Customer-table 2-RG bottleneck** — quick fix, ~5 ms.

**Next:** B.4 (Q04 — 54.30 ms, much smaller; faster verify, may close fast).
