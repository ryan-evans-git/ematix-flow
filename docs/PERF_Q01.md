# PERF_Q01 — Q01 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH Phase B.1). Originally profiled 2026-05-25.
Hardware: M-series arm64, 14 cores.
Data: `examples/tpch/data/sf10/lineitem.parquet`, Snappy, 58 row groups, 60M rows.

## Wall time

### 2026-05-26 refresh (20 trials × 3 warmups, post-Σ.AG.7 bare invocation — canonical)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **235.38** | 10.52 |
| DuckDB | 237.23 | 4.14 |

ematix and DuckDB are now in a statistical tie at SF=10 (ematix 1% faster, well inside the SF=10 noise band). Down from −8% vs DuckDB at the 2026-05-25 profile.

### Stage profile (5 trials × 2 warmups, current run for this verify)

- per-trial (ms): [226.07, 234.11, 235.94, 234.46, 236.12]
- median: 234.46 ms

### 2026-05-25 baseline (for delta tracking, deprecated)

ematix-flow 264.12 ± 13.62, DuckDB 244.31, Polars 444.59. The 264 → 234 ms gain (−11%) is the cumulative impact of: (1) StringView `new_unchecked` (candidate #1, landed 2026-05-25); (2) Σ.AE post-filter cardinality + bitmap-stash + Arc-shared buffer; (3) ematix-parquet 0.16.x SIMD parity; (4) Σ.AG.7 plan cache default-on (Q01 is cacheable; ~0.7 ms saved per repeat trial).

## Physical plan (post-optimizer)

```
ProjectionExec
  FusedAggregateExec<FilterMultiAggSpec>
    EmatixFastParquetExec(partitions=14, row_groups=58, projection=[4,5,6,7,8,9,10])
```

7 columns projected: l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate.

`FusedAggregateExec` collapses the FilterExec (l_shipdate predicate) and the multi-aggregate
into one pass over each batch. There is no separate FilterExec or AggregateExec in the plan
— so per-operator `MetricsSet` timing can't separate filter cost from aggregate cost. We use
function-level self-time (macOS `sample` at 1kHz over a ~10s run, 40 trials) to expand the
black box.

## Per-stage breakdown (operator-level, 5 trials median, 2026-05-26)

| Operator | Median ms compute | Out rows |
|----------|------------------:|---------:|
| EmatixFastParquetExec | 1765.74 (parallel, 7.53× wall) | 59,986,052 |
| FusedAggregateExec | 0 (work credited to upstream pull) | 0 |
| ProjectionExec | 0 | 4 |

`elapsed_compute` for `EmatixFastParquetExec` accumulates time spent inside its `next()` calls, which inline the downstream `FusedAggregateExec::process_batch`. The parallel ratio improved from 6.77× → 7.53× since 2026-05-25 (more cores busy on average), but **half the cores are still starved** at end-of-stream. Plan runs 14 partitions but the scan reads sequentially within each row group, and partitions that finish their RGs early go idle. Candidate #4 still relevant.

## Function self-time (samply / macOS `sample`, 4kHz, work threads only)

Top contributors, aggregated by family:

| Family | % of work compute |
|--------|------------------:|
| FusedAgg `process_batch` (filter eval + 4 sums + 4 avgs + count) | 25.1% |
| UTF-8 validation (`from_utf8` + `validate_string_view`) | 9.0% |
| Snappy decompress (`snap::decompress::Decoder`) | 7.4% |
| Bitpack NEON unpack / dict RLE (across `unpack_*_neon_bw{1,2,4,6,12}`) | 5.0% |
| OS I/O (`pread` + `madvise` + `kevent`) | 6.0% |
| `_platform_memmove` (Arrow buffer copies) | 0.8% |
| Hasher / `hash_one` (group-by hash) | 1.5% |

(Sum < 100% because the long tail of < 0.05%-each functions adds another ~45%.)

## Theoretical floor

Per-stage lower bounds for Q01 SF=10, parallel across 14 cores. **Constants per Phase A.1 audit (2026-05-26 appendix):**

| Stage | Floor formula | Floor (ms) |
|-------|---------------|-----------:|
| File I/O (page cache warm) | ~80 MB compressed bytes / 5 GB/s | ~16 |
| Snappy decompress | 880 MB uncompressed / (1.61 GB/s × 14) | 39 |
| PLAIN i64/f64 unpack + DICT utf8 decode | 60M × 0.6-1 ns/row / 14 | 3-4 |
| Filter (l_shipdate i32 cmp on 60M rows) | 60M × 0.62 ns/row / 14 | 2.7 |
| Multi-agg (4 groups, 4 sums + 4 avg-num/denom + count) | 60M × 2.17 ns/row / 14 | 9.3 |
| Final assembly | — | ~1 |
| **Floor (Snappy, revised)** | | **~71 ms** |
| **Actual** | | **234.46 ms (was 264.12)** |
| **Waste ratio** | | **3.3× (was 4.2×)** |

DuckDB hits 237 ms on the same file — within 1% of ematix — so this floor model is still too optimistic vs what any production engine achieves on this shape. Most of the gap is **memory bandwidth and cache-line traffic** that the simple decode-rate model ignores: touching 3.8 GB of column data once is not free even at 80 GB/s aggregate bandwidth, since each column gets compared and accumulated separately rather than fused into a single sweep over the row.

Updated since 2026-05-25: floor moved up 8 ms (63 → 71) using audit-verified 1.61 GB/s Snappy constant (was 2.0 GB/s). Actual moved down 30 ms. Net waste-vs-floor ratio improved 4.2× → 3.3×.

## Waste candidates worth targeting

Ranked by confidence and self-time impact:

### 1. UTF-8 validation in `StringViewArray::try_new` — LANDED 2026-05-25

[crates/ematix-flow-core/src/emat_arrow_reader.rs:2733](crates/ematix-flow-core/src/emat_arrow_reader.rs:2733) was calling `StringViewArray::try_new`; SAFETY comment already documented why `new_unchecked` is sound. Replaced with `unsafe { StringViewArray::new_unchecked(...) }`. Fix is one line.

**Q01 SF=10 measured impact:** 264.12 → 245.75 ms (−7.0%, in the predicted 16-26 ms band). ematix now slightly ahead of DuckDB (250.77 ms) on Q01.

**22q SF=10 A/B (5 trials × 2 warmups, post/pre, sorted by Δ%):**

| Q | Δ% | Note |
|---|---:|------|
| Q22 | **−20.4%** | confirmed win (outside ±2σ) — customer.c_comment-heavy |
| Q12 | −13.6% | |
| Q13 | **−13.0%** | confirmed win |
| Q21 | **−9.7%** | confirmed win |
| Q20 | −8.6% | |
| Q14 | −7.1% | |
| Q16 | **−6.4%** | confirmed win |
| Q06 | −4.2% | |
| Q19 | −3.5% | |
| Q01 | −1.0% | (Q01 standalone showed −7%; the 22q view is reduced because Q01 is amortized across 7 trials in both pre and post) |
| ... | ... | |
| Q05 | +7.3% | within ±2σ (combined σ=16.5, Δ=13.7) |
| Q17 | +13.5% | within ±2σ (combined σ=29.7, Δ=24.7) — flag for follow-up if seen again |

**22q geomean post/pre: 0.9736 (−2.6%)**. 14/22 faster, 4 wins clearly outside the noise band — exactly the string-heavy queries the fix targets. No statistically significant regressions.

### 2. FusedAgg `process_batch` is 25% of self-time but only 9 ms theoretical

A 16× gap between floor and observed for the filter+agg combined work. Worth a focused profile inside `FilterMultiAggSpec::process_batch` to find what is paying 5-10× over the floor model. Candidates:
- Per-row decimal128 arithmetic in `l_extendedprice * (1 - l_discount)` (Q01 uses decimal columns in TPC-H by spec; we cast to f64?). If we're computing on decimal128 the per-op cost is ~5x scalar f64.
- 4-element group dict is being hashed via `core::hash::BuildHasher::hash_one` (`SipHash` per memory `[[sigma-nf3-beats-stock]]` says we *don't* use `RobinHoodSumF64Exec` for the multi-agg path — only the SUM(f64) path).
- Filter mask is being materialised as an `arrow_buffer::BooleanBuffer` per batch even though the agg loop walks rows directly.

Bench gate: extract `FilterMultiAggSpec::process_batch` to a Criterion microbench, see what the per-row cost is on 5k-row × 7-col batches.

### 3. Snappy → LZ4_RAW codec swap on the canonical file — known knob, deferred

`lineitem.lz4.parquet` already exists alongside the canonical Snappy file. Switching the canonical to LZ4 would shave ~3-4% of Q01 wall (`snap::decompress` is 7.4% of self-time, LZ4 is roughly 2× faster per byte) and benefits all 3 engines proportionally. Deferred: matches industry TPC-H convention (Snappy) and would force us to flag every published number with an asterisk. Documented here so we don't repeat the investigation.

### 4. Parallelism ceiling — 6.77/14 cores effective on a single-table scan

The 14-partition layout reads each row group sequentially. For SF=10's 58 RGs × 14 partitions, some partitions own 4 RGs and others own 5 (uneven split), and within a partition the RG-decode loop is serial. End-of-stream stragglers visibly idle cores. Not specific to Q01 — every scan-heavy SF=10 query likely shows the same effective parallelism ratio.

Potential lever: shuffle RG assignment by predicted decompressed bytes (we have per-RG num_rows + per-column compressed bytes from parquet metadata). Or split RGs into sub-RG work units. Not free — sub-RG splitting interacts with dict-page locality.

### 5. RG decode cache (Σ.O.c) — confirmed wired, not measured here

`EMAT_RG_DECODE_CACHE=1` was set during this profile. Memory record `[[sigma-oc2-provider-landed]]` reports 65× speedup on rep-2+ scans of the same projection. My bench ran 5 timed trials + 2 warmups = 7 sequential executions of Q01, so trials 2-5 should hit cache. The 264 ms median is post-cache-warmth; cold trial 1 was 280 ms (the max in the per-trial vector). Cache is doing its job — there's no further "decode cost" to chase here once the cache is warm.

## Findings to capture as memories

1. **Canonical SF=10 `lineitem.parquet` is Snappy**; LZ4 sibling lives at `lineitem.lz4.parquet` (deliberately, for the Q06 sensitivity panel). Documented so future profile sessions don't get confused.
2. **`StringViewArray::try_new` validation is dead weight in our reader path** — has correctness shortcut available via `new_unchecked`, blocking ~9% of Q01 SF=10 self-time.

## Next levers to evaluate

In order of expected payoff vs effort:

1. ~~**Try `new_unchecked`** for StringViewArray + DictionaryArray construction~~ — **LANDED 2026-05-25**.
2. **Microbench `FilterMultiAggSpec::process_batch`** to find the gap to floor (now 3.3× rather than 4.2×, but still 165 ms of "waste" — the highest absolute candidate). Likely decimal128 or per-batch boolean-mask materialisation.
3. **Investigate scan parallelism imbalance** as a 22q-wide lever — Q01 ran at 7.53/14 effective cores (was 6.77/14). Improvement is real but ceiling is the same. If a future change pushes Q01 past 11/14 effective cores, that's ~50 ms of wall time at the same floor.

---

## Verify pass — 2026-05-26 (Σ.AH B.1)

**What changed since 2026-05-25:**
- Wall time: 264 → 234 ms (−11%) without any Q01-specific work landing. Source: cumulative tax from Σ.AE bitmap-stash, ematix-parquet 0.16.x SIMD parity, Σ.AG.7 plan cache default-on.
- Parallel ratio: 6.77 → 7.53× (more cores busy on average).
- vs DuckDB: was −8% behind, now +0.8% ahead (statistical tie at SF=10 noise band).

**Waste candidates still relevant:** #2 (FilterMultiAggSpec process_batch — same kernel, same gap) and #4 (parallelism ceiling — improved but not closed). Σ.AH Phase C should fold these into the cross-query synthesis if other scan-heavy queries show the same scan-parallelism ceiling.

**No new candidate identified.** Q01 didn't surface a new structural inefficiency that hadn't already been documented in 2026-05-25.

**Next:** B.2 (Q21, 311.87 ms — largest absolute wall time, biggest absolute-waste candidate).
