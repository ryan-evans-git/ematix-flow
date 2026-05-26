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

## Theoretical floor (revised 2026-05-26 — per-column + parallelism-aware)

Q01 SF=10, 60M rows × 7 columns. Each column has very different compression — the prior uniform 1.61 GB/s estimate was wrong.

### Per-column Snappy decompress floor (from `snappy_decompress_probe` 2026-05-26):

| Column | Ratio | Throughput | Unc bytes (60M rows) | Per-thread decode | 14-core parallel |
|--------|------:|-----------:|---------------------:|------------------:|-----------------:|
| l_quantity (f64→dict) | ≈ 1.00 | 10.43 GB/s | ~45 MB | 4 ms | 0.3 ms |
| l_extendedprice (f64) | 0.73 | 1.66 GB/s | 492 MB | 296 ms | 21 ms |
| l_discount (f64, est) | ~0.73 | ~1.66 GB/s | ~492 MB | 296 ms | 21 ms |
| l_tax (f64, est) | ~0.73 | ~1.66 GB/s | ~492 MB | 296 ms | 21 ms |
| l_returnflag (char→dict) | ≈ 1.00 | ~10 GB/s | ~60 MB | 6 ms | 0.4 ms |
| l_linestatus (char→dict) | ≈ 1.00 | ~10 GB/s | ~60 MB | 6 ms | 0.4 ms |
| l_shipdate (i32, est) | ~0.73 | ~1.66 GB/s | ~240 MB | 144 ms | 10 ms |
| **Sum (parallel 14-core)** | | | **~1.88 GB** | | **~74 ms** |

### Full per-stage floor (parallel-equivalent, 14 cores, perfect parallelism):

| Stage | Floor formula | Floor (ms) |
|-------|---------------|-----------:|
| File I/O (page cache warm, ~300 MB compressed for the 7 cols) | 300 MB / 5 GB/s | ~60 ms serial → effectively masked by parallel decode |
| Snappy decompress (column-weighted, see table above) | (slow cols × 0.73 ratio dominate) | **74 ms** |
| PLAIN / DICT unpack | 60M × 0.6-1 ns/row / 14 | 3-4 ms |
| Filter (l_shipdate i32 cmp on 60M rows) | 60M × 0.62 ns/row / 14 | 2.7 ms |
| Multi-agg (4 groups, 4 sums + 4 avg + count) | 60M × 2.17 ns/row / 14 | 9.3 ms |
| Final assembly | — | ~1 ms |
| **Floor (perfect parallelism)** | | **~90 ms** |
| **Actual wall** | | **234.46 ms** |
| **Gap to floor** | | **144 ms / 2.6×** |

### Where the 144 ms gap actually goes

From the 2026-05-25 self-time profile + the 2026-05-26 stage profile:

- **EmatixFastParquetExec parallel compute: 1765.74 ms / 14 cores = 126 ms per-thread.** This is what one thread does end-to-end on its 4 RGs.
- **Observed parallel speedup: 7.53× (not 14×) = 54% effective.** Wall is 1765.74 / 7.53 = 234 ms.
- **Parallelism loss = (1/0.54 − 1) × 126 ms ≈ 108 ms** — the end-of-stream straggler effect. Each partition reads its 4 RGs sequentially; partitions that finish first idle while the slowest finishes.

So the 144 ms gap breaks down as:
- **~108 ms** = parallelism imbalance (54% effective, not 100%). Most of the gap.
- **~36 ms** = per-thread work above the ~90 ms perfect-parallel floor.

The per-thread 126 ms vs the 90 ms floor means each thread is paying ~36 ms over its share of the bandwidth + kernel floor. Likely sources (from 2026-05-25 samply):
- Arrow buffer allocations + ref-count churn between batches (the 45% "tail" in self-time): ~25 ms / thread
- FusedAgg `process_batch` per-row work above the 2.17 ns/row floor: ~10 ms / thread

DuckDB hits 237 ms — same wall, presumably also at ~54% effective parallelism on this scan. The fact that we're at parity with DuckDB suggests both engines are hitting the same parallelism ceiling on TPC-H SF=10 lineitem scans.

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

## Σ.AH waste candidate ranking (revised 2026-05-26)

Now anchored to the per-component decomposition above:

| Rank | Candidate | Est. wall savings | Confidence | Notes |
|-----:|-----------|------------------:|:----------:|-------|
| 1 | **Scan parallelism imbalance** (54% → 80% effective) | **~50 ms** (234 → ~180) | medium | Sub-RG splitting OR shuffle RG assignment by predicted decode cost. 22q-wide lever — every scan-heavy query pays this. |
| 2 | **Arrow buffer allocation tail** (45% self-time tail) | ~10-15 ms | low | Per-batch Arc<Buffer> ref-count churn + small slice allocations. Hard to attack without restructuring batch flow. |
| 3 | FusedAgg `process_batch` above 2.17 ns/row floor | ~5 ms | medium | Decimal128 arithmetic OR per-batch boolean-mask materialization. Microbench needed to confirm. |
| 4 | ~~StringViewArray validation~~ | — | — | **LANDED 2026-05-25**. |
| 5 | Snappy → LZ4_RAW codec swap on canonical file | ~3-4 ms | high | Known knob, deferred (industry convention). |

## Findings to capture as memories

1. **The 2026-05-25 PERF_Q01.md theoretical floor of 71 ms was too optimistic** — using a uniform Snappy constant on a 7-column projection with very different per-column compressibility undercounts the bandwidth-bound work. The correct per-column-weighted floor is ~74 ms for decompress alone, ~90 ms total for the perfect-parallelism case.
2. **Q01's biggest single waste is parallelism imbalance, not kernel inefficiency.** 54% effective parallelism on a 14-partition scan-heavy plan; ~108 ms / 234 ms wall is unrealised parallelism. Same pattern likely on every scan-heavy SF=10 query.

## Next levers from Q01

1. **Cross-query check (Phase C):** measure effective parallelism on every scan-heavy SF=10 query (Q03/Q06/Q12/Q14/Q19). If all show ~54% effective parallelism, parallelism imbalance is a top-priority arc (Σ.AH.X candidate).
2. **Defer Q01-specific microbench of FilterMultiAggSpec::process_batch** — the wall savings are bounded by the parallelism ceiling. A faster `process_batch` doesn't help if cores still idle at end-of-stream. Re-evaluate after the parallelism fix.

---

## Verify pass — 2026-05-26 (Σ.AH B.1)

**What changed since 2026-05-25:**
- Wall time: 264 → 234 ms (−11%) without any Q01-specific work landing. Source: cumulative tax from Σ.AE bitmap-stash, ematix-parquet 0.16.x SIMD parity, Σ.AG.7 plan cache default-on.
- Parallel ratio: 6.77 → 7.53× (more cores busy on average).
- vs DuckDB: was −8% behind, now +0.8% ahead (statistical tie at SF=10 noise band).

**Floor revision (the user's pushback):** the 2026-05-25 floor of 71 ms used a uniform Snappy constant. Replaced with per-column-weighted floor (74 ms decompress + agg/filter overhead = ~90 ms perfect-parallel). The 144 ms gap now breaks down as **~108 ms parallelism imbalance + ~36 ms per-thread overhead above kernel floor**. The parallelism imbalance is the dominant waste candidate, replacing FusedAgg microbench as the top priority.

**Next:** B.3 (Q03 — 145.74 ms, statistical tie with DuckDB).
