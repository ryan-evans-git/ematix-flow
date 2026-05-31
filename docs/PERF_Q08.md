# PERF_Q08 — Q08 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH Phase B.8). Originally profiled 2026-05-25.

## Wall time

### 2026-05-26 refresh (20 trials × 3 warmups, post-Σ.AG.7 bare invocation — canonical)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **188.76** | 5.16 |
| DuckDB | 175.86 | 4.61 |

**7% behind DuckDB** (was 14%; we narrowed the gap by half, mostly from σ stabilising). Stage profile 5-trial: 190.91 ms.

σ collapsed from 58.68 → 5.16 — the 2026-05-25 variability was likely RG decode cache warmth issues that the Σ.O.c.2 default-on now stabilises.

## Physical plan

7-table join, market-share-by-year query. Region(AMERICA) → nation → customer → orders(1995-96) → lineitem → supplier → nation, plus part(ECONOMY ANODIZED STEEL) → lineitem.

```
SortPreservingMergeExec [o_year ASC]
  AggregateExec gby=[o_year] aggregates sum(case n=BRAZIL...)/sum(volume)
    HashJoinExec CollectLeft (r_regionkey, n_regionkey) -- region filter AMERICA
      region
      HashJoinExec CollectLeft (n_nationkey, s_nationkey) -- nation
        nation
        HashJoinExec CollectLeft (n_nationkey, c_nationkey) -- nation #2
          nation
          HashJoinExec Partitioned (c_custkey, o_custkey)
            customer
            HashJoinExec Partitioned (o_orderkey, l_orderkey)
              orders (filter o_orderdate ∈ [1995-01-01, 1996-12-31])  -- 4.5M rows
              HashJoinExec CollectLeft (s_suppkey, l_suppkey)
                supplier
                HashJoinExec Partitioned (p_partkey, l_partkey)         -- HOT
                  part (filter p_type='ECONOMY ANODIZED STEEL')        -- ~24k parts
                  lineitem                                              -- 60M, no filter
```

## Per-stage breakdown (2026-05-26)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | **EmatixFastParquetExec lineitem** (5 cols, no filter) | 22 | **595.99** | 59,986,052 |
| 2 | **HashJoinExec part_filt ⋈ lineitem** Partitioned | 20 | **185.33** | 403,487 |
| 3 | HashJoinExec orders ⋈ (part+supp+line) Partitioned (build 4.5M) | 16 | 78.76 | 122,404 |
| 4 | HashJoinExec customer ⋈ above Partitioned (build 1.5M > probe 122k) | 13 | 29.56 | 122,404 |
| 5 | RepartitionExec on l_partkey (60M memcpy) | 21 | 16.09 | 59,986,052 |
| 6 | EmatixFastParquetExec orders (+BridgeFilter date) | 19 | 8.21 | 4,557,513 |
| 7 | EmatixFastParquetExec customer (2 RGs only) | 15 | 7.59 | 1,500,000 |
| 8 | HashJoinExec supplier ⋈ part+lineitem CollectLeft | 19 | 4.62 | 403,487 |
| 9 | Repartition / nation / region / agg | various | <2 each | tiny |

Σ median compute: **936.44 ms**. Wall: 190.91 ms. **Effective parallelism: 4.91× = 35% — WORST in sweep so far.**

## Theoretical floor (per-stage, projection-cost-aware)

| Stage | Floor formula | Floor (sum ms) | Actual | Status |
|-------|---------------|---------------:|-------:|--------|
| Lineitem scan (60M × 5 cols, no filter, mixed Snappy) | ~2.0 GB / (3 GB/s × 14) | ~600-900 | 595.99 | **at-floor** ✓ |
| HashJoin part_filt ⋈ lineitem Partitioned (build 13k=108 KB→L1, probe 60M, 99.3% drop) | 60M × 8 ns L1-probe | ~480 | 185.33 | **sub-floor** (L1 hot path) ✓ |
| HashJoin orders ⋈ above Partitioned (build 4.5M=36 MB→L3, probe 403k) | 4.5M × 10 ns build + 403k × 30 ns probe = 45 + 12 = 57 | ~60 | 78.76 | 1.3× over (L3-resident build) |
| HashJoin customer ⋈ above Partitioned (build 1.5M=12 MB > probe 122k) | 1.5M × 5 ns build + 122k × 30 ns = 7.5 + 3.7 = 11 | ~12 | 29.56 | **2.5× over — build-side mis-order** (same as Q07) |
| Repartition l_partkey (60M × 8 B memcpy) | 480 MB / 70 GB/s × 14 = 7 ms wall × 14 sum | ~100 | 16.09 | sub-floor ✓ |
| Orders scan + BridgeFilter date (15M → 4.5M) | small (most credited downstream) | <30 | 8.21 | sub-floor (async) ✓ |
| Customer scan (1.5M, 2 RGs) | small, dict-heavy | ~5 | 7.59 | at-floor (2-RG bottleneck) |
| Supplier ⋈ (part+lineitem) CollectLeft (build 100k, probe 403k) | 403k × 15 ns CollectLeft probe | ~6 | 4.62 | at-floor ✓ |
| Nation scans (×2), region scan, top joins, agg | trivial | <5 | <3 | at-floor ✓ |
| **Σ floor** | | **~900** | **936** | **at-floor overall** |
| **Σ effective-parallelism floor** | 936 / 4.91 = | | **191 ms wall** | matches observed ✓ |

**Q08 is at its realistic-parallelism floor.** Every stage is at-or-near its kernel floor.

## Σ.AH waste candidate ranking

| Rank | Candidate | Mechanism | Wall savings | Confidence |
|-----:|-----------|-----------|-------------:|:----------:|
| 1 | **L9 bloom on part_filt (13k) → lineitem in Partitioned mode** | Currently L9 only fires on CollectLeft builds. Q08's part_filt⋈lineitem is Partitioned. Adding Partitioned-mode L9 emitter would drop lineitem 60M → ~400k pre-decode. | **~50-80 ms** | medium |
| 2 | **Build-side mis-order on customer⋈above (depth 13)** | Build (1.5M = 12 MB) > probe (122k = 1.5 MB). Side-swap rule would cut probe-build inversion. Same Q07 pattern. | ~5-10 ms | low (needs optimizer rule) |
| 3 | **Customer 2-RG re-emit** | 2-partition customer scan limits early pipeline. Same Q03/Q05/Q07. | ~3 ms | high (easy) |
| 4 | **Effective parallelism 35% → 50%** | CollectLeft small-dim chain + 8-table sequence; structural limit. | ~20 ms wall | low |

## Findings to capture as memories

- **Q08 is at realistic-parallelism floor.** The 13 ms gap to DuckDB is structural: lineitem-decode + 8-table pipeline serialisation. DuckDB's win is the L9-equivalent (bloom from part_filt → lineitem) which we don't fire in Partitioned-mode joins.
- **L9 emitter doesn't fire on Partitioned-mode joins** — confirmed by absence of `BuildSideBloomEmitterExec` in Q08's plan dump despite part_filt⋈lineitem being a textbook small-build / large-probe edge (probe/build ratio = 60M/13k = 4600, far above the 1024 threshold). **This is a new generalised candidate** — extend L9 to Partitioned-mode small-build joins. Q08 is the canonical case (would gain ~50 ms wall).
- **Build-side mis-order pattern (build > probe)** confirmed on Q07 + Q08 — two queries with the same DataFusion-planner choice that costs 5-10 ms each. Cross-query lever: optimizer rule to swap sides on Inner joins when probe < build.
- **Effective parallelism 35% is the worst in the sweep.** Cross-query trend: parallelism degrades with plan depth + CollectLeft count + Partial→Final agg presence. Q08's 8-table chain with 3× CollectLeft small-dim joins is the bottom of the parallelism scale.
- σ collapsed from 58.68 → 5.16 since 2026-05-25. The variability was RG decode cache warm-up; Σ.O.c.2 default-on stabilises.

## Next levers from Q08

1. **L9 Partitioned-mode extension** — highest-impact lever; Σ.AH cluster candidate. Affects Q08, possibly Q03 (cust+orders ⋈ lineitem build at 1.46M is also "small probe to lineitem" if reversed).
2. **Build-vs-probe side swap rule** (cross-query with Q07) — optimizer-level, needs careful gate.
3. **Customer 2-RG re-emit** (cross-query with Q03/Q05/Q07) — easy.

---

## Verify pass — 2026-05-26 (Σ.AH B.8)

**What changed since 2026-05-25:**
- Wall: 196.20 → 188.76 ms canonical (−4%, mostly noise stabilising).
- σ: 58.68 → 5.16 (huge stabilisation, attributed to Σ.O.c.2 default-on).
- vs DuckDB: was −14% behind → now −7% (we closed half the gap, mostly noise reduction).
- Plan structure: unchanged.
- L9 still not firing on part_filt⋈lineitem (Partitioned mode); 2026-05-25 candidate #1 confirmed as the top lever.

**New finding:** the build-vs-probe side mis-ordering observed in Q07 (depth 13) is the same pattern as Q08's customer⋈above (depth 13). Two queries now share this optimizer-level inefficiency.

**Next:** B.9 (Q09 — 273.41 ms; we win by 13% over DuckDB; largest absolute wall time after Q21).

## Waste candidates

### 1. L9 build-side bloom on part_filt → lineitem appears NOT to fire — check guard

Q08's most selective edge is `part (filter p_type='ECONOMY ANODIZED STEEL') ⋈ lineitem on (p_partkey, l_partkey)`. Part filter yields ~24k matching parts. Lineitem has 60M rows with ~12k distinct l_partkey values (well, more — at SF=10 every part has 30+ lineitem rows). The selectivity ratio probe-to-build = 60M / 24k = 2500 ≫ 1024 — should clear the L9 threshold.

But the plan shows NO `BuildSideBloomEmitterExec` between part and lineitem. Either:
- The rule fires only on CollectLeft mode and this join is Partitioned (need to check `EnableRuntimeBloomSidebandRule`)
- The rule fires but the bloom isn't being injected into the scan
- The rule has a guard against this specific shape

Expected impact: if L9 fires, lineitem scan effectively probes only ~12k partkey-matching keys. Bloom selectivity ~99% drops lineitem rows from 60M decode to ~600k. Wall: 196 → ~100 ms. **High-impact lever if it's a simple guard miss.**

### 2. Lineitem scan 637 ms compute with no filter pushed

Same pattern as Q05/Q07/Q08 — full lineitem decode. No predicate, so the only escape is a runtime bloom from upstream join builds. Without L9 firing on Q08's part→lineitem edge, we eat the 60M decode unconditionally.

### 3. High σ (58.68 ms over median 196) — RG decode cache warmth

Cold trial pays for the full RG decode cache miss. Memory [[sigma-oc1-landed]] notes default cache size 1 GB; lineitem at SF=10 is 2 GB. So lineitem decode doesn't fit fully in cache; cold trials decode from page cache, warm trials hit RG cache for the projected cols only. Variance is intrinsic.

## Findings to capture as memories

- **Q08 candidate for the same L9 audit as Q05/Q07**: small-build → lineitem edges may not all be getting bloom emitters. Compile a list of "where L9 should fire vs where it does" across the 22 queries before designing a fix.

## Next levers from Q08

1. (Cross-Q) **Audit L9 firing**: in each of Q03, Q05, Q07, Q08 the small-build → lineitem edge looks like a textbook L9 case. Verify which fire and which don't. May be a single rule-guard fix.
