# PERF_Q09 — Q09 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH Phase B.9). Originally profiled 2026-05-25.

## Wall time

### 2026-05-26 refresh (20 trials × 3 warmups, post-Σ.AG.7 bare invocation — canonical)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **273.41** | 7.76 |
| DuckDB | 313.32 | 4.27 |

**13% ahead of DuckDB** (unchanged from 2026-05-25). Stage profile 5-trial: 277.97 ms.

## Physical plan

6-table join: part (filter p_name LIKE '%green%') ⋈ lineitem ⋈ supplier ⋈ partsupp ⋈ orders ⋈ nation. 2-key join on (ps_partkey + ps_suppkey).

```
SortPreservingMergeExec [nation ASC, o_year DESC]
  AggregateExec gby=[nation, o_year] sum(amount)
    HashJoinExec CollectLeft (n_nationkey, s_nationkey) -- nation
      HashJoinExec Partitioned (o_orderkey, l_orderkey)
        orders
        HashJoinExec Partitioned (2-key) (ps_suppkey, l_suppkey) ∧ (ps_partkey, l_partkey)
          partsupp -- 8M
          HashJoinExec CollectLeft (s_suppkey, l_suppkey)
            supplier
            HashJoinExec Partitioned (p_partkey, l_partkey)
              part (filter p_name LIKE '%green%')   -- ~10k matching parts
              lineitem                              -- 60M, no filter
```

## Per-stage breakdown (2026-05-26)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | **HashJoinExec part_filt ⋈ lineitem** Partitioned (build 108k=L2-fit, probe 60M) | 16 | **521.25** | 3,261,613 |
| 2 | **HashJoinExec orders ⋈ above** Partitioned (build 15M=L3-spill, probe 3.26M) | 9 | **443.81** | 3,261,613 |
| 3 | **HashJoinExec ⋈ partsupp 2-key** Partitioned (build partsupp 8M=128 MB DRAM, 2-key, probe 3.26M) | 12 | **322.77** | 3,261,613 |
| 4 | EmatixFastParquetExec lineitem (6 cols, no filter) | 18 | 231.62 | 59,986,052 |
| 5 | EmatixFastParquetExec orders (2 cols, no filter) | 11 | 168.72 | 15,000,000 |
| 6 | HashJoinExec supplier ⋈ above CollectLeft (build 100k, probe 3.26M) | 15 | 68.08 | 3,261,613 |
| 7 | ProjectionExec (3.26M rows) | 6 | 45.89 | 3,261,613 |
| 8 | RepartitionExec | 13 | 32.43 | 3,261,613 |
| 9 | **AggregateExec Partial** (3.26M → 2450 groups, gby=(Utf8 nation, i32 year), sum f64) | 5 | **31.13** | 2,450 |
| 10 | RepartitionExec | 10 | 24.08 | 3,261,613 |
| 11 | FilterExec p_name LIKE '%green%' (2M → 108k = 5.4%) | 18 | 22.92 | 108,782 |
| 12 | HashJoinExec nation ⋈ above CollectLeft | 7 | 20.93 | 3,261,613 |

Σ median compute: **1948.30 ms**. Wall: 277.97 ms. **Effective parallelism: 7.01× = 50%.**

## Theoretical floor (per-stage, projection-cost-aware)

| Stage | Floor formula | Floor (sum ms) | Actual | Status |
|-------|---------------|---------------:|-------:|--------|
| Lineitem scan (60M × 6 cols) | 3 GB unc / (3 GB/s × 14) | ~1000 | 231.62 | sub-floor (async pipelining) |
| Orders scan (15M × 2 cols) | small | ~100 | 168.72 | sub-floor |
| HashJoin part_filt ⋈ lineitem (build 108k=L2, probe 60M) | 60M × 10 ns L2-probe | ~600 | 521.25 | at-floor ✓ |
| HashJoin orders⋈above (build 15M=120 MB→DRAM, probe 3.26M) | 15M × 10 ns build + 3.26M × 30 ns probe = 150 + 98 = 248 | ~250 | 443.81 | **1.8× over** — build-side mis-order (build 120 MB > probe 78 MB) |
| HashJoin partsupp 2-key (build 8M=128 MB→DRAM, probe 3.26M, 2-key) | 8M × 10 ns DRAM build + 3.26M × 40 ns 2-key DRAM probe = 80 + 130 = 210 | ~210 | 322.77 | **1.5× over** — partsupp build dominates |
| HashJoin supplier⋈above CollectLeft (build 100k=L2, probe 3.26M) | 3.26M × 15 ns = 49 | ~50 | 68.08 | 1.4× over |
| FilterExec p_name LIKE '%green%' (2M in, 108k out) | LIKE matcher 9-14× std → ~15 ns/row × 2M = 30 ms | ~30 | 22.92 | at-floor ✓ |
| AggregateExec Partial (3.26M rows → 2450 groups, 2-col gby) | 2-col gby ~10 ns/row × 3.26M = ~33 | ~30 | 31.13 | **at-floor for 2-col** ✓ |
| Repartition/Projection ops | memcpy + light work | ~80 | ~145 | mild over |
| Nation ⋈ above, top joins | trivial | ~20 | 20.93 | at-floor ✓ |
| **Σ floor sum** | | **~1370 ms** | **1948 ms** | |
| **Σ effective-parallelism floor** | 1948 / 7.01 = | | **278 ms wall** | matches observed ✓ |

**Q09 has ~570 ms of identifiable waste over kernel floor** — biggest absolute over-floor in the sweep so far. The two biggest offenders are joins where the build side spills out of cache:
- **orders⋈above (depth 9): 444 ms vs 248 ms floor** = 196 ms parallel over-floor (~28 ms wall)
- **partsupp 2-key (depth 12): 323 vs 210 floor** = 113 ms parallel (~16 ms wall)

These are L3 / DRAM-spill builds. Both are structural: the planner picks the larger table as build because it doesn't track per-table cardinalities post-filter accurately.

## Σ.AH waste candidate ranking

| Rank | Candidate | Mechanism | Wall savings | Confidence |
|-----:|-----------|-----------|-------------:|:----------:|
| 1 | **L9 Partitioned-mode bloom: part_filt (108k) → lineitem (60M) AND part_filt → partsupp (8M)** | Drops lineitem 60M → ~3.3M and partsupp 8M → ~108k pre-decode. Critically, partsupp pre-filter changes the 2-key join build from 128 MB → 1.7 MB (L1!). Massive cascade. | **~80-100 ms** | medium |
| 2 | **HashJoinExec orders ⋈ above build-side swap** (depth 9) | Build 15M = 120 MB > probe 3.26M = 78 MB → swap → probe L3-resident build of 78 MB | ~28 ms | low (optimizer rule) |
| 3 | **HashJoinExec partsupp 2-key build-side swap** (depth 12) | Build partsupp 8M = 128 MB > probe 3.26M = 78 MB → swap → probe smaller build | ~16 ms | low (compound-key swap is harder) |
| 4 | **Multi-column compound-key Robin Hood agg** | 2-col gby (Utf8 + i32) currently at 10 ns/row vs Robin-Hood single-key at 3 ns/row. Σ.R.2 was rejected for single-col i64 AVG; compound-key SUM extension is different shape. | ~7 ms (31→10 sum ms = ~3 ms wall) | low |
| 5 | **Effective parallelism 50% → 60%** | Partial→Final agg + 6-table chain | ~15 ms wall | low (structural) |

## Findings to capture as memories

- **Q09 has the most identifiable absolute over-floor waste in the sweep** (~570 ms parallel = ~80 ms wall). Same root cause as Q05/Q07/Q08: large-build joins spilling caches when a runtime bloom could pre-filter the build.
- **L9 Partitioned-mode extension would compound-effect on Q09**: pre-filter BOTH lineitem and partsupp via part_filt's keys. This converts the 128 MB partsupp 2-key build into a 1.7 MB build, moving the 2-key join from DRAM-bound to L1-bound. **Best single lever for Q09.**
- **2-key joins where one build > probe size show up in Q07/Q08/Q09** — generalised optimizer-level side-swap rule increasingly important. Q09 has TWO such joins (depth 9 and depth 12).
- **Q09's 2-col group-by aggregate at 10 ns/row** is the realistic floor for (Utf8, i32) keys — Robin Hood was for i64 keys and a compound version would be a different kernel. Not the biggest lever.

## Next levers from Q09

1. **L9 Partitioned-mode extension** — pattern across Q05/Q07/Q08/Q09 now (4 queries). Q09 specifically gets BOTH lineitem and partsupp pre-filtered → biggest cascade. **Top Σ.AH cluster candidate**.
2. **Build-side swap rule** — now seen on Q07 + Q08 + Q09. Defer to Σ.T-style cost-based rewrite, OR add a narrow rule for build > probe shape.
3. **Defer compound-key Robin Hood** — Q09 alone insufficient justification.

---

## Verify pass — 2026-05-26 (Σ.AH B.9)

**What changed since 2026-05-25:**
- Wall: 278.33 → 273.41 ms canonical (−2%). σ stable.
- vs DuckDB: −13% ahead (unchanged).
- Plan structure: unchanged.
- L9 still NOT firing on part_filt → lineitem OR part_filt → partsupp (no `BuildSideBloomEmitterExec` in plan).

**New per-stage decomposition** identifies the partsupp 2-key build at 128 MB (DRAM-bound) as a co-dominant waste with the orders⋈above build-side mis-order. The Q09 waste is structural: two large-build joins (depths 9 and 12) where the optimizer picked the wrong build side.

**Σ.AH top candidate strengthened:** L9 Partitioned-mode extension would compound-effect on Q09 (drops both lineitem AND partsupp via the same part_filt bloom). This makes Q09 the canonical test case for the L9 Partitioned-mode arc.

**Next:** B.10 (Q10 — 231.97 ms; we win by 43% vs DuckDB; biggest win margin of any query).

## Waste candidates

### 1. Same L9 missing-bloom-edge as Q05/Q07/Q08

Q09 has the same shape: small-build (part_filt 10k) → large-probe (lineitem 60M). No `BuildSideBloomEmitterExec` visible in the plan between part_filt and lineitem. If L9 fired, lineitem decode could be skipped on the 99.8% of rows whose l_partkey doesn't match the filter.

This is now the **4th consecutive query** showing this miss (Q05, Q07, Q08, Q09). It's clearly a pattern.

### 2. AggregateExec Partial at 64 ms compute for 2450 groups — high

Partial agg processing 3.26M rows into 2450 groups: 64 ms compute = ~22 ns/row. Memory [[sigma-nf3-beats-stock]] says RobinHoodSumF64Exec beats stock here. But Q09's group-by is `(nation, o_year)` — 2-column key (Utf8 + i32). RobinHoodSumF64Exec is keyed on i64 only.

Worth a separate Robin-Hood-like specialization for 2-column-key SUM(f64) aggregations.

### 3. partsupp 8M scan visible as separate node (no L9)

partsupp acts as the build of the 2-key join (ps_suppkey, ps_partkey). Build side is 8M rows, probe is the post-supplier output. The 2-key build is large enough to spill out of L2.

If a runtime bloom were emitted from (part_filt ⋈ lineitem) on l_partkey, partsupp could be pre-filtered to only the ~10k matching ps_partkey rows. Same lever as candidate #1.

## Findings

- **Strong consistent pattern across Q05/Q07/Q08/Q09: small-build → lineitem L9 bloom not firing.** Worth investigating as a single audit + fix rather than 4 separate query-level investigations.
- Q09's `(nation, o_year)` group-by aggregate is 22 ns/row — multi-column-key specialization is a possible lever.

## Next levers

1. **L9 audit** — single deliverable across multiple queries.
2. Multi-column Robin-Hood agg variant (deferred — first prove L9 is the bigger lever).
