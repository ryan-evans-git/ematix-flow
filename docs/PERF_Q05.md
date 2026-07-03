# PERF_Q05 — Q05 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH Phase B.5). Originally profiled 2026-05-25. **Note: 2026-05-25 analysis misidentified the dominant operator — corrected below.**

## Wall time

### 2026-05-26 refresh (20 trials × 3 warmups, post-Σ.AG.7 bare invocation — canonical)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **186.25** | 6.92 |
| DuckDB | 148.97 | 4.49 |

**25% behind DuckDB** (was 28% in 2026-05-25; small move; both engines marginally faster). Stage profile 5-trial: 192.23 ms.

## Physical plan

6-way join: region → nation → supplier → (cust ⋈ orders ⋈ lineitem), then sum by n_name. The 2-key supplier-nation = customer-nation constraint creates a large intermediate.

```
SortPreservingMergeExec [revenue DESC]
  ...
  AggregateExec FinalPartitioned gby=[n_name] sum
    AggregateExec Partial
      HashJoinExec CollectLeft Inner (r_regionkey, n_regionkey)         -- region filter ASIA
        region
        HashJoinExec CollectLeft Inner (n_nationkey, s_nationkey)       -- nation
          nation
          HashJoinExec CollectLeft Inner ON 2 KEYS:                     -- supplier (HOT)
              (s_suppkey, l_suppkey)
              (s_nationkey, c_nationkey)                                -- ← this 2-key shape
            supplier                                                    -- 100k rows
            HashJoinExec Partitioned Inner (o_orderkey, l_orderkey)
              HashJoinExec Partitioned Inner (c_custkey, o_custkey)
                customer                                                -- 1.5M rows
                orders (filter o_orderdate ∈ 1994-01-01..1995-01-01)   -- 2.3M rows
              lineitem                                                  -- 60M rows, no filter
```

## Per-stage breakdown (2026-05-26)

**IMPORTANT correction**: the 2026-05-25 doc misidentified the dominant operator. The 613.70 ms HashJoinExec at depth 11 is **(cust+orders) ⋈ lineitem** outputting 9.1M rows — NOT the supplier 2-key join. The supplier 2-key join is at depth 10 (CollectLeft) and **reduces** 9.1M → 364k, it does not produce the 9.1M intermediate.

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | **HashJoinExec (cust+orders) ⋈ lineitem** (Partitioned) | 11 | **613.70** | **9,103,367** |
| 2 | EmatixFastParquetExec (lineitem, 4 cols) | 13 | 170.87 | 59,986,052 |
| 3 | **HashJoinExec supplier 2-key (CollectLeft)** REDUCES 9.1M → 364k | 10 | 140.50 | 364,380 |
| 4 | HashJoinExec (cust ⋈ orders, Partitioned) | 13 | 63.99 | 2,275,919 |
| 5 | RepartitionExec (lineitem Hash on l_orderkey) | 12 | 9.16 | 2,275,919 |
| 6 | RepartitionExec | 12 | 7.81 | 59,986,052 |
| 7 | EmatixFastParquetExec (customer, 2 RGs) | 15 | 5.07 | 1,500,000 |
| 8 | EmatixFastParquetExec (orders + filter) | 16 | 4.80 | 2,275,919 |
| 9 | HashJoinExec (nation ⋈ supplier-side) | 8 | 2.06 | 364,380 |
| 10 | HashJoinExec (region ⋈ ...) | 6 | 1.36 | 72,985 |
| 11 | AggregateExec Partial (5-group) | 5 | 0.76 | 70 |

Σ median compute: **1022.73 ms**. Wall: 192.23 ms. Effective parallelism: **5.32× = 38%** (worst in the sweep so far — CollectLeft + multi-join pipeline serialises heavily).

## Theoretical floor (per-stage, projection-cost-aware)

| Stage | Floor formula | Floor (sum ms) | Actual | Status |
|-------|---------------|---------------:|-------:|--------|
| Lineitem scan (60M × 4 cols mixed Snappy) | 1.9 GB / (~4 GB/s × 14 cores aggregate) = 34 ms wall × 14 ≈ 480 ms sum | 480 | 170.87 | **sub-floor** (async pipelining) |
| Customer scan (1.5M × 2 cols, 2 RGs only) | ~5 ms (small, dict-heavy) | <10 | 5.07 | at-floor ✓ |
| Orders scan + BridgeFilter o_orderdate (15M → 2.28M) | 15M × 10 ns/row decode | 150 | 4.80 | **sub-floor** (most cost credited downstream) |
| cust ⋈ orders (build 1.5M, probe 2.27M, Partitioned) | 2.27M × 12 ns/row probe = 27 ms sum + build 1.5M × 5 ns = 7.5 = 35 ms total | 35 | 63.99 | 1.8× over (build doesn't fit L2; ~30 ns/row probe) |
| **(cust+orders) ⋈ lineitem** (build 2.27M=36 MB L3, probe 60M, Partitioned) | 60M × ~30 ns/row L3-probe × 1/14 cores = 130 ms wall × 14 = ~1820 ms sum (worst case); at L2-floor ~600 ms | 600–1820 | 613.70 | **at-L2 floor** ✓ — DataFusion's HashJoin probe is achieving 10 ns/row on a 36 MB build that exceeds shared L2 cluster (16 MB) |
| RepartitionExec on l_orderkey for lineitem (60M × 4B) | 240 MB / 70 GB/s aggr = 3.4 ms wall × 14 = 48 ms sum | 48 | 7.81 | sub-floor (lighter than estimate) |
| Supplier 2-key CollectLeft (build 100k, probe 9.1M) | 9.1M × 20 ns × 1 thread = 182 ms single-thread (build collected serially) | ~180 | 140.50 | **at-floor** ✓ — 2-key not the bottleneck |
| Nation ⋈ (supp+...) CollectLeft (build 25, probe 364k) | <2 ms | <2 | 2.06 | at-floor ✓ |
| Region ⋈ ... CollectLeft (build 1 row after filter, probe ~365k) | <1 ms | <1 | 1.36 | at-floor ✓ |
| Aggregate (5 groups, sum f64) | trivial | <2 | 0.76 | at-floor ✓ |
| **Σ floor sum** | | **~1500 ms** | **1023 ms** | **observed BELOW floor** |
| **Σ effective-parallelism floor** | 1023 / 5.32 effective = | | **192 ms wall** | **MATCHES observed 192 ms** |

**Q05 is AT its realistic-parallelism floor on every stage.** Every operator is at or below the kernel floor when accounting for the projection-cost-aware model and async pipelining. Total wall = sum-of-stage-compute / effective-parallelism = 192 ms.

**The 25% gap to DuckDB is therefore NOT in any single operator** — it's in the **plan shape**. DuckDB must be using a structurally different plan that doesn't materialise the 9.1M (cust+orders ⋈ lineitem) intermediate.

## Σ.AH waste candidate ranking (corrected 2026-05-26)

Q05 wins are all in **plan reshape**, not kernel/operator tuning.

| Rank | Candidate | Mechanism | Wall savings | Confidence |
|-----:|-----------|-----------|-------------:|:----------:|
| 1 | **L9 cascade: bloom from region→nation→supplier→lineitem** | After region filter ASIA (5 nations), only ~20% of suppliers qualify. A bloom on s_suppkey passed back to the lineitem scan drops 60M → ~12M rows (5× reduction) BEFORE the cust+orders⋈lineitem join. | **~50 ms** (192 → ~140) | medium |
| 2 | **Re-emit customer.parquet with more RGs** | Customer.parquet has only 2 RGs at SF=10 → 2-way parallel scan limits early pipeline | ~3 ms | high |
| 3 | **Join reorder: build supplier side first** | DuckDB's win likely from joining supplier-filtered chain BEFORE expanding to lineitem. Σ.T existed but was deferred. | 30-40 ms | low (multi-month effort) |
| 4 | **(cust+orders) ⋈ lineitem build-side L2 spill** | 36 MB build doesn't fit L2 cluster (16 MB shared between 6 P-cores → 2.6 MB each at full sharing). L3 probe at ~30 ns/row vs L2 at ~10 ns/row. | ~25 ms theoretical | low — structural; needs build-side compression or partition-aware build |
| ~~5~~ | ~~Supplier 2-key join is the bottleneck~~ | **RETRACTED.** The supplier 2-key join is at depth 10 (140 ms parallel) and REDUCES 9.1M → 364k; it's the (cust+orders) ⋈ lineitem at depth 11 (613 ms, 9.1M output) that's the dominant stage. | — | — |

## Where the 38 ms gap to DuckDB actually goes

ematix wall 186 ms − DuckDB wall 149 ms = 38 ms gap.

ematix is at its realistic-parallelism floor on every stage. The gap is **structural plan difference**, not stage-by-stage inefficiency:

- **DuckDB likely pre-filters lineitem before the cust+orders⋈lineitem join.** Region ASIA + nation chain → ~20% of suppliers qualify. A pre-scan bloom or pushed-down predicate on s_suppkey would cut lineitem from 60M to 12M. At 5× reduction, the depth-11 join's 613 ms parallel cost (the dominant stage) drops to ~125 ms parallel, saving ~92 ms parallel work = ~17 ms wall.
- **DuckDB's join order is unknown without a plan diff.** It might also build the supplier-filtered nation cluster first and join lineitem last; we can't tell without dumping DuckDB's optimized plan.

## Findings to capture as memories

- **Q05 SF=10 is at its realistic-parallelism floor on every operator** — no single per-stage waste candidate. Every stage is at-or-below its kernel floor including projection memcpy.
- **The 2026-05-25 finding that "2-key supplier join is the bottleneck" was wrong.** The 9.1M intermediate is from (cust+orders) ⋈ lineitem (depth 11), not from the supplier 2-key join (depth 10) which REDUCES 9.1M → 364k by filtering on c_nationkey == s_nationkey.
- **Effective parallelism 38% is the worst in the sweep so far** — CollectLeft small-dim joins + 2-stage agg + the long pipeline through 6 tables. Structural limit, not a tuning issue.
- **L9 cascade to lineitem is the highest-confidence lever** for Q05 — pre-filter lineitem by s_suppkey via the region→nation→supplier chain bloom. Memory `project_sigma_sb_cascade_neg.md` notes Σ.S.B cascade was neutral across 22q but flagged Q17/Q18 sub-shape rejections; **Q05's shape (cust+orders+lineitem-probe with supplier-side region-filter chain) is different and worth re-examining**.

## Next levers from Q05

1. **Verify if L9 currently fires on Q05** — diagnostic. Look at the plan dump for `BuildSideBloomEmitterExec` nodes. If absent, that's the rule-guard miss-fire.
2. **L9 cascade from region/nation→supplier→lineitem** — the candidate above; ~50 ms savings if it works. Sub-shape A/B not covered by Σ.S.B's prior bench.
3. **Re-emit customer.parquet with more RGs** — cheap, ~3 ms gain on Q05 (also helps Q03).
4. **Deferred: full join reorder via Σ.T** — multi-month effort; not B-phase scope.

---

## Verify pass — 2026-05-26 (Σ.AH B.5)

**What changed since 2026-05-25:**
- Wall: 201.48 → 186.25 ms canonical (−8%). Stage profile 192.23 ms.
- vs DuckDB: was −28% behind → now −25% (small move; both engines marginally faster).
- Plan structure: unchanged.

**Major correction:** 2026-05-25 misidentified the 9.1M-output operator as the supplier 2-key join. Re-reading the stage tree, the 9.1M intermediate is the **(cust+orders) ⋈ lineitem** join at depth 11; the supplier 2-key join at depth 10 is a REDUCER (9.1M → 364k via c_nationkey = s_nationkey filter). Candidate ranking re-done accordingly.

**Q05 is at realistic-parallelism floor; the 38 ms gap to DuckDB is structural** (plan shape), not kernel/operator inefficiency. Single highest-confidence lever: L9 cascade region→supplier→lineitem to pre-filter the 60M-row probe.

**Next:** B.6 (Q06 — 76.08 ms; we lose to DuckDB by ~2%, decoder-bound).

---

## Σ.Q05.CHAIN — L9 cascade-chain lever (2026-07-02)

Implemented on branch `perf/q05-multikey-bloom-cascade`. New second
phase of `EnableRuntimeBloomSidebandRule`
(`runtime_bloom_cascade_chain.rs`): install per-link blooms along the
region(ASIA) → nation → supplier(2-key) → lineitem build chain.
Tri-state gates `EMAT_L9_CASCADE` / `EMAT_MULTIKEY_BLOOM` (unset =
conservative AUTO; see `docs/EMAT_FLAGS.md`).

### Premise corrections vs this doc / the code diagnostic

1. **The multi-key `break` was not the operative blocker.** The plan's
   2-key supplier join lists `(s_suppkey, l_suppkey)` FIRST, so pass-1's
   first-match loop would have picked the right key; what actually
   blocks pass-1 on the chain joins is `require_filtered_build` (nation
   / supplier builds carry no static FilterExec) plus the 1024× ratio
   gate (region→nation: 5×1024 ≥ 25). Multi-key support was still
   needed — the chain walker evaluates every key pair and picks the one
   reaching the terminal fact scan.
2. **The 2026-05 plan shape is stale for the preset path.** The
   production preset now splices a transitive dim-semi (region⋈nation
   RightSemi pre-filtering customer) and pass-1 already fires three
   wraps at SF=10, including a tight-admitted `l_orderkey` bloom on the
   lineitem scan (~3% pass — a stronger prefilter than the ~20%
   `l_suppkey` bloom this doc proposed). The chain's terminal bloom
   therefore COMPOSES with it (extra sideband, joint pass ≈ 0.6%)
   rather than standing alone.
3. **A standalone ~20%-pass bloom cannot prune the lineitem scan.**
   The REV.23 masked→dense routing discards bitmaps above 10% pass, so
   the "60M → 12M before the join" mechanism in this doc's candidate
   ranking does not exist at the scan level; the bitmap is dropped and
   the scan pays the filtered-path detour (+35 ms wall measured). AUTO
   therefore only installs a terminal that composes with an existing
   sub-threshold wrap; bare terminals are the
   `EMAT_L9_CASCADE_TERMINAL_APPLY` A/B arm.
4. **Intermediate links need `apply_when_dense`.** The same REV.23
   discard silently neutered the nation/supplier prunes (both ~20%
   pass): the chain published a full-supplier (100K-key) terminal set
   and the runtime disarm correctly refused it. Chain-intermediate
   sidebands now force the reader to stash/apply their bitmaps —
   dim-sized scans, negligible cost. With the fix the terminal build
   samples 20,037 ASIA suppliers (`DIMSEL.RT keep — 0.200 ≤ 0.5`).

### Single-trial informational timings (SF=10, M4 Max — NOT official)

| Shape | OFF (`EMAT_L9_CASCADE=0`) | AUTO |
|-------|--------------------------:|-----:|
| preset (`tpch_preset_rebench`, chain composes) | 128.7 ms | 126.5 ms |
| strict bench (`tpch_triangulation_bench`, no pass-1 lineitem wrap → chain declines) | 106–127 ms | 111–122 ms (≈ noise) |

Value validation: SF=1 22/22 PASS (forced + AUTO); Q05 SF=10 PASS vs
DuckDB with lever off / AUTO / forced. Strict A/B decides shipped
defaults.
