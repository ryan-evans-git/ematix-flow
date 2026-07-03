# PERF_Q18 — Q18 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.18).
**2026-07-02:** the SF=10 sections below are partially historical — the
RobinHoodSumF64 plan they show was gated out by REV.18 (2026-05-31; Q18
now runs the stock two-phase agg at SF=10). For the SF=100 story, the
current source of truth is [§ SF=100 dig (2026-07-02)](#sf100-dig-2026-07-02)
at the bottom of this file.

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **243.70** | 6.49 |
| DuckDB | 229.81 | 4.58 |

**6% behind DuckDB** (was 3% — slight slip but still at-parity). Stage profile 5-trial: 250.29 ms.

## Per-stage decomposition

Σ compute 1737.68 ms / wall 250.29 ms = **6.94× parallelism = 50%**.

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| **HashJoinExec depth 7** (final outer-join with bloom) | needs probe analysis | 1191.31 | **dominant** — what is this doing? |
| EmatixFastParquetExec depth 9 (orders, ⋈ bloom: 15M → 624) | small | 342.13 | mild over |
| HashJoinExec depth 3 (final cust ⋈ orders+lineitem) | small probe | 143.00 | mild over |
| HashJoinExec depth 5 | small | 30.25 | at-floor ✓ |
| EmatixFastParquetExec depth 13 (lineitem main, 60M, no filter) | small (async) | 18.20 | sub-floor ✓ |
| RepartitionExec 60M | 4.77 | at-floor ✓ |
| FilterExec (sum > 300) | tiny | 3.35 | at-floor ✓ |
| EmatixFastParquetExec depth 5 (lineitem RH-side, 60M, no filter) | small (async) | 2.43 | sub-floor (RH path) ✓ |
| RobinHoodSumF64Exec Partial+Final (gby=l_orderkey, sum f64) | embedded inline; not counted | 0 (inlined) | confirms RH path ✓ |
| BuildSideBloomEmitterExec | tiny | 0 | confirmed firing ✓ |

Σ floor estimate ~700 ms; observed 1738 ms. **~1000 ms parallel over-floor (~150 ms wall).** Σ/6.94 = 250 ms = matches observed.

**The HashJoinExec at depth 7 dominates at 1191 ms parallel for only 624 output rows.** This must be the giant HashJoinExec that ingests the 60M lineitem rows on the outer side, joining against the RH-aggregate-derived order_ids. Build = filtered orders (15M→624 via bloom?), probe = lineitem 60M. So probe is huge × the small build → 60M × ~30 ns probe = 1800 ms parallel floor. Observed 1191 — sub-floor!

So actually the depth-7 join is at-floor for 60M probe against a small build. The "waste" is just the unavoidable lineitem-scan-then-probe cost.

## Findings

- **Q18 is at realistic-parallelism floor** for its plan shape. The 14 ms gap to DuckDB is small.
- **Σ.Q.L10 PushDownLeftSemiRule + L9 bloom + RobinHoodSumF64Exec all working as designed** — visible in plan as `RobinHoodSumF64Exec` (×2) and `BuildSideBloomEmitterExec`. These collectively reduce orders 15M → 624 rows before the outer lineitem join.
- **Remaining gap is the 60M lineitem-scan-then-probe** which is structurally inescapable without pushing the bloom into the EmatixFastParquetExec BridgeFilter (same Q17 lever — L9-to-scan integration).

**Next:** B.19 (Q19 — 138.72 ms, +34% vs DuckDB).

## Physical plan

LeftSemi pushdown is firing: lineitem sums-by-orderkey filter `sum>300` decorrelated into RightSemi, with BuildSideBloomEmitter wrapping it to narrow the outer lineitem read.

```
SortPreservingMergeExec [o_totalprice DESC, o_orderdate ASC]
  AggregateExec gby=[c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice] sum(l_quantity)
    HashJoinExec Partitioned Inner (o_orderkey, l_orderkey)         -- ← dominant
      HashJoinExec Partitioned Inner (c_custkey, o_custkey)
        customer
        HashJoinExec Partitioned RightSemi (l_orderkey, o_orderkey)  -- pushed-down semi
          BuildSideBloomEmitterExec target=orders.o_orderkey
            FilterExec sum > 300
              RobinHoodSumF64Exec FinalPartitioned gby=l_orderkey sum(l_quantity)  -- ← RH path
                RobinHoodSumF64Exec Partial
                  lineitem (full scan)
          orders                                                      -- 15M
      lineitem                                                       -- 60M, full scan again
```

## Per-stage breakdown (top 6)

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | HashJoinExec (orders+cust ⋈ lineitem main, on o_orderkey) | 1128.12 | 624 |
| 2 | EmatixFastParquetExec (lineitem main, no bloom on this side) | 338.76 | 624 |
| 3 | HashJoinExec (cust ⋈ orders+filter) | 140.64 | 4,368 |
| 4 | HashJoinExec RightSemi (sum-filter ⋈ orders) | 33.61 | 624 |
| 5 | EmatixFastParquetExec (lineitem #2, for the sum agg) | 26.16 | 59,986,052 |
| 6 | RepartitionExec | 4.75 | 59,986,052 |

Σ median compute: ~1680 ms. Wall median 248 ms. Parallel speedup ≈ 6.8×.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan #1 for sum agg (60M × 2 cols) | 6 |
| Hash agg (Partial+Final, l_orderkey 15M distinct, sum f64) [RobinHood] | 8 |
| Filter sum > 300 (most rows pass through) | <1 |
| orders scan (15M × 4 cols) | 5 |
| HashJoin RightSemi (filtered ⋈ orders) | 3 |
| customer scan (1.5M × 2 cols) | 1 |
| HashJoin cust ⋈ orders+filter | 2 |
| lineitem scan #2 for the final join (60M × 2 cols) | 6 |
| HashJoin (cust+orders+filter) ⋈ lineitem main (build small, probe 60M × 8 ns / 14) | 34 |
| Final agg + sort 624 rows | <1 |
| **Floor** | **~66 ms** |
| **Actual** | **248 ms** |
| **Waste ratio** | **3.8×** |

## Waste candidates

### 1. Lineitem main scan is unfiltered — no L9 from the (cust+orders+filter) build to lineitem

The plan does L9 on lineitem-#2 → orders (narrowing orders via the sum-filter bloom), but the **lineitem-#1 (the final-join probe)** is full-table 60M. The build side at this point is (cust ⋈ orders ⋈ sum-filtered) which has 4368 rows — a tiny build with a 60M probe. Textbook L9 case.

Memory [[sigma-q-l10-landed]] already closed the bigger gap (153% → 6%). The remaining 3% could be a **secondary** L9 pushing from the customer-orders-orderkey set into the lineitem main scan.

Expected impact: bloom narrows lineitem main from 60M decoded to ~30k rows. Wall: 248 → ~180 ms (~25% improvement, takes us 20% ahead of DuckDB).

### 2. The final HashJoin at 1128 ms compute = 100 ms wall

60M probe rows × 4368 build size → 60M × 8 ns / 14 = ~34 ms floor for the probe alone. We're at 100 ms wall — 3× over floor. Could be:
- Build hashing cost (4368 keys × ~10 ns each = 44 µs — trivial)
- Build dwell waiting for upstream to complete
- Memory pressure with concurrent lineitem scan #2

Same as candidate #1 — if lineitem-main probes fewer rows, this drops.

### 3. RobinHoodSumF64Exec firing correctly on the sum agg

Plan confirms `RobinHoodSumF64Exec` is being used for the `sum(l_quantity) gby l_orderkey` step. Memory [[sigma-nf3-beats-stock]] reports RH beats stock by 1-5% — this is doing its job.

## Findings

- **Q18 has a secondary L9 opportunity** on the final-join lineitem read. Build is 4368 rows (post-filter); probe is 60M lineitem. The existing L9 rule doesn't propagate the bloom through the LeftSemi-pushed structure to the OUTER lineitem read.
- This is structurally similar to Q17's gap — both queries scan lineitem twice and L9 fires on only one of the two scans.

## Next levers

1. (Cross-Q for Q17 + Q18) **Double-scan L9** — detect when the same large fact table is scanned twice in a plan and propagate the most-selective bloom to both. Single rule extension could close residual gaps on Q17 and Q18.

---

<a name="sf100-dig-2026-07-02"></a>
## SF=100 dig (2026-07-02) — the campaign's −1510 ms was a harness artifact; residual ~300 ms is the re-inserted input shuffle

Branch `perf/q18-sf100-dig`. Context: the 2026-07-01 campaign
(`bench-results/campaign-2026-07-01/REPORT.md`) called Q18 SF=100 the
dominant remaining loss — ematix 4087.79 ms vs DuckDB 2578.07 ms
(−1509.72, 2σ bar 647.76 — note the huge ematix variance), with no
lever touching it.

### Root cause: bench/production parity gap (inverse-Σ.V)

The strict harness binary (`tpch_triangulation_bench`, behind
`scripts/bench/strict_22q.sh`) builds its optimizer chain manually and
**never installed `ClusteredSinglePhaseAggRule` (RANGE.AGG)** — a
production-preset default since `f15d2fc` (2026-06-10), the very commit
whose subject line is "Q18 SF=100 flips to a win". That June win was
measured on the preset path (`tpch_preset_rebench` →
`preset::with_optimizer_rules`). The strict campaign therefore planned
Q18's inner subquery as the plan production users never run:

```
AggregateExec(FinalPartitioned, gby=l_orderkey)      -- 150M-group merge
  RepartitionExec(Hash [l_orderkey], 14)             -- 2.2 GB shuffle
    AggregateExec(Partial, gby=l_orderkey)           -- 150M-group table ×14
      lineitem (573 RGs, 600M rows)
```

= the 43.5s-CPU two-phase shape f15d2fc replaced. `tpch_validate` had
the same gap (the 22/22 value gate never exercised the single-phase
plan). Both are fixed on this branch, pinned by
`bench_preset_parity_tests` in the bench example. This is the mirror
image of the Σ.V alignment bug (2026-05-26: rules on in the bench,
missing from the preset) — same root disease: two hand-maintained
copies of the production rule chain. See "hardening" below.

### Measured (M4 Max 36 GB, warm cache, solo, plan cache off, 3 trials / 1 warmup)

| Arm | Q18 SF=100 wall | peak RSS |
|---|---:|---:|
| ematix, strict binary pre-fix (= campaign config) | 3457.02 ± 212.72 ms | 19.06 GB |
| ematix, strict binary + RANGE.AGG (this branch) | **2484.86 ± 73.51 ms** | 16.26 GB |
| DuckDB, same protocol | 2149.49 ± 31.82 ms | 7.56 GB |

−28% wall, −2.8 GB peak RSS, and the campaign's tell-tale variance
(σ 213 → 74; the 150M-group two-phase tables churn memory) collapses.
SF=10: RANGE.AGG declines via the skew gate (traced), plan and wall
unchanged (164.36 ± 2.66 ms this protocol). SF=1: declines;
`tpch_validate` 22/22 PASS with the rule installed.

### Residual ~300 ms: EnforceDistribution re-inserts the input shuffle

The fixed plan is single-phase but NOT shuffle-free:

```
AggregateExec(SinglePartitioned, gby=l_orderkey)
  RepartitionExec(Hash [l_orderkey], 14)     -- ← 600M rows / ~9.6 GB, still here
    EmatixFastParquetExec(14 key-disjoint chunks, 573 RGs)
```

`with_assignments` advertises `UnknownPartitioning`, and
`AggregateExec(SinglePartitioned)` requires hash distribution on the
group key, so the rule's own `EnforceDistribution` re-run inserts a
hash repartition of the full 600M-row scan output. The f15d2fc design
intent ("no shuffle — each chunk aggregates its own key range") is only
half-realized: we save the two-phase double-hashing (the bigger half,
measured above) but still pay the full-input exchange. DataFusion has
no way to express "range-disjoint on the group key" — and falsely
advertising `Partitioning::Hash` on the chunked scan would leak
upward (the agg maps input partitioning to its output), letting the
RightSemi join above elide ITS build-side repartition and mis-pair
hash-partitions with range-chunks → wrong results. Verified same
DataFusion (53.1.0) as at f15d2fc, so June's 2461 ms preset win was
also measured with this shuffle present.

### Proposed arc: RANGE.AGG Stage 2 — shuffle-free sandwich

Confine the partitioning claim strictly to the agg's input and cap it
above the agg so it cannot leak into join planning:

1. `with_assignments_claiming_hash(group_key)` — the chunked scan
   advertises `Partitioning::Hash([group_key], n)` (satisfies the
   SinglePartitioned requirement; row-correct for aggregation because
   chunks are key-disjoint — every group's rows land in exactly one
   partition, which is all HashPartitioned distribution promises a
   consumer).
2. Wrap the rewritten agg in a partitioning-reset pass-through
   (advertise `UnknownPartitioning(n)`, forward everything else) so
   EnforceDistribution re-satisfies any DOWNSTREAM hash requirement
   with a repartition of the agg's output — which for Q18 sits above
   the `HAVING sum > 300` filter: ~10k rows instead of 600M.
3. Plan-diff tests: (a) no RepartitionExec between the chunked scan
   and the SinglePartitioned agg; (b) a RepartitionExec IS present
   above the reset node when a partitioned join consumes it; (c) the
   existing boundary-span e2e stays exact. Value gate: tpch_validate
   22/22 at SF=1 + single-trial SF=100 value match.

Expected impact: the shuffle moves ~9.6 GB (600M × 16 B) through
14×14 exchange queues; eliminating it should be worth −200…−400 ms of
the −335 ms residual → Q18 SF=100 at parity-to-win vs DuckDB. Risk:
the partitioning claim interacting with EquivalenceProperties /
plan-cache keys — hence the reset-node cap and the plan-diff pins.
Effort: 1–2 days including a strict SF=100 A/B and an SF=10 22q
regression sweep.

### Stage 2 LANDED (2026-07-02, branch `perf/q18-range-agg-stage2`)

The sandwich shipped as designed, with one premise adaptation:

- `EmatixFastParquetExec::with_assignments_claiming_hash(chunks,
  [group_key])` — claims `Partitioning::Hash([k], n)`; equivalence
  properties rebuilt fresh (no facts derived from the claim).
- `PartitionClaimResetExec` (new node, `partition_claim_reset_exec.rs`)
  caps the claim directly above the rewritten SinglePartitioned agg:
  advertises `UnknownPartitioning(n)` + fresh equivalences, forwards
  streams/schema/statistics 1:1. Stateless → plan-cache-safe (the
  cache keys on canonicalised SQL, not plan nodes; per-node
  participation is only re-execute-safe `with_new_children`, which
  builds a fresh instance).
- `is_pass_through_node` in the L9 sideband rule now recognizes the
  reset node — otherwise the Q18 HAVING shape (FilterExec → reset →
  agg) loses its selective-build classification and the RightSemi
  build-side bloom emit declines.

**Divergence found vs the design:** DF 53.1's `add_hash_on_top`
re-inserts the hash repartition when `target_partitions > child
partition count` EVEN IF the child's partitioning satisfies the
requirement. So the shuffle stays eliminated only when the planner
achieves `chunks == target_partitions` — true at SF=100 (573 RGs,
dense strict gaps → 14 chunks / 14 targets), and self-correcting
otherwise (the re-inserted repartition genuinely re-hashes, so
sparse-gap files silently fall back to Stage 1 behavior, never to
wrong results).

Verified plan (SF=100, strict binary, EMAT_DUMP_PLAN of the executed
plan):

```
BuildSideBloomEmitterExec (RightSemi build side)
  RepartitionExec(Hash [l_orderkey], 14)     -- ← moved HERE: post-HAVING rows
    FilterExec sum > 300
      PartitionClaimResetExec partitions=14
        AggregateExec(SinglePartitioned, gby=l_orderkey)
          EmatixFastParquetExec(14 chunks, 573 RGs)   -- ← NO shuffle below
```

The orders side keeps its `RepartitionExec(Hash [o_orderkey], 14)` —
the claim did not leak. Leak pin: a lib test plans a Partitioned hash
join over the capped agg and asserts the agg-output repartition sits
above the reset node, the other side keeps its repartition, and the
join values are exact.

Value gates: `tpch_validate` 22/22 PASS at SF=1; Q18 SF=100 PASS vs
DuckDB (6398 rows value-match); rows identical with
`EMAT_RANGE_AGG=0`. Informal single trials (1 warmup, warm-ish cache,
NOT the strict protocol): Stage 2 at 2439/2297 ms vs Stage 1
(ec9e464 binary) at 4479/3151 ms — direction consistent with the
−200…−400 ms estimate; the strict SF=100 A/B and SF=10 22q sweep
remain to be run.

### Hardening (recommended, separate from this branch)

The three hand-maintained rule chains (preset, strict bench,
tpch_validate) have now produced two inverse alignment bugs. Extract a
single `preset`-driven constructor the harnesses call with their extra
opt-in knobs layered on top, or at minimum extend
`bench_preset_parity_tests` to assert name-set equality between the
bench session's ematix rules and the preset's.

### Re-baseline note

The campaign REPORT's Q18 SF=100 row (−1510 ms, and the Σ-medians it
feeds) should be re-measured under the strict protocol with the fixed
binary before it is quoted further; the pending strict-protocol
rebaseline covers this.

### Stage 2 strict verdict (2026-07-02, post-merge 90e6c7c)

Strict interleaved binary A/B (ec9e464 vs Stage-2 main, SF=100, isolate,
4 pairs × 10 trials): **−371.9 ms (−15.5%), clear WIN** (2400.4 →
2028.5 ms, 2σ bar 109.8). Fresh same-session DuckDB solo: 2384.5 ms.
**Q18 SF=100 flips to a clear ematix win (+356 ms ahead).**
Results: `bench-results/q18-stage2-2026-07-02/`. Note: an earlier A/B
accidentally compared the stale ec9e464 binary against itself (masked
cargo failure — the example needs `--features triangulation`); that run
is preserved evidence of the same-binary noise floor: Q18 SF=100 solo
invocations are bimodal (~2500/~2880 ms), so interleaved A/B is
mandatory for verdicts here.
