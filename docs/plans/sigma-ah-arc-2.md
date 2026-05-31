# Σ.AH.2 — L9 emitter Partitioned-mode extension

**Status:** drafted, not active
**Parent:** [`docs/PERF_REVIEW_2026_05.md`](../PERF_REVIEW_2026_05.md) (Phase C ranking #2 by aggregate impact)
**Hypothesis:** The current `BuildSideBloomEmitterExec` only wraps CollectLeft joins. Extending the emitter rule to Partitioned-mode joins where the build side is small enough to make a bloom worthwhile (probe/build ratio > 1024 — same threshold as current L9) will unlock cross-stage bloom propagation for the four queries with textbook small-build/large-probe Partitioned shapes that currently miss out.
**Queries impacted:** Q05 (part_filt→lineitem and supplier→lineitem edges), Q07 (potentially expanded nation chain), Q08 (part_filt 13k → lineitem 60M edge), Q09 (part_filt 108k → BOTH lineitem AND partsupp — compound cascade).
**Predicted impact range:** **~150-200 ms wall across 4 queries** = **4-6 pp SF=10 geomean**.
**Effort estimate:** 2-3 person-weeks (mechanism mirrors existing CollectLeft path; main work is in the partition-aware bloom merge).
**Risk level:** **M**.

## Bench gate (ship-if / reject-if)

### Microbench
- **Kernel:** existing `bloom::BloomBuilder::insert_int64_array` + `BloomFilter::contains` are reused. No new kernel — the change is rule-level + a partition-aware bloom merge.
- **Threshold:** Bloom merge cost (combining N partial blooms from N hash partitions into a single shared bloom) ≤ 1 ms wall on a 14-partition × 100k-rows-per-partition build. Reject if > 5 ms.

### Wall-time
- **Required:** Q08 SF=10 wall drop ≥ **30 ms** (189 → ≤ 159 ms) AND Q09 wall drop ≥ **50 ms** (273 → ≤ 223 ms) AND 22q SF=10 geomean improves by **≥ 3 pp** AND no single query regresses by **> 5%**.
- **Bonus:** Q05 wall drop ≥ 30 ms (186 → ≤ 156 ms).

### Reject-if
- The partition-aware merge serialises (e.g. requires a sync barrier across all 14 partitions before any probe-side scan starts) and the parallelism gain is lost. Per Σ.Q.L13 `[[sigma-q-l13-landed]]` precedent (parallel-bitmap dispatch caused 43× regression on a similar shape), aggressive parallel-aware levers can backfire spectacularly.
- A guard-clause regression on a query that *used to* fire CollectLeft L9 but no longer does (e.g. if the rule expansion accidentally drops an existing fire).
- Microbench bloom merge > 5 ms wall.

## Hard constraints (inherited)

- **No new PhysicalOptimizerRule** — extend `EnableRuntimeBloomSidebandRule` (the existing rule already lives in this code path). Per `[[optimizer-codegen-sensitivity]]`, adding a new rule costs ~7% geomean before doing any work.
- **Pattern-recognition pre-plan walker** — the rule walks the optimised plan tree looking for HashJoinExec nodes; add Partitioned-mode predicate.
- **TDD** per `[[feedback-tdd]]`.
- **No TPC-H-specific hardcoding** — must work on any small-build Partitioned Inner-equijoin.

## Story skeleton (no tasks)

- **Story 1 — partition-aware bloom merge.** When a Partitioned-mode build runs across N hash partitions, each partition independently builds a partial bloom. The emitter must produce a single shared bloom (union of all partials) before any probe-side scan can consume it. Decide: synchronous merge at the end of build-side, or lock-free union as builds complete. Correctness tests: union of N partial blooms = bloom-of-union-of-N-inputs.
- **Story 2 — rule extension.** `EnableRuntimeBloomSidebandRule` currently fires on CollectLeft HashJoinExec; extend pattern to Partitioned HashJoinExec where `probe_rows / build_rows ≥ EMAT_RT_BLOOM_RATIO`. Sidebands per partition or one shared sideband — decision in Story 1.
- **Story 3 — wall-time bench gate + opt-in flag.** Ship behind `EMAT_L9_PARTITIONED=1`. Land Story 2 and run the 22q SF=10 bench. Verify per-query no-regression bar AND geomean gate.
- **Story 4 — cascade verification with Σ.AH.1.** If Σ.AH.1 has landed (or in parallel), test the cascade: Q09 partsupp build should drop from 128 MB DRAM-bound to ~1.7 MB L1-resident.
- **Story 5 — soak + default-on flip.** Run 22q SF=10 across 3 back-to-back trials. If stable, flip the env var default per Σ.AG.7 precedent (opt-out via `=0`).

## Risks + watch-items

- **Bloom merge serialisation.** This is the same risk as Σ.AH.1's plan-time sync race. The partition-aware merge could either (a) be a sync barrier (kills parallelism — see Σ.Q.L13 regression), or (b) be eventually-consistent (some probe-side scans miss part of the bloom but eventually get it). (b) is the safer choice.
- **Q07 nation→customer L9 currently works.** The rule expansion must not break this case. Story 2 explicitly tests Q07's existing pass-rate hold.
- **Q03 cust+orders → lineitem looks like a candidate but the build side (1.46M = 36 MB) may exceed the "small build" threshold.** Memory `[[sigma-q-l9-bloom-consumer-findings]]` notes bloom-on-FK is net-negative when the build is large. The 1024 ratio gate should exclude this; verify explicitly that Q03 doesn't get a new (regressing) bloom.
- **Compound cascade with Σ.AH.1.** Q09 sees the biggest impact only if both arcs are live. Solo Σ.AH.2 still helps Q08 (~50 ms) via operator-level bloom; the bigger cascade win waits for Σ.AH.1.
- **Codegen tax even from rule extension.** Even extending an existing rule's pattern match adds branches. Watch the geomean baseline closely during Story 2; the bloom-not-firing case should not regress.

## References

- Phase C ranking entry: [`docs/PERF_REVIEW_2026_05.md`](../PERF_REVIEW_2026_05.md) Σ.AH.2 section
- Existing CollectLeft L9 implementation: memory `[[sigma-q-l9-landed]]`
- Selective-build gate (already applied): memory `[[sigma-q-l13-to-l16-session]]` mentions `EMAT_L9_REQUIRE_FILTERED_BUILD=1`
- Bloom-on-FK net-negative precedent: memory `[[sigma-q-l9-bloom-consumer-findings]]`
- Parallel-bitmap dispatch backfire precedent: memory `[[sigma-q-l13-landed]]`
- Per-query evidence: [`docs/PERF_Q05.md`](../PERF_Q05.md), [`docs/PERF_Q08.md`](../PERF_Q08.md), [`docs/PERF_Q09.md`](../PERF_Q09.md)
- Related rejection (re-look flag): `[[sigma-sb-cascade-neg]]` — re-look only AFTER this lands
