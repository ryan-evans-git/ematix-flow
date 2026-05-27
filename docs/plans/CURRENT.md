# Σ.AH.2 — L9 emitter Partitioned-mode extension

**Status:** active
**Created:** 2026-05-26 (promoted from Σ.AH Phase D arc shell)
**Active phase:** Pre-work (Σ.AH.4 chore) → Story 1
**Active story:** Σ.AH.4 pre-work (partition-count generalization — code-only), then Story 1 (partition-aware bloom merge)
**Branch policy:** local commits only on this plan. PR only after Story 4 wall-time gate passes.
**Predecessor plan:** [`docs/plans/archive/2026-05-26-sigma-ah-survey.md`](./archive/2026-05-26-sigma-ah-survey.md) — Σ.AH survey (closed 2026-05-26 after Phase D).
**Sibling arc shells (drafted, not active):** [Σ.AH.1](sigma-ah-arc-1.md), [Σ.AH.3](sigma-ah-arc-3.md).

---

## Hypothesis

The current `BuildSideBloomEmitterExec` only wraps **CollectLeft** joins (small-build broadcast joins). Four queries in the 22q SF=10 suite show textbook small-build / large-probe **Partitioned-mode** Inner joins that currently miss the L9 bloom optimisation:

| Query | Build (Partitioned) | Probe (Partitioned) | Probe/build ratio | Currently fires L9? |
|-------|---------------------|---------------------|------------------:|---------------------|
| Q05 | part_filt → ~100k rows | lineitem 60M | 600× | ❌ |
| Q07 | nation+cust merge → 120k | lineitem 18M | 150× | ❌ (operator level only on nation→customer edge) |
| Q08 | part_filt 13k | lineitem 60M | 4600× | ❌ |
| Q09 | part_filt 108k | lineitem 60M AND partsupp 8M | 555× / 74× | ❌ (compound cascade target) |

All four clear the existing 1024 threshold easily but the rule's pattern-match excludes Partitioned mode. Extending the rule will unlock the bloom in these shapes.

**Cascade with Σ.AH.1 (L9 scan-level integration):** Σ.AH.2 alone delivers operator-level bloom filtering on these Partitioned joins. The bigger payoff comes when Σ.AH.1 also lands and the bloom filters at decode time. Q09 is the canonical cascade target — the bloom can drop partsupp 8M → ~108k pre-build, shrinking its 2-key 128 MB DRAM-bound build to ~1.7 MB L1-resident.

## Impact estimate

| Source | Wall savings | Confidence |
|--------|-------------:|:----------:|
| Q05 part_filt → lineitem | ~30 ms | medium |
| Q07 expanded chain | ~20 ms | low (Q07 already at-floor; partial cascade) |
| Q08 part_filt → lineitem | ~50-80 ms | medium |
| Q09 part_filt → lineitem AND partsupp | ~50 ms (without AH.1) / ~80 ms (with AH.1 cascade) | medium |
| **Solo total** | **~150-180 ms wall** | medium |
| **With Σ.AH.1 cascade** | **~200 ms wall** | medium |

**Geomean target: ≥ −3 pp at 22q SF=10** (Σ.AH.2 alone), additional −2-3 pp when Σ.AH.1 lands too.

## Effort estimate

**2-3 person-weeks.** Most of the new code is the partition-aware bloom merge (Story 1). The rule extension (Story 2) is small. The bench gate + soak (Stories 3-5) takes the bulk of calendar time.

## Risk level

**Medium.** Mechanism mirrors existing CollectLeft path so kernel risk is low. Main risks are (1) partition-aware merge serialisation (could backfire like Σ.Q.L13's parallel-bitmap dispatch did — memory `[[sigma-q-l13-landed]]`) and (2) accidentally regressing Q07's existing CollectLeft nation→customer L9 fire.

---

## Bench gate (ship-if / reject-if)

### Microbench (Story 1 gate)

**Kernel:** partition-aware bloom merge — combining N partial blooms from N hash partitions into one shared bloom.

**Threshold:** ≤ 1 ms wall to merge 14 × 100k-row partials. **Reject if > 5 ms.** Per `[[optimizer-codegen-sensitivity]]` precedent — kernel ops on the critical path of every join must be sub-ms.

### Wall-time (Story 3 gate)

**Required (all must pass):**
- Q08 SF=10 wall drop ≥ **30 ms** (189 → ≤ 159 ms)
- Q09 SF=10 wall drop ≥ **50 ms** (273 → ≤ 223 ms)
- 22q SF=10 geomean improves ≥ **3 pp**
- No single query regresses > **5%** (per Σ.O.c.2 noise band audit)

**Bonus:**
- Q05 SF=10 wall drop ≥ 30 ms (186 → ≤ 156 ms)

### Reject-if (any of the following)

- Microbench bloom merge > 5 ms wall (Story 1 gate)
- Any query regresses > 5% on canonical 20-trial bench
- Q07's existing CollectLeft nation→customer L9 pass-rate degrades (regression check, not just wall)
- Partition-aware merge introduces a sync barrier that serialises 14-partition probe-side scans (same pattern as Σ.Q.L13 backfire)

Per `[[sigma-r2-rejected]]` precedent: **microbench pass + wall-time fail = reject.** No exceptions.

---

## Hard constraints (inherited)

1. **Local commits only.** No PRs until Story 4 wall-time gate clears.
2. **No new PhysicalOptimizerRule** (`[[optimizer-codegen-sensitivity]]`) — extend `EnableRuntimeBloomSidebandRule`.
3. **TDD** per `[[feedback-tdd]]` — bloom-merge correctness tests precede the merge implementation.
4. **No TPC-H-specific hardcoding** per `[[feedback-no-tpch-hardcoding]]` — the rule fires on any small-build Partitioned Inner-equijoin.
5. **Bench-gate every story exit.** Microbench at Story 1; wall-time at Story 3; soak at Story 5.

---

## Pre-work — Σ.AH.4 chore (partition-count generalization, code-only)

**Status:** DONE 2026-05-26 (code-only — original data-prep scope deferred to preserve captured baseline).
**Effort:** ~30 minutes actual.
**Expected impact:** **zero measurable wall change on the canonical M3 Pro box** — `available_parallelism()` returns the same 14 as the previous hardcode. Pure portability/maintenance commit.
**Risk:** none.

**Scope change (2026-05-26):** the original chore re-emitted `customer.parquet` from 2 RGs → N RGs to lift partition-count-aware parallelism. While probing this, we observed the data-prep would invalidate every wall-time number measured across Phase A-D (all 22 PERF_Q\*.md docs reference the original Snappy/2-RG customer.parquet). Decision: **revert the parquet file to its pristine state**; keep only the code-level partition-count generalization, which has no perf impact on the bench box but lifts a portability constraint.

### Tasks (revised)

- [x] Inspect current `examples/tpch/data/sf10/customer.parquet` row-group layout (2 RGs, 1048576 + 451424, parquet-rs 58.1.0, Snappy). Confirmed via pyarrow.
- [x] **Reverted** any re-emit; restored from `customer.parquet.bak` to keep baseline measurement valid.
- [x] Drop hardcoded `target_partitions(14)` from `tpch_validate.rs` (was the only example with no env override). Replace with `PARTITIONS` env override + `std::thread::available_parallelism()` default.
- [x] Change `tpch_triangulation_bench.rs` and `stage_profiler.rs` defaults from `14` to `available_parallelism()` (env override unchanged).
- [x] Update stale "`target_partitions=14`" string in bench's results-doc footer.
- [x] Smoke test: `tpch_validate` SF=10 passes byte-identical against the untouched baseline data.
- [x] Commit (this commit).

**Exit criteria:** all 22 SF=10 queries pass `tpch_validate` row-by-row + value-by-value (no perf gate — there's no data change to measure).

**Then:** Story 1 (partition-aware bloom merge).

---

## Story 1 — partition-aware bloom merge [PENDING pre-work]

**Status:** `[ ]` (gated on pre-work)
**Time budget:** 3-4 days.

**Goal:** when a Partitioned-mode build runs across N (= 14) hash partitions, each partition independently inserts into a partial bloom; the emitter merges all 14 partials into a single shared `BloomFilter` before any probe-side scan can consume it.

**Open design question (decide in Story 1):** **synchronous merge at end-of-build** vs **lock-free union as builds complete**. Synchronous is simpler but introduces a sync barrier (same pattern as Σ.Q.L13 backfire). Lock-free is safer for parallelism but harder to write correctly. **Default: lock-free with bitwise-OR per 64-bit word**, since `BloomFilter` is `split-block` and per-block bitwise-OR is correct without locks.

### Tasks

- [ ] **Story 1.0 — design doc.** Pick sync vs lock-free merge. Document the choice in `docs/PHASE_SIGMA_AH_2_DESIGN.md` (1-2 pages). Include correctness proof: `BloomFilter::union(a, b) == BloomFilter::insert_all(a.items ∪ b.items)`.
- [ ] **Story 1.1 — `BloomFilter::union` kernel + correctness tests.** Add a `pub fn union(&self, other: &BloomFilter) -> BloomFilter` (or in-place `merge_into`) to `bloom.rs`. Property-based test: insert random items into A, B; verify `union(A, B).contains(x) == (A.contains(x) || B.contains(x))` for all `x` (true positives) AND FPR ≤ 2× single-bloom FPR.
- [ ] **Story 1.2 — microbench: merge cost on 14 × 100k-item partials.** Add `bench_bloom_merge` example. Microbench threshold: ≤ 1 ms wall to merge 14 partials of 100k items each (10 bits/key sizing).
- [ ] **Story 1.3 — partition-aware emitter.** Extend `BuildSideBloomEmitterExec` to accept a `Vec<Arc<Mutex<BloomFilter>>>` (one per partition) instead of a single `Arc<RuntimeBloomSideband>`. On partition close, the partition's bloom is union-merged into the shared sideband. Add a `pending_partitions: AtomicUsize` counter; consumers poll until it hits 0 OR consume incrementally if lock-free.
- [ ] **Story 1.4 — re-test existing CollectLeft path.** The CollectLeft path uses N=1, so the partition-aware emitter must reduce to the single-partition case without overhead. Add a test that exercises this regression case.

**Exit criteria:**
- [ ] `BloomFilter::union` ships with correctness tests; FPR audit confirms ≤ 2× single-bloom.
- [ ] Microbench `bench_bloom_merge` shows ≤ 1 ms wall on 14 × 100k.
- [ ] Existing CollectLeft L9 unit tests still pass byte-identically.
- [ ] Design doc captures the sync-vs-lock-free decision + rationale.

**Next:** Story 2 (rule extension).

---

## Story 2 — rule extension to Partitioned-mode [PENDING Story 1]

**Status:** `[ ]`
**Time budget:** 2-3 days.

**Goal:** `EnableRuntimeBloomSidebandRule` currently fires on `HashJoinExec(mode = CollectLeft)`. Extend the pattern to also fire on `HashJoinExec(mode = Partitioned)` when `probe_rows / build_rows ≥ ratio` (using `EMAT_RT_BLOOM_RATIO` env, default 1024 — same threshold as CollectLeft).

### Tasks

- [ ] **Story 2.1 — pattern match + cardinality estimate.** Extend the rule's `optimize` walk to recognise Partitioned-mode joins. Use `partition_statistics` from `EmatixFastParquetTableProvider` (memory `[[sigma-ae-complete]]`) for post-filter row counts.
- [ ] **Story 2.2 — wire the partition-aware emitter from Story 1.3.** For each qualifying Partitioned join, wrap the build with the partition-aware `BuildSideBloomEmitterExec`. Target the probe-side scan via the same `RuntimeBloomSideband` mechanism.
- [ ] **Story 2.3 — guard against regression on existing CollectLeft fires.** Q07's nation → customer L9 must continue firing identically. Add a non-regression test that verifies the same plan is produced for Q07's nation join post-rule-extension.
- [ ] **Story 2.4 — opt-in flag wiring.** Land behind `EMAT_L9_PARTITIONED=1`. Default OFF until Story 3 wall-time gate clears.

**Exit criteria:**
- [ ] Q05/Q07/Q08/Q09 plan dumps show `BuildSideBloomEmitterExec` on the Partitioned join edges (verified via plan-explain text comparison).
- [ ] Q01-Q22 row counts byte-identical to pre-rule baseline (correctness suite).
- [ ] CollectLeft fires unchanged.

**Next:** Story 3 (wall-time bench gate).

---

## Story 3 — wall-time bench gate [PENDING Story 2]

**Status:** `[ ]`
**Time budget:** 1 day (bench + analysis).

### Tasks

- [ ] Run `tpch_triangulation_bench` SF=10 with `EMAT_L9_PARTITIONED=1` set, 20 trials × 3 warmups (canonical config). Compare against the post-Σ.AH.4 baseline.
- [ ] Verify gates per the Bench gate section above: Q08 ≥ 30 ms drop, Q09 ≥ 50 ms drop, geomean ≥ 3 pp, no query > 5% regression.
- [ ] Run with `EMAT_L9_PARTITIONED=0` immediately after for A/B; confirm baseline unchanged.
- [ ] If Story 3 fails: per `[[sigma-r2-rejected]]` precedent, REJECT and capture findings in a memory entry. Do NOT proceed to Story 4.

**Exit criteria:**
- [ ] Bench gate clears OR a clean rejection memory is filed.

**Next:** Story 4 (cascade verification with AH.1 if it's live, else skip to Story 5).

---

## Story 4 — cascade verification with Σ.AH.1 [PENDING Story 3]

**Status:** `[ ]` (conditional on Σ.AH.1 being live; skip if not)
**Time budget:** 0.5 day.

**Goal:** if Σ.AH.1 (L9 scan-level integration) is also live by the time Story 3 lands, verify the cascade: Q09's partsupp 2-key build should drop from 128 MB DRAM-bound to ~1.7 MB L1-resident.

### Tasks

- [ ] Stage profile Q09 with both `EMAT_L9_PARTITIONED=1` AND Σ.AH.1's flag (whatever it ends up being).
- [ ] Verify partsupp 2-key join build size measured via `stage_profiler` output drops to ≤ 5 MB.
- [ ] Verify Q09 wall drops ≥ 80 ms (vs the solo Σ.AH.2 number from Story 3).
- [ ] Document the cascade impact in `docs/PERF_REVIEW_2026_05.md` (append a "Cascade results" section).

**Exit criteria:**
- [ ] Cascade impact measured and documented OR explicit "AH.1 not yet live, Story 4 skipped" note.

**Next:** Story 5 (default-on flip).

---

## Story 5 — soak + default-on flip [PENDING Story 3 (or 4)]

**Status:** `[ ]`
**Time budget:** 24-hour soak + 1 day to ship the flip.

### Tasks

- [ ] Run 3 back-to-back full bench runs across 24 hours to confirm no drift. Per `[[sigma-q-l13-to-l16-session]]` precedent — multi-run stability is the bar for default-on.
- [ ] Update `tpch_triangulation_bench.rs` to flip the default for `EMAT_L9_PARTITIONED` from OFF → ON (env becomes opt-OUT via `=0`).
- [ ] Update `feedback_full_bench_env_checklist.md` memory entry to reflect the new default.
- [ ] Update `BENCHMARKS.md` with the new numbers.
- [ ] Final commit + tag the new milestone bench result.

**Exit criteria:**
- [ ] Σ.AH.2 default-ON.
- [ ] Updated memory + BENCHMARKS.md.
- [ ] Σ.AH.2 plan ready to archive; promote next arc (likely Σ.AH.1) to CURRENT.md.

---

## Bench environment SOP

Per `feedback_full_bench_env_checklist.md` (post-Σ.AG.7 defaults are sufficient — no env vars needed for the milestone baseline). For Σ.AH.2 A/B specifically:

```bash
cargo build --release -p ematix-flow-core --example tpch_triangulation_bench --features triangulation
cargo build --release -p ematix-flow-core --example stage_profiler

# A/B for Story 3 wall-time gate
TPCH_DATA_DIR=examples/tpch/data/sf10 \
TPCH_TRIALS=20 TPCH_WARMUPS=3 \
TPCH_SKIP_POLARS=1 \
TPCH_OUT=/tmp/bench-2026-XX-XX/sf10-baseline.md \
./target/release/examples/tpch_triangulation_bench

EMAT_L9_PARTITIONED=1 \
TPCH_DATA_DIR=examples/tpch/data/sf10 \
TPCH_TRIALS=20 TPCH_WARMUPS=3 \
TPCH_SKIP_POLARS=1 \
TPCH_OUT=/tmp/bench-2026-XX-XX/sf10-l9part.md \
./target/release/examples/tpch_triangulation_bench
```

Canonical hardware: Apple M3 Pro, 14 cores. Don't switch mid-arc.

---

## Open questions

### OQ-AH.2-A: bloom merge sync vs lock-free?

**Default: lock-free with per-block bitwise-OR.** Decide at Story 1.0. If lock-free turns out subtler than expected, fall back to synchronous merge at end-of-build and add the sync-barrier cost to the Story 1 microbench gate (must still be ≤ 1 ms wall).

### OQ-AH.2-B: should AH.1 land first?

The cascade (Story 4) requires AH.1. **Default: AH.2 first per the Phase C sequencing recommendation** — AH.2 is lower risk, the solo impact is already material, and AH.1's mechanism is harder. AH.1 picks up the cascade benefit once AH.2 is live.

### OQ-AH.2-C: what if Q07's existing nation→customer L9 fires differently post-rule-extension?

Q07 already gets a CollectLeft L9 on nation→customer (memory `[[sigma-q-l13-to-l16-session]]`). The rule extension to Partitioned could inadvertently re-route Q07's bloom. **Default: Story 2.3 explicitly tests Q07's plan stability before allowing any Partitioned-fire on Q07.**

---

## Risks + watch-items

| Risk | Mitigation |
|---|---|
| **Partition-aware merge serialises (Σ.Q.L13 backfire pattern).** | Default lock-free; if forced synchronous, microbench gate ≤ 1 ms wall. |
| **Q07 existing CollectLeft L9 regresses from rule expansion.** | Story 2.3 non-regression test. |
| **Codegen-tax even from rule extension.** | Watch baseline geomean during Story 2; bloom-not-firing case must not regress. |
| **Compound cascade with Σ.AH.1 doesn't materialise as predicted.** | Story 4 explicitly verifies the partsupp build-size drop; if it doesn't show, AH.1's predicted cascade is questioned. |
| **Q09 partsupp build is 2-key composite — bloom on a single key may not narrow as expected.** | Build the bloom on the larger-cardinality key (ps_partkey); audit selectivity before declaring success. |

---

## Out of scope

- **Σ.AH.1 implementation** — separate arc, draft at [`docs/plans/sigma-ah-arc-1.md`](sigma-ah-arc-1.md).
- **Σ.AH.3 build-vs-probe side-swap** — separate arc, draft at [`docs/plans/sigma-ah-arc-3.md`](sigma-ah-arc-3.md).
- **Σ.AH.5 functional-dep group-by simplifier** — Q10-specific, parallel track.
- **Σ.AH.6 selectivity-gate tune** — Σ.AE.2 tuning, parallel track.

## Future levers (queued, not active)

- **Σ.AH.7 — `StringLike` selectivity from dict pages.** Story 1'.3's selectivity matcher handles `col = literal` (1/distinct_count) and `AND`/`OR` composition, but defaults to 0.2 for `StringLike`. **Q09's `p_name LIKE '%green%'` therefore still hits the conservative default** — `build_rows` shows 400k vs the real ~200k, fails the L9 ratio gate on lineitem 60M, no bloom fires. Q09 was an AH.2 target query so this is the gap. **Lever:** for dict-encoded columns, evaluate the LIKE predicate against the dict entries at planner time to count matching keys, then use `matches/distinct_count` as the selectivity. Cost: O(distinct_count) per LIKE predicate at plan time. Risk: low (planner-only).

---

## Cross-references

- Phase C synthesis: [`docs/PERF_REVIEW_2026_05.md`](../PERF_REVIEW_2026_05.md)
- Arc shell (parent): this file expands [`docs/plans/sigma-ah-arc-2.md`](sigma-ah-arc-2.md)
- Per-query evidence: [Q05](../PERF_Q05.md), [Q07](../PERF_Q07.md), [Q08](../PERF_Q08.md), [Q09](../PERF_Q09.md)
- Existing CollectLeft L9: memory `[[sigma-q-l9-landed]]`
- L9 timing/firing fixes: memory `[[sigma-q-l13-to-l16-session]]`
- Partition-aware merge risk precedent: memory `[[sigma-q-l13-landed]]` (parallel-bitmap dispatch 43× regression)
- Bench env: memory `[[feedback-full-bench-env-checklist]]`
- Tooling: [stage_profiler.rs](../../crates/ematix-flow-core/examples/stage_profiler.rs), [tpch_triangulation_bench.rs](../../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs), [bloom.rs](../../crates/ematix-flow-core/src/bloom.rs), [BuildSideBloomEmitterExec](../../crates/ematix-flow-core/src/build_side_bloom_emitter_exec.rs)
