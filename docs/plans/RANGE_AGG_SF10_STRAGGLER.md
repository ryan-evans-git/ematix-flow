# Project plan: high-cardinality "radix" aggregation — the SF=10 straggler

**Status:** proposed (not started). **Owner:** perf campaign. **Branch base:** `sigma-q20-transitive-semi`.
**Created:** 2026-06-13. **Supersedes the framing of:** REV.5/5.b, REV.8/9, Σ.R.1, Σ.AN ("radix-partitioned hash agg, 2–3 weeks").

---

## TL;DR — what "the radix-agg project" actually is now

The radix-aggregate idea has **already been ~90% realized and shipped** as **RANGE.AGG**
(`crates/ematix-flow-core/src/clustered_agg_rule.rs`, commit `f15d2fc`, default-on). It is a
*plan rewrite*, not a kernel: it collapses DataFusion's
`Partial → hash-Repartition → FinalPartitioned` agg triple into a single
`SinglePartitioned` agg by exploiting the fact that `lineitem` is physically clustered on
`l_orderkey` (dbgen writes it sorted), re-chunking the scan at provably **key-disjoint
row-group boundaries**. That win **flipped Q18 at SF=100 (2461 ms vs DuckDB 2732) and closed
every remaining SF=100 DuckDB loss.** It is general, not TPC-H-specific (any group-by on a
file's physical cluster key qualifies).

So the open question is no longer "build a radix agg." It is precisely:

> **Why does RANGE.AGG decline at SF=10, and is closing that worth doing?**

The answer is a single, narrow mechanism — the **skew gate** (clustered_agg_rule.rs:154-177):
at SF=10 `lineitem` has only ~58 row groups, of which only ~14 are strict-gap split candidates
(the other ~43 boundaries have a key straddling them). Fourteen sparse, unevenly-spaced cut
points cannot balance 14 partitions, so the achieved chunking carries a **1.7× straggler that
measured +18% wall** vs the stock two-phase plan. The rule correctly declines rather than trade
the shuffle for a straggler tail.

**This is a load-balancing problem, not a kernel or cache problem.** The fix is bounded and
low-risk relative to a from-scratch radix kernel — but its payoff is also bounded: it targets
**one query at one scale (Q18 SF=10), a ~1.05× parity coin-flip.** Read §8 before committing.

---

## 1. The corrected target surface (measure-first)

| Query / scale | Status | Does a better high-card agg help? |
|---|---|---|
| **Q18 SF=100** (600M→150M groups) | **WIN ~5× already** (RANGE.AGG) | No — already won. Shuffle-elimination paid here. |
| **Q18 SF=10** (60M→15M groups) | **parity loss ~1.05×** | **Yes — this is the only live target.** Blocked by the skew gate. |
| Q01 SF=10/100 (`GROUP BY returnflag,linestatus`) | win | **No** — 4 groups. Low-card; stock agg is optimal, radix/cluster does nothing. |
| Q13 SF=10/100 (`GROUP BY c_count`) | win | **No** — ~40 groups. Same as Q01. |
| Q15 SF=10/100 (`GROUP BY l_suppkey`, ~100K/1M) | ≈ Polars parity | **No** — cache-miss-bound; `CombineAggExec` measured **wall-neutral** (see `[[pvm8-combine-agg-neutral]]`). Not a cluster key. |

**Correction to an earlier claim:** "radix agg also helps Q01/Q13" is **false** — those are
low-cardinality aggregates where the stock kernel is already at the floor. The high-card
agg lever's entire addressable surface at the current win-frontier is **Q18 SF=10**.

---

## 2. Prior-art lesson chain (why earlier radix work lost, and why RANGE.AGG won)

This is the most important context — it tells us which axis is real.

1. **Kernel swaps lost.** REV.5 / REV.5.b (spill-backed global-radix SUM), REV.8/9
   (`single_pass_radix_sum_exec.rs`, opt-in), Σ.R.1 (`robin_hood_agg.rs` radix SUM) all
   replaced DataFusion's agg *kernel* with a hand-rolled radix kernel. Microbenches looked
   good (single-pass measured **1.71×** the two-phase at the Q18 SF=100 shape) but
   **end-to-end they measured net-negative** (commit `2bae2df`: "both measured net-negative,
   NOT default"). The kernel was never the bottleneck.

2. **The real win was structural — shuffle elimination — and the plan rewrite captured it.**
   RANGE.AGG keeps DataFusion's *stock* `SinglePartitioned` kernel and instead removes the
   `Partial → Repartition(2.2 GB) → Final` shuffle by proving each partition's key range is
   disjoint. That is what flipped Q18 SF=100. Memory had already predicted this:
   *"the realizable win is the shuffle-avoidance, not the kernel"* (`[[q18-sf100-gap-is-agg-shuffle]]`).

3. **The SF=10 residual is a third axis again — not kernel, not shuffle-CPU, not cache.**
   At SF=10 the shuffle is small (`RepartitionExec` ≈ 44 ms *elapsed_compute* ≈ ~3 ms wall),
   so shuffle-elimination buys little; the agg tables are ~1M keys per partition either way,
   so cache footprint barely changes. What RANGE.AGG *would* add at SF=10 is **sequential,
   sorted-order table insertion** (clustered chunks) instead of **hash-scattered Final-phase
   probes** — fewer cache misses on the same-size table. The only thing standing between us and
   that win is the **straggler** introduced by coarse, sparse RG-granular strict-gap cutting.

**Discipline check honored:** the competitor (DuckDB's radix agg) was profiled, the lower
feasible time localized to a specific mechanism, and the prior kernel route was refuted by
measurement, not inference. This plan does not retry the refuted kernel route.

---

## 3. Current RANGE.AGG mechanism + exact decline reason

`ClusteredSinglePhaseAggRule` (wired in `preset.rs:161`, default-on, opt-out `EMAT_RANGE_AGG=0`):

- Matches `FinalPartitioned(gby=[k]) ← Repartition(Hash) ← Partial(gby=[k]) ← EmatixFastParquetExec`.
- Resolves `k` to a file column; pulls per-row-group `(min,max)` for that column in **one footer
  parse** (`rg_i64_ranges_and_counts`, added in `d43e4a1` to kill a +23 ms plan-time tax).
- `plan_disjoint_chunks()` (clustered_agg_rule.rs:91) requires monotone non-overlapping RG
  ranges, then keeps **only strict-gap boundaries** `max(rg_j) < min(rg_{j+1})` as split points.
- **Skew gate** (clustered_agg_rule.rs:154-177): declines when the largest achieved chunk
  exceeds the ideal row share by more than `EMAT_RANGE_AGG_MAX_SKEW` (default **1.25×**).
- On fire: re-chunks the scan via `scan.with_assignments(chunks)` and builds
  `AggregateExec(SinglePartitioned)`; re-runs `EnforceDistribution` (Σ.BS repair).

**Why SF=10 declines, precisely.** With ~4 rows per `l_orderkey` and ~1M rows per RG, a key
straddles a given RG boundary ~3/4 of the time, so only ~1/4 of the 57 boundaries are
strict gaps (~14 candidates — matches the in-code comment). Fourteen unevenly-spaced cut points
cannot place 13 splits near the ideal row offsets, so the largest chunk lands at ~1.7× ideal →
**+18% wall straggler** → skew gate (1.25×) declines. SF=100 has 573 RGs → ~140 strict gaps,
dense enough to land within ±2 RGs of ideal → passes.

**Scan chunking is row-group-granular only.** `with_assignments(Vec<Vec<usize>>)` assigns whole
RGs; the reader has **no intra-RG row-range support** and ematix-parquet exposes **no page/offset
index**. So finer-than-RG cutting is *not* available without multi-week decoder work (see §6
Approach B).

---

## 4. Goal & success criteria

**Goal:** make RANGE.AGG fire on Q18 SF=10 with balanced chunks, flipping it from a ~1.05×
parity loss to a win, with zero correctness or 22q-geomean regression.

A change ships **only if all hold** (strict interleaved A/B protocol, `[[sigma-ai-1-strict-bench-landed]]`):

1. **Correctness:** `tpch_validate` 22/22 at SF=1 **and** SF=10 **and** SF=100 (row-for-row vs DuckDB).
2. **Target win:** Q18 SF=10 RANGE.AGG-firing beats RANGE.AGG-declining by **more than the
   measured noise band** (SF=10 leg-to-leg spread is ~5–11%; need a clean, directionally-consistent
   margin across ≥3 interleaved pairs, not a single run).
3. **No regression:** 22q SF=10 geomean within noise of baseline; **Q18 SF=100 still wins**
   (the new balanced-chunk path must not regress the scale that already works);
   per-query SF=100 sweep unchanged.
4. **No new floor lies:** if Phase 0 (§7) shows the balanced single-phase plan does **not** beat
   stock at SF=10 even with perfect balance, **stop** — the agg is then genuinely at its floor and
   we accept the parity coin-flip (honest NO-GO, banked as the answer).

---

## 5. Phasing (Phase 0 is a kill-gate — do it before writing the operator)

### Phase 0 — De-risk spike: does a *balanced* single-phase plan even win at SF=10? (½–1 day)

The whole project rests on the assumption that the straggler is the *only* thing stopping a
win. Test that cheaply, **before** building anything:

- Manually force RANGE.AGG to fire on Q18 SF=10 with an **artificially balanced** chunking —
  e.g. temporarily raise `EMAT_RANGE_AGG_MAX_SKEW` to accept the current (skewed) chunks **and**
  separately hand-assign near-balanced RG chunks (ignore strict-gap correctness for the *timing*
  probe only — verify rows separately). Measure Q18 SF=10 wall: balanced-single-phase vs stock.
- **Kill criterion:** if even a hand-balanced single-phase agg is **not** faster than stock by a
  clear margin at SF=10, the cache/shuffle benefit doesn't materialize at this scale → **NO-GO**,
  write it up, accept the coin-flip. (This is the `[[pvm8-combine-agg-neutral]]` lesson applied
  pre-emptively: measure the *wall* of the real pipeline, not a cache-warm microbench.)
- **Go criterion:** balanced single-phase beats stock by > noise → proceed to Phase 1.

This phase is the single most important gate. It costs ~1 query A/B (~minutes of compute) and
either greenlights the work or saves a week.

### Phase 1 — Balanced any-boundary chunking + boundary-key merge (the build; ~3–5 days, only if Phase 0 = GO)

The correctness invariant RANGE.AGG relies on is "no group spans a partition." Strict-gap
cutting guarantees it but is too sparse. Relax it to **any RG boundary** and repair the ≤(N−1)
keys that then span a cut:

1. **`plan_balanced_chunks()`** (replaces/augments `plan_disjoint_chunks`): place N−1 cuts at the
   RG boundary nearest each ideal cumulative-row offset (cut anywhere, not only strict gaps).
   Record, per seam, whether it is a **span** (`max(rg_j) == min(rg_{j+1})` ⇒ boundary key value
   `K` known at plan time) or a clean gap. Keep a much weaker skew gate (balanced cuts make ~1.1×
   easy at 58 RGs).
2. **`BoundaryMergeExec`** (new `ExecutionPlan`, emitted by the existing rule — **not** a new
   optimizer rule, to avoid the codegen tax of §6): wraps the N `SinglePartitioned` outputs.
   - Each partition's key range `[min_key, max_key]` is known from RG stats. A row is a
     *boundary candidate* iff its key equals this partition's `min_key` or `max_key`
     (an O(1), almost-always-false compare per row — ~1 ms total across 14 partitions for
     15M rows; the 15M interior rows stream through untouched).
   - Buffer only the ≤2 boundary rows per partition; at the seams, combine the partial for the
     shared boundary key `K` across the adjacent partitions (SUM: add; COUNT: add; AVG: needs
     sum+count split — see §6 limitation) and emit one merged row.
   - Everything else passes through with zero copy.
3. **TDD first** (`[[feedback-tdd]]`): port the existing
   `e2e_clustered_group_by_is_exact_across_boundary_spans` test and add a case that **forces a
   non-strict-gap cut** and asserts the boundary key appears **exactly once** with the correct
   sum. Add unit tests for `plan_balanced_chunks` balance + span-tagging.
4. Gate the new path behind a sub-flag (`EMAT_RANGE_AGG_BALANCED=1`, default OFF) until the A/B
   passes, then flip default-on in a second commit (same discipline as PV.M.7 / REV.10).

### Phase 2 — Gates + ship (≥1 day, settled bench)

- `tpch_validate` 22/22 at SF=1/10/100.
- Strict interleaved A/B: Q18 SF=10 (target win) **and** Q18 SF=100 (no regression) **and**
  22q SF=10 geomean. Settle ≥20–30 min before each pair; `caffeinate -i` + `taskpolicy`;
  median-of-medians (`scripts/bench/strict_ab.sh`).
- `cargo fmt` + clippy clean; workspace `--lib` tests green.
- If all green: flip default-on, update `docs/PERF_Q18.md` + `BENCHMARKS.md`, write memory.
  **Do not publish/deploy** without explicit user sign-off (standing constraint).

---

## 6. Approaches considered

**A. Balanced any-boundary chunking + boundary-key merge — RECOMMENDED (§5).**
RG-granular, no decoder changes, builds directly on the shipped & proven RANGE.AGG. Adds one
small `ExecutionPlan` (emitted by the existing rule, so no *new optimizer rule* and minimal
codegen perturbation per `[[optimizer-codegen-sensitivity]]`). Correct by a trivial ≤(N−1)-key
merge. ~1 week.
*Limitation:* AVG/COUNT(DISTINCT) need care — SUM/COUNT/MIN/MAX merge associatively; AVG must be
carried as (sum,count). Q18 is SUM, so ship SUM/COUNT first and gate other aggregates out.

**B. Sub-RG row-range chunking via page/offset index — DEFERRED.**
Add row-range assignment to the reader + page-level key stats so chunks can be cut to perfect
balance inside a row group. More general (helps even when RG count is tiny, e.g. SF=1) but
requires ematix-parquet page/offset-index support (absent today) + reader row-skip → multi-week
decoder work. Not justified by a single SF=10 parity query. Revisit only if a *broad* class of
clustered-agg queries emerges.

**C. Radix sub-partition the hash table within each chunk (the literal "radix kernel") — REJECTED.**
This attacks **cache footprint**, but §2/§3 show the SF=10 blocker is the **straggler (row
imbalance)**, not cache — the per-chunk tables are ~1M keys regardless of radix bins. This is the
axis the prior REV.5/8/9 kernels optimized, and it is *why they lost*: right tool, wrong
bottleneck. Do not retry without a new precondition that re-establishes cache as the SF=10 limiter.

---

## 7. Risks & failure modes

- **Phase 0 NO-GO (most likely failure).** A balanced single-phase agg may still not beat stock
  at SF=10 — the win at SF=100 came largely from killing a 2.2 GB shuffle that barely exists at
  SF=10. If so, accept the coin-flip. This is the honest expected outcome to weigh against the
  effort. **Mitigation:** Phase 0 is explicitly the cheap kill-gate.
- **Noise floor swallows the win.** Q18 SF=10 is a ~1.05× parity case and SF=10 leg-to-leg spread
  is ~5–11%. Even a real 5–8% improvement needs ≥3 clean interleaved pairs to call. A "win" inside
  the band is not a win. **Mitigation:** success criterion #2 demands a margin above noise, not a
  single favorable run.
- **Boundary-merge correctness.** A split key emitted twice = wrong answer (silent). **Mitigation:**
  TDD with a forced-span case asserting exactly-once emission, plus full `tpch_validate` at all
  scales.
- **Codegen tax (`[[optimizer-codegen-sensitivity]]`).** Any new optimizer *rule* costs ~5–8%
  geomean via LLVM codegen perturbation. **Mitigation:** add **no new rule** — extend the existing
  `ClusteredSinglePhaseAggRule` and emit a new `ExecutionPlan`; keep the merge kernel small (or in
  `ematix-flow-push`, which is codegen-isolated).
- **Plan-cache interaction (`[[sigma-ag-complete]]`).** Confirm the `is_cacheable` walker doesn't
  choke on the new `BoundaryMergeExec` shape (it refuses BloomEmitter/CollectLeft-semi/SharedSubtree;
  a plain agg-finalize wrapper should be fine, but verify or add a refusal term — a cache that
  serves the stock plan would silently mask the win, the documented Σ.Q20 footgun).
- **Thermal / build-drift artifacts on a wrapless A/B (`[[project-win-campaign-l9-probeorder]]`).**
  Confirm RANGE.AGG actually fires (`EMAT_RANGE_AGG_TRACE=1` → `[range_agg] FIRE`) before
  attributing any delta. Never bench with a concurrent `cargo build`.
- **SF=100 regression.** The balanced path changes chunk selection; ensure SF=100 (573 RGs) still
  lands on a passing, winning chunking. Gate Q18 SF=100 in Phase 2.

---

## 8. Honest expected value & recommendation

**Reward, if it works:** Q18 SF=10 flips from ~1.05× loss to a win → contributes toward a clean
**22/22 at SF=10**. That is the entire prize. It does **not** generalize to other current losses
(Q05 is join-order/structural; Q07 is decode; Q14/Q15 are decode vs Polars). The big
high-cardinality-agg prize (SF=100) is **already banked**.

**Cost:** ~1 week *if* Phase 0 greenlights; ~½ day if it kills.

**Probability of a shippable win:** moderate-to-low. The mechanism is sound and proven at
SF=100, but the SF=10 economics are weaker (small shuffle, same-size tables) and the query is a
parity coin-flip in a wide noise band. Phase 0 exists precisely because this is uncertain.

**Recommendation:** **Run Phase 0 only, then decide.** It is the cheapest possible test of the
core assumption and converts "multi-week radix project" into a few-minutes A/B that either
greenlights a scoped 1-week build or definitively closes the SF=10 agg question. Do **not**
pre-commit the full build. This respects the never-declare-a-floor discipline (we profile before
concluding) while refusing to sink a week into a parity coin-flip on faith.

If the user wants a *different* high-leverage target instead, the SF=100 cold-read class
(Q10/Q16/Q18 memory pressure — the user's originally-queued "per-thread heap release / demand
reduction" idea) is the larger surface, now that MI.GATE.3 fixed the self-inflicted SF=10 tax.

---

## 9. Backout / banking

- Phase 1 lands behind `EMAT_RANGE_AGG_BALANCED=1` (default OFF) — zero risk until the A/B flips it.
- If NO-GO: keep `BoundaryMergeExec` + `plan_balanced_chunks` as banked, default-inert,
  tested infra (the RANGE.AGG generalization is correct even if it doesn't win Q18 SF=10; it may
  pay for a future clustered-agg shape with denser keys/larger groups). Record the NO-GO and the
  Phase-0 number in `[[project-win-campaign-session3-q05-splice]]`.

## References
- Code: `crates/ematix-flow-core/src/clustered_agg_rule.rs` (RANGE.AGG), `preset.rs:161` (wiring),
  `single_pass_radix_sum_exec.rs` + `robin_hood_agg.rs` + `combine_agg_exec.rs` (prior/banked infra).
- Commits: `f15d2fc` (RANGE.AGG ship), `d43e4a1` (footer-parse tax fix), `2bae2df`/`7c1080b`/`c5619fc` (refuted kernel routes).
- Memory: `[[q18-sf100-gap-is-agg-shuffle]]`, `[[sigma-am-q18-diagnosis]]`, `[[q18-sf10-duckdb-plan-diff]]`,
  `[[pvm8-combine-agg-neutral]]`, `[[optimizer-codegen-sensitivity]]`, `[[sigma-ai-1-strict-bench-landed]]`.
- Methodology: strict interleaved A/B (`scripts/bench/strict_ab.sh`), TDD, never-declare-a-floor.
