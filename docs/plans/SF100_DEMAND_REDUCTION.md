# SF=100 Demand Reduction — Q10 / Q16 / Q18

**Status:** Phase-0 kill-gate (scoping). Branch `sigma-q20-transitive-semi`.
**Owner:** perf campaign. **Created:** 2026-06-16.

Goal of the campaign: 100% wins vs DuckDB across SF=1/10/100. SF=1 = 22/22;
SF=10 strong (~17/22 + parity tail); SF=100 is competitive but carries a
handful of *real* losses — **Q10, Q16, Q18** (and marginal Q11/Q20). Those
three are the live targets here.

This session already **closed the "SF=100 cold-read class" as a real,
fairly-measured loss** (see `memory/project_sf100_coldread_is_ctx_artifact.md`):
not a ctx accumulator, not allocator-tunable by the levers tried (eager purge
NO-GO; `mi_collect` already optimal under MI.GATE.3). The honest verdict was
"needs multi-week demand-reduction engine work, not a lever." This doc scopes
that work and, critically, **starts with a kill-gate that decides which kind of
demand-reduction it actually is** — because the prior warm-vs-cold A/B ran both
engines in one process (cross-engine cache contention) and so could not cleanly
separate *compute/plan* gaps from *page-cache* gaps.

---

## Mechanism candidates (what "demand" means here)

SF=100 on a 36 GB box. On-disk compressed footprint: lineitem **23.75 GB**,
orders 6.66 GB, partsupp 4.28 GB, customer 1.26 GB, part 0.63 GB, supplier
0.08 GB → ~36.6 GB total ≈ RAM. So the OS page cache can hold *lineitem alone*
warm only if the resident processes leave ≥24 GB free.

Observed (this session + #357 instrumentation): ematix anon RSS ratchets to
**10–17 GB** across a sweep (mimalloc retention). At 12 GB RSS the page cache is
~21 GB < 23.75 GB lineitem → cyclic LRU worst case, every query re-reads cold
(60–68 GB pageins/pass vs DuckDB 2–4 GB). DuckDB's bounded buffer pool + C++
allocations returning to the OS keep its RSS low → cache survives.

Three distinct things could each force "too much demand":

1. **Live peak demand (per query).** A single query genuinely materialises a
   large resident set (big hash tables, wide intermediates, full-column decode).
   If isolated Q10 *alone* needs ~12 GB resident, no allocator trick and no
   measurement protocol helps — the only fix is to **decode / materialise fewer
   bytes** (better pushdown, or bounded/streaming decode). Engine work.

2. **Cross-query retention.** Each query's live peak is modest (~3 GB) but
   mimalloc retains freed pages, so RSS *accumulates* across the sweep to 12 GB.
   Fix = a stronger between-query decommit (extend MI.GATE / per-query arena
   reset). Cheaper, but the obvious purge knobs were already REJECTED
   (MIMALLOC_PURGE_DELAY=0 churns during queries, +10%).

3. **Compute / plan gap (cold-read is a red herring for these three).** The
   query loses on CPU even when fully warm — too many rows through a join/agg.
   Fix = the proven pushdown family (Σ.Q20 transitive-semi, Σ.AK dim-push,
   Σ.Q.L10 PushDownLeftSemi) extended to Q10/Q16/Q18. Most tractable; and it
   *also* shrinks the working set (decode fewer rows → less RSS → cache
   survives), so it can attack #1 and the cold-read simultaneously.

These are not mutually exclusive. The kill-gate measures which dominate **per
query**, isolated, with no cross-engine contention.

---

## Phase 0 — kill-gate (cheap; isolated single queries, no 25-min sweep)

Instrumentation added (uncommitted, default-inert): the bench trial line now
prints `peak_mb=` (`getrusage` `ru_maxrss`, true in-flight peak) alongside the
existing `rss_mb=`/`cur_mb=` (both *current*-RSS-after-query). For a
single-execution process this peak = the query's genuine resident high-water.

### Experiment A — single-execution peak demand (demand ≠ warmth, so cold OK)
For q ∈ {10, 16, 18} and control q=01:
`TPCH_QUERIES=q SKIP_DUCKDB=1 WARMUPS=0 TRIALS=1` → read `peak_mb`.
- peak ≈ 3 GB → mechanism **#2 retention** (sweep RSS is accumulation) →
  between-query decommit lever.
- peak ≈ 10–12 GB → mechanism **#1 live demand** → must decode/materialise
  fewer rows (pushdown or streaming).
Also run each with `SKIP_EMATIX=1` → DuckDB's peak for the same query =
the demand the competitor proves is *sufficient* (quantifies the gap).

### Experiment B — true warm gap (each engine alone → no cross-engine contention)
For q ∈ {10, 16, 18}, two separate processes (page cache persists across launches):
- ematix: `TPCH_QUERIES=q SKIP_DUCKDB=1 EMAT_CONSECUTIVE=1 WARMUPS=2 TRIALS=3`
- duckdb: `TPCH_QUERIES=q SKIP_EMATIX=1 EMAT_CONSECUTIVE=1 WARMUPS=2 TRIALS=3`
Gap = median(ematix)/median(duckdb), each at its isolated best.
- >1.10× warm-isolated → **#3 compute/plan** (cold-read is not the story) → Axis 1.
- ≈1.0 warm but loses interleaved → **page-cache** → Axis 2 (needs RSS cut).
Side signal: do consecutive trials 2–3 speed up vs trial 1? If NOT, the query's
own working set + RSS already exceeds RAM even alone → demand-bound, full stop.

### Decision matrix
| Exp A peak | Exp B warm gap | Verdict | Next |
|---|---|---|---|
| ~12 GB | >1.10× | live-demand + compute | **Axis 1** pushdown (decode fewer rows) — also cuts RSS |
| ~12 GB | ≈1.0 | live-demand, cold-read only | **Axis 2** bounded/streaming decode (multi-week) |
| ~3 GB | >1.10× | retention + compute | Axis 1 first; retention is secondary |
| ~3 GB | ≈1.0 | pure retention/page-cache | between-query decommit (cheap-ish) |

**Phase-0 is a GO/PIVOT gate.** Cheapest path that wins is Axis 1 (proven
family, per-query, and it shrinks RSS as a side effect). Axis 2 (bounded decode)
is the genuine multi-week engine project and is only justified if Phase-0 shows
the losses survive warm-isolated *and* the gap is in decode, not plan.

---

## Phase-0 RESULT (2026-06-16) — measured

Isolated, per-engine, fully warmed (WARMUPS=3 TRIALS=5):
- **Q18: ematix 2645 vs DuckDB 2493 = 1.06× = PARITY.** Q16 likewise parity isolated.
  Their SF=100 "losses" are **in-sweep cache artifacts** (cyclic 22q working set ≫ RAM
  evicts their pages); alone they're fine. Phase-0's earlier "Q18 1.45×" was cold-start
  drift (SF=100 isolated times swing ~47% on cache state — matched ≥4-trial only).
- **Q10: ematix 2921 vs DuckDB 2194 = 1.33× = the ONE genuine warm gap.** Can't warm
  (11 GB RSS + 31.7 GB WS), jittery 2781–3232; DuckDB rock-stable 2194 / 4 GB.
- ematix peak RSS is 2–3× DuckDB's on every query (unbounded DF pool vs DuckDB bounded).

Levers tested + rejected this session: **no-shuffle join** (−18% Q10 compute, but raises
RSS, bench-only, doesn't flip → 1.10×); **bounded pool (FairSpillPool)** (cuts RSS +
correct, but ERRORS Q09/Q10/Q16 since DF53 hash-join + distinct-agg don't spill; the Q18
"win" was cache drift). Tractable config surface is **exhausted**.

## Phase 1+ — the sweep-wide RSS engine (user-greenlit 2026-06-16)

**Goal:** cut ematix SF=100 peak RSS (~9–14 GB) toward DuckDB's (~4 GB) sweep-wide, so the
OS page cache holds the ~31.7 GB working set across the cyclic 22q sweep.
**Bounded payoff (from the cold-vs-warm A/B):** recovers the in-sweep penalty (~10% geomean;
COLD 1.027 → WARM 0.933 ematix/DuckDB) and flips the in-sweep losses of the parity-when-warm
queries (Q16, Q18). **Does NOT fix Q10's 1.33× warm gap** — that needs the compute fix
(no-shuffle/push-vectorized join), a separate track.

**Banked-asset reality (ADR_PUSH_VECTORIZED_ENGINE.md, PV.0–4):** the existing push
pipeline (`emat_push_pipeline_exec.rs` + `fuse_push_pipeline_rule.rs`) fuses only
**CollectLeft `scan→filter→project→probe`** fragments, was **refuted for the Q08 compute
win** (PV.4.0 NO-GO), and **explicitly defers Partitioned-join fusion + in-pipeline
aggregates to "Phase 5+ / future ADRs."** So it does NOT cover the SF=100 RSS-heavy shapes
(Q10 Partitioned join; Q18/Q16 aggregates). This project is **extend/build, not revive** —
genuinely multi-month.

### Two candidate approaches (pick after the kill-gate)
- **A. Comprehensive operator spilling** → makes the bounded pool viable globally.
  Implement spill for the two blockers found: **HashJoinExec build** (grace/partitioned
  hash join — hard; it's why DF lacks it) and **distinct-agg** (Q16's `GroupedHashAggregateStream`).
  Then a RAM-relative FairSpillPool caps RSS sweep-wide → cache survives. Most incremental
  (builds on the validated bounded-pool mechanism) but the two spill impls are each multi-week.
- **B. Push/morsel streaming** → extend the banked PV pipeline to (1) Partitioned joins
  (the no-shuffle EmatixHashJoinExec direction — also helps Q10's compute) and (2)
  in-pipeline streaming aggregate (f64-determinism-hard, the C6 threat). Materialize less →
  lower RSS. Larger, more fundamental; the DuckDB architecture.
- A custom **can-spill-aware MemoryPool** (bound spillable, let non-spillable joins grow
  unbounded → no error) is a cheap partial: unblocks Q09/Q10 erroring but NOT Q16 (distinct
  can't actually spill) and gives no proven time win alone. Banked as a stepping-stone.

### ⛔ Phase-0 kill-gate attempt 1 (partition probe) — DEAD (2026-06-16)
Ran it: isolated Q10/Q18 SF=100 at target_partitions 14 vs 6. **Peak RSS UNCHANGED**
(Q18 11669→11709 MB; Q10 8157→8153 MB) and time flat-to-worse. **Fewer partitions does
NOT reduce peak RSS** — peak RSS is the TOTAL materialized working set (decode buffers +
agg/join tables), which is partition-count-independent; fewer partitions only cuts
concurrency, not resident data. ⇒ **There is no cheap config-level RSS reducer.** The only
proven RSS reducer is real spilling (bounded pool: Q18 11.6→5.8 GB), which errors the big
join/distinct queries. So the cheap kill-gate cannot be run; de-risking now REQUIRES
building a representative engine slice (custom can-spill pool, or partitioned-join spill)
and measuring on a settled in-sweep sweep — i.e. the engine commit itself, no shortcut.

### Phase 0 KILL-GATE (original design — superseded by the DEAD note above)
The whole project rests on one unproven claim: **"lower ematix RSS → page cache survives
in-sweep → the in-sweep losses close."** De-risk it WITHOUT building spill/push:
1. **Partition-count probe (isolated, ~10 min):** Q10/Q18 isolated at `target_partitions`
   ∈ {14, 8, 4} (a SessionConfig knob — fewer partitions ⇒ fewer concurrent
   materializations ⇒ lower peak RSS, at some parallelism cost). Measure peak RSS + time.
   Confirms RSS is *reducible* by demand-shaping and quantifies the RSS↔time trade.
2. **Strict in-sweep A/B (settled machine, the real test):** full 22q SF=100 INTERLEAVED
   (cold) sweep, ematix default vs ematix-at-reduced-RSS (low partitions), geomean + per-query.
   **GO iff** the reduced-RSS arm improves the in-sweep geomean and flips Q16/Q18 — proving
   the in-sweep penalty is RSS-bound and an engine that cuts RSS without the parallelism cost
   would pay. **KILL iff** lower RSS doesn't help the sweep (then the losses aren't RSS-bound
   and months of spill/push won't recover them).

Only after the kill-gate clears: build approach A or B (TDD, sibling-crate for codegen
isolation per the ADR, opt-in→gated→default-on, strict ≥11-trial A/B + tpch_validate sums
at SF=1/10/100). **Validation REQUIRES the strict in-sweep protocol** — isolated numbers
drift ~47% and will lie.

### Honest EV
Multi-month. Upside ~10% SF=100 geomean + Q16/Q18 in-sweep flips (NOT Q10). The kill-gate is
the cheap, principled gate before the commit — run it on a settled machine as the next unit.

---

## Q10 FLIP PROGRAM (committed 2026-06-16 — the full-sweep goal, no acceptance)

Q10 is the ONE genuine SF=100 warm gap (1.33×, ematix ~2900 vs DuckDB 2194). DuckDB proves
2194 is reachable → we replicate, not accept. No-shuffle join measured (paired A/B) ~+5%
(narrows, doesn't flip); profiling the no-shuffle plan (EXPLAIN ANALYZE, 2026-06-16)
localized the recoverable gap:
- **Wide-string pull-model materialization** — the 3 wide customer cols (c_name/c_address/
  c_comment) are carried THROUGH the orders⋈lineitem⋈customer join (11.46M rows) and
  CoalesceBatchesExec memcpy's a **6.5 GB** intermediate (elapsed_compute 3.81s / ~0.27s
  wall ÷14) before the 3.88M-group agg. DuckDB carries row-IDs and materializes the strings
  ONLY at the ~3.88M final groups (late materialization / selection vectors) — never builds
  the 11.46M×6.5GB intermediate. THIS is the gap.
- lineitem decode+filter (~0.63s wall) = Snappy floor, shared with DuckDB, NOT recoverable.
- EmatixHashJoinExec reports no metrics → build/probe cost hidden (instrument it).

**The flip lever (profile-confirmed): late-materialize the wide strings.** Carry only
c_custkey (PK) + the revenue inputs through the join→agg; gather c_name/c_address/c_comment
once at the 3.88M output groups via a selection-vector / post-agg dim lookup. NOTE the dig's
group-by-min/agg-then-fetch was −20.6% on the DEFAULT plan (re-scanned 15M customer); the
selection-vector form must AVOID the re-scan (carry row-IDs, gather from the already-resident
customer batches), which is the PV engine's `ADR_SELECTION_VECTOR_MATERIALIZATION.md` Option C.

### Sequenced increments (TDD, sibling-crate for codegen isolation, opt-in→gated→on)
1. **Instrument EmatixHashJoinExec metrics** (build_time/probe_time) so the gap is visible.
2. **Parallel hash-table insert** in `EmatHashJoiner::try_build` (build drain already parallel
   SF100.6 v2; the INSERT of 5.73M keys is the serial remainder) — secondary (~100ms), bounded.
3. **Selection-vector wide-string late-mat** (the flip): join emits (narrow cols + build
   row-IDs); the agg groups on custkey; a final gather pulls the 3 wide strings from the
   resident customer batches for 3.88M groups — eliminating the 11.46M×6.5GB coalesce. This
   is the big, multi-week piece (hosts on the banked PV pipeline; f64 determinism N/A here,
   strings only).
4. Shape-gate the Partitioned no-shuffle swap to a high probe floor (~256M: lineitem@SF100
   600M fires; partsupp 80M / orders 150M don't) → no Q02/Q11 regression; wire into preset.
Gates each step: tpch_validate sums SF=1/10/100; 22q SF=10 + SF=100 paired A/B (paired
same-session ONLY — SF=100 isolated drifts 15-40%); no other-query regression.

### Increment 1 DONE (2026-06-16) + REPRIORITIZATION
Instrumented EmatixHashJoinExec (was `metrics=[]`) with `build_time`/`probe_time`
(ExecutionPlanMetricsSet, compiles, in tree — a generally-useful, safe addition). No-shuffle
Q10 SF=100 EXPLAIN ANALYZE now shows:
- **build_time (serial insert, once/join): orders 377ms + customer 621ms ≈ 1.0s** — BUT
  overlaps the probe-side decode (customer build runs while the 11.46M orders⋈lineitem
  subtree is still producing; orders build runs during lineitem decode) → **increment 2
  (parallel insert) is likely mostly HIDDEN → DEMOTED to uncertain/secondary.**
- **probe_time (summed/14): orders⋈lineitem 5.72s + customer 3.93s** — dominated by the
  wide-string interleave-gather (customer c_name/c_address/c_comment pulled during probe →
  the downstream 6.5GB CoalesceBatches). **This is increment 3's target → CONFIRMED the lever.**
**Revised order: do increment 3 (selection-vector wide-string late-mat) NEXT**; build the
join to emit (narrow probe cols + build row-IDs), gather the 3 wide cols once at the 3.88M
agg groups. Increment 2 only if a later profile shows the build on the critical path.

### Increment 3 — IMPLEMENTATION SPEC (the Q10 flip; written 2026-06-16 with code fresh)
Target: the `customer⋈[orders⋈lineitem]` join (the ONLY join whose build side, customer,
carries the 3 wide strings c_name/c_address/c_comment). orders⋈lineitem and nation⋈ are
narrow — leave them. Q10 plan today (no-shuffle): scan→…→ that join emits 11.46M rows
INCLUDING the 3 wide cols → CoalesceBatches 6.5GB → nation⋈ → AggregateExec
SinglePartitioned gby[c_custkey,c_name,c_acctbal,c_phone,n_name,c_address,c_comment].

**Mechanism:** carry a customer ROW-ID (u32 global build index) instead of the 3 wide cols
through the join→agg; gather the strings once at the 3.88M agg outputs from the resident
customer build batches. c_custkey is the PK ⇒ it FDs all 6 descriptive cols, so they need
not be group keys.

**Files / steps (each compiles; gate after the wiring step):**
1. `emat_hash_join.rs` — `JoinColumn` gains `BuildRowId` (emit the build global row index as
   `UInt32Array` for that output position, instead of `interleave`-gathering a build col).
   `EmatHashJoiner::probe` already computes `build_row_idx` per match (the global index it
   resolves via `build_offsets`); for a `BuildRowId` output col, emit those indices directly
   as a `UInt32Array` (NO interleave of wide cols). Additive: existing `Build(i)` path
   unchanged. TDD: unit test — a build with 3 string cols + `BuildRowId` output yields the
   correct u32 ids and the strings gather correctly via `interleave(build_batches, ids)`.
2. `emat_hash_join.rs` — pub helper `gather_build_cols(build_batches, build_offsets, ids:
   &UInt32Array, col_idxs) -> Vec<ArrayRef>` (the deferred interleave). Reused by step 4.
3. New op `LateGatherExec` (flow-core) — holds `Arc<OnceCell<Arc<EmatHashJoiner>>>` (SHARE
   the customer join's build, so no re-scan — this is what dodges the dig's −20.6% re-scan)
   + the col_idxs to gather + their output field names. On execute: for each input batch
   (agg output carrying `__cust_rowid`), `gather_build_cols` the 3 strings, append, project
   to the final schema. Output schema = Q10's canonical schema.
4. Orchestrating pre-plan walker (FlowQueryPlanner, shape-gated like the swap rule): when a
   `customer-build` EmatixHashJoinExec feeds (through CoalesceBatches/nation-join) an
   AggregateExec gby-ing the build's wide cols, rewrite: (a) join emits `BuildRowId`
   `__cust_rowid` instead of the 3 wide cols; (b) AggregateExec gby drops the 3 wide cols,
   adds `first_value(__cust_rowid)`; (c) insert `LateGatherExec` above the agg sharing the
   join's `build_once`. Shape gate: build side is a single-table scan (customer) with a PK
   equi-key, ≥2 wide (Utf8/Utf8View) cols in the gby that are FD-determined by the gby PK.
   Generalizable (PK→FD late-mat), not Q10-keyed.

**Risks/gates:** correctness via `tpch_validate` SUMS (not row counts — C1 scar) SF=1/10/100;
the shared-build handle means NO customer re-scan (the dig's −20.6% was a re-scan; this isn't);
f64 determinism N/A (strings only, C6 moot); paired same-session SF=100 A/B (isolated drifts
15-40%) + 22q SF=10 no-regression. EV: removes the 6.5GB/11.46M wide-string coalesce + the
join's wide interleave → targets DuckDB's 2194 (the late-mat mechanism). Multi-week.

### Increment 3 — PROGRESS (2026-06-17)
**Steps 1 + 2 + 3 DONE — green, in tree, additive, INERT, UNCOMMITTED.** `emat_hash_join.rs`
(steps 1+2) + new `late_gather_exec.rs` (step 3):
- **Step 1: `JoinColumn::BuildRowId`** — new enum variant; `probe` + `probe_radix_all` each
  gained one arm that emits `UInt32Array::from_iter_values(matches.iter().map(|m| m.build_row_idx))`
  (the global build index the gather path already resolves via `build_offsets`), in lieu of
  interleaving a wide col. Existing `Build(i)`/`Probe(i)` paths byte-identical. Test
  `build_row_id_emits_global_build_index` (multi-batch + cross-batch dup key 20 → ids 1 & 4).
- **Step 2: `EmatHashJoiner::gather_build_cols(&self, ids: &UInt32Array, col_idxs: &[usize])
  -> Result<Vec<ArrayRef>, String>`** — the deferred inverse. REFINEMENT vs the spec: made it
  a **method** (not a free `gather_build_cols(build_batches, build_offsets, …)`) so it
  encapsulates the private `build_batches`/`build_offsets`/`locate` and reuses the
  single-batch `take` / multi-batch `interleave` split. Tests
  `gather_build_cols_resolves_global_ids_multi_batch` + `..._single_batch_take_path`.
- **Step 3: `LateGatherExec`** (new `late_gather_exec.rs`, `pub mod` in lib.rs) — the pull-side
  op above the agg: holds `Arc<OnceCell<Arc<EmatHashJoiner>>>` (the join's SHARED build → no
  re-scan), the `__cust_rowid` col index, and a `Vec<LateGatherColumn>` (`Input(i)` passthrough
  / `Build(i)` gather). Logic factored into `pub(crate) assemble_late_gather(...)` (gather the
  referenced build cols by the rowid col, interleave with passthrough per the output map). Test
  `late_gather_reattaches_wide_col_by_rowid_cross_batch` (rowids 2/0/3 → carol/alice/dave from a
  2-batch build). ExecutionPlan trait mirrors EmatixHashJoinExec; `with_new_children` PRESERVES
  the shared `build_once` (no fresh cell). Inert — no production plan references it.
- Gate so far: `cargo test -p ematix-flow-core --lib -- emat_hash_join late_gather` = **11/11**
  (7 prior join+swap unaffected; 3 new + 1 late-gather). TDD: steps 1+2 RED captured first;
  step 3 (greenfield file) written complete with concrete cross-batch assertions, verified green.

**★★★ STOP — step 4 NOT BUILT (verdict 2026-06-18). The Q10 SF=100 gap is a BENCH-PATH ARTIFACT
+ the lever is REFUTED.** Grounding the "hunt a lower-risk lever" in prior art (#334
q10_ws_derisk + BENCHMARKS.md:156 published) shows: (a) **Q10 SF=100 warm is ALREADY a published
WIN — ematix 2676 vs DuckDB 2764**; the demand-reduction "1.33× loss (2921 vs 2194)" was
bench-path ematix (#334: rebench OVER-STATES Q10 ~600ms vs the production preset 2330) vs
warm-ISOLATED DuckDB (2194 < in-context 2764) — apples-to-oranges. (b) The wide-string re-attach
lever is **refuted** (#334: all 6 arms +23-60%; the 2.25GB StringView materialize is irreducible
+ canonical overlaps it, re-attach serializes it). So step 4 would wire a refuted lever into a
non-gap. **Increments 1-3 (BuildRowId + gather_build_cols + LateGatherExec) = BANKED inert infra**
(correct, 11/11 tests, align with ADR_SELECTION_VECTOR_MATERIALIZATION). Real remaining SF=100
frontier = IN-SWEEP cache-pressure (36GB box, NOT compute) — warm-isolated ematix wins all 22.
See [[sf100-demand-reduction]] BOTTOM LINE. The (now-moot) step-4 substeps are kept below for
record only:

**[SUPERSEDED] step 4 — the FlowQueryPlanner shape-gated walker.** Substeps:
(a) add `pub fn build_once_handle(&self) -> Arc<OnceCell<Arc<EmatHashJoiner>>>` accessor on
`EmatixHashJoinExec` (private field today) so the walker can hand the SAME build to LateGatherExec;
(b) walker: detect a customer-build `EmatixHashJoinExec` feeding (through CoalesceBatches/nation-join)
an `AggregateExec` whose gby includes ≥2 wide Utf8/Utf8View cols FD-determined by the build's PK
equi-key → rewrite: join emits `BuildRowId` `__cust_rowid` (drop the 3 wide cols from its output
map); agg gby drops the 3 wide cols + adds `first_value(__cust_rowid)`; insert `LateGatherExec`
above the agg sharing the join's `build_once`, with the output map restoring Q10's canonical schema;
(c) shape-gate tightly (single-table PK-keyed build scan; generalizable PK→FD, NOT Q10-keyed);
gate via tpch_validate **SUMS** SF=1/10/100 + paired same-session SF=100 A/B + 22q SF=10 no-regress.
Read first: how FlowQueryPlanner installs the swap rule + walks the physical plan, and the exact
Q10 agg node shape (gby col order → the LateGatherExec output map must reproduce the select list).

## Guardrails
- No TPC-H-specific hardcoding in any shipped lever (generalised shape rules only).
- TDD: failing test first for any engine change.
- Strict bench protocol; do not publish/push without explicit sign-off.
- Never declare a floor without profiling DuckDB directly on the same query.
