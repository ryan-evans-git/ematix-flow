# Σ.AH — clean-slate Q01-Q22 SF=10 execution-plan inefficiency review

**Status:** active
**Created:** 2026-05-26 (post-Σ.AG.7, plan cache default ON)
**Active phase:** Phase A — methodology refresh
**Active story:** A.1 — audit `STAGE_PROFILING_METHODOLOGY.md` constants against post-Σ.AG.7 reality
**Branch policy:** local commits only on this plan. PR only after a Phase D arc is sized and gated.
**Predecessor plan:** [`docs/plans/archive/2026-05-25-sigma-t-v5-tier-1.md`](./archive/2026-05-25-sigma-t-v5-tier-1.md) — Σ.T V5 Tier 1 (L13 custom hash join active on Story 2.1; archived to make room for the new survey). May be re-prioritised after Phase C.

---

## Summary

A clean-slate re-execution of the per-stage waste survey across all 22 TPC-H queries at SF=10, on the current (2026-05-26, post-Σ.AG.7) numbers. The prior 2026-05-25 sweep is stale in three places: (1) it ran on a different lever set (no plan-cache default, pre-PR #146 SIMD parity, pre-Σ.AG.7 numbers); (2) its synthesis ranking was contaminated by prior rejections that were specific to *kernel implementations that have since improved* — those rejections must not silently constrain the new candidate ranking; (3) several "near floor" findings were drawn against a 2025 floor model whose decode-throughput constants may now be wrong given ematix-parquet 0.16.2 + LZ4_RAW fix.

The plan is deliberately survey-then-roadmap, not survey-then-implement. Phases A-C produce documents; Phase D drafts arc plans only and exits this CURRENT.md. Implementation of any arc moves to its own CURRENT.md after this plan archives.

**Baseline (frozen for this plan):** `BENCHMARKS.md` 2026-05-26 refresh. 22q SF=10 ematix-flow vs DuckDB: ematix wins 15, DuckDB wins 7 (Q03 tie attributed to DuckDB, plus Q05/Q06/Q07/Q08/Q17/Q18). 22q geomean is not the gate for this plan — *absolute waste-ms × confidence per query* is. A query we win can still be 5× over its floor; that's the survey's target.

**Hard rules (from caller and project memories):**
1. **Local commits only**, no PRs unless explicitly requested.
2. **Bench-gate every Phase D arc**: ship-if / reject-if criteria defined before any implementation. `project_lever4_microbench_gate_pass.md` and `project_sigma_r2_rejected.md` are the precedents — kernel microbench wins do not predict wall-time.
3. **Generalised wins only** per `feedback_no_tpch_hardcoding.md`. A pattern that helps only one query is out of scope unless it documents a shape that recurs in adjacent workloads.
4. **No new `PhysicalOptimizerRule`** as a primary lever (`project_optimizer_codegen_sensitivity.md`). Pre-plan walkers and sibling-crate kernels only.
5. **TDD on any Phase D arc** per `feedback_tdd.md`.
6. **Reuse existing tooling.** `crates/ematix-flow-core/examples/stage_profiler.rs` (per-operator metrics) and `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs --features triangulation` (wall-time) are the canonical instruments. Do not build new profilers.
7. **Existing PERF_Q*.md files are evidence, not artefacts.** They were profiled 2026-05-25 pre-Σ.AG.7 and pre-StringView-fix. They get overwritten by this plan, not patched.
8. **Time budget per query: 30-60 min.** If a query exceeds 2 hours of investigation without a finding, mark "near-floor, no waste candidate" and move on. Hard cap.
9. **No premature implementation.** The previous V5/L13 cycle stalled because the team jumped ahead from "candidate" to "implementation" before completing the survey. Resist that here per `docs/STAGE_PROFILING_METHODOLOGY.md` §6.
10. **Recommend next step** per `feedback_recommend_next_step.md` — every completed story ends with a concrete `Next:` line.

---

## What this plan deliberately does not do

- Does not design any kernel, operator, or optimizer change. Phase D drafts arc *plans*; implementation moves to a new CURRENT.md.
- Does not re-run Q01 from scratch. Q01 was profiled 2026-05-25 with the StringView fix already landed; we verify against current numbers and move on (Story B.1 below).
- Does not commit to the L13 / L14 sequencing from the archived plan. If Phase C finds higher-impact candidates, L13 / L14 wait. If Phase C re-confirms them, they resume from the archive.
- Does not re-litigate every prior rejection. Phase C lists rejections that the *new* candidate ranking forces a re-look at; the rest stay rejected per their memory files.

---

## Bench environment SOP (one-time setup, applies to every story)

Per `feedback_full_bench_env_checklist.md`. The 22q SF=10 0.80-geomean / 15-win baseline requires ALL of:

```bash
# RG decode cache + RH sum-f64 default ON post-Σ.AG.7 — no env prefix needed for the baseline.
# Plan cache default ON post-Σ.AG.7 — no env prefix needed.

# Build
cargo build --release -p ematix-flow-core --example stage_profiler
cargo build --release -p ematix-flow-core --example tpch_triangulation_bench --features triangulation

# Wall-time (per-query)
TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_QUERIES=<N> \
  TPCH_TRIALS=5 TPCH_WARMUPS=2 \
  ./target/release/examples/tpch_triangulation_bench

# Restore BENCHMARKS.md (single-query runs overwrite it)
git checkout BENCHMARKS.md

# Per-operator metrics
TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_QUERY=<N> \
  TPCH_TRIALS=5 TPCH_WARMUPS=2 \
  ./target/release/examples/stage_profiler

# Self-time (long-running for sample)
TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_QUERY=<N> \
  TPCH_TRIALS=40 TPCH_WARMUPS=2 \
  ./target/release/examples/stage_profiler > /tmp/qNN_stage.log 2>&1 &
BPID=$!
sleep 2
sample $BPID 10 -file /tmp/qNN_sample.txt
wait $BPID
```

Canonical hardware: Apple M3 Pro, 14 cores. Don't switch to minipc or any x86 host mid-survey — the floor constants are calibrated to M-series.

---

## Phase A — methodology refresh

**Goal:** confirm `docs/STAGE_PROFILING_METHODOLOGY.md` is still accurate against post-Σ.AG.7 / post-ematix-parquet-0.16.2 reality. If any constant is now wrong, re-calibrate before Phase B starts — every Phase B writeup's floor table depends on these constants.

**Estimated effort:** 0.5 day. Cap at 1 day; if it bleeds further, ship Phase A with a documented "needs re-calibration" note on specific constants and let Phase B continue against the existing model.

**Why this isn't optional:** the LZ4_RAW decode-bug fix in ematix-parquet 0.14.0 (`project_ematix_parquet_lz4_decode_bug.md`) changed the per-thread LZ4 throughput from a broken-path number to a real one. The methodology lists 5 GB/s — that may be optimistic or pessimistic. Similarly, SIMD parity work (PR #146 / ematix-parquet 0.16.2) may have moved the bit-unpack constants. Without a fresh calibration, every floor table in Phase B is suspect.

### Story A.1 — audit methodology constants against current kernel benches [ACTIVE]

**Status:** `[ACTIVE]`
**Time budget:** 4 hours.

**Tasks:**
- [ ] Re-read `docs/STAGE_PROFILING_METHODOLOGY.md` §"Theoretical floor" table.
- [ ] For each constant, find the supporting kernel-bench in the workspace OR in `ematix-parquet`. Cross-reference:
  - Snappy decompress (2 GB/s per thread) — confirm against `ematix-parquet/benches/snappy_decode.rs` if it exists; else accept as published per `project_q06_sf10_polars_gap_wall.md`'s 1.73 GB/s extprice measurement.
  - LZ4_RAW decompress (5 GB/s per thread) — re-derive from `project_ematix_parquet_lz4_decode_bug.md` (Q06 SF=10 LZ4 = 57.88 ms, lineitem.lz4 size known; back-compute per-thread throughput).
  - PLAIN i64/f64 unpack (1 ns/row) — confirm against ematix-parquet `bench_bit_unpack` (commit bc8c4c3 referenced in `project_ematix_parquet_varint_optimal.md`).
  - Hash agg (≤100 groups) — Σ.N.f.3 RobinHood baseline.
  - Hash join build/probe — `crates/ematix-flow-core/benches/hash_join_*.rs` if any exist; else accept published.
- [ ] Note any constant the audit can't verify with a current bench. Mark it "stale; calibrate in A.2".
- [ ] Write findings inline into `docs/STAGE_PROFILING_METHODOLOGY.md` as a "2026-05-26 audit" appendix. Do NOT rewrite the body — the body's structure is referenced from 22 PERF_Q*.md files.

**Exit criteria:**
- [ ] Every constant either has a "verified 2026-05-26" tag or an "unverified, in use as published" tag.
- [ ] If ≥3 constants are unverified, A.2 fires. If <3, A.2 is skipped and Phase B starts.

**Next:** A.2 (if needed) or B.1 (if not).

### Story A.2 — re-calibrate stale constants [optional, conditional on A.1]

**Status:** `[ ]` (gated on A.1)
**Time budget:** 4 hours total, distributed.

**Tasks:**
- [ ] For each stale constant identified in A.1, run the corresponding kernel-bench OR back-compute from a known TPC-H scan (e.g. LZ4 throughput from Q06 LZ4 wall-time × known compressed bytes).
- [ ] Update `docs/STAGE_PROFILING_METHODOLOGY.md` constants in place. Tag the row with "(recalibrated 2026-05-26)".

**Exit criteria:**
- [ ] Every constant in the methodology table has a verification tag.
- [ ] No constant moved by >2× without a follow-up note explaining why (would affect floor tables in old PERF_Q*.md files; document the delta but don't retroactively patch them — Phase B overwrites those files).

**Next:** B.1.

---

## Phase B — 22-query sweep [PENDING A]

**Goal:** one `docs/PERF_Q<n>.md` per query, overwriting the 2026-05-25 file. Each writeup follows the existing template in `docs/STAGE_PROFILING_METHODOLOGY.md` §"Per-query writeup template":

1. Wall time — median + σ from current `tpch_triangulation_bench` run (BENCHMARKS.md cell).
2. Physical plan — `displayable(plan).indent(true)`.
3. Per-stage breakdown — operator + `elapsed_compute_ms` + `output_rows`.
4. Function self-time — only where the gap to floor is ≥2×. Skip for near-floor queries.
5. Theoretical floor — using the (re-audited) constants from Phase A.
6. Waste candidates — ranked by confidence × impact, file:line + suggested fix.
7. Findings to capture as memories.
8. Next levers.

**Estimated effort:** 22 stories × 30-60 min each = 11-22 hours. With investigation slop, budget **3 person-days**. Hard cap per story: 2 hours.

**Sequencing:** ordered by `(measured_ms − rough_floor_ms)` from `BENCHMARKS.md`, biggest absolute waste first. The team-internal "queries we lose to DuckDB" priority is *not* the sequencing key — Q22 (we win by 6×) might still be 2× over floor and matter more than Q06 (we lose by 0.4% but the entire field is at the decode wall).

**Initial ranking (rough; refined per-story):**

| Order | Query | ematix ms | DuckDB ms | Naive max-waste signal |
|------:|------:|----------:|----------:|-----------------------|
| 1 | Q21 | 311.87 | 443.53 | win, but biggest absolute ms — likely highest absolute waste |
| 2 | Q09 | 273.41 | 313.32 | win, large multi-join |
| 3 | Q18 | 243.70 | 229.81 | lose 6%, structural after Σ.Q.L10 |
| 4 | Q01 | 235.38 | 237.23 | parity, large scan |
| 5 | Q10 | 231.97 | 408.76 | big win, but still 232 ms |
| 6 | Q05 | 186.25 | 148.97 | lose 25%, known gap |
| 7 | Q08 | 188.76 | 175.86 | lose 7%, known gap |
| 8 | Q17 | 175.26 | 165.93 | lose 6%, known gap |
| 9 | Q07 | 157.48 | 142.25 | lose 11%, known gap |
| 10 | Q03 | 145.74 | 145.58 | tie, multi-join |
| 11 | Q19 | 138.72 | 210.30 | win, scan-heavy |
| 12 | Q20 | 131.47 | 150.94 | win, compound-key agg |
| 13 | Q13 | 95.81 | 273.22 | big win, post-StringView |
| 14 | Q12 | 87.64 | 115.52 | win, lineitem-heavy |
| 15 | Q14 | 85.49 | 137.88 | win — historically at decode floor |
| 16 | Q15 | 77.28 | 95.80 | win, subquery shape |
| 17 | Q06 | 76.08 | 74.62 | lose 2%, decode wall |
| 18 | Q04 | 54.30 | 89.44 | win, small |
| 19 | Q16 | 50.01 | 68.51 | win, small |
| 20 | Q02 | 29.37 | 45.30 | win, small |
| 21 | Q22 | 23.35 | 149.95 | huge win, customer-heavy |
| 22 | Q11 | 11.59 | 30.70 | huge win, tiny |

**Per-query stories below.** Each is named `B.<n>` and is independently executable (any of B.1–B.22 can run before any other after Phase A clears).

### Story B.1 — Q01 verify-against-current [PENDING A]

**Status:** `[ ]`
**Time budget:** 30 min. Q01 was profiled 2026-05-25 with the StringView fix; this is a verify-not-redo.

**Tasks:**
- [ ] Re-run `tpch_triangulation_bench TPCH_QUERIES=1` against current numbers. Update wall-time block in `docs/PERF_Q01.md`.
- [ ] Re-run `stage_profiler TPCH_QUERY=1` (5 trials). Update per-stage block if any operator's elapsed_compute moved >20%.
- [ ] If self-time profile unchanged from 2026-05-25 (no Σ.AG.7-touched code in Q01's hot path), skip the sample step. Document the decision.
- [ ] Update the theoretical-floor block if Phase A re-calibrated any constant.
- [ ] Carry forward the existing "waste candidates" list with a 2026-05-26 verification tag. Note any candidate that's no longer relevant (e.g. landed since).

**Exit criteria:**
- [ ] `docs/PERF_Q01.md` header reflects "Status: re-verified 2026-05-26".
- [ ] Waste candidates either re-confirmed or struck through with rationale.

**Next:** B.2 or B.21 (parallel-eligible; pick the highest-absolute-waste query that hasn't started).

### Story B.<n> — per-query writeup [PENDING A], n ∈ {2 … 22}

**Status:** `[ ]`
**Time budget:** 30-60 min. Hard cap 2 hours.

Each B.<n> shares the same task list. Listed once here; per-story notes follow only where the query has known structural twists.

**Tasks (every B.<n>):**
- [ ] Confirm `examples/tpch/data/sf10/` is the canonical data dir; `lineitem.parquet` is Snappy per `project_sf10_canonical_lineitem_snappy.md`.
- [ ] Run wall-time: `TPCH_QUERIES=<n> TPCH_TRIALS=5 TPCH_WARMUPS=2 ./target/release/examples/tpch_triangulation_bench`. Capture ematix/DuckDB/Polars median + σ. `git checkout BENCHMARKS.md` after.
- [ ] Run per-stage: `TPCH_QUERY=<n> TPCH_TRIALS=5 TPCH_WARMUPS=2 ./target/release/examples/stage_profiler > /tmp/q<n>_stage.log`.
- [ ] Compute per-stage theoretical floor using Phase A constants. Identify stages with `actual / floor ≥ 2×`.
- [ ] **If and only if** the worst stage is ≥2× floor: run a 40-trial sample-capture for self-time analysis (10s `sample` window). Aggregate by function family.
- [ ] **If the worst stage is ≥5× floor:** confirm with one kernel-bench probe (a Criterion mini-bench replicating the hot kernel in isolation) per `project_lever4_microbench_gate_pass.md` discipline.
- [ ] Write `docs/PERF_Q<n>.md` from the existing template. Overwrite the 2026-05-25 file.
- [ ] List waste candidates with `file:line` + suggested fix shape (NOT the implementation).
- [ ] List generalisable findings (anything that's not Q<n>-specific) — these feed Phase C synthesis.
- [ ] List 1-3 next levers, ranked.

**Per-query notes:**

- **B.2 — Q21.** Largest absolute wall (312 ms). Multi-table semi-join shape; `project_sigma_q_l13_to_l16_session.md` notes the L9 Inner-join bloom default-off after Q21 correctness fix (162df4f). Look for: bloom default-on opportunities where correctness now passes; FilterMultiAggSpec coverage on the 3-lineitem-scan shape. **Pattern E (multiple identical scans) was already noted in the 2026-05-25 review; verify the count is still 3.**
- **B.3 — Q09.** Multi-join, compound-key agg (`gby=(nation, o_year)`). Prior review identified Pattern C (compound-key Robin Hood gap). Verify the compound-key Final still routes through stock AggregateExec.
- **B.4 — Q18.** Σ.Q.L10 PushDownLeftSemiRule already in (commit 50825c9). Currently +6% behind DuckDB; prior review tagged it "structural — join order, not kernel". Re-confirm against current per-stage breakdown — Σ.AG.7 may have shifted the share.
- **B.5 — Q01.** See B.1 above (verify-against-current path, not full redo).
- **B.6 — Q10.** Compound-key agg with 7 customer columns, 482k groups. Prior Pattern C entry. Verify the group-by FD simplifier opportunity (memory-noted lever #6 in the 2026-05-25 ranking).
- **B.7 — Q05.** 6-way join, 2-key supplier-nation = customer-nation constraint. Prior review tagged it "needs join-reorder via CBO, multi-quarter." Re-confirm the per-stage finding — is the dominant waste actually the 2-key join, or is it elsewhere?
- **B.8 — Q08.** 7-way join. Per-stage: confirm which join is dominant. Σ.S.B cascading-L9 (rejected — `project_sigma_sb_cascade_neg.md`) may still be reconsidered if the dominant waste maps to its target.
- **B.9 — Q17.** Two lineitem scans (main + avg-subquery). Σ.P SharedSubtreeExec exists for Q15-shape; Q17 needs CSE generalisation. Confirm whether the avg-subquery scan is still 247 ms / 60M rows in the per-stage breakdown.
- **B.10 — Q07.** 6-table join + L9 on both nation→supplier and nation→customer edges. Per-stage: confirm L9 is firing on both edges.
- **B.11 — Q03.** 3-way join, BUILDING segment filter. Lineitem filter `l_shipdate>1995-03-15` — does BridgeFilter handle this? Past attempt rejected per `project_path_a_i32_column_pair_rejected.md`, but that was for *column-pair* comparison; single-column range should already work. Confirm.
- **B.12 — Q19.** OR-of-AND disjunctive filter. `project_sigma_e5_late_mat_spike_scope.md` noted DataFusion already pushes per-table predicates. Confirm the current ratio is still ratio-1.5× over floor.
- **B.13 — Q20.** Compound-key agg `gby=(l_partkey, l_suppkey)`, 5.44M groups. Pattern C, largest-group cardinality in the suite. **Highest-confidence candidate for compound-key Robin Hood arc.**
- **B.14 — Q13.** Post-StringView fix is winning by 2.85×. Verify it's now genuinely near floor. Memory `project_sigma_e5_q13_root_cause.md` flagged Utf8View buffer inflation; check if that's still load-bearing.
- **B.15 — Q12.** lineitem-heavy with `l_shipdate` BETWEEN + `l_receiptdate > l_commitdate`. Mixed single-col-range + two-col-compare. Two-col-compare rejected (`project_path_a_i32_column_pair_rejected.md`), but the single-col range may be unfiltered too — confirm.
- **B.16 — Q14.** Per `project_q14_decode_floor.md`, "all four cheap levers tested + rejected; remaining options are polars-parquet integration (multi-session) or accept." Verify floor table to confirm. Likely a short writeup.
- **B.17 — Q15.** SharedSubtreeExec already landed (Σ.P). Verify per-stage shows it firing.
- **B.18 — Q06.** Pure scan benchmark. `project_q06_sf10_polars_gap_wall.md` notes it's at the decode wall. LZ4 sibling exists but isn't canonical. Floor table likely shows ≤2× — near-floor query.
- **B.19 — Q04.** Lineitem `l_receiptdate > l_commitdate` two-column compare. Same lineage as Q12 — confirm whether the filter is currently pushed down or evaluated as a post-scan FilterExec.
- **B.20 — Q16.** Group-by on (`p_brand`, `p_type`, `p_size`) with `count(DISTINCT ps_suppkey)`. Verify whether the DISTINCT count is using stock 2-stage agg or a Robin Hood path.
- **B.21 — Q02.** Already 24% ahead of DuckDB (37 vs 48). Verify near-floor.
- **B.22 — Q22.** 6× ahead of DuckDB; the suite's biggest win. Verify near-floor; document why this shape wins so big (likely the StringView fix + customer-heavy hot path).
- **B.23 — Q11.** 12 ms total. Even if 5× over floor, the absolute waste is ≤10 ms — bottom of any candidate list. Verify near-floor; short writeup expected.

(Story numbers are B.1 ... B.23 — Q01 is B.1, then B.2 = Q21, B.3 = Q09, ... B.23 = Q11, per the absolute-waste ordering above.)

**Exit criteria for Phase B:**
- [ ] 22 PERF_Q*.md files with "Status: re-profiled 2026-05-26" headers.
- [ ] Each has either: (a) at least one ranked waste candidate, OR (b) an explicit "near-floor, no waste candidate" verdict with a one-line justification.
- [ ] Generalisable findings (the "findings to capture as memories" sections) aggregated mentally as input to Phase C — do not write the synthesis yet.

---

## Phase C — cross-query synthesis [PENDING B]

**Goal:** one document, `docs/PERF_REVIEW_2026_05.md`, that:
1. Lists every waste candidate from every PERF_Q*.md, normalised to absolute ms × confidence.
2. Identifies shared waste patterns (≥3 queries paying for the same shape).
3. Clusters candidates into proposed arcs (Σ.AH.1, Σ.AH.2, …) with predicted impact ranges.
4. Ranks arcs by `Σ(query absolute waste-ms) × confidence × generalisability`.
5. Flags previously-rejected rejections that the new evidence suggests revisiting.

**Estimated effort:** 1 day. Hard cap 2 days.

### Story C.1 — extract + normalise candidates [PENDING B]

**Status:** `[ ]`
**Time budget:** 3 hours.

**Tasks:**
- [ ] Walk PERF_Q01.md … PERF_Q22.md. Extract each waste candidate into a flat table.
- [ ] Columns: `query | candidate_name | absolute_waste_ms | confidence (H/M/L) | file:line | suggested_fix_shape | generalisable_pattern_tag`.
- [ ] `absolute_waste_ms` = stage's observed ms − stage's floor ms. Sum if a candidate covers multiple stages.
- [ ] Confidence H = self-time confirmed + kernel-bench probe ran. M = self-time confirmed only. L = floor-math only, no profile confirmation.
- [ ] `generalisable_pattern_tag` is freeform but reuse tags across queries when the shape matches.

**Exit:** flat table in a section of `docs/PERF_REVIEW_2026_05.md` (start drafting the doc here).

### Story C.2 — identify shared patterns [PENDING C.1]

**Status:** `[ ]`
**Time budget:** 2 hours.

**Tasks:**
- [ ] Group rows by `generalisable_pattern_tag`. Any tag with ≥3 distinct queries is a "pattern".
- [ ] For each pattern, write a paragraph: shape description, queries affected, total ms across queries, suggested fix shape.
- [ ] Note overlap with prior review's Patterns A-E (BridgeFilter gap, L9 cross-barrier propagation, compound-key agg, SIMD LIKE wire-up, identical-subtree CSE). Patterns that no longer apply (because Σ.AG.7 / 0.16.2 / etc. moved the line) get explicitly removed.
- [ ] Note new patterns the 2026-05-25 review missed.

**Exit:** patterns section of `docs/PERF_REVIEW_2026_05.md`.

### Story C.3 — cluster into proposed arcs + rank [PENDING C.2]

**Status:** `[ ]`
**Time budget:** 3 hours.

**Tasks:**
- [ ] Group patterns into proposed arcs. Naming: Σ.AH.1, Σ.AH.2, … (Σ.AH is this survey's namespace).
- [ ] For each arc:
  - **Hypothesis**: what the lever does, in one sentence.
  - **Queries impacted**: from C.2.
  - **Predicted impact range**: lower bound = sum of absolute_waste_ms for H-confidence rows; upper bound = sum across H+M+L. Both expressed in ms AND as 22q-geomean-pp delta.
  - **Prior status**: never tried / rejected when / partially tried.
  - **Generalisability**: clear-cut (universal shape), shape-narrow (recurs in 3-5 queries), one-query (out of scope per hard rule #3).
  - **Effort estimate**: rough person-week range.
- [ ] Rank arcs by `predicted_impact_range_lower × confidence × generalisability`. Don't over-mathify — qualitative ordering is fine.
- [ ] Flag the top 3-5 arcs as Phase D inputs.

**Exit:** arc table + ranking in `docs/PERF_REVIEW_2026_05.md`.

### Story C.4 — flag rejection re-looks [PENDING C.3]

**Status:** `[ ]`
**Time budget:** 1 hour.

**Tasks:**
- [ ] For each top-3 arc from C.3, check whether a memory file documents a prior rejection of similar shape. Memory dir: `~/.claude/projects/-Users-ryanevans-RustroverProjects-ematix-flow/memory/`.
- [ ] Known rejection candidates to check (not exhaustive):
  - `project_path_a_i32_column_pair_rejected.md` — column-pair pushdown
  - `project_sigma_qm_slice2_rejected.md`, `project_sigma_qm_slice4_spike_rejected.md` — join-order rewrites
  - `project_sigma_r2_rejected.md` — RobinHoodAvgF64Exec
  - `project_lever4_full_build_rejected.md` — Lever #4 full build
  - `project_emat_parallelism_budget_2x_rejected.md` — 2× emat parallelism
  - `project_sigma_h1d_rejected.md` — numeric-keyed agg
  - `project_sigma_ka_rejected.md` — separate numeric rule
  - `project_sigma_q_l11_rejected.md` — integer-key compression
  - `project_sigma_q_l12_rejected.md` — SIMD-tagged hash agg
- [ ] For each rejection that overlaps a top arc, document: what was rejected, why, what changed since, and whether the new evidence flips the rejection.
- [ ] **Do not flip any rejection in this story** — the flag is for Phase D to handle. Hard rule #9: no premature implementation.

**Exit:** rejection-relook section in `docs/PERF_REVIEW_2026_05.md`.

**Exit criteria for Phase C:**
- [ ] `docs/PERF_REVIEW_2026_05.md` exists with sections: flat candidate table, patterns, arc ranking, rejection re-looks.
- [ ] Top 3-5 arcs identified and ranked; each has hypothesis + impact range + effort estimate.

---

## Phase D — arc roadmap [PENDING C]

**Goal:** one arc-plan-shell per top-3 arc from Phase C, parked as a sibling document under `docs/plans/sigma-ah-arc-<N>.md`. Each shell has: hypothesis, bench-gate criteria, story skeleton (no task detail), risk notes. **No implementation.** Detailed planning happens when one arc is picked as the next `CURRENT.md` after this plan archives.

**Estimated effort:** 1 day for 3 arcs. Hard cap 2 days.

### Story D.1 — draft arc shell for top arc [PENDING C]

**Status:** `[ ]`
**Time budget:** 3 hours.

**Tasks:**
- [ ] Write `docs/plans/sigma-ah-arc-1.md`. Template:
  ```
  # Σ.AH.<N> — <name>
  
  **Status:** drafted, not active
  **Parent:** docs/PERF_REVIEW_2026_05.md (Phase C ranking #N)
  **Hypothesis:** <one sentence>
  **Queries impacted:** <list>
  **Predicted impact range:** <ms> / <pp at 22q geomean>
  **Effort estimate:** <person-weeks>
  **Risk level:** L/M/H
  
  ## Bench gate (ship-if / reject-if)
  - Microbench: <criterion + threshold>
  - Wall-time: <22q geomean threshold AND per-query no-regression bar>
  - Reject-if: <explicit conditions; if microbench passes but wall-time fails, reject per Σ.R.2 precedent>
  
  ## Hard constraints (inherited)
  - No new PhysicalOptimizerRule (codegen-tax)
  - Sibling-crate if kernel; pre-plan walker if pattern recognition
  - TDD
  - No TPC-H-specific hardcoding
  
  ## Story skeleton (no tasks)
  Story 1 — kernel scaffold + correctness tests
  Story 2 — kernel optimisation pass + microbench gate
  Story 3 — DataFusion integration (sibling-op or pre-plan walker)
  Story 4 — wall-time bench gate + opt-in flag
  Story 5 — soak + default-on flip
  
  ## Risks + watch-items
  - <kernel-bench-doesn't-predict-wall-time risk; ref Σ.R.2>
  - <codegen-tax risk if optimizer rule; ref optimizer-codegen-sensitivity>
  - <generalisability risk if pattern was actually shape-narrow>
  
  ## References
  - Phase C ranking entry: <link>
  - Related rejection (if any): <link>
  - Related success (if any): <link>
  ```
- [ ] Bench-gate criteria are not optional and not vague. Numbers, thresholds, what passes, what fails.

### Story D.2 — draft arc shell for 2nd arc [PENDING D.1]

**Status:** `[ ]`
**Time budget:** 2 hours (template is set after D.1).

### Story D.3 — draft arc shell for 3rd arc [PENDING D.2]

**Status:** `[ ]`
**Time budget:** 2 hours.

### Story D.4 — sequencing memo + next-CURRENT recommendation [PENDING D.3]

**Status:** `[ ]`
**Time budget:** 1 hour.

**Tasks:**
- [ ] In `docs/PERF_REVIEW_2026_05.md` append a "Recommended sequencing" section.
- [ ] Pick one arc to be the next `CURRENT.md`. Justify in 3-4 sentences: why this one first, what it unblocks, what risks it carries.
- [ ] Compare with the archived plan's L13 / L14 sequencing. If the new top arc supersedes L13: document that decision. If it composes with L13: document the composition. If L13 is still the top arc independently: archive this plan with a "Σ.T V5 Tier 1 resumes" note and the archived plan becomes the next CURRENT.md.

**Exit criteria for Phase D:**
- [ ] 3 arc shells exist under `docs/plans/`.
- [ ] `docs/PERF_REVIEW_2026_05.md` has a "Recommended sequencing" section.
- [ ] This `CURRENT.md` is ready to archive in favor of the chosen arc's CURRENT.md.

**Next:** archive Σ.AH; promote chosen arc to CURRENT.md; update `docs/progress/CURRENT.md`.

---

## Open questions

Tagged so they don't block work; each carries a default.

### OQ-AH-A: Phase A constants — re-run kernel benches or accept memory-doc evidence?

If `project_q06_sf10_polars_gap_wall.md` and `project_ematix_parquet_lz4_decode_bug.md` already provide back-computable throughput numbers, do we re-run the benches?

**Default: accept memory-doc evidence as published.** Re-run only if the memory-doc number is >2 years old or has no methodology trail. A.2 runs only if A.1 flags ≥3 unverified constants.

### OQ-AH-B: Phase B per-story cap — 60 min target or 2-hour hard cap?

If a query investigation gets interesting at 90 minutes, do we continue or stop?

**Default: stop at 2 hours regardless of perceived progress.** Mark "near-floor, no waste candidate" or "investigation incomplete — see <notes>". Phase C will pick up the threads. The survey discipline is the value, not the depth of any single query.

### OQ-AH-C: Phase C top-arc count — 3 or 5?

3 is tractable for Phase D; 5 covers more risk against a top-3 turning out shape-narrow.

**Default: 3 detailed arc shells, with a 4th-and-5th listed only by name + 1-paragraph hypothesis.** If Phase D capacity remains, expand 4 and 5 into full shells.

### OQ-AH-D: What if Phase C finds no high-confidence arcs ≥5 pp?

If the suite is at-floor and there are no >5pp candidates, do we ship the review as-is and pause perf work?

**Default: ship the review, set Σ.AH.1 as a long-tail low-priority arc, and the next CURRENT.md becomes whatever non-perf work is on the roadmap (e.g. resume the archived Σ.T L13/L14 if it still passes a re-cost-benefit test, OR pivot to sidecar/distributed/UI/etc).** The review documents the ceiling; that's its value.

---

## Risks + watch-items

| Risk | Mitigation |
|---|---|
| **Investigation slop on a "interesting" query consumes the budget.** Q01 / Q05 / Q18 are all known structural and could each absorb a week. | Hard 2-hour cap per query. If a query hits cap, capture state to its PERF_Q<n>.md and move on. Phase C aggregates incompleteness. |
| **Memory-doc rejection cited as "still rejected" when underlying kernel has changed.** Q14 decode-floor lever rejections predate ematix-parquet 0.16.2. | Phase C Story C.4 explicitly flags re-look candidates — but does not flip them. Re-look decisions move to Phase D arc shell. |
| **Floor constants in methodology turn out to be wrong, mid-Phase-B.** Phase A is supposed to catch this, but a constant might pass A.1 and turn out wrong against, e.g., Q06's measured decode rate. | Each Phase B story computes floor and observed; if observed < floor (impossible per definition), flag the constant and pause Phase B until the constant is patched. Document the gap in the PERF_Q<n>.md file's floor block. |
| **A "candidate" turns out to be an existing-but-undefaulted lever.** E.g. EMAT_ADAPTIVE_REORDER (per `project_sigma_l3c_reverted.md`) is opt-in but its kernel is shipped. | Phase C Story C.1 must check for an existing env-flag-only path before logging a candidate as "needs implementation". Existing-but-opt-in candidates get a separate column in the candidate table. |
| **Phase D arc shells are too detailed and trigger premature implementation.** Hard rule #9. | Story D.* tasks explicitly list "skeleton, no task detail" and "no implementation". The shell template enumerates story names without their tasks. |
| **Phase C synthesis re-litigates the entire 2026-05-25 review.** Wasteful if the new numbers haven't moved the line much. | Phase C explicitly starts from "what's different post-Σ.AG.7" and "which prior patterns no longer apply". The 2026-05-25 review's PERF_REVIEW_SF10.md is referenced, not rewritten. |

---

## Out of scope

Cited so the boundary is explicit:

- **Implementation of any arc.** Lives in a future CURRENT.md.
- **Re-running 22q at SF=1 or SF=100.** This plan is SF=10 only. SF=1 microbench correctness is held by existing CI; SF=100 cluster-scale is a separate (distributed) workstream per `project_distributed_is_shipped.md`.
- **Recalibrating cross-engine benchmarks (DuckDB version, Polars build).** The triangulation-bench uses the existing pinned versions. Version-bumping is outside this plan.
- **Documentation site updates.** `ematix.dev` updates ride with arc PRs, not this survey.
- **Per-query SQL rewrites.** Per hard rule #3 — no TPC-H-specific hardcoding.

---

## Cross-references

- Methodology: [`docs/STAGE_PROFILING_METHODOLOGY.md`](../STAGE_PROFILING_METHODOLOGY.md)
- Prior survey (will be partially overwritten): [`docs/PERF_Q01.md`](../PERF_Q01.md) … [`docs/PERF_Q22.md`](../PERF_Q22.md)
- Prior synthesis (will be superseded by `PERF_REVIEW_2026_05.md`): [`docs/PERF_REVIEW_SF10.md`](../PERF_REVIEW_SF10.md)
- Current bench numbers: [`BENCHMARKS.md`](../../BENCHMARKS.md) — 2026-05-26 refresh
- Bench env SOP: memory `feedback_full_bench_env_checklist.md`
- Pin-bump trap: memory `feedback_patch_crates_io_version_match.md`
- Codegen-tax precedent: memory `project_optimizer_codegen_sensitivity.md`
- Kernel-bench-doesn't-predict-wall-time precedent: memories `project_lever4_microbench_gate_pass.md`, `project_sigma_r2_rejected.md`
- No-hardcoding rule: memory `feedback_no_tpch_hardcoding.md`
- Archived plan (L13 / L14): [`docs/plans/archive/2026-05-25-sigma-t-v5-tier-1.md`](./archive/2026-05-25-sigma-t-v5-tier-1.md)
- Tooling: [`crates/ematix-flow-core/examples/stage_profiler.rs`](../../crates/ematix-flow-core/examples/stage_profiler.rs), [`crates/ematix-flow-core/examples/tpch_triangulation_bench.rs`](../../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs)
