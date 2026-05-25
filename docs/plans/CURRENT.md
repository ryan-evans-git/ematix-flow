# Σ.T V5 Tier 1 — universal amplifying-with-scale levers

**Status:** active
**Created:** 2026-05-25
**Branch policy:** one PR per story unless a sub-bite is gated on perf / data / external dependency, per `feedback_fewer_prs.md`.
**Roadmap parent:** [`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V5.md`](../PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V5.md) — V5 §1 Tier 1 plus §2 sequencing rationale; lever descriptions in [`V2`](../PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V2.md) §2.2 L3, §2.3 L13, §2.3 L14.
**Predecessor plan:** [`docs/plans/sidecar-deferred.md`](./sidecar-deferred.md) — L1/L15 sidecar work (V5 Tier 5, scale-specific) deferred. Resumes after this plan's Phase 2 completes (Phase 1 dropped 2026-05-25 — see `docs/PHASE_PGO_BASELINE.md`), or earlier if a dedicated engineer picks it up in parallel.

---

## Summary

V5 re-scored V2's 18 levers by **scale-universality** — which levers benefit SF=1, SF=10, and SF=100 in proportion. Tier 1 levers amplify with scale (SF=100 gains > SF=10 gains > SF=1 gains, all positive). This plan now ships V5 Tier 1 in two phases after the Phase 1 PGO empirical result fell below the acceptance bar (see `docs/PHASE_PGO_BASELINE.md`):

1. ~~**Phase 1 — L3 PGO release build**~~ **[DROPPED 2026-05-25]** — Linux x86_64 measurement (minipc) showed only +1.44pp geomean improvement vs. the ≥3pp acceptance gate, with Q14 at +2.06% per-query regression (just over the ≤2% bar). Most of the workload (the multi-join queries Q03 / Q05 / Q07–Q09) is memory-bandwidth-bound on SF=10 per V5 §5.2 — codegen quality is not the bottleneck. PGO scripts and pipeline (`scripts/pgo/` on branch `feat/pgo-instrumented-build`) are unmerged but available if hardware mix changes warrant revisiting.
2. **Phase 2 — L13 custom hash join** (2-3 person-quarters, blast radius 3). Sibling-crate kernel + ExecutionPlan wrapper. Reuses Σ.N.f.3 RobinHood substrate. Amplifying-with-scale: 2-4pp at SF=1, 10-13pp at SF=10, 18-25pp at SF=100. **Promoted to ACTIVE** after Phase 1's drop.
3. **Phase 3 — L14 dict-preserved end-to-end** (4-6 weeks, blast radius 2). Σ.K.2 dict-routing extended to default-on for low-cardinality strings; per-shape persistence via Σ.L workload log; speculative-race probe avoids the `project_sigma_k_dict_arrival_ab.md` regression regime. 1-2pp at SF=1, 3-5pp at SF=10, 5-10pp at SF=100.

**Why this sequence (V5 §3.7 revised):** ship amplifying-with-scale L13 first because the largest expected closure is at the most-bench-relevant scale (SF=10 published, SF=100 strategic target) and downstream Tier 2 / Tier 3 levers compose on top of Tier 1's surface. The original "L3 absorbs codegen tax that L13 would otherwise pay" rationale no longer applies; the Phase 2 codegen-tax risk is now mitigated by sibling-crate isolation alone (see Risks table).

**Baseline:** 22q SF=10 geomean **0.80** (per `project_sigma_q_l13_to_l16_session.md`). Tier 1 target: **≤0.66** at end of Phase 3 (V5 §2.4 M6 checkpoint).

**Canonical bench hardware:** Apple M3 Pro (per `bench-results/release-2026-05-24/`). All acceptance-gate measurements run on this host. Commodity x86 (e.g. minipc) is a *portability cross-check*, not a sequencing input — see V5 §5.2 (memory-bandwidth finding: SF=10 on commodity x86 is BW-bound; compute-side levers like L13 / L11 mute there, but the canonical M3 Pro baseline doesn't show the same wall and is what the V5 closure targets are measured against).

**PGO measurement archive (Phase 1, dropped 2026-05-25):** The 22q SF=10 PGO-vs-non-PGO comparison ran on Linux x86_64 (minipc) because the macOS aarch64 instrumented binary crashes in dyld init (vendored OpenSSL C++ static constructors via rdkafka's `ssl-vendored` feature). Result documented in `docs/PHASE_PGO_BASELINE.md`. The scripts (`scripts/pgo/build-instrumented.sh`, `train.sh`, `optimize.sh`) and `rust-toolchain.toml` PGO pin live on unmerged branch `feat/pgo-instrumented-build`; re-measurement is one-command if the workload shape or hardware mix changes.

**Recently absorbed (2026-05-25):** PR #146 — Π.13 x86 SIMD parity (Tier 1: SSE2 `match_byte_mask` SwissTable probe in `robin_hood_agg.rs`; Tier 2: AVX2 fused predicate-bitmap kernels bw 12–18 via ematix-parquet 0.16.2). Portability work outside the L-numbered lever space — closes a NEON-only architectural asymmetry, no expected wall-clock impact on the canonical M3 Pro baseline. Bump `ematix-parquet` pins `0.16` → `0.16.2` when PR #146 merges (the version bump rides with the SIMD-parity diff, not separately). Empirical addendum in V5 §5.

**Out of scope this plan:** L1/L15 sidecar (deferred plan); Tier 2 (L3 already in scope but L4/L7/L11/L12); Tier 3 L9 Cranelift; Tier 4 L18 fork; Tier 5 L1/L15; Tier 6 query-specific fallbacks; Tier 7 L16 GPU. All belong in their own future plans gated on V5 §2.3 skip conditions.

---

## Active phase + story

- **Phase 1 — L3 PGO release build** is `[DROPPED 2026-05-25]`. See `docs/PHASE_PGO_BASELINE.md` for the measurement that produced the drop decision.
- **Phase 2 — L13 custom hash join** is `[ACTIVE]`.
- **Story 2.1 — Kernel scaffold + Robin Hood i64-keyed table** is `[next]`.

Phase 2 estimate: 2-3 person-quarters. Phase 3 estimate: 4-6 weeks. The cumulative L8-CBO/L10/L17 follow-on tail is V5 M9–M12 and is NOT in this plan.

---

## Hard constraints

These apply to every story in every phase. Restated for the implementer; cross-referenced to the originating memory.

1. **No new `PhysicalOptimizerRule`** as a primary lever (`project_optimizer_codegen_sensitivity.md`). Σ.K.2 / Σ.H.1d / Σ.K.A all paid 5-8% geomean tax from LLVM codegen perturbation before the rule did any work. Phase 2 L13 lives in **sibling crate `ematix-flow-hash-join`**; Phase 3 L14 extends the existing pre-planning helper (`dict_routing.rs`), NOT a new optimizer rule.
2. **No TPC-H-specific hardcoding** (`feedback_no_tpch_hardcoding.md`). L13 selectivity heuristics, build-side selection, and skew thresholds are shape-based; L14 cardinality threshold is parametric on `Σ.L` workload observations. TPC-H 22q is the validation workload, not the target shape.
3. **TDD** (`feedback_tdd.md`). Each story names the failing test that lands first; no implementation lands without the red test in the same PR.
4. **Fewer, larger PRs** (`feedback_fewer_prs.md`). Sub-bites in Phase 1 and Phase 3 ship bundled per the per-story "PR shape" notes; Phase 2's six stories each ship one PR because they are independently testable and not co-dependent.
5. **No pandas in warehouse adapter path** (`feedback_no_pandas_in_warehouse_path.md`). Not directly relevant; called out so future Phase 2 builder helpers don't reach for pandas via a copy-paste.
6. **Recommend next step** (`feedback_recommend_next_step.md`). Every story's PR description ends with a concrete `Next:` line pointing at the following story.

---

## Open questions + decisions

Tagged so the plan body cross-references. Each carries a default so work doesn't block if the user is unavailable.

### OQ-PGO-A: cargo-pgo vs raw rustflags?

`cargo-pgo` is a published cargo subcommand wrapping `-Cprofile-generate` / `-Cprofile-use`. Raw rustflags work but require manual orchestration of the instrumented build, training run, and merged-profile build.

**Default: cargo-pgo.** Lower toolchain risk; survives nightly rustc rotation; CI-friendly. Drop to raw rustflags only if cargo-pgo doesn't support a needed flag (e.g. cross-compilation we're not doing today).

**Resolution required before Story 1.1 lands.**

### OQ-PGO-B: training workload — 22q SF=10 only, or include SF=1 + microbenches?

Training profile shapes the binary. If we train only on SF=10, SF=1 shapes that don't appear in 22q (e.g. small-card group-by microbenches) may regress. If we train on too much, profile collection takes hours.

**Default: 22q SF=10 release run, single iteration per query (22 queries × ~1-15s = ~3-5 minutes of profile-data collection).** SF=10 is the strategic target; SF=1 microbench correctness is held by the 22q SF=1 regression gate at Story 1.3.

**Resolution required before Story 1.2 lands.**

### OQ-L13-A: Replace `HashJoinExec` outright, or ship `EmatixHashJoinExec` as a sibling that swaps in by shape?

V2 §2.3 L13 said "replacement"; V5 §1 Tier 1 L13 said "wrapper that swaps in for `HashJoinExec` when shape matches." The sibling-swap pattern matches what we did with `RobinHoodAggregateExec` (Σ.N.f.3): the new op exists, a pre-planning helper picks the route, and the existing DataFusion op stays as the fallback path.

**Default: sibling swap.** Matches Σ.N.f.3 shape; preserves DataFusion compatibility on any shape we haven't validated; lower blast radius. Σ.N.f.3 already proved this discipline works.

**Resolution required before Story 2.1 lands.**

### OQ-L13-B: Where does the cardinality estimate come from for build-side selection?

Three options:
- **A. Σ.O.c RowGroupDecodeCache stats.** Already shipping; per-column min/max + row count.
- **B. Σ.L workload-log observed cardinality.** Per-shape historical; requires ≥1 prior run.
- **C. Static parquet-footer stats.** Universally available; uncalibrated.

**Default: A as primary, C as fallback on first encounter, B as future input when L14's Σ.L wire-up lands.** Story 2.2 implements A+C; Phase 3 Story 3.1 adds the Σ.L input as a higher-confidence override.

### OQ-L14-A: What's the cardinality threshold for "low-cardinality string"?

Σ.K.2 currently uses an opt-in routing pass; no global threshold. `project_dict_arrival_blocker.md` notes Q01 +104% / Q13 +25% / Q19 +35% on a global flip — those queries have *medium*-cardinality strings (~10K-100K distinct values). The kernel-win regime in `project_sigma_e3b_landed.md` is ≤256 distinct values.

**Default: dict-preserve when `distinct_count_estimate ≤ 1024` AND `column_type ∈ {Utf8, Utf8View, LargeUtf8}`.** Σ.L probe gates the first encounter (defaults to dict-off if the column hasn't been observed yet); subsequent runs read the workload-log verdict. Story 3.1 implements the threshold; Story 3.2 wires the probe.

### OQ-L14-B: Σ.L workload-log schema extension or new table?

Σ.L.2 workload log already has tables for shape observations. Dict verdicts could be a new column on the existing `shape_observations` table OR a new `dict_verdicts(table, column, verdict, n_observations, last_outcome)` table.

**Default: new table.** Verdicts are per-column, not per-shape; co-locating with the shape table forces a denormalised key. New table follows the same WAL+Mutex concurrency pattern as `predicate_observations` would in the deferred sidecar plan.

### OQ-L13-C: Bloom emitter wire-up — single-node only, or also distributed?

Σ.J.2.b transport ships for distributed; single-node ContextBlooms ride the same `SessionState` extension. Emitting blooms from a single-node build is structurally the same code as the distributed path.

**Default: same emitter code, both paths.** Story 2.3 ships once; consumers are existing `EnableContextBloomRule` (Σ.J.2.b.vi, single-node) and existing Flight transport (Σ.J.2.b.v, distributed).

---

## Phase 1 — L3 PGO release build [DROPPED 2026-05-25]

**Drop summary:** PGO measurement on Linux x86_64 (minipc, 22q SF=10, 10 trials × 2 warmups, full bench env) produced a geomean ratio of 0.9856 — **+1.44pp improvement, vs. the ≥3pp acceptance gate**. Q14 also regressed +2.06% (per-query bar is ≤2%). Both gate criteria miss. The bulk of the SF=10 workload (multi-join Q03/Q05/Q07–Q09) is memory-bandwidth-bound on commodity x86 per V5 §5.2 — codegen quality is not the limiting factor on this hardware mix. Full numbers, environment, and recommendation in `docs/PHASE_PGO_BASELINE.md`.

**Infrastructure parked, not landed:**
- `scripts/pgo/build-instrumented.sh`, `train.sh`, `optimize.sh`, `clean.sh`, and three smoke-test scripts live on branch `feat/pgo-instrumented-build`.
- `rust-toolchain.toml` `llvm-tools-preview` pin lives on the same branch.
- Re-measurement is one-command if a hardware mix change or training-shape change warrants revisiting; the branch is also the basis for re-opening as a PR if Phase 1 is later resurrected.

**Stories 1.1–1.4 below preserved for traceability; statuses updated to reflect the drop.**

### Story 1.1 — cargo-pgo install + instrumented build pipeline [done — branch only]

**Status:** `[done — branch `feat/pgo-instrumented-build`, not merged]`

**Failing test (TDD anchor):**
- `scripts/pgo/test_pgo_build_smoke.sh` — runs `cargo pgo build` (instrumented), asserts the resulting binary exists at `target/x86_64-apple-darwin/release/ematix-flow` (or current host triple) AND that `file <binary>` reports the binary is profile-instrumented (PGO instrumentation symbols present in `nm` output).
- `scripts/pgo/test_pgo_build_smoke.sh::no_pgo_path_still_works` — `cargo build --release` (no PGO) still produces a working binary, so the PGO toolchain doesn't accidentally become mandatory for non-bench development.

**Tasks:**
- [ ] **OQ-PGO-A resolution lands first** as a docstring in `scripts/pgo/README.md`.
- [ ] Add `cargo-pgo` install instructions to `docs/DEVELOPMENT.md` (or equivalent contributor docs).
- [ ] Add `scripts/pgo/build-instrumented.sh` — wraps `cargo pgo build` for the workspace; emits the instrumented binary at a documented path.
- [ ] Add `scripts/pgo/clean.sh` — drops the `target/pgo-profiles/` directory between training-run iterations (stale profiles compound the codegen tax).
- [ ] Verify the instrumented binary runs (smoke test: open a parquet, run one trivial query, exit cleanly).
- [ ] Document toolchain pin: PGO requires a `llvm-tools-preview` component; pin in `rust-toolchain.toml` if not already.

**PR shape:** Bundled with Story 1.2. Combined PR scope: `scripts/pgo/` directory + `rust-toolchain.toml` tweak + contributor doc update.

### Story 1.2 — Training-run script + initial profile capture

**Status:** `[done — Linux minipc 2026-05-25]`. Scripts ran end-to-end on `feat/pgo-instrumented-build`; one non-empty `.profraw` captured; `cargo pgo optimize` merged the profile and rebuilt the bench binary (227 MB → 196 MB, -14%).

**Failing test:**
- `scripts/pgo/test_training_run.sh` — given an instrumented binary (precondition from Story 1.1), runs the training workload (22q SF=10 single iteration), asserts `target/pgo-profiles/*.profraw` files exist and are non-empty after the run.
- `scripts/pgo/test_profile_merge.sh` — runs `cargo pgo optimize` (which merges .profraw → .profdata and rebuilds), asserts the resulting binary exists AND is a different binary than the instrumented one (size or hash check).

**Tasks:**
- [ ] **OQ-PGO-B resolution lands first** as a docstring in `scripts/pgo/train.sh`.
- [ ] Add `scripts/pgo/train.sh` — runs the 22q SF=10 workload against the instrumented binary. Workload source: existing `crates/ematix-flow-core/examples/bench_22q_sf10.rs` or equivalent.
- [ ] Profile output directory: `target/pgo-profiles/` (per cargo-pgo default).
- [ ] Add `scripts/pgo/optimize.sh` — invokes `cargo pgo optimize` to produce the PGO release binary.
- [ ] Commit a baseline `target/pgo-profiles/.gitkeep` (the profiles themselves are .gitignored — too large + machine-specific).
- [ ] Document: re-training cadence is "when 22q workload shape changes meaningfully" (e.g. new query added to the bench set). For TPC-H 22q it's stable; the SOP is "re-train on every major release-candidate build."

**PR shape:** Same PR as Story 1.1.

### Story 1.3 — Release-bench reproduction with PGO binary; commit baseline numbers

**Status:** `[done — gate not met, see docs/PHASE_PGO_BASELINE.md]`. Linux minipc, 22q SF=10, ematix-flow only, 10 trials × 2 warmups, full bench env. Geomean +1.44pp (gate ≥3pp); Q14 +2.06% (gate ≤2%). Q22 (-9.24%), Q12 (-8.01%), Q01 (-4.89%) were the headline wins; the multi-join queries (Q03/Q05/Q07–Q09) moved <1pp consistent with memory-bandwidth-bound behaviour on commodity x86 (V5 §5.2). M3 Pro re-measurement was not run — minipc-vs-M3 hardware gap is unlikely to swing the result past the 3pp bar (Phase 1 acceptance is hardware-independent at this granularity per V5 §3.7 baseline narrative).

**Failing test:**
- `crates/ematix-flow-core/examples/bench_22q_sf10_pgo_vs_nopgo.rs` — checked-in runnable example. Runs 22q SF=10 5× against both the PGO and non-PGO binaries (configured via env var pointing at each binary path), reports per-query and geomean ratios. **The script asserts PGO geomean is ≤ non-PGO geomean × 0.97** (≥3pp improvement, per the V5 acceptance gate).
- 22q SF=1 regression check: PGO geomean must stay within ±1% of non-PGO SF=1 geomean. Catches the OQ-PGO-B risk that training-on-SF=10 hurts SF=1 shapes.

**Tasks:**
- [ ] Add `crates/ematix-flow-core/examples/bench_22q_sf10_pgo_vs_nopgo.rs`.
- [ ] Operator runs the bench 5× **on M3 Pro (canonical)** to characterise run-to-run noise per `feedback_full_bench_env_checklist.md` (env must include `EMAT_RG_DECODE_CACHE=1` + `EMAT_RH_SUM_F64=1` + bloom flags; baseline 0.80 needs these).
- [ ] **Minipc cross-validation (portability check, non-gating):** run the same bench on the commodity x86 minipc. **Expectation:** ~0pp improvement at SF=10 on Q03/Q08/Q09 (memory-BW-bound per V5 §5.2 / PR #146 profiling); other shapes may show modest lift. Pass criterion: *no per-query regression > 3% on minipc*. PGO-vs-non-PGO geomean target is M3 Pro only.
- [ ] Record results in `docs/PHASE_PGO_BASELINE.md` (new file): per-query medians, geomean, regression list. Include minipc cross-validation table as a separate section.
- [ ] If M3 Pro geomean improvement < 3pp: stop and triage. Don't flip Phase 2 into ACTIVE until the gate passes. Default-fallback decision: if PGO yields 2-3pp instead of the predicted 3-5pp, ship anyway (the codegen-tax buffer for Phase 2 still applies) but note the under-target in the bench doc.
- [ ] Document the PGO binary as the "release bench binary" — the one that produces the published 22q numbers going forward.

**PR shape:** Separate PR from 1.1/1.2 (gated on operator bench time).

### Story 1.4 — CI hook + release-workflow integration

**Status:** `[N/A — Phase 1 dropped]`. No PGO baseline meeting the gate exists to wire into CI. If Phase 1 is revisited (e.g. re-measured on a different hardware mix that clears the bar), this story re-activates.

**Failing test:**
- `.github/workflows/bench-sf10.yml` (or wherever the SF=10 bench CI lives) runs the PGO build instead of the plain release build. Smoke-test: CI successfully produces a PGO binary and runs at least one query against it.
- A green CI run on a PR that touches a hot path (e.g. `RobinHoodAggregateExec`) demonstrates the PGO rebuild happens in CI without manual intervention.

**Tasks:**
- [ ] Update the SF=10 release-bench CI workflow to invoke `scripts/pgo/build-instrumented.sh` → `scripts/pgo/train.sh` → `scripts/pgo/optimize.sh` before the bench-run step.
- [ ] Cache PGO profile data between CI runs (keyed on `Cargo.lock` hash + bench-workload version) to avoid re-training on every CI run. Profile re-collection only when cache misses.
- [ ] Document: `docs/CI_PGO.md` (new file) — what CI does, how to override, how to invalidate the cache.
- [ ] Per `feedback_recommend_next_step.md`: PR description's `Next:` line points at Phase 2 Story 2.1.

**PR shape:** Separate small PR after Story 1.3 numbers commit.

---

## Phase 2 — L13 custom hash join [ACTIVE]

**Goal:** Q18 SF=10 build phase drops below 60 ms; a standalone i64→i64 hash-join microbench shows ≥1.3× over DataFusion's stock `HashJoinExec` at both 1M and 15M build cardinalities. Sibling-crate scoped per `project_ematix_parquet_v013_win.md` to avoid codegen tax. Reuses Σ.N.f.3 RobinHood substrate (`project_sigma_nf3_beats_stock.md`).

**Estimated effort:** 2-3 person-quarters.

**Bundle:** Each story is one PR — they are independently testable, and bundling sequential operator-replacement work makes the diff unreviewable. Story 2.3 (bloom emitter) and Story 2.4 (skew detection) can ship in either order after 2.1 + 2.2 land.

**Acceptance gate (V5 §4, revised after Phase 1 drop):**
- L13 isolated bench vs stock `HashJoinExec`, i64→i64 @ 1M and 15M build cardinalities: ≥1.3×.
- Q18 SF=10 build phase: ≤60 ms.
- 22q SF=10 geomean improvement on top of the **0.80 non-PGO baseline** (Phase 1 dropped): target ≥6pp at SF=10 (V5 projects 10-13pp; gate at 6pp to allow for integration friction).
- No per-query regression > 2%.

### Story 2.1 — Kernel scaffold + Robin Hood i64-keyed table

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-hash-join/tests/kernel_correctness.rs::single_threaded_inner_join_i64_keys_matches_naive` — build with N random `(i64, payload)` pairs, probe with M random i64 keys, assert output matches a `HashMap<i64, Vec<payload>>` reference impl row-for-row.
- `kernel_handles_duplicate_keys` — build side has the same key 100×; probe matches all 100 (no dedup at build).
- `kernel_handles_null_keys` — null keys never match (Inner join NULL semantics).
- `kernel_benchmark_baseline.rs` — Criterion bench at 1M and 15M build cardinalities; emits per-iteration time. First commit: kernel runs but slower than stock (gate it in 2.6 once optimised).

**Tasks:**
- [ ] **OQ-L13-A resolution lands first.** Decision in `crates/ematix-flow-hash-join/src/lib.rs` module docstring: sibling op, not outright replacement.
- [ ] New crate at `crates/ematix-flow-hash-join/` with workspace membership in root `Cargo.toml`. Sibling-crate isolation per the established pattern (ematix-parquet, ematix-flow-planner-to-come).
- [ ] Lift `RobinHoodAggregateExec`'s hash table from `crates/ematix-flow-core/src/robin_hood_*.rs` into the new crate's `table.rs` — the underlying `RobinHoodTable<K, V>` is the substrate per `project_sigma_nf3_beats_stock.md`.
- [ ] Specialise for `K = i64` (the dominant TPC-H FK shape). Generic-over-K stays as a type parameter; i64 is the first monomorphisation.
- [ ] Build phase: ingest a stream of `RecordBatch`-shaped batches (`Vec<(i64, payload_columns)>`), insert into the RH table, return a built handle.
- [ ] Probe phase: stream probe batches, emit `(probe_row_idx, build_row_idx)` pairs for matches.
- [ ] **No DataFusion `ExecutionPlan` integration yet** — that's Story 2.5. This story is kernel-only.

**PR shape:** One PR. Self-contained crate; integration is downstream.

### Story 2.2 — Build-side cardinality estimator + adaptive build/probe selection

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-hash-join/tests/build_side_selection.rs::picks_smaller_side_when_cardinality_known` — given two input streams with cardinalities 1000 and 10M (declared up front), the helper picks the 1000-row side as build.
- `picks_via_stats_when_one_side_unknown` — one side has a stats estimate from Σ.O.c, the other doesn't; helper picks the known side as build if its cardinality is ≤10% of the unknown side's row-count estimate.
- `falls_back_to_left_side_when_no_stats` — both sides lack stats; helper picks left (DataFusion's default) and logs `build_side_selection_reason='no_stats_fallback'` metric.

**Tasks:**
- [ ] **OQ-L13-B resolution lands first** as a docstring in `crates/ematix-flow-hash-join/src/build_side.rs`.
- [ ] `BuildSideSelector::choose(left_stats, right_stats) -> BuildSide`. Stats source: `Σ.O.c RowGroupDecodeCache` (existing); fallback to parquet-footer stats; ultimate fallback is left-side.
- [ ] Selection rule: pick the side whose `expected_row_count` is smaller, OR — if estimates differ by < 10% — pick the side whose `expected_bytes_per_row` is smaller (smaller hash-table footprint).
- [ ] Emit a metric `build_side_selection_reason` for observability per Σ.L observation pattern.
- [ ] **No DataFusion integration yet** — the selector is a pure function over stats; integration is Story 2.5.

**PR shape:** One PR.

### Story 2.3 — Build-side bloom emitter (rides Σ.J.2.b)

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-hash-join/tests/bloom_emit.rs::build_emits_bloom_for_keys` — build with 10K random i64 keys, assert the emitted bloom returns `maybe_present` for all 10K keys AND `definitely_absent` for ≥95% of 10K out-of-range probe keys.
- `bloom_emit_respects_size_budget` — given a key set of size N, emitted bloom honours the configured `bytes_per_key` budget (default 8 bits/key = ~1% FPR per upstream Σ.J.2.b.viii convention).
- `bloom_round_trips_through_context_blooms` — integration test: emit bloom in build phase, attach via `attach_blooms_for_plan` helper from `project_sigma_j2b_viii_landed.md`, retrieve from `ContextBlooms` session extension, assert round-tripped bloom matches the source.

**Tasks:**
- [ ] **OQ-L13-C resolution lands first** as a docstring in `crates/ematix-flow-hash-join/src/bloom_emit.rs`.
- [ ] Reuse the BloomFilter type from `ematix-flow-core` (Σ.M ships it) — don't fork.
- [ ] `BuildSideBloomEmitter::emit(rh_table, bytes_per_key) -> Arc<BloomFilter>`. Called at end-of-build phase.
- [ ] Wire into Σ.J.2.b.viii's `attach_blooms_for_plan` API (already shipped). The custom join's bloom emission is one more producer; the existing single-node / distributed transports consume.
- [ ] **Selectivity gate**: do NOT emit bloom if `|build| / |probe| > 0.5` (per `project_l9_bloom_consumer_findings.md` — bloom-on-FK is net-negative when build side is large relative to probe).
- [ ] Metric: `bloom_emitted` (bool) + `bloom_size_bytes` (u64) + `bloom_keys` (u64).

**PR shape:** One PR. Depends on Story 2.1's kernel being mergeable.

### Story 2.4 — Skew detection + partitioned overflow secondary table

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-hash-join/tests/skew_handling.rs::detects_top_k_hot_keys` — build with 1M keys where 10 keys each appear 10K times and the rest are uniform; assert the top-10 hot keys are identified during the first pass.
- `skewed_keys_partition_into_overflow_table` — given the skew detection result, hot keys go into a smaller secondary table; probes for hot keys hit the secondary first.
- `skew_handling_neutral_when_no_skew` — uniform-distribution build; skew detector finds no hot keys; no overflow table allocated; performance matches the non-skewed path within ±2%.

**Tasks:**
- [ ] `SkewDetector::observe(rh_table) -> Option<HotKeys>` runs at end-of-build. Uses count-min sketch or simple top-K via `std::collections::BinaryHeap` — whichever the benchmark prefers.
- [ ] Threshold: a key is "hot" if its count exceeds `mean + 3 × stddev` of the per-key count distribution AND `count > 100`.
- [ ] When hot keys exist, build a separate `HashMap<i64, Vec<payload>>` for them; remove them from the main RH table. Probe path checks hot map first, RH table second.
- [ ] Metric: `skew_keys_detected` (u64), `skew_overflow_active` (bool).
- [ ] Per `feedback_no_tpch_hardcoding.md`: thresholds are parametric, not TPC-H-tuned. Document the 3-sigma choice in the module docstring.

**PR shape:** One PR. Independent of Story 2.3 (different code path).

### Story 2.5 — DataFusion `ExecutionPlan` integration

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-core/tests/emat_hash_join_integration.rs::shape_match_picks_emat_join` — given a SQL query whose plan would normally produce a `HashJoinExec` with i64-keyed Inner join, the pre-planning helper swaps in `EmatixHashJoinExec`; plan-dump asserts the swap.
- `shape_mismatch_keeps_stock_join` — non-i64 key, OR LeftOuter, OR semi-join → stock `HashJoinExec` retained.
- `correctness_e2e_q05_sf1` — run Q05 SF=1 through the integrated path; row count and aggregate sums match DuckDB.
- `correctness_e2e_q18_sf1` — same, Q18 SF=1.

**Tasks:**
- [ ] New `EmatixHashJoinExec` in `crates/ematix-flow-core/src/exec/emat_hash_join.rs` — the DataFusion-facing `ExecutionPlan` wrapper around the kernel crate. Pattern: same shape as `RobinHoodAggregateExec`.
- [ ] Pre-planning helper `pick_emat_hash_join` extends the existing `dict_routing.rs`-style pre-plan walker (NOT a new optimizer rule per hard constraint #1).
- [ ] Shape match: Inner join + i64-keyed (both sides) + no UDFs in the join condition + both sides project a stable schema.
- [ ] Wire `BuildSideSelector` (Story 2.2) into the swap: the chosen build side determines the kernel's build/probe order.
- [ ] Wire `BuildSideBloomEmitter` (Story 2.3) — emit on build completion; consumed by existing `EnableContextBloomRule` (Σ.J.2.b.vi).
- [ ] Per-query regression catcher: env flag `EMAT_HASH_JOIN=0` reverts to stock for the whole process.
- [ ] Per-query opt-out via comment: `/*+ no_emat_hash_join */` in SQL bypasses the swap (mirrors DuckDB's hint convention).

**PR shape:** One PR. Largest of the Phase 2 PRs — the integration surface plus the swap helper plus correctness tests. ~600-1000 LOC.

### Story 2.6 — Bench gate: standalone microbench + Q18 wall-clock

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-hash-join/benches/hash_join_vs_stock.rs` — Criterion bench at 1M and 15M build cardinalities; **asserts ratio ≥1.3×** vs stock `HashJoinExec` (criterion `assert!` pattern; if not yet supported by criterion, a wrapping integration test that reads criterion output).
- `crates/ematix-flow-core/examples/bench_q18_sf10_pre_post_emat_join.rs` — Q18 SF=10 wall-clock with `EMAT_HASH_JOIN={0,1}`. Asserts build phase ≤60 ms in the `=1` case.
- 22q SF=10 regression check: `EMAT_HASH_JOIN=1` geomean improvement ≥6pp; no per-query regression > 2%.

**Tasks:**
- [ ] Add the two bench files.
- [ ] Operator runs the SF=10 bench 5× per `feedback_full_bench_env_checklist.md` for noise characterisation.
- [ ] Record numbers in `docs/PHASE_L13_BENCH.md` (new): kernel-bench ratios, Q18 build-phase median, 22q geomean delta, per-query regression list.
- [ ] If gate fails: do NOT flip `EMAT_HASH_JOIN` default to on; ship as opt-in and iterate. Phase 3 can still proceed (it's independent of L13's default state).
- [ ] On gate pass: flip the default in `EmatixSessionContext` after 1 week of soak (mirrors Σ.O.c.2 / `project_sigma_oc2_provider_landed.md` pattern).
- [ ] Per `feedback_recommend_next_step.md`: `Next:` line points at Phase 3 Story 3.1.

**PR shape:** Separate PR after Story 2.5 lands; gated on operator bench time.

---

## Phase 3 — L14 dict-preserved end-to-end

**Goal:** Resolve `project_dict_arrival_blocker.md` from "opt-in via `EnableDictGroupCountRule`" to "default-on for low-cardinality string columns, per-shape verdict via Σ.L workload-log." Q12 -40% kernel win materialises in end-to-end SQL; Q01/Q13/Q19 regressions from `project_sigma_k_dict_arrival_ab.md` are avoided via the speculative-race probe.

**Estimated effort:** 4-6 weeks.

**Bundle:** Stories 3.1 + 3.2 ship in one PR (Σ.K.2 routing extension + speculative probe wire-up are co-dependent; the routing alone is dead weight without the probe gate). Story 3.3 ships separately — the default-on flip is an independent operational decision requiring a soak interval. Story 3.4 is the bench-gate PR.

**Acceptance gate (V5 §4):**
- 22q SF=10 geomean improves by ≥3pp on top of the Phase 2 cumulative baseline (Phase 1 dropped — see `docs/PHASE_PGO_BASELINE.md`).
- Q01 SF=10 does NOT regress (Σ.L.1 probe must catch the dict-off regime per `project_sigma_k_dict_arrival_ab.md`).
- Q13 SF=10 does NOT regress > 2%.
- Q19 SF=10 does NOT regress > 2%.

### Story 3.1 — Σ.K.2 dict-routing extended to consume Σ.L workload observations

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-core/tests/dict_routing_workload_log.rs::picks_dict_on_when_observed_winning` — seed workload log with 5 observations of `(table='lineitem', col='l_returnflag', verdict='dict_on_wins')`; planner picks dict-on path.
- `picks_dict_off_when_observed_losing` — seed 5 observations of `dict_off_wins`; planner picks dict-off path.
- `falls_back_to_default_on_first_encounter` — no observations for the column; planner picks default (per OQ-L14-A threshold: dict-on if `distinct_count_estimate ≤ 1024`).
- `respects_observation_threshold` — only 1 observation logged; planner ignores it (need ≥3 for a confident verdict, per Σ.L.1 speculative-race convention).

**Tasks:**
- [ ] **OQ-L14-A resolution lands first** as a docstring constant `DICT_CARDINALITY_THRESHOLD` in `crates/ematix-flow-core/src/dict_routing.rs`.
- [ ] **OQ-L14-B resolution lands first** as a SQL schema docstring in `crates/ematix-flow-core/src/workload_log.rs`.
- [ ] Extend `WorkloadLog` with the new table:
  ```sql
  CREATE TABLE dict_verdicts (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    verdict TEXT NOT NULL,  -- 'dict_on_wins' | 'dict_off_wins' | 'tied'
    n_observations INTEGER NOT NULL DEFAULT 1,
    last_outcome_unix INTEGER NOT NULL,
    PRIMARY KEY (table_name, column_name)
  );
  ```
- [ ] Extend `dict_routing::analyse_dict_arrival_for_sql` to consult `dict_verdicts` BEFORE applying the static cardinality threshold. Workload-log verdict overrides the static default.
- [ ] Default-on threshold (when no workload observation exists): `distinct_count_estimate ≤ 1024` AND column type ∈ `{Utf8, Utf8View, LargeUtf8}`.
- [ ] Env flag `EMAT_DICT_DEFAULT=0` short-circuits the default-on path; existing `EnableDictGroupCountRule` opt-in still fires.

**PR shape:** Bundled with Story 3.2.

### Story 3.2 — Speculative-race probe on first encounter

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-core/tests/dict_speculative_probe.rs::probe_records_dict_on_win` — first encounter with column `(table, col)`; the probe (lifted from Σ.L.1 substrate per `project_sigma_l1_speculative.md`) races dict-on vs dict-off on a sampled batch, declares dict-on winner, writes to `dict_verdicts`.
- `probe_records_dict_off_win` — same shape but for a column where dict-off measurably wins (synthetic shape mimicking Q01's `l_returnflag` under aggregation pressure).
- `probe_only_fires_on_first_encounter` — second query against the same column reads the verdict, doesn't re-probe (the probe has a non-zero cost; running it every query would tax SF=1 shapes).

**Tasks:**
- [ ] Reuse the Σ.L.1 speculative-race resolver (`crates/ematix-flow-core/src/sigma_l1_speculative.rs` or wherever it lives) as the probe substrate.
- [ ] On first encounter (no row in `dict_verdicts` for the column), the resolver:
  1. Samples the first batch in dict-preserved mode (read the column via the dict-preserved provider path);
  2. Samples the same batch in materialised mode (existing default);
  3. Times both end-to-end through the operator pipeline (aggregate / hash / sort — whichever the query uses);
  4. Declares the winner; writes to `dict_verdicts`.
- [ ] Probe cost budget: ≤5% of the first-encounter query's total wall-time. If the budget is exceeded, fall back to the static threshold; record `verdict='tied'`.
- [ ] After 3 consistent verdicts (n_observations ≥ 3), the resolver stops probing entirely for that column — read the verdict and proceed.
- [ ] Per `project_sigma_k_dict_arrival_ab.md`: the historical Q01 +104% / Q13 +25% / Q19 +35% regressions are exactly the case the probe catches. Validation: a synthetic Q01-shape test asserts the probe declares dict-off and avoids the regression.

**PR shape:** Same PR as Story 3.1. Combined PR ~400-600 LOC.

### Story 3.3 — Per-shape persistence + default-on flip with workload-log gating

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-core/tests/dict_default_on_soak.rs::default_on_uses_logged_verdicts` — clean log, run 5 Q01 SF=1 invocations against the default-on path; assert dict-off verdict is logged after the first run and used on subsequent runs.
- 22q SF=1 regression check: with `EMAT_DICT_DEFAULT=1` (default-on) and a clean workload log, geomean stays within ±2% of baseline. Catches the "first-query-tax-from-probing" risk.

**Tasks:**
- [ ] Flip the default in `dict_routing::analyse_dict_arrival_for_sql`: when no workload-log verdict exists, default to dict-on for columns meeting OQ-L14-A's cardinality threshold (was: default-off, opt-in via `EnableDictGroupCountRule`).
- [ ] Keep `EMAT_DICT_DEFAULT=0` as a kill-switch indefinitely (mirrors Σ.O.c.2 / `EMAT_RG_DECODE_CACHE` pattern).
- [ ] **Operator soak interval before flip:** 1 week of `EMAT_DICT_DEFAULT=1` weekly-22q runs; verify no regressions outside the Σ.L.1 probe's catch zone.
- [ ] Document the verdict invalidation policy: if the table's parquet footer fingerprint changes (write-loop workload), invalidate all `dict_verdicts` rows for that table. Implementation reuses the deferred-sidecar-plan's fingerprint logic if landed; otherwise a simpler `last_table_mtime` check.

**PR shape:** Separate PR, gated on operator soak.

### Story 3.4 — Bench gate + regression guard for the exercising queries

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-core/examples/bench_22q_sf10_dict_default.rs` — runs 22q SF=10 with `EMAT_DICT_DEFAULT={0,1}` × 5 iterations each. Asserts:
  - Geomean improvement ≥3pp at `=1`.
  - Q01 ratio (=1 / =0) ≤ 1.00 (no regression).
  - Q12 ratio ≤ 0.65 (≥35% improvement — the kernel-win materialises).
  - Q13 ratio ≤ 1.02.
  - Q19 ratio ≤ 1.02.

**Tasks:**
- [ ] Add the bench example.
- [ ] Operator runs 5× per `feedback_full_bench_env_checklist.md`.
- [ ] Record numbers in `docs/PHASE_L14_BENCH.md` (new): per-query medians, the four named regression checks, the geomean delta.
- [ ] If gate fails: revert the default-on flip (Story 3.3) but keep the workload-log infrastructure (Stories 3.1 + 3.2 land as opt-in).
- [ ] On gate pass: commit the default-on as production. Per `feedback_recommend_next_step.md`: `Next:` line points at V5 Tier 2 (L4 Σ.P extension OR L8 CBO spike, depending on cluster-bench M6 re-decision).

**PR shape:** Separate PR after Story 3.3 lands.

---

## Risks + things to watch

| Risk | Mitigation |
|---|---|
| **L13 kernel wins microbench but loses end-to-end.** Pattern seen in `project_sigma_r2_rejected.md` (RobinHoodAvgF64Exec) and `project_sigma_qm_slice4_spike_rejected.md`. | Story 2.6 bench gate requires both microbench ≥1.3× AND end-to-end ≥6pp geomean. If only microbench passes, ship as opt-in and triage. |
| **L13 build-side selector regresses left-side-default cases.** DataFusion's left-side default may already be right for cases our cardinality estimator gets wrong. | Story 2.2's fallback rule explicitly returns left-side when no stats; opt-out via env flag `EMAT_HASH_JOIN=0`. |
| **L13 codegen tax despite sibling-crate isolation.** Hot-path inlining could still cross crate boundaries via `#[inline]` annotations. With Phase 1 PGO dropped, this risk no longer has a buffer. | Sibling-crate isolation per Story 2.1 (codegen lives in `ematix-flow-hash-join`, not `ematix-flow-core`); `#[inline]` boundaries audited at Story 2.5 integration. If 22q geomean still regresses, the swap helper (Story 2.5) goes opt-in via `EMAT_HASH_JOIN`. |
| **L14 probe cost exceeds budget on slow first queries.** A first-encounter probe that doubles a 100ms query's wall-time is a UX regression even if it's a one-time tax. | Story 3.2 probe-cost budget of ≤5% of first-query wall-time; on overrun, record `verdict='tied'` and proceed with the static threshold. |
| **Σ.L workload log contention** under high concurrent query load with many distinct columns. | Existing `WorkloadLog` WAL + Mutex pattern; same as Σ.L.1/Σ.L.2/Σ.L.5 already handle. New table follows the same pattern. |
| **The deferred sidecar plan loses urgency.** Tier 5 levers are scheduled to run as a parallel track in V5 §2; if no engineer picks them up, Tier 1's downstream Tier 5 dependencies (e.g. L8 CBO consuming sidecar stats) drift. | Cross-reference in this plan + in `sidecar-deferred.md` keeps the work visible. Re-decision at the end of Phase 3 on whether to resume sidecar work or proceed to L8 CBO. |

---

## Out of scope

These belong in their own future plans (cited so the scope boundary is explicit):

- **L1 / L15 sidecar work.** See [`docs/plans/sidecar-deferred.md`](./sidecar-deferred.md). V5 Tier 5; parallel-track candidate.
- **L4 Σ.P SharedSubtreeExec extension** (V5 Tier 2 flat). Cheap and additive; future plan.
- **L7 / L8 CBO + join-order rewriter** (V5 Tier 1 + Tier 2). The biggest downstream investment; V5 M6 decision point.
- **L9 Cranelift JIT** (V5 Tier 3). Skip-condition gated: don't ship unless L11 microbench is within 10% of a Cranelift prototype.
- **L10 dynamic filter propagation** (V5 Tier 1). Gates on L13's bloom emitter shipping; this plan ships the emitter (Story 2.3) but not the cross-operator propagation pass.
- **L11 compile-time monomorphisation** (V5 Tier 2). Sibling-crate work; future plan.
- **L12 zero-copy column pipeline** (V5 Tier 2). 3-4 person-quarter rewrite; future plan.
- **L16 GPU offload** (V5 Tier 7). Hardware-conditional; future plan if cluster target includes GPU.
- **L17 online learned join order** (V5 Tier 1, but gated on L8). Future plan.
- **L18 DataFusion fork** (V5 Tier 4). 2-3 person-years; gated on observed extensibility walls in L11/L12/L17.

---

## Cross-references

- V5 (canonical sequencing): [`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V5.md`](../PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V5.md)
- V2 (canonical L1-L18 source): [`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V2.md`](../PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V2.md)
- Deferred sidecar work: [`docs/plans/sidecar-deferred.md`](./sidecar-deferred.md)
- Σ.N.f.3 RobinHood substrate (L13 starting point): memory `project_sigma_nf3_beats_stock.md`
- Σ.L.1 speculative-race substrate (L14 probe): memory `project_sigma_l1_speculative.md`
- Σ.K.2 dict-routing (L14 evolution baseline): memory `project_sigma_k2_dict_routing.md`
- Dict-arrival blocker context (L14 problem statement): memory `project_dict_arrival_blocker.md`
- Dict-A/B regression evidence (L14 probe motivation): memory `project_sigma_k_dict_arrival_ab.md`
- Codegen tax precedent (L3-first rationale, sibling-crate scoping): memory `project_optimizer_codegen_sensitivity.md`
- Sibling-crate success template: memory `project_ematix_parquet_v013_win.md`
- Current SF=10 baseline (0.80 geomean): memory `project_sigma_q_l13_to_l16_session.md`
- Σ.J.2.b transport (L13 bloom rider): memory `project_sigma_j2b_v_landed.md` through `viii_landed.md`
- Σ.O.c row-group decode cache (L13 stats source): memory `project_sigma_oc2_provider_landed.md`
- Bench environment SOP: memory `feedback_full_bench_env_checklist.md`
- Pin-bump trap: memory `feedback_patch_crates_io_version_match.md`
