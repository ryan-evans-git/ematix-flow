# Σ.T (V5) — L1–L18 universality-sorted technical roadmap

**Status:** working architecture doc (not a release artifact)
**Date:** 2026-05-25
**Author:** architect agent (cold-read, no main-thread context)
**Branch:** `perf/sigma-q-single-node-parity`
**Predecessor:** `docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V2.md` (canonical L1–L18 source)
**Supersedes:** V3 / V4 (which drifted into business strategy and are explicitly out-of-scope for V5)

V5 reframes V2's 18 levers around a single technical question: **which levers benefit ematix-flow across SF=1, SF=10, AND SF=100, and in what order should they ship?** V2 enumerated levers and clustered them into three calendar cohorts. V5 ignores the cohorts. The sort axis is **universality across data scales**, secondary axis is **impact per engineering week**.

This doc does not re-derive lever descriptions; cite V2 §2.X for each lever. The V5-specific content is the **per-scale closure curves**, the **single execution sequence**, and the **scale-universality principles** in §3.

No business strategy. No team sizing. No funding. No GTM. No license discussion. Tech only.

---

## 0. Reading guide

The doc is sorted top-down by tier. Highest-leverage scale-universal levers ship first (Tier 1). Lowest-priority hardware-specific levers ship last (Tier 7).

- **§1** — per-lever index, sorted by tier.
- **§2** — single execution sequence (calendar layout, parallel tracks, gating).
- **§3** — scale-universality principles (the *why* of the sort).
- **§4** — go/no-go acceptance gates per lever.
- **§5** — empirical addendum (post-PR #146; portability + memory-BW findings).
- **§6** — what V5 does not address.
- **§7** — references.

Baseline for closure deltas: 22q SF=10 geomean **0.80** (per `project_sigma_q_l13_to_l16_session.md`). SF=1 and SF=100 baselines are extrapolated from the same harness; SF=100 numbers are projections, not measurements.

---

## 1. Per-lever index (sorted by tier)

### Tier 1 — Universal, amplifying with scale

These levers win at SF=1, win more at SF=10, win the most at SF=100. The marginal cost of a bad join order grows with the size of the larger table; the absolute value of dynamic-filter propagation grows with the cardinality of the probe side; build-side bloom precision matters more when the build set is bigger. Ship first.

---

#### L8 — Custom CBO (sibling crate `ematix-flow-planner`)

Tier: 1
Universality: amplifying-with-scale
Origin reference: V2 §2.3 L8

Expected closure (pp of 22q geomean improvement):
- SF=1:   3-5 pp
- SF=10:  12-15 pp
- SF=100: 20-30 pp (CBO drives distributed-shuffle minimisation; absolute wasted bytes grow linearly with N)

Cost: 3 person-quarters
Dependencies: L3 (PGO) must land first to absorb codegen tax; consumes Σ.L workload observer (already shipped)
Blast radius: 4 (replaces optimisation driver; sibling crate scoping per `project_ematix_parquet_v013_win.md` insulates risk)
Scope: planner

What it does: a bounded-fanout DPccp join-order enumerator + statistics-driven cost model + scan-routing pass; replaces DataFusion's fixed-rule physical optimisation pipeline with an `ematix-flow-planner` driver. Statistics come from TableProvider (Σ.O.c row-group decode cache), sidecar `IndexSummary` (when present), and Σ.L workload observer.

Why this tier: dim-filter propagation gaps on Q05/Q07/Q08 grow linearly with `|lineitem|`. At SF=100 the absolute waste from a missed propagation is 10× the SF=10 waste; the optimiser's accuracy compounds with scale.

Acceptance gate: 22q SF=10 geomean drops below 0.68 with no per-query regression > 5%; Q05 specifically drops to ≤1.00× DuckDB.

---

#### L13 — Custom hash join

Tier: 1
Universality: amplifying-with-scale
Origin reference: V2 §2.3 L13

Expected closure:
- SF=1:   2-4 pp
- SF=10:  10-13 pp
- SF=100: 18-25 pp (build-side bloom + skew handling matter most when build-side cardinality is large; distributed shuffle joins ride the same kernel)

Cost: 2-3 person-quarters
Dependencies: Σ.N.f.3 RobinHood substrate (shipped); L14 strongly preferred (dict-keyed builds halve memory); L8 strongly preferred (CBO drives build-side selection)
Blast radius: 3 (operator replacement, but Σ.N.f.3 already proved the kernel beats stock)
Scope: operator

What it does: i64-keyed Robin Hood hash join with build-side cardinality-driven build/probe selection, build-side bloom emitter (rides Σ.J.2.b transport), pre-sized via Σ.N.f.2 dynamic resize, skew detection + partitioned overflow.

Why this tier: every fact-dim join in Q05/Q07/Q08/Q17/Q18 builds and probes a hash table. Build cost scales linearly with the build side; bloom selectivity scales with `1 - (|build|/|probe|)`. Both axes grow with SF.

Acceptance gate: Q18 SF=10 build phase drops below 60 ms; L13 isolated benchmark shows ≥1.3× over DataFusion's stock `HashJoinExec` on i64→i64 at 1M and 15M build cardinalities.

---

#### L10 — Dynamic filter propagation (single-node)

Tier: 1
Universality: amplifying-with-scale
Origin reference: V2 §2.3 L10

Expected closure:
- SF=1:   1-3 pp (Q05/Q07 only)
- SF=10:  10-12 pp
- SF=100: 15-25 pp (selectivity-driven IO reduction compounds with scan size)

Cost: 6-8 weeks
Dependencies: L13 emits the bloom; Σ.J.2.b transport (shipped) for in-process delivery; selectivity-gate per `project_l9_bloom_consumer_findings.md` is non-optional
Blast radius: 3 (sideband timing semantics; documented risk with mitigation)
Scope: planner + runtime

What it does: planner pass that, for each Inner Join, derives the surviving-key set from the smaller (filtered) side and pushes it as a runtime bloom into the larger side's scan. Composes with L1 sidecar (page-Bloom consumer), L13 (build-side emitter), Σ.J.2.b (transport).

Why this tier: this is DuckDB's biggest TPC-H lever at SF=10 and grows further at SF=100. At SF=1 most fact tables are small enough that the propagation cost equals the savings; benefit appears mostly on Q05/Q07. At SF=10+ the lineitem scan dominates and dropping 95% of partitions before decode is worth the planning cost.

Acceptance gate: Q05 SF=10 lineitem scan drops below 50 ms (currently ~150 ms); no per-query regression > 3% from selectivity-gate misfire.

---

#### L14 — Dict-preserved end-to-end (default on)

Tier: 1
Universality: amplifying-with-scale
Origin reference: V2 §2.3 L14

Expected closure:
- SF=1:   2-3 pp (Q01, Q12, Q07)
- SF=10:  3-5 pp
- SF=100: 5-8 pp (dict-coded hash-join keys are the dominant memory saver at cluster scale; halves shuffle bytes for string FKs)

Cost: 4-6 weeks
Dependencies: ematix-parquet PR #34 dict-preserved read path (per `project_emat_dict_preserved_upstream.md`); Σ.L.1 speculative-race substrate (shipped) to avoid the `project_sigma_k_dict_arrival_ab.md` Q01 +104% regression
Blast radius: 3 (per-query A/B already showed both +/− regimes; Σ.L.1 probe is the gate)
Scope: runtime (TableProvider) + operator (group-values intern path)

What it does: flips dict-preservation from opt-in via `EnableDictGroupCountRule` / Σ.K.2 routing to the default for low-cardinality string columns. Σ.L.1 probe + per-shape verdict picks dict-on/off per query.

Why this tier: dict-coded keys cost a fraction of materialised-string keys at any scale, and the savings amplify on the shuffle path at SF=100 (cluster). At SF=1 the savings are real but smaller because the absolute bytes moved are tiny.

Acceptance gate: 22q SF=10 geomean improves by ≥3pp on top of L8+L13 baseline; Q01 SF=10 does not regress (Σ.L.1 probe must catch the dict-off regime).

---

#### L17 — Online learned join order

Tier: 1
Universality: amplifying-with-scale
Origin reference: V2 §2.3 L17

Expected closure:
- SF=1:   1-2 pp (after 5 observations per shape)
- SF=10:  2-4 pp on top of L8
- SF=100: 5-10 pp (cardinality misestimates cost more at cluster scale; learning matters more)

Cost: 4-6 weeks on top of L8
Dependencies: L8 (CBO that consumes cardinality input); Σ.L.2 workload-log (shipped)
Blast radius: 2 (cardinality estimate is one input; cost model still bounds the outcome)
Scope: planner

What it does: persists observed per-vertex cardinality from each query run, hands it to L8's cost model as a higher-confidence input than static stats once n_observations ≥ 5. Snowflake's "intelligence layer" pattern on top of Σ.L.2.

Why this tier: misestimated cardinality is the dominant CBO error source. Static estimates are bad at correlated predicates and join-output sizes; observed cardinality fixes both. The cost of an error grows linearly with the size of the misestimated intermediate, i.e. with SF.

Acceptance gate: re-run of 22q after L8+L17 wire-up shows ≥1pp additional lift over L8-only; observed-cardinality consumption confirmed in plan-dump on Q05.

---

### Tier 2 — Universal, flat across scales

Per-row tax cuts. Same magnitude win regardless of data size. These are not scale-amplifying but they are universal — every query benefits a constant fraction. Ship second (some calendar-first; see L3).

---

#### L3 — PGO release build

Tier: 2 (but **ships first in calendar** — see §2)
Universality: flat
Origin reference: V2 §2.2 L3

Expected closure:
- SF=1:   2-4 pp
- SF=10:  3-5 pp
- SF=100: 2-4 pp

Cost: 3-5 days
Dependencies: none
Blast radius: 1 (build-system only; CI gates per-binary parity)
Scope: build-system

What it does: profile-guided LLVM build using TPC-H 22q as the training workload; published as the release-build profile. Sharpens the binary regardless of data shape.

Why this tier: flat across scales — the codegen sharpening lifts every hot loop by a roughly constant percentage. **L3 is calendar-first** because (a) it ships in a week, (b) blast radius is 1, (c) every subsequent lever rides on the PGO baseline, and (d) per `project_optimizer_codegen_sensitivity.md` it absorbs part of the codegen tax that L8/L13 would otherwise pay.

Acceptance gate: 22q SF=10 geomean improves by ≥3pp on PGO build vs non-PGO with identical source.

---

#### L12 — Zero-copy column pipeline (replace `RecordBatch` boundaries)

Tier: 2
Universality: flat
Origin reference: V2 §2.3 L12

Expected closure:
- SF=1:   3-5 pp
- SF=10:  5-7 pp
- SF=100: 5-7 pp

Cost: 3-4 person-quarters
Dependencies: ideally L8 (so operator-level changes happen behind a stable planner API); ematix-parquet `DecodedColumn` type (shipped)
Blast radius: 5 (touches every operator; DataFusion-upstream-break risk per V2 §2.3 L12)
Scope: runtime + operator

What it does: replace per-stage `RecordBatch` rematerialisation with `Arc<DecodedColumn>` flowing through scan→filter→join→agg. Filter bitmap composes without materialising rows until the final `take_columns_for_output`. Velox's UnifiedRowVector is the existence proof.

Why this tier: the Utf8View buffer inflation tax (`project_sigma_e5_q13_root_cause.md` — 152MB→2.1GB on Q13) is per-row, not per-scale. Closing it lifts Q13/Q14/Q17 at every SF. SF=100 doesn't grow the per-row tax further; only the absolute bytes wasted grow.

Acceptance gate: Q13 SF=10 output_bytes drops below 500 MB (currently 2.1 GB); 22q geomean improves by ≥4pp with no string-column regression > 5%.

---

#### L11 — Σ.F shape catalogue lifted to compile-time monomorphisation

Tier: 2 (with mild inverse-scale character — see "why this tier")
Universality: flat (with inverse-scale tail)
Origin reference: V2 §2.3 L11

Expected closure:
- SF=1:   4-6 pp
- SF=10:  5-7 pp
- SF=100: 3-5 pp (IO dominates at SF=100; the dispatch overhead L11 closes shrinks as a fraction of total)

Cost: 2-3 person-quarters
Dependencies: L8 (CBO picks the template); L14 (dict-preserved is one of the template axes); sibling-crate scope to dodge codegen tax per `project_optimizer_codegen_sensitivity.md`
Blast radius: 3 (new monomorphised kernels; type explosion is the design risk)
Scope: operator (kernel)

What it does: lifts the Σ.F shape catalogue from plan-time operator selection to compile-time monomorphisation. ~10 plan templates × 4 binary axes = ~40 monomorphised functions. Photon's pattern in Rust const generics.

Why this tier: closes interpretive-dispatch overhead, which is a constant fraction of per-batch cost at SF=1 and SF=10. At SF=100, where IO dominates, the closed overhead shrinks as a fraction of wall time — hence the mild inverse-scale tail. Still ships in Tier 2 because the SF=1/SF=10 magnitudes are large and consistent.

Acceptance gate: Q01 SF=10 closes by ≥2pp on top of L3+L14; no codegen-tax regression on the 22q bench (sibling-crate isolation is the mitigation).

---

#### L4 — Σ.P SharedSubtreeExec extension

Tier: 2
Universality: flat
Origin reference: V2 §2.2 L4

Expected closure:
- SF=1:   1-2 pp (Q17, Q18)
- SF=10:  1-2 pp
- SF=100: 1-2 pp

Cost: 2-3 weeks
Dependencies: Σ.P SharedSubtreeExec registry (shipped per `project_sigma_p_subquery_cse.md`); L7's mechanism is V2 §2.2 L4
Blast radius: 1 (extension of an already-landed mechanism)
Scope: planner

What it does: extends Σ.P's session-scoped registry to cover the plan shapes where it currently fails to fire (notably Q17 outer-scan = subquery-scan; Q18 duplicate lineitem scan). Investigation per V2 OQ-5.

Why this tier: scan-sharing is a constant-factor saving; it doesn't grow with SF. But it grounds out at one cached scan per duplicate, which is the same magnitude at every SF.

Acceptance gate: Σ.P fires on Q17 and Q18 (confirmed via plan-dump); Q17 SF=10 ≤230 ms.

---

#### L7 — Real join-order rewriter (subsumed by L8, kept for sequencing optionality)

Tier: 2
Universality: flat (but mostly subsumed by L8)
Origin reference: V2 §2.2 L7

Expected closure (if shipped standalone instead of inside L8):
- SF=1:   2-3 pp
- SF=10:  3-5 pp
- SF=100: 5-10 pp

Cost: 6-10 weeks
Dependencies: none (standalone); if L8 ships, L7 is subsumed
Blast radius: 3 (DataFusion-upstream-or-in-tree-override)
Scope: planner

What it does: standalone join-order rewriter without the full CBO surface. Picks join order from cardinality estimates only; does not own scan-routing or aggregate-shape selection.

Why this tier: smaller-surface alternative to L8 for projects that can't afford 3 person-quarters. **L7 is included for sequencing optionality** — if L8 slips past month 8, L7 alone delivers 60% of L8's join-order wins at 35% of the cost. Otherwise L7 should be dropped.

Acceptance gate: same as L8 but Q05-only. If L8 is on track, do not ship L7.

---

### Tier 3 — Universal magnitude, inverse-scale (decline with size)

Constant-factor wins via codegen / interpretation removal. At SF=1 the interpretation overhead is a meaningful fraction of total; at SF=100 IO dominates and the relative gain shrinks. Ship third (conditional).

---

#### L9 — Cranelift whole-query JIT compilation

Tier: 3
Universality: inverse-scale
Origin reference: V2 §2.3 L9

Expected closure:
- SF=1:   3-5 pp
- SF=10:  1-3 pp
- SF=100: ~0-1 pp (IO-bound; JIT removes dispatch but not the bytes-on-the-wire cost)

Cost: 3-4 person-quarters
Dependencies: L12 (zero-copy memory layout is friendlier to JIT'd loops); Cranelift is already a tree dependency
Blast radius: 5 (whole-query execution rewrite; Photon's anti-JIT thesis per `project_groupby_research_2026_05.md` argues template specialisation wins)
Scope: runtime + operator

What it does: compiles physical plan to native via Cranelift IR at plan time; replaces `ExecutionPlan::execute` per-batch dispatch with a single compiled function per pipeline.

Why this tier: codegen removes interpretive dispatch overhead, which is a SF=1-dominant cost. At SF=100, scan-IO and shuffle-IO dwarf dispatch cost; JIT delivers little additional headroom. Photon explicitly rejected JIT in favour of template specialisation (which is L11).

Acceptance gate: a Q01 spike at SF=1 shows ≥2× win over DataFusion default AND ≥1.2× win over L11 template specialisation. **If L11 is within 10% of the Cranelift prototype, do not ship L9** — L11 covers the same ground at 1/3 the cost.

---

### Tier 4 — Universal magnitude, structural-debt removal (conditional)

The fork. Closes everything; cost is too high to ship unconditionally. Gate on observed extensibility ceiling from Tier 1/Tier 2.

---

#### L18 — Replace DataFusion entirely (fork)

Tier: 4
Universality: universal magnitude, conditional
Origin reference: V2 §2.3 L18

Expected closure:
- SF=1:   10-15 pp
- SF=10:  20-25 pp
- SF=100: 25-35 pp

Cost: 2-3 person-years
Dependencies: keep DataFusion SQL parser + Substrait; replace LogicalPlan/PhysicalPlan/ExecutionPlan
Blast radius: 5 (multi-year, no-feature-shipping interval; fatal if abandoned)
Scope: planner + runtime + operator

What it does: forks DataFusion. Keeps Arrow + SQL parser + Substrait. Replaces everything between. Removes accumulated debt: RecordBatch tax, JoinSelection heuristics, codegen sensitivity, CSE limitations.

Why this tier: the magnitude is the largest of any single lever, but the cost makes it conditional. **L18 ships only if L8 + L13 + L10 + L11 + L12 ship AND a measured extensibility wall blocks further progress on DataFusion.** That decision happens after Tier 1+2 land — earliest at calendar month 12.

Acceptance gate: a written 1-pager (V2 OQ-RISK-A) plus measured evidence that ≥2 of {L11, L12, L17} hit a DataFusion-imposed wall that cannot be worked around in-tree.

---

### Tier 5 — Scale-specific (trigger above a threshold)

Levers that only fire above a data size threshold. SF=1 fits in cache and these are no-ops; SF=10+ they matter.

---

#### L1 — Sidecar Phase 1 (read-side index discovery + planner hook)

Tier: 5
Universality: scale-specific (SF=10+)
Origin reference: V2 §2.2 L1

Expected closure:
- SF=1:   ~0 pp (cache fits in L2/L3; sidecar lookup overhead exceeds savings)
- SF=10:  2 pp (Q08 cleanly closed; Q17 partial)
- SF=100: 3-5 pp (page-Bloom dominates at this scale)

Cost: 2 weeks
Dependencies: sidecar manifest format (per `docs/plans/CURRENT.md`); planner hook in TableProvider
Blast radius: 2 (no-op when sidecar absent; consumer-only)
Scope: runtime (TableProvider) + planner

What it does: read-side index discovery during planning. If a sidecar manifest exists, the planner consumes its zonemaps + page-Blooms + offset index to prune row-groups and pages.

Why this tier: at SF=1 the dataset fits in cache; page-Bloom lookups cost more than the saved decode. At SF=10 lineitem is ~6 GB and sidecar shifts the IO/CPU ratio. At SF=100 sidecar is mandatory.

Acceptance gate: Q08 SF=10 closes by ≥2pp; SF=1 does not regress > 0.5pp (the no-sidecar path must remain fast).

---

#### L15 — Iceberg + sidecar manifests as full storage layer (write-side ownership)

Tier: 5
Universality: scale-specific (SF=10+ for write-tuned wins; SF=100+ for partial-MV layer)
Origin reference: V2 §2.3 L15

Expected closure:
- SF=1:   ~1 pp (write-time sorting helps Q06/Q14 marginally)
- SF=10:  3-5 pp (write-time sort + sidecar generation)
- SF=100: 8-15 pp (partial materialised views amortise across many runs; layout discipline matters)

Cost: 3-4 person-quarters
Dependencies: L1 (read-side sidecar consumer); Σ.L.5 write-tuner (drafted)
Blast radius: 3 (write path is new surface; Iceberg manifest semantics non-trivial)
Scope: storage + runtime

What it does: owns the write path. Iceberg manifests, write-time sort/Z-order, sidecar generation at write, partial materialised views for hot rollups.

Why this tier: write-time layout discipline is a multiplier on every subsequent scan. SF=100 amortises the write cost across many query runs; SF=1 doesn't amortise enough to justify.

Acceptance gate: Q01 SF=100 (when published) drops by ≥5pp vs read-only ematix-flow; Q06 SF=10 closes by ≥3pp via write-time sort on `l_shipdate`.

---

### Tier 6 — Query-specific (point fixes)

Single-query closures. Useful if the cited query matters; do not generalise. Lowest universality.

---

#### L2 — Filter-derived static bloom into provider

Tier: 6 (but L10 subsumes it)
Universality: query-specific (Q05, Q07)
Origin reference: V2 §2.2 L2

Expected closure (if shipped standalone, pre-L10):
- SF=1:   ~0 pp
- SF=10:  2-3 pp (Q05/Q07)
- SF=100: 2-3 pp

Cost: 3-4 weeks
Dependencies: L1 sidecar (for the page-Bloom consumer)
Blast radius: 2
Scope: planner

What it does: static-at-plan-time bloom derived from literal filter predicates; pushed into TableProvider as a filter argument.

Why this tier: query-specific (Q05/Q07 dim filters). Subsumed by L10 (dynamic propagation generalises static). **Ship L2 only if L10 slips past month 6** — L2 is the cheap fallback that catches the most-impactful Q05/Q07 cases.

Acceptance gate: Q05 SF=10 lineitem scan drops by ≥30 ms.

---

#### L5 — Scalar-subquery decorrelation v2 (Q17)

Tier: 6
Universality: query-specific (Q17)
Origin reference: V2 §2.2 L5

Expected closure:
- SF=1:   0.5 pp
- SF=10:  0.5-1 pp
- SF=100: 0.5-1 pp

Cost: 4-6 weeks
Dependencies: L11 subsumes Q17's shape as a monomorphised template; L8's CBO observes the filter-correlation
Blast radius: 2
Scope: planner

What it does: rewrites Q17's correlated `AVG(l_quantity) per p_partkey` to push the outer's partkey-set filter into the subquery's aggregate.

Why this tier: query-specific. **Subsumed by L11.** Ship L5 only if L11 slips and Q17 stays > 1.05× DuckDB.

Acceptance gate: Q17 SF=10 ≤220 ms.

---

#### L6 — LeftSemi build-side swap re-verify (Q18)

Tier: 6 (subsumed by L13)
Universality: query-specific (Q18)
Origin reference: V2 §2.2 L6

Expected closure:
- SF=1:   ~0 pp
- SF=10:  0.3 pp
- SF=100: 0.3 pp

Cost: 1 week
Dependencies: PushDownLeftSemiRule (shipped per `project_sigma_q_l10_landed.md`)
Blast radius: 1
Scope: planner

What it does: re-verifies post-L10 LeftSemi build-side selection.

Why this tier: subsumed by L13 (CBO-driven cardinality-based build-side selection). **Ship L6 only if L13 slips and Q18 build-phase stays large.**

Acceptance gate: Q18 SF=10 build phase drops by ≥10 ms.

---

### Tier 7 — Hardware-specific

Hardware-conditional. Ship only if the target hardware is in scope.

---

#### L16 — GPU offload for filter+agg pipelines

Tier: 7
Universality: hw-specific (GPU-equipped only)
Origin reference: V2 §2.3 L16

Expected closure (only on GPU hardware):
- SF=1:   ~0 pp (dispatch overhead dominates; data too small)
- SF=10:  3-5 pp (filter-bound queries: Q06, Q17)
- SF=100: 8-15 pp (data large enough to amortise dispatch)

Cost: 2-3 person-quarters (prototype); 4+ (production)
Dependencies: L12 (zero-copy column layout amenable to GPU upload); cluster bench target on GPU instances
Blast radius: 3 (new operator; hardware-conditional codepath)
Scope: operator

What it does: `GpuPipelineExec` operator that offloads filter+agg+join over dense columnar batches to GPU. Metal on M-series; CUDA on AWS g4dn.

Why this tier: hardware-specific. Pays off only when GPU is present AND data is large enough to amortise upload + dispatch. Defer until L15 storage layer lands and the SF=100 cluster bench target chooses GPU instances or not.

Acceptance gate: Q06 SF=100 (when published, on GPU-equipped instance) drops by ≥10pp vs CPU-only.

---

## 2. Single execution sequence

```
M0 ─────── M3 ─────── M6 ─────── M9 ─────── M12 ─────── M18 ─────── M24

T1 amplifying:
  L3 ────► L13 ──────► L14 ──────► L8 ──────────► L17 ───────► L10
            (kernel)    (default)    (CBO core)    (learning)   (final wire)

T2 flat:
                       L4 ──────► L12 ────────────► L11
                       (Σ.P ext)   (zero-copy)      (monomorph)

T3 inverse-scale:
                                                   L9 (cond) ◄── decide @ M9
                                                   gate: L11 within 10% → drop L9

T4 structural debt:
                                                              L18 (cond) ◄── decide @ M12
                                                              gate: 2+ extensibility walls

T5 scale-specific:
            L1 ─────────────────► L15 (read) ──────► L15 (write+MV)
            (read sidecar)         (Iceberg)          (parallel to T2)

T6 query-specific (catchup, skip if upstream delivers):
                       L2 (only if L10 slips @ M6)
                       L5 (only if L11 slips @ M9)
                       L6 (only if L13 slips @ M3)
                       L7 (only if L8 slips @ M9)

T7 hardware (conditional, depends on cluster target):
                                                              L16 (cond) ◄── decide @ M12
                                                              gate: cluster bench on GPU
```

### 2.1 Parallel tracks

- **L3** must land first (calendar-first; everything benefits and codegen tax mitigation). Single engineer-week.
- **L13 kernel** and **L1 read-side sidecar** can run in parallel with L3 — independent code surfaces.
- **L14** and **L4** are independent of L13/L10 — can run on a third track in months 3-5.
- **L8 CBO core** and **L12 zero-copy** are the deepest investments and run on separate tracks from M6 onward. L8 is the higher-priority track; L12 is the deeper-investment, slower-payoff track.
- **L15 write-side** is parallel to L12 and L8 from M9 onward; no shared code.
- **L17 production wire-up** gates on L8 completion (consumes the CBO surface).

### 2.2 Dependency / gating edges

- L13 integration gates on L8 build-side-selection API (kernel can ship pre-L8; integration cannot).
- L10 gates on L13 build-side bloom emitter being live.
- L17 gates on L8.
- L11 strongly prefers L8 (CBO picks the template) and L14 (dict-axis is a template parameter).
- L12 wants L8 for stable planner API at the operator boundary; can ship independently with more rework.
- L9 is conditionally gated on L11's outcome — if L11 closes Q01-like shapes to within 10% of a Cranelift spike, L9 drops out.
- L18 conditionally gated on observed extensibility walls in L11/L12/L17 by M12.
- L15 write+MV gates on L15 read sidecar shipping first.
- L16 gates on cluster SF=100 bench target including GPU instances.

### 2.3 Skip conditions

- **L7 dropped** if L8 ships by M9.
- **L2 dropped** if L10 ships by M6.
- **L5 dropped** if L11 ships by M9.
- **L6 dropped** if L13 integration ships by M3.
- **L9 dropped** if L11's microbench is within 10% of a Cranelift prototype on Q01-shape.
- **L18 dropped** unless ≥2 of {L11, L12, L17} hit DataFusion-imposed walls.
- **L16 dropped** if cluster bench target is CPU-only.

### 2.4 Calendar checkpoints

- **M3** — L3 + L13 kernel + L1 read-side + L4 Σ.P extension complete. 22q SF=10 geomean ≤ 0.74.
- **M6** — L14 default flip + L13 integration + L10 spike complete. Geomean ≤ 0.66.
- **M9** — L8 CBO core + L17 partial wire-up + L10 final + L11 first 4 templates. Geomean ≤ 0.58. **L9 / L18 / L16 decision triggered.**
- **M12** — L12 zero-copy + L11 full catalogue + L15 read-sidecar+Iceberg. Geomean ≤ 0.52. **L18 fork decision triggered (gated on extensibility wall evidence).**
- **M18** — L15 write+MV + L17 production. Geomean ≤ 0.48 single-node; cluster SF=100 wedge story shippable.
- **M24** — L18 (if triggered) lands or aborts at the M18→M24 review.

---

## 3. Scale-universality principles

The sort axis is universality across SF=1, SF=10, SF=100. The four classes and the principle behind each:

### 3.1 Amplifying with scale (Tier 1)

**Principle:** the cost of a bad decision grows faster than the cost of the fix. Most planning errors fall here.

- **Join order (L8/L17):** a missed dim-filter propagation wastes time proportional to `|larger side|`. At SF=100 the larger side is 100× SF=1; the absolute waste is 100×. The CBO's per-query planning cost is constant in SF. Ratio of waste-prevented to fix-cost grows linearly with SF.
- **Hash join build (L13):** build cost is O(|build|). At SF=100, build sets are 100× larger; cardinality-driven build-side selection prevents 100× larger missteps. Bloom selectivity is O(1 - |build|/|probe|), which is closer to 1 at large SF.
- **Dynamic filter propagation (L10):** selectivity-driven IO reduction scales with the absolute scan size. Pruning 95% of partitions at SF=100 saves 100× the bytes saved at SF=1.
- **Dict-coded keys (L14):** intern table footprint is constant in row count; saved bytes per row are constant per shape. But shuffle and join build sizes scale with row count, so dict savings on those scale with SF.

### 3.2 Flat across scales (Tier 2)

**Principle:** per-row tax. The fix removes a constant fraction of per-row work. Wins do not amplify but they also do not shrink.

- **PGO (L3):** sharpens every hot loop by ~constant fraction.
- **Zero-copy pipeline (L12):** Utf8View rematerialisation is per-row inflation; absolute bytes wasted scale with SF, but the *fraction* of total work spent on it is invariant.
- **Template monomorphisation (L11):** removes interpretive dispatch per-batch; per-batch dispatch is constant fraction. (Note: mild inverse-scale tail because IO dominates more at SF=100, shrinking dispatch's share of total.)
- **Σ.P scan sharing (L4):** removes one duplicate scan per CSE hit; magnitude is one-scan-worth at any SF.

### 3.3 Inverse-scale (Tier 3)

**Principle:** the win is in the constant factor, not the algorithm. As data grows, IO and decode dominate; the constant factor shrinks as a fraction of total.

- **Cranelift JIT (L9):** removes interpretive dispatch overhead. At SF=1 dispatch is 20-30% of total; at SF=100 dispatch is 1-3% of total because IO has taken the lead. JIT's lift shrinks.

### 3.4 Scale-specific (Tier 5)

**Principle:** the lever only triggers above a data size threshold; below the threshold the lever's overhead exceeds its savings.

- **Sidecar (L1):** at SF=1 the dataset fits in L2/L3 cache and sidecar lookups are slower than just decoding. Above ~5 GB the IO savings dominate.
- **Storage layer (L15):** write-time sort + partial-MV requires amortising the write cost across many query runs. SF=100 in cluster mode amortises; SF=1 does not.

### 3.5 Query-specific (Tier 6)

**Principle:** point fix for one plan shape. Doesn't generalise; ship only if the cited query matters AND the generalised upstream lever doesn't land.

### 3.6 Hardware-specific (Tier 7)

**Principle:** lever exists only when target hardware is present. GPU-only.

### 3.7 Why amplifying-with-scale ships first

Three reasons:

1. **Largest expected closure at the most-bench-relevant scale.** SF=10 is the current published bench; SF=100 is the strategic target. Amplifying levers maximise both.
2. **Compounding with subsequent levers.** L13's build-side bloom feeds L10; L8's CBO drives L13/L14 routing; L17 rides L8. The downstream levers' value depends on amplifying levers' surface.
3. **Codegen tax bound up-front by L3.** Amplifying levers (L8/L13) are the most codegen-sensitive; landing L3 first absorbs the tax. Flat-tier levers (L4/L11/L12) are less sensitive (sibling crates) and can wait.

---

## 4. Acceptance gates summary

| Lever | Gate metric | Threshold |
|---|---|---|
| L3 | 22q SF=10 geomean improvement, PGO vs non-PGO | ≥3pp |
| L13 | L13 isolated bench vs stock HashJoinExec, i64→i64 @ 1M / 15M | ≥1.3× |
| L13 | Q18 SF=10 build phase | ≤60 ms |
| L8 | 22q SF=10 geomean | ≤0.68 |
| L8 | Q05 SF=10 vs DuckDB | ≤1.00× |
| L10 | Q05 SF=10 lineitem scan | ≤50 ms |
| L10 | No per-query regression > 3% from selectivity-gate misfire | held |
| L14 | 22q SF=10 lift on top of L8+L13 | ≥3pp |
| L14 | Q01 SF=10 must not regress (Σ.L.1 probe gate) | no regression |
| L17 | 22q geomean lift over L8-only after observer wire-up | ≥1pp |
| L12 | Q13 SF=10 output_bytes | ≤500 MB |
| L12 | 22q geomean lift; string-column regression bound | ≥4pp; ≤5% |
| L11 | Q01 SF=10 closure on top of L3+L14 | ≥2pp |
| L11 | 22q codegen-tax check | no regression |
| L4 | Σ.P fires on Q17 and Q18 | confirmed via plan-dump |
| L4 | Q17 SF=10 | ≤230 ms |
| L1 | Q08 SF=10 closure | ≥2pp |
| L1 | SF=1 regression bound | ≤0.5pp |
| L15 (write) | Q06 SF=10 closure via write-time sort | ≥3pp |
| L15 (MV) | Q01 SF=100 closure | ≥5pp |
| L9 | Q01 spike at SF=1: Cranelift vs L11 | ≥1.2× → ship; <1.10× → drop |
| L18 | Extensibility wall evidence in L11/L12/L17 | ≥2 walls documented |
| L16 | Q06 SF=100 on GPU vs CPU | ≥10pp |
| L2 (fallback) | Q05 SF=10 lineitem scan | ≥30 ms saved |
| L5 (fallback) | Q17 SF=10 | ≤220 ms |
| L6 (fallback) | Q18 SF=10 build phase | ≥10 ms saved |
| L7 (fallback) | same as L8 but Q05-only | conditional |

---

## 5. Empirical addendum (2026-05-25, post-PR #146)

V5 §§0–4 above are the canonical sort by universality. This addendum
captures evidence that landed *after* the V5 sort and informs *when*
inside Tier 1 to schedule which lever — but does **not** re-tier any
of L1–L18. The universality ordering still holds.

### 5.1 Π.13 — x86 SIMD parity (portability work; outside the L-numbered space)

Landing via [PR #146](https://github.com/ryan-evans-git/ematix-flow/pull/146)
in ematix-flow + ematix-parquet 0.16.2 (sibling PR
[ematix-parquet#94](https://github.com/ryan-evans-git/ematix-parquet/pull/94)
already merged + published).

- **Π.13 Tier 1 (in flow):** SSE2 SwissTable-style `match_byte_mask`
  in `crates/ematix-flow-core/src/robin_hood_agg.rs`. Two-instruction
  `_mm_cmpeq_epi8` + `_mm_movemask_epi8` path replaces the
  16-iteration scalar fallback that previously bit x86_64 builds.
  Matches the existing aarch64 NEON path. Per-arch parity unit test
  validates the dispatch.
- **Π.13 Tier 2 (in ematix-parquet 0.16.2):** Six AVX2 fused
  `decode_predicate_bitmap_avx2_bw{12,14,15,16,17,18}` kernels
  mirroring the existing NEON specializations. Ships via the version
  bump alone; no patch override.
- **Π.13 Tier 3 (deferred):** AVX2 `unpack_indices_into_avx2_bw{22..32}`.
  Deprioritised — TPC-H lineitem dict widths land in the already-
  covered 12–21 range. Revisit only if a workload outside the 12–21
  band materialises.

This is **portability work, not a perf-amplifying lever**. It does not
join L1–L18; it closes a NEON-only architectural asymmetry. Expected
SF=10 wall-clock impact: ~0 on memory-BW-bound shapes, modest lift on
the cache-resident microbench surface.

### 5.2 Memory-bandwidth finding at SF=10 on commodity x86

PR #146's profiling on a commodity x86 minipc:

- **Q09 SF=10:** IPC 0.42, 32% LLC miss rate. Memory-bandwidth-bound.
- **Q03 / Q08 SF=10:** same shape (multi-fact-table joins with
  large materialised intermediates).
- **Win count on minipc SF=10:** 8 / 22 (vs Mac M3 Pro's 9–14 noise
  band per `bench-results/release-2026-05-24/`). Within the published
  spread, but at the bottom edge.

The M3 Pro doesn't show the same wall because Apple silicon has
~4–5× the per-core memory bandwidth of a commodity x86 desktop part.
The same code on the same data hits a different bottleneck.

#### What this implies for the V5 Tier 1 calendar

The V5 §1 Tier 1 sort (L8 → L13 → L10 → L14 → L17) ranks by
*universality across scale*, not by *hardware-bottleneck class*. The
new evidence introduces a secondary dimension:

| Lever | Closes which bottleneck class |
|---|---|
| L10 (dynamic filter propagation) | **IO / bandwidth** — prunes scan bytes before they reach the CPU. |
| L1 (read-side sidecar / page-Bloom) | **IO / bandwidth** — same axis, scale-specific. |
| L12 (zero-copy column pipeline) | **IO / bandwidth** — closes Utf8View buffer inflation that wastes BW. |
| L8 (CBO) | **Cross-cutting** — drives all of the above + compute-side selection. |
| L13 (custom hash join) | **Compute** — kernel speed gains. |
| L11 (template monomorphisation) | **Compute** — interpretive dispatch removal. |
| L17 (online learned join order) | **Cross-cutting** — feeds L8. |

On commodity x86 at SF=10, the BW-bound queries (Q03/Q08/Q09) won't
respond to compute-side improvements; they need IO reduction. The
*BW-bound queries are exactly the subset where ematix currently
loses to DuckDB at SF=10* per the V5 §1 acceptance gates (Q05 / Q07
/ Q08 are explicitly named).

**Calendar implication, not re-tiering:**

- **On Apple Silicon (canonical release bench):** V5's published
  sort (L8 → L13 → L10 → L14 → L17) holds as-is. Mac M3 Pro is not
  BW-bound at SF=10; compute-side levers pay back fully.
- **On commodity x86 (minipc + likely cluster targets):** L10 and L1
  carry more weight earlier; L13's kernel wins will be muted until
  BW pressure is reduced by IO-reduction levers landing first.
- **Cluster SF=100 target:** TBD by the cluster bench target's
  hardware tier. If the cluster target is BW-rich (e.g. EC2
  m7gd / m7i with NVMe + DDR5), L13 pays. If it's a constrained
  x86 SKU, L10/L1 pay first.

V5 §2 sequencing (L3 → L13 → L14 → L8 → L17 → L10) stays unchanged
in CURRENT.md *for the canonical release bench*. The minipc
re-validation is a portability check, not a sequencing override.

### 5.3 Cross-references

- PR #146 (in-flight): `https://github.com/ryan-evans-git/ematix-flow/pull/146`
- ematix-parquet PR #94 (merged → 0.16.2): `https://github.com/ryan-evans-git/ematix-parquet/pull/94`
- Mac M3 Pro baseline: `bench-results/release-2026-05-24/`
- Minipc bench provenance: PR #146 description; not in `bench-results/` yet.

---

## 6. What V5 does not address

For completeness, the items explicitly out of scope (V3/V4 covered some of these and were course-corrected away from):

- Team structure, hiring plan, org chart.
- Funding model, revenue, exit scenarios, commercial positioning.
- Competitive analysis vs DuckDB / Snowflake / Databricks beyond the technical surface needed to scope levers.
- License decisions, Apache Foundation / CNCF discussion.
- Web UI, pipeline runtime, or any non-engine surface.
- Cluster bench publication strategy (the engine perf inputs are scoped; publication is downstream).
- Any business framing of "is beating DuckDB at SF=10 the right goal" — V2 §5 took that up; V5 assumes the answer is "win across SF=1, SF=10, SF=100" and ranks levers accordingly.

---

## 7. References

V2 §9 references carry forward. V5-specific:

- V1: `docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE.md`
- V2: `docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V2.md` (canonical L1–L18 source)
- `project_optimizer_codegen_sensitivity.md` — codegen tax constraint; informs L3-first sequencing + sibling-crate scoping for L8/L11/L13
- `project_sigma_l_adaptive_runtime.md` — Σ.L substrate for L17
- `project_distributed_is_shipped.md` — distributed mode shipped; SF=100 publication open
- `project_sigma_q_l13_to_l16_session.md` — current SF=10 baseline 0.80, 14 wins
- `project_sigma_p_subquery_cse.md` — Σ.P SharedSubtree base for L4
- `project_sigma_j2b_v_landed.md` through `viii_landed.md` — Σ.J.2.b context-bloom transport (L10 / L13 substrate)
- `project_sigma_nf3_beats_stock.md` — Robin Hood substrate for L13
- `project_sigma_oc2_provider_landed.md` — RowGroupDecodeCache for L8 stats
- `project_ematix_parquet_v013_win.md` — sibling-crate scoping success template
- `project_dict_arrival_blocker.md` + `project_sigma_k_dict_arrival_ab.md` — L14 gating regimes
- `project_sigma_e5_q13_root_cause.md` — Utf8View tax case for L12
- `project_groupby_research_2026_05.md` — Photon anti-JIT thesis informing L9 deferral and L11 design
- `project_sigma_q_l10_landed.md` — PushDownLeftSemiRule substrate for L6 / L13 transition

---

*End of V5. Single execution sequence; per-lever acceptance gates; no business framing.*
