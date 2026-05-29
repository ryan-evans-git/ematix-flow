# Σ.BR — SF=100-justified join reorder (left-deep revival → bushy escalation)

**Status:** Phase 0 (SF=100 re-validation spike) — RUNNING 2026-05-29
**Owner:** ryan-evans-git
**Supersedes the disposition of:** [Σ.T](PHASE_SIGMA_T_JOIN_REORDER.md) (shelved opt-in, default OFF)
**Driver:** the SF=100 3-engine result (`bench-results/sf100-3engine-summary-2026-05-29.md`)

---

## 1. Why revisit now — the SF=100 evidence

Σ.T built a working cost-based join-reorder pre-plan walker
(`crates/ematix-flow-core/src/join_reorder.rs`, 1514 lines, 5 tests) and
then **shelved it as opt-in, default OFF**, because at SF=10 on the M3 Pro
it was **neutral on Q05 (+5%, in noise)** and **regressed Q08 (+49%)**.

The shelving decision was correct *for the scale it was measured at* and
wrong as a permanent verdict. The 2026-05-29 SF=100 single-node run shows
the queries the reorder targets are exactly where we now lose to DuckDB,
and the gaps **amplify with scale** (this is the V5 §5.2 bandwidth-bound
prediction, now measured):

| Query | SF=10 ematix/DuckDB | SF=100 ematix/DuckDB | What the reorder would fix |
|---|---:|---:|---|
| Q18 | 1.07× (near-parity) | **3.00×** (6953 vs 2317 ms) | 60M→600M intermediate / build-side |
| Q05 | 1.28× | **1.48×** (2275 vs 1540 ms) | delay region/nation funnel → 24M→2.4M intermediate |
| Q17 | win | **1.19×** (loss) | (decorrelation — *separate* lever, see §6) |
| Q10/Q16 | win | loss | dimension-funnel ordering |

The strategic mandate ([[strategic-goal-floor-focus]]) is to hold ground at
SF=100, where intermediate-materialization waste is the dominant cost — not
to chase SF=10 wins on bandwidth-rich hardware. **SF=100 is the right scale
to validate join reorder.** That is the first thing Σ.T never did.

---

## 2. Learning from why Σ.T "failed"

Two distinct failure causes, both now addressable:

### 2a. It was bench-gated at the wrong scale (SF=10) — FIXED by the target

At SF=10 on the M3 Pro (4–5× the memory bandwidth of commodity x86), the
60M→24M intermediate in Q05 doesn't hurt enough to matter — the reorder is
neutral. The whole premise of the lever (shrinking intermediates) only pays
when the intermediate is large relative to bandwidth. **SF=100 is that
regime.** Phase 0 (below) measures it directly.

### 2b. The cost model is blind to string-equality selectivity — FIXABLE, ~free now

This is the concrete Q08 regression. The DP ranks orderings by summed
intermediate cardinality. Intermediate card = `(prev × leaf) / NDV(keys)`.
The **leaf** cardinality comes from `estimate_leaf_card` →
`predicate_selectivity` ([join_reorder.rs:534](../crates/ematix-flow-core/src/join_reorder.rs:534)),
which only models `col = lit` selectivity for **Int32/Int64** columns (via
min/max range). String equality falls through to a **flat 0.1**
([line 569](../crates/ematix-flow-core/src/join_reorder.rs:569)).

So Q08's `part` leaf (`WHERE p_type = 'ECONOMY ANODIZED STEEL'`) is
estimated at `200K × 0.1 = 20K` rows when the true count is `200K / 150 ≈
1.3K` (p_type has 150 distinct values). The DP therefore under-prioritises
`part`-first and picks `orders`-first — the +49% regression.

**Why it's nearly free to fix now:** when Σ.T was built (2026-05-25),
string-column NDV did not exist in the provider stats. It does now —
Σ.AH.2 Story 1'.2 populates `distinct_count` from dict-page headers
([ematix_fast_parquet.rs:1715](../crates/ematix-flow-core/src/ematix_fast_parquet.rs:1715)),
verified by the test asserting `p_type` lands with
`distinct_count = Inexact(150)`
([ematix_fast_parquet.rs:4274](../crates/ematix-flow-core/src/ematix_fast_parquet.rs:4274)).
The sibling estimator `leaf_col_ndv`
([line 963](../crates/ematix-flow-core/src/join_reorder.rs:963)) **already**
reads `distinct_count`. Only `predicate_selectivity` was never updated to
use it. The fix is: for `col = lit`, prefer `1/distinct_count` over the flat
0.1 — a handful of lines, reusing the exact pattern in `leaf_col_ndv`.

---

## 3. Left-deep vs bushy — the honest scope call

The request was framed as **bushy** join-reorder. The evidence says the
high-ROI lever for the queries in question is the **already-built left-deep
reorder with a fixed cost model** — and bushy is a much larger build whose
incremental benefit on these specific star/snowflake schemas is unproven.

- **Q05** is a dimension funnel (`region→nation→customer→orders→lineitem`,
  plus `supplier` on a 2-key join). Σ.T test #4
  (`reorders_q05_shape_against_real_data`) already proves the left-deep DP
  reaches **DuckDB's exact order** at SF=1. Q05 does not need bushy — it
  needs the funnel ordered first, which left-deep does once the cost model
  sees that `region(ASIA)` is tiny.
- **Q08** is `part(tiny) ⋈ lineitem` then the nation/region funnel — DuckDB's
  plan is "similar shape" (Σ.T §Q08). Left-deep expresses it **iff** the
  cost model knows `part` is tiny → the §2b fix.
- **Q18** is not a reorder problem; it's a semi-join push + build-side choice
  (Σ.Q.L10 `EMAT_PUSH_SEMI`). The SF=100 3.0× says that path isn't winning
  at scale — likely build-side selection on the 600M probe. Investigated in
  Phase 0 via its plan, may split into its own story.

**Decision:** revive left-deep first (Phases 0–2). Build **bushy** (Phase 3)
**only if** SF=100 left-deep leaves Q05/Q08 materially short of DuckDB. This
is the discipline Σ.T lacked: prove the cheap mechanism at the right scale
before building the expensive one. Bushy enumeration is O(3^N) subset-pair
DP vs left-deep's O(N·2^N); it pays only for snowflake dim-subtree pre-joins
that left-deep cannot thread, and we have no measured TPC-H case yet.

---

## 4. Phase 0 — SF=100 re-validation spike (RUNNING)

The decisive, cheap experiment: does the **existing** permissive left-deep
reorder (`EMAT_REORDER=1`, string bug still present) move Q05/Q08/Q18 at
SF=100? Ematix-only A/B, 3 trials / 1 warmup, on the local 34 GB SF=100 set:

```
# baseline (reorder OFF) vs EMAT_REORDER=1, queries 5,8,18
TPCH_DATA_DIR=examples/tpch/data/sf100 TPCH_TRIALS=3 TPCH_WARMUPS=1 \
  TPCH_SKIP_POLARS=1 TPCH_SKIP_DUCKDB=1 TPCH_QUERIES=5,8,18 \
  [EMAT_REORDER=1] TPCH_OUT=/tmp/reorder-spike/{baseline,reorder}.md \
  ./target/release/examples/tpch_triangulation_bench
```

### 4.1 Outcomes & branches

- **(A) Reorder already wins materially at SF=100 even with the string bug**
  → Phase 1 collapses to: fix §2b for Q08 + flip default-on at SF=100, guard
  off at SF=10. Smallest path.
- **(B) Helps Q05/Q18 but Q08 still regresses (the known string bug)** → do
  the §2b fix, re-spike Q08, then Phase 2 gate. Expected outcome.
- **(C) Doesn't move the needle even at SF=100** → the premise (intermediate
  materialisation is the SF=100 cost) is wrong or the reorder doesn't fire on
  the optimized plan. Capture why; pivot to build-side selection (Q18) and/or
  re-scope bushy.

### 4.2 Result (2026-05-29): **Outcome C — existing reorder REGRESSES at SF=100**

Permissive `EMAT_REORDER=1`, ematix-only, 3 trials / 1 warmup, SF=100:

| Q | reorder OFF | reorder ON | Δ | rows |
|---|---:|---:|---:|:--|
| Q05 | 2278.95 ± 42.79 | 2415.08 ± 17.67 | **+6.0%** | 5 (✓) |
| Q08 | 2373.68 ± 71.95 | 3002.59 ± 84.84 | **+26.5%** | 2 (✓) |
| Q18 | 4079.68 ± 841.50 | 4928.21 ± 99.33 | **+20.8%** | 6398 (✓) |

Row counts unchanged → the rewrite is **correct**; this is purely a perf
regression. The premise "SF=100 makes the existing reorder win" is **false as
stated**. But the mechanism isn't the problem — the **cost model is**. Debug
trace (`EMAT_REORDER_DEBUG=1`) of the chosen orders:

- **Q05** → DP picked `[2,1,3,4,5,0]` = **lineitem(600M) FIRST**, then orders,
  supplier, nation, region, customer. The exact inverse of DuckDB's funnel.
  region (est 1) and nation (est 25) are correctly tiny — yet placed **last**.
- **Q08** → `[3,1,0,4,5,2]` = **orders-first, part third** (part should lead).
- **Q18** → orders leaf gets `est_card = i64::MAX/2` (the "stats Absent"
  sentinel) → the DP orders **blind**; Q18 isn't a reorder target anyway.

**Two cost-model defects, both confirmed:**

1. **Leftmost leaf is free.** The DP base case is
   `dp[1<<i] = { cost: 0, card: cards[i] }`
   ([join_reorder.rs:666](../crates/ematix-flow-core/src/join_reorder.rs:666)),
   and cost accumulates only the *post-join* intermediate cards from the 2nd
   leaf on. So the leftmost table's own size is **never** in the cost — putting
   the 600M-row `lineitem` first is "free." But in a left-deep plan the
   leftmost table is the initial build side; its size dominates. Selinger cost
   must charge the left input's build at every join, including the first.
2. **Over-optimistic / absent FK-NDV.** Even the post-join intermediates look
   small because the `(|L|×|R|)/max_ndv` divisor over-estimates NDV on FK keys
   (e.g. `l_orderkey`), and Q18's `orders` stats are Absent → sentinel. Plus
   the §2b string-eq flat-0.1 (Q08 `part`).

**Decision — branch C with a constructive pivot:** the lever is neither "the
reorder mechanism is missing" nor "we need bushy." **The cost model is
inverted, and bushy enumeration would inherit the same broken cost model and
pick equally-bad-or-worse orders over a larger search space.** The prerequisite
is a cost-model rework whose acceptance gate is *"the DP reproduces DuckDB's
order"* — funnel-first for Q05, part-first for Q08 — **validated at SF=100**,
before any default-on or bushy work. See revised § 5.

---

## 5. Phasing

### Phase 0 — SF=100 re-validation spike (running) — ~1 day
Measure existing `EMAT_REORDER=1` at SF=100 on Q05/Q08/Q18. Verify row
counts unchanged (correctness). Pick branch A/B/C. **Decision recorded in
§4.2.**

### Phase 1 — Cost-model: charge the leftmost-leaf build cost (1a) — LANDED 2026-05-29
The DP was parking the largest table leftmost for free. **1a** seeds the base
case with `cost = card` not `0` ([join_reorder.rs:666](../crates/ematix-flow-core/src/join_reorder.rs:666)),
charging the first build side. Validated:
- **Q08 → part-first** (DuckDB's order): SF=10 +42%→−1.4%, SF=100 +26%→**−5.6% win**.
- Q05 → orders-first (off lineitem-first) but NOT the funnel — blocked on a
  missing join edge, see Σ.BR.2.

Unit test `reorders_q08_to_part_first` asserts the chosen order (green). Reorder
stays opt-in (`EMAT_REORDER`), so landing 1a changes no default behaviour. The
order-asserting test is the lesson from Σ.T (its test #4 only asserted "plan
changed" and even rationalised lineitem-first — it never caught the inversion).

Deferred into Σ.BR.2's cost-model revisit (they only bite once Q05's funnel edge
exists): **1b** string-eq NDV selectivity in `predicate_selectivity` (`col=lit`
→ `1/distinct_count`); **1c** FK-NDV realism in `leaf_col_ndv` (cap range-NDV at
`num_rows`).

### Phase 2 — SF=100 re-spike + default-on gate + SF=10 guard — ~2 days
- Gate the reorder on a **scale signal**, not an env var: enable when the
  largest leaf's `num_rows` exceeds a threshold (e.g. ≥100M — the regime
  where intermediates dominate). Below it, no-op (preserves the SF=1/SF=10
  neutral-to-positive behaviour; avoids the codegen-tax risk of a
  globally-on rule, [[optimizer-codegen-sensitivity]]).
- 22q SF=100 A/B (ematix-only) — gate: net-positive geomean, **zero**
  correctness drift, no single-query regression > noise.
- Keep `reorder_inner_joins_shape_gated` guards (LIKE, aggregate-join-key,
  ambiguous-names, LeftSemi/Anti) — they encode real SF=10 regressions
  (Q02/Q07/Q21) that still apply.

### Phase 3 — Bushy escalation — ONLY IF Phase 2 leaves Q05/Q08 short — ~2 wk
- Extend the DP state from `order: Vec<usize>` (left-deep) to subset-pair
  enumeration: `best[S] = min over (S1,S2 partition of S) of
  cost[S1]+cost[S2]+joincost(S1,S2)`. O(3^N), fine for N≤12.
- Rebuild emits a bushy `LogicalPlan` (two `LogicalPlanBuilder` subtrees
  joined). **CSE risk:** DataFusion's CSE does not share Join outputs/builds
  ([[sigma-qm-slice2-rejected]], [[sigma-qm-slice4-spike-rejected]]) — a
  bushy plan that reuses a dim subtree in two branches will double-build it.
  Bushy must produce a *tree* (each leaf used once), which it does by
  construction; the risk is only if a later rule re-expands. Bench-gate.

---

## 6. Out of scope (separate levers)

- **Q17** (SF=100 1.19× loss): scalar-subquery decorrelation
  (`LEFT_DELIM_JOIN`-equivalent), not join order. Σ.U / Σ.R.2 territory.
- **Q07**: OR-predicate splitting on the `nation × nation` tautological pair.
- **Q06/Q01**: decode-bound, not plan-bound.

---

## 7. Risks

1. **Cost-model garbage** → worse plans than FROM-order. Mitigation: the
   §2b fix is the main estimator improvement; bench-gate every change; the
   scale gate means we only act where the upside is large.
2. **Codegen tax** (5–8% from optimizer-rule perturbation,
   [[optimizer-codegen-sensitivity]]). Mitigation: stays a **pre-plan
   walker**, not a PhysicalOptimizerRule (Σ.T's host decision holds).
3. **Reorder ↔ Σ.Q.L10 / L9 composition.** Both rewrite the LogicalPlan.
   The shape-gated guards already reject LeftSemi/Anti subtrees (Σ.AL). Phase
   0 plan dump confirms order of application.
4. **Correctness.** Reorder must be row-count-identical. Σ.T test #5 +
   the bench's per-query row-count check + a 22q SF=100 correctness pass.

---

## 8. Decision points

- [ ] **Phase 0 branch** (A/B/C) — from §4.2 spike result.
- [ ] Scale gate threshold (largest-leaf num_rows ≥ ?) — tune in Phase 2.
- [ ] Bushy or not — gated on Phase 2 residual vs DuckDB.

---

## 9. Σ.BR.2 — equivalence-class / transitive equi-predicate pass (Q05 + general)

**Status: 2a LANDED 2026-05-29.** The transitive-edge derivation is wired into
the reorder DP's connectivity; `reorders_q05_to_region_first` is green. At
SF=100 Q05 now picks the DuckDB funnel
`region→nation→customer→orders→lineitem→supplier` and **wins −9.8%**
(2395→2160 ms, vs +6–10% before), variance tightened ±120→±24 ms; Q08 holds
**−5.0%**. Both row-count-correct. `rewrite_preserves_query_result` confirms the
implied edges are result-preserving. **2b (filter propagation) remains;** so do
the deferred 1b/1c cost-model refinements (not needed for Q05's order — 2a+1a
sufficed).

**Why (confirmed 2026-05-29 against the real plan).** Q05's optimized plan has
`c_nationkey = s_nationkey` ∧ `s_nationkey = n_nationkey` but NOT the transitive
`c_nationkey = n_nationkey`. DataFusion's default logical optimizer doesn't
derive it. Without that edge `customer` is reachable only via a 25-distinct
`nationkey` join to `supplier` (a many-to-many blowup), so **no cost model —
however good — can produce DuckDB's `region→nation→customer→…` funnel.** 1a gets
Q05 off lineitem-first, but only to orders-first.

**Blast radius — narrow in TPC-H, foundational in general.** This shape is
essentially unique to Q05 among the 22 (Q07/Q08/Q09/Q10 write dimension joins
directly). But it is foundational for real star/snowflake workloads, with two
faces:
1. **Join-reorder precondition** — the reorder is blind to any plan needing a
   derived edge; this caps Σ.BR's reach across the whole shared-dimension-key
   class, not just Q05.
2. **Cross-table filter propagation** — equivalence classes let a filter on one
   column (`r_name='ASIA'` → qualifying nationkeys) propagate to *every* equal
   column (pushed onto `customer` and `lineitem` scans), pruning before any
   join. Independent of join order; a large part of DuckDB/Spark/Postgres'
   star-schema speed. Our only current analogue is the **runtime** L9 bloom
   sideband — there is no **plan-time** propagation.

**Design — a pre-plan equivalence-class pass, runs before reorder:**
- Build equivalence classes by union-find over `Column`s, seeded from every
  equi-join predicate (`a = b`) and equality filter (`a = lit`).
- **2a (edges — unblocks Q05 reorder):** for two columns in the same class that
  live in different leaves with no existing `on` edge, synthesize the implied
  equi predicate. Logically redundant ⇒ result-preserving; it only gives the
  planner freedom. Guard the ambiguous-name trap with qualified columns.
- **2b (filter propagation — the general win):** for a class with a
  literal-equality or a value-set derivable from a filtered small dimension,
  emit the derived predicate onto the other members' scans. Phase after 2a.
- Host: pre-plan walker (consistent with reorder; avoids the optimizer-rule
  codegen tax, [[optimizer-codegen-sensitivity]]). Compose: equivalence pass →
  reorder.
- **Fold in the deferred 1b/1c cost-model fixes here** — once the funnel edge
  exists, Q05's NDV/selectivity must be realistic for the DP to pick it.

**Acceptance:** the ignored `reorders_q05_to_region_first` test goes green; Q05
SF=100 flips to a win; 22q SF=10/SF=100 no regression; all 22 row counts
unchanged.

**Risk:** predicate-set explosion on wide classes — cap class size / only emit
edges that connect otherwise-disconnected leaves. Composition with L9 (2b
overlaps the runtime bloom; plan-time should precede/subsume it).

## 10. References

- [Σ.T design + Phase 3 disposition](PHASE_SIGMA_T_JOIN_REORDER.md)
- SF=100 evidence: `bench-results/sf100-3engine-summary-2026-05-29.md`,
  memory [[strategic-goal-floor-focus]] §"SF=100 validation"
- Q05 reorder no-go at SF=10: [[gap-closing-loop-2026-05-28]]
- Q18 plan diff: [[q18-sf10-duckdb-plan-diff]]
- CSE-doesn't-share-Join lessons: [[sigma-qm-slice2-rejected]],
  [[sigma-qm-slice4-spike-rejected]]
- Codegen-tax → pre-plan walker: [[optimizer-codegen-sensitivity]]
- NDV infra: Σ.AH.2 Story 1'.2 dict-page distinct_count
