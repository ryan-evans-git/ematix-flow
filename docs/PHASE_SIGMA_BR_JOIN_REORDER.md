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

#### Phase 2 measured (2026-05-29, cooled HW): SF=10 regressors do NOT flip at SF=100 → **scale-gate alone is net-negative**
Interleaved per-query OFF-vs-permissive-ON A/B at SF=100 (canary-verified
cool: Q14 854ms vs 925 clean; per-query OFF→ON back-to-back so thermal
drift hits both arms equally), 5 trials / 2 warmups:

| query | OFF (ms) | ON (ms) | Δ | verdict |
|-------|---------:|--------:|--:|---------|
| Q05 | 2349.6 | 2112.7 | **−10.1%** | WIN (funnel) |
| Q08 | 2448.0 | 2448.8 | ~0% | neutral |
| Q07 | 1997.7 | 2435.5 | **+21.9%** | REGRESS |
| Q11 | 293.4 | 352.0 | **+20.0%** | REGRESS |
| Q21 | 3682.8 | 4110.8 | **+11.6%** | REGRESS |
| Q02 | 348.7 | 371.6 | **+6.6%** | REGRESS |

**This refutes the V5 §5.2 thesis that the regressors would flip to wins at
scale.** They stay regressors, and big (Q07 +22%, Q11 +20%). So a pure
scale-gate that fires the *permissive* path above a size threshold is
**net-negative** — it fires all six and the four regressions swamp Q05's
single win. The scale gate is necessary (it preserves SF=1/SF=10) but
**not sufficient**: Phase 2 also needs **shape discrimination** that admits
Q05's funnel while rejecting Q07/Q02/Q11/Q21. The existing shape-gated
guards reject the regressors but ALSO reject Q05/Q08 (even at
`max_leaves=8`, post-2a; only Q10 fires) — so the open Phase 2 task is to
pin *which* guard rejects Q05 and relax exactly that one without
re-admitting the regressors. Raw numbers: `/tmp/sf100_reorder_ab.txt`.

#### Phase 2 guard attribution (2026-05-29): the blocker for Q05 is **only `max_leaves`** — regressors are caught by *other* guards
Added per-guard gate logging (`[reorder-gate]` under `EMAT_REORDER_DEBUG`)
and read each query's guard verdicts (ambiguous / like / aggkey are
max_leaves-independent):

| query | leaves | ambiguous | like | aggkey | other | rejected by |
|-------|-------:|-----------|------|--------|-------|-------------|
| Q05 WIN | 6 | false | false | false | — | **only `too_many` (max_leaves=4)** |
| Q09 WIN | 6 | false | false | false | — | **only `too_many`** |
| Q07 reg | 6 | **true** | false | false | — | ambiguous-names |
| Q02 reg | 6 | **true** | **true** | **true** | — | all three |
| Q08 (neutral) | 8 | **true** | false | false | — | ambiguous + too_many |
| Q11 reg | 3 | false | false | false | `jump_on_reject` at a 2-leaf `too_few` chain above it | jump-on-reject |
| Q21 reg | — | — | — | — | LeftSemi/Anti node → walker Jumps before the gate | `reject_under_left_semi_anti` |

**So Q05/Q09 (both wins) are blocked *purely* by the 4-leaf cap; every
regressor trips a *different, semantic* guard.** Raising `max_leaves` to 6
admits Q05/Q09 while ambiguous-names (Q07/Q08), all-guards (Q02),
jump-on-reject (Q11) and semi/anti-jump (Q21) keep the regressors out.
This is the surgical lever Phase 2 was looking for.

**Caveats for the implementation (not a flat bump):**
1. `lever_g_rejects_long_chain` asserts shape-gated rejects a *synthetic*
   single-nation 5-leaf chain at `max_leaves=4` (no ambiguous names, since
   TPC-H columns are table-prefixed). A flat bump to 6 makes that chain
   newly fire → the test breaks and must be re-framed (its "5-leaf ⇒
   reject" premise is exactly the policy we're revising). For the *actual*
   22q suite, the only chains admitted by 4→6 are Q05/Q09 (wins) + Q10
   (already fires) — but the synthetic test shows a flat global bump is
   broader than the suite needs.
2. Because the SF=100 win is scale-dependent (Q05 −10% at SF=100, ~neutral
   SF=10), the bump should be **scale-gated**: raise `max_leaves` to 6 only
   when largest-leaf `num_rows ≥ threshold`. Below it, keep 4.
3. **Harness bug** (`tpch_triangulation_bench`): the fires-detection probe
   `rewrite_fires_for_sql` hardcodes `reorder_inner_joins_shape_gated`
   (default `max_leaves=4`), ignoring `EMAT_REORDER_MAX_LEAVES`; the timed
   path's env override also proved flaky. Validating the bump cleanly needs
   either the env wired through both call sites or a direct default change.
4. **Ambiguous-names is load-bearing** — it rejects Q07 and Q08. It's
   plausibly a genuine proxy (Q07's dual-`nation` OR-pair both *creates*
   the ambiguity and *causes* the regression), but it's worth a confirming
   thought before relying on it as the regressor gate.

Open Phase 2 implementation: scale-gated `max_leaves`-6 + re-frame the
long-chain test + a clean 22q SF=100 A/B on cooled HW + production
OptimizerRule wiring (walkers aren't in `preset.rs`).

#### Phase 2 IMPLEMENTED + VALIDATED (2026-05-29): scale-gate lands the Q05 win, but Q10 (pre-existing) regresses → default-on still a wash
Implemented `ReorderOpts::scale_bump: Option<(min_rows, bumped_leaves)>`,
default `Some((100_000_000, 6))`. In `reorder_inner_joins_with_opts` the
gate computes `effective_max_leaves = if largest_leaf_rows ≥ min_rows {
bumped } else { max_leaves }`. Baked into `ReorderOpts::default()` so both
the shape-gated entry point and the bench inherit it; `unsafe_no_shape_gate`
sets `None`. New test `scale_bump_admits_q05_funnel` (14/14 join_reorder
tests green); the old `lever_g_rejects_long_chain` **still passes unchanged**
because at SF=1 the largest leaf (6M) is below the 100M threshold → no bump
→ the 5-leaf chain is still rejected. So no test reframe was needed — the
scale-gate preserves SF=1/SF=10 behaviour exactly. (Q09 turned out NOT to
be admitted — its `p_name LIKE '%green%'` trips the LIKE guard, which the
guard doc explicitly says "catches Q09". So the SF=100 firing set is just
{Q05, Q10}.)

**SF=100 firing-set probe** (cooled HW, scale-gated default): only **Q05**
(newly admitted, `[5,4,0,1,2,3]`) and **Q10** (`[2,0,1]`, already fired at
cap 4) fire. Nothing else.

**SF=100 A/B** (interleaved OFF vs shape-gated-ON, 8 trials × 2 reps,
canary-verified cool 852ms; agg_semi + dim_push ON in both arms):

| query | OFF (ms) | ON (ms) | Δ | rows |
|-------|---------:|--------:|--:|------|
| Q05 (newly admitted) | 2603 / 2483 | 2113 / 2219 | **−18.8% / −10.6%** WIN | 5 = 5 ✓ |
| Q10 (pre-existing fire) | 2899 / 2999 | 3231 / 3483 | **+11.5% / +16.2%** REGRESS | 3.88M = 3.88M ✓ |

Correctness: all row counts identical OFF↔ON.

**Verdict: the scale-gate works — it admits Q05 and delivers a clean
−14.7% SF=100 win, correctness preserved. BUT turning the shape-gated path
on at SF=100 is a *wash*: Q05 −380ms is cancelled by a Q10 +400ms
regression.** Crucially, **Q10 is a 3-leaf chain that fires at the base
cap of 4 — the scale-bump did NOT introduce it; it's pre-existing Lever-G
behaviour that regresses at SF=100 (the inverse of Q05: Q10 was a *win* at
SF=10, −20ms).** So the scale-gate is sound and ships the Q05 win as opt-in
infra, but **default-on at SF=100 is blocked on a *second* sub-lever**:
gate Q10's 3-leaf customer⋈orders⋈lineitem firing OUT at SF=100 (a scale
*ceiling* for that shape), or diagnose the Q10 reorder↔dim_push / build-side
interaction that flips it negative at scale. Numbers:
`/tmp/sf100_scalegate_ab.txt`, `/tmp/sf100_q5q10_clean.txt`.

#### Phase 2 Q10 root cause + FIX (2026-05-29): composite-leaf guard
Plan-diff (`sigma_q_explain_plan`, Q10 SF=100, OFF vs ON):
- **OFF:** `nation` is a 25-row `CollectLeft` broadcast on the *outermost*
  join — a cheap decoration applied last, after the fact joins narrow.
- **ON (reorder `[2,0,1]`):** `nation⋈customer` pulled *early* (CollectLeft),
  materialising a **15M-row** wide intermediate that becomes the **build
  side** of a Partitioned join vs orders⋈lineitem. Cheap at SF=10 (customer
  1.5M); +14% at SF=100 (15M).

**Why the DP front-loaded the tiny nation:** the reorder chain has only 3
"leaves" and **leaf[1] = `Filter(orders⋈lineitem)` is a composite subtree**
(dim_push emits filter-style joins that `flatten_inner_join_chain` can't
descend through). `estimate_leaf_card` hits its `u64::MAX/2` sentinel for a
Join → `× 0.3` filter selectivity = **2.77e18**. With Σ.BR.1a charging the
leftmost leaf its cardinality, placing the fact subtree first "costs" 2.77e18
→ astronomically avoided → the DP parks the 25-row nation leftmost instead.
So the garbage composite-leaf estimate *inverts* the order. This is the
concrete **reorder↔dim_push interaction**.

**Fix (committed in-tree, 15/15 tests green):** `leaf_is_estimable(plan)`
mirrors `estimate_leaf_card`'s recursable arms (TableScan through
Filter/Projection/SubqueryAlias). A new **unconditional** gate guard
`composite = !chain.leaves.iter().all(leaf_is_estimable)` rejects any chain
with a leaf the cost model can't size. Q10's composite `(orders⋈lineitem)`
leaf → REJECT → reorder no-ops → +14% gone. Q05's leaves are all real
TableScans (customer 15M, orders 11.25M, lineitem 600M, supplier 1M, nation
25, region 1) → still fires → −14.7% win preserved. New test
`rejects_composite_leaf_after_dim_push` (runs Q10 → dim_push → asserts
reorder no-op). Principled, not query-specific: "only reorder chains whose
leaves are base tables the cost model can estimate."

**Post-fix SF=100 A/B** (cooled, canary 812ms, 8 trials × 2 reps, correctness ✓):

| query | OFF (ms) | ON (ms) | Δ |
|-------|---------:|--------:|--:|
| Q05 | 2501 / 2365 | 2157 / 2186 | **−13.7% / −7.6%** (avg −10.7%) WIN |
| Q10 | 2780 / 2985 | 2965 / 3006 | **+6.6% / +0.7%** — neutral (was +13.8%) |

**The Q10 regression is eliminated** (was a tight +13.8% across both reps;
now +0.7% rep2 flat, +6.6% rep1 within Q10's ±6-9% noise band — both ON
ranges overlap OFF). The composite guard rejects Q10's poisoned chain; the
bench then fires a benign 4-leaf `[1,2,0,3]` (orders-first / nation-last,
the DuckDB-style order) that's near-neutral. **Net SF=100 flips from a wash
to positive:** Q05 −260ms vs Q10 ~+100ms (noise) → net ~−160ms. Numbers:
`/tmp/sf100_postfix_ab.txt`.

**Phase 2 status:** scale-gate + composite-leaf guard together make
shape-gated reorder **net-positive at SF=100** (Q05 win, no regressor),
SF=1/SF=10 unchanged, 15/15 join_reorder tests green.

#### Phase 2 production wiring (2026-05-29): `ReorderQueryPlanner` in `preset.rs`
The reorder previously ran only in the bench harness; library users (anyone
through `preset::with_optimizer_rules`) never got it. Wired it in as a
**`QueryPlanner`** (`crates/ematix-flow-core/src/reorder_query_planner.rs`),
NOT an `OptimizerRule` — the latter joins the optimizer's compiled rule loop
and has cost 5–8% geomean across unrelated queries
([[optimizer-codegen-sensitivity]]); a QueryPlanner runs once per query at
physical-planning time, *outside* that loop, and **post-optimization**, which
also exactly reproduces the validated bench config (reorder applied to the
already-optimized plan). `ReorderQueryPlanner` applies
`reorder_inner_joins_shape_gated` then delegates to `DefaultPhysicalPlanner`
(mirrors `DefaultQueryPlanner`). Installed in
`with_optimizer_rules_and_registry` via `with_query_planner`, **default ON**,
`EMAT_REORDER_QP=0` to disable. The bench is unaffected (it builds its own
`SessionStateBuilder`, not via preset, and keeps its manual reorder for A/B).
Tests: `reorder_query_planner` 1/1, `preset` 4/4 (planner installed by default
doesn't break the dict/cache tests), `join_reorder` 15/15.

**Remaining:** a perf confirmation *through the preset path* — the
triangulation bench uses its own context, so a preset-backed timing harness
(or pointing the bench at preset) is the gate before claiming the library-user
win. Mechanism + correctness are validated; the codegen tax is structurally
avoided (QueryPlanner ≠ OptimizerRule). (Committed f1a456b; harness 8d83ada.)

#### Phase 2 / #194 (2026-05-29): generalised to the full walker pipeline (`FlowQueryPlanner`)
The first preset-path timing run (8d83ada) showed the library path was in a
different regime from the bench — because preset installed *only* the reorder,
not the bench's full pre-plan walker pipeline. `ReorderQueryPlanner` →
**`FlowQueryPlanner`** (`crates/ematix-flow-core/src/flow_query_planner.rs`):
applies **agg_semi → dim_push → reorder** in bench order, each self-gated
(`EMAT_AGG_SEMI` / `EMAT_DIM_PUSH` / `EMAT_REORDER_QP`, default ON, opt-out),
then delegates to `DefaultPhysicalPlanner`. So library users now get the same
pre-plan rewrites the bench validates (agg_semi: Q17/Q08/Q18; dim_push: Q10;
reorder: Q05). (Partition/batch autotune is OFF even in the bench since
b190613, so it's not a parity gap.) **Correctness gate: SF=1 row counts
identical ON vs all-OFF across all 22 queries (0 mismatches)**; new test
`plans_all_tpch_queries_through_library_path` plans all 22 through the
pipeline; preset 4/4, join_reorder 15/15 green.

#### Phase 2 / #194b (2026-05-29): re-optimize the rewritten plan — TIMING GATE CLOSED
The first clean preset-path A/B (machine quiet) showed the reorder **neutral**
on Q05 (ON 2603 vs OFF 2629, −1%) even though the funnel was plan-confirmed —
and reorder-ON ran ~0.5s slower through preset than the bench. Root cause:
`SessionState::create_physical_plan` optimizes the plan and *then* calls the
query planner, so `FlowQueryPlanner` receives an already-optimized plan,
applies the walkers (which restructure the joins), and goes straight to
physical planning — the reordered plan is **never re-optimized**, leaving
filters/projections positioned for the old join order. The bench re-optimizes
implicitly (`execute_logical_plan(...).collect()` re-runs the optimizer on the
reordered plan), which is why its −14.7% appeared and preset's didn't.

Fix: `FlowQueryPlanner::create_physical_plan` now calls
`session_state.optimize(&rewritten)` after the walkers (best-effort,
falls back on error). Safe — the bench proves the reorder survives
re-optimization (else its ON-vs-OFF delta would collapse to zero). Re-validated
SF=100 (quiet machine, 4 trials, ±1%): **Q05 reorder ON 2351 ± 25 vs OFF
2598 ± 27 = −9.5% WIN** (was −1% pre-fix; ON now ≈ the bench's reorder-ON
~2.1s). `plans_all_tpch_queries_through_library_path` still green. **The Q05
SF=100 funnel win now reaches library users through the preset path** — the
production-wiring perf gate is closed. (Earlier 40s/128s preset times were
post-session swap + post-reboot daemon storm, not a real regime gap.)

#### Phase 2 prereq (2026-05-29): a **cost-improvement gate is NOT viable** — use the scale gate
Before building a "fire only when modeled cost improves substantially"
gate, we instrumented the DP to emit the modeled input-order vs
chosen-order cost for every chain it fires on (`EMAT_REORDER_COST=1`,
helper `cost_of_fixed_order`), and ran the permissive path over all 22q
at SF=1. Ratio = `chosen_cost / input_cost` (lower = bigger modeled win),
strongest firing chain per query:

| ratio | query | wall-time class | input_cost | chosen_cost |
|------:|-------|-----------------|-----------:|------------:|
| 0.0199 | Q11 | **REGRESS** (SF=10) | 1,632,000 | 32,401 |
| 0.0398 | Q21 | **REGRESS** (SF=10) | 1,824,407 | 72,544 |
| 0.0633 | Q02 | **REGRESS** (SF=10) | 2,560,000 | 162,006 |
| 0.1566 | Q05 | WIN (SF=100 −9.8%) | 622,570 | 97,514 |
| 0.3040 | Q07 | **REGRESS** (SF=10) | 721,142 | 219,251 |
| 0.3465 | Q09 | WIN (SF=10 −18ms) | 14,462,912 | 5,011,001 |
| 0.4554 | Q10 | WIN (SF=10 −20ms) | 269,250 | 122,625 |
| 0.5304 | Q08 | WIN (SF=100 −5%) | 376,197 | 199,536 |

**No single threshold separates wins from regressors**, and the signal is
*inverted* at the strong end:
- The three queries the model is MOST confident about (Q11/Q21/Q02,
  ratio 0.02–0.06, "10–50× less work") are all **regressors**. The
  cardinality reduction the DP credits is waste the runtime levers (L9
  bloom, semi-join pushdown) ALREADY eliminate at SF=10 — so the modeled
  gain double-counts, and the reorder only adds plan churn / build-side
  disruption on top.
- The named counterexample alone is conclusive: regressor **Q07 (0.304)
  sits strictly between wins Q05 (0.157) and Q08 (0.530)** — not linearly
  separable by any threshold.
- **Root reason the ratio can't work:** it is **scale-invariant by
  construction** (both costs scale ~linearly with SF), so it carries zero
  information about the one axis that actually decides the outcome —
  absolute scale (SF=100 wins, SF=10 neutral/regress). Absolute modeled
  cost at SF=1 doesn't separate either (regressors have the *larger* costs
  here). Conclusion: gate on a **scale signal** (largest-leaf `num_rows`,
  per Phase 2 above) validated by SF=100 wall-time — NOT on the model's
  cost or cost-ratio. The model's improvement estimate is uncorrelated
  (even anti-correlated) with marginal wall-time on current HW.

The `EMAT_REORDER_COST` instrumentation stays as infra for future
estimator work.

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
- **2b (filter propagation — the general win): NOT PURSUED (2026-05-29) — it
  splits into a half DataFusion already ships and a half that's fundamentally
  runtime.**
  - *Plan-time-literal half* (`a.x = b.x` ∧ `P(a.x)` ⇒ `P(b.x)` for a sargable
    `P` on the join key): **already implemented by DataFusion's stock
    `push_down_filter::infer_join_predicates`** (`infer_join_predicates_from_predicates`
    + `try_build_predicate`/`replace_col` rewrite the predicate across the
    equi-join key pairs, with inner-join / null-restrict gating —
    datafusion-optimizer-53.1.0/src/push_down_filter.rs:568,625). ematix-flow
    gets it for free; re-implementing it as a walker would be pure duplication.
    And it's moot for TPC-H anyway: **no TPC-H query filters on a join-key
    column** (every dimension filter is on a descriptive attribute — `r_name`,
    `p_type`, `c_mktsegment`, dates).
  - *Data-dependent half* (the doc's own `r_name='ASIA'` → qualifying
    `nationkeys` → prune `customer`/`lineitem` scans): the surviving key set is
    **not knowable at plan time** (it depends on which dimension rows pass the
    descriptive filter), so it is inherently a **runtime** technique — exactly
    what the **L9 bloom sideband** already does (build a bloom from the filtered
    dimension's join keys, probe the fact scan). There is no plan-time lever to
    add here.
  - Net: 2b is a non-lever for both ematix-flow and the TPC-H floor. Closed
    without code.
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
