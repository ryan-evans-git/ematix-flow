# Phase GS — Grouping sets / ROLLUP / CUBE / GROUPING() on the push engine

*(v2.0.0 Sprint S1 — see [`V2_SPRINT_PLAN.md`](V2_SPRINT_PLAN.md) and
[`V2_TARGET.md`](V2_TARGET.md) §2.1.)*

**Goal:** execute `GROUPING SETS`, `ROLLUP`, `CUBE`, and the
`GROUPING()` / `GROUPING_ID()` functions **natively on the ematix
push/fused aggregate engine** — single-pass, vectorized, spillable —
instead of falling through to DataFusion's generic `AggregateExec`.

**Phase code:** `GS`. **Track:** A (engine/SQL). **Est:** one 2-week
sprint (S1), with S1.3 (spill) able to slip into S9 if Π isn't ready.

---

## ⛔ MEASUREMENT VERDICT (2026-07-18) — native operator REVERTED

**The native grouping-set operator was built, measured, and reverted. Do
not rebuild it on the "single-scan" premise — that premise is false.**

S1.2 shipped a working, correct `FusedGroupingSetAggregateExec` +
`InjectGroupingSetRule` (11 TPC-DS queries ran on it, row-parity clean).
Before optimizing per-set aggregation into the fused kernels, we measured
it against DataFusion's native grouping-set exec (`gs_ab_bench`, the
`EMAT_GROUPING_SETS_FUSED` A/B toggle):

| query (SF10) | DF-native | operator | verdict |
|---|---:|---:|---|
| q22 (5-set ROLLUP + AVG over a join) | 1353 ms | 2453 ms | **1.8× slower** |
| q18 (7-set ROLLUP) | 81 ms | 86 ms | 6% slower |
| q27 (avg-heavy ROLLUP) | 110 ms | 112 ms | neutral |

**Root cause — the design's Model A premise (§4.1) is wrong.** It assumed
DF re-scans the child per set, so a single-scan operator would win. But
DataFusion's grouping-set `Partial` node already reads the child **once**
and updates all sets in one pass. The operator therefore added
materialisation + `n_sets` passes over the materialised data for **no
scan saving** — strictly worse. A bespoke fused per-set accumulator would
not fix this; the disadvantage is the orchestration, not the kernel.

**Additional finding:** the existing fused string-key accumulators index
by the **first byte** of the value (correct only for TPC-H's single-char
flag columns), so they cannot correctly aggregate TPC-DS's multi-byte
group keys anyway — a bespoke multi-key kernel would have been required
even to *match* DF.

**Conclusion:** DataFusion's native grouping-set execution is already
correct **and** competitive. The S0.3 gap analysis's assumption that
grouping sets are a benchmark liability is **not borne out**. Reverted the
operator + rule + preset registration; kept the S1.1 value (plan-shape
pin `gs_plan_probe`, the semantic-contract tests, and §4.0 below, which
now document DF's shape for reference). The design in §4–§9 is retained
**as the rejected proposal + its measured refutation**, not as a plan.

---

## 1. Current state (why this phase exists)

Grouping-set aggregation is **correct today but not on our engine.**

- DataFusion 53 parses `GROUPING SETS`/`ROLLUP`/`CUBE` into
  `Expr::GroupingSet(...)` and lowers them to a physical `AggregateExec`
  whose `PhysicalGroupBy` carries **multiple group masks** (`groups:
  Vec<Vec<bool>>`, one null-mask per set) plus a synthesized
  `__grouping_id` column that disambiguates sets and backs `GROUPING()`.
- The ematix fused recognizer explicitly declines anything that isn't a
  **single** grouping expression — `fused_aggregate_filter_multi_agg_rule.rs`
  returns `None` when `exprs.len() != 1`
  ([:541](../crates/ematix-flow-core/src/fused_aggregate_filter_multi_agg_rule.rs)).
  So every grouping-set query bypasses `FusedAggregateExec`, its
  vectorized accumulators, filter-fusion, and the ematix spill path, and
  runs on DataFusion's generic hash aggregate.

**Consequence for v2:** TPC-DS leans on grouping sets far more than
TPC-H does (Q18, Q22, Q27, Q36, Q67, Q70, Q77, Q80, Q86). If we ship the
TPC-DS *benchmark* (S6) while these queries run on DF's generic exec,
we win the queries TPC-H already covered and lose the ones analysts
actually run. **The benchmark story is only honest if grouping-set
aggregation is vectorized on our engine.** Hence a dedicated phase.

---

## 2. Semantics primer (the contract we must reproduce)

### 2.1 The three sugar forms all desugar to a set-of-sets

```
GROUP BY ROLLUP (a, b, c)     ≡ GROUPING SETS ((a,b,c),(a,b),(a),())         -- k+1 sets
GROUP BY CUBE (a, b, c)       ≡ GROUPING SETS (every subset of {a,b,c})      -- 2^k sets
GROUP BY GROUPING SETS (...)  ≡ the sets, verbatim
```

DataFusion already normalizes ROLLUP/CUBE → an explicit list of sets at
logical planning. **We consume the normalized set list; we do not
re-implement the desugar.** (Guard: confirm DF 53 hands us the expanded
`groups` masks on `PhysicalGroupBy` for all three forms — S1.1.)

### 2.2 Rolled-up NULL vs data NULL — the disambiguation problem

For grouping set `(a)` in a `ROLLUP(a,b)` query, column `b` is output as
NULL because it is *aggregated away*, not because the data was NULL. A
correct implementation must distinguish these two NULLs. The mechanism
is a per-output-row **grouping id**: a bitmask where bit *i* = 1 iff
column *i* is **absent** (rolled up) in the set that produced the row.

```
set (a,b) → grouping_id 0b00 = 0    (both present)
set (a)   → grouping_id 0b01 = 1    (b absent)
set ()    → grouping_id 0b11 = 3    (both absent)
```

### 2.3 `GROUPING(col)` and `GROUPING_ID(c1, …)`

- `GROUPING(col)` → 1 if `col` is aggregated-away in this row's set, else
  0. It is exactly *one bit* of the grouping id.
- `GROUPING_ID(c1,…,cn)` → the integer formed by those bits.

TPC-DS uses these in `ORDER BY` and in `CASE` expressions to label
subtotal rows, so they must be first-class output columns, not a
post-hoc reconstruction. **`GROUPING()` reads the grouping id we already
carry — never recompute it from "is the value NULL?", which is wrong for
data NULLs.**

---

## 3. Queries in scope (the gate)

Row-parity vs DuckDB at SF=1, then SF=10:

| Query | Shape | Exercises |
|---|---|---|
| Q18 | `ROLLUP` over 6 cols, AVGs | wide rollup + many aggs |
| Q22 | `ROLLUP` over product hierarchy, `AVG(inv)` | rollup + AVG |
| Q67 | `ROLLUP` + windowed rank over the rollup | rollup feeding a window |
| Q77 | `GROUPING SETS`-style union of subtotals | explicit sets |
| Q27 | `ROLLUP` + `GROUPING()` in output | GROUPING() correctness |
| Q36 | `ROLLUP` + `GROUPING_ID` in `ORDER BY`/rank | GROUPING_ID ordering |

Q18/Q22/Q67/Q77 are the S1 exit gate; Q27/Q36 pull in `GROUPING()` /
`GROUPING_ID` and may extend into S2 if window interplay (Q67) slips.

---

## 4. Design

### 4.0 Pinned DF53 physical shape (S1.1 — risk retired)

Before any kernel (§7 risk "DF's grouping-set physical shape"), the
`gs_plan_probe` example
([`../crates/ematix-flow-core/examples/gs_plan_probe.rs`](../crates/ematix-flow-core/examples/gs_plan_probe.rs))
dumped the real DF53 physical plan for `ROLLUP`/`CUBE`/`GROUPING SETS`
over a hermetic in-memory table. Three facts are now **pinned** (two
corrected earlier assumptions in this doc):

DF lowers every grouping-set form to a two-phase hash aggregate:

```
ProjectionExec ( GROUPING()/GROUPING_ID → bit-extraction over __grouping_id )
  AggregateExec mode=FinalPartitioned  gby=[<cols…>, __grouping_id]   ← single mask; __grouping_id is an ORDINARY key here
    RepartitionExec Hash([<cols…>, __grouping_id])
      AggregateExec mode=Partial  gby=[(set0…),(set1…),…]             ← THE multi-set node (groups().len() > 1)
        <child>
```

1. **The multi-set node is the `Partial` AggregateExec.** `groups().len()
   > 1` appears only there; the `Final` node re-aggregates partials with a
   single mask, treating `__grouping_id` as a normal group key. **Rule
   target = the Partial node** (replace the whole Partial→Repartition→Final
   stack with `FusedGroupingSetAggregateExec` emitting the final
   `[<cols…>, __grouping_id, <aggs…>]` schema; DF's top Projection then
   works unchanged).
2. **Mask polarity — `true` = column ABSENT (rolled up), not present.**
   CUBE(a,b) → masks `[false,false]`=(a,b), `[true,false]`=(NULL,b),
   `[false,true]`=(a,NULL), `[true,true]`=(NULL,NULL). So
   `present(i) == !mask[i]`. *(Corrects §4.2's earlier "indices of present
   cols" framing.)*
3. **`__grouping_id` bit convention — leftmost group col is the HIGH
   bit.** For universe `[c0..c_{n-1}]`, column `i` occupies bit `n-1-i`;
   bit = 1 ⟺ that column is rolled up. So
   `__grouping_id(set) = Σ_{i : mask[i]==true} 2^(n-1-i)`. DF's projection
   reads `grouping(c0)` as `__grouping_id & 2 >> 1`, `grouping(c1)` as
   `& 1`, etc. **We must emit exactly this integer** — then
   `GROUPING()`/`GROUPING_ID` come for free from DF's already-planned
   projection; never recompute them from "is the value NULL?".

### 4.1 Execution model — chosen: single-pass, multi-table

Two candidate models:

- **A. Single-pass, one hash table per set.** Scan the child once; for
  each input row, update the accumulators of every grouping set, each
  set keyed on its own subset of group columns in its own hash table.
  Memory = Σ per-set tables. One pass over the (expensive) child.
- **B. Expand-then-aggregate** (DataFusion's shape). Emit each input row
  ×`n_sets` copies with nulled non-group columns + a grouping id, then a
  single hash aggregate keyed on `(grouping_id, cols…)`. Simpler, one
  table, but pushes **n_sets× the rows** through the accumulator loop.

**Decision: A (single-pass, multi-table), with a set-count fallback to
B.** Rationale:

- In TPC-DS the child is a scan/join over a fact table (`store_sales`,
  `catalog_sales`, `web_sales`) — **the scan is the cost.** Model B pays
  n_sets× on the accumulate loop *and* forces the child's output through
  an expansion; Model A pays the child once.
- Model A reuses the existing `FusedAggregateExec` per-set accumulators
  unchanged — each set is "a single grouping" the current vectorized
  path already handles. That is what keeps the "vectorized, not scalar
  fallback" promise **without** waiting on the Φ kernels crate.
- **CUBE risk:** `CUBE(k cols)` = 2^k tables. Beyond a threshold
  (`GS_MAX_SETS`, default 16 → k≤4 for CUBE) the multi-table memory
  blows up; above it, fall back to Model B (or DF) rather than OOM.
  Logged loudly (no silent cap — project discipline).

### 4.2 New operator: `FusedGroupingSetAggregateExec`

Lives beside `FusedAggregateExec` in `crates/ematix-flow-core/src/`.
Structurally a thin orchestrator over the existing accumulator core:

```
FusedGroupingSetAggregateExec
├── group_cols:  Vec<PhysicalExpr>        // the full column universe {a,b,c}
├── set_masks:   Vec<Vec<bool>>           // DF's masks verbatim; mask[i]==true ⇒ col i ROLLED UP (§4.0)
├── aggs:        Vec<AggExpr>             // reused from the fused path
├── tables:      Vec<GroupHashTable>      // one per set, keyed on the PRESENT cols (!mask[i])
└── grouping_id_of(set_idx) -> u64        // Σ_{mask[i]} 2^(n-1-i), DF's exact convention (§4.0)
```

Execution per input batch (single pass):

1. Evaluate `group_cols` and agg inputs once for the batch (shared).
2. For each set: project the batch's group columns to that set's subset,
   feed the **existing** vectorized accumulate kernel into `tables[i]`.
3. On finalize: for each set, emit its groups with (a) non-set columns
   set to typed-NULL, (b) a literal `__grouping_id` = `grouping_id_of(i)`
   column. Concatenate across sets.
4. `GROUPING(col)` / `GROUPING_ID(...)` output exprs read bit(s) of
   `__grouping_id` — planned as projections over that column, never
   recomputed from data.

Because step 2 delegates to the current fused accumulator, filter-fusion
(`fused_aggregate_filter_*`) and the vectorized SUM/COUNT/AVG kernels
apply **per set, unchanged**. This phase adds orchestration + id
bookkeeping, not new arithmetic kernels.

### 4.3 Planner interception

A physical optimizer rule (mirroring the existing fused rules) that:

1. Matches the **`Partial`** `AggregateExec` whose `PhysicalGroupBy` has
   `groups().len() > 1` (i.e. grouping sets — §4.0 confirms the multi-set
   node is the Partial, not the Final) **and** whose aggregates are all in
   the fused-recognizer's supported set. The rule replaces the whole
   Partial→Repartition→Final stack so the emitted schema
   (`[<cols…>, __grouping_id, <aggs…>]`) feeds DF's top Projection intact.
2. Reads the per-set null-masks from `PhysicalGroupBy.groups()` verbatim
   (mask polarity per §4.0: `true` = rolled up); emits `__grouping_id`
   with DF's exact bit convention so downstream `GROUPING()`/ORDER BY line
   up with no recomputation.
3. If `sets.len() > GS_MAX_SETS` **or** any aggregate is unsupported →
   **decline** (leave DF's exec in place). Correctness-first: we only
   take over shapes we can prove parity on.
4. Otherwise swap in `FusedGroupingSetAggregateExec`.

Opt-out env `EMAT_GROUPING_SETS_FUSED=0` (matches the
`EMAT_SCALAR_AGG_BOOST` convention) forces the DF path for A/B and
debugging.

### 4.4 Memory & spilling (S1.3)

Grouping-set state is the classic memory cliff — n_sets simultaneous
tables (the Q09 lesson: state that can't spill *livelocks*, it doesn't
gracefully degrade).

- **Interim (S1):** bound total live groups; on breach, AQE-style bump
  `target_partitions` and re-run the aggregate (reuse the existing
  scalar-agg-boost partition machinery), and enforce `GS_MAX_SETS`.
- **Full (integrate with Π, may land S9):** each per-set table is an
  independent spill unit → external-sort + run-merge to local SSD via
  the [`PHASE_PI_AGGREGATE_SPILLING`](PHASE_PI_AGGREGATE_SPILLING.md)
  path. Multi-table makes this *easier* than Model B: spill the largest
  table first, keep the small subtotal tables resident.

Do not gate S1's correctness exit on Π; gate it on "no OOM at SF=10 with
`GS_MAX_SETS` enforced," and file the SF=100 spill work into S9.

---

## 5. Testing (RED-first)

- **S1.1 oracle harness.** `tpcds_validate` row-parity vs DuckDB for each
  §3 query at SF=1 — written and RED before any kernel.
- **Semantic unit tests** (not just end-to-end):
  - rolled-up NULL vs genuine data NULL distinguished (inject a real
    NULL in a rollup column, assert `GROUPING()`=0 there, =1 on the
    subtotal).
  - `GROUPING_ID` integer matches the set's mask for every set of a
    `CUBE(3)`.
  - grand-total row (`()` set) present and correct.
- **Fallback parity.** Force `EMAT_GROUPING_SETS_FUSED=0` and assert
  byte-identical results to the fused path (guards the interception rule
  against silent divergence).
- **Vectorization proof.** Micro-bench a `ROLLUP` aggregate confirms the
  per-set path hits the vectorized accumulator, not a scalar fallback
  (S6's benchmark honesty depends on this).
- **Non-regression.** Non-grouping-set aggregates plan byte-identically
  (the rule is disjoint: `groups.len() == 1` never matches).

---

## 6. Sub-story breakdown (maps to S1)

| Story | What | Exit |
|---|---|---|
| **S1.1** ✅ | Plan-shape pin (`gs_plan_probe`, §4.0) + semantic contract tests (`tests/grouping_sets_semantics.rs`) + §3 `tpcds_validate` baseline | **DONE** — shape pinned (2 assumptions corrected); 3 semantic tests green on the shared session (incl. the data-NULL vs rolled-up-NULL gate); §3 queries q18/q22/q27/q36/q67/q77 all row-parity OK at SF1 |
| **S1.2** ⛔ | `FusedGroupingSetAggregateExec` + `InjectGroupingSetRule` — built, measured, **REVERTED** | Operator was correct (11 queries, row-parity clean) but **1.8× slower than DF-native** (see the Measurement Verdict at the top). Reverted; DF-native grouping-set execution retained. |
| **S1.3** | ~~Memory bound + spill~~ — **moot**: no native operator to bound; DF-native handles grouping-set memory. | n/a |

**S1.1 note on RED-ness.** The §3 queries and the semantic contract are
already *correct* on stock DataFusion (the S1 gap is fused *execution*,
not results), so those tests are GREEN now — they pin the exact result
set S1.2's operator must reproduce. The genuinely RED test — "the plan
runs on `FusedGroupingSetAggregateExec`, not DF's generic
`AggregateExec`" — needs the operator's type to exist, so it is written
first in **S1.2** (RED → green as the operator lands), together with the
`EMAT_GROUPING_SETS_FUSED=0` fallback-parity and vectorization-proof
tests from §5.

Q27/Q36 (`GROUPING()`/`GROUPING_ID` in output/order) ride S1.2's id
plumbing; if Q67's rollup→window interplay slips, it hands off to S2
(window frames).

---

## 7. Risks

- **DF's grouping-set physical shape.** ✅ **RETIRED (S1.1).** The
  `gs_plan_probe` example pinned the exact DF53 shape — see §4.0. Net
  corrections: the multi-set node is the `Partial` (not `Final`); mask
  bit `true` = column *rolled up* (not present); `__grouping_id` puts the
  leftmost group col in the high bit. The interception rule and operator
  are designed against these pinned facts.
- **CUBE blow-up.** `GS_MAX_SETS` is a real cap; document it and log when
  we decline, so "TPC-DS runs native" doesn't quietly mean "except the
  wide cubes."
- **GROUPING() correctness is subtle.** The data-NULL-vs-rolled-up-NULL
  test is the one most likely to catch a wrong implementation; it is a
  gate, not a nice-to-have.
- **Spill dependency.** If Π isn't ready, S1 ships with bound+cap (no
  OOM) but not true SF=100 spill; that's an explicit S9 follow, called
  out so the benchmark scope (S6 = SF=100) accounts for it.

---

## 8. Non-goals

- **New arithmetic kernels** — reuse the fused accumulators; Φ (the
  kernels crate) is a separate phase, not a dependency here.
- **The ROLLUP/CUBE desugar** — DF owns it; we consume expanded sets.
- **Window functions over grouping sets** — Q67's window half is S2's
  surface; this phase delivers the grouping-set aggregate it feeds.
- **Distributed grouping-set shuffle** — single-host fused exec here;
  mesh execution stays on the existing distributed path until proven.

---

## 9. Exit criteria (S1)

1. Q18/Q22/Q67/Q77 execute on `FusedGroupingSetAggregateExec`,
   row-parity clean vs DuckDB at SF=1 **and** SF=10.
2. `GROUPING()`/`GROUPING_ID` correct, including the data-NULL vs
   rolled-up-NULL distinction (semantic tests green).
3. Per-set aggregation confirmed vectorized (micro-bench), not scalar.
4. No OOM at SF=10 with `GS_MAX_SETS` enforced; SF=100 spill filed to S9.
5. Non-grouping-set plans byte-identical; `EMAT_GROUPING_SETS_FUSED=0`
   fallback byte-identical to fused output.
