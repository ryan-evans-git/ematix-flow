# Phase DECORR — Correlated subqueries on the push engine (S3)

*(v2.0.0 Sprint S3 — see [`V2_SPRINT_PLAN.md`](V2_SPRINT_PLAN.md) §S3 and
[`plans/V2_SQL_SURFACE_GAPS.md`](plans/V2_SQL_SURFACE_GAPS.md) rows 4 & 5.)*

**Goal (sprint plan):** a subquery-decorrelation pass in the ematix
optimizer that produces joins the reorder/bloom rules can exploit, +
recursive-CTE coverage, driving `tpcds_validate` to 99/99 native.

**Goal (revised by the measurement — see the Verdict):** DataFusion
already decorrelates TPC-DS's correlated subqueries *well*; the actual S3
work is a **bug fix** — an ematix physical optimizer rule regresses 4
correlated-subquery queries that vanilla DataFusion executes fine.

**Phase code:** `DECORR`. **Track:** A (engine/SQL).

---

## ✅ VERDICT (2026-07-18) — S3 is a rule-regression fix, not a decorrelation pass

Two findings from `examples/decorr_probe.rs`, both the opposite of what
the sprint plan / gap-doc row 4 assumed:

**1. DataFusion decorrelates TPC-DS's correlated subqueries cleanly — no
pass needed, no quadratic.** On a **vanilla** DataFusion session
(`EMAT_PROBE_VANILLA=1`), all 6 correlated-subquery queries plan, execute,
and decorrelate to **hash-based Semi/Anti/Mark joins** — zero
`NestedLoopJoin`, zero per-outer-row re-evaluation:

| query | correlated subquery | vanilla DF join types | NLJ |
|---|---|---|---:|
| q10 | 3× correlated `EXISTS` (OR) | `Inner×5, LeftMark×2, LeftSemi×1` | 0 |
| q16 | `EXISTS` + `NOT EXISTS` | `Inner×3, LeftSemi×1, RightAnti×1` | 0 |
| q35 | 3× correlated `EXISTS` | `Inner×5, LeftMark×2, LeftSemi×1` | 0 |
| q41 | correlated **scalar** `count(*)` subquery | `Left×1` | 0 |
| q69 | 2× `NOT EXISTS` + `EXISTS` | `Inner×5, LeftAnti×2, LeftSemi×1` | 0 |
| q94 | `EXISTS` + `NOT EXISTS` | `Inner×3, LeftSemi×1, RightAnti×1` | 0 |

`EXISTS`→`LeftMark`/`LeftSemi`, `NOT EXISTS`→`LeftAnti`/`RightAnti`,
correlated scalar→`Left` join+agg. These are exactly the semi/anti join
modes the **set-op probe** found ematix's rules target *most*
(`push_down_left_semi`, `synthetic_left_semi`,
`force_collect_left_semi_build`, `swap_semi_join_build`, bloom sideband).
So gap-doc row 4's premise ("DF decorrelates, but the resulting joins may
not hit ematix's rules well") is **refuted twice over**: DF's
decorrelation is good, and its output lands on ematix's best-developed
join path. There is **no decorrelation pass to build**.

**2. The real gap: an ematix _physical_ optimizer rule breaks 4 of these
queries.** On the **ematix preset** session (`preset::session_context()`),
q10/q16/q69/q94 fail — while vanilla DF runs them:

| query | ematix preset | vanilla DF |
|---|---|---|
| q10, q69 | `PHYSICAL_PLAN_FAIL: No field named c.c_current_addr_sk` | ✅ runs |
| q16 | `PHYSICAL_PLAN_FAIL: No field named cs1.cs_ship_date_sk` | ✅ runs |
| q94 | `PHYSICAL_PLAN_FAIL: No field named ws1.ws_ship_date_sk` | ✅ runs |
| q35, q41 | ✅ runs | ✅ runs |

Localized precisely:

- **Stage = physical optimization.** The logical plan builds *and*
  optimizes fine (`into_optimized_plan()` succeeds); the failure is in
  `create_physical_plan()`. So the culprit is a **physical** rule in
  `PRODUCTION_PHYSICAL_RULE_NAMES` (`preset.rs`), not a logical/analyzer
  rule and not the dialect translation.
- **Trigger is structural, not join-type.** q35 **passes** with the
  *identical* join histogram to q10 (`Inner×5, LeftMark×2, LeftSemi×1`).
  So it is not "any semi/mark join breaks" — a physical rule drops a
  column (`c.c_current_addr_sk`, an outer join key) when it rewrites a
  specific plan shape that q10/q16/q69/q94 have and q35 does not.
- **Symptom.** A parent operator references an outer-table column that
  the rewritten child no longer exposes → schema-resolution error at
  physical planning.

This also means the S0.3 dialect audit's "all 99 plan" is **logical-plan
only**; 4 queries do not survive ematix's *physical* planning. (Confirmed
in `tpcds_validate`: q10/q16/q69/q94 report `EXEC_FAIL`.)

---

## 1. S3 deliverables (revised)

- **S3.1 — bisect to the culprit physical rule.** Toggle
  `PRODUCTION_PHYSICAL_RULE_NAMES` entries off one at a time (or run the
  physical chain incrementally) against q10 until the failing rewrite is
  isolated. Prime suspects — rules that rewrite Semi/Mark joins or prune
  projections: the semi-join family above, `dim_join_pushdown`,
  `agg_filter_pushdown`, `join_reorder`, `swap_emat_hash_join_rule`. Use
  the q35-passes/q10-fails contrast (identical join types) to characterize
  the exact plan shape that triggers the column drop.
- **S3.2 — fix or guard the rule.** Either correct the rewrite to
  preserve the referenced column, or narrow the rule's match predicate to
  skip the shape it mishandles. The twin-rule/pinned-name infrastructure
  (`production_chain_matches_pinned_names`,
  `twin_rule_chain_equals_production_single_node_preset`) must stay green;
  update the pin if a rule's applicability legitimately changes.
- **S3.3 — parity + regression guard.** After the fix,
  q10/q16/q69/q94 → `PASS parity=OK` vs DuckDB at SF1 in `tpcds_validate`;
  add `decorr_probe` (preset vs `EMAT_PROBE_VANILLA`) as the standing
  regression guard — preset must match vanilla on all 6, no
  `PHYSICAL_PLAN_FAIL`, no quadratic `NestedLoopJoin`.
- **S3.4 — hermetic contract test** (mirror the WIN/SETOP style): a tiny
  correlated `EXISTS` + `NOT EXISTS` + correlated scalar subquery on
  `preset::session_context()`, asserting both correct results *and* that
  the plan has no `NestedLoopJoin` with a Semi/Anti/Mark mode (pins "stays
  decorrelated to hash joins on the ematix session").

**Non-goals (cut from the sprint plan):**

- **No decorrelation pass** — DF's is already good (Finding 1).
- **No recursive-CTE work** — `WITH RECURSIVE` appears in **0 of 99**
  TPC-DS queries (gap-doc row 5 is out-of-suite); defer until a real
  workload needs it.
- **No new operator.**

## 2. Exit criteria

- q10/q16/q69/q94 execute and are row-parity-clean at SF=1 **and** SF=10
  (the sprint plan's 99/99-native bar, for the correlated-subquery tail).
- The culprit rule is fixed/guarded with the regression captured by
  `decorr_probe` + S3.4; pinned-name + twin tests green.
- Confirmed (already true in the probe, re-assert post-fix): every
  correlated subquery stays on hash Semi/Anti/Mark joins — no quadratic.

## 3. Assets

- `examples/decorr_probe.rs` — the diagnosis + repro (preset vs vanilla,
  logical-vs-physical split, join-type histogram, quadratic red-flag,
  join elapsed share). Re-runnable at any scale via `TPCDS_DATA_DIR`.
- Correlated-subquery queries: q10, q16, q35, q41, q69, q94.
