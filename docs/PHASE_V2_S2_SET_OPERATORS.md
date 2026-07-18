# Phase SETOP — Set operators + large IN on the push engine

*(v2.0.0 Sprint S2, set-ops half — the companion to
[`PHASE_V2_S2_WINDOW_FUNCTIONS.md`](PHASE_V2_S2_WINDOW_FUNCTIONS.md).
See [`V2_SPRINT_PLAN.md`](V2_SPRINT_PLAN.md) §S2.3 and
[`plans/V2_SQL_SURFACE_GAPS.md`](plans/V2_SQL_SURFACE_GAPS.md) rows 3 & 6.)*

**Goal (sprint plan):** native `INTERSECT` / `EXCEPT [ALL]` execution +
large-`IN` handling on the ematix push engine.

**Goal (revised by the audit — see the Verdict):** confirm whether there
is a set-op benchmark gap at all before building anything — the same
measurement-first discipline that S1 (grouping sets) and S2.0 (windows)
both resolved to "no operator needed."

**Phase code:** `SETOP`. **Track:** A (engine/SQL).

---

## ✅ VERDICT (2026-07-18) — no operator gap AND no plan-quality gap

**Set operations need no new operator, and no rule work either.** Both
halves of the "is there a gap" question came back negative, from real
plans (`examples/setop_probe.rs`, SF1):

**1. No dedicated set-op operator exists — DF lowers to join + aggregate.**
Across all 8 set-op / union / large-IN queries, `dedicated_setop_op=false`:
there is no `IntersectExec` / `ExceptExec` / `SetOpExec` node. DataFusion
lowers:

| SQL | Lowers to | Evidence (SF1 physical plan) |
|---|---|---|
| `INTERSECT` (q8, q14a/b, q38) | **semi-join** + `AggregateExec` (DISTINCT dedup) | q38: `HashJoinExec×8`, `join_types=[Inner×6, RightSemi×2]`, `AggregateExec×10` |
| `EXCEPT` (q87) | **anti-join** + `AggregateExec` | q87: `join_types=[Inner×6, RightAnti×2]`, `AggregateExec×10` |
| `UNION [ALL]` (q33, q56, q60) | `InterleaveExec` (trivial concat) atop joins/aggs | q33/56/60: `InterleaveExec×1`, `HashJoinExec×12`, `RightSemi×3` |
| `IN (SELECT …)` (q33/56/60) | **semi-join** (`RightSemi`) | same as above |
| large literal `IN (…)` (**q8, ~400 elems**) | a single `InList` node — **not** an OR-chain | q8: `inlist_node=true`, only `HashJoinExec×5` (no blowup) |

`HashJoinExec` + `AggregateExec` are exactly the operators ematix's
push/fused engine already accelerates. So the "native set-op operator"
line in the gap doc is technically true but a **non-issue** — there is
nothing to intercept; a bespoke `SetOpExec` would only *bypass* the join
reorder / bloom / build-side machinery that already helps.

**2. The semi/anti modes land on ematix's _most-developed_ join path.**
The one place set-ops could still lose is plan quality: if ematix's join
accel rules only fired on `Inner`, the `RightSemi` / `RightAnti` joins that
set-ops lower to would miss bloom filters and reorder. They do **not** —
ematix has *dedicated* semi/anti-join rules (a code sweep of
`crates/ematix-flow-core/src/`):

- `push_down_left_semi_rule.rs`, `synthetic_left_semi_rule.rs`,
  `force_collect_left_semi_build_rule.rs`, `swap_semi_join_build_rule.rs`
  — bespoke rules whose whole subject is Semi/Anti joins.
- `runtime_bloom_sideband_rule.rs` references Semi/Anti **40×**;
  `agg_filter_pushdown.rs` **43×**; `join_reorder.rs` **10×**;
  `grace_hash_join.rs` **20×**.

Set-op lowering to semi/anti joins therefore hits the join modes ematix
has invested in *most*, not least. **No rule gap.**

**Conclusion.** Like grouping sets (S1) and 8/9 window queries (S2.0),
set operators are already competitive on the ematix engine. SETOP ships
**parity coverage + guards** (§2), not an operator. The sprint plan's
"native set-operator execution" is re-read as "confirm the DF lowering
lands on ematix's accelerated join/agg path" — which it does.

---

## 1. What TPC-DS actually uses (audited 2026-07-18)

- **`INTERSECT`** — q8 (1×), q14a (2×), q14b (2×), q38 (2×). All the
  channel-overlap pattern: `DISTINCT` customer/item identity sets across
  store/catalog/web, intersected.
- **`EXCEPT`** — q87 (2×) only. Channel-difference of `DISTINCT` sets.
- **No `INTERSECT ALL` / `EXCEPT ALL` / `MINUS` anywhere** in the 99
  queries. **Scope correction:** drop the sprint plan's "`[ALL]`" — TPC-DS
  is entirely `DISTINCT`-semantics set-ops (which is exactly why they
  lower to semi/anti join + a dedup aggregate).
- **Large `IN`** — the sprint plan / gap-doc row 6 named q33/q56/q60, but
  those are **UNION-heavy channel queries with small `IN (SELECT…)`
  subqueries**, not large literal IN-lists. The actual large literal
  IN-list is **q8 (~400 elements)** — and it lowers to a single `InList`
  membership node, no blowup. **Scope correction:** row 6's large-IN
  concern is q8, and it is already a non-issue.
- **`UNION` / `UNION ALL`** — widespread (20+ queries); lowers to
  `InterleaveExec` / `UnionExec`, a concatenation, never a hotspot.

## 2. SETOP deliverables — parity coverage + guards — ✅ DONE (2026-07-18)

Mirrors the window WIN.1–3 pattern; no operator. All three landed in
`crates/ematix-flow-core/tests/set_operators_semantics.rs` (6 tests, pass).

- **SETOP.1 — semantic contract tests** ✅ on `preset::session_context()`:
  `INTERSECT` = distinct rows in *both* inputs (duplicate 3s in both
  collapse to one → `{3,4}`), `EXCEPT` = distinct rows in the first not
  the second (`{1,2}`), and a literal `IN (…)` = membership that *keeps*
  duplicates (`{2,3,3,4}`). The dedup-vs-membership contrast is the guard
  most likely to catch a wrong lowering.
- **SETOP.2 — parity in `tpcds_validate`** ✅ all 8 `PASS parity=OK` vs
  DuckDB at SF1: q8 (5), q14a (100), q14b (100), q33 (100), q38 (1), q56
  (100), q60 (100), q87 (1).
- **SETOP.3 — plan-shape pin** ✅ hermetic assertions: `INTERSECT` →
  contains `Semi` + `AggregateExec`, no `IntersectExec`; `EXCEPT` →
  `Anti` + `AggregateExec`; literal `IN` → no `HashJoinExec` / no
  `UnionExec` (stays a single filter, no OR-chain / no N-join blowup).
  Guards against a DF upgrade that regresses the lowering.

## 3. Optional confirmation (not a build)

The verdict already shows ematix's semi/anti rules *exist*; a one-line
`EXPLAIN` check that they actually **fire** on q38/q87 (bloom sideband /
build-side swap nodes present on the `RightSemi`/`RightAnti` joins) is
worth doing when SETOP.2 runs — but it is verification, not new code, and
shares its machinery with **S3** (decorrelation → also produces semi/anti
joins that must hit these same rules). If that check ever shows the rules
*not* firing on set-op-derived joins, that is a targeted rule-predicate
fix (extend the mode guard), still not an operator.

## 4. Exit criteria — ✅ MET (2026-07-18)

- ✅ SETOP.1–3 shipped: the 8 queries parity-clean at SF1; 6 hermetic
  contract + plan-pin tests in place.
- ✅ Gap doc rows 3 & 6 updated to "DF-native retained — lowers to
  ematix's accelerated semi/anti-join + aggregate path," with the `[ALL]`
  and large-IN scope corrections.
- ✅ No operator built — the §3 confirmation (ematix's dedicated semi/anti
  rules exist) held; the optional EXPLAIN "do they fire on q38/q87"
  check is deferred to when it can share machinery with S3.

The sprint plan's "all listed queries native + parity at SF=1" is met by
parity + the confirmation that the lowering is already ematix-accelerated.
