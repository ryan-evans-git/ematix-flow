# Phase WIN — Window functions on the push engine

*(v2.0.0 Sprint S2 — see [`V2_SPRINT_PLAN.md`](V2_SPRINT_PLAN.md) and
[`V2_TARGET.md`](V2_TARGET.md) §2.1. Companion:
[`plans/V2_SQL_SURFACE_GAPS.md`](plans/V2_SQL_SURFACE_GAPS.md) row 2.)*

**Goal (as written in the sprint plan):** vectorized SQL window-frame
execution on the ematix push engine instead of DataFusion's generic
`WindowAggExec` / `BoundedWindowAggExec`.

**Goal (as this scope revises it — see §1):** *establish whether there
is a window-execution benchmark gap at all* before building an operator,
then deliver whatever the measurement warrants — which may be
correctness/parity coverage only, not a new operator.

**Phase code:** `WIN`. **Track:** A (engine/SQL). **Est:** the
measurement gate (§3) is ~1 day; the build (§5), only if the gate
opens, is the bulk of a sprint.

> **Note on `windowed.rs`.** `crates/ematix-flow-core/src/windowed.rs`
> is the *streaming* tumbling/hopping window aggregator (Phase 39.4,
> watermark-driven). It is **unrelated** to SQL `OVER(...)` window
> functions and is not a starting point for this phase.

---

## ⚠ SCOPE DISCIPLINE — apply the S1 lesson before writing an operator

S1 (grouping sets) built a correct native operator that measured **1.8×
slower** than DataFusion's own path, because DF's path was already
single-scan and vectorized — a fact we only learned by measuring after
building. See the Measurement Verdict in
[`PHASE_V2_S1_GROUPING_SETS.md`](PHASE_V2_S1_GROUPING_SETS.md).

Window functions carry **two independent reasons** to suspect the same
outcome, so this phase is **measurement-gated**: the operator in §5 is
not authorized until the gate in §3 shows a material window-stage cost.

---

## ✅ GATE VERDICT (2026-07-18) — SHUT for 8/9, OPEN for q51 only

Measured with `examples/win_gate_probe.rs` over SF=1 on
`preset::session_context()` — per query, window-operator
`elapsed_compute` as a share of total operator compute (see §3 for the
method; `elapsed_compute` sums across partitions, so it is a CPU-share
ratio, not wall-time):

| query | wall (ms) | window share | gate | note |
|---|---:|---:|:---:|---|
| q36 | 19.7 | 1.6% | shut | rank over grouping-set (133 rows) |
| q44 | 16.8 | 1.3% | shut | rank, 7.4k rows |
| q49 | 20.5 | 0.1% | shut | 6× rank, ≤30 rows each |
| **q51** | **50.9** | **77.8%** | **OPEN** | **cumulative `ROWS` over ~500k rows** |
| q53 | 8.1 | 8.9% | shut | whole-partition avg, 1.1k rows |
| q63 | 8.3 | 1.2% | shut | whole-partition avg, 1.0k rows |
| q67 | 94.5 | 3.2% | shut | slowest query, but **not** window-bound (joins/sort) |
| q70 | 13.6 | 0.3% | shut | rank over grouping-set, ≤3 rows |
| q86 | 3.2 | 2.2% | shut | rank over grouping-set, 133 rows |

**8 of 9 shut, exactly as the post-aggregation structure predicted (§1b).**
Those queries window tiny streams; DF's vectorized window path is
retained and only owes parity coverage (§4).

**q51 opens the gate — and, unlike S1, the measurement _confirms_ an
opportunity rather than refuting one.** q51's per-operator breakdown:

| operator | elapsed | share | out rows |
|---|---:|---:|---:|
| `BoundedWindowAggExec` (outer `max` cumulatives) | 173.1 ms | 44.3% | 499,205 |
| `BoundedWindowAggExec` (web_v1 `sum` cumulative) | 84.1 ms | 21.5% | 138,310 |
| `BoundedWindowAggExec` (store_v1 `sum` cumulative) | 47.2 ms | 12.1% | 114,688 |
| `SortExec` (feeds the window) | 19.1 ms | 4.9% | 499,205 |
| `AggregateExec` | 16.1 ms | 4.1% | 499,205 |

The cost is the **window kernel itself (77.8%), not the sort feeding it
(4.9%)** — so a faster window operator would actually move q51. At
~347 ns/row for a running `max`, the likely slack is DF's
`BoundedWindowAggExec` being a **generic bounded-frame evaluator**
(re-derives the frame per row) operating on **`Decimal128(27,2)`**; the
pure-cumulative special case (`ROWS UNBOUNDED PRECEDING AND CURRENT ROW`,
`mode=[Sorted]`) can be a tight single-pass running accumulator with no
per-row frame derivation.

**This differs from S1:** S1's measurement found *no* algorithmic slack
(DF already single-scan), so the operator lost. Here there is a concrete
mechanism for a win. But the upside is **one query, ~50 ms wall, bounded**
— q51 is not a headline benchmark query. So §5/S2.5 is *authorized but
not obligatory*; it needs an explicit decision that q51 is worth a
bespoke operator, and it ships only under the A/B-or-revert bar (§5).

**Net:** WIN.1–WIN.3 (parity coverage, §4) proceed unconditionally; the
gap doc row 2 → "DF-native retained for 8/9; q51 a scoped candidate."

---

## 1. What TPC-DS actually exercises (audited 2026-07-18)

Nine queries use `OVER(...)`. Flattening and extracting every window
clause from `examples/tpcds/queries/spark/`:

| Query | Window expr | Partition | Order | Frame | Window input |
|---|---|---|---|---|---|
| q44 | `rank()` | — | `rank_col` ASC/DESC | rank-peer | per-item avg sales (small) |
| q49 | `rank()` ×6 | — | return/currency ratio | rank-peer | grouped ratios (small) |
| q67 | `rank()` | `i_category` | `sumsales DESC` | rank-peer | rollup sums (small) |
| q70 | `rank()` | `grouping(...)`, `s_state` | `sum(...) DESC` | rank-peer | **grouping-set** sums |
| q36 | `rank()` | `grouping(i_category)…` | ratio | rank-peer | **grouping-set** sums |
| q86 | `rank()` | `grouping(i_category)…` | ratio | rank-peer | **grouping-set** sums |
| q53 | `avg(sum(ss_sales_price))` | `i_manufact_id` | — | whole partition | grouped sums (small) |
| q63 | `avg(sum(ss_sales_price))` | `i_manager_id` | — | whole partition | grouped sums (small) |
| q51 | `sum(...)` | `ws_item_sk`/`item_sk` | `d_date` | `ROWS UNBOUNDED PRECEDING → CURRENT ROW` | per-item date series |

### 1a. The function surface is far narrower than the sprint plan assumed

`V2_SPRINT_PLAN.md` §S2.1 lists `RANGE`, named windows, `DENSE_RANK`,
`NTILE`, `LEAD`, `LAG` as RED-test targets. **TPC-DS uses none of them.**
The actual surface is:

- **`RANK()`** — 7 of 9 queries; with and without `PARTITION BY`, always
  with `ORDER BY`.
- **`SUM` / `AVG` window aggregates** — 3 queries.

Only three frame shapes appear: **rank-peer** (implicit
`RANGE … CURRENT ROW` under an ORDER), **whole-partition**
(PARTITION-only, no ORDER → single value broadcast), and **cumulative
rows** (`ROWS UNBOUNDED PRECEDING AND CURRENT ROW`, q51 only).

**Scope correction:** narrow S2.1's *native* RED tests to `RANK` +
`SUM`/`AVG` windows over those three frames. Keep `DENSE_RANK`/`NTILE`/
`LEAD`/`LAG`/`RANGE`/named-windows as **DF-native passthrough with a
correctness guard**, not native-operator targets — nothing in the
benchmark drives them, so they earn parity coverage, not fusion.

### 1b. Every TPC-DS window runs on a post-aggregation stream

This is the load-bearing structural fact. In all 9 queries the window
sits **above** the fact-table scan/join/aggregate — it ranks grouped
sums, averages per-manufacturer sums, or cumulates a per-item date
series. The row count reaching the window operator is
hundreds-to-thousands, not millions. The expensive work
(`store_sales`/`web_sales` scans, joins to `date_dim`/`item`, the
`GROUP BY`) is already done upstream.

**Implication:** a "vectorized fused window kernel" would optimize a
stage that is a small fraction of these queries' runtime. That is
exactly the S1 trap. **q51 is the one partial exception** (it windows a
per-item time series that can be larger), so it is the primary — likely
only — measurement subject where a window operator could conceivably pay
off.

### 1c. q36 / q70 / q86 couple window + grouping sets

These `rank() OVER (PARTITION BY grouping(...) ...)` queries stack a rank
on top of the grouping-set aggregation we confirmed DF-native in S1.
They are the natural **correctness** regression targets (S1's semantic
tests already guard the grouping-set half) and must land in the parity
matrix regardless of the operator decision.

---

## 2. What DataFusion 53 gives us today

- `WindowAggExec` (`datafusion-physical-plan-53.1.0/src/windows/
  window_agg_exec.rs`) — whole-partition / unbounded windows.
- `BoundedWindowAggExec` — bounded frames (streaming, sorted-partition),
  which is where `ROWS UNBOUNDED PRECEDING → CURRENT ROW` (q51) lands.
- `rank`, `sum`/`avg` window UDFs already implemented and vectorized
  over Arrow arrays.

All 9 queries already **plan and execute** through these (the S0.3
dialect audit: 99/99 plan). The open question is *speed at SF10*, not
correctness-of-planning.

---

## 3. THE GATE — measure before building (S2.0) — ✅ DONE, see verdict above

Implemented as `crates/ematix-flow-core/examples/win_gate_probe.rs`: for
each of the 9 window queries it translates Spark→DF, builds the physical
plan (records the window shape), executes it timing the wall clock, then
walks the physical tree summing `elapsed_compute` per operator and
reports the window operators' share (+ a full operator breakdown for any
OPEN query). Re-runnable at any scale via `TPCDS_DATA_DIR`. The method,
for the record:

1. **`EXPLAIN` each of the 9 queries** on `preset::session_context()` at
   SF=1 and confirm which physical operator each window lands on
   (`WindowAggExec` vs `BoundedWindowAggExec`) — per the S1 doc's "dump a
   real plan first" discipline. Record the shapes in this doc's §4.
2. **Stage-time the window operator's share of total runtime at SF10**
   for the 9 queries (reuse the `gs_ab_bench` harness pattern / the
   stage-profiling methodology in
   [`STAGE_PROFILING_METHODOLOGY.md`](STAGE_PROFILING_METHODOLOGY.md)).
   The metric that decides the gate: **window-exec wall-time as a % of
   query wall-time.**

**Gate decision:**

- If no query spends a **material** fraction (proposed threshold: **≥15%
  of query time, and ≥50 ms absolute**) in window execution → **the gate
  stays shut.** S2 delivers §4 (parity coverage) only; record the
  measurement here exactly as S1 recorded its verdict, and update the gap
  doc row 2 to "DF-native retained." Prior (stated honestly, to be
  confirmed or refuted by the number): **the gate stays shut for 8 of 9;
  q51 is the one to watch.**
- If any query (expected candidate: q51) clears the threshold → the gate
  opens **for that frame shape only**, and §5 authorizes an operator
  scoped to it.

---

## 4. S2 deliverables that are unconditional (regardless of the gate)

These ship no matter what the measurement says — they are the honest
floor of "window functions are covered."

- **WIN.1 — semantic contract tests** (mirror S1.1's hermetic style,
  `crates/ematix-flow-core/tests/`): a tiny in-memory table, asserting
  observable results of `RANK` (partitioned + global, ties → equal rank
  with gap), whole-partition `AVG`, and cumulative `SUM` on
  `preset::session_context()`. Hermetic, CI-safe, no TPC-DS data. These
  are the standing regression guard and would equally guard any future
  operator.
- **WIN.2 — parity in `tpcds_validate`**: confirm all 9 window queries
  are `PASS parity=OK` at SF=1 (row-count vs DuckDB), and specifically
  that q36/q70/q86 (window-over-grouping-set) are clean. This is a
  harness/coverage task, not new engine code.
- **WIN.3 — plan-shape pin**: an `EXPLAIN`-dumping example (mirror
  `gs_plan_probe.rs`) recording which DF window operator each shape lands
  on, so a future DF upgrade that changes the shape is caught.

## 5. S2 operator — AUTHORIZED ONLY IF §3 GATE OPENS

If and only if a query clears the §3 threshold, build a vectorized
window operator **scoped to the frame shape that cleared it** (expected:
cumulative `ROWS UNBOUNDED PRECEDING → CURRENT ROW`, q51). Requirements
learned from S1:

- **A/B toggle from day one** (`EMAT_WINDOW_FUSED` tri-state, read fresh
  at plan time) so the operator is benchmarked against DF-native in a
  single process — this is what made S1's measurement decisive.
- **Correctness gate before optimization**: row-parity on the target
  query vs DF-native, proven, *before* touching kernels.
- **Re-measure and be willing to revert.** The operator ships only if it
  beats DF-native on the query that opened the gate. Same bar S1 held.

Do **not** build a general multi-frame window engine. TPC-DS does not
need one, and the whole-partition / rank-peer shapes are exactly where DF
is already vectorized and the input is tiny (§1b).

---

## 6. Set operators + large IN (S2.3) — separate sub-arc

`INTERSECT` / `EXCEPT [ALL]` (Q8/Q14/Q38/Q87) and large-`IN`
(Q33/Q56/Q60) are the other half of the sprint-plan's S2. They are
**independent of the window work** and lower-leverage (the gap doc ranks
them #3, "correctness is there, fusion upside smaller"). Scope them the
same measurement-first way in a companion pass; do not block the window
gate on them. Left as a stub here until the window gate is resolved.

---

## 7. Exit criteria (revised from the sprint plan)

- WIN.1–WIN.3 shipped: all 9 window queries parity-clean at SF=1, contract
  tests + plan pin in place.
- §3 gate **measured and recorded** here (open or shut, with the numbers).
- If shut: gap doc row 2 → "DF-native retained," with the measurement,
  exactly as S1 row 1 now reads.
- If open: the scoped operator beats DF-native on its target query (A/B),
  or is reverted with the verdict recorded.

The sprint plan's "all listed queries native + parity at SF=1" is
**re-read** as: parity at SF=1 for all; *native operator* only where
measurement justifies it. Correctness is the exit bar; fusion is
contingent on the number.
