# v2 — TPC-DS SQL-surface gap checklist (S0.3)

Audit of the SQL shapes TPC-DS needs vs what ematix executes **natively
on the push/fused engine** today. Source: [`../V2_TARGET.md`](../V2_TARGET.md)
§2.1; grounded by the `tpcds_dialect_audit` example and a code sweep for
native operators (2026-07-18).

## ✅ STATUS (2026-07-18) — S1–S3 resolved; TPC-DS 103/103 execute, 0 parity mismatches

The S1→S3 SQL-surface investigation is **complete**. The headline finding
across all three sprints: DataFusion's SQL-surface operators are **already
competitive on the ematix engine** — three assumed "build a native
operator" items were each measured to be non-gaps, which freed the effort
to find and fix the *real* defects.

- **S1 grouping sets** — native operator built, measured **1.8× slower**,
  reverted.
- **S2 windows** — 8/9 DF-native; q51 a deferred, bounded candidate.
- **S2 set-ops** — no operator gap *and* no rule gap (lower to ematix's
  best-developed semi/anti-join path).
- **S3 decorrelation** — **not** a missing pass; a real ematix
  physical-rule regression (dim-join pushdown dropped an outer column) —
  **fixed**, 5 queries recovered.
- **Correctness tail** (found during S3 validation, none ematix-engine
  bugs) — Spark true-division (q34/q73) + a column-name mismatch (q30) —
  **fixed** (see the Correctness fixes section below).

**`tpcds_validate` (SF=1) now: 103/103 execute, 0 parity MISMATCH**, 94
row-parity-OK-vs-DuckDB + 9 oracle-skipped (DuckDB-parser limits, not
ematix). Net: the "make it work at all" bar is fully cleared; remaining
v2 SQL-surface work is *fusion/benchmark* (S6), not correctness.

## The key distinction

Two different bars, and they are **not** the same:

- **Plans today (DataFusion)?** — does the query parse, plan, and
  execute at all? The `tpcds_dialect_audit` example
  (`crates/ematix-flow-core/examples/tpcds_dialect_audit.rs`) established
  that all 99 TPC-DS queries translate + build a **logical** plan. That
  was **not** the same as executing: S3 revealed the audit is
  logical-only — 4 queries died in ematix *physical* planning (the
  dim-push regression), and 3 more had correctness/plan issues. All now
  fixed; `tpcds_validate` (which executes end-to-end + row-parity vs
  DuckDB) confirms **103/103 execute, 0 parity MISMATCH** (2026-07-18).
- **Native / fused on the ematix push engine?** — does it run on
  ematix's vectorized/fused operators (the ones that win the benchmark),
  or does it fall back to DataFusion's generic operators? **This is the
  real gap** — and it is what makes or breaks the S6 TPC-DS benchmark
  story (`V2_TARGET.md` §2.2). A query that only DataFusion's generic
  ops handle is correct but not fast.

So v2's SQL-surface work is mostly **"pull onto the push engine," not
"make it work at all."**

## Gap table

| # | Feature | TPC-DS queries (examples) | Plans today (DF)? | Native/fused on push engine? | Sprint |
|---|---------|---------------------------|:---:|---|:---:|
| 1 | `GROUPING SETS` / `ROLLUP` / `CUBE` + `GROUPING()` / `GROUPING_ID` | Q18, Q22, Q27, Q36, Q67, Q77, Q80 | ✅ | ✅ **DF-native retained** — a native operator was built + measured **1.8× slower** and reverted (2026-07-18); DF's grouping-set exec is single-scan + vectorized. | ~~S1~~ done (no-op) |
| 2 | Window functions — TPC-DS uses only `RANK` + `SUM`/`AVG` windows (audit 2026-07-18); **not** `RANGE`/named/`DENSE_RANK`/`NTILE`/`LEAD`/`LAG` | Q36, Q44, Q49, Q51, Q53, Q63, Q67, Q70, Q86 | ✅ | ✅ **DF-native for 8/9 + a NATIVE operator for q51** — gate: window compute <9% for 8 queries (DF retained). **q51 shipped a fused cumulative-window operator** (`FusedCumulativeWindowExec`, default-on): SF10 A/B **1.65–1.73× faster**, row-identical, fires on q51 only, 0 regressions. The only S1–S3 operator to beat DF. Scope: [`../PHASE_V2_S2_WINDOW_FUNCTIONS.md`](../PHASE_V2_S2_WINDOW_FUNCTIONS.md). | ~~S2~~ done |
| 3 | `INTERSECT` / `EXCEPT` (no `ALL` in TPC-DS) | Q8, Q14a/b, Q38, Q87 | ✅ | ✅ **DF-native retained** — probe (`setop_probe`, 2026-07-18) shows no dedicated set-op operator: `INTERSECT`→semi-join+agg, `EXCEPT`→anti-join+agg, which land on ematix's **most-developed** join path (dedicated semi/anti rules + bloom on Semi/Anti). No operator/rule gap. Scope: [`../PHASE_V2_S2_SET_OPERATORS.md`](../PHASE_V2_S2_SET_OPERATORS.md). | **S2** |
| 4 | Correlated + `EXISTS`/`NOT EXISTS` + correlated scalar subqueries | Q10, Q16, Q35, Q41, Q69, Q94 | ✅ | ✅ **FIXED (2026-07-18)** — was an ematix physical-rule regression (dim-join pushdown dropped an outer column on the correlated shape); schema-preservation guard added. 5 queries recovered (q10/16/69/94 + q95), 0 regressions. Guard: `tests/decorrelation_semantics.rs`. [`../PHASE_V2_S3_DECORRELATION.md`](../PHASE_V2_S3_DECORRELATION.md). | ~~S3~~ done |
| 5 | Recursive CTEs | **0 of 99 TPC-DS queries** (out-of-suite) | ✅ | ⚠ DataFusion `RecursiveQueryExec` — generic; **cut from S3** (no TPC-DS driver), defer until a real workload needs it | ~~S3~~ deferred |
| 6 | Large literal `IN`-list (**q8, ~400 elems**; q33/56/60 are UNION+`IN(SELECT)`, not large literal) | Q8 (+ Q33/56/60 unions) | ✅ | ✅ **DF-native retained** — q8's ~400-elem `IN` lowers to a single `InList` membership node (no OR-chain blowup, verified `setop_probe` 2026-07-18); UNIONs → `InterleaveExec`. Scope: [`../PHASE_V2_S2_SET_OPERATORS.md`](../PHASE_V2_S2_SET_OPERATORS.md). | **S2** |

Legend: ✅ yes · ❌ no (gap) · ⚠ works but not ematix-native / needs verification.

## Correctness fixes (2026-07-18) — surfaced by S3 `tpcds_validate`, not in the gap table

These are execution/parity bugs (not native-operator gaps). Each was
classified with a preset-vs-vanilla-DataFusion triage; **none was an
ematix-engine bug**:

- **q10 / q16 / q69 / q94 (+ q95)** — ematix physical-rule regression
  (dim-join pushdown, S3). Fixed by a schema-preservation guard. Gap row 4.
- **q34 / q73** — **Spark true-division not preserved.** Spark `/` is
  float division (`int / int` → double); DataFusion does integer division
  on integer operands, silently truncating. `hd_dep_count /
  hd_vehicle_count > 1.2` dropped matching rows (176 vs 223; 0 vs 1). Fix:
  the Spark dialect translator (`dialect/spark.rs`) casts the left operand
  of every `/` to DOUBLE. Parity-safe (the DuckDB oracle runs the same
  translated SQL). Tests in `tests/dialect_spark.rs`.
- **q30** — **column-name mismatch.** The query named
  `c_last_review_date`, but the DuckDB-generated data (and every
  registered parquet) has `c_last_review_date_sk`; q30 was the only query
  using the bare name. Corrected `q30.sql` to the actual column. (Note:
  `examples/tpcds/schema.sql` still carries the bare `c_last_review_date`
  — a documentation-only inconsistency vs the generator's `_sk` output,
  left as-is to avoid touching the generation path.)

## What each sprint must deliver (the "native" column → ✅)

> **Superseded by measurement (2026-07-18).** The three bullets below were
> the *original* S1–S3 plans (build native operators + a decorrelation
> pass). Every one was refuted or redirected by the actual investigation —
> see the STATUS banner and the gap table for what really happened. Kept
> here as the pre-measurement hypothesis.

- **S1 — grouping sets.** `FusedGroupingSetAggregateExec`, single-pass
  multi-table, reusing the existing fused accumulators; `GROUPING()`
  from a real grouping-id. Full design:
  [`../PHASE_V2_S1_GROUPING_SETS.md`](../PHASE_V2_S1_GROUPING_SETS.md).
- **S2 — window + set-ops.** A vectorized window-frame operator (net-new
  — ematix has none today) and native `INTERSECT`/`EXCEPT` execution +
  large-`IN` handling.
- **S3 — decorrelation + tail.** A subquery-decorrelation pass that
  produces joins the ematix reorder/bloom rules can exploit, plus
  recursive-CTE coverage; drive `tpcds_validate` to 99/99 native at
  SF=1/10.

## Ranking (by benchmark leverage)

Ordered by how much the *benchmark* (S6) depends on it being fused, not
just correct:

1. ~~**Grouping sets (S1)** — highest leverage.~~ **REFUTED by measurement
   (2026-07-18).** A native operator was built and benched against
   DataFusion's grouping-set exec — it was **1.8× slower** (q22 SF10),
   never faster. DF already single-scans the child and updates all sets in
   one pass, so there is no scan-saving to capture; grouping sets are
   **not** a benchmark liability. See the Measurement Verdict in
   [`../PHASE_V2_S1_GROUPING_SETS.md`](../PHASE_V2_S1_GROUPING_SETS.md).
   The premise below ("a scalar fallback loses the benchmark") was wrong:
   DF's path is vectorized, not scalar.
2. **Window functions (S2)** — net-new operator, several queries, and
   window aggregation is exactly where analytical engines differentiate.
3. **Set-ops + large IN (S2)** — moderate; correctness is there, fusion
   upside is smaller.
4. **Decorrelation (S3)** — ~~mostly a plan-quality win~~ **REDIRECTED
   (2026-07-18).** Not a plan-quality problem: DataFusion decorrelates
   TPC-DS's correlated subqueries cleanly to hash Semi/Anti/Mark joins
   (no quadratic), and those land on ematix's best-developed join path.
   The real issue was a **correctness regression** — an ematix physical
   rule broke 4 queries in planning. Fixed; not a missing operator.

## Verification note (do not assume, measure)

"Plans today ✅" comes from the dialect audit (translate + *logical* plan)
— it did **not** prove end-to-end execution or row-correctness, and in
fact hid 4 physical-planning failures + 3 correctness bugs (all now
fixed). **S0.2 `tpcds_validate`** (execute + row-parity vs DuckDB) is the
real bar and now reads **103/103 execute, 0 parity MISMATCH at SF=1**;
what remains for S6 is *timing/fusion*, not correctness. This whole arc
validated the "dump a real plan first / measure, don't assume" discipline
— every S1–S3 assumption that skipped measurement was wrong. The reusable
probes that enforce it: `win_gate_probe`, `setop_probe`, `decorr_probe`,
`decorr_bisect`.

## Assets already in the repo (reused by S1–S3 + S0.2)

- `examples/tpcds/schema.sql` — the 24-table TPC-DS schema.
- `examples/tpcds/queries/spark/` — 103 Spark-canonical query files.
- `crates/ematix-flow-core/examples/tpcds_dialect_audit.rs` — the
  translate-and-plan audit these gaps were read from.
- `examples/tpcds_validate.rs` — execute + row-parity-vs-DuckDB harness
  (the real bar; 103/103, 0 MISMATCH).

### Measurement probes + regression guards built during S1–S3 (2026-07-18)

- `examples/gs_plan_probe.rs` + `tests/grouping_sets_semantics.rs` — S1
  grouping-set shape pin + contract.
- `examples/win_gate_probe.rs` + `tests/window_functions_semantics.rs` —
  S2 window-cost gate + contract/plan-pin.
- `examples/setop_probe.rs` + `tests/set_operators_semantics.rs` — S2
  set-op lowering probe + contract/plan-pin.
- `examples/decorr_probe.rs`, `examples/decorr_bisect.rs` +
  `tests/decorrelation_semantics.rs` — S3 decorrelation probe, rule-chain
  bisector, and the dim-push regression guard.
- `tests/dialect_spark.rs` — Spark true-division translation tests.
