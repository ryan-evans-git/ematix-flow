# v2 — TPC-DS SQL-surface gap checklist (S0.3)

Audit of the SQL shapes TPC-DS needs vs what ematix executes **natively
on the push/fused engine** today. Source: [`../V2_TARGET.md`](../V2_TARGET.md)
§2.1; grounded by the `tpcds_dialect_audit` example and a code sweep for
native operators (2026-07-18).

## The key distinction

Two different bars, and they are **not** the same:

- **Plans today (DataFusion)?** — does the query parse, plan, and
  execute at all? The `tpcds_dialect_audit` example
  (`crates/ematix-flow-core/examples/tpcds_dialect_audit.rs`) already
  establishes: **all 99 TPC-DS queries plan + execute through DataFusion
  53** (PASS = "translator succeeded + DataFusion planned"). So
  correctness/coverage is largely **already there** via stock DataFusion.
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
| 2 | Advanced window frames (`RANGE`, named windows, `RANK`/`DENSE_RANK`/`NTILE`/`LEAD`/`LAG`) | Q44, Q47, Q49, Q51, Q57, Q67 | ✅ | ❌ **no ematix window operator exists** (`windowed.rs` is *streaming* tumbling/hopping windows, not SQL window functions) → DataFusion `BoundedWindowAggExec` | **S2** |
| 3 | `INTERSECT` / `EXCEPT` (+ `ALL`) | Q8, Q14, Q38, Q87 | ✅ | ❌ **no native set-op operator** → DataFusion built-in (semi/anti-join lowering) | **S2** |
| 4 | Correlated + `EXISTS`/`NOT EXISTS` subqueries at depth | Q10, Q35, Q41 | ✅ (DF decorrelates) | ⚠ **DataFusion's built-in decorrelation** — no ematix pass; works, but the resulting joins may not hit ematix's reorder/bloom rules well | **S3** |
| 5 | Recursive CTEs | (out-of-suite; common in analytics) | ✅ | ⚠ DataFusion `RecursiveQueryExec` — generic | **S3** |
| 6 | Large `IN`-lists / `UNION`-heavy set pipelines | Q33, Q56, Q60 | ✅ | ⚠ generic; verify no planning blowup at scale | **S2** |

Legend: ✅ yes · ❌ no (gap) · ⚠ works but not ematix-native / needs verification.

## What each sprint must deliver (the "native" column → ✅)

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
4. **Decorrelation (S3)** — mostly a *plan-quality* win (feed ematix's
   join rules) rather than a missing-operator problem; DataFusion
   already produces correct results.

## Verification note (do not assume, measure)

"Plans today ✅" comes from the dialect audit (translate + plan). It does
**not** prove row-correctness at scale or that the plan is efficient —
that is what **S0.2 `tpcds_validate`** (row-parity vs DuckDB) will
establish per query, and what the S6 benchmark will time. Treat the
"native? ❌/⚠" column as the design signal; confirm the exact operator a
query lands on with an `EXPLAIN` per query as each sprint opens (per the
S1 doc's "dump a real plan first" discipline).

## Assets already in the repo (reused by S1–S3 + S0.2)

- `examples/tpcds/schema.sql` — the 24-table TPC-DS schema.
- `examples/tpcds/queries/spark/` — 103 Spark-canonical query files.
- `crates/ematix-flow-core/examples/tpcds_dialect_audit.rs` — the
  translate-and-plan audit these gaps were read from.
