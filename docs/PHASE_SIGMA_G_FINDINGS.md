# Σ.G — broader-query shape inventory: findings

**Run:** 2026-05-20, against `feat/sigma-f-shape-catalog` (Σ.F.1 +
Σ.F.2 + Σ.F.3 stacked on v0.3.0). Tool: `crates/ematix-flow-core/
examples/sigma_g_shape_inventory.rs`. Reproduce with `cargo run
--release -p ematix-flow-core --example sigma_g_shape_inventory >
/tmp/sigma_g_inventory.md`.

## Headline

**168 queries** audited across three workloads:

- **TPC-H 22** (real parquet via EmatixFastParquetTableProvider)
- **TPC-DS 103** (Spark dialect, schema-only empty MemTable)
- **ClickBench 43** (single-table OLAP, schema-only empty MemTable)

All 168 plan successfully — DataFusion's SQL parser handles every
query, including TPC-DS Spark dialect and ClickBench's
ClickHouse-flavoured SQL. **16/168 (10%)** hit at least one current
catalog rule (12 from TPC-H, 0 from TPC-DS, 4 from ClickBench).

**The two big patterns** that emerge are *orthogonal* — different
workload classes need different shapes:

- **Multi-table OLAP (TPC-H / TPC-DS):** 121/125 queries contain a
  `HashJoinExec`. The join is the gating shape — our aggregate
  rules can't reach the scan through it.
- **Single-table OLAP (ClickBench):** **0 queries have a join.**
  Instead, 31/43 (72%) use `SortExec(TopK)` for ORDER BY + LIMIT.
  The dominant pattern is `Filter → Aggregate → Sort → Limit`.

So "any user's SQL" actually maps to two distinct executor gaps,
not one. The right next-phase ordering reflects this.

## Operator-class footprint (queries that mention each)

| Operator | TPC-H (22) | TPC-DS (103) | ClickBench (43) | **Total / 168** |
|---|---:|---:|---:|---:|
| `AggregateExec` | 22 | 102 | 36 | **160 (95%)** |
| `FilterExec` | 22 | 103 | 26 | **151 (90%)** |
| `RepartitionExec` | 22 | 103 | 21 | **146 (87%)** |
| `ProjectionExec` | 21 | 97 | 39 | **157 (93%)** |
| `HashJoinExec` | 20 | 101 | **0** | **121 (72%)** |
| `SortPreservingMergeExec` | 18 | 84 | 19 | **121 (72%)** |
| `SortExec(TopK)` | 0 | 75 | 31 | **106 (63%)** |
| `SortExec` | 18 | 24 | 1 | **43 (26%)** |
| `CoalescePartitionsExec` | 14 | 25 | 2 | **41 (24%)** |
| `GlobalLimitExec` | 0 | 10 | 6 | **16 (10%)** |
| `UnionExec` | 0 | 12 | 0 | **12 (7%)** |
| `NestedLoopJoinExec` | 2 | 9 | 0 | **11 (7%)** |
| `BoundedWindowAggExec` | 0 | 9 | 0 | **9 (5%)** |
| `WindowAggExec` | 0 | 8 | 0 | **8 (5%)** |
| `CrossJoinExec` | 0 | 5 | 0 | **5 (3%)** |
| `InterleaveExec` | 0 | 5 | 0 | **5 (3%)** |

Read the table by *column* to see workload character:
- TPC-H/TPC-DS: heavy join (97% of those 125 queries).
- ClickBench: heavy TopN (72% — vs 0% join). Different world.

## What the data says clearly

**1. Hash join is the dominant unaddressed shape.** 97% of all queries
have a HashJoinExec somewhere; 0% of our catalog entries touch them.
Every executor optimization we have today (FilterMultiAggSpec,
FilterSumSpec, DictFilterExec, DictGroupCountExec) sits *upstream* of
the join — but in 97% of real queries the join is the bottleneck,
not the aggregate.

**2. TopN (Sort + Limit) is the next-biggest gap.** 60% of TPC-DS
queries use `SortExec(TopK)` — DataFusion's specialized "small-N
sort with limit" exec. There's nothing to *route* here unless we
build something faster than DataFusion's TopK (which is decent), but
it's worth measuring whether a custom heap-based TopK over
predicate+aggregate pipeline beats it.

**3. Window functions matter for TPC-DS.** 17/103 queries (16%) use
either `BoundedWindowAggExec` or `WindowAggExec`. Zero TPC-H queries
do, so our current bench harness can't measure window perf at all.

**4. Aggregate + Filter shapes are already nearly-covered for
single-table workloads.** Where TPC-H rules fire: `dict_filter` (9),
`dict_group_count` (2), `filter_sum` (1, Q06), `filter_multi_agg`
(1, Q01). The multi-agg/filter-sum rules only fire on Q01 and Q06
because every *other* TPC-H aggregate query has a JOIN between the
aggregate and the scan.

**ClickBench fires `filter_multi_agg` on Q13/Q37/Q38/Q39 even with
empty data**, because those queries have a `Filter > Aggregate >
Scan` shape with no join. The rule already works; the queries it
doesn't fire on (the other 39 ClickBench queries) mostly have a
trailing `SortExec(TopK) > GlobalLimitExec` wrapper that the current
shape doesn't accept. **Extending the existing aggregate rules to
match the TopN-wrapped variant is much cheaper than the
join-aware extension.**

**5. DataFusion's parser handles real TPC-DS Spark queries.** This
was the previous risk — Spark dialect specifics (LATERAL VIEW, etc.).
None of the 103 queries failed to plan. So we don't need a dialect
translator for the inventory itself, only for execution.

## Catalog rule activation, per-query

**TPC-H 22 (real parquet — accurate rule firing):**

| Q | Rules fired |
|---|---|
| Q01 | `filter_multi_agg` |
| Q02, Q03, Q05, Q07, Q08, Q10, Q11, Q20, Q21 | `dict_filter` |
| Q04 | `dict_group_count` |
| Q06 | `filter_sum` |
| Q21 | also `dict_group_count` |
| Q09, Q12-Q19, Q22 | — |

**ClickBench 43 (empty MemTable — fires only when shape matches
syntactically, without dict-decoded data):**

| Q | Rules fired |
|---|---|
| Q13, Q37, Q38, Q39 | `filter_multi_agg` |
| Q01-Q12, Q14-Q36, Q40-Q43 | — |

**TPC-DS 103:** zero rules fire (mostly because every query has a
join blocking the catalog's aggregate-side shapes).

Note: on real-parquet TPC-H, **10 queries fire `dict_filter`** (up
from 0 with empty MemTables) — the dict-aware string filter is
*broadly applicable* even with its narrow shape, because the
predicate only needs a dict column. **This is the model new shapes
should follow:** narrow precise pattern, applies across many
queries via the runtime data shape, not by widening the matcher.

## Recommended next phase ordering

Read these as "catalog gap + executor it implies". The first two
phases below are *orthogonal* — they unlock different workload
classes — so they can ship in parallel or in either order. The
join phase has higher per-query leverage on the benches we already
track; the TopN phase touches a workload class we currently can't
optimise at all.

### Σ.H — `Filter + HashJoin + Aggregate` shape (multi-table OLAP)

**Coverage:** 121/125 TPC-H + TPC-DS queries have a HashJoinExec.
Specifically the `filter on dim table → hash-join into fact →
aggregate` pattern: TPC-H Q3 / Q5 / Q7 / Q8 / Q10 / Q11 / Q19 / Q21;
dozens of TPC-DS queries. ClickBench: 0 (no joins).

**Catalog entry:** new shape `Aggregate(Final*) > ... > HashJoinExec >
Filter > Scan` (with the various wrappers we already handle on the
agg side). ~50 LOC catalog code, leveraging the Σ.F substrate.

**Executor work (the substance):** Σ.E6 Appendix B4 covers two
subpaths:
- *Cheap:* pre-filter the build side, then DataFusion's standard
  HashJoinExec, then our existing FilterMultiAggSpec. ~1 wk with
  bench gate.
- *Aggressive:* dict-aware probe-side join (compose with dict-arrival
  decode → dict-coded probe key → translate-once cache). 2-3 wk.

Start with the cheap variant. Bench-gate. Decide on aggressive based
on results.

### Σ.I — `Filter + Aggregate + TopN` shape (single-table OLAP)

**Coverage:** 106/168 total queries contain `SortExec(TopK)`. The
specific high-leverage sub-pattern is `Aggregate > Sort(TopK) > ...`
or `GlobalLimit > SortPreservingMerge > Aggregate > ...` — ClickBench
has ~30 of these; TPC-DS another 75-ish.

**Two distinct sub-pieces:**

1. **Widen the existing `filter_multi_agg` / `filter_sum` shapes**
   to accept a trailing `SortExec(TopK)` or `GlobalLimitExec` above
   the top Projection. The aggregate exec doesn't change — only the
   catalog matcher does. ~30 LOC per shape variant. This alone
   *probably* makes 20-30 ClickBench queries hit `filter_multi_agg`
   that currently miss only because of the wrapper.
2. **Custom heap-based TopK inside the aggregate.** DataFusion's
   `SortExec(TopK)` materialises the full sorted prefix then takes
   N; a TopK that maintains an N-element heap during aggregation
   avoids the materialisation. ~1 wk; worth a 1-day spike first
   to measure whether the win is meaningful vs DF's TopK.

Recommend doing (1) first — it's nearly free and validates that the
trailing-wrapper extension is the actual blocker.

### Σ.J — `WindowAgg` shape (mid-priority)

**Coverage:** 17/103 TPC-DS queries. Common in real analytics
(running totals, ranks, lag/lead). Zero TPC-H, zero ClickBench.

**Executor work:** specialized window kernels for the common cases
(ROW_NUMBER, RANK, SUM OVER, LAG/LEAD with default offsets).
Per-shape templates, similar to FilterMultiAggSpec's family. 2-3 wk.

### Lower priority — `UnionAll`, `NestedLoopJoin`, `CrossJoin`

UnionAll is mostly about avoiding the materialisation copy (small
win). NestedLoopJoin / CrossJoin only show up on small inputs in
both TPC suites; if a query lands on these it's already small enough
that perf doesn't matter much.

## Quality-of-life signal

The inventory itself is cheap to re-run any time the catalog grows.
The "0% of TPC-DS hits any rule" number is the right yardstick to
move: each new phase should move it up by ≥10pp. Σ.H alone (join +
filter + agg) should bring it to >60% on real-parquet TPC-DS once
data + dialect translation are available.

## What the inventory does *not* yet tell us

- **Per-query execution time** — this is plan-only. We don't know
  which of the 75 SortExec(TopK) queries are actually slow.
- **TPC-DS performance vs DataFusion default** — needs data
  generation. dsdgen at SF=1 is ~1GB; a follow-up phase should run
  this.
- **Dialect compatibility for execution** — DataFusion parsed all
  103 Spark TPC-DS queries, but some may fail at execution (
  e.g. ROLLUP, GROUPING SETS, some date arithmetic). Need to run
  to find out.

These are follow-ups for the join phase (Σ.H), once we have
something to bench against.
