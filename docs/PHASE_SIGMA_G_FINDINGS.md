# Σ.G — broader-query shape inventory: findings

**Run:** 2026-05-20, against `feat/sigma-f-shape-catalog` (Σ.F.1 +
Σ.F.2 + Σ.F.3 stacked on v0.3.0). Tool: `crates/ematix-flow-core/
examples/sigma_g_shape_inventory.rs`. Reproduce with `cargo run
--release -p ematix-flow-core --example sigma_g_shape_inventory >
/tmp/sigma_g_inventory.md`.

## Headline

**125 queries** audited (22 TPC-H + 103 TPC-DS Spark dialect).

- **100% plan successfully** — DataFusion's SQL parser handles every
  query in both suites, including all TPC-DS Spark dialect.
- **12/125 (10%) hit at least one current catalog rule** — and all
  12 are TPC-H. **Zero TPC-DS queries** hit any rule.
- **121/125 (97%) of queries contain a `HashJoinExec`** somewhere in
  the plan tree. We have zero catalog entries that handle joins.

## Operator-class footprint (queries that mention each)

| Operator | TPC-H (22) | TPC-DS (103) | **Total / 125** |
|---|---:|---:|---:|
| `HashJoinExec` | 20 | 101 | **121 (97%)** |
| `AggregateExec` | 22 | 102 | **124 (99%)** |
| `FilterExec` | 22 | 103 | **125 (100%)** |
| `RepartitionExec` | 22 | 103 | **125 (100%)** |
| `ProjectionExec` | 21 | 97 | **118 (94%)** |
| `SortPreservingMergeExec` | 18 | 84 | **102 (82%)** |
| `SortExec(TopK)` | 0 | 75 | **75 (60%)** |
| `SortExec` | 18 | 24 | **42 (34%)** |
| `CoalescePartitionsExec` | 14 | 25 | **39 (31%)** |
| `UnionExec` | 0 | 12 | **12 (10%)** |
| `NestedLoopJoinExec` | 2 | 9 | **11 (9%)** |
| `GlobalLimitExec` | 0 | 10 | **10 (8%)** |
| `BoundedWindowAggExec` | 0 | 9 | **9 (7%)** |
| `WindowAggExec` | 0 | 8 | **8 (6%)** |
| `CrossJoinExec` | 0 | 5 | **5 (4%)** |
| `InterleaveExec` | 0 | 5 | **5 (4%)** |

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

**4. Aggregate + Filter shapes are already nearly-covered.** Where
TPC-H rules fire: `dict_filter` (9), `dict_group_count` (2),
`filter_sum` (1, Q06), `filter_multi_agg` (1, Q01). The
multi-agg/filter-sum rules only fire on Q01 and Q06 because every
*other* aggregate query has a JOIN between the aggregate and the
scan, which the current shapes reject. **The join is again the
gating shape.**

**5. DataFusion's parser handles real TPC-DS Spark queries.** This
was the previous risk — Spark dialect specifics (LATERAL VIEW, etc.).
None of the 103 queries failed to plan. So we don't need a dialect
translator for the inventory itself, only for execution.

## Catalog rule activation, per-query (TPC-H)

| Q | Rules fired |
|---|---|
| Q01 | `filter_multi_agg` |
| Q02, Q03, Q05, Q07, Q08, Q10, Q11, Q20, Q21 | `dict_filter` |
| Q04 | `dict_group_count` |
| Q06 | `filter_sum` |
| Q21 | also `dict_group_count` |
| Q09, Q12-Q19, Q22 | — (no rule fired) |

Note that on real-parquet TPC-H, **10 queries fire `dict_filter`**
(up from 0 with empty MemTables) — the dict-aware string filter is
*broadly applicable* even with the existing narrow shape, because the
predicate only needs to be on a dict column. This is the model the
join shape should follow: narrow precise pattern, applies across
many queries.

## Recommended next phase ordering

Read these as "the catalog gap + the executor it implies".

### Σ.H — `Filter + HashJoin + Aggregate` shape (highest leverage)

**Coverage:** ~80+ queries across both suites have this exact
sub-pattern (filter on dim table → hash-join into fact → aggregate).
TPC-H Q3 / Q5 / Q7 / Q8 / Q10 / Q11 / Q19 / Q21; TPC-DS dozens.

**Catalog entry:** new shape `Aggregate(Final*) > ... > HashJoinExec >
Filter > Scan` (with the various wrappers we already handle on the
agg side). ~50 LOC catalog code.

**Executor work (the substance):** Σ.E6 Appendix B4 covers two
subpaths:
- *Cheap:* pre-filter the build side, then DataFusion's standard
  HashJoinExec, then our existing FilterMultiAggSpec. Probably ~1 wk
  including bench gate.
- *Aggressive:* dict-aware probe-side join (compose with dict-arrival
  decode → dict-coded probe key → translate-once cache). 2-3 wk.

Start with the cheap variant. Bench-gate. Decide on aggressive based
on results.

### Σ.I — `TopN` shape (broad, smaller per-query win)

**Coverage:** 75/103 TPC-DS queries use SortExec(TopK). Many real
analytics ("show me top 10 by sales") use this pattern.

**Catalog entry:** `SortExec(fetch=Some(N)) > Aggregate > ...` or
`GlobalLimitExec > SortPreservingMergeExec > ...`.

**Executor work:** custom heap-based TopK that runs inside the
aggregate (avoiding materialising the full sorted result). Hard to
beat DataFusion's TopK by a big margin since theirs is decent;
worth a 1-day spike to measure.

### Σ.J — `WindowAgg` shape

**Coverage:** 17/103 TPC-DS queries. Common in real analytics (
running totals, ranks, lag/lead).

**Executor work:** specialized window kernels for the common cases
(ROW_NUMBER, RANK, SUM OVER, LAG/LEAD with default offsets).
Per-shape templates, similar to FilterMultiAggSpec's family. 2-3 wk.

### Lower priority — `UnionAll`, `NestedLoopJoin`, `CrossJoin`

UnionAll is mostly about avoiding the materialisation copy (small
win). NestedLoopJoin / CrossJoin only show up on small inputs in
both suites; if a query lands on these it's already small enough
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
