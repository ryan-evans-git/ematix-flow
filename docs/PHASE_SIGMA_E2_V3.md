# Σ.E2 v3 — Filter pushdown for FastParquetTableProvider

Status: in progress (started 2026-05-12). Builds on the merged
`FastParquetTableProvider` (PR #58 v2b: 14w/3l/1.10× geomean on
TPC-H SF=1 22-query suite).

## Why

After re-bench on 2026-05-12 main, the three remaining losses are
all JOIN-heavy with selective filters where DataFusion's default
parquet path wins via `DynamicFilter`:

| Query | Δ % | Default's edge |
|-------|-----|----------------|
| Q10 | -24% | DynamicFilter on `o_custkey`, `l_orderkey` + static `l_returnflag=R` |
| Q12 | -36% | DynamicFilter on `o_orderkey` (54MB→7MB scan) + static `l_shipmode IN (MAIL,SHIP)` |
| Q18 | -13% | DynamicFilter on `o_custkey` and both `l_orderkey` scans |

`FastParquetExec` currently exposes no `gather_filters_for_pushdown`
hook, so DataFusion's planner leaves all filters in a `FilterExec`
parent and the scan reads every row group whole.

## Strategy

Implement `ExecutionPlan::gather_filters_for_pushdown` and
`handle_child_pushdown_result` on `FastParquetExec`. Accept incoming
`Arc<dyn PhysicalExpr>` filters (which may be `DynamicFilterPhysicalExpr`
wrappers from a HashJoin build side). Translate them to parquet-rs
`RowFilter` + use row-group statistics for cheap whole-group pruning.

Reference: [Dynamic Filters: Passing Information Between Operators](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters)
and `datafusion-physical-expr-53.1.0/src/expressions/dynamic_filters.rs`.

## PR decomposition

Each PR ships independently with a bench checkpoint, ordered to
expose value early.

### v3.1 — Static-predicate pushdown skeleton

**Status (2026-05-12): scaffolding only, absorption disabled.**

Goal was: prove the `gather_filters_for_pushdown` wiring works, with
a tiny translator that handles `Column = Literal` for primitive types
applied post-decode via `filter_record_batch`.

Wiring landed cleanly and the plan-shape test passed (filters got
absorbed, `FilterExec` removed). The perf result is what changed the
plan: bench regressed from 14w/3l/1.10× to 13w/7l/1.04× even with
narrow acceptance, and to 8w/11l/1.01× at smaller batches. EXPLAIN
ANALYZE audit on Q01 localized the cost:

- DataFusion's `FilterExec` is heavily pipelined and batch-size tuned
  for downstream operators (hash aggregator, sort, etc).
- Absorbing filters into `FastParquetExec`'s `spawn_blocking` decode
  loop forces serialized decode→filter on the same thread, inflating
  `RepartitionExec.fetch_time` 2.5× (270ms → 672ms on Q01).
- 65K-row decode batches (our parquet sweet spot) make the downstream
  hash aggregator's `time_calculating_group_ids` 3.5× slower vs the
  8192-row batches DataFusion is tuned for. 8192 helps that, but
  kills decode throughput on its own.

**Lesson:** post-decode evaluation is the wrong primitive. The win
shape requires either filtering *during* parquet decode (so we never
materialize pruned rows and don't disturb downstream batch sizing) or
*before* decode via row-group statistics pruning (where we skip work
outright). The handful of queries v3.1 helped (Q12, Q18) were ones
where DataFusion's own pipeline was already cold; the net was a
regression.

Where v3.1 lives now (kept as scaffolding for v3.2):

- `FastParquetExec.pushdown_filters: Vec<Arc<dyn PhysicalExpr>>` field.
- `handle_child_pushdown_result` override that *would* absorb filters
  via `filter_is_supported`.
- `filter_is_supported` returns `false` unconditionally as of v3.1.
  v3.2 flips it to a real shape check.
- `build_partition_stream` already takes `pushdown_filters` and would
  apply them via `filter_record_batch` if any were stored.

Tests:
- `pushdown_count_correctness_string_eq` — passes (FilterExec carries).
- `pushdown_count_correctness_int_eq` — passes (FilterExec carries).
- `pushdown_unsupported_falls_back_correctly` — passes (LIKE rejected,
  FilterExec applies).
- `pushdown_plan_eliminates_filter_exec` — `#[ignore]`'d with a
  pointer back to this doc. Re-engages as v3.2's first failing test.

### v3.2 — Decode-time pushdown via parquet-rs `with_row_filter` (attempted, blocked)

**Status (2026-05-12): attempted, blocked at the TableProvider API layer.**

Attempted v3.2 implementation: pre-compute `(parquet_leaf_idx, expr_rewritten_to_col0)`
per accepted filter, wrap each in an `ArrowPredicate` calling the
PhysicalExpr's own `evaluate`, hand them to
`ParquetRecordBatchReaderBuilder::with_row_filter`. Wiring compiled
cleanly, plan-shape test re-passed. Bench: **9w / 13l / 0.87× geomean.**

Two queries blew up catastrophically:
- Q06: 21ms → 251ms (12× slower).
- Q20: 43ms → 595ms (14× slower).

Root cause: per the parquet-rs docs,

> Columns may be decoded multiple times if they appear in multiple
> ProjectionMasks … if a predicate needs several columns of data to
> evaluate but leaves 99% of the rows, it may be better to not filter
> the data from parquet.

On Q06, the predicate references `l_shipdate`, `l_discount`,
`l_quantity` — all three are *also* in the output projection. So
parquet-rs decodes them once per predicate (in sequence, refining the
row selection), then decodes them again for the output, against
millions of rows. The double-decode dominates.

**The deeper architectural finding:** parquet-rs's `RowFilter` is
designed for the case where predicate columns are *not* in the
output projection (high-selectivity filter on a wide table where the
output excludes the predicate columns). DataFusion's current
`TableProvider::scan(projection: Option<&Vec<usize>>)` contract
bundles "columns the output needs" and "columns referenced by
pushed-down predicates" into a single projection vector — there is
no way to mark a column as predicate-only.

Without that distinction, **every** filter we accept has its
predicate column in our output projection (otherwise the column
wouldn't be in `scan()`'s projection in the first place). So every
v3.2 acceptance triggers the double-decode regression.

This isn't fixable inside `FastParquetExec`; it requires either:

1. An upstream DataFusion change so `TableProvider::scan` can
   distinguish output projection from predicate-only columns
   (probably a new method or a richer scan request object). Significant
   upstream contribution.
2. A different optimization target that *doesn't* need the
   predicate-only column concept — e.g. row-group statistics
   pruning (v3.3), where we skip work *before* any decode and the
   projection question never arises.

Action: revert v3.2 implementation. Keep the scaffolding (filter
absorption wiring, `pushdown_filters` field, `extract_column_eq_literal`
helper) for future use once the upstream gap is addressed. Re-ignore
the plan-elimination test.

### v3.3 — Row-group statistics pruning (now the next shippable PR)

**Now the leading v3 candidate** since it avoids both the v3.1 post-
decode pipelining regression and the v3.2 predicate-column-overlap
regression entirely. Pruning happens before any decode, so it's
strictly additive to the existing scan path.

Scope:
- For each row group, evaluate accepted filters against
  `column_statistics[i].min_value/max_value` (already collected in
  v2b).
- If `min > literal` for `eq`, or `max < literal` for `gteq`, etc.,
  remove that row group from the partition's assignment before the
  scan starts.
- Empty assignments produce empty streams (already supported).
- Reduce partition count if all row groups in a partition prune.

Failing tests:
- `row_group_pruning_skips_disjoint_stats` — synthetic 6-row-group
  parquet with `id` 0..1000, query `WHERE id > 5000` should scan
  zero row groups.
- `row_group_pruning_q12_pushes_to_parquet` — Q12's
  `l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'`
  should prune the lineitem row groups whose date ranges don't
  overlap (the default parquet path's `row_groups_pruned_statistics`
  metric shows this kind of pruning happening on Q12).
- Bench-style guard: Q12 should improve from current -36% to within
  -20%, with no other query worse than -5%.

Effort: ~1 week.

### v3.4 — Dynamic filter integration (deferred until v3.3 ships)

Same scope as before. Pre-requisite: v3.3 lands the row-group
pruning path; dynamic filters update the pruning predicate as
build-side rows arrive.

### v3.5 — Bench + write-up + close

Same scope as before.

### v3.3 — Row-group statistics pruning

Goal: skip whole row groups entirely when their column stats
guarantee no rows match the filter.

Scope:
- For each row group, evaluate the filter against
  `column_statistics[i].min_value/max_value` (already collected in
  v2b).
- If `min > literal` for `eq`, or `max < literal` for `gteq`, etc.,
  skip the entire row group from the partition plan.
- Page-index pruning is *not* in v3.3 — that's a parquet-2.4 feature
  that needs additional row-group→page-index metadata. Maybe v3.6.

Failing tests:
- `row_group_pruning_skips_when_stats_disjoint` — synthetic 6-row-group
  parquet with `id` 0..1000, query `WHERE id > 5000` should scan zero
  row groups.

Effort: ~1 week.

### v3.4 — Dynamic filter subscription

Goal: pick up build-side filter from a HashJoin probe scan. **This
is the piece that actually closes Q10/Q12/Q18.**

Scope:
- Detect `DynamicFilterPhysicalExpr` in the incoming filter list
  (via downcast or via the `snapshot_generation` API).
- On every new row group scan, call `current()` to snapshot the latest
  filter expression.
- Optionally `subscribe()` to the `watch::Sender<FilterState>` so we
  block briefly if the build side is still producing.

Failing tests:
- `q12_pushdown_closes_gap` — Q12 against FastParquet should be within
  20% of default DataFusion (currently -36%; passes when ≥ -20%).
- `q10_pushdown_closes_gap` — same shape for Q10.
- `dynamic_filter_picks_up_build_side_update` — synthetic test with
  a small build-side, large probe-side; assert probe scan reads
  only the build-key range after the filter populates.

Effort: ~1-2 weeks.

### v3.5 — Bench + write-up + close

Goal: re-run the 22-query suite, document, claim the geomean delta.

Scope:
- `tpch_fast_parquet_bench` reruns; results into `docs/BENCHMARKS.md`
  under a new "v3" section.
- README update if v3 changes the headline number materially.
- Memory file update closing this loss-list thread (or recording
  what's left).

Effort: ~1 week.

## Out of scope for v3

- **Page-index pruning** (parquet 2.4+). Worth investigating after
  v3 lands; orthogonal to dynamic filters.
- **Bloom filter pruning**. Same: separate feature, separate PR.
- **Predicate cache** (DataFusion's `predicate_cache_records` metric).
  Optimization, not a correctness/perf gap.
- **String column min/max stats**. With Q04/Q20 out of the loss
  column, this is much lower priority than it looked in v2b.

## TDD discipline

Each sub-phase opens with a failing test that names the behavior we
want. The test stays in tree as a regression guard after the
implementation lands. No "tested manually with bench" — if the
behavior matters, it gets a Rust test.
