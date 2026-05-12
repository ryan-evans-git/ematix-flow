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

Goal: prove the `gather_filters_for_pushdown` wiring works.
Translator handles `Column = Literal` for primitive types only.

Scope:
- Implement `gather_filters_for_pushdown` returning incoming filters
  as `SupportsExact` for shapes we can handle, `Unsupported` otherwise.
- Implement `handle_child_pushdown_result` to absorb supported
  filters (so the planner removes the `FilterExec`).
- Mini translator: `BinaryExpr(Column = Literal)` where the column
  is a primitive type → parquet-rs `ArrowPredicate` via
  `RowFilter::new`.
- Apply via `ParquetRecordBatchReaderBuilder::with_row_filter`.

Failing tests (write first):
- `pushdown_plan_eliminates_filter_exec` — `EXPLAIN SELECT ... WHERE l_shipmode = 'MAIL'`
  on FastParquet should not contain `FilterExec` above the scan.
- `pushdown_count_correctness` — `SELECT count(*) WHERE l_shipmode = 'MAIL'`
  matches default DataFusion result.
- `pushdown_unsupported_falls_back` — a shape we don't handle yet
  (e.g. `l_shipmode LIKE 'M%'`) still produces correct results
  (FilterExec stays).

Effort: ~1 week.

### v3.2 — Full PhysicalExpr → ArrowPredicate translator

Goal: cover the predicate shapes TPC-H actually uses.

Scope:
- `eq/lt/gt/lteq/gteq/not_eq/and/or` `BinaryExpr` combinations
- `InListExpr` for small literal sets
- `BetweenExpr`
- Decimal coercion (TPC-H prices are `Decimal128(15,2)`)
- Date literal coercion (`Date32`)
- Utf8 / LargeUtf8 equality (deferred from v2b)

Failing tests:
- Q12 static filter shape: `l_shipmode IN ('MAIL','SHIP') AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'` correctness + plan shape.
- Q14 static: `l_shipdate >= ... AND l_shipdate < ...`.
- Q19 static: complex `(brand AND ship AND quantity) OR (brand AND ship AND quantity) OR ...`.

Effort: ~1-2 weeks.

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
