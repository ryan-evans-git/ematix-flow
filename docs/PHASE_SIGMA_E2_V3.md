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

## Strategy (revised 2026-05-12)

The original v3 strategy assumed filter pushdown was the missing
lever for closing the SF=1 loss queries. Three filter-pushdown
approaches were investigated:

1. **v3.1** — Post-decode filter evaluation: regressed (FilterExec
   is too well-tuned to compete with).
2. **v3.2** — Decode-time pushdown via parquet `with_row_filter`:
   regressed (TableProvider API bundles output and predicate
   projections, triggering double-decode).
3. **v3.3** — Row-group statistics pruning: correct, but inert on
   TPC-H SF=1 data (DataFusion's own pruning also doesn't fire),
   and the SF=10 audit that followed revealed the *real* bottleneck
   is FastParquetExec's RG-count partitioning, not filter pushdown.

**Revised strategy:** v3.3 ships as the substrate. **v4** —
byte-range partitioning + pipelined streaming — is the next real
perf investment. **v3.4** (dynamic filters) waits until v4 lands;
layering it on the current partitioning compounds the SF=10
regression rather than helps.

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

### v3.3 — Row-group statistics pruning (shipped on PR #62)

**Status (2026-05-12): shipped on `feat/fast-parquet-row-group-pruning`
as PR #62.** Correct, ship-ready, but **not the perf lever we hoped
for** — the SF=10 audit that followed told a different story (see
"SF=10 audit + v4 pivot" below).

What landed:
- Per-row-group, per-column `ColumnStatistics` collection at provider
  open (`per_row_group_column_statistics`).
- `supports_filters_pushdown` reports `Inexact` for `Column op Literal`
  shapes so the planner passes filters to `scan()` *without* removing
  the parent `FilterExec` (we never claim ownership — pruning is
  advisory).
- `column_op_literal_for_pruning` + `row_group_can_match` peel
  supported shapes and consult per-RG min/max.
- `scan()` filters the row-group assignment vector to surviving RGs.

Tests:
- `row_group_pruning_impossible_predicate_skips_all` — `l_orderkey >
  999_999_999` against SF=1 lineitem prunes all 6 RGs (was 6 before
  the implementation; now 0).
- `row_group_pruning_inclusive_predicate_keeps_all` — full-range
  predicate keeps all 6 RGs.
- `row_group_pruning_correctness_when_all_pruned` — count returns 0,
  no crash on empty assignments.

Bench:
- **SF=1**: 12w / 6l / 1.08× geomean — within run-to-run variance of
  the v2b baseline (14w/3l/1.10× to 15w/4l/1.38× across runs of
  identical code earlier today). Pruning didn't fire on the loss
  queries (Q10/Q12/Q18) because SF=1 lineitem RGs each span the full
  date and orderkey ranges. DataFusion's *own* parquet path shows the
  same `row_groups_pruned_statistics=N total → N matched` non-pruning
  on those queries in the v3.2 EXPLAIN data — even the default can't
  prune by stats on this data layout.
- **SF=10**: 12w / 7l / 1.03× geomean (vs 7w/11l/0.97× pure-v2b
  baseline via `FASTPARQUET_DISABLE_V33=1` A/B). v3.3 mitigates SF=10
  regressions partially but doesn't reverse them.

v3.3 still earns its place:
- Strictly additive (no regression risk).
- Correct where stats *can* prune (test-confirmed).
- Substrate v3.4 would have built on — but v3.4 is now deferred (see
  below).

### v3.4 — Dynamic filter integration

**Status (2026-05-12): deferred until v4 lands.** Layering dynamic
filters on top of the current FastParquetExec partitioning would
compound the SF=10 regression captured below, not help — the
Q10/Q12/Q18 gaps at SF=10 are dwarfed by the partitioning overhead.
The v3.4 design is unchanged; the gate is "v4 architecture in place
first."

### v3.5 — Bench + write-up + close

Deferred. Will run once v3.4 (post-v4) lands and there's a real win
to claim. The v3.3-only bench is recorded in PR #62 and the memory
file; no headline-level update warranted.

## SF=10 audit + v4 pivot

After v3.3 landed at SF=1 with no perf delta, we re-benched at SF=10
to test the substrate hypothesis (the row-group pruning should help
at scale where the data layout is more selective). The SF=10 result
told a different story:

| | SF=10 v3.3 enabled | SF=10 pure v2b (FASTPARQUET_DISABLE_V33=1) |
|---|---|---|
| Geomean | 1.03× | 0.97× |
| Q01 | -61% | -56% |
| Q06 | -46% | -49% |
| Q12 | -56% | -70% |
| Q13 | **-512%** (1.87s) | **-14134%** (35.5s — 142× slower!) |
| Q18 | -26% (5.8s) | +59% (10.3s) |

v3.3 *mitigates* SF=10 regressions but doesn't cause them. The real
issue is structural in FastParquetExec.

### EXPLAIN ANALYZE on Q13 SF=10 — root cause

Key metric: cumulative `RepartitionExec.fetch_time` on the orders
scan + filter chain:

- Default: **1.35s** (over a 14-byte-range scan of a 564 MB file)
- FastParquet: **129s** (over 14 RG-based partitions of the same file)

The default `ParquetExec` splits each parquet file into N byte-range
partitions sized to balance work across cores. FastParquetExec splits
by row-group count (`min(num_row_groups, target_partitions)`):

- SF=1 lineitem: 6 RGs × ~12 MB. 14 partitions → uneven (8 empty),
  but RGs are small. Works.
- SF=10 orders: 15 RGs × ~38 MB. 14 partitions → 1 with 2 RGs, 13
  with 1 RG. Larger RGs + 14 concurrent file seeks on the same 564
  MB file create IO contention.
- SF=10 lineitem: ~60 RGs × ~38 MB across 2.2 GB. Same pattern,
  worse.

Combined with our `spawn_blocking + Vec<RecordBatch>` pattern (which
decodes all RG batches before yielding any), the partition does no
useful work for the consumer until the entire row group is decoded.
Default's reader yields batches as they decode.

### v4 — byte-range partitioning + pipelined streaming

**Now the next real perf investment.** Without this, scale-out
claims for FastParquet are bounded by the current architecture.

Scope:
- Rewrite FastParquetExec partitioning to split each parquet file
  into N byte-range partitions (instead of RG-count partitions).
  Parquet readers know how to handle a byte range that covers a
  subset of row groups (`with_row_groups` filtered to those
  intersecting the range).
- Pipelined async streaming: yield each RecordBatch as it decodes,
  not after the full RG. Drop the `spawn_blocking + Vec` shape.
- Add metrics to FastParquetExec — today it emits `metrics=[]`,
  which is why this audit had to infer scan cost from RepartitionExec.fetch_time
  upstream.

Failing tests:
- `byte_range_partitioning_matches_target_partitions` — synthetic
  parquet of N RGs, `target_partitions=K`; assert that
  `FastParquetExec.assignments.len() == K` independent of N.
- `pipelined_emission_yields_first_batch_before_rg_complete` —
  observable via stream readiness; assert the first batch arrives
  before all RG batches are decoded.
- `q13_sf10_within_2x_of_default` — bench-style guard; Q13 at SF=10
  must be within 2× of the default (currently 7× slower).

Effort: 2–3 weeks (this is the real perf investment v3 was orbiting).

### v3.4 picks back up after v4

Once FastParquetExec scales correctly, v3.4 (dynamic filters from
HashJoin build-side) can ride on the v3.3 row-group pruning machinery
without the SF=10 architecture amplifying every regression.

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
