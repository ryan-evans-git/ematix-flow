# Σ.E5.4.a — 22-query parity findings

**Status:** Bench shipped (measurement deferred to first run on a quiescent
host). Findings frame the per-query analysis + migration sequencing once
numbers land.
**Phase:** E5.4.a in `docs/PHASE_SIGMA_E5_PARQUET_RS_ELIMINATION.md` §4.
**Acceptance criterion:** 22-query parity within ±5% per-query, geomean ≤ 2% loss.
**Bench source:** `crates/ematix-flow-core/examples/sigma_e5_4a_22_parity_bench.rs`.

This doc compares `FastParquetTableProvider` (parquet-rs) vs
`EmatixFastParquetTableProvider` (ematix-parquet streaming reader,
default since PR #115) across all 22 TPC-H queries on SF=1 parquet.
The goal is end-to-end "what users get with each provider" — same rule
chain registered for both runs, same 8 TPC-H tables, same 14 target
partitions, single mimalloc allocator.

---

## 1. Bench numbers

> **Freeze instructions.** Run the bench
> (`cargo run --release -p ematix-flow-core --example sigma_e5_4a_22_parity_bench`)
> on a quiescent host. The bench writes `BENCH_E5_4A_22_PARITY.md` at the
> workspace root containing the table below. Paste that table over this
> placeholder, then refresh §1's top-line counts + §4's recommendation
> conditional from the bench's stdout summary.

| Query | FastParquet (ms) | EmatixFastParquet (ms) | Δ% (emat vs fast) | Verdict |
|------:|-----------------:|-----------------------:|------------------:|:--------|
| Q01   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q02   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q03   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q04   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q05   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q06   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q07   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q08   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q09   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q10   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q11   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q12   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q13   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q14   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q15   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q16   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q17   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q18   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q19   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q20   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q21   | _pending_         | _pending_              | _pending_         | _pending_ |
| Q22   | _pending_         | _pending_              | _pending_         | _pending_ |

**Top-line:** _pending bench run_. Update with counts (parity / EmatFaster
/ Regression) + geomean(emat/fast) from the bench's stdout summary block.

**Methodology:** median ± σ across 21 timed trials after 3 warmups,
single-machine, 14 target partitions, mimalloc allocator. Both
providers register the same 8 TPC-H tables with the same physical-
optimizer rule chain (`InjectFusedQ{1,3,5,12}Rule` +
`InjectFilterSumRule` + `EnableDictGroupCountRule`). Delta =
`(emat - fast) / fast × 100`. Verdict bands: within ±5% = `parity`;
emat ≥ 5% faster = `EmatFaster`; emat ≥ 5% slower = `Regression`.

### Skips and queries excluded

TPC-H SF=1 covers Int32 / Int64 / Float64 / Date32 / Utf8 — exactly the
set `EmatixFastParquetTableProvider::try_new` validates. Neither
provider is expected to skip any query at SF=1; both planners share
DataFusion's SQL surface. Any skip / fail in the table above is a real
finding (not a feature gap of either provider) — capture the reason
inline.

---

## 2. Per-query analysis

The bench's runtime emits a per-query verdict + a list of regressions
above 5%. Use that list to drive this section. The framework below
applies once numbers land — keep it as the standing template so each
re-run follows the same investigation flow.

For each query with a regression > 5%, attribute the cause from this
ranked candidate list (ordered by prior probability given §3's known
capability gaps):

1. **Filter pushdown is disabled on the streaming reader path.**
   `EmatixFastParquetTableProvider::supports_filters_pushdown`
   short-circuits to `Unsupported` while
   `streaming_arrow_reader = true` (the default since PR #115). On
   FastParquet, Int32/Date32 single-column range predicates push down
   `Exact`. DataFusion's residual `FilterExec` runs the predicates
   instead on EmatixFastParquet — selective queries (canonical
   suspects: **Q06**, **Q14**, **Q19**) hand more rows downstream than
   the FastParquet path does. _Confirm with `EXPLAIN ANALYZE`: the row
   count emerging from the scan node should match the full file row
   count on EmatixFastParquet, and the post-filter row count on
   FastParquet._
2. **Row-group pruning by stats is not driven by the provider.**
   `EmatixFastParquetExec::partition_statistics` returns
   `Statistics::new_unknown(schema)` with only `num_rows` set
   (`ematix_fast_parquet.rs:646`). FastParquet reports typed min/max +
   null_count from `aggregate_column_statistics`. DataFusion's planner
   uses those for cardinality + join-build-side estimates → potentially
   different operator selection on join-heavy queries (canonical
   suspects: **Q05**, **Q07**, **Q08**, **Q09**, **Q21**).
3. **Different decode time on specific column types.** TPC-H SF=1 is
   homogeneous (no Int96, FLBA, Decimal128, nested types), so this
   candidate is unlikely to fire. If it does, the symptom is a Utf8 /
   Utf8View column that takes longer on the streaming reader than on
   parquet-rs's. Cross-check by running `bench_decode` in
   ematix-parquet on the same column.
4. **Different operator selection by the planner.** If candidates 1
   and 2 don't explain the gap, EXPLAIN-diff the two physical plans.
   Common manifestation: a join flips from hash to nested-loop when
   the stats disappear, or an aggregate spills differently.

For queries within the 5–10% regression band, attribute by clustering
(don't deep-dive each one). A typical pattern is the cumulative effect
of #1 + #2 on a query whose hot path is dominated by aggregation, not
scan — the deltas are visible but each individual cause is too small
to single-out.

For queries where EmatixFastParquet **wins by > 5%**, the expected
attribution is:

- **Σ.E5.1.d Utf8View promotion is automatic on EmatixFastParquet's
  streaming path** (PR #113). FastParquet only emits Utf8View when
  `with_supplied_schema` is set with a Utf8View target. The Q1 SQL gate
  (`tpch_q1_e2e_gate.rs`) already shows EmatixStreaming(Utf8View) at
  parity with FastParquet+Utf8View; on queries where the call site
  doesn't override the supplied schema, EmatixFastParquet ships
  Utf8View while FastParquet ships Utf8 — visible win on string-heavy
  shapes (canonical: **Q01**, **Q03**, **Q10**, **Q13**).
- **Per-column parallel decode via `EmatArrowBatchReader`** (Σ.E5.1.b).
  The streaming reader fans out per-column decode across a
  partition-aware thread budget (Σ.E5.1.c). On wide-projection queries
  with many columns and fewer row groups than cores, this can extract
  parallelism FastParquet's per-RG decode misses.

---

## 3. Capability gaps in EmatixFastParquet vs FastParquet

Read directly from `src/ematix_fast_parquet.rs` (PR #115 streaming-
default state). These are properties of the *current* provider, not
codec capability gaps in ematix-parquet itself.

1. **Filter pushdown is deliberately disabled** when the streaming
   reader path is on (Σ.E5.1.b scope cut). `supports_filters_pushdown`
   returns `Unsupported` for every filter. On the bridge path (with
   streaming off), Int32/Date32 single-column range predicates still
   push down — but the bridge path is no longer the default. **Single
   largest end-to-end-visible behaviour gap.**
2. **Row-group pruning by typed min/max stats is not driven by the
   provider.** `partition_statistics` returns
   `Statistics::new_unknown(schema)` with only `num_rows`. The
   planner can't use typed bounds for cardinality / join-build-side
   estimates.
3. **No row-group pruning at scan time.** Both providers assign all
   row groups to partitions; FastParquet additionally drops row groups
   whose stats don't intersect the pushed-down filter (when the filter
   does push down). EmatixFastParquet does no RG pruning today — it
   has no filter to prune against, since #1 is disabled.
4. **Utf8View promotion is automatic on the streaming path** (Σ.E5.1.d,
   PR #113). When `streaming_arrow_reader = true` and
   `dict_preservation = false`, the schema is rewritten Utf8 →
   Utf8View at `try_new` time and the reader emits StringViewArray.
   FastParquet emits Utf8 by default unless a Utf8View supplied schema
   is set. _Net effect: parity at the boundary when FastParquet is
   configured for Utf8View, plus a win on call sites that don't
   explicitly configure it._
5. **Dict preservation is off by default on both providers.**
   Apples-to-apples for the bench — neither path lights up
   `EnableDictGroupCountRule` on real TPC-H data without an explicit
   `with_dict_preservation(true)` (see the `dict-arrival-blocker`
   memory note). Wins from dict-aware execution are outside E5.4.a's
   scope.
6. **Both providers exercise the same per-RG-parallel partition
   layout** (`Partitioning::UnknownPartitioning(N)` with
   `N = min(num_row_groups, target_partitions).max(1)`). Decode
   parallelism beyond row groups is provider-specific: FastParquet uses
   parquet-rs's internal per-page parallelism; EmatixFastParquet uses
   `EmatArrowBatchReader`'s per-column scoped-thread fan-out with the
   Σ.E5.1.c budget cap.

### Capabilities that are NOT gaps (already at parity)

- **Schema derivation.** EmatixFastParquet still uses
  `ArrowReaderMetadata::load` for Arrow-schema synthesis at `try_new`
  time. Parity by definition. (Migrating off parquet-rs for schema
  synth is E5.3, not E5.4.)
- **Per-column-type decode kernels.** Bridge already covers Int32 /
  Int64 / Float64 / Date32 / Utf8. NEON-fused unpack and Snappy
  buffer reuse are EmatixFastParquet **wins** (Π.10 + Π.11), not
  gaps.
- **Late-materialisation (Σ.E5a / Π.10).** Default-on; closes the
  bitmap-first decode in lock-step with filter pushdown. When
  `streaming_arrow_reader = true` AND no filter is pushed (the
  default), late-mat is a no-op anyway. When E5.4.b restores
  pushdown, the existing `filter.is_some()` branch already wires
  through `decode_one_rg_filtered_late_mat`.
- **Outer-partition layout.** Same `Partitioning::UnknownPartitioning`
  count on both.
- **mimalloc allocator.** Same `#[global_allocator]` for both
  providers — every example registers it.

---

## 4. Migration sequencing recommendation

> **Conditional.** The bench output's top-line + geomean drive which
> branch applies. Read the stdout summary block; pick (a) or (b).

### (a) If EmatixFastParquet is already within ±5% per-query and geomean ≤ 1.02

**Proceed to E5.4.b in-tree switch.** Sequencing:

1. **E5.4.b** — flip `tpch_triangulation_bench.rs` and other in-tree
   consumers from `FastParquetTableProvider` to
   `EmatixFastParquetTableProvider` one call site at a time. Gate each
   on its own bench run (Q1 e2e gate, Q14 e2e gate, etc.).
2. **E5.4.c** — restore filter pushdown on the streaming path. Re-
   enable `supports_filters_pushdown` for Int32/Date32 range predicates
   and fuse the bitmap-first decode with the streaming emission.
   Expected wins on Q06/Q14/Q19 — the queries most likely to be at
   parity-or-slight-loss in (a). After this lands, re-run the parity
   bench; queries that were at parity should move into EmatFaster.
3. **E5.4.d** — wire typed `partition_statistics`. Decode
   `ematix_parquet_format::Statistics` for Int32 / Int64 / Float /
   Double / Bool and report typed min/max + null_count from
   `EmatixFastParquetExec::partition_statistics`.
4. **E5.4.e** — delete `FastParquetTableProvider` and its parquet-rs
   imports from `src/fast_parquet.rs`; verify
   `cargo tree -p ematix-flow-core -e=normal | grep parquet` shows no
   direct `parquet 58` edge.

### (b) If any query regresses by > 5% OR geomean > 1.02

**Close the named gaps first; do NOT migrate in-tree consumers yet.**
Ordered by expected impact:

1. **E5.4.b — restore filter pushdown on the streaming reader path.**
   Highest-impact lever. Re-enable
   `supports_filters_pushdown` for Int32/Date32 range predicates and
   wire the bitmap-first decode (already implemented in the bridge
   path) into the streaming emission. Expected to close Q06, Q14,
   Q19, and most regressions in the 5–15% band.
2. **E5.4.c — typed `partition_statistics`.** Decode
   `ematix_parquet_format::Statistics` for the 5 physical types and
   report typed min/max + null_count. Re-runs the planner's
   cardinality estimates on the EmatixFastParquet side; expected to
   close the join-heavy regressions (Q05, Q07, Q08, Q09, Q21) by
   restoring the same hash-build-side selection FastParquet gets.
3. **E5.4.d — row-group pruning at scan time** (rides E5.4.c). Once
   typed stats are present, drop row groups whose stats don't
   intersect any pushed-down filter. Mostly redundant with E5.4.b at
   SF=1 (lineitem has 6 RGs total) but compounds cleanly at SF=10.
4. **E5.4.e — rerun this parity bench**. Acceptance criterion: same
   as E5.4 — within ±5% per-query, geomean ≤ 1.02. Migrate in-tree
   call sites only once the rerun is green.

---

## 5. Bench reproduction

**Prerequisite:** SF=1 TPC-H parquet under `examples/tpch/data/sf1/`.
Generate once (multi-minute):

```sh
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 1 --out examples/tpch/data/sf1
```

Then:

```sh
cargo run --release -p ematix-flow-core --example sigma_e5_4a_22_parity_bench
```

The bench writes `BENCH_E5_4A_22_PARITY.md` at the workspace root with
the table from §1 above, and prints a per-query verdict + geomean to
stdout. To re-freeze this findings doc, copy the table over §1's
placeholder and update the top-line counts + the §4 conditional from
the stdout summary.

Knobs (env):

- `TPCH_DATA_DIR` — override the SF=1 path.
- `TPCH_TRIALS`   — measured trials per (query, provider). Default 21.
- `TPCH_WARMUPS`  — untimed warmups before measured trials. Default 3.
- `TPCH_QUERIES`  — comma-separated subset, e.g. `1,6,14`. Default all 22.
- `TPCH_OUT`      — markdown output path. Default
  `BENCH_E5_4A_22_PARITY.md` at the workspace root.

For an individual-query EXPLAIN-diff during regression triage:

```sh
TPCH_QUERIES=6,14,19 TPCH_TRIALS=5 TPCH_WARMUPS=1 \
  cargo run --release -p ematix-flow-core --example sigma_e5_4a_22_parity_bench
```

To EXPLAIN-diff plans directly, use the existing per-query plan-dump
examples (`tpch_q6_plan_dump`, `tpch_q1_plan_dump`, `tpch_q3q5_plan_dump`,
`tpch_q12_plan_dump`) — register each with both providers in turn and
diff the output.
