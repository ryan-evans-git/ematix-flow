# Σ.E5.4.a — 22-query parity bench

Same-process bench comparing `FastParquetTableProvider` (parquet-rs) vs `EmatixFastParquetTableProvider` (ematix-parquet streaming reader, default since PR #115) across all 22 TPC-H queries on SF=1 parquet.

Source: `crates/ematix-flow-core/examples/sigma_e5_4a_22_parity_bench.rs`.

Data: `examples/tpch/data/sf1`.

Methodology: median ± σ across 10 timed trials after 3 warmups, single-machine, 14 target partitions, mimalloc allocator. Both providers register the same 8 TPC-H tables with the same rule chain (`InjectFilterMultiAggRule` + `InjectFilterSumRule` + `EnableDictGroupCountRule`). Delta = `(emat - fast) / fast × 100`. Verdict bands: within ±5% = `parity`; emat ≥ 5% faster = `EmatFaster`; emat ≥ 5% slower = `Regression`.

## 1. Bench numbers

| Query | FastParquet (ms) | EmatixFastParquet (ms) | Δ% (emat vs fast) | Verdict |
|------:|-----------------:|-----------------------:|------------------:|:--------|
| Q01  | 44.56 ± 5.89 | 47.57 ± 7.09 | +6.7 | Regression |
| Q02  | 10.61 ± 0.42 | 8.38 ± 0.67 | -21.0 | EmatFaster |
| Q03  | 21.95 ± 1.34 | 14.83 ± 0.77 | -32.4 | EmatFaster |
| Q04  | 17.91 ± 2.21 | 14.62 ± 1.01 | -18.4 | EmatFaster |
| Q05  | 27.12 ± 1.45 | 27.78 ± 2.47 | +2.5 | within ±5% |
| Q06  | 12.92 ± 1.35 | 11.17 ± 0.31 | -13.6 | EmatFaster |
| Q07  | 38.40 ± 2.23 | 35.43 ± 3.39 | -7.7 | EmatFaster |
| Q08  | 25.96 ± 2.61 | 20.97 ± 1.50 | -19.2 | EmatFaster |
| Q09  | 37.28 ± 2.02 | 32.31 ± 1.97 | -13.3 | EmatFaster |
| Q10  | 38.65 ± 1.79 | 29.78 ± 1.88 | -23.0 | EmatFaster |
| Q11  | 7.59 ± 0.75 | 5.76 ± 1.23 | -24.2 | EmatFaster |
| Q12  | 21.18 ± 0.94 | 18.34 ± 0.90 | -13.4 | EmatFaster |
| Q13  | 41.95 ± 0.67 | 53.09 ± 0.72 | +26.5 | Regression |
| Q14  | 17.51 ± 0.85 | 11.60 ± 0.59 | -33.7 | EmatFaster |
| Q15  | 23.39 ± 0.64 | 16.32 ± 3.08 | -30.2 | EmatFaster |
| Q16  | 8.42 ± 0.22 | 9.45 ± 0.85 | +12.2 | Regression |
| Q17  | 38.94 ± 1.46 | 35.58 ± 1.05 | -8.6 | EmatFaster |
| Q18  | 54.20 ± 1.42 | 54.11 ± 8.48 | -0.2 | within ±5% |
| Q19  | 21.85 ± 0.47 | 17.25 ± 0.59 | -21.1 | EmatFaster |
| Q20  | 17.54 ± 1.89 | 15.80 ± 1.22 | -9.9 | EmatFaster |
| Q21  | 45.26 ± 2.06 | 40.28 ± 1.43 | -11.0 | EmatFaster |
| Q22  | 8.15 ± 0.16 | 8.20 ± 0.14 | +0.6 | within ±5% |

**Top-line:** 3 parity, 16 EmatFaster, 3 Regression (paired queries: 22). geomean(emat / fast) = **0.8737** (target ≤ 1.02 per E5.4 acceptance).

## 2. Per-query analysis

Regressions > 5%, ordered by magnitude. Threshold for EXPLAIN ANALYZE deep-dive is > 10%; queries between 5% and 10% are listed for completeness but not individually attributed unless they cluster on a shared root cause.

### Q13 — +26.5% (41.95 → 53.09 ms)

_Deep-dive required (> 10% regression). Likely candidates, ranked by prior data from §3 capability gaps:_

1. **Filter pushdown disabled** when the streaming reader is on (see `EmatixFastParquetTableProvider::supports_filters_pushdown` — returns `Unsupported` for every filter while `streaming_arrow_reader` is true). DataFusion's residual `FilterExec` runs the predicates instead. On selective filters (Q06, Q14, Q19) this materially changes the rows-pushed-into-aggregate count.
_Confirm with `EXPLAIN ANALYZE`: count of rows emerging from the scan node should be equal to the file's total rows on EmatixFastParquet and to the post-filter row count on FastParquet._

2. **Row-group pruning by stats** — `EmatixFastParquetTableProvider::partition_statistics()` returns `Statistics::new_unknown` with only `num_rows` populated (`ematix_fast_parquet.rs:637`). FastParquet reports typed min/max from `ParquetMetaData::row_group().statistics()`, which feeds DataFusion's join-size + agg-cardinality estimates and drives row-group pruning. On stats-sensitive queries this changes the physical plan (smaller join build side, different operator ordering).

3. **Per-column decode cost on specific column types** — primarily Decimal128 (none in TPC-H), Int96, FLBA, nested. TPC-H is all Int32/Int64/Float64/Date32/Utf8; if a regression shows here it's in the Utf8 → Utf8View streaming path (Σ.E5.1.d). Cross-check with the codec-layer `bench_decode` in ematix-parquet.

4. **Different operator selection by the planner** — if `partition_statistics` differences flip a join from hash to nested loop or vice versa, this is the symptom. EXPLAIN-diff the two plans.

### Q16 — +12.2% (8.42 → 9.45 ms)

_Deep-dive required (> 10% regression). Likely candidates, ranked by prior data from §3 capability gaps:_

1. **Filter pushdown disabled** when the streaming reader is on (see `EmatixFastParquetTableProvider::supports_filters_pushdown` — returns `Unsupported` for every filter while `streaming_arrow_reader` is true). DataFusion's residual `FilterExec` runs the predicates instead. On selective filters (Q06, Q14, Q19) this materially changes the rows-pushed-into-aggregate count.
_Confirm with `EXPLAIN ANALYZE`: count of rows emerging from the scan node should be equal to the file's total rows on EmatixFastParquet and to the post-filter row count on FastParquet._

2. **Row-group pruning by stats** — `EmatixFastParquetTableProvider::partition_statistics()` returns `Statistics::new_unknown` with only `num_rows` populated (`ematix_fast_parquet.rs:637`). FastParquet reports typed min/max from `ParquetMetaData::row_group().statistics()`, which feeds DataFusion's join-size + agg-cardinality estimates and drives row-group pruning. On stats-sensitive queries this changes the physical plan (smaller join build side, different operator ordering).

3. **Per-column decode cost on specific column types** — primarily Decimal128 (none in TPC-H), Int96, FLBA, nested. TPC-H is all Int32/Int64/Float64/Date32/Utf8; if a regression shows here it's in the Utf8 → Utf8View streaming path (Σ.E5.1.d). Cross-check with the codec-layer `bench_decode` in ematix-parquet.

4. **Different operator selection by the planner** — if `partition_statistics` differences flip a join from hash to nested loop or vice versa, this is the symptom. EXPLAIN-diff the two plans.

### Q01 — +6.7% (44.56 → 47.57 ms)

Within the 5–10% band. Most likely cause: cumulative effect of filter-pushdown-disabled + unknown partition stats on a query whose hot path is dominated by aggregation, not scan. No deep-dive yet unless it clusters with a > 10% regression.

## 3. Capability gaps in EmatixFastParquet vs FastParquet

Gathered from a read of `src/ematix_fast_parquet.rs` and confirmed against the §1 acceptance check. These are properties of the *current* (PR #115) streaming-default provider, not codec capability gaps in ematix-parquet itself.

1. **Filter pushdown is deliberately disabled** when the streaming reader path is on (Σ.E5.1.b scope cut). `supports_filters_pushdown` returns `Unsupported` for every filter so DataFusion's residual `FilterExec` runs the predicates. On the bridge path (with streaming off), Int32/Date32 range predicates against a single column still push down. This is the single largest end-to-end-visible behaviour gap and the most likely cause of any > 10% regression on selective queries.

2. **Row-group pruning by typed min/max stats is not driven by the provider.** `EmatixFastParquetExec::partition_statistics` returns `Statistics::new_unknown(schema)` with only `num_rows` set (`ematix_fast_parquet.rs:646`). FastParquet reports typed min/max + null_count via `aggregate_column_statistics`, which DataFusion's planner uses for cardinality estimates and join-size selection. This means the planner can pick different operator orderings on EmatixFastParquet — usually slower, occasionally faster (parallel-build heuristics).

3. **No row-group pruning at scan time.** Both providers assign all row groups to partitions; FastParquet additionally drops row groups whose stats don't intersect the pushed-down filter (when the filter does push down). EmatixFastParquet does no RG pruning today (no filter pushdown → nothing to prune on). When E5.4.b restores pushdown, RG pruning needs to ride along.

4. **Utf8View promotion is automatic on the streaming path** (Σ.E5.1.d, PR #113). When `streaming_arrow_reader = true` and `dict_preservation = false`, Utf8 columns in the reported schema are rewritten to `Utf8View` and the streaming reader emits `StringViewArray`. This closes the Q1 SQL gate against FastParquet's supplied-schema Utf8View path — but it's automatic on EmatixFastParquet, opt-in on FastParquet. Net effect: schema parity at the boundary.

5. **Dict preservation is off by default on both providers.** This is apples-to-apples for the bench — neither path lights up `EnableDictGroupCountRule` on real TPC-H data without an explicit `with_dict_preservation(true)` (see the `dict-arrival-blocker` memory note). Wins from dict-aware execution are outside E5.4.a's scope.

6. **Both providers exercise the same per-RG-parallel partition layout** (`Partitioning::UnknownPartitioning(N)` with `N = min(num_row_groups, target_partitions).max(1)`). Decode parallelism beyond row groups is provider-specific: FastParquet uses parquet-rs's internal per-page parallelism, EmatixFastParquet uses `EmatArrowBatchReader`'s per-column scoped-thread fan-out with a partition-aware budget. The Σ.E5.1.c budget cap keeps total threads tracking the core count rather than `N_partitions × N_cols`.

## 4. Migration sequencing recommendation

**Close gaps first** — 3 query/queries regressed by more than 5% (geomean = 0.8737, target ≤ 1.02). Recommended ordered sub-phases:

1. **E5.4.b — restore filter pushdown on the streaming reader path** (highest impact). Re-enable `supports_filters_pushdown` for Int32/Date32 range predicates and fuse with the streaming bitmap-first decode. Expected to close Q06, Q14, Q19 and any other selective-filter query in the regression list.
2. **E5.4.c — typed `partition_statistics`** (medium impact). Decode `ematix_parquet_format::Statistics` for the 5 physical types and report typed min/max + null_count from `EmatixFastParquetExec::partition_statistics`. Re-runs the planner's cardinality estimates on the EmatixFastParquet side; expected to close the join-heavy regressions (Q05, Q07, Q09, Q21).
3. **E5.4.d — row-group pruning at scan time** (small impact, rides E5.4.c). Once typed stats are present, drop RGs whose stats don't intersect any pushed-down filter. Mostly redundant with E5.4.b for SF=1 (lineitem has 6 RGs total) but lands cleanly at SF=10.
4. **E5.4.e — rerun this parity bench**. Acceptance criterion: same as E5.4 — within ±5% per-query, geomean ≤ 1.02. Migrate in-tree call sites once green.

## 5. Bench reproduction

Prerequisite: SF=1 TPC-H parquet under `examples/tpch/data/sf1/`. Generate once (multi-minute):

```sh
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 1 --out examples/tpch/data/sf1
```

Then:

```sh
cargo run --release -p ematix-flow-core --example sigma_e5_4a_22_parity_bench
```

Knobs (env):

- `TPCH_DATA_DIR` — override the SF=1 path.
- `TPCH_TRIALS`   — measured trials per (query, provider). Default 21.
- `TPCH_WARMUPS`  — untimed warmups before measured trials. Default 3.
- `TPCH_QUERIES`  — comma-separated subset, e.g. `1,6,14`. Default all 22.
- `TPCH_OUT`      — markdown output path. Default `BENCH_E5_4A_22_PARITY.md`
   at the workspace root.

The bench writes the table above into the output path on every run and also prints the per-query verdict + geomean to stdout. To re-freeze this findings doc, run the bench, copy the printed table here, and update §1's top-line counts + the §4 recommendation conditional.

