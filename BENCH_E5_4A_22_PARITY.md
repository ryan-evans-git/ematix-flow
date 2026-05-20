# Σ.E5.4.a — 22-query parity bench

Same-process bench comparing `FastParquetTableProvider` (parquet-rs) vs `EmatixFastParquetTableProvider` (ematix-parquet streaming reader, default since PR #115) across all 22 TPC-H queries on SF=1 parquet.

Source: `crates/ematix-flow-core/examples/sigma_e5_4a_22_parity_bench.rs`.

Data: `examples/tpch/data/sf1`.

Methodology: median ± σ across 15 timed trials after 3 warmups, single-machine, 14 target partitions, mimalloc allocator. Both providers register the same 8 TPC-H tables with the same rule chain (`InjectFilterMultiAggRule` + `InjectFilterSumRule` + `EnableDictGroupCountRule`). Delta = `(emat - fast) / fast × 100`. Verdict bands: within ±5% = `parity`; emat ≥ 5% faster = `EmatFaster`; emat ≥ 5% slower = `Regression`.

## 1. Bench numbers

| Query | FastParquet (ms) | EmatixFastParquet (ms) | Δ% (emat vs fast) | Verdict |
|------:|-----------------:|-----------------------:|------------------:|:--------|
| Q01  | 28.46 ± 1.44 | 31.58 ± 1.18 | +11.0 | Regression |
| Q02  | 10.12 ± 0.43 | 7.71 ± 0.25 | -23.8 | EmatFaster |
| Q03  | 19.44 ± 0.54 | 13.23 ± 1.56 | -32.0 | EmatFaster |
| Q04  | 15.49 ± 0.38 | 12.35 ± 0.93 | -20.3 | EmatFaster |
| Q05  | 24.41 ± 1.10 | 21.71 ± 0.93 | -11.0 | EmatFaster |
| Q06  | 11.49 ± 0.25 | 9.65 ± 0.68 | -16.0 | EmatFaster |
| Q07  | 30.61 ± 2.69 | 26.37 ± 0.60 | -13.9 | EmatFaster |
| Q08  | 25.09 ± 1.04 | 18.17 ± 0.62 | -27.6 | EmatFaster |
| Q09  | 29.15 ± 1.13 | 25.91 ± 1.48 | -11.1 | EmatFaster |
| Q10  | 34.37 ± 0.93 | 26.26 ± 1.00 | -23.6 | EmatFaster |
| Q11  | 7.30 ± 0.36 | 4.94 ± 0.17 | -32.4 | EmatFaster |
| Q12  | 20.11 ± 0.81 | 16.08 ± 0.63 | -20.0 | EmatFaster |
| Q13  | 41.81 ± 0.89 | 27.54 ± 0.55 | -34.1 | EmatFaster |
| Q14  | 16.65 ± 0.53 | 11.26 ± 1.00 | -32.4 | EmatFaster |
| Q15  | 22.81 ± 0.67 | 16.24 ± 0.88 | -28.8 | EmatFaster |
| Q16  | 8.74 ± 0.16 | 9.33 ± 0.15 | +6.7 | Regression |
| Q17  | 37.54 ± 1.33 | 34.21 ± 1.20 | -8.9 | EmatFaster |
| Q18  | 52.56 ± 2.94 | 47.18 ± 3.77 | -10.2 | EmatFaster |
| Q19  | 20.80 ± 0.36 | 17.18 ± 0.62 | -17.4 | EmatFaster |
| Q20  | 17.68 ± 1.33 | 15.67 ± 0.66 | -11.4 | EmatFaster |
| Q21  | 44.52 ± 2.53 | 40.26 ± 1.39 | -9.6 | EmatFaster |
| Q22  | 8.36 ± 2.13 | 8.20 ± 0.26 | -1.8 | within ±5% |

**Top-line:** 1 parity, 19 EmatFaster, 2 Regression (paired queries: 22). geomean(emat / fast) = **0.8239** (target ≤ 1.02 per E5.4 acceptance).

## 2. Per-query analysis

Regressions > 5%, ordered by magnitude. Threshold for EXPLAIN ANALYZE deep-dive is > 10%; queries between 5% and 10% are listed for completeness but not individually attributed unless they cluster on a shared root cause.

### Q01 — +11.0% (28.46 → 31.58 ms)

_Deep-dive required (> 10% regression). Likely candidates, ranked by prior data from §3 capability gaps:_

1. **Filter pushdown disabled** when the streaming reader is on (see `EmatixFastParquetTableProvider::supports_filters_pushdown` — returns `Unsupported` for every filter while `streaming_arrow_reader` is true). DataFusion's residual `FilterExec` runs the predicates instead. On selective filters (Q06, Q14, Q19) this materially changes the rows-pushed-into-aggregate count.
_Confirm with `EXPLAIN ANALYZE`: count of rows emerging from the scan node should be equal to the file's total rows on EmatixFastParquet and to the post-filter row count on FastParquet._

2. **Row-group pruning by stats** — `EmatixFastParquetTableProvider::partition_statistics()` returns `Statistics::new_unknown` with only `num_rows` populated (`ematix_fast_parquet.rs:637`). FastParquet reports typed min/max from `ParquetMetaData::row_group().statistics()`, which feeds DataFusion's join-size + agg-cardinality estimates and drives row-group pruning. On stats-sensitive queries this changes the physical plan (smaller join build side, different operator ordering).

3. **Per-column decode cost on specific column types** — primarily Decimal128 (none in TPC-H), Int96, FLBA, nested. TPC-H is all Int32/Int64/Float64/Date32/Utf8; if a regression shows here it's in the Utf8 → Utf8View streaming path (Σ.E5.1.d). Cross-check with the codec-layer `bench_decode` in ematix-parquet.

4. **Different operator selection by the planner** — if `partition_statistics` differences flip a join from hash to nested loop or vice versa, this is the symptom. EXPLAIN-diff the two plans.

### Q16 — +6.7% (8.74 → 9.33 ms)

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

**Close gaps first** — 2 query/queries regressed by more than 5% (geomean = 0.8239, target ≤ 1.02). Recommended ordered sub-phases:

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

