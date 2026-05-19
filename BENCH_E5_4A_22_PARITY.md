# Σ.E5.4.a — 22-query parity bench

Same-process bench comparing `FastParquetTableProvider` (parquet-rs) vs `EmatixFastParquetTableProvider` (ematix-parquet streaming reader, default since PR #115) across all 22 TPC-H queries on SF=1 parquet.

Source: `crates/ematix-flow-core/examples/sigma_e5_4a_22_parity_bench.rs`.

Data: `examples/tpch/data/sf1`.

Methodology: median ± σ across 10 timed trials after 3 warmups, single-machine, 14 target partitions, mimalloc allocator. Both providers register the same 8 TPC-H tables with the same rule chain (`InjectFilterMultiAggRule` + `InjectFilterSumRule` + `EnableDictGroupCountRule`). Delta = `(emat - fast) / fast × 100`. Verdict bands: within ±5% = `parity`; emat ≥ 5% faster = `EmatFaster`; emat ≥ 5% slower = `Regression`.

## 1. Bench numbers

| Query | FastParquet (ms) | EmatixFastParquet (ms) | Δ% (emat vs fast) | Verdict |
|------:|-----------------:|-----------------------:|------------------:|:--------|
| Q01  | 28.72 ± 1.43 | 31.39 ± 0.75 | +9.3 | Regression |
| Q02  | 9.63 ± 0.12 | 8.03 ± 2.65 | -16.6 | EmatFaster |
| Q03  | 19.46 ± 0.24 | 12.68 ± 0.60 | -34.8 | EmatFaster |
| Q04  | 15.58 ± 0.30 | 12.81 ± 2.81 | -17.8 | EmatFaster |
| Q05  | 26.47 ± 4.43 | 25.62 ± 7.49 | -3.2 | within ±5% |
| Q06  | 11.23 ± 0.34 | 10.80 ± 1.10 | -3.8 | within ±5% |
| Q07  | 30.64 ± 1.91 | 28.64 ± 8.56 | -6.5 | EmatFaster |
| Q08  | 24.20 ± 1.24 | 18.94 ± 1.55 | -21.7 | EmatFaster |
| Q09  | 28.37 ± 1.34 | 28.24 ± 3.46 | -0.5 | within ±5% |
| Q10  | 34.19 ± 0.76 | 26.45 ± 1.25 | -22.6 | EmatFaster |
| Q11  | 7.44 ± 0.24 | 5.06 ± 1.73 | -31.9 | EmatFaster |
| Q12  | 20.32 ± 0.73 | 15.42 ± 0.40 | -24.1 | EmatFaster |
| Q13  | 41.52 ± 1.31 | 27.51 ± 0.81 | -33.8 | EmatFaster |
| Q14  | 16.54 ± 0.27 | 11.99 ± 0.86 | -27.5 | EmatFaster |
| Q15  | 23.06 ± 1.37 | 15.85 ± 0.80 | -31.3 | EmatFaster |
| Q16  | 8.42 ± 0.17 | 9.30 ± 0.22 | +10.4 | Regression |
| Q17  | 37.17 ± 1.63 | 35.98 ± 1.31 | -3.2 | within ±5% |
| Q18  | 54.36 ± 3.38 | 46.82 ± 6.34 | -13.9 | EmatFaster |
| Q19  | 21.33 ± 2.16 | 16.93 ± 0.51 | -20.6 | EmatFaster |
| Q20  | 17.79 ± 2.61 | 15.74 ± 0.83 | -11.5 | EmatFaster |
| Q21  | 42.99 ± 1.40 | 44.03 ± 3.74 | +2.4 | within ±5% |
| Q22  | 8.26 ± 0.23 | 8.09 ± 0.43 | -2.0 | within ±5% |

**Top-line:** 6 parity, 14 EmatFaster, 2 Regression (paired queries: 22). geomean(emat / fast) = **0.8505** (target ≤ 1.02 per E5.4 acceptance).

## 2. Per-query analysis

Regressions > 5%, ordered by magnitude. Threshold for EXPLAIN ANALYZE deep-dive is > 10%; queries between 5% and 10% are listed for completeness but not individually attributed unless they cluster on a shared root cause.

### Q16 — +10.4% (8.42 → 9.30 ms)

_Deep-dive required (> 10% regression). Likely candidates, ranked by prior data from §3 capability gaps:_

1. **Filter pushdown disabled** when the streaming reader is on (see `EmatixFastParquetTableProvider::supports_filters_pushdown` — returns `Unsupported` for every filter while `streaming_arrow_reader` is true). DataFusion's residual `FilterExec` runs the predicates instead. On selective filters (Q06, Q14, Q19) this materially changes the rows-pushed-into-aggregate count.
_Confirm with `EXPLAIN ANALYZE`: count of rows emerging from the scan node should be equal to the file's total rows on EmatixFastParquet and to the post-filter row count on FastParquet._

2. **Row-group pruning by stats** — `EmatixFastParquetTableProvider::partition_statistics()` returns `Statistics::new_unknown` with only `num_rows` populated (`ematix_fast_parquet.rs:637`). FastParquet reports typed min/max from `ParquetMetaData::row_group().statistics()`, which feeds DataFusion's join-size + agg-cardinality estimates and drives row-group pruning. On stats-sensitive queries this changes the physical plan (smaller join build side, different operator ordering).

3. **Per-column decode cost on specific column types** — primarily Decimal128 (none in TPC-H), Int96, FLBA, nested. TPC-H is all Int32/Int64/Float64/Date32/Utf8; if a regression shows here it's in the Utf8 → Utf8View streaming path (Σ.E5.1.d). Cross-check with the codec-layer `bench_decode` in ematix-parquet.

4. **Different operator selection by the planner** — if `partition_statistics` differences flip a join from hash to nested loop or vice versa, this is the symptom. EXPLAIN-diff the two plans.

### Q01 — +9.3% (28.72 → 31.39 ms)

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

**Close gaps first** — 2 query/queries regressed by more than 5% (geomean = 0.8505, target ≤ 1.02). Recommended ordered sub-phases:

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

