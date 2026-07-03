# `EMAT_*` Environment Flag Inventory

_Generated 2026-06-21._

This is a generated inventory of every `EMAT_*` environment variable in the
`ematix-flow` Rust workspace. Today these flags are read **inline at ~40
call sites** scattered across `crates/ematix-flow-core/src/` (plus the bench
harness in `examples/`), using **three inconsistent idioms** that you cannot
distinguish from the flag name alone:

- `enabled("FLAG")` / `var(...).map(|v| v != "0")...unwrap_or(true)` → **default-ON** (opt-out with `=0`)
- `var(...).as_deref() == Ok("1")` / `var(...).is_ok()` / `...unwrap_or(false)` → **opt-in** (default-off)
- `var(...).parse()...unwrap_or(N)` → **numeric tunable** with default `N`

A typed `flags.rs` module that centralizes parsing and makes default-state
explicit is the planned replacement. Until then, **the source of truth is
still the code** — regenerate this doc after flag changes.

The canonical helper is `fn enabled(var)` in
[`crates/ematix-flow-core/src/flow_query_planner.rs:49`](../crates/ematix-flow-core/src/flow_query_planner.rs)
— it returns `true` unless the value is `0`/`false`.

## How to read default-state

| Idiom in code | Default | How to flip |
| --- | --- | --- |
| `enabled("FLAG")` or `.map(\|v\| v != "0"...).unwrap_or(true)` or `!= Ok("0")` | **ON** | set `FLAG=0` (or `false`) to disable |
| `== Ok("1")` / `== Ok("true")` / `.map(\|v\| v == "1"...).unwrap_or(false)` | **off** | set `FLAG=1` to enable |
| `.is_ok()` / `.is_some()` | **off** | set `FLAG=<anything>` (presence-activated) |
| `.parse()...unwrap_or(N)` | **`N`** | set `FLAG=<number>` to override |
| `scale_gated_large("FLAG")` | **AUTO** (tri-state) | `=1` force ON, `=0` force OFF, unset = ON only for SF≥100-class datasets (any table ≥ 300M rows, `scale_class`); any other value = AUTO |

> Note: a handful of flags documented in MEMORY.md as "default-on" (e.g.
> `EMAT_PLAN_CACHE`, `EMAT_L9_CASCADE`, `EMAT_REORDER`) are **only read in the
> bench harness**, not in the production `src/` path. In production those
> behaviors are wired unconditionally (plan cache) or via a different rule.
> They are listed under **Bench-harness only** with a note; see the Anchors
> section in the task report for the discrepancy.

---

## Production gate (default-ON)

These are read in `src/` and default to enabled. Disable with `=0`.

| Flag | Default | Value/Notes | Owner file | Purpose |
| --- | --- | --- | --- | --- |
| `EMAT_AGG_SEMI` | ON (set =0 to disable) | `enabled()` | [flow_query_planner.rs:68](../crates/ematix-flow-core/src/flow_query_planner.rs) | Agg-side filter/semi pushdown walker (`push_filter_into_agg`); Q17/Q08/Q18. |
| `EMAT_COLLECT_LEFT_SEMI_BROADCAST` | ON (set =0 to disable) | `!= Ok("0")` | [force_collect_left_semi_build_rule.rs:377](../crates/ematix-flow-core/src/force_collect_left_semi_build_rule.rs) | REV.17.4b: broadcast (no-swap CollectLeft) for semi/anti joins. |
| `EMAT_CSE_FILTER_FUSION` | ON (set =0 to disable) | `.unwrap_or(true)` | [dedupe_aggregate_rule.rs:311](../crates/ematix-flow-core/src/dedupe_aggregate_rule.rs) | CSE filter-fusion across deduped aggregate subtrees. |
| `EMAT_CSE_PARALLEL` | ON (set =0 to disable) | `.unwrap_or(true)` | [shared_subtree_exec.rs:296](../crates/ematix-flow-core/src/shared_subtree_exec.rs) | Concurrent (vs serial) drain of SharedSubtree CSE consumers; Q15 −13%. |
| `EMAT_DIM_PUSH` | ON (set =0 to disable) | `enabled()` | [flow_query_planner.rs:73](../crates/ematix-flow-core/src/flow_query_planner.rs) | Dim-join pushdown into the fact join chain (`push_dim_join_into_chain`); Q10. |
| `EMAT_L9_FUSED_PROBE` | ON (set =0 to disable) | `.unwrap_or(true)` | [emat_arrow_reader.rs:1126](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | L9 fused membership-probe arm in the scan (set/bloom + static preds). |
| `EMAT_L9_REQUIRE_FILTERED_BUILD` | ON (set =0 to disable) | `.unwrap_or(true)` | [runtime_bloom_sideband_rule.rs:142](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs) | L9.SelectiveBuild: only emit a runtime bloom when the build side is pre-filtered. |
| `EMAT_L9_TIGHT_CARDINALITY` | ON (set =0 to disable) | `!= Ok("0")` | [runtime_bloom_sideband_rule.rs:806](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs) | L9 deep-semi cap / tight-cardinality rescue (right-sizes the probe wrap). |
| `EMAT_LATE_MAT_AGG` | ON (set =0 to disable) | `enabled()` | [late_mat_agg.rs:109](../crates/ematix-flow-core/src/late_mat_agg.rs) | Wide-string late-materialization: recognizes an FD-reducible wide aggregate over a PK-anchored star (Q10), carries the group cols as a u32 build-rowid through the join+agg, gathers them at the outputs. Fires Q10-only (shape gate); Q10 SF=100 −24% in-sweep / −31% isolated (beats DuckDB). Inert without declared PKs. |
| `EMAT_NO_PROVIDER_CACHE` | ON (set =0 to disable) | inverted: cache on unless `=1` | [ematix_fast_parquet.rs:1991](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | **Inverted name:** provider-metadata cache is ON; `=1` DISABLES it. |
| `EMAT_Q20_SEMI` | ON (set =0 to disable) | `enabled()` | [flow_query_planner.rs:87](../crates/ematix-flow-core/src/flow_query_planner.rs) | Q20 transitive semi-pushdown into correlated agg (`push_transitive_semi_into_agg`). |
| `EMAT_RANGE_AGG` | ON (set =0 to disable) | `.unwrap_or(true)` | [clustered_agg_rule.rs:70](../crates/ematix-flow-core/src/clustered_agg_rule.rs) | RANGE.AGG: key-disjoint partition chunks from clustered RG `[min,max]`. |
| `EMAT_REORDER_QP` | ON (set =0 to disable) | `enabled()` | [flow_query_planner.rs:113](../crates/ematix-flow-core/src/flow_query_planner.rs) | Shape-gated inner-join reorder in the QueryPlanner (`reorder_inner_joins_shape_gated`); Q05. |
| `EMAT_REORDER_STATS_AWARE_GATE` | ON (set =0 to disable) | `.unwrap_or(true)` | [join_reorder.rs:656](../crates/ematix-flow-core/src/join_reorder.rs) | #316: scale-bump fires only when a leaf has a real (known) cardinality. |
| `EMAT_RG_DECODE_CACHE` | ON (set =0 to disable) | `.unwrap_or(true)` | [emat_arrow_reader.rs:232](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | Process-wide row-group decode cache (capacity via `EMAT_RG_DECODE_CACHE_BYTES`). |
| `EMAT_RH_AVG_F64_VEC` | ON (set =0 to disable) | `.map(\|s\| s != "0").unwrap_or(true)` | [robin_hood_avg_f64_exec.rs:286](../crates/ematix-flow-core/src/robin_hood_avg_f64_exec.rs) | Use the vectorized accumulator path in the RobinHood AVG(f64) exec. |
| `EMAT_RH_SUM_F64_VEC` | ON (set =0 to disable) | `.map(\|s\| s != "0").unwrap_or(true)` | [robin_hood_sum_f64_exec.rs:252](../crates/ematix-flow-core/src/robin_hood_sum_f64_exec.rs) | Use the vectorized accumulator path in the RobinHood SUM(f64) exec. |
| `EMAT_SCALAR_AGG_BOOST` | ON (set =0 to disable) | `.unwrap_or(true)` | [auto_target_partitions.rs:135](../crates/ematix-flow-core/src/auto_target_partitions.rs) | Scalar-aggregation partition oversubscription boost. |
| `EMAT_TRANSITIVE_DIM_SEMI` | ON (set =0 to disable) | `enabled()` | [flow_query_planner.rs:106](../crates/ematix-flow-core/src/flow_query_planner.rs) | Σ.Q05/#352: transitive dim-semi splice into deep join inputs; the 22nd SF=10 win. |

## Scale-gated levers (tri-state, Σ.AI.5)

The 2026-07-01 campaign levers (`bench-results/campaign-2026-07-01/REPORT.md`
§2/§4). Read via `flags::scale_gated_large()`: **explicit value wins in both
directions** (`=1` force ON, `=0` force OFF); **unset = AUTO** — ON only when
the process has observed an SF≥100-class dataset (any table footer ≥ 300M
rows — SF=100 lineitem is 600M, SF=10 is 60M; sibling-`*.parquet` scan at
provider construction makes this registration-order independent, see
`crate::scale_class`). Threshold override: `EMAT_LARGE_SCALE_MIN_ROWS`.
Unrecognized values (e.g. `=yes`) mean AUTO, not ON.

| Flag | AUTO behavior | Owner file | Campaign evidence / purpose |
| --- | --- | --- | --- |
| `EMAT_DOWNCAST_KEYS` | ON at SF≥100 | [ematix_fast_parquet.rs (`key_downcast_enabled`)](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | KEYS.2 narrow i64 join/group keys to advertised Int32 (stats-proven). SF=100: Q09 **−1075 ms** (flips the loss — the DRAM-spill build goes cache-resident). SF=10: net **+10%**, 11 clear regressions → must stay off below scale. **Semantics change (2026-07-02):** was presence-activated; `=0` now means OFF. |
| `EMAT_NARROW_KEY_DECODE` | ON at SF≥100 | [ematix_fast_parquet.rs (execute)](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | NARROW.DEC: decode narrowed keys directly at Int32. Gated together with `EMAT_DOWNCAST_KEYS` (only meaningful when the downcast advertised Int32). See `docs/NARROW_KEY_DECODE.md`. |
| `EMAT_DATE_BUILD_SIDE` | ON at SF≥100 (resolved at rule-apply time) | [force_collect_left_semi_build_rule.rs](../crates/ematix-flow-core/src/force_collect_left_semi_build_rule.rs) | Σ.AH.3 date-range corrected build-side swap (DF 53 interval analysis has no Date32 → flat 20% selectivity inverts build sides). With the NDV swap: Q10 SF=100 **−947 ms** (flips the loss); neutral at SF=10. `Default::default()` snapshots the env tri-state; AUTO resolves per-apply because the rule is constructed before table registration. |
| `EMAT_NDV_BUILD_SIDE` | ON at SF≥100 | [force_collect_left_semi_build_rule.rs](../crates/ematix-flow-core/src/force_collect_left_semi_build_rule.rs) + [ematix_fast_parquet.rs (dict-distinct walk cap)](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | True build-cardinality recovery via NDV; also widens the dict-distinct walk cap to 10M at provider construction. |
| `EMAT_FD_GROUPBY` | ON at SF≥100 | [fd_groupby_simplify.rs](../crates/ematix-flow-core/src/fd_groupby_simplify.rs) | Σ.AH.5 FD GROUP BY simplifier. Q10 SF=100 **−99 ms**, no regressions anywhere — gated consistently with its sibling levers. Still needs declared PKs (plan-inert without a catalog PK). |

**Deliberately NOT scale-gated:** `EMAT_L9_PARTITIONED` stays plain opt-in
(below) — the campaign showed it net-negative outside Q09 (Q08 SF=100
+62 ms solo) and its ALL-ON composition hazard with narrow keys (Q08
+1461 ms) was root-caused to the fused bloom-probe path refusing
`I64InBloom`/`I64InSet` on Int32-narrow-decoded keys and bailing to the
legacy per-predicate re-decode — fixed in `BridgeFilter::eval_on_decoded_views`
(widened i32 binding, 2026-07-02). Re-A/B before promoting it.

## Production gate (opt-in)

Read in `src/`, default-OFF. Enable with `=1` (or, where noted, presence).

| Flag | Default | Value/Notes | Owner file | Purpose |
| --- | --- | --- | --- | --- |
| `EMAT_AGG_PARTITION_BOOST` | off (set =1) | `== "1"/"true"`, else off | [agg_partition_boost.rs:110](../crates/ematix-flow-core/src/agg_partition_boost.rs) | Oversubscribe partitions under a FinalPartitioned agg layer. |
| `EMAT_COMBINE_AGG` | off (set =1) | `== Ok("1"/"true"/"TRUE")` | [combine_agg_exec.rs:404](../crates/ematix-flow-core/src/combine_agg_exec.rs) | Swap `Partial→Repartition→Final` for `CombineAggExec` (single-i64-key SUM(f64)). |
| `EMAT_DICT_DISTINCT` | off (set =1) | `== "1"/"true"`, else off | [ematix_fast_parquet.rs:2158](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Header-only dict-distinct (NDV) walk during scan planning. |
| `EMAT_DISABLE_FILTER_MULTI_AGG` | off (set to disable) | `.is_some()` | [fused_aggregate_filter_multi_agg_rule.rs:176](../crates/ematix-flow-core/src/fused_aggregate_filter_multi_agg_rule.rs) | **Kill-switch:** presence disables the fused filter→multi-agg rule. |
| `EMAT_DROP_REDUNDANT_FILTER` | off (set to enable) | `.is_some()` | [drop_redundant_filter_rule.rs:135](../crates/ematix-flow-core/src/drop_redundant_filter_rule.rs) | Drop a filter made redundant by a fused scan predicate. |
| `EMAT_EXACT_PUSHDOWN` | off (set to enable) | `.is_some()` | [ematix_fast_parquet.rs:2588](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Σ.AE: exact (vs conservative) predicate pushdown into the scan. |
| `EMAT_FILTER_MULTI_AGG_USE_REPARTITION` | off (set to enable) | `.is_some()` | [fused_aggregate_filter_multi_agg_rule.rs:371](../crates/ematix-flow-core/src/fused_aggregate_filter_multi_agg_rule.rs) | A/B: use RepartitionExec-based fanout for the fused filter→multi-agg. |
| `EMAT_FORCE_PARALLEL_BITMAP` | off (set to enable) | `.is_some()` | [emat_arrow_reader.rs:1225](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | Force parallel bitmap dispatch (L13: default-off — was 43× regression on lineitem+date). |
| `EMAT_INLINE_STREAMING` | off (set =1) | `== "1"/"true"` | [ematix_fast_parquet.rs:3662](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Force the inline (eager) reader path (overrides the auto inline/page dispatch). |
| `EMAT_HASH_JOIN` | off (set =1) | `== "1"/"true"`, else off | [swap_emat_hash_join_rule.rs:297](../crates/ematix-flow-core/src/swap_emat_hash_join_rule.rs) | Swap DF HashJoin for `EmatixHashJoinExec` (dormant unless enabled). |
| `EMAT_HJ_PARTITIONED` | off (set =1) | `== "1"/"true"`, else off | [swap_emat_hash_join_rule.rs:56](../crates/ematix-flow-core/src/swap_emat_hash_join_rule.rs) | No-shuffle partitioned EmatixHashJoin (drops the 2 RepartitionExecs). |
| `EMAT_HJ_RADIX` | off (set =1) | `== "1"/"true"`, else off | [emat_hash_join.rs:61](../crates/ematix-flow-core/src/emat_hash_join.rs) | RADIX.2 spike: radix-partitioned morsel build (also needs `EMAT_HASH_JOIN=1`). |
| `EMAT_HJ_TAG` | off (set =1) | `== "1"/"true"`, else off | [emat_hash_join.rs:51](../crates/ematix-flow-core/src/emat_hash_join.rs) | HJ.4 SIMD-tag probe kernel (also needs `EMAT_HASH_JOIN=1`); Q08 −18%. |
| `EMAT_L9_BROADCAST_SIBLINGS` | off (set to enable) | `.is_some()` | [broadcast_sibling_blooms_rule.rs:89](../crates/ematix-flow-core/src/broadcast_sibling_blooms_rule.rs) | Broadcast a build bloom to sibling scans sharing the key. |
| `EMAT_L9_EMIT_RANGE` | off (set =1) | `== "1"/"true"`, else off | [build_side_bloom_emitter_exec.rs:795](../crates/ematix-flow-core/src/build_side_bloom_emitter_exec.rs) | Σ.S.B: also emit a `[min,max]` range alongside the bloom. |
| `EMAT_L9_INNER_WITH_SEMI` | off (set =1) | `== "1"/"true"`, else off | [runtime_bloom_sideband_rule.rs:219](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs) | Σ.AM.1: allow inner-HJ bloom emit when build subtree has a semi-filter. |
| `EMAT_L9_PARTITIONED` | off (set =1) | `opt_in()` | [runtime_bloom_sideband_rule.rs](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs) | Σ.AH.2: Partitioned-scale L9 — raises the tight-NDV file ceiling 10M → 32M rows so SF=100-class dimensions (part 20M) stay NDV-correctable and the Q08 SF=100 part_filt→lineitem edge WRAPs. Explicit `EMAT_L9_NDV_MAX_ROWS` wins. DELIBERATELY not scale-gated (see the scale-gated section): campaign net-negative outside Q09; its Q08×narrow-keys composition hazard is fixed (fused-probe i32 widening) but it needs a fresh strict A/B before promotion. |
| `EMAT_LOWCARD_GROUPBY_BOOST` | off (set =1) | `== "1"/"true"`, else off | [auto_target_partitions.rs:160](../crates/ematix-flow-core/src/auto_target_partitions.rs) | Gate-B: low-card GROUP BY partition oversubscription (#158 demoted to opt-in). |
| `EMAT_NO_FILTER_PUSHDOWN` | off (set to disable) | `.is_some()` | [ematix_fast_parquet.rs:2559](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | **Kill-switch:** presence disables filter pushdown into the scan. |
| `EMAT_NO_PARQUET_FILE_CACHE` | off (set to disable) | `.is_ok()` | [ematix_parquet_bridge.rs:112](../crates/ematix-flow-core/src/ematix_parquet_bridge.rs) | **Kill-switch:** presence disables the parquet file-handle cache. |
| `EMAT_NO_STRIP_FUSED_SCAN_FILTER` | off (set to disable) | `.is_some()` | [fused_aggregate_filter_sum_rule.rs:258](../crates/ematix-flow-core/src/fused_aggregate_filter_sum_rule.rs) | **Kill-switch:** presence keeps the redundant BridgeFilter (Q06 masked-pushdown strip off). |
| `EMAT_PAGE_STREAMING` | off (set =1) | `== "1"/"true"`, else off | [ematix_fast_parquet.rs:3665](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Force the page-streaming reader path (overrides the auto inline/page dispatch). |
| `EMAT_PUSH_PIPELINE` | off (set =1) | `== "1"/"true"`, else off | [fuse_push_pipeline_rule.rs:52](../crates/ematix-flow-core/src/fuse_push_pipeline_rule.rs) | PV4 push-pipeline fusion (build→probe push exec). |
| `EMAT_PV4_OVERLAP` | off (set =1) | `== "1"/"true"`, else off | [emat_push_pipeline_exec.rs:77](../crates/ematix-flow-core/src/emat_push_pipeline_exec.rs) | PV4 build/probe overlap (buffer depth via `EMAT_PV4_BUFFER`). |
| `EMAT_RT_BLOOM_INNER_JOIN` | off (set to enable) | `.is_some()` | [runtime_bloom_sideband_rule.rs:139](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs) | Σ.Q.L14: allow runtime-bloom sideband on Inner joins (default-off). |
| `EMAT_SKIP_DICT_DISTINCT` | off (set =1) | `== "1"/"true"`, else off | [ematix_fast_parquet.rs:2192](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Legacy A/B: force-skip the dict-distinct walk (default behavior is already skip). |

## Numeric tunable

Read in `src/` via `.parse()...unwrap_or(N)`. Default value is in the **Default** column.

| Flag | Default | Value/Notes | Owner file | Purpose |
| --- | --- | --- | --- | --- |
| `EMAT_BATCH_SIZE` | `65_536` | `DEFAULT_BATCH_SIZE` | [emat_arrow_reader.rs:288](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | Reader output batch size (rows). |
| `EMAT_COLLECT_LEFT_BROADCAST_RATIO` | `16.0` | f64 | [force_collect_left_semi_build_rule.rs:150](../crates/ematix-flow-core/src/force_collect_left_semi_build_rule.rs) | Probe/build ratio break-even for CollectLeft broadcast (`=0` disables). |
| `EMAT_COLLECT_LEFT_MIN_RATIO` | `0.0` | f64 (0 = disabled) | [force_collect_left_semi_build_rule.rs:142](../crates/ematix-flow-core/src/force_collect_left_semi_build_rule.rs) | REV.17.1 cardinality-ratio guard (min probe/build). |
| `EMAT_DATE_BUILD_SIDE_RATIO` | `2.0` | f64 (clamped ≥ 1.0) | [force_collect_left_semi_build_rule.rs:174](../crates/ematix-flow-core/src/force_collect_left_semi_build_rule.rs) | Σ.AH.3 swap margin: only swap when corrected build ≥ ratio × corrected probe (flap guard). |
| `EMAT_COMBINE_AGG_HINT` | `131_072` | `1<<17` | [combine_agg_exec.rs:413](../crates/ematix-flow-core/src/combine_agg_exec.rs) | Per-partition group-table pre-size hint for CombineAggExec. |
| `EMAT_DECODE_PARALLEL_THRESHOLD` | `4` | pages | [emat_arrow_reader.rs:2556](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | Min pending pages before page-decode goes parallel. |
| `EMAT_LATE_MAT_BATCH` | `1_048_576` | `usize_or` (`=0` disables) | [flow_query_planner.rs](../crates/ematix-flow-core/src/flow_query_planner.rs) | Query-scoped execution batch size the late-mat plan runs under (`BatchSizeOverrideExec`); few large batches make the Utf8View reattach a near-free buffer-sharing gather. |
| `EMAT_LM_OVERLAP` | `1` (ON) | `usize_or != 0` | [late_mat_agg_planner.rs](../crates/ematix-flow-core/src/late_mat_agg_planner.rs) | Bake build/probe overlap into the late-mat join (hides the serial build behind the probe decode); `=0` opts out. |
| `EMAT_LM_MIN_WIDE_COLS` | `3` | `usize_or` | [late_mat_agg.rs](../crates/ematix-flow-core/src/late_mat_agg.rs) | Late-mat shape gate: minimum string-typed group columns to fire (excludes narrow-string aggregates like Q18). |
| `EMAT_DICT_DISTINCT_MAX_ROWS` | `0` (10M if `EMAT_NDV_BUILD_SIDE=1`) | `0` = no walk | [ematix_fast_parquet.rs:2185](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Max table rows eligible for the small-table dict-distinct walk. |
| `EMAT_FILTER_MULTI_AGG_FANOUT` | `(target_partitions * 13/10)` (18 in the repartition path) | usize | [fused_aggregate_exec.rs:167](../crates/ematix-flow-core/src/fused_aggregate_exec.rs) | Worker fan-out for the fused filter→multi-agg exec (and rule.rs:372 repartition path). |
| `EMAT_HJ_MIN_PROBE` | `12_000_000` | rows | [swap_emat_hash_join_rule.rs:145](../crates/ematix-flow-core/src/swap_emat_hash_join_rule.rs) | Min probe-side rows for the EmatixHashJoin swap to fire. |
| `EMAT_HJ_RATIO` | `0` | 0 = off | [swap_emat_hash_join_rule.rs:149](../crates/ematix-flow-core/src/swap_emat_hash_join_rule.rs) | Optional extra build-ratio constraint for the HJ swap (A/B). |
| `EMAT_INLINE_ROW_THRESHOLD` | `derive_inline_row_threshold(n_cols)` (~900k) | 0 = disable | [ematix_fast_parquet.rs:3694](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Row threshold below which small single-RG files use the page-streaming reader. |
| `EMAT_L9_CASCADE_MAX` | `4` | extras/emitter | [runtime_bloom_cascading_rule.rs:87](../crates/ematix-flow-core/src/runtime_bloom_cascading_rule.rs) | Max extra cascaded bloom targets per emitter. |
| `EMAT_L9_CASCADE` | AUTO | `tri_state` | [runtime_bloom_cascade_chain.rs](../crates/ematix-flow-core/src/runtime_bloom_cascade_chain.rs) | Σ.Q05.CHAIN: cascade-CHAIN second phase of the L9 rule (filtered dim → … → large fact). `=1` force on (thresholds relaxed), `=0` off, unset = AUTO (chain start filtered, every build CollectLeft ≤ 4M rows, terminal scan ≥ 20M rows, ≥ 2 links). |
| `EMAT_MULTIKEY_BLOOM` | AUTO | `tri_state` | [runtime_bloom_cascade_chain.rs](../crates/ematix-flow-core/src/runtime_bloom_cascade_chain.rs) | Σ.Q05.CHAIN: allow a chain link to emit a single-key bloom from a MULTI-key equi-join (superset pre-filter; the join still enforces all keys — Q05's 2-key supplier join). `=0` refuses multi-key links; `=1`/unset allow them inside an admitted chain. |
| `EMAT_L9_CASCADE_MIN_TERMINAL_ROWS` | `20_000_000` (AUTO) / `0` (forced) | `usize` | [runtime_bloom_cascade_chain.rs](../crates/ematix-flow-core/src/runtime_bloom_cascade_chain.rs) | Σ.Q05.CHAIN: terminal-scan row floor — the chain must end at a scan at least this large. Explicit value wins over both mode defaults. |
| `EMAT_L9_CASCADE_MAX_BUILD_ROWS` | `4_000_000` (AUTO) / unbounded (forced) | `usize` | [runtime_bloom_cascade_chain.rs](../crates/ematix-flow-core/src/runtime_bloom_cascade_chain.rs) | Σ.Q05.CHAIN: per-link ceiling on the emitting join's build-row estimate. |
| `EMAT_L9_CASCADE_RT_SEL` | `0.5` | `f64`, 0 = off | [runtime_bloom_cascade_chain.rs](../crates/ematix-flow-core/src/runtime_bloom_cascade_chain.rs) | Σ.Q05.CHAIN: terminal-link runtime build-selectivity disarm — publish EMPTY when the chain kept more than this fraction of the terminal build's raw scan (the bloom would not prune enough to pay). |
| `EMAT_L9_CASCADE_MAX_LINKS` | `4` | `usize` | [runtime_bloom_cascade_chain.rs](../crates/ematix-flow-core/src/runtime_bloom_cascade_chain.rs) | Σ.Q05.CHAIN: cap on links per chain. |
| `EMAT_L9_CASCADE_TERMINAL_APPLY` | AUTO (= on only under `EMAT_L9_CASCADE=1`) | `tri_state` | [runtime_bloom_cascade_chain.rs](../crates/ematix-flow-core/src/runtime_bloom_cascade_chain.rs) | Σ.Q05.CHAIN: admit a BARE terminal (fact scan with no existing wrap) and force-apply its bitmap past the REV.23 dense-route discard. AUTO installs only COMPOSED terminals (extra sideband AND-ed with an existing sub-threshold wrap) — a bare ~20%-pass terminal costs the 60M scan its no-filter fast path (+35 ms wall measured) for a discarded bitmap. |
| `EMAT_L9_MAX_EXPECTED_KEYS` | `0` | 0 = unbounded | [runtime_bloom_sideband_rule.rs:155](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs) | Σ.AH.3 Story-2a per-partition build-size ceiling (opt-in via >0). |
| `EMAT_L9_MIN_PROBE_PROJ_COLS` | `0` | 0 = no min | [runtime_bloom_sideband_rule.rs:160](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs) | L9.WIDTH: min projected cols on the probe scan to justify the bloom. |
| `EMAT_L9_NDV_MAX_ROWS` | `10_000_000` (`32_000_000` under `EMAT_L9_PARTITIONED=1`) | 0 = skip | [runtime_bloom_sideband_rule.rs](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs) | Max file rows for the L9 NDV probe; explicit value wins over both defaults (Σ.AH.2). |
| `EMAT_L9_PEEK_TIMEOUT_MS` | `200` | ms | [ematix_fast_parquet.rs:3157](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Sideband wait-for-publish timeout before proceeding un-bloomed. |
| `EMAT_L9_SET_DROP_CAP` | `4_194_304` | clamped ≥ threshold | [build_side_bloom_emitter_exec.rs:103](../crates/ematix-flow-core/src/build_side_bloom_emitter_exec.rs) | L9.BLOOMSIZE.2: per-partition exact-set MEMORY drop cap. |
| `EMAT_L9_SET_THRESHOLD` | `262_144` | rows | [build_side_bloom_emitter_exec.rs:78](../crates/ematix-flow-core/src/build_side_bloom_emitter_exec.rs) | L9 exact-set PUBLISH threshold (set vs bloom). |
| `EMAT_LARGE_SCALE_MIN_ROWS` | `300_000_000` | `usize_or`, read at classification time | [scale_class.rs](../crates/ematix-flow-core/src/scale_class.rs) | Σ.AI.5: AUTO threshold for the scale-gated levers — a dataset is SF≥100-class when any table footer has ≥ this many rows (SF=100 lineitem 600M / SF=10 60M, 2× margin each side). Tests lower it to exercise the auto-ON arms against small fixtures. |
| `EMAT_LARGE_PARTITION_ROWS` | `2_000_000` | rows | [ematix_fast_parquet.rs:3763](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Threshold above which a partition is treated as "large" for reader routing. |
| `EMAT_MASKED_DENSE_PASSRATE` | `0.10` | f64 in [0,1] | [emat_arrow_reader.rs:1889](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | Pass-rate above which a masked filter decodes densely. |
| `EMAT_MI_COLLECT_MIN_RSS_MB` | `max(6144, RAM/4)` | MB | [heap_pressure.rs:111](../crates/ematix-flow-core/src/heap_pressure.rs) | MI.GATE auto-mode RSS threshold for `mi_collect`. |
| `EMAT_PARQUET_FILE_CACHE_MIN_RG` | `128` | row-groups | [ematix_parquet_bridge.rs:103](../crates/ematix-flow-core/src/ematix_parquet_bridge.rs) | Min row-groups for a file to enter the parquet file cache. |
| `EMAT_PV4_BUFFER` | `64` | depth (min 1) | [emat_push_pipeline_exec.rs:79](../crates/ematix-flow-core/src/emat_push_pipeline_exec.rs) | PV4 overlap buffer depth (only when `EMAT_PV4_OVERLAP=1`). |
| `EMAT_RANGE_AGG_MAX_SKEW` | `1.25` | f64 (≥1.0) | [clustered_agg_rule.rs:163](../crates/ematix-flow-core/src/clustered_agg_rule.rs) | Max chunk-size skew tolerated for RANGE.AGG to accept a clustering. |
| `EMAT_READER_PARALLELISM_BUDGET` | `total_threads / outer_partitions` | min 1 | [ematix_fast_parquet.rs:3422](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Per-reader decode parallelism budget. |
| `EMAT_REORDER_BUMP_LEAVES` | `6` | leaves | [join_reorder.rs:194](../crates/ematix-flow-core/src/join_reorder.rs) | Leaf-count side of the reorder scale-bump (Q05.SF10). |
| `EMAT_REORDER_BUMP_MIN_ROWS` | `100_000_000` | rows | [join_reorder.rs:193](../crates/ematix-flow-core/src/join_reorder.rs) | Min-rows side of the reorder scale-bump. |
| `EMAT_RG_DECODE_CACHE_BYTES` | `1_073_741_824` (1 GiB) | bytes | [emat_arrow_reader.rs:237](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | Capacity of the row-group decode cache (when enabled). |
| `EMAT_RH_AVG_F64_INIT_CAP` | (parsed; falls through to clamp) | usize | [robin_hood_avg_f64_exec.rs:292](../crates/ematix-flow-core/src/robin_hood_avg_f64_exec.rs) | Initial table capacity for RobinHood AVG(f64) accumulator. |
| `EMAT_RH_AVG_F64_MAX_GROUPS` | `262_144` | `256*1024` | [robin_hood_avg_f64_exec.rs:450](../crates/ematix-flow-core/src/robin_hood_avg_f64_exec.rs) | Upper group-count gate for enabling the RH AVG(f64) rule. |
| `EMAT_RH_AVG_F64_MIN_GROUPS` | `131_072` | `128*1024` | [robin_hood_avg_f64_exec.rs:454](../crates/ematix-flow-core/src/robin_hood_avg_f64_exec.rs) | Lower group-count gate for enabling the RH AVG(f64) rule. |
| `EMAT_RH_COUNT_MAX_GROUPS` | `262_144` | `256*1024` | [robin_hood_agg_rule.rs:107](../crates/ematix-flow-core/src/robin_hood_agg_rule.rs) | Upper group-count gate for the RH COUNT rule. |
| `EMAT_RH_COUNT_MIN_GROUPS` | `65_536` | `64*1024` | [robin_hood_agg_rule.rs:111](../crates/ematix-flow-core/src/robin_hood_agg_rule.rs) | Lower group-count gate for the RH COUNT rule. |
| `EMAT_RH_INIT_CAP` | `65_536` | usize | [robin_hood_agg.rs:2309](../crates/ematix-flow-core/src/robin_hood_agg.rs) | Initial capacity for the RobinHood COUNT agg table. |
| `EMAT_RH_INITIAL_CAP` | `65_536` | min floor (clamp) | [robin_hood_avg_f64_exec.rs:185](../crates/ematix-flow-core/src/robin_hood_avg_f64_exec.rs) | Min init-cap floor for RH SUM/AVG(f64) execs (also robin_hood_sum_f64_exec.rs:154). |
| `EMAT_RH_SUM_F64_INIT_CAP` | `planner_cap` | usize | [robin_hood_sum_f64_exec.rs:258](../crates/ematix-flow-core/src/robin_hood_sum_f64_exec.rs) | Initial table capacity for RobinHood SUM(f64) accumulator. |
| `EMAT_RH_SUM_F64_MAX_GROUPS` | `262_144` | `256*1024` | [robin_hood_sum_f64_exec.rs:377](../crates/ematix-flow-core/src/robin_hood_sum_f64_exec.rs) | Upper group-count gate for the RH SUM(f64) rule. |
| `EMAT_RH_SUM_F64_MIN_GROUPS` | `131_072` | `128*1024` | [robin_hood_sum_f64_exec.rs:381](../crates/ematix-flow-core/src/robin_hood_sum_f64_exec.rs) | Lower group-count gate for the RH SUM(f64) rule. |
| `EMAT_RT_BLOOM_SELECTIVITY` | `64` | ratio | [runtime_bloom_sideband_rule.rs:130](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs) | Probe/build selectivity ratio gate for the runtime-bloom sideband. |
| `EMAT_SCALAR_AGG_MULT` | (none; falls through to shape default) | usize (≥1) | [auto_target_partitions.rs:196](../crates/ematix-flow-core/src/auto_target_partitions.rs) | Forced scalar-agg partition multiplier (overrides the join/no-join default). |
| `EMAT_SPR_MIN_GROUPS` | `1_000_000` | groups | [single_pass_radix_sum_exec.rs:392](../crates/ematix-flow-core/src/single_pass_radix_sum_exec.rs) | Min group count to enable the single-pass radix SUM rule. |
| `EMAT_SPR_RADIX_BITS` | `10` | capped at 12 | [single_pass_radix_sum_exec.rs:119](../crates/ematix-flow-core/src/single_pass_radix_sum_exec.rs) | Radix bit-count (bins = 1<<bits) for the single-pass radix SUM. |

## Diagnostic / trace

Dev-only (TRACE / DEBUG / TIMING / DUMP / EXPLAIN / PROFILE / PLANTIME). All
presence- or `=1`-activated; none affect query results.

| Flag | Default | Value/Notes | Owner file | Purpose |
| --- | --- | --- | --- | --- |
| `EMAT_AGG_PARTITION_BOOST_TRACE` | off (set to enable) | `.is_some()` | [agg_partition_boost.rs:117](../crates/ematix-flow-core/src/agg_partition_boost.rs) | Trace the agg-partition-boost rule. |
| `EMAT_AGG_TIMING` | off (set to enable) | `.is_some()` | [fused_aggregate_exec.rs:174](../crates/ematix-flow-core/src/fused_aggregate_exec.rs) | Print fused-aggregate timing. |
| `EMAT_COLLECT_LEFT_TRACE` | off (set to enable) | `.is_some()` | [force_collect_left_semi_build_rule.rs:362](../crates/ematix-flow-core/src/force_collect_left_semi_build_rule.rs) | Trace the CollectLeft semi build rule. |
| `EMAT_CSE_FILTER_FUSION_TRACE` | off (set to enable) | `.is_some()` | [drop_redundant_filter_rule.rs:209](../crates/ematix-flow-core/src/drop_redundant_filter_rule.rs) | Trace CSE filter fusion. |
| `EMAT_CSE_TRACE` | off (set to enable) | `.is_some()` | [shared_subtree_exec.rs:250](../crates/ematix-flow-core/src/shared_subtree_exec.rs) | Trace SharedSubtree CSE. |
| `EMAT_DECODE_SERIAL` | off (set to enable) | `.is_some()` | [emat_arrow_reader.rs:2414](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | Force serial page-decode (diagnostic / A/B for parallel decode). |
| `EMAT_DECODE_TIMING` | off (set to enable) | `.is_some()` | [emat_arrow_reader.rs:2413](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | Print page-decode timing. |
| `EMAT_F64_DICT_TRACE` | off (set to enable) | `.is_ok()` | [ematix_fast_parquet.rs:860](../crates/ematix-flow-core/src/ematix_fast_parquet.rs) | Trace f64 dictionary decode. |
| `EMAT_HASH_JOIN_TRACE` | off (set to enable) | `.is_some()` | [swap_emat_hash_join_rule.rs:304](../crates/ematix-flow-core/src/swap_emat_hash_join_rule.rs) | Trace the EmatixHashJoin swap rule. |
| `EMAT_L9_TRACE` | off (set to enable) | `.is_some()` | [runtime_bloom_sideband_rule.rs:185](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs) | Trace the L9 sideband / fused-probe path. |
| `EMAT_MI_COLLECT` | auto (RSS-gated) | `0`/`1`/auto | [heap_pressure.rs:125](../crates/ematix-flow-core/src/heap_pressure.rs) | MI.GATE.3: `mi_collect` control — `0` off, `1` on, else RSS-gated. (Operational, not a result-affecting gate; kept here as it tunes allocator behavior.) |
| `EMAT_PUSH_PIPELINE_TRACE` | off (set to enable) | `.is_some()` | [fuse_push_pipeline_rule.rs:199](../crates/ematix-flow-core/src/fuse_push_pipeline_rule.rs) | Trace the push-pipeline fusion rule. |
| `EMAT_RANGE_AGG_TRACE` | off (set to enable) | `.is_some()` | [clustered_agg_rule.rs:211](../crates/ematix-flow-core/src/clustered_agg_rule.rs) | Trace RANGE.AGG chunk planning. |
| `EMAT_REORDER_COST` | off (set to enable) | `.is_ok()` | [join_reorder.rs:1145](../crates/ematix-flow-core/src/join_reorder.rs) | Print join-reorder cost model output. |
| `EMAT_REORDER_DEBUG` | off (set to enable) | `.is_ok()` | [join_reorder.rs:346](../crates/ematix-flow-core/src/join_reorder.rs) | Debug-print join reorder decisions. |
| `EMAT_RH_TIMING` | off (set to enable) | `.is_ok()` | [robin_hood_agg.rs:2286](../crates/ematix-flow-core/src/robin_hood_agg.rs) | Print RobinHood agg timing. |
| `EMAT_SIGMA_AD_DEBUG` | off (set to enable) | `.is_ok()` | [dim_join_pushdown.rs:116](../crates/ematix-flow-core/src/dim_join_pushdown.rs) | Σ.AD dim-join-pushdown debug. |
| `EMAT_SIGMA_Q05_DEBUG` | off (set to enable) | `.is_ok()` | [agg_filter_pushdown.rs:174](../crates/ematix-flow-core/src/agg_filter_pushdown.rs) | Σ.Q05 transitive-dim-semi debug. |
| `EMAT_SIGMA_Q20_DEBUG` | off (set to enable) | `.is_ok()` | [agg_filter_pushdown.rs:126](../crates/ematix-flow-core/src/agg_filter_pushdown.rs) | Σ.Q20 transitive-semi debug. |
| `EMAT_SIGMA_U_DEBUG` | off (set to enable) | `.is_ok()` | [agg_filter_pushdown.rs:86](../crates/ematix-flow-core/src/agg_filter_pushdown.rs) | Σ.U agg-filter-pushdown debug. |
| `EMAT_TIMING` | off (set to enable) | `.is_some()` | [emat_arrow_reader.rs:1428](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | Print reader/scan timing. |

## Bench-harness only

Read **only** in `examples/` (bench / profile / validate harnesses), not in
`src/`. These do not affect the production/preset path. Where a flag mirrors a
production rule, the harness toggles it before constructing rules manually.

| Flag | Default | Value/Notes | Owner file | Purpose |
| --- | --- | --- | --- | --- |
| `EMAT_ALL_TABLES_EMAT` | off | harness | [examples/paired_ab.rs:255](../crates/ematix-flow-core/examples/paired_ab.rs) | Register all TPC-H tables via the ematix provider. |
| `EMAT_AUTO_BATCH_SIZE` | harness | tunable | [examples/tpch_triangulation_bench.rs:1154](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: auto batch-size selection. |
| `EMAT_AUTO_TARGET_PARTITIONS` | harness | tunable | [examples/tpch_triangulation_bench.rs:1146](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: auto target-partition selection. |
| `EMAT_TPCH_PK` | off (or rule-on) | toggle | [examples/tpch_preset_rebench.rs](../crates/ematix-flow-core/examples/tpch_preset_rebench.rs) | Bench: declare the TPC-H primary keys (scaffolding for the late-mat rule; a real catalog uses DDL). Auto-on when the late-mat rule is enabled. |
| `EMAT_BATCH_RACE_PREFILL` | harness | mode | [examples/tpch_triangulation_bench.rs:339](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: batch race-prefill mode. |
| `EMAT_BLOOM_PUSHDOWN` | harness | toggle | [examples/tpch_triangulation_bench.rs:1212](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: enable bloom pushdown rule. |
| `EMAT_CLEAR_REGISTRY` | off | `.is_some()` | [examples/tpch_preset_rebench.rs:262](../crates/ematix-flow-core/examples/tpch_preset_rebench.rs) | Bench: clear table registry between runs. |
| `EMAT_COLLECT_LEFT_THRESHOLD_ROWS` | harness | tunable | [examples/tpch_triangulation_bench.rs:1609](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: CollectLeft row threshold. |
| `EMAT_DICT_PRESERVATION` | harness | toggle | [examples/tpch_preset_rebench.rs:61](../crates/ematix-flow-core/examples/tpch_preset_rebench.rs) | Bench: dictionary-preservation toggle (preset rebench). |
| `EMAT_DISABLE_PUSHDOWN` | off | `.is_some()` | [examples/sigma_q_l13_q07_walltime.rs:63](../crates/ematix-flow-core/examples/sigma_q_l13_q07_walltime.rs) | Bench: disable pushdown. |
| `EMAT_DUMP_LOGICAL` | off | `.is_ok()`/`.is_some()` | [examples/sigma_q_explain_plan.rs:66](../crates/ematix-flow-core/examples/sigma_q_explain_plan.rs) | Diagnostic: dump the logical plan (bench only). |
| `EMAT_DUMP_PLAN` | off | `.is_some()` | [examples/tpch_triangulation_bench.rs:1111](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Diagnostic: dump the physical plan (bench only). |
| `EMAT_E5_TRIALS` | harness | tunable | [examples/sigma_e5_1_arrow_reader_bench.rs:88](../crates/ematix-flow-core/examples/sigma_e5_1_arrow_reader_bench.rs) | Bench: E5 reader-bench trial count. |
| `EMAT_E5_WARMUPS` | harness | tunable | [examples/sigma_e5_1_arrow_reader_bench.rs:95](../crates/ematix-flow-core/examples/sigma_e5_1_arrow_reader_bench.rs) | Bench: E5 reader-bench warmup count. |
| `EMAT_EXPLAIN` | off | `.is_ok()`/mode | [examples/tpch_preset_rebench.rs:350](../crates/ematix-flow-core/examples/tpch_preset_rebench.rs) | Diagnostic: explain mode (bench only). |
| `EMAT_EXPLAIN_LOGICAL` | off | `== Some("1")` | [examples/sigma_q_explain_analyze.rs:117](../crates/ematix-flow-core/examples/sigma_q_explain_analyze.rs) | Diagnostic: explain logical plan (bench only). |
| `EMAT_FORCE_COLLECT_LEFT` | harness | toggle | [examples/tpch_triangulation_bench.rs:1651](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: force the CollectLeft rule. |
| `EMAT_FRESH_CTX` | off | `.is_some()` | [examples/tpch_preset_rebench.rs:299](../crates/ematix-flow-core/examples/tpch_preset_rebench.rs) | Bench: fresh SessionContext per measurement. |
| `EMAT_L9_CASCADE_STEM` | off | `opt_in` | [examples/tpch_validate.rs](../crates/ematix-flow-core/examples/tpch_validate.rs) | Bench: swap in the LEGACY Σ.S.B stem-fanout cascading rule instead of the base L9 rule (was `EMAT_L9_CASCADE` before Σ.Q05.CHAIN repurposed that name). |
| `EMAT_LATE_MAT` | harness | toggle | [examples/tpch_preset_rebench.rs:65](../crates/ematix-flow-core/examples/tpch_preset_rebench.rs) | Bench: late-materialization toggle. |
| `EMAT_PLANTIME` | off | `.is_some()` | [examples/tpch_preset_rebench.rs:320](../crates/ematix-flow-core/examples/tpch_preset_rebench.rs) | Diagnostic: report plan time (bench only). |
| `EMAT_PLAN_CACHE` | off (bench default-ON) | `.unwrap_or(true)` **in bench** | [examples/tpch_triangulation_bench.rs:1228](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Σ.AG plan cache toggle. **Only read in the bench**; production (`preset.rs` → `plan_cache.rs::is_cacheable`) caches unconditionally. See anchors note. |
| `EMAT_PREFILL` | harness | mode | [examples/tpch_triangulation_bench.rs:315](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: prefill mode. |
| `EMAT_PREFILL_DUMP` | off | toggle | [examples/tpch_triangulation_bench.rs:769](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: dump prefill stats. |
| `EMAT_PROBE_COUNT` | off | `.is_ok()` | [examples/collect_left_card_probe.rs:172](../crates/ematix-flow-core/examples/collect_left_card_probe.rs) | Bench: count probe rows. |
| `EMAT_PROFILE_ITERS` | harness | tunable | [examples/sigma_e5_regression_profile.rs:69](../crates/ematix-flow-core/examples/sigma_e5_regression_profile.rs) | Profiler: iteration count. |
| `EMAT_PROFILE_QUERY` | `"q19"` | string | [examples/sigma_e5_regression_profile.rs:64](../crates/ematix-flow-core/examples/sigma_e5_regression_profile.rs) | Profiler: which query to profile. |
| `EMAT_PROFILE_WARMUPS` | harness | tunable | [examples/sigma_e5_regression_profile.rs:65](../crates/ematix-flow-core/examples/sigma_e5_regression_profile.rs) | Profiler: warmup count. |
| `EMAT_PUSH_SEMI` | harness | toggle | [examples/tpch_triangulation_bench.rs:1666](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: Σ.Q.L10 PushDownLeftSemi (EMAT_PUSH_SEMI). |
| `EMAT_PUSH_THREADS` | harness | tunable | [examples/pv1_fused_q08_pipeline.rs:84](../crates/ematix-flow-core/examples/pv1_fused_q08_pipeline.rs) | Bench: push-pipeline thread count. |
| `EMAT_Q20_TRANSITIVE_SEMI` | harness | toggle | [examples/tpch_triangulation_bench.rs:1283](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: Q20 transitive-semi toggle (the production equivalent is `EMAT_Q20_SEMI`). |
| `EMAT_RACE_PREFILL` | harness | mode | [examples/tpch_triangulation_bench.rs:329](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: race-prefill mode. |
| `EMAT_REGISTER_ORDERS_AS_EMAT` | harness | toggle | [examples/tpch_triangulation_bench.rs:1841](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: register `orders` via the ematix provider. |
| `EMAT_REORDER` | harness | toggle | [examples/tpch_triangulation_bench.rs:1232](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: legacy join-reorder toggle (production uses `EMAT_REORDER_QP`). |
| `EMAT_REORDER_MAX_LEAVES` | harness | tunable | [examples/tpch_triangulation_bench.rs:1491](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: max leaves for reorder. |
| `EMAT_REORDER_SHAPE_GATED` | harness | toggle | [examples/tpch_triangulation_bench.rs:1262](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: shape-gated reorder toggle (production = `EMAT_REORDER_QP`). |
| `EMAT_RH_AVG` | off | `== Some("1")` | [examples/tpch_triangulation_bench.rs:1721](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: enable RobinHood AVG rule. |
| `EMAT_RH_COUNT` | off | `== Some("1")` | [examples/tpch_triangulation_bench.rs:1716](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: enable RobinHood COUNT rule. |
| `EMAT_RH_SUM_F64` | harness | toggle | [examples/tpch_triangulation_bench.rs:1689](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: enable RobinHood SUM(f64) rule (default-on in some harness paths). |
| `EMAT_RT_BLOOM_RATIO` | harness | tunable | [examples/tpch_triangulation_bench.rs:1755](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: runtime-bloom ratio. |
| `EMAT_RT_BLOOM_SIDEBAND` | harness | toggle | [examples/tpch_triangulation_bench.rs:1745](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: enable runtime-bloom sideband rule. |
| `EMAT_RULES` | `"all"` | string | [examples/tpch_triangulation_bench.rs:1588](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: which rule set to apply. |
| `EMAT_SINGLE_PASS_RADIX` | harness | toggle | [examples/tpch_triangulation_bench.rs:1699](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: enable single-pass radix SUM rule. |
| `EMAT_SKIP_PARTIAL_RATIO` | harness | tunable | [examples/q15_serial_scope.rs:90](../crates/ematix-flow-core/examples/q15_serial_scope.rs) | Bench: skip-partial-aggregation ratio (Q15 scope probe). |
| `EMAT_SQL` | (required) | string | [examples/sql_probe.rs:50](../crates/ematix-flow-core/examples/sql_probe.rs) | Bench: SQL string to run (sql_probe). |
| `EMAT_SQL_ANALYZE` | off | `== Ok("1")` | [examples/sql_probe.rs:76](../crates/ematix-flow-core/examples/sql_probe.rs) | Bench: EXPLAIN ANALYZE the probed SQL. |
| `EMAT_SWAP_SEMI` | harness | toggle | [examples/tpch_triangulation_bench.rs:1636](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: semi-join build-side swap toggle. |
| `EMAT_SYNTHETIC_LEFT_SEMI` | harness | toggle | [examples/tpch_triangulation_bench.rs:1674](../crates/ematix-flow-core/examples/tpch_triangulation_bench.rs) | Bench: synthetic LeftSemi injection. |
| `EMAT_TEST_HJ_SWAP` | off | `.is_some()` | [examples/tpch_preset_rebench.rs:46](../crates/ematix-flow-core/examples/tpch_preset_rebench.rs) | Bench: test the HashJoin swap path. |

### Comment-only (no active read site — possibly dead)

| Flag | Owner file | Notes |
| --- | --- | --- |
| `EMAT_FAST_SNAPPY` | [emat_arrow_reader.rs:3059](../crates/ematix-flow-core/src/emat_arrow_reader.rs) | Appears only in a comment (`decompress_snappy_fast_into` via `EMAT_FAST_SNAPPY=1`); **no active `env::var` read site found — possibly dead / never wired.** |

---

## Counts

| Bucket | Count |
| --- | --- |
| Production gate (default-ON) | 18 |
| Production gate (opt-in) | 28 |
| Numeric tunable | 41 |
| Diagnostic / trace | 21 |
| Bench-harness only | 48 |
| Comment-only (possibly dead) | 1 |
| **Grand total (distinct flags)** | **157** |

> `EMAT_FAST_SNAPPY` is counted once, under Comment-only.
> `EMAT_MI_COLLECT` is placed under Diagnostic/trace (allocator-operational,
> not result-affecting); its companion `EMAT_MI_COLLECT_MIN_RSS_MB` is a
> Numeric tunable.
