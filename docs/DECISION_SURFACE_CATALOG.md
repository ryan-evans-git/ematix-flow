# Decision-surface catalog

**Status:** Phase 0 of Σ.AΩ autotune program. Initial draft, 2026-05-28.
**Purpose:** Catalog every hardcoded numeric gate that influences optimization or execution decisions. For each: current default, override mechanism, sensitivity hypothesis, measurement plan, effort to autotune. Output: a prioritized list of which gates Phase 1 attacks first.

## Methodology

For each gate the catalog records:

- **Gate name** — symbol/struct field/const, file:line reference
- **Current default** — value when no override
- **Override** — env var if env-settable, or `code change` if not
- **Decides** — what the gate gates / influences
- **Sensitivity hypothesis** — what kinds of queries / workloads is this expected to matter for?
- **Sweep plan** — concrete A/B values + queries to bench
- **Autotune effort** — estimated work to make it adaptive
- **Phase 1 priority** — H/M/L based on expected ROI × tractability

## Phase 1 priority decision matrix

We attack a gate in Phase 1 if BOTH:
- Sensitivity hypothesis is plausible (some realistic workload moves it)
- Autotune effort ≤ 1 week to first plan-time predicate

Long-tail or low-leverage gates defer to Phase 2+.

---

## A. Partitioning gates (HIGHEST LEVERAGE per Σ.AN.0)

### A.1 `target_partitions`

| Field | Value |
|---|---|
| Symbol | `SessionConfig::with_target_partitions` |
| Location | bench/preset, set at session creation |
| Current default | `std::thread::available_parallelism()` = cores (14 on M3 Pro) |
| Override | `PARTITIONS=N` env (bench-only); `--target-partitions` in CLI |
| Decides | Initial partition count for `RepartitionExec(Hash)`, downstream propagated by `EnforceDistribution` |
| Sensitivity | **CONFIRMED HIGH** — Σ.AN.0 data: Q18 -50ms @ P=112, but +37% on Q02 @ P=56. Q-shape dependent. |
| Sweep plan | (done) P ∈ {2,4,7,14,28,56,112,168,224,336,448,672,896} on 22q SF=10 |
| Autotune effort | **~1 week** — needs to detect high-cardinality agg shapes pre-EnforceDistribution and adjust per-session target_partitions |
| Phase 1 priority | **H** — clear data, known mechanism, big win available |

### A.2 Σ.AN.1 `TARGET_GROUPS_PER_PARTITION`

| Field | Value |
|---|---|
| Symbol | `const TARGET_GROUPS_PER_PARTITION` |
| Location | `agg_partition_boost.rs:82` |
| Current default | 50_000 |
| Override | code change only |
| Decides | Σ.AN.1's clamp formula `ceil(card/50K)` |
| Sensitivity | M3 Pro L3=12MB derived. Different hardware (Xeon L3=30MB) would want different value |
| Sweep plan | Sweep ∈ {10K, 25K, 50K, 100K, 250K} × per-host |
| Autotune effort | **~3 days** if Σ.AN.1 is shipped as default; pointless until then |
| Phase 1 priority | **L** — Σ.AN.1 is opt-in/neutral, no leverage to autotune |

### A.3 Σ.AN.1 `MAX_PARTITIONS_MULTIPLIER`

| Field | Value |
|---|---|
| Symbol | `const MAX_PARTITIONS_MULTIPLIER` |
| Location | `agg_partition_boost.rs:87` |
| Current default | 8 |
| Override | code change only |
| Decides | Ceiling on partition multiplier in Σ.AN.1 formula |
| Sensitivity | Q18 sweep showed P=112 (8× cores) optimum; P=224 (16×) worse |
| Sweep plan | Same as A.2 — sweep formula constants together |
| Autotune effort | Same as A.2 |
| Phase 1 priority | **L** — same as A.2 |

---

## B. L9 sideband / bloom gates

### B.1 L9 `min_probe_to_build_ratio`

| Field | Value |
|---|---|
| Symbol | `EnableRuntimeBloomSidebandRule.min_probe_to_build_ratio` |
| Location | `runtime_bloom_sideband_rule.rs:56,119` |
| Current default | 64 |
| Override | `EMAT_RT_BLOOM_SELECTIVITY=N` |
| Decides | L9 bloom fires only if `build_rows × ratio < probe_rows`. Higher = stricter (fewer fires). |
| Sensitivity | **MEASURED low (Σ.AO)** — 22q SF=10 sweep over {16,32,64,128,256} produced 1-2% diffs at noise floor. Per-query, Q18/Q21 hinted improvement at r=32; needs strict A/B to confirm. |
| Sweep plan | Strict A/B r=32 vs r=64 on 22q SF=10 (8 invocations) |
| Autotune effort | Per-query shape predicate is ~2 days; per-(probe-table-size, build-shape) lookup table is ~1 week |
| Phase 1 priority | **M** — small per-query potential, well-understood mechanism |

### B.2 L9 `allow_inner_join`

| Field | Value |
|---|---|
| Symbol | `EnableRuntimeBloomSidebandRule.allow_inner_join` |
| Location | `runtime_bloom_sideband_rule.rs:71,125` |
| Current default | false |
| Override | `EMAT_RT_BLOOM_INNER_JOIN=1` |
| Decides | Whether Inner-join HashJoinExec gets L9 bloom emitters |
| Sensitivity | Σ.AM.1 showed redundant Inner blooms add cost without benefit (lineitem already filtered upstream). But Σ.AM.1's narrower gate (only Inner-with-LeftSemi-in-build) is also banked opt-in. |
| Sweep plan | A/B with EMAT_AM1 variants on 22q |
| Autotune effort | Already shape-gated via Σ.AM.1; broader autotune would need per-join context tracking |
| Phase 1 priority | **L** — Σ.AM.1 already covers the structural case |

### B.3 L9 `require_filtered_build`

| Field | Value |
|---|---|
| Symbol | `EnableRuntimeBloomSidebandRule.require_filtered_build` |
| Location | `runtime_bloom_sideband_rule.rs:86,128` |
| Current default | true (L9.SelectiveBuild, Σ.S series) |
| Override | `EMAT_L9_REQUIRE_FILTERED_BUILD=0` |
| Decides | Whether to require a FilterExec in the build subtree before emitting bloom |
| Sensitivity | Filter-presence is binary; rule is whether to allow non-filtered fact⋈fact joins to emit |
| Sweep plan | A/B on 22q SF=10 with =0 to confirm current default is best |
| Autotune effort | Per-shape predicate ~2 days |
| Phase 1 priority | **L** — banked since Σ.S, low expected leverage |

### B.4 L9 `expected_keys` fallback (50K)

| Field | Value |
|---|---|
| Symbol | `build_rows.unwrap_or(50_000)` |
| Location | `runtime_bloom_sideband_rule.rs:389,401`; cascade rule:261 |
| Current default | 50_000 |
| Override | code change only |
| Decides | Bloom size when build stats are missing |
| Sensitivity | When stats absent, the bloom may be sized wrong; oversize wastes memory, undersize causes false positives |
| Sweep plan | Test queries where stats are commonly absent (joins through aggregates) at {10K, 50K, 250K, 1M} |
| Autotune effort | Cheap — compute from input.partition_statistics walk if Absent at top level |
| Phase 1 priority | **L** — Σ.AN.1's walk_for_row_count solves the same problem in a different context |

### B.5 L9 `max_expected_keys_per_partition`

| Field | Value |
|---|---|
| Symbol | `EnableRuntimeBloomSidebandRule.max_expected_keys_per_partition` |
| Location | `runtime_bloom_sideband_rule.rs:106,141` |
| Current default | 0 (disabled) |
| Override | `EMAT_L9_MAX_EXPECTED_KEYS=N` |
| Decides | Σ.AH.3 Story 2a — reject L9 emit when per-partition keys exceed N |
| Sensitivity | Opt-in safety net; defaults to off |
| Sweep plan | Σ.AH.3 already did this; bench-gate showed no consistent improvement |
| Autotune effort | N/A — already gate-then-decide |
| Phase 1 priority | **L** — banked, no compelling case |

### B.6 Σ.AM.1 `build_rows` cap

| Field | Value |
|---|---|
| Symbol | (inline literal in Σ.AM.1 gate) |
| Location | `runtime_bloom_sideband_rule.rs:358` |
| Current default | 10_000 |
| Override | none |
| Decides | Cap on the build_rows estimate when LeftSemi/LeftAnti found in build subtree |
| Sensitivity | Per Σ.AM.1 closure: rule is banked opt-in and neutral. Cap rarely matters. |
| Sweep plan | N/A — rule itself doesn't deliver |
| Autotune effort | N/A |
| Phase 1 priority | **L** |

### B.7 BuildSideBloomEmitter `min per-partition`

| Field | Value |
|---|---|
| Symbol | `(expected_keys / n_part).max(64)` |
| Location | `runtime_bloom_sideband_rule.rs:409`; `build_side_bloom_emitter_exec.rs:182` |
| Current default | 64 |
| Override | code change only |
| Decides | Minimum bloom size per partition |
| Sensitivity | Probably fine; 64 is small enough to be inconsequential |
| Sweep plan | A/B at {32, 64, 128, 256} on 22q |
| Autotune effort | Likely no-impact; deprioritize |
| Phase 1 priority | **L** |

### B.8 BuildSideBloomEmitter `L9_SET_THRESHOLD`

| Field | Value |
|---|---|
| Symbol | `const DEFAULT_L9_SET_THRESHOLD` |
| Location | `build_side_bloom_emitter_exec.rs:66` |
| Current default | 32_768 |
| Override | code change only |
| Decides | Threshold above which L9 emits a bloom vs HashSet |
| Sensitivity | Per memory `[[l9.hashset]]` — HashSet is faster for small builds, bloom is faster for large |
| Sweep plan | A/B at {4K, 16K, 32K, 64K, 128K} on 22q |
| Autotune effort | ~2 days — adapt based on observed build cardinality |
| Phase 1 priority | **M** — could move queries with mid-cardinality builds (Q05, Q08 etc) |

---

## C. Join reorder gates (Lever G)

### C.1 `max_leaves`

| Field | Value |
|---|---|
| Symbol | `ReorderOpts.max_leaves` |
| Location | `join_reorder.rs:117,165` |
| Current default | 4 |
| Override | `ReorderOpts` field; not env-controllable |
| Decides | Max chain length for reorder to fire |
| Sensitivity | Σ.AH.X data: estimator quality degrades beyond 4 leaves; Q07/Q09 regressed at 5+ |
| Sweep plan | Add env override, sweep {3, 4, 5, 6} on 22q SF=10 |
| Autotune effort | ~3 days — add env override + bench |
| Phase 1 priority | **M** — known sensitivity but Lever G itself is opt-in (Σ.AL revalidation = neutral) |

### C.2 `reject_string_like`, `reject_aggregate_join_keys`, `jump_on_reject`, `reject_under_left_semi_anti`

| Field | Value |
|---|---|
| Decides | Booleans; either enable or disable shape rejection |
| Sensitivity | Each is binary; have specific motivating queries |
| Sweep plan | A/B each on 22q (already done in Σ.AH.X / Σ.AL bench runs) |
| Autotune effort | N/A — already shape-based decisions |
| Phase 1 priority | **L** |

---

## D. Bloom layout (probably stable)

### D.1 `DEFAULT_BITS_PER_KEY` = 10

| Field | Value |
|---|---|
| Symbol | `bloom::DEFAULT_BITS_PER_KEY` |
| Location | `bloom.rs:46` |
| Decides | Bloom filter false positive rate (10 bits/key ≈ 1% FPR) |
| Sensitivity | LOW — standard bloom param, well-studied tradeoff |
| Phase 1 priority | **L** — not worth surveying |

### D.2 `DEFAULT_K_HASHES` = 8

| Field | Value |
|---|---|
| Symbol | `bloom::DEFAULT_K_HASHES` |
| Decides | Number of hash functions per bloom probe |
| Sensitivity | LOW — paired with bits/key, well-studied |
| Phase 1 priority | **L** |

### D.3 `BLOCK_BITS` = 256, `BLOCK_U32_LANES` = 8

| Field | Value |
|---|---|
| Symbol | `bloom::BLOCK_BITS`, `BLOCK_U32_LANES` |
| Decides | SIMD block layout for splash-bloom |
| Sensitivity | LOW — derived from cache line / SIMD register size |
| Phase 1 priority | **L** |

### D.4 Flight bloom transport caps

| Field | Value |
|---|---|
| Symbol | `FLIGHT_BLOOM_MAX_HEADER_HEX`, `FLIGHT_BLOOM_MAX_TOTAL_HEX` |
| Decides | Per-header / total bloom transport size in distributed mode |
| Sensitivity | Distributed-only; not relevant to local TPC-H |
| Phase 1 priority | **L** |

---

## E. Reader / batch / decode

### E.1 `DEFAULT_BATCH_SIZE` = 65_536

| Field | Value |
|---|---|
| Symbol | `emat_arrow_reader::DEFAULT_BATCH_SIZE`, `fast_parquet::DEFAULT_BATCH_SIZE` |
| Location | `emat_arrow_reader.rs:74`, `fast_parquet.rs:72` |
| Current default | 65_536 |
| Override | code change only |
| Decides | Arrow batch size emitted by parquet readers |
| Sensitivity | **HIGH** — affects every operator downstream. Larger batches = better SIMD utilization but more memory. Smaller = better cache locality but more loop overhead. |
| Sweep plan | A/B at {8K, 32K, 64K, 128K, 256K} on 22q SF=10 |
| Autotune effort | Tricky — interacts with EVERY operator. Could be per-query-shape. ~1 week minimum. |
| Phase 1 priority | **H** — high leverage but high effort. Maybe Phase 1.5. |

### E.2 `MIN_ROWS_FOR_DICT` = 100_000

| Field | Value |
|---|---|
| Symbol | `dict_routing::MIN_ROWS_FOR_DICT` |
| Location | `dict_routing.rs:55` |
| Current default | 100_000 |
| Override | code change only |
| Decides | Minimum row count for a table to be considered for dict-preserved decode |
| Sensitivity | Per `[[dict-routing]]` — small tables don't benefit from dict, threshold avoids overhead. |
| Sweep plan | A/B at {10K, 100K, 1M} on 22q |
| Autotune effort | ~2 days — already shape-routed; tweak threshold |
| Phase 1 priority | **M** — could affect Q01/Q12 type queries |

### E.3 Robin Hood agg `initial cap` = 65_536

| Field | Value |
|---|---|
| Symbol | `unwrap_or(65536)` |
| Location | `robin_hood_agg.rs:1822` |
| Current default | 65_536 |
| Override | code change only |
| Decides | Initial hash table size for RobinHood agg |
| Sensitivity | Per `[[sigma-nf3-beats-stock]]` — sized to fit common L1/L2. Affects only first-batch performance before dynamic resize. |
| Sweep plan | A/B at {16K, 65K, 256K, 1M} on Q18 / Q13 / Q21 |
| Autotune effort | ~3 days — pre-grow based on cardinality estimate |
| Phase 1 priority | **M** — improvement is marginal per existing bench, but trivial-ish to autotune |

---

## F. Fused agg / multi-agg gates

### F.1 `unwrap_or(18)` in InjectFilterMultiAggRule

| Field | Value |
|---|---|
| Symbol | `unwrap_or(18)` (line 362) |
| Location | `fused_aggregate_filter_multi_agg_rule.rs:362` |
| Current default | 18 |
| Override | (need to grep — env? const?) |
| Decides | (need to read context to know) |
| Sensitivity | UNKNOWN — would need to inspect |
| Phase 1 priority | TBD pending inspection |

---

## Phase 1 prioritized list

Based on the above, Phase 1 attacks (in order):

1. **A.1 `target_partitions`** (H sensitivity, ~1 week effort) — biggest known leverage from Σ.AN.0
2. **E.1 `DEFAULT_BATCH_SIZE`** (H sensitivity, ~1 week effort) — needs careful per-query sweep first
3. **B.1 L9 `min_probe_to_build_ratio`** (M sensitivity, ~2 days effort) — per-query predicate
4. **C.1 Reorder `max_leaves`** (M sensitivity, ~3 days effort) — extend env override + sweep
5. **B.8 L9 `SET_THRESHOLD`** (M sensitivity, ~2 days effort) — observed bloom cardinality
6. **E.2 `MIN_ROWS_FOR_DICT`** (M sensitivity, ~2 days effort)
7. **E.3 Robin Hood initial cap** (M sensitivity, ~3 days effort)

Total estimated Phase 1 effort: **3-4 weeks** for the top 7 gates. The remaining gates (D.* bloom layout, A.2/A.3 Σ.AN.1 constants, B.2-B.7 other L9 gates) defer to Phase 2+ unless surveying reveals unexpected sensitivity.

## What's not in this catalog (Phase 0 limits)

- DataFusion's internal `EnforceDistribution` rule constants — would require forking or wrapping the rule. Defer to Phase 1.5 if Phase 1 A.1 work motivates it.
- Logical optimizer thresholds in DataFusion (e.g., predicate selectivity defaults) — out of scope; would need DF patches
- Connection / network / IO buffer sizes — irrelevant to local TPC-H

## Next step

Phase 1 Week 1: pick A.1 `target_partitions` as first lever. Build a plan-time shape predicate that detects high-cardinality `RepartitionExec(Hash) → AggregateExec FinalPartitioned` chains, and BEFORE `EnforceDistribution` runs, raises the session's effective `target_partitions` for that query. Strict A/B 22q SF=10. If clean — flip default-on.

Note: this is a different mechanism from Σ.AN.1 (which ran AFTER EnforceDistribution and required a restore-Repartition). Pre-EnforceDistribution adjustment lets DataFusion's own logic propagate the partition count through the plan, avoiding the manual restore.
