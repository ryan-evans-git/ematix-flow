# Σ.AH.2 — partition-aware bloom merge: design doc

**Status:** drafted 2026-05-26 as Story 1.0 deliverable.
**Author:** initial empirical audit, pre-implementation.
**Decision summary:** The plan's hypothesis is wrong. The bloom-merge mechanism the doc was supposed to design is **already implemented**. Story 1's real work is **gate relaxation**, not merge-strategy choice.

---

## 1. The plan's hypothesis

From [`docs/plans/CURRENT.md`](plans/CURRENT.md) Story 1:

> When a Partitioned-mode build runs across N (= 14) hash partitions, each partition independently builds a partial bloom. The emitter must produce a single shared bloom (union of all partials) before any probe-side scan can consume it.

The doc's stated open question (OQ-AH.2-A) was: **synchronous merge at end-of-build** vs **lock-free union as builds complete**, with the default pick being lock-free per-block bitwise-OR.

The plan further claimed the current `BuildSideBloomEmitterExec` "only wraps CollectLeft joins" and that the rule expansion is what unlocks Q05/Q07/Q08/Q09.

## 2. What the code already does

Both halves of the proposed mechanism are **already in production**:

### 2.1 `BloomFilter::union_with` — already exists

[`crates/ematix-flow-core/src/bloom.rs:296-313`](../crates/ematix-flow-core/src/bloom.rs:296):

```rust
pub fn union_with(&mut self, other: &BloomFilter) -> Result<(), BloomError> {
    if self.n_blocks != other.n_blocks || self.seed != other.seed {
        return Err(BloomError::TooSmall);
    }
    debug_assert_eq!(self.bits.len(), other.bits.len());
    for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
        *a |= *b;
    }
    Ok(())
}
```

**Correctness:** the split-block bloom layout (256-bit cache-line blocks, 8 × u32 lanes per block, deterministic salt-multiplier per lane, single block per hash) means `union_with` is exactly the bitwise-OR of two same-shape blooms. For any `x`:

```
union(A, B).might_contain(x)
  ⇔ all 8 lane bits of x are set in (A ∪ B)
  ⇔ for each lane i: (A.lanes[i] | B.lanes[i]) & mask_i == mask_i
  ⇔ for each lane i: (A.lanes[i] & mask_i == mask_i) OR (B.lanes[i] & mask_i == mask_i)
  ⇒ A.might_contain(x) OR B.might_contain(x)
```

The implication is one-way (a true-positive in the union may not have been a true-positive in either A or B alone — that's the standard bloom union upper bound). FPR is at most 2× the single-bloom rate when both are at-or-below-target population — well within the 1% design margin since each partial holds 1/N of the total population.

### 2.2 `BuildSideBloomEmitterExec` — already partition-aware

[`crates/ematix-flow-core/src/build_side_bloom_emitter_exec.rs:78-118`](../crates/ematix-flow-core/src/build_side_bloom_emitter_exec.rs:78) carries the *exact* mechanism the doc was supposed to design. From the module's own header comments (lines 16–28):

> Per partition:
>   1. Allocate a local BloomFilter sized for `expected_keys / n_partitions`.
>   2. Pull batches from the input. For each batch: insert the i64 key column into the local bloom; forward the batch downstream unchanged.
>   3. On stream-end: acquire the shared `local_blooms` lock, push our local bloom, increment the `completed` counter.
>   4. If `completed == n_partitions`, drain the vec, OR-merge all locals into one union bloom...
>
> Mutex contention is per-partition-finish (once each), not per-row.

The struct stores `local_blooms: Arc<Mutex<Vec<BloomFilter>>>`, `completed: Arc<AtomicUsize>`, and `n_partitions`. `try_new_with_extras` initialises `n_partitions = input.output_partitioning().partition_count().max(1)`, so it adapts to whatever the planner gave it — CollectLeft (1) or Partitioned (N).

**This is the "synchronous merge at end-of-build" design**, already shipped and battle-tested across the L9 milestone work (`[[sigma-q-l9-landed]]`, `[[sigma-q-l13-to-l16-session]]`, `[[sigma-s-b-cascade-neg]]`).

### 2.3 `EnableRuntimeBloomSidebandRule` — no partition-mode filter

[`crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs:120-275`](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs:120). A `grep` for `partition_mode`, `PartitionMode`, or `CollectLeft` in the rule file returns **zero matches**. The rule pattern-matches on join shape (Inner/LeftSemi/RightSemi), build-side filter presence, equi-key type (Int64), and probe-side reachability — nothing about partition mode.

## 3. Empirical trace: what actually fires today

Ran `EMAT_L9_TRACE=1 TPCH_QUERIES=5,7,8,9` on the post-Σ.AH.4 build. Aggregated counts:

| Outcome | Count | What it means |
|---|---:|---|
| `WRAP Inner join — expected_keys=64` | 2 | nation→supplier (build=64, probe=100k) and nation→customer (build=64, probe=1.5M). Both fire. Both at-or-below the threshold. |
| `skip Inner — build subtree has no FilterExec (require_filtered_build=true)` | **13** | Build is a HashAgg or Repartition or HashJoin over filtered tables, but the literal subtree root isn't a `FilterExec`. **This is the real gap.** |
| `skip — gate rejects: b(2999990) × 1024 >= p(59986052)` etc. | 5 | Build cardinality × ratio_gate exceeds probe cardinality. Most are legitimately too-big builds (3M / 12M / 400k vs 60M lineitem at 1:1024). |

**The plan's framing is empirically refuted.** Partitioned-mode wraps already happen on the AH.2 target queries; they're just the wrong wraps (nation-chain only). The gate that excludes the actual interesting wraps (part_filt→lineitem, etc.) is the L9.SelectiveBuild gate — `require_filtered_build=true` looks for an immediate `FilterExec` descendant of the build, which gets covered up by every aggregation, repartition, and inter-join step in a multi-stage TPC-H query.

## 4. Why the gate is too conservative

Memory `[[sigma-q-l9-bloom-consumer-findings]]` records why the gate exists: bloom-on-FK without a build-side filter is net-negative because FK referential integrity makes the bloom pass-through ~100% (every probe row's FK is guaranteed to find a match in the build's PK set). The gate is correct *for the "raw scan → join" case it was designed against* — but it overshoots when the build is several operators downstream of the actual filter.

Concrete missed cases on AH.2 targets:

- **Q08 lineitem ⋈ part_filt-after-agg**. The build is `HashAggregate(GroupBy=p_partkey)` over `FilterExec(p_type='ECONOMY ANODIZED STEEL')`. The filter IS there, just one level deeper than the gate looks. Build cardinality is 13k after filter, probe is 60M lineitem → 1:4600 ratio, well within the bloom regime.
- **Q09 partsupp ⋈ part_filt-after-agg**. Same shape. 108k post-filter build, 8M probe = 1:74 ratio.
- **Q05 supplier ⋈ filtered-region-chain**. Build is filtered through a region join; the literal build subtree is a HashJoin, not a FilterExec.

The bloom-on-FK net-negative pattern these were designed to avoid is the *unfiltered* case (cust ⋈ orders on full 1.5M cust × 15M orders). The 13 skipped joins on AH.2 targets are *all* filter-bearing — just behind another operator.

## 5. Revised Story 1 scope

The right Story 1 work is **not** "design a partition-aware merge" (already exists), it's **gate relaxation + correctness audit**.

### Story 1' (revised) — gate relaxation

1. **Story 1'.1 — diagnostic walker.** Write `build_subtree_has_filter_equivalent(subtree)` that returns true if any of:
   - The subtree IS a `FilterExec` (current behaviour);
   - The subtree is a `HashAggregateExec` (or `RobinHoodSumF64Exec` etc.) whose input has a filter (one step down);
   - The subtree is a `RepartitionExec` whose input has a filter (one step down);
   - The subtree is a `HashJoinExec` whose either side has a filter (one step down).

   Cap recursion at depth ≤ 3 to bound cost. This catches the 13 missed joins above.

2. **Story 1'.2 — wire post-filter cardinality.** The current trace shows `build_rows=Some(2999990)` for what should be a much smaller post-filter build. Confirm whether the rule is using pre- or post-filter cardinality via `partition_statistics` (memory `[[sigma-ae-complete]]`). If pre-, plumb the post-filter estimate through.

3. **Story 1'.3 — gate the relaxation behind correctness tests.** All 22 SF=10 queries must still pass `tpch_validate` byte-identical. The bloom-on-FK net-negative risk is real per `[[sigma-q-l9-bloom-consumer-findings]]` — the relaxation must include a "is the discovered filter selective enough to make the bloom worth it" check, not just "is there a filter somewhere downstream".

4. **Story 1'.4 — guard against Q03 regression.** Q03's (cust+orders) ⋈ lineitem is the canonical bloom-on-FK negative shape. Even with cust+orders both filtered, the build cardinality (1.46M) makes the bloom expensive. Verify the relaxed gate doesn't fire on Q03.

### What was Story 1 in the plan but isn't needed

- ~~`BloomFilter::union` kernel + correctness tests~~ — already at `bloom.rs:296` (`union_with`). Property-based test would be cheap to add as belt-and-braces but doesn't unblock anything.
- ~~Microbench: merge cost on 14 × 100k-item partials~~ — `union_with` is straight byte-OR at memcpy speed; 14 × 100k @ 10 bits/key = ~1.7 MB total = sub-ms trivially.
- ~~Partition-aware emitter (Story 1.3)~~ — emitter already partition-aware.
- ~~Re-test existing CollectLeft path (Story 1.4)~~ — was meant to verify a not-yet-written refactor; with no refactor needed, this collapses into the 22q `tpch_validate` smoke test.

## 5a. Empirical pivot during implementation (2026-05-26)

Story 1'.1's "filter-equivalent walker" premise was **also overturned** when Q08's plan dump was inspected: the existing `build_subtree_has_filter` already recurses through every child node (including HashAgg/Repartition/HashJoin), so the 13 "no FilterExec" skips are all on **legitimately unfiltered** raw scans (nation, customer, supplier as direct build sides). The walker doesn't need relaxing — the gate is doing its job there.

The real gap was the **ratio gate**: it correctly identified the AH.2 target joins as filter-bearing, but their `build_rows` estimate came from DataFusion's `FilterExec.statistics()`, which uses a default 0.2 selectivity for string-Eq predicates (`p_type='ECONOMY...'` → 2M × 0.2 = 400k vs real 13k). Even with the dict-distinct fix from Story 1'.2 populating `distinct_count = Inexact(150)`, DataFusion's FilterExec doesn't consult `distinct_count` for string-Eq.

**Story 1'.3 (actual landed work):** an emat-stats-aware override `estimate_build_rows_via_emat_stats` in the L9 rule, gated by `EMAT_L9_TIGHT_CARDINALITY=1`. Walks the build subtree for `FilterExec` + `EmatixFastParquetExec`, computes `raw × selectivity` directly using our column_stats. Handles `col=literal → 1/distinct`, `AND → ×`, `OR → max`. Q08's part_filt → lineitem now WRAPs (`build_rows = 13333`, ratio gate passes). Q03's canonical reject case still correctly skipped.

**Stretch gap surfaced by Q09:** the matcher defaults to 0.2 for `StringLike` (and ranges via And-of-bounds). Q09's `p_name LIKE '%green%'` therefore still shows `build_rows = 400k` and fails the ratio gate. A future lever (Σ.AH.7, queued in CURRENT.md) would evaluate LIKE predicates against the dict-page entries at planner time to count matching keys, then use `matches/distinct_count` as the selectivity. Cost: O(distinct_count) per LIKE; risk: low (planner-only). **Out of Story 1'.3 scope.**

## 5c. Stage 4 attempted + reverted (2026-05-26)

Tried a "filter-once-per-RG" variant: after Stage 1's dense decode + bitmap construction, apply `filter_record_batch` ONCE on the full RG via converting `cur_rg_columns` (DecodedColumn) → ArrayRef → RecordBatch → filter → store result in a new `cur_rg_filtered_arrays` field; `slice_batch` then slices from the pre-filtered arrays via zero-copy `Array::slice`.

**Result:** broke correctness on **4 queries (Q07, Q08, Q12, Q21)** — `18/22 PASS`. Wall savings on Q08 were ~4 ms (188 → 185 ms) but the conversion path via `slice_decoded(&col, 0, total_pre, target)` for non-primitive columns (StringView, DictUtf8) has a latent bug at the full-RG slice scenario. Reverted; captured as a deferred follow-up.

ROI assessment: Stage 1+2 already closed 95% of the Story 1'.3 wall regression. Stage 4's marginal wall delta (~1-2 ms) doesn't justify the depth of investigation needed to fix the StringView/DictUtf8 full-slice path. Stage 4 stays deferred — pickable up later if 22q SF=10 sweep shows it's still on the critical path.

## 5b. Story 1'.4 — six-stage L9 decode-path optimisation arc (2026-05-26)

After Story 1'.3 committed the cardinality fix and demonstrated Q08 wall +55 ms regression, Story 1'.4 first tried a simple dense-then-bitmap fallback (Option B in design notes). Empirical stage profile showed Option B was essentially the same cost as masked decode (2251 ms vs 2277 ms lineitem-scan compute) — the bottleneck isn't the masked-decode tax, it's the bloom probe itself + per-batch SIMD filter, which BOTH paths pay.

Reframed analysis: Q08's part_filt → lineitem firing adds ~106 ms/partition to scan but saves only ~65 ms/partition downstream. Net +41 ms wall. To recover the regression we need to attack the scan cost itself, not just the masked-decode path. Six stages, in order:

| # | Optimisation | Expected wall Δ | Effort |
|---|---|---|---|
| 1+5 | Fuse bloom probe into dense decode, SIMD-friendly kernel | −25 ms | 1-2 days |
| 3 | Inline bitmap as one parallel-decode column | −5 ms | 0.5 day |
| 2 | Cache decompressed pages across build_bitmap + dense | −10 ms | 0.5 day |
| 4 | Sideband bitmap to HashJoin, skip per-batch filter | −8 ms | 1-2 days |

Plus:
- **Stage 5 — clustering gate**: `build_size / probe_distinct_count >= threshold` predicate at L9 firing. Safety net for shapes where the optimisations don't fully recover. Cheap (~2 hours).
- **Stage 6 — 22q SF=10 A/B + default-on flip**: if geomean ≥ baseline post-optimisations, flip both `EMAT_L9_TIGHT_CARDINALITY` and the clustering gate to default-on.

Hard-stop conditions: 22q geomean regression > 2 pp at any check-in, or per-query regression > 5% on existing L9 wins (Q07 ≤ 175 ms, Q17 ≤ 230 ms).

## 6. What this means for Stories 2-5

The Story 2 ("rule extension to Partitioned-mode") in the plan also evaporates — the rule already fires on Partitioned. Story 2's *spirit* (open the L9 fire on the four AH.2 target queries) is now Story 1' above.

Stories 3 (wall-time bench gate) and 5 (soak + default-on flip) still apply — same gate threshold (Q08 ≥ 30 ms, Q09 ≥ 50 ms, geomean ≥ 3 pp, no regression > 5%). Story 4 (cascade with AH.1) also unchanged in framing.

## 7. Synchronous vs lock-free merge

For completeness — even though Story 1 doesn't need this decision, the design choice was:

**Synchronous merge at end-of-build, mutex-guarded vec append.** This is what the current emitter does. Pros:
- Single-writer, no per-row contention (only per-partition-finish, once each).
- Correctness is trivial: when `completed == n_partitions`, the vec is fully populated and stable.
- HashJoinExec's existing build/probe phase split (build fully drains before probe starts) means the merge timing doesn't bottleneck anything.

The "lock-free per-block bitwise-OR" alternative was considered in the plan, but offers no benefit here because the merge is off the hot path (one merge per join, not one per row). The mutex cost is dominated by the bloom insertion cost itself by several orders of magnitude.

## 8. Acceptance criteria for closing Story 1'

- [ ] `build_subtree_has_filter_equivalent` lands with unit tests covering the 4 cases (immediate FilterExec, HashAgg-over-filter, Repartition-over-filter, HashJoin-with-filter-side).
- [ ] L9 trace on Q05/Q07/Q08/Q09 shows ≥ 4 new WRAPs (one per target query, on the part_filt→lineitem / part_filt→partsupp edges).
- [ ] 22q SF=10 `tpch_validate` passes byte-identical.
- [ ] Q03 trace shows the cust+orders → lineitem join is **still** skipped (the relaxed gate must not regress this).
- [ ] No 22q SF=10 wall-time regression > 5% on any query under `EMAT_L9_RELAX_GATE=1`.

## 9. References

- Empirical trace dump: `bvlu53d5l` (`grep '^\[L9\.trace\]'`)
- Plan: [`docs/plans/CURRENT.md`](plans/CURRENT.md) Story 1
- Existing emitter: [`crates/ematix-flow-core/src/build_side_bloom_emitter_exec.rs`](../crates/ematix-flow-core/src/build_side_bloom_emitter_exec.rs)
- Existing rule: [`crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs`](../crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs)
- Bloom kernel: [`crates/ematix-flow-core/src/bloom.rs`](../crates/ematix-flow-core/src/bloom.rs) — `union_with` at :304
- Gate origin: memory `[[sigma-q-l9-bloom-consumer-findings]]`
- Post-filter cardinality: memory `[[sigma-ae-complete]]`
- Cascade-negative precedent: memory `[[sigma-sb-cascade-neg]]`
