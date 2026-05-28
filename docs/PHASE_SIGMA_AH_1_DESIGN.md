# Σ.AH.1 — L9 scan-level integration: design

**Status:** **REJECTED 2026-05-27** via Phase 0 spike — see § 4. Decision documented in memory `[[sigma-ah-1-arc-rejected]]`.
**Arc shell:** [`docs/plans/sigma-ah-arc-1.md`](plans/sigma-ah-arc-1.md).
**Active plan:** archived at [`docs/plans/archive/2026-05-27-sigma-ah-1.md`](plans/archive/2026-05-27-sigma-ah-1.md).

## 1. Decision summary

**Phase 0 (spike, 2 days) before committing to the full arc.** The arc shell predicts Q17 −80 ms and Q18 −40 ms by "skipping decoding entirely" for rows that miss the bloom. But the mechanism the shell proposes overlaps significantly with what **Σ.AH.2 Story 1'.4 Stage 1 already implemented** (the fused-probe path, opt-in via `EMAT_L9_FUSED_PROBE=1`). Before writing 3-4 weeks of arc, I want to **measure Q17/Q18 wall under the existing Stage 1 path** to see whether the predicted savings are already deliverable.

The spike has three honest outcomes:

- **(A) Existing Stage 1 already delivers the Q17/Q18 gates.** Then Σ.AH.1 collapses to: flip Stage 1 default-on, ship, close arc. Effort: 1 day total (not 3-4 weeks).
- **(B) Existing Stage 1 partially closes the gap.** Then Σ.AH.1 narrows to the specific bottleneck the profile identifies (likely **dict-aware bloom probe** on dict-encoded i64 join keys, e.g. `l_partkey`, `c_nationkey`).
- **(C) Existing Stage 1 doesn't move the needle on Q17/Q18 at all.** Then the original "push bloom into BridgeFilter" framing is dead; reject the arc.

## 2. Why the existing Σ.AH.2 Stage 1 likely overlaps

The arc shell says:

> Pushing the bloom into `EmatixFastParquetExec`'s `BridgeFilter` so rows whose join key isn't in the bloom skip decoding entirely.

But Σ.AH.2 Stage 1 (`8c9a3c2`) already:

1. Routes the L9 runtime-sideband's `I64InBloom` predicate into `BridgeFilter` via `with_added_predicates`.
2. In the fused-probe code path (gated by `EMAT_L9_FUSED_PROBE=1`), does dense-decode of all projected columns, walks the already-decoded i64 buffer for the bloom column, applies the bloom inline to build a bitmap, and stashes the bitmap for the per-batch SIMD filter on emit.
3. Avoids the double-decode of the filter column (the masked-path's `build_bitmap` calls a separate `filter_i64_column_to_bitmap_dense` which fully re-reads the column).

What the arc shell *adds* over Stage 1 is unclear without more specificity. The candidate deltas:

- **Page-level skip on projected non-filter columns when bitmap popcount is zero per page.** This is what the existing `load_row_group_masked` path does (the path the fused-probe path *replaces*). It works when matches cluster into specific pages, allowing entire pages to skip read+decompress. **For Q17's `l_partkey` (random-distribution column), the probability of any page having zero matches at 0.1% selectivity is ~0** — page-skip doesn't help.
- **Dict-aware bloom probe.** For dict-encoded i64 columns, probe the dict once (e.g., 131k entries for l_partkey, 25 for c_nationkey) instead of every data row. Per-row evaluation then becomes a `pre_computed_dict_bits[dict_index]` lookup. For l_partkey this is an 8× reduction in probe count; for c_nationkey it's a 60000× reduction. **This is a real, unimplemented optimization.**

## 3. Phase 0 — the spike

### 3.1 What to measure

Three queries, 10 trials each, three modes (baseline default-off, `EMAT_L9_FUSED_PROBE=1`, `EMAT_L9_TIGHT_CARDINALITY=1 EMAT_L9_FUSED_PROBE=1`).

| Query | Why | Pass-rate hypothesis |
|---|---|---|
| Q17 | Arc shell's headline (predicted −80 ms) | 0.1% (61k of 60M) |
| Q18 | Arc shell's bonus (predicted −30 ms) | 1.0% (orders→lineitem) |
| Q07 | Regression guard (existing nation-chain L9 win) | 5% (post-nation) |

Also a stage profile of Q17 in the fused-probe-on mode to see which stage dominates.

### 3.2 Spike pass / fail criteria

- **Pass (Outcome A)**: existing Stage 1 hits the gates from the arc shell (Q17 ≥ 60 ms drop, Q18 ≥ 30 ms drop), Q07 ≤ 175 ms. Ship Stage 1 default-on, close Σ.AH.1.
- **Partial (Outcome B)**: existing Stage 1 closes some but not all of the gap. The Q17 stage profile shows where the rest goes. Proceed with a narrow Story 2-4 attacking that bottleneck.
- **Fail (Outcome C)**: existing Stage 1 doesn't deliver. Reject the arc, capture finding, pivot to Σ.AH.3 or a different attack.

### 3.3 Phase 0 deliverable

`/tmp/sigma-ah-1-spike/q17-q18-q07-baseline-vs-fused-vs-fused+tight.md` with the table + stage profile interpretation. Decision documented in this file as a § 4 update.

## 4. Phase 0 result (2026-05-27): Outcome C — arc REJECTED

Spike ran at SF=10, Apple M3 Pro 14 cores, 10 timed trials × 2 warmups, three modes:

| Query | A (baseline) | B (EMAT_L9_FUSED_PROBE=1) | Δ B-A | C (fused + EMAT_L9_TIGHT_CARDINALITY=1) | Δ C-A | Gate |
|---|---:|---:|---:|---:|---:|:---|
| Q07 | 166.86 ± 7.52 | 158.95 ± 7.81 | **-7.91** | 178.00 ± 6.65 | **+11.14** | ≤ 175 ms |
| Q17 | 293.44 ± 5.88 | 286.55 ± 9.46 | **-6.89** | 261.11 ± 8.15 | **-32.33** | ≥ 60 ms drop |
| Q18 | 346.81 ± 16.86 | 347.55 ± 31.02 | +0.74 | 386.84 ± 57.25 | +40.03 | ≥ 30 ms drop |

**Neither mode clears the arc-shell gates.** Mode B is directional but tiny. Mode C unlocks Q17 -32 ms (still half the 60 ms gate) at the cost of Q07 (+11 ms, fails the ≤175 ms guard) and Q18 (+40 ms, regression).

### Q17 stage profile (mode B, fused-probe ON)

```
HashJoinExec depth 4    elapsed_compute = 4367 ms   <-- outer Inner join lineitem×subquery
AggregateExec depth 6   elapsed_compute = 1994 ms   <-- FinalPartitioned AVG group-by
EmatixFastParquetExec depth 9   186 ms              <-- inner lineitem scan (AVG side)
EmatixFastParquetExec depth 8   155 ms              <-- outer lineitem scan (bloom side)
Wall median: 276 ms; total elapsed_compute: 6970 ms (25× ratio → high parallel overlap)
```

The outer lineitem scan — the one the L9 bloom would filter at decode time — is **155 ms of 6970 ms total compute**. The shell's prediction (-80 ms by skipping decode) cannot exist: there is no 80 ms of bloom-filterable decode to skip on Q17. The HashJoin probe and AVG accumulation dominate, both **downstream** of the L9 mechanism.

### Why Outcome B's lever (dict-aware bloom probe) also wouldn't help

The arc shell § 5 proposed swapping per-row bloom probes for per-dict-entry probes, predicting ~80 ms Q17 savings from "84 ms / partition" of per-row probe cost. The stage profile refutes this: the outer scan including its probe is only 155 ms total elapsed_compute. Even eliminating the probe entirely would save at most that fraction, and most of it is decode, not probe.

### Decision

**Reject Σ.AH.1.** The arc's mechanism (push bloom into BridgeFilter to skip decode) doesn't have the decode it would skip — Q17's bottleneck is the downstream HashJoin + AVG, not the L9-filterable scan.

The opt-in Σ.AH.2 Stage 1 path (`EMAT_L9_FUSED_PROBE=1`) stands as banked infra. Mode B is a real -7 ms Q07 / -7 ms Q17 win on these queries, but the existing Σ.AH.2 arc closure already showed that flipping it default-on is net-zero at 22q geomean.

**Pivot:** Q17 / Q18 gaps are operator-level (HashJoin probe + AVG accumulate). Future levers should attack those operators directly, not L9. Candidates queued:
- Σ.AH.3 (build-vs-probe side-swap) — operator-level join-order rewrite, not decode-level
- Q17 / Q18 specific HashJoin probe optimization (subquery materialization, build-side reuse)

### Artefacts

- `/tmp/sigma-ah-1-spike/A-baseline.md`, `B-fused.md`, `C-fused-tight.md` — wall-time numbers per mode
- Q17 stage profile preserved in this section

## ~~4. If Outcome A — what shipping looks like~~ (NOT TAKEN)

Trivial: flip `EMAT_L9_FUSED_PROBE` default-on in the env-var dispatch (one line in `emat_arrow_reader.rs`). 22q SF=10 A/B confirms no regression elsewhere (the Σ.AH.2 Stage 6 attempt showed this is approximately net-zero at 22q scale; Σ.AH.1 only wins if Q17/Q18 specifically gain ≥ 60/30 ms).

If Q17 wins but Stage 1 default-on regresses 22q geomean (because of Q07 +9 ms etc.), keep Stage 1 opt-in and **also opt-in `EMAT_L9_FUSED_PROBE`** for explicit per-query enablement (e.g., a query-id allowlist). Less clean than default-on but ships the win.

## 5. If Outcome B — what Story 2-4 looks like

The most likely "B" finding (per my prior analysis of Q17): the **bloom-probe-per-row** cost on l_partkey is the bottleneck. 60M values × 1.4 ns/probe = 84 ms / partition. Even with fused-probe, this is dominant.

**Dict-aware bloom probe** would replace per-row probe with per-dict-entry probe:

1. Detect dict-encoded i64 columns at decode time (Σ.E5 already tracks `column_is_dict_encoded`).
2. When the filter column is dict-encoded AND the BridgeFilter has an `I64InBloom` predicate on that column:
   - Decode the dict page once → `Vec<i64>` of distinct values (e.g., 131k for l_partkey)
   - Run the bloom probe over the dict entries → `Vec<bool>` of dict-pass bits (131k bools)
   - For each row, look up `dict_pass_bits[row_dict_index]` (zero-copy, no bloom call)
3. Build the bitmap from the per-row dict-bit lookups.

**Expected delta on Q17**: bloom probe drops from 84 ms / partition to ~0.2 ms / partition (131k × 1.4 ns + 1M × 1 ns lookup). Wall savings: ~80 ms.

**Effort**: 1 week (kernel + integration into the fused-probe path).

**Risk**: medium. The dict integration uses Σ.E5's `column_is_dict_encoded` flag and the existing dict-decode infrastructure. The kernel is small. The bench-gate is per the arc shell.

## 6. If Outcome C — what we capture

Honest closure: the L9 mechanism's structural payoff was already maximally captured by Σ.AH.2 Stage 1, and the Q17 gap is bottlenecked elsewhere (likely downstream operators: per-row HashJoin probe + AVG subquery eval). Future work would target those operators directly (not L9).

## 7. Bloom-in-BridgeFilter implementation note (for Outcome B)

If we proceed with Stories 2-4, the bloom doesn't need a new `BridgeFilter` field — it already lives in `ColumnPredicate::I64InBloom { col_idx, bloom }`. The decode-time integration extends the per-column decode kernel for dict-encoded i64:

```rust
// Inside the i64 column decode kernel, when filter contains I64InBloom on this col:
let dict = decode_dict_page(file, rg, leaf)?;  // existing, Σ.E5
let dict_pass: Vec<bool> = dict.iter().map(|v| bloom.might_contain_i64(*v)).collect();
// Per-row: row_index → dict_index → dict_pass[dict_index] → bitmap bit
```

`ColumnPredicate::I64InBloom` is already evaluable at decode time (Σ.AH.2 Story 1'.4 Stage 1's `probe_i64_values_from_decoded`). What's new is the **dict-aware shortcut**: if the column is dict-encoded, probe the dict not the values.

## 8. Composition with Σ.AE.2 selectivity gate

The selectivity gate fires at `popcount * 3 > total` (pass rate > 33%) — uses dense fallback when masked-decode wouldn't save enough. For Σ.AH.1 firings, the bloom pass rate is typically << 33% (L9 fires on high-selectivity joins). So the gate doesn't fire, and we stay on the masked path. **No interaction needed.**

If the bloom pass rate IS high (e.g., bloom-on-FK net-negative pattern), the gate correctly falls through to dense; the dict-aware optimization just becomes a no-op (the per-row lookup is still cheap, but no work is saved overall). Still safe.

## 9. Open questions

- **OQ-AH.1-A**: dict-aware probe assumes the filter column is dict-encoded. What fraction of L9 fires today are on dict-encoded columns? — Story 1.2 measurement.
- **OQ-AH.1-B**: does Q18's orders→lineitem fire on `l_orderkey`, which has 15M distinct in 60M rows (likely NOT dict-encoded)? If so, dict-aware doesn't help Q18. — Story 1.2 trace.
- **OQ-AH.1-C**: should we also dict-aware optimize `I64InSet` (the small-build alternative to bloom)? Probably yes; the dict-pass-bits computation is the same shape.

## 10. References

- Empirical context: [Σ.AH.2 arc closure](PHASE_SIGMA_AH_2_DESIGN.md) § 5e, memory `[[sigma-ah-2-arc-closed]]`
- Existing fused-probe: `8c9a3c2` Story 1'.4 Stage 1
- Existing masked-decode path: [emat_arrow_reader.rs `load_row_group_masked`](../crates/ematix-flow-core/src/emat_arrow_reader.rs)
- Dict-encoded column detection: [ematix_fast_parquet.rs `column_is_dict_encoded`](../crates/ematix-flow-core/src/ematix_fast_parquet.rs)
- Σ.E5 dict-decode infrastructure: memory `[[sigma-e5-multi-buffer-stringview]]`, `[[sigma-l1-speculative]]`
- L9 timing/firing fixes: memory `[[sigma-q-l13-to-l16-session]]`
- Page-index pruning rejection precedent (similar shape, similar reason): memory `[[page-index-q14-dead-end]]`
