# Σ.AH.1 — L9 scan-level integration (push bloom into BridgeFilter)

**Status:** drafted, not active
**Parent:** [`docs/PERF_REVIEW_2026_05.md`](../PERF_REVIEW_2026_05.md) (Phase C ranking #1 by per-query impact)
**Hypothesis:** L9 build-side blooms are currently consumed at HashJoinExec probe-time — rows are decoded then dropped. Pushing the bloom into `EmatixFastParquetExec`'s `BridgeFilter` so rows whose join key isn't in the bloom skip decoding entirely will close the largest single per-query gap in the suite (Q17 lineitem main scan paying 1093 ms parallel to produce 61k rows out of 60M).
**Queries impacted:** Q17 (~80 ms wall savings), Q18 (~40 ms), cascade benefits to Q05/Q07/Q08/Q09 if combined with Σ.AH.2.
**Predicted impact range:** **~120 ms wall across 2 queries direct; ~250 ms across 6 queries with Σ.AH.2** = **3-7 pp SF=10 geomean**.
**Effort estimate:** 3-4 person-weeks (decode-time bloom probe is non-trivial; plan-time synchronisation between build and probe sides is the hard part).
**Risk level:** **M-H**.

## Bench gate (ship-if / reject-if)

### Microbench
- **Kernel:** in-scan bloom probe (probe i64 key during decode) measured against the existing `bloom.rs::contains` hot path.
- **Threshold:** in-scan bloom probe ≤ 1.5× the cost of the per-batch bloom probe currently used by `EnableInBloomScanPushdownRule` (i.e., the existing `EMAT_BLOOM_PUSHDOWN=1` mode). Tighter is fine; > 1.5× is reject.

### Wall-time
- **Required:** Q17 SF=10 wall drop ≥ **60 ms** (175 → ≤ 115 ms) AND 22q SF=10 geomean improves by **≥ 2 pp** AND no single query regresses by **> 5%** (5% noise band per Σ.O.c.2 audit).
- **Bonus:** Q18 SF=10 wall drop ≥ 30 ms.

### Reject-if
- Microbench passes but wall-time fails (per `[[sigma-r2-rejected]]` precedent — kernel wins don't predict wall-time).
- Any query regresses > 5% (e.g. Q01 / Q06 / Q14 — scan-heavy with no L9 firing — could be touched if the BridgeFilter API surface changes).
- Plan-time synchronisation cost > 2 ms wall (build-side hash must complete before probe-side scan; if the sync barrier serialises Partitioned-mode joins, the parallelism gain is lost).

## Hard constraints (inherited)

- **No new PhysicalOptimizerRule** (codegen-tax per `[[optimizer-codegen-sensitivity]]`). Extend the existing `EnableRuntimeBloomSidebandRule` if rule changes are needed.
- **Sibling-crate kernel** if a new decode-time bloom probe primitive lands. The kernel goes in `ematix-parquet` (or `ematix-flow-core` if it's scan-orchestration only).
- **TDD** per `[[feedback-tdd]]`.
- **No TPC-H-specific hardcoding** per `[[feedback-no-tpch-hardcoding]]`. The mechanism must work on any small-build/large-probe Inner-equijoin shape.

## Story skeleton (no tasks)

- **Story 1 — bridge plumbing.** Extend `BridgeFilter` to accept an `Arc<BloomFilter>` for one of its columns. Decode-time probe: for each emitted row, check bloom; drop row if absent. Correctness tests: bloom-included-set matches probe output.
- **Story 2 — kernel optimisation pass + microbench gate.** SIMD bloom probe in the decode loop. Microbench against existing post-decode bloom probe; gate per criteria above.
- **Story 3 — scan-side bloom hand-off.** Extend `EmatixFastParquetTableProvider` / scan factory to accept a `RuntimeBloomSideband` reference; consume the bloom when the build side finishes populating. Plan-time synchronisation: how does the scan thread know the bloom is ready? Either (a) the bloom is `Option<Arc<...>>` and is checked per-batch (consume only after non-None), or (b) the scan blocks on a one-shot at startup. (a) is preferred — no sync barrier, but the first few RGs may not benefit.
- **Story 4 — rule extension.** Update `EnableRuntimeBloomSidebandRule` (or whichever rule wires the sideband) to additionally point the bloom at the probe-side scan, not just the operator. Cover both CollectLeft and Partitioned (if Σ.AH.2 has landed). Bench-gate.
- **Story 5 — opt-in flag + soak.** Land behind `EMAT_L9_BRIDGE_PUSH=1`. Bench 22q SF=10 across 3 back-to-back runs. If geomean+per-query gate clears, soak for 24h, then flip default-on.

## Risks + watch-items

- **Plan-time sync race** (highest risk). If the bloom isn't ready when the probe-side scan starts, either we block (kills parallelism) or we miss the bloom-skip (degrades to current behaviour). The first-RG-misses-bloom mitigation acceptable but quantify in Story 3 microbench.
- **Kernel-bench-doesn't-predict-wall-time** per `[[sigma-r2-rejected]]`. Story 2 microbench gate is necessary but not sufficient — Story 4 wall-time gate is the real bar.
- **Cascade with Σ.AH.2** — the biggest payoff is when both arcs are live (Q05/Q07/Q08/Q09 get bloom-at-scan-time on their Partitioned joins). Σ.AH.1 alone delivers Q17/Q18; full impact needs Σ.AH.2 first.
- **Decode-time bloom probe interferes with Σ.AE.2 selectivity-gate fallback.** Both consume the row stream; need careful composition. If both fire on the same RG, the bloom should apply first (since the bridge-filter bitmap may be empty if the bloom drops the row).
- **Selectivity gate may need re-tuning.** If bloom drops 99% of rows, the selectivity-gate "dense vs masked" choice changes. Re-audit thresholds after this arc.

## References

- Phase C ranking entry: [`docs/PERF_REVIEW_2026_05.md`](../PERF_REVIEW_2026_05.md) Σ.AH.1 section
- Existing operator-level L9: memory `[[sigma-q-l9-landed]]`, `[[sigma-q-l13-to-l16-session]]`
- Existing in-scan bloom pushdown (orthogonal mechanism, distributed-bloom): memory `[[sigma-j2b-vi-landed]]`, `[[sigma-j2b-vii-landed]]`
- Σ.AE.2 selectivity-gate fallback (interacts with this arc): memory `[[sigma-ae-complete]]`
- Related rejection: `[[sigma-r2-rejected]]` — kernel-bench-doesn't-predict-wall-time precedent
- Per-query evidence: [`docs/PERF_Q17.md`](../PERF_Q17.md), [`docs/PERF_Q18.md`](../PERF_Q18.md)
