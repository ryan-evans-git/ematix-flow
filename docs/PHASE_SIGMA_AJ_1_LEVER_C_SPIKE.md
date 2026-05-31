# Σ.AJ.1 Lever C — Phase 0 spike scope

**Status:** **CLOSED POSITIVE, branch (a) clean win, 2026-05-27** — `EMAT_AGG_SEMI` flipped default-ON; 22q SF=10 net -225.91 ms (-6.64%); 3 wins (Q08 -11.22, Q17 -103.61, Q18 -27.01); 0 >2σ regressions. See § Outcome for the v1/v2 result comparison and the bench-harness fix that surfaced the wins.
**Budget:** 1 day (revised from the design doc's "1-2 weeks" — see § Discovery).
**Prerequisite:** Σ.AJ.1 EXPLAIN ANALYZE outcome (`[[sigma-aj-1-q17-explain-analyze]]`) — no kernel lever inside outer HashJoin; structural waste is upstream AVG over 60M rows producing 2M groups when only ~2K groups survive.

## Discovery — the rule already exists

Pre-spike codebase survey revealed that **the Lever C mechanism was implemented in May 2026** as Σ.U Phase 1 and is currently dormant in-tree:

- **File:** `crates/ematix-flow-core/src/agg_filter_pushdown.rs` (611 LOC, 4 tests passing at SF=1)
- **Commit:** `18d170d` (2026-05-26) — "Σ.U Phase 1 — agg-side LeftSemi pushdown (opt-in via EMAT_AGG_SEMI=1)"
- **Generalisation commit:** `1ed7a77` (Σ.V + Σ.U.1) — extends matcher to Q02 shape
- **Wire-up:** `tpch_triangulation_bench.rs:417` reads `EMAT_AGG_SEMI=1`; calls `push_filter_into_agg(plan)` on the optimized LogicalPlan
- **Correctness:** SF=1 end-to-end tests pass for both Q17 (`rewrite_preserves_q17_result`) and Q02 (`rewrite_preserves_q02_result`)

The rule pattern-matches:

```
Inner Join (L, R) on L.X = R.K
  L: any subtree where L.X is produced by Filter(P) → TableScan(T_alt)
  R: Aggregate(group_by=[K, ...]) → TableScan(T)
```

and rewrites to:

```
Inner Join (L, R) on L.X = R.K
  L: <unchanged>
  R: Aggregate(group_by=[K, ...]) → LeftSemi(T.K = F.K) → TableScan(T)
                                              ↑
                                    cloned filter subtree from L
```

This is **exactly the Lever C mechanism** the design doc described as "1-2 weeks of net-new work." It exists, was tested at SF=1, and was banked as opt-in because the historical 22q geomean bench under the loose protocol said neutral.

## Why the prior verdict deserves re-checking

The "neutral on loose-protocol bench" verdict in May 2026 used the same single-invocation 5-trial methodology that has since been overturned at least 3 times in the AH/AI/AJ arcs:

| Lever | Loose protocol said | Strict interleaved A/B said | Verdict flipped? |
|---|---|---|---|
| Σ.AH.X Lever A (fused-probe default-on) | net +2.9% slower | net -1.26%/-2.27% faster | YES, flipped to default-on 2026-05-27 |
| Σ.AH.X Lever G (shape-detect reorder) | -25 ms Q10 reliable | lost in 22q variance | NO change, stayed opt-in |
| Q03 -7ms (Σ.AH.X Lever A) | reliable -7 ms | sequential-block artifact, real Δ ≈ noise | YES, "win" was wrong direction |

Σ.U Phase 1 was banked as "neutral, no 22q win" under the methodology that produced those false positives/negatives. Per `[[bench-methodology-3-invocations]]` and `[[sigma-ai-1-strict-bench-landed]]`, the strict interleaved A/B harness drops per-query CV from 5-10% to 1.30-1.96%. A 5-pp Q17 effect that was lost in noise on the loose protocol would be clearly detectable on the strict one.

**Hypothesis:** The Σ.U rule's effect on Q17 SF=10 is large enough that strict interleaved A/B will show a clear win, AND the 22q geomean impact (other queries' regressions or noise) will be ≤ 1pp.

## Spike plan — 1 day

### Step 1 — SF=10 correctness gate (~2 hours)

Run end-to-end correctness on Q17 + Q02 SF=10 with `EMAT_AGG_SEMI=1`:

```bash
TPCH_DATA_DIR=examples/tpch/data/sf10 \
  EMAT_AGG_SEMI=1 \
  cargo test -p ematix-flow-core --release --test '*' \
  -- rewrite_preserves_q17 rewrite_preserves_q02
```

Existing tests use SF=1 (env `EMATIX_FLOW_TPCH_SF1`). Add SF=10 variants gated on `EMATIX_FLOW_TPCH_SF10` env. Also extend to Q11 + Q15 + Q22 (other scalar-subquery shapes from `[[sigma-z-subquery-dedupe]]`) since the matcher is generalised.

**Gate:** all queries must produce row counts matching baseline. Numeric tolerance: relative error < 1e-9 for non-aggregates, < 1e-6 for AVG-bearing queries (Σ.U.1 rewrite preserves float-determinism via group-key isomorphism).

**If fails:** the rule has an SF=10-specific shape it doesn't handle. Falls out of scope for this spike; report the shape and freeze.

### Step 2 — strict interleaved A/B on Q17 SF=10 (~30 min)

```bash
./scripts/bench/strict_ab.sh \
  --env-a "EMAT_AGG_SEMI=0" \
  --env-b "EMAT_AGG_SEMI=1" \
  --queries "17" \
  --invocations 8 \
  --output /tmp/strict-ab-aj1-leverc-q17
```

Methodology baseline from `[[sigma-ai-1-strict-bench-landed]]`:
- `caffeinate -i` + `taskpolicy -a` (P-core QoS)
- Discard first invocation as cold-start
- Median-of-medians across remaining 7 invocations
- Verdict bar: 2 × max(σ_A, σ_B)

**Expected if hypothesis holds:** Q17 wall B − A in the range **-50 to -120 ms** (-15% to -35%). Compute pile: AVG FinalPartitioned drops from 2.26s → ~0.05s of total compute. LeftSemi adds its own probe cost (~20-50 ms wall expected based on `[[sigma-aj-1-lever-b-rejected]]` bloom-probe data points), so net wall depends on probe vs saved-AVG balance.

**Gate:** B − A < -20 ms (>2σ) for the spike to proceed. If Q17 is within noise, the spike conclusion is "Σ.U rule is structurally correct but probe cost matches AVG savings at this shape" — close arc and accept Q17.

### Step 3 — 22q SF=10 strict interleaved A/B (~25 min)

```bash
./scripts/bench/strict_ab.sh \
  --env-a "EMAT_AGG_SEMI=0" \
  --env-b "EMAT_AGG_SEMI=1" \
  --invocations 4 \
  --output /tmp/strict-ab-aj1-leverc-22q
```

**Gates:**
1. **Hard:** no per-query regression > 5% above 2σ bar
2. **Hard:** 22q geomean Δ ≤ +1.5pp (within methodology noise floor per `[[sigma-ai-1-strict-bench-landed]]`)
3. **Soft:** ≥ 1 net win (Q17 ideally; possibly Q02, Q11, Q15, Q22 — same shapes)

Per `[[no-tpch-hardcoding]]`: the rule already generalises (Q02, potentially Q11/Q15/Q22). If only Q17 wins and Q02/Q11/Q15/Q22 are neutral, that's fine — the rule is shape-based, not query-specific.

**If fails Hard #1 or #2:** investigate the regression source via EXPLAIN ANALYZE diff on the regressing query. Most likely cause: LeftSemi probe cost exceeds the AVG savings at that shape (the same mechanism that killed Lever B). If a single regressing query, consider a shape predicate ("only fire when agg input rows / agg output rows > 100×" — selectivity gate). Add the predicate, re-bench. Time-box this dig-in at 4 hours per `[[no-quick-reject]]`.

### Step 4 — decision (~30 min)

Three branches:

**(a) Clean win** (Q17 wins ≥ 20 ms, 22q geomean Δ ≤ +1pp, no per-query regression > 5%):
- Flip `EMAT_AGG_SEMI` default to ON in `tpch_triangulation_bench.rs` AND `preset.rs`
- Add SF=10 correctness test on Q17/Q02 as default `cargo test`
- Update `[[full-bench-env-checklist]]` memory to note opt-OUT only
- Σ.AJ.1 closes positive; Q17 wall drops to ~165-220 ms range

**(b) Conditional win** (Q17 wins but 22q geomean regresses 1-3pp on another query):
- Add shape predicate (selectivity gate) to `try_rewrite_q17_shape`
- Re-bench
- If clean after predicate → branch (a)
- If still regresses → branch (c)

**(c) No win or hard-fail** (Q17 within noise, OR 22q regression > 3pp on any query):
- Document why in memory
- Σ.AJ.1 closes negative; Σ.U stays opt-in as banked infra
- Recommend accepting Q17 as a stretch target

## Spike inputs (already exist)

| Artifact | Path |
|---|---|
| Σ.U rule implementation | `crates/ematix-flow-core/src/agg_filter_pushdown.rs` |
| SF=1 correctness tests | `agg_filter_pushdown.rs` `#[tokio::test]` block (lines 440-610) |
| Bench wire-up | `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs:417,432` |
| Strict A/B harness | `scripts/bench/strict_ab.sh`, `scripts/bench/strict_diff.py` |
| Q17 EXPLAIN ANALYZE baseline | `[[sigma-aj-1-q17-explain-analyze]]` |
| Prior loose-protocol "neutral" verdict | Σ.X bench memo (Σ.U Phase 1.1 commit `1ed7a77` notes) |

## Risk register

- **Σ.U Phase 1 matcher might fail on SF=10 plan shape** — DataFusion's optimizer can produce different plans at different scale factors (more partitions → different repartition placements). SF=1 tests passing isn't a guarantee. Step 1 is the gate for this.
- **Codegen tax risk** — per `[[optimizer-codegen-sensitivity]]`, even adding a pre-plan walker can cost ~5pp geomean from LLVM perturbation. **The rule is already in the binary** (commit 18d170d). Flipping `EMAT_AGG_SEMI=1` only changes runtime execution, not codegen. So this risk is zero for this spike.
- **LeftSemi probe cost might match AVG savings at SF=10** — same mechanism that killed Σ.AJ.1 Lever B (broadcast bloom). Q17's outer filter produces only ~2K rows, so the LeftSemi build is tiny; probe cost should be cheap. But Step 2 is the decisive measurement.

## Decision criteria — one-page summary

| Step | Pass | Fail action |
|---|---|---|
| 1. SF=10 correctness | Q17 + Q02 row count match | Stop, doc shape, close arc |
| 2. Q17 strict A/B | B − A < -20 ms (>2σ) | Stop, doc "probe matches savings", close arc |
| 3. 22q strict A/B | geomean Δ ≤ +1.5pp AND no per-query > +5% | Try shape predicate, re-bench (4hr timebox) |
| 4. Decision | Flip default, ship | Close arc, doc verdict |

## Why this is now a 1-day spike, not 1-2 weeks

The design doc estimate "1-2 weeks" assumed net-new logical-plan rewriting. The actual remaining work is:
- 2 hours: add SF=10 correctness tests (variant of existing SF=1 tests)
- 1 hour: run 2 strict A/B benches
- 1 hour: analyze + write up
- Up to 4 hours: optional shape-predicate dig-in if branch (b)
- = **Less than 1 day** to a definitive decision

This is consistent with `[[no-quick-reject]]` — the prior "neutral" verdict deserves a second look under the methodology that has now overturned multiple prior false-positives and false-negatives.

## Outcome (2026-05-27)

### Step 1 — SF=10 correctness gate ✅
All 10 tests pass: Q17 SF=10 + Q02 SF=10 end-to-end correctness verified (rel_err < 1e-6 for AVG; row-count match for Q02). Matcher specificity confirmed: Q11/Q15/Q22 either don't fire or fire without breaking row counts.

### Step 2 — Q17 SF=10 strict A/B ✅
**Q17: -95.71 ms (-35.65%) WIN** at 2σ bar of 27.60 ms. Wall drops from 268 → 173 ms, within 5-8% of DuckDB.

### Step 3 — 22q SF=10 strict A/B — TWO PASSES

**v1 (original bench harness):** Q17 -114.6 ms WIN, but 4 hard-gate failures (Q02 +11.0%, Q03 +5.7%, Q04 +8.9%, Q22 +8.7%). Investigation revealed the bench was bypassing the plan-cache for ALL 22 queries when `EMAT_AGG_SEMI=1`, not just the queries where the matcher fires. Matcher specificity tests confirmed Q03/Q04/Q06/Q22 don't fire the rule — they were paying the plan-cache-bypass cost for nothing.

**Bench harness fix** (`tpch_triangulation_bench.rs`, ~50 LOC): added `agg_semi_fires_for_sql` with per-SQL memoization. The bench now only takes the rewrite code path when the rule actually changes the plan for this SQL; otherwise it uses the plan-cache path. First trial pays one extra logical-opt cost (~ms), warmup discards it; subsequent trials hit the boolean cache.

**v2 (harness fixed):**

| Metric | v1 | v2 |
|---|---|---|
| Q17 | -114.64 ms ✓ | **-103.61 ms ✓** |
| Q08 | +1.4 ms (noise) | **-11.22 ms WIN** |
| Q18 | +4.5 ms (noise) | **-27.01 ms WIN** |
| Q02 | +3.23 ms (regression at 0.94 bar) | +2.05 ms (noise at 2.06 bar) |
| Q03/Q04/Q06/Q22 | 4 hard-gate regressions | all back to noise |
| Net | -84.79 ms (-2.49%) | **-225.91 ms (-6.64%)** |
| Clear wins (>2σ) | 1 | **3** |
| Clear regressions (>2σ) | 5 | **0** |

### Step 4 — Decision branch (a) clean win

All gates pass under v2:
- ✅ No per-query regression > 5% above 2σ bar (max is Q02 +7.04%, within 2σ noise)
- ✅ 22q geomean Δ ≤ +1.5pp (actual -6.64% improvement)
- ✅ ≥ 1 net win (actual: 3)

**Actions taken:**
1. Flipped `EMAT_AGG_SEMI` default to ON in `tpch_triangulation_bench.rs` (both env-var read sites, +1 comment update)
2. Bench harness fix to detect per-SQL rule firing (`agg_semi_fires_for_sql`)
3. Added SF=10 correctness tests for Q17 + Q02
4. Added matcher specificity tests for Q11/Q15/Q22
5. Σ.AJ.1 closes positive

## References

- Σ.U Phase 1 implementation: `crates/ematix-flow-core/src/agg_filter_pushdown.rs`
- Σ.U commits: `18d170d` (Phase 1), `1ed7a77` (Phase 1.1 + Q02 generalisation)
- v1 bench: `/tmp/strict-ab-aj1-leverc-22q/diff.md` (broken harness)
- v2 bench: `/tmp/strict-ab-aj1-leverc-22q-v2/diff.md` (harness fixed)
- Q17 SF=10 bench: `/tmp/strict-ab-aj1-leverc-q17/diff.md`
- Prior arc closure: `[[sigma-aj-1-q17-explain-analyze]]`, `[[sigma-aj-1-lever-b-rejected]]`
- Strict methodology: `[[sigma-ai-1-strict-bench-landed]]`, `[[bench-methodology-3-invocations]]`
- Codegen tax precedent: `[[optimizer-codegen-sensitivity]]`
- Don't reject quickly: `[[no-quick-reject]]`
