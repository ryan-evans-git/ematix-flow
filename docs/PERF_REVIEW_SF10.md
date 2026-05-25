# TPC-H SF=10 stage-profiling review — Q01–Q22 summary

Done 2026-05-25. Methodology: `docs/STAGE_PROFILING_METHODOLOGY.md`. Per-query writeups: `docs/PERF_Q01.md` ... `docs/PERF_Q22.md`.

## Headline

| Bucket | Count | Queries |
|--------|------:|---------|
| Win >20% over DuckDB | 9 | Q02, Q04, Q09 (partial), Q10, Q11, Q13, Q14, Q19, Q21, Q22 |
| Win 5–20% over DuckDB | 6 | Q01 (post-fix), Q03, Q09, Q12, Q15, Q16, Q20 |
| Parity (±5%) | 2 | Q06, Q18 |
| Lose >5% to DuckDB | 4 | Q05 (-28%), Q07 (-15%), Q08 (-14%), Q17 (-22%) |

Post-fix 22q SF=10 geomean ratio ematix/DuckDB ≈ **0.74** (already established baseline) + StringView fix gave us another 2.6 pp.

## At-floor queries (no realistic lever)

| Query | Floor | Actual | Note |
|-------|------:|-------:|------|
| Q06 | ~16 ms | 72 ms | At DuckDB parity (71 ms); Polars 11% ahead via faster Snappy. Documented in [[q06-sf10-polars-gap-wall]]. |
| Q11 | ~17 ms | 12 ms | Below floor. |
| Q14 | ~17 ms | 84 ms | At decode floor per [[q14-decode-floor]]. |
| Q22 | ~28 ms | 24 ms | Below floor. StringView fix's biggest -20% beneficiary. |

## Cross-query waste patterns

Patterns recurring across ≥3 queries:

### Pattern A — BridgeFilter pattern matcher gap

BridgeFilter (filter pushdown into the scan) handles:
- Single-column equality ✓ (Q10 l_returnflag='R')
- Single-column range ✓ (Q01 l_shipdate <=, Q06 between)
- DICT-aware equality ✓ (Q12 partial)

But misses:
- `a >= x AND a <= y` (BETWEEN) — Q03, Q07, Q08, Q09
- Two-column compare `a > b` — Q04, Q12, Q21 (×2)
- OR-of-AND disjunctive — Q19 (partial pushdown only)

**Impact**: 9 queries scan full lineitem (60M rows decoded) where they could decode only the matching subset (~15-50% of rows depending on selectivity).

**Affected**: Q03, Q04, Q07, Q08, Q09, Q12, Q19, Q21 (×2 scans), Q17 (×2 scans).

### Pattern B — L9 build-side bloom doesn't propagate across plan-tree barriers

L9 fires on the immediate HashJoinExec's build → probe scan edge. It doesn't propagate:
- Through decorrelated subqueries on the same fact table (Q17 sums lineitem twice; only one gets bloom)
- Through multiple Inner joins to the deepest fact-scan (Q05 cust+orders → lineitem)
- Through LeftSemi-pushed shapes (Q18 lineitem main probe is unfiltered)

**Affected**: Q05, Q08, Q09, Q17, Q18 — all are queries where we currently lose or are at parity vs DuckDB.

This is the biggest single payoff lever in the suite — and the hardest.

### Pattern C — Compound-key aggregation lacks specialized path

`RobinHoodSumF64Exec` handles single i64 key + SUM(f64). DataFusion's default `AggregateExec` handles everything else — at ~10-25× the per-row cost.

Affected:
- Q09 `gby=(nation, o_year)` — 64 ms compute on 2450 groups
- Q10 `gby=7 customer cols` — 150 ms on 482k groups
- Q20 `gby=(l_partkey, l_suppkey)` — 380 ms on 5.44M groups (largest)

### Pattern D — SIMD LIKE kernel exists but isn't wired

Memory [[sigma-e5-like-kernel]] shipped a Photon-style SIMD substring kernel (9-14× over std). Currently dormant. Q13's NOT LIKE filter is the textbook case at 444 ms compute; wiring up would shave ~30% wall.

**Affected**: Q13 primarily; smaller impact on Q16, Q09, Q22.

### Pattern E — Multiple identical scan+filter subtrees

Several queries scan the same table 2-3 times with identical projection + filter:
- Q02: 2× partsupp
- Q11: 2× partsupp
- Q17: 2× lineitem (same partial projection)
- Q18: 2× lineitem
- Q21: 3× lineitem (2 identical, 1 with narrower projection)

`SharedSubtreeExec` ([[sigma-p-subquery-cse]]) exists for the Q15 correlated-subquery shape. Generalising to identical-subtree CSE would deduplicate these.

## Ranked lever list (by impact × effort)

| Rank | Lever | Affects | Estimated effort | Expected geomean win | Risk |
|------:|:------|:--------|:----------------|---------------------:|:-----|
| 1 | BridgeFilter: BETWEEN + 2-column compare | Q03, Q04, Q07, Q12, Q21 | 1-2 wk | -8 to -12% | Low — known pattern. |
| 2 | SIMD LIKE wire-up | Q13, Q22, Q16 | ~1 wk | -3 to -5% | Low — kernel already exists. |
| 3 | L9 propagation across subqueries | Q05, Q08, Q09, Q17, Q18 | 4-6 wk | -10 to -15% | Medium — past attempts ([[sigma-sb-cascade-neg]]) had neutral results; need rule-narrowed scope. |
| 4 | Compound-key Robin Hood SUM/AVG (2× i64) | Q09, Q20 | 2-3 wk | -3 to -5% | Low — extends existing kernel. |
| 5 | CSE for identical scan+filter subtrees | Q02, Q11, Q17, Q18, Q21 | 2-3 wk | -2 to -4% | Medium — careful plan-rewrite. |
| 6 | Group-by functional-dependency simplifier | Q10 | 2-3 wk | -1 to -2% | Medium — needs FK/PK metadata path. |
| 7 | Q05 join-reorder via CBO | Q05 | Multi-month | -5% Q05 specifically | High — multi-quarter effort. |
| 8 | RG decode cache bytes default bump | Q12, Q08 (variance) | 1 day | -1 to -2% | Low — config-only. |

## What we landed today

1. **StringViewArray `new_unchecked` fix** ([emat_arrow_reader.rs:2733](crates/ematix-flow-core/src/emat_arrow_reader.rs:2733)): -7% Q01, 22q geomean post/pre 0.9736 (-2.6%).
2. **Per-stage profiler** at [stage_profiler.rs](crates/ematix-flow-core/examples/stage_profiler.rs): reusable for future surveys.
3. **22 query writeups** at `docs/PERF_Q01.md` ... `docs/PERF_Q22.md` with stage tables, theoretical floors, ranked candidates.

## Recommendation for next milestone

Two paths forward, both within the survey's findings:

### Path A — "BridgeFilter sweep" (1-2 weeks)
Land lever #1 (BETWEEN + 2-col compare in BridgeFilter). Single change. Closes scan-decode waste on 5+ queries simultaneously. **Lowest-risk, fastest payoff.** Expected geomean: 0.74 → ~0.66.

### Path B — "L9 generalisation" (4-6 weeks)
Land lever #3 (cross-subquery L9 propagation). Bigger impact but past attempts ([[sigma-sb-cascade-neg]]) were neutral — needs careful rule scoping. **Higher upside, higher risk.** Expected geomean: 0.74 → ~0.62 if it lands cleanly.

**Recommended sequence: A first** (de-risks the BridgeFilter pattern matcher work which we'll need anyway for B), then B once we have evidence the pattern-match infra is solid.

Both paths preserve the survey discipline — finish the gates that exist today (no V5/L13-style multi-week-then-revert cycles).
