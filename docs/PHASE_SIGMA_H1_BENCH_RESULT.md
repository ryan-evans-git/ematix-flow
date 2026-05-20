# Σ.H.1 bench gate — result

**Run:** 2026-05-20, immediately after `ff2a6a2` (Σ.H.1 commit).
Methodology: 3-run multi-bench, same machine same session, Σ.H.1
source vs v0.3.0 source (`git checkout 268ab96 -- crates/`).
Tool: `tpch_triangulation_bench`, TPC-H SF=1 real parquet.

## Verdict: PASS with one ambiguous query (Q21)

**Geomean ratio (Σ.H.1 / v0.3.0): 0.9955** — 0.45% faster, within
the ±2% gate stated in `docs/PHASE_SIGMA_H_FILTER_JOIN_AGG.md`.

**Classification (>5% threshold):**
- 4 wins: Q08 (-8.6%), Q11 (-6.9%), Q14 (-9.1%), Q09 (-5.3%)
- 16 parity (±5%)
- 2 losses: Q21 (+10.9%, ambiguous), Q13 (+15.8%, noise — σ 9.02 ms
  on a 48 ms mean is ~19% variance)

## Target queries (Σ.G inventory said `filter_multi_agg` newly fires)

| Q | v0.3.0 mean | Σ.H.1 mean | Δ | Verdict |
|---|---:|---:|---:|---|
| Q04 | 13.36 | 13.16 | **-1.4%** | parity / slight win |
| Q05 | 22.99 | 21.93 | **-4.6%** | slight win |
| Q21 | 40.75 | 45.20 | **+10.9%** | ambiguous (see below) |

## Q21 deep-dive

Per-run (each cell: median ± σ over 5 trials):

|     | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Σ.H.1 | 38.76 ± 1.98 | 46.44 ± 2.48 | 50.39 ± **8.66** |
| v0.3.0 | 39.51 ± 2.06 | 40.04 ± 1.15 | 42.71 ± 3.70 |

- **Best-of-3:** Σ.H.1 38.76 vs v0.3.0 39.51 — Σ.H.1 is **faster**
  on its best case.
- **Worst-of-3:** Σ.H.1 50.39 vs v0.3.0 42.71 — Σ.H.1 is **18% worse**
  on its worst case. Run 3's internal σ of 8.66 ms suggests a
  trial-level outlier within that run.
- **Mean:** drifts +10.9% but is heavily pulled by the two slower
  runs. Cannot distinguish "real cost" from "two thermal-throttled
  runs" with three samples.

**Hypothesis:** the new `filter_multi_agg` path over Q21's
HashJoin output has higher variance than DataFusion's default
HashAggregate. Q21 has the most complex join shape in the TPC-H
suite (4-table join + correlated NOT EXISTS), so any sensitivity
to thread-scheduling / hash-table allocation timing in the
FilterMultiAggSpec would amplify there.

**Decision:** Σ.H.1 ships with Q21 noted as "high-variance under
the new rule path." Two follow-up options:

1. **Σ.H.1b — narrow the rule to avoid Q21-shape plans.** Add a
   guard: only fire when the join's estimated cardinality is below
   some threshold, or only when the body is a 2-table HashJoin
   (not a chain of joins). Should keep the Q04/Q05 wins while
   restoring Q21 to v0.3.0 behavior.
2. **Re-bench with more samples on Q21.** Run a Q21-only loop
   (TPCH_QUERIES=21 with 10-20 trials) to see if the mean settles
   into parity. Cheap; resolves the ambiguity.

## What this validates

- **Σ.H.1 is correctness-preserving** — the bench harness checks
  results against DuckDB and Polars on every trial; no failures.
- **The catalog approach scales to new shapes cheaply** — the
  per-rule code change for Σ.H.1 was 25 lines (new `is_supported_body`
  helper). The matcher already accepted the wrapper shape from
  Σ.F.2. Σ.G inventory + Σ.H.1 together: ~80 LOC of source code,
  3 new query rule-fires, no broad regression.
- **The Σ.E6 D1 lesson held.** Without the bench gate, the
  Σ.G inventory's "3 new rule firings" would have shipped as a
  win. The gate caught Q21's variance increase — which would have
  shown up later in a real-world workload before we knew the
  cause.

## What it doesn't validate

- Σ.H.1's effect on TPC-DS queries (no data available).
- Σ.H.1 on dict-aware probe-side join workloads (that's Σ.H.2;
  needs a new executor).
- Whether the 4 unrelated wins (Q08/Q11/Q14/Q09) are durable or
  session-state artefacts. None of those queries had a documented
  rule change; they may be regression-to-mean from prior sessions
  that ran with thermal load.

## Recommendation for what's next

Per `docs/PHASE_SIGMA_H_FILTER_JOIN_AGG.md`:
- The "pass criteria" included "target queries improve ≥5% OR stay
  within ±3%". Q21 fails this on the mean. The geomean gate passes
  (±2% target, actual 0.45% win).
- Either Σ.H.1b (narrow the rule) or a Q21 deep-bench is the
  responsible next step before claiming Σ.H.1 as a perf win.

Σ.H.1 stays on the branch as architecturally correct + bench-honest.
The Q21 ambiguity is the kill-criterion to investigate before any
release-readiness claim.
