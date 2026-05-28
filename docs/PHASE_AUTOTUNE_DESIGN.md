# Σ.AΩ — Adaptive autotune program design

**Status:** design draft, 2026-05-28.
**Scope:** multi-month initiative; supersedes Σ.AN.1's narrow per-operator partition routing failure.
**Prerequisites:** `[[autotune-program-deferred]]` (banked); Σ.AN.0 finding (`[[sigma-an-partitions-shape-dependent]]`); Σ.L.1 speculative-race (seed pattern).

## What this is

Every performance-relevant decision we make today is a hardcoded constant tuned on 1-2 queries and frozen. The Σ.AN.0/AN.1 work surfaced a concrete instance: `target_partitions = 14` (cores) and the 8× ceiling we'd want to apply to Q18 are both wrong for most queries — the right number is workload, plan-region, and hardware dependent.

The autotune program replaces those static constants with adaptive decisions informed by:
- **Plan-time signals** — cardinality estimates, predicate selectivity, schema stats
- **Runtime signals** — observed group counts, cache miss rates, repartition queue depths, scan throughput
- **Historical signals** — `[[sigma-l2-adaptive-runtime]]`'s SQLite per-shape outcomes

Same architectural pattern as Σ.L.1 speculative-race (which already does this for one decision — dict-arrival probing). Generalize that to the full set of currently-hardcoded gates.

## Why this is the right next strategic investment

The session's lever-chain has produced ~300 ms / -9% on 22q SF=10 by surfacing dormant rules and adding shape predicates. The remaining gap to DuckDB (15/22 wins, ratio 0.70× on Q18) is increasingly structural — the dominant cost is the SUM hash agg at 15M cardinality, which Σ.AN.1 demonstrated can't be solved by a single hardcoded knob.

Looking at the surveyed gates: every single one (`target_partitions`, `min_probe_to_build_ratio`, `expected_keys_per_partition`, `max_leaves`, `50K target groups`, `8× cores ceiling`) is currently a magic number. Even moving one is potentially worth -10-50 ms. Moving all of them adaptively, with per-query/per-region calibration, is potentially worth a substantial perf step-function.

This is also what DuckDB, Photon, and Spark's Adaptive Query Execution all do internally. We're behind on this surface.

## The decision surface

Static heuristic constants surveyed in our codebase (full list in `[[autotune-program-deferred]]`):

| Decision | Current default | Source |
|---|---|---|
| `target_partitions` | cores (= 14) | session config |
| L9 `min_probe_to_build_ratio` | 64 | runtime_bloom_sideband_rule.rs |
| L9 `expected_keys_per_partition` cap | 25K | same |
| L9 `build_rows` fallback | 50K | same |
| L9 `max_expected_keys_per_partition` | 0 (disabled) | Σ.AH.3 Story 2a |
| Σ.AM.1 `build_rows` cap when semi | 10K | same |
| Reorder `max_leaves` | 4 | join_reorder.rs |
| Σ.AN.1 target groups/partition | 50K | this session |
| Σ.AN.1 max partitions multiplier | 8 | this session |
| BuildSideBloomEmitter min per-partition | 64 | build_side_bloom_emitter_exec.rs |

Plus all of DataFusion's internal `EnforceDistribution` heuristics (which we currently inherit unchanged).

## Architectural integration points

### 1. EnforceDistribution is the partition-count surface

DataFusion's `EnforceDistribution` rule makes partition-count decisions during physical optimization. Our `SessionStateBuilder::with_default_features()` already includes it; we don't customize it. The autotune program needs to either:

- **Wrap it** with a version that consults a model for partition-count decisions
- **Replace it** entirely for ematix-flow workloads
- **Run before it** to set hints that bias its decisions (e.g., raise `target_partitions` per-query)

The wrap/replace approach is the right long-term play but is intrusive (~weeks).

The "run before it" approach is tractable (~days per signal). E.g., a pre-EnforceDistribution rule that detects high-cardinality agg shapes and increases the session's effective `target_partitions` for that query.

### 2. The L9 + bloom layer has its own gates

Σ.S.B cascading L9 already runs after EnforceDistribution. Its `min_probe_to_build_ratio` and other gates are independent of EnforceDistribution. These can be autotuned via either:

- Plan-time shape predicates (cheap, no runtime feedback needed)
- Per-(shape, scale) lookup table populated from historical bench data
- Speculative-race style (Σ.L.1 pattern) where both gate settings race

### 3. The Σ.L.2 SQLite workload database is already in place

Per memory `[[sigma-l-adaptive-runtime]]`: "Σ.L.2 (2026-05-21): adaptive runtime workload feedback. Persists per-shape probe outcomes + per-query observability (selectivity, hash collision rate) to a SQLite file (~/.ematix/workload.db by default)."

This is the foundation. The autotune program builds layers on top:
- Read historical decisions
- Decide whether to speculate / fall back
- Record outcomes for the next session

## Three-phase rollout

### Phase 0 — Catalog + measurement infrastructure (1 week)

- Survey every hardcoded numeric gate in the codebase (started in `[[autotune-program-deferred]]`)
- For each gate: capture current default, sensitivity (does changing it matter?), measurement plan
- Build a uniform A/B sweep harness — extend `strict_ab.sh` to do sweeps automatically across gate values × queries × scale factors
- Output: a "decision-surface catalog" that says, for each gate, the per-query optimum at SF=1 and SF=10

This is the precondition for everything else: we need to know the SHAPE of the function we're trying to learn before we can build a learner.

### Phase 1 — Plan-time shape predicates for the highest-value gates (2-3 weeks)

For the 3-5 gates the catalog identifies as highest-leverage, implement plan-time predicates that pick a per-query value:

- `target_partitions`: high-cardinality agg detection → boost session config before EnforceDistribution
- L9 `min_probe_to_build_ratio`: per-(probe-table-size, build-shape) lookup
- Reorder `max_leaves`: per-query based on chain shape

These are NOT runtime adaptive — they're better static heuristics. Like Σ.AK / Σ.AL / Σ.AM.1 — shape-gated opt-in initially, default-on after strict A/B validation.

### Phase 2 — Runtime feedback integration (3-5 weeks)

Wire each plan-time decision to record its outcome in the Σ.L.2 SQLite. On the next query with similar shape:
- If the speculation was good → repeat
- If it was bad → try the other value
- If consistently bimodal → speculative-race (Σ.L.1 pattern)

Failure recovery: if a query is N× slower than its historical median, abort and re-plan with default values.

### Phase 3 — Cross-host calibration (deferred, 1-2 months)

Different hardware has different optima (M3 Pro L3=12MB vs Xeon L3=30MB vs cloud VM). Phase 3 generalizes the catalog to per-host calibration:

- One-time calibration suite at install (~10 min of representative workloads)
- Periodic recalibration if hardware changes detected
- Per-host historical database

## Risks and trade-offs

### Risk: complexity explosion

The autotune surface is large. Without a clear catalog (Phase 0), we'd over-engineer for unmeasured needs. Mitigation: STRICT phase gating. Phase 1 only attacks 3-5 gates the catalog says are high-leverage; defer the rest.

### Risk: codegen tax from speculative paths

Per `[[optimizer-codegen-sensitivity]]`: adding any new optimizer rule can cost 5-8 pp geomean from LLVM perturbation. Multiple autotune rules compound this. Mitigation: where possible, run autotune logic in EXISTING rules' bodies instead of adding new rules.

### Risk: prod debuggability

If a query is running unexpectedly slow, "the autotuner decided X based on history" is harder to debug than "the rule has a hardcoded 64." Mitigation: comprehensive observability — every autotune decision must be loggable with the reasoning chain.

### Risk: regressions on familiar queries

If autotune changes the default plans for queries we've already tuned (Q17, Q18, etc.), the carefully-tuned current state could regress. Mitigation: regression test suite that locks expected plans / wall times for the 22 TPC-H queries at SF=1 + SF=10.

## What this is NOT

- Not adaptive query execution in the Spark sense (re-planning during execution). That's a much bigger undertaking.
- Not cost-based optimization replacement. DataFusion's CBO stays; we're refining the constants it uses.
- Not a replacement for Σ.L.1's speculative-race. That's a building block we extend.
- Not multi-host distributed autotune. Per-host only initially.

## Concrete first deliverable proposal

**Phase 0, Week 1**: a `decision-surface-catalog.md` that lists every hardcoded numeric gate in the codebase, with:

| Column | Content |
|---|---|
| Gate | Symbol, file:line |
| Current value | Default |
| Variable via | Env var if any |
| Sensitivity hypothesis | "expected to matter for queries with high X" |
| Measurement plan | "sweep 4 values × 22q × SF=10, strict A/B" |
| Estimated effort to autotune | "1 day / 1 week / multi-week" |

This document is the prereq for Phase 1 — it tells us which 3-5 gates are worth attacking first.

## References

- Banked memo: `[[autotune-program-deferred]]`
- Seed pattern: `[[sigma-l1-speculative]]`, `[[sigma-l-adaptive-runtime]]`
- Σ.AN.1 negative result (why per-operator alone fails): `[[sigma-an-q18-diagnosis]]` follow-up (pending commit)
- DataFusion `EnforceDistribution`: `datafusion-physical-optimizer-53.1.0/src/enforce_distribution.rs`
- Codegen tax risk: `[[optimizer-codegen-sensitivity]]`
- Shape catalog direction: `[[shape-catalog-autotune-direction]]`
