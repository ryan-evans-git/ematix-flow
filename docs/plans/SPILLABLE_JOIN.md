# Σ.SP — Spillable-join arc: grace-partitioned join demotion

Status: **Phase 0 (design + box re-baseline), 2026-07-11.** Successor to
the "paging arc" in [MEMORY_BUDGET.md](MEMORY_BUDGET.md) — that doc's
decision gate said *reassess after the next DataFusion upgrade*; the
reassessment is done and recorded here.

## Why now, and what changed since MEMORY_BUDGET.md

The paging arc was framed when Q09 thrashed and parted-SF100 tar-pitted.
Both of those are gone by other means:

- **Q09 is solved at the plan level** (Σ.JS.1/Σ.JS.2, PRs #176/#178):
  honest join-side estimates keep builds small; SF=100 flat suite 51.8 s.
- **The parted tar pit was scan-width, not paging** (Σ.MW.1, PR #179):
  the multi-file union multiplied decode width by the part count. With
  the budget split, the full parted single-node SF=100 suite completes
  (47.7 s / 22.4 GB peak on the 36 GB dev box — first complete run
  anywhere; 32 GB box validation pending, Phase 0).

What remains for THIS arc is the last structural weakness: **oversized
tracked join builds cannot yield memory.** DF 53's `HashJoinExec` cannot
spill; under a cap it deadlocks (the refuted 0.7×RAM blanket cap), and
against the `ElasticFloorPool` it fails the query cleanly at best,
kernel-OOMs at worst when the growth is bursty. Today's exposure:

- Q18/Q21-class plans (giant semi/anti/inner builds) ride the margin on
  32 GB boxes: ~28 GB peaks that pass or die on page-cache luck.
- Any hostile/ad-hoc workload (not TPC-H) can synthesize a build larger
  than RAM and there is no graceful degradation.

## Upstream status (checked 2026-07-11)

- Spillable hash join has **not landed through DataFusion 54.0.0**
  (June 2026, latest). The proposal epic is
  [apache/datafusion#17267](https://github.com/apache/datafusion/issues/17267)
  (hybrid hash join with greedy partition spilling) — design stage,
  "not yet fully underway". apache/datafusion#12952 and #1599 are the
  long-running asks.
- Conclusion per the MEMORY_BUDGET decision gate: build in-repo, keep
  it a *demotion* (stock plans untouched unless the estimate says
  danger), and treat upstream as the eventual replacement.

## Design: plan-time demotion, not runtime panic

The upstream proposal spills *reactively* (notice memory pressure
mid-build, spill largest partitions). We have something DF doesn't: the
Σ.JS.2 **grounded bottom-up row/byte estimates** at plan time. That
enables the simpler, more predictable half of the design space:

**A physical rule (`GraceJoinDemotionRule`, after Σ.JS.1 in the preset
chain) rewrites a `HashJoinExec` whose GROUNDED build-side estimate
exceeds a budget into a `GraceHashJoinExec`.**

- **Budget:** `EMAT_GRACE_BUILD_BYTES` tri-state. AUTO = demote when
  estimated build bytes > `f × sensed MemAvailable at plan time`
  (start f = 0.5; the Σ.AI.6c sensor already exists and is cached).
  Ungrounded estimates never demote (same honesty rule as Σ.JS.1 —
  no decision without proof), so TPC-H flat plans are untouched:
  bench == release means banked numbers cannot move.
- **Operator (`GraceHashJoinExec`):** classic grace hash join.
  1. Partition BOTH inputs to `K` spill files by `hash(join keys)`
     (reuse DF's IPC spill-file machinery used by sort/agg spills).
  2. Join partition pairs sequentially with a stock in-memory
     `HashJoinExec` over per-partition record-batch streams; a pair
     whose build still exceeds the budget recurses with a different
     hash seed (bounded depth, then hard error).
  3. `K` = ceil(estimated build bytes / (budget / 2)) rounded to a
     power of two, min 4.
- **Join types, phased:** Inner first (Phase 1); LeftSemi/LeftAnti
  (the Q21 shape) in Phase 2; outer joins only if evidence demands.
- **Memory accounting:** partition write buffers + the per-pair build
  reserve from the session pool, so the ElasticFloorPool sees the
  truth. Spill bytes/files surfaced as metrics
  (`grace_partitions`, `grace_spill_bytes`, `grace_recursions`).

### Why demotion instead of a spilling retrofit of HashJoinExec

- The refuted-cap evidence shows mid-build reactive spilling under
  DF 53's scheduling is deadlock-prone (concurrent reservations wait on
  each other). A demoted plan never enters that regime: partitioning is
  streaming with O(K buffers) memory, and each pair-join is sized to
  fit by construction.
- SMJ demotion was already tried and refuted (MEMORY_BUDGET evidence
  table: SMJ+cap = `ResourcesExhausted`, SMJ unbounded = kernel OOM).
- Plan-time demotion is testable deterministically (feed the rule a
  plan with grounded stats; assert the rewrite), matching the Σ.JS test
  discipline.

## Phases

- **Phase 0 — re-baseline (cheap, decisive).** One 32 GB box run of the
  post-Σ.MW.1 parted single-node suite. If Q18/Q21 now pass with the
  width fix, the arc's default posture can stay conservative (demotion
  fires only on genuinely-oversized builds, expected: none in TPC-H).
  Exit: banked baseline + updated exposure list.
- **Phase 1 — Inner grace join behind `EMAT_GRACE_JOIN` (default OFF).**
  Operator + demotion rule + unit/property tests (result parity vs
  stock join across seeds/partition counts, spill-file cleanup,
  accounting) + a synthetic oversized-build workload that OOMs stock
  and completes demoted.
- **Phase 2 — semi/anti support + full-suite A/B.** The 22q protocol on
  the 32 GB box, flat + parted, default-ON decision by the same rule as
  every memory lever: zero regression on healthy runs, graceful
  degradation on pressure runs.
- **Phase 3 — upstream watch.** When apache/datafusion#17267 lands,
  benchmark theirs vs ours; prefer deleting ours.

## Non-goals

- Replacing the ElasticFloorPool (it stays as the last-resort guard).
- Spilling aggregates/sorts (DF already does; multi-level merge since
  DF 50).
- Symmetric/streaming joins.
