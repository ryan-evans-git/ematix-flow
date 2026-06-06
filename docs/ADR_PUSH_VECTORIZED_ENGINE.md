# ADR: Push-vectorized (morsel-driven) execution capability for ematix-flow

- **Status:** PROPOSED — draft for human decision (measurement-first perf owner). NOT accepted; Phase 0 is a kill-gate.
- **Date:** 2026-06-04
- **Author:** architect (cold-start design pass)
- **Reviewers:** TBD (perf engineer / owner)
- **Decision scope:** ONE decision — whether to add an *internal* push-based fused-pipeline operator (a hybrid that slots into DataFusion's physical plan as a single `ExecutionPlan` node) to attack the Q08-class HashJoin materialization tax. Full volcano replacement is weighed and **rejected** as an alternative, not proposed.
- **Supersedes / relates to:** `docs/ADR_SELECTION_VECTOR_MATERIALIZATION.md` (OPEN). That ADR attacks the *scan-side* filtered-gather plateau (Q07's masked-decode `take`). This ADR attacks the *operator-boundary* materialization between scan and join (Q08's `take`/repartition tax). They are siblings: both are facets of "DataFusion's pull model compacts a `RecordBatch` at every boundary." If both are pursued, the push-pipeline operator here is the natural host for that ADR's selection-vector envelope. Do not pursue them as two independent engines.

---

## 1. Context

### 1.1 What ematix-flow is, mechanically

ematix-flow is a Rust analytical engine built **on** DataFusion 53.1.0. It does not replace DataFusion's execution model; it injects custom physical operators into the optimized plan. The model is **pull-based (volcano-batch)**: `ExecutionPlan::execute(partition) -> SendableRecordBatchStream`, and each operator pulls `RecordBatch`es from its child stream. Verified in-tree:

- `EmatixFastParquetExec::execute` (`crates/ematix-flow-core/src/ematix_fast_parquet.rs:2510`) returns a `RecordBatchStreamAdapter` (`:2589`, `:2698`); the scan decodes a row group and emits dense `RecordBatch`es.
- `EmatixHashJoinExec::execute` (`crates/ematix-flow-core/src/emat_hash_join_exec.rs:146`) collects the build side once into a shared `OnceCell<Arc<EmatHashJoiner>>` (`:163`), then `.map`s each probe batch through `joiner.probe(&rb)` (`:188`).
- `EmatHashJoiner::probe` (`crates/ematix-flow-core/src/emat_hash_join.rs:100`) probes the L13 RobinHood kernel, collects `Vec<ProbeMatch>`, builds two `UInt32Array` index vectors, then calls Arrow `take` per output column (`:113-114`) and assembles a fresh output `RecordBatch` (`:119`). **This is the materialization tax in code.**

The optimizer-rule layer that injects these operators is rich and already shipped: see `with_optimizer_rules_and_registry` (`crates/ematix-flow-core/src/preset.rs:125`) — `ForceCollectLeftForSemiBoundedBuildRule`, `EnableRuntimeBloomSidebandRule` (the L9 sideband), the RobinHood agg rules, plus a `FlowQueryPlanner` (`:218`) that runs pre-plan walkers (join-reorder / dim-push / agg-semi) **outside** the optimizer's compiled rule loop, explicitly to dodge the codegen tax (§1.4).

### 1.2 The competitive position (so we don't over-invest)

- 22 TPC-H queries pass row-for-row vs DuckDB at SF=1, SF=10, SF=100.
- 22q **SF=10 geomean 0.80** vs DuckDB (ematix ~20% faster overall); SF=1 ematix beats DuckDB **2.7×** (`project_mimalloc_production_gap.md`, scaling table).
- The engine is **already winning**. This ADR targets a small number of SF=10 outliers, chiefly **Q08 (~7-13% slower than DuckDB)**. That asymmetry — winning broadly, losing on one shape at one scale factor — bounds how much risk is rational to take (see §1.6 and Consequences).

### 1.3 The Q08 bottleneck, as measured (not hypothesized)

Q08 SF=10 has been profiled across **seven** independent investigations (REV.19/20/21/23, the mimalloc-gap session, the Q08-probe-kernel session). The findings that constrain this design, with their banked sources:

1. **It is NOT the probe kernel.** A dense-bitset probe measured **10.8× faster than open-addressing in microbench** (0.60 ns vs 6.49 ns/probe) and was wired in end-to-end. Q08 wall went **flat-to-negative** (188→212 ms). Root-cause sample of the dense run: `i64_set::contains` = **6 samples (~0%)** of wall. (`project_mimalloc_production_gap.md`, "Follow-up dig".) The `EmatixHashJoinExec` swap (the L13 kernel, claimed 2.36× in microbench) likewise moved Q08 wall **flat** (178.8→183.8 ms); in the batched 99.3%-miss operator regime the probe is ~parity, not 2.36×. (`project_q08_join_probe_kernel_2026_06.md`, "DIG RESULT".)

2. **It IS the materialization / row-volume tax.** Cross-engine summed-work decomposition (`project_q08_join_probe_kernel_2026_06.md`):
   - lineitem scan/decode: ematix **757 ms** vs DuckDB 1310 ms → **ematix is faster at decode.**
   - part⋈lineitem join: ematix **1130 ms** vs DuckDB 120 ms → the **entire** deficit.
   - ematix probes **60M** lineitem rows; DuckDB pushes the ~13.45K filtered `p_partkey`s into the scan (sideways information passing) and probes **~37K**. The cost that dominates is Arrow `take`/repartition (2875 samples) + HashJoin pair-production (1627) — i.e. **building fresh `RecordBatch`es per boundary**, not membership testing.

3. **The "dynamic filter" shortcut DuckDB uses does NOT close it in our pull model.** REV.21 forced the L9 sideband to emit an **exact** `I64InSet` of the 13,452 part keys into the lineitem scan (`EMAT_RT_BLOOM_RATIO=128`); the scan consumed and applied it on all 14 partitions. Result: **NEUTRAL** (191.2 vs 190.8 ms). A set/bloom on a **uniform FK doesn't reduce probe work — it relocates it** (same ~60M probes, now in the scan instead of the join; no page/RG skips because the FK is uniform across every page). (`project_rev20_q07_q08_decode_bound.md`, "REV.21 A/B RESULT".)

4. **Snappy decompress is a shared floor, and ematix is at parity.** 97% of Q08 lineitem decode is Snappy decompress, 3% materialize; every page must be decompressed (uniform 0.67% survival → no page skip). A libsnappy swap was ~7% (not 2×) and a hand-rolled Snappy lost 12-17% on real Q06/Q14. (`project_rev20_q07_q08_decode_bound.md` Phase-0; `project_mimalloc_production_gap.md` Phase-1.) **Decode is not the lever and is not in scope here.**

5. **Scheduling idle is real but contested, and CPU-bound on the clean measurement.** Self-time profiling shows **~77% of samples are thread-WAIT** (`__psynch_cvwait` 44% + `__psynch_mutexwait` 29%), but the biggest mutex-wait stack traced end-to-end to an **idle parked runtime/pool worker on the work-queue lock** — structural runtime idle on a short query, not lock contention. A separate measurement found Q08 **CPU-bound at ~87% util** (CPU/wall 12.2× of 14), concluding "no scheduling/parallelism lever." (`project_mimalloc_production_gap.md` Follow-up #3; `project_q08_join_probe_kernel_2026_06.md` DIG fact 1.) **These two readings are in tension** (see §1.6); this ADR does not assume the scheduling-idle figure is recoverable.

### 1.4 The forces at play

This is a force analysis, because "push engines are faster" is not a justification — the codebase has a long graveyard of microbench wins that didn't translate.

- **The pull model compacts at every boundary.** Each `ExecutionPlan` hands the next a dense, fully-materialized `RecordBatch`. For part⋈lineitem that means a 60M-row intermediate flows scan→join, and the join `take`s survivors into yet another batch. DuckDB/Velox/Photon carry data through a fused pipeline and emit only at pipeline breakers; the intermediate never exists.
- **The codegen-sensitivity hazard.** Three consecutive attempts to add/refactor optimizer rules in `ematix-flow-core` regressed SF=1 geomean +5-8%, *including on queries the new code cannot touch* — attributed to LLVM codegen perturbation of the optimizer module graph (`project_optimizer_codegen_sensitivity.md`). Counter-evidence (Σ.P, re-benched at 11 trials) showed some of those "regressions" were 3-5-trial measurement noise. **Net discipline:** treat any post-change "regression" with skepticism until ≥11 interleaved trials; AND prefer to ship perf-sensitive code in a **sibling crate** (ematix-parquet, ematix-flow-hash-join) where the crate boundary demonstrably blocked the tax (ematix-parquet v0.13 landed +4% with zero regressions).
- **Microbench-doesn't-translate is the house rule.** Lever-4, hand-rolled Snappy, Σ.N.e/f, the dense-bitset probe, the L13 2.36× probe — all won an isolated microbench and went neutral-or-negative at SQL wall. The team's standing instruction is **measurement-first, end-to-end wall, distrust the kernel gate** (`feedback_review_kernel_construction.md`, `feedback_bench_methodology_3_invocations.md`, MEMORY index).
- **Opt-in-then-gated is the shipping pattern.** Every risky lever ships behind an env flag, dormant by default, then strict interleaved A/B + `tpch_validate` 22/22, default-on only after gates (e.g. `EMAT_HASH_JOIN=1` on the existing `SwapEmatixHashJoinRule`, `swap_emat_hash_join_rule.rs:125`).
- **No TPC-H-specific hardcoding.** Wins must generalize from plan shape, not query identity (`feedback_no_tpch_hardcoding.md`). A fused pipeline keyed to "Q08" is unshippable; one keyed to "Inner CollectLeft join whose probe side is a scan/filter/project chain" is fine.

### 1.5 Why this ADR exists now (and the apparent contradiction)

Two same-lineage memories disagree on the surface:

- `project_mimalloc_production_gap.md` (2026-06-03): "Push-vectorization **REJECTED** for these two" — because a multi-month full-engine rewrite for ~2-4% on one query, leaving Q15 (a decode/orchestration floor) untouched, is a bad trade.
- `project_q08_join_probe_kernel_2026_06.md` (2026-06-04, more recent): "Only un-rejected lever = **push-vectorized execution**" — because every *point* lever (kernel, bloom, dynamic filter, alloc pool, build-side swap, masked→dense) is now exhaustively falsified.

**The reconciliation, and the thesis of this ADR:** what was rejected is a *full volcano replacement* (Alternative B below). What is *not* rejected — and what both memories' evidence actually points to — is a **narrow, hybrid, strictly-opt-in fused pipeline** that eliminates exactly the one cost the evidence isolates (the 60M-row intermediate + per-boundary `take`), without touching decode, without a new optimizer rule, and without committing to an engine. The disagreement is about **scope**, not direction. This ADR proposes the narrow scope and makes Phase 0 a hard kill-gate so we never build the broad one on faith.

### 1.6 Honest unknowns the design must respect

- **Is the gap recoverable at all?** Reading (4)+(5) admit a real possibility that Q08's residual is Snappy-floor + irreducible 60M-row decode that DuckDB *also* pays (DuckDB's lineitem scan is *slower*, 1310 vs 757 ms). If the intermediate-materialization cost is genuinely small once decode is excluded, **no execution-model change helps** and Phase 0 must say so.
- **The two scheduling readings conflict** (87% CPU-bound vs 77% thread-wait). Phase 0 must resolve which regime Q08 is in on the *current* binary (mimalloc, registry decoder), because a morsel scheduler only helps the thread-wait regime.
- **The summed-work "1130 ms join" is from `elapsed_compute`, which is unreliable** — it counts "stream active including waiting," and the CPU sample profile says the probe is ~8% of total / 22% of active (`project_q08_join_probe_kernel_2026_06.md` DIG fact 3). The *real* recoverable quantity is the `take`/alloc work, which the sample profile sizes at the take/repartition frames. Phase 0 must measure the recoverable chunk directly, not via `elapsed_compute`.

---

## 2. Decision

**Adopt a single-node, hybrid "morsel pipeline" operator — `EmatPushPipelineExec` — that implements DataFusion's `ExecutionPlan` and internally runs a fused, push-based pipeline over a pipeline-able subtree (scan → filter → project → hash-probe), materializing a `RecordBatch` only at its output boundary. Gate the entire effort behind a Phase-0 kill-criterion. Reject full volcano replacement.**

Concretely, the decision has these load-bearing parts:

1. **Hybrid, not replacement.** The push pipeline is *one* `ExecutionPlan` node. DataFusion remains the SQL parser, logical optimizer, physical planner, and the executor of everything outside the fused fragment. The pipeline node takes dense `RecordBatch`es in (from whatever DataFusion feeds its leaves) and emits dense `RecordBatch`es out (to whatever consumes it). It is a **pull node on the outside, push on the inside.** This is the same architectural stance the codebase already took for `EmatixHashJoinExec` and `RobinHoodAggregateExec`: a custom operator that swaps in for a validated shape and leaves everything else on stock DataFusion (`emat_hash_join_exec.rs` module doc; `ematix-flow-hash-join/src/lib.rs` "sibling op, not outright replacement").

2. **The push kernel lives in a sibling crate** (`ematix-flow-push` or an extension of `ematix-flow-hash-join`), **Arrow-free at its core where possible**, to keep the codegen tax local (§1.4; the L13 kernel crate's own module doc states this rationale). The DataFusion glue (`EmatPushPipelineExec`) lives in `ematix-flow-core` as the thin consumer.

3. **Subtree selection is a pre-plan walker, not a new `PhysicalOptimizerRule`** — installed via `FlowQueryPlanner` (`preset.rs:218`), the same place join-reorder/dim-push already run *outside* the compiled optimizer loop to avoid the codegen tax. The walker matches the **shape** (a maximal pipeline-able chain ending in a fusable join), never a query name.

4. **Strictly opt-in → gated → default-on only after the full gate passes**, mirroring `EMAT_HASH_JOIN`. Flag: `EMAT_PUSH_PIPELINE=1`. No default-on ship without 22q SF=10 interleaved A/B (≥11 trials, paired sign test) + `tpch_validate` 22/22 at SF=1/10/100.

5. **Phase 0 is a standalone spike with a hard kill number** (§5). If a fused push-pipeline over Q08's *exact* hot loop does not beat the current pull path by the stated margin **on end-to-end wall**, the engine is not built. We do not proceed on a microbench.

### Why this architecture (against the constraints)

- It attacks the **measured** cost (intermediate materialization + per-boundary `take`), not the falsified ones (probe kernel, decode, dynamic filter).
- It contains blast radius: one node, one shape, off by default. The 22 working queries are bit-identical unless the flag is set and the shape matches.
- It keeps the hot code in a sibling crate, the one mitigation that has empirically dodged the codegen tax.
- It generalizes by plan shape (no Q08 hardcoding).
- It gives a **fast path to first measurement** — Phase 0 needs no engine, no operator, no rule; it's a throwaway binary against real SF=10 data, exactly the de-risk discipline the codebase mandates and has used to kill levers cheaply.

---

## 3. The core abstractions / API

This section defines the design precisely enough to build Phase 0 against, and to review for correctness hazards (§4). All of it lives behind the flag.

### 3.1 The morsel

A **morsel** is a unit of columnar work pulled from the pipeline's source and pushed through the operator chain. The decision: **a morsel is a slice of decoded Arrow columns plus a selection.**

```rust
// crate: ematix-flow-push (sibling). Arrow types are unavoidable at the
// envelope boundary (the scan produces them); the *kernels* below operate
// on raw slices to stay monomorphizable + codegen-isolated where it pays.
pub struct Morsel {
    /// Dense decoded columns for this chunk, as produced by the scan.
    /// Borrowed for the push call's lifetime — NOT cloned per operator.
    cols: Vec<ArrayRef>,
    /// Surviving row indices into `cols`. `None` = "all rows live" (the
    /// fast path; avoids allocating a 0..n vector for an unfiltered morsel).
    /// `Some(sel)` after a filter/probe narrows it. THIS is what lets us
    /// avoid the eager `take`: downstream reads cols[c] THROUGH sel.
    sel: Selection,
    /// Absolute row offset of cols[*][0] in the source partition, so
    /// emitted output can carry stable provenance if a breaker needs it.
    row_base: u64,
}

pub enum Selection {
    All(usize),        // n live rows, 0..n
    Indices(Vec<u32>), // explicit surviving indices (post-filter)
}
```

Rationale for "Arrow arrays + selection" rather than "raw column buffers":
- The scan already produces Arrow arrays (the ematix-parquet bridge decodes into `DecodedColumn`/Arrow). Re-deriving raw buffers would duplicate the bridge and risk the null/def-level bugs §4 catalogs.
- A **selection vector** (not eager compaction) is the entire point — it is what defers the `take`. This mirrors `ADR_SELECTION_VECTOR_MATERIALIZATION.md`'s Option C envelope `(RecordBatch, Option<Selection>)`. Aligning the two ADRs on one envelope is deliberate.
- Morsel size: target **L2-resident** chunks (e.g. 4-16K rows, tuned), not whole row groups. DuckDB uses 2048-row vectors; the codebase's prior streaming work found single-digit-ms RG decode makes whole-RG emission stall downstream (`project_sigma_e5_6_streaming_doesnt_win.md`, the Q19/Q18 inline-streaming story). Small morsels are the mechanism by which decode overlaps probe.

### 3.2 The push operator interface

```rust
/// A push operator consumes morsels and either narrows-and-forwards them
/// (pipelined ops: filter, project, probe) or accumulates them (breakers:
/// build side, aggregate, sort). The pipeline driver calls `push` per
/// source morsel and `finish` once the source is drained.
pub trait PushOperator: Send {
    /// Consume one morsel. A pipelined op narrows `m.sel` in place (or
    /// rebinds cols for a projection) and calls `out.emit(m)`. A breaker
    /// absorbs `m` into internal state and emits nothing here.
    fn push(&mut self, m: Morsel, out: &mut dyn Sink) -> Result<(), PushError>;

    /// Source drained. Breakers flush accumulated output here (e.g. an
    /// aggregate emits its groups). Pipelined ops are no-ops.
    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), PushError>;
}

/// Where emitted morsels go. The terminal sink of a pipeline materializes
/// to a RecordBatch (the ONLY compaction point); an intermediate sink hands
/// to the next operator.
pub trait Sink {
    fn emit(&mut self, m: Morsel) -> Result<(), PushError>;
}
```

**Pipeline-breaker semantics.** An operator is a **breaker** iff it cannot emit output until it has seen all input:
- **Hash-join build side** is a breaker (must see all build rows before any probe). In the Q08 shape the build (filtered `part`, ~13K rows) is already collected once by CollectLeft — the build breaker is cheap and runs before the pipeline starts, exactly as `EmatixHashJoinExec` does today (`emat_hash_join_exec.rs:163`).
- **Probe side** is **pipelined** — this is the whole win. A probe morsel narrows its selection to matching rows and forwards; it never builds an intermediate batch.
- **Aggregate / sort** are breakers. **For Phase 1-4 they are NOT inside the pipeline** — the pipeline's output boundary feeds a stock DataFusion `AggregateExec`/`SortExec`. We compact at that boundary (which is unavoidable and correct; see §4 integration).

So a Phase-1 Q08 pipeline is: `[scan-morsel-source] -> filter (l_partkey ∈ build keys via probe) -> project (4 payload cols) -> probe-emit`. The probe IS the filter+narrow; output is the survivor rows' payload columns, compacted **once** at the terminal sink. That is precisely the loop Phase 0 measures.

### 3.3 How a pipeline is assembled

A pipeline is a linear chain `source -> op -> op -> ... -> terminal_sink`. Assembly:

1. The pre-plan walker (§3.5) identifies a fusable subtree in the **physical** plan: a chain of pipelined-able operators (`EmatixFastParquetExec` scan, `FilterExec`, `ProjectionExec`) terminating at the probe side of a fusable join.
2. It constructs an `EmatPushPipelineExec` whose:
   - **leaves** are the original child `ExecutionPlan`s that feed the fusable chain (so DataFusion still produces the input morsels — the pipeline does not re-implement the parquet reader; it consumes the scan's output stream and re-chunks it into morsels). *Design note: for Phase 1 the "scan-morsel-source" wraps the existing `EmatixFastParquetExec` stream and slices each emitted `RecordBatch` into morsels. A later phase MAY teach the scan to emit morsels natively to skip one Arrow round-trip; that is an optimization, not a requirement, and is explicitly deferred.*
   - **build side** is collected once (reusing the `EmatHashJoiner` build path — `emat_hash_join.rs:64`).
   - **internal op chain** is the push operators compiled from the matched physical operators.
   - **output schema** is exactly the matched join's output schema (so the node is a drop-in replacement — same precedent as `SwapEmatixHashJoinRule` deriving `output` by name-matching `join_schema`, `swap_emat_hash_join_rule.rs:76-97`).

`EmatPushPipelineExec::execute(partition)` returns a `RecordBatchStreamAdapter` (same shape as every other operator, `:2698`/`emat_hash_join_exec.rs:196`) whose stream:
- on first poll, awaits the build (shared `OnceCell<Arc<…>>`, the existing pattern, `emat_hash_join_exec.rs:163`);
- then pulls the leaf scan stream, slices each batch into morsels, drives them through the internal push chain, and yields the terminal sink's compacted output batches.

### 3.4 Parallelism — reuse the existing partition model first, morsel work-stealing only if Phase 0 demands it

**Decision: Phase 1-4 reuse DataFusion's existing partition parallelism. Do NOT build a separate morsel scheduler unless Phase 0 proves Q08 is in the thread-wait regime AND partition-level parallelism leaves cores idle.**

Rationale, from the codebase:
- The bench + profiler run `target_partitions=14` on a tokio multi-thread runtime (`profile_query.rs:79`, `paired_ab.rs:223`). DataFusion already spawns one `execute(partition)` stream per partition; `EmatPushPipelineExec` is invoked per partition exactly like every other operator. So **partition-level parallelism is free** — we get 14 independent pipelines, one per partition, no scheduler needed.
- The "~77% thread-wait" reading suggests a morsel scheduler *might* reclaim idle, **but** the competing reading says Q08 is 87% CPU-bound, and a separate same-session conclusion is "Q08 is CPU-bound + fully parallel (~1.2% thread-park) — a morsel/push *scheduler* has no idle to reclaim" (`project_mimalloc_production_gap.md`). Building a work-stealing morsel scheduler **on top of** tokio risks oversubscription — the exact failure REV.21 step 3d/3e hit (column×partition double-parallelism; forcing thread budget to 1 was a no-op because the default was already ~1). **Two layers of work-stealing is a known footgun here.**
- Therefore: the within-pipeline parallelism is **the partition fan-out DataFusion already provides**. The morsel loop inside one pipeline is **single-threaded per partition** (sequential push through the op chain), which is what makes it cache-friendly (a morsel stays in L2 across filter→probe→emit). This is DuckDB's model *per pipeline task* — the parallelism is across tasks (partitions), the fusion is within a task.

If — and only if — Phase 0 shows (a) Q08 has reclaimable idle at 14 partitions, and (b) the idle is at partition boundaries (some partitions finish early and sit idle while stragglers run), then a **morsel work-stealing layer** becomes a *separate* follow-on ADR: split each partition's row groups into morsels in a shared work-stealing deque so an idle worker steals the next morsel rather than waiting at a partition barrier. That is explicitly **out of scope** for the proposed phases and gated on its own evidence.

### 3.5 Subtree selection (the "which subtree to fuse" rule)

A pre-plan walker (in `FlowQueryPlanner`, not a `PhysicalOptimizerRule`) that, per the established `find_probe_scan_for_column` / `try_swap` patterns:

- Finds a `HashJoinExec` (or the existing `EmatixHashJoinExec`) matching the **fusable shape**: `JoinType::Inner` or `LeftSemi`, `PartitionMode::CollectLeft` (build is broadcast-small — doubles as the build-cardinality gate, same logic as `swap_emat_hash_join_rule.rs:51`), single i64-widenable equi-key, no non-equi filter.
- Walks the **probe child** down through pipelined-able wrappers (`FilterExec`, `ProjectionExec`, `CoalesceBatchesExec`) to a single `EmatixFastParquetExec` — the exact descent `find_probe_scan_for_column` already does (`runtime_bloom_sideband_rule.rs:477`).
- If the whole chain is pipeline-able and the column mapping resolves unambiguously by name (the uniqueness guard from `swap_emat_hash_join_rule.rs:87` — bail to stock on ambiguity), replace the chain + join with one `EmatPushPipelineExec`. Otherwise leave the stock plan untouched.

This is **shape-driven and general**: any `Inner/CollectLeft, i64-key, scan→(filter)→(project)→probe` fragment qualifies, across any query. It is the static analogue of the L9 walker that already exists and is trusted.

---

## 4. Threat model

STRIDE-adapted for a query-engine internals change: the "threats" are correctness, performance-regression, and integration failures. Scored **likelihood × impact**; each gets a mitigation classified **Eliminate / Mitigate / Transfer / Accept**.

### 4.1 Correctness threats

| # | Threat | Likelihood | Impact | Mitigation (class) |
|---|---|---|---|---|
| C1 | **Arrow null / definition-level mishandling in morsel slicing.** Slicing a `RecordBatch` into morsels and carrying a `Selection` must preserve each column's null buffer and offset. A wrong null bitmap silently changes join/filter results (NULL keys must never match — Inner semantics). | Med | **High** (silent wrong answers; the codebase has been burned — Q07 sums were 94% wrong for months because the bench only checked row counts, `runtime_bloom_sideband_rule.rs:496` comment). | **Eliminate**: reuse Arrow's own `Array::slice` (offset-correct, null-correct) for morsel boundaries; reuse the L13 kernel's existing null path (`key_as_i64` extracts validity, `emat_hash_join.rs:37-47`; `null_keys_never_match` test `:166`). Never hand-roll null logic in the morsel layer. **Gate**: `tpch_validate` checks **sums**, not just row counts, at SF=1/10/100. TDD: a failing null-join test on the pipeline before any pipeline code. |
| C2 | **Type widening / i64→i32 downcast interaction.** The scan may decode a key as Int32 narrowed from Int64 (`decode_schema` vs `schema`, `ematix_fast_parquet.rs:2243`; KEYS.2 narrowing); the probe key extractor widens Int32→i64 (`emat_hash_join.rs:43`). The morsel source must observe the **advertised** schema after `narrow_stream_to_advertised` (`:2588`), or the pipeline keys on the wrong width. | Med | High | **Mitigate**: the pipeline consumes the scan's *output* stream (post-narrowing), not its internal decode buffers, so it sees the advertised schema by construction (§3.3 design note). Restrict Phase 1 keys to exactly what `widens_to_i64` allows (`swap_emat_hash_join_rule.rs:41`). Bail to stock on any other key type. |
| C3 | **Projection ordering / column-index drift.** The output column mapping (`JoinColumn::Build(i)`/`Probe(i)`) is derived by name-matching the join schema (`swap_emat_hash_join_rule.rs:78`). A fused chain that includes a `ProjectionExec` reorders/renames columns; the mapping must compose through the projection, not assume scan-order. | Med | High | **Eliminate**: compute the output mapping from the **matched join's output schema** exactly as the existing swap rule does, and apply the projection as an explicit push operator that rebinds `cols` by index (no implicit reordering). The uniqueness-guard bail (`:87`) stays. Test: a fused pipeline whose projection permutes columns, asserted against stock-plan output column-for-column. |
| C4 | **NULL join semantics under selection.** Carrying a `Selection` instead of compacting means a downstream consumer could mis-read a "live" row that has a null key. | Low | High | **Eliminate**: the probe operator filters null keys out of the selection at probe time (Inner/Semi never match null), so a null key is never "live" past the probe. Mirror `EmatHashJoiner`'s existing null exclusion. |
| C5 | **Multi-reference table aliasing** (e.g. Q21's three lineitem scans). A walker that fuses the wrong scan instance corrupts a correlated subquery — the exact Q21 bug the L9 rule fixed with `Arc::ptr_eq` (`runtime_bloom_sideband_rule.rs:506-531`). | Low | High | **Eliminate**: match the fusable scan by `Arc` identity, reusing the L9 rule's ptr-eq discipline. The CollectLeft + single-equi-key + no-filter gate already excludes most subquery shapes. |
| C6 | **Determinism for f64 aggregates downstream.** If a later phase pulls an aggregate into the pipeline, f64 SUM ordering changes results (the Q15 `DedupeAggregateForFloatDeterminism` lesson). | Low (agg is out of scope Phases 1-4) | Med | **Accept + Defer**: aggregates stay on stock DataFusion (compact at the pipeline boundary) for the proposed phases. Pulling agg into the pipeline is a future ADR that must address f64 determinism explicitly. |

### 4.2 Memory / spill threats

| # | Threat | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| M1 | **Build side too large to collect.** The pipeline's build breaker collects the build side in memory (like CollectLeft). On a non-broadcast shape this could OOM. | Low (gated to CollectLeft) | High | **Eliminate**: the CollectLeft gate (`PartitionMode::CollectLeft`) IS the build-size guard — DataFusion already proved the build is broadcast-small (`swap_emat_hash_join_rule.rs:51` comment). Never fuse a Partitioned-mode join. |
| M2 | **Selection-vector memory blow-up.** A low-selectivity morsel keeps the full dense columns alive while carrying a tiny selection — fine; a *high*-selectivity morsel that never compacts keeps everything alive across many morsels. | Low | Med | **Mitigate**: compact at the terminal sink per morsel-batch (bounded by morsel size), not at end-of-pipeline. Morsels are L2-sized, so live memory is O(morsel) per partition, not O(partition). |
| M3 | **No spill support.** A push pipeline that accumulated unbounded state (a future in-pipeline agg) would need spill; the proposed pipeline does not accumulate (probe is streaming, build is bounded by M1). | Low | Low | **Accept**: proposed phases have no unbounded accumulator; spill is a non-issue until agg moves in-pipeline (future ADR). |

### 4.3 Integration threats

| # | Threat | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| I1 | **Codegen-sensitivity tax** — adding the operator/walker regresses the 21 *other* queries +5-8% from LLVM perturbation, even though they don't use it (`project_optimizer_codegen_sensitivity.md`). | **Med** | **High** (would erase the broad win to fix one query) | **Mitigate (primary risk)**: (a) push kernel in a **sibling crate** (the empirically-clean mitigation — ematix-parquet/L13 precedent); (b) subtree selection as a **pre-plan walker in `FlowQueryPlanner`**, not a `PhysicalOptimizerRule` (the `preset.rs:218` precedent, chosen for exactly this reason); (c) **measure the tax**: a 22q SF=10 interleaved A/B with the feature compiled-in-but-flag-OFF vs the prior binary, ≥11 trials, paired sign test, BEFORE judging the feature. If flag-off compiled-in regresses, the crate boundary failed and we stop. **Transfer** residual risk to the gate: default-on only after the tax is shown ≤ noise. |
| I2 | **Which subtree stays pull-based** is mis-decided — the walker fuses a chain that *shouldn't* be (e.g. one feeding a selection-unaware consumer), forcing a compaction that costs more than it saves (the Σ.L.3.c failure: a masked-path change won a microbench but starved parallel downstream, gave back 16.8% geomean, `project_sigma_l3c_reverted.md`). | Med | Med | **Mitigate**: the pipeline ALWAYS compacts at its output boundary, so its consumer always gets a dense batch — no consumer is ever starved or fed a non-dense input (unlike Σ.L.3.c's sequential masked decode). The only risk is fusing a fragment where compaction-at-boundary ≈ the per-operator takes it replaced (net zero). Gate: per-query A/B; revert any regressor. |
| I3 | **Interaction with the L9 runtime-bloom sideband.** The fused scan might be a sideband consumer (`runtime_sideband`, `ematix_fast_parquet.rs:2277`); fusing it could bypass the deferred-peek-on-first-poll mechanism (`:2624`). | Med | Med | **Mitigate**: Phase 1 **declines to fuse any scan that has a `runtime_sideband` attached** (check `scan.runtime_sideband().is_some()`, `:2339`) — the sideband path and the fused path are mutually exclusive in v1. They target overlapping wins (both shed probe-side rows); pick one per scan. Re-evaluate composition in a later phase. |
| I4 | **Plan-shape perturbation breaks an unrelated optimization** — the new node changes partitioning/ordering properties (`PlanProperties`) and a downstream rule (EnforceDistribution, the agg-partition-boost) mis-fires. | Med | Med | **Eliminate**: `EmatPushPipelineExec` advertises **exactly** the matched join's `PlanProperties` (output partitioning = probe side's, like `emat_hash_join_exec.rs:60`), so it is properties-indistinguishable from the operator it replaces. `schema_check` returns true honestly (same output schema). |
| I5 | **The walker runs post-optimization and sees a plan the optimizer already transformed** (e.g. ForceCollectLeft already applied). Ordering vs the existing walkers matters. | Low | Med | **Mitigate**: install the pipeline walker in `FlowQueryPlanner` **after** the existing agg-semi→dim-push→reorder steps and after the L9/ForceCollectLeft physical rules, so it sees the final CollectLeft assignment — exactly the ordering note in `swap_emat_hash_join_rule.rs:22` ("runs after the built-in physical rules so partition_mode() is already assigned"). |

### 4.4 Residual risk

After mitigations, the dominant residual risks are:
1. **I1 (codegen tax)** — Med likelihood, High impact. Not fully eliminable; the sibling-crate mitigation has worked before but is not guaranteed. **This is the single most important gate.**
2. **The gap may be unrecoverable (§1.6)** — Phase 0 is designed to surface this; residual is "we spend the Phase-0 day and learn the answer is no," which is cheap and is the point.
3. **Microbench-doesn't-translate** — the house pattern; mitigated by making every phase gate on end-to-end wall, never a kernel bench.

---

## 5. Phased de-risk roadmap

Every phase is independently measurable, has a kill-criterion, and **measures end-to-end wall on real SF=10 data** (`examples/tpch/data/sf10/` — note: **project root**, not under `crates/`). Every code phase passes the **shared gate** before merge:

- **22q SF=10 interleaved A/B**, ≥11 trials, paired sign test (`crates/ematix-flow-core/examples/paired_ab.rs`; `scripts/bench/strict_ab.sh` / `strict_22q.sh`), fresh `SessionContext` per trial (the Σ.P CSE-replay footgun — `paired_ab.rs` already does this).
- **`tpch_validate` 22/22** vs DuckDB at SF=1/10/100 — **sums, not just row counts** (C1).
- **No TPC-H-specific hardcoding** — shape-driven only.
- **`ematix-parquet` / sibling crates are local-modifiable but publish-gated** — build against the vendored/local crate; do not assume a published tag.
- **Codegen-tax check** (I1): compiled-in-flag-OFF vs prior-binary 22q A/B must be ≤ noise.

### Phase 0 — STANDALONE SPIKE (no engine commitment). THE KILL-GATE.

**Goal:** prove or kill the central hypothesis — that a fused push-pipeline eliminating the 60M-row intermediate + per-boundary `take` beats the current pull path on Q08's exact hot loop, end-to-end.

**Build (throwaway, in an example binary, ~1-3 days):** a single `main.rs` that, against **real SF=10** `part.parquet` + `lineitem.parquet`:

- **Arm A (pull baseline):** the current path — decode lineitem (4 payload cols + `l_partkey`) into Arrow `RecordBatch`es, run the existing `EmatHashJoiner::probe` per batch (build = the 13,333 filtered `p_partkey`s from `part` with the `p_type`/region filter applied, 0.67% survival), which does `take`-gather + assembles output batches (`emat_hash_join.rs:100`). This reproduces what `EmatixHashJoinExec` does today.
- **Arm B (fused push):** decode lineitem into **morsels** (L2-sized slices), and for each morsel: probe `l_partkey` against the build set, narrow the selection inline, and emit **only survivor rows' 4 payload columns** — compacting **once** at the terminal sink. **No intermediate full-batch, no per-boundary `take`.** Build the set with both an open-addressing table and a dense bitset (the dense bitset is already shown 10.8× in isolation; here we test whether it lands when the *materialization* is also removed — the prior dense-bitset test left the `take` in place, which is why it was neutral).

Both arms: 14 partitions, mimalloc (match the bench — `profile_query.rs` now sets it), warm + cold, paired interleaved, ≥21 trials with a sign test (the `paired_ab.rs` methodology that corrected the earlier "noise" error).

**Also resolve the §1.6 unknowns in the same spike (cheap add-ons):**
- Per-stage timer split of Arm A: decode vs probe vs `take`/assemble, at 7 and 14 partitions. Confirms how much of the deficit is recoverable `take` vs irreducible decode.
- CPU/wall ratio + thread-park fraction on Arm A at 14 partitions, on the **current** binary — resolves the 87%-CPU-bound vs 77%-thread-wait contradiction and tells us whether §3.4's morsel-scheduler follow-on is even worth a future ADR.

**SUCCESS CRITERION (the kill number):**

> Arm B must be **≥ 12% faster than Arm A on end-to-end wall for the part⋈lineitem loop** (median of paired trials, sign test p < 0.05), AND the per-stage split must attribute the gain to removed `take`/materialize (not to a measurement artifact).

Justification for 12%: Q08's whole-query gap to DuckDB is ~7-13% of *total* Q08 wall, and the part⋈lineitem loop is the dominant but not sole cost. To move Q08 wall enough to flip the DuckDB loss, the loop itself must improve by clearly more than the whole-query gap — call it ~12% on the loop as the floor below which the full-query win cannot materialize once decode (unchanged, at parity) is blended back in. If Arm B is between 0 and 12%, the engine is **not worth building** — the materialization tax is real but too small to pay back the multi-phase cost and the I1 risk. If Arm B ≤ 0%, the hypothesis is **falsified** (the take is not the bottleneck after all, consistent with the §1.6 "unrecoverable" possibility) and this ADR is **closed**.

**Why this is the right kill-gate:** it tests the *exact* claim the whole engine rests on, on the *exact* hot loop, on *real* data, end-to-end — with no engine, no operator, no rule, no codegen tax. It is the discipline the codebase used to kill the dense-bitset, the dynamic-filter, the late-mat, and the Snappy-swap levers cheaply. **Do not write Phase 1 until Phase 0 clears 12%.**

### Phase 1 — Pipeline abstraction (sibling crate), unit-tested only

If Phase 0 clears: build `ematix-flow-push` with `Morsel`, `Selection`, `PushOperator`, `Sink`, and three operators (filter, project, hash-probe) — **pure kernel, no DataFusion**. TDD: null-join correctness, projection-permutation, selection narrowing, multi-batch build. No SQL integration yet. Gate: kernel tests green; **and** a repeat of the Phase-0 Arm-B loop expressed through the new abstraction must still clear 12% (proves the abstraction didn't add overhead).

### Phase 2 — Single-pipeline `ExecutionPlan` operator, direct-construction only

Wrap the pipeline in `EmatPushPipelineExec` (in `ematix-flow-core`, thin glue). No walker yet — construct it directly in a test/bench. Verify it produces **bit-identical** output to the stock `scan→filter→project→HashJoin` chain on the Q08 fragment (column-for-column, sums). Gate: output-equality test + a single-query (Q08) A/B showing the operator (constructed by hand) moves Q08 wall in the direction Phase 0 predicted.

### Phase 3 — Optimizer integration (pre-plan walker), flag-gated OFF

Add the shape-matching walker to `FlowQueryPlanner` behind `EMAT_PUSH_PIPELINE=1`, default OFF. **Gate (the real one):**
- 22q SF=10 interleaved A/B, ≥11 trials, flag ON vs OFF — Q08 improves, **no other query regresses past 2σ**.
- **Codegen-tax check (I1):** flag-OFF-compiled-in vs prior binary ≤ noise. If this fails, the sibling-crate boundary didn't hold — **stop and reassess** (do not ship a broad regression to fix one query).
- `tpch_validate` 22/22 SF=1/10/100, sums.
- Kill-criterion: any of the above fails → stays opt-in or reverts (the `EMAT_HASH_JOIN` precedent — it's still opt-in because it didn't clear).

### Phase 4 — Default-on decision

Only if Phase 3's gate is clean across SF=1/10/100 with the codegen-tax check passing. Until then it ships exactly like `EMAT_HASH_JOIN`: present, correct, dormant, opt-in.

### Phase 5+ (separate ADRs, gated on Phase-0/3 evidence) — explicitly OUT of scope here

- **Morsel work-stealing scheduler** — only if Phase 0 shows reclaimable partition-boundary idle (§3.4).
- **Selection-aware aggregate/sort in-pipeline** — must address f64 determinism (C6); folds in `ADR_SELECTION_VECTOR_MATERIALIZATION.md`'s Option C.
- **Native morsel emission from the scan** (skip the Arrow round-trip at the leaf) — an optimization once the pipeline is proven.

---

## 6. Consequences

### Positive
- Attacks the **one measured, un-falsified** Q08 lever directly (intermediate materialization), after seven investigations ruled out every point lever.
- Blast radius is one node, one shape, off by default — the 22 working queries are untouched unless flag+shape match.
- Hot code in a sibling crate keeps the codegen tax local (the empirically-clean mitigation).
- Phase 0 gives a **cheap, definitive** answer (~1-3 days, throwaway) before any engine commitment — and resolves the §1.6 measurement contradictions as a side effect.
- The morsel/selection envelope aligns with `ADR_SELECTION_VECTOR_MATERIALIZATION.md`, so the two efforts converge on one model rather than forking the engine twice.
- Establishes reusable push-pipeline infrastructure that generalizes beyond Q08 (any Inner/CollectLeft scan→filter→probe fragment) and is the substrate for future selection-aware operators.

### Negative / what becomes harder
- **A second execution model exists in the codebase.** Even gated, contributors must understand that *some* plans run push-internally. Cognitive + maintenance load; the boundary semantics (compact-at-output) must be documented and tested or C1-C5 bite silently.
- **The codegen-tax risk is real and not fully eliminable** (I1). If the sibling-crate boundary doesn't hold, the effort stops at Phase 3 having spent Phases 1-2 — a meaningful sunk cost (though far less than a full engine).
- **The win may be small even if positive.** If Phase 0 lands at, say, 12-20%, Q08 flips to a marginal win but the broad geomean barely moves (Q08 is one query). The ROI is "close a known competitive gap on the headline benchmark," not "make the engine materially faster overall." The owner must want *that* specifically.
- **Aggregates/sorts still compact at the boundary**, so this does not deliver DuckDB's end-to-end selection-vector pipeline — only the scan→join fragment. The full model is deferred to a much larger future ADR.
- **Maintenance coupling to DataFusion internals** — `PlanProperties`, `PartitionMode`, the build-future timing — all of which DataFusion can change across versions (the L9 sideband already carries scar tissue from exactly this, `ematix_fast_parquet.rs:2536-2614`).

### Neutral
- Decode path is unchanged (correctly — it's at parity and not the lever).
- Distributed (Arrow Flight) path is unaffected; this is a single-node operator.

---

## 7. Out of scope

- **Full volcano replacement** (Alternative B) — rejected, see §8.
- **Decode / Snappy-decompress rate** — known shared floor; ematix at parity (`project_rev20_q07_q08_decode_bound.md`).
- **The scan-side filtered-gather plateau** (Q07's masked-decode `take`) — that's `ADR_SELECTION_VECTOR_MATERIALIZATION.md`'s scope; this ADR's pipeline is its eventual host but the gather kernel itself is separate.
- **Dynamic-filter / sideways information passing** — measured neutral on this shape (relocates, doesn't reduce, probe work on a uniform FK).
- **A morsel work-stealing scheduler** — gated on Phase-0 evidence; separate ADR.
- **In-pipeline aggregates/sorts** — future ADR; must solve f64 determinism.
- **SF=100+ / distributed** — single-node SF=10 here.
- **Q15** — decode/orchestration floor, not a materialization-tax query; this engine does not target it.

---

## 8. Alternatives considered

### Alternative A (REJECTED) — Faster join-probe kernel only (no fusion)
Keep the pull model; make the probe faster (dense bitset, SIMD salt/tag like DuckDB's 1-byte reject). **Why not:** measured. Dense bitset was 10.8× in microbench and **neutral on Q08 wall** (probe is ~0% of wall; the cost is `take`/materialize). The L13 kernel (2.36× microbench) moved Q08 wall flat. A SIMD-tagged probe (Σ.Q.L12) was shape-dependent +38%/-19% and never wired in. The kernel is not the bottleneck; a faster one cannot help. (`project_mimalloc_production_gap.md`, `project_q08_join_probe_kernel_2026_06.md`, `project_sigma_q_l12_rejected.md`.)

### Alternative B (REJECTED) — Full volcano replacement (DataFusion as parser/optimizer only)
Keep DataFusion only as SQL frontend + logical optimizer; lower the `LogicalPlan` to a brand-new push engine that executes everything. **Why not:**
- **Disproportionate.** Multi-month, multi-quarter; the target is ~7-13% on *one* query at *one* scale factor while the engine already wins the geomean 0.80 and beats DuckDB 2.7× at SF=1. `project_mimalloc_production_gap.md` rejected exactly this trade.
- **Throws away the moat.** The 22-query correctness + the entire shipped optimizer-rule ecosystem (L9 sideband, RobinHood aggs, join-reorder, dict-aware path, fused aggregates) are built on DataFusion's `ExecutionPlan` contract. A full replacement re-implements all of it or loses it.
- **Maximal codegen-tax + correctness surface.** Every query's correctness re-validated; the I1 risk applies to the entire engine, not one node.
- **No incremental measurement.** Time-to-first-measured-win is months; violates the measurement-first, kill-early discipline. The hybrid gives a Phase-0 answer in days.
- The hybrid (the Decision) captures the *same* per-pipeline fusion win on the fragments that matter, incrementally, at a fraction of the risk.

### Alternative C (REJECTED for now) — Scan-only selection-vector reader (finish #516)
Make the scan emit `(dense pages, selection)` and defer compaction. **Why not:** `ADR_SELECTION_VECTOR_MATERIALIZATION.md` §3 Option B already analyzed this and showed it collapses to "still compacting" the instant the batch crosses into a stock DataFusion operator — the measured-neutral full-Q07 A/B (155.9 vs 160.2 ms). A selection vector is only useful with a **selection-aware consumer**, which is precisely the push pipeline this ADR proposes. So C is not an alternative to the pipeline; it's a *component* of it, folded into Phase 5+.

### Alternative D (REJECTED) — Accept the gap, pivot
Declare Q08 floored and stop (the `project_mimalloc_production_gap.md` 2026-06-03 verdict). **Why not chosen as the recommendation, but why it's the fallback:** this is the *correct* outcome **if Phase 0 fails the kill-gate**. It is rejected only as a pre-emptive conclusion: the most recent investigation (`project_q08_join_probe_kernel_2026_06.md`, 2026-06-04) explicitly leaves push-vectorization as the one un-falsified lever and the user has asked to test it. Phase 0 is the cheap, principled way to either justify the work or confirm Alternative D — so we run Phase 0, and if it fails, **D is the documented result.**

---

## 9. References

### Code (file:line, this repo)
- Pull-model scan: `crates/ematix-flow-core/src/ematix_fast_parquet.rs` — `EmatixFastParquetExec` struct (`:2236`), `execute` + `RecordBatchStreamAdapter` (`:2510`, `:2589`, `:2698`), `runtime_sideband` field + deferred-peek (`:2277`, `:2328`, `:2624`), `partition_statistics` (`:2705`), `BridgeFilter` + `predicted_pass_rate` + `is_runtime_i64_only` (`:68`, `:167`, `:201`).
- Pull-model join + the materialization tax: `crates/ematix-flow-core/src/emat_hash_join_exec.rs` (`execute`/build-once/probe-map `:146`/`:163`/`:188`); `crates/ematix-flow-core/src/emat_hash_join.rs` (`EmatHashJoiner::probe` `take`-gather `:100`-`:121`; `key_as_i64` widening + nulls `:37`; null-join test `:166`).
- Shape-gated swap precedent: `crates/ematix-flow-core/src/swap_emat_hash_join_rule.rs` (`try_swap` gate `:47`, CollectLeft = build-size guard `:51`, name-match + uniqueness bail `:78`-`:97`, opt-in `EMAT_HASH_JOIN` `:125`).
- Pure-kernel sibling-crate pattern: `crates/ematix-flow-hash-join/src/lib.rs` (codegen-isolation rationale `:1`-`:28`; "sibling op, not replacement" `:20`); `table.rs` (RobinHood layout `:1`-`:89`).
- Walker / subtree-selection precedent: `crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs` (`find_probe_scan_for_column` descent `:477`, `Arc::ptr_eq` multi-ref discipline + the silent-wrong-sums scar `:496`/`:506`-`:531`).
- Rule installation + pre-plan walker (codegen-tax dodge): `crates/ematix-flow-core/src/preset.rs` (`with_optimizer_rules_and_registry` `:125`, `FlowQueryPlanner` install `:218`).
- Bench / gate harness: `crates/ematix-flow-core/examples/paired_ab.rs` (paired interleaved A/B, `target_partitions(14)` `:223`, fresh-ctx-per-trial); `crates/ematix-flow-core/examples/profile_query.rs` (`:79`); `scripts/bench/strict_ab.sh`, `strict_22q.sh`.
- Data: `examples/tpch/data/{sf1,sf10,sf100}/` (project root).

### Memory (banked investigations)
- `project_q08_join_probe_kernel_2026_06.md` — the 2026-06-04 root-cause: take/materialize tax, not the kernel; dense-bitset & L13 both flat; `elapsed_compute` unreliable; push-vec the one un-falsified lever.
- `project_mimalloc_production_gap.md` — the 2026-06-03 push-vec rejection (full rewrite, disproportionate); dense-bitset neutral; late-mat tie; decode at parity; mimalloc shipped.
- `project_rev20_q07_q08_decode_bound.md` — REV.20/21/23: Snappy-decompress floor, dynamic-filter neutral (relocates on uniform FK), masked↔dense, push-based-vectorized cited as the architectural gap a pull model can't close with a point lever.
- `project_optimizer_codegen_sensitivity.md` — the +5-8% codegen tax; sibling-crate + pre-plan-walker mitigations; the ≥11-trial skepticism caveat.
- `docs/ADR_SELECTION_VECTOR_MATERIALIZATION.md` — the sibling scan-side ADR; Option B (scan-only selection) measured-neutral; Option C = the operator-chain rewrite this pipeline hosts.
- Microbench-doesn't-translate lineage: `project_lever4_full_build_rejected.md`, `project_hand_rolled_snappy_neg.md`, `project_sigma_nf3_beats_stock.md`, `project_sigma_l3c_reverted.md`, `project_sigma_q_l12_rejected.md`.
- Methodology: `feedback_bench_methodology_3_invocations.md`, `feedback_review_kernel_construction.md`, `feedback_no_tpch_hardcoding.md`, `feedback_tdd.md`, `feedback_dig_dont_revert_sound_levers.md`.
