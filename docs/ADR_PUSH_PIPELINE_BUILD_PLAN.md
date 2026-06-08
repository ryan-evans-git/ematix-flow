# ADR: Hybrid morsel-driven PUSH pipeline — build plan (`EmatPushPipelineExec`)

- **Status:** PROPOSED — build plan, conditional on its own Phase-1 kill-gate. NOT accepted.
- **Date:** 2026-06-04
- **Author:** architect (cold-start design pass)
- **Reviewers:** TBD (perf owner)
- **Decision scope:** ONE decision — the *concrete, phased build plan* for a hybrid push-based fused-pipeline operator (`EmatPushPipelineExec`) that eliminates the pull-model inter-operator materialization tax on the Q08/Q07/Q10/Q17/Q20 join-into-aggregate shape. This ADR specifies *how to build it*: the node, the **adaptive probe-structure selector** (the #1 design surface), correctness for the full 7-join Q08 chain, memory bounds, and four independently-shippable phases each with a measured go/no-go.

- **Supersedes / relates to:**
  - **`docs/ADR_PUSH_VECTORIZED_ENGINE.md`** (PROPOSED) — the *capability decision* ADR: whether to build at all, the original Phase-0 spike, the STRIDE threat model, and the rejection of full volcano replacement. **This build-plan ADR consumes that one's Phase-0 as established** (the spike is now realized in `crates/ematix-flow-core/examples/pv1_fused_q08_pipeline.rs` and has cleared its number — see §1.2), restates only what is load-bearing for the build, and **supersedes its §5 phase roadmap** with the PV.1–PV.4 plan below. Where the two disagree, this ADR wins on phasing; that ADR remains canonical for the threat model and the alternatives analysis.
  - **`docs/PHASE_SIGMA_S_PIPELINED_SCAN_FILTER_JOIN.md`** (design doc) — the prior pipelined-scan attempt. It explicitly deferred "full morsel-driven parallelism" as a "multi-month re-architecture" and pursued cascading-bloom (Σ.S.B) as an 80%-of-the-benefit alternative. **This ADR supersedes that "out of scope" section**: the Phase-0 measurement (§1.2) shows the morsel fusion is *not* a multi-month re-architecture when scoped to a single fused operator — it is a single `ExecutionPlan` node, ~the same blast radius as `EmatixHashJoinExec`. Σ.S.B (cascading bloom) was measured **net-negative on this exact shape** (bloom on a uniform FK relocates probe work, see REV.21 in the capability ADR §1.3(3)); the morsel pipeline removes the materialization instead of relocating the probe, which is why it wins where cascading bloom did not.
  - **`docs/ADR_SELECTION_VECTOR_MATERIALIZATION.md`** (OPEN) — the scan-side selection-vector ADR. Its `(RecordBatch, Option<Selection>)` envelope is the same envelope this pipeline's `Morsel` carries internally (§4.1). The two converge; do not fork the engine twice. The in-pipeline selection-aware aggregate that would fully realize that ADR is **Phase 5+ (out of scope here)**.

---

## 1. Context

### 1.1 What we are building on (mechanics, file:line)

ematix-flow is a Rust analytical engine **on** DataFusion 53.1, pull-based (volcano-batch): `ExecutionPlan::execute(partition) -> SendableRecordBatchStream`; each operator pulls `RecordBatch`es. The build plan reuses four existing, shipped seams — this is not greenfield:

1. **Per-RG decode primitives** — `crates/ematix-flow-core/src/ematix_parquet_bridge.rs`:
   - `open_cached(path: &Path) -> DfResult<Arc<ParquetFile>>` (`:129`) — process-cached file handle; `f.cached_metadata().row_groups` gives the row-group list (`:167`).
   - `masked_decode_i64(file, rg, col, mask) -> DfResult<Vec<i64>>` (`:808`), `masked_decode_f64` (`:821`), `masked_decode_i32` (`:795`) — decode one column of one row group under a byte-mask, returning a dense `Vec`. **These are exactly the per-RG, per-column primitives the morsel loop calls.** The Phase-0 prototype already drives them (`pv1_fused_q08_pipeline.rs:309-313`).
2. **The join kernel + null/widening discipline** — `crates/ematix-flow-core/src/emat_hash_join.rs`:
   - `EmatHashJoiner::try_build(batches, build_key_idx, probe_key_idx, output, out_schema)` (`:64`) and `probe(&RecordBatch)` (`:100`).
   - `key_as_i64(col) -> Option<(Vec<i64>, Option<Vec<bool>>)>` (`:37`) — the **canonical key-extraction + null path** (Int64 direct, Int32 widened `:44`, validity pulled from `col.nulls()` `:38`). `null_keys_never_match` test at `:167`. The morsel layer must route every key through this, never hand-roll nulls (C1 below).
   - `JoinColumn::{Build(i), Probe(i)}` (`:29`) — the output-column mapping enum.
   - **The materialization tax in code:** `probe` collects `Vec<ProbeMatch>`, builds two `UInt32Array` index vectors, calls Arrow `take` per output column (`:113-114`), assembles a fresh `RecordBatch` (`:119`). This is what the pipeline removes.
3. **The integration-seam operator + swap rule** — `crates/ematix-flow-core/src/emat_hash_join_exec.rs` (gated `EMAT_HASH_JOIN=1`) and `swap_emat_hash_join_rule.rs`. `EmatixHashJoinExec` is a working CollectLeft `ExecutionPlan` that builds once into `OnceCell<Arc<EmatHashJoiner>>` (`:163`) and advertises CollectLeft `PlanProperties` (output partitioning = probe side's, `:60`). `try_swap` (`swap_emat_hash_join_rule.rs:47`) is the shape gate: `JoinType::Inner` + `PartitionMode::CollectLeft` + single i64-widenable equi-key + no non-equi filter + name-match-with-uniqueness-bail (`:78-97`). **`EmatPushPipelineExec` is the same architectural move at one level up: fuse the scan→filter→project→probe chain, not just the probe.**
4. **The codegen-tax-safe install seam** — `crates/ematix-flow-core/src/flow_query_planner.rs`: `FlowQueryPlanner::rewrite(LogicalPlan) -> LogicalPlan` (`:63`) runs the pre-plan walkers (agg-semi → dim-push → reorder) **post-optimization, outside the compiled optimizer rule loop**, explicitly to dodge the 5-8% codegen tax (`:19-27`). Installed via `with_query_planner` in `preset.rs:217`. **The fusion recognizer (Phase 3) installs here, not as a `PhysicalOptimizerRule`.**
5. **The domain-bound stat surface** — `crates/ematix-flow-core/src/ematix_fast_parquet.rs`: `int64_col_fits_i32(stats, idx)` (`:1558`) already reads `cs.min_value`/`cs.max_value` from `ColumnStatistics` to decide downcast; `estimate_pass_rate(stats)` (`:713`) reads the same min/max. **The adaptive probe-structure selector (§3, the #1 surface) reuses this exact min/max-from-footer machinery to learn the FK domain bound** (e.g. `p_partkey ∈ [1, 2_000_000]`).

### 1.2 Why (measured this session — established, not hypothesized)

TPC-H **Q08/Q07/Q10/Q17/Q20 lose ~8-16% to DuckDB at SF=10/100.** The root cause is **not decode**:

- ematix decodes a 5-col SF=10 lineitem scan in **143ms** vs DuckDB **157ms** (ematix is *faster* at decode). The payload Snappy decompress is unavoidable and shared: parquet pages hold ~20,480 values, so at Q08's 0.06% selectivity ~100% of pages still hold ≥1 survivor → late-materialization skips ~0% of decompress. Decode is **88% of the floor and shared with DuckDB.** It is explicitly **out of scope.**
- The gap is the **PULL-model inter-operator materialization tax**: DataFusion materializes a 60M-row Arrow `RecordBatch` between the lineitem scan and the part⋈lineitem join, repartitions it, and `take`-gathers at each join. DuckDB push-fuses the probe inline and **never builds the intermediate.**

**Phase-0 prototype result** (`crates/ematix-flow-core/examples/pv1_fused_q08_pipeline.rs`, 14 threads, SF=10, mimalloc, interleaved): a hand-fused row-group-parallel Q08 hot path (per RG: decode 5 cols → probe part-bitset + orders-HashMap + supplier-bitset inline → accumulate the 2-year aggregate, no inter-operator batch) measured:

| Arm | Median | vs PULL | vs DuckDB |
|---|---:|---:|---:|
| PULL (real production Q08) | 205 ms | — | +23% |
| **PUSH fused (dense-bitset probe)** | **161 ms** | **−16% (−44 ms)** | **−4% (beats DuckDB 167)** |
| PUSH fused (naive HashSet probe) | 251 ms | **+22% (LOSES)** | +50% |
| decode-only floor | 141 ms | — | — |

`mkt_share` matches DuckDB (correct: 1995≈0.0345, 1996≈0.0395). Scales 7→14 cores **1.5×** (RG-parallel, no column fan-out). **This clears the capability ADR's 12% kill-gate (−16% on the loop) and resolves it GO.**

### 1.3 The three findings this build plan is built on

1. **The recoverable win (~16%, flips the losses) is removing the materialize/repartition/`take`/coalesce/alloc tax BETWEEN operators — NOT decode.** Decode is at parity-or-better and shared. The pipeline's *only* structural job is to never build the 60M-row intermediate and never `take` per boundary; it compacts exactly once, at its output.

2. **HARD CONSTRAINT — adaptive probe-structure selection is the #1 design surface, and getting it wrong makes PUSH LOSE.** The same fused loop with a **naive `HashSet`** over the 60M-row probe side measured **+22% (251ms, slower than PULL)**; with a **dense bitset** keyed on the bounded FK domain (`partkey ∈ [1, 2M]`) it measured **−16% (161ms)**. A 90ms swing on the probe structure alone. The engine MUST pick a domain-aware fast probe structure (dense bitset / perfect-hash for bounded integer FK domains) from column min/max stats, or the entire win inverts. This is not an optimization knob — it is load-bearing for correctness-of-the-thesis. §3 specifies it.

   > This also retroactively explains the capability ADR's "dense bitset was 10.8× in microbench but neutral on Q08 wall" puzzle: the prior dense-bitset test left the per-boundary `take` in place (it only swapped the probe inside `EmatHashJoiner`), so the probe cost it removed was ~0% of wall while the `take` it left was the actual cost. **The bitset only pays once the materialization is also removed** — which is the whole point of the pipeline. The two levers are inseparable: fused-loop *and* domain-aware probe, together, or neither.

3. **RG-parallel morsel execution scales where the existing path plateaus.** One worker per row-group, columns decoded sequentially within (`pv1_fused_q08_pipeline.rs:303-337`), scaled 7→14 cores at 1.5×. This sidesteps the ~7-core plateau the existing column×partition-parallel masked-decode path hits (memory REV.21/REV.22: forcing the per-column thread budget was a no-op because two layers of work-stealing oversubscribe). The parallelism unit is **the row group**, not the column.

### 1.4 The forces (why this and not something cheaper) — abbreviated; full analysis in the capability ADR §1.4

- **The pull model compacts at every boundary** — measured 60M-row intermediate + per-join `take`. This is the cost.
- **Codegen-sensitivity tax** — new optimizer rules in `ematix-flow-core` have cost 5-8% geomean on *unrelated* queries (`project_optimizer_codegen_sensitivity.md`). Mitigation that has worked: **sibling crate** + **pre-plan walker, not `PhysicalOptimizerRule`**. The build plan uses both.
- **Microbench-doesn't-translate is the house rule** — lever-4, hand-rolled Snappy, Σ.N.e/f, the 2.36× L13 probe artifact, the dense-bitset-neutral result, PV.0's faithful-push-emit tie. **Every phase gates on end-to-end wall, never a kernel bench.** This is why Phase-0 measured the *full Q08 loop on real data*, not a probe microbench.
- **No TPC-H-specific hardcoding** — the recognizer keys on plan *shape* (Inner/CollectLeft, scan→filter→project→probe, small filtered build, bounded-int FK), never on "Q08".
- **Opt-in → gated → default-on only after the gate** — the `EMAT_HASH_JOIN` pattern (`swap_emat_hash_join_rule.rs:125`). Flag: `EMAT_PUSH_PIPELINE=1`.

---

## 2. Decision

**Build `EmatPushPipelineExec`: a single DataFusion `ExecutionPlan` node that internally runs a fused, RG-parallel, push-based morsel pipeline over a `scan → filter → project → hash-probe → (Partial-agg)` subtree, materializing a `RecordBatch` only at its output boundary, with an adaptive probe-structure selector that picks a dense bitset / perfect-hash for bounded-integer FK domains and a hash table otherwise. Build it in four independently-shippable phases (PV.1–PV.4), each gated by `tpch_validate` 22/22 (sums, not row counts) + 22q SF=10/SF=100 strict interleaved A/B + a per-phase measured go/no-go, behind `EMAT_PUSH_PIPELINE=1`, default OFF until Phase 4.**

Load-bearing parts:

1. **Hybrid, not replacement.** One `ExecutionPlan` node. Pull on the outside (consumes dense `RecordBatch`es from DataFusion-planned leaves; emits dense `RecordBatch`es to whatever consumes it), push on the inside. DataFusion stays the parser, logical optimizer, physical planner, and executor of everything outside the fused fragment. Same stance as `EmatixHashJoinExec`/`RobinHoodAggregateExec`. Full volcano replacement is **rejected** (capability ADR §8 Alternative B).

2. **Push kernel in a sibling crate.** New crate `ematix-flow-push` (or a module added to `ematix-flow-hash-join`), **pure kernel, no DataFusion/Arrow dep at the core** where it pays, to keep the codegen tax local (the empirically-clean mitigation, `ematix-flow-hash-join/src/lib.rs:1-28`). The DataFusion glue (`EmatPushPipelineExec`) is a thin consumer in `ematix-flow-core`.

3. **The recognizer is a pre-plan walker in `FlowQueryPlanner`**, not a `PhysicalOptimizerRule` (`flow_query_planner.rs:19-27` rationale).

4. **Adaptive probe-structure selection is the central component**, with its decision rule sourced from column min/max footer stats (the same surface `int64_col_fits_i32`/`estimate_pass_rate` already read), not a guess. §3.

5. **Strictly opt-in → gated → default-on only after the full gate passes.** No default-on without 22q SF=10 *and* SF=100 interleaved A/B (≥11 trials, paired sign test) + `tpch_validate` 22/22 SF=1/10/100 on **sums**, + the codegen-tax check (compiled-in-flag-OFF vs prior binary ≤ noise).

---

## 3. Adaptive probe-structure selection (THE #1 DESIGN SURFACE)

This section is first among the design sections because finding 2 (§1.3) makes it load-bearing: the wrong choice inverts the win (+22% vs −16%). It lives in the sibling crate as a pure function over per-key stats, mirroring `build_side::choose` (`ematix-flow-hash-join/src/build_side.rs:137`).

### 3.1 The structures

```rust
// crate: ematix-flow-push. Pure kernel. The build side (small, filtered)
// is collected once; the probe key is tested against ONE of these per row.
pub enum ProbeStructure {
    /// Dense membership bitset indexed directly by the key value.
    /// O(1) probe, one bit per domain value, zero hashing, branch-light.
    /// REQUIRES a bounded non-negative integer domain [0, hi] with hi
    /// small enough that hi/8 bytes is affordable (the gate, §3.3).
    /// This is the structure that measured −16% on Q08.
    DenseBitset { hi: i64, bits: Box<[u64]> },

    /// Dense value→payload map for non-unique builds or when the probe
    /// must recover a build-row index / payload (e.g. orders_year:
    /// o_orderkey -> year-bucket). Direct-indexed Vec<Option<T>> when the
    /// domain is bounded; this is the perfect-hash case (the key IS the
    /// index). O(1), no hashing.
    DensePayload { hi: i64, slots: Box<[PayloadSlot]> },

    /// Fallback: open-addressing hash table (the L13 RobinHood kernel,
    /// `RobinHoodHashJoinI64Table`). Used when the domain is unbounded,
    /// too large to densify, or signed/negative. This is what makes a
    /// non-bounded-FK join still correct (just not bitset-fast).
    HashTable(RobinHoodHashJoinI64Table),
}
```

`DensePayload` generalizes the bitset: a bitset is a `DensePayload` whose payload is one bit. The Q08 chain needs both forms — `part` survivors are a pure membership bitset; `orders` survivors carry a year-bucket payload; `supplier` BRAZIL is a bitset (`pv1_fused_q08_pipeline.rs:56-60` uses exactly these three: `part_bitset: Vec<bool>`, `orders_year: HashMap<i64,u8>` → becomes `DensePayload`, `supplier_brazil: Vec<bool>`).

### 3.2 The decision rule (deterministic, stats-driven)

For each build side, after it is collected (so we know the actual surviving key set) AND from the **probe-key column's footer min/max** (so we know the domain the probe will test against):

```
let (key_min, key_max) = probe_key_column_stats.min_max();   // from ColumnStatistics, as int64_col_fits_i32 already reads
let domain_hi = max(key_max, build_key_max);                 // domain we must cover
let build_is_unique = build_side has no duplicate keys;      // known after collect

choose_probe_structure(domain_min, domain_hi, build_is_unique, payload_needed):
  // GATE 1: bounded non-negative integer domain, densifiable.
  if domain_min >= 0
     && domain_hi >= 0
     && (domain_hi as u128 + 1) <= DENSE_MAX_DOMAIN          // §3.3 size gate
     && key column type widens_to_i64 (Int32/Int64):
        if !payload_needed:
            DenseBitset    { hi: domain_hi, .. }             // membership only (part, supplier)
        else:
            DensePayload   { hi: domain_hi, .. }             // carries payload (orders→year)
  // FALLBACK: anything else stays correct on the hash table.
  else:
        HashTable(RobinHoodHashJoinI64Table)                 // unbounded / huge / signed / non-int
```

- **`domain_min`/`domain_hi` come from `ColumnStatistics.min_value`/`max_value`** on the probe-key column — the exact field `int64_col_fits_i32` (`ematix_fast_parquet.rs:1558`) and `estimate_pass_rate` (`:713`) already extract. If stats are **absent or `Unknown`** (the `Precision::Absent` case those helpers already handle), we **fall back to `HashTable`** — never guess a bound (a wrong bound = out-of-bounds index = panic or wrong answer; the fallback is the safe default, exactly as `build_side::choose` defaults to Left on `None`).
- **`build_is_unique`** decides bitset-vs-payload only when payload isn't otherwise needed; non-unique builds that need to enumerate all matches use the hash table (the pipeline shape in Phases 1-2 is FK-reduction-into-agg where the build is a dimension PK — unique by construction — so the dense path dominates; multi-match is §5/C-table).

### 3.3 The size gate (memory bound on densification)

`DENSE_MAX_DOMAIN` bounds the bitset/payload allocation so a pathological domain (e.g. a synthetic key with `max = i64::MAX`) can never OOM:

- **`DenseBitset`**: `domain_hi/8` bytes. Gate at **`domain_hi + 1 <= 256M` → 32 MB** per bitset. SF=10 `p_partkey` (2M) = 256 KB; SF=100 (20M) = 2.5 MB; SF=1000 (200M) = 25 MB — all under the gate. Above it → `HashTable`.
- **`DensePayload<T>`**: `(domain_hi+1) * size_of::<PayloadSlot>` bytes. With a 1-byte bucket payload (Q08's year), gate at `domain_hi + 1 <= 32M` → 32 MB. SF=10 `o_orderkey` domain is ~15M (orders is 1.5M rows but orderkey is sparse to ~6× at SF=10 per TPC-H gen) — **this is the one to watch**; if the orderkey domain exceeds the gate at high SF, `orders` falls back to the hash table while `part`/`supplier` stay dense. That mixed outcome is fine and is the designed behavior.
- The gate is per-structure and read at build time from the (already-collected) build key max AND the probe-column footer max, taking the **min of (footer-derived bound, an absolute ceiling)** so we never trust an adversarial footer beyond the ceiling.

### 3.4 Fallback correctness invariant

**Every shape that does not pass the dense gate is still executed correctly via `HashTable`.** The dense path is a *performance* specialization; the hash table is the *correctness* floor. This means a query whose FK domain is unbounded does not bail out of the pipeline — it runs the pipeline with a hash-table probe (still removing the materialization tax, finding 1), just without the bitset speedup (finding 2). The recognizer (§5.5) only declines to *fuse* on shape grounds (non-CollectLeft, non-Inner, etc.), never on probe-structure grounds.

> Open question (OQ-PS-1): on a hash-table-probe fragment, does removing the materialization tax *alone* (without the bitset) still beat pull? Phase 0 measured both arms with the materialization removed; the hash-table-equivalent (open-addressing) arm was not separately reported at −16%. **Phase 1's gate (§6) must measure the hash-table-probe pipeline against pull on a bounded-but-not-densified shape** to confirm the fallback is at worst neutral, not a regression. If the hash-table-probe pipeline regresses, the recognizer must additionally gate on "domain is densifiable" — making the dense path a fusion precondition, not just a specialization.

---

## 4. The morsel pipeline — abstractions and execution

(Envelope and operator interface are carried from the capability ADR §3 with the build-relevant detail made concrete. Where that ADR sketched, this one commits to signatures Phase 1 builds against.)

### 4.1 The morsel

```rust
// crate: ematix-flow-push.
pub struct Morsel {
    /// Dense decoded columns for this chunk (as produced by masked_decode_*
    /// or sliced from the scan's RecordBatch). Borrowed for the push call.
    cols: Vec<ArrayRef>,
    /// Surviving row indices. `All(n)` = fast path, no allocation. `Indices`
    /// after a filter/probe narrows it. THIS defers the take: downstream
    /// reads cols[c] THROUGH sel; no eager gather until the terminal sink.
    sel: Selection,
    /// Absolute row offset of cols[*][0] in the source partition (provenance
    /// for a breaker that needs stable row numbering, e.g. multi-batch build).
    row_base: u64,
}
pub enum Selection { All(usize), Indices(Vec<u32>) }
```

**Morsel size: 4-16K rows (tuned), NOT whole row groups.** Whole-RG emission stalls downstream at single-digit-ms RG decode (`project_sigma_e5_6_streaming_doesnt_win.md`). A morsel stays L2-resident across filter→probe→emit — the mechanism by which decode overlaps probe. *Note:* the Phase-0 prototype decoded a whole RG then looped rows (`pv1_fused_q08_pipeline.rs:314-335`) and still won −16%; sub-RG morselization is expected to widen, not create, the margin. Phase 1 makes morsel size a tunable and measures it (gate in §6).

Why "Arrow arrays + selection" not "raw buffers": the bridge decodes into Arrow (`masked_decode_*` return `Vec<i64/f64/i32>` which the scan wraps as Arrow; the scan's output stream is Arrow). Re-deriving raw buffers duplicates the bridge and risks the null/def-level bugs §4.4 catalogs. Reuse `Array::slice` (offset-correct, null-correct) for morsel boundaries.

### 4.2 The push operator interface

```rust
pub trait PushOperator: Send {
    /// Pipelined op: narrow m.sel (or rebind cols for a projection) and
    /// out.emit(m). Breaker: absorb m into internal state, emit nothing.
    fn push(&mut self, m: Morsel, out: &mut dyn Sink) -> Result<(), PushError>;
    /// Source drained. Breakers flush here (an agg emits its groups).
    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), PushError>;
}
pub trait Sink { fn emit(&mut self, m: Morsel) -> Result<(), PushError>; }
```

The three Phase-1 operators:
- `FilterPush` — narrows `sel` by a `BridgeFilter`-equivalent predicate (reuse the existing predicate eval; range/eq on decoded columns).
- `ProjectPush` — rebinds `cols` to the projected subset by **explicit index** (no implicit reordering; C3 below).
- `ProbePush` — the heart: tests each live key against the `ProbeStructure` (§3), narrows `sel` to matches, and (for a payload probe) attaches the recovered payload column. **This IS the filter+narrow that replaces the join `take`.**

Breaker semantics:
- **Build side** is a breaker, collected once before the pipeline starts (reuse `EmatixHashJoinExec`'s `OnceCell<Arc<…>>` pattern, `:163`), then frozen into a `ProbeStructure`.
- **Probe side** is **pipelined** — the win.
- **Partial-aggregate** (Phase 2+ for the Q08 chain) is a breaker that accumulates per-group state and flushes at `finish`. In Phase 1 the agg is **stock DataFusion** outside the pipeline boundary (compact-at-boundary, correct).

### 4.3 Execution and DataFusion composition

`EmatPushPipelineExec` implements `ExecutionPlan`:
- `children()` = the original leaf `ExecutionPlan`s feeding the fused chain (the scan; and the build-side subtree). The pipeline **does not re-implement the parquet reader** — Phase 1 wraps the existing `EmatixFastParquetExec` output stream and slices each `RecordBatch` into morsels. (A later phase MAY teach the scan to emit morsels natively to skip one Arrow round-trip — deferred, §5.)
- `properties()` advertises **exactly** the matched join's `PlanProperties` — output partitioning = probe side's, `EmissionType::Incremental`, `Boundedness::Bounded` — so the node is properties-indistinguishable from the operator chain it replaces (`emat_hash_join_exec.rs:60-66`). This keeps EnforceDistribution and the agg-partition rules from mis-firing (I4 below).
- `schema()` = the matched join's output schema; `schema_check()` returns true honestly.
- `execute(partition)` returns a `RecordBatchStreamAdapter` (`:196` pattern) whose stream: on first poll awaits the shared build (`OnceCell`), then pulls the leaf scan stream for `partition`, slices into morsels, drives them through the compiled `PushOperator` chain, and yields the terminal sink's compacted batches.

**Parallelism: reuse DataFusion's partition fan-out; do NOT build a separate morsel scheduler in Phases 1-4.** DataFusion spawns one `execute(partition)` stream per partition (14 at `target_partitions=14`, `paired_ab.rs:223`); the pipeline runs per-partition exactly like every operator → 14 independent pipelines, no scheduler. The within-pipeline loop is single-threaded per partition (sequential push = cache-friendly). The Phase-0 prototype's RG-parallel scaling (1.5× 7→14) is *itself* the partition fan-out (one worker per RG = DataFusion's per-partition stream consuming that RG range). A work-stealing morsel scheduler on top of tokio risks the REV.21 double-parallelism oversubscription footgun and is **out of scope** (§5), gated on Phase-0's CPU/wall + thread-park measurement (which the capability ADR's Phase-0 add-on resolves).

### 4.4 Correctness + safety (the full Q08 chain)

The Q08 chain is **7 joins**: `part⋈lineitem`, `lineitem⋈orders`, `lineitem⋈supplier`, `supplier⋈nation` (n1), `orders⋈customer`, `customer⋈nation` (n2), `nation⋈region`. The build plan does NOT fuse all seven into one pipeline. The structure (matching the Phase-0 prototype and DuckDB's plan) is:

- The **dimension reductions** (`part` filtered by `p_type`; `supplier⋈nation(n1)` filtered to BRAZIL; `orders⋈customer⋈nation(n2)⋈region` filtered to AMERICA + date, projected to `o_orderkey → year-bucket`) are **build sides** — each collected once, frozen into a `ProbeStructure`. In Phases 1-2 these are produced by **stock DataFusion subplans** (exactly as `build_dims` does via SQL, `pv1_fused_q08_pipeline.rs:199-281`); the pipeline consumes their collected output. They are small (13K / ~thousands / ~survivors), CollectLeft-eligible, and not on the hot path.
- The **one fused pipeline** is the lineitem hot path: `scan(lineitem) → probe(part-bitset) → probe(orders-payload, attach year) → probe(supplier-bitset) → emit (year, ext, disc, brazil-flag)`, feeding a stock `Partial`-aggregate. Three probes, one decode pass, one compaction. **This is the loop Phase-0 measured.**

Correctness threats (STRIDE-adapted; full table in capability ADR §4; build-specific deltas here):

| # | Threat | Mitigation (class) |
|---|---|---|
| C1 | **Null / def-level mishandling in morsel slicing.** A wrong null bitmap silently changes results; NULL keys must never match (Inner). The codebase was burned — Q07 sums 94% wrong for months, masked by row-count-only checks (`runtime_bloom_sideband_rule.rs:496`). | **Eliminate**: route every key through `key_as_i64` (`emat_hash_join.rs:37`, which pulls validity from `col.nulls()`); use `Array::slice` for morsel boundaries; never hand-roll nulls. **Gate**: `tpch_validate` checks **sums**, SF=1/10/100. TDD: a failing null-join test before any pipeline code. |
| C2 | **i64→i32 downcast interaction.** The scan may advertise Int32 narrowed from Int64 (KEYS.2; `decode_schema` vs `schema`, `ematix_fast_parquet.rs`). | **Mitigate**: consume the scan's *output* stream (post-narrowing) so the advertised schema holds by construction; restrict keys to `widens_to_i64` (`swap_emat_hash_join_rule.rs:41`); bail to stock otherwise. The `ProbeStructure` domain bound uses the *advertised* (possibly-narrowed) column's stats. |
| C3 | **Projection / column-index drift.** A `ProjectPush` in the chain reorders columns; the `JoinColumn::{Build,Probe}` output mapping must compose through it, not assume scan-order. | **Eliminate**: derive the output mapping from the matched join's output schema by name-with-uniqueness-bail (the exact `swap_emat_hash_join_rule.rs:78-97` logic); `ProjectPush` rebinds by explicit index. Test: a fused pipeline whose projection permutes columns, asserted column-for-column vs the stock plan. |
| C4 | **Multi-match (non-unique build).** A build side with duplicate keys (not the Q08 dimension-PK case, but a general FK⋈FK) must enumerate ALL matches per probe row — a bitset can't, a `DensePayload` with one slot can't. | **Eliminate**: `build_is_unique` (§3.2) is computed at collect time; non-unique builds route to `HashTable` (which enumerates via `ProbeMatch` chains, `emat_hash_join.rs:103`). The dense path is gated on uniqueness. A non-unique probe emits a morsel whose `Selection::Indices` may repeat a source row (one per match) — the terminal sink's `take` handles repeats natively (Arrow `take` allows duplicate indices). Test: non-unique build, multi-match, vs stock. |
| C5 | **Partial-agg merge correctness.** The pipeline feeds a stock `Partial` agg; DataFusion's `Final` agg merges across partitions. f64 SUM ordering across morsels/partitions must match the validated result. | **Mitigate / Accept**: Phase 1-2 keep the agg on stock DataFusion (the pipeline emits the same rows the stock join would, just without the intermediate), so the agg sees identical input multisets → identical partial+final merge. The `DedupeAggregateForFloatDeterminism` rule (preset.rs:130) still applies to the post-pipeline plan. Pulling the agg *into* the pipeline (Phase 5+) must re-address f64 determinism explicitly — **out of scope here.** |
| C6 | **Multi-reference table aliasing** (Q21's three lineitem scans). Fusing the wrong scan instance corrupts a correlated subquery. | **Eliminate**: match the fusable scan by `Arc::ptr_eq` (the L9 discipline, `runtime_bloom_sideband_rule.rs:506`); the CollectLeft + single-equi-key + no-filter gate excludes most subquery shapes anyway. |

### 4.5 Memory / spill

- **Build-side size bound:** the CollectLeft gate IS the build-size guard — DataFusion already proved the build is broadcast-small (`swap_emat_hash_join_rule.rs:51`). Never fuse a `Partitioned`-mode join. Plus the §3.3 `DENSE_MAX_DOMAIN` gate bounds the densified structure independently of row count (a small build can still have a large *domain*; the gate catches that).
- **What if a build side isn't small:** the recognizer declines to fuse (non-CollectLeft → bail), leaving the stock plan. There is no "fuse then OOM" path.
- **Morsel buffer sizing:** O(morsel) live memory per partition (4-16K rows × cols), not O(partition) — compaction at the terminal sink is per-morsel-batch, bounded. No unbounded accumulator in Phases 1-4 (probe is streaming, build is gated) → **no spill needed.** Spill becomes relevant only if an agg moves in-pipeline (Phase 5+).

---

## 5. Scope: which subtrees to fuse, and the recognizer

### 5.1 The target shape (precise)

Fuse **a linear chain of Inner equi-joins (FK reductions) feeding a `Partial` aggregate, with small filtered build sides.** Concretely the recognizer fires only when ALL hold:

1. The fragment root is an `AggregateExec(Partial)` **or** a fusable `HashJoinExec`/`EmatixHashJoinExec` whose output feeds a `Partial` agg.
2. Each join in the chain is `JoinType::Inner` (or `LeftSemi` for a pure reduction) **and** `PartitionMode::CollectLeft` (the build-size guard).
3. Each join has exactly one equi-key, both sides a bare `Column`, both `widens_to_i64` (Int32/Int64). No non-equi `filter`.
4. The probe spine walks down through pipelined wrappers (`FilterExec`, `ProjectionExec`, `CoalesceBatchesExec`) to a single `EmatixFastParquetExec`, matched by `Arc::ptr_eq` (C6).
5. The build side of each join is "small" — guaranteed by (2)'s CollectLeft.
6. Output column mapping resolves unambiguously by name (uniqueness-bail, C3).

### 5.2 Where pull already wins — DO NOT fire

The recognizer must **not** fire where the materialization tax is absent or where pull is already winning:
- **No `Partial` agg downstream** (the fragment is a join feeding a sort/limit/output) — the materialization tax this targets is the scan→join→agg path; without the agg the win evidence (Phase-0) doesn't apply. *Conservative Phase-1 stance: require the `Partial` agg.*
- **Build side not CollectLeft** (Partitioned join) — large build, different regime, M1 OOM risk. Bail.
- **A `runtime_sideband` is already attached to the scan** (`scan.runtime_sideband().is_some()`, `ematix_fast_parquet.rs`) — the L9 sideband and the fused path are mutually exclusive in v1 (both shed probe-side rows; pick one per scan). Bail to leave L9 in place (I3). Re-evaluate composition later.
- **Non-bounded / non-densifiable FK domain** — *only if* OQ-PS-1 (§3.4) shows the hash-table-probe pipeline regresses. If it's neutral-or-better, fuse anyway with a `HashTable` probe.
- **Any shape where SF=1 or the broad geomean regresses in A/B** — the empirical backstop; the gate (§6) reverts any regressor.

### 5.3 Generality (not Q08-specific)

The shape "Inner/CollectLeft, i64-key, scan→(filter)→(project)→probe-chain → Partial agg, bounded-int FK" matches across queries: Q08 (part/orders/supplier FK reductions into the mkt_share agg), and structurally Q07/Q10/Q17/Q20 (the other measured losers — each an FK reduction into an aggregate). The recognizer is the static analogue of the trusted L9 walker (`find_probe_scan_for_column`, `runtime_bloom_sideband_rule.rs:477`). **No query-name matching.**

### 5.4 Where it installs

In `FlowQueryPlanner::rewrite` (`flow_query_planner.rs:63`), as a 4th step **after** agg-semi → dim-push → reorder, and (since it operates on the *physical* plan) actually as a post-`create_physical_plan` transform — running after EnforceDistribution/ForceCollectLeft have assigned `partition_mode()`, so the CollectLeft gate sees the final assignment (the ordering note in `swap_emat_hash_join_rule.rs:22`). Gated `EMAT_PUSH_PIPELINE=1`, default OFF.

> Design note: the existing `FlowQueryPlanner` rewrites the *logical* plan then delegates to `DefaultPhysicalPlanner`. The fusion recognizer needs the *physical* plan (to see `PartitionMode`). So Phase 3 wraps the physical-plan output: `create_physical_plan` → (existing logical rewrites) → `DefaultPhysicalPlanner` → **fusion transform** → return. This is the same `transform_up` pattern as `SwapEmatixHashJoinRule::optimize` (`swap_emat_hash_join_rule.rs:133`), invoked from the QueryPlanner rather than registered as a `PhysicalOptimizerRule` (codegen-tax avoidance).

---

## 6. Phasing — PV.1 through PV.4 (each independently shippable, each gated)

Maps onto / replaces the pending PV.1–PV.4 tasks and the capability ADR §5 roadmap. **Phase 0 is DONE (§1.2, the `pv1_fused_q08_pipeline.rs` spike cleared −16% > the 12% kill-gate).** Every code phase passes the **shared gate** before merge:

- **22q SF=10 AND SF=100 interleaved A/B**, ≥11 trials, paired sign test (`crates/ematix-flow-core/examples/paired_ab.rs`; `scripts/bench/strict_ab.sh`/`strict_22q.sh`), fresh `SessionContext` per trial (Σ.P CSE-replay footgun).
- **`tpch_validate` 22/22** vs DuckDB SF=1/10/100 on **sums**, not row counts (C1).
- **No TPC-H hardcoding** (shape-driven).
- **Codegen-tax check** (I1): compiled-in-flag-OFF vs prior-binary 22q A/B ≤ noise.

### PV.1 — Push kernel crate (`ematix-flow-push`), unit-tested only. No DataFusion.

**Build:** `Morsel`, `Selection`, `PushOperator`, `Sink`; the three operators (`FilterPush`, `ProjectPush`, `ProbePush`); and **the `ProbeStructure` selector (§3) — the centerpiece of this phase.** Pure kernel. TDD anchors: null-join correctness, projection-permutation, selection-narrowing, multi-batch build, multi-match (C4), **and the probe-structure decision table** (bounded→bitset, payload→DensePayload, unbounded→HashTable, absent-stats→HashTable, over-gate-domain→HashTable).

**Go/no-go (measured, not just green tests):** re-express the Phase-0 Arm-B Q08 loop *through the new abstraction* (still a throwaway harness, no SQL integration) and confirm it **still clears 12% vs the pull baseline** on real SF=10 data. This proves the abstraction added no overhead (the morsel/trait indirection didn't eat the win). **Kill:** if the abstracted loop drops below 12%, the trait/morsel layering is too costly — redesign the operator interface (e.g. monomorphize the chain, drop `dyn`) before proceeding. **Additionally measure OQ-PS-1** (§3.4): the hash-table-probe variant of the same loop vs pull — record whether the fallback is neutral or a regression (decides whether §5.2 must gate on densifiability).

### PV.2 — `EmatPushPipelineExec` operator, direct-construction only. No recognizer.

**Build:** the thin `ExecutionPlan` glue in `ematix-flow-core` (§4.3): build-once `OnceCell`, morsel-slicing of the scan stream, drive the PV.1 chain, `RecordBatchStreamAdapter` output, matched-join `PlanProperties`. Construct it **by hand** in a test/bench against the Q08 lineitem fragment (build sides from stock SQL subplans, as `pv1` does).

**Go/no-go:** (a) **bit-identical output** to the stock `scan→filter→project→HashJoin→Partial-agg` chain, column-for-column AND on summed values, at SF=1 and SF=10 (the `tpch_validate`-on-a-fragment check); (b) a single-query Q08 A/B (hand-constructed operator vs stock, ≥11 trials) showing Q08 wall moves in the Phase-0-predicted direction (toward −16% on the loop, net positive on whole-query Q08). **Kill:** output mismatch → correctness bug, stop and fix (do not proceed with a known-wrong operator). Whole-query Q08 fails to improve despite the loop improving → the loop is not the whole-query bottleneck after blending decode back in (the capability ADR §1.6 "unrecoverable" possibility resurfacing at operator scale) → reassess before PV.3.

### PV.3 — Recognizer (pre-plan walker), flag-gated OFF.

**Build:** the shape-matching walker (§5) in `FlowQueryPlanner` (physical-plan transform, §5.4), behind `EMAT_PUSH_PIPELINE=1`, default OFF. This is the first phase touching the production plan path → the codegen-tax check is mandatory and primary.

**Go/no-go (the real gate):**
- 22q SF=10 **and SF=100** interleaved A/B, flag ON vs OFF, ≥11 trials: **Q08 improves; no other query regresses past 2σ.** (Q07/Q10/Q17/Q20 ideally also improve if the recognizer generalizes; at minimum they must not regress.)
- **Codegen-tax check (I1):** flag-OFF-compiled-in vs prior binary ≤ noise. **Kill:** if flag-OFF-compiled-in regresses the 21 other queries, the sibling-crate boundary did NOT hold — **stop and reassess**; do not ship a broad regression to fix one query (the `Σ.K.A`/`Σ.H.1d` lesson).
- `tpch_validate` 22/22 SF=1/10/100 on sums.
- **Kill (recognizer over-firing):** the dim-push depth-1 descent regressed the geomean 0.80→1.02 by over-firing + double-eval (`project_sigma_qm_slice2_rejected.md`). If the recognizer fires on a shape where compaction-at-boundary ≈ the takes it replaced (net zero or worse on some query), tighten the gate (require the `Partial` agg, require densifiable domain) or revert to opt-in.

### PV.4 — Default-on decision.

Only if PV.3's gate is clean across SF=1/10/100 with the codegen-tax check passing AND no query regresses past 2σ. Until then it ships exactly like `EMAT_HASH_JOIN`: present, correct, dormant, opt-in. **This is a decision phase, not a build phase** — the artifact is a flip of the default and a banked A/B table, or a documented "stays opt-in" (the `EMAT_HASH_JOIN` outcome — it's still opt-in because it didn't clear its whole-query gate, `swap_emat_hash_join_rule.rs:125`).

### Phase 5+ — explicitly OUT of scope (separate ADRs, gated on PV.3 evidence)

- **Selection-aware `Partial` agg in-pipeline** — folds in `ADR_SELECTION_VECTOR_MATERIALIZATION.md` Option C; must solve f64 determinism (C5). The single largest remaining win after PV.4 (removes the compact-at-boundary into the agg).
- **Morsel work-stealing scheduler** — only if Phase-0's CPU/wall + thread-park add-on shows reclaimable partition-boundary idle (§4.3); REV.21 oversubscription footgun gates it.
- **Native morsel emission from the scan** (skip the leaf Arrow round-trip) — an optimization once proven.
- **Cascading-bloom composition (Σ.S.B)** — currently mutually exclusive with the fused path per-scan (I3); revisit whether they compose on multi-hop FK chains.

---

## 7. Risks + kill-criteria (per phase)

This codebase has a long history of microbench wins not translating: lever-4 (`project_lever4_full_build_rejected.md`), hand-rolled Snappy (`project_hand_rolled_snappy_neg.md`), Σ.N.f.3 (`project_sigma_nf3_beats_stock.md`), the 2.36× L13 probe artifact (`project_q08_join_probe_kernel_2026_06.md`), the dim-push depth-1 over-fire (`project_sigma_qm_slice2_rejected.md`), and PV.0's faithful-push-emit *tie*. Every phase therefore has a **measured** go/no-go on end-to-end wall, never a kernel bench.

| Phase | Dominant risk | Kill-criterion (measured) |
|---|---|---|
| PV.1 | The morsel/trait indirection eats the win (over-abstraction). | Abstracted Q08 loop < 12% vs pull on real SF=10 → redesign interface (monomorphize, drop `dyn`) before proceeding. |
| PV.1 | OQ-PS-1: hash-table-probe fallback regresses. | If the hash-table-probe loop regresses vs pull → §5.2 must additionally gate fusion on "densifiable domain"; the fallback is then a bail, not a specialization. |
| PV.2 | Operator produces wrong answers (null/projection/multi-match). | Any bit-level output mismatch vs stock (sums, SF=1/10) → stop and fix; never proceed with a known-wrong operator (the Q07-94%-wrong-sums scar). |
| PV.2 | Loop wins but whole-query Q08 doesn't (decode dominates after blend). | Hand-constructed operator fails to move whole-query Q08 in the predicted direction over ≥11 trials → reassess; the capability ADR §1.6 "unrecoverable" possibility at operator scale. |
| PV.3 | **Codegen tax (I1)** — the 21 other queries regress 5-8% from compiling-in the new code, even flag-OFF. | flag-OFF-compiled-in vs prior binary regresses past noise → the sibling-crate boundary failed; **STOP**, do not ship a broad regression for one query. |
| PV.3 | Recognizer over-fires / net-zero fusion. | Any non-Q08 query regresses past 2σ in 22q A/B (SF=10 or SF=100) → tighten the gate or revert to opt-in (the dim-push-slice2 lesson). |
| PV.3 | Correctness on a shape the fragment-tests missed. | `tpch_validate` < 22/22 on sums at any SF → revert; the gate is row-AND-sum, not row-count-only. |
| PV.4 | Default-on regresses a real (non-TPC-H) workload shape. | Stays opt-in (the `EMAT_HASH_JOIN` precedent) until the broad gate is unambiguous. |

**Residual risk after mitigations:** (1) PV.3 codegen tax — Med likelihood, High impact, not fully eliminable (sibling-crate has worked before but isn't guaranteed); this is the single most important gate. (2) The whole-query win may be small even if positive (Q08 is one query; the broad geomean barely moves) — the ROI is "close a known competitive gap on the headline benchmark," and the owner must want *that* specifically. (3) Microbench-doesn't-translate — mitigated by every phase gating on end-to-end wall.

---

## 8. Consequences

### Positive
- Attacks the **one measured, un-falsified** lever (inter-operator materialization), with a **realized −16% Phase-0 result** that beats DuckDB on the exact loop — not a hypothesis.
- Blast radius is one node, one shape, off by default — the 22 working queries are bit-identical unless flag+shape match.
- Hot code (kernel + probe selector) in a sibling crate keeps the codegen tax local (the empirically-clean mitigation).
- The adaptive probe selector is **reusable** beyond the pipeline (any bounded-int FK probe) and is the component that makes the difference between +22% and −16%.
- Generalizes to Q07/Q10/Q17/Q20 by shape (the other measured losers are the same FK-reduction-into-agg pattern).
- Converges with `ADR_SELECTION_VECTOR_MATERIALIZATION.md` on one morsel/selection envelope — no second engine fork.

### Negative / what becomes harder
- **A second execution model exists** (even gated): contributors must understand that some plans run push-internally; the compact-at-output boundary semantics must stay documented and tested or C1-C6 bite silently.
- **The codegen-tax risk (I1) is real and not fully eliminable** — if the sibling boundary doesn't hold, the effort stops at PV.3 having spent PV.1-2 (a meaningful but bounded sunk cost, far less than a full engine).
- **The whole-query win may be modest** even if the loop win is large — decode is 88% of the floor and unchanged.
- **Aggregates still compact at the boundary** (Phases 1-4) — this does not deliver DuckDB's full end-to-end selection pipeline, only the scan→join fragment. The full model is Phase 5+.
- **Maintenance coupling to DataFusion internals** (`PlanProperties`, `PartitionMode`, build-future timing) — the L9 sideband already carries scar tissue from exactly this.

### Neutral
- Decode path unchanged (correctly — at parity, not the lever).
- Distributed (Arrow Flight) path unaffected — single-node operator.

---

## 9. Out of scope

- **Full volcano replacement** — rejected (capability ADR §8 Alternative B).
- **Decode / Snappy-decompress rate** — shared floor, ematix at parity (`project_rev20_q07_q08_decode_bound.md`).
- **Dynamic-filter / cascading bloom (Σ.S.B)** — measured neutral/net-negative on uniform FK (relocates, doesn't reduce); mutually exclusive with the fused path per-scan in v1.
- **Morsel work-stealing scheduler** — gated on Phase-0 thread-park evidence; separate ADR.
- **In-pipeline aggregates/sorts** — Phase 5+; must solve f64 determinism (C5).
- **Native morsel emission from the scan** — optimization, post-proof.
- **Q15** — decode/orchestration floor, not a materialization-tax query.

---

## 10. References

### Code (file:line, this repo)
- Phase-0 prototype: `crates/ematix-flow-core/examples/pv1_fused_q08_pipeline.rs` (fused RG-parallel loop `:285-352`; dims build `:199-281`; dense-bitset vs HashSet bracket `:317-321`; decode-only floor `:355-393`).
- Per-RG decode primitives: `crates/ematix-flow-core/src/ematix_parquet_bridge.rs` — `open_cached` `:129`, `cached_metadata().row_groups` `:167`, `masked_decode_i32` `:795`, `masked_decode_i64` `:808`, `masked_decode_f64` `:821`.
- Join kernel + null/widening: `crates/ematix-flow-core/src/emat_hash_join.rs` — `key_as_i64` (+ nulls/widen) `:37`, `try_build` `:64`, `probe` (the `take` tax) `:100`-`:121`, `JoinColumn` `:29`, `null_keys_never_match` `:167`.
- Integration-seam operator: `crates/ematix-flow-core/src/emat_hash_join_exec.rs` — build-once `OnceCell` `:163`, CollectLeft `PlanProperties` `:60`, `RecordBatchStreamAdapter` `:196`.
- Shape gate + swap precedent: `crates/ematix-flow-core/src/swap_emat_hash_join_rule.rs` — `try_swap` gate `:47`, CollectLeft=build-size-guard `:51`, `widens_to_i64` `:41`, name-match-uniqueness-bail `:78`-`:97`, `transform_up` `:133`, opt-in `EMAT_HASH_JOIN` `:125`, post-physical-rules ordering note `:22`.
- Probe-structure precedent (pure-fn selector pattern): `crates/ematix-flow-hash-join/src/build_side.rs` — `choose` `:137`, `SideStats`/`StatsSource` `:51`-`:82`. Crate codegen-isolation rationale: `crates/ematix-flow-hash-join/src/lib.rs:1`-`:28`.
- Domain-bound stat surface (for §3): `crates/ematix-flow-core/src/ematix_fast_parquet.rs` — `int64_col_fits_i32` (min/max from `ColumnStatistics`) `:1558`, `estimate_pass_rate` `:713`.
- Codegen-tax-safe install seam: `crates/ematix-flow-core/src/flow_query_planner.rs` — `rewrite` `:63`, QueryPlanner-not-OptimizerRule rationale `:19`-`:27`; `crates/ematix-flow-core/src/preset.rs` — `with_query_planner` install `:217`, rule chain `:125`-`:223`.
- Walker / multi-ref discipline: `crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs` — `find_probe_scan_for_column` descent `:477`, `Arc::ptr_eq` + silent-wrong-sums scar `:496`/`:506`.
- Bench/gate harness: `crates/ematix-flow-core/examples/paired_ab.rs` (`target_partitions(14)` `:223`, fresh-ctx-per-trial); `scripts/bench/strict_ab.sh`, `strict_22q.sh`. Data: `examples/tpch/data/{sf1,sf10,sf100}/` (project root).

### Sibling docs
- `docs/ADR_PUSH_VECTORIZED_ENGINE.md` — capability decision, threat model (STRIDE), alternatives (full-volcano rejected), original Phase-0 spec. **This build plan supersedes its §5 phase roadmap; it remains canonical for §4 threat model and §8 alternatives.**
- `docs/PHASE_SIGMA_S_PIPELINED_SCAN_FILTER_JOIN.md` — prior pipelined-scan design (cascading bloom); its "full morsel-driven = multi-month, out of scope" section is **superseded** by the Phase-0 measurement (single node, not a re-architecture).
- `docs/ADR_SELECTION_VECTOR_MATERIALIZATION.md` — sibling scan-side ADR; same morsel/selection envelope; in-pipeline agg = Phase 5+.

### Memory (banked)
- `project_q08_join_probe_kernel_2026_06.md` — take/materialize tax not the kernel; push-vec the one un-falsified lever.
- `project_mimalloc_production_gap.md` — dense-bitset neutral (the take was left in); decode at parity.
- `project_rev20_q07_q08_decode_bound.md` — Snappy-decompress floor; dynamic-filter neutral on uniform FK.
- `project_optimizer_codegen_sensitivity.md` — the 5-8% codegen tax; sibling-crate + pre-plan-walker mitigations.
- Microbench-doesn't-translate lineage: `project_lever4_full_build_rejected.md`, `project_hand_rolled_snappy_neg.md`, `project_sigma_nf3_beats_stock.md`, `project_sigma_qm_slice2_rejected.md`, `project_sigma_l3c_reverted.md`.
- Methodology: `feedback_bench_methodology_3_invocations.md`, `feedback_review_kernel_construction.md`, `feedback_no_tpch_hardcoding.md`, `feedback_tdd.md`.
