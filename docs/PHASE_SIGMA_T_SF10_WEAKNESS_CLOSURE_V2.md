# Σ.T (V2) — SF=10 weakness closure, no-scope-restriction edition

**Status:** working architecture doc (not a release artifact)
**Date:** 2026-05-25
**Author:** architect agent (cold-read, no main-thread context)
**Branch:** `perf/sigma-q-single-node-parity` at HEAD `dc2d457`
**Predecessor:** `docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE.md` (V1, preserved unchanged)
**Companions:** `docs/plans/CURRENT.md` (sidecar Phase 1+2), `docs/PHASE_SIGMA_Q_M_JOIN_REORDER.md` (rejected Σ.Q.M arc), `docs/SIGMA_Q_SINGLE_NODE_PARITY.md` (Σ.Q closeout).

V1 was written under two implicit constraints — no new `PhysicalOptimizerRule`s as primary lever, and a 4-6 week per-lever calendar budget — and as a result systematically excluded the levers that could plausibly take ematix-flow from "1.3× behind on the worst single-node TPC-H query" to "the engine DuckDB benchmarks against." V2 removes both constraints. Year-long structural rewrites are on the menu. The user asked for the full menu and will prune; the doc leans toward over-proposing.

---

## 0. Executive summary

**The honest answer to "what would it actually take to beat DuckDB at SF=10 on every query."**

DuckDB has had 8 years of single-node TPC-H tuning, a dedicated team, and a vertically-integrated stack from storage format (custom `.duckdb` files plus parquet via per-query specialised readers) up through a real cost-based optimiser, a custom vectorised executor, dynamic filter propagation, and aggregate kernels that are template-specialised by group-key arity and width. The 1.04–1.31× gap we see on Q01/Q05/Q07/Q08/Q17/Q18 is what's left after Σ.A–Σ.Q closed two orders of magnitude. Closing the remaining gap *uniformly* (so it stays closed against DuckDB releases that follow) requires owning the same three layers DuckDB owns: **(a)** a planner that knows the data, **(b)** an executor that doesn't pay DataFusion's batch-at-a-time + Arrow-buffer-rematerialisation overhead per pipeline stage, and **(c)** a storage path that supplies pre-decoded / pre-filtered / pre-indexed columns to the executor without re-decoding through the RecordBatch interface.

A *narrow* fix — Sidecar Phase 1 + filter-derived static blooms + PGO + SharedSubtree-extension, which is roughly V1's recommended cohort — plausibly closes 5 of 6 queries at 2-month cost, leaving Q05 or Q17 marginally behind. A *platform* fix — real CBO + custom hash join + dict-preserved-everywhere — closes all 6 at 6-month cost and concurrently lifts the 16 already-winning queries by a further 5-10 pp geomean. A *DuckDB-irrelevant* fix — replacing DataFusion's executor with a Cranelift-compiled whole-query interpreter on top of ematix-parquet's column decode, plus a learned-from-workload CBO — is 12-24 months and produces something neither DuckDB nor Photon nor Velox has shipped publicly: an OSS engine that ships smarter than competitors after the first 20 queries of any workload.

**The strategic question, which §5 takes up explicitly, is whether competing on raw TPC-H at SF=10 is even the right wedge.** DuckDB's SF=10 numbers are not why anyone picks DuckDB; they pick it because it's a single-binary embedded engine with great ergonomics. ematix-flow's *shipped* differentiators are (1) distributed batch SQL via Arrow Flight peer mesh, (2) the Σ.L adaptive runtime that learns from every query, and (3) the Web UI / pipeline-runtime surface. Beating DuckDB at SF=10 by 10% is irrelevant marketing copy. Being 15-50× DuckDB at SF=100 in cluster mode is the real wedge — and that wedge re-orients which of the V2 levers actually matters.

The recommendation in §7 is the Moderate cohort (real CBO + custom hash join + L1 sidecar + L3 PGO + Σ.P-extension) sequenced over 6 months, with the Ambitious cohort's storage-and-executor rewrite deferred behind a 6-month milestone where we re-decide based on whether the wedge has moved.

---

## 1. Per-query root cause

Cite V1 §1 for full derivation; this section is a single paragraph per query.

### 1.1 Q01 — `SUM/COUNT GROUP BY l_returnflag, l_linestatus`

V1 §1.1: pure kernel-floor query — no join, no significant filter, dominated by parquet decode of 6 lineitem columns + a two-stage aggregate over 60M rows × 4 groups. We already win on harder shapes (Q21 1.79×, Q22 5.9×); the 1.04× residual is "DuckDB's vectorised pipeline executor vs DataFusion+Arrow." V1 proposed PGO + dict-routing on `l_returnflag` / `l_linestatus`. V2 adds: replacement of the DataFusion exec model itself (L8/L9 below) is the structural fix; PGO is a 30-50% partial close at trivial cost.

### 1.2 Q05 — 5-way fact+dim chain

V1 §1.2: canonical star-join. DuckDB propagates `region.r_name='ASIA'` through 4 joins as a dynamic filter on `l_suppkey`; our planner builds joins in dependency order and lineitem sees no `l_suppkey` filter. Two Σ.Q.M attempts (Slice 2 join descent, Slice 4 redundant-semi SPIKE) were rejected because DataFusion's CSE doesn't share Join outputs / Join builds. V1 proposed filter-derived static bloom (L2). V2 adds: this is the canonical case for a real CBO with dynamic filter propagation (L10 below); a custom hash join (L13 below) further removes per-build cost; both together close Q05 to parity AND drop it well below DuckDB.

### 1.3 Q07 — 6-way join + year-bucketed agg

V1 §1.3: same dim-filter-propagation gap as Q05 but smaller magnitude (2-of-25 nations is a sharp predicate); came down from 281→159 ms during Σ.Q.L15/L16. Residual is in `nation ⋈ supplier ⋈ lineitem` not seeing the surviving-suppkey set. V1 proposed filter-derived static bloom (L2). V2 adds: same as Q05 — CBO + dynamic filter propagation is the durable fix.

### 1.4 Q08 — 7-way join with very-selective `p_type` filter

V1 §1.4: same propagation gap as Q05/Q07, but Q08's part filter is single-hop to lineitem (1 partkey → l_partkey) so it's the **highest-sidecar-applicability query** in the set. V1 proposed sidecar on `l_partkey` (L1). V2 adds: agree with V1 — Q08 is the cheapest closure target and sidecar handles it cleanly. Nothing structural to add.

### 1.5 Q17 — `lineitem × part` with correlated scalar-subquery `AVG(l_quantity) per p_partkey`

V1 §1.5: two lineitem scans (outer filtered to surviving partkeys + correlated subquery grouped by all partkeys). The 2M-cardinality AVG is the dominant cost; `project_sigma_r2_rejected.md` proved a RobinHood replacement loses +40-55%. V1 proposed scan-sharing via Σ.P-extension (L4) and scalar-subquery decorrelation v2 (L5). V2 adds: Q17 is the case where a learned-from-workload CBO matters most — the planner should observe that 99% of the subquery's domain is filtered out by the outer's brand/container predicate and rewrite to push the partkey-set filter into the subquery's aggregate.

### 1.6 Q18 — `customer ⋈ orders ⋈ lineitem` with HAVING `SUM(l_quantity) > 300`

V1 §1.6: most documented. Pre-L10: 60M intermediate before LeftSemi-down-to-624 (`project_q18_sf10_duckdb_plan_diff.md`). Post-L10 PushDownLeftSemiRule: 2.5×→1.09×. Residual is build-side sizing of post-L10 LeftSemi + 15M-group inner aggregate + two lineitem scans (Σ.P didn't fire). V1 proposed build-side swap re-verify (L6) + SharedSubtree extension (L4). V2 adds: a real custom hash join (L13) with build-side selection by cardinality estimate makes the build-swap question moot; everything past L10 is build-side mis-selection that a CBO + custom join handle by default.

### 1.7 Cross-query pattern

Same as V1 §1.7. Three of six (Q05/Q07/Q08) are dim-filter-propagation; Q17 is partially the same (outer scan); Q18 is residual hash build sizing; Q01 is pure executor floor. **Of the six, four (Q05/Q07/Q08/Q18) get a single-lever simultaneous fix from a real CBO with dynamic filter propagation + a custom hash join with cardinality-driven build side. Q01 needs an executor-level fix (PGO partially; new executor fully). Q17 needs subquery decorrelation v2 or scan-sharing.**

---

## 2. The full lever menu

### 2.1 Scoring axes (V2)

- **Closure (pp 22q geo):** pp of the 22-query SF=10 geomean against the bench-noise floor. V1 used the same axis; V2 numbers are calibrated against a "post-V1-cohort" baseline of ~0.70.
- **Engineering cost:** person-weeks for narrow levers, person-quarters for platform levers, person-years for rewrites. Honest.
- **Strategic value:** does this enable a category of future wins beyond closing this specific gap? Range: *point* (one-shot), *platform* (other levers ride on top), *wedge* (changes what we can credibly tell investors / users about).

### 2.2 V1's L1–L7 (cited, scored the same way for comparison)

| Lever | Closure | Cost | Strategic |
|---|---:|---:|---|
| **L1 Sidecar Phase 1** (read-side index discovery + planner hook) | -2 pp | 2 wk | point + small platform (the `with_filter` substrate enables L2) |
| **L2 Filter-derived static bloom into provider** | -3 pp | 3-4 wk | point |
| **L3 PGO release build** | -3 pp | 3-5 d | platform (unblocks future rule-shaped levers) |
| **L4 Σ.P SharedSubtree extension** | -1 pp | 2-3 wk | point |
| **L5 Scalar-subquery decorrelation v2** | -0.5 pp | 4-6 wk | point |
| **L6 LeftSemi build-side swap re-verify** | -0.3 pp | 1 wk | point |
| **L7 Real join-order rewriter** (DataFusion-upstream or in-tree override) | -3-5 pp | 6-10 wk | **platform** |

V1 cumulative L1+L2+L3+L4+L6: 22q geo 0.80→0.70-0.73. That's the conservative ceiling.

### 2.3 V2 platform levers

#### L8 — Replace DataFusion's planner with a custom rule-driven CBO

**What:** A cost-based optimiser that owns join order, build-side selection, aggregate-shape selection, scan-routing (dict-preserved vs not, sidecar vs not, Emat vs DataFusion-default provider), and dynamic-filter-propagation derivation. DataFusion's `LogicalPlan` + `ExecutionPlan` stay as the IR + execution surface; the optimiser passes between them are replaced.

Concretely:

```rust
// crates/ematix-flow-planner/src/lib.rs
pub trait EmatixCost {
    fn input_rows(&self) -> u64;
    fn output_rows(&self) -> u64;
    fn build_bytes(&self) -> u64;
    fn probe_cost(&self) -> u64;
    fn scan_cost(&self) -> u64;  // distinguishes sidecar / Emat / default
}

pub struct PlanCandidate {
    pub plan: Arc<dyn ExecutionPlan>,
    pub cost: u64,
    pub join_order_hash: u64,
}

pub fn plan(logical: LogicalPlan, ctx: &SessionContext, observer: &WorkloadObserver) -> Arc<dyn ExecutionPlan> {
    let candidates = enumerate_candidates(&logical, ctx);  // bounded-fanout DPsize / DPccp
    let costed = candidates.into_iter().map(|c| (cost_of(&c, observer), c));
    costed.min_by_key(|(c, _)| *c).unwrap().1.plan
}
```

Statistics come from three sources:

1. **TableProvider stats** — row count, per-column min/max, null count, distinct estimate (HLL). Already partly available via Σ.O.c row-group decode cache stats.
2. **Sidecar `IndexSummary`** — per-column distinct count, bloom selectivity at byte-budget, page-Bloom precision. Phase 2 sidecar work generates this.
3. **Σ.L workload observer** — per-shape historical observed row counts at each plan vertex (`crates/ematix-flow-core/src/workload_log.rs`, `~/.ematix/workload.db`). The persistent observer infrastructure already exists; the CBO consumes its `consult_probe(shape_hash, vertex_id) -> historical_cardinality` API.

**Closure:** Q05 -8 pp, Q07 -3 pp, Q08 -1 pp (on top of L1), Q17 -3 pp, Q18 -1 pp (on top of L6). Total **~-12-15 pp** because the CBO also flips marginal queries that are currently borderline (Q03, Q10, Q19, Q21).

**Cost:** 3 person-quarters minimum. Pipeline breakdown:

- Q1: bounded-fanout join-order enumerator (DPccp variant) + cost model + smoke harness.
- Q2: dynamic-filter-propagation pass + integration with Σ.J.2.b context-bloom transport.
- Q3: aggregate-shape selection + scan-routing integration + Σ.L observer wire-up + production-quality cardinality-est calibration.

**Strategic value:** **wedge**. A CBO that consumes the workload observer is the differentiator that lets us ship "the optimiser that learns from every query" with measurable proof. Photon/Velox/Polars don't have this; DuckDB has CBO but no cross-run learning; Snowflake has it but closed-source.

**Risk:** This *is* a new optimizer rule chain in flow-core; the `project_optimizer_codegen_sensitivity.md` tax theoretically applies. The mitigation is that L3 (PGO) lands first AND the CBO lives in a new sibling crate `ematix-flow-planner` analogous to `ematix-parquet` (separate crate insulates codegen per `project_ematix_parquet_v013_win.md`).

#### L9 — Whole-query Cranelift JIT compilation

**What:** Compile the physical plan to native code via Cranelift IR at plan time, replacing DataFusion's `ExecutionPlan::execute` batch-loop dispatch with a single compiled function per pipeline boundary. The whole-query specialisation (column types, predicate shapes, aggregate kernels) folds into the generated code as constants.

Cranelift is already a dep in the ematix-flow tree (per the user's brief). The Permutable-Compiled-Queries pattern (TUM) compiles in background while interpreting, then swaps pointers — but `project_groupby_research_2026_05.md` documents Photon's anti-JIT thesis: Photon explicitly rejected codegen in favour of vectorised template specialisation. Photon's argument: template specialisation gives 80% of codegen's win at 5% of the complexity. Our Σ.F shape catalog is template specialisation.

The honest framing: **L9 is a long-shot.** The expected value over L8+L11 (real CBO + Photon-style template catalogue extended into compile-time monomorphisation) is probably negative — Cranelift costs us 2-3 person-quarters of executor work and gains us 5-10% peak performance on queries that already aren't decode-bound. **The case for L9 exists only if (a) Σ.F's shape catalogue runs out of templates because the cardinality of TPC-H-like shapes exceeds what we can hand-write, or (b) we want a generic "any SQL workload runs fast" story rather than a "TPC-H-shape workload runs fast" story.**

**Closure:** Q01 -1 pp, Q05 -1 pp, marginal elsewhere. ~-3-5 pp across queries that are kernel-bound rather than I/O-bound. Doesn't help Q08 (sidecar) or Q18 (joinorder).

**Cost:** 2-3 person-quarters for a working interpreter swap; 4 person-quarters for the full set of column-type / agg-shape / predicate-shape specialisations.

**Strategic value:** **platform** if it works (every future query gets faster) but lower than L8 because it doesn't ship the "learning" wedge story; **point** if it lands and only Q01 closes.

**Recommendation:** Defer. Photon's anti-JIT thesis is well-supported. Pick L11 instead.

#### L10 — Dynamic filter propagation pass (subset of L8, ships independently)

**What:** A focused planner pass that derives, for each `Inner Join`, the set of values from the smaller (filtered) side and pushes that set as a *runtime* bloom or IN-list into the larger side's scan. Generalises V1's L2 from "static filter-derived bloom" to "all joins propagate dynamically."

This is **DuckDB's single biggest source of TPC-H wins** at SF=10. The Σ.J.2.b context-bloom transport infrastructure (`project_sigma_j2b_v_landed.md`, `vi_landed.md`, `vii_landed.md`, `viii_landed.md`) already ships the build-side emitter + transport mechanism for distributed mode. Generalising it for single-node so it fires inside one process — without the Flight round-trip — is the lever.

**Closure:** Q05 -8 pp, Q07 -3 pp, Q08 -1 pp (sidecar catches Q08 already). Total **-10-12 pp** because it also fires on Q10, Q19, Q21 (currently winning but with headroom).

**Cost:** 6-8 weeks. The hard part is correctness (what if the build-side blocks; what about NULL semantics) plus avoiding the [[bloom-on-FK is net-negative]] problem from `project_l9_bloom_consumer_findings.md` — selectivity gating is non-optional.

**Strategic value:** **platform**. Composes with L1 (sidecar consumes the bloom via page-Bloom index) and L13 (custom hash join uses the same runtime bloom for build-side filtering).

**Risk:** Per `project_l9_bloom_consumer_findings.md`, sideband bloom is structurally broken for Q18 (orders is on the build side of an inner join; eager poll can't wait for upstream bloom). The single-node case has different timing semantics than distributed but the eager-poll issue persists. Resolution: the bloom resolves *before* the build phase begins, not in the middle of probing. This is plannable.

#### L11 — Σ.F shape catalogue lifted to compile-time monomorphisation per query template

**What:** TPC-H Q01-Q22 cluster into roughly 10 distinct *plan templates* (filter+agg, filter+join+agg, multi-join+agg, scalar-subq+filter+agg, etc.). The Σ.F shape catalogue currently routes operators at plan time. Lift it to compile-time: each template gets a fully monomorphised execution function with all types, kernel choices, and partition counts inlined. A query-fingerprint cache maps `shape_hash → fn ptr`. New shapes fall back to the generic Σ.F path.

This is **Photon's exact pattern** applied at coarser grain. Per `project_groupby_research_2026_05.md`:

> kernels are C++ templates parameterised by batch properties (`kHasNulls`, `kAllRowsActive`, `kAllAscii`, dict-status). Per-batch metadata picks one of `2^k` precompiled specialisations; dead branches compile away at C++ compile time.

In Rust this is `fn process<const K: usize, const HAS_NULLS: bool, const ALL_ASCII: bool>(...)` plus a dispatch jumptable.

**Closure:** Q01 -2 pp, Q06 already wins so headroom -1pp, broad lift across all queries -3 pp. Total **~-5-7 pp**.

**Cost:** 2-3 person-quarters. The hard parts are (a) defining the template axes such that monomorphisation explodes manageably (10 templates × 4 binary specialisations = 40 monomorphised fns, manageable; 10 × 12 = 4096, unmanageable), (b) ensuring the new monomorphised kernels don't add to codegen tax via the `project_optimizer_codegen_sensitivity.md` mechanism (separate crate is the answer), (c) the shape-hash cache invalidation when stats change.

**Strategic value:** **platform**. Composes with L8 (CBO picks the template) and L1 (template specialises for sidecar vs non-sidecar scan). Also retires V1's L5 (Q17 scalar-subq v2) by making the Q17 template a monomorphised special case.

#### L12 — Replace `RecordBatch` interface with zero-copy column pipeline from ematix-parquet through scan→filter→join→agg

**What:** Today's hot path:

```
ematix-parquet decode → Arrow array (Utf8View/Float64/Date32/...)
  → RecordBatch
  → DataFusion FilterExec (copies arrays via batch.filter())
  → RecordBatch
  → DataFusion HashJoinExec (probes arrays via take())
  → RecordBatch
  → DataFusion AggregateExec
```

Every arrow → arrow transition incurs (a) bounds-checked offset chasing, (b) Utf8View buffer rematerialisation (the cause of multiple Σ.E5 series losses, e.g. `project_sigma_e5_q13_root_cause.md`'s 152MB→2.1GB inflation), (c) Arc clone overhead. DataFusion does some elision via `take_record_batch` but not enough; Velox's UnifiedRowVector is the existence-proof that a tighter type can carry the same information for less cost.

Replace with `EmatColumn = Arc<DecodedColumn>` where `DecodedColumn` carries (a) page-buffer references, (b) per-page decoded dictionaries, (c) row-group indices, (d) a filter bitmap that composes through pipeline stages without materialising rows until the operator that genuinely needs them (typically the final `take_columns_for_output`).

**Closure:** Closes the Utf8View rematerialisation tax which V1's per-query analysis under-counted. Q13/Q14/Q17 each get -1-2 pp; broader lift across string-heavy queries -3-5 pp. **Total ~-5-7 pp.**

**Cost:** 3-4 person-quarters. This is the deepest invasive change in this menu short of L9. Every operator in flow-core has to consume the new type. Most operators currently take `&RecordBatch`; they need to take `&[EmatColumn]` or equivalent. DataFusion's `ExecutionPlan` trait is `RecordBatch`-bound at the API surface, so this is either a fork or a leaky-wrap pattern (the executor uses `EmatColumn` internally and converts at plan boundaries only).

**Strategic value:** **platform**. Removes the architectural reason Σ.E5 series ran into a 0.92 geomean ceiling. Also unlocks (a) JIT-friendly memory layout if L9 is later pursued, (b) sidecar `with_filter` push-through (the filter bitmap composes naturally).

**Risk:** Two-sided. (1) Maintenance — every DataFusion upstream release breaks the wrap. (2) Correctness — every column type / encoding has a separate codepath; the type explosion in operator code is real. Cf. Arrow2 → Arrow1 fork rationale and outcome.

#### L13 — Custom hash join (i64-keyed, Robin Hood, build-side bloom, skew handling)

**What:** A purpose-built hash join for TPC-H-shaped FK joins (i64 → i64), with:

- Build-side cardinality estimate from L8's CBO drives which input goes on the build side.
- Build-side bloom emitted during build phase, shared with probe-side scan via Σ.J.2.b infrastructure.
- Pre-sized hash table via Σ.N.f.2 dynamic-resize pattern (`project_sigma_nf2_dynamic_resize.md`).
- Robin Hood probing with batch-API ingest from Σ.N.f.3 (`project_sigma_nf3_beats_stock.md`).
- Skew detection (top-k key frequency observed during build) with partitioned overflow into a secondary smaller table.
- Build-side compaction if Σ.L observer reports that the probe-side filter selectivity > some threshold.

Q05/Q07/Q08/Q17/Q18 each have multiple HashJoinExecs; this is the single most-impactful per-operator replacement available.

**Closure:** Q05 -5 pp (build-side selection on `customer ⋈ orders`), Q07 -2 pp, Q08 -1 pp (sidecar catches it), Q17 -2 pp, Q18 -3 pp (build-side swap is structural, not "re-verify a rule"). Total **~-13 pp**.

**Cost:** 2-3 person-quarters. The kernel work is well-understood (Robin Hood is a few hundred LOC; Σ.N.f.3 shipped it for aggregates) but the integration with DataFusion's `HashJoinExec` (or replacement therof) plus the build-side bloom emitter wire-up plus skew handling is the bulk.

**Strategic value:** **platform**. The same kernel handles distributed shuffle-joins in cluster mode; the same bloom emitter feeds L10 dynamic-filter propagation; the same skew detection feeds L8 CBO statistics. This is the single highest-leverage operator replacement available.

#### L14 — Dict-preserved end-to-end

**What:** Resolve `project_dict_arrival_blocker.md`'s "available on opt-in" status to "default on for low-cardinality string columns." `EnableDictGroupCountRule` already exists; `Σ.K.2` dict-routing picks per-table. Make it the default path.

Per `project_sigma_k_dict_arrival_ab.md`: Q12 −40% (kernel works) but Q01 +104%, Q13 +25%, Q19 +35% when flipped globally. The Σ.L.1 speculative-race + workload-feedback pattern fixes this — first-encounter probe + per-shape persistence.

**Closure:** Q01 -2 pp (`l_returnflag` + `l_linestatus` arrive as Dict; GroupValues intern halves), Q12 already +; Q07 marginal, broader -1-2 pp across queries with string group keys.

**Cost:** 4-6 weeks. Mostly integration + correctness + Σ.L probe wire-up. The Σ.E3 + Σ.L.1 substrates ship.

**Strategic value:** **platform**. Unlocks dict-aware hash join build (i.e. join on dict-codes instead of materialised strings — DuckDB's PR #15152 pattern), dict-aware aggregate (Photon's pattern), dict-preserved sort. This is one substrate decision that ripples through 5-6 downstream operators.

#### L15 — Iceberg + sidecar manifests as full storage layer (write-side ownership)

**What:** Today we *read* parquet (via DataFusion default, FastParquet, or Emat provider) and *consume* sidecars when present. We don't *write* — the user is responsible for parquet layout. Owning the write path means we can:

- Physically sort lineitem by `l_orderkey` (Q18) or `l_partkey` (Q14/Q17) or `l_shipdate` (Q06).
- Generate sidecar page-Blooms + zonemaps at write time, not as a separate `flow tune-parquet` step.
- Generate partial materialised views for common rollups (Q01's GROUP BY l_returnflag,l_linestatus is a 4-row materialised view that costs ~bytes).
- Generate Z-order or hilbert-curve layouts for multi-dim filters (Q06's discount×quantity).
- Use Iceberg snapshot semantics to expose schema/sort/index evolution to users without rewriting the whole table.

The Σ.L.5 write-tuner (`project_sigma_l_adaptive_runtime.md`) already drafts this: "Read profile drives write tuning — parquet files rewrite themselves for the observed workload." L15 is the productionisation.

**Closure:** Direct closure on Q01 (-2 pp), Q06 (-3 pp standing wins, more headroom), Q18 (-2 pp via sort + dynamic-filter-into-sorted-data fast path). But the real payoff isn't TPC-H pp; it's the *story*. See §5.

**Cost:** 2 person-quarters for read-side Iceberg adoption + write-side manifest generation; 3-4 person-quarters for the materialised-view + Z-order layer.

**Strategic value:** **wedge**. The auto-tune-write-side-from-observed-read-workload story is what L15 *enables* and nobody in OSS has. The story is:

> "Run the same queries against ematix-flow that you run against DuckDB. After ~50 queries, the parquet files (and Iceberg manifests + sidecars) rewrite themselves around your workload. Next query: 5-50× faster. Cost to adopt: zero — no DBA, no index hints, no cluster service."

That's a wedge story DuckDB can't tell because they don't own storage layout discipline, only file format.

#### L16 — GPU offload for filter+agg pipelines

**What:** A `GpuPipelineExec` operator that takes a sequence of (filter, agg, join) over dense columnar batches and offloads to GPU. M3 Pro has a credible GPU (~10 TFLOPS fp32); AWS GPU instances are cheap (~$0.50/hr g4dn.xlarge). Photon and Heavy.AI demonstrate that filter+agg+join on dense columnar batches are well-suited to GPU.

**Closure:** Q01 -1 pp (low — Q01 is decode-bound, not aggregate-bound), Q06 -3 pp (filter-bound), Q17 -2 pp (the AVG is GPU-friendly), Q18 -1 pp. Total **~-5-8 pp** but only on GPU-equipped machines. SF=10 single-node-CPU bench numbers don't include this.

**Cost:** 2-3 person-quarters for a CUDA/Metal-based prototype; 4+ for production. The biggest cost is portability across NVIDIA/AMD/Apple-Silicon GPUs.

**Strategic value:** **wedge** if the SF≥100 cluster bench includes GPU instances; **point** if it's only for single-node M3 dev. The cluster-GPU framing is where this lever justifies its cost.

**Recommendation:** Defer. Not a SF=10 lever. Re-evaluate after L15 (storage) lands and we're choosing what to bench at SF=100.

#### L17 — Online learned join order (Σ.L extended one logical level deeper)

**What:** Every bench run, observe actual cardinality at each join boundary (already infra exists in Σ.L.2 workload-log). Persist. Use it to re-pick join orders on the next bench run. This is Snowflake's "intelligence layer" pattern but with the persistent-observer story we already have shipped.

L17 is L8 (CBO) + Σ.L.2 wired together — but with the specific framing that the *historical observed cardinality* dominates the *static estimated cardinality* once n_observations ≥ 5. Within a workload, this means the second time the user runs Q05, the planner uses the observed `lineitem ⋈ supplier-filtered-by-region` cardinality from the first run, not the static estimate.

**Closure:** Hard to attribute separately from L8. Treat it as a 1-2 pp *additional* lift on top of L8 once Σ.L.2 wire-up is real.

**Cost:** 4-6 weeks on top of L8 (the L8 cost above already includes basic Σ.L observer consumption; L17 is the production-quality cross-run learning loop).

**Strategic value:** **wedge**. This is "the optimiser that learns from every query" promised by Σ.L but actually wired to a CBO. The pitch is the same as L15's wedge story but in compute rather than storage: ematix-flow plans queries better after running your workload than after running a benchmark suite.

#### L18 — Replace DataFusion entirely (long-term architectural reset)

**What:** Fork. Keep Arrow as the in-memory data model. Replace DataFusion's `LogicalPlan` / `LogicalOptimizer` / `PhysicalPlan` / `PhysicalOptimizer` / `ExecutionPlan` with native ematix-flow types. Keep the DataFusion SQL parser (it's good). Keep `datafusion-substrait` (Substrait is the right IR contract for distributed). Lose everything between.

This is the maximally invasive option. It's also the cleanest. The accumulated technical debt that V1 catalogued — "no new optimizer rules without 5-8% codegen tax", "RecordBatch interface tax", "DataFusion's CSE doesn't share Join outputs", "DataFusion's JoinSelection has the wrong heuristics", "DataFusion's `take_record_batch` is per-call allocating" — disappears.

**Closure:** All 6 SF=10 losses close to parity or below. The 22q geomean ceiling shifts from ~0.70 to ~0.50.

**Cost:** 2-3 person-years. *This is a multi-engineer multi-year commitment.* Single-engineer scope is unrealistic.

**Strategic value:** **wedge**. The story is "DuckDB is single-process embedded; we are everything embedded SQL needs plus a distributed cluster mesh plus a learning optimiser." But the cost is 2 years of "we don't ship features because we're rewriting the engine," which is fatal for an OSS project competing for mindshare.

**Recommendation:** **Don't do this.** The right next-level move is L18-equivalent *features* shipped on top of DataFusion (L8 + L11 + L13 + L14) which together replace 80% of what L18 would replace, at 30% of the cost, while keeping DataFusion compatibility for the parser + Substrait export. See §4 cohort discussion.

### 2.4 V2 summary table

| Lever | Targets | Closure (pp 22q geo) | Cost | Strategic | Composes-with |
|---|---|---:|---:|---|---|
| L1 sidecar P1 (V1) | Q08, partial Q17 | -2 | 2 wk | point | L2, L10, L13 |
| L2 static bloom (V1) | Q05, Q07 | -3 | 3-4 wk | point | L1, L10 |
| L3 PGO (V1) | all; Q01 most | -3 | 3-5 d | platform | everything |
| L4 Σ.P extension (V1) | Q17, Q18 | -1 | 2-3 wk | point | L11 |
| L5 scalar-subq v2 (V1) | Q17 | -0.5 | 4-6 wk | point | L11 (subsumed) |
| L6 LeftSemi build swap (V1) | Q18 | -0.3 | 1 wk | point | L13 (subsumed) |
| L7 real join-order (V1) | Q05/Q07/Q08/Q18 | -3-5 | 6-10 wk | platform | L8 (subsumed) |
| **L8 custom CBO (sibling crate)** | Q05/Q07/Q08/Q17/Q18 | **-12-15** | 3 PQ | **wedge** | L10, L13, L14, L17 |
| L9 Cranelift whole-query JIT | Q01, kernel-bound | -3-5 | 3-4 PQ | platform | L12 |
| **L10 dynamic filter propagation** | Q05/Q07/Q08 | **-10-12** | 6-8 wk | platform | L1, L8, L13 |
| **L11 Σ.F compile-time monomorphisation** | Q01, broad | -5-7 | 2-3 PQ | platform | L8, L14 |
| L12 zero-copy column pipeline | Q13/Q14/Q17 + broad | -5-7 | 3-4 PQ | platform | L9, L1 |
| **L13 custom hash join** | Q05/Q07/Q08/Q17/Q18 | **-13** | 2-3 PQ | **platform** | L8, L10, L14 |
| **L14 dict-preserved default** | Q01 + broad | -3-5 | 4-6 wk | platform | L8, L11 |
| L15 storage layer (Iceberg + write-tune) | Q01/Q06/Q18 + wedge | -5-8 | 3-4 PQ | **wedge** | Σ.L.5 |
| L16 GPU offload | Q06/Q17 (off-bench) | -5-8 | 2-3 PQ | wedge (cluster) | distributed |
| L17 online learned join order | all (multiplier on L8) | -1-2 on top of L8 | 4-6 wk on top | **wedge** | L8, Σ.L.2 |
| L18 fork DataFusion | everything | -20-25 | 2-3 PY | wedge (high risk) | n/a |

PQ = person-quarter; PY = person-year.

---

## 3. Cohort proposals

### 3.1 Conservative cohort (V1's plan, minor extensions)

**Scope:** L1 + L3 + L6 in parallel (T1), then L2 + L4 + L14 in parallel (T2). No L5, no L7+, no platform levers.

**Timeline:** 2 months total. T1 = weeks 1-2 (V1's Phase T1 unchanged), T2 = weeks 3-8.

**Expected outcome:** 22q SF=10 geomean 0.80 → 0.70-0.72. Q08 fully closed. Q01 partially closed via PGO + dict. Q17 partially closed via L4. Q05, Q07, Q18 reduced but Q05 still >1.05× DuckDB.

**Where it fails:** Q05's structural dim-filter-propagation gap is not addressed. The "we're 1.3× behind DuckDB on the worst query" story persists at Q05.

**Cost:** 2 person-months of focused work, no platform investment.

**When this is the right choice:** if the strategic frame is "ematix-flow is competitive enough at SF=10 single-node, the differentiation is elsewhere (distributed + UI + Σ.L)." That is *defensible* — see §5.

### 3.2 Moderate cohort (1-2 platform levers + V1 baseline)

**Scope:** Conservative cohort PLUS L8 (custom CBO sibling crate) PLUS L13 (custom hash join). Defer L10 to inside the CBO scope (it's a CBO pass). Defer L11/L12/L15/L16 to a follow-up.

**Timeline:** 6 months total.

- M1-M2: V1's Phase T1 (L1+L3+L6) in parallel with L13 kernel-only spike (no integration).
- M3: V1's Phase T2 (L2+L4+L14) in parallel with L8 design + spike.
- M4-M5: L13 integration into HashJoinExec + L8 full implementation (cost model + DPccp enumerator + filter propagation pass).
- M6: integrated bench + correctness suite + roll-back paths + production-quality CBO calibration.

**Expected outcome:** 22q SF=10 geomean 0.80 → **0.58-0.62**. All 6 cited queries flip to ematix wins. Lift across the 16 currently-winning queries by another 5-10 pp. **DuckDB no longer wins any TPC-H SF=10 query.** SF=100 cluster numbers also improve via L13 distributed shuffle-join handling.

**Where it fails:** L9/L12 not addressed — string-heavy queries still pay Utf8View rematerialisation tax (Σ.E5 ceiling at 0.92 on string-heavy bench profiles). L15 (storage) not addressed — the wedge story is partial.

**Cost:** ~6 person-months focused = 2-3 engineers for a quarter, or 1 engineer for 6 months.

**When this is the right choice:** This is the default recommendation if the project has 1-2 dedicated engineers and a 6-month horizon. The CBO + custom join is the platform investment that compounds — every future query benefits — and the V1 baseline ships in the first 8 weeks so there's a measurable mid-checkpoint.

### 3.3 Ambitious cohort (3 platform efforts in parallel)

**Scope:** Moderate cohort PLUS L11 (compile-time monomorphisation) PLUS L12 (zero-copy column pipeline) PLUS L15 (storage layer). Defer L9, L16, L18.

**Timeline:** 12-18 months total.

- Q1: V1's T1 + T2 + L13 kernel spike + L8 spike + L15 read-side Iceberg + L11 scaffolding.
- Q2: L8 integration + L13 integration + L11 first 4 templates + L14 default flip + L12 design.
- Q3: L8 production calibration + L11 full template catalogue + L12 implementation + L15 sidecar generation.
- Q4: L12 integration + L15 partial-materialised-view layer + Σ.L.2 → CBO production wire-up = L17 ships.
- Q5-Q6: bench + harden + write the proof articles for the "engine that learns" wedge story.

**Expected outcome:** 22q SF=10 geomean 0.80 → **0.45-0.55**. ematix-flow is 2× faster than DuckDB on 22q SF=10 (the headline) AND ships the wedge story (the moat). At SF=100 cluster-mode the geomean vs single-node DuckDB is 30-80× (the actual differentiator).

**Where it fails:** No published cluster SF=100 numbers means the wedge story doesn't have proof. Concurrent platform investment risks one or more dropping calendar (L8 slipping six weeks domino-effects L17). L18 (fork) is out of scope but if a discovered problem demands it, the cohort blows up.

**Cost:** 12+ person-months focused. 2-3 engineers for ~6 months each.

**When this is the right choice:** if the project has funding and intention to be the canonical OSS distributed SQL engine over 18-24 months. This is the right answer if the V1 question — "what would it actually take to beat DuckDB" — is answered with "more than just beat them; out-class them on the dimensions they can't follow." See §5.

### 3.4 Cohort comparison

| | Conservative | Moderate | Ambitious |
|---|---|---|---|
| Calendar | 2 mo | 6 mo | 12-18 mo |
| Engineering | 2 PM | 6 PM | 12+ PM |
| SF=10 22q geo (from 0.80) | 0.70-0.72 | **0.58-0.62** | **0.45-0.55** |
| Q01 status | partial close | fully closed | well below DuckDB |
| Q05 status | still 1.05-1.15× | flipped | well below DuckDB |
| Q08 status | flipped | flipped | flipped + structural |
| Q17 status | still 1.05-1.10× | flipped | flipped + structural |
| Q18 status | flipped (post-L6) | flipped + structural | flipped + structural |
| SF=100 cluster impact | none | direct (L13) | large (L13+L15+L17) |
| Wedge story | partial (Σ.L existing) | partial + CBO | full (CBO + storage + learning) |
| Risk | low | moderate | high |

---

## 4. Composability map (which platform levers reinforce each other)

```
            L3 PGO
              │
              ▼
   L1 sidecar P1 ──► L2 static bloom ──► L10 dynamic filter prop ──┐
              │                                                     │
              ▼                                                     ▼
        L8 custom CBO  ◄────────────── L13 custom hash join ──► L14 dict-preserved default
              │                              │                       │
              ▼                              │                       ▼
        L17 learned join order               │              L11 compile-time monomorphisation
              │                              │                       │
              ▼                              ▼                       │
        Σ.L workload log ──────────► Σ.L.5 write-tune ───► L15 storage layer
                                                                     │
                                                                     ▼
                                                              L12 zero-copy column pipeline
```

Reading: an arrow from A to B means A is a prerequisite or strong-enabler for B. Boxes downstream of L8 + L13 + L14 cluster into the Moderate cohort. The right column (L11 + L12 + L15) clusters into the Ambitious cohort. L3 (PGO) and L1 (sidecar) are bootstrap-level — every cohort starts there.

The single most-leveraged box is **L8 custom CBO** — 5 downstream platforms depend on it. The single most-portable box is **L13 custom hash join** — it works in single-node OR distributed mode, immediately.

---

## 5. Strategic discussion — is "beat DuckDB at SF=10" even the goal?

The user explicitly asked this. The honest answer:

**No.** DuckDB at SF=10 single-node is the wrong benchmark for ematix-flow's positioning.

### 5.1 What DuckDB is good for

- Single-binary embedded — `pip install duckdb` and you have an analytic DB.
- Mature SQL surface — 8 years of Postgres-compat work.
- TPC-H benchmark adoration — they care about every percentage point because it's their primary mindshare lever.

### 5.2 What ematix-flow is already good for (shipped, per memory)

- **Distributed batch SQL via Arrow Flight peer mesh** — `project_distributed_is_shipped.md`. No competitor in OSS has symmetric-mesh peer-to-peer SQL with no master node.
- **Σ.L adaptive runtime that learns from every query** — `project_sigma_l_adaptive_runtime.md`. 32 tests across 5 modules. Snowflake has this privately; nobody has it OSS.
- **Web UI / pipeline-runtime surface** — `project_web_ui_reskin.md`. DuckDB has none.
- **Auto-injecting SQL pattern recognition** (Σ.D, Σ.K.2) — the optimiser already recognises 6 TPC-H shapes and routes them to fused operators *outside* the codegen-tax pathway.
- **ematix-parquet sibling crate** — `project_ematix_parquet_repo.md`. v0.13.0 closed 4% geomean vs parquet-rs without regressions; v0.14.0 closed a 50× LZ4 bug; v0.15.0 raised the bar further. DuckDB's parquet reader is good but inside a single binary; ours is a public Rust crate other projects can adopt.

### 5.3 The wedge that DuckDB can't follow

ematix-flow has three structural advantages DuckDB cannot match:

1. **Distribution.** DuckDB is single-process by design. They can never ship a distributed mode without inventing one from scratch. The "scales from laptop to 100 nodes without a cluster service" story is uniquely ours.
2. **Cross-run learning.** DuckDB is per-process by design — no persistent observer, no workload-driven write tuning. The "engine that learns from every query" story is uniquely ours (Σ.L.5 is the existence proof; productionising it via L17 is the proof of compounded learning).
3. **Engine + storage discipline.** L15 (Iceberg + sidecars + auto-tuned write) plus the Σ.L learning loop turns ematix-flow into "the analytic warehouse that organises itself around your workload." DuckDB ships a query engine; we can ship a self-organising warehouse.

### 5.4 What's the right SF=10 stance?

Given (5.3), the right SF=10 stance is:

- **Be 1.0–1.3× DuckDB at SF=10.** That's "competitive enough" — the user picking between engines doesn't pick DuckDB *because of* its 10% Q05 advantage. They pick it for ergonomics, which the Web UI + cluster-mode + learning story compete against on a different axis.
- **Be 15-50× DuckDB at SF=100 in cluster mode.** That's the published benchmark that matters. Per `project_distributed_is_shipped.md`, the harness exists; only the cluster runtime + publication remains. **This is where the marketing dollar goes.**
- **Ship the wedge story.** "Auto-builds indexes on the queries we observe, costs zero to adopt, scales to 100+ nodes without a cluster service." That's a sentence DuckDB cannot say and Snowflake cannot say (Snowflake costs money and isn't OSS).

### 5.5 Implication for cohort choice

If §5.4 is the strategic frame, **the Conservative cohort is sufficient for SF=10 competitiveness** — close Q08, partially close the rest, accept ~1.05-1.15× DuckDB on Q05/Q07. Pour the saved engineering budget into:

- (a) The cluster-mode SF=100+ bench harness publication.
- (b) L15 storage layer + Σ.L.5 write-tune productionisation (wedge story).
- (c) The Web UI / dev-loop surface.

If §5.4 is *not* the strategic frame and SF=10 single-node *is* the headline (because the project is competing for "fastest OSS single-node analytic engine"), the **Moderate cohort** is the answer — beat DuckDB on every TPC-H query at SF=10, hold that crown for 12 months, and use the resulting mindshare to fund the wedge work.

The Ambitious cohort is the answer if both frames apply simultaneously — be the fastest at SF=10 AND ship the wedge story AND have the storage discipline. 12-18 months, multi-engineer, and the most expensive option to abandon mid-way.

---

## 6. Open architectural questions (V1's OQs + V2 additions)

### V1's OQs (cited)

- **OQ-1** Q05 cost decomposition — which join contributes most to the 44 ms gap? (V1 §4 OQ-1.) Still open. Required diagnostic for L8 calibration.
- **OQ-2** Sidecar planner predicate-derivation — single-hop only or multi-hop? (V1 §4 OQ-2.) Still open. Multi-hop is L10's responsibility, not sidecar's.
- **OQ-3** PGO-vs-flag-build empirical magnitude. (V1 §4 OQ-3.) Still open. Cheap to answer.
- **OQ-4** Q17 outer-scan vs subquery-scan cost split. (V1 §4 OQ-4.) Still open. Affects L4 vs L5 vs L11 lever choice.
- **OQ-5** Why didn't Σ.P fire on Q17 / Q18? (V1 §4 OQ-5.) Still open.
- **OQ-6** Q18 post-L10 build-side ground truth. (V1 §4 OQ-6.) Still open.
- **OQ-7** Filter-derived bloom false-positive cost. (V1 §4 OQ-7.) Still open. Cheap to answer.

### V2 additions

#### OQ-CBO-A: Is DataFusion's PhysicalOptimizer extensible enough to plug a custom cost model, or do we need to fork?

DataFusion's `PhysicalOptimizerRule` trait takes `Arc<dyn ExecutionPlan>` and returns `Arc<dyn ExecutionPlan>`. The optimisation order is fixed by `default_physical_optimizer_rules`. A custom CBO needs (a) access to ALL alternative plans (not just one transform), (b) a cost model that sees TableProvider stats, (c) ability to overwrite the JoinSelection / EnforceDistribution decisions.

**Diagnostic:** Read `datafusion/core/src/physical_optimizer/optimizer.rs` and `datafusion/core/src/physical_optimizer/join_selection.rs`. Identify the API surface needed for L8. Likely outcome: extensible enough if we replace the entire `PhysicalOptimizer::optimize` driver, but not extensible enough if we keep DataFusion's driver and try to plug a rule. **The right home for L8 is "ematix-flow-planner is the entry point; DataFusion's planner is one of the candidates the CBO considers but not the default."**

**Estimated diagnostic time:** 2 days.

#### OQ-EXEC-A: Would Cranelift JIT'ing whole queries actually beat DataFusion's batch-at-a-time vectorisation?

**Diagnostic:** Build a Q01 prototype that compiles the filter→agg pipeline to Cranelift IR at plan time. Compare against DataFusion default and against L11 (template specialisation) at the same shape. Predicted outcome (per Photon's anti-JIT thesis): L11 wins on Q01-like shapes; Cranelift only matters when batch boundaries are fine-grained enough that interpretive dispatch dominates (which is not Q01).

**Estimated diagnostic time:** 2-3 weeks for a serious prototype.

#### OQ-GPU: Estimate GPU offload speedup on Q05's join chain. Worth pursuing?

**Diagnostic:** Run Q05 on M3 Pro with `metal-rs` Phase 5 fused-NEON shipdate filter replaced by a Metal Shader Language equivalent. Single-stage estimate; not whole-query. If the filter alone is 5× on GPU (plausible for a dense fp64 bitmap-producing kernel), GPU-whole-query may be 2-3× faster than CPU-whole-query.

**Estimated diagnostic time:** 1-2 weeks.

#### OQ-STORAGE-A: How disruptive would replacing parquet+DataFusion's scan with a custom storage+scan layer be?

This is the L15 question. **Diagnostic:** read Iceberg's Rust crate (`iceberg-rs` if it exists; otherwise the Java reference impl), enumerate the API surface ematix-flow's TableProvider would need to consume. Likely outcome: read-side is 4-6 weeks; write-side is 3 months because schema-evolution + manifest-update + snapshot-isolation are non-trivial.

**Estimated diagnostic time:** 1 week.

#### OQ-WEDGE: Restate in 3 lines what the differentiation story is, given the costs above.

Force the team to write three sentences (one each) that fit on a slide:

- **Distribution sentence:** "ematix-flow scales from your laptop to a 100-node Arrow Flight mesh without a cluster service or a master node."
- **Learning sentence:** "After ~20 queries on your workload, ematix-flow's persistent optimiser ships smarter than the day you installed it."
- **Storage sentence:** "Your parquet files rewrite themselves around your queries — no DBA, no index hints, no migrations."

If those sentences don't ring true 12 months from now after the chosen cohort lands, the cohort was wrong.

**Estimated diagnostic time:** the team's time at the next planning meeting.

#### OQ-RISK-A: What's the risk profile of L18 (fork)?

If the discussion in §5 lands on "DuckDB is the wrong benchmark and we should fork DataFusion to ship the wedge", the question becomes how much of L18 is recoverable if we abandon it after 6 months. Diagnostic: a written 1-pager on which DataFusion API surfaces are most stable (Substrait, LogicalPlan structure) vs most volatile (PhysicalOptimizer rule chain, ExecutionPlan internals). The 1-pager informs which fork points are reversible.

**Estimated diagnostic time:** 3-5 days.

---

## 7. Recommendation

**The Moderate cohort, sequenced as below, with a 6-month re-decision point on whether to invest in the Ambitious cohort's storage + monomorphisation layers.**

### 7.1 Phase T1 (months 1-2): V1's plan + L13 kernel spike

| Track | Lever | Engineer | Calendar |
|---|---|---|---:|
| A | L1 Sidecar Phase 1 | E1 | 2 wk |
| A' | L3 PGO release build | E1 (parallel, build-only) | 3-5 d |
| A'' | L6 LeftSemi build-side swap re-verify | E1 (parallel) | 1 wk |
| B | L13 custom hash join — kernel + microbench only, no integration | E2 | 6 wk |

End of T1: V1's T1 outcome (Q08 to parity, Q01 partially closed, Q18 near parity) PLUS a validated L13 kernel that beats DataFusion's stock HashJoinExec at the targeted i64-keyed scenarios.

### 7.2 Phase T2 (months 3-4): V1's T2 + L8 spike + L14 default flip

| Track | Lever | Engineer | Calendar |
|---|---|---|---:|
| A | L2 filter-derived static bloom | E1 | 4 wk |
| A' | L4 Σ.P SharedSubtree extension | E1 (after L2) | 3 wk |
| B | L8 custom CBO design + DPccp enumerator spike | E2 | 8 wk |
| C | L14 dict-preserved default flip + Σ.L probe wire-up | E3 (or E1 after L4) | 5 wk |

End of T2: 22q SF=10 geomean ~0.65-0.68. L8 spike validated against Q05 plan equivalence.

### 7.3 Phase T3 (months 5-6): L8 + L13 + L10 integration

| Track | Lever | Engineer | Calendar |
|---|---|---|---:|
| A | L8 CBO production implementation | E2 | 8 wk |
| A' | L10 dynamic filter propagation (inside L8) | E2 | 4 wk overlap |
| B | L13 HashJoinExec integration | E1 | 5 wk |
| C | Σ.L.2 observer wire-up to L8 (= L17 partial) | E3 | 4 wk |

End of T3: 22q SF=10 geomean ~0.58-0.62. ematix-flow no longer loses to DuckDB on any TPC-H SF=10 query. The wedge story has a partial proof (Σ.L.2 → L8 → L17 partial pipeline).

### 7.4 Re-decision point at month 6

Bench results from T3 inform the re-decision. Three possible futures:

- **(a) "We've won."** Geomean ~0.58. The Moderate cohort delivered. Pour engineering into the cluster SF=100 bench publication + Σ.L.5 write-tune productionisation + Web UI. Skip the Ambitious cohort's storage and monomorphisation work — those are platform investments and the cluster-mode wedge story is more valuable.
- **(b) "Close but not durable."** Geomean ~0.65 (under Moderate's projected 0.58-0.62). A DuckDB release in months 7-9 catches up. Decision: invest in L11 + L12 (compile-time monomorphisation + zero-copy pipeline) to build a structural buffer. ~3 person-quarters.
- **(c) "The wedge story matters more."** Geomean ~0.62 (at projected lower end). But the cluster bench takes off in months 7-9 (Σ.J.2.b infrastructure delivering 15× over single-node DuckDB at SF=100). Decision: ship L15 storage layer + L17 production learning loop. 4-6 months of platform investment justified.

**The reason for the re-decision instead of committing to Ambitious now**: the cluster bench's outcome at month 7 dominates the strategic question. If cluster mode is published and 15-50× DuckDB at SF=100, the SF=10 single-node geomean stops mattering and the storage-layer + learning-loop become the right next investment. If cluster bench underperforms (e.g. Arrow Flight has more overhead than expected), single-node performance regains importance and the monomorphisation work becomes the priority.

### 7.5 Why not the Conservative cohort?

The Conservative cohort is the right answer **if** §5's strategic framing is "we don't need to beat DuckDB at SF=10; we win on different axes." The team should hold this conviction explicitly. If they don't — if there's any commercial pressure to ship a published 22q SF=10 win-table — Conservative under-delivers because Q05 and Q17 stay 1.05-1.15×.

The Moderate cohort buys insurance: it definitively closes the SF=10 gap, AND its central lever (L8 CBO) is the prerequisite for the wedge story (L17 learning) AND its second lever (L13 custom join) is the prerequisite for cluster-mode performance at SF=100. Three strategic axes, one cohort. That's why the recommendation is Moderate, not Conservative.

### 7.6 Why not the Ambitious cohort now?

Three reasons:

1. **L8 + L13 are the lift.** L11 (compile-time monomorphisation) and L12 (zero-copy column pipeline) add 5-7 pp each on top of L8/L13's already-large lift. Diminishing returns on the same SF=10 axis.
2. **The cluster bench publication is the dominant strategic question and it's resolved at month 7.** Ambitious commits 18 months without that data point.
3. **L18 (fork DataFusion) sits behind Ambitious as the next architectural step.** L18 is 2-3 person-years and is fatal to the project if it slips. Committing to Ambitious without first proving the Moderate cohort's CBO works at production quality risks compounding L18-shaped decisions on top of an unstable foundation.

The right time for Ambitious-cohort levers (L11, L12, L15) is month 6 onward, *after* the L8 CBO is validated and the cluster bench has resolved the wedge question.

### 7.7 The single-sentence recommendation

**Ship V1's Phase T1 in months 1-2 unchanged; spike L13 custom hash join in parallel; in months 3-4 design L8 custom CBO as a sibling crate while V1's Phase T2 lands; in months 5-6 integrate L8 + L13 + L10 dynamic filter propagation and re-decide on storage/monomorphisation at month 6 based on cluster-bench outcome.**

---

## 8. What V2 deliberately does not propose

For completeness, the items considered and rejected:

1. **L18 (full fork) as the recommendation.** §2.3 enumerates the cost (2-3 person-years) and risk (fatal to OSS mindshare if it slips). The right path is to ship 80% of L18's wins (L8 + L11 + L13 + L14) at 30% of the cost, on top of DataFusion, while keeping Substrait + SQL parser compatibility.
2. **L9 (Cranelift whole-query JIT) as primary lever.** Photon's anti-JIT thesis is well-supported; `project_groupby_research_2026_05.md` documents this. Template specialisation (L11) is the right pattern.
3. **L16 (GPU offload) for SF=10.** Q01-Q22 at SF=10 are too small to amortise GPU dispatch overhead on most queries. GPU pays off at SF=100+ where the per-query data exceeds dispatch costs by 2+ orders of magnitude. L16 is a cluster-mode lever, not an SF=10 lever.
4. **Σ.Q.M Slice 2 / Slice 4 reattempts.** `project_sigma_qm_slice2_rejected.md` and `project_sigma_qm_slice4_spike_rejected.md` documented the structural reason these fail (DataFusion CSE doesn't share Join outputs or builds). L8's CBO + L13's custom hash join replace these structurally; reattempting them in the rule chain is wasted effort.
5. **TPC-H-specific rules** (per `feedback_no_tpch_hardcoding.md`). Every V2 lever is a generalised pattern. L8 CBO recognises any join-order rewrite that the cost model prefers; L11 recognises any plan shape that monomorphises cleanly; etc.
6. **Σ.R RobinHood AVG variants.** `project_sigma_r2_rejected.md` documented the +40-55% Q17 regression. The lesson — "21.6% self-time ≠ 21.6% reclaimable" — applies; don't re-litigate.

---

## 9. References

V1's references all carry forward (see V1 §6). V2 additions:

- `project_sigma_l_adaptive_runtime.md` — Σ.L 32-test pipeline; foundation for L17 + L8 observer wire-up.
- `project_distributed_is_shipped.md` — distributed is shipped; cluster benchmark publication is the dominant strategic next step at month 6.
- `project_ematix_parquet_q14_integration.md` — proof that owning the storage decode path (ematix-parquet) lands clean wins outside the codegen-tax pathway; template for how to scope L8/L13/L11 (sibling crate).
- `project_ematix_parquet_repo.md` — ematix-parquet repo scope decisions; the same scoping discipline applies to ematix-flow-planner (L8 home).
- `project_dict_arrival_blocker.md` — dict-preserved is opt-in via FastParquetTableProvider; L14 makes it default with Σ.L.1 speculative race.
- `project_sigma_p_subquery_cse.md` — Σ.P SharedSubtreeExec registry; the registry shape is the template for L8's plan-candidate cache.
- `project_groupby_research_2026_05.md` — Photon anti-JIT thesis; informs L9 deferral and L11 design.
- `project_shape_catalog_autotune_direction.md` — Σ.F shape catalogue → autotune; this is the L11 substrate.
- `project_sigma_j2b_v_landed.md`, `project_sigma_j2b_vi_landed.md`, `project_sigma_j2b_vii_landed.md`, `project_sigma_j2b_viii_landed.md` — Σ.J.2.b context-bloom transport infrastructure; L10 generalises this for single-node.
- `project_sigma_nf3_beats_stock.md` — RobinHoodSumF64 beats DataFusion stock; the kernel substrate for L13's hash join.
- `project_sigma_oc2_provider_landed.md` — RowGroupDecodeCache provider; the substrate for L8 CBO statistics consumption.
- `project_optimizer_codegen_sensitivity.md` — codegen tax; the rationale for L3 PGO + sibling-crate strategy for L8.
- `project_ematix_parquet_v013_win.md` — proof that sibling-crate changes (ematix-parquet v0.13) land at +4% with zero regressions, while in-tree rule additions don't. This is the empirical basis for sibling-crate scoping of L8/L13/L11.

---

*End of V2. The user will read this and prune; the doc has deliberately over-proposed.*
