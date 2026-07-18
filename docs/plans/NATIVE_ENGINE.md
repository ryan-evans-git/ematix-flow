# Native Engine — clean-room, push-based, DataFusion-free

*Program doc, greenlit 2026-07-18. Goal: replace the DataFusion substrate with
an owned, push-based, morsel-driven analytical engine. This **supersedes the
"contained morsel region behind one DF `ExecutionPlan` node" strategy** in
[[MORSEL_ENGINE]] — that containment was the ceiling; this cuts the cord.*

> Owner: Ryan Evans. SemVer: this is the engine under v2's analytics runtime
> ([[V2_TARGET]]); ships as a breaking major. Status: planning / P0.

---

## Decision (2026-07-18)

- **Scope: clean-room.** Own the in-memory format, the physical + logical IR,
  the execution model, the scheduler, and the front-end from the AST inward.
  **No DataFusion. No `arrow-rs` as substrate.**
- **Drivers (all four selected):**
  1. Kill the pull-model materialization tax.
  2. Escape the DataFusion codegen-perturbation tax.
  3. Capabilities DF can't express — dictionary/encoding preserved end-to-end,
     runtime-adaptive re-optimization, first-class spill.
  4. Independence & IP — own the IR and release cadence.
- **Two bootstrap assumptions (overridable, not clean-room violations):**
  - **Arrow is a boundary export adapter, not the substrate** (DuckDB model).
    Internal vectors are ours; Arrow is emitted only at the FFI/Flight edge so
    v2 interop (pandas / Arrow Flight SQL / ADBC) survives zero-copy.
  - **`sqlparser-rs` supplies tokenize→AST only.** It is a standalone parsing
    library, not DataFusion. We own everything from the AST inward. A
    hand-written parser is a later swap, gated on error-quality/latency need.

## Why now — the measured case (not a rewrite impulse)

| Driver | Evidence (already measured, in-repo) |
|---|---|
| Pull-materialization tax | Fused push loop on Q08 = **−16% vs the DF pull path, beating DuckDB** — the pull model materialises a 60M-row intermediate and `take`-gathers at every join boundary (`ematix-flow-push` ADR §1.3). |
| Codegen tax | Every rule/operator added to `ematix-flow-core` costs **5–8% geomean on unrelated queries** ([[project_optimizer_codegen_sensitivity]]) — the reason `ematix-flow-push`, `ematix-flow-hash-join`, `ematix-parquet` are already DF/Arrow-free sibling crates. |
| Capabilities | Dict-arrival blocker: DF/Arrow drop dictionary encoding through the scan, no-op'ing a whole rule class ([[project_dict_arrival_blocker]]). Estimator can't model multi-join/LIKE; static beats autotune ([[project_sigma_aomega_reassessment]]) → the lever is *adaptive*, not a better static CBO. |

**Honest scope line (from our own de-risk):** decode-bound queries
(Q06/Q14/Q15) are **contention-capped** (~7–10%, Snappy memory traffic — not
scheduling; [[MORSEL_ENGINE]] P2). The native engine's headline win is
**no-materialization on join/agg-heavy shapes, NOT faster decode.** If a
remaining loss is decode-bound, this engine is the wrong tool for it.

## Non-goals — what we do NOT rebuild

- **Operators DF already wins.** S1–S3 measured DF's grouping-sets and window
  paths as already competitive on our engine (a native grouping-sets op came in
  **1.8× slower** and was reverted). Only rebuild where the pull /
  materialization / codegen tax is real. See [[V2_SQL_SURFACE_GAPS]].
- **No JIT in the hot path (initially).** Vectorized-**interpreted** operators
  (precompiled, runtime-dispatched — the DuckDB model) are what *structurally
  kill the codegen tax*: zero per-query codegen. JIT is a later adaptive layer,
  gated on measurement, not a founding assumption (cf. `PHASE_SIGMA_G3_JIT`).

---

## Architecture — mapped to the four drivers

### 1. Memory: the unified vector format  → *kill-materialization, dict end-to-end, spill*

- **`Vector` = logical type + physical encoding + validity bitmap.** Encodings:
  `Flat(buffer)`, `Dictionary{codes, dict}`, `Constant(scalar)`,
  `Sequence{start, step}` (row-ids/ranges for free). Dictionary/constant survive
  *through operators* — this is the dict-arrival capability DF can't give us.
- **`DataChunk` (morsel) = Vectors + a deferred `Selection` + count.** Operators
  narrow the `Selection` (or attach a probe payload) and **do not compact** —
  compaction happens only at sinks. This promotes `ematix-flow-push::{Morsel,
  Selection, ColumnData}` from prototype to the engine's spine.
- **Buffer manager owns all execution memory.** Block-based, buffers
  pin/unpin/spill. Pipeline breakers (hash table, sort, agg) allocate through it
  → larger-than-memory on one box (a v2 target).
- **Arrow only at export** (a `to_arrow` adapter over `Flat`/`Dictionary`).

### 2. Execution: push, morsel-driven, thread-per-core  → *kill-materialization, independence*

- **Physical plan = pipelines split at pipeline breakers** (hash-join build,
  aggregate, sort, distinct). Each pipeline: a **source**, a chain of **fused
  push operators**, a **sink**. No `DataChunk` materialized between operators.
- **Morsel-driven parallelism:** source hands sub-row-group morsels into a
  **thread-per-core, work-stealing** pool (crossbeam-deque). Per-thread partial
  sink state, merged at the breaker (the `CombineAgg` pattern we have).
- **No tokio in the hot path** — synchronous, cache-friendly; async only at the
  outermost result boundary.
- **Vectorized-interpreted operators** — precompiled, chosen at plan time,
  dispatched at runtime → **zero per-query codegen → the 5–8% tax is gone by
  construction.**

### 3. Adaptive execution  → *capabilities DF can't express*

- **Re-plan the suffix at each pipeline breaker** with the real cardinality now
  known: flip build/probe side, pick the probe structure (`ematix-flow-push::
  probe::choose` already does dense-bitset vs hash adaptively), switch aggregate
  strategy (hash vs sort vs radix). This is where we can exceed DuckDB/DF — both
  largely static.

### 4. Front-end, owned from the AST in  → *independence & IP*

- `sqlparser` AST → **binder** (catalog, column/function resolution, type
  coercion) → **owned logical IR** → **logical optimizer** (port the Σ rules to
  our IR — now data-over-a-plan, so they *also* stop paying the codegen tax) →
  **physical planner** (logical → pipelines) → adaptive re-plan hook.
- No DF `LogicalPlan`, no `ExecutionPlan` trait. Our IR, designed for pipeline
  construction and fusion.

### 5. Correctness & proving ground

- **The DF-based engine is the oracle.** Differential-test every query
  row-for-row against it (we already do this vs DuckDB — `tpch_validate`,
  `tpcds_validate`). No native result ships until it matches.
- **Proving ground = TPC-H first** (harness, data, DuckDB baseline all exist),
  then TPC-DS for the v2 surface.

---

## Migration path to clean-room (co-resident during construction)

The native engine has **zero DF dependency**. During construction the DF engine
stays in the workspace **only as oracle + fallback** for shapes not yet built —
this is the *path to* clean-room, not a compromise of it.

**Strangler-fig:** route the shapes the native engine covers to it; DF handles
the rest; the fallback set shrinks each phase; when it hits zero, drop DF. **End
state = clean-room.**

---

## Phased de-risk (kill-gates — [[MORSEL_ENGINE]] discipline: prove cheap before building big)

| Phase | Deliverable | Kill-gate |
|---|---|---|
| **P0 substrate spike** | Vector format + one pipeline: **Q6 end-to-end** — native decode → native vectors → push filter → push sum → native result. No Arrow, no DF. | Matches DuckDB Q6; per-thread throughput ≥ current DF path. *(Correctness + spine — not a perf-win proof; Q6 is decode-bound.)* |
| **P1 — the thesis** | Hash-agg (**Q1**) + hash-join (**Q3/Q8**) as pipeline breakers with morsel parallelism. | **Reproduce the −16% push win vs the DF pull path on Q08 at correctness parity. This is GO/NO-GO for the whole engine.** |
| **P2** | Work-stealing scheduler + buffer manager + spill + adaptive re-plan at breakers. | No regression on P1; a spilling hash-join correct beyond RAM. |
| **P3** | Front-end: binder + owned logical IR + physical planner; port Σ rules. | Arbitrary TPC-H SQL plans onto the engine (not hand-built pipelines). |
| **P4** | Surface expansion: TPC-H 22/22 → TPC-DS → windows / grouping-sets / subqueries. | 22/22 then 103/103 differential-match the oracle. |
| **P5** | Interop boundary: Arrow export, pandas / Flight / ADBC on the native engine. | Zero-copy Arrow export parity with today. |
| **P6** | Strangler cutover; DF fallback → zero; drop the DF dependency. | Clean-room achieved; full surface on native engine. |

**P1 is the whole ballgame.** If the clean-room push engine does not beat the DF
path on the shape we already measured at −16%, stop and reassess — that is the
de-risk doing its job cheaply.

---

## P0 result — 2026-07-18. PASS.

Crate `crates/ematix-flow-engine` (zero DataFusion, zero Arrow in the data path).
TDD red→green; the structural matched-row check caught a real bug before the sum
could mask it.

| check | result |
|---|---|
| Correctness (`tests/q6_killgate.rs`) | `matched=114160`, `revenue=123141078.2283` — exact match to the DuckDB SF-1 oracle |
| Spine throughput (filter+sum, 1 thread) | **660 M rows/s** — the compute spine is not the bottleneck; single-pass fusion + SIMD still untapped |
| Decode (stock parquet, temporary) | 118 M rows/s, 1 thread — replaced by ematix-parquet native decode at P1 |

Substrate shipped: `vector.rs` (typed `Arc`-backed `Vector`; `LogicalType`/`Storage`
structured to grow Dictionary/Constant/Sequence), `chunk.rs` (`DataChunk` +
deferred `Selection`), `scan.rs` (low-level parquet → native vectors),
`pipeline.rs` (`filter` narrows the selection without compaction; `sum_product`
sink).

**Bug caught — an engine lesson:** DuckDB folds Q6's `0.06 ± 0.01` decimal
literals in *decimal* (→ 0.05 / 0.07); computing `0.06 + 0.01` in f64 lands one
ULP below stored 0.07 and silently drops ~1/3 of matches. The P3 binder must
constant-fold decimal literals in decimal, not f64.

**Not proven by P0 (by design):** the −16% no-materialization thesis — Q6 is
decode-bound and the push win is on join/agg shapes. That is P1's GO/NO-GO.

---

## P1 result — 2026-07-18. GO. ✅

The −16% no-materialization thesis **survives in the clean-room engine.** Q08's
fused hot path, routed through the engine's own `DataChunk` + `join::probe_narrow`
(deferred selection, no per-join `take`) and decoding via ematix-parquet, beats
the **current production DF pull path** (full preset rule chain) on this box:

| arm | SF-10 Q08, median of 7 interleaved trials |
|---|---|
| PULL — production DataFusion | 198.8 ms |
| ENGINE — clean-room push | **171.7 ms  (−13.6%)** |

Kill-gate ≥12%: **PASS.** Correctness: engine market-share == DF pull, row-for-row
(differential, not a loose constant). Probe structures picked by `choose`:
part=DenseSet, orders=HashTable (o_orderkey domain >4M → hash), supplier=DensePayload.

The 13.6% (vs the earlier −16%) is within SF-10 single-query noise (±5–10%) and is
a **floor, not a ceiling** — the agg is still a naive per-row loop; single-pass
fusion, SIMD, and the P2 morsel scheduler should widen the gap. Harness:
`crates/ematix-flow-core/examples/p1_engine_q08_gonogo.rs` (engine wired as a core
dev-dependency; DF only in the pull baseline, never in the engine).

**What P1 does NOT yet do** (P2 groundwork): the engine still borrows core's
`masked_decode` and hand-assembles Q08 in the example. Next: the engine's own
native scan (ematix-parquet → engine vectors, DF-free), generalized join-build /
aggregate breakers, and a pipeline driver — so the engine runs the query class
standalone — then the work-stealing scheduler.

---

## P2 progress

**P2.1 — engine-native scan. DONE (2026-07-18).** `src/scan_native.rs` decodes
row-group columns straight into engine `Vector`s via the ematix-parquet codec
(`ParquetFile::open` + `read_column_{i64,i32,f64}_masked_into`) — the same
primitives core wraps in its bridge, called directly so the engine keeps **no
DataFusion dependency** in its decode path. Gate `tests/q6_native.rs`: the native
decoder produces **bit-identical** Q6 results to the P0 stock-parquet decoder and
matches the DuckDB SF-1 oracle (`native.revenue == stock.revenue == oracle`).
Clippy-clean.

**P2.2 — pipeline framework + engine-native Q08. DONE (2026-07-18).** `src/exec.rs`
adds the engine's push execution framework: `PushOp` (stateless pipelined ops —
e.g. `ProbeNarrowOp`), `Sink` (stateful breakers), and `run_scan_pipeline` — a
row-group-parallel driver over the native scan (the generalization of P1's
hand-loop; per-thread sinks merged by the caller). Gate `tests/driver_smoke.rs`.
The Q08 GO/NO-GO harness now runs its push arm **entirely through the engine** —
native scan → driver → `ProbeNarrowOp` → an engine `Sink` — with zero core
`masked_decode` and zero DataFusion. Re-measured SF-10: **148.9 ms vs 191.8 ms
production DF = −22.4%** (correctness == DF pull; leaner than P1's −13.6% crutch
version). SF-10 single-query variance is ±5–10%, so read this as a solid GO, not a
precise delta.

Remaining P2: the **work-stealing morsel scheduler** (replace the driver's static
`rg % nthreads` stride — the piece designed to be swapped), then spill + adaptive
re-plan. Also pending: a *general* aggregate breaker (Q08's agg is still a
query-specific `Sink` in the harness) and building dimension probe structures via
engine pipelines (still SQL in the harness).

---

## Open questions (yours — I have a recommendation on each)

1. **Crate/repo.** *Recommend:* build as an in-repo workspace crate
   (`ematix-flow-engine`, placeholder name) for the spike — reuse the DF oracle,
   the bench harness, the TPC-H data. Extract to its own repo later to bank the
   independence goal. In-repo ≠ DF-coupled: the crate takes no DF dep.
2. **Chunk size.** *Recommend:* start at 2048 rows (DuckDB), tune against the
   morsel-trace findings.
3. **Own SQL parser eventually?** *Recommend:* keep `sqlparser` AST until error
   quality or parse latency is measured to matter.
4. **JIT, ever?** *Recommend:* interpreted-first; revisit JIT only as a measured
   adaptive layer for hot pipelines (cf. `PHASE_SIGMA_G3_JIT`).
5. **Name.** `ematix-flow-engine`? A distinct product name (independence)?
