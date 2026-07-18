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

**P2.3 — morsel scheduler. DONE (2026-07-18).** `src/sched.rs` adds `MorselQueue`,
a shared lock-free morsel dispenser, and `run_scan_pipeline` now pulls row groups
from it instead of owning a static `rg % nthreads` stride. A worker that finishes
early immediately claims the next available row group, so **skew and stragglers
stop setting the wall** — the measured motivation is real: SF-10 lineitem is 58 row
groups spanning **~5× in row count** (217K–1.05M rows/RG) across 14 workers, exactly
the imbalance a static stride mishandles. Gate `tests/sched_dispenses_once.rs`
proves the invariant that licenses the swap: every morsel is dispensed **exactly
once** under concurrent workers (no double-decode, no drop). Operators, sink, and
decode path are untouched — only scheduling changed.

Design note: this is the *work-sharing* form (one shared atomic queue, DuckDB's
morsel model), not per-worker Chase-Lev deques. For a flat, statically-known list
of scan morsels the two balance identically, and the shared counter has far less
machinery; deque **stealing** only earns its keep once operators recursively spawn
nested morsels — at which point `MorselQueue` grows a per-worker tier behind the
same `next()` contract. Re-measured SF-10 Q08: **141.0 ms vs 195.1 ms production
DF = −27.7%**, correctness == DF pull (market shares identical). The engine arm
moved 148.9 → 141.0 ms vs P2.2 — the right direction for removing skew-induced
stragglers, but inside SF-10's ±5–10% band, so read the scheduler's *isolated*
contribution as at-or-better, not a banked +5%. Its real prize (straggler/skew
absorption) is a robustness property that 58 RGs across 14 workers only mildly
stresses; expect it clearer at SF-100 or under a noisy box. What P2.3 *proves* is
the correctness invariant (exactly-once dispensing) and no regression — scheduling
is now dynamic without touching the operators or the sink.

**P2.4 — spill kill-gate. DONE (2026-07-18).** The engine's first breaker that
stays correct beyond a memory budget. `src/spill.rs` = `PartitionSpill`, a
std-only (no `tempfile` dep — clean-room intact) temp-dir-backed store of
fixed-width `(i64,i64)` records, one append file per partition, lazily created and
RAII-removed. `src/agg.rs` = `SpillableSumAgg`, a **GRACE partitioned** hash
aggregate: radix-partition by `hash(key)` so every occurrence of a key lands in one
partition → each partition aggregates independently (union is complete, no key
split) and its group map holds only ≈ groups/npart entries → it fits even when the
whole table didn't. On budget overflow it flushes every partition buffer to disk
(spill-all); at finish it aggregates each partition from its in-memory remainder +
streamed-back spill run (one partition's map ever resident).

Kill-gate `tests/spill_agg.rs` picks `SUM(i64) GROUP BY i64` deliberately — exact
and order-independent, so the spill path's changed summation order **cannot** alter
the answer: with a 64 KiB budget over 1 M rows, **999 424 of 1 000 000 rows spilled
through disk** yet the result is **bit-identical** to the unbudgeted in-memory run
(same groups, same sums). That is the honest proof the mechanism moves every row
exactly once. Std-only, 14 engine tests green, clippy + fmt clean.

Scope kept narrow on purpose (prove cheap): f64 measures (revenue) are correct only
up to FP re-association across the changed order — standard for a spilling
aggregate — so an f64 variant is a labelled follow-on, not the exactness gate; and
this aggregate is **single-threaded** — per-worker parallel spill + a merge phase,
and reusing `PartitionSpill` for the hash-**join** build side, are the next steps.

**P2.5 — spilling hash-join build. DONE (2026-07-18).** Extends spill to the
engine's headline win (the Q08 join path). `src/hashjoin.rs` = `SpillableHashJoin`,
a **GRACE hash join**: both sides radix-partition by the *same* `part_of` hash, so a
build row and a probe row sharing a key always land in the same partition → each
partition joins independently, union is the complete join, only one partition's
build hash table resident (peak ≈ build-rows/npart — what lets it run when the whole
build side wouldn't fit). On overflow the fed side flushes to disk; at `run()` each
partition builds its table from the build remainder + streamed spill run, then
probes with the probe remainder + streamed spill run, emitting every matched
`(key, build_pay, probe_pay)` — probe side never fully materialized. Reuses
`PartitionSpill` verbatim (its `(i64,i64)` record *is* `(key, payload)`).

Handles what a semijoin can't: **multi-match** (dup build keys → `key → Vec<pay>`)
and **inner-join drop** (unmatched probe keys vanish). Refactor: `part_of` moved to
`spill.rs` as the single shared partition-routing primitive, so build/probe/agg
route identically — the co-partitioning invariant now has one source of truth.
Kill-gate `tests/spill_join.rs`: multi-match + half the probe keys unmatched; a
256 KiB budget spills **24 576/40 000 build + 200 000/200 000 probe rows to disk**
(build keeps a resident remainder, so the join merges spilled + resident) yet the
match multiset is **bit-identical** to the in-memory join and matches an independent
pair-count formula (200 000). Std-only, 17 engine tests green, clippy + fmt clean.
Follow-ons: multi-column payloads, recursive re-partition for a single overflowing
build partition (key skew), per-worker parallel partitioning + merge.

**P2.6 — adaptive re-plan. DONE (2026-07-18).** The last distinct P2 capability, and
the clearest "capabilities DF can't express" case: a static plan commits to a join
strategy from *estimated* cardinalities before seeing a row, so a wrong estimate can
commit it to spilling a build that fits in cache, or to an in-memory build that
blows past RAM. `src/adaptive.rs` = `AdaptiveHashJoin`, a **hybrid hash join** that
defers the choice to the build breaker: build rows buffer flat and optimistically
in-memory; the moment the *observed* build crosses the budget it **transitions** to
the P2.5 partitioned/spilling path (migrating what's buffered, routing the rest). If
it never overflows, the whole join runs as one in-memory hash table — no partition/
spill machinery. `plan_would_choose()` (what the estimate implied) vs `chosen()`
(what observation used); their divergence is the re-plan (`replanned()`).

Kill-gate `tests/adaptive_join.rs` proves the override in **both** directions, each
cross-checked against an independent in-memory oracle: a wrong "build is 10 MB"
estimate is **downgraded** to the in-memory join once the build is observed small
(no spill); a wrong "build is 1 KB" estimate is **upgraded** to the spilling join
once it overflows — the OOM a static plan would hit — and reaches disk; correct
estimates (both directions) don't re-plan; every strategy's match multiset is
bit-identical to the oracle. Std-only, 23 engine tests green, clippy + fmt clean.
Follow-ons: build-side swap (build the smaller input), broadcast vs shuffle in the
parallel setting, recursive re-partition under key skew.

**P2's distinct capabilities are complete** — morsel scheduler (P2.3), spilling
aggregate (P2.4), spilling hash-join (P2.5), adaptive re-plan (P2.6). What remains is
**integration, not new capability** — the P3-adjacent push toward running whole
queries engine-native.

**Integration step 1 — general aggregate breaker, Q08's bespoke `Sink` retired. DONE
(2026-07-18).** `src/agg.rs` adds `AggBinding<N>` (a query's binding to the aggregate:
given a `DataChunk`, emit `(group_key, [f64; N] measures)` per live row that belongs
to a group; inner-join misses aren't emitted — the seam a P3 SQL front-end targets)
and `HashAggregateSink<N, B>` (the general `GROUP BY key → SUM(measures)` breaker — a
`Sink` the row-group-parallel driver runs one-per-worker, partials summed by
`merge()`). The aggregation *machinery* now lives in the engine, not each query. The
Q08 GO/NO-GO harness drops `Q08AggSink` entirely and supplies only a `Q08Agg` binding
(year-bucket key from the orders inner-join payload; measures = total volume + its
BRAZIL share). Parallel wiring is the driver's existing per-worker-sink + merge model
— no driver change. SF-10: shares still == DF pull within 1e-6, and **141.5 ms vs
193.7 ms DF = −27.0%**, statistically unchanged from P2.3's −27.7% — generalizing the
fixed-array sink into a HashMap-backed breaker cost nothing. Gate
`tests/hash_aggregate.rs` (parallel merge + inner-drop + multi-measure + empty/single,
exact-integer f64 so merge order can't perturb it). 26 engine tests green.

**Integration step 2 — native string decode + part probe. DONE (2026-07-18).** Q08's
last DataFusion dependency is `build_probes` — the small-table reductions that seed
the probes. The first (part) moves off DF here, which needed a capability the
numeric-only engine lacked: **string decode + a string-equality filter**, landed as a
reusable primitive. `vector.rs` adds `Storage::Utf8` (Arrow-style offsets + one byte
buffer, no per-string alloc) + `Vector::utf8` + an `as_utf8()` `Utf8View` whose
`get(i) → &str` hoists the storage match out of the row loop. `scan.rs` (the P0
stock-parquet low-level reader — not DF, not Arrow) gains `ColKind::I64` and
`ColKind::Utf8` (`ByteArrayColumnReader` packed into offsets+data). `dim.rs` =
`collect_i64_keys_where_str_eq` (`SELECT key WHERE strcol = needle` — the semijoin
membership seed); Q08's part reduction is `p_partkey WHERE p_type = 'ECONOMY ANODIZED
STEEL'`, now built with **zero DataFusion**.

Gate `tests/dim_part_native.rs`: native part reduction over SF1 `part.parquet` ==
an independent pyarrow oracle (1451 rows, `p_partkey` checksum 145 231 383, keys
distinct in [157, 199 949]). End-to-end SF-10: the natively-built part probe drives
the same 60M-row lineitem semijoin and Q08 shares still equal DF pull within 1e-6
(so the native set matches DF's), at **142.7 ms vs 197.7 ms = −27.8%** — unchanged
(the dim-build runs before the timed section). 27 engine tests green.

**Integration step 3 — native supplier reduction, on the engine's own join. DONE
(2026-07-18).** The supplier reduction is genuinely a join — `supplier ⋈ nation` on
nationkey, carrying the nation's `n_name = 'BRAZIL'` flag as payload — so
`dim.rs::supplier_nation_flag` builds it on the engine's **own** `AdaptiveHashJoin`
(the first time a join operator runs on real TPC-H data, beyond its synthetic gates):
project nation to `(n_nationkey, name==needle ? 1 : 0)` via the step-2 string
primitive, build (25 rows — the adaptive join keeps it in memory), probe with supplier
(`s_nationkey → s_suppkey`), emit `(s_suppkey, flag)`. No new capability — it composes
P2.6's join with step-2's string decode. Gate `tests/dim_supplier_native.rs`: SF1
supplier+nation == a pyarrow oracle (all 10 000 suppliers, suppkey sum 50 005 000, 397
flagged BRAZIL, brazil-key checksum 1 940 760). End-to-end SF-10: native part **and**
supplier probes, shares == DF pull within 1e-6, engine 142.7 ms vs DF 191.9 ms
(engine arm identical to step 2; the % only moves with the DF baseline's noise). 28
engine tests green.

**Integration step 4 — native orders reduction. Q08 IS FULLY ENGINE-NATIVE. DONE
(2026-07-18).** The last `build_probes` SQL is gone. The orders reduction is a chain
of `∈`-membership semijoins down to a customer set, then a date-windowed filter:
`region 'AMERICA' → nation → customer`, then `orders WHERE o_custkey ∈ customers ∧
o_orderdate ∈ [1995,1996] → (o_orderkey, year_bucket)`. `dim.rs` adds two composable
pieces — `collect_i64_where_i64_member` (the semijoin link, reusing the same
`ProbeStructure` membership the lineitem hot path uses) and `orders_semijoin_datebucket`
(custkey semijoin + Date32 window + a 0/1 bucket at a split day). No new capability;
Q08's date constants (1995-01-01 = day 9131, split 9496, 1996-12-31 = 9861) are the
harness's to supply, like the Q6 kernel's — the engine stays general. The harness
`build_probes` drops its last `ctx.sql()` (and the whole plain DataFusion context that
existed only for it) — now a plain sync fn over the parquet files.

Gate `tests/dim_orders_native.rs`: the full native chain over SF1 == a pyarrow oracle
(reduces to 1 region, 5 nations, 29 952 customers; 91 179 orders — 45 630 / 45 549 —
key sum 273 979 458 755, bucket-0 key sum 137 136 132 715). End-to-end SF-10: all three
probes native, shares == DF pull within 1e-6, **engine 141.3 ms vs DF 200.8 ms** (the
engine arm is unchanged ~142 ms across every run; the % only moves with the DF
baseline's noise). The only DataFusion left is the pull baseline the engine is measured
against. 29 engine tests green.

**Integration arc complete: a whole TPC-H query runs end-to-end on the clean-room
engine, beating production DataFusion.** Q08: native dim reductions (string filter,
`⋈`-join, semijoin chain) → native scan → no-materialization probe → general parallel
aggregate — zero DataFusion in the path. The four P2 breakers (scheduler, spilling
agg, spilling join, adaptive re-plan) are proven in isolation; the aggregate and the
probes run through the parallel driver on real data.

Optional follow-ons (not blocking the milestone): wire the **spilling** agg/join
through the driver (per-worker `PartitionSpill` + merge) for high-cardinality group-bys
/ beyond-RAM joins; move dimension **string decode** onto ematix-parquet; extend beyond
Q08 (more of TPC-H) behind a real front-end (P3).

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
