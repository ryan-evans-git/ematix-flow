# PV.M.5 — Work-stealing sub-row-group morsel scan-decode (program / ADR)

- **Status:** PROPOSED — scoping/design only. NOT accepted. Phase 0 is a hard kill-gate; no source is written past it unless Phase 0 is GO.
- **Date:** 2026-06-05
- **Author:** architect (cold-start design pass)
- **Reviewers:** TBD (perf owner)
- **Decision scope:** ONE decision — whether to commission a multi-session program that introduces a **work-stealing sub-row-group morsel decode pool** into `EmatixFastParquetExec` to recover Q15's residual f64-decode loss vs Polars, by replacing the current **one-coarse-task-per-scan-partition** decode with finer, stealable decode units. Full executor replacement is out of scope.
- **Relates to / contrasts with:** `docs/PV4_MORSEL_PIPELINE_PROGRAM.md` (SHELVED) and `docs/ADR_PUSH_VECTORIZED_ENGINE.md` (PROPOSED). Those target the join/agg **TAIL** (Q08 materialization tax). **This program targets the scan DECODE** — a different and, on current evidence, more promising application of work-stealing. See §8.

---

## 1. Thesis

> Q15's entire residual loss to Polars at SF=10 is the lineitem f64 decode stage (decode `l_extendedprice` + `l_discount` under a ~3.6%-selective `l_shipdate` filter, then SUM). The total decode **CPU** is at parity with Polars — including the Snappy decompress, which Polars does with the **same `snap` crate**. The difference is **parallel efficiency**: Polars schedules the same CPU tighter (sub-row-group morsels in a work-stealing async executor), hitting ~1.1× the single-thread/14-wide ideal; ematix decodes **one coarse task per scan partition** (~4-5 row-groups burst-decoded sequentially), so a straggling partition strands its core and we land at ~1.4× the ideal. The program asks: can a work-stealing sub-RG morsel decode pool recover that efficiency inside DataFusion's pull model — or learn precisely why the pull model can't?

### Measured evidence (this session — ground truth)

1. **Q15 is the only remaining Polars loss at SF=10.** Was 1.30×; a CSE-parallel-drain fix shipped −13% → now ~1.17×.
2. **The residual is the f64 decode stage**, isolated by staged decomposition (`q15_decompose_ab.rs`: COUNT → SCALAR → REVENUE → FULL). We **win** the date-decode+filter, **tie** the group-by SUM agg, **tie** the join/sort tail (measured −0.2 ms). The gap is the `SCALAR − COUNT` step: f64-decode + scalar-sum.
3. **Every cheap f64-stage lever is REFUTED with fresh measurement this session:**
   - morsel-pipeline of **operators** (the PV.4 tail) — the tail is already free (−0.2 ms);
   - join-reorder — Polars uses the identical plan order;
   - agg / RobinHood SUM kernel — parity;
   - dense-vs-masked f64 decode — **parity** on current kernels (`f64_masked_vs_dense.rs`); the prior "~12× dense wins" note was **STALE** (kernels improved since);
   - the **"Snappy codec floor"** — **REFUTED** by profiling Polars directly: Polars uses the exact same `snap` crate with ~equal per-iteration decompress CPU.
4. **The real, measured difference is scheduling, not work.** Same total decode CPU; Polars achieves ~1.3× lower wall by scheduling it tighter. Polars' profile shows a work-stealing async executor (`polars_stream::async_executor::try_steal_task`) decoding **sub-row-group morsels**. Ours is **one coarse `spawn_blocking` task per scan partition** (~4-5 row-groups each, burst-decoded), so a straggler strands its core.
5. **CPU math.** Single-thread decode CPU ≈ **510 ms** (extprice 317 + discount 44 + shipdate 150). Perfect 14-wide ideal ≈ **36 ms**. We hit ~**51 ms** (1.4× off ideal); Polars ~**40 ms** (1.1× off ideal). The gap is **parallel efficiency** — not CPU, not codec, not kernel.

The recoverable quantity is the **51 → ~36 ms** band on the SCALAR stage (≈ 15 ms of stranded-core slack), which is approximately the whole Q15 residual once the (parity) tail and agg are blended back.

---

## 2. THE RED FLAG (central risk, stated up front)

**Two parallelism-knob sweeps were already run this session and BOTH were FLAT:**

| knob | values | SCALAR-stage wall (ms) |
|---|---|---|
| `target_partitions` | 7 / 14 / 28 / 42 / 56 | 63 / 51 / 51 / 52 / 52 |
| reader-parallelism budget (`EMAT_READER_PARALLELISM_BUDGET`, the REV.22 cap = `total_threads / outer_partitions`) | 1 / 2 / 3 / 4 / 6 | 51 / 49 / 50 / 51 / 51 |

`target_partitions` is flat at and above core count (14). The budget knob is flat everywhere.

**This is a warning that the work-stealing thesis might be WRONG.** If `target_partitions=56` already created ~56 fine-grained (~1-RG) decode tasks fed to tokio's (work-stealing) scheduler and wall was STILL 52 ms, then finer granularity does **not** help and the program's central premise is dead.

### What the code says about that sweep (read this session — sharpens the flag into a single testable question)

The flatness is **not yet decisive**, because the code reveals a confound. From `crates/ematix-flow-core/src/ematix_fast_parquet.rs`:

- **The provider caps partitions and round-robins row-groups** (`:2179-2184`):
  ```rust
  let target_partitions = state.config_options().execution.target_partitions;
  let num_partitions = num_rgs.min(target_partitions).max(1);   // CAP at RG count
  assignments[rg % num_partitions].push(rg);                    // round-robin
  ```
  SF=10 lineitem has ~57-60 row-groups (2.0 GB file, 121 KB footer). So `PARTS=56` **does** create ~56 partitions of ~1 RG each — `min(60, 56) = 56`. **PARTS=56 genuinely fanned the decode out to ~56 tokio tasks.** That pushes the thesis toward *doubt*.
- **BUT the within-RG decode is COLUMN-parallel, not page/row-parallel**, and its thread budget collapses to 1 at high partition counts. `load_row_group_dense` (`emat_arrow_reader.rs:1588`) spawns a `std::thread::scope` over a shared atomic work-queue **of the missing columns** (3 for Q15), capped at `budget = max(1, total_threads / outer_partitions)` (`:2872`). At PARTS=14 → budget=1; at PARTS=56 → budget = max(1, 14/56) = **1**. So at PARTS=56 each 1-RG partition decodes its 3 columns **sequentially on one thread**, and there are 56 such tasks contending for 14 cores.
- **A row-group is decoded as an atom.** `Iterator::next` (`:1737`) advances `cur_rg_idx` one RG at a time and calls `load_row_group_dense` synchronously; there is **no sub-RG (page-range) unit** anywhere, and **no shared decode pool across partitions** — each `spawn_blocking` worker owns its RGs privately.

**So the flat sweep means one of two things, and Phase 0 must decide which:**

- **(a) The thesis is in serious doubt.** PARTS=56 already gave tokio ~56 stealable ~1-RG tasks and wall stayed at 52 ms. If finer-than-1-RG morsels behave the same, sub-RG granularity buys nothing in *this* runtime, and Polars' edge is something else (e.g. its decode kernel is faster per-byte at this column shape, or its scheduler overlaps decompress with the next page's I/O in a way tokio `spawn_blocking` cannot). **→ the program is dead; ship nothing.**
- **(b) The de-risk is still open.** PARTS=56 created 56 *coarse, RG-atomic, single-thread, oversubscribed* tasks — which is NOT the Polars regime. Polars steals **sub-RG morsels** across a **bounded** pool sized to cores (not 56 tasks on 14 cores), with no per-task RG-atomicity barrier. The flatness may be the oversubscription (56 tasks / 14 cores, budget=1) and RG-atomic straggling cancelling out, not proof that fine granularity fails. **→ the de-risk needs a real sub-RG work-stealing spike to settle it.**

Phase 0 exists to resolve **exactly (a) vs (b)**, cheaply, on the real lineitem, end-to-end. **The thesis is presumed dead until Phase 0 proves the gap is recoverable by sub-RG work-stealing specifically.**

---

## 3. Phase 0 — decisive de-risk spike (GO / NO-GO). THE KILL-GATE.

**Goal:** determine whether a **bounded work-stealing pool over sub-RG decode morsels** moves the SCALAR-stage wall toward the ~36 ms 14-wide ideal — and disambiguate red-flag (a) vs (b). No engine, no operator, no DataFusion integration. One throwaway example binary against **real SF=10** `lineitem.parquet`.

### 3.1 First: instrument what `PARTS=56` actually did (cheap, ~half a day)

Before building the spike, settle the confound with counters, reusing `f64_decode_profile.rs` (which already has a `PARTS` override and runs the SCALAR query on a shared ctx):

- Log, per run at PARTS ∈ {14, 28, 56}: **number of `spawn_blocking` decode tasks actually created** (instrument `build_partition_stream` / `build_streaming_partition_stream`), the **per-partition RG count**, the **resolved `parallelism_budget`**, and **which decode path the lineitem scan took** (eager bridge `build_partition_stream` vs `streaming_arrow_reader` vs auto-inline — the dispatch at `:3215-3233`).
- Record **CPU/wall ratio** and **per-task start/end timestamps** at PARTS=14 and PARTS=56 to see whether stragglers + idle cores are present, or whether all 56 tasks finish near-simultaneously (the latter would say the work is already balanced and the 1.4× is per-task overhead, not straggling).

**This alone may kill or confirm the thesis** before any spike code:
- If PARTS=56 shows **all cores busy, no idle tail, balanced finish, and wall still 52 ms** → red-flag (a) is real; the 1.4× is *not* straggling; **NO-GO**, the gap is per-byte decode or a tokio-vs-Polars overlap property, not granularity. Stop. (Redirect note in §7.)
- If PARTS=56 shows **idle cores in a tail while a few RG-atomic tasks straggle**, or shows that the lineitem scan **did not actually fan to 56 tasks** (path coalesced, or budget=1 serialized the columns so each task is long) → red-flag (b); proceed to the spike.

### 3.2 The spike (only if 3.1 lands in (b) or is ambiguous; ~2-3 days)

Build one `main.rs` (model it on `f64_masked_vs_dense.rs` + `f64_decode_profile.rs`) that decodes the SCALAR-stage columns of **real SF=10 lineitem** — `l_extendedprice` (col 5), `l_discount` (col 6), `l_shipdate` — under the ~3.6% shipdate filter, and SUMs `extendedprice*(1-discount)` over survivors. Three arms, paired/interleaved, mimalloc, warm + cold, ≥21 trials with a sign test (the `paired_ab.rs` methodology):

- **Arm A — current coarse path (baseline).** Reproduce the production decode: `min(60, 14)=14` partitions, each a `spawn_blocking` worker looping ~4 RGs sequentially through `load_row_group_dense` at budget=1 (3 columns sequential per RG). This is what Q15 runs today. Measure its wall and its CPU/wall.
- **Arm B — bounded work-stealing over sub-RG morsels.** Enumerate every (row-group × column-chunk × **page-range**) decode unit for the 3 columns across all ~60 RGs — i.e. **sub-RG morsels** (a page-range of one column chunk; the `PageWalker` already walks pages, so a page-range start/len is a natural unit). Push all units into a **single shared work-stealing deque** (e.g. `crossbeam-deque` or a simple `Arc<Mutex<VecDeque>>` + atomic cursor — the spike does not need to be production-grade). Run a **bounded pool of exactly `available_parallelism()` (14) worker threads** that steal the next morsel, decode it, and accumulate a partial masked-sum. **No RG-atomic barrier; no per-partition private ownership; bounded to core count (not 56 tasks).** This is the Polars regime translated to a spike.
- **Arm C — same as B but whole-RG-per-column units** (no page-range subdivision), still in the shared bounded pool. Arm C isolates **"shared bounded work-stealing pool"** (the scheduling change) from **"sub-RG page granularity"** (the unit-size change). If B ≈ C ≈ A, granularity is irrelevant. If C beats A but B ≈ C, the win is the **shared bounded pool**, not page-level morsels (cheaper to build, and reshapes the architecture in §4). If B beats C, sub-RG granularity is load-bearing.

All arms must produce the **identical SUM** (correctness assertion in the harness, as `f64_masked_vs_dense.rs` already does). Report median wall, CPU/wall, and per-arm idle-tail.

### 3.3 The kill number (hard gate)

> **GO** iff the best of Arm B / Arm C is **≤ ~42 ms** median (recovers **≥ half** the 51 → 36 gap, i.e. ≥ ~7.5 ms of the ~15 ms slack), with a paired sign test **p < 0.05** vs Arm A, AND the per-arm timing attributes the gain to **reduced idle-core tail** (not a measurement artifact — recall PV.4's "−16%" prebuilt-slice artifact, §8).

- **B/C ≤ 42 ms, attributed to idle-tail recovery → GO.** Proceed to §4 architecture. The doc records which arm won (B vs C decides whether page-granularity or just the shared pool is needed).
- **B/C between 42 and 51 ms → MARGINAL → default NO-GO.** A < 18% loop win that requires a shared decode pool fighting DataFusion's per-partition model (§4, R2) is not worth the multi-session spend and integration risk. Ship nothing; record the number.
- **B/C ≥ 51 ms (≈ A) → NO-GO, thesis FALSIFIED.** Sub-RG work-stealing does not beat the coarse path on the real columns; red-flag (a) confirmed. The Polars edge is per-byte decode or an overlap property tokio can't express — a **valid, cheap-to-learn outcome.** Close this program; the redirect is §7.

**NO phase past Phase 0 runs unless §3.3 returns GO.** This is the same single-session kill discipline that correctly shelved PV.4 after `PV.4.0` (§8).

---

## 4. Architecture (meaningful ONLY if Phase 0 is GO)

The hard part is reconciling a **shared work-stealing decode pool** (one queue across all row-groups, bounded to cores) with DataFusion's **per-partition pull contract**: `execute(partition) -> SendableRecordBatchStream`, where DataFusion independently polls 14 partition streams and expects each to yield that partition's rows. A shared pool that decodes morsels in steal-order does not naturally respect partition boundaries. Four sub-problems:

### 4.1 Morsel granularity

- **Unit = a (row-group, column-chunk, page-range) decode task.** The existing `PageWalker` (in `ematix-parquet`) already iterates pages; a morsel is "decode pages `[p0, p1)` of column `c` in RG `r` into a typed buffer slice." Whole-column-chunk-per-RG (Arm C) is the coarser fallback if Phase 0 shows page-granularity isn't load-bearing.
- **Output rows for one morsel are a contiguous sub-range of one RG.** Filter (shipdate) and payload (extprice/discount) morsels for the same page-range must align so the masked-sum (or, in the general case, the survivor selection) is computed over matching rows. **Decision: a morsel is keyed by `(rg, page_range)` and carries all 3 projected columns' page-ranges for that span** — i.e. the morsel is row-aligned across columns, not a single column chunk. This keeps the `Morsel`/`Selection` envelope from `ematix-flow-push` applicable (decoded columns + deferred selection over a contiguous row span).

### 4.2 Shared work queue vs tokio `spawn` (the crux)

Three integration shapes, in increasing invasiveness; pick the least invasive that clears Phase 0's Arm-B/C result:

- **Shape α — one shared pool, partition streams are consumers.** A process- or query-scoped **bounded decode pool** (sized to `available_parallelism()`) owns the work-stealing deque of all morsels for the scan. Each `execute(partition)` stream does **not** own its RGs; instead it pulls **completed morsels for its assigned RGs** off a per-partition completion channel that the pool fills. The pool decodes in steal-order (any worker, any morsel); a completed morsel is routed to the partition that owns its RG (round-robin assignment is preserved as *ownership for output routing only*, not for decode scheduling). This is the truest translation of Polars and the only shape that fully decouples decode scheduling from partition boundaries. **Cost:** a new pool lifecycle, backpressure, and the routing layer; highest R2 risk.
- **Shape β — shared pool, single output partition.** Collapse the scan to **one** output partition (`UnknownPartitioning(1)`) and let the shared pool feed that one stream, then rely on a downstream `RepartitionExec` to fan back out for the agg. Simpler pool↔stream mapping (one consumer), but **reintroduces a repartition boundary** the engine spent effort removing, and serializes the scan's output emission. Likely loses the win to the repartition tax; recorded as a rejected-by-default sub-option.
- **Shape γ — keep `spawn_blocking` per partition, but steal across partitions via a shared deque.** Each partition still has a `spawn_blocking` worker, but instead of looping *its own* RGs, all workers pull from **one shared atomic morsel cursor over all RGs** (exactly the within-RG column work-queue at `emat_arrow_reader.rs:1588`, lifted **up** to span all RGs of all partitions, and **out** to page-range units). A worker decodes whatever morsel it steals and sends the resulting batch to **the owning partition's channel**. This is **Shape α implemented with the machinery already in the file** — the atomic-cursor + scoped-threads pattern is proven in-tree; we widen its domain from "missing columns of one RG" to "page-range morsels of all RGs," and add output routing. **Lowest-risk path to α's behavior; the recommended starting integration.**

**Recommendation (conditional on GO):** start at **Shape γ** (reuse the in-tree atomic-cursor work-queue, widened), because it (a) keeps `spawn_blocking` (no new runtime), (b) reuses a proven pattern, (c) bounds the pool to cores naturally (the worker count is the bound), and (d) leaves DataFusion's partition count unchanged. Escalate to Shape α's dedicated pool only if γ's per-partition `spawn_blocking` framing leaves measurable slack.

### 4.3 Backpressure / bounded buffering

- The existing path already uses a **bounded mpsc** (`tokio::sync::mpsc::channel(8)`, `:2917`) per partition; keep that as the completion channel. A worker that finishes a morsel for a slow-draining partition **blocks on `blocking_send`** (natural backpressure), then steals the next morsel for *any* partition. This prevents re-materializing the whole column set in memory (the §4.5 memory bound) and is the same backpressure the engine already trusts.
- **Bound the in-flight decoded-morsel memory** explicitly: the shared pool must cap total outstanding decoded bytes (a semaphore over morsel byte-size), because steal-order decode can race ahead of slow consumers across *all* partitions at once — a larger blast radius than the current per-partition-of-8 bound. This is a new failure mode (§4.5 / R-mem).

### 4.4 Mapping decoded morsels back to partition output streams + correctness

- **Output routing:** each morsel carries its owning partition id (`rg % num_partitions`, preserving the existing assignment). The pool sends completed batches to `completion_tx[owner]`. A partition stream yields batches from its own channel in **RG-then-page order** (a small per-partition reorder buffer, since steal-order completes morsels out of order). Whether downstream cares about order: the SUM agg does **not**; but —
- **Determinism (Q15's float-determinism CSE).** Q15 ships `DedupeAggregateForFloatDeterminism` (sort-then-sum) precisely because f64 SUM is order-sensitive. **A work-stealing decode that changes the order survivors reach the SUM can change the low bits of the result and break the 22/22 sum match.** Decision: **the morsel layer must emit each partition's rows in deterministic (RG, page, row) order** regardless of steal order — the per-partition reorder buffer above is therefore **mandatory, not an optimization** — so the downstream agg sees the identical row order it sees today. This is the single most important correctness constraint and gets a dedicated TDD test (sum-equality vs the current path at SF=1/10) before any integration code.
- **Null / validity:** decode primitives (`read_column_f64`, `masked_decode_f64`, `decode_one_column`) already produce correct validity; the morsel layer must not hand-roll null logic — reuse the bridge primitives unchanged (the same C1 discipline as `ematix-flow-push/morsel.rs`, where a null-handling miss left Q07 sums 94% wrong for months).
- **L9 sideband interaction:** a scan with a `runtime_sideband` attached uses the deferred-peek-on-first-poll path (`:2567` vs `:2585`). The work-stealing pool must either (a) decline to engage when `runtime_sideband.is_some()` (v1; mutually exclusive, like the PV pipeline's I3), or (b) resolve the filter once before seeding the morsel queue. **v1: decline** — Q15 has no sideband, so this is free for the target query.

### 4.5 Memory

Steal-order decode across all RGs raises peak resident decode memory vs the current per-partition-of-8 bound: in the worst case, one morsel per worker (14) plus the per-partition reorder buffers plus the in-flight semaphore budget. At SF=10 a lineitem RG column is ~tens of MB; the design must cap in-flight decoded bytes (a global semaphore, §4.3) and size the reorder buffers to the morsel granularity, so peak stays O(cores × morsel) + O(partitions × reorder-window), not O(file). This is a **new** memory failure mode the current coarse path does not have (R-mem).

---

## 5. Phased plan (each one session where possible; each default-OFF; each gated)

The shared gate for every code phase (mirrors the house discipline):
- **22q SF=10 interleaved A/B**, ≥11 trials, paired sign test (`paired_ab.rs`, fresh ctx per trial — the Σ.P CSE-replay footgun); flag ON vs OFF.
- **`tpch_validate` 22/22 vs DuckDB at SF=1/10/100 — SUMS, not just row counts** (the Q07-94%-wrong scar; and Q15's float determinism specifically).
- **No TPC-H-specific hardcoding** — the morsel decode is keyed on plan shape / scan size, never query identity (scaffolds + tests may be Q15-specific).
- **Codegen-tax check:** compiled-in-flag-OFF vs prior binary, 22q SF=10 A/B ≤ noise. Decode-pool code should live where it minimizes the tax (the work-queue widening is inside `emat_arrow_reader` / the bridge, not a new optimizer rule — no rule is added by this program).

- **Phase 0 — de-risk spike (GO/NO-GO).** §3. The kill-gate. Nothing below runs unless GO.
- **Phase 1 — sub-RG morsel decode kernel, unit-tested only (sibling/bridge crate).** Build the shared-work-queue morsel decoder as a pure function over a `ParquetFile` + RG/page-range list + projection, producing ordered decoded batches, **outside** DataFusion. TDD: sum-equality vs sequential decode, deterministic row order, null correctness, page-range boundary alignment across the 3 columns. Gate: kernel tests green AND a re-run of the Phase-0 Arm-B/C loop expressed through this kernel still clears §3.3 (the abstraction added no overhead).
- **Phase 2 — wire into `EmatixFastParquetExec` behind `EMAT_WS_DECODE=1`, default OFF (Shape γ).** Widen the in-tree atomic-cursor work-queue to span all RGs of all partitions as page-range morsels; add per-partition completion channels + mandatory reorder buffer + in-flight byte semaphore. **Only engage for large multi-RG scans with no `runtime_sideband`** (the same large-partition gate already at `:3205`, generalized). Gate: Q15 SF=10 A/B moves wall in the Phase-0-predicted direction; **bit-identical sums** at SF=1/10; no other query regresses past 2σ; codegen-tax check ≤ noise.
- **Phase 3 — generalize + scale.** Confirm the win on the **other f64-heavy scans** (SF=1 f64-heavy queries; Q06; any scan dominated by f64/decimal decode under a selective filter) — **the win must generalize, not be Q15-only** (R3). Confirm at **SF=100** (the win should grow with fact-table size) and **SF=1** (must not regress the small-RG case — at SF=1 lineitem is ~6 RGs / 6 partitions, so the pool degenerates; the gate guarding `:3205` must keep SF=1 on the current path if WS doesn't help there). Gate: ≥2 non-Q15 f64-scan wins or neutral; SF=100 neutral-or-better; SF=1 neutral; `tpch_validate` 22/22 at all three SF.
- **Phase 4 — default-on proposal.** Only if Phase 3 is clean across SF=1/10/100 with codegen-tax ≤ noise. Until then ships exactly like `EMAT_HASH_JOIN`: present, correct, dormant, opt-in.

---

## 6. Risks & kill criteria

- **R1 — the flat knob sweeps mean the gap may NOT be coarse-task-straggler.** Highest-priority risk. If PARTS=56 already balanced the work (all cores busy, no idle tail) and wall stayed 52 ms, sub-RG morsels change nothing and Polars' edge is per-byte decode or a tokio-vs-Polars overlap property. **Kill:** Phase 0 §3.1 instrumentation + §3.3 falsification arm. If Arm B/C ≈ Arm A, the program is **closed**; record the number and redirect (§7). This is the *expected* outcome the program must be willing to accept cheaply.
- **R2 — DataFusion's pull model resists a shared work-stealing pool.** The per-partition `execute(p)` contract wants each partition to own its decode; a shared pool with output routing + reorder buffers + a global in-flight semaphore is real machinery against the grain of the model. The PV ADR's I1/I3/I4 scar tissue (PlanProperties, L9 timing, deferred-peek) shows how brittle this surface is. **Mitigation:** Shape γ (reuse the proven in-tree atomic-cursor pattern, widened; keep `spawn_blocking`; keep partition count unchanged). **Kill:** if γ can't beat Arm A end-to-end once routing+reorder+backpressure are added (even though the *spike* did), the pull-model overhead ate the win — ship dormant, record "pull model can't host a shared decode pool cheaply," redirect.
- **R3 — only Q15 wins.** A decode change that helps only Q15's exact shape is unshippable as a generalized win (no-TPC-H-hardcoding). **Mitigation/kill:** Phase 3 requires ≥2 non-Q15 f64-scan wins; if none, ship default-OFF infra and redirect to where the gaps are larger (SF=100 / distributed).
- **R-mem — steal-order decode balloons peak RSS.** New failure mode vs the per-partition-of-8 bound (§4.5). **Mitigation:** global in-flight-byte semaphore + morsel-sized reorder windows; **gate** every phase on peak RSS not ballooning (the PV.4.1-style memory gate).
- **R-det — work-stealing breaks Q15's float determinism.** f64 SUM is order-sensitive (`DedupeAggregateForFloatDeterminism`). **Mitigation:** mandatory per-partition (RG, page, row)-order reorder buffer so the agg sees today's row order; **TDD sum-equality test before any integration** (Phase 1). **Kill:** if deterministic ordering costs back the win (reorder buffering serializes what stealing parallelized), the lever is self-defeating for f64-SUM queries — record and redirect.
- **Time-box:** Phase 0 IS the go/no-go. Do not grind Phases 1-3 if the spike doesn't clear §3.3.

---

## 7. Expected value (honest)

- **Upside (if GO):** close most of Q15's residual (the last SF=10 Polars loss) **and** establish a generalized sub-RG work-stealing decode that helps **any** f64/decimal-decode-dominated selective scan at SF=1/10/100 — a reusable scan-side primitive the engine lacks, with a clear path to grow with fact-table size. This is a broader prize than Q08's single-query materialization fix, because decode-bound selective scans are common.
- **Downside (if NO-GO):** a Phase-0 spend (≈ 3-4 days, throwaway) that **confirms the gap is unrecoverable in DataFusion's pull model** — i.e. that PARTS=56's flatness was real and Polars' edge is per-byte decode or a scheduler-overlap property tokio can't express. That is a **valid, banked outcome**: it converts "we think work-stealing would help" into "we measured that sub-RG work-stealing does/doesn't recover the gap, and why," and stops the recurring temptation to re-open this lever. Phase 0 buys that knowledge for days, not the multi-session program.
- **Do NOT declare a floor.** Polars proves the floor is lower; the program either closes the gap or learns *precisely why DataFusion's pull-based decode can't* — both are shippable conclusions. The one outcome that is **not** allowed is "we assume it's a floor without the spike."

---

## 8. Relationship to PV.4 (shelved) and the existing morsel scaffolding

- **PV.4 / `ADR_PUSH_VECTORIZED_ENGINE` targeted the join/agg TAIL** (Q08): overlap the dim build under the fact decode, and push survivors through a fused probe to kill the 60M-row intermediate + per-boundary `take`. `PV.4.0` killed it in one session: Q08 is CPU-saturated, so overlapping the build with the decode caused contention, not hiding; and the headline "−16%" was a **prebuilt-slice measurement artifact** (the dim build was excluded from the timed region). **Lesson carried forward:** the Phase-0 spike here measures the **full, real** decode path (real lineitem, real filter, real SUM, no prebuilt slices), and the program is gated on it.
- **THIS program targets the scan DECODE, not the tail.** The tail is now *proven free* for Q15 (−0.2 ms this session). Work-stealing applied to **decode** is a different and better-motivated application: the measured slack is in parallel decode efficiency (51 vs 36 ms), and Polars' own profile shows work-stealing **on decode morsels** specifically. PV.4's failure does not bear on this thesis; only its *discipline* (full-path spike, kill-gate) does.
- **Existing scaffolding to reuse:**
  - `crates/ematix-flow-push/src/morsel.rs` — the `Morsel` / `Selection` / `Column` envelope (decoded columns + deferred selection, Arc-backed, null-aware). The §4.1 row-aligned morsel maps onto this directly; the deferred-selection mechanism is exactly the masked-decode-without-materialize the SCALAR stage wants. **Note this scaffolding was built for PV.1's push pipeline and is currently dormant/uncommitted-adjacent — reuse the types, not the pipeline driver.**
  - `crates/ematix-flow-core/src/emat_arrow_reader.rs:1588` — the **proven in-tree atomic-cursor + `std::thread::scope` work-queue** over missing columns. Shape γ (§4.2) is literally "widen this cursor's domain from columns-of-one-RG to page-range-morsels-of-all-RGs, and route outputs to owning partitions." This is the single most important asset: a work-stealing decode pool that already exists in the file and is trusted in production.
  - `crates/ematix-flow-core/examples/{f64_decode_profile.rs, f64_masked_vs_dense.rs, q15_decompose_ab.rs}` — Phase-0 scaffolding (shared-ctx loop with `PARTS` override; masked-vs-dense decode harness with sum-equality assertion; the staged decompose that isolated the SCALAR stage). The spike is a fourth sibling built from these.
  - `crates/ematix-flow-core/examples/paired_ab.rs` — the interleaved-A/B-with-sign-test methodology (the one that corrected the earlier "noise" error); every gate uses it.
- **Do NOT pursue PV.4's pipeline as part of this** — it is shelved for a reason (its thesis is dead). This program reuses PV.4's *types and discipline*, not its operator.

---

## 9. References

### Code (file:line, this repo — read this session)
- Provider partition cap + RG round-robin: `crates/ematix-flow-core/src/ematix_fast_parquet.rs:2179-2184` (`num_partitions = num_rgs.min(target_partitions)`, `assignments[rg % num_partitions]`).
- Per-partition coarse decode task + bounded mpsc: `:2902-2945` (`build_partition_stream`, `spawn_blocking`, `channel(8)`), `:3145-3311` (`build_streaming_partition_stream`).
- The REV.22 reader-parallelism budget: `:2869-2877` (`computed_budget = max(1, total_threads / outer_partitions)`, `EMAT_READER_PARALLELISM_BUDGET`).
- Within-RG **column**-parallel work-queue (the Shape-γ seed): `crates/ematix-flow-core/src/emat_arrow_reader.rs:1588-1624` (atomic cursor + `std::thread::scope`, capped by `parallelism_budget`), RG-atomic `Iterator::next` `:1737-1781`, threadpool-sizing doc `:1039-1049`.
- Decode primitives to reuse unchanged: `crates/ematix-flow-core/src/ematix_parquet_bridge.rs` (`masked_decode_f64` `:821`, `open_cached` `:129`, `decode_column_chunk_f64` `:217`); `read_column_f64` (`ematix-parquet`).
- L9 sideband / deferred-peek path the pool must decline in v1: `ematix_fast_parquet.rs:2567` (no-sideband fast path) vs `:2585-2691` (deferred peek), `runtime_sideband` field `:2269`.
- Existing morsel envelope: `crates/ematix-flow-push/src/morsel.rs` (`Morsel`/`Selection`/`Column`), `lib.rs` (sibling-crate codegen rationale).
- Phase-0 scaffolding: `crates/ematix-flow-core/examples/{f64_decode_profile.rs, f64_masked_vs_dense.rs, q15_decompose_ab.rs, paired_ab.rs}`.
- Data: `examples/tpch/data/{sf1,sf10,sf100}/lineitem.parquet` (project root; SF=10 = 2.0 GB, ~57-60 RGs).

### Prior programs / ADRs
- `docs/PV4_MORSEL_PIPELINE_PROGRAM.md` — SHELVED; the join/agg-tail morsel program; `PV.4.0` kill + the prebuilt-slice-artifact lesson.
- `docs/ADR_PUSH_VECTORIZED_ENGINE.md` — PROPOSED hybrid push pipeline (tail); its I1 codegen-tax / I3 L9 / I4 PlanProperties scar tissue informs §4.4 and R2 here.

### Memory (banked, this lineage)
- `project_polars_q15_sf10_phase0_2026_06.md` — Q15 is the only Polars SF=10 loss; drift-free same-process confirmation; decode+agg decomposition direction.
- `project_q15_*` / the CSE-parallel-drain −13% ship and the SCALAR-stage isolation (this session).
- Methodology: `feedback_tdd.md`, `feedback_no_tpch_hardcoding.md`, `feedback_no_quick_reject.md`, `feedback_dig_dont_revert_sound_levers.md`; the stale-"~12×" catch (`f64_masked_vs_dense.rs` header).

---

## 10. Phase-0 RESULT (2026-06-05) — EXECUTED. Decision: **NO-GO (MARGINAL), with redirect.**

§3.1 (`ws_scan_balance.rs`) landed in red-flag **(b)** (real straggler tail at every granularity; ~16ms recoverable slack at PARTS=56) → de-risk stayed open → §3.2 spike built and run.

**§3.2 spike (`crates/ematix-flow-core/examples/ws_decode_spike.rs`)** — 3 arms, real SF=10 lineitem, 58 RGs, 14 threads, identical masked decode primitives, byte-identical SUM asserted every trial, interleaved A/C/B per trial, paired sign test, parallel-efficiency (CPU/wall) attribution. **Stable across 4 independent runs:**

| Arm | wall (median) | par-eff | vs A | sign test |
|---|---|---|---|---|
| **A** static round-robin (= production decode shape) | **46.0–46.2 ms** | 84% | — | — |
| **C** RG work-steal (shared bounded pool, whole-RG units) | **42.4–43.4 ms** | 91–92% | −6.2…−7.8% | 23–25/25, p<0.0001 |
| **B** col-split work-steal (finer than RG, masked, dynamic-enqueue) | **42.2–42.6 ms** | 92% | **−7.9…−8.2%** | **25/25, p<0.0001** |

(B vs C: −0.4…−1.8%, only 14–20/25 — **not** significant.)

### What the spike PROVED (all measured, none assumed)
1. **Work-stealing the decode works** — par-eff 84%→92%, a dead-stable −8% loop win vs the production-shaped static baseline (25/25 every run, p<0.0001). Red-flag (b), thesis **not** falsified.
2. **Finer-than-RG granularity is exhausted.** Col-split B beats whole-RG C by only −0.4…−1.8% and never significantly. So the expensive Phase-1 **page-range** kernel (even finer) would add **≤1ms** — the col-split proxy already captures the granularity benefit. (This is the clean test; the first dense Arm B was confounded by dense-vs-masked decode and was corrected to masked.)
3. **The decode is near its 14-core CPU floor.** par-eff 92% at 42.3ms ⇒ total decode CPU ≈ 545ms ⇒ perfect-14-wide floor ≈ **39 ms**. Both my best arm (42.3) and **Polars (~40 ms)** sit just above the **same** floor. *This is NOT a declared "floor we can't beat"* — it is the measured arithmetic limit for this decode on 14 cores, and Polars confirms it by landing there. The constructive-floor principle is honored: Polars (40) proved the **production** 51ms was beatable; this spike measured exactly where the beatable margin lives.

### Why NO-GO (pre-registered §3.3 MARGINAL clause)
- Best arm **42.3 ms ∈ [42, 51]** → MARGINAL → **default NO-GO**: a <18% loop win (~4 ms) that needs Shape γ (widen the cross-partition atomic cursor + per-partition completion routing + **mandatory** reorder buffer for Q15 float-determinism + in-flight byte semaphore, all against DataFusion's pull contract, R2) is not worth the multi-session spend + integration risk.
- **The spike is the BEST CASE** (decode-only, no Arrow array build, no routing/reorder/backpressure tax). In-engine Shape γ would be **slower** than 42.3 — so the realizable win is **< 4 ms**.
- **Decisive:** the spike DECOMPOSED Q15's residual and the work-stealing program does **not** close it. Production scan-decode ≈ **51 ms** (§3.1); clean-thread decode (Arm A) = **46 ms**; best work-steal (Arm B) = **42 ms**; CPU floor ≈ **39 ms**; Polars ≈ **40 ms**. So the production→Polars gap (~11 ms) = **~5 ms orchestration** (51→46: Arrow batch materialization + provider streaming + spawn_blocking/channel/CSE machinery, *not* decode parallelism) + **~4 ms decode-parallelism** (46→42: what this program recovers) + **~2 ms last-mile-to-floor** (42→40). **Work-stealing the decode targets only the middle ~4 ms and leaves the larger ~5 ms orchestration piece untouched — it cannot close Q15 alone.**

### Redirect (the banked value — §7 outcome, not an empty rejection)
The largest single recoverable chunk of Q15's residual is the **~5 ms between raw decode (46 ms) and production scan-through-DataFusion (51 ms)** — Arrow `Float64Array` batch assembly + provider streaming + orchestration. Part is genuine array materialization Polars also pays; part is DF-specific per-batch/streaming overhead. **Next probe = that orchestration/batch-assembly path** (overlaps **HJ.7** per-batch allocation reuse, "+290ms general"), which is shape-general and avoids the pull-model fight. SF=100 is unlikely to grow the *relative* work-stealing win (more RGs ⇒ static round-robin balances *better*; the par-eff ceiling is scale-invariant), so SF=100 does not rescue this program.

**Status: CLOSED at Phase 0. No source written past the kill-gate.** The spike + balance probe + this decomposition are the banked deliverable. Phases 1–4 do not run.
