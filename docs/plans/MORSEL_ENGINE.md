# Morsel-driven execution spine — closing the decode parallelism gap

*Multi-month program, greenlit 2026-06-20. Goal: convert "we own the fastest
per-thread decode kernel" into "we own the fastest wall-clock" by fixing core
utilization during decode. This is the "own the execution model" lever from the
fastest-theoretical analysis — the highest-ceiling remaining architectural work.*

## Success criterion
- **Apples-to-apples** (every engine reads the same file): ematix ≥ Polars on the
  decode-bound queries (Q06/Q14/Q15) at SF=10 — the flip the codec lever
  provably *couldn't* deliver (see [[project_parquet_writer_phase0]]: Polars
  banks more from a cheaper codec purely because it schedules decode better).
- **Q06 pure-decode efficiency** → near the realistic heterogeneous-core ceiling
  (today ~66%; target ≥85%).
- No regression on the 22q geomean; correctness 22/22.

## Phase-0 attribution (measured 2026-06-20 — this drives the design)

`stage_profiler` + `sample` + a partition sweep on SF=10, decode-bound queries:

| Finding | Evidence | Implication |
|---|---|---|
| **Decode dominates** | Q06 100% / Q14 90% of operator compute is the lineitem scan | High Amdahl ceiling — fixing decode parallelism is worth it |
| **Parallel efficiency is low** | Q06 7.1×, Q14 10.2× on a 14-core box | 25–50% of core throughput unused vs nominal 14× |
| **NOT E-core stragglers** | partition sweep 8→28 = 63/52/48/32/31/30 ms — *more* partitions help, *fewer* hurt | refutes the 14-equal-split-on-10P+4E straggler guess |
| **Coarse-granularity imbalance** | monotone gain with finer partitions, plateau ~28 (~+7% over 14) | the gap is load-balance + per-partition overhead, reclaimable by finer work units |
| **NOT memory-bandwidth-bound** | Q06 ≈670 MB materialized → ~1.7 ms BW floor at 400 GB/s, but runs 32 ms (~19× above floor) | decode is **compute-bound** → more parallelism genuinely helps (project isn't doomed by physics) |
| **Q15 is metrics-blind** | `SharedSubtreeExec` hides decode; Σ elapsed_compute = 1.99 ms vs 63 ms wall | CSE caching hides the work; sample/instrumentation needed, not DF metrics |
| **Workers mostly parked** | `sample` Q06 loop: ~78% thread-time in park/sync | corroborates underutilization (exact % conflated by fresh-ctx-per-trial) |

**Net:** decode is compute-bound and decode-dominated, so the parallelism gap is
real and reclaimable — but **bounded**. Realistic ceiling on 10 P + 4 E cores is
~12× a single P-core (≈21 ms ideal for Q06); we're at 32 ms ≈ **66% of the
realistic ceiling**, ~70% when oversubscribed. So the prize on decode-bound
queries is **~20–40%**, not 2× — enough to flip the Q14/Q15 Polars losses
(gaps are 3–12%) and lift every decode-bound query, but size the build to that.

This also reconciles the prior "work-steal kill-gated at ~10%" result: bolting
work-stealing onto the coarse-partition model captured the easy ~10% (same as
oversubscription here); the deeper ~20–30% needs the per-partition *overhead*
removed, which only a real morsel engine does.

## P1 RESULTS — measured 2026-06-20 (per-core decode trace). KILL-GATE PASSES.

Built an env-gated per-RG decode trace (`crates/ematix-flow-core/src/morsel_trace.rs`
+ `examples/morsel_trace_run.rs`, hooked at the `load_row_group` chokepoint;
analyzer `/tmp/morsel_trace/analyze.py`). Each event = (thread, rg, rows, cols,
start_ns, end_ns) → a real per-core busy/idle timeline. SF=10, preset rules,
all output partitions driven concurrently, warmups + last-trial traced.

| Query | class | cpu/wall (of 14) | reclaimable¹ | finding |
|---|---|---|---|---|
| **Q06** | decode-bound | **4.5–7.8×** | **44–68%** | 8 of 14 cores idle after ~5ms; active workers flat-out (gaps <7%) → **idle cores, not backpressure** |
| **Q14** | decode-bound | 10.1× (of 16) | 37% | 14 active, gaps 0.1% → same: idle cores |
| **Q01** | **agg-bound** | 4.0× | n/a | all 14 active but ~28% busy — **164 ms of inter-RG gaps = decode blocked on the output channel** (agg can't drain). Not a decode problem. |
| **Q15** | — | 0 events | — | decode bypasses `load_row_group` (inline/page-streaming reader or CSE SharedSubtree) — the "metrics-blind" note, surfaced. P5 must instrument those paths. |

¹ reclaimable = (decode_wall − total_busy/n_threads)/decode_wall.

**Mechanism (pinned, not inferred):** decode is parallelised purely by N output
partitions (`computed_budget = max(1, cores/partitions) = 1` at the default p14 →
no column-level parallelism, no oversubscription). Row groups are assigned to
partitions by **static round-robin `rg % N` at plan time, blind to per-RG decode
cost.** Per-RG cost is highly non-uniform (Q06's filter prunes ~60% of RGs to
~0.002 ms; surviving heavy RGs cost 6–9 ms). The per-RG dump shows the heavy RGs
arrive in runs that **alias onto the same few partition residues** → 23 heavy RGs
pile onto ~5 of 14 threads. The wall is set by the unluckiest thread.

**Static partition count cannot fix it** (the down-payment ceiling): Q06
p14→p28→p56 wall = 37→34→28 ms while the balance ceiling drops 11.2→9.6→4.1 ms —
at p56 we are **85% off the ceiling** and cpu/wall is *still* ~7.5×.
Oversubscribing partitions spreads heavy RGs a little but the OS is not an
RG-granularity work-stealing scheduler. **Only dynamic work-stealing reaches the
ceiling.** (This also revises the down-payment estimate down — partition
oversubscription is weaker than the ~7–10% the plan assumed.)

**This revises Phase-0 in two ways:** (a) efficiency is *worse* than the "66%"
estimate — true cpu/wall is 4.5–10× = **32–64%**; (b) the prize is *bigger* than
"bounded 20–40%" — Q06 is **44–68% reclaimable (1.8–3×)**, Q14 37%. The bound
was conservative because Phase-0 measured aggregate compute/wall, not the per-core
timeline; the imbalance is severe and structural.

**Two failure modes → two parts of the architecture, confirmed needed:**
- *Decode-bound + imbalanced* (Q06, Q14, Q19) → **P3 work-stealing scheduler**:
  idle workers steal heavy RGs. Captures most of the prize on its own. Work-
  stealing is agnostic to *why* per-RG cost varies, so it generalises.
- *Agg-bound + backpressured* (Q01) → **P4 fused decode→agg pipeline**: remove the
  bounded channel + per-batch handoff so a slow consumer can't throttle decode.
  Work-stealing decode alone does nothing for Q01.

**KILL-GATE: PASS.** Idle ≫10%, attributable (cost-blind static assignment),
reclaimable (dynamic work-stealing over RG/morsel units). Heterogeneity is a
*minor* factor (active workers homogeneous per-RG, ~1.2× spread, not 2×) →
core-class sizing is lower priority than the plan implied; **dynamic assignment is
the dominant lever, not sub-RG fineness.** Proceed to P2 (the work-stealing
spike) to convert "should reach the ceiling" into "measured to."

## P2 RESULT — work-stealing spike: balances cores but CONTENTION-LIMITED. Premise revised.

Built the real lever (not a model): `SharedRgCursor` — one shared lock-free
queue of row groups that every partition decode stream pulls from, replacing
static `rg % N`, forcing the eager reader so each RG decodes once. Gated
`EMAT_MORSEL_STEAL=1`, default-off, committed as opt-in infra (d78a62d).
**Correctness: tpch_validate 9/9 vs DuckDB at SF=10**, incl. multi-join
Q05/Q08/Q18 (each scan owns its cursor) and 114K-row Q03.

| measurement | baseline (rg%N) | STEAL (shared cursor) |
|---|---|---|
| Q06 cpu/wall (of 14) | 4.5–7.8× | **11.5×** (all cores active) |
| Q06 finish spread | 3 → 35 ms (8 idle) | 23 → 34 ms (balanced) |
| Q06 wall | ~36 ms | **~35 ms (NEUTRAL)** |
| Q14 wall | 88 ms | **82 ms (~7%)** |

**It balances the cores exactly as designed — and the wall barely moves.** The
reason is the killer: a **contention tax**. Concurrency sweep (steal, vary
threads):

```
threads:        4     6     8     10    14
wall_ms:        88    64    63    50    45
total_busy_ms:  339   361   441   454   524   ← grows +54% as threads 4→14
```

`total_busy` is **not invariant** under redistribution — per-RG decode gets
~1.5× slower as more threads decode concurrently. So spreading 5→14 cores trades
per-core throughput for parallelism at ~break-even. **Decode at SF=10 is
contention-limited, not idle-limited.** P1's "44–68% reclaimable" was an artifact
of the balance-ceiling metric (`total_busy/N`) assuming total work is conserved.

**Contention source (sample profile, baseline vs steal):** `malloc`/`mi_*` ≈ 0
(**not** allocator/lock contention → per-thread arenas won't help); `memcpy/memset`
+27%, `snappy` +20%. The tax is **memory-subsystem traffic in the Snappy
decompress itself** (Snappy decode is inherently memcpy-heavy: literal runs +
back-references). This matches the standing decode finding ([[project_rev20_q07_q08_decode_bound]]:
"97% decompress / 3% materialize") — the concurrent decompress memory traffic is
the wall, and it's the codec's nature, not a separable copy we can delete.

**This independently re-derives the prior PV.M.5 "work-steal kill-gated ~10%"**
— now with the mechanism. Neither P3 (work-stealing) nor P4 (fuse decode→agg,
which removes the channel but not the decompress) addresses the decode contention.

### Revised verdict (2026-06-20)
The morsel engine's **core premise — reclaim decode parallel inefficiency by
balancing cores — does NOT hold at SF=10.** The inefficiency is contention
(codec memory traffic), not reclaimable idle. The realized prize is ~7–10%
(matching the independent prior result), not the hoped 20–40%.

**What this does NOT kill:** (1) the ~7% on Q14-class decode-bound queries is
real and *might* flip the narrow SF=10 Polars losses (3–12%) — worth a focused
apples-to-apples check before discarding; (2) the agg-bound class (Q01,
backpressure) is a *separate* bottleneck the fused pipeline could still help; (3)
the don't-declare-a-floor rule: Polars IS faster on Q14/Q15, so the floor is
lower — but the lever to reach it is **per-thread decode memory-efficiency or
codec**, not core-balancing. The next honest step is to **profile Polars'
decode directly** (threads × per-thread throughput × memory traffic on the
identical file) to decide whether a per-thread decode lever exists, rather than
build a 3-month scheduler the spike shows yields ~7%.

**Recommendation:** do NOT proceed to the full P3→P6 build on the core-balancing
premise. Either (a) pivot the decode effort to per-thread memory-traffic
reduction (decoder kernel, profile-the-competitor first), or (b) redirect to the
non-decode campaign levers (plan/join/agg) where wins are not contention-capped.
Decision is the user's — this is the de-risk doing its job (found cheaply, at P2,
that the big build won't pay).

## The critical fork — verify BEFORE the big build (Phase 1)
Phase-0 strongly indicates scheduling-reclaimable, but two things must be nailed
before committing months, because each has a different fix:
1. **Where does the residual idle come from** — load imbalance (tail), per-batch
   handoff/allocation overhead, or runtime (tokio park/wake) latency? → a real
   **per-core busy/idle timeline** of Q06 + Q15 (not DF metrics; an off-CPU or
   manual span trace). This sets the build order.
2. **Confirm the heterogeneous-core ceiling** — measure per-partition finish
   times pinned to P vs E cores. If E-cores cap aggregate throughput more than
   modeled, the morsel scheduler must be core-class-aware (give E-cores smaller
   morsels), not just work-stealing.

**Kill-gate:** if the per-core trace shows decode already saturates the realistic
ceiling (idle is <~10% and not reclaimable), STOP — the remaining lever is
*fewer bytes* (pushdown/format), not scheduling. Phase-0 says we're at ~66%, so
this is unlikely, but prove it before building.

## Target architecture — morsel-driven, thread-per-core, work-stealing region
The HyPer/DuckDB/Velox model, contained behind one DataFusion `ExecutionPlan`
node so we don't fork DF's whole engine:

- **Morsel source**: the scan emits *sub-row-group* decode units (page-ranges)
  into a shared work queue, not 14 coarse partition streams. Our decoder is
  already page-granular (the PageWalker), so morsels can be small + cheap.
- **Thread-per-core pinned pool + work-stealing deque** (crossbeam-deque /
  rayon-style): a worker that finishes a morsel grabs the next. Fine units →
  no tail straggler; core-class-aware sizing → E-cores take smaller morsels.
- **Fused pipeline per morsel**: decode→filter→partial-agg run to completion on
  each morsel, *no `RecordBatch` materialized between operators* (the push
  spine). Removes the per-partition stream + per-batch handoff overhead that
  oversubscription couldn't.
- **Per-thread partial agg state, merged at the end** — the `CombineAgg`
  pattern we already have.
- **No tokio in the hot decode path** — synchronous, cache-friendly. The region
  node bridges back to DF's async stream only at its output boundary.

This attacks every Phase-0 mechanism at once: fine morsels (imbalance), no
per-partition stream (overhead), core-class sizing (heterogeneity), sync pool
(runtime latency).

## What exists vs. net-new
**Have** (the payoff of the earlier inventory):
- `ematix-flow-push` morsel kernel — `Morsel`/`Column`/`Selection`/`PushOperator`/`Sink`/`run_pipeline`.
- `EmatPushPipelineExec` — a fused scan→subtree node (today consumes Arrow batches; gate `EMAT_PUSH_PIPELINE`).
- `CombineAggExec` / `I64SumF64` — per-thread partial + parallel merge.
- Page-granular decoder (`emat_arrow_reader` PageWalker); fused filter+agg execs (Q1/Q6).

**Net-new:**
1. **Shared morsel source** over parquet row groups → page-range units (not partition streams). The decoder must expose "decode this page-range" (it decodes per-page internally; needs a dispatchable API).
2. **Thread-per-core work-stealing scheduler** (crossbeam-deque, pinned, core-class-aware). This is the part PV.M.5 attempted partially on the wrong (coarse) granularity.
3. **`MorselRegionExec`** — extend `EmatPushPipelineExec` into a node that swallows the whole decode→filter→agg subtree, runs it on the scheduler internally, emits only final batches. Optimizer rule detects the fusable region (we have the rule scaffold).
4. **Core-class-aware morsel sizing** (M4-Max-specific; generalizes to any P/E or NUMA topology).

## Phased plan (each phase gated)
- **P1 — De-risk (days, no engine code):** per-core busy/idle trace of Q06+Q15; per-partition P/E finish times; confirm compute-bound + quantify reclaimable headroom. Kill-gate above.
- **P2 — Morsel source (1–2 wk):** decoder page-range dispatch API + a shared queue; keep the existing scheduler. Measure: does fine-grained pull alone move Q06? (expect the ~10% oversubscription-class win.)
- **P3 — Thread-per-core work-stealing scheduler (3–5 wk):** crossbeam pool, pinned, core-class sizing; run the morsel source on it for the scan only. Measure Q06 efficiency → target ≥85%.
- **P4 — Fuse decode→filter→agg in the region (3–6 wk):** no inter-operator RecordBatch; per-thread partial agg → CombineAgg merge. `MorselRegionExec` + rule. Measure Q06/Q14.
- **P5 — Q15 + CSE (2–4 wk):** make the CSE'd revenue subtree a morsel region (the metrics-blind one); ensure the dedup/shared-subtree path participates.
- **P6 — Gate + flip (1–2 wk):** apples-to-apples vs Polars on Q06/Q14/Q15; 22q SF=10/SF=100 regression; correctness 22/22; default-on decision.

## Risks & containment
- **Bypassing DF's pull/async model** → contained behind one `ExecutionPlan` node (proven pattern: `EmatixHashJoinExec` already owns its own threads). The rest of the plan stays vanilla DF.
- **Heterogeneous-core ceiling** → core-class-aware sizing; P1 quantifies the ceiling so we don't over-promise.
- **Bounded prize (~20–40%)** → size effort to it; the win is the Q14/Q15 flips + an all-decode-bound-query lift + the foundation the SF=100 and fused-pipeline work both need. Not a 2× headline.
- **Down-payment option:** shape-gated partition oversubscription (~2×cores on decode-bound scans) banks ~7–10% now via an env/rule while P2–P4 build the real thing. (Explored before as Σ.AN/Σ.AΩ — query-dependent, hence opt-in; revisit as a gated stopgap.)

## Provenance
Phase-0 logs: `stage_profiler` Q06/Q14/Q15, `/tmp/q6_sample.txt`, partition
sweep above. Supersedes the codec/decode-floor line of attack
([[project_parquet_writer_phase0]] — kernel is at floor; the gap is scheduling).
