# Memory budget — evidence, what shipped, and the paging arc

Status: **ElasticFloorPool landed (Σ.AI.6c, 2026-07-08)**;
**decode-pressure shedding prototype landed (Σ.AI.6d, 2026-07-08 —
opt-in `EMAT_DECODE_SHED=1`, box A/B pending)**; per-query paging
= designed, not started. Owner ask: make SF100-class workloads safe and
eventually fast on memory-tight boxes, with **bench == release** (no
bench-only settings).

## Evidence base (2026-07-08, c7i.4xlarge 32 GB, TPC-H SF=100)

| Configuration | flat suite (22q) | parted suite | Q09 isolated |
|---|---|---|---|
| Unbounded pool (shipped default) | 82.5 s | **kernel OOM-kill @ Q07** | 94.3 s (thrash) |
| Greedy cap 0.7×RAM (`EMAT_MEM_POOL_FRACTION`) | **140.2 s** (Q10 3.2→57.5 s, EBS spill) | **livelock** (loadavg 26→0, alive, no progress) | **6.58 s** (DuckDB parity 6.37) |
| SortMergeJoin, unbounded | — | — | kernel OOM-kill @ 31.7 GB RSS |
| SMJ + cap | — | — | clean `ResourcesExhausted` |
| Build-subtree swap | — | — | 109 s (harmful) |

Reading: a **blanket cap helps a cold isolated query and taxes or
deadlocks a warm suite** (DF 53 hash-join builds cannot spill; concurrent
reservations under a Greedy cap wait on each other). The **unbounded**
default is fast when RAM suffices and *lethal* when it doesn't.

## What shipped

1. `EMAT_MEM_POOL_FRACTION` — hard cap, **opt-in only** (refuted as a
   default; kept for deployments that prefer per-query errors +
   documented deadlock caveat).
2. **`ElasticFloorPool` — the default guard (Linux).** Unbounded while
   `MemAvailable` is healthy (zero tax: every banked benchmark number is
   what a default install gets); refuses an allocation only when it would
   sink `MemAvailable` below `EMAT_MEM_FLOOR_BYTES` (auto = max(1 GiB,
   3 % RAM)). Converts the kernel OOM-kill into a recoverable per-query
   `ResourcesExhausted`. No artificial mid-plenty ceiling → the cap's
   spill-tax/livelock shape cannot form: if a spillable consumer spills,
   real RAM frees and the floor un-binds.

Validation (run6, 2026-07-08, zero-override default build):

- **flat SF100 = zero tax CONFIRMED** — 89.9 s total (unbounded-regime
  spread 82.5–89.9 across runs 3/4/6), Q10 3.44 s (the refuted cap's
  57.5 s spill tax absent), Q09 42.2 s (thrash regime, floor never
  fired, correct rows).
- **parted SF100 = floor DOES NOT GUARD this path.** The box entered an
  SSH-starving thrash tar pit (>35 min, no kernel OOM-kill, no floor
  refusal) — the pressure comes from allocations the DataFusion pool
  never sees: the ematix decode buffers / rayon decode fan-out under
  the 8-part union scan are **untracked**, so `try_grow` is never asked
  and the floor has nothing to hook. Box force-terminated.

Scope conclusion: the floor guards **tracked** consumers (joins, sorts,
aggregates, repartition buffers — Linux CI pins the 1 TiB refusal) and
is still the right default; the parted decode path needs **option 3
(decode-pressure shedding)** below — the shedding hook is exactly the
missing link between MemAvailable and the untracked decode memory.
Honest guidance unchanged: distributed is the SF100-parted story on
32 GB boxes.

## The paging arc (future — the actual Q09 fix)

Goal: Q09-class joins fast under memory pressure, not merely safe.
Candidate designs, in rough order of leverage:

1. **Upstream: spillable hash join in DataFusion.** The structural fix
   (DuckDB-style partitioned hash join with per-partition spill).
   Watch/upstream-contribute; anything we build in-repo is a workaround.
2. **Per-query memory scope.** A fresh `RuntimeEnv`/pool per query with a
   budget = f(MemAvailable at query start) — bounds a single query's
   blast radius without cross-query interference. Needs a session-rebuild
   (or TaskContext override) hook at ematix's `run_query` layer; measure
   plan-cache interaction first.
3. **Decode-pressure shedding.** Status: **prototype landed (Σ.AI.6d,
   2026-07-08, opt-in `EMAT_DECODE_SHED=1`), box A/B pending.**
   Design note: instead of a `MemoryPool` shim (the pool never sees the
   decode side — run6 proved `try_grow` is simply not asked), the gate
   lives in the decode path itself: `mem_pressure::decode_gate_enter()`
   is consulted at every row-group-decode entry point (the eager
   reader's `load_row_group`, both streaming readers' `open_row_group`,
   and the legacy whole-RG bridge loop). While sensed `MemAvailable` <
   `EMAT_SHED_AVAILABLE_FRACTION` (0.10) × RAM, a `max(1, cores/4)`
   semaphore bounds concurrent RG decodes — "degrade decode parallelism
   instead of tar-pitting the box" — and the Normal→Shed transition
   clears the RG decode cache once, returning its up-to-1-GiB to the
   page cache. Healthy path pays one `OnceLock` load (disabled) or the
   25 ms-cached sensor read + one relaxed atomic load (enabled+Normal):
   no locks, no syscalls — the refuted blanket-cap's healthy-run tax is
   structurally excluded. Permits are never held across a downstream
   send (no cross-scan wait cycles). Counters `shed_gate_entries` /
   `shed_cache_drops` via `mem_pressure_metrics()`. Default stays OFF
   until the FULL 22q suite A/B on the 32 GB box decides it.

Decision gate: reassess after the next DataFusion upgrade (spillable
hash join is on their roadmap); if not landed, prototype (3) then (2),
each behind a tri-state, each validated with the FULL 22q suite on the
32 GB box (isolated A/Bs of memory levers do not transfer — proven above).
