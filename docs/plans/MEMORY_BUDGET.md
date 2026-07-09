# Memory budget — evidence, what shipped, and the paging arc

Status: **ElasticFloorPool landed (Σ.AI.6c, 2026-07-08)**;
**decode-pressure shedding prototype landed (Σ.AI.6d, 2026-07-08 —
opt-in `EMAT_DECODE_SHED=1`, box A/B pending)**; **decode-cache
retention landed (Σ.AI.6e, 2026-07-08 — `EMAT_RG_CACHE_RETENTION`
AUTO = ON, box A/B pending, option 4 below; Σ.AI.6f, 2026-07-09 —
policy-freeze fix, ghost-assisted demotion, see option 4)**; per-query paging
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

4. **Decode-cache retention.** Status: **landed (Σ.AI.6e, 2026-07-08,
   `EMAT_RG_CACHE_RETENTION` tri-state, AUTO = ON), box A/B pending.**
   The settled Q09 SF=100 mechanism (2026-07-09 diagnostics — replaced
   two earlier wrong theories): on the 32 GB box Q09 runs **6.5 s when
   the RG decode cache serves its working set** and 16–50 s
   (IO/decode-bound) when it doesn't — DuckDB's steady state is 6.4 s,
   so cache retention IS the whole gap. Observed pattern (fresh
   process, warmup + 6 trials, page cache dropped first): trials 1–2
   fast (~6.5 s, warmup-seeded hits), trial 3+ collapse (eviction),
   then oscillation; with `EMAT_RG_DECODE_CACHE=0` no fast trials at
   all ([26, 16, 49, …] s) — proving the fast trials were the cache.
   Root cause confirmed in code: the eviction was pure FIFO (`get`
   never reordered), so a scan whose insert traffic exceeds capacity
   evicts the seeded working set in insertion order *even while it is
   being hit* — steady-state hit rate collapses to ~0 although the
   set fits with room to spare right after seeding.
   Design: segmented LRU with **admission-on-second-touch** — new
   entries land in a probationary segment; only a second touch
   promotes to the protected segment (≤ 4/5 of capacity, LRU overflow
   demotes back to probation); eviction takes probation-LRU victims
   first and touches protected only when probation is dry. Sequential
   one-pass floods die in probation and cannot displace the re-touched
   working set. O(1) amortised per op (lazy stamp-validated queues,
   no per-op allocation); legacy FIFO preserved bit-exact behind
   `EMAT_RG_CACHE_RETENTION=0`. Deterministic unit benches (hit
   rates): 2×-capacity flood hot-set survival 0/16 → 16/16; Q09-shaped
   seed+re-stream steady state 0/40 → 40/40 (FIFO reproduces the
   fast-fast-collapse curve [40, 40, 0, 0, 0, 0]); LRU-friendly loop
   exact parity (no regression). Probes: `retention_stats()` =
   (admits, rejects, protected_evictions). AUTO ships ON on the unit
   evidence; the FULL 22q suite A/B on the 32 GB box makes the final
   call, same rule as (3).

   **Σ.AI.6f (2026-07-09) — policy freeze found and fixed.** Per-query
   field counters on the AWS SF=100 box (logs banked) showed that once
   the cache fills (~1 GiB), the Σ.AI.6e layout FREEZES for the rest
   of the process: `retention(admit+0 … prot_evict+0)` on virtually
   every subsequent query while rejects count tens of thousands.
   Mechanism (a policy deadlock, not a code bug): protected only
   demotes on promotion overflow; promotion requires a probation
   second touch; effective probation (capacity − protected ≈ 1/5) is
   smaller than later queries' re-touch distance, so no second touches
   ever happen → no promotions → no demotions — whoever filled
   protected first owns it forever (order-lucky poisoning). Fix:
   ghost-assisted adaptive demotion (ARC-lite). Keys evicted from
   probation WITHOUT promotion go to a bounded keys-only ghost list
   (cap = max(live entries, 64)); a miss on a ghosted key is proof a
   live working set re-touches beyond probation's reach and demotes
   protected-LRU entries that are *stale* (untouched since that key's
   eviction) to probation-MRU. The staleness gate preserves every
   Σ.AI.6e guarantee: one-touch floods never re-request their keys
   (no ghost hits → protected untouched), and a still-hitting
   protected set carries fresh stamps (never demoted). Unit evidence:
   poisoning repro (48-entry stale protected set vs a 32-entry live
   set with re-touch distance > probation) goes from
   [0,0,0,0,0,0,0,0] hits/pass (frozen, forever) to
   [0, 0, 25, 32, 32, 32, 32, 32] — full residency by pass 4 while
   the stale set fully drains (48/48 ghost demotions); flood
   hot-set survival, Q09-shaped, and LRU-parity benches unchanged.
   New probe: `retention_ghost_stats()` = (ghost_hits,
   ghost_demotions); `EMAT_RG_CACHE_RETENTION=0` legacy FIFO stays
   bit-exact.

   **Honest scope note:** the ghost fix removes the *policy* deadlock —
   cross-query cache utility no longer depends on arrival order. It
   does NOT make SF100 big-query caching win: the big queries' decoded
   working sets are structurally capacity-bound (they exceed the 1 GiB
   cap regardless of layout), and the remaining Q09 gap belongs to the
   spillable-join arc (decision gate below), not to retention tuning.

Decision gate: reassess after the next DataFusion upgrade (spillable
hash join is on their roadmap); if not landed, prototype (3) then (2),
each behind a tri-state, each validated with the FULL 22q suite on the
32 GB box (isolated A/Bs of memory levers do not transfer — proven above).
