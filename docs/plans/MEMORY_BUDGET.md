# Memory budget — evidence, what shipped, and the paging arc

Status: **ElasticFloorPool landed (Σ.AI.6c, 2026-07-08)**; per-query paging
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

Expected SF100 behavior on the 32 GB box under the shipped default:
flat = unchanged (82.5 s); parted = Q07(±Q09/Q21) fail individually with
a clear error naming the floor, the other queries bank, the process
survives. The honest single-node guidance stays "use distributed for
SF100 parted on 32 GB" — now enforced gracefully instead of by kernel.

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
3. **Build-side paging shim.** A `MemoryPool` that, when the floor
   approaches, triggers ematix-side mitigation (drop decode caches,
   shrink RG-decode fan-out via the partition registry) before refusing —
   "degrade decode parallelism instead of failing the join". Cheap,
   composable with (1).

Decision gate: reassess after the next DataFusion upgrade (spillable
hash join is on their roadmap); if not landed, prototype (3) then (2),
each behind a tri-state, each validated with the FULL 22q suite on the
32 GB box (isolated A/Bs of memory levers do not transfer — proven above).
