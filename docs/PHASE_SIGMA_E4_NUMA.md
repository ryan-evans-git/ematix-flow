# Σ.E4 — NUMA-aware execution

**Status:** scoped 2026-05-15. Sequenced after Σ.E3 dict-aware
execution, executed once Σ.B+ cluster hardware (Mac + Beelink + future
multi-socket box) is in place to actually validate the wins.

**One-line goal:** partition hot operator state (hash tables, sort
runs, aggregation accumulators) and the worker threads that operate
on it across NUMA nodes, so cross-socket memory traffic doesn't gate
single-host throughput on multi-socket boxes.

**Why now (not deferred):** the project owner has cluster hardware in
the runway. Building NUMA infra into the operator framework while
those execs are still small is much cheaper than retrofitting it
later — and Σ.E3's `DictHashAggregateExec` and `DictHashJoinExec`
land *before* Σ.E4, so the right move is to design their hash-table
layout NUMA-friendly from day one (single source of truth, single
review pass) instead of doing it twice.

## Where we are today

* **No thread-to-core affinity** anywhere. Tokio's executor and
  rayon's work-stealing pool both schedule tasks round-robin across
  whatever cores the OS hands them.
* **No NUMA-local allocation.** mimalloc is the global allocator
  (Σ.E1) but we don't use its node-aware API
  (`mi_heap_new_in_arena`). Every `Vec::new()` allocates from
  whichever node the calling thread happens to be on, which is
  effectively random.
* **No hash-table partitioning by node.** DataFusion's
  `HashAggregateExec` and `HashJoinExec` build one global table per
  partition (where "partition" is a logical compute partition, not a
  NUMA partition). On a 2-socket box that means half the probes
  read across the QPI / UPI link.
* **Single-host fan-out is the regime that bites.** On the Mac
  (single-socket M3 Pro, unified memory) the problem doesn't exist.
  On a dual-socket Xeon / EPYC server it can be 30-60% of the cost
  of hash-heavy queries (Photon paper §5.1).

## Scope

**In:**

1. **Topology discovery at startup.** New `core::topology` module wraps
   `hwloc2` (or equivalent) to enumerate NUMA nodes, cores per node,
   and the calling thread's home node. Single source of truth.
2. **NUMA-aware thread pool.** New `core::numa_pool::NumaPool` —
   one rayon-style work-stealing pool *per node*, with task affinity:
   submit work to a specific node, threads pinned to that node's
   cores via `core_affinity`. Falls back to a single global pool on
   single-node boxes (Mac, single-socket Linux) so nothing breaks
   on dev hardware.
3. **NUMA-local allocation helpers.** Thin wrapper over mimalloc's
   arena API: `numa_vec::<T>(node, capacity)` and
   `numa_alloc_arena(node, size)`. Used by the operators below.
4. **Partition operator state by node, not just by logical partition.**
   `DictHashAggregateExec` and `DictHashJoinExec` (from Σ.E3) gain a
   `node_partitions: usize` knob; the hash function picks the
   destination node before the destination thread, so every node
   builds its own subtable from rows it owns. Probes go to the
   node-local subtable first.
5. **Sort buffers + accumulators allocated on the worker's home
   node.** Add `with_alloc_node` builder on the existing sort/agg
   execs.

**Out (Σ.E5+ or never):**

* GPU offload / DPU offload — Photon doesn't do this either; out
  of scope for a Rust DataFusion-based engine.
* User-tunable NUMA policy via session config. v1 picks the partition
  count from topology; expose as config only if a workload demands
  override.
* Cross-host NUMA. Already handled by Σ.B distributed-execution.

## Concrete shape

### Σ.E4a — topology + thread pool (~1 wk)

* New crate dep: `hwloc2 = "2.x"` (Linux + macOS; Windows ungated for
  now — small minority of our deployment surface).
* `core::topology::Topology` — built once at process start; carries
  node count, cores-per-node, total cores. On single-node boxes
  (Mac, single-socket Linux) it reports `nodes = 1` and the rest of
  the framework no-ops correctly.
* `core::numa_pool::NumaPool::new(topology)` builds N rayon pools.
  Each pool's threads are pinned via `core_affinity::set_for_current`
  to the cores of that node.
* `NumaPool::spawn(node, task)` — submit a task to a specific
  node's pool. `NumaPool::spawn_local(task)` — submit to the calling
  thread's home node.
* Acceptance: smoke test `process::TopologyReport::print()` runs on
  Mac (reports `nodes=1`), on the Beelink (single-socket,
  `nodes=1`), and on a dual-socket Linux box (`nodes=2`). Behavior
  is functionally equivalent on the 1-node hosts; benchmark gains
  show up only on the 2-node host.

### Σ.E4b — NUMA-local allocation (~3 days)

* `core::numa_alloc::numa_vec_with_capacity::<T>(node, n)` — uses
  mimalloc's `mi_heap_new_in_arena` to allocate from the requested
  node's memory bank.
* `core::numa_alloc::numa_arena(node)` — returns a `'static` heap
  handle scoped to the node, usable for hash-table buckets and sort
  spills.
* Falls back to the global allocator on single-node boxes.
* Acceptance: leak-free under valgrind / heaptrack; `/proc/<pid>/numa_maps`
  on Linux shows allocations correctly distributed when the dual-
  socket box is online.

### Σ.E4c — node-partition the DictHash execs (~1 wk)

* `DictHashAggregateExec::with_node_partitions(n)` and the analogous
  call on `DictHashJoinExec`. Default = `topology.nodes()`.
* Hash function used for partition selection: `xxhash(key) >> shift`
  picks the node first, then the standard intra-node partition picks
  the thread. Two levels of partitioning, same hash.
* Per-node subtables built and stored in node-local memory via
  Σ.E4b's helpers; per-node threads (Σ.E4a's pool) own their own
  partition's build + probe.
* Acceptance: TPC-H Q01, Q05, Q09, Q18 on a 2-socket box show
  ≥1.4× speedup vs the global-table baseline at SF=10. Mac /
  single-socket runs are within ±5% noise (graceful degradation).

### Σ.E4d — sort + agg accumulator on home node (~3 days)

* `SortExec` and `HashAggregateExec` (non-dict variants) get the
  same `with_alloc_node` builder. Limited to the obvious cases:
  per-partition sort runs and per-thread agg accumulators.
* Acceptance: no regression on single-node boxes; whatever the
  2-socket bench says is the answer.

## Implementation surface

```
crates/ematix-flow-core/src/topology/mod.rs           (~150 loc)
crates/ematix-flow-core/src/topology/hwloc_inner.rs   (~200 loc)
crates/ematix-flow-core/src/numa_pool/mod.rs          (~300 loc)
crates/ematix-flow-core/src/numa_alloc/mod.rs         (~200 loc)
```

Touches:

* `crates/ematix-flow-core/src/dict_exec/aggregate.rs` (Σ.E3b output) — add
  `node_partitions` field + two-level hash.
* `crates/ematix-flow-core/src/dict_exec/join.rs` (Σ.E3c output) — same.
* `crates/ematix-flow-core/src/lib.rs` — initialize `Topology` once
  at first `SessionContext` creation; thread the handle through the
  Σ.E3 execs.

## Acceptance criteria for the phase as a whole

1. **Topology reports correctly** on every box we can test: Mac M3
   Pro (1 node), Beelink mini-PC (1 node), and the multi-socket box
   when it arrives (≥2 nodes). Reports go into structured logs the
   first time `SessionContext::new()` runs.
2. **Functionally a no-op on single-node boxes.** SF=1 + SF=10 TPC-H
   bench results on the Mac are within ±5% of the pre-Σ.E4 baseline.
   No new losses.
3. **Wins on multi-socket.** On the future dual-socket box, ≥3 of
   Q01/Q05/Q09/Q14/Q18 show ≥1.4× over the pre-Σ.E4 baseline; geomean
   improvement ≥1.2× across the 22-query suite.
4. **Memory accounting.** `/proc/<pid>/numa_maps` (Linux) or
   equivalent macOS tooling shows hash-table state distributed
   across nodes, not concentrated on one.
5. **Documented operator knobs.** README + USER_GUIDE entry explains
   what NUMA-aware mode does, when it turns on, and how to override
   `node_partitions`.

## Risks + open questions

* **hwloc on macOS.** `hwloc2`'s macOS support has historically been
  rough around the M-series chips. Worst case: feature-gate Linux-
  only, accept that NUMA mode is no-op on macOS (which is correct
  anyway — M-series unified memory). Plan B: pure `sysctl`-based
  topology probe for macOS (kept simple — it only ever reports 1
  node).
* **Tokio integration.** Our streaming side runs on Tokio, not rayon.
  Σ.E4's NumaPool is rayon-shaped; the batch execs use rayon already
  (via `tokio::task::spawn_blocking` + the rayon pool). Σ.E4 doesn't
  touch the Tokio side. If a future workload needs NUMA-pinned
  streaming, that's a Σ.E5 / Σ.F follow-up.
* **Datafusion-distributed interaction.** When `engine = "distributed"`
  is in play, each Arrow Flight worker is its own process — NUMA
  affinity within that worker is exactly Σ.E4's territory.
  Cross-worker / cross-host distribution is already handled by Σ.B
  and is orthogonal.
* **Test hardware risk.** The 2-socket validation hardware doesn't
  exist yet. Σ.E4a/b/c can be **built** on single-socket boxes (the
  graceful-degradation tests prove the wiring is correct); the win
  measurements wait for the multi-socket arrival. Don't gate the
  build on the hardware — gate the *bench claim* on it.

## Effort + sequencing

* **Σ.E4a** topology + thread pool: ~1 week
* **Σ.E4b** NUMA-local alloc helpers: ~3 days
* **Σ.E4c** node-partitioned DictHash execs: ~1 week (extends Σ.E3
  output — must land *after* Σ.E3b/c so we change one set of execs,
  not two)
* **Σ.E4d** sort + agg accumulator pinning: ~3 days
* **Total:** ~3 weeks single-developer, sequenced after Σ.E3
  (~5 weeks). Combined Σ.E3 + Σ.E4 runway is ~2 months.

## What this is not

* **Not** automatic NUMA balancing at the kernel level (that's
  `numactl --balanced`, which we shouldn't override). We only place
  *our* state; the kernel still owns scheduling decisions for
  everything else.
* **Not** a Photon clone — Photon does NUMA as part of a much larger
  C++ engine architecture. We pick the operator-state-pinning idea
  that maps onto DataFusion's `ExecutionPlan` extension points.
* **Not** distributed scheduling. Σ.B already handles cross-host;
  Σ.E4 is purely within-host.
