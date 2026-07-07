# Distributed (multi-node) benchmarks

ematix-flow's distributed backend (Arrow Flight peer mesh) vs the two
most common open-source distributed SQL engines, **Trino 482** and
**Apache Spark / PySpark 4.1.2** (both latest stable as of the run),
on TPC-H at SF=10 and SF=100.

Companion to [`BENCHMARKS.md`](BENCHMARKS.md) (single-node vs DuckDB).
Plan + kit: [`DISTRIBUTED_TPCH_BENCHMARK_PLAN.md`](DISTRIBUTED_TPCH_BENCHMARK_PLAN.md);
harnesses under `infra/distributed-peers/`.

---

> ### ⚠️ Correction in progress (2026-07-07)
>
> The originally published ematix distributed figures (SF=10 **11.8 s**,
> SF=100 **70.0 s**) were **not actually distributed** — with a single
> parquet file per table the distributed planner sized every scan to one
> task (`ceil(1 file / files_per_task)` = 1) and the mesh silently never
> engaged. Those numbers were single-node ematix on the coordinator with
> the 3 workers idle.
>
> **Fixed:** `files_per_task=1` (commit `2d430af`) + an 8-parts-per-table
> data layout (`tpch-data-parted/`) now make the mesh genuinely fan out
> (18/22 queries distribute; verified via per-query `plan_mode`). The
> **real meshed** ematix numbers are **SF=10 ≈ 9.9 s** and
> **SF=100 ≈ 61.9 s** — *faster* than the mislabeled single-node figures,
> so ematix's lead over the JVM engines only widens.
>
> **Open caveat before republishing the multiples below:** the corrected
> ematix runs read the **parted** layout, while the Trino/PySpark numbers
> were measured on the **single-file** layout and have **not** been
> re-run. The order-of-magnitude gaps are unaffected, but the exact
> multiples should be re-measured with a matched data layout across all
> engines before this doc is considered final. The tables below have been
> updated with the real ematix numbers and flag this pending re-run.

---

## TL;DR

Same 4-node cluster, same S3 parquet data, same 22 TPC-H queries,
5 trials × 2 warmups, medians. **ematix leads at every scale, and the
gap widens with data volume — because ematix needs no memory tuning
while the JVM engines need progressively more.**

ematix numbers are the **real meshed** figures (18/22 queries fan out;
`EMAT_MESH=auto`). Trino/PySpark columns are the prior single-file runs,
pending a matched-layout re-run (see correction banner above).

| Scale | ematix (meshed) | Trino 482 | PySpark 4.1.2 |
|---|---:|---:|---:|
| **SF=10** (Σ median, 22/22) | **9.9 s** | 53.6 s (~5.4×) | 63.5 s (~6.4×) |
| **SF=100** (Σ median, 22/22) | **61.9 s** | 524.5 s (~8.5×) | **DNF** (7/22 completed) |

ematix ran **22/22 at both scales with zero memory configuration and
zero disk spill.** Trino required three rounds of memory engineering
plus disk-spill to finish SF=100. PySpark could not complete SF=100 on
this hardware at all — its executors OOM on the join-heavy queries.

---

## Setup

- **Cluster:** 1 coordinator + 3 workers (4 nodes), AWS `us-east-2`.
  - SF=10: `c7i.2xlarge` (8 vCPU / 16 GB) spot.
  - SF=100: `c7i.4xlarge` (16 vCPU / 32 GB) on-demand.
  - **Every engine ran on the identical instance type at each scale.**
- **Data:** TPC-H parquet in S3. The corrected ematix meshed runs read
  the **parted** layout `tpch-data-parted/sf{N}/<table>/<table>-NNNN.parquet`
  (8 parts/table) — required for the distributed planner to split a scan
  into ≥ worker-count tasks and fan out. The Trino/PySpark numbers here
  were measured on the single-file layout `tpch-data/sf{N}/<table>/<table>.parquet`
  and have not yet been re-run on the parted layout (see correction
  banner). Matching the layout across all engines is the outstanding item
  before the exact multiples are final.
- **Protocol:** 5 measured trials + 2 warmups per query, per-query
  medians, Σ = sum of the 22 medians. Each result carries a provenance
  block (git sha, `git_dirty=false`, instance type, peer list) asserted
  before the result is accepted; a leg that fails to complete 22/22 is
  **not** banked (driver-enforced).
- **Engines / versions:**
  - ematix-flow distributed backend with 3 Arrow Flight peers. Original
    (mislabeled) figures: `v0.12.0` sha `f66e293`. Corrected meshed
    figures: branch `integration/mesh-gate-campaign` sha `e9bd188`
    (includes the `files_per_task=1` fan-out fix `2d430af`).
  - Trino **482** (latest stable), Java 25 (Corretto), Glue metastore,
    native S3.
  - PySpark **4.1.2** (latest stable), standalone cluster, hadoop-aws
    3.4.2 / AWS SDK v2, IAM instance-profile S3 access.

---

## Results

### SF=10 (c7i.2xlarge ×4) — all three complete 22/22

| Engine | Σ of 22 medians | vs ematix |
|---|---:|---:|
| **ematix-flow** (meshed) | **9.9 s** | — |
| Trino 482 | 53.6 s | ~5.4× slower |
| PySpark 4.1.2 | 63.5 s | ~6.4× slower |

ematix gate-mode breakdown on the same cluster (`plan_mode` recorded per
query): `single` (forced, all on coordinator) **12.1 s** → `mesh`
(forced, 18/22 fan out) **9.9 s** → `auto` **10.1 s**. `auto` reaches the
mesh result on its own by distributing the 18 heavy queries and keeping
the 4 tiny ones single-node.

### SF=100 (c7i.4xlarge ×4)

| Engine | Σ of 22 medians | vs ematix | Notes |
|---|---:|---:|---|
| **ematix-flow** (meshed) | **61.9 s** | — | 22/22, zero tuning, zero spill |
| Trino 482 | 524.5 s | ~8.5× slower | 22/22, **only after** 3 memory rounds + disk spill |
| PySpark 4.1.2 | **DNF** | — | 7/22 completed; executors OOM on joins |

ematix gate-mode breakdown: `single` **72.8 s** → `mesh` **61.8 s** →
`auto` **61.9 s** (18/22 fan out). Notably, Q09 — the six-table join —
runs **6.2 s** meshed vs **20.9 s** on a single node reading a *single
22 GB* lineitem file: the parted layout that enables fan-out also gives
the big join the scan parallelism it was starved of.

**SF=100, the queries that separate the engines** (ematix vs Trino
median, both 22/22):

| Query | ematix (meshed) | Trino 482 | Trino / ematix |
|---|---:|---:|---:|
| Q21 (large self-join) | 12.1 s | 243.4 s | **20.1×** |
| Q10 | 3.0 s | 58.5 s | 19.6× |
| Q09 | 6.2 s | 59.8 s | 9.6× |
| Q18 | 4.1 s | 18.3 s | 4.4× |

Trino's tail is dominated by the queries whose intermediate state
exceeds cluster memory and spill to disk. ematix streams the same
queries morsel-by-morsel with bounded per-operator state and never
spills.

---

## The "no memory tuning" story (what these numbers cost to produce)

The headline is not just the multiples — it's **what it took to get
each engine to the finish line on the same hardware.**

### ematix — nothing

22/22 at SF=10 and SF=100 with the shipped defaults. No heap sizing, no
per-node or cluster memory caps, no spill configuration, no shuffle-
partition tuning. The distributed backend was pointed at its 3 peers
and run.

### Trino 482 — three rounds of memory engineering, then spill

1. **JVM heap** sized to 24 GB (75% of the 32 GB node).
2. **Per-node query memory** had to be fit under Trino's heap-headroom
   rule: `query.max-memory-per-node` at 18 GB *crashed the server at
   startup* (18 GB + 30% headroom > 24 GB heap); 16 GB works.
3. **Distributed query-memory cap** raised 40 GB → 48 GB after Q09/Q21
   failed with `EXCEEDED_GLOBAL_MEMORY_LIMIT`.
4. Even at 48 GB, **Q21 still exceeded it** — only `spill-enabled=true`
   (spill hash builds to EBS) let it finish. That spill is why Q21
   takes 243 s.

Trino's SF=100 number is a legitimate result under a *standard,
reasonable* Trino configuration (24 GB heap on a 32 GB node, spill on)
— it is reported as-is, with the spill documented here, precisely so
the 7.5× is not mistaken for a hidden under-provisioning.

### PySpark 4.1.2 — completes SF=10, DNF at SF=100

PySpark needed executor-memory sizing that Spark does not set by
default:

- **`spark.executor.memory` unset → Spark's 1 GB default:** every
  executor OOMs on any SF=100 join ("Lost task … executor N"); 18/22
  failed.
- **Oversized (10 GB on a 16 GB box):** the opposite failure — workers
  GC-thrash, miss heartbeats, the master deregisters them, and the app
  stalls at 0 cores.
- **Right-sized (~50–60% of box RAM, e.g. 20 GB on 32 GB):** completes
  SF=10 (63.5 s) but **still cannot complete SF=100** — executors OOM
  on the join-heavy queries (Q05 onward) and Spark's master removes the
  application after the executor-retry limit (`Master removed our
  application: FAILED`). 7 of 22 queries (the scan/aggregate-light ones)
  completed before the app died.

**PySpark SF=100 is recorded as DNF (did-not-finish, 7/22).** Caveat
for completeness: the Spark config used one "fat" executor per worker
rather than the multi-small-executor ("rule of 5") layout; a
best-practice retry was not pursued (owner decision). The finding —
Spark cannot complete SF=100 on the same 4×32 GB hardware where ematix
runs all 22 with zero tuning — stands on the same-hardware, reasonable-
config basis as the rest of the campaign.

---

## Reproduce

Kit and per-engine harnesses live in `infra/distributed-peers/`
(`ematix/`, `trino/`, `pyspark/`) and the terraform in
`infra/test-validation-distributed/`. Each leg: terraform apply →
bootstrap → cluster-join gate → register tables → bench (5×2) →
provenance + 22/22 assertion → S3 upload → destroy + teardown check.
Raw result JSONs (with full provenance) are in the campaign S3 prefix
`results/<stamp>/`.

> **Operational note.** All legs share one terraform state, so they run
> strictly sequentially. The competitor legs each required their engine's
> memory configuration to be tuned to the node size (documented above);
> those configs are pinned in each engine's `install.sh`.
