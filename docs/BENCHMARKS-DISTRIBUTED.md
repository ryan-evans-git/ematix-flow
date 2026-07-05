# Distributed (multi-node) benchmarks

ematix-flow's distributed backend (Arrow Flight peer mesh) vs the two
most common open-source distributed SQL engines, **Trino 482** and
**Apache Spark / PySpark 4.1.2** (both latest stable as of the run),
on TPC-H at SF=10 and SF=100.

Companion to [`BENCHMARKS.md`](BENCHMARKS.md) (single-node vs DuckDB).
Plan + kit: [`DISTRIBUTED_TPCH_BENCHMARK_PLAN.md`](DISTRIBUTED_TPCH_BENCHMARK_PLAN.md);
harnesses under `infra/distributed-peers/`.

---

## TL;DR

Same 4-node cluster, same S3 parquet data, same 22 TPC-H queries,
5 trials × 2 warmups, medians. **ematix leads at every scale, and the
gap widens with data volume — because ematix needs no memory tuning
while the JVM engines need progressively more.**

| Scale | ematix | Trino 482 | PySpark 4.1.2 |
|---|---:|---:|---:|
| **SF=10** (Σ median, 22/22) | **11.8 s** | 53.6 s (4.5×) | 63.5 s (5.4×) |
| **SF=100** (Σ median, 22/22) | **70.0 s** | 524.5 s (7.5×) | **DNF** (7/22 completed) |

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
- **Data:** one canonical TPC-H parquet copy in S3
  (`s3://<bucket>/tpch-data/sf{N}/<table>/<table>.parquet`), read by all
  three engines. No per-engine data massaging beyond the directory
  layout Hive/Glue requires (Spark and ematix read the same files).
- **Protocol:** 5 measured trials + 2 warmups per query, per-query
  medians, Σ = sum of the 22 medians. Each result carries a provenance
  block (git sha, `git_dirty=false`, instance type, peer list) asserted
  before the result is accepted; a leg that fails to complete 22/22 is
  **not** banked (driver-enforced).
- **Engines / versions:**
  - ematix-flow `v0.12.0` (sha `f66e293`), distributed backend with 3
    Arrow Flight peers.
  - Trino **482** (latest stable), Java 25 (Corretto), Glue metastore,
    native S3.
  - PySpark **4.1.2** (latest stable), standalone cluster, hadoop-aws
    3.4.2 / AWS SDK v2, IAM instance-profile S3 access.

---

## Results

### SF=10 (c7i.2xlarge ×4) — all three complete 22/22

| Engine | Σ of 22 medians | vs ematix |
|---|---:|---:|
| **ematix-flow** | **11.8 s** | — |
| Trino 482 | 53.6 s | 4.5× slower |
| PySpark 4.1.2 | 63.5 s | 5.4× slower |

### SF=100 (c7i.4xlarge ×4)

| Engine | Σ of 22 medians | vs ematix | Notes |
|---|---:|---:|---|
| **ematix-flow** | **70.0 s** | — | 22/22, zero tuning, zero spill |
| Trino 482 | 524.5 s | 7.5× slower | 22/22, **only after** 3 memory rounds + disk spill |
| PySpark 4.1.2 | **DNF** | — | 7/22 completed; executors OOM on joins |

**SF=100, the queries that separate the engines** (ematix vs Trino
median, both 22/22):

| Query | ematix | Trino 482 | Trino / ematix |
|---|---:|---:|---:|
| Q21 (large self-join) | 12.5 s | 243.4 s | **19.5×** |
| Q10 | 3.8 s | 58.5 s | 15.6× |
| Q09 | 6.5 s | 59.8 s | 9.2× |
| Q18 | 6.1 s | 18.3 s | 3.0× |

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
