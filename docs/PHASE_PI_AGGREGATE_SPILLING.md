# Π — Aggregate (and join) spilling

**Goal:** ematix-flow doesn't OOM when the hash table doesn't fit
in memory. Spill to local SSD by default; S3 as a backstop via the
existing `LambdaExecutor` / `S3RunLog` plumbing.

**Status:** stub. Will be promoted to a full plan after Σ.G + Φ.1
have landed and we have a clean baseline.

## Why it's its own phase

- Production correctness: any user with a SF=100+ workload hits OOM
  on aggregate hash tables today.
- Independent of Σ.G + Φ: spilling is a runtime infrastructure
  concern, not a kernel or planner concern. Different engineer can
  work on it in parallel.
- Different test surface: needs fault-injection harness (kill the
  process mid-bucket-flush, recover, verify result), not microbenches.

## Scope (rough)

| Sub-phase | What | Effort |
|---|---|---|
| Π.1 | Spill-aware `AggregateExec`: external sort + run merging to local SSD when hash size > threshold | ~2 wk |
| Π.2 | Same for hash join | ~2 wk |
| Π.3 | S3 backstop for the "local SSD ran out too" case — chunk the spill to S3 with multipart upload, reuse the [[S3RunLog]] pattern | ~2 wk |
| Π.4 | AQE-style retry: if a hash exec spills, restart with higher partition count and skip spilling | ~3 wk |

Total: **~9 wk** for the full surface.

## Non-goals

- **Distributed shuffle spilling** — overlaps with the Phase Z
  distributed campaign. That work owns network-side spilling; this
  phase owns local SSD + S3 spilling for single-host workers.
- **DataFusion's `MemoryReservation` quota system** — DF already has
  one. We integrate, not replace.

## Gate

- TPC-H SF=100 (~75 GB) runs to completion on a 16 GB t3.xlarge
  without OOM
- Comparison: how does our spilling overhead compare to DuckDB and
  Spark at SF=100? (Industry knows Spark's spilling is heavyweight,
  DuckDB's is competitive)

## What we don't know yet

- The right threshold for spill decision (memory pressure %? hash
  cardinality estimate?)
- Whether we need our own spill format or can reuse Arrow IPC
- How aggressive the AQE retry should be

These get scoped in the planning doc proper.

## Sequencing

- **Prerequisites:** none — independent of Σ.G + Φ. Could start any
  time after the current AWS campaign closes out.
- **Why we defer:** Σ.G + Φ deliver visible wins on existing TPC-H
  numbers, which the team needs to validate direction. Π pays off
  on workloads we don't currently benchmark (SF=100+). Lower
  immediate signal-to-effort.
