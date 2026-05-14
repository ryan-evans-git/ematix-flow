# Phase Σ.B+ — Cross-host validation of the distributed batch SQL backend

Status: draft (2026-05-11). Extension of the already-shipped Σ.B
(see `docs/PHASE_SIGMA_PLAN.md`). Not a new Σ letter — this is the
deferred-from-Σ.C cross-host work, finally feasible because the
project owner now has user-owned cluster hardware (Mac + Beelink +
UPS + SSD).

## The gap

Σ.B shipped a distributed-execution backend on `datafusion-distributed`
with Arrow Flight shuffle. Σ.C benchmarked it against PySpark and
won by 5.87× geomean at SF=1 / 3.3× at SF=10 — but **all three
"workers" ran as in-process peers on a single M3 Pro Mac**. The
Σ.C writeup acknowledges this:

> Cross-host numbers + the originally-specced `m6i.4xlarge × 4`
> recipe deferred (no AWS in this project's runway; `infra/`
> retained for users with cluster access).

In-process peers share the kernel, RAM bandwidth, and (effectively)
zero-latency loopback. The numbers do not exercise:

1. **Arrow Flight over a real NIC** — TCP stack, MTU, congestion control.
2. **Cross-host parquet reads** — workers fetching from a shared
   object store / NFS / S3-compatible endpoint rather than the local
   filesystem.
3. **Genuine NUMA / cache isolation** — every worker today shares
   the same L3 cache and memory controller.
4. **Failure modes that only show up under real hardware** — power
   loss (UPS), disk failure, network partition, host reboot during
   query.

Until those are exercised, the "5.87× vs PySpark" claim is qualified.
This phase removes the qualifier.

## Hardware setup (target)

Minimal viable cluster:

| Host | Role | Spec | Notes |
|------|------|------|-------|
| Mac (M3 Pro) | coordinator + worker 1 | 18 GB / SSD | dev machine |
| Beelink mini-PC | worker 2 | 16–32 GB / NVMe SSD + UPS | user-owned |

Wired ethernet between the two via a gigabit (or 2.5 GbE) switch
makes shuffle perf representative. Wi-Fi for both is the worst case
and acceptable as a documented lower bound.

A 4-node target adds two more Beelinks (or any commodity Linux
boxes) if the 2-node numbers are encouraging. Don't pre-buy — 2
nodes is enough to find the architectural bottlenecks; node counts
3–4 just scale them.

Shared storage for parquet: simplest path is one Beelink hosting an
SMB / NFS share off its SSD, or a self-hosted MinIO container (the
existing `ObjectStoreBackend` already speaks S3). Avoid S3 itself
for the first cut — adds variance the benchmark shouldn't pay for.

## Sub-phases

### Σ.B+.1 — 2-node bring-up + correctness

Goal: TPC-H 22-query suite passes on a 2-node cross-host cluster.
No perf claim — just "queries return the same rows DataFusion does
locally, every time."

Work:

1. Document the existing `flow-worker` deployment for non-Docker
   Linux (the example today is docker-compose only).
2. Validate the existing `examples/distributed-cluster/`
   docker-compose on the Beelink as a sanity check.
3. Run `tpch_22_audit` integration test, but pointed at the cross-
   host cluster instead of in-process workers. Likely needs a thin
   test harness that registers a remote-parquet source from the
   shared volume.

Acceptance: 22/22 PASS, no flakiness over 10 consecutive runs.

Effort: ~3–5 days. Mostly setup + harness, not new code.

### Σ.B+.2 — Cross-host TPC-H benchmark (SF=1, SF=10)

Goal: run the existing Σ.C benchmark suite cross-host and publish
the numbers. Side-by-side with the in-process baseline so the
shuffle-cost story is visible.

Work:

1. Reuse the existing benchmark harness (`cargo bench -p
   ematix-flow-core`); add a "distributed-2-node" target alongside
   "datafusion / distributed-3-in-process / pyspark-local".
2. Measure per-query and geomean; bootstrap CIs as Σ.C does.
3. Note which queries are dominated by shuffle (Q5, Q9, Q21) and
   which by scan (Q1, Q6, Q19). The shuffle-heavy ones are where
   network shows up.

Acceptance: numbers committed to `docs/BENCHMARKS.md` under a new
"Σ.B+ cross-host (2-node)" section. No specific geomean target —
this is informational, not a gate. If we lose to PySpark at SF=10
cross-host, that's a finding worth knowing.

Effort: ~3–5 days, mostly running and writing up.

### Σ.B+.3 — Failure-mode coverage

Goal: characterize how the cluster handles realistic hardware
events. The Σ.B PR 5 work already covered network-level failures
in unit tests; this is the integration version on real hardware.

Cases to exercise (each is one entry in a new
`docs/CLUSTER_FAILURE_MATRIX.md`):

1. **Worker process kill mid-query** — Σ.B retry semantics should
   recover; verify on the Beelink with `kill -9`.
2. **Worker host hard power-off mid-query** — pull the UPS plug
   (or `systemctl poweroff -ff` on the Beelink). What does the
   coordinator see? Recoverable?
3. **Network partition** — `iptables -A INPUT -s <coordinator> -j
   DROP` for 30s, then restore. Idempotent re-run?
4. **Disk full on worker** — fill the SSD to 99% via a `dd`. Does
   parquet scratch handling degrade gracefully?
5. **UPS-triggered graceful shutdown** — `apcupsd` / `nut` sends
   SIGTERM on AC loss; does the worker drain its in-flight stages
   before exiting?

Acceptance: each case has a documented expected behavior and an
observed behavior. Where they diverge, file an issue. No expectation
of fixing everything in this phase — just *characterizing* the
gap honestly.

Effort: ~1 week, half writing tooling and half running scenarios.

### Σ.B+.4 — Deployment docs

Goal: a non-Docker quickstart that a user with two Linux boxes
could follow end-to-end in <30 minutes.

Work:

1. `docs/CLUSTER_QUICKSTART.md` — systemd unit for `flow-worker`,
   firewall rules, shared-volume options (NFS / SMB / MinIO),
   minimum hardware sizing notes.
2. Optional: a Kubernetes manifest (`examples/distributed-k8s/`) as
   a counterpoint to the docker-compose example. Defer if 2-node
   bare-metal carries the story.

Acceptance: a fresh reader can stand up a 2-node cluster from this
doc alone. Validated by the project owner walking through it on
clean Beelink reinstall.

Effort: ~2 days.

## Σ.B+ sizing summary

| Sub-phase | Effort | Calendar |
|-----------|--------|----------|
| Σ.B+.1 — 2-node bring-up + correctness | ~3–5 days | week 1 |
| Σ.B+.2 — Cross-host benchmark | ~3–5 days | week 2 |
| Σ.B+.3 — Failure-mode coverage | ~1 week | week 3 |
| Σ.B+.4 — Deployment docs | ~2 days | week 4 |
| **Total** | **~3 weeks calendar** | one dev |

## Open questions

- **What's the shuffle baseline we'd be embarrassed to publish?**
  If cross-host SF=10 is 50% slower than in-process, that's
  expected and we report it. If it's 5× slower, something's wrong
  with the shuffle implementation and we file before publishing.
  Need to pick the "embarrassment threshold" before we run — say
  >2× slowdown vs in-process triggers an investigation gate.
- **Object store choice for shared parquet.** MinIO is the
  closest to S3 and the existing `ObjectStoreBackend` covers it.
  NFS is simpler but introduces filesystem semantics the production
  story doesn't have. Probably MinIO; revisit if MinIO setup
  becomes a yak.
- **Should Σ.B+.3 (failure modes) gate Σ.B+.2 (benchmarks)?** Arg
  for gating: don't publish numbers from a config you can't recover.
  Arg against: characterize first, fix later — same as the parquet
  losses story. Lean toward "publish benchmark, also publish
  failure matrix, both as informational."
- **PySpark on the same cluster?** The Σ.C comparison was
  PySpark `local[*]` vs ematix in-process. Cross-host needs
  PySpark Standalone or PySpark-on-K8s on the same hardware to be
  apples-to-apples. Significant operational cost; consider deferring
  to "Σ.B+.5 — PySpark cross-host comparison" only if Σ.B+.2 lands
  numbers worth comparing.
