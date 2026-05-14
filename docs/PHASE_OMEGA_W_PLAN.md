# Ω.W — central scheduler + dispatched workers

**Status:** draft (2026-05-14). Sketches the protocol + Rust/Python surfaces
needed to move ematix-flow from "one fat `run-due` process executes every
due pipeline" to "central scheduler claims work in a shared store, an
Executor backend spawns a worker per fire."

This plan covers **batch / declarative pipelines only.** Streaming pipelines
(`flow consume`) stay as long-lived k8s Deployments — the trigger-spawn
model doesn't apply to always-on consumers, and shoehorning them adds risk
without payoff.

---

## 1. Motivation + non-goals

### Goal

A user with N pipelines spread across a Postgres warehouse + an S3 lake +
a Kafka stream wants:

- One scheduler process (single replica, leader-elected) that owns
  "what's due, what's blocked, what's been claimed."
- Pipeline fires dispatched to **per-fire workers** — a k8s Job, a Lambda
  invoke, an ECS Run-Task — each running exactly one pipeline body and
  exiting.
- The same orchestration semantics we already ship: DAG ordering,
  retry-with-backoff, alerters, metrics, cross-backend reads/writes.

### Non-goals (this phase)

- Cross-region scheduling, multi-tenant isolation, per-job resource quotas.
  These are downstream of the protocol, not the protocol itself.
- Streaming pipeline dispatch (see above).
- Replacing k8s/Lambda/ECS as the spawn mechanism — we wrap them, we
  don't reinvent them.
- A new RunLog backend. Lease semantics ride on the existing 8 backends
  (with a SQL-only constraint for distributed mode — see §4).

---

## 2. What's already done that this builds on

| Building block | Phase | What it gives us here |
|---|---|---|
| Durable RunLog (8 backends) | Ω.D1a–h | The shared state store the scheduler and workers coordinate through. |
| `restore_into_process()` | Ω.D1b | Worker can load attempt count + retry deadline from any backend. |
| `@register depends_on=...` + cycle detection | Ω.1 | Scheduler decides next-fire from metadata alone — never executes a pipeline body to plan. |
| `retry={max_attempts, backoff_seconds, backoff_factor}` | Ω.2 | Retry math lives in the metadata, not the worker. |
| DAG-aware `run-due` | Ω.D2 | The "decide what's ready" half is already factored out — `run_due_with_dag_detailed` returns `RunDueResult`. The scheduler reuses that walker; it just dispatches instead of running. |
| URL-based RunLog / alerter / metrics | Ω.D3 / Ω.D4 | Workers self-configure from env + CLI. No shared in-memory state. |
| `flow run --module M name` | Ω.D1 era | Single-pipeline entrypoint. The Executor invokes exactly this command. |

The gap is **three concrete pieces**: lease semantics, the Executor trait,
and a long-running scheduler loop. The rest is already shaped right.

---

## 3. Architecture

```
┌─────────────────────────────────┐                ┌──────────────────────────────────┐
│   flow scheduler (1 replica)    │                │   Executor backend (pluggable)   │
│                                 │                │                                  │
│  loop every poll_interval:      │  fire event    │  - SubprocessExecutor (local)    │
│    walker = run_due_with_dag…   │ ─────────────► │  - K8sJobExecutor                │
│    for each due fire:           │                │  - LambdaExecutor                │
│      claim(pipeline) → RunLog   │                │  - ECSRunTaskExecutor            │
│      executor.dispatch(claim)   │                │  - CloudRunJobExecutor           │
│    sweep expired leases         │                │                                  │
└──────────────┬──────────────────┘                └──────────────────┬───────────────┘
               │                                                      │
               │ both read/write the same RunLog                      │ spawns:
               ▼                                                      ▼
        ┌────────────────────┐                              ┌──────────────────────┐
        │  RunLog (Postgres/ │ ◄────────── heartbeat ─────  │  flow run --module M │
        │  MySQL preferred   │             record_run       │  <pipeline>          │
        │  for distributed)  │ ◄───────────────────────────  │  (one fire, exits)  │
        └────────────────────┘                              └──────────────────────┘
```

Two principles drive everything below:

1. **The scheduler never runs pipeline bodies.** It only reads metadata
   (DAG, retry, schedule) and writes lease/dispatch rows. The Executor's
   spawn is fire-and-forget — the scheduler doesn't wait for it to finish,
   it just watches the RunLog for the worker's `record_run`.

2. **The worker never reads scheduler state.** It picks up its claim,
   runs one pipeline, heartbeats, writes the final `record_run`, and
   exits. No coordination with the scheduler beyond the shared RunLog.

This means: a scheduler crash mid-dispatch can leave a claim with no
worker (caught by lease-expiry sweep). A worker crash mid-run leaves a
half-written `record_run` (also caught by lease-expiry; the next
scheduler pass re-claims and re-dispatches with attempt-count++ from
the existing retry path).

---

## 4. Lease / claim semantics

### New RunLog operations

Three methods added to the existing `RunLog` Protocol (Python) +
`RunLogBackend` trait (Rust). Same shape on both sides; same TOML wire
format.

```python
class RunLog(Protocol):
    # ... existing record_run / record_attempt / restore_into_process / close

    def claim(
        self,
        pipeline: str,
        worker_id: str,
        lease_seconds: int,
    ) -> ClaimResult: ...
    """Atomic compare-and-set: insert a row marking `pipeline` as
    claimed by `worker_id` with a lease expiring at `now() +
    lease_seconds`. Returns `ClaimResult.acquired` with the claim
    token, or `ClaimResult.busy(holder=..., expires_at=...)` if
    someone else holds an unexpired claim."""

    def heartbeat(self, claim_token: str, lease_seconds: int) -> None: ...
    """Extend the lease for the given claim. No-op if the token is
    stale (lease already expired + reclaimed)."""

    def sweep_expired_leases(self, now: datetime) -> list[ExpiredClaim]: ...
    """Return claims whose `expires_at < now`. The scheduler uses this
    to detect worker death and re-mark the pipeline for re-claim."""
```

### Backend support matrix

| Backend | Distributed scheduler-safe? | Mechanism |
|---|---|---|
| Postgres | ✅ | `INSERT … ON CONFLICT (pipeline) DO UPDATE … WHERE expires_at < EXCLUDED.now()` returning the claim row. |
| MySQL | ✅ | `INSERT … ON DUPLICATE KEY UPDATE … WHERE expires_at < VALUES(now)` with `ROW_COUNT()` to detect contention. |
| SQLite | ⚠️ single-process only | SQLite's busy-wait + `BEGIN IMMEDIATE` is fine when the scheduler is the only writer, but local dev only. |
| DuckDB | ⚠️ single-process only | Same as SQLite. |
| InMemory | ✅ tests only | Trivial. |
| S3 / Azure / GCS | ❌ | Blob stores don't expose CAS without an external lock service (DynamoDB, Cosmos, Firestore). Out of scope. Users picking distributed-mode pick SQL. |

A distributed deployment running on S3 today would migrate to Postgres
*just for the RunLog* — the per-pipeline target/source backends are
unaffected.

### Schema (Postgres)

Single new table; existing `run_history` table is unchanged:

```sql
CREATE TABLE ematix_flow.pipeline_claims (
    pipeline_name   TEXT        PRIMARY KEY,
    claim_token     UUID        NOT NULL,
    worker_id       TEXT        NOT NULL,
    claimed_at      TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    attempt_count   INT         NOT NULL DEFAULT 0
);

CREATE INDEX pipeline_claims_expires ON ematix_flow.pipeline_claims (expires_at);
```

Reads from `record_run` and writes to `pipeline_claims` share a single
transaction at fire-completion time so the claim is released atomically
with the run-history append.

---

## 5. Executor trait

Mirror of the RunLog: trait in Rust, Protocol in Python, single concrete
shape on both sides.

```python
@dataclass
class DispatchSpec:
    pipeline_name: str
    module: str                          # how to import the @register'd pipelines
    claim_token: str                     # worker passes this on heartbeat
    run_log_url: str                     # so the worker can write its own outcome
    alerter_urls: list[str]
    metrics_url: str | None
    env: dict[str, str]                  # passed through; secrets stay out of the spec

class Executor(Protocol):
    def dispatch(self, spec: DispatchSpec) -> DispatchHandle: ...
    """Spawn one worker for this fire. Returns a handle the scheduler
    can poll/cancel, but the scheduler does not wait on it — the
    worker's outcome lands in the RunLog independently."""

    def cancel(self, handle: DispatchHandle) -> None: ...
    """Best-effort cancel — used when a lease expires and we're about
    to re-claim. Workers should be idempotent enough that double-fire
    is safe (existing load strategies already guarantee this)."""
```

### Shipping the first three impls

1. **`SubprocessExecutor`** (~100 LOC). Local dev + smoke testing.
   `subprocess.Popen(["flow", "run", "--module", ..., pipeline])`,
   handle is the PID. CI runs the full scheduler protocol against
   this backend.

2. **`K8sJobExecutor`** (~300 LOC). Templated `batch/v1.Job` with
   `restartPolicy: Never` (the *RunLog* owns retry, not the pod). The
   handle is `{namespace, job_name}`; cancel is `kubectl delete job`.
   Requires `kubernetes` Python client as an optional extra
   (`ematix-flow[executor-k8s]`).

3. **`LambdaExecutor`** (~150 LOC). `lambda_client.invoke(... InvocationType="Event")`.
   Worker code lives in a Lambda layer or container image; the
   `DispatchSpec` rides in the event payload. Optional extra
   `ematix-flow[executor-lambda]`.

Deferred: `ECSRunTaskExecutor`, `CloudRunJobExecutor`,
`AzureContainerInstanceExecutor`. Same shape — pattern set by the
k8s impl.

---

## 6. Scheduler loop

```python
def run_scheduler(
    *,
    module: str,
    run_log: RunLog,
    executor: Executor,
    alerters: list[Alerter],
    metrics: MetricsSink,
    poll_interval_seconds: int = 10,
    lease_seconds: int = 300,
    worker_id: str | None = None,
) -> None:
    """Long-running scheduler. Run-due loop, but instead of executing
    each due pipeline in-process, claims it and hands it to the
    executor."""

    worker_id = worker_id or f"scheduler-{socket.gethostname()}-{os.getpid()}"

    while True:
        now = datetime.now(tz=UTC)

        # 1. Sweep expired leases. Pipelines whose workers died are
        #    reset so the next walk re-claims them with attempt_count
        #    bumped by the existing retry path.
        for expired in run_log.sweep_expired_leases(now):
            metrics.inc("scheduler_lease_expired_total", pipeline=expired.pipeline)
            log.warning("lease expired: %s held by %s", expired.pipeline, expired.worker_id)

        # 2. Walk the DAG and find due pipelines.
        due = run_due_with_dag_detailed(module=module, now=now, run_log=run_log)

        # 3. Claim + dispatch.
        for fire in due.fires:
            claim = run_log.claim(fire.pipeline, worker_id, lease_seconds)
            if not claim.acquired:
                continue  # someone else has it

            spec = DispatchSpec(
                pipeline_name=fire.pipeline,
                module=module,
                claim_token=claim.token,
                run_log_url=run_log.url,
                alerter_urls=[a.url for a in alerters],
                metrics_url=metrics.url,
                env=os.environ.copy(),
            )

            try:
                executor.dispatch(spec)
                metrics.inc("scheduler_dispatched_total", pipeline=fire.pipeline)
            except DispatchError as e:
                # Failed to spawn (k8s API down, Lambda throttled). Release
                # the claim so another scheduler replica / next pass retries.
                run_log.release(claim.token)
                for alerter in alerters:
                    alerter.notify(DispatchFailedEvent(fire.pipeline, e))

        time.sleep(poll_interval_seconds)
```

### Worker side

`flow run --module M <pipeline>` gains four flags (all optional, all
auto-populated by the Executor):

- `--claim-token <uuid>` — passed to RunLog so heartbeats and the
  final `record_run` are joined to the right claim row.
- `--heartbeat-interval <seconds>` — default 30, scheduler's
  `lease_seconds` should be a few multiples of this.
- `--run-log <url>` / `--alerter <url>` / `--metrics <url>` — same as
  today.

The worker code path is mostly unchanged: load module → look up
pipeline → run body → `record_run`. A `HeartbeatThread` starts at
top-of-worker and stops on either exit or exception.

---

## 7. Leader election (single scheduler replica)

Two writers competing on the same claim row are safe (the CAS rejects
the loser), but two schedulers walking the DAG at the same poll
interval is wasteful. Lightweight leader election:

```sql
-- Once per poll cycle, before walking:
INSERT INTO ematix_flow.scheduler_lock (id, holder, expires_at)
VALUES ('singleton', :worker_id, now() + interval '30s')
ON CONFLICT (id) DO UPDATE
    SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
    WHERE scheduler_lock.expires_at < now() OR scheduler_lock.holder = EXCLUDED.holder
RETURNING holder;
```

If the returned holder is us, we walk. Otherwise we sleep one cycle
and try again. Not strict (a network partition can transiently
elect two leaders) but **the claim CAS is what's strict** — leader
election is just a optimization to avoid wasted polls.

---

## 8. CLI

```sh
flow scheduler \
    --module my_pipelines \
    --run-log postgres://flow:pw@logdb/flow_history \
    --executor k8s://default?service-account=flow-worker \
    --alerter slack://hooks.slack.com/services/... \
    --metrics prometheus://:9100 \
    --poll-interval 10 \
    --lease-seconds 300
```

URL forms for `--executor`:

| Scheme | Concrete impl |
|---|---|
| `subprocess://` | Local subprocess (default for dev). |
| `k8s://<namespace>[?service-account=...&image=...]` | k8s `batch/v1.Job`. |
| `lambda://<function-name>?region=...` | AWS Lambda. |
| `ecs://<cluster>/<task-def>?subnet=...&security-group=...` | ECS Run-Task. (deferred) |
| `cloudrun://<job-name>?project=...` | GCP Cloud Run Job. (deferred) |

`flow run-due` stays as today — single-process fire of every due
pipeline. It remains the right primitive for cron-style deployments
that don't need horizontal scale. The new `flow scheduler` is opt-in.

---

## 9. Phase breakdown

| Sub-phase | Scope | Rough size |
|---|---|---|
| **Ω.W.1** | Add `claim` / `heartbeat` / `sweep_expired_leases` + `release` to `RunLog` Protocol. SQLite + InMemory impls (single-process safe). Tests against the Protocol so all 8 backends are covered structurally; the 3 SQL backends get real CAS impls. | ~3 days |
| **Ω.W.2** | Postgres + MySQL CAS impls — the actual distributed-mode path. Testcontainers tests covering concurrent claims, lease expiry, worker-death recovery. | ~2 days |
| **Ω.W.3** | `Executor` Protocol + `SubprocessExecutor`. `flow run` worker-side flags. End-to-end test: one scheduler process + one subprocess executor + Postgres RunLog, watching N pipelines complete on the right cadence. | ~3 days |
| **Ω.W.4** | `K8sJobExecutor` + `ematix-flow[executor-k8s]` extra. Manifest template review; kind-cluster integration test (Docker-gated, like the existing testcontainers suite). | ~4 days |
| **Ω.W.5** | `LambdaExecutor` + `ematix-flow[executor-lambda]` extra. Mocked-AWS test via `moto`; documented manual smoke recipe. | ~3 days |
| **Ω.W.6** | `flow scheduler` CLI subcommand + leader election + supervised-restart loop (mirror the existing streaming-daemon supervisor). | ~2 days |
| **Ω.W.7** | Docs: USER_GUIDE walkthrough, DEPLOYMENT.md recipes for k8s + Lambda + ECS, migration guide from `flow run-due` cron model. | ~2 days |

**Total: 2–3 weeks** of focused engineering. Each sub-phase is shippable
on its own — Ω.W.1–3 alone (single-host subprocess executor + lease
semantics) is already a meaningful step toward correctness even without
distributed compute.

---

## 10. Open questions / decisions deferred

1. **Worker code packaging.** k8s workers need the user's pipeline
   module on the image. Three approaches; we pick one before Ω.W.4:
   - **User builds the image** (most flexible, most onboarding friction).
   - **Bake into a base image, mount module via ConfigMap** (good for
     dev, breaks for big code bases).
   - **Pull from S3/git on worker boot** (good for big code bases,
     adds startup latency).

2. **`record_run` schema evolution.** Adding `claim_token` + `worker_id`
   columns to `run_history`. SQL backends get an `ALTER TABLE` migration;
   blob backends (S3/Azure/GCS) just add the field to the JSONL record.
   No back-compat concern — old records simply have NULL.

3. **Streaming pipeline cohabitation.** A user mixing streaming + batch
   in one module shouldn't be surprised when `flow scheduler` ignores the
   streaming pipelines. Document it; consider a `flow streaming-supervisor`
   companion daemon in a follow-on phase.

4. **Cancel semantics.** What does `flow scheduler` do when a user
   wants to stop a mid-flight pipeline? Today's answer: kill the worker
   pod / Lambda. Future: explicit `flow cancel <run-id>` that flips a
   row in the RunLog and the worker's heartbeat-thread checks it.

---

## 11. Why this is "2–3 weeks" not "2–3 months"

The standard Temporal/Airflow build is months because they ship:

- A custom protocol for durable activity execution.
- Their own retry / backoff / DAG engine.
- Their own UI + metrics tier.
- Per-tenant resource isolation.
- A claim/lease/heartbeat layer in their own database.

We already have the first three on the list (durable state via RunLog,
retry math from Ω.2, DAG from Ω.1, observability from Ω.Q3+Ω.M1). What
remains is the claim/lease layer on top of RunLog and a thin Executor
adapter to whichever compute the user runs on. The hard parts are
already done — Ω.W is the wiring phase.
