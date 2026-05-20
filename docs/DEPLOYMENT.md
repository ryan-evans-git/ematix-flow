# Deployment Guide

Pick the shape that matches your infrastructure. Each recipe shows
the **minimal install**, the **invocation**, and a **state-survival**
explanation so you know what loses or keeps history across restarts.

The orchestrator surface to keep in mind:

| Concern | What it is | Configurable via |
| --- | --- | --- |
| Schedule | `@register(schedule="@hourly")` declares when a pipeline fires | `--interval` (cron tick window) |
| Run history | last-run timestamps + retry state | `--run-log-url` |
| Alerts | push events on failure / give-up / recovery | `--alerter` (repeatable) |
| Metrics | counters, histograms, gauges to a monitoring stack | `--metrics` |

The defaults are: **SQLite at `~/.ematix-flow/run_log.db`, no alerters,
NullSink metrics.** Operators opt into everything beyond that.

---

## Recipe 1 — Single laptop / dev

The lightest possible install. Schedule fires from a cron entry; state
lives in a local SQLite file in your home dir.

**Install**

```bash
pip install ematix-flow
```

**Invocation** (cron, every minute)

```cron
* * * * * /usr/local/bin/flow run-due --module my_app.pipelines --interval 60
```

**State survival**

- SQLite at `~/.ematix-flow/run_log.db` survives restarts.
- Single host, single writer — no concurrency concerns.

---

## Recipe 2 — Single pod / systemd service

Same shape as recipe 1 but on a server. Mount a writable volume for
the SQLite file so pod restarts don't lose state.

**Install**

```bash
pip install ematix-flow
```

**systemd unit** (`/etc/systemd/system/flow.timer` + `.service`)

```ini
# flow.service
[Unit]
Description=ematix-flow run-due tick

[Service]
Type=oneshot
Environment=EMATIX_FLOW_RUN_LOG_URL=sqlite:///var/lib/ematix-flow/run_log.db
Environment=EMATIX_FLOW_ALERTERS=stdout://
User=flow
ExecStart=/usr/local/bin/flow run-due --module my_app.pipelines --interval 60
```

```ini
# flow.timer
[Unit]
Description=Run ematix-flow every minute

[Timer]
OnCalendar=*:0/1
Persistent=true

[Install]
WantedBy=timers.target
```

**State survival**

- `/var/lib/ematix-flow/` mounted persistently → survives pod restarts.
- Logs flow to journald via stdout/stderr; `flow status` shows current
  pipeline state.

---

## Recipe 3 — Multi-host with shared Postgres

Several hosts run `flow run-due` against the same orchestrator state.
SQLite can't be safely shared across hosts; use Postgres.

**Install**

```bash
pip install "ematix-flow[runlog-postgres]"
```

**Connection setup** (one-time, on the Postgres side)

```sql
CREATE DATABASE orchestrator;
CREATE USER flow WITH PASSWORD '...';
GRANT CONNECT ON DATABASE orchestrator TO flow;
GRANT USAGE, CREATE ON SCHEMA public TO flow;
```

`PostgresRunLog` auto-creates the two tables (`run_log`,
`attempt_state`) on first connect. You can pre-create them and pass
`create_tables=False` if your role lacks DDL privilege.

**Invocation**

```bash
EMATIX_FLOW_RUN_LOG_URL=postgresql://flow:...@db.internal/orchestrator \
EMATIX_FLOW_ALERTERS=slack://hooks.slack.com/services/X/Y/Z \
EMATIX_FLOW_METRICS=prometheus://:9090 \
flow run-due --module my_app.pipelines --interval 60
```

**State survival**

- Postgres is the source of truth; any host can join or leave the
  fleet without losing history.
- `INSERT ... ON CONFLICT DO UPDATE` upserts are atomic, so two cron
  ticks racing on the same pipeline name produce a consistent
  outcome.

---

## Recipe 4 — Kubernetes CronJob

Each tick is a fresh pod. Run-log must live outside the pod
(Postgres, S3, GCS, Azure Blob, or NFS-mounted SQLite).

**Install** (`Dockerfile`)

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir "ematix-flow[runlog-postgres,metrics-prometheus]"
COPY my_app/ /app/my_app/
WORKDIR /app
CMD ["flow", "run-due", "--module", "my_app.pipelines"]
```

**CronJob**

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: flow
spec:
  schedule: "* * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: flow
              image: my-registry/flow:latest
              env:
                - name: EMATIX_FLOW_RUN_LOG_URL
                  valueFrom:
                    secretKeyRef:
                      name: flow-secrets
                      key: pg-url
                - name: EMATIX_FLOW_ALERTERS
                  value: "slack://hooks.slack.com/services/X/Y/Z"
                - name: EMATIX_FLOW_METRICS
                  value: "otlp://otel-collector.observability:4317"
              args:
                - "run-due"
                - "--module=my_app.pipelines"
                - "--interval=60"
          restartPolicy: Never
```

**State survival**

- Postgres holds run-log.
- OTel collector aggregates metrics across all replicas + every tick.
- Slack receives the same events from any pod that runs.

---

## Recipe 5 — AWS Lambda (single-region)

Lambda has a read-only filesystem except `/tmp`, and `/tmp` doesn't
persist across invocations. Use S3 for run history.

**Install** (Lambda layer)

```bash
pip install -t lambda_layer/python \
  "ematix-flow[runlog-s3]"
zip -r layer.zip lambda_layer
```

**Handler**

```python
# handler.py
import os
import sys
from ematix_flow import cli

def lambda_handler(event, context):
    sys.argv = [
        "flow",
        "run-due",
        "--module", "my_app.pipelines",
        "--interval", "60",
        "--run-log-url", os.environ["EMATIX_FLOW_RUN_LOG_URL"],
        "--alerter", os.environ["EMATIX_FLOW_ALERTERS"],
    ]
    return cli.main()
```

**Environment**

```
EMATIX_FLOW_RUN_LOG_URL=s3://my-bucket/flow-state/prod
EMATIX_FLOW_ALERTERS=slack://hooks.slack.com/services/X/Y/Z
```

**State survival**

- S3 is the orchestrator's memory between invocations.
- IAM role on the Lambda needs `s3:GetObject`, `s3:PutObject`,
  `s3:DeleteObject`, and `s3:ListBucket` on the prefix.
- Each pipeline name is one key under `{prefix}/run_log/` and
  `{prefix}/attempt_state/` — easy to inspect with `aws s3 ls`.

---

## Recipe 6 — GCP Cloud Run / Cloud Scheduler

Symmetrical to the Lambda recipe but on GCP.

**Install**

```bash
pip install "ematix-flow[runlog-gcs]"
```

**Cloud Run service entry**

```python
# main.py
import os, sys
from flask import Flask
from ematix_flow import cli

app = Flask(__name__)

@app.route("/run-due", methods=["POST"])
def tick():
    sys.argv = [
        "flow", "run-due",
        "--module", "my_app.pipelines",
        "--run-log-url", os.environ["EMATIX_FLOW_RUN_LOG_URL"],
        "--metrics", "otlp://otel-collector:4317",
    ]
    rc = cli.main()
    return {"rc": rc}, 200 if rc == 0 else 500
```

**Cloud Scheduler** posts to `/run-due` every minute.

```
EMATIX_FLOW_RUN_LOG_URL=gs://my-bucket/flow-state/prod
```

**State survival**

- GCS holds run-log. ADC credentials on the Cloud Run service handle
  auth.
- For multi-region, point all regions at the same bucket; the
  orchestrator state is consistent.

---

## Recipe 7 — Azure Functions / Container Apps

```bash
pip install "ematix-flow[runlog-azure]"
```

```
EMATIX_FLOW_RUN_LOG_URL=azure://myaccount/mycontainer/flow-state
```

The `azure://` URL scheme synthesises the blob endpoint
(`https://myaccount.blob.core.windows.net`). For non-default
endpoints (Azure Stack, Azurite emulator), construct
`AzureBlobRunLog` directly with `account_url=` from Python instead
of using the URL form.

---

## Recipe 8 — central scheduler, fan-out to workers

Recipes 1–7 all use the **cron-tick** model: an external scheduler
wakes `flow run-due` once per interval, fires whatever's due, and
exits. This recipe uses `flow scheduler` instead — one long-running
controller process holds a leader lease, walks the DAG every
`--poll-interval` seconds, and dispatches each eligible pipeline to a
disposable worker via an `Executor`. The controller never runs
pipeline code itself.

When to pick this over recipes 1–7:

- You want sub-minute reactions (cron's minimum is one minute).
- You want each pipeline isolated in its own pod / container instead
  of crammed into one cron host.
- You want one observable controller process (alerts, metrics, logs
  in one place) instead of N-per-tick cron processes.

Run multiple scheduler replicas for HA. Leader election uses the same
`RunLog.claim` machinery as pipelines — there's no extra table, no
separate consensus service. At most one replica walks the DAG per
tick; the others log "leader is X" and sleep.

**Install** (`Dockerfile`)

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir \
    "ematix-flow[runlog-postgres,metrics-prometheus,executor-k8s]"
COPY my_app/ /app/my_app/
WORKDIR /app
```

**Scheduler Deployment** (k8s controller)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flow-scheduler
spec:
  replicas: 2                          # for HA; leader lease serialises them
  selector: { matchLabels: { app: flow-scheduler } }
  template:
    metadata: { labels: { app: flow-scheduler } }
    spec:
      serviceAccountName: flow-scheduler   # needs k8s Job create + delete
      containers:
        - name: scheduler
          image: my-registry/flow:latest
          args:
            - "flow"
            - "scheduler"
            - "--module=my_app.pipelines"
            - "--executor=k8s://flow?image=my-registry/flow:latest&service-account=flow-worker"
            - "--poll-interval=10"
            - "--lease-seconds=300"
          env:
            - name: EMATIX_FLOW_RUN_LOG_URL
              valueFrom: { secretKeyRef: { name: flow-secrets, key: pg-url } }
            - name: EMATIX_FLOW_ALERTERS
              value: "slack://hooks.slack.com/services/X/Y/Z"
            - name: EMATIX_FLOW_METRICS
              value: "otlp://otel-collector.observability:4317"
```

**RBAC for the controller** (`flow-scheduler` SA needs to create
`Job`s in its namespace):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: flow-job-runner, namespace: flow }
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get", "list", "watch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: { name: flow-scheduler, namespace: flow }
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: flow-job-runner
subjects:
  - { kind: ServiceAccount, name: flow-scheduler, namespace: flow }
```

The worker `Job`s (created by the controller) run as
`flow-worker`, which only needs network access to the RunLog —
no k8s API perms.

**Executor URL alternatives**

| URL | Spawns | Use when |
| --- | --- | --- |
| `subprocess://` | local `flow run` subprocess | Single host, no orchestrator. |
| `subprocess+python://` | `python -m ematix_flow.cli run` via `sys.executable` | Dev / CI; no wheel on `PATH`. |
| `k8s://<ns>?image=<img>&service-account=<sa>` | `batch/v1 Job` per pipeline | Multi-tenant cluster; pipeline isolation. |
| `lambda://<fn>?qualifier=<alias>` | Async `Invoke(InvocationType="Event")` | AWS-native dispatch; lease-expiry handles death since Lambda has no cancel. |

**State survival**

- Postgres (or any durable RunLog) is the only shared state. The
  scheduler holds no in-memory queue.
- Killing a scheduler pod is safe — the leader lease expires after
  `--lease-seconds`, the surviving replica takes over on the next
  tick. Any worker `Job` already in flight keeps running and writes
  outcome back to the RunLog independently.
- A worker that dies mid-pipeline stops heartbeating; the next
  scheduler tick sees the expired lease and the row becomes
  claimable again. No separate "release" step.

**Migrating from `flow run-due`** — run both side-by-side against the
same RunLog. The per-pipeline claim CAS guarantees no
double-dispatch, so you can leave cron up while you trust the new
daemon, then remove the cron entry once you're satisfied.

---

## Recipe 9 — Distributed mesh with peer auto-detection

When the workload outgrows a single process, ematix-flow can fan
queries out across a peer mesh of `flow-worker` processes via Apache
Arrow Flight.

### Engine modes

| `engine` | Behavior |
|---|---|
| `"single"` *(alias of `"datafusion"`)* | Always in-process. Specifying `peers` is rejected (clear pointer to use `"auto"` or `"distributed"`). |
| `"distributed"` | Always peer-distributed. Requires `peers = [...]`. Window/join transforms are rejected — not yet supported. |
| **`"auto"` *(default when `engine` is absent)*** | Try distributed if peers expand to ≥1 URL at startup AND no window/join is configured; otherwise fall back to in-process. The choice is logged at `info!` level so operators can verify which path was taken. |

`engine = "auto"` is the default starting in Phase 3.5. Existing
configs that didn't specify `engine` previously defaulted to
`"datafusion"`; with the new default they behave **identically** as
long as they had no `peers` block (which earlier validation rejected
anyway, so this is a no-op for every shipped config).

Each peer URL in the `peers` list can be one of three shapes — mix
freely:

| Scheme | Example | When to use |
| --- | --- | --- |
| `http://host:port` | `http://flow-01.local:50051` | Fixed-membership clusters, dev meshes, anything where pod IPs are stable. |
| `dns://host:port` | `dns://flow-workers.flow.svc.cluster.local:50051` | Any DNS-driven registry — K8s headless services, Consul DNS, AWS Cloud Map, on-prem DNS round-robin. Resolves the A records once at backend open. |
| `k8s://service.namespace:port` | `k8s://flow-workers.flow:50051` | Sugar for the `dns://` form targeting `*.svc.cluster.local`. Lets one line replace a 50-pod list. |

### K8s headless service example

```yaml
# Service: headless so each pod gets its own A record.
apiVersion: v1
kind: Service
metadata:
  name: flow-workers
  namespace: flow
spec:
  clusterIP: None            # headless — exposes per-pod A records
  selector: { app: flow-worker }
  ports:
    - name: grpc
      port: 50051
      targetPort: 50051
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: flow-worker, namespace: flow }
spec:
  serviceName: flow-workers
  replicas: 5
  selector: { matchLabels: { app: flow-worker } }
  template:
    metadata: { labels: { app: flow-worker } }
    spec:
      containers:
        - name: flow-worker
          image: my-registry/flow:latest
          command: ["flow-worker"]
          args:  ["--bind=0.0.0.0:50051"]
          ports: [{ containerPort: 50051, name: grpc }]
```

Coordinator config (anywhere — same cluster, another K8s ns, or
outside K8s entirely as long as the DNS resolves):

```toml
# pipeline.toml
[engine]
kind = "distributed"
peers = ["k8s://flow-workers.flow:50051"]
```

`flow run pipeline.toml` resolves `flow-workers.flow.svc.cluster.local`
at start-up, fans out across whatever pods are alive at that
moment, and the Flight mesh balances work.

### Refresh semantics

DNS resolution happens **once** when the backend opens. If pods
churn (scale event, rolling restart), the SessionContext won't
re-resolve until the process restarts or rebuilds the backend.
For batch-job-style usage (Kubernetes Jobs, cron, `flow scheduler`
launches) this is fine — every invocation gets a fresh peer list.
Long-lived coordinator processes that need to track autoscaling
events should run on a restart-on-config-change loop today;
periodic refresh is a Phase 3b follow-up.

### Diagnostic

Bad peer entries surface at `flow run` startup with a clear
quoted-entry error:

```
$ flow run pipeline.toml
Error: peer #1 ("k8s://flow-workers.flow"): missing port in
"flow-workers.flow" (expected host:port)
```

---

## Observability cheat sheet

### Alerters

| URL | Sends to | Dep |
| --- | --- | --- |
| `stdout://` | stderr | stdlib |
| `slack://hooks.slack.com/services/X/Y/Z` | Slack channel | stdlib (urllib) |
| `https://hooks.slack.com/services/X/Y/Z` | Slack channel (passthrough) | stdlib |

Repeat `--alerter` to send to multiple destinations. A failing alerter
doesn't take the orchestrator down — exceptions are logged-and-swallowed
so the next alerter in the chain still fires.

### Metrics

| URL | Sink | Dep |
| --- | --- | --- |
| `null://` | no-op (default) | stdlib |
| `stdout://` | one line per event to stderr | stdlib |
| `memory://` | in-process dicts (tests) | stdlib |
| `prometheus://:9090` | HTTP `/metrics` server on port 9090 | `prometheus_client` |
| `prometheus://` | metrics recorded only; scrape via the API | `prometheus_client` |
| `otlp://collector:4317` | OTLP gRPC | `opentelemetry-sdk` + `-exporter-otlp` |
| `otlp+http://collector:4318` | OTLP HTTP | same |

Three orchestrator-level metrics every operator gets:

```
pipeline_runs_total{pipeline, outcome}    # outcome ∈ {success, failure, skipped}
pipeline_duration_seconds{pipeline}        # histogram
pipeline_retry_attempt{pipeline}           # gauge (0 when idle)
```

### Run-log backends

| URL prefix | Backend | Install extra |
| --- | --- | --- |
| `sqlite:///path` or bare path | SQLite (default) | (none — stdlib) |
| `memory://` | InMemoryRunLog | (none) |
| `postgres://...` / `postgresql://...` | PostgresRunLog | `runlog-postgres` |
| `mysql://...` / `mariadb://...` | MySQLRunLog | `runlog-mysql` |
| `duckdb:///path` or `duckdb://:memory:` | DuckDBRunLog | `runlog-duckdb` |
| `s3://bucket/prefix` | S3RunLog (incl. MinIO, R2 via custom client) | `runlog-s3` |
| `gs://bucket/prefix` | GcsRunLog | `runlog-gcs` |
| `azure://account/container/prefix` | AzureBlobRunLog | `runlog-azure` |

---

## Graceful degradation

If the configured run-log location can't be opened (read-only FS,
network unreachable, bad credentials), `flow run-due` prints a stderr
warning and continues **without** persistence. Pipelines still fire
according to their declared schedule; only the durable history
side-effect is skipped.

To silence the warning intentionally, pass `--no-run-log` or set
`EMATIX_FLOW_RUN_LOG_URL=` to empty.

---

## What you don't need to worry about

- **DDL on Postgres / MySQL**: tables auto-create on first connect via
  `IF NOT EXISTS`. Schema auto-creates on Postgres (use `create_tables=False`
  from Python if your role lacks DDL privilege).
- **Cross-tick state coherence**: every RunLog backend is upsert-based,
  so two ticks racing on the same pipeline name produce a single final
  state record.
- **Retry semantics**: `flow run-due` honors `retry=` policy from
  `@register(...)` automatically — backoff windows, gave-up gating,
  and recovery events all fire from the CLI without further wiring.
- **Pipeline data path**: Rust drivers (`tokio-postgres`, `mysql_async`,
  deltalake-rs) handle bulk row movement; Python's only on the
  orchestrator-state and dev-helper paths. The RunLog backend you
  pick doesn't bottleneck pipeline throughput.
