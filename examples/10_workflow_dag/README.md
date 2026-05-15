# Demo 10 — workflow DAG + central scheduler

Three pipelines wired into a DAG. The `flow scheduler` daemon
walks the DAG every tick, claims each eligible pipeline via a
distributed lease, and dispatches it as a worker subprocess.

```
raw_orders ──► enriched_orders ──► daily_summary  (flaky; retries)
```

**Shape:** declarative `@register`'d Python pipelines → central
scheduler → subprocess workers → SQLite RunLog (run history) +
SQLite warehouse (target data).

## Run

From the repo root, in one terminal:

```sh
make demo-workflow-scheduler
```

This launches `flow scheduler` against the pipelines in
`pipelines.py`. The scheduler walks the DAG every 5 seconds and
dispatches whatever's due. The cron schedule on every pipeline is
`* * * * *` (every minute) for demo speed; in production these
would naturally space out.

## Watch it in action

In a second terminal:

```sh
# Operator view: per-pipeline status table.
make demo-workflow-status

# Or watch it refresh every 2 seconds:
watch -n 2 make demo-workflow-status
```

You'll see the DAG fire in order, then enter retry-backoff
windows when `daily_summary` hits its synthetic flake. The
status table shows:

- **last status**: success / failure / waiting-on-upstream
- **next due**: when the next firing window opens
- **attempts**: current retry count + the gave-up gate

In a third terminal (optional) — inspect the actual data:

```sh
# Live row counts in the warehouse:
watch -n 2 'sqlite3 /tmp/ematix-demo-10.db \
  "SELECT '\''raw'\'' tbl, COUNT(*) n FROM raw_orders
   UNION ALL SELECT '\''enriched'\'', COUNT(*) FROM enriched_orders
   UNION ALL SELECT '\''summary'\'', COUNT(*) FROM daily_summary"'
```

## What's happening under the hood

- **`@register(...)`**: each pipeline is a plain Python function
  with a `name`, `schedule`, optional `depends_on`, and optional
  `retry` policy. No frameworks to inherit, no YAML to write.
- **`flow scheduler --executor "subprocess+python://"`**: the
  central daemon. Each tick it sweeps expired claim leases, walks
  the DAG, and for every pipeline that's due **and** upstream-fresh
  **and** not in retry-backoff, it `claim`s the row in the RunLog
  (single-round-trip CAS) and dispatches a worker via the configured
  Executor (here: a local subprocess via `python -m ematix_flow.cli
  run`).
- **HA via leader election**: the scheduler reserves a special
  `_scheduler_singleton` row in the RunLog; multiple replicas race
  for it each tick. Whoever wins walks the DAG; the others log
  "leader is X" and sleep. Same `claim()` machinery — no extra
  table.
- **Retry semantics**: `daily_summary` declares
  `retry={"max_attempts": 4, "backoff": "exponential", "base_secs": 5}`.
  When it raises, the next tick waits 5s, then 10s, then 20s before
  re-firing. After 4 failed attempts the pipeline is marked
  **gave-up** and the scheduler stops re-claiming it until the next
  schedule window opens.
- **Upstream-freshness gating**: `enriched_orders.depends_on =
  ["raw_orders"]`. The scheduler doesn't dispatch `enriched_orders`
  until `raw_orders` has a successful run for the current
  schedule-day (idempotent — running today's `enriched_orders` twice
  is fine because the `INSERT ... NOT IN (...)` clause is
  duplicate-safe).

## Stop everything

In the scheduler terminal: Ctrl+C. The scheduler catches SIGINT,
releases its leader lease, and exits cleanly.

To reset the demo state:

```sh
rm /tmp/ematix-demo-10.db /tmp/ematix-demo-10-runs.db
```
