# Σ.W — workflow trigger model

**Status:** design — pre-implementation
**Target release:** v0.7.0
**Predecessor work:** v0.6.0 (`@ematix.workflow` / `@ematix.job`), v0.6.1 (streaming on Workflows tab)

## Problem

v0.6.0 declared a workflow as `name + jobs[] + depends_on{dict}`. Two issues
surfaced once the model was used:

1. **Per-job cron schedules are ambiguous inside a workflow.** Jobs carry
   their own `schedule="*/5 * * * *"`. When a workflow groups several jobs
   each with their own cron, the scheduler fires every job independently
   and uses freshness gating to defer downstreams. That works mechanically
   but reads incoherently: the "9 PM cadence" people communicate
   externally doesn't correspond to anything in code.
2. **Per-workflow trigger model is too narrow.** v0.6.0 has no concept of
   "fire this workflow when other workflows complete." The only triggers
   are individual job crons or implicit streaming. Real-world workflows
   need composite conditions like "fire when workflow_A AND workflow_B
   have both completed since I last ran, AND it's after 21:00."

v0.7.0 reorganises the trigger surface around the workflow.

## The model

### Trigger conditions

A workflow declares an **AND-conjunction** of trigger conditions. All
declared conditions must hold relative to `last_successful_run_of_self`
for the workflow to fire.

| Kwarg | Type | Semantics |
|---|---|---|
| `triggered_by` | `list[str]` | Each name is a job or workflow that must have succeeded since this workflow last succeeded. |
| `schedule` | `str` (cron) | Next cron tick after `last_successful_run_of_self` must have reached or passed. |
| `timezone` | `str` (IANA) | Tz the cron is interpreted in. Only meaningful when `schedule` is set. |
| `on_message` | source object | Per-message trigger. Exclusive with `triggered_by` and `schedule` — one workflow run per inbound message. |

Implicit streaming: if the workflow's `jobs=[…]` contains a streaming
pipeline, the workflow is treated as streaming and none of the above
trigger kwargs are required (or allowed).

### Validation rules (at registration time)

- A workflow must have **≥1 trigger condition** declared, OR be implicitly
  streaming. Otherwise `ValueError("workflow {name!r}: needs a trigger")`.
- `on_message` is exclusive with `triggered_by` / `schedule`. Setting both
  raises `ValueError`.
- `timezone` without `schedule` raises (no cron to apply it to).
- `triggered_by` cycle detection across workflow→workflow chains.
- Per-job `depends_on` cycles within the workflow (existing v0.6 check).

### Job-level trigger kwargs

`@ematix.job(...)` still accepts `schedule=` / `triggered_by=` /
`on_message=` for standalone jobs (jobs not listed in any workflow's
`jobs=`). When a job is a member of a workflow, those kwargs are ignored
and a `DeprecationWarning` is emitted at registration time, since the
workflow's trigger supersedes them.

Per-job `depends_on=[…]` keeps its meaning — DAG edges within the
workflow — and is the only way to declare job ordering.

### Firing semantics

Pseudocode for the per-tick decision (run inside `flow run-due` /
`flow scheduler`):

```python
def should_fire(workflow):
    last = last_successful_run(workflow.name)  # None if never run

    # streaming workflow: no firing logic, runs continuously
    if workflow.is_streaming:
        return False

    # on_message workflow: fires from message-arrival callback, not run-due
    if workflow.on_message is not None:
        return False  # handled by message dispatcher

    # Otherwise: AND-conjunction of all declared conditions
    for upstream in workflow.triggered_by:
        ok, _ = last_successful_run(upstream)
        if ok is None or (last is not None and ok <= last):
            return False

    if workflow.schedule is not None:
        next_tick = next_cron_tick_after(workflow.schedule, last or epoch_zero,
                                          tz=workflow.timezone)
        if now() < next_tick:
            return False

    return True
```

Once `should_fire(workflow) → True`, the worker materialises an internal
topological order from each member job's `depends_on=` and executes the
DAG, recording success of every job + an overall workflow-success record
that becomes the new `last_successful_run_of_self`.

### Scenarios

| `triggered_by` | `schedule` | wf_A done | wf_B done | tick @ 21:00 | Fires? |
|---|---|---|---|---|---|
| `["wf_A", "wf_B"]` | `"0 21 * * *"` | 18:00 | 19:30 | 21:00 reached | yes, at 21:00 |
| `["wf_A", "wf_B"]` | `"0 21 * * *"` | 18:00 | 22:00 | 21:00 reached | yes, at 22:00 (immediate) |
| `["wf_A", "wf_B"]` | `"0 21 * * *"` | 18:00 | failed | 21:00 reached | no |
| `["wf_A", "wf_B"]` | `"0 21 * * *"` | 12:00 | 14:00 | not yet | no — waits for 21:00 |
| none | `"*/5 * * * *"` | — | — | every 5 min | yes per cron tick |
| `["wf_A"]` | none | 09:00 | — | n/a | yes immediately after wf_A |

## API shape — full v0.7.0 surface

```python
from ematix_flow import ematix, KafkaConnection

@ematix.job(
    name="extract_orders",
    source_connection="app_db",
    target=OrdersExtracted, target_connection="warehouse",
    mode="merge", keys=("order_id",),
    # depends_on is for within-workflow ordering.
    # When this job is in a workflow, its own schedule/triggered_by are ignored.
)
def extract_orders(conn): return "SELECT ..."

@ematix.job(
    name="enrich_orders",
    target=OrdersEnriched, target_connection="warehouse",
    mode="merge", keys=("order_id",),
    depends_on=["extract_orders"],          # waits inside the workflow
)
def enrich_orders(conn): return "SELECT ..."

@ematix.job(
    name="aggregate_orders",
    target=OrdersByCustomer, target_connection="warehouse",
    mode="merge", keys=("customer_id",),
    depends_on=["extract_orders"],
)
def aggregate_orders(conn): return "SELECT ..."

@ematix.job(
    name="report_orders",
    target=OrdersReport, target_connection="warehouse",
    mode="merge", keys=("customer_id",),
    depends_on=["enrich_orders", "aggregate_orders"],
)
def report_orders(conn): return "SELECT ..."

ematix.workflow(
    name="evening_combined_report",
    triggered_by=["workflow_A", "workflow_B"],
    schedule="0 21 * * *",
    timezone="America/New_York",
    jobs=[
        "extract_orders",
        "enrich_orders",
        "aggregate_orders",
        "report_orders",
    ],
)
```

## Implementation slices

### Σ.W.1 — framework: Workflow dataclass + validation + trigger evaluation

- Extend `Workflow` dataclass in `pipeline.py`: add `schedule`, `timezone`,
  `triggered_by: tuple[str, ...]`, `on_message`. Remove the v0.6.0
  `depends_on: dict[str, tuple[str, ...]]`.
- Rewrite `register_workflow(...)` for the new validation rules.
- Add `next_fire_decision(workflow_name) -> FireDecision` helper used by
  the scheduler.
- Hard-break: v0.6.0-shaped `ematix.workflow(..., depends_on={...})`
  raises a `ValueError` pointing at the new model.

### Σ.W.2 — scheduler / run-due wiring

- `flow run-due` and `flow scheduler` consult `next_fire_decision` for
  workflows instead of relying solely on per-job cron freshness gating.
- The DAG inside a workflow is materialised from member jobs' `depends_on`.
- Per-job `schedule` / `triggered_by` on workflow members are stripped at
  registration time (logged, then ignored).

### Σ.W.3 — `/api/workflows` endpoint + UI rendering

- API returns each workflow's resolved trigger summary (`schedule`,
  `timezone`, `triggered_by`, `on_message` kind), the computed
  `next_fire_at`, and the DAG edges (walked from member jobs).
- `Workflows.svelte` card shows the trigger summary in the header
  (e.g. "After: workflow_A, workflow_B · Schedule: 0 21 * * *
  America/New_York · Next: 2026-05-22 21:00").
- Job-level `schedule` shown in the Jobs tab applies only to standalone
  jobs; jobs that are workflow members show "via workflow {name}".

### Σ.W.4 — message-triggered workflows

- `on_message=<KafkaConnection.topic("…")>` / RabbitMQ / Pub/Sub / Kinesis
  source objects. Each message dispatches one workflow run; the
  workflow's internal DAG runs against that message's payload as input
  context.
- Worker-side: a long-lived message-listener spawns workflow runs.

### Σ.W.5 — validation harness + docs

- `ematix-flow-local-validation` `dag_pipelines.py` rewritten to the new
  model.
- USER_GUIDE Workflows chapter updated.
- ematix.dev guide updated (separate PR).

### Σ.W.6 — home-page screenshot regenerated

- Update the validation demo to declare workflows with the new trigger
  model + a per-workflow `schedule=`. Retake `workflows-view.png`.
- ematix.dev `index.astro` code example replaced with the new shape.

## What this is NOT

- Not a change to the per-job decorator (`@ematix.job` / `@ematix.pipeline`
  alias) signature beyond clarifying that per-job triggers are ignored
  inside workflows.
- Not a change to streaming pipelines — those continue to use
  `@ematix.streaming_pipeline` and surface as `kind: "streaming"`
  workflows-of-one on the Workflows tab (v0.6.1 behavior).
- Not OR-composition. All declared trigger conditions are AND-ed. If we
  ever need OR, it can be a `triggers=` higher-level kwarg with a typed
  AST — out of scope here.
