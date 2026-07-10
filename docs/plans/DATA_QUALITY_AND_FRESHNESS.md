# PRD + Plan — Data-Quality Expectations & Freshness SLOs

**Status:** Draft (proposed)
**Author:** (drafted with Claude)
**Created:** 2026-07-09
**Owner:** TBD
**Related:** `SQL_LAB_DASHBOARDS.md` (web surface reused here), sibling repo `../ematix-probe`

---

## 1. Summary

ematix-flow moves and transforms data well, but it cannot yet **assert anything about the
data itself**. There is schema-drift detection (`on_drift`) but no equivalent of dbt tests /
Great Expectations: no row-count / null / uniqueness / range / referential checks, no
**freshness SLOs**, no volume or distribution guards. Today a user bolts on a second tool.

We already own the missing engine: **`ematix-probe`** (published to PyPI as
`ematix-probe>=0.1.1`) is a Rust+Python data-quality library with 10 assertion types
(including freshness), SQL-pushdown execution, and a `probe_from_table(...)` helper that
auto-derives `not_null`/`unique` from an ematix-flow `ManagedTable`. It has **zero hard
dependency** on ematix-flow (duck-typed).

**This project wires `ematix-probe` into the pipeline lifecycle and the web UI** so that:
1. a pipeline can declare `expectations=` and a `freshness_sla=` inline;
2. checks run automatically as a post-write stage, with a `warn | fail` policy;
3. freshness is evaluated **even when a pipeline stops running** (a stalled pipeline is the
   whole point of a freshness SLO);
4. results surface in the existing web UI and fire through the existing alerters.

The scope is deliberately an **integration + surfacing** effort, not a new engine. Most
check logic already exists in `ematix-probe`; the work is authoring ergonomics, lifecycle
placement, durable result storage, the scheduled freshness evaluator, and UI/alert wiring.

---

## 2. Problem & motivation

- **Trust gap.** "The pipeline ran green" ≠ "the data is correct." A merge that silently
  drops 90% of rows, a nulled-out join key, a stale upstream — all pass today.
- **Freshness blindness.** A pipeline that *stops firing* (bad cron, dead scheduler, upstream
  outage) produces no failure event at all; the data just quietly goes stale.
- **Tool sprawl.** Users layer dbt tests + Great Expectations + a freshness monitor on top of
  ematix, duplicating the catalog/connection/alerting ematix already has.
- **We're one wire away.** `ematix-probe` exists and is published. Not integrating it leaves
  a finished asset on the shelf.

---

## 3. Goals / non-goals

### Goals
- Declare table-level expectations inline on `@ematix.pipeline`, plus auto-derived checks from
  the target `ManagedTable` (PK → `unique`, non-nullable → `not_null`).
- Run expectations as a **post-write** stage with an explicit failure policy (`warn` / `fail`).
- First-class **freshness SLO** per pipeline, evaluated both at run-end and on a schedule
  (to catch pipelines that aren't running).
- Persist results durably and surface them in the web UI (a **Quality** view + a **freshness
  badge** on pipeline cards), RBAC-gated.
- Fire `quality_failed` and `sla_breached` events through the existing alerters
  (Slack/email/PagerDuty/stdout).
- TDD throughout; opt-in behavior (a pipeline with no `expectations`/`freshness_sla` is
  unchanged).

### Non-goals (this iteration)
- **Row-level quarantine / DLQ routing of bad rows.** Phase-1 checks are table-level
  (pass/warn/fail on the landed table), not per-row rerouting. (Deferred — §11.)
- **Staging-then-swap "block before publish"** atomic gating. Phase-1 asserts on already-landed
  data. (Deferred — §11.)
- **Distribution-drift / volume-anomaly / statistical baselines.** Point-in-time thresholds
  only for now. (Deferred — §11.)
- **A YAML/TOML suite format.** Expectations are declared in Python, matching `ematix-probe`.
- **Load-testing.** `ematix-probe`'s load probes are out of scope.

---

## 4. Users & use cases

| Persona | Use case |
|---|---|
| Pipeline author | "Fail the run if `orders.id` isn't unique or `email` is null; warn if row count drops below 100k." |
| Data platform operator | "Page me if `dim_customer` is more than 6h stale, even if the pipeline silently stopped." |
| Analyst / viewer | "Is the data behind this dashboard fresh and passing its checks right now?" (Quality view) |
| On-call | Receives a Slack alert naming the pipeline, the failed assertion, and the observed vs expected value. |

---

## 5. Design overview

### 5.1 Reuse boundary
`ematix-probe` owns: assertion types, SQL pushdown/scan execution, `RunReport`/`AssertionResult`,
source adapters (`source.postgres/duckdb/parquet/s3_parquet`), and `probe_from_table()`.

ematix-flow owns (this project): the **authoring kwargs**, **lifecycle placement**, the
**failure policy**, **durable result storage**, the **scheduled freshness evaluator**, and
**UI/alert surfacing**.

```
@ematix.pipeline(expectations=…, freshness_sla=…)
        │  (decorator threads kwargs → pipeline.sync)
        ▼
pipeline.sync():  read → transform_pre → WRITE → [NEW: quality stage] → transform_post
        │                                              │
        │                           ematix_probe: probe_from_table(target, source=<target DSN>)
        │                                              │  .run() → RunReport
        ▼                                              ▼
   RunRecord.extras["quality"] = summary      QualityStore (durable)  ── /api/quality ──► Quality view
                                                                          /api/freshness ─► freshness badge
scheduler tick ──► FreshnessEvaluator (reads RunLog last-success) ──► sla_breached ──► alerters
```

### 5.2 Where the check runs
- **Execution point:** a new stage in `pipeline.sync()` (`python/ematix_flow/pipeline.py`,
  the `sync()` body ~L168–344), **after** the strategy write (`run_append/truncate/merge/scd2`)
  and **before** `_run_transforms_post()` (`decorators.py` ~L601–715). Placing it before
  post-transforms means a failed expectation can short-circuit downstream SQL.
- **Data locality:** `ematix-probe` does **SQL pushdown** for Postgres/DuckDB — the check runs
  *in the target database*, no data movement. The probe `source` is derived from the pipeline's
  **target connection DSN**. Parquet/S3 targets use `ematix-probe`'s scan path.
- **Failure policy** (`on_quality_failure`, mirroring the existing `transform_on_error`
  vocabulary in `streaming.py`):
  - `warn` (default): record + alert, run still succeeds.
  - `fail`: mark the run **failed** (`RunRecord.status="failed"`, `failed_step="quality"`),
    skip `transforms_post`, fire `quality_failed`.

### 5.3 Authoring API (new decorator kwargs)
```python
from ematix_flow import pipeline
from ematix_probe import Tester   # re-exported as ematix_flow.quality.Tester

@ematix.pipeline(
    target=DimCustomer,                      # ManagedTable → auto not_null(PK-cols)/unique(PK)
    mode="scd2",
    expectations=lambda t: (                 # Callable[[Tester], None] — same builder ematix-probe uses
        t.column("email").not_null().regex(r".+@.+\..+"),
        t.column("status").is_in(["active", "churned", "trial"]),
        t.row_count(at_least=1_000),
    ),
    on_quality_failure="fail",               # "warn" | "fail"   (default "warn")
    freshness_sla="6h",                       # str duration | timedelta | None
    freshness_column="updated_at",            # optional; else uses run-completion time
)
def dim_customer(conn): ...
```
- `expectations` accepts the **same `Tester` builder callable** that `ematix-probe`'s
  `@probe.data` and `probe_from_table(extend=…)` take — so there's one mental model.
- Auto-derivation: when `target` is a `ManagedTable`, we call
  `probe_from_table(target, source=<target DSN>, extend=expectations)` so PK→`unique` and
  non-nullable→`not_null` come for free; `expectations` extends them.
- All kwargs are **opt-in**; omitting them leaves the pipeline behavior byte-for-byte unchanged.

### 5.4 Freshness SLO — the non-trivial part
A freshness SLO must fire **when the pipeline is NOT running**. Two evaluation triggers:
1. **At run-end** (cheap): after a successful run, if `freshness_column` is set, run
   `ematix-probe`'s `freshness(col, within=sla)` assertion against the target; otherwise record
   `finished_at` as the freshness anchor.
2. **On a schedule** (the important one): a **`FreshnessEvaluator`** invoked from the scheduler
   tick (`scheduler/loop.py`) reads each SLA-bearing pipeline's **last successful
   `RunRecord.finished_at`** (via `RunHistoryStore`) and compares `now - finished_at` to the SLA.
   On breach it fires `sla_breached` and records the state — with **de-dupe** (one alert per
   breach edge, not every tick).

This is exposed as `flow freshness-check` (one-shot, for external cron) and wired into the
built-in scheduler loop.

---

## 6. Data model

### 6.1 In-run capture (no migration)
Reuse `RunRecord.extras` (`run_log/history.py` L54–93; surfaced by `to_detail_dict()`):
```python
extras["quality"] = {
    "verdict": "fail",                 # pass | warn | fail | error
    "checks_total": 6, "checks_failed": 2,
    "assertions": [                    # trimmed AssertionResult list
        {"name": "email.not_null", "verdict": "fail",
         "message": "1 NULL value(s) in column 'email'"},
        {"name": "row_count", "verdict": "pass", "message": None},
    ],
    "duration_seconds": 0.42,
}
extras["freshness"] = {"sla_seconds": 21600, "lag_seconds": 900, "state": "healthy"}
```

### 6.2 Durable store (web UI history/trend)
Extend the existing web analytics store (`web/analytics_store.py`, currently schema **v4**;
bump to **v5** — same `CREATE TABLE IF NOT EXISTS` + `_SCHEMA_VERSION` pattern used for
saved_queries/charts/dashboards/alerts):

```sql
-- v5
CREATE TABLE IF NOT EXISTS quality_runs (
    id            TEXT PRIMARY KEY,
    pipeline      TEXT NOT NULL,
    run_id        TEXT,                     -- FK to run_history (nullable for scheduled freshness)
    verdict       TEXT NOT NULL,            -- pass | warn | fail | error
    checks_total  INTEGER NOT NULL DEFAULT 0,
    checks_failed INTEGER NOT NULL DEFAULT 0,
    detail_json   TEXT NOT NULL DEFAULT '[]',
    started_at    TEXT NOT NULL,
    finished_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_qr_pipeline ON quality_runs(pipeline, finished_at);

CREATE TABLE IF NOT EXISTS freshness_state (
    pipeline      TEXT PRIMARY KEY,
    sla_seconds   INTEGER NOT NULL,
    lag_seconds   INTEGER,
    state         TEXT NOT NULL,            -- healthy | warning | breached | unknown
    last_success  TEXT,
    evaluated_at  TEXT NOT NULL
);
```
> Note: the engine process must not import pyarrow (dual-mimalloc SIGSEGV — see
> `SQL_LAB_DASHBOARDS.md`). The store is stdlib `sqlite3` only; `ematix-probe` returns plain
> dataclasses, so this constraint is naturally satisfied.

---

## 7. Web UI

Reuse everything the analytics surface already established (RBAC middleware, `create_app`,
`lib/api.js`, status-pill CSS).

- **Endpoints** (`web/server.py`, after the `/api/dag` block; RBAC-gated like the rest):
  - `GET /api/quality?pipeline=&limit=&offset=` → recent `quality_runs`.
  - `GET /api/freshness?pipeline=` → `freshness_state` rows (or fold into `/api/pipelines`
    under a `freshness` key so cards get it for free).
- **Frontend:**
  - New nav link + `routes/Quality.svelte` (`#/quality`) — reuse the sortable `Runs.svelte`
    table shape: pipeline · verdict pill · checks failed/total · last-checked. Row → assertion
    detail (reuse the drill-down modal from the analytics work).
  - **Freshness badge** folded into `routes/Pipelines.svelte` card footer (next to median
    duration): `healthy | warning | breached` using the existing `.status--*` color tokens.
  - `lib/api.js`: `listQualityChecks(...)`, `getFreshnessStatus(...)` (thin fetch wrappers).
  - RBAC: viewer reads Quality/freshness; editor+ can (later) manage checks. Gate with the
    existing `can($me, perm)` helper.

---

## 8. Alerting

Extend `alerters/__init__.py` `AlertEvent.kind` with `"quality_failed"` and `"sla_breached"`
(currently `failed | gave_up | recovered`). Add `elif` format branches to `SlackAlerter`,
`EmailAlerter`, `PagerDutyAlerter`, `StdoutAlerter`. Fire from:
- the quality stage (on `fail`, and on `warn` if `alert_on_warn=True`), and
- the `FreshnessEvaluator` (on the healthy→breached edge only; de-duped).

Message payload names the pipeline, the failing assertion(s), and observed-vs-expected
(`ematix-probe`'s `AssertionResult.message` already provides this, e.g.
`"row_count 50000 < at_least 100000"`).

---

## 9. Phased delivery (TDD)

Each phase is independently shippable and gated by a failing-test-first cycle.

### Phase 0 — Spec, dep, scaffolding  ·  ~0.5 day
- Add `ematix-probe>=0.1.1` as an **opt-in extra**: `pyproject.toml` `[quality]` extra
  (keeps the base install lean; mirrors `[web]`).
- `python/ematix_flow/quality.py` module skeleton: re-export `Tester`; define
  `QualityPolicy`, `QualityResult` adapter types; `evaluate_expectations(...)` /
  `evaluate_freshness(...)` signatures (raise `NotImplementedError`).
- Failing unit tests for the adapter contract.
- **Exit:** contracts compile; red tests exist.

### Phase 1 — Expectations engine (post-write, table-level)  ·  ~2–3 days
- Thread `expectations`, `on_quality_failure` kwargs through `decorators.py` → `sync()`.
- New quality stage in `pipeline.sync()` after write / before `transforms_post`: build the
  probe (`probe_from_table` when target is a `ManagedTable`, else a bare `Tester`), `.run()`,
  map `RunReport` → `extras["quality"]`, enforce `warn`/`fail`.
- Fire `quality_failed` on `fail` (Phase 8 alerters extended minimally here).
- CLI: `flow run` prints a quality summary line; `RunRecord.extras` populated.
- **Tests (TDD):** unique/not_null/row_count against a DuckDB + Postgres target; `warn` lets
  the run pass, `fail` marks it failed and skips `transforms_post`; no-kwargs pipeline
  unchanged; auto-derivation from a PK/non-null `ManagedTable`.
- **Exit:** expectations enforced end-to-end on batch pipelines, opt-in, green.

### Phase 2 — Freshness SLOs  ·  ~2–3 days
- `freshness_sla`, `freshness_column` kwargs; at-run evaluation via `ematix-probe.freshness`.
- `FreshnessEvaluator` reading last-success from `RunHistoryStore`; `flow freshness-check`
  one-shot command; wire into `scheduler/loop.py` tick with **breach-edge de-dupe**.
- Fire `sla_breached`; write `freshness_state`.
- **Tests (TDD):** stalled pipeline (no recent run) breaches; recovered pipeline clears; SLA
  met stays healthy; alert fires once per breach edge, not per tick.
- **Exit:** freshness enforced even when pipelines don't run.

### Phase 3 — Web UI surfacing  ·  ~2–3 days
- analytics_store **v5** (`quality_runs`, `freshness_state`) + CRUD/read methods + writes from
  Phases 1–2.
- `/api/quality`, `/api/freshness` endpoints (RBAC-gated).
- `routes/Quality.svelte` + nav link; freshness badge in `Pipelines.svelte`; `lib/api.js`
  wrappers; status-pill CSS.
- **Tests:** endpoint unit tests (RBAC gating, pagination); a Svelte smoke check via the
  existing preview harness.
- **Exit:** operators see quality + freshness in the UI.

### Phase 4 — Alerter polish & docs  ·  ~1 day
- Full `quality_failed` / `sla_breached` format branches across all four alerters; `alert_on_warn`.
- Docs: `docs/USER_GUIDE.md` section + ematix.dev `how-to/web-ui.mdx` Quality/Freshness section
  + a screenshot (reuse the puppeteer capture flow from the analytics work).
- **Exit:** feature documented and alert-complete.

**Rough total:** ~8–11 working days across 5 phases. Phases 1 and 2 are the load-bearing
engine work; 3 is surfacing; 0/4 are thin.

---

## 10. Risks & open questions

| Risk / question | Mitigation / proposed answer |
|---|---|
| **Source DSN derivation** — can every target connection produce a `source.*` for the probe? | Postgres/DuckDB/Parquet/S3 map directly. Warehouse targets (Snowflake/BQ/Redshift) have **no** `ematix-probe` source yet → **out of scope Phase 1**; document as unsupported, revisit. |
| **Post-write "fail" means data already landed.** | Acceptable for v1 (assert + alert + mark-failed). True pre-publish gating = staging-swap, deferred (§11). Call this out in docs. |
| **Probe adds latency to every run.** | Pushdown keeps it in-DB and fast; make the stage skippable via `EMATIX_SKIP_QUALITY=1`; log stage duration. |
| **Freshness double-fire** (at-run + scheduled). | Single source of truth = `freshness_state` table; scheduled evaluator owns alert edges; at-run only updates state. |
| **`ematix-probe` API drift** (it's v0.1). | Pin `>=0.1.1,<0.2`; adapter layer (`quality.py`) isolates flow from probe's surface. |
| **Version-match trap** on the extra. | Follow the `[patch.crates-io]`/pin discipline already noted for sibling repos. |

---

## 11. Deferred / follow-on (explicitly out of scope)
1. **Row-level quarantine / DLQ routing** of failing rows (compose with the existing
   streaming DLQ + the new batch DLQ-replay work).
2. **Staging-then-atomic-swap** "block before publish" gating.
3. **Volume-anomaly / distribution-drift** detection with historical baselines (needs the
   `quality_runs` trend table this project creates — natural next step).
4. **Warehouse-target support** (Snowflake/BigQuery/Redshift sources in `ematix-probe`).
5. **Column-level lineage** tie-in (separate gap; a failed check could point at upstream).

---

## 12. Success metrics
- A pipeline can declare expectations + freshness in ≤5 lines and get enforcement + UI + alerts.
- Zero overhead when unused (byte-identical behavior; verified by test).
- A stalled pipeline breaches its freshness SLO and pages within one scheduler interval.
- Docs + screenshot shipped in both repos.

---

## 13. Rollout
- Opt-in extra `pip install "ematix-flow[quality]"`; kwargs default to no-op.
- `EMATIX_SKIP_QUALITY` kill-switch env var.
- Land behind the normal PR flow; the web pieces reuse the already-shipped RBAC gate.
