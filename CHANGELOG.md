# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

(no entries yet — anything landing on `main` after v0.6.0 goes here)

## [0.6.0] — 2026-05-21

Workflow + Job model — the previous flat "Pipelines" page becomes
**Workflows** (user-named groupings) on top of **Jobs** (individual
tasks). The DAG between jobs lives on the workflow declaration, not
on individual jobs. Existing `@ematix.pipeline` code keeps working as
single-job workflows-of-one, so this is a non-breaking model upgrade.

### Added

- `@ematix.workflow(name=..., jobs=[...], depends_on={...})` — new
  decorator-style call that registers a named group of jobs plus the
  DAG edges between them. The depends_on dict reads as
  `{downstream: [upstream, ...]}`. Edges are mirrored into the legacy
  per-job depends-on table so the scheduler keeps gating downstream
  jobs on upstream freshness without changes.
- `@ematix.job(...)` — alias for `@ematix.pipeline(...)`. Both names
  resolve to the same decorator; new code should prefer `.job` so the
  Python surface matches the Workflow/Job terminology the UI uses.
- `flow web --module <name>` (repeatable) — imports a pipelines
  module into the web-server process so the UI can render schedules,
  next-run times, and the DAG view without a separate scheduler tick
  having to populate the rich-history first.
- `/api/workflows` endpoint — returns declared workflows + their
  member jobs and DAG edges. Jobs not assigned to any workflow are
  surfaced as synthetic `kind: "single"` workflow-of-one entries so
  the UI doesn't need a separate "orphan jobs" code path.
- Pipelines API now surfaces `next_run_at` for batch jobs by
  forecasting from the registered cron + timezone when no scheduler
  tick has populated the rich-history yet. Streaming jobs continue
  to render `LIVE STREAMING` instead.

### Changed

- **Web UI** is reorganised around the new model:
  - **Workflows** tab (default) — one card per workflow with the
    member jobs laid out as an inline flowchart. Click any node or
    the workflow title to refocus the full DAG view on it.
  - **Jobs** tab — flat list of individual jobs (this is the
    previous "Pipelines" page; the cards, last-10-strip,
    next-run-at, and streaming throughput footer are unchanged).
    Adds filter inputs (name, kind, latest status) and sort
    buttons (name / kind / status / next / duration).
  - **Runs** tab — renamed from the previous "Jobs" tab; same run
    history table. Column headers are now clickable to sort by
    pipeline / status / started / duration / attempt.
  - **DAG** tab — same data, rendered as an SVG flowchart with
    cubic-Bézier arrows from each upstream to each downstream. The
    rank-as-column layout is gone; topological order is now
    expressed by arrow direction. `#/dag/<job>` focuses the
    subgraph on a single job's ancestors + descendants.
  - Loopback bind no longer requires a bearer token by default.
    Set `--token <secret>` (or `EMATIX_FLOW_WEB_TOKEN`) explicitly
    when binding to a non-loopback address.
- The pipeline DAG sub-component is extracted to
  `web-ui/src/lib/DagFlowchart.svelte` so the Workflows card
  preview and the full DAG view share one renderer. SVG sizes to
  natural node-grid dimensions and only scales down when the
  container is narrower than the canvas — single-job cards no
  longer fill the panel.

### Migration notes

- Existing `@ematix.pipeline(depends_on=[...])` declarations
  continue to work. Their edges show up on the Workflows page as
  `kind: "single"` cards that link to the focused DAG, exactly like
  jobs without any declared workflow.
- To group existing jobs into a named workflow: drop the per-job
  `depends_on=` kwargs and add one `ematix.workflow(...)` call that
  enumerates the member jobs + the DAG between them.
- URL bookmarks for `#/pipelines` are auto-redirected to `#/jobs`.

## [0.5.0] — 2026-05-21

Operational milestone — adds the user-facing surface (CLIs, Web UI,
alerters, observability) on top of v0.4.0's backend matrix. Same
query-execution surface as v0.4.0; per-query TPC-H times unchanged.
Highlights: four new CLI subcommands (`flow doctor` / `init` / `logs`
/ `secrets test`), bearer-token Web UI auth + cross-pipeline DAG
view, email + PagerDuty alerters, OTEL trace spans + a starter
Grafana dashboard, AWS Glue Schema Registry end-to-end Kafka
dispatch, Arrow-native warehouse adapters, streaming pipeline live
throughput in the Web UI, and the Rust executor for
`@ematix.warehouse_pipeline` via the new PyO3 callback bridge.

### Added

- **`@ematix.warehouse_pipeline` decorator** (Phase 2d slice 1, #125).
  Wires `WarehouseSource` / `WarehouseTarget` into the
  scheduler-registered pipeline registry so warehouse-shaped pipelines
  participate in cron scheduling, retries, `depends_on` DAG, and
  `flow run-due` the same way DB-backed `@ematix.pipeline` pipelines
  do. The wrapped function is zero-arg; returning a `str` forwards
  it as `transform_sql=` to `run_warehouse_pipeline` (DuckDB transform
  in-flight on the Arrow table). Slice 2 ships in v0.5.0 across
  #135 (PyO3 callback bridge) + #136 (Rust `invoke_warehouse_pipeline`
  executor); slice 3 adds warehouse-side watermark cursors.
- **AWS Glue Schema Registry — end-to-end** (#126 + #135). Slice 1
  (#126) shipped the typed `GlueSchemaRegistryConnection` (kind
  `glue_schema_registry`, fields `registry_name` / `region` / auth
  via `aws_profile=` / explicit static creds / boto3 default chain),
  the Rust `glue_schema_registry` module with the Glue wire format
  (`0x03` header + 16-byte UUID + 1-byte compression byte) exposed as
  `parse_glue_frame` / `build_glue_frame` / `GlueFrame` / `GlueCodec`,
  and the `[schema-registry-glue]` extra (boto3 +
  aws-glue-schema-registry). Slice 2 (#135) wired the Rust Kafka
  backend to dispatch on registry kind for both consumer and
  producer paths via a `SchemaRegistryKind::{Confluent, Glue {…}}`
  enum, added per-backend schema caches (one boto3 round-trip per
  UUID / per schema name), zlib codec (`GlueCodec::Zlib`,
  byte 0x05) using flate2, producer-side encode
  (`encode_batch_as_glue_avro`) via Arrow → JSONL → Avro, kafka
  connection-time validation (Glue + non-Avro fails at construction),
  and a LocalStack integration suite at
  `tests/python/integration/test_glue_localstack.py` (gated on
  `EMATIX_FLOW_LOCALSTACK_ENDPOINT`). Confluent path
  (`kind = "schema_registry"`) unchanged.
- **PyO3 callback bridge** (#135, task #559 slice 2). Process-global
  registry of named Rust callbacks at
  `ematix_flow_core::py_callbacks` (`register` / `unregister` /
  `is_registered` / `invoke`, JSON-in / JSON-out adapter). Concrete
  Python wiring in `ematix-flow-py::py_callbacks` exposes
  `register_python_callback` / `unregister_python_callback` /
  `is_python_callback_registered` / `invoke_python_callback`. The
  Glue Kafka backend routes schema lookups (by-UUID for consumers,
  by-name for producers) through this primitive; same primitive
  carries the warehouse-pipeline executor in #136.
- **Rust executor for `@ematix.warehouse_pipeline`** (#136,
  task #559 slice 2 final piece). New
  `ematix_flow_core::warehouse_executor::invoke_warehouse_pipeline(name)`
  dispatches a registered warehouse pipeline by name through
  `py_callbacks`, no subprocess. Python side: the
  `@ematix.warehouse_pipeline` decorator now registers every wrapped
  function as a callback at
  `ematix_flow.warehouse_pipeline:<pipeline_name>` so the Rust
  scheduler / worker can drive it directly. Three error variants
  surface common failure modes (`NotRegistered` /
  `CallbackFailed` / `BadResponseShape`); response shape is the
  same dict the in-process scheduler builds (`status` / `pipeline` /
  `rows_read` / `rows_written` / `duration_ms` / `watermark`).
- **`flow init` project scaffold** (#136). `flow init <dir>` writes
  a runnable starter project: `pipelines.py`, `connections.toml`,
  `Dockerfile`, `flow.service` (systemd unit), `.gitignore`,
  `README.md`. Refuses to overwrite without `--force`. Maven-archetype
  shape — one command to a working `flow run-due` loop.
- **`flow logs <run_id>`** (#136). Tails the captured stdout / stderr
  for a given run. Capture is opt-in via the
  `EMATIX_FLOW_CAPTURE_LOGS=1` env var (so existing deployments see
  no disk / latency change). Logs land at
  `$EMATIX_FLOW_LOGS_DIR/<run_id>.log`
  (default `~/.ematix-flow/logs/`); `run_id` is pinned to
  `<pipeline>-<UTC ts>-<attempt>` so the same record matches the
  RunLog. Tee-based capture (original stream still gets the bytes);
  atomic write (tmp + rename) + 30-day prune helper.
- **`flow doctor`** (#136). Connection health probes by kind:
  postgres (`SELECT 1`), kafka (TCP bootstrap probe), glue
  (`list_registries`), pubsub (`get_topic`), rabbitmq (AMQP
  handshake), s3 (`head_bucket`), snowflake / bigquery (`SELECT 1`).
  Renders a one-pass status table; non-zero exit on any failure so it
  fits CI / pre-deploy checks.
- **`flow secrets test`** (#136). Resolves every `${…}` placeholder
  in `connections.toml` (or whichever path is passed) and reports
  per-secret outcome (provider, key, OK / missing / error) without
  printing the resolved value. Useful for validating Vault / AWS /
  GCP secret-store wiring before a deploy.
- **Web UI bearer-token auth** (#136). New `--token <value>` flag on
  `flow web` (and `bearer_token=` kwarg on `create_app` /
  `run_server`) gates every `/api/*` route except `/api/health`
  behind `Authorization: Bearer <token>`, compared with
  `hmac.compare_digest`. `/api/health` stays open for load-balancer
  probes. When a token is set, the "non-loopback bind without auth"
  warning at startup is suppressed.
- **Cross-pipeline DAG view in the Web UI** (#136). New `/api/dag`
  endpoint returns `{nodes, edges}` from the `depends_on` registry
  (each node carries `name` / `schedule` / `timezone`); new Svelte
  route `#/dag` lays nodes out in topological-rank columns
  (upstreams always left-of downstreams) with fan-out counts.
- **Email + PagerDuty alerters** (#136). Two new alerter URL
  schemes register through the same `--alerter <url>` flag the
  Slack / webhook / stdout alerters use:
  - `email://user:pass@host:port?from=...&to=...&starttls=1` — stdlib
    `smtplib`; default port 587 (STARTTLS) / 465 (implicit SSL).
    Errors are caught + logged (an alerter outage never breaks the
    pipeline run).
  - `pagerduty://<routing_key>?service=...&severity=...` — Events
    API v2 trigger / resolve, with `dedup_key = "<service>:<pipeline>"`
    so a recovered pipeline auto-resolves its open incident. Maps
    `failed → trigger(error)`, `gave_up → trigger(critical)`,
    `recovered → resolve`.
- **OpenTelemetry trace spans for pipeline runs** (#136). New
  `ematix_flow.tracing` module: `pipeline_run_span(name, attempt)`
  context manager wraps every `@ematix.pipeline` /
  `@ematix.warehouse_pipeline` execution; configure once via
  `configure_tracer_from_url(...)` with `otel://stdout`,
  `otel+otlp+grpc://collector:4317`, or
  `otel+otlp+http://collector:4318`. Span attributes include
  pipeline name, attempt number, run_id, status, and exception
  on failure. Sits alongside the existing OTel-metrics export — same
  collector can receive both.
- **Streaming-pipeline live stats in the Web UI** (#136). Streaming
  pipelines used to show a useless "Median duration: —" on the
  Pipelines view (one open-ended record, no duration). The
  ``flow consume`` daemon now opens an optional RunLog
  (``--run-log-url`` / ``$EMATIX_FLOW_RUN_LOG_URL``) and spawns a
  background thread that scrapes its own ``/metrics`` endpoint every
  ~30s, computing rolling 1m + 5m windows from
  ``ematix_streaming_rows_consumed_total`` /
  ``ematix_streaming_rows_written_total`` /
  ``ematix_streaming_batches_total`` /
  ``ematix_streaming_errors_total`` and writing the result into the
  running record's ``extras``. ``/api/pipelines`` surfaces these
  fields; ``Pipelines.svelte`` renders "Throughput: X rps in (1m) /
  Y rps in (5m) · Batch cycle: A ms avg (1m)" in place of the median
  footer when ``kind == "streaming"``. SqliteRunLog now also
  implements the rich-history protocol
  (``record_run_record`` / ``list_runs`` / ``get_run`` against a
  new ``run_records`` table), so the same SQLite file backs both the
  ``flow consume`` daemon and ``flow web``.
- **Grafana dashboard JSON** (#136). New
  `examples/grafana/ematix-flow-dashboard.json` — 6-panel starter
  board driven by the Prometheus metrics ematix-flow already
  exports: runs/min by outcome, success-rate stat, in-flight retries,
  p50 / p95 / p99 duration, per-pipeline run rate, top-20 failure
  counts. `$pipeline` templated variable. Import in Grafana via the
  JSON Model field.
- **Cron schedule timezone support** (#127, task #558 slice 1).
  `is_due()` now accepts a keyword-only `tz=` argument that, when
  set, interprets the cron expression in that timezone instead of
  whatever timezone `now` carries (effectively UTC for
  `flow run-due` today). Accepts a `zoneinfo.ZoneInfo` instance or a
  tz name string. DST transitions are honored via croniter's
  zoneinfo-aware path. `tz=None` (the default) preserves today's
  behavior bit-for-bit. Slice 2 wires `timezone=` into
  `@ematix.pipeline` / `@ematix.warehouse_pipeline`; slice 3 surfaces
  the configured tz in the Web UI's "Next: …" rendering.

### Design

- **Arrow-native warehouse adapters** (#128, task #557). Three-slice
  plan to drop pandas from the Snowflake / BigQuery / Redshift
  adapter paths. Slice 1 = Snowflake PUT + COPY INTO via parquet
  staging; slice 2 = Redshift S3 + COPY (extend merge-mode path to
  append); slice 3 = BigQuery GCS + `load_table_from_uri`. Open
  questions on type fidelity, Storage Write API vs GCS staging, and
  the backward-compat shim policy captured for review before slice 1
  implementation lands. See
  `docs/PHASE_557_ARROW_NATIVE_WAREHOUSES.md`.

## [0.4.0] — 2026-05-20

Alpha milestone — closes the ematix.dev "What's not shipped" list.

### Added — closing "What's not shipped"

- **Pluggable secret stores** (Phase 1). `${...}` interpolation
  now supports provider prefixes: bare `${VAR}` (env, unchanged),
  `${vault:path#key}` (Vault KV v2), `${aws:secret#field}` (AWS
  Secrets Manager JSON), `${gcp:secret#version}` (GCP). Register
  via `ematix_flow.secrets.register_resolver(prefix, resolver)`.
  Extras: `[secrets-vault]` / `[secrets-aws]` / `[secrets-gcp]` /
  `[secrets]`.
- **Snowflake / BigQuery / Redshift connections** (Phase 2).
  Typed connection dataclasses + `*_query_to_arrow` adapters
  with `${...}` interpolation and repr redaction. Extras:
  `[snowflake]` / `[bigquery]` / `[redshift]` / `[warehouses]`.
- **Warehouse pipeline orchestrator** (Phase 2b).
  `Source.snowflake_query` / `bigquery_query` / `redshift_query`
  factories + `WarehouseTarget` classmethods + `run_warehouse_pipeline`
  for end-to-end read → DuckDB SQL transform → bulk write.
  `snowflake_write_arrow` (write_pandas), `bigquery_write_arrow`
  (load_table_from_dataframe), `redshift_write_arrow` (S3-staged
  COPY) ship under the same extras.
- **Distributed peer auto-detection** (Phase 3). `peers = [...]`
  accepts three schemes (mix freely): `http://host:port` (static,
  unchanged), `dns://host:port` (A-record lookup),
  `k8s://service.namespace:port` (sugar for
  `*.svc.cluster.local`). Resolution at backend open via
  stdlib `ToSocketAddrs`.
- **`engine = "auto"` + default switch** (Phase 3.5). Picks
  distributed when peers expand to ≥1 URL at startup, in-process
  otherwise (with an `info!` log). Default when `engine` is
  absent is now `"auto"` (was `"datafusion"`); identical behavior
  for configs without `peers`.
- **Web UI** (Phase 4). New `flow web` CLI subcommand serves a
  FastAPI + Svelte SPA matching ematix.dev's Pip-Boy theme at
  `http://127.0.0.1:8080/`. Read endpoints (`/api/runs`,
  `/api/runs/:id`, `/api/pipelines`) + mutating endpoints
  (`/restart`, `/rerun`, `/pause`, `/resume`). Localhost-only by
  default; binding off-host logs a loud warning. Extras: `[web]`.
  - New `RunHistoryStore` Protocol (separate from `RunLog`) with
    `list_runs` / `get_run` / `record_run_record` / `enqueue_restart`
    / `enqueue_rerun` / `set_pause` / `pending_actions` /
    `consume_requested_run`.
  - In-memory impl shipped; scheduler loop's `_pickup_pending_actions`
    walks "requested" rows + dispatches via the existing executor,
    carrying `EMATIX_FLOW_RESTART_FROM_STEP` /
    `EMATIX_FLOW_RERUN_FULL` / `EMATIX_FLOW_PRIOR_RUN_ID` env vars.
  - `PauseChecker` helper for worker-side pause/resume at step /
    batch / watermark boundaries.
- **Warehouse Rust-side type shape** (Phase 2c).
  `Dialect::Snowflake` / `BigQuery` / `Redshift` + the matching
  `*Config` structs. Full Rust dispatch (PyO3 bridge into the
  Python adapters) is Phase 2d.

### Changed

- **Status: alpha** (was pre-alpha). PyPI classifier was already
  "Development Status :: 3 - Alpha"; ematix.dev's status banner,
  nav pill, and `01-advantages.mdx` updated to match.

### Added

- **Startup banner** on long-running CLI commands. `flow run`,
  `flow consume`, and `flow run-due` now print an ANSI-shadow
  "EMATIX" block-letter banner with version + tagline to **stderr**
  on launch. JSON results on stdout are unaffected, so existing
  `flow run … | jq` pipelines keep working. Suppressed by default
  on non-TTY streams (CI logs, captured subprocesses); honors
  `EMATIX_FLOW_NO_BANNER=1` to silence even on a TTY and
  `EMATIX_FLOW_BANNER=1` to force on. Quick read-only subcommands
  (`list`, `validate`, `preview`, `connections …`) stay quiet.

## [0.2.1] — 2026-05-10

Sdist-publish fix. **No functional code changes** — wheels remain
bit-equivalent to v0.2.0; sdist now packages correctly so source-
build platforms (Windows, Linux ARM/musl, macOS x86_64) install
again. Cut because v0.2.0's PyPI publish job failed on the sdist
upload with HTTP 400 (`License-File LICENSE does not exist in
distribution file`) — the eight v0.2.0 wheels are live, but no
v0.2.0 sdist.

### Fixed

- **Sdist now ships the `LICENSE` file**. `pyproject.toml`
  switched from the deprecated `license = { text = "Apache-2.0" }`
  form to PEP 639's `license = "Apache-2.0"` + `license-files =
  ["LICENSE"]`. Maturin reads the latter, includes the listed
  files in the sdist tarball, and emits matching `License-File:
  LICENSE` + `License-Expression: Apache-2.0` in the sdist's
  PKG-INFO. PyPI's PEP 639 metadata validation passes.

  Locally verified: `maturin sdist` → tarball contains
  `ematix_flow-0.2.1/LICENSE`, PKG-INFO declares `License-File:
  LICENSE` matching the actual archive entry.

### Changed

- README "Status" line bumped to **v0.2.1 on PyPI** with a
  one-line summary of what landed across the 0.2 series
  (`@udf`/`@udaf`, Phase Δ CDC, object-store streaming source,
  Σ.A2 dialect translator).

## [0.2.0] — 2026-05-10

Major feature release. **Headline**: Python `@udf` / `@udaf`
decorators for in-pipeline UDFs, plus the full **Phase Δ**
change-data-capture surface (Postgres, MySQL, SQLite, DuckDB,
Delta Lake CDC executors), object-store as a streaming source,
Σ.A2 SQL dialect translator (Spark / DuckDB → DataFusion, 103/103
TPC-DS PASS), and Π.5 unified streaming-API knobs. 43 commits
since v0.1.2.

### Added

- **Python `@udf` + `@udaf` decorators — user-defined functions
  in `transform_sql`.** The full DataFusion UDF surface, now
  Python-native. Closes the "what if my math isn't in DataFusion's
  stdlib" gap (cumulative-normal CDF for Black-Scholes greeks,
  volume-weighted average price, custom percentiles, financial
  day-count conventions, …).
  - `@ematix_flow.udf(args=..., returns=...)` wraps a Python
    callable as a DataFusion `ScalarUDF`. Per-batch dispatch
    through PyArrow zero-copy — one PyO3 GIL acquisition +
    PyArrow round-trip per *batch* (typically thousands of rows),
    so vectorised `numpy` / `pyarrow.compute` inside the
    callable amortises the overhead. Argument and return types
    are DataFusion `DataType` strings (`"Int64"`, `"Float64"`,
    `"Utf8"`, `"Boolean"`, etc.); unsupported types raise
    `ValueError` at decoration time. Mismatched call sites
    surface at plan-compile time.
  - `@ematix_flow.udaf(args=..., state=..., returns=...)` wraps
    a Python *class* as a DataFusion `AggregateUDF`. DataFusion
    instantiates one accumulator per group; the class must
    expose `update_batch` / `merge_batch` / `evaluate` / `state`
    methods matching the `Accumulator` trait. `evaluate()` and
    `state()` return length-1 PyArrow Arrays of the declared
    types so Rust can round-trip them back to `ScalarValue`
    without per-type glue code. Useful errors when the returned
    dtype doesn't match the declaration. VWAP is the canonical
    example.
  - `run_streaming_pipeline(udfs=[...], aggregate_udfs=[...])`
    threads handles through the whole pipeline:
    `PyHandle` → `Vec<Arc<ScalarUDF | AggregateUDF>>` →
    `ConsumeOptions` → `streaming_config_with_lookups_udfs_aggregate_udfs_and_metrics`
    → `LazySqlTransform::new_with_lookups_udfs_and_aggregate_udfs`
    → `SessionContext::register_udf` / `register_udaf`.
  - Pure-Rust escape hatch unchanged: implement `ScalarUDFImpl`
    / `AggregateUDFImpl` and pass `Arc<…UDF>` directly into
    `DataFusionTransform::new_with_lookups_udfs_and_aggregate_udfs`
    or `LazySqlTransform`'s matching constructor. Same wire — no
    GIL round-trip when contention dominates.
  - Coverage: 7 scalar UDF tests + 7 aggregate UDF tests in
    `tests/python/test_python_udf{,_udaf}.py` (Int round-trip,
    multi-arg positional, realistic Black-Scholes call delta,
    realistic VWAP over options ticks, naming, dtype-mismatch
    diagnostics, `run_streaming_pipeline` kwarg surface check).
    Rust unit tests in `transform.rs` cover the
    `SessionContext::register_udf` / `register_udaf` path +
    duplicate-name rejection at construction. CLI unit tests
    (`streaming_config_threads_udfs_into_lazy_sql_transform` +
    `streaming_config_threads_aggregate_udfs_into_lazy_sql_transform`)
    lock the streaming wiring.
  - README + `docs/USER_GUIDE.md` both lead with the
    Black-Scholes example for `@udf` and the VWAP example for
    `@udaf`. PRs #33–#40.

### Changed

- **CI: switched Python dep installs to `uv`.** `astral-sh/setup-uv@v4`
  in `.github/workflows/ci.yml` + `.github/workflows/docs.yml`
  with `enable-cache: true` and an explicit
  `cache-dependency-glob` (the repo doesn't commit a `uv.lock`,
  so the default `**/uv.lock` glob errored). Heavy scientific
  wheels (numpy, pyarrow) install dramatically faster than pip.
  `numpy` + `pyarrow>=15` are now pinned explicitly in the CI
  install step — both were transitive deps of other CI tools
  before, but the new UDF tests import them at module level so
  the dependency is now declared.

### Added

- **CI: integration-tests + coverage gate workflow.**
  `.github/workflows/integration.yml` runs `cargo llvm-cov`
  across the whole workspace with `--include-ignored` so the
  100+ testcontainer-gated tests (Postgres CDC, MinIO/S3 Delta,
  cross-pod distributed, etc.) are exercised in CI for the first
  time — previously only the unit-test set ran via `ci.yml`.
  - **Coverage measurement (no hard gate yet)**. After 6 rounds
    of unit-test backfill (commits 18d62ed..bcff28b — 41 new
    tests across cli/lib.rs, backend.rs, delta_backend.rs,
    session_blob.rs, transform.rs, objectstore date helpers)
    local measurement is at **86.54% line coverage** with the
    same scope (excluding PyO3 wrappers + entrypoint binaries),
    up from 84.84% baseline. The user's stated target is 90%;
    `--fail-under-lines` is deliberately NOT set on the workflow
    until that threshold lands — enforcing a below-target floor
    creates noise on PRs without bringing real correctness
    benefit. The workflow runs cargo-llvm-cov, prints the
    summary, and uploads the lcov artifact for human review.
    Path to 90%: a single Postgres testcontainer test exercising
    every Arrow column type closes ~500 lines simultaneously
    across backend.rs/pg.rs/duckdb_backend.rs/mysql_backend.rs's
    COPY-BINARY type-binding paths; windowed.rs internal-state
    edges close another ~100.
  - **paths-ignore filter**: the integration workflow skips
    when only LICENSE / NOTICE / docs / markdown changes — so a
    license-or-docs-only PR doesn't pay the 30-min testcontainer
    spin-up cost.
  - **Excluded from the coverage denominator**:
    `ematix-flow-py/src/*.rs` (PyO3 wrappers covered only via
    `pytest`, invisible to `cargo-llvm-cov`),
    `cli/src/main.rs` and `distributed/src/bin/flow_worker.rs`
    (process entrypoints — thin wrappers around library code
    that *is* covered).
  - **Triggers**: every PR + push to main + nightly schedule +
    `workflow_dispatch`. Adding the
    `integration tests + coverage (ubuntu-latest)` job to the
    `main` branch's required-status-checks is the next step
    after one successful run lands the check name in GitHub's
    discoverable list.

### Added

- **Phase Δ — CDC source mode**. Streaming pipelines can now
  treat each Kafka batch as a CDC envelope and apply per-event
  changes to a Postgres mirror table.
  - `[transform.cdc]` TOML block + `CDC(envelope="debezium")`
    Python dataclass — peer-equivalent paths into the same
    `CdcConfig`. Mutually exclusive with `[transform.window]` /
    `[transform.join]` / a SQL pre-stage; cross-validated at
    config-load.
  - Debezium + Maxwell + custom envelopes. Custom requires every
    field path + `op_map` set explicitly; the validator names
    what's missing.
  - `Backend::run_cdc` trait method + Postgres impl. Per-batch
    transactional with prepared-statement reuse across same-op
    events; UPSERT / UPDATE / DELETE all coerce JSON → row types
    via `jsonb_populate_record(NULL::<table>, $1::jsonb)`.
    `delete_mode = "soft"` flips a configured column instead of
    DELETE.
  - **Idempotency gate**: per-`(pipeline, pk_json)` last-seen-ts
    tracked in `ematix_flow.cdc_idempotency`. Single-round-trip
    `INSERT … ON CONFLICT DO UPDATE … WHERE … RETURNING 1`
    inside the executor's transaction — Kafka redeliveries are
    suppressed atomically with the data write. Surfaced via
    `ematix_streaming_cdc_idempotent_skipped_total` so absorbed
    redeliveries are visible to operators.
  - **Schema-evolution detection**: default `Skip` warns once
    per drift column per batch then lets Postgres's coercion
    discard the unknown key; `Fail` returns an error and rolls
    the batch back transactionally. `AlterTable` deferred — see
    plan.
  - **Streaming-runtime dispatch wiring**: when `[transform.cdc]`
    is set, the per-batch loop routes through `Backend::run_cdc`
    instead of the universal `write_arrow_stream` append path.
    Target schemas are reflected once at startup via the new
    `Backend::reflect_table_spec` trait method (Postgres impl
    via `information_schema.columns`); non-CDC pipelines never
    pay the reflection round-trip.
  - **Five new Prometheus counters** under `pipeline=<name>`:
    `ematix_streaming_cdc_creates_total`,
    `ematix_streaming_cdc_updates_total`,
    `ematix_streaming_cdc_deletes_total`,
    `ematix_streaming_cdc_skipped_total`,
    `ematix_streaming_cdc_idempotent_skipped_total`.
  - **`examples/cdc-debezium/`**: docker-compose stack
    (Postgres source + Debezium + Kafka + Postgres mirror) with
    a connector-registration helper and a step-by-step README.
  - **`docs/USER_GUIDE.md` § "CDC source mode (Δ)"**: full
    surface — TOML + Python + envelopes + cross-validation +
    metrics + multi-target reach.
  - Plan: [`docs/PHASE_DELTA_CDC_PLAN.md`](docs/PHASE_DELTA_CDC_PLAN.md).
    PRs 1–6 shipped + a PR 5.5 ("wire dispatch into the runtime")
    that filled a gap discovered during PR 6 scoping.

Multi-target reach (Delta Lake, DuckDB / SQLite / MySQL, object
stores, streaming targets) is catalogued as Phase Δ extensions
and unshipped — see the plan's "Phase Δ extensions" section.

- **Δ.X1 PR 1 — Delta Lake CDC target.** Single-MERGE-per-batch
  CDC apply on `DeltaBackend` via `deltalake::DeltaOps::merge`.
  Three branches dispatch on a synthesized `__op` Utf8 column:
  `when_matched_delete` for `__op = 'd'`, `when_matched_update`
  for c/u/r overwrite, `when_not_matched_insert` for the
  INSERT-when-absent path. Within-batch dedupe by primary key
  keeps the highest-`ts_ms` event so `c` then `u` for the same
  row collapses to one INSERT carrying the post-image of the `u`.
  - Schema evolution: `Skip` rides Delta's
    `with_merge_schema(true)` for auto-evolution (cleaner than
    Postgres's "warn + drop"); `Fail` pre-flights against the
    spec and aborts before MERGE with the offending column named.
  - Soft-delete: replaces `when_matched_delete` with a
    `when_matched_update` that flips the configured column to
    `current_timestamp()`. Reported as `updates`, not `deletes`.
  - Reflection: `Backend::reflect_table_spec` on `DeltaBackend`
    returns columns from Delta's Arrow schema. PK info is **not**
    surfaced — Delta tables don't carry PK constraints natively
    and the kernel crate gates `Metadata.configuration` behind an
    `internal_api` macro. Direct callers of `Backend::run_cdc`
    (hand-built `TableSpec` with PK info) work; streaming-runtime
    auto-dispatch waits on Δ.X1.2.
  - Tests: 4 new tempdir-rooted unit tests cover multi-batch
    dispatch counters, within-batch dedupe, soft-delete, and
    Fail-policy abort.
  - Residual gaps documented inline: between-batch idempotency
    (Δ.X1.1 — needs `_cdc_last_ts` hidden column or sidecar
    table), Numeric column-type support on the source-batch path.
- **Δ.X1.2 — user-declared PK threading for streaming-runtime
  CDC dispatch.** The Δ.X1 PR 1 reflect path returns Delta
  columns with `primary_key = false` (Delta tables don't carry
  PK constraints natively, and the kernel crate's
  `Metadata.configuration` is gated behind an `internal_api`
  macro). Δ.X1.2 routes around it: users declare the PK on the
  target spec and the streaming runtime augments the reflected
  spec before dispatching to `Backend::run_cdc`. Three
  equivalent surfaces:
  - `[target.table].primary_key = ["id"]` TOML field on every
    table-bearing target kind (Postgres / MySQL / SQLite /
    DuckDB / DeltaLocal / DeltaS3). Lowering hooks land via
    `PipelineCliConfig::target_primary_keys()`.
  - `Target(primary_key=["id"])` field on the typed-Python
    streaming spec; emitter writes it into the rendered TOML.
  - `target_primary_key=["id"]` kwarg on
    `run_streaming_pipeline` for the legacy single-target shape.
  - `StreamingPipeline::ensure_cdc_target_specs` validates each
    declared column against the live reflected schema and fails
    loud on a typo, naming the offending column.
  - 2 new dispatch-wiring unit tests cover augmentation +
    typo-detection. CLI parse test locks the new TOML field.
  - Postgres CDC unaffected: declaration is optional; reflection
    already surfaces PK info, augmentation matches existing PK
    flags rather than overriding them.
- Python 3.14 support. CI matrix and the wheel-build matrix in
  `release.yml` now include `cp314-cp314` for both
  `linux-x86_64` (manylinux_2_28) and `macos-aarch64` (Apple
  Silicon). `pyproject.toml` carries the matching trove
  classifier. Python 3.14 went stable in October 2025; pyo3 0.28
  in the workspace already supports the 3.14 ABI. The next tag
  push will publish 3.14 wheels and end the source-build fallback
  for users on the current stable Python.

### Changed

- CI Python matrix expanded from `{3.11, 3.12}` to
  `{3.11, 3.12, 3.13, 3.14}` so every Python we publish a wheel
  for is covered by the test suite. Previously 3.13 wheels shipped
  unexercised by CI.

## [0.1.2] — 2026-05-06

Documentation polish only. **No functional changes** — the wheel
+ sdist contents are bit-equivalent to v0.1.1 modulo the embedded
README. Cut so the PyPI project page picks up two specific
clarifications + the SEO scaffolding shipped between releases.

### Changed

- README Quickstart 1 now shows explicit connection wiring at
  the top of the snippet (`@ematix.connection class warehouse:
  kind = "postgres"; url = "${EMATIX_FLOW_DSN}"` plus
  `target_connection="warehouse"` on the pipeline). The
  previous version started directly with `@ematix.table` and
  the `conn` argument arrived without a visible source —
  readers had to scroll to the "Configuring connections"
  section below to figure out where the DB handle came from.
- README's "Multi-backend, write once" bullet no longer
  implies TOML is the only switching mechanism. Both the
  decorator (`target_connection=` argument) and the TOML
  field compile to the same `BackendConfig` enum + identical
  Rust execution path; pick whichever fits the workflow.
  The previous wording read "Switching the target of a
  pipeline is a TOML one-liner" — accurate but misleading.

### Added (infrastructure)

- `overrides/main.html` mkdocs partial template wired via
  `theme.custom_dir: overrides` in `mkdocs.yml`. Reserves
  `<head>` slots for Google Search Console + Bing Webmaster
  verification meta tags, currently commented out via Jinja
  `{# … #}` syntax (so the deployed HTML stays empty until
  real tokens land).
- `docs/googleaa93d0535024bf3c.html` — Google Search Console
  verification file. Served at the site root via mkdocs's
  static-passthrough; Google fetches it to confirm ownership
  of the GitHub Pages docs site.

## [0.1.1] — 2026-05-06

Metadata + documentation polish only. **No functional changes** —
the wheel + sdist contents are bit-equivalent to v0.1.0 modulo
the embedded `pyproject.toml` / `Cargo.toml` description fields
and the README. Cut so the PyPI project page reflects the
corrected pitch (v0.1.0's PyPI metadata is locked once published).

### Changed

- Package descriptions no longer single out Postgres. The
  pyproject `description` and the `ematix-flow-core` crate
  `description` now name the full backend surface — SQL
  databases (Postgres, MySQL, SQLite, DuckDB), object stores +
  Delta Lake (Parquet, CSV, JSON, ORC, local FS or S3), and
  streaming sources (Kafka, RabbitMQ, Pub/Sub, Kinesis) — and
  cite the 5.87× SF=1 22-query TPC-H DataFusion-vs-PySpark
  geomean.
- `pyproject.toml` keywords broadened: dropped Postgres-only
  framing, added `datafusion`, `delta-lake`, `parquet`,
  `kafka`, `data-pipeline`.
- README headline rewritten to lead with *why* (declarative,
  single-binary footprint, multi-backend write-once, correct by
  default, faster than PySpark single-node) before the feature
  inventory. New "Why ematix-flow" section. "What it is"
  expanded from three to four surfaces — Σ.B distributed batch
  SQL was missing.
- Stale "Status: alpha" + "On PyPI once wheel-build CI tasks
  land" lines removed; replaced with current "v0.1.0 on PyPI"
  status.

## [0.1.0] — 2026-05-05

First public release.

> **TPC-H headline:** at SF=1, ematix-flow's single-node DataFusion
> path is **5.87× faster than PySpark `local[*]`** geomean across
> all 22 TPC-H queries (range 1.78× to 16.74×). At SF=10 on the
> representative set (Q1/Q3/Q6/Q19), geomean **3.3× faster**. Same
> M3 Pro, same Snappy Parquet, same SQL. Single-host only —
> cross-host distributed-plan numbers stay deferred (no real
> cluster hardware in this project's runway). Full method +
> per-query bootstrap CIs in
> [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

### Added

#### Declarative table management for Postgres (Phases 0–14)

- `@ematix.table(schema=...)` decorator for declaring target tables
  with PEP-593 `Annotated` markers (`pk()`, `natural_key()`,
  nullable, type widths).
- `@ematix.pipeline(target=, schedule=, mode=, ...)` decorator for
  declaring loads. Modes: `append`, `truncate`, `merge`/`scd1`,
  `scd2` (with optional event-time `valid_from` and TTL expiry).
- Same-DB short-circuit (`INSERT … SELECT`) plus cross-DB Arrow
  streaming bridge via `COPY BINARY` staging — auto-detected,
  force-overrideable.
- Watermarks (`ematix_flow.watermarks`) + run history
  (`ematix_flow.run_history`). Restart-safe: watermark advances
  only after successful commit.
- Normalization markers (`trim`, `lower`, `empty_to_null`,
  `parse_timestamp`, `default`, `parse_int`, `regex_replace`,
  `derive`, raw `sql`). All compile to in-database SQL.
- Pipeline-level `transforms_pre=[deduplicate_by, filter_where, ...]`.
- Post-load `transforms_post=[sql_string, callable, transform_ref]`,
  each in its own transaction with optional `continue_on_failure_post`.
- `flow` CLI: `list`, `run`, `run-due`, `preview`, `dry-run`,
  `validate`, `transform list/run`, `connections list/check/set`.
- Connection registry: env vars (`EMATIX_FLOW_DSN_<NAME>`) +
  `~/.ematix-flow/connections.toml` with `${VAR}` interpolation.

#### ML feature store (Phases 15–20)

- `@ematix.feature_view` decorator for point-in-time feature views.
- PIT join helpers, online materialized view, training-set builder.
- DataFrame interop via `ematix-flow[df]` extra (polars / pandas).

#### Decorator API ergonomics (Phases 21–25)

- Multi-target fan-out (`targets=[ematix.target(...), ...]`).
- `transforms_pre` / `transforms_post`.
- `column_map` / `source_table` for static-source pipelines without
  hand-rolled SQL.
- `__merge_keys__` / `__unique_constraints__` resolution chain.

#### Multi-backend support (Phases 30–37)

- DB targets: MySQL, SQLite, DuckDB (Phases 31–33).
- Object-store targets: Parquet / CSV / ORC / JSONL on local FS or S3
  (Phase 34).
- Delta Lake target with DataFusion-backed MERGE on local FS or S3
  (Phase 35a–f).
- Streaming sources / targets: Kafka with SASL/PLAIN, SCRAM, mTLS,
  AWS MSK IAM, Confluent Schema Registry-aware Avro/Protobuf;
  RabbitMQ; GCP Pub/Sub; AWS Kinesis (Phases 36–37).
- Manual offset commit / ack across all four streaming sources
  (at-least-once with idempotent targets).
- App-level DLQ (`dead_letter_topic`) plus broker-level DLQ
  (RabbitMQ `x-dead-letter-exchange`, Pub/Sub `dead_letter_policy`).
- Kafka exactly-once via transactions + `KafkaToKafkaEosPipeline`
  (Phase 36j).

#### `flow consume` daemon (Phase 38)

- Long-running consumer binary: `flow consume <toml>`.
- Prometheus `/metrics` endpoint via `--metrics-port`.
- Restart-on-error supervisor with exponential backoff
  (`--restart-on-error`, `--max-backoff-ms`, `--max-restarts`).

#### Python streaming bindings (Phases Py.1–Py.6)

- `run_pipeline(config=...)` in-process runner.
- pyclass wrappers for each streaming backend.
- `ArrowBatchIter` lazy iterator over PyArrow `RecordBatch`es.
- Typed connection objects (`KafkaConnection`,
  `PostgresConnection`, etc.) — preferred over name-string lookup.

#### Stream processing (Phase 39)

- **39.1–39.3**: DataFusion-backed mid-stream SQL transforms
  (`[transform.sql]`). Static lookup tables loaded from any DB
  backend at startup. Refreshing lookups via `refresh_interval_ms`
  with atomic `MemTable` swap.
- **39.4**: Tumbling + hopping windows. 9 aggregators including
  HLL+ approximate + exact `count_distinct`.
  `late_data = "drop"`, `"reopen"` (with `allowed_lateness_ms`
  retention + re-emit on dirty), and `"dlq"` (rows past budget
  routed through `dead_letter_topic`). Idle-tick emission.
  Multi-source `min`-with-idleness watermark.
- **39.5a**: Session windows with mandatory `max_session_duration_ms`
  hard cap. Postgres- or in-memory-backed `StateStore` with postcard
  wire format and forward-only state-version migrations. Per-emit
  atomic state+offsets commit. Periodic dirty-only checkpoint
  ticker (default 60s) so long-idle pipelines still flush state.
  `seek_to` on Kafka and Kinesis (per-shard sequence numbers);
  Pub/Sub + RabbitMQ accepted via broker-tracked offsets. Recovery
  rehydrates per-key session state on restart.
- **39.5b**: Keyed time-windowed stream-stream joins across two
  `[[sources]]`. Reuses the 39.5a `StateStore` with side-prefixed
  keys. Per-source `BatchContext::source_id` routing. Inner +
  outer (LEFT / RIGHT / FULL) joins, `late_data="reopen"` for
  retained-buffer late-row matching, asymmetric time windows
  (`min_delta_ms` / `max_delta_ms`).
- Per-batch `transform_on_error = "fail" | "drop" | "dlq"` policy
  for transform failures.
- Lookup schema-drift detection on refresh — fails fast with a
  schema diff rather than swapping in a bad MemTable.
- `InMemoryStateStore` emits a `tracing::warn!` at config-load
  when paired with a session window or stream-stream join.

#### Unified pipeline API — Π.1 / Π.3 / Π.1.4 / Π.5

- **Π.1 — typed connections + advanced knobs in Python.**
  `SchemaRegistryConnection` is a typed connection alongside
  Kafka / Postgres / etc.; `KafkaConnection.schema_registry`
  accepts an instance or a registered SR name (with HTTP Basic
  auth via `basic_auth_user` / `basic_auth_password`).
  `payload_format` and `schema_registry_url` now plumb through the
  streaming TOML emitter (was silently dropped in the
  `run_streaming_pipeline` path before). `Watermark(lateness_ms=,
  source_idleness_ms=)` exposes the previously-hardcoded watermark
  knobs. `transform_on_error` exposed on
  `run_streaming_pipeline` / `@ematix.streaming_pipeline`.
- **Π.3 — `flow consume --module`.** `@ematix.streaming_pipeline`
  registers into a process-global name-keyed registry; the Python
  `flow` CLI gains `flow consume --module my_pipelines <name>` and
  `flow consume-list --module my_pipelines`. Implemented in the
  Python entry point (no PyO3 added to the Rust CLI binary).
- **Π.1.4 — object-store per-format options.** `Target` accepts
  `parquet_compression="zstd"` (or snappy / gzip / uncompressed),
  `csv_delimiter=";"`, `csv_header=False`. Plumbed through
  `ObjectStoreBackend::with_write_options` →
  `ObjectWriteOptions { parquet_compression, csv_delimiter,
  csv_header }`. Typed-Python boundary catches mis-shaped combos
  (Parquet option on CSV target, etc.) before TOML round-trip.
- **Π.5 — inline-credentials deprecation warning.** `flow consume
  <toml>` emits a `tracing::warn!` when the TOML carries inline
  DSNs / passwords (`postgres://user:pw@...`,
  `sasl_plain_password`, `secret_access_key`,
  `schema_registry_basic_auth_password`, RabbitMQ `amqp_url` with
  userinfo). Each finding includes a migration pointer to the
  connection registry. Silenced by `EMATIX_FLOW_NO_DEPRECATION=1`.
  Removal one minor release later.
- **Kafka SASL / MSK-IAM through the streaming TOML.** The
  `KafkaConnection` SASL fields (`sasl_plain_*`, `sasl_scram_*`,
  `msk_iam_region`) are now emitted by `_source_fields` /
  `_target_fields`; the CLI's `SourceConfig::Kafka` /
  `TargetConfig::Kafka` accept and validate them, applying via
  `KafkaBackend::with_sasl_plain` / `with_sasl_scram` /
  `with_msk_iam`.
- **SR basic-auth Rust plumbing.** `SrAuth` value type bundles SR
  URL + optional credentials; threaded through every SR helper
  (`encode_batch_as_avro`, `decode_payloads_as_avro`,
  `encode_batch_as_protobuf`, `decode_payloads_as_protobuf`).
  `SrSettings::set_basic_authorization` applied when the auth
  pair is configured.

#### Σ.A1 — single-node DataFusion baseline + TPC-H harness

- `tpch_generate` example: pure-Rust SF=1/10/100/1000 Parquet
  generator via `tpchgen 2.0.2`. Idempotent; data dir is
  `.gitignore`d.
- `tpch_extract_queries` example: dumps all 22 TPC-H spec
  queries to `examples/tpch/queries/qNN.sql` with the
  canonical TPC-H validation-set parameters substituted in.
  One-source-of-truth for both the Rust + PySpark bench
  harnesses.
- `tpch_22_audit` example: plans + executes all 22 against
  SF=1, reports per-query rows + timings. Confirms 22/22
  queries plan + execute on DataFusion 53.1 with no SQL-
  surface gaps.
- `cargo bench -p ematix-flow-core --bench tpch`: criterion
  harness covering all 22 TPC-H queries. Group label derived
  from data-dir basename (`sf1`, `sf10`, ...) so multi-SF runs
  don't clobber each other's history.
- `tpch_q6_tune` example: MemTable isolation + EXPLAIN ANALYZE
  breakdown. Localises the 1.82× Polars-on-Q6 gap to parquet
  decode (not aggregate compute); DataFusion's aggregate is
  faster than Polars's once decode is amortised.
- `scripts/bench-tpch-pyspark.py`: PySpark `local[*]` bench
  driver covering all 22 queries; works on JDK 17 / 21 / 23.
- `scripts/bench-tpch-polars.py`: Polars head-to-head on the
  same hardware.

#### Σ.A2 — SQL dialect translator

- `[transform] dialect = "datafusion" | "spark" | "duckdb"`
  (default `"datafusion"`, zero-cost passthrough). Translates
  source SQL → DataFusion SQL via `sqlparser`-rs.
- Spark dialect: function-name remap (~50 entries),
  `LATERAL VIEW EXPLODE → UNNEST`, `INTERVAL` literal rewrites,
  window-frame syntax. **103/103 PASS** on the canonical Apache
  Spark TPC-DS suite (no plan-time failures, no translate
  failures).
- DuckDB dialect: function-name remap. Audit shows DuckDB →
  DataFusion is essentially a no-op for the canonical query
  surface; translator value-add is on user-explicit DuckDB-isms
  (currently just `list_value`).

#### Σ.B — distributed batch SQL

Peer-to-peer distributed execution across ematix-flow processes;
no separate scheduler/executor binaries, no cluster service to
operate.

- `crates/ematix-flow-distributed`: library + `flow-worker`
  binary (`cargo run --bin flow-worker -- --port 50051`).
  Built on [`datafusion-distributed 1.0`](https://crates.io/crates/datafusion-distributed)
  (originally specced against Apache Ballista; pivoted mid-
  implementation when Ballista's DataFusion ^52 pin collided
  with the workspace's DataFusion 53 — see
  [`docs/PHASE_SIGMA_B_TRAIT_SPIKE.md`](docs/PHASE_SIGMA_B_TRAIT_SPIKE.md)
  PR 2 pivot block).
- `[transform] engine = "datafusion" | "distributed"` +
  `peers = ["http://flow-01:50051", ...]`. Default is
  `"datafusion"` (in-process).
- `BackendConfig` tagged-enum + `Backend::config()` trait
  method + `backend_from_config(cfg)` reverse constructor.
  All 10 in-tree backends migrated. **Connector trait
  refactor** absorbs Σ.A2 + Σ.B + Σ.D-prep needs in one
  pre-1.0 unified shape (the trait carries `'static` +
  `fn config(&self) -> BackendConfig`).
- Cross-pod `transform.lookups`: lookup tables registered on
  the coordinator are auto-broadcast to peer workers via
  Arrow Flight. Configure
  `[transform.lookups.<name>]` blocks alongside
  `engine = "distributed"` — they Just Work.
- TLS / mTLS for the worker mesh: `flow-worker` accepts
  `--tls-cert PATH --tls-key PATH [--tls-client-ca PATH]`;
  coordinator-side configurable via `[transform.tls]` TOML
  block (CA bundle + optional client identity for mTLS +
  optional SNI override) round-tripped through
  `BackendConfig`.
- Window+distributed and join+distributed combos rejected at
  config-load with a clear error pointing at the workaround
  (typed wrappers still hard-pin to in-process `LazySqlTransform`).
- Streaming-backend builder-state round-trip (Kafka, Kinesis,
  PubSub, RabbitMQ): `BackendConfig` carries every builder
  knob (auth, payload format, delivery semantics, batch
  config, etc.) so a backend reconstructed from JSON matches
  the original instance bit-for-bit.
- `examples/distributed-cluster/` docker-compose stack: 3
  `flow-worker` peers + Parquet volume mount, with optional
  `EMATIX_DISTRIBUTED_PEERS` env-var hook on the bench harness
  for pointing it at the compose stack.
- `as_postgres()` doc-hidden + contract-pinned (PostgresBackend's
  cross-DB COPY BINARY fast path retained as an internal
  escape hatch; full removal stays a deferred follow-up).

#### Σ.C — TPC-H head-to-head vs PySpark

- `docs/BENCHMARKS.md` Σ.C extension: 22-query SF=1 single-
  node DataFusion-vs-PySpark head-to-head. **5.87× geomean
  DataFusion speedup** (range 1.78× Q19 to 16.74× Q22).
  21/22 queries see a ≥3× DataFusion win.
- Σ.C PR 2 SF=10 multi-process baseline: 3 in-process
  workers + docker-compose stack target. **3.3× geomean
  DataFusion-over-PySpark** on the representative set
  (Q1/Q3/Q6/Q19). Distributed-of-one and 3-worker configs
  within ±13% of single-node DataFusion (multi-host scaling
  story stays deferred — single-host loopback gives no
  hardware isolation).
- Top-of-`BENCHMARKS.md` paste-into-HN TL;DR header.
- `infra/cloud-init-worker.sh` + `infra/README.md`: AWS EC2
  bootstrap recipe for users with cluster access (retained
  but not canonical — this project's published numbers are
  M3 Pro single-host).
- `scripts/tpch-bench-multi.sh` + `scripts/tpch-bench-multi-summarize.py`:
  multi-engine driver feeding criterion + PySpark output to
  a markdown comparison table.

#### Σ.D — distributed streaming spike

- [`docs/PHASE_SIGMA_D_SPIKE.md`](docs/PHASE_SIGMA_D_SPIKE.md):
  research-level evaluation of four candidate paths (Arroyo,
  RisingWave, Denormalized, DIY on Arrow Flight + the
  existing `state_store/`). Recommendation: **defer Σ.D
  until demand**. Watching brief on Denormalized (right
  architectural shape, pre-1.0 / 0 releases). Trigger
  conditions documented for picking Σ.D back up.

### Tests

- 459 Rust core lib unit tests
- 124 Rust CLI lib unit tests
- 27 backend-config scaffold round-trip tests across all 10
  backends + the distributed config + TLS config
- 22-query TPC-H audit: 22/22 PASS at SF=1
- 103-query TPC-DS Spark-dialect audit: 103/103 PASS plan-time
- `crates/ematix-flow-distributed/tests/cross_pod.rs`:
  in-process N-worker integration tests covering distributed
  SQL transform + cross-pod lookup broadcast
- `crates/ematix-flow-distributed/tests/cross_pod_tls.rs`:
  end-to-end TLS test using `rcgen` to mint a self-signed
  CA + leaf cert per run; covers both server-auth and mTLS
  paths
- ~80 Rust testcontainers integration tests (`--ignored`;
  opt-in Docker)
- 376 default Python tests + ~196 testcontainers-gated Python
  tests

clippy + fmt clean on stable Rust.

### Wheel matrix

- **Linux x86_64** (manylinux2014) — Python 3.11 / 3.12 / 3.13.
- **macOS aarch64** (Apple Silicon) — Python 3.11 / 3.12 / 3.13.
- **Source distribution** — included for every other platform
  (Python 3.10, Intel Mac, Linux aarch64, etc.). `pip install
  ematix-flow` falls through to the sdist; needs Rust + cmake
  locally for the build. `pyproject.toml` declares
  `requires-python = ">=3.10"`.
- **Windows + Intel Mac + Python 3.10 wheels are intentionally not
  built.** The Windows decision is structural (librdkafka +
  deltalake don't support Windows on pinned versions). Intel Mac
  wheels are dropped because GitHub's macos-13 runner pool is being
  phased out. Python 3.10 wheels are dropped to halve the macOS
  billing burn on the private repo (each macos-aarch64 build is
  billed at 10×). All three groups install from sdist when needed.

### Known limitations

See [`docs/ROADMAP.md`](docs/ROADMAP.md) for the full list.
Highlights of what's NOT in v0.1.0:

- **Cross-host distributed numbers.** All published benchmarks
  are single-host (loopback network, shared kernel/CPU/memory/
  disk). Real cross-host scaling claims need network-separated
  hardware (homelab k3s, rented bare-metal); the `infra/`
  recipe still works for AWS users.
- **Σ.D distributed streaming** — see the spike doc.
  Single-node streaming (Phase 39.4 / 39.5 / 39.5b) is fully
  shipped.
- **Static peer membership only.** `StaticWorkerResolver`
  carries a fixed `Vec<Url>`. Dynamic membership (k8s pods
  via DNS, service-mesh integration) is a follow-up.
- HLL+ approximate-mode `count_distinct` aggregator in stateful
  sessions. Exact-mode (`mode = "exact"`) is fully supported and
  HashSet-backed; HLL+'s register state lives in private fields
  upstream so postcard ser/de needs an upstream change or a fork.
- Iceberg target backend. Deferred until `iceberg-rust` 0.6+ for
  arrow 58 ABI parity.
- Object-store as a streaming source (today batch-only via
  `read_arrow_stream`).
- Streaming Parquet writes — today's writer buffers every batch
  in memory before emitting one Parquet file; for >GB inputs to
  S3, `object_store::buffered::BufWriter` would let it stream out
  in 5-MiB multipart-upload chunks.
- Iceberg-style transactional updates against object stores (use
  Delta for that today).

[Unreleased]: https://github.com/ryan-evans-git/ematix-flow/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/ryan-evans-git/ematix-flow/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/ryan-evans-git/ematix-flow/compare/v0.1.0...v0.4.0
[0.1.0]: https://github.com/ryan-evans-git/ematix-flow/releases/tag/v0.1.0
