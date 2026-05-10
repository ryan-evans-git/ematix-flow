# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/ryan-evans-git/ematix-flow/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ryan-evans-git/ematix-flow/releases/tag/v0.1.0
