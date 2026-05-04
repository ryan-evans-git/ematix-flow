# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Nothing pending — see [`docs/ROADMAP.md`](docs/ROADMAP.md) for the
prioritized list of remaining work.

## [0.1.0] — 2026-05-04

First public release.

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

### Tests

- 459 Rust core lib unit tests
- 108 Rust CLI lib unit tests
- ~80 Rust testcontainers integration tests (`--ignored`; opt-in
  Docker)
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
