# ematix-flow — Multi-backend, Multi-format, Streaming Plan

A planning doc parallel to `docs/NORMALIZATION_TRANSFORMS_PLAN.md` and
`docs/ML_FEATURE_STORE_PLAN.md`. Covers the architectural lift to add:

1. **DB backends**: SQLite, MySQL, DuckDB
2. **Object storage**: S3, Azure Blob, GCS — with file formats CSV,
   Parquet, ORC, JSON Lines
3. **Streaming**: Kafka source + sink, RabbitMQ, Pub/Sub, Kinesis
4. **In-memory**: DuckDB (in-memory), SQLite (`:memory:`)

The current Rust core is tightly coupled to Postgres (`pg.rs` is ~1600
lines of `tokio-postgres` calls and `ON CONFLICT` SQL strings). To
support these backends without a rewrite per backend, we build an
abstraction layer: a `Backend` trait + dialect-aware SQL generation +
Arrow as the universal IO contract.

---

## 1. Goals

- **One framework, many backends.** Same `@ematix.table`,
  `@ematix.pipeline`, `@ematix.feature_view`, `transforms_pre`,
  `transforms_post` whether the target is Postgres, S3 Parquet, or a
  Kafka topic.
- **Native fast paths preserved.** Same-backend pairs (PG → PG,
  MySQL → MySQL) keep their existing optimizations (COPY BINARY,
  LOAD DATA, etc.).
- **Cross-backend by default via Arrow streaming.** `Source` on backend
  A + `target` on backend B works without operator effort.
- **Streaming as a first-class differentiator.** Long-running consumer
  model with proper auth-renewal handling (especially MSK IAM), DLQ,
  exactly-once semantics — not just micro-batch via cron.
- **No SaaS lock-in.** Everything runs OSS; cloud-managed instances
  (AWS RDS, Aurora, Cloud SQL, MSK, Confluent Cloud, Azure Event Hubs)
  work via standard auth flows.

---

## 2. Locked design log

| ID | Question | Locked | Notes |
|---|---|---|---|
| Q1 | Storage model for object storage | **A** — Phase 34 ships append/truncate on raw files; Phase 35 adds Iceberg/Delta for merge/scd2. | No hand-rolled "rewrite-the-file"; raw files for the common case, Iceberg/Delta for transactional needs. |
| Q2 | Streaming source/sink model | **B** — long-running consumer from day one. | Real differentiator. `@ematix.streaming_pipeline` + `flow consume <pipeline>`. Phase 36 grows to 3–4w to include process supervision, lag metrics, DLQ, exactly-once semantics. |
| Q3 | Schema for schemaless backends | **B** — `ManagedTable` optional everywhere with inference fallback; opt-in `strict_schema=True`. | Consistent with Phase 27e `write_df` precedent. Drift caught via `flow validate` + opt-in strict mode. |
| Q4 | Packaging strategy | **C** — single `ematix-flow` package with all Rust DB backends compiled in; Python adapters (S3, Kafka, etc.) via `[extras]`. | Rust binary +20MB acceptable. Heavy native deps (boto3, librdkafka, JVM Spark) stay behind extras. |
| Q5 | Cross-backend sync | **D** — Arrow streaming bridge as the universal IO contract; native fast paths preserved for matched pairs. | Each backend implements `read_arrow_stream` + `write_arrow_stream`. RecordBatch streaming, never materializes full dataset in memory. |
| Q6 | Connection registry expansion | **C** — TOML canonical for any backend; DSN env vars stay as the shortcut for DB-shaped backends; `EMATIX_FLOW_<BACKEND>_<NAME>_<PROPERTY>` env-var convention for container deployments. | Resolution order: kwarg → env → TOML → error. Backwards-compatible with today's PG users. |

---

## 3. Architecture

### 3.1 The `Backend` trait (Rust)

```rust
pub trait Backend: Send + Sync {
    fn dialect(&self) -> Dialect;

    // Schema management.
    async fn ensure_table(&self, spec: &TableSpec, on_drift: OnDrift)
        -> Result<DriftResult, Error>;

    // Native-fast-path strategy execution (same-backend).
    async fn run_append(&self, spec: &TableSpec, source: SourceQuery, ...)
        -> Result<RunResult, Error>;
    async fn run_merge(&self, ...) -> Result<RunResult, Error>;
    async fn run_scd2(&self, ...) -> Result<RunResult, Error>;
    // ...

    // Universal Arrow IO (cross-backend).
    async fn read_arrow_stream(&self, query: SourceQuery)
        -> Result<ArrowBatchStream, Error>;
    async fn write_arrow_stream(
        &self,
        spec: &TableSpec,
        stream: ArrowBatchStream,
        mode: Mode,
        ...
    ) -> Result<RunResult, Error>;

    // Connection metadata.
    fn connection_info(&self) -> ConnectionInfo;
    fn dsn(&self) -> Option<String>;  // None for non-DSN backends
}

pub enum Dialect {
    Postgres,
    MySQL,
    SQLite,
    DuckDB,
    ObjectStore { format: FileFormat },
    Streaming { kind: StreamingKind },
}
```

Cross-backend dispatch:

```
pipeline.sync(source=Source.from(backend_a), target=Cls@backend_b, ...)
  → if backend_a == backend_b: backend_a.run_<mode>(...)        // native fast path
  → else:                       stream = backend_a.read_arrow_stream(...)
                                backend_b.write_arrow_stream(stream, ...)
```

### 3.2 Backend variants

| Backend | Native fast path | Arrow IO | Notes |
|---|---|---|---|
| `PostgresBackend` | COPY BINARY, ON CONFLICT, SCD2 SQL | `tokio-postgres` + `arrow-rs` adapter | Existing impl becomes the reference |
| `MySQLBackend` | LOAD DATA INFILE, ON DUPLICATE KEY UPDATE | `mysql_async` + arrow adapter | No partial indexes; SCD2 row hash via SHA2() built-in |
| `SQLiteBackend` | INSERT OR REPLACE, INSERT OR IGNORE | `rusqlite` + arrow | App-side SHA-256 for SCD2 (no extension) |
| `DuckDBBackend` | DuckDB native UPSERT | `duckdb-rs` (Arrow-native) | Easiest backend; great test target |
| `ObjectStoreBackend` | Multipart PUT for append/truncate | `object_store` crate + `parquet`/`arrow-csv` | No native merge/scd2 — Phase 35 (Iceberg/Delta) |
| `IcebergBackend` | Iceberg MERGE INTO | `iceberg-rust` | Phase 35; transactional merge/scd2 against object storage |
| `DeltaBackend` | Delta `MERGE INTO` | `deltalake-rs` | Phase 35; alternative to Iceberg |
| `KafkaBackend` (source) | Continuous consumer with auth refresh, manual commits | `rdkafka` + arrow | Phase 36; see §6 for MSK IAM constraint |
| `KafkaBackend` (sink) | Producer with batch + idempotent / exactly-once | `rdkafka` | Phase 36; mode='append' only |
| `RabbitMQBackend` | Standard AMQP | `lapin` crate | Phase 37 |
| `KinesisBackend` | KCL/KPL equivalents | `aws-sdk-kinesis` | Phase 37 |
| `PubSubBackend` | GCP Pub/Sub | `google-cloud-pubsub` | Phase 37 |

### 3.3 Connection registry (Q6 → C)

```toml
# ~/.ematix-flow/connections.toml

# DB-shaped — also expressible via env DSN
[connections.warehouse]
type = "postgres"
dsn = "postgres://user:pass@host/db"

[connections.analytics]
type = "duckdb"
path = "/var/data/analytics.duckdb"   # or ":memory:"

[connections.lake]
type = "s3"
bucket = "analytics-prod"
region = "us-east-1"
prefix = "ematix-flow/"
# Credentials via standard AWS chain (env / profile / IAM role / IRSA)

[connections.events_kafka]
type = "kafka"
brokers = ["b1.kafka:9092", "b2.kafka:9092"]
security_protocol = "SASL_SSL"
sasl_mechanism = "AWS_MSK_IAM"
oauthbearer_token_refresh_cb = "ematix_flow.kafka.msk_iam:refresh"
enable_auto_commit = false                              # MSK IAM safety
session_timeout_ms = 45000
client_dns_lookup = "use_all_dns_ips"
```

```sh
# Env-var equivalents (DB-shaped only via DSN)
export EMATIX_FLOW_DSN_WAREHOUSE=postgres://user:pass@host/db

# Per-backend per-property fallback for container deployments
export EMATIX_FLOW_KAFKA_EVENTS_BROKERS=b1.kafka:9092,b2.kafka:9092
export EMATIX_FLOW_KAFKA_EVENTS_SECURITY_PROTOCOL=SASL_SSL
```

Resolution order: explicit `connect(name=, type=, ...)` kwargs → env vars
→ TOML → error.

---

## 4. Phased rollout

### Phase 29 — Transport optimizations (≈3–5d)

Quick win, doesn't require the backend trait yet.

- Switch `Connection.write_df` from CSV `COPY` to `COPY BINARY` (~30%
  faster, smaller wire bytes). Use `pyarrow` to encode Arrow → PG
  binary.
- Add `compress: bool = False` opt-in to `pipeline.sync(force_path=
  "cross_db", compress=True)`. zstd-compress the wire bytes between
  source and target Pg pools. Decode on the target side.
- Tests: write_df throughput regression (smaller wire bytes), zstd
  cross-DB integration test, bench against uncompressed baseline.

### Phase 30 — Backend trait refactor (≈2w)

The architectural backbone. No new backend yet — just the abstraction.

- Define the `Backend` trait + `Dialect` enum + `ArrowBatchStream` type
  in `ematix-flow-core`.
- Refactor `pg.rs` to be `PostgresBackend: Backend`.
- Define cross-backend dispatch at the `pipeline.sync` boundary:
  match source.backend.dialect() == target.backend.dialect() →
  native fast path; else → Arrow streaming bridge.
- Implement `read_arrow_stream` / `write_arrow_stream` on
  PostgresBackend (uses tokio-postgres COPY → Arrow IPC).
- All existing tests keep passing — refactor is internal.

Exit: Postgres → Postgres still works through the new trait.
Internal-only change; no public-API impact.

### Phase 31 — DuckDB backend (≈3–5d)

Smallest new backend; validates the trait shape.

- `DuckDBBackend: Backend` using `duckdb-rs`.
- DuckDB is Arrow-native — `read_arrow_stream` / `write_arrow_stream`
  are 1:1 with DuckDB's native batch interface.
- Postgres-compatible SQL dialect for most cases; some quirks (no
  pgcrypto for SCD2 — use built-in `sha256()`).
- File-based + in-memory both supported.
- Tests: all five strategies (append/truncate/merge/scd1/scd2) against
  DuckDB; Postgres → DuckDB cross-backend via Arrow.

Exit: `Source.duckdb_query(conn, sql)` and `target_connection=
duckdb_conn` both work. First proof of cross-backend.

### Phase 32 — SQLite backend (≈5–7d)

- `SQLiteBackend: Backend` using `rusqlite`.
- Dialect quirks: `INSERT OR REPLACE`, `INSERT OR IGNORE`, no partial
  indexes (handle SCD2 `WHERE is_current` via app-side filter where
  needed), app-side SHA-256 for row hash (no built-in extension).
- Single-file durability.
- Tests: all strategies; Postgres → SQLite cross-backend.

### Phase 33 — MySQL backend (≈1–1.5w)

- `MySQLBackend: Backend` using `mysql_async`.
- Dialect quirks: `ON DUPLICATE KEY UPDATE` (different from PG), no
  partial indexes, JSON column type behavior differs, timestamp
  precision quirks, charset/collation handling.
- Native fast path: `LOAD DATA LOCAL INFILE` for append/truncate.
- Tests: all strategies; cross-backend pairs PG↔MySQL.

### Phase 34 — Object storage targets (raw files) (≈2–3w)

Q1 → A: append/truncate only on raw files.

- `ObjectStoreBackend: Backend` parameterized by store kind
  (`s3`/`azure`/`gcs`) + format (`parquet`/`csv`/`orc`/`jsonl`).
- `object_store` crate for the storage layer (handles S3/Azure/GCS
  with one trait).
- Format encoders: `parquet` crate for Parquet, `arrow-csv` for CSV,
  `arrow-orc` (or fall back to Python pyorc) for ORC, `serde_json` for
  JSON Lines.
- File naming: `<prefix>/<batch_id>.parquet` (uuid7 for sortable).
- `mode='append'` writes a new file; `mode='truncate'` deletes prefix
  contents (DELETE on empty prefix is no-op), then writes.
- `mode='merge' / 'scd1' / 'scd2'` raise NotImplementedError pointing
  at Phase 35 (Iceberg/Delta).
- Cross-backend: PG → S3 Parquet, MySQL → GCS CSV, etc.
- Tests against MinIO (via testcontainers).

### Phase 35 — Iceberg / Delta on object storage (≈2–3w)

Unblocks merge/scd2 against object storage with proper transactional
semantics, time-travel, and concurrent-writer correctness.

- `IcebergBackend: Backend` via `iceberg-rust` crate.
- `DeltaBackend: Backend` via `deltalake-rs` crate.
- Both backends support all five strategy modes — `MERGE INTO`,
  schema evolution, partitioning hints.
- Catalog support: AWS Glue, Hive Metastore, REST catalog, file-based.
- `__partition_columns__ = ("year", "month")` dunder for partitioned
  writes.
- Tests: full strategy matrix on both formats, cross-format reads
  (read Iceberg, write Delta).

### Phase 36 — Streaming consumer model (≈3–4w)

Q2 → B. **Real differentiator.** The big phase.

- `@ematix.streaming_pipeline(target=, source_topic=, group_id=,
  delivery=, ...)` decorator. Long-running consumer process.
- `flow consume <pipeline>` entry point. Process model:
  - Subprocess pool with one consumer per pipeline (or one per
    partition, configurable).
  - Supervised restart on crash with exponential backoff.
  - Graceful SIGTERM: stop polling → process in-flight batch →
    commit offset → exit.
- Consumer batching: configurable `batch_size`, `batch_window_ms`,
  `batch_bytes` — flush whichever fires first.
- **MSK IAM auth renewal (locked design constraint)**:
  - Use `oauthbearer_token_refresh_cb` to refresh tokens proactively
    (~80% of TTL), never reactively on auth failure.
  - **Manual offset commits only** (`enable.auto.commit=false`) so a
    disconnect during auth renewal can't accidentally commit offsets
    for messages we haven't actually written.
  - Test specifically: pause consumer, force token refresh, resume —
    verify zero message loss and exactly-once delivery semantics.
  - Document the MSK IAM gotcha prominently in CLI help and quickstart.
- Delivery semantics: `delivery="at_least_once"` (default) or
  `delivery="exactly_once"` (uses Kafka transactions; trades throughput
  for correctness).
- DLQ: `dead_letter_topic="..."` opt-in. Failed-batch records get
  produced there with original-offset metadata.
- Lag metrics exported as Prometheus on configurable port (`flow consume
  --metrics-port 9100`).
- Streaming sinks (writing TO Kafka): `mode='append'` produces source
  rows as messages; `mode='upsert'` uses log-compacted topics keyed by
  natural key. Other modes raise.
- Source format support: JSON, JSON Schema Registry, Avro Schema
  Registry, Protobuf Schema Registry, raw bytes.
- Exit: a Kafka topic can drive an SCD2 PG load with sub-minute
  latency; auth renewal is bulletproof under load.

### Phase 37 — RabbitMQ + Pub/Sub + Kinesis (≈1–2w)

Reuse Phase 36's streaming abstraction.

- `RabbitMQBackend` via `lapin`.
- `PubSubBackend` via `google-cloud-pubsub`.
- `KinesisBackend` via `aws-sdk-kinesis`.
- All slot into `@ematix.streaming_pipeline`.

### Phase 38 — In-memory backends polish (≈2d)

- DuckDB-in-memory and SQLite `:memory:` get test fixtures.
- Useful for unit tests that don't want a Docker container.
- `pipeline.sync(target=Cls, target_connection=":memory:")` shortcut.

---

## 5. Test strategy

The backend matrix multiplies the test count. To keep CI tractable:

- **Per-backend unit tests** — each backend ships its own test module
  validating the five strategy modes against a backend-specific test
  fixture (testcontainers PG/MySQL, in-memory SQLite/DuckDB, MinIO
  for object storage, embedded Kafka via `kafka-streams` for
  streaming).
- **Cross-backend smoke tests** — one canonical test per
  source-backend × target-backend pair: load a small dataset,
  verify roundtrip. Most pairs share Arrow streaming code so the cost
  scales linearly, not multiplicatively.
- **Marker-gated heavy tests**: `@pytest.mark.streaming`,
  `@pytest.mark.object_store`, `@pytest.mark.iceberg` for opt-in
  CI lanes.

CI matrix grows from 2 OS × 4 Python to roughly 2 OS × 4 Python × 3
backend-test-lanes (default / integration / streaming). Default lane
stays under 30s.

---

## 6. Documented design constraints (carry-forward)

- **MSK IAM auth renewal**: `oauthbearer_token_refresh_cb` proactive
  refresh, manual commits only, explicit "force refresh under load"
  test. Document prominently. (Phase 36)
- **Schema drift on schemaless backends**: opt-in `strict_schema=True`;
  default is inference. (Phase 34+)
- **No hand-rolled merge on raw object storage**: use Iceberg/Delta;
  reject raw-file merge with a clear pointer. (Phase 34/35)
- **Same-backend native fast paths preserved**: cross-backend Arrow
  bridge is the *fallback*, not the always-on path. (Phase 30+)

---

## 7. Out of scope (for now)

- Distributed query execution (DataFusion / Spark / Trino). Cross-
  backend works via Arrow streaming; "compute over the joined data"
  is the user's responsibility.
- Catalog services (Hive, Glue, Unity) beyond what Iceberg/Delta need.
- Automatic schema migration across backends. Drift detection per
  backend; user owns migration.
- A managed-service control plane (UI, multi-tenant RBAC).
- Real-time CDC (Debezium-shaped). The Kafka source covers the
  consumer side; the *production* side of CDC is upstream.

---

## 8. Estimate

| Phase | Effort | Dependency |
|---|---|---|
| 29 (transport opts) | 3–5d | none |
| 30 (backend trait) | 2w | 29 |
| 31 (DuckDB) | 3–5d | 30 |
| 32 (SQLite) | 5–7d | 30 |
| 33 (MySQL) | 1–1.5w | 30 |
| 34 (object storage raw) | 2–3w | 30 |
| 35 (Iceberg/Delta) | 2–3w | 34 |
| 36 (streaming, MSK IAM) | 3–4w | 30 |
| 37 (RMQ/PubSub/Kinesis) | 1–2w | 36 |
| 38 (in-memory polish) | 2d | 31, 32 |

**Total: ≈4–5 months for one engineer working steady.** Phases 31, 32,
34, 36 fan out in parallel after Phase 30 lands. Phase 36 is the
biggest single chunk and the strongest competitive differentiator.

Critical path for first user-visible win: 29 → 30 → 31 (DuckDB
cross-backend in ~3 weeks).
Critical path for streaming: 30 → 36 (~5–6 weeks).
