# ematix-flow

**The fast, declarative way to move data — without standing up
Spark or Airflow.**

ematix-flow is a Python library for moving data between
databases (Postgres, MySQL, SQLite, DuckDB), files (Parquet,
CSV, JSON, ORC, Delta Lake — local disk or S3), and streaming
sources (Kafka, Pub/Sub, Kinesis, RabbitMQ). Declare a target
table and a load strategy — append, merge, or slowly-changing
dimension — and the Rust + Apache Arrow engine handles
correctness: schema evolution, watermarks, at-least-once
delivery, change-data-capture.

**5.87× faster than single-node PySpark on TPC-H** (SF=1,
geomean across all 22 queries). No JVM, no cluster, no
scheduler — one `pip install`.

## Where to start

- **[User guide](USER_GUIDE.md)** — step-by-step for every surface.
- **[Roadmap](ROADMAP.md)** — what's still on the todo list, by
  priority.
- **[GitHub repo](https://github.com/ryan-evans-git/ematix-flow)** —
  source, issue tracker, discussions.

## What it is

Three complementary surfaces in one repo:

1. **Declarative table management for Postgres** — the original v0.1
   scope. Decorator-driven schemas, normalization markers, SCD2 with
   event-time, run history, watermarks, post-load transforms,
   polars/pandas/pyspark interop, ML feature store. **Stable.**

2. **Multi-backend streaming pipelines** — Phases 30–38. Source from
   any of 4 streaming backends, write to any of 6 storage backends,
   manual offset commits, dead-letter patterns, Confluent Schema
   Registry support, long-running `flow consume` daemon binary.
   **Stable.**

3. **Stream processing** — Phase 39. DataFusion-backed mid-stream
   SQL transforms, tumbling / hopping / session windows with
   watermark-driven emit + late-data handling, keyed time-windowed
   stream-stream inner joins backed by a durable Postgres
   `StateStore` for crash-recoverable state + atomic offset
   commits. **Recently shipped.**

All three share a common Rust core that does Arrow record-batch IO
under the hood.

## Install

```sh
pip install ematix-flow
# Or: pip install "ematix-flow[df]"     for polars/pandas helpers
# Or: pip install "ematix-flow[spark]"  for pyspark helpers
pip install pyarrow                    # required for streaming pyclasses
```

## Tests

- 428 Rust core lib unit tests
- 81 Rust CLI lib unit tests
- ~80 Rust testcontainers integration tests (`--ignored`; opt-in
  Docker)
- ~313 default Python tests + ~196 testcontainers-gated Python
  tests

clippy + fmt clean on stable Rust.

## Phase status

| Phase | What | Status |
|---|---|---|
| 0–14 | v0.1 declarative Postgres | ✅ |
| 15–20 | ML feature store | ✅ |
| 21–25 | Decorator API | ✅ |
| 26–28 | Normalization + transforms | ✅ |
| 30–33 | Multi-backend DBs | ✅ |
| 34 | Object stores | ✅ |
| 35 | Delta Lake | ✅ |
| 36–37 | Streaming sources/targets | ✅ |
| 38 | `flow consume` CLI | ✅ |
| Py.1–Py.6 | Python streaming bindings | ✅ |
| 39.1–39.3 | SQL transforms + lookups | ✅ |
| 39.4 | Tumbling + hopping windows | ✅ |
| 39.5a | Session windows + `StateStore` | ✅ |
| 39.5b | Stream-stream join | ✅ |
| Iceberg | Iceberg target backend | ⏸ deferred (arrow ABI) |
| Unified API | Single declaration surface | ⏸ design only |

See **[Roadmap](ROADMAP.md)** for remaining work.

## License

[Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0)
