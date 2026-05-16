# Examples

Self-contained runnable examples. Each script is < 60 lines and
covers one shape.

| File | Shape | What you need |
|---|---|---|
| `01_append.py` | Declarative append into Postgres | local Postgres |
| `02_truncate.py` | Truncate-replace into Postgres | local Postgres |
| `03_merge.py` | MergeUpsert (SCD1) into Postgres | local Postgres |
| `04_scd2.py` | SCD2 with event-time into Postgres | local Postgres |
| `05_streaming_kafka_to_pg.toml` | Streaming Kafka → Postgres | local Kafka + Postgres |
| `06_windowed_tumbling.py` | Tumbling window aggregation | local Kafka + SQLite |
| `07_session_window.toml` | Per-user session windows + StateStore | local Kafka + Postgres |
| `08_stream_join.toml` | Keyed time-windowed stream-stream join | local Kafka + Postgres |
| **`09_streaming_clickstream/`** | **End-to-end Kafka → Postgres with bundled producer** — `make demo-streaming-*` | local Kafka + Postgres |
| **`10_workflow_dag/`** | **Workflow DAG + `flow scheduler` daemon + retries** — `make demo-workflow-*` | local SQLite only |
| **`11_s3_parquet_to_postgres/`** | **MinIO (S3 API, no AWS) parquet → Postgres** — `make demo-s3-*` | local MinIO + Postgres |

**Demos 09–11** are higher-touch — they include `make` targets for
one-command setup, bundled synthetic producers, and inline READMEs
that walk you through "see it in action" steps (live row counts,
status tables, MinIO web console). Quick start:

```sh
make demo-deps   # installs confluent-kafka, boto3, pyarrow
make up          # brings up postgres + kafka + minio
make help        # lists every demo-* target
```

Each demo has its own README under `examples/0X_*/` with full
walkthrough.

All examples need at minimum a local Postgres — the v0.1 declarative
`@ematix.pipeline` decorator is Postgres-specific (multi-backend
support landed for streaming targets and the cross-DB Arrow path,
not for the decorator path). Streaming examples (05–08) additionally
need a local Kafka.

`docker compose -f examples/docker-compose.yml up -d` brings up both
in one command.

## Running examples 01–04

```sh
pip install ematix-flow

# Either set the DSN env var per-run:
EMATIX_FLOW_DSN=postgres://postgres:postgres@localhost/postgres \
    python examples/01_append.py

# Or persist it in ~/.ematix-flow/connections.toml:
#   [connections.default]
#   url = "postgres://postgres:postgres@localhost/postgres"
```

## Running examples 05–08

Either via the CLI binary:

```sh
flow consume examples/05_streaming_kafka_to_pg.toml \
    --metrics-port 9100 \
    --restart-on-error
```

Or from Python:

```python
from ematix_flow import run_pipeline
run_pipeline(config="examples/05_streaming_kafka_to_pg.toml",
             metrics_port=9100)
```

For 06 (Python-driven windowed pipeline), just run the script:

```sh
python examples/06_windowed_tumbling.py
```
