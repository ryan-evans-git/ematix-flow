# Unified pipeline API — design proposal

**Status:** design — not yet implemented.

**Driver:** the framework currently has two pipeline-declaration
surfaces with different ergonomics. A user who wants both a v0.1
declarative DB pipeline and a streaming pipeline has to learn two
configuration models. This proposes consolidating onto the existing
decorator surface so there's exactly one way.

## Today's split

| Surface | Where you declare | Connections | Multi-source / multi-target |
|--|--|--|--|
| **v0.1 declarative** | `@ematix.pipeline(target=, source_connection=, target_connection=, ...)` Python decorator | Named connections via the registry: env var → project TOML → user TOML → inline. Credentials with `${VAR}` interpolation. | `targets=[ematix.target(Cls1, ...), ematix.target(Cls2, ...)]` (Phase 24b) — multi-target works. Multi-source not yet. |
| **Streaming (`flow consume`)** | `pipeline.toml` with `[source]` / `[target]` tables + `flow consume <toml>` binary | Inline credentials in the TOML (`url = "postgres://user:pw@..."`, `access_key_id = "..."`). | One source + one target. |

**Friction:**

1. Two mental models: decorator + Python registry vs TOML + inline
   credentials.
2. The streaming TOML can't express multi-source / multi-target —
   common patterns like "drain Kafka into both Postgres warehouse +
   Delta archive" need ad-hoc orchestration.
3. Inline TOML credentials are easy to commit to git by mistake.
4. Connection definitions get duplicated between the registry (for
   declarative) and the TOML (for streaming).

## Proposed surface

A single decorator-based path. The streaming TOML config goes
away (or becomes deprecated alongside the unified decorator); the
`flow consume` binary continues to exist but it loads pipelines
from a Python module instead of reading TOML.

```python
from ematix_flow import ematix
from ematix_flow.streaming import KafkaSource, PostgresSink, DeltaSink

@ematix.streaming_pipeline(
    name="events-fanout",
    source=KafkaSource(
        connection="kafka_prod",
        topic="events",
        group_id="ematix-flow",
        payload_format="avro",
        schema_registry="sr_prod",
    ),
    targets=[
        PostgresSink(connection="warehouse", table="public.events"),
        DeltaSink(connection="lake", path="events", partition_by=["year", "month"]),
    ],
    metrics_port=9100,
    restart_on_error=True,
)
def events_fanout():
    pass  # body is empty for pure source→target streaming pipelines
```

Run from the existing CLI shape (mirroring `flow run` for
declarative pipelines):

```sh
flow consume --module my_pipelines events_fanout
flow consume-due --module my_pipelines     # if a schedule is set
```

### Multi-source

The same decorator with `sources=[...]`:

```python
@ematix.streaming_pipeline(
    name="merge-streams",
    sources=[
        KafkaSource(connection="kafka_a", topic="events"),
        RabbitMQSource(connection="amqp_b", queue="events"),
    ],
    target=PostgresSink(connection="warehouse", table="public.events"),
)
def merge_streams():
    pass
```

Internally this fans out: each source is consumed concurrently;
records are interleaved into the target. Manual ack happens
per-source after the target write lands.

### Connection references

`connection="kafka_prod"` is a **name**, resolved through the
existing registry (the same one declarative pipelines use):

1. Env var `EMATIX_FLOW_DSN_KAFKA_PROD` (or the connection-typed
   equivalent for non-DB sources — e.g.
   `EMATIX_FLOW_KAFKA_KAFKA_PROD={"bootstrap": "...", "auth": "..."}`).
2. `./.ematix-flow.toml`:
   ```toml
   [connections.kafka_prod]
   kind = "kafka"
   bootstrap_servers = "${KAFKA_BOOTSTRAP}"
   group_id = "ematix-flow"
   sasl_plain_username = "${KAFKA_USER}"
   sasl_plain_password = "${KAFKA_PASS}"  # ${VAR} interpolation
   ```
3. `~/.ematix-flow/connections.toml` (same shape).
4. Inline `connect(...)` escape hatch.

Pipeline code never sees credentials directly. The `KafkaSource`
constructor takes a connection *name*; the runtime resolves it
through the registry. This matches the v0.1 declarative pattern.

### Schema Registry as a connection too

```toml
[connections.sr_prod]
kind = "schema_registry"
url = "${SR_URL}"
basic_auth_user = "${SR_USER}"
basic_auth_password = "${SR_PASS}"
```

Then `KafkaSource(..., schema_registry="sr_prod")` resolves the SR
through the same registry.

## What stays the same

- The Rust runtime under the hood (KafkaBackend, RabbitMQBackend,
  etc.) is unchanged. Only the Python-side glue + the CLI
  pipeline-loader change.
- `flow consume <toml>` keeps working for one major version after
  the unified API ships, with a deprecation warning. After that
  the TOML loader is removed.
- The connection registry continues to be the single source of
  truth for credentials.
- All credentials still get redacted in `Debug` (see the recent
  fixes — Kafka `AuthMode`, RabbitMQ `amqp_url`,
  `PipelineCliConfig`, etc.).

## Implementation phases

| Phase | Scope | Effort |
|--|--|--|
| **Π.1** | Add the connection-registry concept for non-DB sources. Extend the TOML schema (`[connections.kafka_prod] kind = "kafka" ...`); resolver returns a typed `KafkaConnection` / `AmqpConnection` / etc. struct. | 2-3 days |
| **Π.2** | `KafkaSource` / `RabbitMQSource` / `PubSubSource` / `KinesisSource` Python facade classes. Each one's constructor takes a `connection=` name + per-source knobs. Internal: at decorator-resolution time, fetch the typed connection and pass through to the Rust backend constructor. | 3-4 days |
| **Π.3** | `@ematix.streaming_pipeline(source=, target=, ...)` decorator that registers like `@ematix.pipeline` does today. CLI gets `flow consume --module my_pipelines <name>` (alongside the existing `--toml` form). | 2-3 days |
| **Π.4** | Multi-source / multi-target. Concurrent draining; manual ack per-source after the slowest target write. | 1 week |
| **Π.5** | Deprecate the inline-credentials TOML loader (`flow consume <toml>`). Emit a warning with a migration pointer. Plan removal one minor release later. | 0.5 day |

## Trade-offs

**Why this is right:**
- Single mental model for users.
- Credentials live in one place (the registry), get redacted by
  one set of `Debug` impls.
- Multi-source/multi-target falls out naturally — same decorator,
  list-shaped args.
- Reuses the existing registry (`config.connect`,
  `~/.ematix-flow/connections.toml`) rather than inventing a
  parallel one.

**Why we haven't built it yet:**
- The TOML path was the right MVP — got `flow consume` working
  end-to-end without forcing every streaming user through the
  Python decorator.
- The unified API requires Python wrappers around each Rust
  backend, which we're already partway through (Phases Py.2-Py.6
  — `KafkaBackend` / `RabbitMQBackend` / `PubSubBackend` /
  `KinesisBackend` pyclasses already exist). This builds on that.

**Why I'd defer until users ask:**
- The TOML path works for the most common case (single source +
  single target); multi-source is real but rarer.
- Users who *want* a Python-driven streaming pipeline can already
  call `KafkaBackend.iter_arrow_stream(...)` (Phase Py.6) plus
  `PostgresBackend.write_arrow_stream(...)` from a script — it's
  imperative, but it works.
- A premature API freeze on the streaming-pipeline decorator
  would be hard to evolve.

## When to revisit

- A user has a real multi-source / multi-target streaming need
  and the TOML path can't express it.
- The TOML path's inline-credentials footgun causes a
  near-miss (someone almost commits a config with secrets).
- A second user independently asks "can the streaming pipeline
  be a Python decorator?".

Until one of those happens, this doc captures the design so the
next implementer doesn't start from scratch.
