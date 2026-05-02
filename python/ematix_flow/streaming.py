"""Streaming pipeline runner — Python facade over the Rust ``flow consume`` runtime.

Two equivalent entry points:

1. **``run_streaming_pipeline(...)``** (Π.1, recommended) — takes
   typed connection objects from
   :mod:`ematix_flow.connections`. The framework serializes the
   pipeline shape internally and hands off to the Rust runner.

2. **``run_pipeline(config_str=...)`` / ``run_pipeline(config=...)``**
   (Phase Py.1) — takes a TOML string or path. Useful for the
   ``flow consume <path.toml>`` CLI shape and for callers who
   already manage their own TOML.

Both invoke the same Rust runtime — the pipeline runs on
multi-thread tokio with no records crossing the Python FFI
boundary. Python only does one-time configuration; per-batch
work is pure Rust. ``${VAR}`` env interpolation in connection
fields happens at runner-build time.

Errors raise ``ValueError`` with the underlying error string;
SIGTERM / SIGINT exits cleanly with ``shutdown_triggered=True``.

Example (Π.1, decorator-based):

    from ematix_flow import ematix
    from ematix_flow.streaming import run_streaming_pipeline

    @ematix.connection
    class kafka_prod:
        kind = "kafka"
        bootstrap_servers = "${KAFKA_BOOTSTRAP}"
        group_id = "ematix-flow"

    @ematix.connection
    class warehouse:
        kind = "postgres"
        url = "${WAREHOUSE_DSN}"

    metrics = run_streaming_pipeline(
        name="events-to-pg",
        source=kafka_prod,                        # typed connection
        source_query="events",                    # topic / queue / sub / stream
        target=warehouse,                         # typed connection
        target_table=("public", "events"),
        metrics_port=9100,
    )
    print(metrics["total_rows"], metrics["shutdown_triggered"])
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional, Union

from ematix_flow._core import (
    run_pipeline_from_path,
    run_pipeline_from_toml_str,
)
from ematix_flow.connections import (
    Connection,
    DeltaLocalConnection,
    DeltaS3Connection,
    DuckDBConnection,
    KafkaConnection,
    KinesisConnection,
    MySQLConnection,
    ObjectStoreLocalConnection,
    ObjectStoreS3Connection,
    PostgresConnection,
    PubSubConnection,
    RabbitMQConnection,
    SQLiteConnection,
    get_connection,
    resolve,
)

# `PipelineMetrics` is the dict shape returned by the Rust runner:
# {"total_rows": int, "iterations": int, "shutdown_triggered": bool}.
# Kept as a type alias so callers can annotate against it.
PipelineMetrics = dict[str, Any]


@dataclass(frozen=True)
class Target:
    """Π.4a: one entry in a multi-target fan-out pipeline.

    Bundles a connection (or registered name) with its per-kind
    placement spec — ``table`` for DB / Delta targets, ``topic`` /
    ``queue`` / ``partition_key_prefix`` for streaming targets, etc.
    Pass a ``list[Target]`` as ``targets=`` to
    :func:`run_streaming_pipeline` to fan a single source's
    batches out to multiple sinks; the framework writes to every
    target before advancing source offsets.
    """

    connection: Union[Connection, str]
    table: Optional[tuple[str, str]] = None
    topic: Optional[str] = None
    queue: Optional[str] = None
    partition_key_prefix: Optional[str] = None
    prefix: Optional[str] = None
    message_key_column: Optional[str] = None
    partition_by: Optional[list[str]] = field(default=None)


__all__ = [
    "run_pipeline",
    "run_streaming_pipeline",
    "PipelineMetrics",
    "Target",
]


def run_pipeline(
    *,
    config: str | os.PathLike[str] | None = None,
    config_str: str | None = None,
    metrics_port: int | None = None,
) -> PipelineMetrics:
    """Run a streaming pipeline from a TOML config (string or path).

    Exactly one of ``config`` (path) or ``config_str`` (TOML
    contents) must be provided. Returns a dict with
    ``total_rows``, ``iterations``, and ``shutdown_triggered``.
    Blocks until SIGTERM / SIGINT or clean exit.

    For new code, prefer :func:`run_streaming_pipeline` with typed
    :class:`Connection` objects — this form stays for back-compat
    with the ``flow consume <toml>`` CLI shape.
    """
    if config is None and config_str is None:
        raise ValueError("run_pipeline: either `config` (path) or `config_str` is required")
    if config is not None and config_str is not None:
        raise ValueError("run_pipeline: pass `config` OR `config_str`, not both")
    if config_str is not None:
        return run_pipeline_from_toml_str(config_str, metrics_port)
    assert config is not None
    return run_pipeline_from_path(os.fspath(config), metrics_port)


# ---------- Π.1: typed-connection-based runner --------------------


def run_streaming_pipeline(
    *,
    name: str,
    source: Union[Connection, str],
    source_query: str,
    target: Optional[Union[Connection, str]] = None,
    # Single-target placement (legacy shape; mutually exclusive with
    # ``targets=``). Exactly one of these applies depending on the
    # target connection's kind:
    target_table: Optional[tuple[str, str]] = None,
    target_topic: Optional[str] = None,
    target_queue: Optional[str] = None,
    target_partition_key_prefix: Optional[str] = None,
    target_prefix: Optional[str] = None,
    target_message_key_column: Optional[str] = None,
    target_partition_by: Optional[list[str]] = None,
    # Π.4a multi-target: list of typed Target specs. Mutually
    # exclusive with the single ``target=`` shape above.
    targets: Optional[list[Target]] = None,
    idle_pause_ms: int = 500,
    dead_letter_topic: Optional[str] = None,
    metrics_port: Optional[int] = None,
) -> PipelineMetrics:
    """Run a streaming pipeline driven by typed :class:`Connection` objects.

    ``source`` accepts either a :class:`Connection` instance
    (recommended — no string lookup) or a registry name string
    (resolved via :func:`get_connection`).

    **Single-target (legacy shape).** Pass ``target=`` plus the
    placement kwarg matching your target's kind:

    - DB targets (Postgres / MySQL / SQLite / DuckDB / Delta\\*):
      ``target_table=(schema, name)``.
    - Kafka target: ``target_topic="..."``; optional
      ``target_message_key_column="user_id"``.
    - RabbitMQ target: ``target_queue="..."``.
    - Pub/Sub target: ``target_topic="..."``.
    - Kinesis target: ``target_partition_key_prefix="..."``.
    - Object store: ``target_prefix="..."``.
    - Delta\\* targets also accept
      ``target_partition_by=["year", "month"]``.

    **Multi-target (Π.4a).** Pass ``targets=[Target(...), ...]`` to
    fan one source's batches out to N sinks. Each ``Target`` carries
    its connection plus the placement field for that sink's kind.
    The pipeline writes to every target before advancing source
    offsets — at-least-once across the fan-out.

    The two forms are mutually exclusive; pass one or the other.
    """
    if target is not None and targets is not None:
        raise ValueError(
            "run_streaming_pipeline: pass `target=` or `targets=`, not both"
        )
    if target is None and not targets:
        raise ValueError(
            "run_streaming_pipeline: a target is required — set `target=` "
            "(single-target shape) or `targets=[Target(...), ...]` (multi-target)"
        )

    src_conn = source if isinstance(source, Connection) else get_connection(source)

    if targets is not None:
        target_specs: list[Target] = list(targets)
    else:
        # Legacy shape: collapse the flat kwargs into a single Target.
        target_specs = [
            Target(
                connection=target,  # type: ignore[arg-type]
                table=target_table,
                topic=target_topic,
                queue=target_queue,
                partition_key_prefix=target_partition_key_prefix,
                prefix=target_prefix,
                message_key_column=target_message_key_column,
                partition_by=target_partition_by,
            )
        ]

    toml = _build_toml_multi(
        name=name,
        source=src_conn,
        source_query=source_query,
        targets=target_specs,
        idle_pause_ms=idle_pause_ms,
        dead_letter_topic=dead_letter_topic,
    )
    return run_pipeline_from_toml_str(toml, metrics_port)


# ---------- TOML emission (private) -------------------------------


def _build_toml(
    *,
    name: str,
    source: Connection,
    source_query: str,
    target: Connection,
    target_table: Optional[tuple[str, str]],
    target_topic: Optional[str],
    target_queue: Optional[str],
    target_partition_key_prefix: Optional[str],
    target_prefix: Optional[str],
    target_message_key_column: Optional[str],
    target_partition_by: Optional[list[str]],
    idle_pause_ms: int,
    dead_letter_topic: Optional[str],
) -> str:
    """Single-target back-compat shim. Wraps the flat kwargs into a
    one-element ``[Target(...)]`` and delegates to
    :func:`_build_toml_multi`."""
    return _build_toml_multi(
        name=name,
        source=source,
        source_query=source_query,
        targets=[
            Target(
                connection=target,
                table=target_table,
                topic=target_topic,
                queue=target_queue,
                partition_key_prefix=target_partition_key_prefix,
                prefix=target_prefix,
                message_key_column=target_message_key_column,
                partition_by=target_partition_by,
            )
        ],
        idle_pause_ms=idle_pause_ms,
        dead_letter_topic=dead_letter_topic,
    )


def _build_toml_multi(
    *,
    name: str,
    source: Connection,
    source_query: str,
    targets: list[Target],
    idle_pause_ms: int,
    dead_letter_topic: Optional[str],
) -> str:
    """Π.4a: serialize a pipeline shape into the TOML config format.

    Single target → emits ``[target]`` + ``[target.table]`` (the
    legacy compact shape). Two or more targets → emits one
    ``[[targets]]`` block per spec. The Rust runner accepts both
    forms (Π.4a-1).

    The TOML round-trip is the transport between the Python facade
    and the Rust runner. It happens once at pipeline start; per-
    batch work stays pure Rust.
    """
    if not targets:
        raise ValueError("_build_toml_multi: targets must be non-empty")

    lines: list[str] = [
        f"pipeline_name = {_q(name)}",
        f"source_query = {_q(source_query)}",
        f"idle_pause_ms = {int(idle_pause_ms)}",
    ]
    if dead_letter_topic is not None:
        lines.append(f"dead_letter_topic = {_q(dead_letter_topic)}")

    lines.append("")
    lines.append("[source]")
    lines.extend(_source_fields(source))

    if len(targets) == 1:
        lines.extend(_emit_single_target_block(targets[0]))
    else:
        for spec in targets:
            lines.extend(_emit_targets_array_entry(spec))

    return "\n".join(lines) + "\n"


def _resolve_target_connection(spec: Target) -> Connection:
    return (
        spec.connection
        if isinstance(spec.connection, Connection)
        else get_connection(spec.connection)
    )


def _emit_single_target_block(spec: Target) -> list[str]:
    """Emit `[target]` (+ `[target.table]` for DB-shaped sinks)."""
    conn = _resolve_target_connection(spec)
    lines: list[str] = ["", "[target]"]
    lines.extend(
        _target_fields(
            conn,
            target_table=spec.table,
            target_topic=spec.topic,
            target_queue=spec.queue,
            target_partition_key_prefix=spec.partition_key_prefix,
            target_prefix=spec.prefix,
            target_message_key_column=spec.message_key_column,
            target_partition_by=spec.partition_by,
        )
    )
    if spec.table is not None:
        schema, table = spec.table
        lines.append("")
        lines.append("[target.table]")
        lines.append(f"schema = {_q(schema)}")
        lines.append(f"name = {_q(table)}")
    return lines


def _emit_targets_array_entry(spec: Target) -> list[str]:
    """Emit one `[[targets]]` block for the multi-target shape."""
    conn = _resolve_target_connection(spec)
    lines: list[str] = ["", "[[targets]]"]
    lines.extend(
        _target_fields(
            conn,
            target_table=spec.table,
            target_topic=spec.topic,
            target_queue=spec.queue,
            target_partition_key_prefix=spec.partition_key_prefix,
            target_prefix=spec.prefix,
            target_message_key_column=spec.message_key_column,
            target_partition_by=spec.partition_by,
        )
    )
    if spec.table is not None:
        schema, table = spec.table
        lines.append("")
        lines.append("[targets.table]")
        lines.append(f"schema = {_q(schema)}")
        lines.append(f"name = {_q(table)}")
    return lines


def _source_fields(conn: Connection) -> list[str]:
    """Emit the `kind = ...` line + per-kind fields for [source]."""
    if isinstance(conn, KafkaConnection):
        out = ['kind = "kafka"']
        out.append(f"bootstrap_servers = {_q(_resolve_required(conn.bootstrap_servers, 'bootstrap_servers'))}")
        if conn.group_id is not None:
            out.append(f"group_id = {_q(resolve(conn.group_id))}")
        return out
    if isinstance(conn, RabbitMQConnection):
        return ['kind = "rabbitmq"', f"amqp_url = {_q(_resolve_required(conn.amqp_url, 'amqp_url'))}"]
    if isinstance(conn, PubSubConnection):
        out = ['kind = "pubsub"', f"project_id = {_q(_resolve_required(conn.project_id, 'project_id'))}"]
        if conn.endpoint:
            out.append(f"endpoint = {_q(resolve(conn.endpoint))}")
        if conn.anonymous_auth:
            out.append("anonymous_auth = true")
        return out
    if isinstance(conn, KinesisConnection):
        out = ['kind = "kinesis"', f"stream_name = {_q(_resolve_required(conn.stream_name, 'stream_name'))}"]
        if conn.region:
            out.append(f"region = {_q(resolve(conn.region))}")
        if conn.endpoint:
            out.append(f"endpoint = {_q(resolve(conn.endpoint))}")
        if conn.access_key_id:
            out.append(f"access_key_id = {_q(resolve(conn.access_key_id))}")
        if conn.secret_access_key:
            out.append(f"secret_access_key = {_q(resolve(conn.secret_access_key))}")
        return out
    raise ValueError(
        f"connection {conn.name!r} of kind {conn.kind!r} cannot be used as a source "
        "(streaming sources: kafka, rabbitmq, pubsub, kinesis)"
    )


def _target_fields(
    conn: Connection,
    *,
    target_table: Optional[tuple[str, str]],
    target_topic: Optional[str],
    target_queue: Optional[str],
    target_partition_key_prefix: Optional[str],
    target_prefix: Optional[str],
    target_message_key_column: Optional[str],
    target_partition_by: Optional[list[str]],
) -> list[str]:
    """Emit the `kind = ...` line + per-kind fields for [target]."""

    def need(label: str, value: Any) -> Any:
        if value is None:
            raise ValueError(
                f"target connection {conn.name!r} (kind={conn.kind!r}) requires {label}=... "
                "on run_streaming_pipeline()"
            )
        return value

    if isinstance(conn, PostgresConnection):
        need("target_table", target_table)
        return ['kind = "postgres"', f"url = {_q(_resolve_required(conn.url, 'url'))}"]
    if isinstance(conn, MySQLConnection):
        need("target_table", target_table)
        return ['kind = "mysql"', f"url = {_q(_resolve_required(conn.url, 'url'))}"]
    if isinstance(conn, SQLiteConnection):
        need("target_table", target_table)
        return ['kind = "sqlite"', f"path = {_q(_resolve_required(conn.path, 'path'))}"]
    if isinstance(conn, DuckDBConnection):
        need("target_table", target_table)
        return ['kind = "duckdb"', f"path = {_q(_resolve_required(conn.path, 'path'))}"]
    if isinstance(conn, KafkaConnection):
        topic = need("target_topic", target_topic)
        out = [
            'kind = "kafka"',
            f"bootstrap_servers = {_q(_resolve_required(conn.bootstrap_servers, 'bootstrap_servers'))}",
            f"topic = {_q(topic)}",
        ]
        if conn.group_id is not None:
            out.append(f"group_id = {_q(resolve(conn.group_id))}")
        if target_message_key_column is not None:
            out.append(f"message_key_column = {_q(target_message_key_column)}")
        return out
    if isinstance(conn, RabbitMQConnection):
        queue = need("target_queue", target_queue)
        return [
            'kind = "rabbitmq"',
            f"amqp_url = {_q(_resolve_required(conn.amqp_url, 'amqp_url'))}",
            f"queue = {_q(queue)}",
        ]
    if isinstance(conn, PubSubConnection):
        topic = need("target_topic", target_topic)
        out = [
            'kind = "pubsub"',
            f"project_id = {_q(_resolve_required(conn.project_id, 'project_id'))}",
            f"topic = {_q(topic)}",
        ]
        if conn.endpoint:
            out.append(f"endpoint = {_q(resolve(conn.endpoint))}")
        if conn.anonymous_auth:
            out.append("anonymous_auth = true")
        return out
    if isinstance(conn, KinesisConnection):
        prefix = need("target_partition_key_prefix", target_partition_key_prefix)
        out = [
            'kind = "kinesis"',
            f"stream_name = {_q(_resolve_required(conn.stream_name, 'stream_name'))}",
            f"partition_key_prefix = {_q(prefix)}",
        ]
        if conn.region:
            out.append(f"region = {_q(resolve(conn.region))}")
        if conn.endpoint:
            out.append(f"endpoint = {_q(resolve(conn.endpoint))}")
        if conn.access_key_id:
            out.append(f"access_key_id = {_q(resolve(conn.access_key_id))}")
        if conn.secret_access_key:
            out.append(f"secret_access_key = {_q(resolve(conn.secret_access_key))}")
        return out
    if isinstance(conn, DeltaLocalConnection):
        need("target_table", target_table)
        out = ['kind = "delta_local"', f"path = {_q(_resolve_required(conn.path, 'path'))}"]
        if target_partition_by:
            out.append(f"partition_by = {_toml_list(target_partition_by)}")
        return out
    if isinstance(conn, DeltaS3Connection):
        need("target_table", target_table)
        out = [
            'kind = "delta_s3"',
            f"endpoint = {_q(_resolve_required(conn.endpoint, 'endpoint'))}",
            f"bucket = {_q(_resolve_required(conn.bucket, 'bucket'))}",
            f"region = {_q(_resolve_required(conn.region, 'region'))}",
            f"access_key_id = {_q(_resolve_required(conn.access_key_id, 'access_key_id'))}",
            f"secret_access_key = {_q(_resolve_required(conn.secret_access_key, 'secret_access_key'))}",
        ]
        if conn.prefix:
            out.append(f"prefix = {_q(resolve(conn.prefix))}")
        if target_partition_by:
            out.append(f"partition_by = {_toml_list(target_partition_by)}")
        return out
    if isinstance(conn, ObjectStoreLocalConnection):
        prefix = need("target_prefix", target_prefix)
        return [
            'kind = "object_store_local"',
            f"path = {_q(_resolve_required(conn.path, 'path'))}",
            f"format = {_q(conn.format)}",
            f"prefix = {_q(prefix)}",
        ]
    if isinstance(conn, ObjectStoreS3Connection):
        prefix = need("target_prefix", target_prefix)
        return [
            'kind = "object_store_s3"',
            f"endpoint = {_q(_resolve_required(conn.endpoint, 'endpoint'))}",
            f"bucket = {_q(_resolve_required(conn.bucket, 'bucket'))}",
            f"region = {_q(_resolve_required(conn.region, 'region'))}",
            f"access_key_id = {_q(_resolve_required(conn.access_key_id, 'access_key_id'))}",
            f"secret_access_key = {_q(_resolve_required(conn.secret_access_key, 'secret_access_key'))}",
            f"format = {_q(conn.format)}",
            f"prefix = {_q(prefix)}",
        ]
    raise ValueError(f"unknown target connection kind {conn.kind!r}")


def _resolve_required(value: str, label: str) -> str:
    """Resolve env vars; treat empty/None as missing required field."""
    resolved = resolve(value)
    if not resolved:
        raise ValueError(f"connection field {label!r} resolves to empty after ${{VAR}} interpolation")
    return resolved


def _q(s: str) -> str:
    """TOML basic-string quoting. Backslashes and quotes escaped;
    rest passes through. Sufficient for the URL / table / topic
    field shapes the runner sees — TOML basic strings handle these."""
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _toml_list(items: list[str]) -> str:
    quoted = ", ".join(_q(s) for s in items)
    return f"[{quoted}]"
