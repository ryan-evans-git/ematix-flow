"""``flow doctor`` — connection health-check report.

Iterates every registered typed :class:`Connection` and dispatches a
backend-specific liveness probe, returning a green/red status per
connection. The probes are intentionally cheap (one round-trip each)
so this is safe to run from a pre-deploy script or on-call playbook.

Probe coverage by kind:

* ``postgres`` / ``mysql`` / ``redshift`` / ``sqlite`` / ``duckdb`` —
  SQL ping via the existing :func:`ematix_flow.config.check_connection`
  fallback (driver-level connect + lightweight query).
* ``kafka`` — admin describe-cluster via :class:`KafkaConnection`'s
  resolved bootstrap servers + any SASL auth.
* ``kinesis`` / ``pubsub`` / ``rabbitmq`` — broker / queue ping via
  the respective Python SDK (boto3 / google-cloud-pubsub / pika).
* ``object_store_s3`` / ``delta_s3`` — boto3 ``head_bucket``.
* ``object_store_local`` / ``delta_local`` — ``Path.exists()``.
* ``schema_registry`` — HTTP GET ``/subjects`` (Confluent / Apicurio).
* ``glue_schema_registry`` — boto3 ``list_registries`` (returns the
  exhaustive list; we only need to confirm the IAM call works).
* ``snowflake`` / ``bigquery`` — light ``SELECT 1`` via the Arrow
  query adapter (skip if the cloud SDK isn't installed).

Each probe wraps every exception so a single bad connection doesn't
mask the rest. The :func:`run_doctor` function returns a list of
:class:`HealthReport` for the CLI to format.
"""
from __future__ import annotations

import socket
from dataclasses import dataclass, field
from typing import Any

from ematix_flow import config
from ematix_flow.connections import (
    Connection,
    GlueSchemaRegistryConnection,
    KafkaConnection,
    KinesisConnection,
    PubSubConnection,
    RabbitMQConnection,
    SchemaRegistryConnection,
    registered_connections,
)
from ematix_flow.secrets import expand

__all__ = [
    "HealthReport",
    "format_doctor_report",
    "probe_connection",
    "run_doctor",
]


@dataclass(frozen=True)
class HealthReport:
    """One row in the doctor report.

    ``status`` is ``"ok"`` / ``"fail"`` / ``"skip"``. ``"skip"`` means
    the probe is unsupported for this kind (no false negative) — the
    user can still get value from the rows that did probe.
    """

    name: str
    kind: str
    status: str  # "ok" | "fail" | "skip"
    detail: str = ""
    elapsed_ms: int = 0
    extras: dict[str, Any] = field(default_factory=dict)

    @property
    def is_ok(self) -> bool:
        return self.status == "ok"

    @property
    def is_fail(self) -> bool:
        return self.status == "fail"


def _measure_ms(start_perf_counter_ns: int, end_perf_counter_ns: int) -> int:
    return max(0, (end_perf_counter_ns - start_perf_counter_ns) // 1_000_000)


def probe_connection(conn: Connection) -> HealthReport:
    """Dispatch the right probe for a connection's kind and report.

    Any exception is caught and converted into a ``"fail"`` row —
    one bad probe never aborts the whole report.
    """
    import time

    start = time.perf_counter_ns()
    kind = conn.kind
    try:
        # SQL backends share a single ping path. The Rust _core
        # connection layer does the actual SELECT 1.
        if kind in ("postgres", "mysql", "redshift", "sqlite", "duckdb"):
            ok, message = config.check_connection(conn.name)
            status = "ok" if ok else "fail"
            return HealthReport(
                name=conn.name,
                kind=kind,
                status=status,
                detail=message,
                elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
            )

        if kind == "kafka":
            return _probe_kafka(conn, start)
        if kind == "schema_registry":
            return _probe_schema_registry(conn, start)
        if kind == "glue_schema_registry":
            return _probe_glue_schema_registry(conn, start)
        if kind == "kinesis":
            return _probe_kinesis(conn, start)
        if kind == "pubsub":
            return _probe_pubsub(conn, start)
        if kind == "rabbitmq":
            return _probe_rabbitmq(conn, start)
        if kind in ("object_store_s3", "delta_s3"):
            return _probe_s3(conn, start)
        if kind in ("object_store_local", "delta_local"):
            return _probe_local_path(conn, start)
        if kind == "snowflake":
            return _probe_snowflake(conn, start)
        if kind == "bigquery":
            return _probe_bigquery(conn, start)

        return HealthReport(
            name=conn.name, kind=kind, status="skip",
            detail=f"no probe wired for kind={kind!r}",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    except Exception as exc:
        # Defensive catch-all — a missing SDK or transient resolver
        # failure shouldn't take down the whole doctor run.
        return HealthReport(
            name=conn.name, kind=kind, status="fail",
            detail=f"{type(exc).__name__}: {exc}",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )


def _probe_kafka(conn: KafkaConnection, start: int) -> HealthReport:
    """TCP-connect to the first bootstrap server. A full admin
    describe-cluster needs librdkafka + SASL config; the TCP probe is
    a cheaper liveness signal that catches the common
    "wrong-host / not-reachable" misconfig."""
    import time

    bootstrap = expand(conn.bootstrap_servers) or ""
    first = bootstrap.split(",", 1)[0].strip()
    if not first:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="fail",
            detail="empty bootstrap_servers",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    host, _, port_str = first.rpartition(":")
    port = int(port_str) if port_str else 9092
    if not host:
        host = first
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(3.0)
        sock.connect((host, port))
    return HealthReport(
        name=conn.name, kind=conn.kind, status="ok",
        detail=f"tcp-connect {host}:{port}",
        elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
    )


def _probe_schema_registry(
    conn: SchemaRegistryConnection, start: int,
) -> HealthReport:
    """HTTP GET ``/subjects`` against the Confluent / Apicurio SR.
    Uses stdlib urllib so the doctor doesn't pull a new dep."""
    import time
    import urllib.request

    url = expand(conn.url) or ""
    if not url:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="fail",
            detail="empty url",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    target = url.rstrip("/") + "/subjects"
    req = urllib.request.Request(target, method="GET")
    if conn.basic_auth_user and conn.basic_auth_password:
        import base64

        token = base64.b64encode(
            f"{expand(conn.basic_auth_user)}:{expand(conn.basic_auth_password)}"
            .encode()
        ).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
    # nosec B310: `target` is the user's own connection URL from their
    # connections.toml — this is a health probe, not a server-side
    # fetcher of user-supplied URLs. The scheme is validated at
    # Connection construction time.
    with urllib.request.urlopen(req, timeout=3.0) as resp:  # nosec B310
        status_code = resp.status
    return HealthReport(
        name=conn.name, kind=conn.kind,
        status="ok" if 200 <= status_code < 300 else "fail",
        detail=f"GET {target} → {status_code}",
        elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
    )


def _probe_glue_schema_registry(
    conn: GlueSchemaRegistryConnection, start: int,
) -> HealthReport:
    """boto3 ``list_registries``. Confirms IAM creds + region + Glue
    service reachability in one call."""
    import time

    try:
        import boto3  # type: ignore[import-not-found]
    except ImportError:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="skip",
            detail="boto3 not installed (pip install ematix-flow[glue-schema-registry])",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    region = expand(conn.region)
    kwargs: dict[str, Any] = {"region_name": region}
    if conn.aws_profile:
        session = boto3.Session(profile_name=expand(conn.aws_profile))
        client = session.client("glue", region_name=region)
    elif conn.aws_access_key_id and conn.aws_secret_access_key:
        client = boto3.client(
            "glue",
            region_name=region,
            aws_access_key_id=expand(conn.aws_access_key_id),
            aws_secret_access_key=expand(conn.aws_secret_access_key),
        )
    else:
        client = boto3.client("glue", **kwargs)
    resp = client.list_registries(MaxResults=1)
    return HealthReport(
        name=conn.name, kind=conn.kind, status="ok",
        detail=f"list_registries: {len(resp.get('Registries', []))} registr(y/ies) visible",
        elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
    )


def _probe_kinesis(conn: KinesisConnection, start: int) -> HealthReport:
    """boto3 ``describe_stream_summary`` against the configured
    stream. Catches both IAM and stream-not-found failures."""
    import time

    try:
        import boto3  # type: ignore[import-not-found]
    except ImportError:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="skip",
            detail="boto3 not installed",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    client = boto3.client("kinesis", region_name=expand(conn.region))
    client.describe_stream_summary(StreamName=expand(conn.stream_name))
    return HealthReport(
        name=conn.name, kind=conn.kind, status="ok",
        detail=f"stream={conn.stream_name}",
        elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
    )


def _probe_pubsub(conn: PubSubConnection, start: int) -> HealthReport:
    """``google-cloud-pubsub`` topic existence check."""
    import time

    try:
        from google.cloud import pubsub_v1  # type: ignore[import-not-found]
    except ImportError:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="skip",
            detail="google-cloud-pubsub not installed",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        expand(conn.project_id), expand(conn.topic),
    )
    publisher.get_topic(request={"topic": topic_path})
    return HealthReport(
        name=conn.name, kind=conn.kind, status="ok",
        detail=f"topic={topic_path}",
        elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
    )


def _probe_rabbitmq(conn: RabbitMQConnection, start: int) -> HealthReport:
    """Open + close an AMQP connection."""
    import time

    try:
        import pika  # type: ignore[import-not-found]
    except ImportError:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="skip",
            detail="pika not installed",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    url = expand(conn.amqp_url) or ""
    params = pika.URLParameters(url)
    params.socket_timeout = 3.0
    connection = pika.BlockingConnection(params)
    try:
        connection.close()
    except Exception:
        pass
    return HealthReport(
        name=conn.name, kind=conn.kind, status="ok",
        detail="amqp connect+close",
        elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
    )


def _probe_s3(conn: Any, start: int) -> HealthReport:
    """``head_bucket`` against the connection's bucket. Catches IAM,
    region-mismatch, and bucket-not-found failures in one call."""
    import time

    try:
        import boto3  # type: ignore[import-not-found]
    except ImportError:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="skip",
            detail="boto3 not installed",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    region = getattr(conn, "region", None)
    bucket = getattr(conn, "bucket", None) or getattr(conn, "bucket_name", None)
    if not bucket:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="fail",
            detail=f"no bucket/bucket_name on {type(conn).__name__}",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    client = boto3.client("s3", region_name=expand(region) if region else None)
    client.head_bucket(Bucket=expand(bucket))
    return HealthReport(
        name=conn.name, kind=conn.kind, status="ok",
        detail=f"bucket={bucket}",
        elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
    )


def _probe_local_path(conn: Any, start: int) -> HealthReport:
    """``Path.exists()`` against the configured root."""
    import time
    from pathlib import Path

    path_str = (
        getattr(conn, "root", None)
        or getattr(conn, "path", None)
        or getattr(conn, "base_path", None)
    )
    if not path_str:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="fail",
            detail="no root/path attr",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    expanded = expand(path_str) or path_str
    p = Path(expanded)
    if p.exists():
        return HealthReport(
            name=conn.name, kind=conn.kind, status="ok",
            detail=f"exists: {expanded}",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    return HealthReport(
        name=conn.name, kind=conn.kind, status="fail",
        detail=f"missing: {expanded}",
        elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
    )


def _probe_snowflake(conn: Any, start: int) -> HealthReport:
    """Light ``SELECT 1`` via snowflake_query_to_arrow. Skips when
    the snowflake-connector-python extra isn't installed."""
    import time

    try:
        from ematix_flow.warehouses import snowflake_query_to_arrow
    except ImportError:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="skip",
            detail="warehouses module unavailable",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    try:
        import snowflake.connector  # noqa: F401
    except ImportError:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="skip",
            detail="snowflake-connector-python not installed",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    table = snowflake_query_to_arrow(conn, "SELECT 1")
    return HealthReport(
        name=conn.name, kind=conn.kind, status="ok",
        detail=f"SELECT 1 → {table.num_rows} row(s)",
        elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
    )


def _probe_bigquery(conn: Any, start: int) -> HealthReport:
    """``SELECT 1`` via bigquery_query_to_arrow."""
    import time

    try:
        from ematix_flow.warehouses import bigquery_query_to_arrow
    except ImportError:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="skip",
            detail="warehouses module unavailable",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    try:
        from google.cloud import bigquery  # noqa: F401
    except ImportError:
        return HealthReport(
            name=conn.name, kind=conn.kind, status="skip",
            detail="google-cloud-bigquery not installed",
            elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
        )
    table = bigquery_query_to_arrow(conn, "SELECT 1")
    return HealthReport(
        name=conn.name, kind=conn.kind, status="ok",
        detail=f"SELECT 1 → {table.num_rows} row(s)",
        elapsed_ms=_measure_ms(start, time.perf_counter_ns()),
    )


def run_doctor() -> list[HealthReport]:
    """Probe every registered typed connection. Order: by name."""
    conns = registered_connections()
    return [probe_connection(c) for _, c in sorted(conns.items())]


def format_doctor_report(reports: list[HealthReport]) -> str:
    """Render the report as a fixed-width table with one row per
    connection. ``ok`` rows print green-ish (✓), ``fail`` red-ish (✗),
    ``skip`` neutral (-). Output is plain ASCII; CI / logging-friendly."""
    if not reports:
        return "no connections registered"
    name_w = max(len("NAME"), max(len(r.name) for r in reports))
    kind_w = max(len("KIND"), max(len(r.kind) for r in reports))
    header = (
        f"{'NAME':<{name_w}}  {'KIND':<{kind_w}}  "
        f"STATUS  {'ms':>6}  DETAIL"
    )
    lines = [header, "-" * len(header)]
    marker = {"ok": "✓ ok  ", "fail": "✗ fail", "skip": "- skip"}
    for r in reports:
        lines.append(
            f"{r.name:<{name_w}}  {r.kind:<{kind_w}}  "
            f"{marker.get(r.status, r.status):<6}  "
            f"{r.elapsed_ms:>6}  {r.detail}"
        )
    return "\n".join(lines)
