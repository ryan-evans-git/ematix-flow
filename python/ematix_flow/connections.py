"""Typed connection registry for streaming pipelines (Π.1).

This module is the Python-side answer to the question "where do
credentials live, and how does a pipeline reference them?". A
**connection** is a typed Python object that carries the
configuration for a backend (Kafka broker, Postgres DSN, S3
bucket, etc.) plus a `name`. Pipelines reference connections by
name; the framework resolves them at run time.

Connections are **symmetric** — the same object can be used as a
source in one pipeline and a target in another. There's no
``KafkaSource`` / ``KafkaSink`` split; just ``KafkaConnection``,
used either way depending on which slot it occupies in
``run_streaming_pipeline``.

## Three ways to define a connection

All three produce the same registered ``Connection`` object —
pick whichever reads best for your codebase.

**Form 1 — declarative class:**

```python
from ematix_flow import ematix

@ematix.connection
class kafka_prod:
    kind = "kafka"
    bootstrap_servers = "${KAFKA_BOOTSTRAP}"
    group_id = "ematix-flow"
```

**Form 2 — typed instance, explicit register:**

```python
from ematix_flow.connections import KafkaConnection, register_connection

kafka_prod = KafkaConnection(
    name="kafka_prod",
    bootstrap_servers="${KAFKA_BOOTSTRAP}",
    group_id="ematix-flow",
)
register_connection(kafka_prod)
```

**Form 3 — typed instance, no registry (pass by reference):**

```python
kafka_prod = KafkaConnection(
    name="kafka_prod",
    bootstrap_servers="${KAFKA_BOOTSTRAP}",
)
# No register_connection call.
# Pipelines reference `kafka_prod` directly (the object), not
# by name string. Useful for tests or scripts that don't share
# connections across pipelines.
```

## ${VAR} env interpolation

Any ``str`` field on a connection can contain ``${VAR}``
references. The interpolation happens **at backend-build time**
(when the pipeline starts), not at connection-definition time —
so changing an env var between definition and run picks up the
new value, and undefined vars surface as a clear ``KeyError``.

```python
@ematix.connection
class warehouse:
    kind = "postgres"
    url = "postgres://app:${WAREHOUSE_PASSWORD}@${WAREHOUSE_HOST}/main"
```

## Credential safety

The dataclasses derive ``@dataclass(repr=False)`` — the default
``repr`` is replaced with one that redacts password / secret
fields. Logging a connection (directly or via a containing
pipeline object) is safe:

```python
>>> repr(kafka_prod)
"KafkaConnection(name='kafka_prod', bootstrap_servers='localhost:9092',
sasl_plain_password='<redacted>', ...)"
```

The Rust ``Debug`` impls for the same backends already redact (see
``Phase 196``).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field, fields
from typing import Any

__all__ = [
    "Connection",
    "DeltaLocalConnection",
    "DeltaS3Connection",
    "DuckDBConnection",
    "KafkaConnection",
    "KinesisConnection",
    "MySQLConnection",
    "ObjectStoreLocalConnection",
    "ObjectStoreS3Connection",
    "PostgresConnection",
    "PubSubConnection",
    "RabbitMQConnection",
    "SQLiteConnection",
    "SchemaRegistryConnection",
    "clear_registry",
    "connection",
    "get_connection",
    "redact",
    "register_connection",
    "registered_connections",
    "resolve",
]

# Same env-var pattern the v0.1 connection registry uses
# (config.py). Keeping the regex byte-identical so a single TOML
# file works against either path.
_INTERP = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")

# Field names whose values are credentials and must be redacted in
# repr() output. Matched case-insensitively against the dataclass
# field name.
_SECRET_TOKENS = frozenset({"password", "secret", "token"})
_SECRET_FIELDS = frozenset(
    {
        "password",
        "secret",
        "secret_access_key",
        "access_key_id",
        "key_password",
        "token",
        "api_key",
    }
)


def resolve(value: str | None) -> str | None:
    """Replace ``${VAR}`` references with ``os.environ[VAR]``.

    Returns ``None`` unchanged. Raises ``KeyError`` (with a clear
    pointer) if a referenced variable isn't set.
    """
    if value is None:
        return None

    def sub(match: re.Match[str]) -> str:
        var = match.group(1)
        if var not in os.environ:
            raise KeyError(
                f"environment variable {var!r} is referenced by an ematix-flow "
                "connection but is not set"
            )
        return os.environ[var]

    return _INTERP.sub(sub, value)


def redact(field_name: str, value: Any) -> Any:
    """Return ``"<redacted>"`` for known secret fields, else ``value``.

    Match is on substring — any field name that contains a known
    secret token (e.g. ``sasl_plain_password`` contains ``password``)
    redacts.
    """
    if value is None:
        return None
    lower = field_name.lower()
    if lower in _SECRET_FIELDS:
        return "<redacted>"
    # Compound names like "sasl_plain_password" — split on `_` and
    # match any segment against the secret-token whitelist.
    if any(seg in _SECRET_TOKENS for seg in lower.split("_")):
        return "<redacted>"
    # AMQP URLs carry user:password@host; redact the password.
    if field_name == "amqp_url" and isinstance(value, str):
        return _redact_url_password(value)
    # SQL DSNs carry user:password@host too.
    if field_name == "url" and isinstance(value, str) and "://" in value:
        return _redact_url_password(value)
    return value


def _redact_url_password(url: str) -> str:
    """``scheme://user:pw@host/...`` → ``scheme://user:<redacted>@host/...``."""
    if "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    if "/" in rest:
        authority, tail = rest.split("/", 1)
        tail = "/" + tail
    else:
        authority, tail = rest, ""
    if "@" not in authority:
        return url
    userinfo, host = authority.rsplit("@", 1)
    user = userinfo.split(":", 1)[0]
    if not user:
        return f"{scheme}://<redacted>@{host}{tail}"
    return f"{scheme}://{user}:<redacted>@{host}{tail}"


# ---------- Connection base class ----------------------------------


@dataclass(repr=False)
class Connection:
    """Marker base class for typed connection objects.

    Concrete subclasses (``KafkaConnection``, ``PostgresConnection``,
    etc.) declare the fields each backend requires. The ``kind``
    class attribute tags each subclass for the registry / TOML
    bridge.
    """

    name: str
    kind: str = field(init=False, default="")

    def __repr__(self) -> str:  # pragma: no cover (dataclass-style)
        cls = type(self).__name__
        parts = [f"name={self.name!r}", f"kind={self.kind!r}"]
        for f in fields(self):
            if f.name in ("name",) or not f.init:
                continue
            v = getattr(self, f.name)
            parts.append(f"{f.name}={redact(f.name, v)!r}")
        return f"{cls}({', '.join(parts)})"


# ---------- Streaming sources / sinks ------------------------------


@dataclass(repr=False)
class SchemaRegistryConnection(Connection):
    """Confluent-style Schema Registry handle (Π.1).

    Pass either an instance or its registered name to
    ``KafkaConnection(schema_registry=...)`` so SR config lives in
    the typed-connection registry alongside every other credential
    instead of inline in the streaming TOML.

    ``basic_auth_user`` / ``basic_auth_password`` are accepted on
    the dataclass but the Rust runtime can't yet apply them — the
    streaming TOML emitter raises ``NotImplementedError`` if they
    reach the emit step. Plumbing through ``SrSettings::new_basic_auth``
    is a small Rust-core follow-up.
    """

    url: str = ""
    basic_auth_user: str | None = None
    basic_auth_password: str | None = None

    def __post_init__(self) -> None:
        self.kind = "schema_registry"
        if not self.url:
            raise ValueError(
                f"SchemaRegistryConnection({self.name!r}): url is required"
            )


@dataclass(repr=False)
class KafkaConnection(Connection):
    """Kafka cluster handle.

    Used as a source (consumer) or target (producer). The same
    instance can be shared by both roles in different pipelines.
    """

    bootstrap_servers: str = ""
    group_id: str | None = None
    payload_format: str | None = None  # "json" | "raw_bytes" | "avro" | "protobuf"
    schema_registry_url: str | None = None
    # Π.1: typed Schema Registry reference. Accepts a
    # `SchemaRegistryConnection` instance or a registered SR name
    # string. Mutually exclusive with `schema_registry_url=`.
    schema_registry: str | SchemaRegistryConnection | None = None
    sasl_plain_username: str | None = None
    sasl_plain_password: str | None = None
    sasl_scram_username: str | None = None
    sasl_scram_password: str | None = None
    sasl_scram_mechanism: str | None = None  # "sha-256" | "sha-512"
    msk_iam_region: str | None = None

    def __post_init__(self) -> None:
        self.kind = "kafka"
        if not self.bootstrap_servers:
            raise ValueError(f"KafkaConnection({self.name!r}): bootstrap_servers is required")
        if self.schema_registry is not None and self.schema_registry_url is not None:
            raise ValueError(
                f"KafkaConnection({self.name!r}): set either "
                "`schema_registry=` (typed SR connection or name) OR the "
                "legacy `schema_registry_url=` shorthand, not both"
            )


@dataclass(repr=False)
class RabbitMQConnection(Connection):
    """RabbitMQ broker handle (AMQP 0.9.1)."""

    amqp_url: str = ""
    consumer_tag: str | None = None

    def __post_init__(self) -> None:
        self.kind = "rabbitmq"
        if not self.amqp_url:
            raise ValueError(f"RabbitMQConnection({self.name!r}): amqp_url is required")


@dataclass(repr=False)
class PubSubConnection(Connection):
    """GCP Pub/Sub project handle."""

    project_id: str = ""
    endpoint: str | None = None  # e.g. http://localhost:8085 for emulator
    anonymous_auth: bool = False  # True for emulator; production uses ADC

    def __post_init__(self) -> None:
        self.kind = "pubsub"
        if not self.project_id:
            raise ValueError(f"PubSubConnection({self.name!r}): project_id is required")


@dataclass(repr=False)
class KinesisConnection(Connection):
    """AWS Kinesis stream handle."""

    stream_name: str = ""
    region: str | None = None
    endpoint: str | None = None  # e.g. http://localhost:4566 for LocalStack
    access_key_id: str | None = None
    secret_access_key: str | None = None

    def __post_init__(self) -> None:
        self.kind = "kinesis"
        if not self.stream_name:
            raise ValueError(f"KinesisConnection({self.name!r}): stream_name is required")


# ---------- DB targets ---------------------------------------------


@dataclass(repr=False)
class PostgresConnection(Connection):
    """Postgres connection."""

    url: str = ""

    def __post_init__(self) -> None:
        self.kind = "postgres"
        if not self.url:
            raise ValueError(f"PostgresConnection({self.name!r}): url is required")


@dataclass(repr=False)
class MySQLConnection(Connection):
    """MySQL connection."""

    url: str = ""

    def __post_init__(self) -> None:
        self.kind = "mysql"
        if not self.url:
            raise ValueError(f"MySQLConnection({self.name!r}): url is required")


@dataclass(repr=False)
class SQLiteConnection(Connection):
    """SQLite database (file path or ``:memory:``)."""

    path: str = ""

    def __post_init__(self) -> None:
        self.kind = "sqlite"
        if not self.path:
            raise ValueError(f"SQLiteConnection({self.name!r}): path is required")


@dataclass(repr=False)
class DuckDBConnection(Connection):
    """DuckDB database (file path or ``:memory:``)."""

    path: str = ""

    def __post_init__(self) -> None:
        self.kind = "duckdb"
        if not self.path:
            raise ValueError(f"DuckDBConnection({self.name!r}): path is required")


# ---------- Lake / object-store targets ----------------------------


@dataclass(repr=False)
class DeltaLocalConnection(Connection):
    """Local-filesystem Delta Lake root directory."""

    path: str = ""

    def __post_init__(self) -> None:
        self.kind = "delta_local"
        if not self.path:
            raise ValueError(f"DeltaLocalConnection({self.name!r}): path is required")


@dataclass(repr=False)
class DeltaS3Connection(Connection):
    """S3-backed Delta Lake bucket."""

    endpoint: str = ""
    bucket: str = ""
    region: str = ""
    access_key_id: str = ""
    secret_access_key: str = ""
    prefix: str = ""

    def __post_init__(self) -> None:
        self.kind = "delta_s3"
        for required in ("endpoint", "bucket", "region", "access_key_id", "secret_access_key"):
            if not getattr(self, required):
                raise ValueError(
                    f"DeltaS3Connection({self.name!r}): {required} is required"
                )


@dataclass(repr=False)
class ObjectStoreLocalConnection(Connection):
    """Local-filesystem object store with a chosen file format."""

    path: str = ""
    format: str = ""  # "parquet" | "csv" | "orc" | "json_lines"

    def __post_init__(self) -> None:
        self.kind = "object_store_local"
        if not self.path:
            raise ValueError(f"ObjectStoreLocalConnection({self.name!r}): path is required")
        if not self.format:
            raise ValueError(f"ObjectStoreLocalConnection({self.name!r}): format is required")


@dataclass(repr=False)
class ObjectStoreS3Connection(Connection):
    """S3-backed object store with a chosen file format."""

    endpoint: str = ""
    bucket: str = ""
    region: str = ""
    access_key_id: str = ""
    secret_access_key: str = ""
    format: str = ""

    def __post_init__(self) -> None:
        self.kind = "object_store_s3"
        for required in (
            "endpoint",
            "bucket",
            "region",
            "access_key_id",
            "secret_access_key",
            "format",
        ):
            if not getattr(self, required):
                raise ValueError(
                    f"ObjectStoreS3Connection({self.name!r}): {required} is required"
                )


# ---------- Registry -----------------------------------------------

_REGISTRY: dict[str, Connection] = {}

# Map kind → factory class, used by the @ematix.connection class
# decorator to dispatch.
_KIND_FACTORIES: dict[str, type] = {
    "kafka": KafkaConnection,
    "rabbitmq": RabbitMQConnection,
    "pubsub": PubSubConnection,
    "kinesis": KinesisConnection,
    "schema_registry": SchemaRegistryConnection,
    "postgres": PostgresConnection,
    "mysql": MySQLConnection,
    "sqlite": SQLiteConnection,
    "duckdb": DuckDBConnection,
    "delta_local": DeltaLocalConnection,
    "delta_s3": DeltaS3Connection,
    "object_store_local": ObjectStoreLocalConnection,
    "object_store_s3": ObjectStoreS3Connection,
}


def register_connection(conn: Connection) -> Connection:
    """Register a connection under its ``name``. Returns the connection.

    Re-registration with the same name overwrites silently — the
    common case is a test that swaps in a fixture connection over a
    production-named one.
    """
    if not isinstance(conn, Connection):
        raise TypeError(
            f"register_connection: expected a Connection instance, got {type(conn).__name__}"
        )
    _REGISTRY[conn.name] = conn
    return conn


def get_connection(name: str) -> Connection:
    """Look up a registered connection by name.

    Raises ``KeyError`` if no connection with that name is registered;
    the error message lists the currently-registered names.
    """
    if name not in _REGISTRY:
        known = sorted(_REGISTRY)
        raise KeyError(
            f"connection {name!r} is not registered "
            f"(registered names: {known if known else '<none>'})"
        )
    return _REGISTRY[name]


def registered_connections() -> dict[str, Connection]:
    """Return a copy of the current registry. Useful for inspection / tests."""
    return dict(_REGISTRY)


def clear_registry() -> None:
    """Drop every registered connection. Tests reach for this between cases."""
    _REGISTRY.clear()


# ---------- Decorator ----------------------------------------------


def connection(cls: type) -> Connection:
    """Class decorator. The class body declares the connection's
    fields; the decorator builds the typed connection instance,
    registers it, and *returns* the instance — so the class binding
    in the user's module becomes the connection itself.

    The class must declare a ``kind = "..."`` class attribute (one
    of the registered ``Connection`` kinds). The connection's
    ``name`` defaults to the class name; override with ``name = "..."``
    on the class body.

    Example:

        @ematix.connection
        class kafka_prod:
            kind = "kafka"
            bootstrap_servers = "${KAFKA_BOOTSTRAP}"
            group_id = "ematix-flow"

        # `kafka_prod` is now a KafkaConnection instance, registered
        # under the name "kafka_prod".
    """
    if not isinstance(cls, type):
        raise TypeError(
            "@ematix.connection expects a class. For instance-based registration, "
            "call register_connection(conn) directly."
        )
    kind = getattr(cls, "kind", None)
    if not kind:
        raise TypeError(
            f"@ematix.connection class {cls.__name__} must declare a `kind` class "
            f"attribute (one of: {sorted(_KIND_FACTORIES)})"
        )
    factory = _KIND_FACTORIES.get(kind)
    if factory is None:
        raise TypeError(
            f"@ematix.connection class {cls.__name__}: unknown kind {kind!r} "
            f"(known: {sorted(_KIND_FACTORIES)})"
        )

    # Pull every declared field off the class. We accept any field
    # that exists on the target factory's dataclass (including
    # `name`, which defaults to the class name).
    factory_field_names = {f.name for f in fields(factory) if f.init}
    declared = {
        k: getattr(cls, k)
        for k in factory_field_names
        if k != "kind" and hasattr(cls, k)
    }
    declared.setdefault("name", cls.__name__)

    inst = factory(**declared)
    register_connection(inst)
    return inst
