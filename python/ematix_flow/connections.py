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

## ${...} interpolation — env + pluggable secret stores

Any ``str`` field on a connection can contain ``${...}``
references. The interpolation happens **at backend-build time**
(when the pipeline starts), not at connection-definition time —
so changing the underlying secret between definition and run picks
up the new value.

Supported reference shapes:

- ``${VAR}`` — bare env-var name. Resolves against ``os.environ``.
  Existing v0.1+ syntax; unchanged.
- ``${vault:path/to/secret#key}`` — HashiCorp Vault KV v2.
- ``${aws:secret-name#field}`` — AWS Secrets Manager (JSON field
  selector via ``#``).
- ``${gcp:secret-name#version}`` — GCP Secret Manager (short form;
  version defaults to ``latest``).

For the non-env resolvers to work the caller must register them at
app startup; see :mod:`ematix_flow.secrets`. Unresolvable references
raise :class:`ematix_flow.secrets.MissingSecretError` (a ``KeyError``
subclass, so legacy ``except KeyError`` handlers continue to work).

```python
@ematix.connection
class warehouse:
    kind = "postgres"
    # Mix and match — host from env, password from Vault.
    url = "postgres://app:${vault:db/prod#password}@${WAREHOUSE_HOST}/main"
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

import re
from dataclasses import dataclass, field, fields
from typing import Any

__all__ = [
    "Connection",
    "DeltaLocalConnection",
    "DeltaS3Connection",
    "DuckDBConnection",
    "GlueSchemaRegistryConnection",
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
_SECRET_TOKENS = frozenset({"password", "secret", "token", "key"})
_SECRET_FIELDS = frozenset(
    {
        "password",
        "secret",
        "secret_access_key",
        "access_key_id",
        "key_password",
        "private_key",
        "token",
        "api_key",
    }
)


def resolve(value: str | None) -> str | None:
    """Replace ``${...}`` references with values from registered
    secret resolvers.

    Bare ``${VAR}`` resolves against ``os.environ`` (backwards-compatible
    with v0.1). Prefixed forms like ``${vault:path#key}``,
    ``${aws:secret-name}``, ``${gcp:secret-name}`` dispatch to the
    matching resolver registered via
    :func:`ematix_flow.secrets.register_resolver`.

    Returns ``None`` unchanged. Raises
    :class:`ematix_flow.secrets.MissingSecretError` (a ``KeyError``
    subclass, so existing ``except KeyError`` handlers still catch it)
    if a referenced secret can't be resolved.

    Delegates the actual interpolation to :func:`ematix_flow.secrets.expand`
    so connection strings and DSN templates share one syntax.
    """
    from ematix_flow.secrets import expand

    return expand(value)


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

    ``basic_auth_user`` / ``basic_auth_password`` are emitted as
    ``schema_registry_basic_auth_{user,password}`` TOML fields and
    consumed by the Rust runner via
    ``KafkaBackend::with_schema_registry_basic_auth(...)``, which
    feeds ``SrSettings::set_basic_authorization`` for every Avro /
    Protobuf encode/decode against the registry. ``repr()`` redacts
    the password.
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
class GlueSchemaRegistryConnection(Connection):
    """AWS Glue Schema Registry handle.

    Different wire format from Confluent SR (``0x03`` header byte +
    16-byte schema UUID + 1-byte compression byte + payload, vs
    Confluent's ``0x00`` + 4-byte BE schema id), and AWS IAM auth
    instead of HTTP basic. The Rust runner picks the framing based on
    the connection ``kind``.

    Auth options, in priority order:

    1. ``aws_profile=`` — read credentials from ``~/.aws/credentials``
       under the named profile (best for dev).
    2. ``aws_access_key_id=`` + ``aws_secret_access_key=`` — explicit
       static creds (discouraged; prefer IAM role / SSO / env vars).
       Both must be set together.
    3. Implicit — boto3's default credential chain (env vars, EC2
       instance metadata, EKS pod identity, etc.). This is the
       recommended production path; leave the auth fields unset.
    """

    registry_name: str = ""
    region: str = ""
    aws_profile: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    def __post_init__(self) -> None:
        self.kind = "glue_schema_registry"
        if not self.registry_name:
            raise ValueError(
                f"GlueSchemaRegistryConnection({self.name!r}): "
                "registry_name is required"
            )
        if not self.region:
            raise ValueError(
                f"GlueSchemaRegistryConnection({self.name!r}): region is required"
            )
        # Both halves of static creds must be set together; partial is
        # an obvious misuse — fail at construction, not on first call.
        has_key = bool(self.aws_access_key_id)
        has_secret = bool(self.aws_secret_access_key)
        if has_key != has_secret:
            raise ValueError(
                f"GlueSchemaRegistryConnection({self.name!r}): "
                "aws_access_key_id and aws_secret_access_key must be set "
                "together (or both omitted to use boto3's default "
                "credential chain)"
            )
        # Profile + explicit creds is ambiguous — the user picked two
        # auth modes. Reject up front.
        if self.aws_profile and (has_key or has_secret):
            raise ValueError(
                f"GlueSchemaRegistryConnection({self.name!r}): pass either "
                "aws_profile= OR aws_access_key_id/aws_secret_access_key=, "
                "not both"
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
    # Consumer-side `auto.offset.reset` — where a fresh consumer
    # group with no committed offsets starts reading.
    #   "earliest" (default): from the start of the topic
    #   "latest": only events produced AFTER the consumer subscribes
    # Maps directly to rdkafka's `auto.offset.reset`.
    auto_offset_reset: str | None = None
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
    "glue_schema_registry": GlueSchemaRegistryConnection,
    "postgres": PostgresConnection,
    "mysql": MySQLConnection,
    "sqlite": SQLiteConnection,
    "duckdb": DuckDBConnection,
    "delta_local": DeltaLocalConnection,
    "delta_s3": DeltaS3Connection,
    "object_store_local": ObjectStoreLocalConnection,
    "object_store_s3": ObjectStoreS3Connection,
}


# Π.4d: file-format auto-detection from path / URL extension.
#
# Maps to the same string values ObjectStoreLocalConnection.format and
# ObjectStoreS3Connection.format accept. Use this when you have a path
# that already names the format ("s3://bucket/data.csv") and don't want
# to repeat yourself in the connection construction:
#
#     fmt = format_from_path("s3://bucket/raw/data.tsv.gz")
#     # fmt == "csv"
#     conn = ObjectStoreS3Connection(..., format=fmt)

# Order matters for `.json` vs `.json_lines`: longer / more specific
# suffixes are checked first so `.ndjson` doesn't get matched as `.json`
# (it can't — different suffix — but kept here for clarity if more
# variants are added).
_FORMAT_EXTENSIONS = (
    # (suffix, format)
    (".parquet", "parquet"),
    (".pq", "parquet"),
    (".tsv", "csv"),
    (".csv", "csv"),
    (".ndjson", "json_lines"),
    (".jsonl", "json_lines"),
    (".json", "json_lines"),  # treat single-file JSON as JSONL for now
    (".orc", "orc"),
)

# Compression suffixes we strip before format matching — `data.csv.gz`
# and `data.csv.zst` are both still CSV, just compressed in transit.
_COMPRESSION_STRIP_SUFFIXES = (".gz", ".bz2", ".zst", ".zstd", ".snappy", ".lz4")


def format_from_path(path: str) -> str | None:
    """Pick an ObjectStore format string from a filesystem path or URL.

    Returns one of "parquet" | "csv" | "json_lines" | "orc", or None
    when the extension isn't recognized. Compression suffixes
    (.gz, .zst, .bz2, .lz4, .snappy) are stripped before matching so
    e.g. ``"events.csv.gz"`` → ``"csv"``. Case-insensitive.

    Examples::

        format_from_path("s3://bucket/data.parquet")     == "parquet"
        format_from_path("/tmp/events.csv.gz")           == "csv"
        format_from_path("https://host/logs.ndjson")     == "json_lines"
        format_from_path("/tmp/no-extension")            is None
    """
    s = path.lower()
    # Strip a trailing compression suffix (only one — `csv.gz.gz`
    # isn't a thing in practice).
    for comp in _COMPRESSION_STRIP_SUFFIXES:
        if s.endswith(comp):
            s = s[: -len(comp)]
            break
    for ext, fmt in _FORMAT_EXTENSIONS:
        if s.endswith(ext):
            return fmt
    return None


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
