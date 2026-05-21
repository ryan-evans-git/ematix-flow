"""Python-side AWS Glue Schema Registry client (task #556 slice 2).

The Rust runner carries the wire-format primitives
(:mod:`crates/ematix-flow-core/src/glue_schema_registry.rs`) — header
byte, UUID parse, codec dispatch. The schema-fetch + schema-register
operations themselves live here in Python because:

1. The official AWS SDK for Rust has no Glue Schema Registry client at
   time of writing; ``boto3`` is the maintained surface.
2. The decode hot path doesn't need the SDK on every message — schemas
   are looked up once per UUID and cached. The Rust hot path takes the
   already-parsed Avro/Protobuf schema as input.

Two helpers:

* :func:`fetch_schema_by_uuid` — Glue's ``GetSchemaVersion`` API. Given
  a registry handle + schema UUID (parsed from the wire frame), returns
  the schema text + format ("AVRO" / "PROTOBUF" / "JSON").
* :func:`register_schema` — Glue's ``RegisterSchemaVersion`` /
  ``CreateSchema``. Producers call this once when a topic is created
  or when a schema evolves; the returned UUID is what they embed in
  every subsequent message frame.

Both helpers accept a ``_client`` test hook so unit tests can pin the
shape without touching real AWS.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from ematix_flow.connections import GlueSchemaRegistryConnection
from ematix_flow.secrets import expand

__all__ = [
    "GLUE_SCHEMA_LOOKUP_BY_NAME_CALLBACK",
    "GLUE_SCHEMA_LOOKUP_CALLBACK",
    "GlueSchema",
    "fetch_schema_by_name",
    "fetch_schema_by_uuid",
    "register_glue_schema_lookup_callback",
    "register_schema",
    "unregister_glue_schema_lookup_callback",
]


# Allowed Glue data formats. Anything else is rejected at the API call
# rather than waiting for the network round-trip to surface the error.
_VALID_DATA_FORMATS = frozenset({"AVRO", "PROTOBUF", "JSON"})


# Name the Rust Kafka backend looks up when it needs to resolve a
# Glue schema UUID (consumer path — the UUID arrives in the wire
# frame). Treat as a stable identifier — must match the value used
# in ``crates/ematix-flow-core/src/kafka_backend.rs``.
GLUE_SCHEMA_LOOKUP_CALLBACK = (
    "ematix_flow.glue_schema_registry.fetch_schema_by_uuid"
)


# Name the Rust Kafka backend looks up when it needs to resolve a
# Glue schema by its registered name (producer path — the producer
# knows the schema name from config and needs the latest UUID +
# definition to Avro-encode + Glue-frame each batch).
GLUE_SCHEMA_LOOKUP_BY_NAME_CALLBACK = (
    "ematix_flow.glue_schema_registry.fetch_schema_by_name"
)


# Per-registry-name connection map used by the dispatcher callback.
# Populated by :func:`register_glue_schema_lookup_callback`. A single
# process can serve multiple Glue registries (different regions,
# different IAM roles) by registering each connection in turn.
_REGISTRY_CONNECTIONS: dict[str, GlueSchemaRegistryConnection] = {}


@dataclass(frozen=True)
class GlueSchema:
    """A schema fetched from Glue Schema Registry.

    - ``schema_uuid``: the schema-version UUID that was embedded in the
      Kafka message frame (and that produced this lookup).
    - ``data_format``: ``"AVRO"`` / ``"PROTOBUF"`` / ``"JSON"`` — drives
      which decoder the Rust runner picks.
    - ``schema_definition``: the schema text (Avro JSON, .proto source,
      or JSON Schema). Caller hands this to the decoder.
    - ``schema_arn``: the parent-schema ARN, used to resolve sibling
      versions for compat checks.
    - ``version_number``: monotonic version index assigned by Glue.
    """

    schema_uuid: str
    data_format: str
    schema_definition: str
    schema_arn: str
    version_number: int


def _build_glue_client(
    conn: GlueSchemaRegistryConnection, _client: Any | None
) -> Any:
    """Construct a boto3 Glue client honouring the connection's auth
    fields. ``_client`` is the test hook — when supplied we return it
    untouched and skip the boto3 import."""
    if _client is not None:
        return _client
    try:
        import boto3  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ImportError(
            "boto3 is required for Glue Schema Registry access; "
            "install with `pip install ematix-flow[glue-schema-registry]` "
            "or `pip install boto3`"
        ) from exc
    region = expand(conn.region)
    if conn.aws_profile:
        session = boto3.Session(profile_name=expand(conn.aws_profile))
        return session.client("glue", region_name=region)
    if conn.aws_access_key_id and conn.aws_secret_access_key:
        return boto3.client(
            "glue",
            region_name=region,
            aws_access_key_id=expand(conn.aws_access_key_id),
            aws_secret_access_key=expand(conn.aws_secret_access_key),
        )
    return boto3.client("glue", region_name=region)


def fetch_schema_by_name(
    conn: GlueSchemaRegistryConnection,
    schema_name: str,
    *,
    version: int | str = "LATEST",
    _client: Any | None = None,
) -> GlueSchema:
    """Resolve a Glue schema by registered name to its UUID + text.

    Used by the Kafka *producer* path (task #556 producer slice): the
    producer knows the schema name from config and needs the latest
    UUID + definition text on the first send so it can Avro-encode +
    Glue-frame each batch. Subsequent batches use the cached UUID.

    ``version`` accepts an integer version number or the string
    ``"LATEST"`` (the default). Glue's API uses
    ``SchemaVersionNumber={"LatestVersion": True}`` for the latest
    case; specific version numbers go through
    ``SchemaVersionNumber={"VersionNumber": N}``.
    """
    client = _build_glue_client(conn, _client)
    registry_name = expand(conn.registry_name)
    schema_id = {"RegistryName": registry_name, "SchemaName": schema_name}
    if version == "LATEST":
        version_arg: dict[str, Any] = {"LatestVersion": True}
    else:
        version_arg = {"VersionNumber": int(version)}
    response = client.get_schema_version(
        SchemaId=schema_id,
        SchemaVersionNumber=version_arg,
    )
    data_format = response.get("DataFormat")
    if data_format not in _VALID_DATA_FORMATS:
        raise ValueError(
            f"Glue schema {schema_name!r} has unsupported DataFormat "
            f"{data_format!r}; expected one of "
            f"{sorted(_VALID_DATA_FORMATS)}"
        )
    return GlueSchema(
        schema_uuid=response["SchemaVersionId"],
        data_format=data_format,
        schema_definition=response["SchemaDefinition"],
        schema_arn=response["SchemaArn"],
        version_number=int(response["VersionNumber"]),
    )


def fetch_schema_by_uuid(
    conn: GlueSchemaRegistryConnection,
    schema_uuid: str,
    *,
    _client: Any | None = None,
) -> GlueSchema:
    """Resolve a Glue schema UUID to its definition.

    Used by the Kafka decoder: when a message arrives with Glue framing
    (header ``0x03`` + 16-byte UUID + 1-byte codec + payload), the UUID
    gets passed here to recover the schema.

    The returned :class:`GlueSchema` is what the Rust Avro / Protobuf
    decoder needs to turn the payload bytes into rows. Callers should
    cache by UUID — schemas are immutable per version.
    """
    client = _build_glue_client(conn, _client)
    response = client.get_schema_version(
        SchemaVersionId=schema_uuid,
    )
    data_format = response.get("DataFormat")
    if data_format not in _VALID_DATA_FORMATS:
        raise ValueError(
            f"Glue schema {schema_uuid!r} has unsupported DataFormat "
            f"{data_format!r}; expected one of "
            f"{sorted(_VALID_DATA_FORMATS)}"
        )
    return GlueSchema(
        schema_uuid=schema_uuid,
        data_format=data_format,
        schema_definition=response["SchemaDefinition"],
        schema_arn=response["SchemaArn"],
        version_number=int(response["VersionNumber"]),
    )


def register_schema(
    conn: GlueSchemaRegistryConnection,
    *,
    schema_name: str,
    data_format: str,
    schema_definition: str,
    compatibility: str = "BACKWARD",
    description: str | None = None,
    _client: Any | None = None,
) -> GlueSchema:
    """Register a new schema (or new version of an existing schema).

    First attempts ``RegisterSchemaVersion`` against the parent
    ``schema_name``; if Glue reports the parent doesn't exist yet (it's
    the producer's first message), falls back to ``CreateSchema`` which
    creates both the parent and version 1.

    Returns the newly-issued :class:`GlueSchema` (including the UUID
    producers must embed in every subsequent message frame).
    """
    if data_format not in _VALID_DATA_FORMATS:
        raise ValueError(
            f"data_format={data_format!r} is not valid; expected one of "
            f"{sorted(_VALID_DATA_FORMATS)}"
        )
    client = _build_glue_client(conn, _client)
    registry_name = expand(conn.registry_name)
    # SchemaId is the parent-schema reference; Glue uses (registry,
    # schema name) as the natural key, not the UUID.
    schema_id = {"RegistryName": registry_name, "SchemaName": schema_name}
    try:
        response = client.register_schema_version(
            SchemaId=schema_id,
            SchemaDefinition=schema_definition,
        )
    except Exception as exc:
        # boto3 botocore.exceptions.ClientError raised with EntityNotFound
        # → fall back to CreateSchema. We match on the exception text so
        # the mock client only has to opt in via side_effect=…
        msg = str(exc)
        if "EntityNotFound" not in msg and "does not exist" not in msg:
            raise
        kwargs = {
            "RegistryId": {"RegistryName": registry_name},
            "SchemaName": schema_name,
            "DataFormat": data_format,
            "Compatibility": compatibility,
            "SchemaDefinition": schema_definition,
        }
        if description is not None:
            kwargs["Description"] = description
        response = client.create_schema(**kwargs)
    return GlueSchema(
        schema_uuid=response["SchemaVersionId"],
        data_format=data_format,
        schema_definition=schema_definition,
        schema_arn=response["SchemaArn"],
        version_number=int(response.get("VersionNumber", 1)),
    )


# ---------------------------------------------------------------------------
# Rust callback wiring (task #556 dispatch slice)
# ---------------------------------------------------------------------------


def _glue_lookup_by_name_dispatcher(req_bytes: bytes) -> bytes:
    """Adapter invoked by the Rust Kafka *producer* when it needs to
    resolve a schema by name to its UUID + text before encoding a
    batch.

    Request JSON shape:

    .. code-block:: json

        {"schema_name": "Order", "registry_name": "my-registry",
         "region": "us-east-1"}

    Response shape mirrors :func:`_glue_lookup_dispatcher` — the same
    :class:`GlueSchemaResponse` structure on the Rust side
    deserializes both producer and consumer responses.
    """
    req = json.loads(req_bytes.decode("utf-8"))
    registry_name = req["registry_name"]
    schema_name = req["schema_name"]
    conn = _REGISTRY_CONNECTIONS.get(registry_name)
    if conn is None:
        raise KeyError(
            f"no GlueSchemaRegistryConnection registered for "
            f"registry_name={registry_name!r}; call "
            f"register_glue_schema_lookup_callback(conn) at startup"
        )
    schema = fetch_schema_by_name(conn, schema_name)
    resp = {
        "schema_uuid": schema.schema_uuid,
        "data_format": schema.data_format,
        "schema_definition": schema.schema_definition,
    }
    return json.dumps(resp).encode("utf-8")


def _glue_lookup_dispatcher(req_bytes: bytes) -> bytes:
    """Adapter invoked by the Rust Kafka backend when it sees a Glue-
    framed message and needs the schema text for the embedded UUID.

    The Rust side passes a JSON-encoded :class:`GlueSchemaRequest`:

    .. code-block:: json

        {"schema_uuid": "12345678-...", "region": "us-east-1",
         "registry_name": "my-registry"}

    We dispatch by ``registry_name`` to the connection the user
    registered earlier with :func:`register_glue_schema_lookup_callback`
    and return a JSON-encoded :class:`GlueSchemaResponse`:

    .. code-block:: json

        {"schema_uuid": "...", "data_format": "AVRO",
         "schema_definition": "{\\"type\\":\\"record\\",...}"}

    Unknown ``registry_name`` raises so the user sees a clear error
    on the Rust side ("CallbackFailed") instead of a silent fall-
    through.
    """
    req = json.loads(req_bytes.decode("utf-8"))
    registry_name = req["registry_name"]
    schema_uuid = req["schema_uuid"]
    conn = _REGISTRY_CONNECTIONS.get(registry_name)
    if conn is None:
        raise KeyError(
            f"no GlueSchemaRegistryConnection registered for "
            f"registry_name={registry_name!r}; call "
            f"register_glue_schema_lookup_callback(conn) at startup "
            f"with the matching connection"
        )
    schema = fetch_schema_by_uuid(conn, schema_uuid)
    resp = {
        "schema_uuid": schema.schema_uuid,
        "data_format": schema.data_format,
        "schema_definition": schema.schema_definition,
    }
    return json.dumps(resp).encode("utf-8")


def register_glue_schema_lookup_callback(
    conn: GlueSchemaRegistryConnection,
    *,
    _registry_module: Any | None = None,
) -> None:
    """Bind a Glue connection to the Rust-side schema-lookup callback.

    Idempotent — call once per connection at process startup, before
    any Kafka consumer reads from a Glue-framed topic. Multiple
    connections can coexist; the dispatcher routes by
    ``conn.registry_name``.

    ``_registry_module`` is an internal test hook so unit tests can
    inject a fake registry without depending on the compiled
    extension. Production callers pass only ``conn``.
    """
    _REGISTRY_CONNECTIONS[conn.registry_name] = conn
    if _registry_module is None:
        try:
            from ematix_flow import _core as _registry_module  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "ematix_flow._core (compiled extension) is required to "
                "register a Rust-callable Glue schema lookup; install the "
                "wheel or run `maturin develop`"
            ) from exc
    _registry_module.register_python_callback(
        GLUE_SCHEMA_LOOKUP_CALLBACK,
        _glue_lookup_dispatcher,
    )
    # Producer-side by-name lookup. Wired together with the consumer
    # callback so a single ``register_glue_schema_lookup_callback``
    # call covers both directions.
    _registry_module.register_python_callback(
        GLUE_SCHEMA_LOOKUP_BY_NAME_CALLBACK,
        _glue_lookup_by_name_dispatcher,
    )


def unregister_glue_schema_lookup_callback(
    registry_name: str | None = None,
    *,
    _registry_module: Any | None = None,
) -> None:
    """Remove a Glue registry binding. With ``registry_name=None``
    (the default) clears every binding and unregisters the Rust-side
    dispatcher entirely; useful at test teardown."""
    if registry_name is None:
        _REGISTRY_CONNECTIONS.clear()
        if _registry_module is None:
            try:
                from ematix_flow import _core as _registry_module  # type: ignore
            except ImportError:
                return  # nothing to unregister if the ext wasn't loaded
        _registry_module.unregister_python_callback(GLUE_SCHEMA_LOOKUP_CALLBACK)
        _registry_module.unregister_python_callback(
            GLUE_SCHEMA_LOOKUP_BY_NAME_CALLBACK,
        )
    else:
        _REGISTRY_CONNECTIONS.pop(registry_name, None)
