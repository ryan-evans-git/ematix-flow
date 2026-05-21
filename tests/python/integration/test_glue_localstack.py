"""Task #556 — LocalStack-backed integration tests for Glue Schema Registry.

These tests round-trip the Python boto3-backed helpers
(:mod:`ematix_flow.glue_schema_registry`) against a real
LocalStack-emulated Glue service. They cover what unit tests with
mocked boto3 clients cannot:

* Real ``CreateSchema`` + ``RegisterSchemaVersion`` payloads land
  correctly in LocalStack and round-trip through ``GetSchemaVersion``.
* The Rust→Python callback dispatcher returns the right schema text
  for a known UUID when wired through the compiled extension.

Gated on ``EMATIX_FLOW_LOCALSTACK_ENDPOINT`` — when unset, the whole
module is skipped so a developer with no Docker running doesn't see
spurious failures.

Setup (one-time):

.. code-block:: shell

    docker compose -f examples/glue-localstack/docker-compose.yml up -d
    export EMATIX_FLOW_LOCALSTACK_ENDPOINT=http://localhost:4566
    export AWS_ACCESS_KEY_ID=test
    export AWS_SECRET_ACCESS_KEY=test
    export AWS_DEFAULT_REGION=us-east-1
"""
from __future__ import annotations

import json
import os
import uuid

import pytest

LOCALSTACK_ENDPOINT = os.environ.get("EMATIX_FLOW_LOCALSTACK_ENDPOINT")

if not LOCALSTACK_ENDPOINT:
    pytest.skip(
        "EMATIX_FLOW_LOCALSTACK_ENDPOINT unset — run "
        "examples/glue-localstack/docker-compose.yml first",
        allow_module_level=True,
    )

boto3 = pytest.importorskip("boto3", reason="boto3 required for Glue integration")

# Imports come after the skip / importorskip so a developer without
# LocalStack or boto3 doesn't see misleading collection errors.
from ematix_flow.connections import GlueSchemaRegistryConnection  # noqa: E402
from ematix_flow.glue_schema_registry import (  # noqa: E402
    GLUE_SCHEMA_LOOKUP_CALLBACK,
    fetch_schema_by_uuid,
    register_glue_schema_lookup_callback,
    register_schema,
    unregister_glue_schema_lookup_callback,
)


def _glue_client():
    """Build a boto3 Glue client pointing at LocalStack. Honors the
    ambient ``AWS_*`` env vars the developer set up before running."""
    return boto3.client(
        "glue",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )


@pytest.fixture
def localstack_registry():
    """Create a fresh Glue registry in LocalStack for each test, and
    tear it down after — keeps tests hermetic without rebuilding the
    container.

    LocalStack initialises Glue with no default registry, so we have
    to create one. We use a UUID-suffixed name per test so parallel
    test workers don't collide.
    """
    client = _glue_client()
    registry_name = f"ematix-test-{uuid.uuid4().hex[:8]}"
    client.create_registry(RegistryName=registry_name)
    yield registry_name
    # Tear down: list all schemas under this registry, then delete
    # them, then delete the registry itself. LocalStack's delete_
    # registry doesn't cascade.
    try:
        schemas = client.list_schemas(
            RegistryId={"RegistryName": registry_name},
        ).get("Schemas", [])
        for s in schemas:
            client.delete_schema(SchemaId={"SchemaArn": s["SchemaArn"]})
        client.delete_registry(RegistryId={"RegistryName": registry_name})
    except Exception:
        # Best-effort — leaving state in LocalStack is fine since
        # the container restarts fresh each `docker compose up`.
        pass


def _conn(registry_name: str) -> GlueSchemaRegistryConnection:
    return GlueSchemaRegistryConnection(
        name="localstack",
        registry_name=registry_name,
        region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )


def _client_for(conn):
    """Build a boto3 client that the helpers will use — pointed at
    LocalStack via endpoint_url override."""
    return boto3.client(
        "glue",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=conn.region,
        aws_access_key_id=conn.aws_access_key_id,
        aws_secret_access_key=conn.aws_secret_access_key,
    )


class TestGlueRegisterAndFetchRoundTrip:
    """End-to-end: register a schema, fetch it back by UUID, verify
    the schema text + format survive the round trip."""

    def test_create_schema_then_fetch_by_uuid(self, localstack_registry):
        conn = _conn(localstack_registry)
        client = _client_for(conn)

        # First registration creates the parent schema.
        avro_text = (
            '{"type":"record","name":"Order",'
            '"fields":[{"name":"id","type":"long"},'
            '{"name":"qty","type":"int"}]}'
        )
        result = register_schema(
            conn,
            schema_name="Order",
            data_format="AVRO",
            schema_definition=avro_text,
            _client=client,
        )
        assert result.data_format == "AVRO"
        assert result.version_number == 1
        first_uuid = result.schema_uuid

        # Fetch by UUID — should round-trip the schema text.
        fetched = fetch_schema_by_uuid(conn, first_uuid, _client=client)
        assert fetched.schema_uuid == first_uuid
        assert fetched.data_format == "AVRO"
        assert "Order" in fetched.schema_definition
        assert "qty" in fetched.schema_definition

    def test_register_new_version_uses_register_path(self, localstack_registry):
        conn = _conn(localstack_registry)
        client = _client_for(conn)

        v1_text = (
            '{"type":"record","name":"Order","fields":'
            '[{"name":"id","type":"long"}]}'
        )
        v1 = register_schema(
            conn,
            schema_name="Order",
            data_format="AVRO",
            schema_definition=v1_text,
            _client=client,
        )

        # Schema-evolution-compatible v2 (add optional field).
        v2_text = (
            '{"type":"record","name":"Order","fields":'
            '[{"name":"id","type":"long"},'
            '{"name":"qty","type":["null","int"],"default":null}]}'
        )
        v2 = register_schema(
            conn,
            schema_name="Order",
            data_format="AVRO",
            schema_definition=v2_text,
            _client=client,
        )
        # v2 must be a NEW UUID with version_number incremented; the
        # fallback-to-create path would have collided.
        assert v2.schema_uuid != v1.schema_uuid
        assert v2.version_number == v1.version_number + 1


class TestCallbackDispatcherEndToEnd:
    """Verify the Python-side dispatcher works end-to-end against
    LocalStack: register the callback, invoke it via the public
    dispatcher entrypoint, confirm the response shape matches what
    the Rust Kafka backend expects."""

    def test_dispatcher_round_trip_against_localstack(self, localstack_registry):
        from ematix_flow.glue_schema_registry import _glue_lookup_dispatcher

        conn = _conn(localstack_registry)
        client = _client_for(conn)

        avro_text = (
            '{"type":"record","name":"X","fields":'
            '[{"name":"i","type":"int"}]}'
        )
        registered = register_schema(
            conn,
            schema_name="X",
            data_format="AVRO",
            schema_definition=avro_text,
            _client=client,
        )

        # Register the dispatcher's connection map manually since
        # we're driving _glue_lookup_dispatcher directly (the
        # public register_*_callback assumes the compiled extension
        # is available).
        from ematix_flow.glue_schema_registry import _REGISTRY_CONNECTIONS
        _REGISTRY_CONNECTIONS[conn.registry_name] = conn

        # Monkey-patch fetch_schema_by_uuid to use the LocalStack
        # client. In production the connection would carry boto3
        # default-chain creds; here we need to inject the endpoint.
        from unittest.mock import patch
        try:
            with patch(
                "ematix_flow.glue_schema_registry._build_glue_client",
                return_value=client,
            ):
                req = json.dumps({
                    "schema_uuid": registered.schema_uuid,
                    "region": conn.region,
                    "registry_name": conn.registry_name,
                }).encode("utf-8")
                resp_bytes = _glue_lookup_dispatcher(req)
                resp = json.loads(resp_bytes.decode("utf-8"))
                assert resp["data_format"] == "AVRO"
                assert "record" in resp["schema_definition"]
                assert resp["schema_uuid"] == registered.schema_uuid
        finally:
            _REGISTRY_CONNECTIONS.pop(conn.registry_name, None)


class TestPyO3BridgeIfBuilt:
    """Wire the dispatcher through ``ematix_flow._core`` (the compiled
    extension). Skipped when the extension isn't installed — the
    Python-only dispatcher tests above still cover the JSON shape."""

    def test_registered_callback_invokable_via_rust(self, localstack_registry):
        try:
            from ematix_flow import _core  # type: ignore
        except ImportError:
            pytest.skip("ematix_flow._core not built; run `maturin develop`")

        conn = _conn(localstack_registry)
        client = _client_for(conn)
        avro_text = (
            '{"type":"record","name":"X","fields":'
            '[{"name":"i","type":"int"}]}'
        )
        registered = register_schema(
            conn,
            schema_name="X",
            data_format="AVRO",
            schema_definition=avro_text,
            _client=client,
        )

        # Register the dispatcher with the Rust side. We can't use
        # the production helper because it constructs a non-mocked
        # boto3 client; instead inject the LocalStack client through
        # the same patch the dispatcher honours.
        from unittest.mock import patch

        from ematix_flow.glue_schema_registry import _REGISTRY_CONNECTIONS
        _REGISTRY_CONNECTIONS[conn.registry_name] = conn
        try:
            register_glue_schema_lookup_callback(conn)
            assert _core.is_python_callback_registered(GLUE_SCHEMA_LOOKUP_CALLBACK)

            with patch(
                "ematix_flow.glue_schema_registry._build_glue_client",
                return_value=client,
            ):
                req_bytes = json.dumps({
                    "schema_uuid": registered.schema_uuid,
                    "region": conn.region,
                    "registry_name": conn.registry_name,
                }).encode("utf-8")
                resp_bytes = bytes(
                    _core.invoke_python_callback(
                        GLUE_SCHEMA_LOOKUP_CALLBACK, req_bytes,
                    )
                )
                resp = json.loads(resp_bytes.decode("utf-8"))
                assert resp["schema_uuid"] == registered.schema_uuid
                assert resp["data_format"] == "AVRO"
        finally:
            unregister_glue_schema_lookup_callback()
            _REGISTRY_CONNECTIONS.pop(conn.registry_name, None)
