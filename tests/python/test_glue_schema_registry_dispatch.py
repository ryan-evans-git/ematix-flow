"""Task #556 dispatch slice — Rust→Python schema-lookup callback.

The Rust Kafka backend calls this dispatcher when it sees a Glue-
framed message and needs the schema text for the embedded UUID.
Tests here verify the JSON marshalling + per-registry dispatch
without needing the compiled extension — the production wiring goes
through ``ematix_flow._core.register_python_callback``, but the
dispatcher itself is pure Python and unit-testable in isolation.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from ematix_flow.connections import GlueSchemaRegistryConnection
from ematix_flow.glue_schema_registry import (
    _REGISTRY_CONNECTIONS,
    GLUE_SCHEMA_LOOKUP_CALLBACK,
    _glue_lookup_dispatcher,
    register_glue_schema_lookup_callback,
    unregister_glue_schema_lookup_callback,
)


@pytest.fixture(autouse=True)
def _reset_dispatcher_state():
    """Each test starts with a fresh dispatcher state — registry-name
    bindings from one test shouldn't leak into the next."""
    _REGISTRY_CONNECTIONS.clear()
    yield
    _REGISTRY_CONNECTIONS.clear()


class TestRegisterAndUnregister:
    def test_register_stores_connection_under_registry_name(self) -> None:
        from ematix_flow.glue_schema_registry import (
            GLUE_SCHEMA_LOOKUP_BY_NAME_CALLBACK,
        )

        conn = GlueSchemaRegistryConnection(
            name="g", registry_name="orders-registry", region="us-east-1",
        )
        # Use a fake _registry_module so the test doesn't require the
        # compiled extension. We just need the call shape verified.
        fake = MagicMock()
        register_glue_schema_lookup_callback(conn, _registry_module=fake)
        assert _REGISTRY_CONNECTIONS["orders-registry"] is conn
        # Both callbacks register together — one for consumer (by UUID),
        # one for producer (by name).
        assert fake.register_python_callback.call_count == 2
        callback_names = {
            call.args[0]
            for call in fake.register_python_callback.call_args_list
        }
        assert GLUE_SCHEMA_LOOKUP_CALLBACK in callback_names
        assert GLUE_SCHEMA_LOOKUP_BY_NAME_CALLBACK in callback_names

    def test_register_is_idempotent(self) -> None:
        conn = GlueSchemaRegistryConnection(
            name="g", registry_name="r", region="us-east-1",
        )
        fake = MagicMock()
        register_glue_schema_lookup_callback(conn, _registry_module=fake)
        register_glue_schema_lookup_callback(conn, _registry_module=fake)
        # Calling twice overwrites the binding rather than appending —
        # the registry map should still have exactly one entry.
        assert len(_REGISTRY_CONNECTIONS) == 1

    def test_multiple_registries_coexist(self) -> None:
        a = GlueSchemaRegistryConnection(
            name="a", registry_name="orders", region="us-east-1",
        )
        b = GlueSchemaRegistryConnection(
            name="b", registry_name="events", region="eu-west-1",
        )
        fake = MagicMock()
        register_glue_schema_lookup_callback(a, _registry_module=fake)
        register_glue_schema_lookup_callback(b, _registry_module=fake)
        assert _REGISTRY_CONNECTIONS["orders"] is a
        assert _REGISTRY_CONNECTIONS["events"] is b

    def test_unregister_clears_everything_by_default(self) -> None:
        from ematix_flow.glue_schema_registry import (
            GLUE_SCHEMA_LOOKUP_BY_NAME_CALLBACK,
        )

        conn = GlueSchemaRegistryConnection(
            name="g", registry_name="r", region="us-east-1",
        )
        fake = MagicMock()
        register_glue_schema_lookup_callback(conn, _registry_module=fake)
        unregister_glue_schema_lookup_callback(_registry_module=fake)
        assert _REGISTRY_CONNECTIONS == {}
        # Both callbacks unregister together.
        cleared = {
            call.args[0]
            for call in fake.unregister_python_callback.call_args_list
        }
        assert GLUE_SCHEMA_LOOKUP_CALLBACK in cleared
        assert GLUE_SCHEMA_LOOKUP_BY_NAME_CALLBACK in cleared

    def test_unregister_by_name_only_removes_that_binding(self) -> None:
        a = GlueSchemaRegistryConnection(
            name="a", registry_name="orders", region="us-east-1",
        )
        b = GlueSchemaRegistryConnection(
            name="b", registry_name="events", region="eu-west-1",
        )
        fake = MagicMock()
        register_glue_schema_lookup_callback(a, _registry_module=fake)
        register_glue_schema_lookup_callback(b, _registry_module=fake)
        unregister_glue_schema_lookup_callback("orders", _registry_module=fake)
        assert "orders" not in _REGISTRY_CONNECTIONS
        assert "events" in _REGISTRY_CONNECTIONS


class TestDispatcher:
    """The dispatcher takes bytes (JSON request) in, returns bytes
    (JSON response). The actual schema fetch goes through
    ``fetch_schema_by_uuid``, which we patch with a mock boto3 client
    so the test stays offline."""

    def _conn(self) -> GlueSchemaRegistryConnection:
        return GlueSchemaRegistryConnection(
            name="g", registry_name="r", region="us-east-1",
        )

    def _register(self) -> GlueSchemaRegistryConnection:
        conn = self._conn()
        _REGISTRY_CONNECTIONS[conn.registry_name] = conn
        return conn

    def test_dispatch_returns_schema_response_bytes(self) -> None:
        self._register()
        req_bytes = json.dumps({
            "schema_uuid": "12345678-1234-5678-1234-567812345678",
            "region": "us-east-1",
            "registry_name": "r",
        }).encode("utf-8")

        # Patch fetch_schema_by_uuid's underlying boto3 client.
        from unittest.mock import patch

        from ematix_flow.glue_schema_registry import GlueSchema

        with patch(
            "ematix_flow.glue_schema_registry.fetch_schema_by_uuid"
        ) as mock_fetch:
            mock_fetch.return_value = GlueSchema(
                schema_uuid="12345678-1234-5678-1234-567812345678",
                data_format="AVRO",
                schema_definition='{"type":"record","name":"X","fields":[]}',
                schema_arn="arn:aws:glue:us-east-1:123:schema/r/X",
                version_number=1,
            )
            resp_bytes = _glue_lookup_dispatcher(req_bytes)

        # Verify fetch_schema_by_uuid was called with the right args.
        mock_fetch.assert_called_once()
        called_conn, called_uuid = mock_fetch.call_args.args
        assert called_conn.registry_name == "r"
        assert called_uuid == "12345678-1234-5678-1234-567812345678"

        # Verify the response JSON.
        resp = json.loads(resp_bytes.decode("utf-8"))
        assert resp["data_format"] == "AVRO"
        assert "record" in resp["schema_definition"]
        assert resp["schema_uuid"] == "12345678-1234-5678-1234-567812345678"

    def test_unknown_registry_name_raises_clear_error(self) -> None:
        # No registration — dispatcher should fail loudly.
        req_bytes = json.dumps({
            "schema_uuid": "u",
            "region": "us-east-1",
            "registry_name": "never-registered",
        }).encode("utf-8")
        with pytest.raises(KeyError, match="never-registered"):
            _glue_lookup_dispatcher(req_bytes)

    def test_malformed_json_request_raises(self) -> None:
        with pytest.raises(json.JSONDecodeError):
            _glue_lookup_dispatcher(b"not json at all")

    def test_missing_required_field_raises_keyerror(self) -> None:
        # Missing registry_name → the dispatcher's dict access fails.
        req_bytes = json.dumps({"schema_uuid": "u"}).encode("utf-8")
        with pytest.raises(KeyError):
            _glue_lookup_dispatcher(req_bytes)

    def test_by_name_dispatcher_returns_schema_response_bytes(self) -> None:
        # Producer-side: the Rust Kafka producer asks "what's the
        # latest UUID + definition for this schema name?"
        from unittest.mock import patch

        from ematix_flow.glue_schema_registry import (
            GlueSchema,
            _glue_lookup_by_name_dispatcher,
        )

        self._register()
        req_bytes = json.dumps({
            "schema_name": "Order",
            "region": "us-east-1",
            "registry_name": "r",
        }).encode("utf-8")
        with patch(
            "ematix_flow.glue_schema_registry.fetch_schema_by_name"
        ) as mock_fetch:
            mock_fetch.return_value = GlueSchema(
                schema_uuid="aabbccdd-0000-1111-2222-3344556677ff",
                data_format="AVRO",
                schema_definition='{"type":"record","name":"Order","fields":[]}',
                schema_arn="arn:aws:glue:us-east-1:123:schema/r/Order",
                version_number=3,
            )
            resp_bytes = _glue_lookup_by_name_dispatcher(req_bytes)
        mock_fetch.assert_called_once()
        called_conn, called_name = mock_fetch.call_args.args
        assert called_conn.registry_name == "r"
        assert called_name == "Order"
        resp = json.loads(resp_bytes.decode("utf-8"))
        assert resp["schema_uuid"] == "aabbccdd-0000-1111-2222-3344556677ff"
        assert resp["data_format"] == "AVRO"
        assert "Order" in resp["schema_definition"]

    def test_dispatcher_routes_to_right_connection(self) -> None:
        # Two registries; the dispatcher must pick the one matching
        # the request's registry_name, not just "any registered conn".
        a = GlueSchemaRegistryConnection(
            name="a", registry_name="orders", region="us-east-1",
        )
        b = GlueSchemaRegistryConnection(
            name="b", registry_name="events", region="eu-west-1",
        )
        _REGISTRY_CONNECTIONS["orders"] = a
        _REGISTRY_CONNECTIONS["events"] = b

        from unittest.mock import patch

        from ematix_flow.glue_schema_registry import GlueSchema

        req_bytes = json.dumps({
            "schema_uuid": "u",
            "region": "eu-west-1",
            "registry_name": "events",
        }).encode("utf-8")

        with patch(
            "ematix_flow.glue_schema_registry.fetch_schema_by_uuid"
        ) as mock_fetch:
            mock_fetch.return_value = GlueSchema(
                schema_uuid="u",
                data_format="AVRO",
                schema_definition="{}",
                schema_arn="arn",
                version_number=1,
            )
            _glue_lookup_dispatcher(req_bytes)
            called_conn = mock_fetch.call_args.args[0]
            assert called_conn is b  # NOT `a`
