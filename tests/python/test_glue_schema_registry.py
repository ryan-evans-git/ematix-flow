"""Task #556 slice 2 — Glue Schema Registry Python helpers.

The Rust side already ships the framing primitives (``GLUE_HEADER_BYTE``
+ ``parse_glue_frame`` / ``build_glue_frame`` in
``crates/ematix-flow-core/src/glue_schema_registry.rs``). This slice
adds the Python-side schema fetch / register helpers and the
``KafkaConnection.schema_registry`` widening that lets users actually
configure a Glue registry.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ematix_flow.connections import (
    GlueSchemaRegistryConnection,
    KafkaConnection,
    SchemaRegistryConnection,
)
from ematix_flow.glue_schema_registry import (
    GlueSchema,
    fetch_schema_by_name,
    fetch_schema_by_uuid,
    register_schema,
)

# ---------------------------------------------------------------------------
# KafkaConnection.schema_registry — accepts Glue
# ---------------------------------------------------------------------------


class TestKafkaConnectionAcceptsGlueRegistry:
    def test_glue_registry_assignable(self) -> None:
        glue = GlueSchemaRegistryConnection(
            name="g", registry_name="my-registry", region="us-east-1",
        )
        k = KafkaConnection(
            name="orders",
            bootstrap_servers="b:9092",
            payload_format="avro",
            schema_registry=glue,
        )
        # The runtime check is a stored reference, not a type narrowing.
        assert k.schema_registry is glue
        assert k.schema_registry.kind == "glue_schema_registry"

    def test_confluent_registry_still_assignable(self) -> None:
        # Regression guard: widening shouldn't break the existing
        # Confluent path.
        sr = SchemaRegistryConnection(name="sr", url="https://sr:8081")
        k = KafkaConnection(
            name="orders",
            bootstrap_servers="b:9092",
            payload_format="avro",
            schema_registry=sr,
        )
        assert k.schema_registry is sr
        assert k.schema_registry.kind == "schema_registry"

    def test_glue_registry_string_name_still_accepted(self) -> None:
        # The schema_registry field also accepts a registered name
        # string, identifying a typed connection elsewhere in the
        # config. That string path is unchanged by the widening.
        k = KafkaConnection(
            name="orders",
            bootstrap_servers="b:9092",
            payload_format="avro",
            schema_registry="my_glue_sr",
        )
        assert k.schema_registry == "my_glue_sr"

    def test_glue_registry_collides_with_legacy_url(self) -> None:
        glue = GlueSchemaRegistryConnection(
            name="g", registry_name="r", region="us-east-1",
        )
        # The legacy ``schema_registry_url=`` is mutually exclusive
        # with the typed ``schema_registry=`` field — regardless of
        # whether the typed value is Confluent or Glue.
        with pytest.raises(ValueError, match="schema_registry_url"):
            KafkaConnection(
                name="orders",
                bootstrap_servers="b:9092",
                payload_format="avro",
                schema_registry=glue,
                schema_registry_url="https://sr:8081",
            )

    def test_glue_registry_requires_payload_format_set(self) -> None:
        # Pairing Glue SR with no payload_format is almost always a
        # copy-paste error — fail at construction with a clear hint.
        glue = GlueSchemaRegistryConnection(
            name="g", registry_name="r", region="us-east-1",
        )
        with pytest.raises(ValueError, match="payload_format='avro'"):
            KafkaConnection(
                name="orders",
                bootstrap_servers="b:9092",
                schema_registry=glue,
                # No payload_format
            )

    def test_glue_registry_rejects_json_payload(self) -> None:
        # Glue SR doesn't make sense with JSON payloads (no schema
        # wire frame). Catch the misconfig at construction.
        glue = GlueSchemaRegistryConnection(
            name="g", registry_name="r", region="us-east-1",
        )
        with pytest.raises(ValueError, match="does not use a schema-registry"):
            KafkaConnection(
                name="orders",
                bootstrap_servers="b:9092",
                payload_format="json",
                schema_registry=glue,
            )

    def test_glue_registry_rejects_raw_bytes_payload(self) -> None:
        glue = GlueSchemaRegistryConnection(
            name="g", registry_name="r", region="us-east-1",
        )
        with pytest.raises(ValueError, match="does not use a schema-registry"):
            KafkaConnection(
                name="orders",
                bootstrap_servers="b:9092",
                payload_format="raw_bytes",
                schema_registry=glue,
            )

    def test_glue_registry_protobuf_payload_accepted(self) -> None:
        # Protobuf via Glue is on the future-work list — the
        # validation accepts it so the connection is constructable;
        # the Rust dispatch will surface a "not yet implemented" error
        # at the first read, not at config time.
        glue = GlueSchemaRegistryConnection(
            name="g", registry_name="r", region="us-east-1",
        )
        k = KafkaConnection(
            name="orders",
            bootstrap_servers="b:9092",
            payload_format="protobuf",
            schema_registry=glue,
        )
        assert k.schema_registry is glue


# ---------------------------------------------------------------------------
# fetch_schema_by_uuid — turn a wire-frame UUID into a schema definition
# ---------------------------------------------------------------------------


def _glue_conn() -> GlueSchemaRegistryConnection:
    return GlueSchemaRegistryConnection(
        name="glue", registry_name="my-registry", region="us-east-1",
    )


class TestFetchSchemaByUuid:
    def test_returns_glue_schema(self) -> None:
        client = MagicMock()
        client.get_schema_version.return_value = {
            "DataFormat": "AVRO",
            "SchemaDefinition": '{"type":"record","name":"Order"}',
            "SchemaArn": (
                "arn:aws:glue:us-east-1:123456789012:schema/my-registry/Order"
            ),
            "VersionNumber": 3,
        }
        schema = fetch_schema_by_uuid(
            _glue_conn(),
            "12345678-1234-5678-1234-567812345678",
            _client=client,
        )
        assert isinstance(schema, GlueSchema)
        assert schema.data_format == "AVRO"
        assert schema.version_number == 3
        assert "Order" in schema.schema_definition
        # The UUID we asked for is round-tripped verbatim — the
        # caller wires it back to its wire-frame source.
        assert schema.schema_uuid == "12345678-1234-5678-1234-567812345678"

    def test_passes_uuid_through_to_glue_api(self) -> None:
        client = MagicMock()
        client.get_schema_version.return_value = {
            "DataFormat": "JSON",
            "SchemaDefinition": "{}",
            "SchemaArn": "arn",
            "VersionNumber": 1,
        }
        fetch_schema_by_uuid(_glue_conn(), "abc-uuid", _client=client)
        client.get_schema_version.assert_called_once_with(
            SchemaVersionId="abc-uuid",
        )

    def test_rejects_unknown_data_format(self) -> None:
        client = MagicMock()
        client.get_schema_version.return_value = {
            "DataFormat": "CSV",  # not a Glue-supported format
            "SchemaDefinition": "",
            "SchemaArn": "arn",
            "VersionNumber": 1,
        }
        with pytest.raises(ValueError, match="unsupported DataFormat"):
            fetch_schema_by_uuid(_glue_conn(), "uuid", _client=client)

    def test_passes_through_protobuf(self) -> None:
        client = MagicMock()
        client.get_schema_version.return_value = {
            "DataFormat": "PROTOBUF",
            "SchemaDefinition": 'syntax = "proto3"; message X {}',
            "SchemaArn": "arn",
            "VersionNumber": 1,
        }
        schema = fetch_schema_by_uuid(_glue_conn(), "u", _client=client)
        assert schema.data_format == "PROTOBUF"


class TestFetchSchemaByName:
    """Producer-side helper: resolve the latest version of a named
    schema. Used by the Rust Kafka producer on first send to learn
    which UUID to embed in each Glue frame."""

    def test_returns_latest_version_by_default(self) -> None:
        client = MagicMock()
        client.get_schema_version.return_value = {
            "SchemaVersionId": "aa-bb-cc",
            "DataFormat": "AVRO",
            "SchemaDefinition": '{"type":"record","name":"Order","fields":[]}',
            "SchemaArn": "arn:aws:glue:us-east-1:123:schema/r/Order",
            "VersionNumber": 5,
        }
        schema = fetch_schema_by_name(_glue_conn(), "Order", _client=client)
        # Glue's LatestVersion flag is what we want by default.
        client.get_schema_version.assert_called_once()
        kwargs = client.get_schema_version.call_args.kwargs
        assert kwargs["SchemaId"] == {
            "RegistryName": "my-registry", "SchemaName": "Order",
        }
        assert kwargs["SchemaVersionNumber"] == {"LatestVersion": True}
        assert schema.version_number == 5
        assert schema.data_format == "AVRO"

    def test_specific_version_number(self) -> None:
        client = MagicMock()
        client.get_schema_version.return_value = {
            "SchemaVersionId": "x",
            "DataFormat": "AVRO",
            "SchemaDefinition": "{}",
            "SchemaArn": "arn",
            "VersionNumber": 3,
        }
        fetch_schema_by_name(_glue_conn(), "Order", version=3, _client=client)
        kwargs = client.get_schema_version.call_args.kwargs
        assert kwargs["SchemaVersionNumber"] == {"VersionNumber": 3}

    def test_rejects_unknown_data_format(self) -> None:
        client = MagicMock()
        client.get_schema_version.return_value = {
            "DataFormat": "CSV",
            "SchemaDefinition": "",
            "SchemaArn": "arn",
            "VersionNumber": 1,
        }
        with pytest.raises(ValueError, match="unsupported DataFormat"):
            fetch_schema_by_name(_glue_conn(), "Order", _client=client)


# ---------------------------------------------------------------------------
# register_schema — create the parent schema or a new version
# ---------------------------------------------------------------------------


class TestRegisterSchema:
    def test_existing_schema_register_version(self) -> None:
        client = MagicMock()
        client.register_schema_version.return_value = {
            "SchemaVersionId": "new-uuid",
            "SchemaArn": "arn:aws:glue:us-east-1:123:schema/r/Orders",
            "VersionNumber": 4,
        }
        result = register_schema(
            _glue_conn(),
            schema_name="Orders",
            data_format="AVRO",
            schema_definition='{"type":"record","name":"Orders"}',
            _client=client,
        )
        assert result.schema_uuid == "new-uuid"
        assert result.version_number == 4
        # RegisterSchemaVersion path: no fallback to CreateSchema.
        assert client.register_schema_version.called
        assert not client.create_schema.called

    def test_fallback_to_create_schema_on_missing_parent(self) -> None:
        client = MagicMock()
        client.register_schema_version.side_effect = RuntimeError(
            "EntityNotFound: registry/Orders does not exist"
        )
        client.create_schema.return_value = {
            "SchemaVersionId": "first-uuid",
            "SchemaArn": "arn:aws:glue:us-east-1:123:schema/r/Orders",
            "VersionNumber": 1,
        }
        result = register_schema(
            _glue_conn(),
            schema_name="Orders",
            data_format="AVRO",
            schema_definition='{"type":"record","name":"Orders"}',
            compatibility="FORWARD",
            description="first version",
            _client=client,
        )
        assert result.schema_uuid == "first-uuid"
        assert result.version_number == 1
        # Both calls made; the fallback path is what we test here.
        assert client.register_schema_version.called
        assert client.create_schema.called
        # Compatibility + description propagate.
        kwargs = client.create_schema.call_args.kwargs
        assert kwargs["Compatibility"] == "FORWARD"
        assert kwargs["Description"] == "first version"
        assert kwargs["DataFormat"] == "AVRO"

    def test_non_missing_error_reraised(self) -> None:
        # Throttling / IAM-denied errors must NOT silently fall through
        # to CreateSchema — that would mask a real failure.
        client = MagicMock()
        client.register_schema_version.side_effect = RuntimeError(
            "AccessDeniedException: not authorized to register schema"
        )
        with pytest.raises(RuntimeError, match="AccessDenied"):
            register_schema(
                _glue_conn(),
                schema_name="Orders",
                data_format="AVRO",
                schema_definition="{}",
                _client=client,
            )
        assert not client.create_schema.called

    def test_rejects_unknown_data_format(self) -> None:
        with pytest.raises(ValueError, match="data_format"):
            register_schema(
                _glue_conn(),
                schema_name="Orders",
                data_format="CSV",
                schema_definition="",
                _client=MagicMock(),
            )
