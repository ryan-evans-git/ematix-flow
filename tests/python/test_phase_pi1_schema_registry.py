"""Π.1: Schema-Registry-as-connection.

Two coupled changes covered here:

1. ``SchemaRegistryConnection`` is a typed connection that carries
   the SR URL (and optional basic-auth fields) the same way every
   other typed connection does — credentials redacted in repr,
   ``${VAR}`` interpolation supported, registry-resolvable by name.

2. ``KafkaConnection.schema_registry`` accepts either a
   ``SchemaRegistryConnection`` instance or a registered SR name
   (string). When set, the streaming TOML emitter resolves it
   through the registry and emits the ``schema_registry_url`` /
   ``payload_format`` fields in the Kafka source / target block —
   a gap that today silently drops Schema Registry config from
   ``run_streaming_pipeline`` pipelines.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from ematix_flow import KafkaConnection, SQLiteConnection, register_connection
from ematix_flow.connections import clear_registry


@pytest.fixture(autouse=True)
def _isolated_registry() -> Iterator[None]:
    clear_registry()
    yield
    clear_registry()


class TestSchemaRegistryConnectionType:
    def test_constructs_with_url_only(self):
        from ematix_flow import SchemaRegistryConnection

        sr = SchemaRegistryConnection(name="sr_prod", url="http://sr:8081")
        assert sr.name == "sr_prod"
        assert sr.url == "http://sr:8081"
        assert sr.kind == "schema_registry"
        assert sr.basic_auth_user is None
        assert sr.basic_auth_password is None

    def test_repr_redacts_basic_auth_password(self):
        from ematix_flow import SchemaRegistryConnection

        sr = SchemaRegistryConnection(
            name="sr_prod",
            url="http://sr:8081",
            basic_auth_user="alice",
            basic_auth_password="hunter2",
        )
        text = repr(sr)
        assert "alice" in text
        assert "hunter2" not in text
        assert "<redacted>" in text

    def test_url_is_required(self):
        from ematix_flow import SchemaRegistryConnection

        with pytest.raises(ValueError, match="url is required"):
            SchemaRegistryConnection(name="sr", url="")

    def test_register_and_lookup_by_name(self):
        from ematix_flow import SchemaRegistryConnection, get_connection

        sr = SchemaRegistryConnection(name="sr_prod", url="http://sr:8081")
        register_connection(sr)
        assert get_connection("sr_prod") is sr

    def test_env_var_interpolation_via_resolve(self, monkeypatch):
        from ematix_flow import SchemaRegistryConnection
        from ematix_flow.connections import resolve

        monkeypatch.setenv("SR_URL", "http://sr.prod:8081")
        sr = SchemaRegistryConnection(name="sr_prod", url="${SR_URL}")
        # The interpolation happens at TOML-emit time via `resolve`.
        assert resolve(sr.url) == "http://sr.prod:8081"


class TestKafkaConnectionSchemaRegistryField:
    def test_accepts_sr_connection_instance(self):
        from ematix_flow import SchemaRegistryConnection

        sr = SchemaRegistryConnection(name="sr_prod", url="http://sr:8081")
        kafka = KafkaConnection(
            name="kafka_prod",
            bootstrap_servers="b:9092",
            payload_format="avro",
            schema_registry=sr,
        )
        assert kafka.schema_registry is sr

    def test_accepts_sr_connection_name_string(self):
        kafka = KafkaConnection(
            name="kafka_prod",
            bootstrap_servers="b:9092",
            payload_format="avro",
            schema_registry="sr_prod",  # name-only; resolved at emit time
        )
        assert kafka.schema_registry == "sr_prod"

    def test_rejects_both_schema_registry_and_inline_url(self):
        with pytest.raises(ValueError, match="schema_registry"):
            KafkaConnection(
                name="kafka",
                bootstrap_servers="b:9092",
                payload_format="avro",
                schema_registry_url="http://sr:8081",
                schema_registry="sr_prod",
            )


class TestStreamingTomlEmitsSchemaRegistry:
    """The streaming TOML emitter currently silently drops both
    ``payload_format`` and ``schema_registry_url`` — a real bug. Π.1
    plumbs them through both the inline-URL path and the typed-SR
    path."""

    def _setup(self) -> tuple[KafkaConnection, SQLiteConnection]:
        src = KafkaConnection(
            name="src", bootstrap_servers="b:9092", group_id="g"
        )
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        return src, tgt

    def test_kafka_source_emits_inline_schema_registry_url(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src = KafkaConnection(
            name="src",
            bootstrap_servers="b:9092",
            group_id="g",
            payload_format="avro",
            schema_registry_url="http://sr:8081",
        )
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'payload_format = "avro"' in toml
        assert 'schema_registry_url = "http://sr:8081"' in toml

    def test_kafka_source_resolves_typed_sr_connection_by_name(self):
        from ematix_flow import (
            SchemaRegistryConnection,
            Source,
            Target,
        )
        from ematix_flow.streaming import _build_toml_multi

        sr = SchemaRegistryConnection(name="sr_prod", url="http://sr:8081")
        register_connection(sr)
        src = KafkaConnection(
            name="src",
            bootstrap_servers="b:9092",
            group_id="g",
            payload_format="avro",
            schema_registry="sr_prod",  # registry name reference
        )
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'payload_format = "avro"' in toml
        assert 'schema_registry_url = "http://sr:8081"' in toml

    def test_kafka_source_typed_sr_connection_instance(self):
        from ematix_flow import (
            SchemaRegistryConnection,
            Source,
            Target,
        )
        from ematix_flow.streaming import _build_toml_multi

        sr = SchemaRegistryConnection(name="sr_prod", url="http://sr:8081")
        # Note: NOT calling register_connection — instance is passed
        # directly through the connection field.
        src = KafkaConnection(
            name="src",
            bootstrap_servers="b:9092",
            group_id="g",
            payload_format="avro",
            schema_registry=sr,
        )
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'schema_registry_url = "http://sr:8081"' in toml

    def test_kafka_target_emits_payload_format_and_sr_url(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src = SQLiteConnection(name="src", path=":memory:")
        tgt = KafkaConnection(
            name="tgt",
            bootstrap_servers="b:9092",
            payload_format="avro",
            schema_registry_url="http://sr:8081",
        )
        # SQLite as source needs a query that's a SQL-ish string;
        # use any non-empty query — emitter's source-shape handling is
        # exercised elsewhere.
        with pytest.raises(ValueError):
            # SQLite isn't a streaming source → emit_fields raises.
            # We don't actually need to round-trip the full TOML for
            # the target side; we'll cover target emission with a
            # Kafka→Kafka pipeline instead.
            _build_toml_multi(
                name="p",
                sources=[Source(connection=src, query="events")],
                targets=[Target(connection=tgt, topic="out")],
                idle_pause_ms=500,
                dead_letter_topic=None,
            )

    def test_kafka_to_kafka_target_emits_sr(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src = KafkaConnection(name="src", bootstrap_servers="b:9092", group_id="g")
        tgt = KafkaConnection(
            name="tgt",
            bootstrap_servers="b:9092",
            payload_format="avro",
            schema_registry_url="http://sr:8081",
        )
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, topic="out")],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        # Both the [source] (without SR — src has no SR config) and
        # the [target] (with SR) should be present.
        assert 'topic = "out"' in toml
        # Per-target SR config should land inside the [target] block.
        assert 'payload_format = "avro"' in toml
        assert 'schema_registry_url = "http://sr:8081"' in toml

    def test_unregistered_sr_name_fails_loud(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src = KafkaConnection(
            name="src",
            bootstrap_servers="b:9092",
            group_id="g",
            payload_format="avro",
            schema_registry="never-registered",
        )
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        with pytest.raises(KeyError):
            _build_toml_multi(
                name="p",
                sources=[Source(connection=src, query="events")],
                targets=[Target(connection=tgt, table=("main", "events"))],
                idle_pause_ms=500,
                dead_letter_topic=None,
            )

    def test_sr_basic_auth_emits_through_to_toml(self):
        # Π.1 follow-up: SR basic auth is now plumbed through the
        # Rust runtime via `SrSettings::set_basic_authorization`.
        # The emitter renders both fields when set.
        from ematix_flow import (
            SchemaRegistryConnection,
            Source,
            Target,
        )
        from ematix_flow.streaming import _build_toml_multi

        sr = SchemaRegistryConnection(
            name="sr",
            url="http://sr:8081",
            basic_auth_user="alice",
            basic_auth_password="hunter2",
        )
        register_connection(sr)
        src = KafkaConnection(
            name="src",
            bootstrap_servers="b:9092",
            group_id="g",
            payload_format="avro",
            schema_registry="sr",
        )
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'schema_registry_url = "http://sr:8081"' in toml
        assert 'schema_registry_basic_auth_user = "alice"' in toml
        assert 'schema_registry_basic_auth_password = "hunter2"' in toml

    def test_sr_basic_auth_half_set_rejected(self):
        # Setting only the user (or only the password) is almost
        # certainly a typo — fail loud rather than passing only one
        # half through to the runtime.
        from ematix_flow import (
            SchemaRegistryConnection,
            Source,
            Target,
        )
        from ematix_flow.streaming import _build_toml_multi

        sr = SchemaRegistryConnection(
            name="sr",
            url="http://sr:8081",
            basic_auth_user="alice",
            # basic_auth_password missing
        )
        register_connection(sr)
        src = KafkaConnection(
            name="src",
            bootstrap_servers="b:9092",
            group_id="g",
            payload_format="avro",
            schema_registry="sr",
        )
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        with pytest.raises(ValueError, match="must both be set"):
            _build_toml_multi(
                name="p",
                sources=[Source(connection=src, query="events")],
                targets=[Target(connection=tgt, table=("main", "events"))],
                idle_pause_ms=500,
                dead_letter_topic=None,
            )
