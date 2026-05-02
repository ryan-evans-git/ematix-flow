"""Π.1: typed connection registry + decorator surface.

Tests the Python-side connection objects, registry, decorator,
and the TOML-emission shim that bridges to the existing Rust
runner. No Docker / no Rust extension load — these tests run on
the default lane.
"""

from __future__ import annotations

import os

import pytest

from ematix_flow import (
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
    ematix,
    get_connection,
    register_connection,
    registered_connections,
)
from ematix_flow.connections import clear_registry, redact, resolve
from ematix_flow.streaming import _build_toml


@pytest.fixture(autouse=True)
def _isolated_registry():
    """Each test gets a fresh registry."""
    clear_registry()
    yield
    clear_registry()


# ---------- Connection construction --------------------------------


class TestConnectionConstruction:
    def test_kafka_requires_bootstrap_servers(self):
        with pytest.raises(ValueError, match="bootstrap_servers is required"):
            KafkaConnection(name="bad", bootstrap_servers="")

    def test_postgres_requires_url(self):
        with pytest.raises(ValueError, match="url is required"):
            PostgresConnection(name="bad", url="")

    def test_kinesis_requires_stream_name(self):
        with pytest.raises(ValueError, match="stream_name is required"):
            KinesisConnection(name="bad", stream_name="")

    def test_delta_s3_requires_full_credentials(self):
        with pytest.raises(ValueError, match="region is required"):
            DeltaS3Connection(
                name="bad",
                endpoint="http://s3.local",
                bucket="b",
                region="",
                access_key_id="a",
                secret_access_key="s",
            )

    def test_object_store_local_requires_format(self):
        with pytest.raises(ValueError, match="format is required"):
            ObjectStoreLocalConnection(name="bad", path="/data", format="")


# ---------- Registry -----------------------------------------------


class TestRegistry:
    def test_register_and_lookup(self):
        c = PostgresConnection(name="dw", url="postgres://localhost/db")
        assert register_connection(c) is c
        assert get_connection("dw") is c

    def test_lookup_unknown_connection_lists_known(self):
        register_connection(PostgresConnection(name="warehouse", url="postgres://x/db"))
        register_connection(KafkaConnection(name="kafka_prod", bootstrap_servers="b:9092"))
        with pytest.raises(KeyError) as excinfo:
            get_connection("not-here")
        msg = str(excinfo.value)
        assert "not-here" in msg
        assert "kafka_prod" in msg
        assert "warehouse" in msg

    def test_lookup_with_empty_registry_says_none(self):
        with pytest.raises(KeyError, match="<none>"):
            get_connection("anything")

    def test_re_register_overwrites(self):
        register_connection(PostgresConnection(name="dw", url="postgres://a/db"))
        register_connection(PostgresConnection(name="dw", url="postgres://b/db"))
        assert get_connection("dw").url == "postgres://b/db"

    def test_register_rejects_non_connection(self):
        with pytest.raises(TypeError, match="expected a Connection"):
            register_connection("not a connection")  # type: ignore[arg-type]

    def test_registered_connections_returns_copy(self):
        register_connection(PostgresConnection(name="dw", url="postgres://x/db"))
        snap = registered_connections()
        clear_registry()
        # The snapshot is unaffected by clear_registry — it's a copy.
        assert "dw" in snap


# ---------- Decorator ----------------------------------------------


class TestDecorator:
    def test_class_decorator_registers_kafka(self):
        @ematix.connection
        class kafka_prod:  # noqa: N801 — name-as-id is intentional
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            group_id = "ematix-flow"

        # The class binding is now the typed instance.
        assert isinstance(kafka_prod, KafkaConnection)
        assert kafka_prod.name == "kafka_prod"
        assert kafka_prod.bootstrap_servers == "localhost:9092"
        assert get_connection("kafka_prod") is kafka_prod

    def test_class_decorator_postgres(self):
        @ematix.connection
        class warehouse:  # noqa: N801
            kind = "postgres"
            url = "postgres://app@localhost/main"

        assert isinstance(warehouse, PostgresConnection)
        assert warehouse.kind == "postgres"

    def test_class_decorator_overrides_name(self):
        @ematix.connection
        class warehouse:  # noqa: N801
            name = "real_dw"
            kind = "postgres"
            url = "postgres://app@localhost/main"

        assert warehouse.name == "real_dw"
        assert get_connection("real_dw") is warehouse

    def test_class_decorator_requires_kind(self):
        with pytest.raises(TypeError, match="must declare a `kind`"):

            @ematix.connection
            class missing_kind:  # noqa: N801
                url = "postgres://x/y"

    def test_class_decorator_unknown_kind(self):
        with pytest.raises(TypeError, match="unknown kind"):

            @ematix.connection
            class bogus:  # noqa: N801
                kind = "wormhole"

    def test_decorator_rejects_non_class(self):
        with pytest.raises(TypeError, match="expects a class"):

            @ematix.connection  # type: ignore[arg-type]
            def some_func():
                pass

    def test_decorator_propagates_validation_errors(self):
        # Required bootstrap_servers missing → ValueError surfaces.
        with pytest.raises(ValueError, match="bootstrap_servers is required"):

            @ematix.connection
            class incomplete:  # noqa: N801
                kind = "kafka"


# ---------- ${VAR} interpolation -----------------------------------


class TestInterpolation:
    def test_resolve_substitutes_env_var(self, monkeypatch):
        monkeypatch.setenv("MY_HOST", "broker.example.com")
        assert resolve("${MY_HOST}:9092") == "broker.example.com:9092"

    def test_resolve_passes_through_plain_string(self):
        assert resolve("plain-string") == "plain-string"

    def test_resolve_handles_none(self):
        assert resolve(None) is None

    def test_resolve_missing_var_raises_clearly(self, monkeypatch):
        monkeypatch.delenv("DEFINITELY_NOT_SET", raising=False)
        with pytest.raises(KeyError, match="DEFINITELY_NOT_SET"):
            resolve("${DEFINITELY_NOT_SET}")

    def test_interpolation_happens_at_build_time_not_definition_time(self, monkeypatch):
        # Define connection now, with var unset.
        monkeypatch.delenv("LATE_BOUND_HOST", raising=False)
        c = KafkaConnection(name="k", bootstrap_servers="${LATE_BOUND_HOST}:9092")
        # Now set the var and build TOML — interpolation kicks in.
        monkeypatch.setenv("LATE_BOUND_HOST", "broker.lateral.com")
        register_connection(c)
        toml = _build_toml(
            name="p",
            source=c,
            source_query="topic",
            target=SQLiteConnection(name="t", path=":memory:"),
            target_table=("main", "events"),
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert "broker.lateral.com:9092" in toml


# ---------- Repr redaction -----------------------------------------


class TestReprRedaction:
    def test_kafka_sasl_password_redacted_in_repr(self):
        c = KafkaConnection(
            name="k",
            bootstrap_servers="b:9092",
            sasl_plain_username="alice",
            sasl_plain_password="DO_NOT_LEAK",
        )
        s = repr(c)
        assert "DO_NOT_LEAK" not in s
        assert "alice" in s
        assert "<redacted>" in s

    def test_postgres_url_password_redacted_in_repr(self):
        c = PostgresConnection(
            name="dw",
            url="postgres://app:DO_NOT_LEAK@host/db",
        )
        s = repr(c)
        assert "DO_NOT_LEAK" not in s
        assert "app" in s
        assert "host" in s

    def test_kinesis_credentials_redacted_in_repr(self):
        c = KinesisConnection(
            name="k",
            stream_name="events",
            access_key_id="AKIA_DO_NOT_LEAK",
            secret_access_key="SECRET_DO_NOT_LEAK",
        )
        s = repr(c)
        assert "AKIA_DO_NOT_LEAK" not in s
        assert "SECRET_DO_NOT_LEAK" not in s
        assert "events" in s

    def test_amqp_url_password_redacted_in_repr(self):
        c = RabbitMQConnection(
            name="rb",
            amqp_url="amqp://guest:DO_NOT_LEAK@broker.local/vh",
        )
        s = repr(c)
        assert "DO_NOT_LEAK" not in s
        assert "guest" in s

    def test_redact_function_handles_known_secret_fields(self):
        assert redact("password", "secret") == "<redacted>"
        assert redact("secret_access_key", "AK") == "<redacted>"
        # Compound names containing a secret token also redact.
        assert redact("sasl_plain_password", "p") == "<redacted>"
        # Non-secret field names pass through unchanged.
        assert redact("bootstrap_servers", "b:9092") == "b:9092"
        assert redact("group_id", "g") == "g"
        assert redact("anything", None) is None


# ---------- TOML emission ------------------------------------------


class TestTomlEmission:
    def test_kafka_to_postgres_round_trip(self):
        src = KafkaConnection(name="src", bootstrap_servers="b:9092", group_id="g")
        tgt = PostgresConnection(name="tgt", url="postgres://app@host/db")
        toml = _build_toml(
            name="kp",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("public", "events"),
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=250,
            dead_letter_topic=None,
        )
        # Sanity: looks like the existing TOML format.
        assert 'pipeline_name = "kp"' in toml
        assert 'source_query = "events"' in toml
        assert "idle_pause_ms = 250" in toml
        assert 'kind = "kafka"' in toml
        assert 'bootstrap_servers = "b:9092"' in toml
        assert 'group_id = "g"' in toml
        assert 'kind = "postgres"' in toml
        assert 'url = "postgres://app@host/db"' in toml
        assert "[target.table]" in toml
        assert 'schema = "public"' in toml
        assert 'name = "events"' in toml

    def test_kafka_to_kafka_with_message_key(self):
        src = KafkaConnection(name="s", bootstrap_servers="b:9092", group_id="g")
        tgt = KafkaConnection(name="t", bootstrap_servers="b2:9092")
        toml = _build_toml(
            name="kk",
            source=src,
            source_query="in",
            target=tgt,
            target_table=None,
            target_topic="out",
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column="user_id",
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'topic = "out"' in toml
        assert 'message_key_column = "user_id"' in toml
        # No [target.table] for streaming targets.
        assert "[target.table]" not in toml

    def test_delta_local_with_partition_by(self):
        src = KafkaConnection(name="s", bootstrap_servers="b:9092")
        tgt = DeltaLocalConnection(name="t", path="/data/lake")
        toml = _build_toml(
            name="kd",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("default", "events"),
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=["year", "month"],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'partition_by = ["year", "month"]' in toml
        assert 'path = "/data/lake"' in toml

    def test_object_store_s3_target(self):
        src = KafkaConnection(name="s", bootstrap_servers="b:9092")
        tgt = ObjectStoreS3Connection(
            name="t",
            endpoint="http://s3.local",
            bucket="lake",
            region="us-east-1",
            access_key_id="ak",
            secret_access_key="sk",
            format="parquet",
        )
        toml = _build_toml(
            name="kos",
            source=src,
            source_query="events",
            target=tgt,
            target_table=None,
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix="raw",
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'kind = "object_store_s3"' in toml
        assert 'format = "parquet"' in toml
        assert 'prefix = "raw"' in toml

    def test_db_target_requires_target_table_kwarg(self):
        src = KafkaConnection(name="s", bootstrap_servers="b:9092")
        tgt = PostgresConnection(name="t", url="postgres://x/y")
        with pytest.raises(ValueError, match="requires target_table"):
            _build_toml(
                name="p",
                source=src,
                source_query="q",
                target=tgt,
                target_table=None,  # missing → error
                target_topic=None,
                target_queue=None,
                target_partition_key_prefix=None,
                target_prefix=None,
                target_message_key_column=None,
                target_partition_by=None,
                idle_pause_ms=500,
                dead_letter_topic=None,
            )

    def test_kafka_target_requires_topic(self):
        src = KafkaConnection(name="s", bootstrap_servers="b:9092")
        tgt = KafkaConnection(name="t", bootstrap_servers="b2:9092")
        with pytest.raises(ValueError, match="requires target_topic"):
            _build_toml(
                name="p",
                source=src,
                source_query="q",
                target=tgt,
                target_table=None,
                target_topic=None,  # missing → error
                target_queue=None,
                target_partition_key_prefix=None,
                target_prefix=None,
                target_message_key_column=None,
                target_partition_by=None,
                idle_pause_ms=500,
                dead_letter_topic=None,
            )

    def test_db_used_as_source_rejects(self):
        # Postgres is target-only.
        src = PostgresConnection(name="s", url="postgres://x/y")
        tgt = SQLiteConnection(name="t", path=":memory:")
        with pytest.raises(ValueError, match="cannot be used as a source"):
            _build_toml(
                name="p",
                source=src,
                source_query="q",
                target=tgt,
                target_table=("main", "t"),
                target_topic=None,
                target_queue=None,
                target_partition_key_prefix=None,
                target_prefix=None,
                target_message_key_column=None,
                target_partition_by=None,
                idle_pause_ms=500,
                dead_letter_topic=None,
            )


# ---------- Symmetry: same connection used as source AND target ---


class TestSymmetry:
    def test_kafka_connection_used_as_both_source_and_target(self):
        # The whole point of this design — connections aren't
        # role-labeled. Same instance can drive both slots.
        kc = KafkaConnection(name="k", bootstrap_servers="b:9092", group_id="g")
        toml = _build_toml(
            name="kk",
            source=kc,
            source_query="in",
            target=kc,
            target_table=None,
            target_topic="out",
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        # bootstrap_servers appears in both [source] and [target].
        assert toml.count('bootstrap_servers = "b:9092"') == 2


# ---------- Π.4a-4: multi-target API -------------------------------


class TestMultiTarget:
    """run_streaming_pipeline accepts targets=[Target(...), ...]
    and emits the [[targets]] TOML shape for multi-target fan-out."""

    def test_target_dataclass_exposed_from_package(self):
        from ematix_flow import Target

        warehouse = PostgresConnection(name="wh", url="postgres://app@host/db")
        t = Target(connection=warehouse, table=("public", "events"))
        assert t.connection is warehouse
        assert t.table == ("public", "events")
        # Defaults — every other placement field is None / empty.
        assert t.topic is None
        assert t.partition_by is None

    def test_two_targets_emits_array_of_tables(self):
        from ematix_flow import Target
        from ematix_flow.streaming import _build_toml_multi

        src = KafkaConnection(name="src", bootstrap_servers="b:9092", group_id="g")
        wh = PostgresConnection(name="wh", url="postgres://app@host/db")
        lake = DeltaLocalConnection(name="lake", path="/data/lake")
        toml = _build_toml_multi(
            name="fanout",
            source=src,
            source_query="events",
            targets=[
                Target(connection=wh, table=("public", "events")),
                Target(
                    connection=lake,
                    table=("default", "events_archive"),
                    partition_by=["year", "month"],
                ),
            ],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        # Multi-target emits [[targets]] (an array of tables in TOML).
        assert toml.count("[[targets]]") == 2
        # The single-target shape should not appear.
        assert "[target]\n" not in toml
        # First target is Postgres, second is delta_local.
        assert 'kind = "postgres"' in toml
        assert 'kind = "delta_local"' in toml
        assert 'partition_by = ["year", "month"]' in toml
        # Per-target table blocks bind to the latest [[targets]].
        assert toml.count("[targets.table]") == 2

    def test_single_target_via_targets_list_keeps_back_compat_shape(self):
        # A 1-element targets list should still emit the legacy
        # `[target]` block — the Rust runner accepts both forms,
        # but keeping the smaller TOML for the single-target case
        # is what callers expect.
        from ematix_flow import Target
        from ematix_flow.streaming import _build_toml_multi

        src = KafkaConnection(name="src", bootstrap_servers="b:9092")
        wh = PostgresConnection(name="wh", url="postgres://app@host/db")
        toml = _build_toml_multi(
            name="single",
            source=src,
            source_query="events",
            targets=[Target(connection=wh, table=("public", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert "[[targets]]" not in toml
        assert "[target]" in toml
        assert "[target.table]" in toml

    def test_run_streaming_pipeline_rejects_target_and_targets_together(self):
        from ematix_flow import Target
        from ematix_flow.streaming import run_streaming_pipeline

        src = KafkaConnection(name="src", bootstrap_servers="b:9092")
        wh = PostgresConnection(name="wh", url="postgres://app@host/db")
        with pytest.raises(ValueError, match="target.*targets"):
            run_streaming_pipeline(
                name="bad",
                source=src,
                source_query="events",
                target=wh,
                target_table=("public", "events"),
                targets=[Target(connection=wh, table=("public", "events"))],
            )

    def test_run_streaming_pipeline_rejects_neither_target_nor_targets(self):
        from ematix_flow.streaming import run_streaming_pipeline

        src = KafkaConnection(name="src", bootstrap_servers="b:9092")
        with pytest.raises(ValueError, match="target"):
            run_streaming_pipeline(
                name="bad",
                source=src,
                source_query="events",
            )

    def test_target_resolves_string_connection_name_via_registry(self):
        from ematix_flow import Target
        from ematix_flow.streaming import _build_toml_multi

        # Register a connection so a string name resolves.
        warehouse = PostgresConnection(
            name="warehouse_prod", url="postgres://app@host/db"
        )
        register_connection(warehouse)

        src = KafkaConnection(name="src", bootstrap_servers="b:9092")
        toml = _build_toml_multi(
            name="byname",
            source=src,
            source_query="events",
            targets=[
                # String references resolve against the registry.
                Target(connection="warehouse_prod", table=("public", "events")),
            ],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'url = "postgres://app@host/db"' in toml
