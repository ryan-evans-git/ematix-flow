"""Kafka SASL / MSK-IAM through the streaming TOML emitter.

`KafkaConnection` has carried the SASL fields since Phase 36, but
they were only consumed by the direct PyO3 `KafkaBackend`
constructor — the streaming TOML emitter (`run_streaming_pipeline`,
`@ematix.streaming_pipeline`) silently dropped them. These tests
cover the typed-Python emit path; the matching Rust CLI lib tests
cover the schema and runtime wiring.
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


def _kafka_to_sqlite_with(**kafka_kwargs) -> tuple[KafkaConnection, SQLiteConnection]:
    src = KafkaConnection(
        name="src",
        bootstrap_servers="b:9092",
        group_id="g",
        **kafka_kwargs,
    )
    tgt = SQLiteConnection(name="tgt", path=":memory:")
    return src, tgt


class TestKafkaAuthEmission:
    def test_no_auth_omits_all_sasl_fields(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite_with()
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        for field in (
            "sasl_plain_username",
            "sasl_plain_password",
            "sasl_scram_username",
            "sasl_scram_password",
            "sasl_scram_mechanism",
            "msk_iam_region",
        ):
            assert field not in toml, f"{field} should be omitted when unset"

    def test_sasl_plain_emits_both_fields(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite_with(
            sasl_plain_username="alice",
            sasl_plain_password="s3cret",
        )
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'sasl_plain_username = "alice"' in toml
        assert 'sasl_plain_password = "s3cret"' in toml

    def test_sasl_scram_emits_three_fields_with_mechanism(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite_with(
            sasl_scram_username="bob",
            sasl_scram_password="scram-secret",
            sasl_scram_mechanism="sha-512",
        )
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'sasl_scram_username = "bob"' in toml
        assert 'sasl_scram_password = "scram-secret"' in toml
        assert 'sasl_scram_mechanism = "sha-512"' in toml

    def test_msk_iam_emits_region(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite_with(msk_iam_region="us-east-1")
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'msk_iam_region = "us-east-1"' in toml

    def test_env_var_interpolation_through_resolve(self, monkeypatch):
        # The emitter uses resolve() so users can keep credentials
        # in env vars rather than checking them into the module.
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        monkeypatch.setenv("KAFKA_USER", "alice_from_env")
        monkeypatch.setenv("KAFKA_PASS", "s3cret_from_env")
        src, tgt = _kafka_to_sqlite_with(
            sasl_plain_username="${KAFKA_USER}",
            sasl_plain_password="${KAFKA_PASS}",
        )
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'sasl_plain_username = "alice_from_env"' in toml
        assert 'sasl_plain_password = "s3cret_from_env"' in toml
        # The literal `${VAR}` form should never appear in output.
        assert "${KAFKA" not in toml

    def test_combined_plain_and_scram_rejected(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite_with(
            sasl_plain_username="alice",
            sasl_plain_password="p",
            sasl_scram_username="bob",
            sasl_scram_password="p2",
            sasl_scram_mechanism="sha-256",
        )
        with pytest.raises(ValueError, match="at most one"):
            _build_toml_multi(
                name="p",
                sources=[Source(connection=src, query="events")],
                targets=[Target(connection=tgt, table=("main", "events"))],
                idle_pause_ms=500,
                dead_letter_topic=None,
            )

    def test_partial_sasl_plain_rejected(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite_with(
            sasl_plain_username="alice",
            # sasl_plain_password missing
        )
        with pytest.raises(ValueError, match="SASL/PLAIN requires both"):
            _build_toml_multi(
                name="p",
                sources=[Source(connection=src, query="events")],
                targets=[Target(connection=tgt, table=("main", "events"))],
                idle_pause_ms=500,
                dead_letter_topic=None,
            )

    def test_kafka_target_emits_sasl_fields(self):
        # Mirror of the source-side test on the target branch.
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src = KafkaConnection(
            name="src", bootstrap_servers="b:9092", group_id="g"
        )
        tgt = KafkaConnection(
            name="tgt",
            bootstrap_servers="b:9092",
            sasl_plain_username="alice",
            sasl_plain_password="s3cret",
        )
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, topic="out")],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'sasl_plain_username = "alice"' in toml
        assert 'sasl_plain_password = "s3cret"' in toml
