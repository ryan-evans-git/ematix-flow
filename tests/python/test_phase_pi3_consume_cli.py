"""Π.3: `flow consume --module M name` and `flow consume-list --module M`.

These cover the CLI wiring only — that argparse routes `consume` and
`consume-list` to the right code path and that the registry-keyed
TOML rendering reaches :func:`ematix_flow.streaming.run_pipeline`.
End-to-end execution against a real Kafka / Postgres lives in the
Rust CLI's testcontainers suite.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from ematix_flow import KafkaConnection, SQLiteConnection, register_connection
from ematix_flow.cli import main
from ematix_flow.connections import clear_registry
from ematix_flow.streaming import (
    _clear_streaming_pipelines,
    register_streaming_pipeline,
)


@pytest.fixture(autouse=True)
def _isolated_registry() -> Iterator[None]:
    clear_registry()
    _clear_streaming_pipelines()
    yield
    clear_registry()
    _clear_streaming_pipelines()


def _register_kafka_to_sqlite(name: str = "events-clean") -> None:
    """Drop a representative Kafka→SQLite pipeline into the streaming
    registry so the CLI has something to render."""
    src = KafkaConnection(name="src", bootstrap_servers="b:9092", group_id="g")
    tgt = SQLiteConnection(name="tgt", path=":memory:")
    register_connection(src)
    register_connection(tgt)
    register_streaming_pipeline(
        name,
        {
            "name": name,
            "source": src,
            "source_query": "events",
            "sources": None,
            "target": tgt,
            "target_table": ("main", "events_clean"),
            "target_topic": None,
            "target_queue": None,
            "target_partition_key_prefix": None,
            "target_prefix": None,
            "target_message_key_column": None,
            "target_partition_by": None,
            "targets": None,
            "idle_pause_ms": 500,
            "dead_letter_topic": None,
            "transform_sql": "SELECT * FROM source",
            "lookups": None,
            "window": None,
            "join": None,
            "state_store": None,
            "metrics_port": None,
        },
    )


def test_consume_renders_toml_and_runs(monkeypatch, capsys) -> None:
    _register_kafka_to_sqlite("events-clean")

    captured: dict[str, Any] = {}

    def fake_run_pipeline(*, config_str: str, metrics_port: int | None = None, **_):
        captured["config_str"] = config_str
        captured["metrics_port"] = metrics_port
        return {"total_rows": 0, "iterations": 0, "shutdown_triggered": False}

    # The `consume` handler imports run_pipeline lazily, so patch on
    # the streaming module where it lives.
    monkeypatch.setattr(
        "ematix_flow.streaming.run_pipeline", fake_run_pipeline
    )

    # `--module` is required; we point at any importable module since
    # the registry was populated directly above.
    rc = main(["consume", "--module", "ematix_flow", "events-clean"])
    assert rc == 0, capsys.readouterr().err

    toml = captured["config_str"]
    assert 'pipeline_name = "events-clean"' in toml
    assert "[source]" in toml
    assert "[target]" in toml
    assert "events_clean" in toml
    assert captured["metrics_port"] is None


def test_consume_passes_metrics_port(monkeypatch) -> None:
    _register_kafka_to_sqlite()
    captured: dict[str, Any] = {}

    def fake_run_pipeline(*, config_str: str, metrics_port: int | None = None, **_):
        captured["metrics_port"] = metrics_port
        return {"total_rows": 0, "iterations": 0, "shutdown_triggered": False}

    monkeypatch.setattr(
        "ematix_flow.streaming.run_pipeline", fake_run_pipeline
    )
    rc = main(
        [
            "consume",
            "--module",
            "ematix_flow",
            "events-clean",
            "--metrics-port",
            "9100",
        ]
    )
    assert rc == 0
    assert captured["metrics_port"] == 9100


def test_consume_unknown_pipeline_fails_clearly(monkeypatch, capsys) -> None:
    _register_kafka_to_sqlite("only-this-one")

    # No-op runner so the test fails on lookup, not on execution.
    monkeypatch.setattr(
        "ematix_flow.streaming.run_pipeline",
        lambda **_: {"total_rows": 0, "iterations": 0, "shutdown_triggered": False},
    )

    rc = main(["consume", "--module", "ematix_flow", "does-not-exist"])
    assert rc != 0
    err = capsys.readouterr().err
    assert "does-not-exist" in err
    # The error should help the user — list what's actually registered.
    assert "only-this-one" in err


def test_consume_list_shows_registered_pipelines(capsys) -> None:
    _register_kafka_to_sqlite("first")
    _register_kafka_to_sqlite("second")

    rc = main(["consume-list", "--module", "ematix_flow"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "first" in out
    assert "second" in out


def test_consume_list_empty_module_prints_nothing_extra(capsys) -> None:
    rc = main(["consume-list", "--module", "ematix_flow"])
    assert rc == 0
    # Tolerant: either empty stdout or a message; we just don't crash.
    capsys.readouterr()
