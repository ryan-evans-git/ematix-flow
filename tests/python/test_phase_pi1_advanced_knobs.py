"""Π.1 follow-up: expose `transform_on_error` and `WatermarkConfig`
tuning via the typed-Python streaming surface so users don't have
to hand-write TOML for these advanced knobs.

The matching Rust CLI tests (parses+wires) live in the CLI crate's
lib tests; here we cover only the Python emission layer.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from ematix_flow import KafkaConnection, SQLiteConnection, register_connection
from ematix_flow.connections import clear_registry


@pytest.fixture(autouse=True)
def _isolated_registry() -> Iterator[None]:
    from ematix_flow.streaming import _clear_streaming_pipelines

    clear_registry()
    _clear_streaming_pipelines()
    yield
    clear_registry()
    _clear_streaming_pipelines()


def _kafka_to_sqlite() -> tuple[KafkaConnection, SQLiteConnection]:
    src = KafkaConnection(name="src", bootstrap_servers="b:9092", group_id="g")
    tgt = SQLiteConnection(name="tgt", path=":memory:")
    return src, tgt


class TestTransformOnError:
    def test_default_omits_on_error_field(self):
        # Default behavior matches Rust CLI default ("fail") — emitter
        # leaves it out so the TOML stays minimal.
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite()
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
            transform_sql="SELECT * FROM source",
        )
        assert "on_error" not in toml

    def test_emits_on_error_drop(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite()
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
            transform_sql="SELECT * FROM source",
            transform_on_error="drop",
        )
        assert 'on_error = "drop"' in toml

    def test_emits_on_error_dlq(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite()
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic="dlq-topic",
            transform_sql="SELECT * FROM source",
            transform_on_error="dlq",
        )
        assert 'on_error = "dlq"' in toml

    def test_rejects_unknown_value(self):
        # Better to fail at the typed-Python boundary than wait for
        # the Rust validator's error to bubble back.
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite()
        with pytest.raises(ValueError, match="on_error"):
            _build_toml_multi(
                name="p",
                sources=[Source(connection=src, query="events")],
                targets=[Target(connection=tgt, table=("main", "events"))],
                idle_pause_ms=500,
                dead_letter_topic=None,
                transform_sql="SELECT * FROM source",
                transform_on_error="ignore",  # not a real policy
            )

    def test_run_streaming_pipeline_accepts_kwarg(self, monkeypatch):
        # End-to-end the kwarg should reach the TOML the runner sees.
        from ematix_flow.streaming import run_streaming_pipeline

        captured: dict[str, str] = {}

        def fake_runner(toml: str, _port: int | None):
            captured["toml"] = toml
            return {"total_rows": 0, "iterations": 0, "shutdown_triggered": False}

        monkeypatch.setattr(
            "ematix_flow.streaming.run_pipeline_from_toml_str", fake_runner
        )
        src, tgt = _kafka_to_sqlite()
        run_streaming_pipeline(
            name="p",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events"),
            transform_sql="SELECT * FROM source",
            transform_on_error="drop",
        )
        assert 'on_error = "drop"' in captured["toml"]

    def test_decorator_accepts_kwarg_and_registers(self):
        from ematix_flow import ematix
        from ematix_flow.streaming import (
            get_streaming_pipeline,
            render_streaming_pipeline_toml,
        )

        src, tgt = _kafka_to_sqlite()
        register_connection(src)
        register_connection(tgt)

        @ematix.streaming_pipeline(
            name="dec-on-error",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events"),
            transform_sql="SELECT * FROM source",
            transform_on_error="drop",
        )
        def p():
            pass

        captured = get_streaming_pipeline("dec-on-error")
        assert captured["transform_on_error"] == "drop"
        assert 'on_error = "drop"' in render_streaming_pipeline_toml(
            "dec-on-error"
        )


class TestWatermarkTuning:
    def test_default_omits_watermark_block(self):
        from ematix_flow import Source, Target
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite()
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert "[watermark]" not in toml

    def test_emits_watermark_lateness_and_idleness(self):
        from ematix_flow import Source, Target, Watermark
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite()
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
            watermark=Watermark(lateness_ms=5_000, source_idleness_ms=120_000),
        )
        assert "[watermark]" in toml
        assert "lateness_ms = 5000" in toml
        assert "source_idleness_ms = 120000" in toml

    def test_emits_only_set_fields(self):
        # Watermark with only lateness customized should still emit a
        # block (so the runner sees the override) but only the
        # non-default field. Source-idleness defaults to 60s in core.
        from ematix_flow import Source, Target, Watermark
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_sqlite()
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
            watermark=Watermark(lateness_ms=2_500),
        )
        assert "[watermark]" in toml
        assert "lateness_ms = 2500" in toml
        # source_idleness_ms not set → field not emitted; CLI takes
        # the default.
        assert "source_idleness_ms" not in toml

    def test_watermark_negative_values_rejected(self):
        from ematix_flow import Watermark

        with pytest.raises(ValueError):
            Watermark(lateness_ms=-1)
        with pytest.raises(ValueError):
            Watermark(source_idleness_ms=-1)

    def test_decorator_accepts_watermark(self):
        from ematix_flow import ematix
        from ematix_flow.streaming import (
            Watermark,
            get_streaming_pipeline,
            render_streaming_pipeline_toml,
        )

        src, tgt = _kafka_to_sqlite()
        register_connection(src)
        register_connection(tgt)

        @ematix.streaming_pipeline(
            name="dec-wm",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events"),
            watermark=Watermark(lateness_ms=10_000),
        )
        def p():
            pass

        captured = get_streaming_pipeline("dec-wm")
        assert captured["watermark"].lateness_ms == 10_000
        toml = render_streaming_pipeline_toml("dec-wm")
        assert "lateness_ms = 10000" in toml
