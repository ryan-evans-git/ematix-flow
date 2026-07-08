"""DLQ Phase 4: `dlq_store` / `dlq_max_attempts` on the typed
streaming surface.

The matching Rust CLI tests (parse + validate + wire into the core
config) live in the CLI crate's lib tests; here we cover only the
Python emission layer — mirroring how `transform_on_error` is
covered in test_phase_pi1_advanced_knobs.py.

TDD note: written FIRST, red, before the kwargs existed.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from ematix_flow import KafkaConnection, SQLiteConnection
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


def _emit(**overrides) -> str:
    from ematix_flow.streaming import _run_streaming_pipeline_emit_toml

    src, tgt = _kafka_to_sqlite()
    kwargs = dict(
        name="p",
        source=src,
        source_query="events",
        target=tgt,
        target_table=("main", "events"),
    )
    kwargs.update(overrides)
    return _run_streaming_pipeline_emit_toml(**kwargs)


class TestDlqStoreKwarg:
    def test_default_omits_dlq_fields(self):
        toml = _emit()
        assert "dlq_store" not in toml
        assert "dlq_max_attempts" not in toml

    def test_dlq_store_emitted_top_level(self):
        toml = _emit(dlq_store="table")
        assert 'dlq_store = "table"' in toml

    def test_dlq_max_attempts_emitted_top_level(self):
        toml = _emit(dlq_max_attempts=5)
        assert "dlq_max_attempts = 5" in toml

    def test_invalid_dlq_store_raises(self):
        with pytest.raises(ValueError, match="dlq_store"):
            _emit(dlq_store="yolo")

    def test_zero_dlq_max_attempts_raises(self):
        with pytest.raises(ValueError, match="dlq_max_attempts"):
            _emit(dlq_max_attempts=0)

    def test_all_valid_modes_accepted(self):
        for mode in ("auto", "topic", "table"):
            toml = _emit(dlq_store=mode, dead_letter_topic="dlq-events")
            assert f'dlq_store = "{mode}"' in toml

    def test_run_streaming_pipeline_accepts_the_kwargs(self):
        # Kwarg-shape parity: run_streaming_pipeline must accept the
        # same names (validation fires before any runner invocation,
        # so an invalid value proves the plumbing without running).
        from ematix_flow.streaming import run_streaming_pipeline

        src, tgt = _kafka_to_sqlite()
        with pytest.raises(ValueError, match="dlq_store"):
            run_streaming_pipeline(
                name="p",
                source=src,
                source_query="events",
                target=tgt,
                target_table=("main", "events"),
                dlq_store="nope",
            )
