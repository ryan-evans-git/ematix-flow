"""DLQ Phase 4: pyo3 ops smoke against a live (empty) store.

Builds a real operations pipeline from TOML through the Rust ops
layer (constructing a Kafka backend never contacts the broker —
pinned in the Rust suites) and drives every binding end to end. A
seeded live-store round trip (records in → stats/replay/park/purge)
is covered in Rust by the CLI crate's ``dlq_ops`` integration suite
— the pyo3 layer adds only dict conversion, which empty-store shapes
pin fine.

TDD note: written FIRST, red, before the bindings existed.
"""

from __future__ import annotations

import pytest

from ematix_flow import _core

_TOML = """
pipeline_name = "py-live-ops"
source_query = "events"
dlq_store = "table"
dlq_max_attempts = 2

[source]
kind = "kafka"
bootstrap_servers = "localhost:9092"
group_id = "py-live-g"

[target]
kind = "sqlite"
path = ":memory:"

[target.table]
schema = "main"
name = "events"
"""


class TestLiveOps:
    def test_stats_shape_on_empty_store(self):
        stats = _core.dlq_stats(_TOML, 1_700_000_000_000)
        assert stats["pending"] == 0
        assert stats["parked"] == 0
        assert stats["by_stage"] == {}
        assert stats["arrivals"] == {
            "last_1m": 0,
            "last_5m": 0,
            "last_15m": 0,
            "last_60m": 0,
        }
        assert stats["truncated"] is False

    def test_records_empty(self):
        assert _core.dlq_records(_TOML, None, 0, 50) == []

    def test_record_by_id_missing(self):
        assert _core.dlq_record_by_id(_TOML, "nope") is None

    def test_replay_empty_report(self):
        report = _core.dlq_replay(_TOML, '{"kind":"all"}')
        assert report["taken"] == 0
        assert report["succeeded"] == 0
        assert report["finished_at_ms"] >= report["started_at_ms"] > 0

    def test_park_and_purge_zero(self):
        assert _core.dlq_park(_TOML, '{"kind":"all"}') == 0
        assert _core.dlq_purge(_TOML, '{"kind":"all"}') == 0

    def test_bad_selection_raises_value_error(self):
        with pytest.raises(ValueError, match="selection"):
            _core.dlq_replay(_TOML, '{"kind":"everything"}')

    def test_rewind_bad_offset_bytes_raise(self):
        with pytest.raises(ValueError, match="offset decode"):
            _core.stream_rewind(_TOML, '{"kind":"offset","bytes":[0]}', False)

    def test_bad_toml_raises_value_error(self):
        with pytest.raises(ValueError):
            _core.dlq_stats("not toml at all = [", 0)
