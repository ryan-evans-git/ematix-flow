"""Phase Π.4b — Python decorator csv_read_options + json_read_options.

Tests that the new dict-shaped options on `streaming.Target` (and
implicit Source plumbing) emit the right TOML nested tables that the
Rust spec deserializer picks up. Pure-Python tests; Rust round-trip
is in test_phase_pi_4a (Rust unit tests).
"""

from __future__ import annotations

import pytest

from ematix_flow.connections import ObjectStoreLocalConnection
from ematix_flow.streaming import (
    Target,
    _csv_read_options_lines,
    _json_read_options_lines,
    _object_store_format_lines,
)


# ---- _csv_read_options_lines ----------------------------------------


def test_csv_read_options_emits_nested_table():
    lines = _csv_read_options_lines({
        "has_header": False,
        "delimiter": "\t",
        "comment": "#",
    })
    assert lines[0] == ""
    assert lines[1] == "[target.read_options.csv]"
    body = "\n".join(lines[2:])
    assert "has_header = false" in body
    assert "delimiter = 9" in body          # b'\t'
    assert "comment = 35" in body           # b'#'


def test_csv_read_options_all_known_keys():
    """Every documented key should serialize without error."""
    lines = _csv_read_options_lines({
        "has_header": True,
        "delimiter": ",",
        "quote": '"',
        "escape": "\\",
        "terminator": "\n",
        "comment": "#",
        "null_regex": "^(NA|NULL)$",
        "truncated_rows_ok": True,
        "schema_infer_max_records": 4096,
    })
    body = "\n".join(lines)
    assert "delimiter = 44" in body
    assert "quote = 34" in body
    assert 'null_regex = "^(NA|NULL)$"' in body
    assert "schema_infer_max_records = 4096" in body
    assert "truncated_rows_ok = true" in body


def test_csv_read_options_unknown_key_rejected():
    with pytest.raises(ValueError) as ei:
        _csv_read_options_lines({"unknown_key": True})
    assert "unknown_key" in str(ei.value)


def test_csv_read_options_non_ascii_delimiter_rejected():
    with pytest.raises(ValueError) as ei:
        _csv_read_options_lines({"delimiter": "→"})  # multi-byte UTF-8
    assert "delimiter" in str(ei.value)


def test_csv_read_options_multi_char_quote_rejected():
    with pytest.raises(ValueError) as ei:
        _csv_read_options_lines({"quote": "||"})
    assert "quote" in str(ei.value)


def test_csv_read_options_bad_type_rejected():
    with pytest.raises(ValueError) as ei:
        _csv_read_options_lines({"has_header": "yes"})  # str, not bool
    assert "has_header" in str(ei.value)


# ---- _json_read_options_lines ---------------------------------------


def test_json_read_options_emits_nested_table():
    lines = _json_read_options_lines({
        "schema_infer_max_records": 2000,
        "batch_size": 8192,
    })
    body = "\n".join(lines)
    assert "[target.read_options.json]" in body
    assert "schema_infer_max_records = 2000" in body
    assert "batch_size = 8192" in body


def test_json_read_options_negative_value_rejected():
    with pytest.raises(ValueError):
        _json_read_options_lines({"batch_size": -1})


# ---- _object_store_format_lines (the high-level emitter) ----------


def test_object_store_format_lines_csv_with_read_options():
    """End-to-end: pass csv_read_options to _object_store_format_lines
    and verify the nested table appears."""
    lines = _object_store_format_lines(
        "csv",
        parquet_compression=None,
        csv_delimiter=None,
        csv_header=None,
        csv_read_options={"delimiter": "|", "has_header": False},
    )
    body = "\n".join(lines)
    assert "[target.read_options.csv]" in body
    assert "delimiter = 124" in body  # b'|'


def test_object_store_format_lines_rejects_csv_read_options_on_parquet():
    with pytest.raises(ValueError) as ei:
        _object_store_format_lines(
            "parquet",
            parquet_compression="snappy",
            csv_delimiter=None,
            csv_header=None,
            csv_read_options={"delimiter": "|"},
        )
    assert "csv" in str(ei.value).lower()


def test_object_store_format_lines_json_read_options_with_jsonlines():
    """JSON read options work with format='json_lines'."""
    lines = _object_store_format_lines(
        "json_lines",
        parquet_compression=None,
        csv_delimiter=None,
        csv_header=None,
        json_read_options={"batch_size": 1024},
    )
    body = "\n".join(lines)
    assert "[target.read_options.json]" in body
    assert "batch_size = 1024" in body


def test_object_store_format_lines_rejects_json_read_options_on_parquet():
    with pytest.raises(ValueError) as ei:
        _object_store_format_lines(
            "parquet",
            parquet_compression=None,
            csv_delimiter=None,
            csv_header=None,
            json_read_options={"batch_size": 1024},
        )
    assert "json" in str(ei.value).lower()


# ---- Target dataclass accepts the new fields -----------------------


def test_target_accepts_csv_read_options():
    """Smoke test: the dataclass takes the new kwargs without complaint."""
    t = Target(
        connection="dummy",
        prefix="some/prefix",
        csv_read_options={"delimiter": "\t"},
        json_read_options={"batch_size": 256},
    )
    assert t.csv_read_options == {"delimiter": "\t"}
    assert t.json_read_options == {"batch_size": 256}


def test_target_defaults_to_none_for_read_options():
    t = Target(connection="dummy", prefix="some/prefix")
    assert t.csv_read_options is None
    assert t.json_read_options is None
