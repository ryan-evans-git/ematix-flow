"""Phase Π.4e — write-side CSV options parity.

`Target` exposes csv_quote / csv_escape / csv_null_value to complete
the round-trip story started in Π.4b (read options). Validation runs
at decorator-evaluation time so users see the error before any TOML
is handed to Rust. Rust-side honoring is covered in
`objectstore_backend::tests::csv_write_honors_quote_escape_null`.
"""

from __future__ import annotations

import pytest

from ematix_flow.streaming import Target, _object_store_format_lines

# ---- _object_store_format_lines: emission --------------------------


def test_csv_write_options_emit_lines():
    lines = _object_store_format_lines(
        "csv",
        parquet_compression=None,
        csv_delimiter=None,
        csv_header=None,
        csv_quote="'",
        csv_escape="\\",
        csv_null_value="\\N",
    )
    body = "\n".join(lines)
    assert "csv_quote = \"'\"" in body
    assert 'csv_escape = "\\\\"' in body
    assert 'csv_null_value = "\\\\N"' in body


def test_csv_null_value_can_be_empty_string():
    """Empty-string null sentinel is meaningful (matches Arrow default
    explicitly). Should emit, not be treated as "unset"."""
    lines = _object_store_format_lines(
        "csv",
        parquet_compression=None,
        csv_delimiter=None,
        csv_header=None,
        csv_null_value="",
    )
    assert 'csv_null_value = ""' in "\n".join(lines)


def test_csv_null_value_can_be_multichar():
    lines = _object_store_format_lines(
        "csv",
        parquet_compression=None,
        csv_delimiter=None,
        csv_header=None,
        csv_null_value="NULL",
    )
    assert 'csv_null_value = "NULL"' in "\n".join(lines)


# ---- _object_store_format_lines: validation ------------------------


def test_csv_quote_must_be_single_ascii():
    with pytest.raises(ValueError, match="csv_quote"):
        _object_store_format_lines(
            "csv",
            parquet_compression=None,
            csv_delimiter=None,
            csv_header=None,
            csv_quote="''",
        )


def test_csv_escape_must_be_single_ascii():
    with pytest.raises(ValueError, match="csv_escape"):
        _object_store_format_lines(
            "csv",
            parquet_compression=None,
            csv_delimiter=None,
            csv_header=None,
            csv_escape="--",
        )


def test_csv_quote_rejected_on_parquet():
    with pytest.raises(ValueError, match="csv_quote"):
        _object_store_format_lines(
            "parquet",
            parquet_compression=None,
            csv_delimiter=None,
            csv_header=None,
            csv_quote="'",
        )


def test_csv_escape_rejected_on_parquet():
    with pytest.raises(ValueError, match="csv_escape"):
        _object_store_format_lines(
            "parquet",
            parquet_compression=None,
            csv_delimiter=None,
            csv_header=None,
            csv_escape="\\",
        )


def test_csv_null_value_rejected_on_json_lines():
    with pytest.raises(ValueError, match="csv_null_value"):
        _object_store_format_lines(
            "json_lines",
            parquet_compression=None,
            csv_delimiter=None,
            csv_header=None,
            csv_null_value="\\N",
        )


# ---- Target dataclass surface --------------------------------------


def test_target_accepts_csv_write_options():
    t = Target(
        connection="dummy",
        prefix="some/prefix",
        csv_quote="'",
        csv_escape="\\",
        csv_null_value="\\N",
    )
    assert t.csv_quote == "'"
    assert t.csv_escape == "\\"
    assert t.csv_null_value == "\\N"


def test_target_defaults_to_none_for_csv_write_options():
    t = Target(connection="dummy", prefix="some/prefix")
    assert t.csv_quote is None
    assert t.csv_escape is None
    assert t.csv_null_value is None
