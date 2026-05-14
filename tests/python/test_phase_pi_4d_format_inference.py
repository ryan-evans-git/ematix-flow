"""Phase Π.4d — `format_from_path` extension-based inference.

Public utility for picking an object-store format string from a path
or URL that names the file. The connection construction stays
explicit; this is a building block users can apply themselves.
"""

from __future__ import annotations

import pytest

from ematix_flow.connections import format_from_path


@pytest.mark.parametrize(
    "path,expected",
    [
        # Parquet
        ("data.parquet", "parquet"),
        ("/tmp/events.parquet", "parquet"),
        ("s3://bucket/prefix/file.parquet", "parquet"),
        ("data.pq", "parquet"),
        # CSV + TSV
        ("data.csv", "csv"),
        ("/etc/users.tsv", "csv"),
        ("s3://bucket/year=2026/month=05/data.csv", "csv"),
        # JSONL family
        ("events.ndjson", "json_lines"),
        ("logs.jsonl", "json_lines"),
        ("dump.json", "json_lines"),
        # ORC
        ("hive_warehouse/table.orc", "orc"),
        # Case-insensitive
        ("DATA.CSV", "csv"),
        ("File.Parquet", "parquet"),
    ],
)
def test_recognized_extensions(path, expected):
    assert format_from_path(path) == expected


@pytest.mark.parametrize(
    "path,expected",
    [
        ("events.csv.gz", "csv"),
        ("data.parquet.gz", "parquet"),
        ("logs.ndjson.zst", "json_lines"),
        ("events.csv.bz2", "csv"),
        ("events.csv.lz4", "csv"),
        ("events.csv.snappy", "csv"),
        ("events.tsv.zstd", "csv"),
    ],
)
def test_strips_compression_suffix(path, expected):
    assert format_from_path(path) == expected


@pytest.mark.parametrize(
    "path",
    [
        "no-extension",
        "weird.xml",
        "data.txt",
        "logs.log",
        "",
        "/path/to/dir/",
    ],
)
def test_unknown_returns_none(path):
    assert format_from_path(path) is None


def test_url_with_query_string_not_supported_returns_none():
    """Query strings shift the trailing token away from the extension.
    Users who want this case can strip the query themselves before
    calling format_from_path. Documenting the limit, not a feature."""
    # ?download=true after .csv → matcher sees `.csv?download=true`
    assert format_from_path("https://host/data.csv?download=true") is None
