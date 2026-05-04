"""Π.1.4: object-store per-format write options exposed on
``Target`` so users can configure Parquet compression / CSV
delimiter / CSV header without hand-writing TOML.

Round-trip behavior (the actual write happening with the right
codec / delimiter) is exercised by the Rust core lib tests in
``crates/ematix-flow-core/src/objectstore_backend.rs``; this file
covers only the typed-Python emitter shape.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from ematix_flow import (
    KafkaConnection,
    ObjectStoreLocalConnection,
    Source,
    Target,
    register_connection,
)
from ematix_flow.connections import clear_registry


@pytest.fixture(autouse=True)
def _isolated_registry() -> Iterator[None]:
    clear_registry()
    yield
    clear_registry()


def _kafka_to_objectstore_local(
    *,
    fmt: str = "parquet",
) -> tuple[KafkaConnection, ObjectStoreLocalConnection]:
    src = KafkaConnection(name="src", bootstrap_servers="b:9092", group_id="g")
    tgt = ObjectStoreLocalConnection(
        name="lake", path="/data/lake", format=fmt
    )
    return src, tgt


class TestParquetCompression:
    def test_default_omits_compression(self):
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_objectstore_local()
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, prefix="events")],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert "parquet_compression" not in toml

    @pytest.mark.parametrize(
        "codec", ["uncompressed", "snappy", "gzip", "zstd"]
    )
    def test_each_supported_codec_emits(self, codec: str):
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_objectstore_local()
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[
                Target(
                    connection=tgt,
                    prefix="events",
                    parquet_compression=codec,
                )
            ],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert f'parquet_compression = "{codec}"' in toml

    def test_unknown_codec_rejected_at_emit(self):
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_objectstore_local()
        with pytest.raises(ValueError, match="parquet_compression"):
            _build_toml_multi(
                name="p",
                sources=[Source(connection=src, query="events")],
                targets=[
                    Target(
                        connection=tgt,
                        prefix="events",
                        parquet_compression="lzo",
                    )
                ],
                idle_pause_ms=500,
                dead_letter_topic=None,
            )

    def test_compression_on_csv_format_rejected(self):
        # Setting parquet_compression on a CSV target is almost
        # certainly a mistake — fail loud.
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_objectstore_local(fmt="csv")
        with pytest.raises(ValueError, match="parquet"):
            _build_toml_multi(
                name="p",
                sources=[Source(connection=src, query="events")],
                targets=[
                    Target(
                        connection=tgt,
                        prefix="events",
                        parquet_compression="zstd",
                    )
                ],
                idle_pause_ms=500,
                dead_letter_topic=None,
            )


class TestCsvOptions:
    def test_default_omits_delimiter_and_header(self):
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_objectstore_local(fmt="csv")
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[Target(connection=tgt, prefix="events")],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert "csv_delimiter" not in toml
        assert "csv_header" not in toml

    def test_emits_delimiter_and_header_when_set(self):
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_objectstore_local(fmt="csv")
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[
                Target(
                    connection=tgt,
                    prefix="events",
                    csv_delimiter=";",
                    csv_header=False,
                )
            ],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert 'csv_delimiter = ";"' in toml
        assert "csv_header = false" in toml

    def test_multi_byte_delimiter_rejected(self):
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_objectstore_local(fmt="csv")
        with pytest.raises(ValueError, match="single ASCII"):
            _build_toml_multi(
                name="p",
                sources=[Source(connection=src, query="events")],
                targets=[
                    Target(
                        connection=tgt,
                        prefix="events",
                        csv_delimiter="||",
                    )
                ],
                idle_pause_ms=500,
                dead_letter_topic=None,
            )

    def test_csv_options_on_parquet_format_rejected(self):
        from ematix_flow.streaming import _build_toml_multi

        src, tgt = _kafka_to_objectstore_local(fmt="parquet")
        with pytest.raises(ValueError, match="csv_delimiter"):
            _build_toml_multi(
                name="p",
                sources=[Source(connection=src, query="events")],
                targets=[
                    Target(
                        connection=tgt,
                        prefix="events",
                        csv_delimiter=";",
                    )
                ],
                idle_pause_ms=500,
                dead_letter_topic=None,
            )
