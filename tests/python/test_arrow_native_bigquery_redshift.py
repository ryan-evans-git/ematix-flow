"""Task #557 slices 2+3 — Arrow-native BigQuery and Redshift writes.

Slice 1 (Snowflake) is covered in ``test_arrow_native_snowflake.py``.
This file pins the contract for the remaining two backends:

* BigQuery used to go through ``table.to_pandas()`` →
  ``load_table_from_dataframe``. It now writes a temp parquet via
  ``pyarrow.parquet`` and calls ``load_table_from_file(... PARQUET)``.
* Redshift was already Arrow-native (S3 + ``COPY FROM ... PARQUET``)
  but the ``[redshift]`` extra still pinned pandas. The pin is dropped
  here; this test guards against accidental re-add.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from ematix_flow.warehouses import (
    BigQueryConnection,
    RedshiftConnection,
    bigquery_write_arrow,
    redshift_write_arrow,
)

# ---------------------------------------------------------------------------
# Extras-pin contract (pyproject.toml)
# ---------------------------------------------------------------------------


def _load_pyproject() -> dict:
    import tomllib
    from pathlib import Path

    here = Path(__file__).resolve()
    repo_root = here
    for _ in range(6):
        if (repo_root / "pyproject.toml").exists():
            break
        repo_root = repo_root.parent
    else:
        pytest.skip("repo root not found")
    with open(repo_root / "pyproject.toml", "rb") as f:
        return tomllib.load(f)


class TestExtrasDoNotPinPandas:
    """Each cloud-warehouse extra is meant to be installable without
    pandas. The catch-all ``[warehouses]`` extra likewise."""

    @pytest.mark.parametrize("extra", ["bigquery", "redshift", "warehouses"])
    def test_extra_does_not_pin_pandas(self, extra: str) -> None:
        cfg = _load_pyproject()
        deps = cfg["project"]["optional-dependencies"][extra]
        assert not any("pandas" in dep for dep in deps), (
            f"[{extra}] extra still pins pandas: {deps}"
        )


# ---------------------------------------------------------------------------
# Source-grep: confirm no pandas API calls leak back into the write paths
# ---------------------------------------------------------------------------


class TestNoPandasApiInWritePaths:
    @pytest.mark.parametrize(
        "fn",
        [bigquery_write_arrow, redshift_write_arrow],
    )
    def test_write_arrow_body_has_no_pandas_calls(self, fn) -> None:
        import inspect

        src = inspect.getsource(fn)
        doc = fn.__doc__ or ""
        body = src.replace(doc, "")
        for forbidden in (".to_pandas(", ".write_pandas(", "DataFrame(", "import pandas"):
            assert forbidden not in body, (
                f"{fn.__name__} body contains {forbidden!r} — regression vs "
                "Arrow-native design (task #557)"
            )


# ---------------------------------------------------------------------------
# BigQuery write path — load_table_from_file with parquet
# ---------------------------------------------------------------------------


class TestBigQueryArrowNative:
    def _conn(self) -> BigQueryConnection:
        return BigQueryConnection(name="bq", project="p", dataset="d")

    def _client(self) -> MagicMock:
        """Mock BigQuery client recording load_table_from_file calls."""
        client = MagicMock()
        job = MagicMock()
        job.result.return_value = None
        client.load_table_from_file.return_value = job
        return client

    def test_load_table_from_file_called_with_parquet_handle(self) -> None:
        """The Arrow-native path opens the temp parquet file as a
        binary file handle and hands it to load_table_from_file.
        That replaces the old load_table_from_dataframe call."""
        client = self._client()
        table = pa.table({"id": [1, 2, 3]})
        nrows = bigquery_write_arrow(
            self._conn(),
            table,
            table_name="t",
            create_if_not_exists=True,
            _client=client,
        )
        assert nrows == 3
        assert client.load_table_from_file.called
        args, kwargs = client.load_table_from_file.call_args
        # Positional args: (file_handle, table_ref) — handle must be
        # a readable binary stream (BufferedReader / file-like).
        file_handle = args[0]
        assert hasattr(file_handle, "read"), file_handle
        table_ref = args[1]
        assert table_ref == "p.d.t"

    def test_does_not_call_load_table_from_dataframe(self) -> None:
        """Regression guard: the old pandas path is gone."""
        client = self._client()
        bigquery_write_arrow(
            self._conn(),
            pa.table({"id": [1]}),
            table_name="t",
            _client=client,
        )
        assert not client.load_table_from_dataframe.called

    def test_temp_parquet_cleaned_up(self) -> None:
        """The temp parquet path must be unlinked on success — leaving
        Snowflake-sized parquets in /tmp would silently fill disks."""
        import os
        import tempfile

        before = set(os.listdir(tempfile.gettempdir()))
        client = self._client()
        bigquery_write_arrow(
            self._conn(),
            pa.table({"id": list(range(50))}),
            table_name="t",
            _client=client,
        )
        after = set(os.listdir(tempfile.gettempdir()))
        new = after - before
        leaked_parquets = [p for p in new if p.endswith(".parquet")]
        assert not leaked_parquets, f"leaked temp parquet(s): {leaked_parquets}"

    def test_write_error_wrapped_in_warehouse_sync_error(self) -> None:
        from ematix_flow.warehouses import WarehouseSyncError

        client = self._client()
        client.load_table_from_file.side_effect = RuntimeError("boom")
        with pytest.raises(WarehouseSyncError, match="bigquery_write_arrow"):
            bigquery_write_arrow(
                self._conn(),
                pa.table({"id": [1]}),
                table_name="t",
                _client=client,
            )


# ---------------------------------------------------------------------------
# Redshift write path — already Arrow-native; pin the COPY shape
# ---------------------------------------------------------------------------


class TestRedshiftArrowNative:
    def _conn(self) -> RedshiftConnection:
        return RedshiftConnection(
            name="rs",
            host="h",
            database="d",
            user="u",
            password="p",
            s3_staging_dir="s3://bucket/staging/",
            iam_role="arn:aws:iam::123:role/r",
        )

    def test_copy_from_parquet_emitted(self) -> None:
        cursor = MagicMock()
        executed: list[str] = []
        cursor.execute.side_effect = lambda sql, *a, **kw: executed.append(sql)
        client = MagicMock()
        client.cursor.return_value.__enter__.return_value = cursor

        s3 = MagicMock()

        rows = redshift_write_arrow(
            self._conn(),
            pa.table({"id": [1, 2, 3]}),
            table_name="t",
            _client=client,
            _s3_client=s3,
        )
        assert rows == 3
        # PUT to S3 must happen before the COPY.
        assert s3.put_object.called
        # COPY ... FROM 's3://...' IAM_ROLE 'arn:...' FORMAT PARQUET
        assert len(executed) == 1
        copy_sql = executed[0]
        assert copy_sql.startswith("COPY t FROM 's3://bucket/staging/")
        assert "IAM_ROLE 'arn:aws:iam::123:role/r'" in copy_sql
        assert "FORMAT PARQUET" in copy_sql

    def test_staging_file_cleaned_up_after_copy(self) -> None:
        cursor = MagicMock()
        client = MagicMock()
        client.cursor.return_value.__enter__.return_value = cursor

        s3 = MagicMock()
        redshift_write_arrow(
            self._conn(),
            pa.table({"id": [1]}),
            table_name="t",
            _client=client,
            _s3_client=s3,
        )
        # Best-effort delete after COPY succeeds.
        assert s3.delete_object.called

    def test_staging_file_cleaned_up_after_copy_failure(self) -> None:
        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("COPY failed")
        client = MagicMock()
        client.cursor.return_value.__enter__.return_value = cursor

        s3 = MagicMock()
        with pytest.raises(RuntimeError):
            redshift_write_arrow(
                self._conn(),
                pa.table({"id": [1]}),
                table_name="t",
                _client=client,
                _s3_client=s3,
            )
        # Even on COPY failure, staging file must be deleted.
        assert s3.delete_object.called

    def test_rejects_malformed_s3_staging_dir(self) -> None:
        from ematix_flow.warehouses import WarehouseSyncError

        conn = RedshiftConnection(
            name="rs",
            host="h",
            database="d",
            user="u",
            password="p",
            s3_staging_dir="not-a-url",  # missing s3://
            iam_role="arn:r",
        )
        with pytest.raises(WarehouseSyncError, match="s3://bucket"):
            redshift_write_arrow(
                conn,
                pa.table({"id": [1]}),
                table_name="t",
                _client=MagicMock(),
                _s3_client=MagicMock(),
            )
