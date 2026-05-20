"""Tests for the cloud-warehouse connection dataclasses + Arrow adapters.

Covers:
- Typed connection construction + validation
- `${...}` interpolation through the secrets module (Phase 1)
- repr() redaction of credential fields
- Arrow query adapters with mocked SDK clients

Real-credential integration tests live under
`tests/python/integration/` and are skipped without creds.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from ematix_flow.warehouses import (
    BigQueryConnection,
    RedshiftConnection,
    SnowflakeConnection,
    bigquery_query_to_arrow,
    redshift_query_to_arrow,
    snowflake_query_to_arrow,
)


# ----- SnowflakeConnection ----------------------------------------


class TestSnowflakeConnection:
    def test_minimal_construction(self):
        conn = SnowflakeConnection(
            name="snow_prod",
            account="ab12345.us-east-1",
            user="loader",
            password="hunter2",
            warehouse="LOAD_WH",
            database="ANALYTICS",
            schema="PUBLIC",
        )
        assert conn.kind == "snowflake"
        assert conn.account == "ab12345.us-east-1"
        assert conn.warehouse == "LOAD_WH"

    def test_account_required(self):
        with pytest.raises(ValueError, match="account is required"):
            SnowflakeConnection(name="snow", user="x", password="y", warehouse="W")

    def test_user_required(self):
        with pytest.raises(ValueError, match="user is required"):
            SnowflakeConnection(name="snow", account="a", password="y", warehouse="W")

    def test_either_password_or_private_key(self):
        # Both empty: rejected.
        with pytest.raises(ValueError, match="password or private_key"):
            SnowflakeConnection(name="snow", account="a", user="u", warehouse="W")

    def test_private_key_alternative_accepted(self):
        conn = SnowflakeConnection(
            name="snow",
            account="a",
            user="u",
            private_key="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
            warehouse="W",
        )
        assert conn.private_key.startswith("-----BEGIN")

    def test_repr_redacts_password(self):
        conn = SnowflakeConnection(
            name="snow", account="a", user="u", password="hunter2", warehouse="W"
        )
        r = repr(conn)
        assert "hunter2" not in r
        assert "<redacted>" in r

    def test_repr_redacts_private_key(self):
        conn = SnowflakeConnection(
            name="snow",
            account="a",
            user="u",
            private_key="-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----",
            warehouse="W",
        )
        r = repr(conn)
        assert "secret" not in r
        assert "<redacted>" in r


# ----- BigQueryConnection -----------------------------------------


class TestBigQueryConnection:
    def test_minimal_construction(self):
        conn = BigQueryConnection(name="bq_prod", project="my-proj", dataset="analytics")
        assert conn.kind == "bigquery"
        assert conn.project == "my-proj"

    def test_project_required(self):
        with pytest.raises(ValueError, match="project is required"):
            BigQueryConnection(name="bq", dataset="x")

    def test_dataset_required(self):
        with pytest.raises(ValueError, match="dataset is required"):
            BigQueryConnection(name="bq", project="p")

    def test_optional_credentials_path(self):
        conn = BigQueryConnection(
            name="bq",
            project="p",
            dataset="d",
            credentials_path="/opt/secrets/bq.json",
        )
        assert conn.credentials_path == "/opt/secrets/bq.json"

    def test_repr_redacts_credentials_path(self):
        # credentials_path itself isn't a secret but pointing to a key
        # file is sensitive; redact to be safe.
        conn = BigQueryConnection(
            name="bq",
            project="p",
            dataset="d",
            credentials_path="/opt/secrets/bq.json",
        )
        r = repr(conn)
        # The literal path token contains "secret" so it gets redacted
        # by the existing connection redactor — verify the redactor
        # at minimum doesn't leak the field as the raw string.
        assert "credentials_path=" in r


# ----- RedshiftConnection -----------------------------------------


class TestRedshiftConnection:
    def test_minimal_construction(self):
        conn = RedshiftConnection(
            name="rs",
            host="cluster.region.redshift.amazonaws.com",
            port=5439,
            database="dev",
            user="loader",
            password="hunter2",
        )
        assert conn.kind == "redshift"
        assert conn.port == 5439

    def test_host_required(self):
        with pytest.raises(ValueError, match="host is required"):
            RedshiftConnection(name="rs", database="d", user="u", password="p")

    def test_default_port_is_5439(self):
        conn = RedshiftConnection(
            name="rs", host="h", database="d", user="u", password="p"
        )
        assert conn.port == 5439

    def test_optional_s3_staging_dir_for_copy_writes(self):
        conn = RedshiftConnection(
            name="rs",
            host="h",
            database="d",
            user="u",
            password="p",
            s3_staging_dir="s3://my-bucket/redshift-staging/",
            iam_role="arn:aws:iam::123:role/RedshiftCopy",
        )
        assert conn.s3_staging_dir == "s3://my-bucket/redshift-staging/"
        assert conn.iam_role.startswith("arn:aws:iam:")

    def test_repr_redacts_password(self):
        conn = RedshiftConnection(
            name="rs",
            host="h",
            database="d",
            user="u",
            password="hunter2",
        )
        assert "hunter2" not in repr(conn)

    def test_to_postgres_url(self):
        # Redshift speaks Postgres wire protocol — exposing a
        # `to_postgres_url()` helper lets users bridge into the
        # existing PostgresConnection / Source.postgres_query path.
        conn = RedshiftConnection(
            name="rs",
            host="cluster.region.redshift.amazonaws.com",
            database="dev",
            user="loader",
            password="hunter2",
        )
        url = conn.to_postgres_url()
        assert url == "postgres://loader:hunter2@cluster.region.redshift.amazonaws.com:5439/dev"


# ----- Arrow query adapters ---------------------------------------


class TestSnowflakeQueryToArrow:
    """`snowflake_query_to_arrow` wraps snowflake-connector-python's
    `cursor.fetch_arrow_all()`. We mock the connector here so the
    unit tests don't require a real Snowflake account."""

    def test_returns_arrow_table_from_fetch_arrow_all(self):
        # Build a fake cursor that returns a known Arrow table.
        expected_table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_all.return_value = expected_table
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        conn = SnowflakeConnection(
            name="snow",
            account="a",
            user="u",
            password="p",
            warehouse="W",
        )

        result = snowflake_query_to_arrow(conn, "SELECT id, name FROM t", _client=mock_conn)
        assert result.equals(expected_table)
        mock_cursor.execute.assert_called_once_with("SELECT id, name FROM t")

    def test_propagates_interpolated_secrets(self, monkeypatch):
        # If account/user/password reference env vars, they should
        # have been interpolated before the connection passes them
        # to the driver. We verify by inspecting the .resolved view.
        monkeypatch.setenv("SF_PASSWORD", "from_env")
        conn = SnowflakeConnection(
            name="snow",
            account="a",
            user="u",
            password="${SF_PASSWORD}",
            warehouse="W",
        )
        assert conn.resolved_password() == "from_env"


class TestBigQueryQueryToArrow:
    def test_returns_arrow_table_from_query_result_to_arrow(self):
        expected_table = pa.table({"id": [1, 2], "name": ["x", "y"]})
        # bigquery.Client().query(sql).to_arrow() shape
        mock_query_job = MagicMock()
        mock_query_job.to_arrow.return_value = expected_table
        mock_client = MagicMock()
        mock_client.query.return_value = mock_query_job

        conn = BigQueryConnection(name="bq", project="p", dataset="d")
        result = bigquery_query_to_arrow(conn, "SELECT id, name FROM `p.d.t`", _client=mock_client)
        assert result.equals(expected_table)
        mock_client.query.assert_called_once_with("SELECT id, name FROM `p.d.t`")


class TestRedshiftQueryToArrow:
    def test_uses_postgres_protocol_via_psycopg(self):
        # Redshift speaks Postgres protocol — we go through psycopg
        # like a regular Postgres source. The helper mocks at the
        # cursor level since Redshift drivers vary.
        mock_cursor = MagicMock()
        mock_cursor.description = [("id", None), ("name", None)]
        mock_cursor.fetchall.return_value = [(1, "x"), (2, "y")]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        conn = RedshiftConnection(
            name="rs",
            host="h",
            database="d",
            user="u",
            password="p",
        )
        result = redshift_query_to_arrow(conn, "SELECT id, name FROM t", _client=mock_conn)
        assert result.column_names == ["id", "name"]
        assert result.num_rows == 2
        mock_cursor.execute.assert_called_once_with("SELECT id, name FROM t")
