"""Phase Ω.D3 — URL-based RunLog backend selection.

`from_url(url)` is the single entry point that constructs the right
RunLog backend based on a URL scheme. CLI plumbs `--run-log-url` to
it; everything composes.

Two layers of testing:

  1. `_resolve_backend(url)` — pure parsing, no I/O. Tests every scheme
     and every URL form. Doesn't need any real DB or cloud creds.
  2. `from_url(url)` — actually instantiates the backend. Tested
     end-to-end for local backends (sqlite, memory, duckdb); for SQL
     and cloud backends we just verify the right class is selected
     (full construction needs real creds, covered in per-backend
     test files).
"""

from __future__ import annotations

import argparse

import pytest

from ematix_flow.run_log import _resolve_backend, from_url

# ---- _resolve_backend dispatch ---------------------------------------


def test_sqlite_url_resolves_to_sqlite_runlog():
    from ematix_flow.run_log import SqliteRunLog

    cls, kwargs = _resolve_backend("sqlite:///tmp/foo.db")
    assert cls is SqliteRunLog
    assert kwargs["path"] == "/tmp/foo.db"


def test_bare_path_resolves_to_sqlite():
    """A path with no scheme should default to SQLite — preserves the
    pre-URL CLI shape."""
    from ematix_flow.run_log import SqliteRunLog

    cls, kwargs = _resolve_backend("/tmp/run.db")
    assert cls is SqliteRunLog
    assert kwargs["path"] == "/tmp/run.db"

    cls2, kwargs2 = _resolve_backend("~/run.db")
    assert cls2 is SqliteRunLog
    # Path is expanded so ~ resolves to the user's home.
    assert "/" in kwargs2["path"]
    assert not kwargs2["path"].startswith("~")


def test_memory_scheme():
    from ematix_flow.run_log import InMemoryRunLog

    cls, kwargs = _resolve_backend("memory://")
    assert cls is InMemoryRunLog
    assert kwargs == {}


def test_postgres_scheme():
    from ematix_flow.run_log import PostgresRunLog

    cls, kwargs = _resolve_backend("postgres://u:p@h:5432/db")
    assert cls is PostgresRunLog
    # The full URL is passed through as dsn (libpq accepts it).
    assert kwargs["dsn"] == "postgres://u:p@h:5432/db"


def test_postgresql_alias_also_works():
    from ematix_flow.run_log import PostgresRunLog

    cls, _ = _resolve_backend("postgresql://u@h/db")
    assert cls is PostgresRunLog


def test_mysql_scheme():
    from ematix_flow.run_log import MySQLRunLog

    cls, kwargs = _resolve_backend("mysql://u:p@h:3306/db")
    assert cls is MySQLRunLog
    assert kwargs["url"] == "mysql://u:p@h:3306/db"


def test_mariadb_scheme():
    from ematix_flow.run_log import MySQLRunLog

    cls, _ = _resolve_backend("mariadb://u@h/db")
    assert cls is MySQLRunLog


def test_duckdb_scheme():
    pytest.importorskip("duckdb", reason="DuckDBRunLog requires the duckdb extra")
    from ematix_flow.run_log import DuckDBRunLog

    cls, kwargs = _resolve_backend("duckdb:///tmp/run.duckdb")
    assert cls is DuckDBRunLog
    assert kwargs["path"] == "/tmp/run.duckdb"


def test_duckdb_memory():
    pytest.importorskip("duckdb", reason="DuckDBRunLog requires the duckdb extra")
    from ematix_flow.run_log import DuckDBRunLog

    cls, kwargs = _resolve_backend("duckdb://:memory:")
    assert cls is DuckDBRunLog
    assert kwargs["path"] == ":memory:"


def test_s3_scheme():
    from ematix_flow.run_log import S3RunLog

    cls, kwargs = _resolve_backend("s3://my-bucket/orchestrator/")
    assert cls is S3RunLog
    assert kwargs["bucket"] == "my-bucket"
    assert kwargs["prefix"] == "orchestrator/"


def test_s3_bucket_only():
    cls, kwargs = _resolve_backend("s3://my-bucket")
    assert kwargs["bucket"] == "my-bucket"
    assert kwargs["prefix"] == ""


def test_gcs_scheme():
    from ematix_flow.run_log import GcsRunLog

    cls, kwargs = _resolve_backend("gs://my-bucket/flow/")
    assert cls is GcsRunLog
    assert kwargs["bucket"] == "my-bucket"
    assert kwargs["prefix"] == "flow/"


def test_azure_scheme():
    """Custom convention: azure://<account>/<container>/<prefix>.
    Resolver synthesises the blob endpoint URL."""
    from ematix_flow.run_log import AzureBlobRunLog

    cls, kwargs = _resolve_backend("azure://myaccount/mycontainer/flow/")
    assert cls is AzureBlobRunLog
    assert kwargs["account_url"] == "https://myaccount.blob.core.windows.net"
    assert kwargs["container"] == "mycontainer"
    assert kwargs["prefix"] == "flow/"


def test_unknown_scheme_raises_with_helpful_message():
    with pytest.raises(ValueError) as ei:
        _resolve_backend("foo://bar")
    msg = str(ei.value).lower()
    assert "unknown" in msg or "unsupported" in msg
    assert "foo" in msg


def test_empty_string_rejected():
    with pytest.raises(ValueError):
        _resolve_backend("")


# ---- from_url() end-to-end for local backends -----------------------


def test_from_url_sqlite_round_trip(tmp_path):
    path = tmp_path / "run.db"
    log = from_url(f"sqlite:///{path}")
    try:
        from ematix_flow.run_log import SqliteRunLog
        assert isinstance(log, SqliteRunLog)
    finally:
        log.close()


def test_from_url_memory_round_trip():
    log = from_url("memory://")
    from ematix_flow.run_log import InMemoryRunLog
    assert isinstance(log, InMemoryRunLog)


def test_from_url_duckdb_round_trip(tmp_path):
    pytest.importorskip("duckdb", reason="DuckDBRunLog requires the duckdb extra")
    path = tmp_path / "run.duckdb"
    log = from_url(f"duckdb:///{path}")
    try:
        from ematix_flow.run_log import DuckDBRunLog
        assert isinstance(log, DuckDBRunLog)
    finally:
        log.close()


# ---- CLI integration -----------------------------------------------


def test_cli_run_log_url_picks_memory_backend(monkeypatch):
    """`flow run-due --run-log-url memory://` constructs an
    InMemoryRunLog. The legacy `--run-log-path` flag still works
    for SQLite."""
    from ematix_flow import cli
    from ematix_flow.run_log import InMemoryRunLog

    ns = argparse.Namespace(
        no_run_log=False,
        run_log_path=None,
        run_log_url="memory://",
    )
    log = cli._open_run_log_or_none(ns)
    assert isinstance(log, InMemoryRunLog)


def test_cli_legacy_run_log_path_still_works(tmp_path):
    """Back-compat: `--run-log-path /foo/bar.db` continues to work
    even though `--run-log-url` is the new canonical form."""
    from ematix_flow import cli
    from ematix_flow.run_log import SqliteRunLog

    ns = argparse.Namespace(
        no_run_log=False,
        run_log_path=str(tmp_path / "legacy.db"),
        run_log_url=None,
    )
    log = cli._open_run_log_or_none(ns)
    assert isinstance(log, SqliteRunLog)
    log.close()


def test_cli_url_env_var_override(monkeypatch):
    """$EMATIX_FLOW_RUN_LOG_URL overrides the default when no flag set."""
    from ematix_flow import cli
    from ematix_flow.run_log import InMemoryRunLog

    monkeypatch.setenv("EMATIX_FLOW_RUN_LOG_URL", "memory://")
    monkeypatch.delenv("EMATIX_FLOW_RUN_LOG_PATH", raising=False)
    ns = argparse.Namespace(
        no_run_log=False, run_log_path=None, run_log_url=None,
    )
    log = cli._open_run_log_or_none(ns)
    assert isinstance(log, InMemoryRunLog)


def test_cli_url_flag_beats_env_var(monkeypatch, tmp_path):
    """--run-log-url on the command line wins over $EMATIX_FLOW_RUN_LOG_URL."""
    from ematix_flow import cli
    from ematix_flow.run_log import SqliteRunLog

    monkeypatch.setenv("EMATIX_FLOW_RUN_LOG_URL", "memory://")
    ns = argparse.Namespace(
        no_run_log=False,
        run_log_path=None,
        run_log_url=f"sqlite:///{tmp_path}/run.db",
    )
    log = cli._open_run_log_or_none(ns)
    assert isinstance(log, SqliteRunLog)
    log.close()
