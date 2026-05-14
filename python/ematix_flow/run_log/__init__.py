"""Durable run-history backends.

All backends satisfy the same `RunLog` Protocol (record_run,
record_attempt, clear_attempt_state, restore_into_process, close) and
are interchangeable in `pipeline.run_due_with_dag(run_log=...)`.

Default (always available):
  - SqliteRunLog    — local file, stdlib sqlite3
  - InMemoryRunLog  — non-persistent, for tests

Optional (import errors when their dep isn't installed):
  - PostgresRunLog   — needs `psycopg`
  - MySQLRunLog      — needs `PyMySQL`
  - DuckDBRunLog     — needs `duckdb`
  - S3RunLog         — needs `boto3`
  - AzureBlobRunLog  — needs `azure-storage-blob`
  - GcsRunLog        — needs `google-cloud-storage`

Import lazily so a missing optional dep doesn't break the default
import:

    from ematix_flow.run_log import S3RunLog   # raises on import if
                                              # boto3 not installed
"""

from .memory import InMemoryRunLog
from .protocol import RunLog
from .sqlite import SqliteRunLog

# ---- URL-based backend factory ---------------------------------------------
#
# `from_url(url)` lets users pick any backend with one CLI flag
# (`--run-log-url`) instead of bespoke per-backend wiring. Scheme
# dispatch:
#
#   sqlite:///path                  → SqliteRunLog(path=...)
#   /bare/path/run.db               → SqliteRunLog(path=...)
#   memory://                       → InMemoryRunLog()
#   postgres://u:p@h:port/db        → PostgresRunLog(dsn=full url)
#   postgresql://...                → PostgresRunLog (alias)
#   mysql://u:p@h:port/db           → MySQLRunLog(url=full url)
#   mariadb://...                   → MySQLRunLog (alias)
#   duckdb:///path                  → DuckDBRunLog(path=...)
#   duckdb://:memory:               → DuckDBRunLog(path=":memory:")
#   s3://bucket/prefix              → S3RunLog(bucket, prefix)
#   gs://bucket/prefix              → GcsRunLog(bucket, prefix)
#   azure://<account>/<container>/<prefix>
#                                   → AzureBlobRunLog (account synthesised
#                                     to "https://<account>.blob.core.windows.net")
#
# Tested in `tests/python/test_phase_omega_d3_runlog_urls.py`.


def from_url(url: str) -> "RunLog":
    """Construct a RunLog from a URL. See module docstring for schemes."""
    cls, kwargs = _resolve_backend(url)
    return cls(**kwargs)


def _resolve_backend(url: str):
    """Pure URL → (BackendClass, kwargs) dispatcher. Doesn't import
    optional deps or open connections — those happen when the caller
    instantiates the class.

    Returns `(class, kwargs)` so tests can verify dispatch without
    needing real databases or cloud credentials.
    """
    import os
    from urllib.parse import urlparse

    if not url:
        raise ValueError("RunLog URL must not be empty")

    # Bare path? (no scheme, or scheme is a single-letter drive on Windows
    # — but that's `file://` territory; ignore here)
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme == "" or scheme == "file":
        # Treat as SQLite local path.
        path = parsed.path if scheme == "file" else url
        return SqliteRunLog, {"path": os.path.expanduser(path)}

    if scheme == "sqlite":
        # SQLAlchemy convention: sqlite:///abs/path or sqlite:///./rel
        path = url[len("sqlite://"):]
        # urlparse munges sqlite:///path → netloc="", path="/path"; strip
        # the netloc if present (sqlite://host/path is non-standard but
        # tolerate it).
        if path.startswith("/") and parsed.netloc == "":
            path = path  # keep absolute
        return SqliteRunLog, {"path": os.path.expanduser(path)}

    if scheme == "memory":
        return InMemoryRunLog, {}

    if scheme in ("postgres", "postgresql"):
        from .postgres import PostgresRunLog
        return PostgresRunLog, {"dsn": url}

    if scheme in ("mysql", "mariadb"):
        from .mysql import MySQLRunLog
        return MySQLRunLog, {"url": url}

    if scheme == "duckdb":
        from .duckdb import DuckDBRunLog
        path = url[len("duckdb://"):]
        return DuckDBRunLog, {"path": path}

    if scheme == "s3":
        from .s3 import S3RunLog
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        return S3RunLog, {"bucket": bucket, "prefix": prefix}

    if scheme in ("gs", "gcs"):
        from .gcs import GcsRunLog
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        return GcsRunLog, {"bucket": bucket, "prefix": prefix}

    if scheme == "azure":
        from .azure_blob import AzureBlobRunLog
        # azure://<account>/<container>/<prefix>
        account = parsed.netloc
        path_parts = parsed.path.lstrip("/").split("/", 1)
        if not account or not path_parts[0]:
            raise ValueError(
                f"azure:// URL must be azure://<account>/<container>[/<prefix>], "
                f"got {url!r}"
            )
        container = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        return AzureBlobRunLog, {
            "account_url": f"https://{account}.blob.core.windows.net",
            "container": container,
            "prefix": prefix,
        }

    raise ValueError(
        f"unknown / unsupported RunLog URL scheme {scheme!r} in {url!r}. "
        f"Supported: sqlite, file, memory, postgres, postgresql, mysql, "
        f"mariadb, duckdb, s3, gs, gcs, azure, or a bare path."
    )


def __getattr__(name: str):
    """Lazy-load optional backends so `from ematix_flow.run_log import X`
    only fails when X's optional dep is missing, not for every import."""
    if name == "PostgresRunLog":
        from .postgres import PostgresRunLog
        return PostgresRunLog
    if name == "MySQLRunLog":
        from .mysql import MySQLRunLog
        return MySQLRunLog
    if name == "DuckDBRunLog":
        from .duckdb import DuckDBRunLog
        return DuckDBRunLog
    if name == "S3RunLog":
        from .s3 import S3RunLog
        return S3RunLog
    if name == "AzureBlobRunLog":
        from .azure_blob import AzureBlobRunLog
        return AzureBlobRunLog
    if name == "GcsRunLog":
        from .gcs import GcsRunLog
        return GcsRunLog
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AzureBlobRunLog",
    "DuckDBRunLog",
    "GcsRunLog",
    "InMemoryRunLog",
    "MySQLRunLog",
    "PostgresRunLog",
    "RunLog",
    "S3RunLog",
    "SqliteRunLog",
]
