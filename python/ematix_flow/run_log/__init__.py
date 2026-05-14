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

from .protocol import RunLog
from .sqlite import SqliteRunLog
from .memory import InMemoryRunLog


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
    "RunLog",
    "SqliteRunLog",
    "InMemoryRunLog",
    # Optional backends — lazily imported. Naming them here so static
    # tools can discover the full surface.
    "PostgresRunLog",
    "MySQLRunLog",
    "DuckDBRunLog",
    "S3RunLog",
    "AzureBlobRunLog",
    "GcsRunLog",
]
