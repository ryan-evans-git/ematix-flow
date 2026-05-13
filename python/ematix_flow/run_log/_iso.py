"""Shared ISO-8601 helpers for RunLog backends.

Every backend stores timestamps as strings in `YYYY-MM-DDTHH:MM:SSZ`
form. Centralising the parse/format keeps the wire format consistent
across SQLite, Postgres, and the three object stores.
"""

from __future__ import annotations

from datetime import datetime, timezone


def iso_utc(ts: datetime) -> str:
    """Format a UTC datetime as ISO8601 with a trailing Z (seconds
    precision; sub-second components are dropped)."""
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(s: str) -> datetime:
    """Inverse of `iso_utc`. Accepts the trailing-Z form we emit and
    any other ISO-8601 string Python's `fromisoformat` understands."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)
