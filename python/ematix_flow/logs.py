"""Per-run log capture + retrieval.

Today's RunLog stores per-run *summary* state (status, duration,
error_summary). What's missing for on-call workflows is the full
stdout/stderr capture from the pipeline body so you can answer "why
did this fail" without re-running.

This module adds:

* :func:`capture_pipeline_logs` — context manager that tees stdout
  + stderr into both the original streams (for live tailing) and a
  per-``run_id`` file under :func:`logs_dir`.
* :func:`logs_dir` — resolves the on-disk log directory, honoring
  ``EMATIX_FLOW_LOGS_DIR`` then falling back to
  ``~/.ematix-flow/logs/``.
* :func:`read_run_logs` — load the captured text for a ``run_id``.
* :func:`prune_old_logs` — best-effort cleanup so the directory
  doesn't grow unbounded.

The scheduler (`flow run-due` / `flow scheduler`) writes logs
when ``EMATIX_FLOW_CAPTURE_LOGS`` is set (off by default to keep
the historical no-op path). ``flow logs <run_id>`` reads them back
regardless of capture mode.
"""
from __future__ import annotations

import io
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

__all__ = [
    "capture_pipeline_logs",
    "logs_dir",
    "prune_old_logs",
    "read_run_logs",
]


def logs_dir() -> Path:
    """Where per-run logs land. ``EMATIX_FLOW_LOGS_DIR`` env var wins;
    otherwise ``~/.ematix-flow/logs/``. The directory is created on
    first access so callers don't need to mkdir."""
    override = os.environ.get("EMATIX_FLOW_LOGS_DIR")
    if override:
        path = Path(override).expanduser()
    else:
        path = Path.home() / ".ematix-flow" / "logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


class _TeeStream(io.TextIOBase):
    """Write-through tee: every write goes to ``original`` (so the
    user still sees output in their terminal) AND to ``capture`` (a
    StringIO that the caller will flush to disk)."""

    def __init__(self, original, capture):
        self._original = original
        self._capture = capture

    def write(self, data: str) -> int:
        # Best-effort: if the original stream is in some unwriteable
        # state (closed in tests, etc.), don't take the capture down
        # with it.
        try:
            self._original.write(data)
        except Exception:
            pass
        self._capture.write(data)
        return len(data)

    def flush(self) -> None:
        try:
            self._original.flush()
        except Exception:
            pass

    def isatty(self) -> bool:
        try:
            return bool(self._original.isatty())
        except Exception:
            return False


@contextmanager
def capture_pipeline_logs(run_id: str):
    """Tee stdout+stderr to ``logs_dir()/<run_id>.log`` for the
    duration of the block. The original streams keep working (so a
    user watching the terminal still sees output); the file is
    written atomically on exit. Always yields the path so the caller
    can record it in RunRecord.extras["logs_path"]."""
    buf = io.StringIO()
    orig_stdout, orig_stderr = sys.stdout, sys.stderr
    sys.stdout = _TeeStream(orig_stdout, buf)
    sys.stderr = _TeeStream(orig_stderr, buf)
    log_path = logs_dir() / f"{run_id}.log"
    try:
        yield log_path
    finally:
        sys.stdout, sys.stderr = orig_stdout, orig_stderr
        contents = buf.getvalue()
        # Write to a temp path + rename so the file is never half-
        # written; readers (flow logs <run_id>) see all-or-nothing.
        tmp = log_path.with_suffix(".log.tmp")
        tmp.write_text(contents)
        tmp.replace(log_path)


def read_run_logs(run_id: str) -> str | None:
    """Return the captured text for ``run_id``, or ``None`` if no
    log file exists for that run."""
    path = logs_dir() / f"{run_id}.log"
    if not path.exists():
        return None
    return path.read_text()


def prune_old_logs(max_age_days: int = 30) -> int:
    """Delete log files older than ``max_age_days``. Returns the
    number of files removed. Best-effort: errors on individual file
    removals are silently ignored so a stuck file doesn't abort the
    whole sweep."""
    cutoff = time.time() - max_age_days * 86400
    removed = 0
    for path in logs_dir().glob("*.log"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        except OSError:
            continue
    return removed
