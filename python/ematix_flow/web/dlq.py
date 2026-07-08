"""DLQ Phase 4: the server's DLQ/rewind operations layer.

``StreamDlqOps`` is the default implementation ``create_app`` uses
when no ``dlq_ops`` is injected: it resolves a stream name through
the streaming-pipeline registry (``@ematix.streaming_pipeline``
decorated pipelines in the operator's imported module), renders the
registered shape to TOML, and drives the pyo3-backed operations in
``ematix_flow._core`` (which build a cached, operations-only Rust
pipeline and hit the SAME dead-letter store resolution the running
pipeline uses).

Tests inject a fake with the same method surface — the protocol is
duck-typed on purpose (mirrors how ``history`` is injected).

Method contract (shared with the fakes in ``test_web_dlq.py``):

- unknown stream name → raise ``KeyError`` (the server maps it to
  404);
- typed operation failures (unsatisfiable ``dlq_store`` demand,
  missing ``confirm_state_reset``, unsupported seek, …) → raise
  ``ValueError`` with the Rust error string (server maps to 400);
- ``selection`` / ``to`` are the JSON-dict shapes the HTTP API
  accepts; this layer serializes them for the pyo3 boundary.
"""

from __future__ import annotations

import json
from typing import Any

__all__ = ["StreamDlqOps"]


class StreamDlqOps:
    """pyo3-backed ops over registered streaming pipelines."""

    def _toml(self, name: str) -> str:
        # KeyError propagates for unknown names — the server's 404.
        from ematix_flow.streaming import render_streaming_pipeline_toml

        return render_streaming_pipeline_toml(name)

    def stats(self, name: str, now_ms: int) -> dict[str, Any]:
        from ematix_flow import _core

        return _core.dlq_stats(self._toml(name), now_ms)

    def records(
        self, name: str, status: str | None, page: int, page_size: int
    ) -> list[dict[str, Any]]:
        from ematix_flow import _core

        return _core.dlq_records(self._toml(name), status, page, page_size)

    def record_by_id(self, name: str, record_id: str) -> dict[str, Any] | None:
        from ematix_flow import _core

        return _core.dlq_record_by_id(self._toml(name), record_id)

    def replay(
        self, name: str, selection: dict[str, Any], max_attempts: int | None
    ) -> dict[str, Any]:
        from ematix_flow import _core

        return _core.dlq_replay(
            self._toml(name), json.dumps(selection), max_attempts
        )

    def park(self, name: str, selection: dict[str, Any]) -> int:
        from ematix_flow import _core

        return _core.dlq_park(self._toml(name), json.dumps(selection))

    def purge(self, name: str, selection: dict[str, Any]) -> int:
        from ematix_flow import _core

        return _core.dlq_purge(self._toml(name), json.dumps(selection))

    def rewind(
        self, name: str, to: dict[str, Any], confirm_state_reset: bool
    ) -> dict[str, Any]:
        from ematix_flow import _core

        return _core.stream_rewind(
            self._toml(name), json.dumps(to), confirm_state_reset
        )
