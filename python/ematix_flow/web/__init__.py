"""ematix-flow Web UI server (Phase 4 of "What's not shipped").

Read + mutate HTTP API + embedded Vite/Svelte SPA for browsing run
history and triggering pipeline restarts / reruns / pauses.

The server is a FastAPI app served via uvicorn; it talks to any
existing RunLog backend via the standard
:class:`ematix_flow.run_log.protocol.RunLog` Protocol. Mutating
actions (restart, rerun, pause, resume) write new RunLog rows; the
existing scheduler daemon (Ω.W.6) picks them up via its claim loop.

Binds to ``127.0.0.1`` by default; remote access goes through an
SSH tunnel or a reverse proxy. ``--bind 0.0.0.0`` is accepted but
logs a loud warning that mutating endpoints are reachable off-host
without auth.

See ``docs/PHASE_WEB_UI_DESIGN.md`` for the full architecture.

This first slice ships the server skeleton with stub data — the
read endpoints return placeholder responses so the SPA and CLI
plumbing can be tested in isolation before the RunLog Protocol
gets its ``list_runs`` / ``get_run`` extension in the next slice.
"""
from __future__ import annotations

__all__ = ["create_app", "run_server"]


def create_app(**kwargs):
    """Lazy import wrapper around :func:`.server.create_app`.

    Keeps ``import ematix_flow.web`` cheap; FastAPI / starlette pull
    on first call. Users who don't run the Web UI pay nothing.
    """
    from ematix_flow.web.server import create_app as _create

    return _create(**kwargs)


def run_server(**kwargs):
    """Lazy import wrapper around :func:`.server.run_server`."""
    from ematix_flow.web.server import run_server as _run

    return _run(**kwargs)
