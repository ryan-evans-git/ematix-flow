#!/usr/bin/env python3
"""Capture Web UI screenshots via Playwright for ematix.dev.

Usage:

    python scripts/capture_web_ui_screenshots.py [OUT_DIR]

Default OUT_DIR is ``../ematix.dev/public/screenshots/`` relative to
the repo root. The script:

1. Builds three flavors of demo data into in-memory ``RunHistoryStore``
   instances (one per screenshot scenario, kept separate so the views
   are uncluttered).
2. Launches the FastAPI app on a free port in a background thread for
   each scenario.
3. Drives a headless Chromium via Playwright to navigate + capture
   PNGs at 1440×900.

PNGs land at:

    OUT_DIR/runs-list.png
    OUT_DIR/runs-successful.png
    OUT_DIR/run-failed-restart.png

Requires: ``ematix-flow[web]`` (FastAPI + uvicorn) and Playwright with
the chromium browser installed (``playwright install chromium``).
"""
from __future__ import annotations

import socket
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import uvicorn

from ematix_flow.run_log.history import InMemoryRunHistory, RunRecord
from ematix_flow.web.server import create_app


UTC = timezone.utc


def _free_port() -> int:
    """Pick an ephemeral port the OS hasn't assigned yet."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _start_server(store: InMemoryRunHistory, port: int) -> threading.Thread:
    """Start uvicorn in a background thread bound to 127.0.0.1:port."""
    app = create_app(history=store)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)

    def _run() -> None:
        server.run()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    # Poll until the port responds.
    for _ in range(50):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return t
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"server failed to come up on 127.0.0.1:{port}")


# ----- Demo data factories ------------------------------------------


def _runs_list_store() -> InMemoryRunHistory:
    """Mix of streaming + batch + a failure for the headline screenshot."""
    store = InMemoryRunHistory()
    base = datetime(2026, 5, 20, 16, 0, 0, tzinfo=UTC)
    # Streaming run, currently running.
    store.record_run_record(
        RunRecord(
            run_id="01HQ-stream-running",
            pipeline="events_stream",
            status="running",
            started_at=base,
            kind="streaming",
        )
    )
    # Three successful warehouse_etl runs (oldest first).
    for i, mins in enumerate([-90, -150, -210]):
        ts = base + timedelta(minutes=mins)
        store.record_run_record(
            RunRecord(
                run_id=f"01HQ-wh-{i}",
                pipeline="warehouse_etl",
                status="succeeded",
                started_at=ts,
                finished_at=ts + timedelta(seconds=47),
            )
        )
    # The failed warehouse_etl that lives between the green ones.
    store.record_run_record(
        RunRecord(
            run_id="01HQ-warehouse-fail",
            pipeline="warehouse_etl",
            status="failed",
            started_at=base + timedelta(minutes=-60),
            finished_at=base + timedelta(minutes=-60, seconds=35),
            attempt=2,
            failed_step="merge_payments",
            error_summary="ValueError: amount column missing in batch 47",
        )
    )
    # A long-running streaming history (succeeded after 3 hours).
    store.record_run_record(
        RunRecord(
            run_id="01HQ-stream-prior",
            pipeline="events_stream",
            status="succeeded",
            started_at=base + timedelta(hours=-3),
            finished_at=base + timedelta(minutes=-1),
            kind="streaming",
        )
    )
    # A few succeeded orders_etl runs.
    for i in range(3):
        ts = base + timedelta(hours=-1 - i)
        store.record_run_record(
            RunRecord(
                run_id=f"01HQ-orders-{i}",
                pipeline="orders_etl",
                status="succeeded",
                started_at=ts,
                finished_at=ts + timedelta(seconds=45 + i),
            )
        )
    return store


def _pipelines_view_store() -> InMemoryRunHistory:
    """Four pipelines with rich last-10 histories for the pipelines
    overview screenshot.

    Mix:
    - ``events_stream``  — streaming, currently running (LIVE pill).
    - ``orders_etl``     — clean 10/10 green.
    - ``warehouse_etl``  — mostly green with one failure 4 runs back
                            (retried-then-succeeded shows as success).
    - ``customers_sync`` — recent green track + ``next_run_at``.
    """
    store = InMemoryRunHistory()
    base = datetime(2026, 5, 20, 16, 0, 0, tzinfo=UTC)

    # events_stream: currently running (kind=streaming, no
    # next_run_at — UI renders the LIVE STREAMING pill instead).
    for i in range(9):
        ts = base + timedelta(hours=-3 - i * 3)
        store.record_run_record(
            RunRecord(
                run_id=f"01HQ-stream-{i}",
                pipeline="events_stream",
                status="succeeded",
                started_at=ts,
                finished_at=ts + timedelta(hours=2, minutes=58),
                kind="streaming",
            )
        )
    store.record_run_record(
        RunRecord(
            run_id="01HQ-stream-current",
            pipeline="events_stream",
            status="running",
            started_at=base,
            kind="streaming",
        )
    )

    # orders_etl: 10 consecutive succeeds, next run forecast.
    next_orders = (base + timedelta(minutes=22)).isoformat()
    for i in range(10):
        ts = base + timedelta(minutes=-15 - i * 30)
        store.record_run_record(
            RunRecord(
                run_id=f"01HQ-orders-{i}",
                pipeline="orders_etl",
                status="succeeded",
                started_at=ts,
                finished_at=ts + timedelta(seconds=44 + i % 4),
                extras={"next_run_at": next_orders},
            )
        )

    # warehouse_etl: mostly green, one failed 4 runs back. The
    # subsequent run after the failure succeeded (retry honored).
    next_wh = (base + timedelta(minutes=58)).isoformat()
    for i in range(10):
        if i == 4:
            store.record_run_record(
                RunRecord(
                    run_id="01HQ-wh-fail",
                    pipeline="warehouse_etl",
                    status="failed",
                    started_at=base + timedelta(minutes=-90 - i * 60),
                    finished_at=base + timedelta(minutes=-89 - i * 60),
                    attempt=2,
                    failed_step="merge_payments",
                    error_summary="ValueError: amount column missing in batch 47",
                    extras={"next_run_at": next_wh},
                )
            )
        else:
            ts = base + timedelta(minutes=-30 - i * 60)
            store.record_run_record(
                RunRecord(
                    run_id=f"01HQ-wh-{i}",
                    pipeline="warehouse_etl",
                    status="succeeded",
                    started_at=ts,
                    finished_at=ts + timedelta(seconds=52 + i % 3),
                    extras={"next_run_at": next_wh},
                )
            )

    # customers_sync: tidy green track, next run forecast.
    next_cust = (base + timedelta(hours=5)).isoformat()
    for i in range(7):
        ts = base + timedelta(hours=-6 - i * 6)
        store.record_run_record(
            RunRecord(
                run_id=f"01HQ-cust-{i}",
                pipeline="customers_sync",
                status="succeeded",
                started_at=ts,
                finished_at=ts + timedelta(seconds=18 + i % 4),
                extras={"next_run_at": next_cust},
            )
        )
    return store


def _workflow_dag_store() -> InMemoryRunHistory:
    """In-flight workflow with a parallel branch — for the DAG shot.

    Topology:

        extract_orders ──┬─► transform_orders ─────────┐
                         │                              ├─► merge_payments ─► load_warehouse
                         └─► transform_payments ───────┘
    """
    store = InMemoryRunHistory()
    ts = datetime(2026, 5, 20, 16, 15, 0, tzinfo=UTC)
    steps = [
        {
            "name": "extract_orders",
            "status": "succeeded",
            "depends_on": [],
            "duration_ms": 4_200,
        },
        {
            "name": "transform_orders",
            "status": "succeeded",
            "depends_on": ["extract_orders"],
            "duration_ms": 8_750,
        },
        {
            "name": "transform_payments",
            "status": "running",
            "depends_on": ["extract_orders"],
        },
        {
            "name": "merge_payments",
            "status": "pending",
            "depends_on": ["transform_orders", "transform_payments"],
        },
        {
            "name": "load_warehouse",
            "status": "pending",
            "depends_on": ["merge_payments"],
        },
    ]
    store.record_run_record(
        RunRecord(
            run_id="01HQ-orders-dag",
            pipeline="orders_etl",
            status="running",
            started_at=ts,
            extras={"steps": steps},
        )
    )
    return store


def _failed_detail_store() -> InMemoryRunHistory:
    """One failed warehouse_etl run for the detail-page shot."""
    store = InMemoryRunHistory()
    ts = datetime(2026, 5, 20, 14, 30, 0, tzinfo=UTC)
    store.record_run_record(
        RunRecord(
            run_id="01HQ-warehouse-fail",
            pipeline="warehouse_etl",
            status="failed",
            started_at=ts,
            finished_at=ts + timedelta(seconds=35),
            attempt=2,
            failed_step="merge_payments",
            error_summary="ValueError: amount column missing in batch 47",
        )
    )
    return store


# ----- Playwright capture -------------------------------------------


def _capture(url: str, out_path: Path, *, focus_selector: str | None = None) -> None:
    from playwright.sync_api import sync_playwright

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(
            viewport={"width": 1440, "height": 900},
            color_scheme="dark",
            device_scale_factor=2,  # retina for sharper PNGs
        )
        page = ctx.new_page()
        page.goto(url, wait_until="networkidle")
        # Give the SPA a beat to render after fetch completes.
        page.wait_for_timeout(800)
        if focus_selector:
            try:
                page.hover(focus_selector)
                page.wait_for_timeout(200)
            except Exception:  # noqa: BLE001
                pass  # hover is best-effort
        page.screenshot(path=str(out_path), full_page=False)
        browser.close()
    print(f"  → {out_path} ({out_path.stat().st_size:,} bytes)")


def main() -> int:
    out_dir = Path(
        sys.argv[1]
        if len(sys.argv) > 1
        else "../ematix.dev/public/screenshots"
    ).resolve()
    print(f"writing screenshots to {out_dir}")

    print("[1/4] pipelines-view — per-pipeline last-10 strip + next-run / LIVE")
    port = _free_port()
    _start_server(_pipelines_view_store(), port)
    _capture(
        f"http://127.0.0.1:{port}/#/pipelines",
        out_dir / "pipelines-view.png",
    )

    print("[2/4] jobs-list — flat table of run records")
    port = _free_port()
    _start_server(_runs_list_store(), port)
    _capture(f"http://127.0.0.1:{port}/#/runs", out_dir / "jobs-list.png")

    print("[3/4] run-failed-restart — detail with action button hovered")
    port = _free_port()
    _start_server(_failed_detail_store(), port)
    _capture(
        f"http://127.0.0.1:{port}/#/runs/01HQ-warehouse-fail",
        out_dir / "run-failed-restart.png",
        focus_selector="button.action:has-text('Restart')",
    )

    print("[4/4] workflow-dag — running DAG with parallel branch")
    port = _free_port()
    _start_server(_workflow_dag_store(), port)
    _capture(
        f"http://127.0.0.1:{port}/#/runs/01HQ-orders-dag",
        out_dir / "workflow-dag.png",
    )

    print(f"done — {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
