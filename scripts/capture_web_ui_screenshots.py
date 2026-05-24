#!/usr/bin/env python3
"""Capture Web UI screenshots via Playwright for ematix.dev.

Usage:

    python scripts/capture_web_ui_screenshots.py [OUT_DIR]

Default OUT_DIR is ``../ematix.dev/public/screenshots/`` relative to
the repo root. The script:

1. Starts ``npm run dev`` in ``web-ui/`` — the Vite dev server picks
   up ``web-ui/mock-api.mjs`` as a middleware so /api/* returns
   realistic fixture JSON without needing a Python backend.
2. Waits for the dev server to be ready on http://127.0.0.1:5173.
3. Drives a headless Chromium via Playwright at 1440×900 (retina)
   in dark color scheme to capture five PNGs:

       workflows-view.png   — #/workflows page with sub-DAG flowcharts
       jobs-view.png        — #/jobs page with per-job last-10 strips
       runs-list.png        — #/runs flat run history table
       dag-flowchart.png    — #/dag cross-job flowchart
       run-failed-restart.png — failed run detail with Restart hovered

4. Stops the dev server.

The mock data is in ``web-ui/mock-api.mjs`` — extend / tweak the
WORKFLOWS / PIPELINES / RUNS / DAG fixtures there to evolve what the
screenshots show.

Requires: ``npm`` (Vite dev server) and Playwright with the chromium
browser installed (``playwright install chromium``).
"""
from __future__ import annotations

import socket
import subprocess
import sys
import time
from pathlib import Path

DEV_PORT = 5173
DEV_URL = f"http://localhost:{DEV_PORT}"
WEB_UI_DIR = Path(__file__).resolve().parent.parent / "web-ui"


def _port_open(port: int) -> bool:
    # Vite binds to `localhost` (IPv6 ::1 + IPv4 127.0.0.1 only when
    # explicitly configured) — using "localhost" lets getaddrinfo pick
    # the right family.
    try:
        with socket.create_connection(("localhost", port), timeout=0.2):
            return True
    except OSError:
        return False


def _start_dev_server() -> subprocess.Popen | None:
    """Start `npm run dev` and wait for it to be ready. Returns the
    Popen handle (so the caller can terminate it), or None if a dev
    server is already running on port 5173 (re-use it)."""
    if _port_open(DEV_PORT):
        print(f"  (dev server already up on {DEV_PORT} — re-using)")
        return None
    print("  starting npm run dev …")
    proc = subprocess.Popen(
        ["npm", "run", "dev"],
        cwd=str(WEB_UI_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(100):  # up to 20s
        if _port_open(DEV_PORT):
            time.sleep(0.4)  # extra moment for Vite to wire mock middleware
            return proc
        time.sleep(0.2)
    proc.terminate()
    raise RuntimeError("npm run dev failed to come up on 127.0.0.1:5173")


def _capture(page, hash_path: str, out_path: Path, *, hover: str | None = None,
             scroll_to: int | None = None) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    page.goto(f"{DEV_URL}/#{hash_path}", wait_until="networkidle")
    # Force route re-evaluation if the hashchange listener was already
    # bound (the page may have been at a different hash before).
    page.evaluate("window.dispatchEvent(new HashChangeEvent('hashchange'))")
    # Give the SPA + data fetch a beat to settle.
    page.wait_for_timeout(900)
    if scroll_to is not None:
        page.evaluate(f"window.scrollTo(0, {scroll_to})")
        page.wait_for_timeout(150)
    if hover:
        try:
            page.hover(hover, timeout=2000)
            page.wait_for_timeout(200)
        except Exception:  # noqa: BLE001
            pass  # hover is best-effort
    page.screenshot(path=str(out_path), full_page=False)
    print(f"  → {out_path} ({out_path.stat().st_size:,} bytes)")


def main() -> int:
    # Default to the sibling ematix.dev checkout's screenshots dir,
    # resolved relative to the script (not cwd), so it works no
    # matter where the user invokes from.
    default_out = (
        Path(__file__).resolve().parent.parent.parent
        / "ematix.dev" / "public" / "screenshots"
    )
    out_dir = (
        Path(sys.argv[1]).resolve()
        if len(sys.argv) > 1
        else default_out
    )
    print(f"writing screenshots to {out_dir}")

    dev_proc = _start_dev_server()
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch()
            ctx = browser.new_context(
                viewport={"width": 1440, "height": 900},
                color_scheme="dark",
                device_scale_factor=2,  # retina
            )
            page = ctx.new_page()

            print("[1/5] workflows-view — Workflows landing with inline sub-DAGs")
            _capture(page, "/workflows", out_dir / "workflows-view.png")

            print("[2/5] jobs-view — Jobs with per-job last-10 duration strips")
            _capture(page, "/jobs", out_dir / "jobs-view.png")

            print("[3/5] runs-list — Runs table across all jobs")
            _capture(page, "/runs", out_dir / "runs-list.png")

            print("[4/5] dag-flowchart — cross-job DAG")
            _capture(page, "/dag", out_dir / "dag-flowchart.png")

            print("[5/5] run-failed-restart — failed run detail w/ Restart hovered")
            _capture(
                page,
                "/runs/r-99fc",  # nightly_reporting.materialize_dashboards failed run from mock
                out_dir / "run-failed-restart.png",
                hover="button.action:has-text('Restart')",
            )

            browser.close()
    finally:
        if dev_proc is not None:
            print("  stopping dev server …")
            dev_proc.terminate()
            try:
                dev_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                dev_proc.kill()

    print(f"done — {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
