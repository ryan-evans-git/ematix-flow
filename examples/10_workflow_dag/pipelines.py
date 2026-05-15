"""Workflow demo — three pipelines wired into a DAG, dispatched by the
Ω.W central scheduler.

Shape:
    raw_orders  (every minute)
        │
        ▼
    enriched_orders  (every minute, depends_on=raw_orders)
        │
        ▼
    daily_summary  (every minute, depends_on=enriched_orders)
                   (flaky — fails ~30% of the time to show retries)

Each pipeline writes a row to a SQLite "warehouse" file
(`/tmp/ematix-demo-10.db`) so you can `sqlite3` it and see results
accumulate. Retries, gave-up gating, upstream-freshness gating, and
the run-history are observable via:

    flow status \\
        --module pipelines \\
        --run-log-url sqlite:///tmp/ematix-demo-10-runs.db

Run the scheduler (dispatches each due pipeline):

    flow scheduler \\
        --module pipelines \\
        --executor "subprocess+python://" \\
        --run-log-url sqlite:///tmp/ematix-demo-10-runs.db \\
        --poll-interval 5 \\
        --interval 60

For the demo, every pipeline fires every minute. In production the
schedules would naturally space out (`raw_orders` hourly,
`enriched_orders` after `raw_orders` lands, `daily_summary` at 2am).
"""

from __future__ import annotations

import random
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from ematix_flow.pipeline import register

WAREHOUSE = Path("/tmp/ematix-demo-10.db")


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(WAREHOUSE)
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER, amount REAL, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS enriched_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER, amount REAL, tier TEXT, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS daily_summary (
            d TEXT PRIMARY KEY, order_count INTEGER, total_amount REAL
        );
        """
    )
    return c


@register(name="raw_orders", schedule="* * * * *")
def raw_orders() -> dict:
    """Simulate ingest from a source-of-record. Writes 5 new orders
    per run."""
    now = datetime.now(UTC).isoformat()
    with _conn() as c:
        cursor = c.executemany(
            "INSERT INTO raw_orders (order_id, customer_id, amount, created_at) "
            "VALUES (?, ?, ?, ?)",
            [
                (random.randint(10_000_000, 99_999_999),
                 random.randint(1, 50),
                 round(random.uniform(10, 500), 2),
                 now)
                for _ in range(5)
            ],
        )
    return {"inserted": cursor.rowcount, "at": now}


@register(
    name="enriched_orders",
    schedule="* * * * *",
    depends_on=["raw_orders"],
)
def enriched_orders() -> dict:
    """Add a customer-tier column based on amount. Only fires after
    raw_orders has succeeded today (upstream-freshness gate)."""
    with _conn() as c:
        rows = c.execute(
            "SELECT order_id, customer_id, amount, created_at "
            "FROM raw_orders "
            "WHERE order_id NOT IN (SELECT order_id FROM enriched_orders)"
        ).fetchall()
        enriched = [
            (oid, cid, amt, "premium" if amt > 250 else "standard", ts)
            for (oid, cid, amt, ts) in rows
        ]
        c.executemany(
            "INSERT INTO enriched_orders "
            "(order_id, customer_id, amount, tier, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            enriched,
        )
    return {"enriched": len(enriched)}


@register(
    name="daily_summary",
    schedule="* * * * *",
    depends_on=["enriched_orders"],
    retry={"max_attempts": 4, "backoff": "exponential", "base_secs": 5},
)
def daily_summary() -> dict:
    """Aggregate enriched orders by day. **Flaky on purpose** —
    raises 30% of the time so you can watch the scheduler retry with
    exponential backoff (5s, 10s, 20s)."""
    if random.random() < 0.30:
        raise RuntimeError("synthetic flake — exercise the retry path")
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO daily_summary (d, order_count, total_amount) "
            "SELECT substr(created_at, 1, 10), COUNT(*), ROUND(SUM(amount), 2) "
            "FROM enriched_orders GROUP BY 1"
        )
        days = c.execute("SELECT COUNT(*) FROM daily_summary").fetchone()[0]
    return {"days_summarized": days}
