"""Declarative append-only pipeline against Postgres.

Demonstrates the shortest-possible v0.1 path: declare a target
table, declare a pipeline that returns SQL, fire it.

Requires a local Postgres. Spin one up with:

    docker compose -f examples/docker-compose.yml up -d

Run:
    EMATIX_FLOW_DSN=postgres://postgres:postgres@localhost/postgres \\
        python examples/01_append.py
"""

from typing import Annotated

from ematix_flow import ematix, pk
from ematix_flow.types import BigInt, Text, TimestampTZ


@ematix.table(schema="analytics")
class Events:
    event_id: Annotated[BigInt, pk()]
    name: Text | None
    received_at: TimestampTZ


@ematix.pipeline(target=Events, schedule=None, mode="append")
def ingest_events(conn):
    # In a real pipeline, replace this with a SELECT from your
    # source table. Synthetic source for the demo.
    return """
        SELECT 1::bigint AS event_id, 'first'  AS name,
               '2026-05-01T10:00:00Z'::timestamptz AS received_at
        UNION ALL
        SELECT 2::bigint AS event_id, 'second' AS name,
               '2026-05-01T10:05:00Z'::timestamptz AS received_at
    """


if __name__ == "__main__":
    # Connection picked up from EMATIX_FLOW_DSN — see
    # docs/USER_GUIDE.md "Connections".
    metrics = ingest_events.run()
    print(f"Inserted {metrics.rows_inserted} rows.")
