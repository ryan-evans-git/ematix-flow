"""Declarative append-only pipeline against Postgres.

Shows the full v0.1 flow end-to-end:

  1. **Register a connection** by name. The pipeline references it
     by name string, not by importing a module-level handle.
  2. **Declare a target table** with PEP-593-annotated columns.
  3. **Declare a pipeline** that returns the source SQL. The
     decorator wires it to the target + connection.
  4. **Run it.** ``ingest_events.run()`` returns a metrics dataclass
     with row counts per outcome.

Requires a local Postgres. Spin one up with:

    docker compose -f examples/docker-compose.yml up -d

Run:
    EMATIX_FLOW_DSN=postgres://postgres:postgres@localhost/postgres \\
        python examples/01_append.py
"""

from typing import Annotated

from ematix_flow import ematix, pk
from ematix_flow.connections import PostgresConnection, register_connection
from ematix_flow.types import BigInt, Text, TimestampTZ

# ---------------------------------------------------------------
# 1. Register the warehouse connection.
#
# Three things to notice:
#
# - `name="warehouse"` is the handle pipelines reference via
#   `target_connection="warehouse"`. Multiple pipelines can
#   share one connection.
# - `url="${EMATIX_FLOW_DSN}"` is interpolated from the
#   environment **at backend-build time** (when `.run()` fires),
#   not at definition time — change the env between definition
#   and run and the new value is picked up; undefined vars
#   surface as a clear `KeyError`.
# - The dataclass `repr` redacts password / secret fields, so
#   logging a connection (or any pipeline that holds one) is safe.
#
# Form alternatives + the full connection surface:
# `docs/USER_GUIDE.md` "Connections".
# ---------------------------------------------------------------
register_connection(
    PostgresConnection(
        name="warehouse",
        url="${EMATIX_FLOW_DSN}",
    )
)


@ematix.table(schema="analytics")
class Events:
    event_id: Annotated[BigInt, pk()]
    name: Text | None
    received_at: TimestampTZ


@ematix.pipeline(
    target=Events,
    target_connection="warehouse",
    schedule=None,
    mode="append",
)
def ingest_events(conn):
    # The `conn` argument is the resolved warehouse connection,
    # supplied by the runtime. Most pipelines just return SQL —
    # `conn` is here for the rare case of needing live metadata
    # at SQL-build time. In a real pipeline, replace the body
    # below with a SELECT from your source table; the demo
    # below is a synthetic two-row source.
    return """
        SELECT 1::bigint AS event_id, 'first'  AS name,
               '2026-05-01T10:00:00Z'::timestamptz AS received_at
        UNION ALL
        SELECT 2::bigint AS event_id, 'second' AS name,
               '2026-05-01T10:05:00Z'::timestamptz AS received_at
    """


if __name__ == "__main__":
    metrics = ingest_events.run()
    print(f"Inserted {metrics.rows_inserted} rows.")
