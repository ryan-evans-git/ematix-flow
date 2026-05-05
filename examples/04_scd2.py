"""SCD2 strategy: versioned history with `valid_from` / `valid_to`.

Demonstrates the canonical Type-2 dimension pattern:
- One row per (business_key, valid_from) pair.
- Updates close out the old version (`valid_to = new.valid_from`,
  `is_current = false`) and insert a fresh version.
- `event_timestamp_column` makes `valid_from` event-time rather
  than load-time — restart-safe + replay-safe.

Requires a local Postgres because SQLite's column-add semantics
don't compose with the SCD2 augmentation that adds `valid_from`,
`valid_to`, `is_current`, and `row_hash` automatically. Spin one
up with:

    docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres \\
        --name ematix-pg postgres:16

Run:
    EMATIX_FLOW_DSN=postgres://postgres:postgres@localhost/postgres \\
        python examples/04_scd2.py
"""

from typing import Annotated

from ematix_flow import ematix, pk
from ematix_flow.connections import PostgresConnection, register_connection
from ematix_flow.normalize import lower, parse_timestamp, trim
from ematix_flow.types import BigInt, String, Text, TimestampTZ

# Connection registration — see `01_append.py` for the rationale.
register_connection(
    PostgresConnection(
        name="warehouse",
        url="${EMATIX_FLOW_DSN}",
    )
)


@ematix.table(schema="analytics")
class CustomerDim:
    # Business key — stays the natural identifier across versions.
    customer_id: Annotated[BigInt, pk()]
    email: Annotated[String[256] | None, lower(), trim()]
    name: Text | None
    # Event-time column — used as `valid_from` for the version row.
    updated_at: Annotated[TimestampTZ, parse_timestamp()]


@ematix.pipeline(
    target=CustomerDim,
    target_connection="warehouse",
    schedule=None,
    mode="scd2",
    compare_columns=["email", "name"],
    event_timestamp_column="updated_at",
    # ttl_seconds=86_400 * 30,  # auto-close versions older than 30d
)
def sync_customers(conn):
    return """
        SELECT 1 AS customer_id, 'alice@example.com' AS email, 'Alice' AS name,
               '2026-05-01T10:00:00Z'::timestamptz AS updated_at
        UNION ALL
        SELECT 1 AS customer_id, 'alice@example.com' AS email, 'Alice Smith' AS name,
               '2026-05-02T15:00:00Z'::timestamptz AS updated_at
    """


if __name__ == "__main__":
    metrics = sync_customers.run()
    print(
        f"SCD2 sync: inserted {metrics.rows_inserted}, "
        f"closed {metrics.rows_closed} versions."
    )
