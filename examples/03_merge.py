"""MergeUpsert (SCD1) strategy: upsert by primary key.

New rows insert; rows whose key already exists update in place.
Source-rows that are absent from the target are left alone (no
delete) unless `handle_deletes="hard"` or `"soft"` is set.

Requires a local Postgres (see `docker-compose.yml` in this dir).

Run:
    EMATIX_FLOW_DSN=postgres://postgres:postgres@localhost/postgres \\
        python examples/03_merge.py
"""

from typing import Annotated

from ematix_flow import ematix, pk
from ematix_flow.connections import PostgresConnection, register_connection
from ematix_flow.normalize import lower, trim
from ematix_flow.types import BigInt, String, Text

# Connection registration — see `01_append.py` for the rationale.
register_connection(
    PostgresConnection(
        name="warehouse",
        url="${EMATIX_FLOW_DSN}",
    )
)


@ematix.table(schema="analytics")
class Customer:
    customer_id: Annotated[BigInt, pk()]
    email: Annotated[String[256] | None, lower(), trim()]
    name: Text | None


@ematix.pipeline(
    target=Customer,
    target_connection="warehouse",
    schedule=None,
    mode="merge",
    compare_columns=["email", "name"],
)
def sync_customers(conn):
    # `compare_columns` controls which columns trigger an UPDATE
    # vs leave the row unchanged. Useful when audit columns
    # (`created_at`, `updated_at`) shouldn't cause merge churn.
    return """
        SELECT 1::bigint AS customer_id, '  Alice@Example.com ' AS email, 'Alice' AS name
        UNION ALL
        SELECT 2::bigint AS customer_id, 'bob@example.com'      AS email, 'Bob'   AS name
    """


if __name__ == "__main__":
    # First run: both rows insert.
    m = sync_customers.run()
    print(f"Run 1: inserted {m.rows_inserted}, updated {m.rows_updated}")

    # Second run with the same source: rows match, no churn.
    m = sync_customers.run()
    print(
        f"Run 2: inserted {m.rows_inserted}, updated {m.rows_updated}, "
        f"unchanged {m.rows_unchanged}"
    )
