"""Truncate-replace strategy: full snapshot replacement on each run.

Useful for small dimension tables where the upstream produces a
complete snapshot each refresh and incremental merge isn't worth
the complexity.

Requires a local Postgres (see `docker-compose.yml` in this dir).

Run:
    EMATIX_FLOW_DSN=postgres://postgres:postgres@localhost/postgres \\
        python examples/02_truncate.py
"""

from typing import Annotated

from ematix_flow import ematix, pk
from ematix_flow.connections import PostgresConnection, register_connection
from ematix_flow.types import BigInt, String, Text

# Same connection-registration pattern as `01_append.py` — see that
# example for the rationale (named handle, lazy env interpolation,
# safe-to-log repr). `docs/USER_GUIDE.md` "Connections" has the full
# surface (env-driven defaults, the `@ematix.connection` declarative
# form, multi-warehouse pipelines, etc.).
register_connection(
    PostgresConnection(
        name="warehouse",
        url="${EMATIX_FLOW_DSN}",
    )
)


@ematix.table(schema="analytics")
class Country:
    country_id: Annotated[BigInt, pk()]
    code: String[2]
    name: Text


@ematix.pipeline(
    target=Country,
    target_connection="warehouse",
    schedule=None,
    mode="truncate",
)
def refresh_countries(conn):
    return """
        SELECT 1::bigint AS country_id, 'US' AS code, 'United States'  AS name
        UNION ALL
        SELECT 2::bigint AS country_id, 'GB' AS code, 'United Kingdom' AS name
        UNION ALL
        SELECT 3::bigint AS country_id, 'JP' AS code, 'Japan'          AS name
    """


if __name__ == "__main__":
    metrics = refresh_countries.run()
    print(f"Truncate-replaced {metrics.rows_inserted} rows.")
