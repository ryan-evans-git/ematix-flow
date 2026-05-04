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
from ematix_flow.types import BigInt, String, Text


@ematix.table(schema="analytics")
class Country:
    country_id: Annotated[BigInt, pk()]
    code: String[2]
    name: Text


@ematix.pipeline(target=Country, schedule=None, mode="truncate")
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
