"""Shared pytest fixtures.

`pg_url` provides a connection URL to a throwaway Postgres container.
`pg_url_secondary` provides a second, independent container — used by
cross-DB tests that want source and target on different databases. Both
fixtures are session-scoped so containers are reused across the run;
data isolation is each test's responsibility (drop tables in setup or
use unique schema names).
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest


def _start_postgres() -> Iterator[str]:
    pytest.importorskip("testcontainers.postgres")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine", driver=None) as container:
        # testcontainers' default `get_connection_url` adds a +psycopg2 driver
        # suffix; we want a plain postgres:// URL for tokio-postgres.
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5432)
        user = container.username
        password = container.password
        dbname = container.dbname
        yield f"postgres://{user}:{password}@{host}:{port}/{dbname}"


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    yield from _start_postgres()


@pytest.fixture(scope="session")
def pg_url_secondary() -> Iterator[str]:
    yield from _start_postgres()
