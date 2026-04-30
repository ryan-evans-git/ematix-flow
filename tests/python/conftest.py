"""Shared pytest fixtures.

`pg_url` provides a connection URL to a throwaway Postgres container.
The fixture is session-scoped so the container is reused across tests
in a single run; data isolation is the test's responsibility (drop
tables in test setup or use unique table names).
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
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
