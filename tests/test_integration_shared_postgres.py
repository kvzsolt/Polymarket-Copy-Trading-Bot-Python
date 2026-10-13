"""Contract test for the ONE shared integration Postgres container.

Task 25 anti-churn fixture: every integration test gets a fresh DATABASE
inside a single long-lived container (``pmcopy-integration-pg``) instead of
a disposable ``testcontainers`` container per test/file. This suite pins
that contract:

1. two ``PostgresContainer`` facades map to the SAME container id (no
   container is created or removed — the docker container-id set is
   byte-identical before, between, and after);
2. each facade is a distinct, isolated database (data written through one
   facade is invisible to the next, and is dropped on stop);
3. the facade URL keeps the testcontainers DSN shape so the historical
   ``.replace("postgresql+asyncpg://", "postgresql://")`` call sites work
   unchanged.
"""

from __future__ import annotations

import subprocess

import pytest

pytestmark = pytest.mark.integration


def _docker_container_ids() -> list[str]:
    out = subprocess.run(
        ["docker", "ps", "-aq"], capture_output=True, text=True, check=True
    )
    return sorted(out.stdout.split())


async def test_shared_facade_reuses_one_container_across_databases() -> None:
    import asyncpg

    from tests.integration_postgres import PostgresContainer, shared_container_id

    ids_before = _docker_container_ids()
    container_id_before = shared_container_id()

    urls = []
    with PostgresContainer("postgres:16-alpine") as first:
        ids_during_first = _docker_container_ids()
        url = first.get_connection_url(driver="asyncpg")
        urls.append(url)
        dsn = url.replace("postgresql+asyncpg://", "postgresql://")
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute("CREATE TABLE churn_probe(marker text)")
            await conn.execute("INSERT INTO churn_probe VALUES ('first')")
        finally:
            await conn.close()

    with PostgresContainer("postgres:16-alpine") as second:
        ids_during_second = _docker_container_ids()
        url = second.get_connection_url(driver="asyncpg")
        urls.append(url)
        conn = await asyncpg.connect(
            url.replace("postgresql+asyncpg://", "postgresql://")
        )
        try:
            # Fresh database: the previous facade's table must not exist.
            row = await conn.fetchval(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_name = 'churn_probe'"
            )
            assert row == 0, "each facade must be a fresh isolated database"
        finally:
            await conn.close()

    ids_after = _docker_container_ids()

    # ONE shared container: the docker container set never changes, and the
    # shared container id is stable across facades/databases. (``docker ps``
    # emits short ids; inspect emits the full id — compare by prefix.)
    assert ids_before == ids_during_first == ids_during_second == ids_after
    assert container_id_before == shared_container_id()
    assert any(
        container_id_before.startswith(short) for short in ids_before
    ), "shared container must be the already-present docker container"

    # Distinct database per facade, both inside the same container.
    assert urls[0] != urls[1]
    for url in urls:
        assert url.startswith("postgresql+asyncpg://postgres:")
        assert "@127.0.0.1:" in url


async def test_facade_drop_database_on_stop() -> None:
    import asyncpg

    from tests.integration_postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as facade:
        dbname = facade.get_connection_url().rsplit("/", 1)[-1]
        conn = await asyncpg.connect(
            facade.get_connection_url(driver="asyncpg").replace(
                "postgresql+asyncpg://", "postgresql://"
            )
        )
        await conn.close()

    from tests.integration_postgres import shared_server

    listing = shared_server().psql(
        "SELECT 1 FROM pg_database WHERE datname = '%s'" % dbname
    )
    assert "1" not in listing.strip(), "database must be dropped on stop()"


def test_psql_default_url_keeps_testcontainers_shape() -> None:
    from tests.integration_postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as facade:
        url = facade.get_connection_url()
        assert url.startswith("postgresql+psycopg2://")
        dsn = url.replace("postgresql+psycopg2://", "postgresql://")
        assert dsn.count("postgresql://") == 1
        assert "@127.0.0.1:" in dsn
