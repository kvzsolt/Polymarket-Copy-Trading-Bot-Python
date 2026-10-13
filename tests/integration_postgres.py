"""ONE shared Postgres container for the whole integration tier (Task 25).

Why this module exists
----------------------
The integration tier previously instantiated a disposable
``testcontainers.postgres.PostgresContainer`` per test/file: every test
booted and tore down its own container. That is Docker container churn —
it hogs the machine's network and disk for no benefit. This module
replaces it with a single, long-lived, named container that is started
once and NEVER removed and NEVER re-pulled:

* container name ``pmcopy-integration-pg``, bound to ``127.0.0.1:5433``
  (override with ``PMCOPY_INTEGRATION_PG_PORT``);
* ``docker image inspect`` is checked before anything — if the image is
  absent locally the fixture refuses to pull (exception: ephemeral CI
  runners, detected via the standard ``CI=true`` environment variable,
  where a single pull per job is expected);
* an existing stopped container is ``docker start``-ed; an absent one is
  created exactly once with ``docker run -d`` (no ``--rm``);
* per-TEST isolation does not recreate containers: every
  ``PostgresContainer`` facade below is one fresh DATABASE inside the
  shared server (``CREATE DATABASE`` on ``start()``, ``DROP DATABASE ...
  WITH (FORCE)`` on ``stop()``) — all local SQL, no network churn.

The ``PostgresContainer`` class is a drop-in testcontainers-compatible
facade: tests keep calling ``.start()`` / ``.stop()`` /
``.get_connection_url(driver=...)`` (including the sync context-manager
form), so converting a historical test file is a one-line import change.
Databases leaked by a crashed process are garbage-collected best-effort
on the next run (names embed the creating PID; dead-PID leftovers are
dropped).
"""

from __future__ import annotations

import itertools
import os
import subprocess
import time
from typing import Any

__all__ = [
    "CONTAINER_NAME",
    "HOST",
    "IMAGE",
    "PORT",
    "IntegrationPostgresUnavailable",
    "PostgresContainer",
    "shared_container_id",
    "shared_server",
]

CONTAINER_NAME = "pmcopy-integration-pg"
IMAGE = "postgres:16-alpine"
HOST = "127.0.0.1"
PORT = int(os.environ.get("PMCOPY_INTEGRATION_PG_PORT", "5433"))
USER = "postgres"
# Obviously-fake placeholder credential for a localhost-bound, non-secret
# test container. Not a secret; never used outside 127.0.0.1.
PASSWORD = "pmcopy-integration-local-only-not-a-secret"
MAINTENANCE_DB = "postgres"

_DB_COUNTER = itertools.count(1)


class IntegrationPostgresUnavailable(RuntimeError):
    """Raised when the shared container cannot be provided.

    Historical tests catch ``Exception`` around container startup and
    ``pytest.skip`` — this subclass flows through the same path, keeping
    "Docker unavailable → skip" behavior intact.
    """


def _docker(args: list[str], *, timeout: float = 120.0) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["docker", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:  # docker CLI not installed
        raise IntegrationPostgresUnavailable("docker CLI not available") from exc
    except subprocess.TimeoutExpired as exc:
        raise IntegrationPostgresUnavailable(f"docker {args[:2]} timed out") from exc


class _SharedServer:
    """Process-wide handle to the single shared container."""

    def __init__(self) -> None:
        self.container_id: str | None = None

    # -- container lifecycle ------------------------------------------------
    def ensure(self) -> str:
        if self.container_id is not None:
            return self.container_id
        self._verify_image()
        existing = _docker(
            ["container", "inspect", "--format", "{{.Id}} {{.State.Running}}", CONTAINER_NAME]
        )
        if existing.returncode == 0:
            container_id, running = existing.stdout.strip().rsplit(" ", 1)
            if running != "true":
                started = _docker(["start", CONTAINER_NAME])
                if started.returncode != 0:
                    raise IntegrationPostgresUnavailable(
                        f"docker start {CONTAINER_NAME} failed: {started.stderr.strip()}"
                    )
            self.container_id = container_id
        else:
            # Absent: create exactly once. No --rm: the container persists.
            created = _docker(
                [
                    "run",
                    "-d",
                    "--name",
                    CONTAINER_NAME,
                    "-p",
                    f"{HOST}:{PORT}:5432",
                    "-e",
                    f"POSTGRES_PASSWORD={PASSWORD}",
                    IMAGE,
                ]
            )
            if created.returncode != 0:
                raise IntegrationPostgresUnavailable(
                    f"docker run {CONTAINER_NAME} failed (is host port {PORT} taken?): "
                    f"{created.stderr.strip()}"
                )
            self.container_id = created.stdout.strip()
        self._wait_ready()
        self._gc_stale_databases()
        return self.container_id

    def _verify_image(self) -> None:
        inspect = _docker(["image", "inspect", IMAGE])
        if inspect.returncode == 0:
            return
        if os.environ.get("CI") == "true":
            # Ephemeral CI runner: a single pull per job is the documented
            # exception (the image is not part of the runner image).
            pulled = _docker(["pull", IMAGE], timeout=600.0)
            if pulled.returncode != 0:
                raise IntegrationPostgresUnavailable(
                    f"{IMAGE} absent and CI pull failed: {pulled.stderr.strip()}"
                )
            return
        raise IntegrationPostgresUnavailable(
            f"{IMAGE} is not present locally and this fixture never pulls "
            "(run `docker pull postgres:16-alpine` once, then re-run)"
        )

    def _wait_ready(self) -> None:
        deadline = time.monotonic() + 40.0
        while time.monotonic() < deadline:
            probe = _docker(
                ["exec", CONTAINER_NAME, "pg_isready", "-U", USER, "-d", MAINTENANCE_DB],
                timeout=15.0,
            )
            if probe.returncode == 0:
                return
            time.sleep(0.25)
        raise IntegrationPostgresUnavailable(
            f"shared container {CONTAINER_NAME} did not become ready within 40s"
        )

    # -- local SQL helpers (no network churn) --------------------------------
    def psql(self, sql: str) -> str:
        """Run one SQL statement inside the shared container via psql."""
        result = _docker(
            [
                "exec",
                CONTAINER_NAME,
                "psql",
                "-U",
                USER,
                "-d",
                MAINTENANCE_DB,
                "-v",
                "ON_ERROR_STOP=1",
                "-Atc",
                sql,
            ]
        )
        if result.returncode != 0:
            raise IntegrationPostgresUnavailable(
                f"psql failed ({sql.split()[0]}): {result.stderr.strip()}"
            )
        return result.stdout

    def _gc_stale_databases(self) -> None:
        """Best-effort cleanup of databases leaked by crashed processes.

        Database names embed the PID of the creating process. Leftovers
        whose PID no longer exists (Linux /proc check) are dropped so the
        single container never accumulates garbage. Never fails the run.
        """
        try:
            listing = self.psql(
                "SELECT datname FROM pg_database WHERE datname LIKE 'pmcopy_it_%'"
            )
            for name in listing.split():
                embedded_pid = name.rsplit("_", 2)[-2]
                if embedded_pid.isdigit() and not os.path.exists(f"/proc/{embedded_pid}"):
                    self.psql(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        except Exception:  # noqa: BLE001 - GC is strictly best-effort
            pass


_SERVER: _SharedServer | None = None


def shared_server() -> _SharedServer:
    global _SERVER
    if _SERVER is None:
        _SERVER = _SharedServer()
        _SERVER.ensure()
    return _SERVER


def shared_container_id() -> str:
    """Container id of the one shared container (starts it if needed)."""
    return shared_server().ensure()


def _new_database_name() -> str:
    return f"pmcopy_it_{os.getpid()}_{next(_DB_COUNTER)}"


class PostgresContainer:
    """testcontainers-compatible facade over ONE fresh database.

    Not a container: ``start()`` creates a database inside the single
    shared server; ``stop()`` drops it (``WITH (FORCE)``). The container
    itself is never created, removed, or re-pulled by this class.
    """

    def __init__(self, image: str = IMAGE, **_kwargs: Any) -> None:
        if image != IMAGE:
            raise IntegrationPostgresUnavailable(
                f"shared fixture only provides {IMAGE} (requested {image!r})"
            )
        self._dbname: str | None = None

    # -- lifecycle -----------------------------------------------------------
    def start(self) -> None:
        if self._dbname is not None:
            raise RuntimeError("facade already started")
        shared_server().ensure()
        self._dbname = _new_database_name()
        shared_server().psql(f'CREATE DATABASE "{self._dbname}"')

    def stop(self) -> None:
        if self._dbname is None:
            return
        try:
            shared_server().psql(
                f'DROP DATABASE IF EXISTS "{self._dbname}" WITH (FORCE)'
            )
        finally:
            self._dbname = None

    def __enter__(self) -> "PostgresContainer":
        self.start()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.stop()

    # -- DSN -----------------------------------------------------------------
    def get_connection_url(
        self,
        host: str | None = None,
        port: int | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        driver: str | None = None,
    ) -> str:
        """testcontainers-shaped URL: ``postgresql+<driver>://...``.

        The default driver segment is ``psycopg2`` (exactly what
        testcontainers emits) so the historical
        ``.replace("postgresql+psycopg2://", "postgresql://")`` and
        ``driver="asyncpg"`` call sites keep working unchanged.
        """
        scheme = f"postgresql+{driver or 'psycopg2'}"
        return (
            f"{scheme}://{user or USER}:{password or PASSWORD}"
            f"@{host or HOST}:{port or PORT}/{dbname or self._dbname}"
        )
