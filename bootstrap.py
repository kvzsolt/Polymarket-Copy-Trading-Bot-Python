"""Composition root (Phase 5).

Wires the top-level shared graph — config, readiness tracker, DB, shared
repositories and the leader registry — into a single :class:`Application`
container. Per-user state (CLOB client + execution service + repos keyed
on ``user_id``) still requires an async startup path with real network
resources, so it is assembled in ``app.py::main`` from the shared pieces
returned here.

This module has **no import-time side effects**. ``build_app`` is the sole
factory; it reads the supplied env mapping (or ``os.environ`` when ``None``)
and returns a fresh graph each call. Two invocations produce independent
objects with no shared mutable state.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from config.app_config import AppConfig, load_config
from orchestrators.leader_registry import LeaderRegistry
from services.healthServer import ReadinessTracker
from services.traderDiscoveryService import TraderDiscoveryService
from storage.postgres.connection import init_pool
from storage.postgres.leaders_repo import LeadersRepo
from storage.postgres.migrations import apply_migrations as apply_pg_migrations
from utils.logger import get_logger

__all__ = ["Application", "build_app"]


@dataclass
class Application:
    """Shared top-level wiring produced by :func:`build_app`."""

    config: AppConfig
    readiness: ReadinessTracker
    db_path: str
    leaders_repo: LeadersRepo
    discovery_service: TraderDiscoveryService
    registry: LeaderRegistry


async def build_app(env: Mapping[str, str] | None = None) -> Application:
    """Construct the shared application graph.

    Parameters
    ----------
    env:
        Environment mapping. Defaults to ``os.environ`` when ``None`` — the
        same default used by :func:`config.app_config.load_config`.

    Returns
    -------
    Application
        A freshly wired, fully typed graph. Successive calls yield
        independent instances (no module-level caching).

    Notes
    -----
    PG-A5: builds an async ``LeadersRepo`` against the singleton asyncpg
    pool. The pool is brought online here (``init_pool`` is idempotent —
    a second caller returns the same pool) and the canonical schema is
    applied before any repo touches it. Sqlite legacy initialisation
    continues to run via ``config.db.connect_db()`` at the runtime
    entrypoint until the remaining sqlite callers migrate.
    """
    config = load_config(env)

    pool = await init_pool(
        config.database.dsn,
        min_size=config.database.pool_min_size,
        max_size=config.database.pool_max_size,
        command_timeout_seconds=config.database.command_timeout_seconds,
    )
    async with pool.acquire() as conn:
        await apply_pg_migrations(conn)

    readiness = ReadinessTracker()
    readiness.update(db_ok=config.database.enabled)

    leaders_repo = LeadersRepo(pool)
    discovery_service = TraderDiscoveryService()
    registry = LeaderRegistry(
        discovery_service=discovery_service,
        leaders_repo=leaders_repo,
        readiness_tracker=readiness,
        config=config,
        logger_instance=get_logger("LeaderRegistry"),
    )

    return Application(
        config=config,
        readiness=readiness,
        db_path=config.database.db_path,
        leaders_repo=leaders_repo,
        discovery_service=discovery_service,
        registry=registry,
    )
