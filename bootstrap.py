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
from pathlib import Path

from config.app_config import AppConfig, load_config
from orchestrators.leader_registry import LeaderRegistry
from services.healthServer import ReadinessTracker
from services.traderDiscoveryService import TraderDiscoveryService
from storage.sqlite.connection import create_connection
from storage.sqlite.leaders_repo import LeadersRepo
from storage.sqlite.migrations import apply_migrations
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


def _init_database(db_path: str) -> None:
    """Apply migrations against ``db_path`` exactly once per call."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.commit()


def build_app(env: Mapping[str, str] | None = None) -> Application:
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
    """
    config = load_config(env)

    db_path = config.database.db_path
    if config.database.enabled:
        _init_database(db_path)

    readiness = ReadinessTracker()
    readiness.update(db_ok=config.database.enabled)

    leaders_repo = LeadersRepo(db_path)
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
        db_path=db_path,
        leaders_repo=leaders_repo,
        discovery_service=discovery_service,
        registry=registry,
    )
