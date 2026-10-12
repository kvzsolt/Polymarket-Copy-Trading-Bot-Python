"""Phase 5 — composition root tests (PG-A5 async version).

Verifies that ``bootstrap.build_app`` wires a complete, typed graph from
an injected env mapping and that successive calls yield independent
graphs. Backed by a live ``postgres:16-alpine`` testcontainer because
``build_app`` brings the asyncpg pool online + applies migrations.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace

import pytest

from bootstrap import Application, build_app
from config.app_config import AppConfig
from domain.ports.repositories import AbstractLeaderRepository
from orchestrators.leader_registry import LeaderRegistry
from services.discovery.falcon_discovery_service import (
    FalconDiscoveryService,
)
from services.healthServer import ReadinessTracker
from storage.postgres.discovery_repo import DiscoveryRepo
from storage.postgres.leaders_repo import LeadersRepo

pytestmark = pytest.mark.asyncio


_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40


@contextlib.asynccontextmanager
async def _pg_container():
    """Spin a Postgres testcontainer + yield the DSN. Resets the
    ``storage.postgres.connection`` singleton so ``build_app`` brings
    its own pool online cleanly."""
    try:
        from testcontainers.postgres import PostgresContainer
        from storage.postgres import connection as conn_mod
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        conn_mod._reset_for_tests()
        try:
            yield dsn
        finally:
            try:
                await conn_mod.shutdown_pool()
            except Exception:
                pass
            conn_mod._reset_for_tests()
    finally:
        ctr.stop()


def _valid_env(dsn: str) -> dict:
    return {
        "database": {"dsn": dsn},
        "wallet": {
            "private_key": _VALID_PRIVATE_KEY,
            "funder_address": _VALID_FUNDER,
            "polygon_rpc_url": "https://polygon-rpc.example.com",
        },
        "network": {"rpc_url": "https://polygon-rpc.example.com"},
    }


@pytest.fixture(autouse=True)
def _supported_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "config.app_config.sys.version_info",
        SimpleNamespace(major=3, minor=14),
    )


class TestBuildApp:
    async def test_returns_application(self) -> None:
        async with _pg_container() as dsn:
            app = await build_app(_valid_env(dsn))
            assert isinstance(app, Application)

    async def test_config_loaded(self) -> None:
        async with _pg_container() as dsn:
            app = await build_app(_valid_env(dsn))
            assert isinstance(app.config, AppConfig)
            assert app.config.wallet.funder_address == _VALID_FUNDER

    async def test_readiness_tracker_is_fresh(self) -> None:
        async with _pg_container() as dsn:
            app = await build_app(_valid_env(dsn))
            assert isinstance(app.readiness, ReadinessTracker)
            snap = app.readiness.snapshot()
            assert snap["db_ok"] is True
            assert snap["api_ok"] is False

    async def test_shared_repos_satisfy_ports(self) -> None:
        async with _pg_container() as dsn:
            app = await build_app(_valid_env(dsn))
            assert isinstance(app.leaders_repo, LeadersRepo)
            assert isinstance(app.leaders_repo, AbstractLeaderRepository)
            assert isinstance(app.discovery_repo, DiscoveryRepo)

    async def test_registry_and_discovery_wired(self) -> None:
        async with _pg_container() as dsn:
            app = await build_app(_valid_env(dsn))
            assert isinstance(app.registry, LeaderRegistry)
            assert isinstance(app.discovery_service, FalconDiscoveryService)
            assert app.registry.leaders_repo is app.leaders_repo
            assert app.registry.config is app.config
            assert app.registry.readiness is app.readiness

    async def test_two_calls_yield_independent_graphs(self) -> None:
        """Two ``build_app`` calls against the same testcontainer DSN
        share the singleton asyncpg pool (idempotent ``init_pool``) but
        yield independent ``Application`` graphs."""
        async with _pg_container() as dsn:
            env = _valid_env(dsn)
            app1 = await build_app(env)
            app2 = await build_app(env)

            assert app1 is not app2
            assert app1.config is not app2.config
            assert app1.readiness is not app2.readiness
            assert app1.registry is not app2.registry
            assert app1.discovery_service is not app2.discovery_service

            # Mutating one tracker should not touch the other.
            app1.readiness.update(api_ok=True)
            assert app2.readiness.snapshot()["api_ok"] is False

    async def test_invalid_env_surfaces_validation_error(self) -> None:
        from config.app_config import EnvValidationError

        async with _pg_container() as dsn:
            env = _valid_env(dsn)
            env["wallet"]["funder_address"] = "invalid"
            with pytest.raises(EnvValidationError):
                await build_app(env)
