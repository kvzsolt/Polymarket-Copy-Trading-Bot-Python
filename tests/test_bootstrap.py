"""Phase 5 — composition root tests.

Verifies that ``bootstrap.build_app`` wires a complete, typed graph from an
injected env mapping and that successive calls yield independent graphs.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from bootstrap import Application, build_app
from config.app_config import AppConfig
from domain.ports.repositories import AbstractLeaderRepository
from orchestrators.leader_registry import LeaderRegistry
from services.healthServer import ReadinessTracker
from services.traderDiscoveryService import TraderDiscoveryService
from storage.sqlite.leaders_repo import LeadersRepo


_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40


def _valid_env(db_path: str, **overrides: str) -> dict[str, str]:
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "USER_ADDRESS": _VALID_USER,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "BOT_STATE_DB": db_path,
    }
    env.update(overrides)
    return env


@pytest.fixture
def env(tmp_path: Path) -> dict[str, str]:
    return _valid_env(str(tmp_path / "bot_state.db"))


class TestBuildApp:
    def test_returns_application(self, env: dict[str, str]) -> None:
        app = build_app(env)
        assert isinstance(app, Application)

    def test_config_loaded(self, env: dict[str, str]) -> None:
        app = build_app(env)
        assert isinstance(app.config, AppConfig)
        assert app.config.wallet.funder_address == _VALID_FUNDER

    def test_readiness_tracker_is_fresh(self, env: dict[str, str]) -> None:
        app = build_app(env)
        assert isinstance(app.readiness, ReadinessTracker)
        snap = app.readiness.snapshot()
        # db_ok should reflect config.database.enabled (default True).
        assert snap["db_ok"] is True
        assert snap["api_ok"] is False

    def test_db_path_matches_config(self, env: dict[str, str]) -> None:
        app = build_app(env)
        assert app.db_path == env["BOT_STATE_DB"]
        assert Path(app.db_path).exists()

    def test_shared_repos_satisfy_ports(self, env: dict[str, str]) -> None:
        app = build_app(env)
        assert isinstance(app.leaders_repo, LeadersRepo)
        assert isinstance(app.leaders_repo, AbstractLeaderRepository)

    def test_registry_and_discovery_wired(self, env: dict[str, str]) -> None:
        app = build_app(env)
        assert isinstance(app.registry, LeaderRegistry)
        assert isinstance(app.discovery_service, TraderDiscoveryService)
        # Registry receives the shared leaders_repo (no duplicate instances).
        assert app.registry.leaders_repo is app.leaders_repo

    def test_two_calls_yield_independent_graphs(self, tmp_path: Path) -> None:
        env1 = _valid_env(str(tmp_path / "a.db"))
        env2 = _valid_env(str(tmp_path / "b.db"))
        app1 = build_app(env1)
        app2 = build_app(env2)

        assert app1 is not app2
        assert app1.config is not app2.config
        assert app1.readiness is not app2.readiness
        assert app1.leaders_repo is not app2.leaders_repo
        assert app1.registry is not app2.registry
        assert app1.discovery_service is not app2.discovery_service

        # Mutating one tracker should not touch the other.
        app1.readiness.update(api_ok=True)
        assert app2.readiness.snapshot()["api_ok"] is False

    def test_db_disabled_skips_schema_init(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "volatile.db")
        env = _valid_env(db_path, DB_ENABLED="false")
        app = build_app(env)
        # With DB disabled we still record the configured path but do not
        # apply migrations on disk.
        assert app.db_path == db_path
        assert app.readiness.snapshot()["db_ok"] is False
        assert not Path(db_path).exists()

    def test_invalid_env_surfaces_validation_error(self, tmp_path: Path) -> None:
        from config.app_config import EnvValidationError

        env = _valid_env(str(tmp_path / "x.db"))
        env.pop("PRIVATE_KEY")
        with pytest.raises(EnvValidationError):
            build_app(env)
