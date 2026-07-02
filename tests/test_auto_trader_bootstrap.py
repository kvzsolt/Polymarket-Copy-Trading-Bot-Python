from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrators.auto_trader_bootstrap import resolve_startup_leaders
from services.discovery.auto_trader_selector import AutoTraderSelection
from services.metrics import metrics

pytestmark = pytest.mark.asyncio


def _config(*, enabled: bool = True, max_count: int = 2):
    return SimpleNamespace(
        discovery=SimpleNamespace(
            allow_auto_trader_detection=enabled,
            max_auto_traders_count=max_count,
            discovery_auto_write_leaders=False,
            discovery_dynamic_monitoring_enabled=False,
        )
    )


@pytest.fixture(autouse=True)
def _clear_metrics():
    metrics.clear()


async def test_auto_bootstrap_manual_leaders_win():
    selector = SimpleNamespace(select_from_db=AsyncMock())
    discovery = SimpleNamespace(discover_candidates=AsyncMock())

    result = await resolve_startup_leaders(
        configured_leaders=["0xAAA"],
        config=_config(enabled=True),
        selector=selector,
        discovery_service=discovery,
    )

    assert result.active_leaders == ["0xaaa"]
    assert result.discovery_ran is False
    assert result.mode == "configured"
    selector.select_from_db.assert_not_called()
    discovery.discover_candidates.assert_not_called()


async def test_auto_bootstrap_disabled_returns_no_live_leaders():
    selector = SimpleNamespace(select_from_db=AsyncMock())
    discovery = SimpleNamespace(discover_candidates=AsyncMock())

    result = await resolve_startup_leaders(
        configured_leaders=[],
        config=_config(enabled=False),
        selector=selector,
        discovery_service=discovery,
    )

    assert result.active_leaders == []
    assert result.discovery_ran is False
    assert result.reason == "auto_detection_disabled"
    selector.select_from_db.assert_not_called()
    discovery.discover_candidates.assert_not_called()


async def test_auto_bootstrap_selects_existing_db_candidates():
    selector = SimpleNamespace(
        select_from_db=AsyncMock(
            return_value=AutoTraderSelection(
                wallets=["0x1", "0x2"],
                source="db",
                reason="eligible_candidates_found",
                db_candidate_count=2,
            )
        )
    )
    discovery = SimpleNamespace(discover_candidates=AsyncMock())

    result = await resolve_startup_leaders(
        configured_leaders=[],
        config=_config(enabled=True),
        selector=selector,
        discovery_service=discovery,
    )

    assert result.active_leaders == ["0x1", "0x2"]
    assert result.discovery_ran is False
    assert result.mode == "auto_detected"
    discovery.discover_candidates.assert_not_called()


async def test_auto_bootstrap_empty_db_runs_discovery_once():
    selector = SimpleNamespace(
        select_from_db=AsyncMock(
            side_effect=[
                AutoTraderSelection([], "db", "no_eligible_db_candidates", 0),
                AutoTraderSelection(["0x1"], "db", "eligible_candidates_found", 1),
            ]
        )
    )
    discovery = SimpleNamespace(discover_candidates=AsyncMock())

    result = await resolve_startup_leaders(
        configured_leaders=[],
        config=_config(enabled=True),
        selector=selector,
        discovery_service=discovery,
    )

    discovery.discover_candidates.assert_awaited_once()
    assert selector.select_from_db.await_count == 2
    assert result.active_leaders == ["0x1"]
    assert result.discovery_ran is True
    assert result.mode == "auto_detected_bootstrapped"


async def test_auto_bootstrap_no_candidates_after_discovery_blocks_runtime():
    selector = SimpleNamespace(
        select_from_db=AsyncMock(
            return_value=AutoTraderSelection(
                wallets=[],
                source="db",
                reason="no_eligible_db_candidates",
                db_candidate_count=0,
            )
        )
    )
    discovery = SimpleNamespace(discover_candidates=AsyncMock())

    result = await resolve_startup_leaders(
        configured_leaders=[],
        config=_config(enabled=True),
        selector=selector,
        discovery_service=discovery,
    )

    assert selector.select_from_db.await_count == 2
    discovery.discover_candidates.assert_awaited_once()
    assert result.active_leaders == []
    assert result.mode == "no_live_candidates"
    assert result.reason == "no_candidates_after_discovery_bootstrap"


async def test_auto_bootstrap_does_not_write_runtime_env(tmp_path):
    env_file = tmp_path / "runtime.local.env"
    env_file.write_text(
        "LEADER_WALLETS=\nALLOW_AUTO_TRADER_DETECTION=true\n",
        encoding="utf-8",
    )
    before = env_file.read_text(encoding="utf-8")
    selector = SimpleNamespace(
        select_from_db=AsyncMock(
            return_value=AutoTraderSelection(
                wallets=[],
                source="db",
                reason="no_eligible_db_candidates",
                db_candidate_count=0,
            )
        )
    )
    discovery = SimpleNamespace(discover_candidates=AsyncMock())

    await resolve_startup_leaders(
        configured_leaders=[],
        config=_config(enabled=True),
        selector=selector,
        discovery_service=discovery,
    )

    after = env_file.read_text(encoding="utf-8")
    assert after == before


async def test_no_clob_init_when_auto_bootstrap_finds_no_candidates(monkeypatch):
    from config.app_config import load_config
    from services.healthServer import ReadinessTracker
    import app as app_module

    monkeypatch.setattr(
        "config.app_config.sys.version_info",
        SimpleNamespace(major=3, minor=12),
    )
    cfg = load_config(
        {
            "PRIVATE_KEY": "0x" + "a" * 64,
            "FUNDER_ADDRESS": "0x" + "b" * 40,
            "RPC_URL": "https://polygon-rpc.example.com",
            "CHAIN_ID": "137",
            "LEADER_WALLETS": "",
            "USER_ADDRESS": "",
            "PAPER_TRADE_ONLY": "true",
            "ENFORCE_GEOBLOCK": "false",
            "ALLOW_AUTO_TRADER_DETECTION": "true",
            "MAX_AUTO_TRADERS_COUNT": "2",
        }
    )
    fake_application = SimpleNamespace(
        config=cfg,
        readiness=ReadinessTracker(),
        discovery_repo=SimpleNamespace(
            list_evaluations=AsyncMock(return_value=[]),
        ),
        discovery_service=SimpleNamespace(
            discover_candidates=AsyncMock(return_value=[]),
        ),
        leaders_repo=SimpleNamespace(),
        registry=SimpleNamespace(),
        trust_store=SimpleNamespace(),
    )
    create_clob = AsyncMock()
    load_users = MagicMock()

    monkeypatch.setattr(app_module, "validate_runtime_env", lambda: None)
    monkeypatch.setattr(app_module, "load_config", lambda: cfg)
    monkeypatch.setattr(app_module, "build_app", AsyncMock(return_value=fake_application))
    monkeypatch.setattr(app_module, "create_clob_client", create_clob)
    monkeypatch.setattr(app_module, "load_runtime_users", load_users)

    await app_module.main()

    assert fake_application.discovery_service.discover_candidates.await_count == 1
    create_clob.assert_not_called()
    load_users.assert_not_called()
