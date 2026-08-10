from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


def _module():
    path = Path(__file__).resolve().parents[1] / "scripts/run_discovery_only.py"
    spec = importlib.util.spec_from_file_location("discovery_runner", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _application() -> SimpleNamespace:
    roster = [
        SimpleNamespace(
            wallet="0x" + "1" * 40,
            tier="active",
            score=0.8,
            cycles_in_tier=3,
            promote_streak=0,
            demote_streak=0,
        )
    ]
    return SimpleNamespace(
        discovery_service=SimpleNamespace(
            run=AsyncMock(return_value=[]),
            config_hash="hash",
        ),
        registry=SimpleNamespace(reconcile_candidates=AsyncMock()),
        discovery_repo=SimpleNamespace(list_snapshots=AsyncMock(return_value=[])),
        leaders_repo=SimpleNamespace(
            list_roster=AsyncMock(return_value=roster),
            list_active=AsyncMock(return_value=[roster[0].wallet]),
        ),
        aclose=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_list_only_uses_persisted_roster(monkeypatch, capsys) -> None:
    application = _application()
    monkeypatch.setattr("bootstrap.build_app", AsyncMock(return_value=application))
    monkeypatch.setattr("storage.postgres.connection.shutdown_pool", AsyncMock())
    result = await _module()._amain(["--list-only"])
    assert result == 0
    application.discovery_service.run.assert_not_awaited()
    payload = json.loads(capsys.readouterr().out)
    assert payload["active_wallets"] == ["0x" + "1" * 40]


@pytest.mark.asyncio
async def test_run_calls_funnel_without_constructing_trading_clients(
    monkeypatch,
) -> None:
    application = _application()
    monkeypatch.setattr("bootstrap.build_app", AsyncMock(return_value=application))
    monkeypatch.setattr("storage.postgres.connection.shutdown_pool", AsyncMock())
    result = await _module()._amain([])
    assert result == 0
    application.discovery_service.run.assert_awaited_once()
    application.registry.reconcile_candidates.assert_awaited_once_with([])
    application.aclose.assert_awaited_once()
