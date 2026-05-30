from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


def _load_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_discovery_only.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_discovery_only_test_module",
        path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _stdout_payload(text: str) -> dict:
    start = text.find("{")
    end = text.rfind("}")
    assert start != -1 and end != -1
    return json.loads(text[start : end + 1])


def _evaluation(**overrides):
    data = {
        "wallet": "0x" + "1" * 40,
        "status": "rejected",
        "eligible_live": False,
        "eligible_shadow": False,
        "final_score": 0.0,
        "falcon_quality_score": 0.0,
        "copyability_score": 0.0,
        "recent_state_score": 0.0,
        "market_fit_score": 0.0,
        "copyable_signal_ratio": 0.0,
        "copyable_notional_ratio": 0.0,
        "meaningful_signals": 0,
        "copyable_signals": 0,
        "sports_esports_ratio": 0.0,
        "fast_crypto_signal_ratio": 0.0,
        "fast_crypto_notional_ratio": 0.0,
        "pnl_7d": "0",
        "pnl_3d": "0",
        "rejection_reason": "insufficient_history",
        "updated_at": 0.0,
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def _app(evaluations):
    return SimpleNamespace(
        config=SimpleNamespace(
            discovery=SimpleNamespace(
                falcon_api_token="token",
                polymarket_history_lookback_days=30,
                falcon_page_size=100,
                falcon_max_pages=5,
            )
        ),
        discovery_service=SimpleNamespace(
            discover_candidates=AsyncMock(return_value=[SimpleNamespace(wallet="0x1")])
        ),
        discovery_repo=SimpleNamespace(
            list_evaluations=AsyncMock(return_value=evaluations)
        ),
    )


def _patch_runner(monkeypatch, fake_app):
    monkeypatch.setattr("config.runtime_env.load_runtime_env", lambda **_kwargs: None)
    monkeypatch.setattr("config.env.rehydrate_env", lambda **_kwargs: None)
    monkeypatch.setattr("config.env.validate_runtime_env", lambda: None)
    monkeypatch.setattr("config.app_config.load_config", lambda: fake_app.config)
    monkeypatch.setattr("utils.logger.configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(
        "bootstrap.build_app",
        AsyncMock(return_value=fake_app),
    )
    shutdown_pool = AsyncMock()
    monkeypatch.setattr("storage.postgres.connection.shutdown_pool", shutdown_pool)
    return shutdown_pool


@pytest.mark.asyncio
async def test_discovery_only_list_only_does_not_call_discover(monkeypatch, capsys):
    module = _load_module()
    fake_app = _app([_evaluation(rejection_reason=None)])
    _patch_runner(monkeypatch, fake_app)

    result = await module._amain(["--list-only", "--limit", "5"])

    assert result == 0
    fake_app.discovery_service.discover_candidates.assert_not_called()
    fake_app.discovery_repo.list_evaluations.assert_awaited_once_with(limit=5)
    payload = _stdout_payload(capsys.readouterr().out)
    assert payload["discovery_ran"] is False
    assert payload["rejection_counts"] == {"none": 1}


@pytest.mark.asyncio
async def test_discovery_only_once_calls_discover(monkeypatch, capsys):
    module = _load_module()
    fake_app = _app([_evaluation(eligible_live=True, rejection_reason=None)])
    _patch_runner(monkeypatch, fake_app)

    result = await module._amain(["--once"])

    assert result == 0
    fake_app.discovery_service.discover_candidates.assert_awaited_once()
    payload = _stdout_payload(capsys.readouterr().out)
    assert payload["discovery_ran"] is True
    assert payload["profiles_returned"] == 1
    assert payload["live_eligible_count"] == 1


@pytest.mark.asyncio
async def test_discovery_only_require_live_candidates_exit_1_when_none(monkeypatch):
    module = _load_module()
    fake_app = _app([_evaluation()])
    _patch_runner(monkeypatch, fake_app)

    result = await module._amain(["--list-only", "--require-live-candidates"])

    assert result == 1


@pytest.mark.asyncio
async def test_discovery_only_does_not_create_clob_client(monkeypatch):
    module = _load_module()
    fake_app = _app([_evaluation(eligible_live=True, rejection_reason=None)])
    _patch_runner(monkeypatch, fake_app)
    create_clob_client = AsyncMock()
    monkeypatch.setattr(
        "utils.createClobClient.create_clob_client",
        create_clob_client,
    )

    result = await module._amain(["--once"])

    assert result == 0
    create_clob_client.assert_not_called()
