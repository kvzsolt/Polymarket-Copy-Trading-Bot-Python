from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.discovery.copyability_replay import StaticBalanceProvider
from services.discovery.evaluation_context import build_evaluation_context_hash
from services.discovery.auto_trader_selector import AutoTraderSelector
from services.metrics import metrics

pytestmark = pytest.mark.asyncio


def _config(*, max_count: int = 2, reject_stale: bool = False):
    cfg = SimpleNamespace(
        wallet=SimpleNamespace(funder_address="0x" + "f" * 40),
        sizing=SimpleNamespace(
            mode="proportional",
            percentage=2,
            fixed_amount=3,
            buy_min_amount=6,
            buy_max_amount=15,
            trade_multiplier=1,
        ),
        execution=SimpleNamespace(
            max_position_size_usdc=0,
            min_balance_headroom_usdc=0,
        ),
        risk=SimpleNamespace(max_open_positions=40),
        discovery=SimpleNamespace(
            max_auto_traders_count=max_count,
            discovery_min_copyable_grouped_signal_ratio=0.75,
            discovery_min_copyable_notional_ratio=0.60,
            discovery_min_group_notional_usdc=5,
            discovery_min_auto_sports_esports_ratio=0.75,
            discovery_auto_prefer_sports_esports=True,
            discovery_evaluation_ttl_seconds=3600,
            discovery_reject_stale_evaluations=reject_stale,
            discovery_require_7d_non_negative=True,
            discovery_min_7d_pnl_usdc=0.0,
        ),
        leader=SimpleNamespace(auto_discover_min_score=0.45),
    )
    return cfg


def _row(wallet: str, **overrides):
    data = {
        "wallet": wallet,
        "status": "live_eligible",
        "eligible_live": True,
        "rejection_reason": None,
        "final_score": 0.80,
        "copyable_signal_ratio": 0.90,
        "copyable_notional_ratio": 0.85,
        "sports_esports_ratio": 0.90,
        "total_grouped_signals": 10,
        "pnl_7d": Decimal("1.0"),
        "updated_at": 9_999_999_999.0,
        "config_hash": "",
    }
    data.update(overrides)
    return SimpleNamespace(**data)


@pytest.fixture(autouse=True)
def _clear_metrics():
    metrics.clear()


async def test_auto_trader_selector_selects_eligible_live_rows():
    repo = SimpleNamespace(
        list_evaluations=AsyncMock(
            return_value=[
                _row("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                _row("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
            ]
        )
    )

    selection = await AutoTraderSelector(repo, _config(max_count=2)).select_from_db()

    assert selection.wallets == [
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ]
    assert selection.reason == "eligible_candidates_found"
    assert selection.db_candidate_count == 2
    repo.list_evaluations.assert_awaited_once_with(
        statuses=["perfect_candidate", "live_eligible"],
        eligible_live=True,
        limit=25,
    )
    _counters, gauges = metrics.snapshot()
    assert gauges["auto_trader_bootstrap_db_candidates"] == 2.0


async def test_auto_trader_selector_respects_zero_limit():
    repo = SimpleNamespace(list_evaluations=AsyncMock())

    selection = await AutoTraderSelector(repo, _config(max_count=0)).select_from_db()

    assert selection.wallets == []
    assert selection.reason == "max_auto_traders_count_zero"
    repo.list_evaluations.assert_not_called()


async def test_auto_trader_selector_filters_non_live_rows():
    repo = SimpleNamespace(
        list_evaluations=AsyncMock(
            return_value=[
                _row("0x1111111111111111111111111111111111111111"),
                _row(
                    "0x2222222222222222222222222222222222222222",
                    final_score=0.20,
                ),
                _row(
                    "0x3333333333333333333333333333333333333333",
                    rejection_reason="fast_crypto_specialist",
                ),
                _row(
                    "0x4444444444444444444444444444444444444444",
                    total_grouped_signals=0,
                ),
                _row(
                    "0x5555555555555555555555555555555555555555",
                    copyable_notional_ratio=0.10,
                ),
            ]
        )
    )

    selection = await AutoTraderSelector(repo, _config(max_count=5)).select_from_db()

    assert selection.wallets == ["0x1111111111111111111111111111111111111111"]
    assert selection.db_candidate_count == 5


async def test_auto_trader_selector_filters_non_sports_when_preference_enabled():
    repo = SimpleNamespace(
        list_evaluations=AsyncMock(
            return_value=[
                _row("0x1111111111111111111111111111111111111111", sports_esports_ratio=0.0),
                _row("0x2222222222222222222222222222222222222222", sports_esports_ratio=0.80),
            ]
        )
    )

    selection = await AutoTraderSelector(repo, _config(max_count=2)).select_from_db()

    assert selection.wallets == ["0x2222222222222222222222222222222222222222"]


async def test_auto_trader_selector_overfetches_to_find_valid_lower_ranked_candidate():
    rows = [
        _row(f"0x{i:040x}", sports_esports_ratio=0.0)
        for i in range(1, 8)
    ]
    rows.append(_row("0x9999999999999999999999999999999999999999", sports_esports_ratio=0.95))
    repo = SimpleNamespace(list_evaluations=AsyncMock(return_value=rows))

    selection = await AutoTraderSelector(repo, _config(max_count=1)).select_from_db()

    assert selection.wallets == ["0x9999999999999999999999999999999999999999"]
    repo.list_evaluations.assert_awaited_once_with(
        statuses=["perfect_candidate", "live_eligible"],
        eligible_live=True,
        limit=25,
    )


async def test_auto_trader_selector_rejects_stale_config_hash():
    cfg = _config(max_count=1, reject_stale=True)
    current_hash, _context = build_evaluation_context_hash(cfg, balance_usdc=Decimal("100"))
    repo = SimpleNamespace(
        list_evaluations=AsyncMock(
            return_value=[
                _row("0x1111111111111111111111111111111111111111", config_hash="old"),
                _row("0x2222222222222222222222222222222222222222", config_hash=current_hash),
            ]
        )
    )

    selection = await AutoTraderSelector(
        repo,
        cfg,
        balance_provider=StaticBalanceProvider("100"),
    ).select_from_db()

    assert selection.wallets == ["0x2222222222222222222222222222222222222222"]


async def test_auto_trader_selector_rejects_expired_evaluations(monkeypatch):
    cfg = _config(max_count=1, reject_stale=True)
    current_hash, _context = build_evaluation_context_hash(cfg, balance_usdc=Decimal("100"))
    monkeypatch.setattr("services.discovery.auto_trader_selector.time.time", lambda: 10_000.0)
    repo = SimpleNamespace(
        list_evaluations=AsyncMock(
            return_value=[
                _row(
                    "0x1111111111111111111111111111111111111111",
                    config_hash=current_hash,
                    updated_at=1.0,
                )
            ]
        )
    )

    selection = await AutoTraderSelector(
        repo,
        cfg,
        balance_provider=StaticBalanceProvider("100"),
    ).select_from_db()

    assert selection.wallets == []
