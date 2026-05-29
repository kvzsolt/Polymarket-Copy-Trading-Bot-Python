from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.discovery.auto_trader_selector import AutoTraderSelector
from services.metrics import metrics

pytestmark = pytest.mark.asyncio


def _config(*, max_count: int = 2):
    return SimpleNamespace(
        discovery=SimpleNamespace(
            max_auto_traders_count=max_count,
            discovery_min_copyable_grouped_signal_ratio=0.75,
            discovery_min_copyable_notional_ratio=0.60,
            discovery_require_7d_non_negative=True,
            discovery_min_7d_pnl_usdc=0.0,
        ),
        leader=SimpleNamespace(auto_discover_min_score=0.45),
    )


def _row(wallet: str, **overrides):
    data = {
        "wallet": wallet,
        "status": "live_eligible",
        "eligible_live": True,
        "rejection_reason": None,
        "final_score": 0.80,
        "copyable_signal_ratio": 0.90,
        "copyable_notional_ratio": 0.85,
        "total_grouped_signals": 10,
        "pnl_7d": Decimal("1.0"),
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
        limit=2,
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
