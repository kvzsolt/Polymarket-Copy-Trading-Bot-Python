"""Cash invested in remaining inventory is not a trading drawdown."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from services.pnlTracker import PnLTracker

NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)


def test_partial_sale_does_not_report_invested_cash_as_drawdown(tmp_path):
    tracker = PnLTracker(str(tmp_path / "pnl.jsonl"))
    tracker.record_buy("market", ".50", "200", "100")
    tracker.record_sell("market", ".55", "100", "55", entry_price=".50", realized_pnl="5")
    assert tracker.metrics()["gross_realized_pnl"] == Decimal("5")
    assert tracker.metrics()["max_drawdown"] is None


def test_drawdown_uses_explicit_transfer_adjusted_marked_equity_series(tmp_path):
    tracker = PnLTracker(str(tmp_path / "pnl.jsonl"))
    tracker.record_equity(
        cash=Decimal(100), inventory_value=Decimal(0), net_transfers=Decimal(0), observed_at=NOW
    )
    tracker.record_equity(
        cash=Decimal(0),
        inventory_value=Decimal(100),
        net_transfers=Decimal(0),
        observed_at=NOW + timedelta(seconds=1),
    )
    tracker.record_equity(
        cash=Decimal(55),
        inventory_value=Decimal(55),
        net_transfers=Decimal(0),
        observed_at=NOW + timedelta(seconds=2),
    )
    assert tracker.metrics()["economic_pnl"] == Decimal(10)
    assert tracker.metrics()["max_drawdown"] == 0
    tracker.record_equity(
        cash=Decimal(155),
        inventory_value=Decimal(45),
        net_transfers=Decimal(100),
        observed_at=NOW + timedelta(seconds=3),
    )
    assert tracker.metrics()["max_drawdown"] == Decimal(10)
    assert tracker.metrics()["equity_observed_at"] == NOW + timedelta(seconds=3)


def test_unknown_cost_basis_is_not_realized_zero(tmp_path):
    tracker = PnLTracker(str(tmp_path / "pnl.jsonl"))
    tracker.record_sell("market", ".55", "100", "55", entry_price=None, realized_pnl=None)
    assert tracker.metrics()["unknown_cost_basis_sells"] == 1
    assert tracker.metrics()["win_rate"] is None


@pytest.mark.parametrize("value", [True, "NaN", "Infinity"])
def test_invalid_money_does_not_mutate_tracker(tmp_path, value):
    tracker = PnLTracker(str(tmp_path / "pnl.jsonl"))
    with pytest.raises(ValueError):
        tracker.record_buy("market", value, "1", "1")
    assert tracker.metrics()["total_trades"] == 0


@pytest.mark.asyncio
async def test_local_pnl_query_preserves_unobserved_drawdown(tmp_path):
    from tests.control_plane.test_extended_reads import pnl_service

    tracker = PnLTracker(str(tmp_path / "pnl.jsonl"))
    service = pnl_service(quotes={}, positions=(), tracker=tracker)
    assert (await service.get("account-a")).realized.max_drawdown_usdc is None
