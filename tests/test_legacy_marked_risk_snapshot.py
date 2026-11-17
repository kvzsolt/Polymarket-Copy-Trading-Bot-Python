"""Legacy BUY admission values inventory and preserves missing risk evidence."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.domain.position import Position
from pmcopy.orchestrators import trade_pipeline
from tests.config_helpers import load_test_config


class FixedQuote:
    """Provide a deterministic fresh midpoint without a venue call."""

    best_bid = Decimal("0.54")
    best_ask = Decimal("0.56")

    def is_fresh(self, max_age_seconds: float) -> bool:
        """Return fixture freshness independently of wall time."""
        return True


class QuoteSource:
    """Return only the requested account's cached token evidence."""

    def __init__(self, quote: FixedQuote | None) -> None:
        self._quote = quote

    def get_quote(self, token_id: str) -> FixedQuote | None:
        """Read the cached quote."""
        assert token_id == "token"
        return self._quote


def context(*, quote: FixedQuote | None, entry_price: Decimal = Decimal("0.5")):
    """Assemble the real snapshot consumer with fake account I/O."""
    return SimpleNamespace(
        user_id="account-a",
        wallet_address="0x" + "1" * 40,
        extra={},
        config=load_test_config(
            risk={"enable_balance_tiered_policy": True, "day_start_equity_usdc": 100}
        ),
        market_quotes=QuoteSource(quote),
        positions_repo=SimpleNamespace(
            list_held_positions=AsyncMock(
                return_value=[
                    Position("account-a", "condition", "token", "leader", Decimal(100), entry_price)
                ]
            )
        ),
    )


@pytest.mark.asyncio
async def test_partial_exit_equity_marks_remaining_shares(monkeypatch) -> None:
    """Spend 100, receive 55, retain 55 of inventory: equity is 110, cost is 50."""
    monkeypatch.setattr(trade_pipeline, "get_my_balance", AsyncMock(return_value=Decimal(55)))
    snapshot = await trade_pipeline._fetch_balance_snapshot(
        context(quote=FixedQuote()), "condition", "leader"
    )
    assert snapshot is not None
    assert snapshot.equity_usdc == Decimal(110)
    assert snapshot.total_exposure_usdc == Decimal(50)
    assert snapshot.market_exposure_usdc == Decimal(50)
    assert snapshot.leader_exposure_usdc == Decimal(50)


@pytest.mark.asyncio
async def test_missing_mark_does_not_substitute_historical_cost(monkeypatch) -> None:
    monkeypatch.setattr(trade_pipeline, "get_my_balance", AsyncMock(return_value=Decimal(55)))
    current = context(quote=None)
    snapshot = await trade_pipeline._fetch_balance_snapshot(current, "condition", "leader")
    assert snapshot is not None
    assert snapshot.equity_usdc is None
    cap = await trade_pipeline._resolve_buy_risk_cap(current, "leader", SimpleNamespace(), snapshot)
    assert not cap.allow
    assert cap.reason == "marked_equity_unavailable"


@pytest.mark.asyncio
async def test_unknown_entry_cost_keeps_equity_but_withholds_cost_based_headroom(
    monkeypatch,
) -> None:
    monkeypatch.setattr(trade_pipeline, "get_my_balance", AsyncMock(return_value=Decimal(55)))
    current = context(quote=FixedQuote(), entry_price=Decimal(0))
    snapshot = await trade_pipeline._fetch_balance_snapshot(current, "condition", "leader")
    assert snapshot is not None
    assert snapshot.equity_usdc == Decimal(110)
    assert snapshot.total_exposure_usdc is None
    cap = await trade_pipeline._resolve_buy_risk_cap(current, "leader", SimpleNamespace(), snapshot)
    assert not cap.allow
    assert cap.reason == "position_cost_basis_unavailable"


@pytest.mark.asyncio
async def test_absent_account_baseline_does_not_read_global_json(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(trade_pipeline, "get_my_balance", AsyncMock(return_value=Decimal(55)))
    current = context(quote=FixedQuote())
    current.config = load_test_config(
        risk={"enable_balance_tiered_policy": True, "day_start_equity_usdc": 0}
    )
    snapshot = await trade_pipeline._fetch_balance_snapshot(current, "condition", "leader")
    assert snapshot is not None
    cap = await trade_pipeline._resolve_buy_risk_cap(current, "leader", SimpleNamespace(), snapshot)
    assert not cap.allow
    assert cap.reason == "account_day_start_equity_unavailable"
    assert not (tmp_path / "day_equity.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("balance", [True, Decimal("NaN"), Decimal("Infinity"), Decimal("-1")])
async def test_invalid_balance_fails_closed_before_sizing(monkeypatch, balance) -> None:
    monkeypatch.setattr(trade_pipeline, "get_my_balance", AsyncMock(return_value=balance))
    snapshot = await trade_pipeline._fetch_balance_snapshot(
        context(quote=FixedQuote()), "condition", "leader"
    )
    assert snapshot is None
