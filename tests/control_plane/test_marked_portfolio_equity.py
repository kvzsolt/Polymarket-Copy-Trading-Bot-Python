"""Runtime portfolio equity includes marked remaining shares, never guessed cost."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrators.control_plane_portfolio import PortfolioAccountBinding, PortfolioRuntimeFactory
from tests.control_plane.test_portfolio_runtime import MutableClock


class Quote:
    """Expose exact injected quote evidence."""

    best_bid = Decimal("0.54")
    best_ask = Decimal("0.56")

    def __init__(self, fresh=True):
        self.fresh = fresh

    def is_fresh(self, max_age_seconds):
        return self.fresh


class Quotes:
    """Read a deterministic cached quote without transport I/O."""

    def __init__(self, quote):
        self.quote = quote

    def get_quote(self, token_id):
        assert token_id == "token"
        return self.quote


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("quote", "cost", "equity", "unrealized"),
    [
        (Quote(), "0.5", "110.00", "5.00"),
        (Quote(), "0", "110.00", None),
        (Quote(False), "0.5", None, None),
        (None, "0.5", None, None),
    ],
)
async def test_real_portfolio_factory_uses_fresh_marks_and_preserves_missing_cost(
    quote, cost, equity, unrealized
):
    positions = AsyncMock(
        list_held_positions=AsyncMock(
            return_value=[
                SimpleNamespace(
                    token_id="token",
                    size_tokens=Decimal(100),
                    avg_entry_price=Decimal(cost),
                )
            ]
        )
    )
    reservations = AsyncMock(sum_active_quote_for_user=AsyncMock(return_value=Decimal(0)))
    balance = AsyncMock(read=AsyncMock(return_value=Decimal(55)))
    bundle = PortfolioRuntimeFactory(clock=MutableClock(), balance=balance).build(
        bindings=(
            PortfolioAccountBinding(
                "account", "wallet", positions, reservations, quotes=Quotes(quote)
            ),
        ),
        refresh_interval_seconds=10,
        stale_after_seconds=30,
    )
    result = await bundle.refresh.refresh_once()
    assert result.updated_count == 1
    view = await bundle.cache.get_cached_portfolio("account")
    assert view.equity_usdc == equity
    assert view.unrealized_pnl_usdc == unrealized
    assert view.positions_value_usdc == ("55.00" if equity is not None else None)
    assert view.positions_cost_usdc == ("50.0" if cost == "0.5" else None)
    assert view.valuation_basis == ("market_mark" if equity is not None else "unavailable")
