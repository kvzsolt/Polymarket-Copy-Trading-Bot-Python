"""The operator projection cannot invent profits on imported holdings."""

from decimal import Decimal

import pytest

from tests.control_plane.test_extended_reads import FakePosition, FakeQuote, pnl_service


@pytest.mark.asyncio
async def test_unknown_initial_cost_is_missing_in_position_and_account_pnl():
    """A fresh mark values inventory but does not reveal its acquisition cost."""
    service = pnl_service(
        positions=(FakePosition("condition-a", "token-a", None, Decimal("10"), Decimal("0")),),
        quotes={"token-a": FakeQuote(Decimal("0.5"), Decimal("0.6"), True)},
    )
    view = await service.get("account-a")
    assert view.positions[0].market_value_usdc == "5.50"
    assert view.positions[0].avg_entry_price is None
    assert view.positions[0].cost_basis_usdc is None
    assert view.positions[0].unrealized_pnl_usdc is None
    assert view.cost_basis_usdc is None
    assert view.unrealized_pnl_usdc is None
    assert view.unrealized_basis == "unknown_cost_basis"


@pytest.mark.asyncio
@pytest.mark.parametrize(("bid", "ask"), [("NaN", "0.6"), ("0.8", "0.6"), ("-0.1", "0.6")])
async def test_invalid_mark_evidence_never_becomes_a_portfolio_mid(bid, ask):
    """Freshness alone cannot make crossed or malformed quotes valid."""
    service = pnl_service(
        positions=(FakePosition("condition-a", "token-a", None, Decimal("10"), Decimal("0.4")),),
        quotes={"token-a": FakeQuote(Decimal(bid), Decimal(ask), True)},
    )
    view = await service.get("account-a")
    assert view.positions[0].mark_price is None
    assert view.unrealized_pnl_usdc is None
