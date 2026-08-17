"""Paper-mode execution boundary contracts.

Paper mode uses CCXT for live public market reads and the local simulator for
order writes. The boundary must be structural: the paper gateway never
forwards a write to the CCXT transport, so a configuration mistake cannot
place a live order from a paper runtime.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from integrations.polymarket.models import (
    LimitOrderRequest,
    MarketableOrderRequest,
    OrderBook,
)
from integrations.polymarket.paper_execution import (
    PaperExecutionGateway,
    PaperOrderSimulator,
)

TOKEN = "123"
CONDITION = "condition"


class RecordingMarketGateway:
    """Stand-in for the CCXT market-data gateway that records every call."""

    def __init__(self) -> None:
        self.create_order = AsyncMock(side_effect=AssertionError("live write attempted"))
        self.cancel_order = AsyncMock(side_effect=AssertionError("live cancel attempted"))
        self.get_orderbook_calls: list[str] = []
        self.compatibility = {
            "available": True,
            "transport": "ccxt.prediction.polymarket",
            "authenticated": False,
            "live_execution_ready": False,
        }

    async def get_market_info(self, token_id: str) -> dict[str, object]:
        return {"token_id": token_id, "tick_size": "0.01"}

    async def get_orderbook(self, token_id: str) -> OrderBook:
        self.get_orderbook_calls.append(token_id)
        return OrderBook(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            bids=[],
            asks=[],
            best_bid=None,
            best_ask=None,
            spread_bps=None,
        )

    async def get_price(self, token_id: str, side: str) -> dict[str, str]:
        return {"price": "0.42"}

    async def check_compatibility(self) -> dict[str, object]:
        return dict(self.compatibility)

    async def close(self) -> None:
        self.create_order.assert_not_awaited()
        self.cancel_order.assert_not_awaited()


def _paper_gateway() -> tuple[PaperExecutionGateway, RecordingMarketGateway]:
    market = RecordingMarketGateway()
    gateway = PaperExecutionGateway(
        market_data_gateway=market,
        simulator=PaperOrderSimulator(),
    )
    return gateway, market


def _buy_request(size: str = "10", limit: str = "0.40") -> MarketableOrderRequest:
    return MarketableOrderRequest(
        token_id=TOKEN,
        side="BUY",
        size=Decimal(size),
        price_limit=Decimal(limit),
        condition_id=CONDITION,
    )


def _sell_request(size: str = "10", limit: str = "0.60") -> MarketableOrderRequest:
    return MarketableOrderRequest(
        token_id=TOKEN,
        side="SELL",
        size=Decimal(size),
        price_limit=Decimal(limit),
        condition_id=CONDITION,
    )


@pytest.mark.asyncio
async def test_paper_marketable_buy_never_reaches_the_transport() -> None:
    gateway, market = _paper_gateway()

    result = await gateway.submit_marketable_order(_buy_request())

    assert result.success is True
    assert result.order_id is not None
    assert result.ambiguous_post_failure is not True
    market.create_order.assert_not_awaited()
    market.cancel_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_paper_marketable_sell_never_reaches_the_transport() -> None:
    gateway, market = _paper_gateway()

    result = await gateway.submit_marketable_order(_sell_request())

    assert result.success is True
    market.create_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_paper_limit_order_rests_locally() -> None:
    gateway, market = _paper_gateway()

    result = await gateway.submit_limit_order(
        LimitOrderRequest(
            token_id=TOKEN,
            side="BUY",
            price=Decimal("0.40"),
            size=Decimal("10"),
            condition_id=CONDITION,
        )
    )

    assert result.success is True
    open_orders = await gateway.list_open_orders()
    assert [order.order_id for order in open_orders] == [result.order_id]
    market.create_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_paper_cancel_is_simulated_and_idempotent() -> None:
    gateway, market = _paper_gateway()
    result = await gateway.submit_limit_order(
        LimitOrderRequest(
            token_id=TOKEN,
            side="BUY",
            price=Decimal("0.40"),
            size=Decimal("10"),
            condition_id=CONDITION,
        )
    )

    canceled = await gateway.cancel_order(result.order_id)
    again = await gateway.cancel_order(result.order_id)

    assert canceled.success is True
    assert again.success is True
    assert again.status == "already_gone"
    market.cancel_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_paper_buy_fill_derives_shares_from_spend_and_limit() -> None:
    gateway, _market = _paper_gateway()

    result = await gateway.submit_marketable_order(_buy_request(size="10", limit="0.40"))

    assert result.success is True
    fills = await gateway.get_fills()
    assert len(fills) == 1
    fill = fills[0]
    assert fill.order_id == result.order_id
    assert fill.price == Decimal("0.40")
    # 10 USDC spend at a 0.40 cap buys at most 25 shares.
    assert fill.size == Decimal("25")
    assert fill.side == "BUY"


@pytest.mark.asyncio
async def test_paper_sell_fill_sells_the_full_share_size() -> None:
    gateway, _market = _paper_gateway()

    await gateway.submit_marketable_order(_sell_request(size="7.3", limit="0.60"))

    fills = await gateway.get_fills()
    assert fills[0].size == Decimal("7.3")
    assert fills[0].price == Decimal("0.60")
    assert fills[0].side == "SELL"


@pytest.mark.asyncio
async def test_paper_get_order_serves_simulated_state() -> None:
    gateway, _market = _paper_gateway()
    result = await gateway.submit_marketable_order(_buy_request())

    state = await gateway.get_order(result.order_id)

    assert state.order_id == result.order_id
    assert state.status == "closed"


@pytest.mark.asyncio
async def test_paper_reads_delegate_to_the_ccxt_market_gateway() -> None:
    gateway, market = _paper_gateway()

    book = await gateway.get_orderbook(TOKEN)
    info = await gateway.get_market_info(TOKEN)
    price = await gateway.get_price(TOKEN, "SELL")
    compat = await gateway.check_compatibility()

    assert book.token_id == TOKEN
    assert market.get_orderbook_calls == [TOKEN]
    assert info["tick_size"] == "0.01"
    assert price == {"price": "0.42"}
    assert compat["available"] is True
    assert compat["transport"] == "ccxt.prediction.polymarket"


@pytest.mark.asyncio
async def test_paper_compatibility_reports_no_live_execution() -> None:
    gateway, _market = _paper_gateway()

    compat = await gateway.check_compatibility()

    assert compat["authenticated"] is False
    assert compat["live_execution_ready"] is False


@pytest.mark.asyncio
async def test_paper_gateway_close_never_touches_write_methods() -> None:
    gateway, market = _paper_gateway()
    await gateway.submit_marketable_order(_buy_request())

    await gateway.close()

    market.create_order.assert_not_awaited()
    market.cancel_order.assert_not_awaited()
