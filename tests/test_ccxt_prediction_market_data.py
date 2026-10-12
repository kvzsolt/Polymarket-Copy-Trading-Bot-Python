from __future__ import annotations

from decimal import Decimal

import pytest

from pmcopy.integrations.polymarket.ccxt_prediction_market_data import (
    CcxtPredictionCapabilityError,
    CcxtPredictionMarketData,
)


class Exchange:
    def __init__(self) -> None:
        self.has = {
            "prediction": True,
            "fetchEvents": True,
            "fetchEvent": True,
            "fetchTicker": True,
            "fetchTickers": True,
            "fetchTrades": True,
            "fetchOHLCV": True,
            "fetchBalance": True,
            "fetchPositions": True,
            "fetchPosition": True,
            "fetchTradingFee": True,
            "fetchOpenInterest": True,
            "fetchMarkets": True,
            "fetchStatus": True,
            "fetchTime": True,
            "fetchMyTrades": True,
            "fetchOrderTrades": True,
            "fetchOpenOrders": True,
            "fetchOrder": True,
            "watchOrderBook": True,
            "watchTicker": True,
            "watchTrades": True,
            "watchOrders": True,
            "watchMyTrades": True,
        }
        self.calls = []

    async def fetch_events(self, params):
        self.calls.append(("fetch_events", params))
        return [{"id": "event", "markets": []}]

    async def fetch_event(self, event_id, params):
        self.calls.append(("fetch_event", event_id, params))
        return {"id": event_id}

    async def fetch_ticker(self, outcome, params):
        self.calls.append(("fetch_ticker", outcome, params))
        return {"outcomeId": outcome, "bid": 0.4, "ask": 0.42}

    async def fetch_tickers(self, outcomes, params):
        self.calls.append(("fetch_tickers", outcomes, params))
        return {outcome: {"outcomeId": outcome} for outcome in outcomes}

    async def fetch_trades(self, outcome, since, limit, params):
        self.calls.append(("fetch_trades", outcome, since, limit, params))
        return [{"id": "t", "outcomeId": outcome}]

    async def fetch_ohlcv(self, outcome, timeframe, since, limit, params):
        self.calls.append(("fetch_ohlcv", outcome, timeframe, since, limit, params))
        return [[1, 0.4, 0.5, 0.3, 0.45, 10]]

    async def fetch_balance(self, params):
        self.calls.append(("fetch_balance", params))
        return {"USDC": {"free": 12}}

    async def fetch_positions(self, outcomes, params):
        self.calls.append(("fetch_positions", outcomes, params))
        return [{"outcomeId": outcomes[0], "contracts": 2}]

    async def fetch_position(self, outcome, params):
        self.calls.append(("fetch_position", outcome, params))
        return {"outcomeId": outcome, "contracts": 2}

    async def fetch_trading_fee(self, outcome, params):
        return {"outcomeId": outcome, "taker": 0.01}

    async def fetch_open_interest(self, outcome, params):
        return {"outcomeId": outcome, "openInterest": 100}

    async def fetch_markets(self, params):
        return [{"market": "m", "outcomes": [{"outcomeId": "123"}]}]

    async def fetch_status(self, params):
        return {"status": "ok"}

    async def fetch_time(self, params):
        return 1_785_000_000_000

    async def fetch_my_trades(self, outcome, since, limit, params):
        return [{"id": "mine", "outcomeId": outcome}]

    async def fetch_order_trades(self, order_id, outcome, since, limit, params):
        return [{"id": "fill", "order": order_id, "outcomeId": outcome}]

    async def fetch_open_orders(self, outcome, since, limit, params):
        return [{"id": "open", "outcomeId": outcome}]

    async def fetch_order(self, order_id, outcome, params):
        return {"id": order_id, "outcomeId": outcome}

    async def load_outcome(self, outcome):
        return {"outcomeId": outcome, "outcome": "EVENT:YES"}

    async def fetch_order_book(self, outcome, limit=None, params=None):
        return {
            "outcomeId": outcome,
            "bids": [[0.40, 10], [0.39, 20]],
            "asks": [[0.42, 5], [0.43, 10]],
        }

    async def watch_order_book(self, outcome, limit, params):
        return {"kind": "book", "outcomeId": outcome}

    async def watch_ticker(self, outcome, params):
        return {"kind": "ticker", "outcomeId": outcome}

    async def watch_trades(self, outcome, since, limit, params):
        return [{"kind": "trade", "outcomeId": outcome}]

    async def watch_orders(self, outcome, since, limit, params):
        return [{"kind": "order", "outcomeId": outcome}]

    async def watch_my_trades(self, outcome, since, limit, params):
        return [{"kind": "my-trade", "outcomeId": outcome}]


@pytest.mark.asyncio
async def test_full_read_surface_uses_raw_outcome_ids() -> None:
    exchange = Exchange()
    service = CcxtPredictionMarketData(exchange)

    assert (await service.fetch_events(query="btc"))[0]["id"] == "event"
    assert (await service.fetch_event("event"))["id"] == "event"
    assert (await service.fetch_ticker("123"))["outcomeId"] == "123"
    assert "123" in await service.fetch_tickers(["123"])
    assert (await service.fetch_trades("123"))[0]["outcomeId"] == "123"
    assert len(await service.fetch_ohlcv("123", "1m")) == 1
    assert (await service.fetch_balance())["USDC"]["free"] == 12
    assert (await service.fetch_positions(["123"]))[0]["outcomeId"] == "123"
    assert (await service.fetch_position("123"))["outcomeId"] == "123"
    assert (await service.fetch_trading_fee("123"))["taker"] == 0.01
    assert (await service.fetch_open_interest("123"))["openInterest"] == 100


@pytest.mark.asyncio
async def test_visible_depth_respects_buy_max_and_sell_min_price() -> None:
    service = CcxtPredictionMarketData(Exchange())

    buy = await service.visible_capacity("123", side="BUY", price_limit=Decimal("0.425"))
    sell = await service.visible_capacity("123", side="SELL", price_limit=Decimal("0.395"))

    assert buy.shares == Decimal("5")
    assert buy.notional_usdc == Decimal("2.10")
    assert sell.shares == Decimal("10")
    assert sell.notional_usdc == Decimal("4.00")


@pytest.mark.asyncio
async def test_watch_surface_is_available_without_replacing_custom_wss() -> None:
    service = CcxtPredictionMarketData(Exchange())

    assert (await service.watch_order_book("123"))["kind"] == "book"
    assert (await service.watch_ticker("123"))["kind"] == "ticker"
    assert (await service.watch_trades("123"))[0]["kind"] == "trade"
    assert (await service.watch_orders("123"))[0]["kind"] == "order"
    assert (await service.watch_my_trades("123"))[0]["kind"] == "my-trade"


@pytest.mark.asyncio
async def test_missing_capability_fails_explicitly() -> None:
    exchange = Exchange()
    exchange.has["fetchPositions"] = False
    service = CcxtPredictionMarketData(exchange)

    with pytest.raises(CcxtPredictionCapabilityError, match="fetchPositions"):
        await service.fetch_positions(["123"])


@pytest.mark.asyncio
async def test_complete_prediction_read_surface_includes_account_and_status_methods() -> None:
    service = CcxtPredictionMarketData(Exchange())

    assert (await service.fetch_markets())[0]["market"] == "m"
    assert (await service.fetch_status())["status"] == "ok"
    assert await service.fetch_time() == 1_785_000_000_000
    assert (await service.fetch_my_trades("123"))[0]["id"] == "mine"
    assert (await service.fetch_order_trades("order-1", "123"))[0]["order"] == "order-1"
    assert (await service.fetch_open_orders("123"))[0]["id"] == "open"
    assert (await service.fetch_order("order-1", "123"))["id"] == "order-1"
    assert (await service.resolve_outcome("123"))["outcome"] == "EVENT:YES"
