from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from integrations.polymarket.errors import VenueInvalidOrderError
from integrations.polymarket.models import LimitOrderRequest, MarketableOrderRequest

TOKEN = "123"


class FakeExchange:
    def __init__(self) -> None:
        self.has = {
            "prediction": True,
            "createOrder": True,
            "fetchOrderBook": True,
            "fetchOrder": True,
            "fetchOpenOrders": True,
            "fetchMyTrades": True,
            "cancelOrder": True,
        }
        self.options = {"chainId": 137, "signatureType": 3}
        self.privateKey = "secret"
        self.walletAddress = "0xwallet"
        self._pmcopy_precision_patch = "pmcopy-arbitrary-tick-v1"
        self.created: list[tuple] = []
        self.fetch_order_book_calls = 0
        self.book = {
            "outcomeId": TOKEN,
            "timestamp": 1_750_000_000_000,
            "bids": [["0.40", "20"]],
            "asks": [["0.42", "10"]],
            "info": {
                "tick_size": "0.0025",
                "min_order_size": "5",
                "neg_risk": True,
            },
        }
        self.outcome_value = {
            "outcomeId": TOKEN,
            "precision": {"price": "0.0025"},
            "limits": {"amount": {"min": "5"}},
            "negRisk": True,
        }
        self.create_response = {
            "id": "order-1",
            "status": "open",
            "info": {"success": True},
        }
        self.order = {
            "id": "order-1",
            "status": "LIVE",
            "side": "BUY",
            "price": "0.42",
            "amount": "10",
            "filled": "2",
            "average": "0.415",
            "timestamp": 1_750_000_000_000,
        }
        self.open_orders = [self.order]
        self.trades = []
        self.cancel_response = {"canceled": ["order-1"]}

    async def load_outcome(self, token_id):
        assert token_id == TOKEN
        return self.outcome_value

    async def fetch_order_book(self, token_id):
        assert token_id == TOKEN
        self.fetch_order_book_calls += 1
        return self.book

    async def create_order(self, *args):
        self.created.append(args)
        return self.create_response

    async def fetch_order(self, order_id):
        assert order_id == "order-1"
        return self.order

    async def clob_private_get_data_orders(self, params):
        return {"data": self.open_orders, "next_cursor": ""}

    async def clob_private_get_data_trades(self, params):
        return {"data": self.trades, "next_cursor": "LTE="}

    async def fetch_open_orders(self, outcome, since, limit, params):
        assert outcome is since is limit is None
        return self.open_orders

    async def fetch_my_trades(self, outcome, since, limit, params):
        assert outcome is since is limit is None
        return self.trades

    async def cancel_order(self, order_id):
        assert order_id == "order-1"
        return self.cancel_response


@pytest.mark.asyncio
async def test_orderbook_maps_arbitrary_tick_and_quality_fields() -> None:
    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(exchange)

    book = await gateway.get_orderbook(TOKEN)

    assert book.token_id == TOKEN
    assert book.tick_size == Decimal("0.0025")
    assert book.min_order_size == Decimal("5")
    assert book.neg_risk is True
    assert book.best_bid == Decimal("0.40")
    assert book.best_ask == Decimal("0.42")
    assert book.spread_bps == 487
    assert book.timestamp == datetime.fromtimestamp(1_750_000_000, tz=timezone.utc)


@pytest.mark.asyncio
async def test_missing_execution_metadata_fails_closed() -> None:
    exchange = FakeExchange()
    exchange.book["info"] = {}
    exchange.outcome_value = {}
    gateway = CcxtPredictionGateway(exchange)

    with pytest.raises(VenueInvalidOrderError, match="omitted"):
        await gateway.get_orderbook(TOKEN)


@pytest.mark.asyncio
async def test_limit_order_carries_exact_metadata_and_quantized_price() -> None:
    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(exchange)

    result = await gateway.submit_limit_order(
        LimitOrderRequest(
            token_id=TOKEN,
            side="BUY",
            price=Decimal("0.411"),
            size=Decimal("5"),
            condition_id="condition",
        )
    )

    assert result.success is True
    outcome, kind, side, amount, price, params = exchange.created[0]
    assert (outcome, kind, side) == (TOKEN, "limit", "buy")
    assert Decimal(str(amount)) == Decimal("5")
    assert Decimal(str(price)) == Decimal("0.4100")
    assert params["tickSize"] == "0.0025"
    assert params["negRisk"] is True
    assert params["orderType"] == "GTC"


@pytest.mark.asyncio
async def test_market_buy_uses_cost_and_explicit_price_not_cached_outcome_price() -> None:
    exchange = FakeExchange()
    exchange.outcome_value["price"] = "0.99"
    gateway = CcxtPredictionGateway(exchange)

    await gateway.submit_marketable_order(
        MarketableOrderRequest(
            token_id=TOKEN,
            side="BUY",
            size=Decimal("5.12"),
            price_limit=Decimal("0.411"),
            condition_id="condition",
            order_type="FOK",
        )
    )

    _, kind, side, amount, price, params = exchange.created[0]
    assert (kind, side) == ("market", "buy")
    assert Decimal(str(amount)) == Decimal("5.12")
    assert Decimal(str(price)) == Decimal("0.4100")
    assert Decimal(str(params["cost"])) == Decimal("5.12")
    assert params["orderType"] == "FOK"


@pytest.mark.asyncio
async def test_market_sell_uses_shares_and_fak() -> None:
    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(exchange)

    await gateway.submit_marketable_order(
        MarketableOrderRequest(
            token_id=TOKEN,
            side="SELL",
            size=Decimal("7"),
            price_limit=Decimal("0.411"),
            condition_id="condition",
            order_type="FAK",
        )
    )

    _, kind, side, amount, price, params = exchange.created[0]
    assert (kind, side) == ("market", "sell")
    assert Decimal(str(amount)) == Decimal("7")
    assert Decimal(str(price)) == Decimal("0.4125")
    assert "cost" not in params
    assert params["orderType"] == "FAK"
    assert params["timeInForce"] == "IOC"


@pytest.mark.asyncio
async def test_market_order_without_price_limit_never_reaches_ccxt() -> None:
    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(exchange)

    with pytest.raises(VenueInvalidOrderError, match="price_limit"):
        await gateway.submit_marketable_order(
            MarketableOrderRequest(
                token_id=TOKEN,
                side="BUY",
                size=Decimal("5"),
                price_limit=None,
                condition_id="condition",
            )
        )
    assert exchange.created == []


@pytest.mark.asyncio
async def test_order_state_uses_real_average_and_never_substitutes_limit_price() -> None:
    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(exchange)

    order = await gateway.get_order("order-1")
    assert order.avg_fill_price == Decimal("0.415")

    exchange.order = {**exchange.order, "average": None}
    order_without_average = await gateway.get_order("order-1")
    assert order_without_average.avg_fill_price == Decimal("0")


@pytest.mark.asyncio
async def test_open_order_filters_are_exact() -> None:
    exchange = FakeExchange()
    exchange.open_orders = [
        exchange.order,
        {**exchange.order, "id": "other", "info": {"market": "other-market"}},
    ]
    gateway = CcxtPredictionGateway(exchange)

    orders = await gateway.list_open_orders(order_id="order-1", market="market-1")
    assert [order.order_id for order in orders] == ["order-1"]
    assert orders[0].status == "open"


@pytest.mark.asyncio
async def test_maker_fill_attribution_and_page_duplicate_elimination() -> None:
    exchange = FakeExchange()
    raw_trade = {
        "id": "trade-1",
        "timestamp": 1_750_000_000_000,
        "fee": {"cost": "0.01"},
        "info": {
            "trader_side": "MAKER",
            "status": "CONFIRMED",
            "bucket_index": 7,
            "transaction_hash": "0xsettled",
            "maker_orders": [
                {
                    "order_id": "maker-order",
                    "token_id": "maker-token",
                    "side": "SELL",
                    "matched_amount": "4",
                    "price": "0.41",
                }
            ],
        },
    }
    exchange.trades = [raw_trade, raw_trade]
    gateway = CcxtPredictionGateway(exchange)

    fills = await gateway.get_fills(
        market="market-1",
        trade_id="trade-1",
        order_id="maker-order",
    )

    assert len(fills) == 1
    fill = fills[0]
    assert fill.order_id == "maker-order"
    assert fill.token_id == "maker-token"
    assert fill.size == Decimal("4")
    assert fill.price == Decimal("0.41")
    assert fill.fee == Decimal("0.01")
    assert fill.trader_side == "MAKER"
    assert fill.bucket_index == 7


@pytest.mark.asyncio
async def test_cancel_maps_success() -> None:
    gateway = CcxtPredictionGateway(FakeExchange())
    result = await gateway.cancel_order("order-1")
    assert result.success is True
    assert result.status == "canceled"


@pytest.mark.asyncio
async def test_compatibility_probe_reports_prediction_and_patch() -> None:
    report = await CcxtPredictionGateway(FakeExchange()).check_compatibility()
    assert report["sdk_available"] is True
    assert report["prediction"] is True
    assert report["signature_type"] == 3
    assert report["arbitrary_tick_patch"] == "pmcopy-arbitrary-tick-v1"


@pytest.mark.asyncio
async def test_market_buy_minimum_is_checked_in_shares_not_usdc_cost() -> None:
    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(exchange)

    accepted = await gateway.submit_marketable_order(
        MarketableOrderRequest(
            token_id=TOKEN,
            side="BUY",
            size=Decimal("2.05"),  # 5 shares at a 0.41 max price
            price_limit=Decimal("0.41"),
            condition_id="condition",
        )
    )
    assert accepted.success is True

    with pytest.raises(VenueInvalidOrderError, match="below minimum"):
        await gateway.submit_marketable_order(
            MarketableOrderRequest(
                token_id=TOKEN,
                side="BUY",
                size=Decimal("2.04"),
                price_limit=Decimal("0.41"),
                condition_id="condition",
            )
        )


@pytest.mark.asyncio
async def test_execution_metadata_refreshes_before_each_signed_order() -> None:
    exchange = FakeExchange()
    exchange.book["info"]["tick_size"] = "0.01"
    exchange.outcome_value["precision"]["price"] = "0.01"
    gateway = CcxtPredictionGateway(exchange)

    # Seed the adapter with the old rule, then simulate Polymarket's runtime
    # tick-size transition before the next order is signed.
    await gateway.get_orderbook(TOKEN)
    exchange.book["info"]["tick_size"] = "0.0025"
    exchange.outcome_value["precision"]["price"] = "0.0025"

    await gateway.submit_limit_order(
        LimitOrderRequest(
            token_id=TOKEN,
            side="BUY",
            price=Decimal("0.413"),
            size=Decimal("5"),
            condition_id="condition",
        )
    )

    _, _, _, _, price, params = exchange.created[-1]
    assert exchange.fetch_order_book_calls >= 2
    assert Decimal(str(price)) == Decimal("0.4125")
    assert params["tickSize"] == "0.0025"


@pytest.mark.asyncio
async def test_compatibility_fails_when_full_gateway_contract_or_type3_identity_is_missing() -> (
    None
):
    exchange = FakeExchange()
    exchange.has["fetchOrder"] = False
    report = await CcxtPredictionGateway(exchange).check_compatibility()
    assert report["sdk_available"] is False
    assert "fetchOrder" in report["missing_methods"]

    exchange = FakeExchange()
    exchange.options["signatureType"] = 0
    report = await CcxtPredictionGateway(exchange).check_compatibility()
    assert report["sdk_available"] is False
    assert report["error"] is not None
