from __future__ import annotations

from decimal import Decimal

import pytest

from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from integrations.polymarket.errors import VenueUnavailableError


class PaginatedExchange:
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
        self.order_requests: list[dict[str, object]] = []
        self.trade_requests: list[dict[str, object]] = []
        self.unified_open_orders_called = False
        self.unified_trades_called = False
        self.order_pages: dict[str | None, dict[str, object]] = {
            None: {
                "data": [
                    {
                        "id": "order-1",
                        "status": "LIVE",
                        "side": "BUY",
                        "price": "0.40",
                        "original_size": "10",
                        "size_matched": "2",
                        "created_at": 1_750_000_000,
                        "market": "market-1",
                        "asset_id": "token-1",
                    }
                ],
                "next_cursor": "MTAw",
            },
            "MTAw": {
                "data": [
                    {
                        "id": "order-2",
                        "status": "LIVE",
                        "side": "SELL",
                        "price": "0.60",
                        "original_size": "7",
                        "size_matched": "1",
                        "created_at": 1_750_000_001,
                        "market": "market-1",
                        "asset_id": "token-2",
                    }
                ],
                "next_cursor": "",
            },
        }
        maker_trade = {
            "id": "trade-maker",
            "market": "market-1",
            "asset_id": "taker-token",
            "side": "BUY",
            "size": "4",
            "price": "0.41",
            "status": "CONFIRMED",
            "match_time": "1750000000",
            "bucket_index": 7,
            "transaction_hash": "0xsettled",
            "trader_side": "MAKER",
            "maker_orders": [
                {
                    "order_id": "maker-order",
                    "token_id": "maker-token",
                    "side": "SELL",
                    "matched_amount": "4",
                    "price": "0.41",
                }
            ],
        }
        self.trade_pages: dict[str | None, dict[str, object]] = {
            None: {"data": [maker_trade], "next_cursor": "MTAw"},
            "MTAw": {
                # Repeat the first row to prove cross-page deduplication, then add
                # a second taker fill that must not be lost.
                "data": [
                    maker_trade,
                    {
                        "id": "trade-taker",
                        "taker_order_id": "taker-order",
                        "market": "market-1",
                        "asset_id": "taker-token",
                        "side": "BUY",
                        "size": "3",
                        "price": "0.43",
                        "status": "CONFIRMED",
                        "match_time": "1750000001",
                        "bucket_index": 8,
                        "transaction_hash": "0xsettled2",
                        "trader_side": "TAKER",
                        "maker_orders": [],
                    },
                ],
                "next_cursor": "LTE=",
            },
        }

    async def clob_private_get_data_orders(self, params):
        request = dict(params)
        self.order_requests.append(request)
        return self.order_pages[request.get("next_cursor")]

    async def clob_private_get_data_trades(self, params):
        request = dict(params)
        self.trade_requests.append(request)
        return self.trade_pages[request.get("next_cursor")]

    async def fetch_open_orders(self, *_args):
        self.unified_open_orders_called = True
        return []

    async def fetch_my_trades(self, *_args):
        self.unified_trades_called = True
        return []


@pytest.mark.asyncio
async def test_open_orders_consume_every_private_cursor_page() -> None:
    exchange = PaginatedExchange()
    gateway = CcxtPredictionGateway(exchange)

    orders = await gateway.list_open_orders(market="market-1")

    assert [order.order_id for order in orders] == ["order-1", "order-2"]
    assert exchange.order_requests == [
        {"market": "market-1"},
        {"market": "market-1", "next_cursor": "MTAw"},
    ]
    assert exchange.unified_open_orders_called is False


@pytest.mark.asyncio
async def test_fills_consume_every_page_and_parse_raw_maker_and_taker_legs() -> None:
    exchange = PaginatedExchange()
    gateway = CcxtPredictionGateway(exchange)

    fills = await gateway.get_fills(market="market-1")

    assert [(fill.fill_id, fill.order_id) for fill in fills] == [
        ("trade-maker", "maker-order"),
        ("trade-taker", "taker-order"),
    ]
    assert fills[0].token_id == "maker-token"
    assert fills[0].trader_side == "MAKER"
    assert fills[0].size == Decimal("4")
    assert fills[1].token_id == "taker-token"
    assert fills[1].trader_side == "TAKER"
    assert exchange.trade_requests == [
        {"market": "market-1"},
        {"market": "market-1", "next_cursor": "MTAw"},
    ]
    assert exchange.unified_trades_called is False


@pytest.mark.asyncio
async def test_repeated_private_cursor_fails_closed_instead_of_returning_partial_history() -> None:
    exchange = PaginatedExchange()
    exchange.order_pages["MTAw"] = {"data": [], "next_cursor": "MTAw"}

    with pytest.raises(VenueUnavailableError, match="cursor repeated"):
        await CcxtPredictionGateway(exchange).list_open_orders()


@pytest.mark.asyncio
async def test_authenticated_compatibility_requires_raw_paginated_history_methods() -> None:
    exchange = PaginatedExchange()
    # Shadow the class methods with non-callables to simulate a CCXT build whose
    # unified helpers exist but whose raw cursor endpoints are unavailable.
    exchange.clob_private_get_data_orders = None  # type: ignore[method-assign]
    exchange.clob_private_get_data_trades = None  # type: ignore[method-assign]

    report = await CcxtPredictionGateway(exchange).check_compatibility()

    assert report["sdk_available"] is False
    assert report["private_history_paginated"] is False
    assert set(report["missing_private_history_methods"]) == {"orders", "trades"}
