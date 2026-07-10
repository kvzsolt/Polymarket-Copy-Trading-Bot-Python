"""Phase 3 tests — concrete ``clients/`` adapters honour their ports.

Every adapter is exercised with ``AsyncMock`` stand-ins for the underlying
SDK / HTTP session. No network I/O, no DB writes, no ``ENV`` reads.

Goals:

* Each adapter is registered as a subclass of its port.
* Delegation contract: adapter methods forward to the underlying primitive
  with the expected arguments and return the underlying response.
* Error translation: analytics adapter swallows HTTP failures and returns
  an empty list (so the service layer never crashes on rate limits).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from clients.analytics_client import AnalyticsClient
from clients.balance_client import BalanceClient
from clients.market_client import PolymarketClobClient
from clients.network.http_client import AsyncHttpClient, HttpRetryPolicy
from clients.order_gateway import OrderGateway
from domain.ports.clients import (
    AbstractAnalyticsClient,
    AbstractBalanceClient,
    AbstractMarketClient,
    AbstractOrderGateway,
)


# ---------------------------------------------------------------------------
# ABC registration
# ---------------------------------------------------------------------------


class TestClientAbcRegistration:
    def test_market_client(self) -> None:
        assert issubclass(PolymarketClobClient, AbstractMarketClient)

    def test_balance_client(self) -> None:
        assert issubclass(BalanceClient, AbstractBalanceClient)

    def test_order_gateway(self) -> None:
        assert issubclass(OrderGateway, AbstractOrderGateway)

    def test_analytics_client(self) -> None:
        assert issubclass(AnalyticsClient, AbstractAnalyticsClient)


# ---------------------------------------------------------------------------
# AsyncHttpClient — thin passthrough to aiohttp session
# ---------------------------------------------------------------------------


class TestHttpClientDelegation:
    @pytest.mark.asyncio
    async def test_get_json_forwards(self) -> None:
        mock_resp = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_resp.json = AsyncMock(return_value={"ok": True})
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        session = MagicMock()
        session.request = MagicMock(return_value=mock_resp)

        client = AsyncHttpClient(session, retry_policy=HttpRetryPolicy(attempts=1))
        result = await client.get_json("https://x.example", silent=True, params={"a": 1})

        assert result == {"ok": True}
        session.request.assert_called_once()
        args, kwargs = session.request.call_args
        assert args == ("GET", "https://x.example")
        assert kwargs.get("params") == {"a": 1}

    @pytest.mark.asyncio
    async def test_post_json_forwards(self) -> None:
        mock_resp = MagicMock()
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_resp.json = AsyncMock(return_value={"id": 1})
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        session = MagicMock()
        session.request = MagicMock(return_value=mock_resp)

        client = AsyncHttpClient(session, retry_policy=HttpRetryPolicy(attempts=1))
        result = await client.post_json("https://x.example", json_body={"a": 1})

        assert result == {"id": 1}
        session.request.assert_called_once()
        args, kwargs = session.request.call_args
        assert args == ("POST", "https://x.example")
        assert kwargs.get("json") == {"a": 1}


# ---------------------------------------------------------------------------
# PolymarketClobClient — delegates to wrapper + sync SDK
# ---------------------------------------------------------------------------


class TestPolymarketClobClient:
    @pytest.mark.asyncio
    async def test_get_order_book_delegates(self) -> None:
        wrapper = MagicMock()
        wrapper.get_order_book = AsyncMock(return_value={"bids": [], "asks": []})
        client = PolymarketClobClient(wrapper)
        book = await client.get_order_book("tok1")
        assert book == {"bids": [], "asks": []}
        wrapper.get_order_book.assert_awaited_once_with("tok1")

    @pytest.mark.asyncio
    async def test_get_market_meta_delegates(self) -> None:
        wrapper = MagicMock()
        wrapper.get_market_meta = AsyncMock(return_value=SimpleNamespace(token_id="tok1"))
        client = PolymarketClobClient(wrapper)
        meta = await client.get_market_meta("cond-1")
        assert meta.token_id == "tok1"
        wrapper.get_market_meta.assert_awaited_once_with("cond-1")

    @pytest.mark.asyncio
    async def test_get_last_trade_price_uses_async_sdk(self) -> None:
        sdk = MagicMock()
        sdk.get_last_trade_price = AsyncMock(return_value=Decimal("0.5"))
        wrapper = MagicMock()
        wrapper.public_client = sdk
        client = PolymarketClobClient(wrapper)
        result = await client.get_last_trade_price("tok1")
        assert result == Decimal("0.5")
        sdk.get_last_trade_price.assert_awaited_once_with(token_id="tok1")

    @pytest.mark.asyncio
    async def test_get_midpoint_uses_async_sdk(self) -> None:
        sdk = MagicMock()
        sdk.get_midpoint = AsyncMock(return_value=Decimal("0.5"))
        wrapper = MagicMock()
        wrapper.public_client = sdk
        client = PolymarketClobClient(wrapper)
        result = await client.get_midpoint("tok1")
        assert result == Decimal("0.5")
        sdk.get_midpoint.assert_awaited_once_with(token_id="tok1")


# ---------------------------------------------------------------------------
# BalanceClient — ENV-free wallet balance
# ---------------------------------------------------------------------------


class TestBalanceClient:
    @pytest.mark.asyncio
    async def test_empty_address_short_circuits_usdc(self) -> None:
        client = BalanceClient(rpc_url="http://rpc", usdc_contract_address="0x" + "a" * 40)
        assert await client.get_usdc_balance("") is None

    @pytest.mark.asyncio
    async def test_empty_address_short_circuits_native(self) -> None:
        client = BalanceClient(rpc_url="http://rpc", usdc_contract_address="0x" + "a" * 40)
        assert await client.get_native_balance_wei("") is None

    @pytest.mark.asyncio
    async def test_usdc_invalid_contract_returns_none(self) -> None:
        client = BalanceClient(rpc_url="http://rpc", usdc_contract_address="not-a-hex")
        # Invalid checksum address path is caught inside the sync helper.
        assert await client.get_usdc_balance("0x" + "b" * 40) is None


# ---------------------------------------------------------------------------
# OrderGateway — wraps the CLOB wrapper's post_order
# ---------------------------------------------------------------------------


class TestOrderGateway:
    @pytest.mark.asyncio
    async def test_post_order_builds_args_and_decorates_response(self) -> None:
        from integrations.polymarket.models import VenueSubmitResult

        gateway = OrderGateway(MagicMock())
        gateway._gateway.submit_limit_order = AsyncMock(
            return_value=VenueSubmitResult(success=True, order_id="x1", status="live")
        )
        result = await gateway.post_order(
            token_id="tok1",
            side="BUY",
            size=10.0,
            price=0.5,
            order_type="GTC",
            idempotency_key="idem-1",
            extra={"condition_id": "cond-1"},
        )
        assert result["success"] is True
        assert result["idempotency_key"] == "idem-1"
        assert result["order_type"] == "GTC"
        request = gateway._gateway.submit_limit_order.await_args.args[0]
        assert request.condition_id == "cond-1"

    @pytest.mark.asyncio
    async def test_cancel_order_uses_typed_gateway(self) -> None:
        from integrations.polymarket.models import VenueCancelResult

        gateway = OrderGateway(MagicMock())
        gateway._gateway.cancel_order = AsyncMock(
            return_value=VenueCancelResult(success=True, order_id="ord-1", status="cancelled")
        )
        result = await gateway.cancel_order("ord-1")
        assert result["success"] is True
        gateway._gateway.cancel_order.assert_awaited_once_with("ord-1")

    @pytest.mark.asyncio
    async def test_market_order_requires_positive_venue_minimum(self) -> None:
        gateway = OrderGateway(MagicMock())

        with pytest.raises(ValueError, match="min_order_size must be positive"):
            await gateway.post_market_order(
                token_id="tok1",
                side="BUY",
                amount=5.0,
                max_price=0.5,
                tick_size="0.01",
                neg_risk=False,
                min_order_size=0.0,
            )


# ---------------------------------------------------------------------------
# AnalyticsClient — filtering and graceful failure
# ---------------------------------------------------------------------------


class TestAnalyticsClient:
    @pytest.mark.asyncio
    async def test_filters_by_thresholds(self) -> None:
        http = MagicMock()
        http.get_json = AsyncMock(
            return_value={
                "data": [
                    {
                        "trader": "0x" + "a" * 40,
                        "overall_gain": 5000,
                        "win_rate": 0.7,
                        "total_positions": 30,
                    },
                    {
                        "trader": "0x" + "b" * 40,
                        "overall_gain": 100,  # below min_pnl
                        "win_rate": 0.7,
                        "total_positions": 30,
                    },
                    {
                        "trader": "0x" + "c" * 40,
                        "overall_gain": 5000,
                        "win_rate": 0.2,  # below min_win_rate
                        "total_positions": 30,
                    },
                ]
            }
        )
        client = AnalyticsClient(http=http)
        rows = await client.fetch_top_traders(
            limit=10,
            min_pnl=1000.0,
            min_win_rate=0.5,
            min_positions=10,
        )
        assert len(rows) == 1
        assert rows[0]["trader"].endswith("a" * 40)

    @pytest.mark.asyncio
    async def test_http_failure_returns_empty(self) -> None:
        http = MagicMock()
        http.get_json = AsyncMock(side_effect=RuntimeError("boom"))
        client = AnalyticsClient(http=http)
        rows = await client.fetch_top_traders(
            limit=10,
            min_pnl=1000.0,
            min_win_rate=0.5,
            min_positions=10,
        )
        assert rows == []

    @pytest.mark.asyncio
    async def test_missing_data_key_returns_empty(self) -> None:
        http = MagicMock()
        http.get_json = AsyncMock(return_value={"unexpected": True})
        client = AnalyticsClient(http=http)
        rows = await client.fetch_top_traders(
            limit=10, min_pnl=0.0, min_win_rate=0.0, min_positions=0
        )
        assert rows == []
