"""Phase 6 tests: V2 market metadata, tick-size quantization, and order options.

Validates that:
- quantize_to_tick floors prices correctly for all known tick sizes
- ClobClientWrapper.get_market_meta propagates tick_size and neg_risk from V2 SDK
- OrderGateway.post_market_order rejects orders below min_order_size
- neg_risk flag is forwarded into create_market_order options
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from utils.createClobClient import quantize_to_tick


# ---------------------------------------------------------------------------
# Tick-size quantization
# ---------------------------------------------------------------------------


class TestQuantizeToTick:
    """Validate that quantize_to_tick floors prices to the nearest tick boundary."""

    @pytest.mark.parametrize(
        "price, tick_size, expected",
        [
            # 0.01 tick — basic
            (Decimal("0.523"), Decimal("0.01"), Decimal("0.52")),
            (Decimal("0.50"), Decimal("0.01"), Decimal("0.50")),
            (Decimal("0.999"), Decimal("0.01"), Decimal("0.99")),
            # 0.1 tick
            (Decimal("0.75"), Decimal("0.1"), Decimal("0.7")),
            (Decimal("0.3"), Decimal("0.1"), Decimal("0.3")),
            # 0.001 tick
            (Decimal("0.1234"), Decimal("0.001"), Decimal("0.123")),
            (Decimal("0.450"), Decimal("0.001"), Decimal("0.450")),
            # 0.0001 tick
            (Decimal("0.12345"), Decimal("0.0001"), Decimal("0.1234")),
        ],
    )
    def test_quantize_floors_correctly(
        self, price: Decimal, tick_size: Decimal, expected: Decimal
    ) -> None:
        assert quantize_to_tick(price, tick_size) == expected

    def test_quantize_zero_tick_returns_price_unchanged(self) -> None:
        price = Decimal("0.523")
        assert quantize_to_tick(price, Decimal("0")) == price

    def test_quantize_already_aligned_stays_same(self) -> None:
        assert quantize_to_tick(Decimal("0.52"), Decimal("0.01")) == Decimal("0.52")


# ---------------------------------------------------------------------------
# V2 metadata propagation via get_market_meta
# ---------------------------------------------------------------------------


class TestV2MetadataPropagation:
    """Validate that get_market_meta uses V2 SDK methods for tick_size/neg_risk."""

    @pytest.fixture()
    def mock_wrapper(self) -> Any:
        """Build a ClobClientWrapper with a mocked V2 ClobClient."""
        from utils.createClobClient import ClobClientWrapper
        from config.env import WalletConfig, SignatureType

        client = MagicMock()
        client.get_tick_size = MagicMock(return_value="0.01")
        client.get_neg_risk = MagicMock(return_value=True)
        client.get_order_book = MagicMock(
            return_value=MagicMock(
                bids=[MagicMock(price="0.45")],
                asks=[MagicMock(price="0.55")],
                tick_size="0.01",
                min_order_size="5",
                neg_risk=False,
            )
        )

        cfg = WalletConfig(
            private_key="0x" + "ab" * 32,
            chain_id=137,
            signature_type=SignatureType.EOA,
            funder_address=None,
            polygon_rpc_url="http://rpc",
        )
        return ClobClientWrapper(client, wallet_config=cfg)

    @pytest.mark.asyncio
    async def test_tick_size_from_v2_sdk(self, mock_wrapper: Any) -> None:
        meta = await mock_wrapper.get_market_meta("token_abc")
        assert meta.tick_size == Decimal("0.01")
        mock_wrapper.client.get_tick_size.assert_called_once_with("token_abc")

    @pytest.mark.asyncio
    async def test_neg_risk_from_v2_sdk(self, mock_wrapper: Any) -> None:
        meta = await mock_wrapper.get_market_meta("token_abc")
        assert meta.neg_risk is True
        mock_wrapper.client.get_neg_risk.assert_called_once_with("token_abc")

    @pytest.mark.asyncio
    async def test_neg_risk_false_propagated(self, mock_wrapper: Any) -> None:
        mock_wrapper.client.get_neg_risk.return_value = False
        meta = await mock_wrapper.get_market_meta("token_xyz")
        assert meta.neg_risk is False

    @pytest.mark.asyncio
    async def test_fallback_on_tick_size_failure(self, mock_wrapper: Any) -> None:
        mock_wrapper.client.get_tick_size.side_effect = Exception("timeout")
        meta = await mock_wrapper.get_market_meta("token_abc")
        # Should still produce metadata, falling back to orderbook tick_size
        assert meta.token_id == "token_abc"
        assert meta.tick_size is not None


# ---------------------------------------------------------------------------
# OrderGateway min-size gate and V2 option propagation
# ---------------------------------------------------------------------------


class TestOrderGatewayV2Options:
    """Validate OrderGateway.post_market_order with V2 order options."""

    @pytest.fixture()
    def gateway(self) -> Any:
        from clients.order_gateway import OrderGateway
        from utils.createClobClient import ClobClientWrapper

        wrapper = MagicMock(spec=ClobClientWrapper)
        wrapper.create_market_order = AsyncMock(return_value={"orderID": "ord1"})
        wrapper.post_signed_order = AsyncMock(
            return_value={"success": True, "orderID": "ord1", "data": {}}
        )
        gw = OrderGateway(wrapper)
        return gw

    @pytest.mark.asyncio
    async def test_rejects_below_min_order_size(self, gateway: Any) -> None:
        result = await gateway.post_market_order(
            token_id="t1",
            side="BUY",
            amount=0.5,
            max_price=0.50,
            tick_size="0.01",
            neg_risk=False,
            min_order_size=5.0,
        )
        assert result["success"] is False
        assert "size_below_min" in result["error"]
        assert result["non_retryable"] is True

    @pytest.mark.asyncio
    async def test_passes_tick_size_and_neg_risk(self, gateway: Any) -> None:
        await gateway.post_market_order(
            token_id="t1",
            side="BUY",
            amount=10.0,
            max_price=0.50,
            tick_size="0.01",
            neg_risk=True,
            min_order_size=5.0,
        )
        gateway._wrapper.create_market_order.assert_awaited_once_with(
            token_id="t1",
            side="BUY",
            amount=10.0,
            max_price=0.50,
            tick_size="0.01",
            neg_risk=True,
            order_type=gateway._wrapper.create_market_order.call_args[1]["order_type"],
        )

    @pytest.mark.asyncio
    async def test_neg_risk_false_propagated(self, gateway: Any) -> None:
        await gateway.post_market_order(
            token_id="t2",
            side="SELL",
            amount=20.0,
            max_price=0.80,
            tick_size="0.1",
            neg_risk=False,
        )
        call_kwargs = gateway._wrapper.create_market_order.call_args[1]
        assert call_kwargs["neg_risk"] is False
        assert call_kwargs["tick_size"] == "0.1"
