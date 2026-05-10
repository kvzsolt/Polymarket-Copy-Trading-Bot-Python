"""Phase 6 tests: tick quantization, min_order_size gate, neg_risk propagation, V2 meta methods."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from utils.createClobClient import ClobClientWrapper, MarketMeta, quantize_to_tick


def _dummy_wallet():
    from config.env import SignatureType, WalletConfig

    return WalletConfig(
        private_key="0x" + "a" * 64,
        chain_id=137,
        signature_type=SignatureType.EOA,
        funder_address="0x" + "0" * 40,
        polygon_rpc_url="http://localhost",
    )


class TestQuantizeToTick:
    def test_already_aligned(self):
        assert quantize_to_tick(Decimal("0.50"), Decimal("0.01")) == Decimal("0.50")

    def test_unaligned_floored(self):
        assert quantize_to_tick(Decimal("0.556"), Decimal("0.01")) == Decimal("0.55")

    def test_small_tick(self):
        assert quantize_to_tick(Decimal("0.5555"), Decimal("0.001")) == Decimal("0.555")

    def test_zero_tick_passthrough(self):
        price = Decimal("0.556")
        assert quantize_to_tick(price, Decimal("0")) == price

    def test_negative_tick_passthrough(self):
        price = Decimal("0.556")
        assert quantize_to_tick(price, Decimal("-0.01")) == price

    def test_tick_01(self):
        assert quantize_to_tick(Decimal("0.999"), Decimal("0.1")) == Decimal("0.9")

    def test_boundary_exact_multiple(self):
        assert quantize_to_tick(Decimal("0.600"), Decimal("0.01")) == Decimal("0.60")

    def test_buy_rounds_up(self):
        assert quantize_to_tick(Decimal("0.551"), Decimal("0.01"), side="BUY") == Decimal("0.56")

    def test_sell_rounds_down(self):
        assert quantize_to_tick(Decimal("0.559"), Decimal("0.01"), side="SELL") == Decimal("0.55")

    def test_buy_exact_stays(self):
        assert quantize_to_tick(Decimal("0.55"), Decimal("0.01"), side="BUY") == Decimal("0.55")


class TestMinOrderSizeGate:
    def _make_gateway(self):
        from clients.order_gateway import OrderGateway

        wrapper = MagicMock()
        wrapper.create_market_order = AsyncMock(return_value=object())
        wrapper.post_signed_order = AsyncMock(return_value={"success": True, "orderID": "x"})
        return OrderGateway(wrapper)

    @pytest.mark.asyncio
    async def test_below_min_rejected_without_exchange_call(self):
        gw = self._make_gateway()
        result = await gw.post_market_order(
            token_id="tok", side="BUY", amount=3.0, max_price=0.5,
            tick_size="0.01", neg_risk=False, min_order_size=5.0,
        )
        assert result["success"] is False
        assert "size_below_min" in result["error"]
        assert result["non_retryable"] is True
        gw._wrapper.create_market_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_exactly_at_min_allowed(self):
        gw = self._make_gateway()
        result = await gw.post_market_order(
            token_id="tok", side="BUY", amount=5.0, max_price=0.5,
            tick_size="0.01", neg_risk=False, min_order_size=5.0,
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_above_min_allowed(self):
        gw = self._make_gateway()
        result = await gw.post_market_order(
            token_id="tok", side="BUY", amount=10.0, max_price=0.5,
            tick_size="0.01", neg_risk=False, min_order_size=5.0,
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_zero_min_always_passes(self):
        gw = self._make_gateway()
        result = await gw.post_market_order(
            token_id="tok", side="BUY", amount=0.001, max_price=0.5,
            tick_size="0.01", neg_risk=False, min_order_size=0.0,
        )
        assert result["success"] is True


class TestNegRiskPropagation:
    def _make_gateway(self):
        from clients.order_gateway import OrderGateway

        wrapper = MagicMock()
        wrapper.create_market_order = AsyncMock(return_value=object())
        wrapper.post_signed_order = AsyncMock(return_value={"success": True})
        return OrderGateway(wrapper), wrapper

    @pytest.mark.asyncio
    async def test_neg_risk_true_forwarded(self):
        gw, wrapper = self._make_gateway()
        await gw.post_market_order(
            token_id="tok", side="BUY", amount=10.0, max_price=0.5,
            tick_size="0.01", neg_risk=True,
        )
        assert wrapper.create_market_order.call_args.kwargs["neg_risk"] is True

    @pytest.mark.asyncio
    async def test_neg_risk_false_forwarded(self):
        gw, wrapper = self._make_gateway()
        await gw.post_market_order(
            token_id="tok", side="BUY", amount=10.0, max_price=0.5,
            tick_size="0.01", neg_risk=False,
        )
        assert wrapper.create_market_order.call_args.kwargs["neg_risk"] is False

    @pytest.mark.asyncio
    async def test_tick_size_forwarded(self):
        gw, wrapper = self._make_gateway()
        await gw.post_market_order(
            token_id="tok", side="BUY", amount=10.0, max_price=0.5,
            tick_size="0.001", neg_risk=False,
        )
        assert wrapper.create_market_order.call_args.kwargs["tick_size"] == "0.001"


class TestInvalidOrderTypeRejected:
    def test_unknown_order_type_raises(self):
        from clients.order_gateway import _resolve_order_type

        with pytest.raises(ValueError, match="Unsupported order_type"):
            _resolve_order_type("NONEXISTENT_TYPE")


class TestGetMarketMetaV2Methods:
    def _make_wrapper(self, *, tick_size_v2=None, neg_risk_v2=None, ob_tick="0.01",
                      ob_neg_risk=False, tick_size_raises=False, neg_risk_raises=False):
        client = MagicMock()
        if tick_size_raises:
            client.get_tick_size.side_effect = RuntimeError("V2 unavailable")
        else:
            client.get_tick_size.return_value = tick_size_v2
        if neg_risk_raises:
            client.get_neg_risk.side_effect = RuntimeError("V2 unavailable")
        else:
            client.get_neg_risk.return_value = neg_risk_v2

        ob = MagicMock()
        ob.tick_size = ob_tick
        ob.neg_risk = ob_neg_risk
        ob.bids = []
        ob.asks = []
        ob.min_order_size = "5"
        client.get_order_book.return_value = ob

        return ClobClientWrapper(client, _dummy_wallet())

    @pytest.mark.asyncio
    async def test_v2_tick_size_overrides_orderbook(self):
        wrapper = self._make_wrapper(tick_size_v2="0.001", neg_risk_v2=False, ob_tick="0.01")
        meta = await wrapper.get_market_meta("tok")
        assert meta.tick_size == Decimal("0.001")

    @pytest.mark.asyncio
    async def test_fallback_to_orderbook_when_v2_tick_fails(self):
        wrapper = self._make_wrapper(tick_size_raises=True, neg_risk_v2=False, ob_tick="0.01")
        meta = await wrapper.get_market_meta("tok")
        assert meta.tick_size == Decimal("0.01")

    @pytest.mark.asyncio
    async def test_price_quantized_in_create_market_order(self):
        from py_clob_client_v2.clob_types import OrderType

        client = MagicMock()
        client.create_market_order.return_value = object()
        wrapper = ClobClientWrapper(client, _dummy_wallet())

        await wrapper.create_market_order(
            token_id="tok", side="BUY", amount=10.0, max_price=0.556,
            tick_size="0.01", neg_risk=False, order_type=OrderType.FOK,
        )

        args_passed = client.create_market_order.call_args[0][0]
        # BUY side now rounds UP: 0.556 -> 0.56
        assert float(args_passed.price) == pytest.approx(0.56, rel=1e-9)
