from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from integrations.polymarket.backend_factory import VenueBackend, select_polymarket_gateway
from integrations.polymarket.hybrid_gateway import HybridClobGateway, ShadowMismatchRecorder
from integrations.polymarket.models import (
    LimitOrderRequest,
    MarketableOrderRequest,
    OrderBook,
    OrderBookLevel,
    VenueCancelResult,
    VenueOrderState,
    VenueSubmitResult,
)


def book(ask: str = "0.42") -> OrderBook:
    bids = [OrderBookLevel(price=Decimal("0.40"), size=Decimal("20"))]
    asks = [OrderBookLevel(price=Decimal(ask), size=Decimal("10"))]
    return OrderBook(
        token_id="123",
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=False,
        bids=bids,
        asks=asks,
        best_bid=bids[0].price,
        best_ask=asks[0].price,
        spread_bps=487,
        timestamp=datetime(2026, 8, 13, tzinfo=timezone.utc),
    )


class Gateway:
    def __init__(self, *, orderbook: OrderBook | None = None) -> None:
        self.get_market_info = AsyncMock(return_value={"token_id": "123", "tick_size": "0.01"})
        self.get_orderbook = AsyncMock(return_value=orderbook or book())
        self.submit_marketable_order = AsyncMock(
            return_value=VenueSubmitResult(success=True, order_id="market-order")
        )
        self.submit_limit_order = AsyncMock(
            return_value=VenueSubmitResult(success=True, order_id="limit-order")
        )
        self.get_order = AsyncMock(
            return_value=VenueOrderState(
                order_id="order",
                status="open",
                side="BUY",
                price=Decimal("0.42"),
                size=Decimal("5"),
                filled_size=Decimal("0"),
                avg_fill_price=Decimal("0"),
                created_at=None,
                raw={},
            )
        )
        self.list_open_orders = AsyncMock(return_value=[])
        self.get_fills = AsyncMock(return_value=[])
        self.cancel_order = AsyncMock(
            return_value=VenueCancelResult(success=True, order_id="order", status="canceled")
        )
        self.check_compatibility = AsyncMock(return_value={"sdk_available": True})
        self.close = AsyncMock()


@pytest.mark.asyncio
async def test_shadow_reads_return_primary_without_waiting_for_shadow_completion() -> None:
    primary = Gateway()
    shadow = Gateway()
    release = __import__("asyncio").Event()

    async def delayed(_token):
        await release.wait()
        return book()

    shadow.get_orderbook.side_effect = delayed
    hybrid = HybridClobGateway(primary, shadow)

    result = await hybrid.get_orderbook("123")

    assert result is primary.get_orderbook.return_value
    assert len(hybrid._tasks) == 1
    release.set()
    await hybrid.drain()


@pytest.mark.asyncio
async def test_shadow_mismatch_is_recorded_without_changing_primary_result() -> None:
    recorder = ShadowMismatchRecorder()
    primary = Gateway(orderbook=book("0.42"))
    shadow = Gateway(orderbook=book("0.43"))
    hybrid = HybridClobGateway(primary, shadow, mismatch_sink=recorder)

    result = await hybrid.get_orderbook("123")
    await hybrid.drain()

    assert result.best_ask == Decimal("0.42")
    assert len(recorder.items) == 1
    assert recorder.items[0].operation == "get_orderbook"
    assert recorder.items[0].error is None


@pytest.mark.asyncio
async def test_shadow_exception_is_contained_and_recorded() -> None:
    recorder = ShadowMismatchRecorder()
    primary = Gateway()
    shadow = Gateway()
    shadow.get_order.side_effect = RuntimeError("shadow down")
    hybrid = HybridClobGateway(primary, shadow, mismatch_sink=recorder)

    order = await hybrid.get_order("order")
    await hybrid.drain()

    assert order.order_id == "order"
    assert len(recorder.items) == 1
    assert recorder.items[0].error == "RuntimeError"


@pytest.mark.asyncio
async def test_submit_and_cancel_never_touch_shadow() -> None:
    primary = Gateway()
    shadow = Gateway()
    hybrid = HybridClobGateway(primary, shadow)
    market_request = MarketableOrderRequest(
        token_id="123",
        side="BUY",
        size=Decimal("5"),
        price_limit=Decimal("0.42"),
        condition_id="condition",
    )
    limit_request = LimitOrderRequest(
        token_id="123",
        side="BUY",
        size=Decimal("5"),
        price=Decimal("0.42"),
        condition_id="condition",
    )

    await hybrid.submit_marketable_order(market_request)
    await hybrid.submit_limit_order(limit_request)
    await hybrid.cancel_order("order")

    primary.submit_marketable_order.assert_awaited_once()
    primary.submit_limit_order.assert_awaited_once()
    primary.cancel_order.assert_awaited_once()
    shadow.submit_marketable_order.assert_not_awaited()
    shadow.submit_limit_order.assert_not_awaited()
    shadow.cancel_order.assert_not_awaited()


def test_backend_default_is_sdk() -> None:
    sdk = Gateway()
    assert select_polymarket_gateway(sdk_gateway=sdk) is sdk


def test_ccxt_primary_is_blocked_in_live_without_explicit_gate() -> None:
    with pytest.raises(RuntimeError, match="explicit"):
        select_polymarket_gateway(
            sdk_gateway=Gateway(),
            ccxt_gateway=Gateway(),
            backend=VenueBackend.CCXT,
            trading_mode="live",
        )


def test_ccxt_primary_can_be_used_in_paper() -> None:
    ccxt = Gateway()
    selected = select_polymarket_gateway(
        sdk_gateway=Gateway(),
        ccxt_gateway=ccxt,
        backend=VenueBackend.CCXT,
        trading_mode="paper",
    )
    assert selected is ccxt


def test_shadow_mode_is_sdk_primary() -> None:
    sdk = Gateway()
    selected = select_polymarket_gateway(
        sdk_gateway=sdk,
        ccxt_gateway=Gateway(),
        backend=VenueBackend.SDK_SHADOW_CCXT,
        trading_mode="live",
    )
    assert isinstance(selected, HybridClobGateway)
    assert selected.primary is sdk


@pytest.mark.asyncio
async def test_drain_timeout_drops_straggler_shadow_comparison() -> None:
    """Phase 4.15 — drain() must bound a hung shadow task so shutdown cannot
    hang indefinitely. A straggler comparison is dropped (counted), not
    awaited forever."""
    import asyncio

    primary = Gateway()
    shadow = Gateway()
    release = asyncio.Event()

    async def hung(_token):
        await release.wait()
        return book()

    shadow.get_orderbook = hung
    hybrid = HybridClobGateway(primary, shadow)
    await hybrid.get_orderbook("123")
    # 0.01s timeout — the hung shadow task cannot finish in time.
    await hybrid.drain(timeout_seconds=0.01)
    assert hybrid.dropped_comparisons >= 1
    release.set()  # cleanup so the task can finish before teardown


@pytest.mark.asyncio
async def test_shadow_exception_detail_is_redacted() -> None:
    """Never retain secrets embedded in a shadow transport exception."""

    recorder = ShadowMismatchRecorder()
    primary = Gateway()
    shadow = Gateway()
    shadow.get_order.side_effect = RuntimeError("private-key=do-not-retain")
    hybrid = HybridClobGateway(primary, shadow, mismatch_sink=recorder)

    await hybrid.get_order("order")
    await hybrid.drain()

    assert recorder.items[0].error == "RuntimeError"
    assert "do-not-retain" not in repr(recorder.items[0])
