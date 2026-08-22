"""Fail-closed leader-price band enforcement inside ``ExecutionService``.

The copy pipeline derives a FAK ``price_limit`` from the leader's own fill
price widened by ``execution.max_entry_price_drift_pct``. When the executable
side of the book has already moved past that band, clamping the marketable
price down to the limit only produces a FAK that cannot cross and must expire.
The band is therefore enforced as a pre-submit rejection, not a silent clamp.

``max_entry_price_drift_pct = 0`` disables the band entirely: the caller limit
stops binding and pricing falls back to ``quote * (1 +/- slippage)``.
"""

from __future__ import annotations

import time as _time
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from domain.execution import ExecutionOrderType
from services.execution.types import ExecutionRequest
from services.executionService import ExecutionService
from integrations.polymarket.models import VenueSubmitResult
from services.market_data_service import (
    MarketDataBundle,
    MarketDataSource,
)
from tests.test_execution_kill_switch_acttime import (
    _FeeSvc,
    _StubClob,
    _make_exec_config,
    _mock_order_repo,
    _success_gateway,
)

TOKEN = "t1"


def _config(drift_pct: float):
    cfg = _make_exec_config()
    cfg.execution.max_entry_price_drift_pct = drift_pct
    cfg.execution.exit_fak_max_attempts = 3
    return cfg


def _mds(*, bid: str, ask: str, tick: str = "0.01") -> AsyncMock:
    now = _time.monotonic()
    best_bid, best_ask = Decimal(bid), Decimal(ask)
    bundle = MarketDataBundle(
        token_id=TOKEN,
        quote=QuoteSnapshot(
            token_id=TOKEN,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id=TOKEN,
            bids={best_bid: Decimal("500")},
            asks={best_ask: Decimal("500")},
            best_bid=best_bid,
            best_ask=best_ask,
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id=TOKEN,
            tick_size=Decimal(tick),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )
    mds = AsyncMock()
    mds.require_fresh_data = AsyncMock(return_value=bundle)
    return mds


def _service(gateway, *, drift_pct: float, bid: str, ask: str) -> ExecutionService:
    return ExecutionService(
        _StubClob(),
        _FeeSvc(),
        _mock_order_repo(),
        config=_config(drift_pct),
        clob_gateway=gateway,
        market_data_service=_mds(bid=bid, ask=ask),
    )


def _copy_req(*, side: str, price_limit: str, leader_price: str) -> ExecutionRequest:
    return ExecutionRequest(
        user_id="u1",
        leader_id="l1",
        token_id=TOKEN,
        side=side,
        amount=Decimal("5"),
        max_slippage_bps=200,
        reason="copy_trade",
        signal_id=f"sig-{side}",
        condition_id="cond-1",
        recovery_metadata={
            "source": "copy_trade",
            "order_type": "FAK",
            "price_limit": price_limit,
            "leader_price": leader_price,
        },
        order_type=ExecutionOrderType.FAK,
        price_limit=Decimal(price_limit),
    )


def _submitted(gateway):
    return gateway.submit_marketable_order.await_args[0][0]


@pytest.mark.asyncio
async def test_buy_quote_beyond_the_band_is_refused_before_any_post() -> None:
    """Ask 0.53 against a 0.45 leader (+5 % = 0.4725) never reaches the venue."""
    gw = _success_gateway()
    svc = _service(gw, drift_pct=5.0, bid="0.52", ask="0.53")

    result = await svc.execute(_copy_req(side="BUY", price_limit="0.4725", leader_price="0.45"))

    assert result.accepted is False
    assert result.status == "price_drift_exceeded"
    gw.submit_marketable_order.assert_not_awaited()
    assert "0.53" in result.error
    assert "0.4725" in result.error
    assert "0.45" in result.error
    assert "5" in result.error


@pytest.mark.asyncio
async def test_sell_quote_beneath_the_band_is_refused_before_any_post() -> None:
    """Bid 0.50 against a 0.55 leader (-5 % = 0.5225) never reaches the venue."""
    gw = _success_gateway()
    svc = _service(gw, drift_pct=5.0, bid="0.50", ask="0.51")

    result = await svc.execute(_copy_req(side="SELL", price_limit="0.5225", leader_price="0.55"))

    assert result.accepted is False
    assert result.status == "price_drift_exceeded"
    gw.submit_marketable_order.assert_not_awaited()
    assert "0.50" in result.error
    assert "0.5225" in result.error


@pytest.mark.asyncio
async def test_buy_inside_the_band_submits_a_marketable_limit() -> None:
    """Ask 0.46 inside the 0.4725 cap submits at the marketable slippage price."""
    gw = _success_gateway()
    svc = _service(gw, drift_pct=5.0, bid="0.45", ask="0.46")

    result = await svc.execute(_copy_req(side="BUY", price_limit="0.4725", leader_price="0.45"))

    assert result.status != "price_drift_exceeded"
    gw.submit_marketable_order.assert_awaited()
    submitted = _submitted(gw)
    # quote * (1 + 200bps) = 0.4692, ceiled to the 0.01 tick = 0.47,
    # still under the 0.4725 band cap.
    assert submitted.price_limit == Decimal("0.47")
    assert submitted.price_limit >= Decimal("0.46")


@pytest.mark.asyncio
async def test_zero_drift_disables_the_band_and_the_caller_clamp() -> None:
    """With the band off, pricing is pure quote * (1 + slippage)."""
    gw = _success_gateway()
    svc = _service(gw, drift_pct=0.0, bid="0.52", ask="0.53")

    result = await svc.execute(_copy_req(side="BUY", price_limit="0.45", leader_price="0.45"))

    assert result.status != "price_drift_exceeded"
    gw.submit_marketable_order.assert_awaited()
    # 0.53 * 1.02 = 0.5406 -> ceiled to the 0.01 tick = 0.55, above the
    # caller limit, which no longer binds.
    assert _submitted(gw).price_limit == Decimal("0.55")


def _bundle(*, bid: str, ask: str, tick: str = "0.01") -> MarketDataBundle:
    """Build one WSS-fresh bundle with the given top of book."""
    now = _time.monotonic()
    best_bid, best_ask = Decimal(bid), Decimal(ask)
    return MarketDataBundle(
        token_id=TOKEN,
        quote=QuoteSnapshot(
            token_id=TOKEN,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id=TOKEN,
            bids={best_bid: Decimal("500")},
            asks={best_ask: Decimal("500")},
            best_bid=best_bid,
            best_ask=best_ask,
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id=TOKEN,
            tick_size=Decimal(tick),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )


@pytest.mark.asyncio
async def test_repost_stops_when_the_book_walks_out_of_the_band() -> None:
    """A partially filled forced exit must not repost beyond the band floor."""
    gw = _success_gateway()
    gw.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=True,
            order_id="ord-1",
            status="matched",
            raw={"size_matched": "2", "avg_price": "0.53"},
        )
    )
    mds = AsyncMock()
    bundles = [_bundle(bid="0.53", ask="0.54"), _bundle(bid="0.50", ask="0.51")]
    mds.require_fresh_data = AsyncMock(
        side_effect=lambda *_a, **_k: bundles[
            min(len(mds.require_fresh_data.await_args_list) - 1, 1)
        ]
    )
    svc = ExecutionService(
        _StubClob(),
        _FeeSvc(),
        _mock_order_repo(),
        config=_config(5.0),
        clob_gateway=gw,
        market_data_service=mds,
    )
    req = ExecutionRequest(
        user_id="u1",
        leader_id="l1",
        token_id=TOKEN,
        side="SELL",
        amount=Decimal("10"),
        max_slippage_bps=200,
        reason="risk_exit",
        signal_id="sig-exit",
        condition_id="cond-1",
        recovery_metadata={
            "source": "risk_exit",
            "order_type": "FAK",
            "price_limit": "0.5225",
            "leader_price": "0.55",
        },
        order_type=ExecutionOrderType.FAK,
        price_limit=Decimal("0.5225"),
    )

    await svc.execute(req)

    assert gw.submit_marketable_order.await_count == 1


@pytest.mark.asyncio
async def test_repost_continues_while_the_book_stays_inside_the_band() -> None:
    """Control for the repost gate: an in-band second quote is still reposted."""
    gw = _success_gateway()
    gw.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=True,
            order_id="ord-1",
            status="matched",
            raw={"size_matched": "2", "avg_price": "0.53"},
        )
    )
    mds = AsyncMock()
    bundles = [_bundle(bid="0.53", ask="0.54"), _bundle(bid="0.53", ask="0.54")]
    mds.require_fresh_data = AsyncMock(
        side_effect=lambda *_a, **_k: bundles[
            min(len(mds.require_fresh_data.await_args_list) - 1, 1)
        ]
    )
    svc = ExecutionService(
        _StubClob(),
        _FeeSvc(),
        _mock_order_repo(),
        config=_config(5.0),
        clob_gateway=gw,
        market_data_service=mds,
    )
    req = ExecutionRequest(
        user_id="u1",
        leader_id="l1",
        token_id=TOKEN,
        side="SELL",
        amount=Decimal("10"),
        max_slippage_bps=200,
        reason="risk_exit",
        signal_id="sig-exit",
        condition_id="cond-1",
        recovery_metadata={
            "source": "risk_exit",
            "order_type": "FAK",
            "price_limit": "0.5225",
            "leader_price": "0.55",
        },
        order_type=ExecutionOrderType.FAK,
        price_limit=Decimal("0.5225"),
    )

    await svc.execute(req)

    assert gw.submit_marketable_order.await_count > 1
