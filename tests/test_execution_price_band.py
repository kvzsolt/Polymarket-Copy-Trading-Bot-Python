"""Copied-price FAK behavior and protective-exit band regressions."""

from __future__ import annotations

import time as _time
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from pmcopy.cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from pmcopy.domain.execution import ExecutionOrderType
from pmcopy.services.execution.types import ExecutionRequest
from pmcopy.services.execution.execution_service import ExecutionService
from pmcopy.integrations.polymarket.models import VenueSubmitResult
from pmcopy.services.market_data.market_data_service import (
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


def _config(
    drift_pct: float,
    *,
    min_share_price: float = 0.0,
    max_share_price: float = 0.0,
):
    cfg = _make_exec_config()
    cfg.execution.max_entry_price_drift_pct = drift_pct
    cfg.execution.min_share_price = min_share_price
    cfg.execution.max_share_price = max_share_price
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


def _service(
    gateway,
    *,
    drift_pct: float,
    bid: str,
    ask: str,
    min_share_price: float = 0.0,
    max_share_price: float = 0.0,
) -> ExecutionService:
    return ExecutionService(
        _StubClob(),
        _FeeSvc(),
        _mock_order_repo(),
        config=_config(
            drift_pct,
            min_share_price=min_share_price,
            max_share_price=max_share_price,
        ),
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
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        neg_risk=False,
    )


def _submitted(gateway):
    return gateway.submit_marketable_order.await_args[0][0]


@pytest.mark.asyncio
async def test_buy_copy_ignores_book_quote_and_submits_copied_limit() -> None:
    """A moved ask cannot reprice a one-shot tolerance-bounded BUY."""
    gw = _success_gateway()
    svc = _service(gw, drift_pct=5.0, bid="0.52", ask="0.53")

    result = await svc.execute(_copy_req(side="BUY", price_limit="0.4725", leader_price="0.45"))

    assert result.status != "price_drift_exceeded"
    gw.submit_marketable_order.assert_awaited_once()
    # The BUY ceiling remains within the caller bound after tick alignment.
    assert _submitted(gw).price_limit == Decimal("0.47")
    svc._market_data_service.require_fresh_data.assert_not_awaited()


@pytest.mark.asyncio
async def test_sell_copy_ignores_book_quote_and_submits_copied_limit() -> None:
    """A moved bid cannot reprice a one-shot tolerance-bounded SELL."""
    gw = _success_gateway()
    svc = _service(gw, drift_pct=5.0, bid="0.50", ask="0.51")

    result = await svc.execute(_copy_req(side="SELL", price_limit="0.5225", leader_price="0.55"))

    assert result.status != "price_drift_exceeded"
    gw.submit_marketable_order.assert_awaited_once()
    # The SELL floor rounds up so execution cannot undercut the caller bound.
    assert _submitted(gw).price_limit == Decimal("0.53")
    svc._market_data_service.require_fresh_data.assert_not_awaited()


@pytest.mark.asyncio
async def test_buy_copy_does_not_add_local_slippage_to_the_limit() -> None:
    """The copied BUY limit remains unchanged by local slippage settings."""
    gw = _success_gateway()
    svc = _service(gw, drift_pct=5.0, bid="0.45", ask="0.46")

    result = await svc.execute(_copy_req(side="BUY", price_limit="0.4725", leader_price="0.45"))

    assert result.status != "price_drift_exceeded"
    gw.submit_marketable_order.assert_awaited()
    submitted = _submitted(gw)
    assert submitted.price_limit == Decimal("0.47")


@pytest.mark.asyncio
async def test_executor_does_not_rederive_the_pipeline_limit() -> None:
    """The executor submits the already-derived limit exactly once."""
    gw = _success_gateway()
    svc = _service(gw, drift_pct=10.0, bid="0.52", ask="0.53")

    result = await svc.execute(_copy_req(side="BUY", price_limit="0.4725", leader_price="0.45"))

    assert result.status != "price_drift_exceeded"
    gw.submit_marketable_order.assert_awaited()
    assert _submitted(gw).price_limit == Decimal("0.47")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("leader_price", "price_limit", "expected_fragment"),
    [
        ("0.09", "0.10", "below min_share_price"),
        ("0.91", "0.90", "above max_share_price"),
    ],
)
async def test_copy_buy_rejects_out_of_range_leader_reference(
    leader_price: str,
    price_limit: str,
    expected_fragment: str,
) -> None:
    """A clamped limit must not hide an already-forbidden leader reference."""
    gw = _success_gateway()
    svc = _service(
        gw,
        drift_pct=5.0,
        bid="0.50",
        ask="0.51",
        min_share_price=0.10,
        max_share_price=0.90,
    )

    result = await svc.execute(
        _copy_req(side="BUY", price_limit=price_limit, leader_price=leader_price)
    )

    assert result.accepted is False
    assert result.status == "rejected"
    assert expected_fragment in (result.error or "")
    gw.submit_marketable_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_copy_buy_retains_quote_price_boundary_reason() -> None:
    """Leader-reference wording must stay scoped to copied FAK metadata."""
    gw = _success_gateway()
    svc = _service(
        gw,
        drift_pct=5.0,
        bid="0.08",
        ask="0.09",
        min_share_price=0.10,
        max_share_price=0.90,
    )
    request = ExecutionRequest(
        user_id="u1",
        leader_id="l1",
        token_id=TOKEN,
        side="BUY",
        amount=Decimal("5"),
        max_slippage_bps=200,
        reason="manual",
        signal_id="sig-manual",
        condition_id="cond-1",
    )

    result = await svc.execute(request)

    assert result.accepted is False
    assert result.error == "price below min_share_price (0.09 < 0.1)"
    gw.submit_marketable_order.assert_not_awaited()


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
