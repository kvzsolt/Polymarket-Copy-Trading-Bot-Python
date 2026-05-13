"""Release-gate: NearOneExitService replaces on-chain redemption (issue-78).

Verifies the dwell-clock + SELL-emit pipeline behaves identically to a
protective exit, without touching CTF.redeemPositions or the deleted
``services.redemptionService`` / ``services.autoRedeem`` modules.
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from cache.market_state_store import QuoteSnapshot, TickSnapshot
from services.near_one_exit import NearOneExitService, _DwellState


def _make_config(*, threshold: str = "0.99", dwell: int = 60, check: int = 1) -> SimpleNamespace:
    return SimpleNamespace(
        near_one_exit=SimpleNamespace(
            enable_near_one_exit=True,
            threshold=Decimal(threshold),
            dwell_seconds=dwell,
            check_interval=check,
        ),
        execution=SimpleNamespace(slippage_tolerance=2.0),
    )


def _make_position(condition_id: str, token_id: str, size: float) -> SimpleNamespace:
    return SimpleNamespace(
        condition_id=condition_id,
        asset=token_id,
        size_tokens=size,
        entry_price=0.5,
        leader_wallet="0xleader",
        event_slug="event-x",
    )


def _make_service(
    *,
    config: SimpleNamespace,
    positions: dict,
    quote_midpoint: Decimal | None,
    tick_min_order_size: Decimal = Decimal("5"),
    accepted: bool = True,
) -> tuple[NearOneExitService, AsyncMock, MagicMock]:
    pm = MagicMock()
    pm.open_positions = positions
    exec_svc = MagicMock()
    exec_svc.execute = AsyncMock(
        return_value=SimpleNamespace(
            accepted=accepted,
            filled_amount=Decimal("0"),
            order_id=None,
            avg_price=None,
            rejection_reason="venue_reject",
            error=None,
        )
    )
    mds = MagicMock()
    if quote_midpoint is not None:
        quote = QuoteSnapshot(
            token_id="tok-1",
            best_bid=quote_midpoint,
            best_ask=quote_midpoint,
            last_trade_price=None,
            source="test",
            monotonic_ts=time.monotonic(),
        )
    else:
        quote = None
    mds.get_quote_or_none = AsyncMock(return_value=quote)
    mds.get_tick_or_none = AsyncMock(
        return_value=TickSnapshot(
            token_id="tok-1",
            tick_size=Decimal("0.01"),
            min_order_size=tick_min_order_size,
            neg_risk=False,
            source="rest_snapshot",
            monotonic_ts=time.monotonic(),
        )
    )
    svc = NearOneExitService(
        position_manager=pm,
        execution_service=exec_svc,
        market_data_service=mds,
        config=config,
        readiness=MagicMock(),
        user_id="user-0",
        funder_address="0xfunder",
    )
    return svc, exec_svc.execute, mds


# --- observe_price unit-ish behavior ----------------------------------------


def test_observe_price_arms_above_threshold() -> None:
    cfg = _make_config()
    svc, _, _ = _make_service(
        config=cfg,
        positions={},
        quote_midpoint=Decimal("0.995"),
    )
    svc.observe_price("tok-1", Decimal("0.995"), ts=100.0)
    assert svc._dwell["tok-1"].first_seen_above_ts == 100.0


def test_observe_price_clears_below_threshold() -> None:
    cfg = _make_config()
    svc, _, _ = _make_service(
        config=cfg,
        positions={},
        quote_midpoint=Decimal("0.5"),
    )
    svc._dwell["tok-1"] = _DwellState(first_seen_above_ts=10.0)
    svc.observe_price("tok-1", Decimal("0.5"), ts=20.0)
    assert svc._dwell["tok-1"].first_seen_above_ts is None


# --- _tick: threshold not crossed → no SELL ---------------------------------


@pytest.mark.asyncio
async def test_tick_no_sell_when_below_threshold() -> None:
    cfg = _make_config()
    positions = {"k": _make_position("0xcond", "tok-1", 10.0)}
    svc, execute, _ = _make_service(
        config=cfg,
        positions=positions,
        quote_midpoint=Decimal("0.80"),
    )
    await svc._tick(slippage_bps=200)
    execute.assert_not_awaited()


# --- _tick: above threshold but < dwell → no SELL ---------------------------


@pytest.mark.asyncio
async def test_tick_no_sell_before_dwell_elapsed() -> None:
    cfg = _make_config(dwell=60)
    positions = {"k": _make_position("0xcond", "tok-1", 10.0)}
    svc, execute, _ = _make_service(
        config=cfg,
        positions=positions,
        quote_midpoint=Decimal("0.995"),
    )
    await svc._tick(slippage_bps=200)  # arm
    execute.assert_not_awaited()
    state = svc._dwell["tok-1"]
    assert state.first_seen_above_ts is not None


# --- _tick: above threshold + dwell elapsed → SELL fires --------------------


@pytest.mark.asyncio
async def test_tick_fires_sell_after_dwell_elapsed() -> None:
    cfg = _make_config(dwell=60)
    positions = {"k": _make_position("0xcond-abc", "tok-1", 42.5)}
    svc, execute, mds = _make_service(
        config=cfg,
        positions=positions,
        quote_midpoint=Decimal("0.995"),
    )
    # Pre-seed dwell so the tick is already past dwell_seconds.
    svc._dwell["tok-1"] = _DwellState(first_seen_above_ts=time.monotonic() - 120.0)

    await svc._tick(slippage_bps=200)
    execute.assert_awaited_once()
    # Verify tick metadata was fetched through MarketDataService — not the store.
    mds.get_tick_or_none.assert_awaited()
    req = execute.await_args.args[0]
    assert req.side == "SELL"
    assert req.token_id == "tok-1"
    assert req.condition_id == "0xcond-abc"
    assert req.signal_id.startswith("near_one_exit:")
    assert req.amount == Decimal("42.5")
    assert req.reason == "near_one_exit"


# --- timer resets when price falls below threshold --------------------------


@pytest.mark.asyncio
async def test_tick_resets_dwell_when_price_drops() -> None:
    cfg = _make_config()
    positions = {"k": _make_position("0xcond", "tok-1", 10.0)}
    svc, execute, _ = _make_service(
        config=cfg,
        positions=positions,
        quote_midpoint=Decimal("0.80"),
    )
    svc._dwell["tok-1"] = _DwellState(first_seen_above_ts=time.monotonic() - 30.0)
    await svc._tick(slippage_bps=200)
    assert svc._dwell["tok-1"].first_seen_above_ts is None
    execute.assert_not_awaited()


# --- held size below venue min → SELL skipped at sizing layer ---------------


@pytest.mark.asyncio
async def test_tick_skips_when_held_below_venue_min() -> None:
    cfg = _make_config(dwell=0)  # dwell=0 forces immediate trigger
    positions = {"k": _make_position("0xcond", "tok-1", 0.5)}
    svc, execute, _ = _make_service(
        config=cfg,
        positions=positions,
        quote_midpoint=Decimal("0.995"),
        tick_min_order_size=Decimal("10"),
    )
    # Arm dwell at exactly now → 0-second dwell is "elapsed".
    svc._dwell["tok-1"] = _DwellState(first_seen_above_ts=time.monotonic() - 0.001)
    await svc._tick(slippage_bps=200)
    execute.assert_not_awaited()


# --- stale dwell state cleaned up when position closes ----------------------


@pytest.mark.asyncio
async def test_tick_cleans_dwell_for_closed_positions() -> None:
    cfg = _make_config()
    svc, _, _ = _make_service(
        config=cfg,
        positions={},
        quote_midpoint=Decimal("0.995"),
    )
    svc._dwell["tok-stale"] = _DwellState(first_seen_above_ts=time.monotonic())
    svc._firing.add("tok-stale")
    await svc._tick(slippage_bps=200)
    assert "tok-stale" not in svc._dwell
    assert "tok-stale" not in svc._firing


# --- loop() short-circuits when disabled ------------------------------------


@pytest.mark.asyncio
async def test_loop_returns_when_disabled() -> None:
    cfg = _make_config()
    cfg.near_one_exit = SimpleNamespace(
        enable_near_one_exit=False,
        threshold=Decimal("0.99"),
        dwell_seconds=60,
        check_interval=5,
    )
    svc, execute, _ = _make_service(
        config=cfg,
        positions={},
        quote_midpoint=Decimal("0.995"),
    )
    await asyncio.wait_for(svc.loop(), timeout=1.0)
    execute.assert_not_awaited()