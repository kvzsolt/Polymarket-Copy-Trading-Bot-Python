"""PR 5: ambiguous-timeout auto-release with strict probe matrix.

Pins that the reconciler only auto-releases a reservation after every probe is
clean. Probe errors or inconclusive results keep the reservation active.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from config.app_config import OrderConfirmationConfig
from services.execution.reconciliation_service import ReconciliationService


def _cfg(**overrides) -> OrderConfirmationConfig:
    defaults = dict(
        initial_delay_seconds=2.0,
        poll_interval_seconds=3.0,
        max_wait_seconds=10.0,
        delayed_extra_seconds=2.0,
        token_delay_overrides={},
        condition_delay_overrides={},
        auto_release_ambiguous=True,
        auto_release_grace_seconds=5.0,
        auto_release_require_all_probes_clean=True,
        auto_release_max_probe_errors=0,
    )
    defaults.update(overrides)
    return OrderConfirmationConfig(**defaults)


class _OrdersRepo:
    def __init__(self):
        self.update_calls: list[dict[str, Any]] = []

    async def update_submission_status(self, **kwargs) -> None:
        self.update_calls.append(kwargs)


class _PositionsRepo:
    def __init__(self, local_size: str = "0"):
        self._size = Decimal(local_size)

    async def get_position(self, *args, **kwargs):
        return MagicMock(size_tokens=self._size)


class _ReservationService:
    def __init__(self):
        self.releases: list[tuple[str, str]] = []

    async def release_terminal(self, reservation_id: str, reason: str) -> None:
        self.releases.append((reservation_id, reason))


def _record(*, age_seconds: float = 100.0) -> dict[str, Any]:
    return {
        "order_id": "ord-amb-1",
        "venue_order_id": "venue-ord-amb-1",
        "user_id": "user-1",
        "asset": "0xtok",
        "condition_id": "cond-1",
        "request": {
            "side": "BUY",
            "amount": "5",
            "market": "0xmarket",
            "reservation_id": "res-amb-1",
        },
        "reservation_id": "res-amb-1",
        "created_at": time.time() - age_seconds,
        "response": {"size_matched": "0"},
        "status": "ambiguous_timeout",
    }


def _gateway(
    *,
    order_status: str = "absent",
    fills: list | None = None,
    list_orders: list | None = None,
):
    gw = MagicMock()
    gw.get_order = AsyncMock(return_value={"status": order_status})
    gw.get_fills = AsyncMock(return_value=fills or [])
    if list_orders is not None:
        gw.list_open_orders = AsyncMock(return_value=list_orders)
    else:
        # Simulate not supported by deleting the attr.
        if hasattr(gw, "list_open_orders"):
            del gw.list_open_orders
    return gw


@pytest.mark.asyncio
async def test_auto_release_fires_when_all_probes_clean():
    orders = _OrdersRepo()
    positions = _PositionsRepo(local_size="0")
    reservation = _ReservationService()
    gw = _gateway(order_status="absent", fills=[], list_orders=[])

    svc = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=positions,
        reservation_service=reservation,
        clob_gateway=gw,
    )
    with patch("services.metrics.metrics.inc_sync") as inc_sync:
        released = await svc._maybe_auto_release_ambiguous(
            _record(),
            side="BUY",
            order_confirmation_cfg=_cfg(),
        )

    assert released is True
    assert reservation.releases == [
        ("res-amb-1", "auto_release_after_ambiguous_timeout")
    ]
    assert any(c["status"] == "auto_released_ambiguous" for c in orders.update_calls)
    metric_calls = [
        c for c in inc_sync.call_args_list if "auto_release_ambiguous" in str(c.args)
    ]
    assert metric_calls


@pytest.mark.asyncio
async def test_auto_release_aborted_when_get_order_returns_live():
    orders = _OrdersRepo()
    positions = _PositionsRepo(local_size="0")
    reservation = _ReservationService()
    gw = _gateway(order_status="live", fills=[], list_orders=[])

    svc = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=positions,
        reservation_service=reservation,
        clob_gateway=gw,
    )
    released = await svc._maybe_auto_release_ambiguous(
        _record(),
        side="BUY",
        order_confirmation_cfg=_cfg(),
    )

    assert released is False
    assert reservation.releases == []


@pytest.mark.asyncio
async def test_auto_release_aborted_when_fills_present():
    orders = _OrdersRepo()
    positions = _PositionsRepo(local_size="0")
    reservation = _ReservationService()
    gw = _gateway(
        order_status="absent",
        fills=[{"order_id": "ord-amb-1", "size": "5"}],
        list_orders=[],
    )

    svc = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=positions,
        reservation_service=reservation,
        clob_gateway=gw,
    )
    released = await svc._maybe_auto_release_ambiguous(
        _record(),
        side="BUY",
        order_confirmation_cfg=_cfg(),
    )

    assert released is False
    assert reservation.releases == []


@pytest.mark.asyncio
async def test_auto_release_aborted_on_probe_error():
    orders = _OrdersRepo()
    positions = _PositionsRepo(local_size="0")
    reservation = _ReservationService()
    gw = _gateway(order_status="absent", fills=[], list_orders=[])
    gw.get_order = AsyncMock(side_effect=RuntimeError("network"))

    svc = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=positions,
        reservation_service=reservation,
        clob_gateway=gw,
    )
    released = await svc._maybe_auto_release_ambiguous(
        _record(),
        side="BUY",
        order_confirmation_cfg=_cfg(auto_release_max_probe_errors=0),
    )

    assert released is False
    assert reservation.releases == []


@pytest.mark.asyncio
async def test_auto_release_disabled_via_config():
    orders = _OrdersRepo()
    positions = _PositionsRepo(local_size="0")
    reservation = _ReservationService()
    gw = _gateway(order_status="absent", fills=[], list_orders=[])

    svc = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=positions,
        reservation_service=reservation,
        clob_gateway=gw,
    )
    # auto_release_ambiguous=False is enforced via is_past_auto_release_grace,
    # which returns False short-circuiting the helper. Verify the helper still
    # treats inconclusive grossly when require_all_probes_clean=True even if
    # called directly (defensive).
    cfg = _cfg(auto_release_ambiguous=False)
    assert (
        svc._is_past_auto_release_grace(
            _record(age_seconds=10_000), order_confirmation_cfg=cfg
        )
        is False
    )


@pytest.mark.asyncio
async def test_auto_release_aborted_when_position_size_matches_order():
    orders = _OrdersRepo()
    # Local position already absorbed >= the order size — could be a fill.
    positions = _PositionsRepo(local_size="5")
    reservation = _ReservationService()
    gw = _gateway(order_status="absent", fills=[], list_orders=[])

    svc = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=positions,
        reservation_service=reservation,
        clob_gateway=gw,
    )
    released = await svc._maybe_auto_release_ambiguous(
        _record(),
        side="BUY",
        order_confirmation_cfg=_cfg(),
    )

    assert released is False
    assert reservation.releases == []


# ---------------------------------------------------------------------------
# Issue #96 — unsupported / inconclusive probes must not auto-release.
# The audit specifically called out get_orders_for_market being optional:
# when the SDK does not expose it, the probe is inconclusive and (with the
# default require_all_probes_clean=True) the reservation must stay locked.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auto_release_aborts_when_list_open_orders_unsupported():
    """When the SDK lacks list_open_orders the probe is inconclusive;
    require_all_probes_clean=True (the default) must abort the release so
    the reservation stays locked rather than being freed off partial evidence."""
    orders = _OrdersRepo()
    positions = _PositionsRepo(local_size="0")
    reservation = _ReservationService()
    # list_orders=None → _gateway() deletes get_orders_for_market so the
    # probe reports "not_supported" and is counted as inconclusive.
    gw = _gateway(order_status="absent", fills=[], list_orders=None)
    assert not hasattr(gw, "list_open_orders")

    svc = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=positions,
        reservation_service=reservation,
        clob_gateway=gw,
    )
    released = await svc._maybe_auto_release_ambiguous(
        _record(),
        side="BUY",
        order_confirmation_cfg=_cfg(),
    )

    assert released is False
    assert reservation.releases == []
    # The order row must NOT have been transitioned to auto_released_ambiguous.
    assert not any(
        c["status"] == "auto_released_ambiguous" for c in orders.update_calls
    )


@pytest.mark.asyncio
async def test_auto_release_aborts_when_fills_throw_delayed_api():
    """get_fills timing out / raising counts as a probe error; with
    auto_release_max_probe_errors=0 (default safe stance) the release
    aborts and the reservation stays locked. Models a delayed venue fills
    endpoint that would otherwise be silently treated as 'clean'."""
    orders = _OrdersRepo()
    positions = _PositionsRepo(local_size="0")
    reservation = _ReservationService()
    gw = _gateway(order_status="absent", list_orders=[])
    gw.get_fills = AsyncMock(side_effect=TimeoutError("fills_endpoint_slow"))

    svc = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=positions,
        reservation_service=reservation,
        clob_gateway=gw,
    )
    released = await svc._maybe_auto_release_ambiguous(
        _record(),
        side="BUY",
        order_confirmation_cfg=_cfg(),
    )

    assert released is False
    assert reservation.releases == []


# ---------------------------------------------------------------------------
# Venue outage handling: transient unavailability is inconclusive (release
# still blocked) instead of a hard probe error, the probes participate in the
# shared CLOB backoff, and repeated aborts are paced + throttled.
# ---------------------------------------------------------------------------


def _svc(gw, *, reservation=None, positions=None, orders=None, cfg=None):
    return ReconciliationService(
        orders_repo=orders if orders is not None else _OrdersRepo(),
        fills_repo=None,
        positions_repo=positions if positions is not None else _PositionsRepo("0"),
        reservation_service=reservation
        if reservation is not None
        else _ReservationService(),
        clob_gateway=gw,
        order_confirmation_cfg=cfg,
    )


@pytest.mark.asyncio
async def test_venue_unavailable_is_inconclusive_not_error():
    from integrations.polymarket.errors import VenueUnavailableError

    reservation = _ReservationService()
    gw = _gateway(order_status="absent", fills=[], list_orders=[])
    gw.get_order = AsyncMock(side_effect=VenueUnavailableError("5xx"))
    svc = _svc(gw, reservation=reservation)

    probe = await svc._run_auto_release_probes(
        order_id="ord-amb-1",
        venue_order_id="venue-ord-amb-1",
        market_id="0xmarket",
        trade_ids=[],
        user_id="user-1",
        condition_id="cond-1",
        token_id="0xtok",
        expected_size=Decimal("5"),
    )
    assert probe["errors"] == 0
    assert probe["inconclusive"] == 1
    assert probe["details"]["get_order"] == "venue_unavailable"
    # The failing probe feeds the shared CLOB backoff.
    assert svc._venue_poll_backoffs["order"]["failures"] == 1

    released = await svc._maybe_auto_release_ambiguous(
        _record(),
        side="BUY",
        order_confirmation_cfg=_cfg(),
    )
    assert released is False
    assert reservation.releases == []


@pytest.mark.asyncio
async def test_probes_deferred_while_source_backed_off():
    gw = _gateway(order_status="absent", fills=[], list_orders=[])
    svc = _svc(gw)
    svc._venue_poll_backoffs["order"] = {
        "failures": 2,
        "until": time.monotonic() + 60.0,
        "reason": "venue_unavailable",
        "base_delay": 10.0,
    }

    probe = await svc._run_auto_release_probes(
        order_id="ord-amb-1",
        venue_order_id="venue-ord-amb-1",
        market_id="0xmarket",
        trade_ids=[],
        user_id="user-1",
        condition_id="cond-1",
        token_id="0xtok",
        expected_size=Decimal("5"),
    )
    assert probe.get("deferred") is True
    gw.get_order.assert_not_awaited()
    gw.get_fills.assert_not_awaited()

    released = await svc._maybe_auto_release_ambiguous(
        _record(),
        side="BUY",
        order_confirmation_cfg=_cfg(),
    )
    assert released is False


@pytest.mark.asyncio
async def test_repeated_aborts_are_log_throttled():
    import logging as _logging

    gw = _gateway(order_status="absent", fills=[], list_orders=[])
    gw.get_order = AsyncMock(side_effect=RuntimeError("boom"))
    svc = _svc(gw)

    with patch("services.execution.reconciliation_service._log.log") as log_spy:
        for _ in range(11):
            released = await svc._maybe_auto_release_ambiguous(
                _record(),
                side="BUY",
                order_confirmation_cfg=_cfg(),
            )
            assert released is False

    abort_calls = [
        c
        for c in log_spy.call_args_list
        if "auto_release_aborted_probe_errors" in str(c.args)
    ]
    levels = [c.args[0] for c in abort_calls]
    # 1st and 10th abort are WARNING, the other nine are DEBUG.
    assert levels.count(_logging.WARNING) == 2
    assert levels.count(_logging.DEBUG) == 9


@pytest.mark.asyncio
async def test_timeout_handler_paces_auto_release_attempts(monkeypatch):
    gw = _gateway(order_status="absent", fills=[], list_orders=[])
    gw.get_order = AsyncMock(side_effect=RuntimeError("boom"))
    cfg = _cfg(
        auto_release_retry_backoff_seconds=30.0,
        auto_release_retry_backoff_max_seconds=900.0,
    )
    svc = _svc(gw, cfg=cfg)

    fake_now = [1000.0]
    monkeypatch.setattr(
        "services.execution.reconciliation_service.time.monotonic",
        lambda: fake_now[0],
    )

    record = _record()
    timed_out, released = await svc._handle_unresolved_confirmation_timeout(
        record, side="BUY", fill_info={"status": "poll_unavailable"}
    )
    assert (timed_out, released) == (True, False)
    assert gw.get_order.await_count == 1
    next_at, interval = svc._auto_release_next_probe_at["ord-amb-1"]
    assert interval == 30.0
    assert next_at == 1030.0

    # Within the pacing window: no new probe attempt.
    fake_now[0] = 1010.0
    await svc._handle_unresolved_confirmation_timeout(
        record, side="BUY", fill_info={"status": "poll_unavailable"}
    )
    assert gw.get_order.await_count == 1

    # Past the window: probes again and the interval doubles.
    fake_now[0] = 1031.0
    await svc._handle_unresolved_confirmation_timeout(
        record, side="BUY", fill_info={"status": "poll_unavailable"}
    )
    assert gw.get_order.await_count == 2
    _, interval = svc._auto_release_next_probe_at["ord-amb-1"]
    assert interval == 60.0


@pytest.mark.asyncio
async def test_timeout_handler_clears_pacing_on_release():
    gw = _gateway(order_status="absent", fills=[], list_orders=[])
    reservation = _ReservationService()
    svc = _svc(gw, reservation=reservation, cfg=_cfg())
    svc._auto_release_next_probe_at["ord-amb-1"] = (0.0, 30.0)
    svc._auto_release_abort_counts["ord-amb-1"] = 3

    with patch("services.metrics.metrics.inc_sync"):
        timed_out, released = await svc._handle_unresolved_confirmation_timeout(
            _record(), side="BUY", fill_info={"status": "poll_unavailable"}
        )
    assert (timed_out, released) == (True, True)
    assert reservation.releases
    assert "ord-amb-1" not in svc._auto_release_next_probe_at
    assert "ord-amb-1" not in svc._auto_release_abort_counts
