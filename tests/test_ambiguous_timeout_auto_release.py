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

    def update_submission_status(self, **kwargs) -> None:
        self.update_calls.append(kwargs)


class _PositionsRepo:
    def __init__(self, local_size: str = "0"):
        self._size = Decimal(local_size)

    def get_position(self, *args, **kwargs):
        return MagicMock(size_tokens=self._size)


class _ReservationService:
    def __init__(self):
        self.releases: list[tuple[str, str]] = []

    def release_terminal(self, reservation_id: str, reason: str) -> None:
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


def _gateway(*, order_status: str = "absent", fills: list | None = None, list_orders: list | None = None):
    gw = MagicMock()
    gw.get_order = AsyncMock(return_value={"status": order_status})
    gw.get_fills = AsyncMock(return_value=fills or [])
    if list_orders is not None:
        gw.get_orders_for_market = AsyncMock(return_value=list_orders)
    else:
        # Simulate not supported by deleting the attr.
        if hasattr(gw, "get_orders_for_market"):
            del gw.get_orders_for_market
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
    assert reservation.releases == [("res-amb-1", "auto_release_after_ambiguous_timeout")]
    assert any(c["status"] == "auto_released_ambiguous" for c in orders.update_calls)
    metric_calls = [c for c in inc_sync.call_args_list if "auto_release_ambiguous" in str(c.args)]
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
    assert svc._is_past_auto_release_grace(_record(age_seconds=10_000), order_confirmation_cfg=cfg) is False


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
async def test_auto_release_aborts_when_get_orders_for_market_unsupported():
    """When the SDK lacks get_orders_for_market the probe is inconclusive;
    require_all_probes_clean=True (the default) must abort the release so
    the reservation stays locked rather than being freed off partial evidence."""
    orders = _OrdersRepo()
    positions = _PositionsRepo(local_size="0")
    reservation = _ReservationService()
    # list_orders=None → _gateway() deletes get_orders_for_market so the
    # probe reports "not_supported" and is counted as inconclusive.
    gw = _gateway(order_status="absent", fills=[], list_orders=None)
    assert not hasattr(gw, "get_orders_for_market")

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
    assert not any(c["status"] == "auto_released_ambiguous" for c in orders.update_calls)


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
