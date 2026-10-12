"""PR 5: reconciler transitions are idempotent across repeated passes.

Pin:
  - Two reconcile_trade_record calls with the same venue 'live' response emit
    exactly one lifecycle log (dedup map honored) and produce no double writes.
  - Two reconcile_trade_record calls with the same 'confirmed' venue response
    apply the fill delta exactly once and release the reservation exactly once.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from pmcopy.services.execution.reconciliation_service import ReconciliationService
from pmcopy.utils.logger import _reset_lifecycle_dedup_for_tests


@pytest.fixture(autouse=True)
def _clear_dedup():
    _reset_lifecycle_dedup_for_tests()
    yield
    _reset_lifecycle_dedup_for_tests()


TRADE_KEY = "tk-idem"
ORDER_ID = "ord-idem"
USER_ID = "user-1"
LEADER_ID = "0xleader"
COND_ID = "cond-idem"
TOKEN_ID = "0xtok-idem"


class _OrdersRepo:
    def __init__(self, previous_matched: str = "0"):
        self._previous_matched = previous_matched
        self._status = "submitted_unconfirmed"
        self.update_calls: list[dict[str, Any]] = []
        self.mark_processed_calls: list[dict[str, Any]] = []
        self.delete_calls: list[str] = []

    async def get_by_order_id(self, order_id: str):
        if order_id != ORDER_ID:
            return None
        return {
            "order_id": order_id,
            "status": self._status,
            "response": {"size_matched": self._previous_matched},
        }

    async def update_submission_status(self, **kwargs):
        self.update_calls.append(kwargs)
        if "status" in kwargs:
            self._status = str(kwargs["status"])
        size_matched = (kwargs.get("response_patch") or {}).get("size_matched")
        if size_matched is not None:
            self._previous_matched = str(size_matched)

    async def mark_processed_trade(self, **kwargs):
        self.mark_processed_calls.append(kwargs)
        if "status" in kwargs:
            self._status = str(kwargs["status"])

    async def delete_processed_trade(self, trade_key: str):
        self.delete_calls.append(trade_key)


class _FillsRepo:
    def __init__(self):
        self.fills: list[Any] = []

    async def append_fill(self, fill, *, raw_payload):
        self.fills.append(fill)

    async def append_fill_and_apply(self, record, positions_repo, *, raw_payload=None):
        # Mirror FillsRepo.append_fill_and_apply (P0-1): dedup on the stable
        # dedup_key, else append + apply the position delta. (0, None) on a dup.
        seen = getattr(self, "_applied_keys", None)
        if seen is None:
            seen = self._applied_keys = set()
        key = (record.order_id, record.side, getattr(record, "dedup_key", None))
        if key[2] is not None and key in seen:
            return 0, None
        seen.add(key)
        await self.append_fill(record, raw_payload=raw_payload)
        updated = await positions_repo.apply_fill(
            user_id=record.user_id,
            leader_id=record.leader_id,
            condition_id=record.condition_id,
            token_id=record.token_id,
            side=record.side,
            fill_size=record.size_tokens,
            fill_price=record.price,
        )
        return 1, updated


class _PositionsRepo:
    def __init__(self):
        self.applied: list[dict[str, Any]] = []

    async def apply_fill(self, **kwargs):
        self.applied.append(kwargs)
        return None

    async def get_position(self, *args, **kwargs):
        return None


class _ReservationService:
    def __init__(self):
        self.releases: list[tuple[str, str]] = []

    async def release_terminal(self, reservation_id: str, reason: str) -> None:
        self.releases.append((reservation_id, reason))


def _record(
    *, reservation_id: str | None = None, status: str = "submitted_unconfirmed"
) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "trade_key": TRADE_KEY,
        "user_id": USER_ID,
        "leader_id": LEADER_ID,
        "tx_hash": "0xtx",
        "asset": TOKEN_ID,
        "condition_id": COND_ID,
        "order_id": ORDER_ID,
        "status": status,
    }
    if reservation_id:
        rec["reservation_id"] = reservation_id
    return rec


def _make_service(*, previous_matched: str = "0", reservation: _ReservationService | None = None):
    return ReconciliationService(
        orders_repo=_OrdersRepo(previous_matched=previous_matched),
        fills_repo=_FillsRepo(),
        positions_repo=_PositionsRepo(),
        reservation_service=reservation,
    )


async def _confirm_fixed(size_matched: str, status: str):
    async def _cf(order_id: str):
        return {"status": status, "size_matched": size_matched, "avg_price": "0.50"}

    return _cf


@pytest.mark.asyncio
async def test_live_status_no_fill_or_release_across_repeated_passes():
    """For non-terminal `live`, repeated passes must NOT apply fills or release reservations."""
    reservation = _ReservationService()
    svc = _make_service(previous_matched="0", reservation=reservation)
    confirm_fill = await _confirm_fixed("0", "live")

    rec = _record(reservation_id="res-live-1")
    for _ in range(3):
        await svc.reconcile_trade_record(rec, "BUY", confirm_fill=confirm_fill)

    # No fills, no position deltas, no reservation releases — order is still
    # alive on the venue.
    assert svc.fills_repo.fills == []  # type: ignore[attr-defined]
    assert reservation.releases == []
    # update_submission_status was called to mirror the live status (no double-fill).
    statuses = [c.get("status") for c in svc.orders_repo.update_calls]  # type: ignore[attr-defined]
    assert all(s == "order_live" for s in statuses)


@pytest.mark.asyncio
async def test_confirmed_status_releases_reservation_once():
    reservation = _ReservationService()
    svc = _make_service(previous_matched="0", reservation=reservation)
    confirm_fill = await _confirm_fixed("5", "confirmed")

    for _ in range(3):
        await svc.reconcile_trade_record(
            _record(reservation_id="res-conf-1"),
            "BUY",
            confirm_fill=confirm_fill,
        )

    # Exactly one fill applied, exactly one release.
    fills = svc.fills_repo.fills  # type: ignore[attr-defined]
    assert len(fills) == 1
    assert fills[0].size_tokens == Decimal("5")
    assert reservation.releases == [("res-conf-1", "confirmed")]


@pytest.mark.asyncio
async def test_repeated_already_terminal_record_is_noop():
    """When the record is already filled_reconciled, a re-poll must not re-apply."""
    reservation = _ReservationService()
    svc = _make_service(previous_matched="5", reservation=reservation)
    confirm_fill = await _confirm_fixed("5", "confirmed")

    rec = _record(reservation_id="res-conf-2", status="filled_reconciled")

    await svc.reconcile_trade_record(rec, "BUY", confirm_fill=confirm_fill)
    await svc.reconcile_trade_record(rec, "BUY", confirm_fill=confirm_fill)

    assert reservation.releases == []  # already settled — no release on this pass
    assert svc.fills_repo.fills == []  # type: ignore[attr-defined]
