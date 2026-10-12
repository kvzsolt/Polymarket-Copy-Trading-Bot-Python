"""A14: expiry must never release an unresolved no-ID submission.

The failure mode is a double-spend window: an ambiguous venue POST (a
``submitted_unconfirmed`` order row without a venue identity, linked to its
reservation only through ``recovery_metadata``) leaves the reserved
quote/share capacity held. Releasing that capacity on the reservation TTL
alone lets the next intent spend collateral or shares a live venue order may
already be consuming.

These contracts pin the typed expiry decision tree:

* an unresolved no-ID submission row KEEPS the reservation (recovery owns it);
* a failed persistence lookup is NOT absence — the reservation is kept and
  the outcome distinguishes the outage;
* a pre-submit orphan releases only on POSITIVE proof of absence (the lookup
  succeeded and found no submission row — the crash happened before the
  write-ahead submission row was persisted, so no POST can have occurred);
* a terminal no-ID row status is authoritative terminal evidence and
  releases exactly once;
* a delayed fill arriving after the keep is reconciled, applied once, and
  only then releases the reservation.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import orchestrators.runtime_bootstrap as runtime_bootstrap
from services.execution.reservation_expiry import (
    OrderRowLookup,
    ReservationExpiryDecision,
    classify_no_id_reservation_expiry,
)
from services.execution.reservation_service import ReservationService

_TOKEN = "71321045679252212594626385532706912750332728571942532289631379312455583992561"
_CONDITION = "0x" + "ab" * 32
_NOW = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)


class _Clock:
    def __init__(self, now: int) -> None:
        self._now = now

    def utcnow_timestamp(self) -> int:
        return self._now

    def advance(self, seconds: int) -> None:
        self._now += seconds


class _ReservationsRepo:
    """Faithful fake: only ACTIVE expired rows are swept, release is CAS."""

    def __init__(self) -> None:
        self.rows: dict[str, SimpleNamespace] = {}
        self.releases: list[tuple[str, str]] = []

    def add(self, reservation: SimpleNamespace) -> None:
        self.rows[reservation.id] = reservation

    async def list_expired_active(self, now_ts: int) -> list[SimpleNamespace]:
        return [
            row
            for row in self.rows.values()
            if str(row.status).lower() == "active" and row.expires_at.timestamp() < now_ts
        ]

    async def get_by_id(self, res_id: str) -> SimpleNamespace | None:
        return self.rows.get(res_id)

    async def mark_released(self, reservation_id: str, reason: str, released_at: int) -> None:
        row = self.rows.get(reservation_id)
        if row is None or str(row.status).lower() != "active":
            return
        row.status = "released"
        row.release_reason = reason
        self.releases.append((reservation_id, reason))


def _reservation(*, rid: str = "res-1", order_id: str | None = None) -> SimpleNamespace:
    from domain.order_lifecycle import IntentSide

    return SimpleNamespace(
        id=rid,
        status="active",
        order_id=order_id,
        venue_order_id=None,
        user_id="user-1",
        leader_wallet="leader-1",
        condition_id=_CONDITION,
        token_id=_TOKEN,
        side=IntentSide.BUY,
        intent_key="intent-1",
        reserved_usdc=Decimal("5"),
        reserved_size=Decimal("0"),
        expires_at=_NOW - timedelta(seconds=1),
    )


def _order_row(
    *,
    order_id: str | None = None,
    status: str = "submitted_unconfirmed",
) -> dict:
    return {
        "idempotency_key": "idem-1",
        "user_id": "user-1",
        "leader_id": "leader-1",
        "order_id": order_id,
        "status": status,
        "request": {
            "user_id": "user-1",
            "leader_id": "leader-1",
            "condition_id": _CONDITION,
            "token_id": _TOKEN,
            "side": "BUY",
            "signal_id": "tx-1",
            "recovery_metadata": {"reservation_id": "res-1"},
        },
        "matched_total": "0",
    }


class _OrdersRepo:
    def __init__(self, row: dict | None, *, error: Exception | None = None) -> None:
        self.row = row
        self.error = error
        self.updated: list[tuple[str, str, object]] = []
        self.processed: list[dict] = []

    async def get_unconfirmed_buys(self, max_age_seconds=300.0, *, user_id=None):
        return []

    async def get_unconfirmed_sells(self, max_age_seconds=300.0, *, user_id=None):
        return []

    async def get_unconfirmed_order_submissions(self, *, user_id=None, side=None, limit=200):
        return []

    async def get_by_reservation_id(self, reservation_id: str):
        assert reservation_id == "res-1"
        if self.error is not None:
            raise self.error
        return self.row

    async def get_by_order_id(self, order_id: str):
        return self.row if self.row and self.row.get("order_id") == order_id else None

    async def update_submission_status(self, *, order_id: str, status: str, response_patch=None):
        self.updated.append((order_id, status, response_patch))

    async def mark_processed_trade(self, **kwargs):
        self.processed.append(kwargs)


class _FillsRepo:
    def __init__(self) -> None:
        self.fills: list[object] = []

    async def append_fill(self, fill, raw_payload=None):
        self.fills.append(fill)

    async def append_fill_and_apply(self, record, positions_repo, *, raw_payload=None):
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
    def __init__(self) -> None:
        self.applied: list[dict] = []

    async def apply_fill(self, **kwargs):
        self.applied.append(kwargs)

    async def get_position(self, *args, **kwargs):
        return None


def _bind(
    *,
    reservation: SimpleNamespace,
    orders_repo: _OrdersRepo,
    fills_repo: _FillsRepo,
    positions_repo: _PositionsRepo,
    get_order=None,
):
    """Wire the real bootstrap expiry callback around fakes."""
    reservations_repo = _ReservationsRepo()
    reservations_repo.add(reservation)
    reservation_service = ReservationService(
        reservations_repo=reservations_repo,
        positions_repo=object(),
        get_balance_func=None,
        funder_address="0xfunder",
        clock=_Clock(int(_NOW.timestamp())),
        ttl_seconds=60,
    )
    ctx = SimpleNamespace(
        user_id="user-1",
        wallet_address="0xfunder",
        execution_service=object(),
        extra={"clob_gateway": SimpleNamespace(get_order=get_order)},
        orders_repo=orders_repo,
        fills_repo=fills_repo,
        positions_repo=positions_repo,
        risk_blocks_repo=None,
        risk_triggers_repo=None,
        audit_logger=None,
        pnl_tracker=None,
    )
    coord = SimpleNamespace(
        reservation_service=reservation_service,
        reservations_repo=reservations_repo,
    )
    runtime_bootstrap._bind_reservation_runtime(ctx, coord)
    return reservation_service, reservations_repo


def _get_filled_order(order_id: str):
    from integrations.polymarket.models import VenueOrderState

    async def _fetch(oid: str):
        return VenueOrderState(
            order_id=oid,
            status="confirmed",
            filled_size=Decimal("2"),
            avg_fill_price=Decimal("0.50"),
        )

    return _fetch


# ---------------------------------------------------------------------------
# Pure classifier contracts
# ---------------------------------------------------------------------------


def test_lookup_error_is_persistence_unavailable_not_absence() -> None:
    decision = classify_no_id_reservation_expiry(
        OrderRowLookup.unavailable(RuntimeError("db down"))
    )
    assert decision is ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE
    assert decision.allows_release is False


def test_proven_absence_classifies_pre_submit_orphan() -> None:
    decision = classify_no_id_reservation_expiry(OrderRowLookup.absent())
    assert decision is ReservationExpiryDecision.RELEASE_PRE_SUBMIT_ORPHAN
    assert decision.allows_release is True


def test_unresolved_row_status_keeps_pending_recovery() -> None:
    for status in ("submitted_unconfirmed", "pending", "pending_lock", "live"):
        decision = classify_no_id_reservation_expiry(
            OrderRowLookup.found(_order_row(status=status))
        )
        assert decision is ReservationExpiryDecision.KEEP_PENDING_RECOVERY, status
        assert decision.allows_release is False


def test_terminal_row_status_is_terminal_evidence() -> None:
    for status in ("rejected", "failed", "cancelled", "expired", "confirmed_absent"):
        decision = classify_no_id_reservation_expiry(
            OrderRowLookup.found(_order_row(status=status))
        )
        assert decision is ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE, status
        assert decision.allows_release is True


# ---------------------------------------------------------------------------
# Callback decision tree through the real bootstrap wiring
# ---------------------------------------------------------------------------


async def test_unresolved_no_id_submission_survives_expiry() -> None:
    """TTL alone must never free capacity for an ambiguous no-ID POST."""

    reservation = _reservation()
    orders_repo = _OrdersRepo(_order_row(order_id=None, status="submitted_unconfirmed"))
    svc, reservations_repo = _bind(
        reservation=reservation,
        orders_repo=orders_repo,
        fills_repo=_FillsRepo(),
        positions_repo=_PositionsRepo(),
    )

    decision = await svc.reconciler_callback("res-1")

    assert decision is ReservationExpiryDecision.KEEP_PENDING_RECOVERY
    # Driving the full sweep must keep the reservation active (no release).
    enqueued = await svc.release_expired_active()
    assert enqueued == []
    assert reservations_repo.releases == []
    assert str(reservations_repo.rows["res-1"].status).lower() == "active"


async def test_orders_lookup_failure_keeps_reservation_as_persistence_outage() -> None:
    reservation = _reservation()
    orders_repo = _OrdersRepo(None, error=RuntimeError("orders table unavailable"))
    svc, reservations_repo = _bind(
        reservation=reservation,
        orders_repo=orders_repo,
        fills_repo=_FillsRepo(),
        positions_repo=_PositionsRepo(),
    )

    decision = await svc.reconciler_callback("res-1")

    assert decision is ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE
    assert await svc.release_expired_active() == []
    assert reservations_repo.releases == []
    assert str(reservations_repo.rows["res-1"].status).lower() == "active"


async def test_reservations_repo_lookup_failure_is_persistence_unavailable() -> None:
    reservation = _reservation()
    orders_repo = _OrdersRepo(None)  # proven absence — must NOT be reached
    svc, reservations_repo = _bind(
        reservation=reservation,
        orders_repo=orders_repo,
        fills_repo=_FillsRepo(),
        positions_repo=_PositionsRepo(),
    )

    original = reservations_repo.get_by_id

    async def _boom(res_id: str):
        raise RuntimeError("reservations table unavailable")

    reservations_repo.get_by_id = _boom
    try:
        decision = await svc.reconciler_callback("res-1")
    finally:
        reservations_repo.get_by_id = original

    assert decision is ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE
    assert reservations_repo.releases == []


async def test_proven_pre_submit_orphan_releases_exactly_once() -> None:
    """Crash before the write-ahead row: positive proof releases, once."""

    reservation = _reservation()
    orders_repo = _OrdersRepo(None)  # lookup succeeds, finds no row
    svc, reservations_repo = _bind(
        reservation=reservation,
        orders_repo=orders_repo,
        fills_repo=_FillsRepo(),
        positions_repo=_PositionsRepo(),
    )

    decision = await svc.reconciler_callback("res-1")
    assert decision is ReservationExpiryDecision.RELEASE_PRE_SUBMIT_ORPHAN

    first = await svc.release_expired_active()
    second = await svc.release_expired_active()

    assert first == ["res-1"]
    assert second == []  # no longer active — swept exactly once
    assert reservations_repo.releases == [("res-1", "expired_pre_submit_orphan")]
    assert str(reservations_repo.rows["res-1"].status).lower() == "released"


async def test_terminal_no_id_row_releases_via_terminal_evidence_once() -> None:
    reservation = _reservation()
    orders_repo = _OrdersRepo(_order_row(order_id=None, status="confirmed_absent"))
    svc, reservations_repo = _bind(
        reservation=reservation,
        orders_repo=orders_repo,
        fills_repo=_FillsRepo(),
        positions_repo=_PositionsRepo(),
    )

    decision = await svc.reconciler_callback("res-1")
    assert decision is ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE

    await svc.release_expired_active()
    await svc.release_expired_active()

    assert reservations_repo.releases == [("res-1", "expired_terminal")]
    assert str(reservations_repo.rows["res-1"].status).lower() == "released"


async def test_delayed_fill_after_keep_is_accounted_and_releases_once() -> None:
    """No double-spend: the kept reservation is reconciled, not leaked.

    First sweep: the unresolved no-ID row keeps the reservation. The recovery
    machinery then attaches the venue identity; the next sweep reconciles the
    delayed fill (applied exactly once) and only then releases.
    """

    reservation = _reservation()
    row = _order_row(order_id=None, status="submitted_unconfirmed")
    orders_repo = _OrdersRepo(row)
    fills_repo = _FillsRepo()
    positions_repo = _PositionsRepo()
    svc, reservations_repo = _bind(
        reservation=reservation,
        orders_repo=orders_repo,
        fills_repo=fills_repo,
        positions_repo=positions_repo,
        get_order=_get_filled_order("venue-9"),
    )

    with patch(
        "orchestrators.runtime_bootstrap.rehydrate_position_manager",
        new=AsyncMock(return_value=None),
    ):
        kept = await svc.reconciler_callback("res-1")
        assert kept is ReservationExpiryDecision.KEEP_PENDING_RECOVERY
        assert await svc.release_expired_active() == []
        assert reservations_repo.releases == []

        # Recovery attached the venue identity (T8 attach path).
        row["order_id"] = "venue-9"
        orders_repo.row = row

        reconciled = await svc.reconciler_callback("res-1")
        assert reconciled is ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE
        # The reconciler itself released the reservation (reason "confirmed");
        # the subsequent sweep finds nothing left to release.
        assert reservations_repo.releases == [("res-1", "confirmed")]
        assert await svc.release_expired_active() == []
        # Repeated callbacks apply no second fill and record no second release.
        assert await svc.reconciler_callback("res-1") is (
            ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE
        )
        await svc.release_expired_active()

    assert len(fills_repo.fills) == 1
    assert positions_repo.applied[0]["fill_size"] == Decimal("2")
    assert len(reservations_repo.releases) == 1
    assert str(reservations_repo.rows["res-1"].status).lower() == "released"


# ---------------------------------------------------------------------------
# ReservationService decision -> allow/deny mapping (legacy bools stay valid)
# ---------------------------------------------------------------------------


class _FakeExpiryRepo:
    def __init__(self) -> None:
        self.released: list[tuple[str, str, int]] = []

    async def list_expired_active(self, now_ts: int):
        from domain.order_lifecycle import IntentReason, IntentSide, Reservation

        return [
            Reservation(
                id="r1",
                user_id="u1",
                intent_key="u1:cond:tok:BUY:copy",
                condition_id="cond",
                token_id="tok",
                side=IntentSide.BUY,
                reason=IntentReason.COPY_BUY,
                reserved_usdc=Decimal("5"),
                reserved_size=Decimal("0"),
                status="active",
                created_at=datetime.fromtimestamp(0, tz=timezone.utc),
                expires_at=datetime.fromtimestamp(100, tz=timezone.utc),
            )
        ]

    async def mark_released(self, reservation_id, reason, released_at):
        self.released.append((reservation_id, reason, released_at))


def _svc_with_callback(callback) -> tuple[ReservationService, _FakeExpiryRepo]:
    repo = _FakeExpiryRepo()
    svc = ReservationService(
        reservations_repo=repo,
        positions_repo=object(),
        get_balance_func=None,
        funder_address="0xfunder",
        clock=_Clock(200),
        ttl_seconds=60,
    )
    svc.reconciler_callback = callback
    return svc, repo


async def test_decision_pre_submit_orphan_releases_with_distinct_reason() -> None:
    async def _orphan(_rid):
        return ReservationExpiryDecision.RELEASE_PRE_SUBMIT_ORPHAN

    svc, repo = _svc_with_callback(_orphan)
    out = await svc.release_expired_active()

    assert out == ["r1"]
    assert repo.released == [("r1", "expired_pre_submit_orphan", 200)]


async def test_decision_terminal_evidence_releases_with_terminal_reason() -> None:
    async def _terminal(_rid):
        return ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE

    svc, repo = _svc_with_callback(_terminal)
    out = await svc.release_expired_active()

    assert out == ["r1"]
    assert repo.released == [("r1", "expired_terminal", 200)]


@pytest.mark.parametrize(
    "decision",
    [
        ReservationExpiryDecision.KEEP_PENDING_RECOVERY,
        ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE,
    ],
)
async def test_deny_decisions_keep_the_reservation(decision: ReservationExpiryDecision) -> None:
    async def _deny(_rid):
        return decision

    svc, repo = _svc_with_callback(_deny)
    out = await svc.release_expired_active()

    assert out == []
    assert repo.released == []


async def test_legacy_bool_callback_contract_is_preserved() -> None:
    async def _true(_rid):
        return True

    svc_true, repo_true = _svc_with_callback(_true)
    assert await svc_true.release_expired_active() == ["r1"]
    assert repo_true.released == [("r1", "expired_terminal", 200)]

    async def _false(_rid):
        return False

    svc_false, repo_false = _svc_with_callback(_false)
    assert await svc_false.release_expired_active() == []
    assert repo_false.released == []


async def test_expire_with_reconciliation_maps_decisions() -> None:
    from domain.order_lifecycle import IntentReason, IntentSide, Reservation

    res = Reservation(
        id="r1",
        user_id="u1",
        intent_key="u1:cond:tok:BUY:copy",
        condition_id="cond",
        token_id="tok",
        side=IntentSide.BUY,
        reason=IntentReason.COPY_BUY,
        reserved_usdc=Decimal("5"),
        reserved_size=Decimal("0"),
        status="active",
        created_at=datetime.fromtimestamp(0, tz=timezone.utc),
        expires_at=datetime.fromtimestamp(100, tz=timezone.utc),
    )

    async def _keep(_rid):
        return ReservationExpiryDecision.KEEP_PENDING_RECOVERY

    svc, repo = _svc_with_callback(_keep)
    assert await svc._expire_with_reconciliation(res) is False
    assert repo.released == []

    async def _orphan(_rid):
        return ReservationExpiryDecision.RELEASE_PRE_SUBMIT_ORPHAN

    svc, repo = _svc_with_callback(_orphan)
    assert await svc._expire_with_reconciliation(res) is True
    assert repo.released == [("r1", "expired_pre_submit_orphan", 200)]


# ---------------------------------------------------------------------------
# Risk-loop SELL orphan release maps onto the same outcome classes
# ---------------------------------------------------------------------------


class _RiskOrdersRepo:
    def __init__(self, row: dict | None, *, error: Exception | None = None) -> None:
        self.row = row
        self.error = error

    async def get_by_reservation_id(self, reservation_id: str):
        if self.error is not None:
            raise self.error
        return self.row


def _risk_context(
    *,
    orders_repo: object | None,
    reservation_id: str = "res-orphan",
) -> SimpleNamespace:
    from unittest.mock import MagicMock

    expired = datetime.now(timezone.utc) - timedelta(seconds=10)
    repo = MagicMock()
    repo.list_active_for_scope = AsyncMock(
        return_value=[
            SimpleNamespace(
                id=reservation_id,
                side="SELL",
                status="active",
                reserved_size=Decimal("5"),
                order_id=None,
                venue_order_id=None,
                expires_at=expired,
            )
        ]
    )
    reservation_service = SimpleNamespace(
        reservations_repo=repo,
        release_terminal=AsyncMock(),
    )
    return SimpleNamespace(
        execution_coordinator=SimpleNamespace(
            reservations_repo=repo,
            reservation_service=reservation_service,
        ),
        orders_repo=orders_repo,
        extra={},
    )


async def test_risk_sell_orphan_with_unresolved_no_id_row_is_not_released() -> None:
    from services.risk.risk_monitor import _active_sell_reserved_size

    context = _risk_context(orders_repo=_RiskOrdersRepo(_order_row(order_id=None)))

    reserved = await _active_sell_reserved_size(
        context, user_id="u1", condition_id=_CONDITION, token_id=_TOKEN
    )

    assert reserved == Decimal("5")
    context.execution_coordinator.reservation_service.release_terminal.assert_not_awaited()


async def test_risk_sell_orphan_releases_only_on_proven_absence() -> None:
    from services.risk.risk_monitor import _active_sell_reserved_size

    context = _risk_context(orders_repo=_RiskOrdersRepo(None))

    reserved = await _active_sell_reserved_size(
        context, user_id="u1", condition_id=_CONDITION, token_id=_TOKEN
    )

    assert reserved == Decimal("0")
    context.execution_coordinator.reservation_service.release_terminal.assert_awaited_once_with(
        "res-orphan",
        reason="expired_orphan_before_risk_exit",
    )


async def test_risk_sell_orphan_lookup_failure_keeps_reservation() -> None:
    from services.risk.risk_monitor import _active_sell_reserved_size

    context = _risk_context(
        orders_repo=_RiskOrdersRepo(None, error=RuntimeError("orders table down"))
    )

    reserved = await _active_sell_reserved_size(
        context, user_id="u1", condition_id=_CONDITION, token_id=_TOKEN
    )

    assert reserved == Decimal("5")
    context.execution_coordinator.reservation_service.release_terminal.assert_not_awaited()


async def test_risk_sell_orphan_without_orders_lookup_fails_closed() -> None:
    from services.risk.risk_monitor import _active_sell_reserved_size

    context = _risk_context(orders_repo=None)

    reserved = await _active_sell_reserved_size(
        context, user_id="u1", condition_id=_CONDITION, token_id=_TOKEN
    )

    assert reserved == Decimal("5")
    context.execution_coordinator.reservation_service.release_terminal.assert_not_awaited()


# ---------------------------------------------------------------------------
# Typed lookup normalization: a garbage lookup return is an error, not absence
# ---------------------------------------------------------------------------


def test_found_non_dict_row_is_unavailable_not_proven_absent() -> None:
    lookup = OrderRowLookup.found("not-a-dict")  # type: ignore[arg-type]

    assert lookup.lookup_succeeded is False
    assert lookup.order_row is None
    assert classify_no_id_reservation_expiry(lookup) is (
        ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE
    )


def test_from_lookup_normalizes_none_dict_and_garbage() -> None:
    absent = OrderRowLookup.from_lookup(None)
    assert absent.lookup_succeeded is True
    assert absent.order_row is None
    assert classify_no_id_reservation_expiry(absent) is (
        ReservationExpiryDecision.RELEASE_PRE_SUBMIT_ORPHAN
    )

    found = OrderRowLookup.from_lookup({"status": "submitted_unconfirmed"})
    assert found.lookup_succeeded is True
    assert found.order_row is not None

    garbage = OrderRowLookup.from_lookup(["not-a-dict"])
    assert garbage.lookup_succeeded is False
    assert classify_no_id_reservation_expiry(garbage) is (
        ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE
    )


# ---------------------------------------------------------------------------
# CLI resolve: "safe_to_release" demands the same typed evidence (A14 fix)
# ---------------------------------------------------------------------------


def _cli_expired_row(*, rid: str = "r-expired-orphan", order_id: str | None = None) -> dict:
    from bot_cli.commands.reservations import _classify_expired_reservation

    now_ts = int(_NOW.timestamp())
    # Shape mirrors bot_cli _row_to_dict: expires_at is an epoch-seconds int.
    row = {
        "id": rid,
        "user_id": "user-1",
        "order_intent_key": "intent-1",
        "condition_id": _CONDITION,
        "token_id": _TOKEN,
        "side": "BUY",
        "reason": "copy",
        "reserved_usdc": "5.00",
        "reserved_size": "0",
        "status": "active",
        "created_at": now_ts - 3600,
        "expires_at": now_ts - 1,
        "order_id": order_id,
        "venue_order_id": None,
    }
    return _classify_expired_reservation(row, now_ts=now_ts)


class _CliOrdersRepo:
    def __init__(self, row: dict | None, *, error: Exception | None = None) -> None:
        self.row = row
        self.error = error

    async def get_by_reservation_id(self, reservation_id: str):
        assert reservation_id == "r-expired-orphan"
        if self.error is not None:
            raise self.error
        return self.row


class _NoLookupOrdersRepo:
    pass


async def test_cli_rowbacked_unresolved_no_id_reservation_is_not_safe_to_release() -> None:
    """A TTL-expired reservation with a live no-ID submission row defers.

    The runtime never frees this capacity on TTL; the operator --apply
    channel must not contradict it.
    """
    from bot_cli.commands.reservations import _demand_submission_evidence

    row = _cli_expired_row()
    assert row["classification"] == "safe_to_release"  # pre-evidence tier

    out = await _demand_submission_evidence(
        row, _CliOrdersRepo(_order_row(order_id=None, status="submitted_unconfirmed"))
    )

    assert out["classification"] == "recovery_pending"
    assert out["action"] == "defer"
    assert out["defer_reason"] == "unresolved_submission_row"


async def test_cli_proven_pre_submit_orphan_remains_releasable() -> None:
    from bot_cli.commands.reservations import _demand_submission_evidence

    row = _cli_expired_row()

    out = await _demand_submission_evidence(row, _CliOrdersRepo(None))

    assert out["classification"] == "safe_to_release"
    assert out["action"] == "release"
    assert "defer_reason" not in out


async def test_cli_orders_lookup_failure_fails_closed() -> None:
    from bot_cli.commands.reservations import _demand_submission_evidence

    row = _cli_expired_row()

    out = await _demand_submission_evidence(
        row, _CliOrdersRepo(None, error=RuntimeError("orders table down"))
    )

    assert out["classification"] == "recovery_pending"
    assert out["action"] == "defer"
    assert out["defer_reason"] == "orders_lookup_unavailable"


async def test_cli_missing_lookup_capability_fails_closed() -> None:
    from bot_cli.commands.reservations import _demand_submission_evidence

    row = _cli_expired_row()

    out = await _demand_submission_evidence(row, _NoLookupOrdersRepo())

    assert out["classification"] == "recovery_pending"
    assert out["action"] == "defer"
    assert out["defer_reason"] == "orders_lookup_unavailable"


async def test_cli_linked_order_reservation_still_defers_for_reconciliation() -> None:
    from bot_cli.commands.reservations import _demand_submission_evidence

    row = _cli_expired_row(order_id="local-order-1")
    assert row["classification"] == "deferred_reconciliation_required"

    # The evidence pass must not upgrade a linked-order reservation.
    out = await _demand_submission_evidence(row, _CliOrdersRepo(None))
    assert out["classification"] == "deferred_reconciliation_required"
    assert out["action"] == "defer"
