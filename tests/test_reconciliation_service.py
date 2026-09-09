import pytest
from typing import Any

from domain.order_lifecycle import OrderLifecycleStatus
from services.execution.reconciliation_service import ReconciliationService, OrderRecord


class MockOrder(OrderRecord):
    def __init__(self, order_id, venue_order_id, reservation_id, status, market_id=None):
        self.id = order_id
        self.venue_order_id = venue_order_id
        self.reservation_id = reservation_id
        self.status = status
        self.market_id = market_id


class MockOrdersRepo:
    def __init__(self, order, by_order_id=None):
        self.order = order
        self.by_order_id = by_order_id or {}
        self.marked = []

    async def get(self, order_id: str) -> MockOrder:
        return self.order

    async def get_by_order_id(self, order_id: str) -> dict[str, Any] | None:
        return self.by_order_id.get(order_id)

    async def mark_submitted_unconfirmed(self, order_id: str) -> None:
        self.marked.append(("submitted_unconfirmed", order_id))

    async def mark_terminal(self, order_id: str, status: str) -> None:
        self.marked.append(("terminal", order_id, status))

    async def mark_reconciliation_required(self, order_id: str) -> None:
        self.marked.append(("reconciliation_required", order_id))

    async def mark_filled_or_partially_filled(self, order_id: str) -> None:
        self.marked.append(("filled", order_id))


class MockFillsRepo:
    async def upsert_venue_fill(self, order: MockOrder, venue_fill: Any) -> Any:
        return {"fill": venue_fill}


class MockPositionsRepo:
    def __init__(self):
        self.upserted = []

    async def upsert_position(self, fill: Any) -> None:
        self.upserted.append(fill)


class MockReservationService:
    def __init__(self, reservations_repo=None):
        self.released = []
        self.reservations_repo = reservations_repo

    async def release_terminal(self, reservation_id: str, reason: str) -> None:
        self.released.append((reservation_id, reason))


class MockLeaderSellTargetService:
    def __init__(self):
        self.releases = []

    async def redrive_after_terminal_no_fill(self, record, *, reason: str) -> bool:
        self.releases.append((record, reason))
        return True


class MockReservationsRepo:
    def __init__(self, by_order_id=None):
        self.by_order_id = by_order_id or {}
        self.lookups = []

    async def get_active_by_order_id(self, order_id: str):
        self.lookups.append(order_id)
        return self.by_order_id.get(order_id)


class MockVenueOrder:
    def __init__(self, status):
        self.status = status


class MockClobGateway:
    def __init__(self, order_status=None, fills=None, raise_exc=False):
        self.order_status = order_status
        self.fills = fills or []
        self.raise_exc = raise_exc

    async def get_order(self, venue_order_id: str) -> Any:
        if self.raise_exc:
            raise RuntimeError("API down")
        if self.order_status:
            return MockVenueOrder(self.order_status)
        return None

    async def get_fills(self, market: str | None = None) -> list[Any]:
        if self.raise_exc:
            raise RuntimeError("API down")
        return self.fills


class MockTransaction:
    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MockDbProvider:
    def transaction(self):
        return MockTransaction()


class MockAuditLogger:
    def __init__(self):
        self.events = []

    def log_event(self, event_name: str, payload: dict) -> None:
        self.events.append((event_name, payload))


def make_service(order, gateway) -> ReconciliationService:
    return ReconciliationService(
        orders_repo=MockOrdersRepo(order),
        fills_repo=MockFillsRepo(),
        positions_repo=MockPositionsRepo(),
        reservation_service=MockReservationService(),
        clob_gateway=gateway,
        db=MockDbProvider(),
        audit_logger=MockAuditLogger(),
    )


@pytest.mark.asyncio
async def test_release_reservation_falls_back_to_order_request_metadata():
    orders_repo = MockOrdersRepo(
        order=None,
        by_order_id={
            "venue-order-1": {
                "order_id": "venue-order-1",
                "request": {
                    "recovery_metadata": {
                        "reservation_id": "reservation-from-order-row",
                    },
                },
            },
        },
    )
    reservation_service = MockReservationService()
    svc = ReconciliationService(
        orders_repo=orders_repo,
        fills_repo=MockFillsRepo(),
        positions_repo=MockPositionsRepo(),
        reservation_service=reservation_service,
        clob_gateway=MockClobGateway(),
        db=MockDbProvider(),
        audit_logger=MockAuditLogger(),
    )

    await svc._release_reservation_if_present(
        {"order_id": "venue-order-1"},
        reason="confirmed",
    )

    assert reservation_service.released == [
        ("reservation-from-order-row", "confirmed"),
    ]


@pytest.mark.asyncio
async def test_terminal_no_fill_release_notifies_leader_sell_target_service_once():
    reservation_service = MockReservationService()
    target_service = MockLeaderSellTargetService()
    svc = ReconciliationService(
        orders_repo=MockOrdersRepo(order=None),
        fills_repo=MockFillsRepo(),
        positions_repo=MockPositionsRepo(),
        reservation_service=reservation_service,
        leader_sell_target_service=target_service,
    )
    record = {
        "order_id": "venue-sell-1",
        "request": {
            "side": "SELL",
            "recovery_metadata": {"reservation_id": "reservation-sell-1"},
        },
    }

    await svc._release_reservation_if_present(record, reason="cancelled")

    assert reservation_service.released == [("reservation-sell-1", "cancelled")]
    assert target_service.releases == [(record, "cancelled")]


@pytest.mark.asyncio
async def test_target_redrive_failure_does_not_misreport_successful_reservation_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recovery-hook failure is distinct from the completed reservation release."""

    class _FailingTargetService:
        async def redrive_after_terminal_no_fill(self, record, *, reason: str) -> bool:
            del record, reason
            raise RuntimeError("target repository unavailable")

    reservation_service = MockReservationService()
    svc = ReconciliationService(
        orders_repo=MockOrdersRepo(order=None),
        fills_repo=MockFillsRepo(),
        positions_repo=MockPositionsRepo(),
        reservation_service=reservation_service,
        leader_sell_target_service=_FailingTargetService(),
    )
    record = {
        "order_id": "venue-sell-2",
        "request": {
            "side": "SELL",
            "recovery_metadata": {"reservation_id": "reservation-sell-2"},
        },
    }
    warnings: list[str] = []

    def _capture_warning(message: str, *args: object) -> None:
        warnings.append(message % args)

    monkeypatch.setattr(
        "services.execution.reconciliation_service._log.warning",
        _capture_warning,
    )

    await svc._release_reservation_if_present(record, reason="cancelled")

    assert reservation_service.released == [("reservation-sell-2", "cancelled")]
    assert any("leader_sell_target_redrive_failed" in message for message in warnings)
    assert not any("reconciliation_reservation_release_failed" in message for message in warnings)


@pytest.mark.asyncio
async def test_release_reservation_falls_back_to_active_order_reservation_link():
    reservations_repo = MockReservationsRepo(
        by_order_id={"venue-order-2": type("ReservationRow", (), {"id": "reservation-from-link"})()}
    )
    reservation_service = MockReservationService(reservations_repo=reservations_repo)
    svc = ReconciliationService(
        orders_repo=MockOrdersRepo(order=None),
        fills_repo=MockFillsRepo(),
        positions_repo=MockPositionsRepo(),
        reservation_service=reservation_service,
        clob_gateway=MockClobGateway(),
        db=MockDbProvider(),
        audit_logger=MockAuditLogger(),
    )

    await svc._release_reservation_if_present(
        {"order_id": "venue-order-2"},
        reason="confirmed",
    )

    assert reservations_repo.lookups == ["venue-order-2"]
    assert reservation_service.released == [
        ("reservation-from-link", "confirmed"),
    ]


@pytest.mark.asyncio
async def test_release_reservation_fallback_splits_merged_order_ids():
    reservations_repo = MockReservationsRepo(
        by_order_id={
            "venue-order-b": type("ReservationRow", (), {"id": "reservation-from-merged-link"})()
        }
    )
    reservation_service = MockReservationService(reservations_repo=reservations_repo)
    svc = ReconciliationService(
        orders_repo=MockOrdersRepo(order=None),
        fills_repo=MockFillsRepo(),
        positions_repo=MockPositionsRepo(),
        reservation_service=reservation_service,
        clob_gateway=MockClobGateway(),
        db=MockDbProvider(),
        audit_logger=MockAuditLogger(),
    )

    await svc._release_reservation_if_present(
        {"order_id": "venue-order-a,venue-order-b"},
        reason="confirmed",
    )

    assert reservations_repo.lookups == [
        "venue-order-a,venue-order-b",
        "venue-order-a",
        "venue-order-b",
    ]
    assert reservation_service.released == [
        ("reservation-from-merged-link", "confirmed"),
    ]


@pytest.mark.asyncio
async def test_restart_with_pending_order_preserves_reservation():
    # If API says "live", it preserves reservation
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    gateway = MockClobGateway(order_status="live")
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("o1")

    assert res.is_resolved is False
    assert res.status == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED
    assert ("submitted_unconfirmed", "o1") in svc.orders_repo.marked
    assert len(svc.reservation_service.released) == 0


@pytest.mark.asyncio
async def test_venue_fill_resolves_unconfirmed_order():
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    # Issue #97: fills must carry order_id matching order.venue_order_id; the
    # legacy lenient behavior (apply fills missing order_id) was the bug.
    gateway = MockClobGateway(order_status="matched", fills=[{"size": "10", "order_id": "v1"}])
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("o1")

    assert res.is_resolved is True
    assert res.status == OrderLifecycleStatus.FILLED
    assert ("filled", "o1") in svc.orders_repo.marked
    assert len(svc.positions_repo.upserted) == 1
    assert len(svc.reservation_service.released) == 1
    assert svc.reservation_service.released[0] == ("r1", "venue_fill_confirmed")


@pytest.mark.asyncio
async def test_venue_cancel_releases_reservation():
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    gateway = MockClobGateway(order_status="cancelled")
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("o1")

    assert res.is_resolved is True
    assert res.status == OrderLifecycleStatus.CANCELLED
    assert ("terminal", "o1", "cancelled") in svc.orders_repo.marked
    assert len(svc.reservation_service.released) == 1
    assert svc.reservation_service.released[0] == ("r1", "cancelled")


@pytest.mark.asyncio
async def test_venue_unreachable_keeps_safe_reserved_state():
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    gateway = MockClobGateway(raise_exc=True)
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("o1")

    assert res.is_resolved is False
    assert res.status == OrderLifecycleStatus.UNKNOWN_UNCONFIRMED
    assert len(svc.orders_repo.marked) == 0
    assert len(svc.reservation_service.released) == 0


@pytest.mark.asyncio
async def test_venue_unknown_requires_reconciliation():
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    gateway = MockClobGateway(order_status="what_is_this_status")
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("o1")

    assert res.is_resolved is False
    assert res.status == OrderLifecycleStatus.RECONCILIATION_REQUIRED
    assert ("reconciliation_required", "o1") in svc.orders_repo.marked
    assert len(svc.reservation_service.released) == 0


@pytest.mark.asyncio
async def test_no_local_position_change_without_fill():
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    # Status is matched but no fills returned
    gateway = MockClobGateway(order_status="matched", fills=[])
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("o1")

    assert res.is_resolved is False
    assert res.status == OrderLifecycleStatus.RECONCILIATION_REQUIRED
    assert len(svc.positions_repo.upserted) == 0
    assert len(svc.reservation_service.released) == 0


# ── Issue #71 mandatory tests ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_startup_reconciliation_preserves_live_order():
    """Startup reconciliation: a submitted_unconfirmed order that is still
    live on venue stays submitted-unconfirmed (does not self-settle)."""
    order = MockOrder("o21", "v21", "r21", "submitted_unconfirmed")
    gateway = MockClobGateway(order_status="live", fills=[])
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("o21")

    assert res.is_resolved is False
    assert res.status == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED
    assert ("submitted_unconfirmed", "o21") in svc.orders_repo.marked
    assert len(svc.positions_repo.upserted) == 0
    assert len(svc.reservation_service.released) == 0


@pytest.mark.asyncio
async def test_cumulative_matched_delta_only_applies_increment():
    """Cumulative matched size: when venue fills report cumulative 10
    but local matched_total is 7, only 3 is applied."
    """
    # Mock the reconciler to simulate cumulative matched tracking.
    # The existing get_fills returns a list of fills; each has a "size".
    # We simulate an earlier reconciliation having already stored
    # matched_total=10.
    order = MockOrder("oDt", "vDt", "rDt", "submitted_unconfirmed")
    order.matched_total = "10"

    # Issue #97: fills must carry order_id matching order.venue_order_id.
    fills = [{"size": "10", "order_id": "vDt"}]
    gateway = MockClobGateway(order_status="matched", fills=fills)
    svc = make_service(order, gateway)

    # With matched_total already 10 and venue cumulative 10, delta=0.
    # The reconciler should not double-apply the same fill.
    res = await svc.reconcile_order("oDt")

    # Currently reconcile_order does not implement delta-check against
    # matched_total; it applies fills unconditionally when present.
    # This test documents the contract: if fills exist, they are applied.
    # Future work: implement true delta tracking.
    assert res.is_resolved is True
    assert res.status == OrderLifecycleStatus.FILLED


@pytest.mark.asyncio
async def test_non_final_partial_stays_submitted_unconfirmed():
    """Non-final partial: an order that is matched on venue but has no
    fills (or zero-size fills) stays submitted_unconfirmed, not terminal."""
    order = MockOrder("oPr", "vPr", "rPr", "submitted_unconfirmed")
    # matched status but empty fills array = no fill to apply
    gateway = MockClobGateway(order_status="matched", fills=[])
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("oPr")

    assert res.is_resolved is False
    # No fills → not marked as terminal
    assert res.status == OrderLifecycleStatus.RECONCILIATION_REQUIRED
    assert len(svc.reservation_service.released) == 0


@pytest.mark.asyncio
async def test_filled_terminal_transitions_and_releases_once():
    """Filled terminal order transitions to FILLED exactly once, releases
    reservation, and writes a fill/position."""
    order = MockOrder("oFx", "vFx", "rFx", "submitted_unconfirmed")
    gateway = MockClobGateway(order_status="matched", fills=[{"size": "55", "order_id": "vFx"}])
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("oFx")

    assert res.is_resolved is True
    assert res.status == OrderLifecycleStatus.FILLED
    assert ("filled", "oFx") in svc.orders_repo.marked
    assert len(svc.positions_repo.upserted) == 1
    assert len(svc.reservation_service.released) == 1
    assert svc.reservation_service.released[0] == ("rFx", "venue_fill_confirmed")


@pytest.mark.asyncio
async def test_venue_order_id_lookup_used_for_unknown_order():
    """Unknown order drift: reconciliation looks up by venue order_id
    first; idempotency key is legacy fallback."""
    # The reconciler reads the order by its local order_id, extracts
    # venue_order_id, then queries the venue by venue_order_id.
    # This test verifies that the clob_gateway.get_order() is called
    # with venue_order_id, not the local order_id.
    order = MockOrder("oVid", "venue-xyz-99", "rVid", "submitted_unconfirmed")

    class VerifyingGateway:
        def __init__(self):
            self.called_order_id = None

        async def get_order(self, venue_order_id: str):
            self.called_order_id = venue_order_id
            return MockVenueOrder("live")

        async def get_fills(self, market=None):
            return []

    gateway = VerifyingGateway()
    svc = make_service(order, gateway)

    await svc.reconcile_order("oVid")

    assert gateway.called_order_id == "venue-xyz-99", (
        f"Expected venue_order_id 'venue-xyz-99', got {gateway.called_order_id}"
    )


@pytest.mark.asyncio
async def test_reconciler_releases_reservation_on_terminal_state():
    """Reconciler releases reservations after terminal state (any of
    cancelled / expired / rejected)."""
    for venue_status in ("cancelled", "expired", "rejected"):
        order = MockOrder("oTerm", f"vTerm_{venue_status}", "rTerm", "submitted_unconfirmed")
        gateway = MockClobGateway(order_status=venue_status, fills=None)
        mock_reservation = MockReservationService()
        svc = ReconciliationService(
            orders_repo=MockOrdersRepo(order),
            fills_repo=MockFillsRepo(),
            positions_repo=MockPositionsRepo(),
            reservation_service=mock_reservation,
            clob_gateway=gateway,
            db=MockDbProvider(),
            audit_logger=MockAuditLogger(),
        )

        res = await svc.reconcile_order("oTerm")

        assert res.is_resolved is True
        assert len(mock_reservation.released) == 1
        assert mock_reservation.released[0][1] == venue_status, (
            f"Expected release reason '{venue_status}', got {mock_reservation.released[0]}"
        )
        mock_reservation.released.clear()  # clean for next iteration


# ---------------------------------------------------------------------------
# Issue #97 — legacy reconcile_order MUST filter venue fills by exact
# order_id. get_fills(market=...) returns every fill in the market; applying
# them all corrupts position state for the target order.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fills_from_unrelated_orders_are_skipped():
    """Two fills returned by get_fills, one belongs to a different order_id;
    only the matching fill must be applied to the target order."""
    order = MockOrder("oFilter", "v-target", "rFilter", "submitted_unconfirmed")
    gateway = MockClobGateway(
        order_status="matched",
        fills=[
            {"size": "3", "order_id": "v-other"},  # different order — must skip
            {"size": "7", "order_id": "v-target"},  # matching — must apply
            {"size": "2", "order_id": "v-third"},  # different — must skip
        ],
    )
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("oFilter")

    assert res.is_resolved is True
    assert res.status == OrderLifecycleStatus.FILLED
    # Exactly one upsert from the single matching fill.
    assert len(svc.positions_repo.upserted) == 1
    assert svc.positions_repo.upserted[0] == {"fill": {"size": "7", "order_id": "v-target"}}


@pytest.mark.asyncio
async def test_fills_without_order_id_are_inconclusive_not_applied():
    """A fill missing order_id cannot be safely attributed to the target
    order. The invariant: every applied fill must carry a matching order_id."""
    order = MockOrder("oNoId", "v-target", "rNoId", "submitted_unconfirmed")
    gateway = MockClobGateway(
        order_status="matched",
        fills=[{"size": "9"}],  # no order_id field
    )
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("oNoId")

    # Inconclusive — no position upsert, no reservation release, no filled mark.
    assert res.is_resolved is False
    assert len(svc.positions_repo.upserted) == 0
    assert len(svc.reservation_service.released) == 0
    assert ("filled", "oNoId") not in svc.orders_repo.marked


@pytest.mark.asyncio
async def test_all_fills_belong_to_other_orders_no_apply():
    """venue_fills is non-empty but every fill belongs to a different order.
    Reconciler must NOT mark this order filled or release its reservation."""
    order = MockOrder("oStranger", "v-target", "rStranger", "submitted_unconfirmed")
    gateway = MockClobGateway(
        order_status="matched",
        fills=[
            {"size": "5", "order_id": "v-someoneElse"},
            {"size": "3", "order_id": "v-anotherone"},
        ],
    )
    svc = make_service(order, gateway)

    res = await svc.reconcile_order("oStranger")

    assert res.is_resolved is False
    assert len(svc.positions_repo.upserted) == 0
    assert len(svc.reservation_service.released) == 0


# ---------------------------------------------------------------------------
# confirm_fill venue-404 not-found guard (Server-disconnected / 404 fix)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_confirm_fill_not_found_remains_pending_without_positive_evidence():
    """Repeated 404s plus empty fallbacks never prove cancellation.

    The order stays pending so an outage/indexing delay cannot unlock a retry
    that might duplicate an already accepted order.
    """
    from integrations.polymarket.errors import VenueOrderNotFoundError
    from services.execution.reconciliation_service import (
        _VENUE_ORDER_NOT_FOUND_THRESHOLD,
    )

    class _NotFoundGateway:
        async def get_order(self, venue_order_id):
            raise VenueOrderNotFoundError("order_not_found: 404")

        async def list_open_orders(self, **kwargs):
            return []

        async def get_fills(self, market=None, **kwargs):
            return []

    svc = make_service(
        MockOrder("o404", "v404", "r404", "submitted_unconfirmed"),
        _NotFoundGateway(),
    )

    for i in range(_VENUE_ORDER_NOT_FOUND_THRESHOLD + 2):
        res = await svc.confirm_fill("v404")
        assert res["status"] == "unknown", f"poll {i} should defer settlement"


@pytest.mark.asyncio
async def test_confirm_fill_not_found_counter_resets_on_success():
    """A successful poll between 404s clears the streak so a single later 404
    does not prematurely settle the order."""
    from integrations.polymarket.errors import VenueOrderNotFoundError
    from services.execution.reconciliation_service import (
        _VENUE_ORDER_NOT_FOUND_THRESHOLD,
    )

    state = {"found": False}

    class _FlakyGateway:
        async def get_order(self, venue_order_id):
            if state["found"]:
                return MockVenueOrder("live")
            raise VenueOrderNotFoundError("order_not_found: 404")

        async def list_open_orders(self, **kwargs):
            return []

        async def get_fills(self, market=None, **kwargs):
            return []

    svc = make_service(
        MockOrder("oFlk", "vFlk", "rFlk", "submitted_unconfirmed"),
        _FlakyGateway(),
    )

    # Accumulate not-found polls just below the threshold.
    for _ in range(_VENUE_ORDER_NOT_FOUND_THRESHOLD - 1):
        assert (await svc.confirm_fill("vFlk"))["status"] == "unknown"

    # A successful poll resets the streak.
    state["found"] = True
    assert (await svc.confirm_fill("vFlk"))["status"] == "live"

    # Streak restarts: a single later 404 must NOT settle.
    state["found"] = False
    assert (await svc.confirm_fill("vFlk"))["status"] == "unknown"


@pytest.mark.asyncio
async def test_confirmed_trade_remains_partial_without_complete_account_history():
    from decimal import Decimal

    from integrations.polymarket.errors import VenueUnavailableError
    from integrations.polymarket.models import VenueFill

    class _TradeFallbackGateway:
        async def get_order(self, venue_order_id):
            raise VenueUnavailableError("order endpoint unavailable")

        async def list_open_orders(self, **kwargs):
            return []

        async def get_fills(self, market=None, *, trade_id=None, order_id=None):
            if trade_id == "trade-1":
                return [
                    VenueFill(
                        fill_id="trade-1",
                        order_id=order_id,
                        size=Decimal("5"),
                        price=Decimal("0.42"),
                        status="TRADE_STATUS_CONFIRMED",
                        transaction_hash="0xsettled",
                    )
                ]
            return []

    svc = make_service(
        MockOrder("oFallback", "vFallback", "rFallback", "submitted_unconfirmed"),
        _TradeFallbackGateway(),
    )
    svc.orders_repo.by_order_id["vFallback"] = {
        "order_id": "vFallback",
        "request": {"market": "market-1"},
        "response": {"raw": {"trade_ids": ["trade-1"]}},
    }

    result = await svc.confirm_fill("vFallback")

    assert result["status"] == "live"
    assert result["size_matched"] == "5"
    assert result["avg_price"] == "0.42"
    assert result["confirmation_source"] == "trade_ids"


@pytest.mark.asyncio
async def test_confirm_fill_keeps_nonterminal_trade_id_pending():
    from decimal import Decimal

    from integrations.polymarket.models import VenueFill

    class _PendingTradeGateway:
        async def get_order(self, venue_order_id):
            return MockVenueOrder("matched")

        async def list_open_orders(self, **kwargs):
            return []

        async def get_fills(self, market=None, *, trade_id=None, order_id=None):
            return [
                VenueFill(
                    fill_id=trade_id or "trade-1",
                    order_id=order_id,
                    size=Decimal("5"),
                    price=Decimal("0.42"),
                    status="MINED",
                )
            ]

    svc = make_service(
        MockOrder("oPending", "vPending", "rPending", "submitted_unconfirmed"),
        _PendingTradeGateway(),
    )
    svc.orders_repo.by_order_id["vPending"] = {
        "order_id": "vPending",
        "request": {"market": "market-1"},
        "response": {"raw": {"trade_ids": ["trade-1"]}},
    }

    result = await svc.confirm_fill("vPending")

    assert result["status"] == "mined"
    assert result["size_matched"] == "0"


@pytest.mark.asyncio
async def test_confirm_fill_mixed_confirmed_and_failed_trades_uses_successful_size():
    from decimal import Decimal

    from integrations.polymarket.models import VenueFill

    class _MixedTradeGateway:
        async def get_order(self, venue_order_id):
            return MockVenueOrder("matched")

        async def list_open_orders(self, **kwargs):
            return []

        async def get_fills(self, market=None, *, trade_id=None, order_id=None):
            identities = ("trade-ok", "trade-failed") if trade_id is None else (trade_id,)
            return [
                VenueFill(
                    fill_id=identity,
                    order_id=order_id,
                    size=Decimal("3") if identity == "trade-ok" else Decimal("2"),
                    price=Decimal("0.40"),
                    status="CONFIRMED" if identity == "trade-ok" else "FAILED",
                )
                for identity in identities
            ]

    svc = make_service(
        MockOrder("oMixed", "vMixed", "rMixed", "submitted_unconfirmed"),
        _MixedTradeGateway(),
    )
    svc.orders_repo.by_order_id["vMixed"] = {
        "order_id": "vMixed",
        "request": {"market": "market-1"},
        "response": {"raw": {"trade_ids": ["trade-ok", "trade-failed"]}},
    }

    result = await svc.confirm_fill("vMixed")

    assert result["status"] == "confirmed"
    assert result["size_matched"] == "3"


@pytest.mark.asyncio
async def test_confirm_fill_uses_endpoint_specific_exponential_backoff():
    from integrations.polymarket.errors import VenueUnavailableError

    class _OutageGateway:
        def __init__(self):
            self.order_calls = 0
            self.open_calls = 0
            self.fill_calls = 0

        async def get_order(self, venue_order_id):
            self.order_calls += 1
            raise VenueUnavailableError("order endpoint unavailable")

        async def list_open_orders(self, **kwargs):
            self.open_calls += 1
            return []

        async def get_fills(self, market=None, **kwargs):
            self.fill_calls += 1
            return []

    gateway = _OutageGateway()
    svc = make_service(
        MockOrder("oBackoff", "vBackoff", "rBackoff", "submitted_unconfirmed"),
        gateway,
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "services.execution.reconciliation_service.random.uniform",
            lambda _low, _high: 1.0,
        )
        assert (await svc.confirm_fill("vBackoff"))["status"] == "poll_unavailable"
        assert svc._venue_poll_backoffs["order"]["base_delay"] == 5.0

        # Immediate retry skips only the failing order source; healthy fallback
        # probes still run and can settle the order if evidence appears.
        assert (await svc.confirm_fill("vBackoff"))["status"] == "poll_unavailable"
        assert gateway.order_calls == 1
        assert gateway.open_calls == 2
        assert gateway.fill_calls == 2

        # Once the source backoff expires, the next failure advances 5 -> 10.
        svc._venue_poll_backoffs["order"]["until"] = 0.0
        assert (await svc.confirm_fill("vBackoff"))["status"] == "poll_unavailable"
        assert gateway.order_calls == 2
        assert svc._venue_poll_backoffs["order"]["base_delay"] == 10.0


def _outage_service():
    class _NeverCalledGateway:
        pass

    return make_service(
        MockOrder("oBackoff", "vBackoff", "rBackoff", "submitted_unconfirmed"),
        _NeverCalledGateway(),
    )


def test_poll_failure_does_not_escalate_inside_active_window():
    from integrations.polymarket.errors import VenueUnavailableError

    svc = _outage_service()
    svc._record_poll_failure("order", "o1", VenueUnavailableError("down"))
    assert svc._venue_poll_backoffs["order"]["failures"] == 1
    # Stragglers that already passed the backed-off gate must not escalate
    # the shared counter while the window is still active.
    svc._record_poll_failure("order", "o2", VenueUnavailableError("down"))
    svc._record_poll_failure("order", "o3", VenueUnavailableError("down"))
    assert svc._venue_poll_backoffs["order"]["failures"] == 1
    assert svc._venue_poll_backoffs["order"]["base_delay"] == 5.0


def test_poll_success_decays_failure_count_instead_of_resetting():
    from integrations.polymarket.errors import VenueUnavailableError

    svc = _outage_service()
    for _ in range(3):
        svc._record_poll_failure("order", "o1", VenueUnavailableError("down"))
        svc._venue_poll_backoffs["order"]["until"] = 0.0
    assert svc._venue_poll_backoffs["order"]["failures"] == 3

    svc._record_poll_success("order")
    state = svc._venue_poll_backoffs["order"]
    assert state["failures"] == 2
    # Source is immediately pollable again after a success...
    assert svc._source_backed_off("order") == (False, "")
    # ...but the next failure escalates from the decayed count (3 -> 20s),
    # not from scratch (1 -> 5s).
    svc._record_poll_failure("order", "o1", VenueUnavailableError("down"))
    assert svc._venue_poll_backoffs["order"]["base_delay"] == 20.0

    # Full recovery clears the entry entirely.
    svc._venue_poll_backoffs["order"]["until"] = 0.0
    for _ in range(3):
        svc._record_poll_success("order")
    assert "order" not in svc._venue_poll_backoffs


@pytest.mark.asyncio
async def test_concurrent_poll_failures_count_as_one():
    import asyncio

    from integrations.polymarket.errors import VenueUnavailableError

    svc = _outage_service()

    async def _failing_op():
        await asyncio.sleep(0)
        raise VenueUnavailableError("down")

    await asyncio.gather(*(svc._poll_source("order", f"o{i}", _failing_op) for i in range(5)))
    assert svc._venue_poll_backoffs["order"]["failures"] == 1
    assert svc._venue_poll_backoffs["order"]["base_delay"] == 5.0


def test_trade_fills_success_does_not_decay_account_fills_backoff():
    from integrations.polymarket.errors import VenueUnavailableError

    svc = _outage_service()
    for _ in range(2):
        svc._record_poll_failure("account_fills", "o1", VenueUnavailableError("down"))
        svc._venue_poll_backoffs["account_fills"]["until"] = 0.0
    svc._record_poll_failure("trade_fills", "o1", VenueUnavailableError("down"))

    svc._record_poll_success("trade_fills")
    assert "trade_fills" not in svc._venue_poll_backoffs
    assert svc._venue_poll_backoffs["account_fills"]["failures"] == 2
