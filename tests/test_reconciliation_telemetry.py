"""Operator telemetry emitted by the canonical reconciler.

Reconciliation is the authority for post-submission truth, so it owns three
operator-visible transitions:

* an applied matched-size delta is the only *observed fill* the stream
  reports (a submission is never a fill);
* a negative delta is reported as reconciliation drift;
* an auto-released ambiguous submission resolves an earlier ambiguous event.

The fakes are reused from the existing delta-anomaly and ambiguous-timeout
suites so this module adds no new reconciler harness.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest

from pmcopy.services.execution.reconciliation_service import ReconciliationService
from pmcopy.services.telemetry import (
    AmbiguousEvent,
    AmbiguousOperation,
    FillEvent,
    FillOutcome,
    ReconciliationEvent,
    ReconciliationOutcome,
    TelemetryProducer,
    safe_ref,
)
from tests.telemetry_helpers import raising_emitters, recording_emitters
from tests.test_ambiguous_timeout_auto_release import (
    _cfg,
    _gateway,
    _record as _ambiguous_record,
    _OrdersRepo as _AmbiguousOrdersRepo,
    _PositionsRepo as _AmbiguousPositionsRepo,
    _ReservationService,
)
from tests.test_reconciliation_delta_anomaly import (
    ORDER_ID,
    _AuditLogger,
    _FillsRepo,
    _OrdersRepo,
    _PositionsRepo,
    _confirm_factory,
    _record,
)


def _service(previous_matched: str, telemetry) -> ReconciliationService:
    return ReconciliationService(
        orders_repo=_OrdersRepo(previous_matched=previous_matched),
        fills_repo=_FillsRepo(),
        positions_repo=_PositionsRepo(),
        audit_logger=_AuditLogger(),
        telemetry=telemetry,
    )


class TestFillObserved:
    @pytest.mark.asyncio
    async def test_applied_delta_reports_an_observed_fill(self) -> None:
        emitters, service = recording_emitters()
        svc = _service("0", emitters.execution)
        confirm_fill = await _confirm_factory("5", status="matched")

        applied = await svc.reconcile_trade_record(_record(), "BUY", confirm_fill=confirm_fill)

        assert applied is True
        [event] = service.payloads(FillEvent)
        assert event.outcome is FillOutcome.OBSERVED
        assert event.order_ref == safe_ref(ORDER_ID)
        # The fill reference is keyed by cumulative matched size, so a repeated
        # sweep over the same cumulative total cannot double-count.
        assert event.fill_ref == safe_ref(f"{ORDER_ID}:5")
        assert service.calls[0].producer is TelemetryProducer.EXECUTION

    @pytest.mark.asyncio
    async def test_zero_delta_reports_no_fill(self) -> None:
        emitters, service = recording_emitters()
        svc = _service("8", emitters.execution)
        confirm_fill = await _confirm_factory("8", status="confirmed")

        applied = await svc.reconcile_trade_record(_record(), "BUY", confirm_fill=confirm_fill)

        assert applied is True
        assert service.payloads(FillEvent) == []

    @pytest.mark.asyncio
    async def test_reconciler_without_telemetry_is_unchanged(self) -> None:
        svc = _service("0", None)
        confirm_fill = await _confirm_factory("5", status="matched")

        assert await svc.reconcile_trade_record(_record(), "BUY", confirm_fill=confirm_fill)

    @pytest.mark.asyncio
    async def test_raising_telemetry_backend_never_changes_the_outcome(self) -> None:
        emitters, service = raising_emitters()
        svc = _service("0", emitters.execution)
        confirm_fill = await _confirm_factory("5", status="matched")

        applied = await svc.reconcile_trade_record(_record(), "BUY", confirm_fill=confirm_fill)

        assert applied is True
        assert service.calls == 1
        assert svc.fills_repo.fills[0]["fill"].size_tokens == Decimal("5")


class TestDrift:
    @pytest.mark.asyncio
    async def test_negative_delta_reports_drift(self) -> None:
        emitters, service = recording_emitters()
        svc = _service("8", emitters.execution)
        confirm_fill = await _confirm_factory("5", status="confirmed")

        applied = await svc.reconcile_trade_record(
            _record(),
            "BUY",
            confirm_fill=confirm_fill,
            logger=logging.getLogger("TradePipeline"),
        )

        assert applied is True
        [event] = service.payloads(ReconciliationEvent)
        assert event.outcome is ReconciliationOutcome.DRIFT
        assert event.affected_count == 1
        assert service.calls[0].correlation_id == safe_ref(ORDER_ID)
        # Drift is not a fill.
        assert service.payloads(FillEvent) == []


class TestAmbiguousRelease:
    @pytest.mark.asyncio
    async def test_auto_release_resolves_the_ambiguous_submission(self) -> None:
        emitters, service = recording_emitters()
        svc = ReconciliationService(
            orders_repo=_AmbiguousOrdersRepo(),
            fills_repo=None,
            positions_repo=_AmbiguousPositionsRepo(local_size="0"),
            reservation_service=_ReservationService(),
            clob_gateway=_gateway(order_status="absent", fills=[], list_orders=[]),
            telemetry=emitters.execution,
        )

        released = await svc._maybe_auto_release_ambiguous(
            _ambiguous_record(),
            side="BUY",
            order_confirmation_cfg=_cfg(),
        )

        assert released is True
        [event] = service.payloads(AmbiguousEvent)
        assert event.operation is AmbiguousOperation.ORDER_SUBMISSION
        assert event.reconciliation_required is False
        assert event.order_ref == safe_ref("ord-amb-1")

    @pytest.mark.asyncio
    async def test_aborted_release_reports_nothing(self) -> None:
        emitters, service = recording_emitters()
        svc = ReconciliationService(
            orders_repo=_AmbiguousOrdersRepo(),
            fills_repo=None,
            positions_repo=_AmbiguousPositionsRepo(local_size="0"),
            reservation_service=_ReservationService(),
            clob_gateway=_gateway(order_status="live", fills=[], list_orders=[]),
            telemetry=emitters.execution,
        )

        released = await svc._maybe_auto_release_ambiguous(
            _ambiguous_record(),
            side="BUY",
            order_confirmation_cfg=_cfg(),
        )

        assert released is False
        assert service.calls == []
