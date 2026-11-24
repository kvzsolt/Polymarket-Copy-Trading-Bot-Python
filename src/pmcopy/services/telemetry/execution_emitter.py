"""Venue execution, fill, ambiguity, and reconciliation telemetry emitter."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from pmcopy.services.telemetry.contracts import (
    AmbiguousEvent,
    AmbiguousOperation,
    FailureComponent,
    FailureEvent,
    FillEvent,
    FillOutcome,
    OrderOutcome,
    ReconciliationEvent,
    ReconciliationOutcome,
    TelemetryProducer,
    TelemetrySeverity,
    TradeEvent,
    TradeOutcome,
    TradeSide,
)
from pmcopy.services.telemetry.emitter_base import (
    _ORDER_OUTCOME_BY_STATUS,
    _TRADE_OUTCOME_BY_STATUS,
    _UNCONFIRMED_STATUS,
    OrderRequestLike,
    OrderResultLike,
    TelemetryEmitter,
    bounded_code,
    exception_code,
    safe_ref,
)
from pmcopy.services.telemetry.latency import PipelineLatency

if TYPE_CHECKING:
    from pmcopy.services.telemetry.service import TelemetryService


class ExecutionTelemetryEmitter(TelemetryEmitter):
    """Project venue-order outcomes and post-submission evidence.

    Args:
        service: Telemetry service, or ``None`` for a null object.
        loop: Optional owning event loop override.
    """

    __slots__ = ("_latency",)

    def __init__(
        self,
        service: "TelemetryService | None",
        loop: asyncio.AbstractEventLoop | None = None,
        *,
        latency: PipelineLatency | None = None,
    ) -> None:
        super().__init__(service, TelemetryProducer.EXECUTION, loop)
        self._latency = latency

    def order_result(self, request: OrderRequestLike, result: OrderResultLike) -> bool:
        """Publish exactly one bounded event for one execution attempt.

        Submission is never reported as a fill. An accepted result is
        ``TradeOutcome.SUBMITTED`` unless the venue itself already reported
        matched size in the same response, and an accepted submission whose POST
        outcome is ambiguous is reported as an ambiguous operation requiring
        reconciliation, never as a fill and never as a plain failure a caller
        might retry.

        Args:
            request: The execution request that was attempted.
            result: The result returned by the execution service.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._order_result(request, result))

    def _order_result(self, request: OrderRequestLike, result: OrderResultLike) -> bool:
        order_id = getattr(result, "order_id", None)
        order_ref = safe_ref(str(order_id)) if order_id else None
        status = str(getattr(result, "status", "") or "").strip().lower()
        accepted = bool(getattr(result, "accepted", False))
        error = getattr(result, "error", None)

        if accepted and status == _UNCONFIRMED_STATUS and error:
            return self._publish(
                lambda: AmbiguousEvent(
                    operation=AmbiguousOperation.ORDER_SUBMISSION,
                    order_ref=order_ref,
                    reconciliation_required=True,
                ),
                severity=TelemetrySeverity.WARNING,
            )
        task_ref = self._task_ref(request)
        if accepted:
            outcome = _TRADE_OUTCOME_BY_STATUS.get(status, TradeOutcome.SUBMITTED)
            return self._publish(
                lambda: TradeEvent(
                    outcome=outcome,
                    task_ref=task_ref,
                    side=self._side(request),
                    order_ref=order_ref,
                    order_outcome=OrderOutcome.SUBMITTED,
                ),
                severity=TelemetrySeverity.INFO,
            )
        return self._publish(
            lambda: FailureEvent(
                component=FailureComponent.EXECUTION,
                code=bounded_code(status or "failed"),
                retryable=False,
                order_outcome=_ORDER_OUTCOME_BY_STATUS.get(status),
            ),
            severity=TelemetrySeverity.WARNING,
        )

    def order_exception(self, request: OrderRequestLike, exc: BaseException) -> bool:
        """Publish a bounded failure for an exception escaping execution.

        Args:
            request: The execution request that was attempted.
            exc: The escaped exception; only its class name is published.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._order_exception(request, exc))

    def _order_exception(self, request: OrderRequestLike, exc: BaseException) -> bool:
        code = exception_code(exc)
        task_ref = self._task_ref(request)
        return self._publish(
            lambda: FailureEvent(
                component=FailureComponent.EXECUTION,
                code=code,
                retryable=False,
            ),
            severity=TelemetrySeverity.ERROR,
            correlation_id=task_ref,
        )

    def fill_observed(self, order_id: str, matched_total: object) -> bool:
        """Publish one observed fill keyed by its cumulative matched size.

        Args:
            order_id: Venue order identity.
            matched_total: Cumulative matched size or cost for the order.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        if self._latency is not None:
            self._latency.first_fill(order_id)
        return self._guarded(lambda: self._fill_observed(order_id, matched_total))

    def _fill_observed(self, order_id: str, matched_total: object) -> bool:
        order_ref = safe_ref(str(order_id))
        fill_ref = safe_ref(f"{order_id}:{matched_total}")
        return self._publish(
            lambda: FillEvent(
                outcome=FillOutcome.OBSERVED,
                order_ref=order_ref,
                fill_ref=fill_ref,
            ),
            severity=TelemetrySeverity.INFO,
        )

    def reconciliation_drift(self, order_id: str) -> bool:
        """Publish detected drift for one reconciled order.

        Args:
            order_id: Venue order identity used as the correlation token.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(
            lambda: self._publish(
                lambda: ReconciliationEvent(
                    outcome=ReconciliationOutcome.DRIFT,
                    affected_count=1,
                ),
                severity=TelemetrySeverity.WARNING,
                correlation_id=safe_ref(str(order_id)),
            )
        )

    def ambiguous_released(self, order_id: str) -> bool:
        """Publish the resolution of a previously ambiguous submission.

        Args:
            order_id: Venue order identity that reconciliation resolved.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(
            lambda: self._publish(
                lambda: AmbiguousEvent(
                    operation=AmbiguousOperation.ORDER_SUBMISSION,
                    order_ref=safe_ref(str(order_id)),
                    reconciliation_required=False,
                ),
                severity=TelemetrySeverity.NOTICE,
            )
        )

    @staticmethod
    def _task_ref(request: OrderRequestLike) -> str:
        signal_id = getattr(request, "signal_id", None)
        if signal_id:
            return safe_ref(str(signal_id))
        user_id = getattr(request, "user_id", "") or ""
        leader_id = getattr(request, "leader_id", "") or ""
        token_id = getattr(request, "token_id", "") or ""
        return safe_ref(f"{user_id}:{leader_id}:{token_id}")

    @staticmethod
    def _side(request: OrderRequestLike) -> TradeSide:
        return TradeSide(str(getattr(request, "side", "") or "").strip().upper())

