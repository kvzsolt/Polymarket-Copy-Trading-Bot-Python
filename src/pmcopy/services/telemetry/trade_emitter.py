"""Copy-task and trade-pipeline decision telemetry emitter."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import TYPE_CHECKING

from pmcopy.services.telemetry.contracts import (
    FailureComponent,
    FailureEvent,
    ReconciliationEvent,
    ReconciliationOutcome,
    SkipEvent,
    SkipReason,
    TaskDecisionDetails,
    TelemetryPayload,
    TelemetryProducer,
    TelemetrySeverity,
    TradeEvent,
    TradeOutcome,
    TradeSide,
)
from pmcopy.services.telemetry.emitter_base import (
    PipelineOutcomeLike,
    SignalLike,
    TelemetryEmitter,
    _skip_payload_factory,
    exception_code,
    safe_ref,
    skip_reason_for_code,
)

if TYPE_CHECKING:
    from pmcopy.services.copy_tasks.domain import CopyTask
    from pmcopy.services.copy_tasks.event_routing import (
        CopyTaskSourceEvent,
        TaskRouteResult,
    )
    from pmcopy.services.telemetry.service import TelemetryService


class TradeTelemetryEmitter(TelemetryEmitter):
    """Project post-submission copy-pipeline outcomes onto bounded events.

    The pipeline owns skips only. Venue outcomes belong to
    :class:`ExecutionTelemetryEmitter`, so an accepted, failed, or unconfirmed
    pipeline row produces no event here and is never counted twice.

    Args:
        service: Telemetry service, or ``None`` for a null object.
        loop: Optional owning event loop override.
    """

    __slots__ = ()

    def __init__(
        self,
        service: "TelemetryService | None",
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(service, TelemetryProducer.WORKER, loop)

    def task_decision(
        self,
        task: "CopyTask",
        event: "CopyTaskSourceEvent",
        result: "TaskRouteResult",
        evaluated_at: datetime,
        max_signal_age_seconds: float,
    ) -> bool:
        """Publish actual task admission evidence without counting another venue outcome."""

        def payload() -> TelemetryPayload:
            runtime = result.runtime_decision
            envelope = None if runtime is None else runtime.envelope
            details = TaskDecisionDetails(
                account_ref=safe_ref(task.follower_account),
                task_version=str(task.version),
                reason_code=result.reason,
                source_price=event.trade.price,
                source_shares=event.trade.shares,
                source_timestamp=event.trade.venue_timestamp,
                evaluated_at=evaluated_at,
                max_signal_age_seconds=max_signal_age_seconds,
                price_limit=None if envelope is None else envelope.entry_price_decision.price_limit,
            )
            if not result.allowed:
                return SkipEvent(
                    reason=skip_reason_for_code(result.reason),
                    task_ref=safe_ref(task.task_id),
                    decision=details,
                )
            return TradeEvent(
                outcome=TradeOutcome.CANDIDATE_EVALUATED,
                task_ref=safe_ref(task.task_id),
                side=TradeSide(event.trade.side.value),
                order_outcome=None,
                decision=details,
            )

        return self._publish(payload, severity=TelemetrySeverity.INFO)

    def pipeline_results(
        self,
        user_id: str,
        leader_wallet: str,
        results: Iterable[PipelineOutcomeLike],
        signals_by_key: Mapping[str, SignalLike],
    ) -> bool:
        """Publish one bounded skip event per skipped or paper-tier result.

        Args:
            user_id: Follower account identity.
            leader_wallet: Leader the results belong to.
            results: Pipeline outcome rows, already returned by the pipeline.
            signals_by_key: Normalized signals keyed by trade key, used to
                recover the copy correlation id.

        Returns:
            ``True`` when every derived event was accepted; ``False`` when the
            emitter is disabled or any event was dropped.
        """
        if not self.enabled:
            return False
        return self._guarded(
            lambda: self._pipeline_results(user_id, leader_wallet, results, signals_by_key)
        )

    def _pipeline_results(
        self,
        user_id: str,
        leader_wallet: str,
        results: Iterable[PipelineOutcomeLike],
        signals_by_key: Mapping[str, SignalLike],
    ) -> bool:
        fallback_ref = safe_ref(f"{user_id}:{leader_wallet}")
        accepted = True
        for result in results:
            reason = self._skip_reason(result)
            if reason is None:
                continue
            trade_key = getattr(result, "trade_key", "") or ""
            task_ref = safe_ref(trade_key) if trade_key else fallback_ref
            severity = (
                TelemetrySeverity.INFO
                if reason is SkipReason.PAPER_ONLY
                else TelemetrySeverity.NOTICE
            )
            accepted = (
                self._publish(
                    _skip_payload_factory(reason, task_ref),
                    severity=severity,
                    correlation_id=self._correlation(signals_by_key, trade_key),
                )
                and accepted
            )
        return accepted

    def pipeline_failed(self, user_id: str, leader_wallet: str, exc: BaseException) -> bool:
        """Publish a bounded worker failure for an escaped pipeline exception.

        Args:
            user_id: Follower account identity.
            leader_wallet: Leader whose batch raised.
            exc: The escaped exception; only its class name is published.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._pipeline_failed(user_id, leader_wallet, exc))

    def _pipeline_failed(self, user_id: str, leader_wallet: str, exc: BaseException) -> bool:
        code = exception_code(exc)
        correlation = safe_ref(leader_wallet) if leader_wallet else safe_ref(user_id)
        return self._publish(
            lambda: FailureEvent(
                component=FailureComponent.WORKER,
                code=code,
                retryable=False,
            ),
            severity=TelemetrySeverity.ERROR,
            correlation_id=correlation,
        )

    def reconciliation_run(self, affected: int) -> bool:
        """Publish one completed reconciliation sweep.

        Args:
            affected: Number of records the sweep touched.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._publish(
            lambda: ReconciliationEvent(
                outcome=ReconciliationOutcome.RUN,
                affected_count=int(affected),
            ),
            severity=TelemetrySeverity.INFO,
        )

    @staticmethod
    def _skip_reason(result: PipelineOutcomeLike) -> SkipReason | None:
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status == "paper":
            return SkipReason.PAPER_ONLY
        if status == "skipped":
            return skip_reason_for_code(getattr(result, "reason", None))
        return None

    @staticmethod
    def _correlation(signals_by_key: Mapping[str, SignalLike], trade_key: str) -> str | None:
        signal = signals_by_key.get(trade_key) if signals_by_key else None
        tx_hash = getattr(signal, "tx_hash", None)
        return safe_ref(str(tx_hash)) if tx_hash else None

