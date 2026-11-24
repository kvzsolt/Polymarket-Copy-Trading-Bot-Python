"""Risk, protective-exit, and WebSocket stream telemetry emitters."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from pmcopy.services.telemetry.contracts import (
    FailureComponent,
    FailureEvent,
    OrderOutcome,
    ProtectiveExitEvent,
    ProtectiveOutcome,
    RiskEvent,
    RiskOutcome,
    TelemetryProducer,
    TelemetrySeverity,
    WSS_STREAM_NAMES,
    WssEvent,
    WssOutcome,
)
from pmcopy.services.telemetry.emitter_base import (
    _LOGGER,
    TelemetryEmitter,
    bounded_code,
    safe_ref,
)

if TYPE_CHECKING:
    from pmcopy.services.telemetry.service import TelemetryService


class RiskTelemetryEmitter(TelemetryEmitter):
    """Project protective-exit and risk-decision transitions.

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
        super().__init__(service, TelemetryProducer.RISK, loop)

    def exit_triggered(self, condition_id: str, token_id: str, reason: str) -> bool:
        """Publish a triggered protective exit.

        ``reason`` keeps one vocabulary across the three exit methods. The
        frozen ``ProtectiveExitEvent`` contract carries no free-text field, so
        the reason is only serialized on the not-filled failure path.

        Args:
            condition_id: Market condition identity.
            token_id: Outcome token identity.
            reason: Internal exit reason code.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(
            lambda: self._protective(
                condition_id,
                token_id,
                ProtectiveOutcome.TRIGGERED,
                TelemetrySeverity.WARNING,
            )
        )

    def exit_filled(self, condition_id: str, token_id: str, reason: str) -> bool:
        """Publish a filled protective exit.

        Args:
            condition_id: Market condition identity.
            token_id: Outcome token identity.
            reason: Internal exit reason code, not serialized.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(
            lambda: self._protective(
                condition_id,
                token_id,
                ProtectiveOutcome.FILLED,
                TelemetrySeverity.NOTICE,
            )
        )

    def _protective(
        self,
        condition_id: str,
        token_id: str,
        outcome: ProtectiveOutcome,
        severity: TelemetrySeverity,
    ) -> bool:
        task_ref = _position_ref(condition_id, token_id)
        return self._publish(
            lambda: ProtectiveExitEvent(outcome=outcome, task_ref=task_ref),
            severity=severity,
        )

    def exit_not_filled(self, condition_id: str, token_id: str, reason: str) -> bool:
        """Publish a protective exit that did not fill and stays retryable.

        Args:
            condition_id: Market condition identity.
            token_id: Outcome token identity.
            reason: Internal exit reason code folded into the bounded code.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._exit_not_filled(condition_id, token_id, reason))

    def _exit_not_filled(self, condition_id: str, token_id: str, reason: str) -> bool:
        code = bounded_code(f"exit_not_filled_{reason}" if reason else "exit_not_filled")
        correlation = _position_ref(condition_id, token_id)
        return self._publish(
            lambda: FailureEvent(
                component=FailureComponent.PROTECTIVE_EXIT,
                code=code,
                retryable=True,
                order_outcome=OrderOutcome.NOT_FILLABLE,
            ),
            severity=TelemetrySeverity.ERROR,
            correlation_id=correlation,
        )

    def risk_rejected(self, task_ref: str) -> bool:
        """Publish one risk rejection.

        Args:
            task_ref: Task or trade identity; hashed when not a safe token.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(
            lambda: self._publish(
                lambda: RiskEvent(
                    outcome=RiskOutcome.REJECTED,
                    task_ref=safe_ref(task_ref) if task_ref else None,
                ),
                severity=TelemetrySeverity.NOTICE,
            )
        )


class WssTelemetryEmitter(TelemetryEmitter):
    """Project bounded WebSocket lifecycle transitions.

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
        super().__init__(service, TelemetryProducer.WSS, loop)

    def connected(self, stream: str) -> bool:
        """Publish a stream connection.

        Args:
            stream: One of ``leader``, ``market``, or ``user``.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._stream_event(stream, WssOutcome.CONNECTED, TelemetrySeverity.INFO)

    def reconnected(self, stream: str) -> bool:
        """Publish a stream reconnection.

        Args:
            stream: One of ``leader``, ``market``, or ``user``.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._stream_event(stream, WssOutcome.RECONNECTED, TelemetrySeverity.NOTICE)

    def disconnected(self, stream: str) -> bool:
        """Publish a stream disconnection.

        Args:
            stream: One of ``leader``, ``market``, or ``user``.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._stream_event(stream, WssOutcome.DISCONNECTED, TelemetrySeverity.WARNING)

    def stalled(self, stream: str) -> bool:
        """Publish a stalled stream heartbeat.

        Args:
            stream: One of ``leader``, ``market``, or ``user``.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._stream_event(stream, WssOutcome.STALLED, TelemetrySeverity.WARNING)

    def _stream_event(
        self,
        stream: str,
        outcome: WssOutcome,
        severity: TelemetrySeverity,
    ) -> bool:
        try:
            name = str(stream or "").strip().lower()
        except Exception:
            self._record_failure()
            return False
        if name not in WSS_STREAM_NAMES:
            # A caller typo is a wiring bug, not a telemetry backend fault, so it
            # must not land on telemetry_emit_failure_total and make the operator
            # chase a healthy hub. DEBUG keeps it free in production.
            _LOGGER.debug("telemetry_wss_stream_rejected | stream=%s", name)
            return False
        return self._publish(
            lambda: WssEvent(outcome=outcome, stream=name),
            severity=severity,
        )


def _position_ref(condition_id: str, token_id: str) -> str:
    """Return a stable opaque reference for one market position."""
    return safe_ref(f"{condition_id}:{token_id}")

