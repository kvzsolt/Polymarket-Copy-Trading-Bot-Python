"""Operational control-plane command services and narrow ports."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from services.control_plane.contracts import (
    AuditEvent,
    ControlPlaneError,
    ControlPlaneReason,
    DiscoveryRunCommand,
    DiscoveryRunReceipt,
    EntryControlState,
    LockResetCommand,
    LockResetOutcome,
    LockResetResult,
    RuntimeControlCommand,
)


class Clock(Protocol):
    """UTC clock seam for deterministic audit events."""

    def now(self) -> datetime:
        """Return the current timezone-aware time."""

        ...


class AuditSink(Protocol):
    """Append-only audit seam."""

    async def append(self, event: AuditEvent) -> None:
        """Append one event."""

        ...


class DiscoveryCommandPort(Protocol):
    """Asynchronous discovery job queue boundary."""

    async def enqueue(self, command: DiscoveryRunCommand) -> DiscoveryRunReceipt:
        """Enqueue work and return immediately with a receipt."""

        ...


class EntryControlPort(Protocol):
    """Durable compare-and-set boundary for entry safety state."""

    async def get(self) -> EntryControlState:
        """Return current entry control state."""

        ...

    async def compare_and_set(
        self,
        *,
        expected_revision: int,
        entries_paused: bool,
        full_stop: bool,
        actor: str,
        reason_code: str,
    ) -> EntryControlState | None:
        """Apply a matching revision or return ``None``."""

        ...


class RuntimeSignalPort(Protocol):
    """Idempotent runtime application boundary for durable control state."""

    async def apply(self, state: EntryControlState) -> None:
        """Apply one durable state to running workers."""

        ...


class ReentryLockResetPort(Protocol):
    """CAS boundary for manual protective-lock resets."""

    async def reset_if_current(self, command: LockResetCommand) -> LockResetResult:
        """Reset only the requested current lock revision."""

        ...


class DiscoveryCommandService:
    """Accept discovery commands without invoking provider I/O inline."""

    def __init__(self, *, port: DiscoveryCommandPort, audit_sink: AuditSink, clock: Clock) -> None:
        """Store the queue and audit collaborators."""

        self._port = port
        self._audit_sink = audit_sink
        self._clock = clock

    async def request(self, command: DiscoveryRunCommand) -> DiscoveryRunReceipt:
        """Queue one discovery run and return its immediate receipt."""

        try:
            receipt = await self._port.enqueue(command)
        except ControlPlaneError:
            raise
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.DISCOVERY_UNAVAILABLE) from None
        if not receipt.accepted:
            raise ControlPlaneError(ControlPlaneReason.DISCOVERY_BUSY)
        try:
            await self._audit_sink.append(
                AuditEvent.record(
                    actor=command.actor,
                    action="discovery.request",
                    target_type="discovery_run",
                    target_id=receipt.run_id,
                    outcome="accepted",
                    reason_code=receipt.reason_code,
                    occurred_at=self._clock.now(),
                )
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.AUDIT_UNAVAILABLE) from None
        return receipt


class RuntimeCommandService:
    """Apply durable entry pause and irreversible-process full stop commands."""

    def __init__(
        self,
        *,
        entry_control: EntryControlPort,
        runtime_signal: RuntimeSignalPort,
        audit_sink: AuditSink,
        clock: Clock,
    ) -> None:
        """Store durable and live runtime collaborators."""

        self._entry_control = entry_control
        self._runtime_signal = runtime_signal
        self._audit_sink = audit_sink
        self._clock = clock

    async def pause_entries(self, command: RuntimeControlCommand) -> EntryControlState:
        """Pause only new entries, leaving risk-reducing paths available."""

        await self._reject_if_full_stop(command, action="runtime.pause_entries")
        return await self._apply(
            command,
            entries_paused=True,
            full_stop=False,
            action="runtime.pause_entries",
        )

    async def resume_entries(self, command: RuntimeControlCommand) -> EntryControlState:
        """Resume entries unless a process full stop remains active."""

        await self._reject_if_full_stop(command, action="runtime.resume_entries")
        return await self._apply(
            command,
            entries_paused=False,
            full_stop=False,
            action="runtime.resume_entries",
        )

    async def _reject_if_full_stop(
        self,
        command: RuntimeControlCommand,
        *,
        action: str,
    ) -> None:
        """Keep entry-only commands from clearing an active full stop."""

        try:
            current = await self._entry_control.get()
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.RUNTIME_UNAVAILABLE) from None
        if current.revision != command.expected_revision:
            return
        if current.full_stop:
            await self._audit(
                command,
                action=action,
                outcome="rejected",
                reason=ControlPlaneReason.FULL_STOP_ACTIVE.value,
            )
            raise ControlPlaneError(ControlPlaneReason.FULL_STOP_ACTIVE)

    async def full_stop(self, command: RuntimeControlCommand) -> EntryControlState:
        """Request a fail-closed full stop, which always pauses entries."""

        return await self._apply(
            command,
            entries_paused=True,
            full_stop=True,
            action="runtime.full_stop",
        )

    async def _apply(
        self,
        command: RuntimeControlCommand,
        *,
        entries_paused: bool,
        full_stop: bool,
        action: str,
    ) -> EntryControlState:
        """Persist through CAS before signaling the idempotent runtime port."""

        try:
            state = await self._entry_control.compare_and_set(
                expected_revision=command.expected_revision,
                entries_paused=entries_paused,
                full_stop=full_stop,
                actor=command.actor,
                reason_code=command.reason_code,
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.RUNTIME_UNAVAILABLE) from None
        if state is None:
            await self._audit(
                command,
                action=action,
                outcome="rejected",
                reason=ControlPlaneReason.REVISION_MISMATCH.value,
            )
            raise ControlPlaneError(ControlPlaneReason.REVISION_MISMATCH)
        try:
            await self._runtime_signal.apply(state)
        except Exception:
            await self._audit(
                command,
                action=action,
                outcome="failed",
                reason=ControlPlaneReason.RUNTIME_UNAVAILABLE.value,
            )
            raise ControlPlaneError(ControlPlaneReason.RUNTIME_UNAVAILABLE) from None
        await self._audit(command, action=action, outcome="succeeded", reason="ok")
        return state

    async def _audit(
        self,
        command: RuntimeControlCommand,
        *,
        action: str,
        outcome: str,
        reason: str,
    ) -> None:
        """Append one runtime control audit event."""

        try:
            await self._audit_sink.append(
                AuditEvent.record(
                    actor=command.actor,
                    action=action,
                    target_type="entry_control",
                    target_id="global",
                    outcome=outcome,
                    reason_code=reason,
                    occurred_at=self._clock.now(),
                )
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.AUDIT_UNAVAILABLE) from None


class ReentryLockCommandService:
    """Expose safe manual lock reset through one CAS operation."""

    def __init__(self, *, port: ReentryLockResetPort, audit_sink: AuditSink, clock: Clock) -> None:
        """Store the reset and audit collaborators."""

        self._port = port
        self._audit_sink = audit_sink
        self._clock = clock

    async def reset(self, command: LockResetCommand) -> LockResetResult:
        """Reset one current lock or return a stable stale/not-found error."""

        try:
            result = await self._port.reset_if_current(command)
        except ControlPlaneError:
            raise
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        reasons = {
            LockResetOutcome.NOT_FOUND: ControlPlaneReason.NOT_FOUND,
            LockResetOutcome.REVISION_MISMATCH: ControlPlaneReason.REVISION_MISMATCH,
        }
        reason = reasons.get(result.outcome)
        if reason is not None:
            await self._append(command, outcome="rejected", reason_code=reason.value)
            raise ControlPlaneError(reason)
        await self._append(command, outcome="succeeded", reason_code="ok")
        return result

    async def _append(
        self,
        command: LockResetCommand,
        *,
        outcome: str,
        reason_code: str,
    ) -> None:
        """Append one sanitized manual-reset audit record."""

        try:
            await self._audit_sink.append(
                AuditEvent.record(
                    actor=command.actor,
                    action="reentry_lock.reset",
                    target_type="reentry_lock",
                    target_id=command.market_id,
                    outcome=outcome,
                    reason_code=reason_code,
                    occurred_at=self._clock.now(),
                    follower_account=command.follower_account,
                )
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.AUDIT_UNAVAILABLE) from None


__all__ = [
    "DiscoveryCommandPort",
    "DiscoveryCommandService",
    "EntryControlPort",
    "ReentryLockCommandService",
    "ReentryLockResetPort",
    "RuntimeCommandService",
    "RuntimeSignalPort",
]
