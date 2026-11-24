"""Fail-closed live-submission permission policy.

The canonical PostgreSQL singleton distinguishes an operator entry pause from
a full stop through its stable reason code. A normal pause blocks copy entries
while preserving risk-reducing protective exits; ``runtime.full_stop`` blocks
every venue submission.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol

from pmcopy.storage.postgres.trading_control_repo import TradingControlState


class SubmissionPurpose(StrEnum):
    """Low-cardinality submission purpose used by the safety policy."""

    STANDARD = "standard"
    PROTECTIVE_EXIT = "protective_exit"


class TradingControlReader(Protocol):
    """Read the canonical durable live-trading control state."""

    async def get(self) -> TradingControlState:
        """Return the current singleton state."""

        ...


class TradingPausedError(RuntimeError):
    """Raised when durable live-trading policy blocks a submission."""


class TradingPermissionService:
    """Enforce canonical live pause and full-stop semantics before every POST."""

    _FULL_STOP_REASON = "runtime.full_stop"

    def __init__(self, repo: TradingControlReader, *, live_mode: bool) -> None:
        """Store the durable reader and explicit live-mode gate.

        Args:
            repo: Canonical PostgreSQL trading-control reader.
            live_mode: Explicit typed runtime mode; credentials never imply it.
        """

        self._repo = repo
        self._live_mode = live_mode
        self._state: TradingControlState | None = None

    @property
    def state(self) -> TradingControlState | None:
        """Return the currently cached immutable control state."""

        return self._state

    async def refresh(self) -> TradingControlState | None:
        """Refresh durable control state outside the submission hot path.

        Returns:
            The cached durable state, or ``None`` in paper mode.

        Raises:
            TradingPausedError: If live control state cannot be loaded.
        """

        if not self._live_mode:
            return None
        try:
            state = await self._repo.get()
        except Exception as exc:
            self._state = None
            raise TradingPausedError(
                "trading control unavailable; live submission blocked"
            ) from exc
        self._state = state
        return state

    def replace(self, state: TradingControlState) -> None:
        """Replace cached state from a user-feed or reconciliation update.

        Args:
            state: Newly observed durable control state.
        """

        self._state = state

    async def require_submission_allowed(
        self,
        *,
        purpose: SubmissionPurpose = SubmissionPurpose.STANDARD,
    ) -> TradingControlState | None:
        """Return current state or raise when live submission is blocked.

        Args:
            purpose: Whether the order is ordinary flow or a protective exit.

        Returns:
            The live durable state, or ``None`` in explicit paper mode.

        Raises:
            TypeError: If ``purpose`` is not a supported enum value.
            TradingPausedError: If state is unavailable, full stop is active,
                or an ordinary order is attempted while entries are paused.
        """

        if not isinstance(purpose, SubmissionPurpose):
            raise TypeError("submission purpose must be a SubmissionPurpose")
        if not self._live_mode:
            return None
        state = self._state
        if state is None:
            raise TradingPausedError("trading control unavailable; live submission blocked")
        protective_pause_exception = (
            purpose is SubmissionPurpose.PROTECTIVE_EXIT and state.reason != self._FULL_STOP_REASON
        )
        if not state.enabled and not protective_pause_exception:
            raise TradingPausedError(
                f"live trading paused (generation={state.generation}, reason={state.reason})"
            )
        return state


__all__ = [
    "SubmissionPurpose",
    "TradingControlReader",
    "TradingPausedError",
    "TradingPermissionService",
]
