"""PostgreSQL CAS adapter for manual protective re-entry lock reset."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Protocol

from services.control_plane.contracts import (
    ControlPlaneError,
    ControlPlaneReason,
    LockResetCommand,
    LockResetOutcome,
    LockResetResult,
)


class DatabasePool(Protocol):
    """Small row-query surface needed by the reset adapter."""

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Return one optional row."""

        ...


class Clock(Protocol):
    """Clock used to timestamp a successful reset result."""

    def now(self) -> datetime:
        """Return the current timezone-aware time."""

        ...


class PostgresControlPlaneReentryLockRepository:
    """Delete exactly one current lock revision in a single SQL statement."""

    def __init__(self, pool: DatabasePool, *, clock: Clock) -> None:
        """Store database and clock collaborators."""

        self._pool = pool
        self._clock = clock

    async def reset_if_current(self, command: LockResetCommand) -> LockResetResult:
        """Classify reset, missing, and stale revision atomically."""

        try:
            row = await self._pool.fetchrow(
                """
                WITH current_lock AS (
                    SELECT revision
                    FROM market_reentry_locks
                    WHERE follower_account = $1
                      AND task_id = $2
                      AND leader_wallet = $3
                      AND market_id = $4
                ), deleted AS (
                    DELETE FROM market_reentry_locks
                    WHERE follower_account = $1
                      AND task_id = $2
                      AND leader_wallet = $3
                      AND market_id = $4
                      AND revision = $5
                    RETURNING 1
                )
                SELECT CASE
                    WHEN EXISTS (SELECT 1 FROM deleted) THEN 'reset'
                    WHEN EXISTS (SELECT 1 FROM current_lock) THEN 'revision_mismatch'
                    ELSE 'not_found'
                END AS outcome
                """,
                command.follower_account,
                command.task_id,
                command.leader_wallet.casefold(),
                command.market_id,
                command.expected_revision,
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        if row is None:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        try:
            outcome = LockResetOutcome(str(row["outcome"]))
        except (KeyError, ValueError):
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        return LockResetResult(
            outcome=outcome,
            released_at=self._clock.now() if outcome is LockResetOutcome.RESET else None,
        )


__all__ = ["PostgresControlPlaneReentryLockRepository"]
