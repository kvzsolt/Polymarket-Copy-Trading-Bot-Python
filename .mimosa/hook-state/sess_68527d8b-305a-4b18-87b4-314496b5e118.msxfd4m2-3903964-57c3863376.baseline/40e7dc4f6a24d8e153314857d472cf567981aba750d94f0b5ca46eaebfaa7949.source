"""PostgreSQL adapter for the canonical operator trading-control row.

The control-plane contract has richer names (entry pause and full stop), while
the execution authority intentionally remains the existing ``trading_control``
singleton.  This adapter translates between those contracts without creating a
second durable source of truth.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Protocol

from services.control_plane.contracts import (
    ContractValidation,
    ControlPlaneError,
    ControlPlaneReason,
    EntryControlState,
)


class DatabasePool(Protocol):
    """Small row-query surface needed by the entry-control adapter."""

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Return one optional row."""

        ...


class PostgresEntryControlRepository:
    """Project and update the canonical singleton through optimistic CAS."""

    def __init__(self, pool: DatabasePool) -> None:
        """Store the injected database pool."""

        self._pool = pool

    async def get(self) -> EntryControlState:
        """Return the durable global entry-control row."""

        try:
            row = await self._pool.fetchrow(
                """
                SELECT enabled, generation, reason, updated_at, updated_by
                FROM trading_control
                WHERE singleton_id = 1
                """
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.RUNTIME_UNAVAILABLE) from None
        if row is None:
            raise ControlPlaneError(ControlPlaneReason.RUNTIME_UNAVAILABLE)
        return self._state(row)

    async def compare_and_set(
        self,
        *,
        expected_revision: int,
        entries_paused: bool,
        full_stop: bool,
        actor: str,
        reason_code: str,
    ) -> EntryControlState | None:
        """Update a matching revision and return ``None`` when it is stale."""

        ContractValidation.require_revision(expected_revision)
        ContractValidation.require_text(actor)
        ContractValidation.require_text(reason_code)
        if full_stop and not entries_paused:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        stored_reason = "runtime.full_stop" if full_stop else reason_code
        try:
            row = await self._pool.fetchrow(
                """
                UPDATE trading_control
                SET enabled = $2,
                    generation = generation + 1,
                    reason = $3,
                    updated_at = NOW(),
                    updated_by = $4
                WHERE singleton_id = 1
                  AND generation = $1
                RETURNING enabled, generation, reason, updated_at, updated_by
                """,
                expected_revision,
                not entries_paused,
                stored_reason,
                actor,
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.RUNTIME_UNAVAILABLE) from None
        return None if row is None else self._state(row)

    @staticmethod
    def _state(row: Mapping[str, object]) -> EntryControlState:
        """Translate one validated database row."""

        updated_at = row.get("updated_at")
        if not isinstance(updated_at, datetime) or updated_at.tzinfo is None:
            raise ControlPlaneError(ControlPlaneReason.RUNTIME_UNAVAILABLE)
        revision = row.get("generation")
        if isinstance(revision, bool) or not isinstance(revision, (int, str)):
            raise ControlPlaneError(ControlPlaneReason.RUNTIME_UNAVAILABLE)
        try:
            reason = str(row["reason"])
            return EntryControlState(
                entries_paused=not bool(row["enabled"]),
                full_stop=reason == "runtime.full_stop",
                revision=int(revision),
                reason_code=reason,
                updated_at=updated_at,
                updated_by=str(row["updated_by"]),
            )
        except (KeyError, TypeError, ValueError, ControlPlaneError):
            raise ControlPlaneError(ControlPlaneReason.RUNTIME_UNAVAILABLE) from None


__all__ = ["PostgresEntryControlRepository"]
