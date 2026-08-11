"""Insert-only PostgreSQL adapter for control-plane operator audit."""

from __future__ import annotations

import json
from typing import Protocol

from services.control_plane.contracts import (
    AuditEvent,
    ControlPlaneError,
    ControlPlaneReason,
)


class DatabasePool(Protocol):
    """Small scalar-query surface needed by the audit adapter."""

    async def fetchval(self, query: str, *args: object) -> object | None:
        """Return the first value from an optional row."""

        ...


class PostgresControlPlaneAuditRepository:
    """Append immutable audit events idempotently by event identity."""

    def __init__(self, pool: DatabasePool) -> None:
        """Store the injected database pool."""

        self._pool = pool

    async def append(self, event: AuditEvent) -> None:
        """Insert one event; an identical event identity is already durable."""

        details = json.dumps(dict(event.details), sort_keys=True, separators=(",", ":"))
        try:
            await self._pool.fetchval(
                """
                INSERT INTO control_plane_audit_log (
                    event_id, occurred_at, actor, action, target_type,
                    target_id, follower_account, outcome, reason_code,
                    details_json
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb
                )
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                event.event_id,
                event.occurred_at,
                event.actor,
                event.action,
                event.target_type,
                event.target_id,
                event.follower_account,
                event.outcome,
                event.reason_code,
                details,
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.AUDIT_UNAVAILABLE) from None


__all__ = ["PostgresControlPlaneAuditRepository"]
