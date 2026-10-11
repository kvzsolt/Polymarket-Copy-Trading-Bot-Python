"""Follower fill evidence shares the authoritative accounting transaction."""

from collections.abc import Mapping
import json
from typing import TYPE_CHECKING, cast

import asyncpg

from integrations.pmrecon.transaction_outbox import (
    EvidenceTransactionConnection,
    PostgresEvidenceOutbox,
)
from interfaces.source_time import ReceiptClock
from services.evidence_fills import FollowerFillEvidenceBuilder

if TYPE_CHECKING:
    from storage.postgres.fills_repo import FillRecord


class PostgresFillEvidenceProjection:
    """Persist evidence locally without invoking publisher/network operations."""

    def __init__(
        self,
        *,
        clock: ReceiptClock,
        outbox: PostgresEvidenceOutbox | None = None,
        execution_mode: str = "unknown",
    ) -> None:
        """Use the application clock and an adapter that never owns commit."""
        self._clock = clock
        if execution_mode not in {"paper", "live", "unknown"}:
            raise ValueError("Follower evidence execution mode is unsupported")
        self._execution_mode = execution_mode
        self._outbox = outbox or PostgresEvidenceOutbox()
        self._builder = FollowerFillEvidenceBuilder()

    async def apply(
        self,
        connection: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
        fill_id: int,
        record: "FillRecord",
    ) -> None:
        """Capture a fresh row and resolve only explicitly persisted attribution."""
        receipt = self._clock.capture()
        source = record.source_action_id
        # A cumulative settlement key also identifies older recovery callers
        # which predate the accounted_shares watermark.
        recovered = record.accounted_shares is not None or record.dedup_key is not None
        execution_mode = "unknown" if recovered else self._execution_mode
        if record.order_id is not None:
            raw = await connection.fetchval(
                "SELECT request_json FROM orders WHERE user_id = $1 AND order_id = $2",
                record.user_id,
                record.order_id,
            )
            request = json.loads(raw) if isinstance(raw, str) else raw
            recovery = request.get("recovery_metadata") if isinstance(request, Mapping) else None
            linked = recovery.get("source_action_id") if isinstance(recovery, Mapping) else None
            original_mode = (
                recovery.get("execution_mode") if isinstance(recovery, Mapping) else None
            )
            if original_mode is not None:
                if original_mode not in {"paper", "live", "unknown"}:
                    raise ValueError("Follower order execution mode is unsupported")
                if (
                    not recovered
                    and execution_mode != "unknown"
                    and original_mode != "unknown"
                    and original_mode != execution_mode
                ):
                    raise ValueError("Follower fill execution mode conflicts with its order")
                execution_mode = original_mode
            if linked is not None:
                if source is not None and source != linked:
                    raise ValueError("Follower fill attribution conflicts with its order")
                if not isinstance(linked, str):
                    raise ValueError("Follower fill attribution must be explicit text")
                source = linked
        event = self._builder.build(
            fill_id,
            record,
            receipt,
            source_action_id=source,
            recovery=recovered,
            execution_mode=execution_mode,
        )
        # asyncpg's pool proxy forwards these methods dynamically; its stubs do
        # not expose them. Real PostgreSQL tests verify the transaction contract.
        await self._outbox.append(cast(EvidenceTransactionConnection, connection), event)
