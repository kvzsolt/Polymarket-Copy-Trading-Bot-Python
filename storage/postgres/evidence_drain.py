"""Move atomic fill evidence into the local replayable publisher before marking it."""

import json
from typing import Protocol

import asyncpg

from domain.evidence import EvidenceError, EvidenceEvent
from integrations.pmrecon.codec import PmreconEvidenceCodec


class DurableEvidenceSink(Protocol):
    """Local durable append only; remote delivery has a separate lifetime."""

    def append_event(self, value: EvidenceEvent) -> int:
        """Return a cursor only after identical evidence is durably retained."""
        ...


class PostgresEvidenceDrain:
    """Replay bounded rows without changing their IDs, digests, or receipt clocks."""

    def __init__(
        self, pool: asyncpg.Pool, sink: DurableEvidenceSink, *, batch_size: int = 100
    ) -> None:
        """Inject the application pool and its independent local publisher spool."""
        if type(batch_size) is not int or not 1 <= batch_size <= 100:
            raise ValueError("Evidence drain batch size must be between 1 and 100")
        self._pool = pool
        self._sink = sink
        self._batch_size = batch_size

    async def drain_once(self) -> int:
        """Durably append each row before marking; crashes safely replay exact bytes.

        Row locks serialize concurrent local drains. A failed append rolls back
        every mark in the batch; already appended SQLite rows remain idempotent.
        No Hungary network operation is called from this path.
        """
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    "SELECT seq,event_id,digest,payload FROM evidence_transaction_outbox "
                    "WHERE drained_at IS NULL ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED",
                    self._batch_size,
                )
                for row in rows:
                    payload = row["payload"]
                    event = PmreconEvidenceCodec.encode(
                        json.loads(payload) if isinstance(payload, str) else payload
                    )
                    if event.event_id != row["event_id"] or event.digest != row["digest"]:
                        raise EvidenceError("evidence_identity_conflict")
                    sequence = self._sink.append_event(event)
                    if type(sequence) is not int or sequence <= 0:
                        raise EvidenceError("evidence_durable_append_required")
                    await connection.execute(
                        "UPDATE evidence_transaction_outbox SET drained_at=now() "
                        "WHERE seq=$1 AND drained_at IS NULL",
                        row["seq"],
                    )
                return len(rows)
