"""Follower-evidence append seam joining the caller's PostgreSQL fill transaction.

The application migration owns evidence_transaction_outbox. This adapter never
opens a connection or transaction, commits, performs HTTP, or constructs fills.
"""

import json
from collections.abc import Mapping
from typing import Protocol

from domain.evidence import EvidenceError, EvidenceEvent
from integrations.pmrecon.codec import PmreconEvidenceCodec


class EvidenceTransactionConnection(Protocol):
    """Minimal asyncpg-compatible transaction owner supplied by the fill ledger."""

    def is_in_transaction(self) -> bool:
        """Report whether atomicity with the caller's ledger is currently possible."""
        ...

    async def fetchrow(self, query: str, *args: object) -> Mapping[str, object] | None:
        """Execute one parameterized append returning its immutable identity digest."""
        ...


class PostgresEvidenceOutbox:
    """Append only within an existing fill transaction, leaving commit to its owner."""

    async def append(self, connection: EvidenceTransactionConnection, event: EvidenceEvent) -> int:
        """Return stable local sequence; a conflict raises so the owner rolls back."""
        if not connection.is_in_transaction():
            raise EvidenceError("evidence_transaction_required")
        if PmreconEvidenceCodec.encode(event.wire()) != event:
            raise EvidenceError("evidence_invalid_payload")
        row = await connection.fetchrow(
            "INSERT INTO evidence_transaction_outbox(event_id,digest,payload) VALUES($1,$2,$3::jsonb) "
            "ON CONFLICT(event_id) DO UPDATE SET event_id=EXCLUDED.event_id RETURNING seq,digest",
            event.event_id,
            event.digest,
            json.dumps(event.wire(), sort_keys=True, separators=(",", ":")),
        )
        if row is None or row["digest"] != event.digest:
            raise EvidenceError("evidence_identity_conflict")
        sequence = row["seq"]
        if type(sequence) is not int or sequence <= 0:
            raise EvidenceError("evidence_invalid_sequence")
        return sequence
