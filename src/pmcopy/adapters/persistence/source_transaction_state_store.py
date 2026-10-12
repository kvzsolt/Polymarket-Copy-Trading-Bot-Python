"""PostgreSQL persistence for source reservation and consumption snapshots."""

from __future__ import annotations

from typing import Protocol, cast

from pydantic import TypeAdapter, ValidationError

from pmcopy.domain.source_transactions import SourceTransactionSnapshot


class SourceTransactionStatePool(Protocol):
    """Small database surface needed by the snapshot store."""

    async def fetchval(self, query: str, *args: object) -> object | None:
        """Return the first value from one database query."""

        ...

    async def execute(self, query: str, *args: object) -> str:
        """Execute one mutation and return its status string."""

        ...


class SourceTransactionSnapshotCodec:
    """Encode validated immutable source state as portable JSON."""

    def __init__(self) -> None:
        """Build the strongly typed dataclass adapter once."""

        self._adapter: TypeAdapter[SourceTransactionSnapshot] = TypeAdapter(
            SourceTransactionSnapshot
        )

    def encode(self, snapshot: SourceTransactionSnapshot) -> str:
        """Return a JSON document for ``snapshot``.

        Args:
            snapshot: Immutable source transaction recovery image.

        Returns:
            UTF-8 JSON text preserving Decimal and timezone-aware datetime data.
        """

        encoded = cast(bytes, self._adapter.dump_json(snapshot))
        return encoded.decode("utf-8")

    def decode(self, payload: str) -> SourceTransactionSnapshot:
        """Return a validated immutable snapshot from JSON text.

        Args:
            payload: Persisted JSON snapshot text.

        Returns:
            Validated source transaction recovery image.

        Raises:
            ValueError: If persisted state is malformed or violates domain invariants.
        """

        try:
            return cast(SourceTransactionSnapshot, self._adapter.validate_json(payload))
        except ValidationError:
            raise ValueError("persisted source transaction state is invalid") from None


class PostgresSourceTransactionStateStore:
    """Atomically persist one complete source idempotency state image."""

    def __init__(
        self,
        pool: SourceTransactionStatePool,
        codec: SourceTransactionSnapshotCodec | None = None,
    ) -> None:
        """Initialize the PostgreSQL snapshot adapter.

        Args:
            pool: Application-owned async database pool.
            codec: Optional injected validated JSON codec.
        """

        self._pool = pool
        self._codec = codec or SourceTransactionSnapshotCodec()

    async def load(self) -> SourceTransactionSnapshot:
        """Return the durable snapshot or an empty fresh-state image."""

        payload = await self._pool.fetchval(
            """
            SELECT snapshot_json::text
              FROM source_transaction_state
             WHERE singleton_id = 1
            """
        )
        if payload is None:
            return SourceTransactionSnapshot.empty()
        if not isinstance(payload, str):
            raise ValueError("persisted source transaction state must be JSON text")
        return self._codec.decode(payload)

    async def save(self, snapshot: SourceTransactionSnapshot) -> None:
        """Atomically replace the durable source transaction snapshot.

        Args:
            snapshot: Complete reservation, correlation, and consumption image.
        """

        payload = self._codec.encode(snapshot)
        await self._pool.execute(
            """
            INSERT INTO source_transaction_state(singleton_id, snapshot_json, updated_at)
            VALUES (1, $1::jsonb, NOW())
            ON CONFLICT (singleton_id) DO UPDATE
            SET snapshot_json = EXCLUDED.snapshot_json,
                updated_at = EXCLUDED.updated_at
            """,
            payload,
        )


__all__ = [
    "PostgresSourceTransactionStateStore",
    "SourceTransactionSnapshotCodec",
]
