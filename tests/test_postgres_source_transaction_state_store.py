"""Tests for durable PostgreSQL source-transaction snapshots."""

from __future__ import annotations

import pytest

from adapters.persistence.source_transaction_state_store import (
    PostgresSourceTransactionStateStore,
    SourceTransactionSnapshotCodec,
)
from domain.source_transactions import (
    CopyConsumptionKey,
    SourceTransactionRecordSnapshot,
    SourceTransactionSnapshot,
    TransactionState,
)
from storage.postgres import migrations
from tests.test_source_transaction_registry_persistence import (
    _confirmed,
    _correlation,
    _pending,
)


class FakeDatabasePool:
    """Capture one singleton JSON snapshot row."""

    def __init__(self) -> None:
        self.payload: str | None = None
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, query: str, *args: object) -> object | None:
        """Return the configured singleton payload."""

        del query, args
        return self.payload

    async def execute(self, query: str, *args: object) -> str:
        """Capture an upsert and retain its JSON argument."""

        self.execute_calls.append((query, args))
        if args:
            self.payload = str(args[0])
        return "INSERT 0 1"


def _snapshot() -> SourceTransactionSnapshot:
    pending = _pending()
    reservation = _confirmed_reservation = None
    from domain.source_transactions import ReservedTransaction

    reservation = ReservedTransaction(
        reservation_id=f"{pending.chain_id}:{pending.transaction_hash}",
        transaction=pending,
        reserved_at=pending.observed_at,
    )
    confirmed = _confirmed(reservation)
    correlation = _correlation()
    key = CopyConsumptionKey(
        follower_account=correlation.follower_account,
        copy_task_id=correlation.copy_task_id,
        source_fill=confirmed.fills[0].identity,
    )
    del _confirmed_reservation
    return SourceTransactionSnapshot(
        records=(
            SourceTransactionRecordSnapshot(
                reservation=reservation,
                state=TransactionState.CONSUMED,
                correlations=(correlation,),
                confirmed=confirmed,
                terminal_reason=None,
            ),
        ),
        waiting_correlations=(),
        claimed=(key,),
    )


def test_snapshot_codec_round_trips_exact_domain_values() -> None:
    """Decimals, datetimes, enums, and composite identities round-trip exactly."""

    codec = SourceTransactionSnapshotCodec()
    snapshot = _snapshot()

    assert codec.decode(codec.encode(snapshot)) == snapshot


@pytest.mark.asyncio
async def test_postgres_store_atomically_replaces_and_loads_singleton_snapshot() -> None:
    """The adapter persists one complete idempotency image per save."""

    pool = FakeDatabasePool()
    store = PostgresSourceTransactionStateStore(pool=pool)
    snapshot = _snapshot()

    await store.save(snapshot)
    restored = await store.load()

    assert restored == snapshot
    assert len(pool.execute_calls) == 1
    query, args = pool.execute_calls[0]
    assert "ON CONFLICT (singleton_id) DO UPDATE" in query
    assert len(args) == 1


@pytest.mark.asyncio
async def test_postgres_store_returns_empty_snapshot_when_row_is_absent() -> None:
    """A fresh database starts with empty fail-closed source state."""

    store = PostgresSourceTransactionStateStore(pool=FakeDatabasePool())

    assert await store.load() == SourceTransactionSnapshot.empty()


def test_core_migration_creates_source_transaction_state_table() -> None:
    """Startup migrations provision the durable singleton state row."""

    ddl = "\n".join(str(statement) for statement in migrations._CORE_DDL)

    assert "CREATE TABLE IF NOT EXISTS source_transaction_state" in ddl
