"""PostgreSQL adapter contracts for durable protective-risk lots."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from services.copy_tasks.lots import DurableRiskLot, ProtectiveLotExitFill, RiskLotStatus
from services.copy_tasks.protective_exits import (
    FillRiskLot,
    LayeredLevel,
    ProtectiveRiskPolicy,
)
from storage.postgres.risk_lot_repository import PostgresRiskLotRepository, RiskLotCodec


def _aggregate() -> DurableRiskLot:
    """Return one deterministic aggregate."""

    return DurableRiskLot(
        follower_account="deposit-a",
        leader_wallet="0x1111111111111111111111111111111111111111",
        entry_fill_id="entry-a",
        lot=FillRiskLot(
            lot_id="lot-a",
            task_id="task-a",
            market_id="market-a",
            outcome_id="token-a",
            entry_price=Decimal("0.40"),
            original_shares=Decimal("10"),
            remaining_shares=Decimal("10"),
            opened_at=datetime(2026, 8, 11, 12, tzinfo=UTC),
            peak_price=Decimal("0.55"),
            triggered_levels=("tp:0",),
        ),
        policy=ProtectiveRiskPolicy(
            stop_loss_percent=Decimal("10"),
            trailing_stop_percent=Decimal("5"),
            layered_take_profit=(LayeredLevel(Decimal("20"), Decimal("0.5")),),
        ),
        status=RiskLotStatus.OPEN,
        version=3,
    )


def _row(aggregate: DurableRiskLot | None = None) -> dict[str, object]:
    """Encode an aggregate as an asyncpg-like row."""

    item = aggregate or _aggregate()
    return {
        "lot_id": item.lot.lot_id,
        "entry_fill_id": item.entry_fill_id,
        "follower_account": item.follower_account,
        "task_id": item.lot.task_id,
        "leader_wallet": item.leader_wallet,
        "market_id": item.lot.market_id,
        "outcome_id": item.lot.outcome_id,
        "entry_price": item.lot.entry_price,
        "original_shares": item.lot.original_shares,
        "remaining_shares": item.lot.remaining_shares,
        "peak_price": item.lot.peak_price,
        "policy_snapshot": RiskLotCodec().encode_policy(item.policy),
        "triggered_levels": list(item.lot.triggered_levels),
        "status": item.status.value,
        "opened_at": item.lot.opened_at,
        "version": item.version,
    }


class _Transaction:
    """No-op deterministic transaction context."""

    async def __aenter__(self) -> None:
        """Enter the fake transaction."""

    async def __aexit__(self, *_args: object) -> None:
        """Exit the fake transaction."""


class _Connection:
    """Script asyncpg results and retain SQL evidence."""

    def __init__(self) -> None:
        self.fetchrow_results: list[Mapping[str, object] | None] = []
        self.fetch_results: list[list[Mapping[str, object]]] = []
        self.calls: list[tuple[str, str, tuple[object, ...]]] = []

    def transaction(self) -> _Transaction:
        """Return a fake transaction."""

        return _Transaction()

    async def fetchrow(self, sql: str, *args: object) -> Mapping[str, object] | None:
        """Return the next scripted row."""

        self.calls.append(("fetchrow", sql, args))
        return self.fetchrow_results.pop(0)

    async def fetch(self, sql: str, *args: object) -> list[Mapping[str, object]]:
        """Return the next scripted row collection."""

        self.calls.append(("fetch", sql, args))
        return self.fetch_results.pop(0)

    async def execute(self, sql: str, *args: object) -> str:
        """Record one statement."""

        self.calls.append(("execute", sql, args))
        return "OK"


class _Acquire:
    """Return one fake connection from an async pool context."""

    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _Connection:
        """Acquire the fake connection."""

        return self._connection

    async def __aexit__(self, *_args: object) -> None:
        """Release the fake connection."""


class _Pool:
    """Expose one deterministic connection."""

    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    def acquire(self) -> _Acquire:
        """Acquire the deterministic connection."""

        return _Acquire(self._connection)


def test_codec_round_trips_exact_policy_and_lot_state() -> None:
    """Preserve decimals, layered rules, peaks, and triggered levels."""

    codec = RiskLotCodec()

    assert codec.decode(_row()) == _aggregate()


@pytest.mark.asyncio
async def test_repository_creates_lists_and_monotonically_updates_lot() -> None:
    """Use atomic SQL and return immutable restart state."""

    connection = _Connection()
    raised = replace(
        _aggregate(),
        lot=replace(_aggregate().lot, peak_price=Decimal("0.60")),
        version=4,
    )
    connection.fetchrow_results = [{"created": 1}, _row(raised)]
    connection.fetch_results = [[_row()]]
    repository = PostgresRiskLotRepository(_Pool(connection))

    assert await repository.create_if_absent(_aggregate()) is True
    assert await repository.list_open("deposit-a") == (_aggregate(),)
    assert await repository.observe_peak("lot-a", Decimal("0.60")) == raised

    sql = "\n".join(call[1] for call in connection.calls)
    assert "INSERT INTO copy_task_risk_lots" in sql
    assert "status = 'open'" in sql
    assert "peak_price <" in sql


@pytest.mark.asyncio
async def test_repository_applies_exit_fill_and_returns_updated_lot() -> None:
    """Lock the lot and persist fill plus remaining shares in one transaction."""

    connection = _Connection()
    updated = replace(
        _aggregate(),
        lot=replace(
            _aggregate().lot,
            remaining_shares=Decimal("8"),
            triggered_levels=("tp:0", "tp:1"),
        ),
        version=4,
    )
    connection.fetchrow_results = [None, _row(), _row(updated)]
    repository = PostgresRiskLotRepository(_Pool(connection))
    fill = ProtectiveLotExitFill(
        fill_id="exit-a",
        lot_id="lot-a",
        shares=Decimal("2"),
        price=Decimal("0.50"),
        occurred_at=datetime(2026, 8, 11, 13, tzinfo=UTC),
        level_id="tp:1",
    )

    result = await repository.record_exit_fill(fill)

    assert result.created is True
    assert result.lot == updated
    sql = "\n".join(call[1] for call in connection.calls)
    assert "copy_task_protective_exit_fill_ledger" in sql
    assert "remaining_shares = remaining_shares -" in sql
