"""Durable settlement classification and guarded inventory pruning contracts."""

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from domain.position_lifecycle import PositionLifecycle
from storage.postgres.positions_repo import PositionRecord
from tests.test_positions_repo_set_position_size import VALID_TOKEN, _repo_with_current


@pytest.mark.asyncio
async def test_settlement_metadata_survives_same_size_snapshot_write():
    repo, _, captured = _repo_with_current(Decimal("10"))
    assert await repo.set_position_size(
        user_id="user",
        condition_id="condition",
        token_id=VALID_TOKEN,
        size_tokens=Decimal("10"),
        avg_entry_price=Decimal("0.5"),
        expected_size=Decimal("10"),
        lifecycle=PositionLifecycle.SETTLED,
        settlement_price=Decimal("0"),
    )
    record = captured["upserts"][0]
    assert record.lifecycle is PositionLifecycle.SETTLED
    assert record.settlement_price == Decimal("0")


@pytest.mark.asyncio
async def test_open_read_excludes_settled_but_held_read_retains_history():
    repo, conn, _ = _repo_with_current(None)
    rows = [
        dict(
            user_id="user",
            condition_id="condition",
            token_id=str(i),
            leader_id=None,
            size_tokens="10",
            avg_entry_price="0.5",
            lifecycle=state,
            settlement_price="0" if state == "settled" else None,
        )
        for i, state in enumerate(("unknown", "active", "settled"))
    ]
    conn.fetch = AsyncMock(return_value=rows)
    assert len(await repo.list_held_positions("user")) == 3
    assert [p.lifecycle for p in await repo.list_open_positions("user")] == [
        PositionLifecycle.UNKNOWN,
        PositionLifecycle.ACTIVE,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size,pending,deleted", [("10", False, True), ("11", False, False), ("10", True, False)]
)
async def test_pruning_checks_locked_size_and_unresolved_ownership(size, pending, deleted):
    repo, conn, captured = _repo_with_current(Decimal(size))
    conn.fetchval = AsyncMock(return_value=pending)
    result = await repo.delete_position_if_unchanged(
        "user",
        "condition",
        token_id=VALID_TOKEN,
        expected_size=Decimal("10"),
    )
    assert result is deleted
    assert bool(captured["deletes"]) is deleted
    assert any("pg_advisory_xact_lock" in sql for sql, _ in conn.executed)


@pytest.mark.asyncio
async def test_partial_sell_preserves_settled_claim_classification():
    repo, conn, captured = _repo_with_current(None)
    repo._get_position_in_conn = AsyncMock(
        return_value=PositionRecord(
            "user",
            "condition",
            VALID_TOKEN,
            None,
            Decimal("10"),
            Decimal("0.5"),
            lifecycle=PositionLifecycle.SETTLED,
            settlement_price=Decimal("1"),
        )
    )
    result = await repo._apply_fill_in_conn(
        conn,
        user_id="user",
        leader_id=None,
        condition_id="condition",
        token_id=VALID_TOKEN,
        side="SELL",
        fill_size=Decimal("3"),
        fill_price=Decimal("0.99"),
    )
    assert result.size_tokens == Decimal("7")
    assert result.lifecycle is PositionLifecycle.SETTLED
    assert result.settlement_price == Decimal("1")


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [None, Decimal("0"), Decimal("1")])
async def test_recovered_sell_absence_guard_rechecks_position_under_lock(size):
    repo, conn, captured = _repo_with_current(size)
    if size:
        with pytest.raises(ValueError, match="inventory"):
            await repo._assert_absent_position_in_conn(
                conn, user_id="user", condition_id="condition", token_id=VALID_TOKEN
            )
    else:
        await repo._assert_absent_position_in_conn(
            conn, user_id="user", condition_id="condition", token_id=VALID_TOKEN
        )
    assert captured["upserts"] == []
    assert any("pg_advisory_xact_lock" in sql for sql, _ in conn.executed)
