"""Issue #9 tests — market structure split/merge/resolution foundation."""

import contextlib
import time
from decimal import Decimal
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from storage.postgres.market_structure_repo import (
    MarketStructureEvent,
    MarketStructureRepo,
)


def _token(n: int) -> str:
    return str(10**75 + n)


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply

        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


class TestMarketStructureRepo:
    """PG-A5: legacy sqlite TestMarketStructureRepo dropped, replaced
    with async PG-backed equivalents. Each test owns its own
    testcontainer pool via the inline ``_pg_pool`` ctx mgr below."""

    @pytest.mark.asyncio
    async def test_record_and_get_split_event(self) -> None:
        async with _pg_pool() as pool:
            repo = MarketStructureRepo(pool)
            eid = await repo.record_event(
                MarketStructureEvent(
                    event_type="split",
                    condition_id="cond-1",
                    old_token_id=_token(1),
                    new_token_id=_token(2),
                    ratio="2:1",
                    timestamp=time.time(),
                    payload_json=repo.payload({"kind": "split"}),
                )
            )
            assert eid > 0

            rows = await repo.get_events_for_condition("cond-1")
        assert len(rows) == 1
        assert rows[0]["event_type"] == "split"

    @pytest.mark.asyncio
    async def test_record_merge_and_resolution(self) -> None:
        async with _pg_pool() as pool:
            repo = MarketStructureRepo(pool)
            await repo.record_event(
                MarketStructureEvent(
                    event_type="merge",
                    condition_id="cond-2",
                    old_token_id=f"{_token(1)},{_token(2)}",
                    new_token_id=_token(3),
                    ratio=None,
                    timestamp=time.time(),
                    payload_json=repo.payload({"kind": "merge"}),
                )
            )
            await repo.record_event(
                MarketStructureEvent(
                    event_type="resolution",
                    condition_id="cond-2",
                    old_token_id=None,
                    new_token_id=None,
                    ratio=None,
                    timestamp=time.time(),
                    payload_json=repo.payload(
                        {"kind": "resolution", "winning_outcome": 1}
                    ),
                )
            )

            rows = await repo.get_events_for_condition("cond-2")
        assert len(rows) == 2
        assert {r["event_type"] for r in rows} == {"merge", "resolution"}

    @pytest.mark.asyncio
    async def test_pending_remappings_and_mark(self) -> None:
        async with _pg_pool() as pool:
            repo = MarketStructureRepo(pool)
            eid = await repo.record_event(
                MarketStructureEvent(
                    event_type="split",
                    condition_id="cond-3",
                    old_token_id=_token(1),
                    new_token_id=_token(2),
                    ratio="2:1",
                    timestamp=time.time(),
                    payload_json=repo.payload({"kind": "split"}),
                )
            )
            pending = await repo.get_pending_remappings()
            assert len(pending) == 1
            assert pending[0]["id"] == eid

            assert await repo.mark_remapped(eid) is True
            assert await repo.get_pending_remappings() == []


class TestMarketStructureService:
    """PG-A5 cutover: MarketStructureService now requires an asyncpg pool.

    These tests spin a postgres:16-alpine testcontainer per test method
    (cheap — the container starts once per pytest session for the rest
    of PG-A6). Auto-skip when Docker is unavailable so the suite stays
    runnable without Docker.
    """

    @pytest.mark.asyncio
    async def test_service_records_events(self) -> None:
        async with _pg_pool() as pool:
            from services.marketStructureService import MarketStructureService
            svc = MarketStructureService(pool)

            split_id = await svc.handle_split(
                condition_id="cond-a",
                old_token_id=_token(10),
                new_token_id=_token(20),
                ratio="2:1",
            )
            merge_id = await svc.handle_merge(
                condition_id="cond-a",
                source_token_ids=[_token(20), _token(21)],
                target_token_id=_token(30),
            )
            res_id = await svc.handle_resolution(
                condition_id="cond-a", winning_outcome=1
            )
            assert split_id > 0 and merge_id > 0 and res_id > 0

    @pytest.mark.asyncio
    async def test_reconcile_positions_applies_remap(self) -> None:
        async with _pg_pool() as pool:
            from services.marketStructureService import MarketStructureService
            from storage.postgres.positions_repo import PositionsRepo
            positions = PositionsRepo(pool)
            svc = MarketStructureService(pool)

            old_token = _token(100)
            new_token = _token(200)
            await positions.apply_fill(
                user_id="u1",
                leader_id="l1",
                condition_id="cond-x",
                token_id=old_token,
                side="BUY",
                fill_size=Decimal("50"),
                fill_price=Decimal("0.40"),
            )
            await svc.handle_split(
                condition_id="cond-x",
                old_token_id=old_token,
                new_token_id=new_token,
                ratio="2:1",
            )
            updated = await svc.reconcile_positions("u1")
            assert updated == 1
            pos = await positions.get_position("u1", "cond-x")
            assert pos is not None
            assert pos.token_id == new_token

    @pytest.mark.asyncio
    async def test_per_user_remap_independent(self) -> None:
        async with _pg_pool() as pool:
            from services.marketStructureService import MarketStructureService
            from storage.postgres.positions_repo import PositionsRepo
            positions = PositionsRepo(pool)
            svc = MarketStructureService(pool)

            old_token = _token(300)
            new_token = _token(400)
            await positions.apply_fill(
                user_id="u1", leader_id="l1", condition_id="cond-multi",
                token_id=old_token, side="BUY",
                fill_size=Decimal("30"), fill_price=Decimal("0.50"),
            )
            await positions.apply_fill(
                user_id="u2", leader_id="l1", condition_id="cond-multi",
                token_id=old_token, side="BUY",
                fill_size=Decimal("20"), fill_price=Decimal("0.50"),
            )
            await svc.handle_split(
                condition_id="cond-multi",
                old_token_id=old_token,
                new_token_id=new_token,
                ratio="1:1",
            )

            assert await svc.reconcile_positions("u1") == 1
            pos_u1 = await positions.get_position("u1", "cond-multi")
            assert pos_u1 is not None
            assert pos_u1.token_id == new_token

            pos_u2_before = await positions.get_position("u2", "cond-multi")
            assert pos_u2_before is not None
            assert pos_u2_before.token_id == old_token
            assert await svc.reconcile_positions("u2") == 1
            pos_u2_after = await positions.get_position("u2", "cond-multi")
            assert pos_u2_after is not None
            assert pos_u2_after.token_id == new_token
