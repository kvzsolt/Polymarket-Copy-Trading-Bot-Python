"""Storage adapter tests — every concrete PG repo implements its
domain port AND round-trips its primary CRUD path through a live
``postgres:16-alpine`` testcontainer (PG-A5 sweep).

The goals of this module are:

* Prove each concrete repository is registered as a subclass of its ABC.
* Round-trip the primary CRUD / upsert paths through a real PG pool
  so the ABC surface remains honest (no stale method renames, no
  unintended signature drift).
* Isolate every test with its own testcontainer — no shared state.
"""

from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

from domain.ports import (
    AbstractCursorRepository,
    AbstractDiscoveryRepository,
    AbstractFillRepository,
    AbstractLeaderRepository,
    AbstractOrderRepository,
    AbstractPositionRepository,
)
from services.discovery.models import Candidate
from storage.postgres.cursors_repo import CursorsRepo, TradeCursor
from storage.postgres.discovery_repo import DiscoveryRepo
from storage.postgres.fills_repo import FillRecord, FillsRepo
from storage.postgres.leaders_repo import LeaderRosterRecord, LeadersRepo
from storage.postgres.orders_repo import OrdersRepo, OrderSubmissionRecord
from storage.postgres.positions_repo import PositionRecord, PositionsRepo


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
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


# ---------------------------------------------------------------------------
# ABC registration — every concrete repo must be a subclass of its port
# ---------------------------------------------------------------------------


class TestAbcRegistration:
    """Subclass checks need no pool. Sync tests."""

    def test_orders_repo(self) -> None:
        assert issubclass(OrdersRepo, AbstractOrderRepository)

    def test_positions_repo(self) -> None:
        assert issubclass(PositionsRepo, AbstractPositionRepository)

    def test_fills_repo(self) -> None:
        assert issubclass(FillsRepo, AbstractFillRepository)

    def test_leaders_repo(self) -> None:
        assert issubclass(LeadersRepo, AbstractLeaderRepository)

    def test_cursors_repo(self) -> None:
        assert issubclass(CursorsRepo, AbstractCursorRepository)

    def test_discovery_repo(self) -> None:
        assert issubclass(DiscoveryRepo, AbstractDiscoveryRepository)


# ---------------------------------------------------------------------------
# Smoke-level CRUD round-trips — confirm the ABC-exposed methods work
# ---------------------------------------------------------------------------


class TestOrdersRepo:
    @pytest.mark.asyncio
    async def test_acquire_then_is_locked(self) -> None:
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            assert await repo.try_acquire_submission_lock(
                "k1", "u1", "l1", {"a": 1}
            ) is True
            assert await repo.is_submission_locked("k1") is True
            # Re-acquiring must fail deterministically.
            assert await repo.try_acquire_submission_lock(
                "k1", "u1", "l1", {"a": 1}
            ) is False

    @pytest.mark.asyncio
    async def test_save_submission_readback(self) -> None:
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            await repo.save_submission(
                OrderSubmissionRecord(
                    idempotency_key="k2",
                    user_id="u1",
                    leader_id="l1",
                    order_id="ord-xyz",
                    status="submitted",
                    request_payload={"a": 1},
                    response_payload={"orderID": "ord-xyz"},
                )
            )
            row = await repo.get_by_idempotency_key("k2")
        assert row is not None
        assert row["order_id"] == "ord-xyz"
        assert row["status"] == "submitted"

    @pytest.mark.asyncio
    async def test_processed_trade_lifecycle(self) -> None:
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            assert await repo.has_processed_trade("tk1") is False
            await repo.mark_processed_trade(
                trade_key="tk1",
                user_id="u1",
                leader_id="l1",
                condition_id="c1",
                side="BUY",
                status="ok",
                tx_hash="0xdead",
                asset="3" * 20,
            )
            assert await repo.has_processed_trade("tk1") is True
            assert await repo.get_processed_trade("tk1") is not None


class TestPositionsRepo:
    @pytest.mark.asyncio
    async def test_upsert_and_list(self) -> None:
        async with _pg_pool() as pool:
            repo = PositionsRepo(pool)
            await repo.upsert_position(
                PositionRecord(
                    user_id="u1",
                    condition_id="c1",
                    token_id="1" * 20,
                    leader_id="l1",
                    size_tokens=Decimal("10"),
                    avg_entry_price=Decimal("0.5"),
                )
            )
            rows = await repo.list_open_positions("u1")
        assert len(rows) == 1
        assert rows[0].condition_id == "c1"

    @pytest.mark.asyncio
    async def test_delete_position(self) -> None:
        async with _pg_pool() as pool:
            repo = PositionsRepo(pool)
            await repo.upsert_position(
                PositionRecord(
                    user_id="u1",
                    condition_id="c1",
                    token_id="2" * 20,
                    leader_id="l1",
                    size_tokens=Decimal("5"),
                    avg_entry_price=Decimal("0.6"),
                )
            )
            await repo.delete_position("u1", "c1", token_id="2" * 20)
            got = await repo.get_position("u1", "c1", token_id="2" * 20)
        assert got is None


class TestFillsRepo:
    @pytest.mark.asyncio
    async def test_append_and_list(self) -> None:
        async with _pg_pool() as pool:
            repo = FillsRepo(pool)
            fid = await repo.append_fill(
                FillRecord(
                    user_id="u1",
                    leader_id="l1",
                    order_id="ord-1",
                    condition_id="c1",
                    token_id="3" * 20,
                    side="BUY",
                    size_tokens=Decimal("1"),
                    price=Decimal("0.5"),
                )
            )
            assert isinstance(fid, int)
            user_rows = await repo.list_fills_for_user("u1")
            cond_rows = await repo.list_fills_for_user_condition("u1", "c1")
        assert user_rows
        assert cond_rows


class TestLeadersRepo:
    @pytest.mark.asyncio
    async def test_save_roster_and_list_active(self) -> None:
        async with _pg_pool() as pool:
            repo = LeadersRepo(pool)
            await repo.save_roster(
                [
                    LeaderRosterRecord(
                        wallet="0xaaa",
                        tier="active",
                        score=1.0,
                        base_score=1.0,
                        longitudinal_score=1.0,
                        cycles_in_tier=3,
                        promote_streak=0,
                        demote_streak=0,
                        tier_changed_at=1.0,
                        first_promoted_at=1.0,
                        last_seen_at=1.0,
                        config_hash="test",
                    ),
                    LeaderRosterRecord(
                        wallet="0xbbb",
                        tier="watchlist",
                        score=0.5,
                        base_score=0.5,
                        longitudinal_score=0.5,
                        cycles_in_tier=1,
                        promote_streak=1,
                        demote_streak=0,
                        tier_changed_at=1.0,
                        first_promoted_at=None,
                        last_seen_at=1.0,
                        config_hash="test",
                    ),
                ]
            )
            actives = await repo.list_active()
            matching = await repo.list_active(config_hash="test")
            stale = await repo.list_active(config_hash="different")
        assert "0xaaa" in actives
        assert "0xbbb" not in actives
        assert matching == ["0xaaa"]
        assert stale == []


class TestDiscoveryRepo:
    @pytest.mark.asyncio
    async def test_latest_candidates_scopes_to_the_requested_config_hash(self) -> None:
        """A base_score is only meaningful under the config that produced it.

        ``DiscoveryFunnel.rescore`` reuses the stored base_score and re-stamps
        the row with the *current* hash, so an old-hash snapshot must not be
        readable through the current roster.
        """
        async with _pg_pool() as pool:
            repo = DiscoveryRepo(pool)

            async def _cycle(config_hash: str, wallet: str) -> None:
                cycle_id = await repo.begin_cycle(config_hash)
                candidate = Candidate(wallet=wallet, hscore={})
                candidate.base_score = 1.0
                candidate.score = 1.0
                await repo.persist_cycle(cycle_id, [candidate], config_hash)
                await repo.complete_cycle(
                    cycle_id,
                    status="completed",
                    candidate_count=1,
                    kept_count=1,
                )

            await _cycle("old-hash", "0xold")
            await _cycle("current-hash", "0xcurrent")

            scoped = await repo.latest_candidates(config_hash="current-hash")
            stale = await repo.latest_candidates(config_hash="old-hash")
            unknown = await repo.latest_candidates(config_hash="never-used")
            unscoped = await repo.latest_candidates()

        assert [candidate.wallet for candidate in scoped] == ["0xcurrent"]
        # An older cycle is still readable when explicitly asked for by hash.
        assert [candidate.wallet for candidate in stale] == ["0xold"]
        # A hash no cycle ever ran under yields nothing — never a silent
        # fallback to the newest cycle.
        assert unknown == []
        # None keeps the unscoped latest-completed-cycle read.
        assert [candidate.wallet for candidate in unscoped] == ["0xcurrent"]


class TestCursorsRepo:
    @pytest.mark.asyncio
    async def test_default_cursor_then_roundtrip(self) -> None:
        async with _pg_pool() as pool:
            repo = CursorsRepo(pool)
            initial = await repo.get_cursor("leader-a")
            assert initial.timestamp == 0
            assert initial.tx_hash == ""

            await repo.set_cursor(
                "leader-a",
                TradeCursor(timestamp=1234, tx_hash="0xdead", trade_id="t1"),
            )
            reloaded = await repo.get_cursor("leader-a")
        assert reloaded.timestamp == 1234
        assert reloaded.tx_hash == "0xdead"
