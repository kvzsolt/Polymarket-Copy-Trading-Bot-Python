"""Regression: positions_repo.apply_fill must be atomic + immutable.

Pre-fix:
  * apply_fill opened one connection for ``get_position`` and a separate
    connection for ``upsert_position`` / ``delete_position``. Two
    concurrent SELLs could both observe ``size=5``, both compute
    ``new=5-2=3``, both upsert ``size=3`` — the second fill silently
    overwrites the first. Lost-update race.
  * apply_fill mutated the in-memory ``PositionRecord`` dataclass in
    place before calling upsert_position. If upsert raised, callers
    holding the reference observed half-mutated state. Per CLAUDE.md
    coding-style.md: "ALWAYS create new objects, NEVER mutate existing
    ones."

PG-A5: PG ``PositionsRepo.apply_fill`` holds a per-row advisory lock
(``pg_advisory_xact_lock`` keyed on (user, condition, token)) + uses
``SELECT FOR UPDATE`` inside a transaction so the SELECT → arithmetic
→ UPSERT/DELETE chain is serialised against concurrent writers — even
when the row doesn't exist yet on the first fill.
"""

from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal

import pytest

from pmcopy.storage.postgres.positions_repo import PositionRecord, PositionsRepo

pytestmark = pytest.mark.asyncio


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
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
        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=6)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


async def _seed_position(repo: PositionsRepo, size: Decimal) -> None:
    await repo.upsert_position(
        PositionRecord(
            user_id="u",
            condition_id="c",
            token_id="1" * 64,
            leader_id="L",
            size_tokens=size,
            avg_entry_price=Decimal("0.50"),
        )
    )


async def test_concurrent_sells_no_lost_update():
    """Two concurrent SELLs of 2 each on a held=5 must leave size=1."""
    async with _pg_pool() as pool:
        repo = PositionsRepo(pool)
        held = Decimal("5")
        await _seed_position(repo, held)

        async def _sell(amount: Decimal) -> None:
            await repo.apply_fill(
                user_id="u",
                leader_id="L",
                condition_id="c",
                token_id="1" * 64,
                side="SELL",
                fill_size=amount,
                fill_price=Decimal("0.50"),
            )

        await asyncio.gather(_sell(Decimal("2")), _sell(Decimal("2")))

        final = await repo.get_position("u", "c", token_id="1" * 64)
        assert final is not None
        assert final.size_tokens == Decimal("1"), (
            f"lost-update race: expected 1 token left after 5 - 2 - 2, got {final.size_tokens}"
        )


async def test_apply_fill_returns_fresh_record_not_mutated_in_place():
    """The PositionRecord returned must not be the same object the caller
    pre-seeded — protects against partial-mutation visibility if upsert
    raises mid-operation."""
    async with _pg_pool() as pool:
        repo = PositionsRepo(pool)
        await _seed_position(repo, Decimal("10"))
        pre = await repo.get_position("u", "c", token_id="1" * 64)
        assert pre is not None
        pre_id = id(pre)

        returned = await repo.apply_fill(
            user_id="u",
            leader_id="L",
            condition_id="c",
            token_id="1" * 64,
            side="BUY",
            fill_size=Decimal("1"),
            fill_price=Decimal("0.60"),
        )
        assert returned is not None
        assert id(returned) != pre_id, (
            "apply_fill must return a NEW PositionRecord, not mutate the "
            "caller's pre-fetched one in place (CLAUDE.md immutability "
            "invariant)."
        )
        assert pre.size_tokens == Decimal("10"), (
            f"pre-fill snapshot was mutated under the caller: now {pre.size_tokens}"
        )
