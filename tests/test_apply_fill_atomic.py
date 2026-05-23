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

Tests:
  * Two concurrent SELL coroutines applying delta against the same
    position end up with the correct final size (no lost update).
  * The PositionRecord returned by apply_fill is a fresh object —
    distinct from any pre-fill record the caller might still hold.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.positions_repo import PositionRecord, PositionsRepo


@pytest.fixture()
def repo(tmp_path):
    db_path = str(tmp_path / "atomic.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.commit()
    return PositionsRepo(db_path)


def _seed_position(repo: PositionsRepo, size: Decimal) -> None:
    repo.upsert_position(
        PositionRecord(
            user_id="u",
            condition_id="c",
            token_id="1" * 64,
            leader_id="L",
            size_tokens=size,
            avg_entry_price=Decimal("0.50"),
        )
    )


@pytest.mark.asyncio
async def test_concurrent_sells_no_lost_update(repo):
    """Two concurrent SELLs of 2 each on a held=5 must leave size=1."""
    held = Decimal("5")
    _seed_position(repo, held)

    async def _sell(amount: Decimal) -> None:
        # asyncio.to_thread releases the GIL across blocking SQLite IO so
        # the two coroutines genuinely interleave their SELECT + UPSERT.
        await asyncio.to_thread(
            repo.apply_fill,
            user_id="u",
            leader_id="L",
            condition_id="c",
            token_id="1" * 64,
            side="SELL",
            fill_size=amount,
            fill_price=Decimal("0.50"),
        )

    await asyncio.gather(_sell(Decimal("2")), _sell(Decimal("2")))

    final = repo.get_position("u", "c", token_id="1" * 64)
    assert final is not None
    assert final.size_tokens == Decimal("1"), (
        f"lost-update race: expected 1 token left after 5 - 2 - 2, got {final.size_tokens}"
    )


def test_apply_fill_returns_fresh_record_not_mutated_in_place(repo):
    """The PositionRecord returned must not be the same object the caller
    pre-seeded — protects against partial-mutation visibility if upsert
    raises mid-operation."""
    _seed_position(repo, Decimal("10"))
    pre = repo.get_position("u", "c", token_id="1" * 64)
    assert pre is not None
    pre_id = id(pre)

    returned = repo.apply_fill(
        user_id="u",
        leader_id="L",
        condition_id="c",
        token_id="1" * 64,
        side="BUY",
        fill_size=Decimal("1"),
        fill_price=Decimal("0.60"),
    )
    assert returned is not None
    # Distinct identity from the caller's pre-fill snapshot.
    assert id(returned) != pre_id, (
        "apply_fill must return a NEW PositionRecord, not mutate the caller's "
        "pre-fetched one in place (CLAUDE.md immutability invariant)."
    )
    # And the caller's pre-fetched snapshot is unchanged.
    assert pre.size_tokens == Decimal("10"), (
        f"pre-fill snapshot was mutated under the caller: now {pre.size_tokens}"
    )
