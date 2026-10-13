"""Disposable-Postgres gate for the cross-process account lease (Task 21).

The unit contracts in ``tests/test_account_lease.py`` pin the manager and
typed-identity semantics against a fake store; these tests run the REAL
``AccountLeaseRepo`` SQL against a throwaway Postgres (testcontainers) with
the REAL migration catalog applied:

* the additive migration creates ``account_execution_leases``;
* a duplicate process acquire fails closed and reports the live holder;
* renewal extends ``expires_at`` for the holder only;
* graceful release frees the lease for a clean successor acquire;
* a crashed holder (no release) blocks until TTL expiry, then takeover
  succeeds (flagged) with the previous holder replaced;
* leases are per-account rows — two accounts never collide on one PK.

The repo's conftest auto-marks this file ``integration`` (it mentions
PostgresContainer), so run it explicitly:

    .venv/bin/python -m pytest -m integration \
        tests/test_account_lease_postgres.py -q
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from pmcopy.storage.postgres.account_lease_repo import AccountLeaseRepo

pytestmark = pytest.mark.asyncio

_USER_A = "copier-alpha"
_USER_B = "copier-beta"
_TTL_SECONDS = 60.0


@asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer and apply the full schema."""
    try:
        from tests.integration_postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:  # pragma: no cover - dependency guard
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:  # pragma: no cover - environment guard
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


async def _force_expiry(pool, user_id: str) -> None:
    """Simulate a crashed holder's TTL lapsing: rewind expires_at."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE account_execution_leases SET expires_at = now() - interval '1 second' "
            "WHERE user_id = $1",
            user_id,
        )


async def test_migration_creates_lease_table() -> None:
    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT to_regclass('account_execution_leases') IS NOT NULL"
            )
            assert exists is True
            columns = {
                record["column_name"]
                for record in await conn.fetch(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'account_execution_leases'"
                )
            }
    assert {
        "user_id",
        "holder_host",
        "holder_token",
        "mode",
        "acquired_at",
        "renewed_at",
        "expires_at",
        "released_at",
    } <= columns


async def test_second_process_acquire_fails_closed_with_holder_info() -> None:
    async with _pg_pool() as pool:
        repo = AccountLeaseRepo(pool)
        first = await repo.acquire(
            user_id=_USER_A,
            holder_host="host-1",
            holder_token="token-1",
            mode="live",
            ttl_seconds=_TTL_SECONDS,
        )
        assert first.acquired is True
        assert first.takeover is False
        assert first.lease is not None
        assert first.lease.holder_token == "token-1"

        second = await repo.acquire(
            user_id=_USER_A,
            holder_host="host-2",
            holder_token="token-2",
            mode="live",
            ttl_seconds=_TTL_SECONDS,
        )
        assert second.acquired is False
        assert second.takeover is False
        assert second.held_by is not None
        assert second.held_by.holder_host == "host-1"
        assert second.held_by.holder_token == "token-1"
        assert second.held_by.released_at is None


async def test_same_holder_reacquire_is_idempotent() -> None:
    async with _pg_pool() as pool:
        repo = AccountLeaseRepo(pool)
        assert (
            await repo.acquire(
                user_id=_USER_A,
                holder_host="host-1",
                holder_token="token-1",
                mode="paper",
                ttl_seconds=_TTL_SECONDS,
            )
        ).acquired
        again = await repo.acquire(
            user_id=_USER_A,
            holder_host="host-1",
            holder_token="token-1",
            mode="paper",
            ttl_seconds=_TTL_SECONDS,
        )
        assert again.acquired is True
        assert again.reacquired is True
        assert again.takeover is False


async def test_renewal_extends_expiry_for_holder_only() -> None:
    async with _pg_pool() as pool:
        repo = AccountLeaseRepo(pool)
        assert (
            await repo.acquire(
                user_id=_USER_A,
                holder_host="host-1",
                holder_token="token-1",
                mode="live",
                ttl_seconds=_TTL_SECONDS,
            )
        ).acquired

        # A non-holder renew is a no-op (confirmed loss for that process).
        assert (
            await repo.renew(
                user_id=_USER_A, holder_token="token-2", ttl_seconds=_TTL_SECONDS
            )
            is None
        )

        renewed_at = await repo.renew(
            user_id=_USER_A, holder_token="token-1", ttl_seconds=_TTL_SECONDS
        )
        assert renewed_at is not None
        current = await repo.get(_USER_A)
        assert current is not None
        assert current.expires_at == renewed_at
        assert current.released_at is None


async def test_graceful_release_frees_lease_for_clean_successor() -> None:
    async with _pg_pool() as pool:
        repo = AccountLeaseRepo(pool)
        assert (
            await repo.acquire(
                user_id=_USER_A,
                holder_host="host-1",
                holder_token="token-1",
                mode="live",
                ttl_seconds=_TTL_SECONDS,
            )
        ).acquired

        assert (
            await repo.release(user_id=_USER_A, holder_token="token-2") is False
        )  # a non-holder cannot release
        assert await repo.release(user_id=_USER_A, holder_token="token-1") is True
        assert await repo.release(user_id=_USER_A, holder_token="token-1") is False  # once

        successor = await repo.acquire(
            user_id=_USER_A,
            holder_host="host-2",
            holder_token="token-2",
            mode="live",
            ttl_seconds=_TTL_SECONDS,
        )
        assert successor.acquired is True
        assert successor.takeover is False  # clean handoff, not expiry takeover
        assert successor.reacquired is False


async def test_crashed_holder_blocks_until_ttl_then_takeover_succeeds() -> None:
    async with _pg_pool() as pool:
        repo = AccountLeaseRepo(pool)
        assert (
            await repo.acquire(
                user_id=_USER_A,
                holder_host="host-1",
                holder_token="token-1",
                mode="live",
                ttl_seconds=_TTL_SECONDS,
            )
        ).acquired
        # Crash: no renewal, no release; lease still inside its TTL.
        early = await repo.acquire(
            user_id=_USER_A,
            holder_host="host-2",
            holder_token="token-2",
            mode="live",
            ttl_seconds=_TTL_SECONDS,
        )
        assert early.acquired is False
        assert early.held_by is not None

        await _force_expiry(pool, _USER_A)
        takeover = await repo.acquire(
            user_id=_USER_A,
            holder_host="host-2",
            holder_token="token-2",
            mode="live",
            ttl_seconds=_TTL_SECONDS,
        )
        assert takeover.acquired is True
        assert takeover.takeover is True
        assert takeover.lease is not None
        assert takeover.lease.holder_token == "token-2"
        assert takeover.lease.released_at is None


async def test_leases_are_per_account_rows() -> None:
    async with _pg_pool() as pool:
        repo = AccountLeaseRepo(pool)
        assert (
            await repo.acquire(
                user_id=_USER_A,
                holder_host="host-1",
                holder_token="token-1",
                mode="live",
                ttl_seconds=_TTL_SECONDS,
            )
        ).acquired
        # The same process (or another) can hold the OTHER account freely.
        assert (
            await repo.acquire(
                user_id=_USER_B,
                holder_host="host-1",
                holder_token="token-1",
                mode="live",
                ttl_seconds=_TTL_SECONDS,
            )
        ).acquired
        assert (
            await repo.acquire(
                user_id=_USER_B,
                holder_host="host-2",
                holder_token="token-2",
                mode="live",
                ttl_seconds=_TTL_SECONDS,
            )
        ).acquired is False
        assert await repo.get(_USER_B) is not None
        assert await repo.get("missing-account") is None


async def test_manager_end_to_end_against_real_sql() -> None:
    from pmcopy.domain.account_identity import CopierAccountId
    from pmcopy.services.account_lease import AccountLeaseManager

    async with _pg_pool() as pool:
        holder = AccountLeaseManager(pool=pool, ttl_seconds=_TTL_SECONDS)
        challenger = AccountLeaseManager(pool=pool, ttl_seconds=_TTL_SECONDS)

        assert (
            await holder.acquire(CopierAccountId(_USER_A), mode="live")
        ).acquired is True
        assert (
            await challenger.acquire(CopierAccountId(_USER_A), mode="live")
        ).acquired is False

        renewals = await holder.renew_held()
        assert _USER_A in renewals
        assert await holder.release_all() == 1
        # Challenger succeeds on the released row — a clean, non-takeover win.
        outcome = await challenger.acquire(CopierAccountId(_USER_A), mode="live")
        assert outcome.acquired is True
        assert outcome.takeover is False
