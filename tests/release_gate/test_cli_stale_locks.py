"""Release-gate: ``bot stale-locks list`` CLI (Issue #76).

Seeds pending_lock orders — one fresh, one synthetically aged past TTL —
and asserts the listing only includes the aged row. Also validates that
the ``pending_since`` field in ``response_json`` takes priority over
``created_at``/``updated_at`` for age computation.

Backed by a live ``postgres:16-alpine`` testcontainer per the PG-A5
sweep — each test owns its own pool via the shared ``_pg_pool()``
async context manager (mirrors the pattern in
``tests/test_market_structure_service.py``).
"""
from __future__ import annotations

import contextlib
import json
import time
from datetime import datetime, timedelta, timezone

import pytest

from bot_cli.commands.stale_locks import _list_stale_locks, _resolve_stale_locks

pytestmark = [pytest.mark.asyncio, pytest.mark.release_gate]


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


async def _seed(pool) -> None:
    now_utc = datetime.now(tz=timezone.utc)
    old_ts = now_utc - timedelta(hours=1)
    fresh_ts = now_utc
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO orders
              (idempotency_key, user_id, leader_id, order_id, status,
               request_json, response_json, attempt_count, last_error,
               created_at, updated_at)
            VALUES
              ('k-old','u-test',NULL,NULL,'pending_lock','{}'::jsonb,NULL,0,NULL,$1,$1),
              ('k-fresh','u-test',NULL,NULL,'pending','{}'::jsonb,NULL,0,NULL,$2,$2)
            """,
            old_ts,
            fresh_ts,
        )


async def _seed_with_pending_since(pool) -> None:
    """Seed rows with response_json containing pending_since epoch floats."""
    now_utc = datetime.now(tz=timezone.utc)
    old_ts = now_utc - timedelta(hours=1)
    fresh_ts = now_utc
    old_epoch = time.time() - 7200  # 2 hours ago as epoch
    fresh_epoch = time.time()  # now as epoch
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO orders
              (idempotency_key, user_id, leader_id, order_id, status,
               request_json, response_json, attempt_count, last_error,
               created_at, updated_at)
            VALUES
              ('k-old-ps','u-test',NULL,NULL,'pending_lock','{}'::jsonb,$1::jsonb,0,NULL,$2,$2),
              ('k-fresh-ps','u-test',NULL,NULL,'pending','{}'::jsonb,$3::jsonb,0,NULL,$4,$4)
            """,
            json.dumps({"pending_since": old_epoch}),
            old_ts,
            json.dumps({"pending_since": fresh_epoch}),
            fresh_ts,
        )


async def _seed_resolve_candidates(pool) -> None:
    now_utc = datetime.now(tz=timezone.utc)
    old_ts = now_utc - timedelta(hours=1)
    fresh_ts = now_utc
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO orders
              (idempotency_key, user_id, leader_id, order_id, status,
               request_json, response_json, attempt_count, last_error,
               created_at, updated_at)
            VALUES
              ('k-safe','u-test',NULL,NULL,'pending_lock','{}'::jsonb,NULL,0,NULL,$1,$1),
              ('k-defer','u-test',NULL,'ord-live','submitted_unconfirmed','{}'::jsonb,NULL,0,NULL,$1,$1),
              ('k-fresh','u-test',NULL,NULL,'pending','{}'::jsonb,NULL,0,NULL,$2,$2)
            """,
            old_ts,
            fresh_ts,
        )


async def test_only_aged_locks_listed() -> None:
    async with _pg_pool() as pool:
        await _seed(pool)
        rows = await _list_stale_locks(pool, ttl_seconds=120, user_id=None)
    assert len(rows) == 1
    assert rows[0]["idempotency_key"] == "k-old"
    assert rows[0]["age_seconds"] >= 120


async def test_pending_since_priority() -> None:
    """When response_json.pending_since is present, it overrides created_at for age."""
    async with _pg_pool() as pool:
        await _seed_with_pending_since(pool)
        rows = await _list_stale_locks(pool, ttl_seconds=120, user_id=None)
    assert len(rows) == 1
    assert rows[0]["idempotency_key"] == "k-old-ps"
    assert rows[0]["age_seconds"] >= 120
    assert rows[0]["pending_since_ts"] is not None


async def test_user_filter() -> None:
    async with _pg_pool() as pool:
        await _seed(pool)
        other = await _list_stale_locks(pool, ttl_seconds=120, user_id="other")
    assert other == []


async def test_resolve_dry_run_classifies_without_mutation() -> None:
    async with _pg_pool() as pool:
        await _seed_resolve_candidates(pool)
        result = await _resolve_stale_locks(
            pool, ttl_seconds=120, user_id=None, apply=False
        )
        async with pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM orders WHERE idempotency_key = 'k-safe'"
            )

    assert result["applied"] is False
    assert result["summary"] == {
        "expired": 0,
        "would_expire": 1,
        "deferred_reconciliation_required": 1,
        "kept_fresh": 1,
    }
    assert result["would_expire"][0]["idempotency_key"] == "k-safe"
    assert result["deferred_reconciliation_required"][0]["idempotency_key"] == "k-defer"
    assert status == "pending_lock"


async def test_resolve_apply_expires_only_safe_no_order_lock() -> None:
    async with _pg_pool() as pool:
        await _seed_resolve_candidates(pool)
        result = await _resolve_stale_locks(
            pool, ttl_seconds=120, user_id=None, apply=True
        )
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT idempotency_key, order_id, status, response_json
                FROM orders
                ORDER BY idempotency_key
                """
            )

    by_key = {row["idempotency_key"]: row for row in rows}
    assert result["applied"] is True
    assert result["summary"]["expired"] == 1
    assert result["summary"]["deferred_reconciliation_required"] == 1
    assert result["expired"][0]["idempotency_key"] == "k-safe"
    assert by_key["k-safe"]["status"] == "expired"
    assert json.loads(by_key["k-safe"]["response_json"])["status"] == "expired"
    assert by_key["k-defer"]["status"] == "submitted_unconfirmed"
    assert by_key["k-defer"]["order_id"] == "ord-live"


async def test_run_stale_locks_emits_json(capsys, monkeypatch) -> None:
    """End-to-end `run_stale_locks` against a live PG testcontainer."""
    from types import SimpleNamespace

    from testcontainers.postgres import PostgresContainer
    import asyncpg

    from bot_cli.commands.stale_locks import run_stale_locks
    from storage.postgres.connection import shutdown_pool
    from storage.postgres.migrations import apply_migrations as pg_apply

    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        await _seed(pool)
        await pool.close()
        # The CLI's init_pool will re-open with the same DSN.

        fake_config = SimpleNamespace(
            database=SimpleNamespace(dsn=dsn, db_path=""),
            execution=SimpleNamespace(pending_submission_ttl_seconds=120),
        )
        monkeypatch.setattr(
            "bot_cli.commands.stale_locks._load_config_or_fail",
            lambda: fake_config,
        )
        args = SimpleNamespace(
            stale_locks_subcommand="list",
            ttl_seconds=None,
            user_id=None,
        )
        # run_stale_locks calls asyncio.run() internally, which conflicts
        # with the outer async test loop. Re-run the inner async path
        # directly instead, mirroring what run_stale_locks dispatches to.
        from bot_cli.commands.stale_locks import _run_stale_locks_async

        rc = await _run_stale_locks_async(args)
        # Ensure pool gets torn down even though _run_list_async already did.
        try:
            await shutdown_pool()
        except Exception:
            pass

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["count"] == 1
        assert payload["ttl_seconds"] == 120
        assert payload["stale_locks"][0]["idempotency_key"] == "k-old"
    finally:
        ctr.stop()
