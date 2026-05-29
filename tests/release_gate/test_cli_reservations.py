"""Release-gate: ``bot reservations list`` CLI (Issue #76).

Seeds a per-test PG testcontainer with two active reservations (one BUY,
one SELL) plus one released reservation, then asserts the CLI summary
matches the expected per-user totals and that released rows are
excluded by default.
"""
from __future__ import annotations

import contextlib
import json
import time

import pytest

from bot_cli.commands.reservations import _list_reservations, _summarize

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
            yield pool, dsn
        finally:
            await pool.close()
    finally:
        ctr.stop()


async def _seed(pool) -> None:
    now = int(time.time())
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO order_reservations
            (id, user_id, order_intent_key, condition_id, token_id, side, reason,
             reserved_usdc, reserved_size, status, created_at, updated_at, expires_at,
             order_id, venue_order_id, metadata_json)
            VALUES
            ('r1','u-test','k1','cond-x','tok-x','BUY','copy','25.50','0','active',$1,$1,$2,NULL,NULL,'{}'::jsonb),
            ('r2','u-test','k2','cond-x','tok-x','SELL','exit','0','7.0','active',$1,$1,$2,NULL,NULL,'{}'::jsonb),
            ('r3','u-test','k3','cond-x','tok-x','BUY','copy','100','0','released',$1,$1,$2,NULL,NULL,'{}'::jsonb)
            """,
            now,
            now + 600,
        )


async def test_list_reservations_filters_released() -> None:
    async with _pg_pool() as (pool, _dsn):
        await _seed(pool)
        rows = await _list_reservations(pool, user_id=None, include_expired=False)
    assert len(rows) == 2
    assert {r["id"] for r in rows} == {"r1", "r2"}


async def test_summary_per_user_totals() -> None:
    async with _pg_pool() as (pool, _dsn):
        await _seed(pool)
        rows = await _list_reservations(pool, user_id=None, include_expired=False)
    summary = _summarize(rows)
    assert summary["total_active"] == 2
    assert summary["total_buy_usdc"] == "25.50"
    assert summary["total_sell_size"] == "7.0"
    user_bucket = summary["per_user"]["u-test"]
    assert user_bucket["count"] == 2
    assert user_bucket["buy_usdc"] == "25.50"
    assert user_bucket["sell_size"] == "7.0"


async def test_user_filter_isolates() -> None:
    async with _pg_pool() as (pool, _dsn):
        await _seed(pool)
        none_rows = await _list_reservations(
            pool, user_id="other-user", include_expired=False
        )
    assert none_rows == []


async def test_run_reservations_invocation_exits_zero(capsys, monkeypatch) -> None:
    """End-to-end ``run_reservations`` against a live PG testcontainer."""
    from types import SimpleNamespace

    from bot_cli.commands.reservations import _run_reservations_async
    from storage.postgres.connection import shutdown_pool

    async with _pg_pool() as (pool, dsn):
        await _seed(pool)
        # Close the seeding pool — CLI will open its own via init_pool.
        await pool.close()

        fake_config = SimpleNamespace(database=SimpleNamespace(dsn=dsn, db_path=""))
        monkeypatch.setattr(
            "bot_cli.commands.reservations._load_config_or_fail",
            lambda: fake_config,
        )

        args = SimpleNamespace(
            reservations_subcommand="list",
            user_id=None,
            include_expired=False,
        )
        # Drive the inner async path directly (run_reservations uses
        # asyncio.run() which would clash with the outer test loop).
        rc = await _run_reservations_async(args)
        try:
            await shutdown_pool()
        except Exception:
            pass

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["total_active"] == 2
