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

from pmcopy.bot_cli.commands.reservations import (
    _list_reservations,
    _resolve_expired_reservations,
    _summarize,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.release_gate]


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply

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


async def _seed_expired_candidates(pool) -> None:
    now = int(time.time())
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO order_reservations
            (id, user_id, order_intent_key, condition_id, token_id, side, reason,
             reserved_usdc, reserved_size, status, created_at, updated_at, expires_at,
             order_id, venue_order_id, metadata_json)
            VALUES
            ('r-expired-orphan','u-test','k-exp-orphan','cond-x','tok-x','BUY','copy',
             '5.00','0','active',$1,$1,$2,NULL,NULL,'{}'::jsonb),
            ('r-expired-linked','u-test','k-exp-linked','cond-x','tok-x','SELL','exit',
             '0','3.0','active',$1,$1,$2,'local-order-1',NULL,'{}'::jsonb),
            ('r-fresh','u-test','k-fresh','cond-x','tok-x','BUY','copy',
             '7.00','0','active',$1,$1,$3,NULL,NULL,'{}'::jsonb)
            """,
            now,
            now - 600,
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
        none_rows = await _list_reservations(pool, user_id="other-user", include_expired=False)
    assert none_rows == []


async def test_resolve_expired_reservations_dry_run() -> None:
    async with _pg_pool() as (pool, _dsn):
        await _seed_expired_candidates(pool)
        result = await _resolve_expired_reservations(pool, user_id=None, apply=False)
        async with pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM order_reservations WHERE id = 'r-expired-orphan'"
            )

    assert result["applied"] is False
    assert result["summary"] == {
        "released": 0,
        "would_release": 1,
        "deferred_reconciliation_required": 1,
    }
    assert result["would_release"][0]["id"] == "r-expired-orphan"
    assert result["deferred_reconciliation_required"][0]["id"] == "r-expired-linked"
    assert status == "active"


async def test_resolve_expired_reservations_apply_releases_only_orphan() -> None:
    async with _pg_pool() as (pool, _dsn):
        await _seed_expired_candidates(pool)
        result = await _resolve_expired_reservations(pool, user_id=None, apply=True)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, status, metadata_json
                FROM order_reservations
                ORDER BY id
                """
            )

    by_id = {row["id"]: row for row in rows}
    assert result["applied"] is True
    assert result["summary"]["released"] == 1
    assert result["summary"]["deferred_reconciliation_required"] == 1
    assert result["released"][0]["id"] == "r-expired-orphan"
    assert by_id["r-expired-orphan"]["status"] == "released"
    metadata = json.loads(by_id["r-expired-orphan"]["metadata_json"])
    assert metadata["release_reason"] == "expired_orphan_no_order"
    assert by_id["r-expired-linked"]["status"] == "active"
    assert by_id["r-fresh"]["status"] == "active"


async def _seed_rowbacked_candidate(pool) -> None:
    """An expired no-link reservation WITH a live no-ID submission row.

    The orders row is linked to the reservation only through the persisted
    ``recovery_metadata.reservation_id`` — the same link the runtime expiry
    classifier uses. It is unresolved post-submit state, never safe_to_release.
    """
    now = int(time.time())
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO order_reservations
            (id, user_id, order_intent_key, condition_id, token_id, side, reason,
             reserved_usdc, reserved_size, status, created_at, updated_at, expires_at,
             order_id, venue_order_id, metadata_json)
            VALUES
            ('r-expired-rowbacked','u-test','k-exp-rowbacked','cond-x','tok-x','BUY','copy',
             '9.00','0','active',$1,$1,$2,NULL,NULL,'{}'::jsonb)
            """,
            now,
            now - 600,
        )
        await conn.execute(
            """
            INSERT INTO orders
            (idempotency_key, user_id, leader_id, order_id, status,
             request_json, response_json, attempt_count, last_error,
             created_at, updated_at)
            VALUES
            ('idem-rowbacked','u-test','leader-1',NULL,'submitted_unconfirmed',
             $1::jsonb, $2::jsonb, 1, NULL, now(), now())
            """,
            json.dumps(
                {
                    "user_id": "u-test",
                    "leader_id": "leader-1",
                    "condition_id": "cond-x",
                    "token_id": "tok-x",
                    "side": "BUY",
                    "signal_id": "event-rowbacked",
                    "recovery_metadata": {"reservation_id": "r-expired-rowbacked"},
                }
            ),
            json.dumps({"status": "submitted_unconfirmed", "success": False}),
        )


async def test_resolve_defers_rowbacked_unresolved_no_id_reservation() -> None:
    """A14: --apply must not release a reservation with a live no-ID row."""
    async with _pg_pool() as (pool, _dsn):
        await _seed_rowbacked_candidate(pool)
        dry = await _resolve_expired_reservations(pool, user_id=None, apply=False)
        applied = await _resolve_expired_reservations(pool, user_id=None, apply=True)
        async with pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM order_reservations WHERE id = 'r-expired-rowbacked'"
            )

    assert dry["applied"] is False
    assert dry["summary"]["would_release"] == 0
    assert dry["summary"]["deferred_reconciliation_required"] == 1
    deferred = dry["deferred_reconciliation_required"][0]
    assert deferred["id"] == "r-expired-rowbacked"
    assert deferred["classification"] == "recovery_pending"
    assert deferred["defer_reason"] == "unresolved_submission_row"

    assert applied["summary"]["released"] == 0
    assert status == "active"


async def test_run_reservations_invocation_exits_zero(capsys, monkeypatch) -> None:
    """End-to-end ``run_reservations`` against a live PG testcontainer."""
    from types import SimpleNamespace

    from pmcopy.bot_cli.commands.reservations import _run_reservations_async
    from pmcopy.storage.postgres.connection import shutdown_pool

    async with _pg_pool() as (pool, dsn):
        await _seed(pool)
        # Close the seeding pool — CLI will open its own via init_pool.
        await pool.close()

        fake_config = SimpleNamespace(database=SimpleNamespace(dsn=dsn, db_path=""))
        monkeypatch.setattr(
            "pmcopy.bot_cli.commands.reservations._load_config_or_fail",
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
