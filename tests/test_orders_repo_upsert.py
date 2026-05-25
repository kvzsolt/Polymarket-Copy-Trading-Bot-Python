from __future__ import annotations

import contextlib

import pytest

from storage.postgres.orders_repo import OrderSubmissionRecord, OrdersRepo

pytestmark = pytest.mark.asyncio


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


async def test_order_repo_upsert_replaces_failed_submission():
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        key = "abc123"

        await repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key=key,
                user_id="u1",
                leader_id="l1",
                order_id=None,
                status="submission_failed",
                request_payload={"x": 1},
                response_payload={"error": "timeout"},
                last_error="timeout",
            )
        )

        await repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key=key,
                user_id="u1",
                leader_id="l1",
                order_id="ord-789",
                status="submitted",
                request_payload={"x": 1},
                response_payload={"orderID": "ord-789"},
                last_error=None,
            )
        )

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT order_id, status, attempt_count FROM orders "
                "WHERE idempotency_key = $1",
                key,
            )

    assert row is not None
    assert row["order_id"] == "ord-789"
    assert row["status"] == "submitted"
    assert int(row["attempt_count"]) >= 2


async def test_persist_execution_atomically_scopes_position_by_token_id():
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO positions
                  (user_id, condition_id, token_id, leader_id,
                   size_tokens, avg_entry_price)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                "u1", "cond-1", "token-a", "leader-a", "10", "0.40",
            )
            await conn.execute(
                """
                INSERT INTO positions
                  (user_id, condition_id, token_id, leader_id,
                   size_tokens, avg_entry_price)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                "u1", "cond-1", "token-b", "leader-b", "4", "0.60",
            )

        persisted = await repo.persist_execution_atomically(
            fill={"order_id": "ord-buy-b", "fill_id": "fill-1"},
            user_id="u1",
            leader_id="leader-b",
            condition_id="cond-1",
            token_id="token-b",
            side="BUY",
            fill_size="3",
            fill_price="0.70",
            fill_fee="0.01",
            fill_ts=123456,
            fill_json='{"order_id":"ord-buy-b","fill_id":"fill-1"}',
            trade_key="trade-key-b",
            tx_hash="0xabc",
            asset="token-b",
            trade_status="executed",
        )

        assert persisted is True

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT token_id, size_tokens, avg_entry_price FROM positions "
                "WHERE user_id = $1 AND condition_id = $2 "
                "ORDER BY token_id",
                "u1", "cond-1",
            )

    assert len(rows) == 2
    assert rows[0]["token_id"] == "token-a"
    assert rows[0]["size_tokens"] == "10"
    assert rows[0]["avg_entry_price"] == "0.40"
    assert rows[1]["token_id"] == "token-b"
    assert rows[1]["size_tokens"] == "7"
    assert rows[1]["avg_entry_price"] == "0.6428571428571428571428571429"
