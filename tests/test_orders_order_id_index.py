"""Regression: orders.order_id must be uniquely indexed.

Pre-fix (sqlite era): orders only had idempotency_key as PRIMARY KEY.
Reconciliation queries by venue order_id (``WHERE order_id = ?``) did a
full-table scan on every cycle and, if two rows ever shared an
order_id, the ``ORDER BY updated_at DESC LIMIT 1`` query could return
the wrong row silently.

This test pins on PG:
  * A unique index ``idx_orders_order_id`` exists on
    ``orders(order_id) WHERE order_id IS NOT NULL``
  * Query plan for ``WHERE order_id = $1`` uses that index
  * Inserting two rows with the same non-NULL order_id raises
    ``UniqueViolationError`` (so a duplicate cannot land silently)
  * Multiple rows with NULL order_id coexist (partial-unique semantics)
"""

from __future__ import annotations

import contextlib

import pytest

pytestmark = pytest.mark.asyncio


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from tests.integration_postgres import PostgresContainer
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
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


async def test_orders_has_unique_partial_index_on_order_id():
    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename = 'orders'
                  AND indexname = 'idx_orders_order_id'
                """
            )
    assert row is not None, "expected idx_orders_order_id on orders table"
    indexdef = (row["indexdef"] or "").upper()
    assert "UNIQUE" in indexdef, f"index must be UNIQUE; got {indexdef}"
    # Partial index on non-NULL — NULL order_ids can repeat during pending state.
    assert "WHERE" in indexdef and "ORDER_ID IS NOT NULL" in indexdef, (
        f"index must be partial on order_id IS NOT NULL; got {indexdef}"
    )


async def test_query_plan_uses_order_id_index():
    """With seq-scan disabled the planner must reach the unique partial
    index — this confirms the index covers the predicate ``order_id = $1``.

    A small empty-ish test table would otherwise let PG prefer a seq
    scan on cost grounds; forcing ``enable_seqscan = off`` for the
    duration of the EXPLAIN is the standard PG idiom for asserting
    "the index is reachable" independent of table statistics.
    """
    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO orders (idempotency_key, order_id, request_json) "
                "VALUES ($1, $2, $3::jsonb)",
                "idem-plan",
                "venue_plan",
                "{}",
            )
            await conn.execute("ANALYZE orders")
            async with conn.transaction():
                # SET LOCAL only applies inside a transaction. Wrap the
                # planner-flag flip + EXPLAIN so they share the same tx.
                await conn.execute("SET LOCAL enable_seqscan = off")
                plan_rows = await conn.fetch(
                    "EXPLAIN SELECT * FROM orders WHERE order_id = $1",
                    "venue_123",
                )
    detail = " ".join(str(row[0]) for row in plan_rows).lower()
    assert "idx_orders_order_id" in detail, (
        f"WHERE order_id = $1 must use the unique index; plan was:\n{[r[0] for r in plan_rows]}"
    )


async def test_duplicate_order_id_insert_raises():
    import asyncpg

    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO orders (idempotency_key, order_id, request_json) "
                "VALUES ($1, $2, $3::jsonb)",
                "idem-a",
                "venue-X",
                "{}",
            )
            with pytest.raises(asyncpg.exceptions.UniqueViolationError):
                await conn.execute(
                    "INSERT INTO orders (idempotency_key, order_id, request_json) "
                    "VALUES ($1, $2, $3::jsonb)",
                    "idem-b",
                    "venue-X",
                    "{}",
                )


async def test_null_order_ids_allowed_to_repeat():
    """Multiple pending rows with NULL order_id must coexist (partial index)."""
    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO orders (idempotency_key, order_id, request_json) "
                "VALUES ($1, $2, $3::jsonb)",
                "idem-pending-1",
                None,
                "{}",
            )
            await conn.execute(
                "INSERT INTO orders (idempotency_key, order_id, request_json) "
                "VALUES ($1, $2, $3::jsonb)",
                "idem-pending-2",
                None,
                "{}",
            )
            count = await conn.fetchval("SELECT COUNT(*) FROM orders WHERE order_id IS NULL")
    assert count == 2
