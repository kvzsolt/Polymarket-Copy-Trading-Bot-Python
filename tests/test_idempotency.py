"""Tests for ``utils.idempotency`` (PG-A5 async PG version)."""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

pytestmark = pytest.mark.asyncio


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
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
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)

        # Patch the singleton pool getter so the module-under-test
        # acquires this testcontainer pool. Restores the prior state
        # via _reset_for_tests at exit.
        from pmcopy.storage.postgres import connection as conn_mod

        conn_mod._reset_for_tests()
        prev_pool = conn_mod._pool
        prev_dsn = conn_mod._pool_dsn
        conn_mod._pool = pool
        conn_mod._pool_dsn = dsn
        try:
            yield pool
        finally:
            conn_mod._pool = prev_pool
            conn_mod._pool_dsn = prev_dsn
            await pool.close()
    finally:
        ctr.stop()


def test_build_trade_key():
    """Pure-function test — sync, no pool needed."""
    from pmcopy.utils.idempotency import build_trade_key

    trade = {
        "transactionHash": "0xABC",
        "side": "buy",
        "asset": "12345",
    }
    key = build_trade_key("0xLeader", trade)
    assert key == "0xleader|0xabc|BUY|12345"


class TestIdempotencyStore:
    async def test_mark_and_has_processed(self):
        async with _pg_pool():
            import pmcopy.utils.idempotency as idem

            trade = {
                "transactionHash": "0x1",
                "side": "BUY",
                "asset": "a1",
                "conditionId": "c1",
            }
            key = idem.build_trade_key("0xleader", trade)
            assert await idem.has_processed(key) is False

            await idem.mark_processed("0xleader", trade, status="executed")
            assert await idem.has_processed(key) is True

    async def test_mark_processed_all_tx_hashes_marks_aggregated(self):
        async with _pg_pool():
            import pmcopy.utils.idempotency as idem

            trade = {
                "transactionHash": "0xNEW",
                "side": "BUY",
                "asset": "a1",
                "conditionId": "c1",
                "_txHashes": ["0xOLD1", "0xOLD2"],
            }

            keys = await idem.mark_processed_all_tx_hashes("0xleader", trade, status="executed")
            assert len(keys) >= 3

            for txh in ["0xnew", "0xold1", "0xold2"]:
                k = idem.build_trade_key(
                    "0xleader",
                    {"transactionHash": txh, "side": "BUY", "asset": "a1"},
                )
                assert await idem.has_processed(k) is True
