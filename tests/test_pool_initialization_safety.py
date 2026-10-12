"""Pool ownership must remain bounded, configuration-safe, and secret-safe."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from pmcopy.storage.postgres import connection


@pytest.fixture(autouse=True)
def reset_pool():
    """Isolate the canonical singleton without opening a database."""
    connection._reset_for_tests()
    yield
    connection._reset_for_tests()


@pytest.mark.asyncio
async def test_reinitialization_does_not_expose_connection_secrets(monkeypatch, caplog):
    pool = AsyncMock()
    monkeypatch.setattr(connection.asyncpg, "create_pool", AsyncMock(return_value=pool))
    with caplog.at_level(logging.INFO):
        await connection.init_pool("postgresql://user:SYNTHETIC_OLD_SECRET@localhost/db")
        with pytest.raises(RuntimeError) as error:
            await connection.init_pool("postgresql://user:SYNTHETIC_NEW_SECRET@localhost/db")
    assert "SYNTHETIC" not in str(error.value) + caplog.text
    assert await connection.get_pool() is pool


@pytest.mark.asyncio
async def test_concurrent_initialization_rechecks_configuration(monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()
    pool = AsyncMock()

    async def create_pool(**kwargs):
        entered.set()
        await release.wait()
        return pool

    monkeypatch.setattr(connection.asyncpg, "create_pool", create_pool)
    first = asyncio.create_task(connection.init_pool("postgresql://user:first@localhost/db"))
    await entered.wait()
    second = asyncio.create_task(connection.init_pool("postgresql://user:second@localhost/db"))
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert results[0] is pool
    assert isinstance(results[1], RuntimeError)


@pytest.mark.asyncio
async def test_reinitialization_rejects_changed_timeout(monkeypatch):
    monkeypatch.setattr(connection.asyncpg, "create_pool", AsyncMock(return_value=AsyncMock()))
    await connection.init_pool("postgresql://localhost/db", acquire_timeout_seconds=2)
    with pytest.raises(RuntimeError, match="configuration"):
        await connection.init_pool("postgresql://localhost/db", acquire_timeout_seconds=30)


@pytest.mark.asyncio
async def test_pool_construction_failure_has_safe_error_and_no_published_pool(monkeypatch, caplog):
    monkeypatch.setattr(
        connection.asyncpg,
        "create_pool",
        AsyncMock(side_effect=RuntimeError("SYNTHETIC_PROVIDER_SECRET")),
    )
    with pytest.raises(RuntimeError) as error:
        await connection.init_pool("postgresql://localhost/db")
    assert "SYNTHETIC" not in str(error.value) + caplog.text
    assert error.value.__suppress_context__
    with pytest.raises(RuntimeError, match="not initialised"):
        await connection.get_pool()


@pytest.mark.asyncio
async def test_failed_timeout_install_closes_unpublished_pool(monkeypatch):
    pool = AsyncMock()
    monkeypatch.setattr(connection.asyncpg, "create_pool", AsyncMock(return_value=pool))

    def fail_install(pool, timeout):
        raise TypeError("unsupported pool layout")

    monkeypatch.setattr(connection, "_install_default_acquire_timeout", fail_install)
    with pytest.raises(RuntimeError, match="bounded"):
        await connection.init_pool("postgresql://localhost/db")
    pool.close.assert_awaited_once()
    with pytest.raises(RuntimeError, match="not initialised"):
        await connection.get_pool()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout_value", [0, -1, float("nan"), float("inf"), True])
async def test_invalid_timeout_never_opens_pool(monkeypatch, timeout_value):
    create = AsyncMock(return_value=AsyncMock())
    monkeypatch.setattr(connection.asyncpg, "create_pool", create)
    with pytest.raises(ValueError, match="timeout"):
        await connection.init_pool(
            "postgresql://localhost/db", acquire_timeout_seconds=timeout_value
        )
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_install_cleanup_preserves_cancellation(monkeypatch):
    """Cancellation during cleanup must terminate initialization as cancellation."""
    pool = AsyncMock()
    pool.close.side_effect = asyncio.CancelledError
    monkeypatch.setattr(connection.asyncpg, "create_pool", AsyncMock(return_value=pool))

    def fail_install(pool, timeout):
        raise TypeError("unsupported pool layout")

    monkeypatch.setattr(connection, "_install_default_acquire_timeout", fail_install)
    with pytest.raises(asyncio.CancelledError):
        await connection.init_pool("postgresql://localhost/db")
