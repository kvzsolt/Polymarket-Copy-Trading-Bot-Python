"""``_shutdown_runtime`` must honour the asyncpg pool shutdown contract.

``storage/postgres/connection.py`` documents ``shutdown_pool()`` as *the* app
shutdown path, but ``_shutdown_runtime`` never called it — leaking the pool on
exit. The call must run, be idempotent, and be warning-only on failure.
"""

from __future__ import annotations

import pytest

import app
import storage.postgres.connection as connection

pytestmark = pytest.mark.asyncio


async def test_shutdown_runtime_closes_pool(monkeypatch):
    calls = {"n": 0}

    async def _spy() -> None:
        calls["n"] += 1

    monkeypatch.setattr(connection, "shutdown_pool", _spy)

    await app._shutdown_runtime(
        monitor_tasks=[],
        supervised=[],
        market_data_runtimes={},
        user_data_runtimes=[],
        metrics_runner=None,
    )

    assert calls["n"] == 1


async def test_shutdown_runtime_pool_failure_is_warning_only(monkeypatch):
    async def _raise() -> None:
        raise RuntimeError("pool gone")

    monkeypatch.setattr(connection, "shutdown_pool", _raise)

    # Must not propagate — a teardown hiccup should never mask the real
    # shutdown reason.
    await app._shutdown_runtime(
        monitor_tasks=[],
        supervised=[],
        market_data_runtimes={},
        user_data_runtimes=[],
        metrics_runner=None,
    )


async def test_close_polymarket_clients_is_deduplicated_and_warning_only():
    from unittest.mock import AsyncMock

    healthy = AsyncMock()
    failing = AsyncMock()
    failing.close.side_effect = RuntimeError("transport already gone")

    await app._close_polymarket_clients([healthy, failing, healthy])

    healthy.close.assert_awaited_once()
    failing.close.assert_awaited_once()
