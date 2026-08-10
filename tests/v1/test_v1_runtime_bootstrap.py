"""Composition-root wiring for the v1 copy-task runtime.

``start_v1_copy_task_runtime`` is the single production seam that turns the
typed v1 configuration into a running (or absent) copy-task runtime. Disabled
must mean no repositories, no background tasks, and zero pool access; enabled
must restore durable task/budget state before any worker accepts events.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from config.v1_runtime import V1RuntimeConfig
from orchestrators.v1_runtime_bootstrap import (
    CopyTaskV1RuntimeHandle,
    start_v1_copy_task_runtime,
)
from config.app_config import load_config

_ADDRESS = "0x" + "1" * 40


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {
            "funder_address": _ADDRESS,
            "polygon_rpc_url": "https://rpc.invalid",
        },
        "network": {"rpc_url": "https://rpc.invalid"},
    }


class _FakeAcquire:
    def __init__(self, connection: "_FakeConnection") -> None:
        self._connection = connection

    async def __aenter__(self) -> "_FakeConnection":
        return self._connection

    async def __aexit__(self, *_exc_info: object) -> bool:
        return False


class _FakeConnection:
    """Connection stub for the startup budget sweep."""

    def __init__(self, journal: list[str]) -> None:
        self._journal = journal

    def transaction(self) -> _FakeAcquire:
        return _FakeAcquire(self)

    async def fetch(self, query: str, *args: object) -> list[object]:
        self._journal.append(query)
        return []

    async def fetchval(self, query: str, *args: object) -> object | None:
        self._journal.append(query)
        return None

    async def fetchrow(self, query: str, *args: object) -> object | None:
        self._journal.append(query)
        return None

    async def execute(self, query: str, *args: object) -> str:
        self._journal.append(query)
        return "UPDATE 0"


class _FakePool:
    """Narrow pool stub: the enabled path only lists tasks and sweeps budgets."""

    def __init__(self) -> None:
        self.fetch_queries: list[str] = []
        self.execute_queries: list[str] = []

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(_FakeConnection(self.execute_queries))

    async def fetch(self, query: str, *args: object) -> list[object]:
        self.fetch_queries.append(query)
        return []

    async def fetchval(self, query: str, *args: object) -> object | None:
        self.fetch_queries.append(query)
        return None

    async def fetchrow(self, query: str, *args: object) -> object | None:
        self.fetch_queries.append(query)
        return None

    async def execute(self, query: str, *args: object) -> str:
        self.execute_queries.append(query)
        return "DELETE 0"


def _app_config():
    return load_config(_overrides())


def _v1_config(*, enabled: bool) -> V1RuntimeConfig:
    return V1RuntimeConfig.model_validate({"copy_tasks": {"enabled": enabled}})


@pytest.mark.asyncio
async def test_disabled_config_builds_nothing_and_never_touches_the_pool() -> None:
    pool = MagicMock()

    handle = await start_v1_copy_task_runtime(
        pool=pool,
        config=_app_config(),
        v1_config=_v1_config(enabled=False),
    )

    assert handle is None
    assert pool.mock_calls == []


@pytest.mark.asyncio
async def test_enabled_config_restores_durable_state_and_starts_workers() -> None:
    pool = _FakePool()
    fixed_now = datetime(2026, 8, 11, tzinfo=timezone.utc)

    handle = await start_v1_copy_task_runtime(
        pool=pool,
        config=_app_config(),
        v1_config=_v1_config(enabled=True),
        clock=lambda: fixed_now,
    )

    assert isinstance(handle, CopyTaskV1RuntimeHandle)
    assert handle.restored_task_count == 0
    assert handle.swept_reservation_count == 0
    # Restoration touched durable state (task list + expired-budget sweep).
    assert any("copy_tasks" in query for query in pool.fetch_queries)
    bundle = handle.bundle
    assert bundle.enabled is True
    assert bundle.coordinator is not None
    assert bundle.lifecycle is not None
    assert bundle.workers is not None
    assert bundle.price_cache is not None
    # Workers accept events only because start() already ran.
    await bundle.workers.submit("0xleader", object())
    await handle.close()


@pytest.mark.asyncio
async def test_close_is_idempotent() -> None:
    pool = _FakePool()
    handle = await start_v1_copy_task_runtime(
        pool=pool,
        config=_app_config(),
        v1_config=_v1_config(enabled=True),
        clock=lambda: datetime(2026, 8, 11, tzinfo=timezone.utc),
    )
    assert handle is not None
    await handle.close()
    await handle.close()
