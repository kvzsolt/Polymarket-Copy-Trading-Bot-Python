"""Composition-root wiring for the v1 copy-task runtime.

``start_v1_copy_task_runtime`` is the single production seam that turns the
typed v1 configuration into a running (or absent) copy-task runtime. Disabled
must mean no repositories, no background tasks, and zero pool access; enabled
must restore durable task/budget state before any worker accepts events.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pmcopy.config.v1_runtime import CopyTasksRuntimeConfig
from pmcopy.orchestrators.v1_runtime_bootstrap import (
    CopyTaskV1RuntimeHandle,
    start_v1_copy_task_runtime,
)
from pmcopy.config.app_config import load_config

_ADDRESS = "0x" + "1" * 40


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {
            "funder_address": _ADDRESS,
            "polygon_rpc_url": "https://rpc.invalid",
        },
        "polymarket": {"rpc_url": "https://rpc.invalid"},
        "execution": {"trading_mode": "paper"},
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


class _BalanceReader:
    """Fail if a no-task fixture unexpectedly asks for follower cash."""

    async def read(self, wallet: str) -> Decimal | None:
        """Return a deterministic unused balance."""

        del wallet
        return Decimal("100")


class _PausedBuyScopeGuard:
    """Pause every BUY while recording the ingress scope."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def is_buy_paused(self, leader_wallet: str, token_id: str) -> bool:
        """Record one query and return a paused decision."""

        self.calls.append((leader_wallet, token_id))
        return True


def _app_config():
    return load_config(_overrides())


def _copy_tasks_config(*, enabled: bool) -> CopyTasksRuntimeConfig:
    return CopyTasksRuntimeConfig.model_validate({"enabled": enabled})


@pytest.mark.asyncio
async def test_disabled_config_builds_nothing_and_never_touches_the_pool() -> None:
    pool = MagicMock()

    handle = await start_v1_copy_task_runtime(
        pool=pool,
        config=_app_config(),
        copy_tasks_config=_copy_tasks_config(enabled=False),
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
        copy_tasks_config=_copy_tasks_config(enabled=True),
        contexts=[],
        balance_reader=_BalanceReader(),
        clock=lambda: fixed_now,
    )

    assert isinstance(handle, CopyTaskV1RuntimeHandle)
    assert handle.restored_task_count == 0
    assert handle.restored_lot_count == 0
    assert handle.swept_reservation_count == 0
    # Restoration touched durable state (task list + expired-budget sweep).
    assert any("copy_tasks" in query for query in pool.fetch_queries)
    bundle = handle.bundle
    assert bundle.enabled is True
    assert bundle.coordinator is not None
    assert bundle.lifecycle is not None
    assert bundle.risk_lots is not None
    assert bundle.workers is not None
    assert bundle.price_cache is not None
    assert handle.quote_sink is not None
    assert handle.worker_count == 0
    assert handle.queue_depth == 0
    assert any("copy_task_risk_lots" in query for query in pool.execute_queries)
    # Both detection sources can reach the bound processor; an empty durable
    # task set produces no money-path side effects.
    accepted = await handle.submit_raw_batch(
        _ADDRESS,
        [
            {
                "conditionId": "condition-a",
                "asset": "token-a",
                "side": "BUY",
                "size": "10",
                "price": "0.4",
                "usdcSize": "4",
                "transactionHash": "0xabc",
                "timestamp": 1786459200,
            }
        ],
    )
    assert accepted == 1
    assert handle.worker_count == 1
    assert handle.queue_depth >= 0
    await handle.close()
    assert handle.worker_count == 0
    assert handle.queue_depth == 0


@pytest.mark.asyncio
async def test_enabled_runtime_threads_buy_scope_guard_into_polling_ingress() -> None:
    """The production runtime cannot bypass an emergency SELL scope pause."""

    pool = _FakePool()
    guard = _PausedBuyScopeGuard()
    handle = await start_v1_copy_task_runtime(
        pool=pool,
        config=_app_config(),
        copy_tasks_config=_copy_tasks_config(enabled=True),
        contexts=[],
        balance_reader=_BalanceReader(),
        clock=lambda: datetime(2026, 8, 11, tzinfo=timezone.utc),
        buy_scope_guard=guard,
    )
    assert handle is not None

    accepted = await handle.submit_raw_batch(
        _ADDRESS,
        [
            {
                "conditionId": "condition-a",
                "asset": "token-a",
                "side": "BUY",
                "size": "10",
                "price": "0.4",
                "usdcSize": "4",
                "transactionHash": "0xpaused",
                "timestamp": 1786459200,
            }
        ],
    )

    assert accepted == 0
    assert guard.calls == [(_ADDRESS, "token-a")]
    assert handle.worker_count == 0
    await handle.close()


@pytest.mark.asyncio
async def test_close_is_idempotent() -> None:
    pool = _FakePool()
    handle = await start_v1_copy_task_runtime(
        pool=pool,
        config=_app_config(),
        copy_tasks_config=_copy_tasks_config(enabled=True),
        contexts=[],
        balance_reader=_BalanceReader(),
        clock=lambda: datetime(2026, 8, 11, tzinfo=timezone.utc),
    )
    assert handle is not None
    await handle.close()
    await handle.close()


@pytest.mark.asyncio
async def test_lifecycle_binds_before_workers_are_explicitly_started() -> None:
    """Make startup reconciliation see task settlement before event ingress."""

    pool = _FakePool()
    existing_reconciler = SimpleNamespace(task_budget_settlement=None)
    context = SimpleNamespace(
        user_id="user-a",
        wallet_address="0xfollower",
        extra={
            "leader_wallet": _ADDRESS,
            "_reconciliation_service": existing_reconciler,
        },
    )
    handle = await start_v1_copy_task_runtime(
        pool=pool,
        config=_app_config(),
        copy_tasks_config=_copy_tasks_config(enabled=True),
        contexts=[context],
        balance_reader=_BalanceReader(),
        clock=lambda: datetime(2026, 8, 11, tzinfo=timezone.utc),
        start_workers=False,
    )
    assert handle is not None
    assert context.extra["task_budget_settlement"] is existing_reconciler.task_budget_settlement
    assert context.extra["task_budget_settlement"] is not None

    with pytest.raises(RuntimeError, match="not started"):
        await handle.submit_raw_batch(
            _ADDRESS,
            [
                {
                    "conditionId": "condition-a",
                    "asset": "token-a",
                    "side": "BUY",
                    "size": "10",
                    "price": "0.4",
                    "usdcSize": "4",
                    "transactionHash": "0xabc",
                    "timestamp": 1786459200,
                }
            ],
        )

    await handle.start()
    assert await handle.submit_raw_batch(_ADDRESS, []) == 0
    await handle.close()


def test_bootstrap_contains_no_unrouted_event_drop_handler() -> None:
    """Prevent regression to a runtime that logs and drops every task event."""

    source = Path("src/pmcopy/orchestrators/v1_runtime_bootstrap.py").read_text(encoding="utf-8")
    assert "_unrouted_leader_event_handler" not in source
    assert "leader_event_unrouted" not in source


@pytest.mark.asyncio
async def test_handle_reports_zero_worker_state_when_workers_are_disabled() -> None:
    """Keep the runtime query port valid when worker processing is disabled."""

    pool = _FakePool()
    config = CopyTasksRuntimeConfig.model_validate(
        {
            "enabled": True,
            "workers": {"enabled": False},
        }
    )

    handle = await start_v1_copy_task_runtime(
        pool=pool,
        config=_app_config(),
        copy_tasks_config=config,
        contexts=[],
        balance_reader=_BalanceReader(),
        clock=lambda: datetime(2026, 8, 11, tzinfo=timezone.utc),
    )

    assert handle is not None
    assert handle.worker_count == 0
    assert handle.queue_depth == 0
    await handle.close()
