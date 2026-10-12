"""Shared leader observation cannot serialize unrelated follower I/O."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pmcopy.orchestrators.v1_runtime_bootstrap import start_v1_copy_task_runtime
from pmcopy.services.copy_tasks.service import InMemoryCopyTaskRepository
from tests.v1.test_copy_task_event_routing import _LEADER, _NOW, task
from tests.v1.test_v1_runtime_bootstrap import _FakePool, _app_config, _copy_tasks_config


class BlockingAccountSnapshots:
    """Block one account's I/O while recording another account's progress."""

    def __init__(self) -> None:
        self.slow_entered = asyncio.Event()
        self.release_slow = asyncio.Event()
        self.fast_transactions: list[str] = []
        self.slow_transactions: list[str] = []

    async def snapshot_for(self, task, event):
        """Finish with an explicit unavailable snapshot after observing ordering."""
        transaction = event.trade.transaction_hash
        if task.follower_account == "account-a":
            self.slow_entered.set()
            await self.release_slow.wait()
            self.slow_transactions.append(transaction)
        else:
            self.fast_transactions.append(transaction)
        raise LookupError("Synthetic snapshot unavailable")


@pytest.mark.asyncio
async def test_fast_account_processes_later_source_events_while_other_account_waits() -> None:
    repository = InMemoryCopyTaskRepository()
    await repository.create(replace(task(task_id="a-slow"), follower_account="account-a"))
    await repository.create(replace(task(task_id="b-fast"), follower_account="account-b"))
    snapshots = BlockingAccountSnapshots()
    contexts = tuple(
        SimpleNamespace(user_id=account, wallet_address=account, extra={"leader_wallet": _LEADER})
        for account in ("account-a", "account-b")
    )
    with (
        patch(
            "pmcopy.orchestrators.v1_runtime_bootstrap.PostgresCopyTaskRepository", return_value=repository
        ),
        patch(
            "pmcopy.orchestrators.v1_runtime_bootstrap.TradingContextTaskSnapshotProvider",
            return_value=snapshots,
        ),
    ):
        handle = await start_v1_copy_task_runtime(
            pool=_FakePool(),
            config=_app_config(),
            copy_tasks_config=_copy_tasks_config(enabled=True),
            contexts=contexts,
            clock=lambda: _NOW,
        )
    assert handle is not None
    try:
        accepted = await handle.submit_raw_batch(
            _LEADER,
            [
                {
                    "conditionId": "condition-a",
                    "asset": "token-a",
                    "side": "BUY",
                    "size": "10",
                    "price": "0.4",
                    "usdcSize": "4",
                    "transactionHash": transaction,
                    "timestamp": int(_NOW.timestamp()),
                }
                for transaction in ("tx-first", "tx-second")
            ],
        )
        assert accepted == 2
        await snapshots.slow_entered.wait()
        assert snapshots.fast_transactions == ["tx-first", "tx-second"]
        assert snapshots.slow_transactions == []
        assert handle.worker_count == 3
        assert handle.queue_depth == 1
    finally:
        snapshots.release_slow.set()
        await handle.close()
    assert snapshots.slow_transactions == ["tx-first", "tx-second"]
    assert handle.queue_depth == 0
