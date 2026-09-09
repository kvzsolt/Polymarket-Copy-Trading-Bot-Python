"""Task state changes are checked at the last safe boundary before venue POST."""

from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from interfaces.clob_gateway import ClobGateway
from services.copy_tasks.domain import CopyTaskStatus
from services.copy_tasks.entry_permission import TaskEntryPermissionService
from services.copy_tasks.service import InMemoryCopyTaskRepository
from services.executionService import ExecutionService
from tests.test_execution_fak_repost_ambiguity import _make_config, _mock_mds, _order_repo, _req
from tests.v1.test_copy_task_event_routing import _NOW, task


class Clock:
    def now(self):
        """Return a deterministic schedule time."""
        return _NOW


@pytest.mark.asyncio
async def test_task_paused_after_order_preparation_never_reaches_venue():
    """A pause during preparatory I/O wins before the first POST is sent."""
    tasks = InMemoryCopyTaskRepository()
    active = replace(task(task_id="task-a"), follower_account="u1")
    await tasks.create(active)
    permission = TaskEntryPermissionService(tasks, follower_accounts=("u1",), clock=Clock())
    orders = _order_repo()

    async def pause_during_prepare(record):
        existing = await tasks.get(active.task_id)
        if existing.status is CopyTaskStatus.ACTIVE:
            await tasks.save(
                replace(existing, status=CopyTaskStatus.PAUSED, version=2), expected_version=1
            )

    orders.save_submission.side_effect = pause_during_prepare
    gateway = MagicMock(spec=ClobGateway)
    service = ExecutionService(
        AsyncMock(),
        AsyncMock(get_fee_bps=AsyncMock(return_value=Decimal(0))),
        orders,
        config=_make_config(),
        clob_gateway=gateway,
        market_data_service=_mock_mds(),
        task_entry_permission=permission,
    )
    request = replace(
        _req(),
        leader_id=active.leader_wallet,
        recovery_metadata={
            "task_id": "task-a",
            "task_version": 1,
        },
    )
    result = await service.execute(request)
    assert result.status == "task_paused"
    gateway.submit_marketable_order.assert_not_awaited()
