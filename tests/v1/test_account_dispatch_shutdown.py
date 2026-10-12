"""Two dispatch stages share one shutdown bound and deterministic abort path."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.orchestrators.v1_runtime_bootstrap import CopyTaskV1RuntimeHandle


def handle_for(producer, accounts):
    return CopyTaskV1RuntimeHandle(
        bundle=SimpleNamespace(workers=producer),
        restored_task_count=0,
        swept_reservation_count=0,
        restored_lot_count=0,
        ingress=None,
        snapshot_provider=object(),
        execution_adapter=object(),
        quote_sink=None,
        account_dispatcher=accounts,
    )


@pytest.mark.asyncio
async def test_cancelled_producer_drain_aborts_accounts_without_another_drain() -> None:
    producer = SimpleNamespace(
        close=AsyncMock(side_effect=asyncio.CancelledError), abort=AsyncMock()
    )
    accounts = SimpleNamespace(
        close=AsyncMock(side_effect=AssertionError("Do not start a new drain after cancellation")),
        abort=AsyncMock(),
    )
    with pytest.raises(asyncio.CancelledError):
        await handle_for(producer, accounts).close()
    accounts.close.assert_not_awaited()
    accounts.abort.assert_awaited_once()
    producer.abort.assert_awaited_once()


@pytest.mark.asyncio
async def test_one_total_deadline_aborts_both_stages(monkeypatch) -> None:
    bounds = []

    class ExpiredDeadline:
        """Expire at entry without relying on elapsed test time."""

        async def __aenter__(self):
            raise TimeoutError

        async def __aexit__(self, *_args):
            return False

    def deadline(seconds):
        bounds.append(seconds)
        return ExpiredDeadline()

    producer = SimpleNamespace(close=AsyncMock(), abort=AsyncMock())
    accounts = SimpleNamespace(close=AsyncMock(), abort=AsyncMock())
    monkeypatch.setattr(asyncio, "timeout", deadline)
    await handle_for(producer, accounts).close()
    assert bounds == [30.0]
    producer.close.assert_not_awaited()
    accounts.close.assert_not_awaited()
    producer.abort.assert_awaited_once()
    accounts.abort.assert_awaited_once()
