from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from orchestrators.runtime_monitor import (
    LeaderBatchDispatcher,
    _coalesce_trade_batches,
    run_leader_monitor,
)


def test_coalesce_preserves_trade_id_aliases_without_transaction_hash() -> None:
    first = [
        {
            "tradeID": "trade-a",
            "asset": "asset-1",
            "side": "BUY",
            "timestamp": 1,
        }
    ]
    second = [
        {
            "trade_id": "trade-b",
            "asset": "asset-1",
            "side": "BUY",
            "timestamp": 2,
        }
    ]

    combined = _coalesce_trade_batches(first, second)

    assert combined == [first[0], second[0]]


def test_coalesce_preserves_distinct_anonymous_trades() -> None:
    """Rows carrying neither a transaction hash nor a trade id are distinct.

    Coalescing runs only under backlog pressure, so collapsing two different
    anonymous fills onto one empty identity silently drops a leader trade the
    copy pipeline would otherwise mirror.
    """
    first = [
        {
            "asset": "asset-1",
            "side": "BUY",
            "size": "10",
            "price": "0.40",
            "timestamp": 1,
        }
    ]
    second = [
        {
            "asset": "asset-1",
            "side": "BUY",
            "size": "25",
            "price": "0.55",
            "timestamp": 2,
        }
    ]

    combined = _coalesce_trade_batches(first, second)

    assert combined == [first[0], second[0]]


def test_coalesce_dedupes_identical_anonymous_rows() -> None:
    """The same anonymous row seen in both batches still collapses to one."""
    row = {
        "asset": "asset-1",
        "side": "BUY",
        "size": "10",
        "price": "0.40",
        "timestamp": 1,
    }

    combined = _coalesce_trade_batches([dict(row)], [dict(row)])

    assert combined == [row]


def test_coalesce_still_dedupes_on_transaction_hash() -> None:
    """Keyed dedupe must stay intact: the later row wins for one identity."""
    first = [
        {
            "transactionHash": "0xabc",
            "asset": "asset-1",
            "side": "BUY",
            "size": "10",
            "timestamp": 1,
        }
    ]
    second = [
        {
            "transactionHash": "0xabc",
            "asset": "asset-1",
            "side": "BUY",
            "size": "11",
            "timestamp": 1,
        }
    ]

    combined = _coalesce_trade_batches(first, second)

    assert combined == [second[0]]


@pytest.mark.asyncio
async def test_v1_batch_sink_replaces_legacy_per_context_execution() -> None:
    """Use exactly one copy authority when the copy-task runtime is enabled."""

    v1_batches: list[tuple[str, list[dict[str, object]]]] = []
    legacy_calls: list[object] = []

    async def v1_sink(leader: str, rows: list[dict[str, object]]) -> int:
        v1_batches.append((leader, rows))
        return len(rows)

    async def legacy(
        context: object,
        leader: str,
        rows: list[dict[str, object]],
        now: float,
    ) -> None:
        legacy_calls.append((context, leader, rows, now))

    dispatcher = LeaderBatchDispatcher(
        leader="0xleader",
        contexts=[object(), object()],
        legacy_executor=legacy,
        v1_sink=v1_sink,
        clock=lambda: 123.0,
    )
    rows: list[dict[str, object]] = [{"id": "trade-a"}]

    failures = await dispatcher.dispatch(rows)

    assert failures == ()
    assert v1_batches == [("0xleader", rows)]
    assert legacy_calls == []


@pytest.mark.asyncio
async def test_legacy_dispatch_remains_available_only_when_v1_is_disabled() -> None:
    """Preserve the legacy path for explicitly disabled copy tasks."""

    contexts = [object(), object()]
    legacy_calls: list[tuple[object, str, float]] = []

    async def legacy(
        context: object,
        leader: str,
        rows: list[dict[str, object]],
        now: float,
    ) -> None:
        del rows
        legacy_calls.append((context, leader, now))

    dispatcher = LeaderBatchDispatcher(
        leader="0xleader",
        contexts=contexts,
        legacy_executor=legacy,
        clock=lambda: 456.0,
    )

    failures = await dispatcher.dispatch([{"id": "trade-a"}])

    assert failures == ()
    assert legacy_calls == [
        (contexts[0], "0xleader", 456.0),
        (contexts[1], "0xleader", 456.0),
    ]


@pytest.mark.asyncio
async def test_v1_source_callback_acknowledges_only_after_atomic_batch_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the source cursor behind a batch until the v1 queue accepts it."""

    sink_started = asyncio.Event()
    release_sink = asyncio.Event()
    callback_returned = asyncio.Event()
    finish_monitor = asyncio.Event()
    rows = [{"id": "trade-a"}, {"id": "trade-b"}]

    async def v1_sink(leader: str, batch: list[dict[str, object]]) -> int:
        assert leader == "0xleader"
        assert batch == rows
        sink_started.set()
        await release_sink.wait()
        return len(batch)

    async def fake_trade_monitor(
        _params: object,
        callback: object,
        **_kwargs: object,
    ) -> None:
        assert _kwargs["aggregate_signals"] is False
        await callback(rows)
        callback_returned.set()
        await finish_monitor.wait()

    monkeypatch.setattr("orchestrators.runtime_monitor.trade_monitor", fake_trade_monitor)
    runtime = asyncio.create_task(
        run_leader_monitor(
            "0xleader",
            [SimpleNamespace(user_id="user-a")],
            SimpleNamespace(),
            SimpleNamespace(detection=SimpleNamespace(trade_queue_max_batches=1)),
            fetch_interval=0.1,
            v1_sink=v1_sink,
        )
    )
    try:
        await asyncio.wait_for(sink_started.wait(), timeout=0.5)
        assert callback_returned.is_set() is False
        release_sink.set()
        await asyncio.wait_for(callback_returned.wait(), timeout=0.5)
    finally:
        finish_monitor.set()
        await asyncio.wait_for(runtime, timeout=0.5)


@pytest.mark.asyncio
async def test_v1_monitor_does_not_require_legacy_follower_contexts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A durable v1 task leader can be monitored without legacy fan-out state."""

    observed: list[tuple[str, list[dict[str, object]]]] = []

    async def v1_sink(leader: str, rows: list[dict[str, object]]) -> int:
        observed.append((leader, rows))
        return len(rows)

    async def fake_trade_monitor(
        _params: object,
        callback: object,
        **kwargs: object,
    ) -> None:
        assert kwargs["aggregate_signals"] is False
        await callback([{"id": "trade-a"}])

    monkeypatch.setattr("orchestrators.runtime_monitor.trade_monitor", fake_trade_monitor)

    await run_leader_monitor(
        "0xleader",
        [],
        SimpleNamespace(),
        SimpleNamespace(detection=SimpleNamespace(trade_queue_max_batches=1)),
        fetch_interval=0.1,
        v1_sink=v1_sink,
    )

    assert observed == [("0xleader", [{"id": "trade-a"}])]
