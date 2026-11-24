"""§9 bullet 13 — saturating one leader's bounded work queue has a
DELIBERATE observable effect (oldest batch coalesced with the newest, counter
incremented, depth gauge pinned at the bound) and does not unboundedly grow
memory or stall unrelated work on another leader's monitor.

The production object under test is ``orchestrators.runtime_monitor.
run_leader_monitor``: one bounded ``asyncio.Queue`` per leader, one worker
per leader, dispatching to that leader's follower contexts. The polling
transport (``services.tradeMonitor.trade_monitor``) is replaced by a
deterministic stub that hands back the batch callback, exactly the seam the
real polling adapter consumes.
"""

from __future__ import annotations

import asyncio

import pytest

from pmcopy.orchestrators.lifecycle.runtime_monitor import run_leader_monitor
from pmcopy.services.observability.health_server import ReadinessTracker
from pmcopy.services.observability.metrics import metrics

from tests.multicopier.conftest import LEADER_1, LEADER_2, make_config


class _TradeMonitorStub:
    """Capture each monitor's on_trade callback; never poll anything."""

    def __init__(self) -> None:
        self.callbacks_by_leader: dict[str, object] = {}

    async def __call__(self, settings, on_trade, **_kwargs):
        self.callbacks_by_leader[str(settings["leaderWallet"])] = on_trade
        await asyncio.Event().wait()  # park the monitor loop forever


def _batch(tx_hash: str) -> list[dict]:
    return [{"transactionHash": tx_hash, "side": "BUY", "asset": "token-1", "timestamp": 1}]


def _counter_value(name: str, labels: dict[str, str]) -> float:
    from pmcopy.services.observability.metrics import _metric_key

    return metrics.counters.get(_metric_key(name, labels), 0.0)


def _gauge_value(name: str, labels: dict[str, str]) -> float:
    from pmcopy.services.observability.metrics import _metric_key

    return metrics.gauges.get(_metric_key(name, labels), 0.0)


@pytest.mark.asyncio
async def test_saturating_leader_a_queue_coalesces_without_stalling_leader_b(
    monkeypatch,
):
    config = make_config(**{"detection": {"trade_queue_max_batches": 2}})
    stub = _TradeMonitorStub()
    monkeypatch.setattr("pmcopy.orchestrators.lifecycle.runtime_monitor.trade_monitor", stub)

    processed: list[tuple[str, str]] = []
    release_a = asyncio.Event()

    async def executor(context, leader_id, trades, now_ts):
        if leader_id == LEADER_1:
            # A's follower pipeline is deliberately slow (e.g. slow venue
            # read) — its worker stays busy so the queue fills.
            await release_a.wait()
        processed.extend((leader_id, trade["transactionHash"]) for trade in trades)
        return []

    # The monitor wires the real pipeline entry as its legacy executor at
    # module scope; replace it at that seam so determinism lives in one place.
    monkeypatch.setattr("pmcopy.orchestrators.lifecycle.runtime_monitor.execute_trade_pipeline", executor)

    class _Ctx:
        user_id = "copier-alpha"

    monitor_a = asyncio.create_task(
        run_leader_monitor(
            LEADER_1,
            [_Ctx()],
            ReadinessTracker(),
            config,
            fetch_interval=60,
        )
    )
    monitor_b = asyncio.create_task(
        run_leader_monitor(
            LEADER_2,
            [_Ctx()],
            ReadinessTracker(),
            config,
            fetch_interval=60,
        )
    )
    try:
        # Wait until both monitors have handed their batch callbacks over.
        for _ in range(100):
            if len(stub.callbacks_by_leader) == 2:
                break
            await asyncio.sleep(0)
        on_trade_a = stub.callbacks_by_leader[LEADER_1]
        on_trade_b = stub.callbacks_by_leader[LEADER_2]

        # Saturate A: the worker is blocked on the first batch, so the next
        # pushes overflow the bounded queue (maxsize=2).
        await on_trade_a(_batch("tx-a-0"))
        await asyncio.sleep(0)
        await on_trade_a(_batch("tx-a-1"))
        await on_trade_a(_batch("tx-a-2"))
        await on_trade_a(_batch("tx-a-3"))
        await asyncio.sleep(0.05)

        coalesced_after_saturation = _counter_value(
            "trade_queue_batches_coalesced_total", {"leader_id": LEADER_1}
        )
        depth_a = _gauge_value("trade_queue_depth", {"leader_id": LEADER_1})
        assert coalesced_after_saturation >= 1, "saturation must be observable"
        # The queue never grew beyond its bound: at most maxsize batches
        # buffered (one may have been coalesced out).
        assert depth_a <= 2

        # B's queue is a different bounded queue with a free worker: B's
        # batches process promptly while A is still blocked.
        await on_trade_b(_batch("tx-b-1"))
        for _ in range(200):
            if processed:
                break
            await asyncio.sleep(0.005)
        assert processed == [(LEADER_2, "tx-b-1")]
        assert _counter_value("trade_queue_batches_coalesced_total", {"leader_id": LEADER_2}) == 0

        # Release A's worker: the coalesced backlog drains and the depth
        # gauge returns to its bounded steady state. Every pushed trade —
        # including the ones coalesced into a merged batch — is delivered:
        # money-related work is never silently dropped, only merged.
        release_a.set()
        expected_a = {f"tx-a-{i}" for i in range(4)}
        for _ in range(400):
            a_hashes = {tx for leader, tx in processed if leader == LEADER_1}
            if a_hashes == expected_a:
                break
            await asyncio.sleep(0.005)
        assert {tx for leader, tx in processed if leader == LEADER_1} == expected_a
        await asyncio.sleep(0.05)
        assert _gauge_value("trade_queue_depth", {"leader_id": LEADER_1}) <= 2
    finally:
        release_a.set()
        for task in (monitor_a, monitor_b):
            task.cancel()
        await asyncio.gather(monitor_a, monitor_b, return_exceptions=True)
