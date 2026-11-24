"""Detection telemetry on the REST-polling leader dispatcher.

``LeaderBatchDispatcher`` is the polling twin of the on-chain dispatch
adapter: it is the single place where a polled leader batch enters the copy
path, so it owns the one permitted pre-submit emit — exactly one
``signal_received(source="polling", ...)`` per trade row, on both the v1 and
the legacy branch.

``services/trading/trade_monitor.py`` is deliberately untouched; it stays a pure
transport that already counts ``detection_signals_total{source="polling"}``.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from pmcopy.orchestrators.lifecycle.runtime_monitor import LeaderBatchDispatcher, run_leader_monitor
from pmcopy.services.telemetry import LeaderEvent, LeaderOutcome, TelemetryProducer, safe_ref
from tests.telemetry_helpers import raising_emitters, recording_emitters


LEADER = "0x" + "ab" * 20
TX_A = "0x" + "11" * 32
TX_B = "0x" + "22" * 32


def _row(tx_hash: str, *, key: str = "transactionHash") -> dict[str, object]:
    return {
        key: tx_hash,
        "side": "BUY",
        "asset": "123456",
        "conditionId": "0x" + "ef" * 32,
        "size": 10,
        "price": 0.5,
        "timestamp": 1_700_000_000,
    }


class _Executor:
    """Record every legacy fan-out call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, list[dict[str, object]]]] = []

    async def __call__(self, context, leader, trades, now_ts):
        self.calls.append((context.user_id, leader, trades))
        return []


class _RaisingExecutor:
    """Fail the legacy fan-out for one follower."""

    async def __call__(self, context, leader, trades, now_ts):
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_dispatcher_emits_one_polling_signal_per_trade() -> None:
    emitters, service = recording_emitters()
    executor = _Executor()
    dispatcher = LeaderBatchDispatcher(
        leader=LEADER,
        contexts=[SimpleNamespace(user_id="u1")],
        legacy_executor=executor,
        clock=lambda: 100.0,
        telemetry=emitters.detection,
    )

    failures = await dispatcher.dispatch([_row(TX_A), _row(TX_B, key="txHash")])

    assert failures == ()
    assert len(executor.calls) == 1
    events = service.payloads(LeaderEvent)
    assert len(events) == 2
    assert {event.outcome for event in events} == {LeaderOutcome.RECEIVED}
    assert {event.source for event in events} == {"polling"}
    assert {event.leader_ref for event in events} == {safe_ref(LEADER)}
    assert [call.correlation_id for call in service.calls] == [safe_ref(TX_A), safe_ref(TX_B)]
    assert {call.producer for call in service.calls} == {TelemetryProducer.DETECTION}


@pytest.mark.asyncio
async def test_dispatcher_emits_on_the_v1_branch_too() -> None:
    emitters, service = recording_emitters()
    seen: list[tuple[str, list[dict[str, object]]]] = []

    async def v1_sink(leader: str, trades: list[dict[str, object]]) -> int:
        seen.append((leader, trades))
        return len(trades)

    dispatcher = LeaderBatchDispatcher(
        leader=LEADER,
        contexts=[],
        legacy_executor=_Executor(),
        v1_sink=v1_sink,
        telemetry=emitters.detection,
    )

    await dispatcher.dispatch([_row(TX_A)])

    assert len(seen) == 1
    assert len(service.payloads(LeaderEvent)) == 1


@pytest.mark.asyncio
async def test_dispatcher_without_telemetry_is_unchanged() -> None:
    executor = _Executor()
    dispatcher = LeaderBatchDispatcher(
        leader=LEADER,
        contexts=[SimpleNamespace(user_id="u1")],
        legacy_executor=executor,
        clock=lambda: 100.0,
    )

    failures = await dispatcher.dispatch([_row(TX_A)])

    assert failures == ()
    assert len(executor.calls) == 1


@pytest.mark.asyncio
async def test_raising_telemetry_backend_never_stops_polling_dispatch() -> None:
    emitters, service = raising_emitters()
    executor = _Executor()
    dispatcher = LeaderBatchDispatcher(
        leader=LEADER,
        contexts=[SimpleNamespace(user_id="u1")],
        legacy_executor=executor,
        clock=lambda: 100.0,
        telemetry=emitters.detection,
    )

    failures = await dispatcher.dispatch([_row(TX_A)])

    assert failures == ()
    assert len(executor.calls) == 1
    assert service.calls == 1


@pytest.mark.asyncio
async def test_dispatcher_failures_are_still_reported_with_telemetry() -> None:
    """Telemetry must not swallow the follower-specific legacy failure."""
    emitters, _service = recording_emitters()
    dispatcher = LeaderBatchDispatcher(
        leader=LEADER,
        contexts=[SimpleNamespace(user_id="u1")],
        legacy_executor=_RaisingExecutor(),
        clock=lambda: 100.0,
        telemetry=emitters.detection,
    )

    failures = await dispatcher.dispatch([_row(TX_A)])

    assert len(failures) == 1
    assert isinstance(failures[0].error, RuntimeError)


@pytest.mark.asyncio
async def test_run_leader_monitor_forwards_the_detection_emitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The composition root's emitter must reach the dispatcher."""
    emitters, service = recording_emitters()

    async def v1_sink(_leader: str, rows: list[dict[str, object]]) -> int:
        return len(rows)

    async def fake_trade_monitor(_params, callback, **_kwargs) -> None:
        await callback([_row(TX_A)])

    monkeypatch.setattr("pmcopy.orchestrators.lifecycle.runtime_monitor.trade_monitor", fake_trade_monitor)

    await run_leader_monitor(
        LEADER,
        [],
        SimpleNamespace(),
        SimpleNamespace(detection=SimpleNamespace(trade_queue_max_batches=1)),
        fetch_interval=0.1,
        v1_sink=v1_sink,
        telemetry=emitters.detection,
    )

    assert len(service.payloads(LeaderEvent)) == 1


@pytest.mark.asyncio
async def test_mixed_case_roster_leader_publishes_the_lowercase_reference() -> None:
    """The polling roster keeps the operator's spelling; the stream must not.

    ``LeaderBatchDispatcher`` passes the roster wallet through verbatim, so the
    case-fold has to happen inside the emitter — otherwise a checksummed roster
    entry and the on-chain dispatcher's lower-cased wallet publish the same
    leader under two references.
    """
    from pmcopy.services.telemetry import leader_ref, safe_ref

    checksummed = "0x" + "AB" * 20
    assert checksummed != checksummed.lower()
    # Precondition for the bug this pins: the raw contract accepts both.
    assert safe_ref(checksummed) != safe_ref(checksummed.lower())

    emitters, service = recording_emitters()
    dispatcher = LeaderBatchDispatcher(
        leader=checksummed,
        contexts=[SimpleNamespace(user_id="u1")],
        legacy_executor=_Executor(),
        clock=lambda: 100.0,
        telemetry=emitters.detection,
    )

    await dispatcher.dispatch([_row(TX_A)])

    [event] = service.payloads(LeaderEvent)
    assert event.leader_ref == leader_ref(checksummed) == checksummed.lower()


@pytest.mark.asyncio
async def test_polling_and_onchain_spellings_collapse_onto_one_leader() -> None:
    """Cross-source proof: both dispatch paths report one ``leader_ref``."""
    from pmcopy.services.telemetry import leader_ref

    emitters, service = recording_emitters()
    checksummed = "0x" + "AB" * 20

    polling = LeaderBatchDispatcher(
        leader=checksummed,
        contexts=[SimpleNamespace(user_id="u1")],
        legacy_executor=_Executor(),
        clock=lambda: 100.0,
        telemetry=emitters.detection,
    )
    await polling.dispatch([_row(TX_A)])
    # What the on-chain adapter passes: the lower-cased wallet.
    emitters.detection.signal_received("on_chain_pending", checksummed.lower(), TX_B)

    refs = {event.leader_ref for event in service.payloads(LeaderEvent)}
    assert refs == {leader_ref(checksummed)}
    # And the bounded per-leader counter table is keyed once, not twice.
    snapshot = emitters.detection.snapshot()
    assert list(snapshot.leaders) == [leader_ref(checksummed)]
    assert snapshot.leaders[leader_ref(checksummed)].signals_total == 2
