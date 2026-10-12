"""Subscription admission and incoming bytes are inseparable from observation."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from pmcopy.domain.grouped_detection import TraderDetectionPolicy
from pmcopy.domain.provider_budget import BudgetExhausted
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.services.grouped_detection import DetectionSubscription
from pmcopy.storage.detection_state import SqliteDetectionState
from tests.test_aethertrade_native_owner import OWNER, LogFixture


class SessionFixture:
    """Assemble a finite in-memory transport with real durable source state."""

    def __init__(self, path: Path, frames: list[object]) -> None:
        self.socket = Mock(
            send=AsyncMock(),
            recv=AsyncMock(side_effect=[*(json.dumps(frame) for frame in frames), EOFError()]),
            close=AsyncMock(),
        )
        self.connector = Mock(open=AsyncMock(return_value=self.socket))
        self.admission = Mock()
        self.observer = Mock(observe=AsyncMock(return_value="new"))
        self.recovery = Mock(recover=AsyncMock(return_value=15))
        self.raw, self.clock = Mock(), Mock()
        self.receipt = TransportReceipt(wall_timestamp_ms=123456, monotonic_ns=50, boot_id="boot")
        self.clock.capture.return_value = self.receipt
        self.state = SqliteDetectionState(path)
        self.state.configure((TraderDetectionPolicy(OWNER),))

    def build(self, **kwargs: object):
        """Construct only after the test imports the newly implemented boundary."""
        from pmcopy.clients.network.grouped_websocket import GroupedWebSocketSession

        return GroupedWebSocketSession(
            groups=(DetectionSubscription("alchemy", "mined", (OWNER,)),),
            connector=self.connector,
            admission=self.admission,
            observer=self.observer,
            recovery=self.recovery,
            raw=self.raw,
            clock=self.clock,
            state=self.state,
            **kwargs,
        )


def notification(log: dict[str, object]) -> dict[str, object]:
    """Return a real JSON-RPC log notification."""
    return {
        "jsonrpc": "2.0",
        "method": "eth_subscription",
        "params": {"subscription": "s1", "result": log},
    }


@pytest.mark.asyncio
async def test_budget_rejection_prevents_subscription_send(tmp_path: Path) -> None:
    """Even control messages require successful admission before reaching the socket."""
    fixture = SessionFixture(tmp_path / "state", [])
    fixture.admission.admit_rpc.side_effect = BudgetExhausted("quota")
    with pytest.raises(BudgetExhausted):
        await fixture.build().run()
    fixture.socket.send.assert_not_awaited()
    fixture.socket.close.assert_awaited_once()
    fixture.state.close()


@pytest.mark.asyncio
async def test_all_bytes_are_charged_and_original_arrival_precedes_recovery(tmp_path: Path) -> None:
    """Buffered notification clocks remain original; native callbacks see exact logs."""
    from pmcopy.clients.network.grouped_websocket import StreamUnavailable

    frames = [{"jsonrpc": "2.0", "id": 1, "result": "s1"}, notification(LogFixture.make())]
    fixture = SessionFixture(tmp_path / "state", frames)
    session = fixture.build()
    with pytest.raises(StreamUnavailable):
        await session.run()
    fixture.admission.admit_rpc.assert_called_once_with(
        "alchemy", "eth_subscribe", category="essential"
    )
    assert [c.args[1] for c in fixture.admission.charge_stream.call_args_list] == [
        len(json.dumps(frame).encode()) for frame in frames
    ]
    fixture.observer.observe.assert_awaited_once_with(
        LogFixture.make(), fixture.receipt, recovered=False
    )
    fixture.raw.record.assert_called_once_with(
        "alchemy", "mined", LogFixture.make(), fixture.receipt, recovered=False
    )
    request = json.loads(fixture.socket.send.await_args.args[0])
    assert request["params"][1]["topics"][2] == ["0x" + OWNER[2:].rjust(64, "0")]
    assert session.connected is False
    assert fixture.state.states()[0].effective_mode == "paused"
    fixture.state.close()


@pytest.mark.asyncio
async def test_delivered_budget_failure_is_visible_and_never_decoded(tmp_path: Path) -> None:
    """Delivered excess is charged/rejected before it can become a trade signal."""
    fixture = SessionFixture(tmp_path / "state", [{"jsonrpc": "2.0", "id": 1, "result": "s1"}])
    fixture.admission.charge_stream.side_effect = BudgetExhausted("stream quota")
    with pytest.raises(BudgetExhausted):
        await fixture.build().run()
    fixture.observer.observe.assert_not_awaited()
    fixture.state.close()


@pytest.mark.asyncio
async def test_malformed_or_unknown_subscription_stops_without_payload_leak(tmp_path: Path) -> None:
    """Unknown upstream data cannot bypass configured groups or leak in an error."""
    from pmcopy.clients.network.grouped_websocket import StreamUnavailable

    frames = [{"jsonrpc": "2.0", "id": 1, "result": "s1"}, {"error": "credential-secret"}]
    fixture = SessionFixture(tmp_path / "state", frames)
    with pytest.raises(StreamUnavailable) as exc:
        await fixture.build().run()
    assert "credential-secret" not in str(exc.value)
    fixture.observer.observe.assert_not_awaited()
    fixture.state.close()


@pytest.mark.asyncio
async def test_infura_cannot_open_unmetered_standing_logs(tmp_path: Path) -> None:
    """Infura is available only through bounded RPC recovery until block billing exists."""
    from pmcopy.clients.network.grouped_websocket import GroupedWebSocketSession

    fixture = SessionFixture(tmp_path / "state", [])
    with pytest.raises(ValueError, match="Alchemy"):
        GroupedWebSocketSession(
            groups=(DetectionSubscription("infura", "mined", (OWNER,)),),
            connector=fixture.connector,
            admission=fixture.admission,
            observer=fixture.observer,
            recovery=fixture.recovery,
            raw=fixture.raw,
            clock=fixture.clock,
            state=fixture.state,
        )
    fixture.connector.open.assert_not_awaited()
    fixture.state.close()


@pytest.mark.asyncio
async def test_malformed_log_is_recorded_before_decode_rejection(tmp_path: Path) -> None:
    """Raw evidence survives invalid financial coordinates."""
    from pmcopy.clients.network.grouped_websocket import StreamUnavailable

    malformed = LogFixture.make() | {"blockNumber": "bad"}
    fixture = SessionFixture(
        tmp_path / "state", [{"jsonrpc": "2.0", "id": 1, "result": "s1"}, notification(malformed)]
    )
    with pytest.raises(StreamUnavailable):
        await fixture.build().run()
    assert fixture.raw.record.call_args.args[2] == malformed
    fixture.observer.observe.assert_not_awaited()
    fixture.state.close()


@pytest.mark.asyncio
async def test_arrival_is_metered_and_recorded_while_recovery_is_blocked(tmp_path: Path) -> None:
    """Transport receipt and billing do not wait behind recovery RPC work."""
    import asyncio

    fixture = SessionFixture(tmp_path / "state", [])
    frames: asyncio.Queue[str] = asyncio.Queue()
    fixture.socket.recv.side_effect = frames.get
    recovery_started, release = asyncio.Event(), asyncio.Event()

    async def recover(_group):
        recovery_started.set()
        await release.wait()
        return 15

    fixture.recovery.recover.side_effect = recover
    task = asyncio.create_task(fixture.build().run())
    await frames.put(json.dumps({"jsonrpc": "2.0", "id": 1, "result": "s1"}))
    await asyncio.wait_for(recovery_started.wait(), 1)
    await frames.put(json.dumps(notification(LogFixture.make())))
    for _ in range(10):
        await asyncio.sleep(0)
    try:
        assert fixture.admission.charge_stream.call_count == 2
        fixture.raw.record.assert_called_once()
        fixture.observer.observe.assert_not_awaited()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        fixture.state.close()


@pytest.mark.asyncio
async def test_transport_close_error_cannot_replace_sanitized_failure(tmp_path: Path) -> None:
    """Credential-bearing transport errors never escape the session shutdown boundary."""
    from pmcopy.clients.network.grouped_websocket import StreamUnavailable

    fixture = SessionFixture(tmp_path / "state", [])
    fixture.socket.close.side_effect = RuntimeError("credential-secret")
    with pytest.raises(StreamUnavailable) as exc:
        await fixture.build().run()
    assert "credential-secret" not in str(exc.value)
    fixture.state.close()
