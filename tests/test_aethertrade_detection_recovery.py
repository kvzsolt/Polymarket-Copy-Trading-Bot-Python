"""Reconnect reads are bounded, admitted, reorg-aware, and evidence-only."""

from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from pmcopy.services.grouped_detection import DetectionSubscription
from pmcopy.storage.detection_state import SqliteDetectionState
from tests.test_aethertrade_native_owner import OWNER, LogFixture
from pmcopy.services.onchain_detection.native_owner import NativeOwnerLogDecoder
from pmcopy.domain.source_time import TransportReceipt


@pytest.mark.asyncio
async def test_recovery_uses_chunks_and_marks_every_result_historical(tmp_path: Path) -> None:
    """A known gap is recovered inclusively without periodic polling or fresh entry."""
    from pmcopy.services.onchain_detection.bounded_recovery import BoundedLogRecovery

    state = SqliteDetectionState(tmp_path / "state.sqlite3")
    group = DetectionSubscription("alchemy", "mined", (OWNER,))
    gateway, observer, raw, clock = Mock(), Mock(), Mock(), Mock()
    gateway.call = AsyncMock(side_effect=["0x20", [LogFixture.make()], [], []])
    observer.observe = AsyncMock()
    receipt = TransportReceipt(wall_timestamp_ms=123456, monotonic_ns=50, boot_id="boot")
    clock.capture.return_value = receipt
    recovery = BoundedLogRecovery(
        state=state,
        gateway=gateway,
        observer=observer,
        clock=clock,
        raw=raw,
        provider="infura",
        overlap=8,
    )
    state.advance_checkpoint(recovery.source_key(group), 16)
    assert await recovery.recover(group) == 32
    ranges = [
        call.args[2][0] for call in gateway.call.await_args_list if call.args[1] == "eth_getLogs"
    ]
    assert [(row["fromBlock"], row["toBlock"]) for row in ranges] == [
        ("0x8", "0x11"),
        ("0x12", "0x1b"),
        ("0x1c", "0x20"),
    ]
    assert all(len(row["topics"]) == 3 for row in ranges)
    observer.observe.assert_awaited_once_with(LogFixture.make(), receipt, recovered=True)
    raw.record.assert_called_once_with(
        "infura", "mined", LogFixture.make(), receipt, recovered=True
    )
    assert state.checkpoint(recovery.source_key(group)) == 32
    state.close()


@pytest.mark.asyncio
async def test_gap_above_bound_recovers_recent_overlap_before_advancing(tmp_path: Path) -> None:
    """Oversized downtime resumes after verified recent evidence without a manual reset."""
    from pmcopy.services.onchain_detection.bounded_recovery import BoundedLogRecovery

    state = SqliteDetectionState(tmp_path / "state.sqlite3")
    group = DetectionSubscription("alchemy", "mined", (OWNER,))
    gateway = Mock(call=AsyncMock(side_effect=["0x2000", []]))
    recovery = BoundedLogRecovery(
        state=state, gateway=gateway, observer=Mock(), clock=Mock(), raw=Mock(), provider="infura"
    )
    state.advance_checkpoint(recovery.source_key(group), 1)
    assert await recovery.recover(group) == 8192
    assert gateway.call.await_count == 2
    assert gateway.call.await_args.args[2][0]["fromBlock"] == "0x1ff8"
    assert state.checkpoint(recovery.source_key(group)) == 8192
    state.close()


@pytest.mark.asyncio
async def test_recovery_invalidates_replaced_blocks_before_processing_new_logs(
    tmp_path: Path,
) -> None:
    """A missed removed notification is reconstructed from canonical block identity."""
    from pmcopy.services.onchain_detection.bounded_recovery import BoundedLogRecovery

    state = SqliteDetectionState(tmp_path / "state.sqlite3")
    fill = NativeOwnerLogDecoder().decode(LogFixture.make())
    state.observe(fill, recovered=False)
    group = DetectionSubscription("alchemy", "mined", (OWNER,))
    gateway = Mock(
        call=AsyncMock(side_effect=["0x10", {"number": "0x10", "hash": "0x" + "dd" * 32}, []])
    )
    raw = Mock()
    recovery = BoundedLogRecovery(
        state=state, gateway=gateway, observer=Mock(), clock=Mock(), raw=raw, provider="infura"
    )
    await recovery.recover(group)
    assert state.observe(fill, recovered=False) == "removed"
    assert raw.record.call_args.args[1] == "block_header"
    state.close()


@pytest.mark.asyncio
async def test_recovery_bound_counts_both_inclusive_endpoints(tmp_path: Path) -> None:
    """An inclusive getLogs range never exceeds the configured block count."""
    from pmcopy.services.onchain_detection.bounded_recovery import BoundedLogRecovery

    state = SqliteDetectionState(tmp_path / "state")
    group = DetectionSubscription("alchemy", "mined", (OWNER,))
    gateway = Mock(call=AsyncMock(side_effect=["0xa", [], []]))
    recovery = BoundedLogRecovery(
        state=state,
        gateway=gateway,
        observer=Mock(),
        clock=Mock(),
        raw=Mock(),
        provider="infura",
        overlap=8,
        maximum_gap=10,
    )
    state.advance_checkpoint(recovery.source_key(group), 8)
    assert await recovery.recover(group) == 10
    assert gateway.call.await_count == 2
    assert gateway.call.await_args.args[2][0]["fromBlock"] == "0x2"
    state.close()
