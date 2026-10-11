"""A stale source checkpoint must not strand the live detector after boot."""

import logging
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from domain.grouped_detection import TraderDetectionPolicy
from domain.source_time import TransportReceipt
from services.grouped_detection import DetectionSubscription
from services.onchain_detection.bounded_recovery import BoundedLogRecovery, RecoveryUnavailable
from services.onchain_detection.native_observer import NativeOwnerObserver
from services.onchain_detection.native_owner import NativeOwnerLogDecoder
from storage.detection_state import SqliteDetectionState
from tests.test_aethertrade_native_owner import OWNER, LogFixture


class TestStaleDetectionCheckpoint:
    """Recover a recent historical overlap and resume fresh observation after downtime."""

    async def test_old_checkpoint_recovers_recent_window_without_copying_history(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The reproduced 78k-block gap neither disables entries nor replays old trades."""
        from utils.logger import get_logger

        app_logger = get_logger()
        monkeypatch.setattr(app_logger, "handlers", [caplog.handler])
        monkeypatch.setattr(app_logger, "level", logging.INFO)
        state = SqliteDetectionState(tmp_path / "state.sqlite")
        state.configure((TraderDetectionPolicy(OWNER),))
        group = DetectionSubscription("alchemy", "mined", (OWNER,))
        key = BoundedLogRecovery.source_key(group)
        previous, head = 93_975_165, 94_052_902
        old_fill = NativeOwnerLogDecoder().decode(LogFixture.make())
        state.observe(old_fill, recovered=False)
        state.advance_checkpoint(key, previous)
        payload = LogFixture.make() | {
            "blockNumber": hex(head),
            "transactionHash": "0x" + "ab" * 32,
            "blockHash": "0x" + "bc" * 32,
        }
        sink = AsyncMock()
        observer = NativeOwnerObserver(
            wallets=(OWNER,),
            state=state,
            condition_resolver=Mock(),
            block_timestamps=Mock(),
            sink=sink,
        )
        gateway = Mock(call=AsyncMock(side_effect=[hex(head), [payload]]))
        receipt = TransportReceipt(wall_timestamp_ms=123456, monotonic_ns=50, boot_id="boot")
        recovery = BoundedLogRecovery(
            state=state,
            gateway=gateway,
            observer=observer,
            clock=Mock(capture=Mock(return_value=receipt)),
            raw=Mock(),
            provider="infura",
        )
        try:
            with caplog.at_level(logging.CRITICAL):
                caplog.handler.setLevel(logging.WARNING)
                assert await recovery.recover(group) == head
            request = gateway.call.await_args_list[1].args[2][0]
            assert request["fromBlock"] == hex(head - 8)
            assert request["toBlock"] == hex(head)
            assert state.checkpoint(key) == head
            assert state.observe(old_fill, recovered=False) == "duplicate"
            assert (
                state.observe(NativeOwnerLogDecoder().decode(payload), recovered=False)
                == "duplicate"
            )
            sink.assert_not_awaited()
            assert "native_detection_recovery_gap" in caplog.text
            assert f"previous_checkpoint={previous}" in caplog.text
            assert f"resume_from_block={head - 8}" in caplog.text
        finally:
            state.close()

    @pytest.mark.parametrize("failure", [RuntimeError("read unavailable"), {"invalid": "logs"}])
    async def test_failed_recent_recovery_keeps_old_checkpoint(
        self, tmp_path: Path, failure: object
    ) -> None:
        """No checkpoint is advanced until the recent window is actually validated."""
        state = SqliteDetectionState(tmp_path / "state.sqlite")
        group = DetectionSubscription("alchemy", "mined", (OWNER,))
        key = BoundedLogRecovery.source_key(group)
        state.advance_checkpoint(key, 1)
        gateway = Mock(call=AsyncMock(side_effect=["0x2000", failure]))
        recovery = BoundedLogRecovery(
            state=state,
            gateway=gateway,
            observer=Mock(),
            clock=Mock(),
            raw=Mock(),
            provider="infura",
        )
        try:
            with pytest.raises((RuntimeError, RecoveryUnavailable)):
                await recovery.recover(group)
            assert gateway.call.await_count == 2
            assert state.checkpoint(key) == 1
        finally:
            state.close()

    async def test_provider_head_behind_checkpoint_still_rejects_inconsistent_data(
        self, tmp_path: Path
    ) -> None:
        """An old provider head cannot erase known source progress."""
        state = SqliteDetectionState(tmp_path / "state.sqlite")
        group = DetectionSubscription("alchemy", "mined", (OWNER,))
        key = BoundedLogRecovery.source_key(group)
        state.advance_checkpoint(key, 20)
        gateway = Mock(call=AsyncMock(return_value="0x10"))
        recovery = BoundedLogRecovery(
            state=state,
            gateway=gateway,
            observer=Mock(),
            clock=Mock(),
            raw=Mock(),
            provider="infura",
        )
        try:
            with pytest.raises(RecoveryUnavailable, match="precedes"):
                await recovery.recover(group)
            assert state.checkpoint(key) == 20
        finally:
            state.close()
