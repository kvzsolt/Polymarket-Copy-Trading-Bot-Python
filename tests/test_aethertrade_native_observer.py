"""Native-owner evidence enters the existing dispatcher only once and only fresh."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from pmcopy.domain.grouped_detection import TraderDetectionPolicy
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.storage.detection_state import SqliteDetectionState
from tests.test_aethertrade_native_owner import COUNTERPARTY, EXCHANGE, OWNER, LogFixture


@pytest.mark.asyncio
async def test_owner_only_dispatch_preserves_native_taker_and_original_receipt(
    tmp_path: Path,
) -> None:
    """Topic3 matching does not create a counterparty action; topic2 taker summary does."""
    from pmcopy.services.onchain_detection.native_observer import NativeOwnerObserver

    state = SqliteDetectionState(tmp_path / "state.sqlite3")
    state.configure((TraderDetectionPolicy(OWNER), TraderDetectionPolicy(COUNTERPARTY)))
    sink, condition, timestamp = AsyncMock(), AsyncMock(), AsyncMock()
    condition.resolve.return_value = "condition"
    timestamp.timestamp_ms.return_value = 123000
    receipt = TransportReceipt(wall_timestamp_ms=123456, monotonic_ns=50, boot_id="boot")
    observer = NativeOwnerObserver(
        wallets=(OWNER, COUNTERPARTY),
        state=state,
        condition_resolver=condition,
        block_timestamps=timestamp,
        sink=sink,
    )
    await observer.observe(LogFixture.make(counterparty=EXCHANGE), receipt, recovered=False)
    assert sink.await_count == 1
    signal = sink.await_args.args[0]
    assert signal.leader_wallet == OWNER
    assert signal.side == "BUY"
    assert signal.watched_role == "native_taker"
    assert signal.source_receipt is receipt
    assert signal.event_ts == 123000
    assert signal.provisional is False
    assert state.states()[0].last_observation_ms == 123456
    state.close()


@pytest.mark.asyncio
async def test_removal_recovery_reinclusion_duplicates_and_counterparty_never_dispatch(
    tmp_path: Path,
) -> None:
    """Evidence-only transitions never turn historical data into a new trade signal."""
    from pmcopy.services.onchain_detection.native_observer import NativeOwnerObserver

    state = SqliteDetectionState(tmp_path / "state.sqlite3")
    sink, condition, timestamp = AsyncMock(), AsyncMock(), AsyncMock()
    condition.resolve.return_value = "condition"
    timestamp.timestamp_ms.return_value = None
    receipt = TransportReceipt(wall_timestamp_ms=123456, monotonic_ns=50, boot_id="boot")
    observer = NativeOwnerObserver(
        wallets=(OWNER,),
        state=state,
        condition_resolver=condition,
        block_timestamps=timestamp,
        sink=sink,
    )
    log = LogFixture.make()
    assert await observer.observe(log | {"removed": True}, receipt, recovered=False) == "removed"
    assert await observer.observe(log, receipt, recovered=False) == "removed"
    assert (
        await observer.observe(log | {"blockHash": "0x" + "dd" * 32}, receipt, recovered=False)
        == "reincluded"
    )
    historical = log | {"transactionHash": "0x" + "ee" * 32, "logIndex": "0x4"}
    assert await observer.observe(historical, receipt, recovered=True) == "recovered"
    assert await observer.observe(historical, receipt, recovered=False) == "duplicate"
    other = LogFixture.make()
    other["topics"][2], other["topics"][3] = other["topics"][3], other["topics"][2]
    assert await observer.observe(other, receipt, recovered=False) == "unwatched"
    sink.assert_not_awaited()
    condition.resolve.assert_not_awaited()
    timestamp.timestamp_ms.assert_not_awaited()
    state.close()


@pytest.mark.asyncio
async def test_missing_enrichment_is_visible_and_never_uses_wall_time_as_block_time(
    tmp_path: Path,
) -> None:
    """Incomplete enrichment is explicit evidence and cannot submit a fresh copy."""
    from pmcopy.services.onchain_detection.native_observer import NativeOwnerObserver

    state = SqliteDetectionState(tmp_path / "state.sqlite3")
    state.configure((TraderDetectionPolicy(OWNER),))
    sink, condition, timestamp = AsyncMock(), AsyncMock(), AsyncMock()
    condition.resolve.return_value = ""
    timestamp.timestamp_ms.return_value = None
    receipt = TransportReceipt(wall_timestamp_ms=123456, monotonic_ns=50, boot_id="boot")
    observer = NativeOwnerObserver(
        wallets=(OWNER,),
        state=state,
        condition_resolver=condition,
        block_timestamps=timestamp,
        sink=sink,
    )
    assert (
        await observer.observe(LogFixture.make(), receipt, recovered=False)
        == "enrichment_unavailable"
    )
    sink.assert_not_awaited()
    assert state.states()[0].degraded_reason == "enrichment_unavailable"
    state.close()


@pytest.mark.asyncio
async def test_retirement_during_enrichment_blocks_dispatch(tmp_path: Path) -> None:
    """Roster retirement cannot race an outstanding market lookup into a new copy."""
    from pmcopy.services.onchain_detection.native_observer import NativeOwnerObserver

    state = SqliteDetectionState(tmp_path / "state")
    sink, condition, timestamp = AsyncMock(), AsyncMock(), AsyncMock()
    timestamp.timestamp_ms.return_value = 123000
    observer = NativeOwnerObserver(
        wallets=(OWNER,),
        state=state,
        condition_resolver=condition,
        block_timestamps=timestamp,
        sink=sink,
    )

    async def retire(_token):
        observer.retain_wallets(())
        return "condition"

    condition.resolve.side_effect = retire
    receipt = TransportReceipt(wall_timestamp_ms=123456, monotonic_ns=50, boot_id="boot")
    assert await observer.observe(LogFixture.make(), receipt, recovered=False) == "retired"
    sink.assert_not_awaited()
    state.close()
