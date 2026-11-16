"""Observed follower fills preserve native attribution and unknown evidence."""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.domain.source_time import TransportReceipt
from pmcopy.storage.postgres.fills_repo import FillRecord


class FillClock:
    """Capture the transaction observation, independent of venue fill time."""

    def capture(self):
        return TransportReceipt(1800000000123, 123, "fill-clock", 1800000000123456789)


def fill_record(**changes):
    """Build exact confirmed economics with a deliberately old venue timestamp."""
    return replace(
        FillRecord(
            "follower-a",
            None,
            "order-a",
            "condition",
            "123",
            "BUY",
            Decimal("2.0100"),
            Decimal("0.5000"),
            fill_ts=1000,
        ),
        **changes,
    )


def test_fill_builder_preserves_unknowns_receipt_precision_and_account_identity():
    from pmcopy.services.evidence_fills import FollowerFillEvidenceBuilder

    builder = FollowerFillEvidenceBuilder()
    first = builder.build(7, fill_record(), FillClock().capture())
    wire = first.wire()
    assert wire["kind"] == "follower_fill_observed"
    assert wire["schema_version"] == 2
    assert wire["execution_mode"] == "unknown"
    assert wire["received_ns"] == "1800000000123456789"
    assert wire["clock_id"] == "fill-clock"
    assert wire["fill_id"] == "fill:7"
    assert wire["price"] == "0.5000" and wire["size"] == "2.0100"
    assert wire.get("fee") is None and wire.get("source_action_id") is None
    other_account = builder.build(7, fill_record(user_id="follower-b"), FillClock().capture())
    next_fill = builder.build(8, fill_record(), FillClock().capture())
    assert len({first.event_id, other_account.event_id, next_fill.event_id}) == 3
    assert first.event_id == builder.build(7, fill_record(), FillClock().capture()).event_id


def test_fill_builder_requires_native_receipt_and_keeps_explicit_zero_fee():
    from pmcopy.services.evidence_fills import FollowerFillEvidenceBuilder

    builder = FollowerFillEvidenceBuilder()
    with pytest.raises(ValueError, match="native"):
        builder.build(7, fill_record(), TransportReceipt(1800000000123, 123, "fill-clock"))
    wire = builder.build(
        7,
        fill_record(fee=Decimal("0.0000")),
        FillClock().capture(),
        source_action_id="native-action",
    ).wire()
    assert wire["fee"] == "0.0000"
    assert wire["source_action_id"] == "native-action"


@pytest.mark.parametrize("mode", ["paper", "live", "unknown"])
def test_fill_builder_preserves_explicit_execution_source(mode):
    from pmcopy.services.evidence_fills import FollowerFillEvidenceBuilder

    wire = (
        FollowerFillEvidenceBuilder()
        .build(7, fill_record(), FillClock().capture(), execution_mode=mode)
        .wire()
    )
    assert wire["execution_mode"] == mode


def test_fill_projection_requires_a_declared_supported_execution_mode():
    from pmcopy.storage.postgres.fill_evidence_projection import PostgresFillEvidenceProjection

    with pytest.raises(ValueError, match="execution mode"):
        PostgresFillEvidenceProjection(clock=FillClock(), execution_mode="simulated-live")


@pytest.mark.asyncio
async def test_native_action_survives_both_normalizers_and_order_metadata(tmp_path):
    from pmcopy.domain.detection import DetectionMode
    from pmcopy.orchestrators.trade_pipeline import _copy_entry_price_plan, normalize_signals
    from pmcopy.orchestrators.v1_event_adapters import LeaderEventNormalizer
    from pmcopy.services.onchain_detection.native_observer import NativeOwnerObserver
    from pmcopy.services.onchain_detection.native_owner import NativeOwnerLogDecoder
    from pmcopy.storage.detection_state import SqliteDetectionState
    from tests.test_aethertrade_native_owner import OWNER, LogFixture

    state = SqliteDetectionState(tmp_path / "state")
    sink, condition, timestamp = AsyncMock(), AsyncMock(), AsyncMock()
    condition.resolve.return_value, timestamp.timestamp_ms.return_value = "condition", 123000
    observer = NativeOwnerObserver(
        wallets=(OWNER,),
        state=state,
        condition_resolver=condition,
        block_timestamps=timestamp,
        sink=sink,
    )
    log = LogFixture.make()
    # Use a realistic uint256 token to exercise the legacy pipeline's token gate.
    data = log["data"]
    log["data"] = data[:66] + f"{2**250:064x}" + data[130:]
    await observer.observe(log, FillClock().capture(), recovered=False)
    native = sink.await_args.args[0]
    action = NativeOwnerLogDecoder().decode(log).action_id
    assert native.source_action_id == action
    raw = native.as_raw_signal_dict()
    assert raw["source_action_id"] == action
    signal = normalize_signals([raw], OWNER)[0][0]
    assert signal.source_action_id == action
    _, metadata = _copy_entry_price_plan(SimpleNamespace(), signal, "BUY", leader_id=OWNER)
    assert metadata["source_action_id"] == action
    normalized = LeaderEventNormalizer().normalize_raw(
        OWNER, raw, mode=DetectionMode.ON_CHAIN_LOGS_ONLY
    )
    assert normalized.trade.source_action_id == action
    raw.pop("source_action_id")
    assert normalize_signals([raw], OWNER)[0][0].source_action_id is None
    assert (
        LeaderEventNormalizer()
        .normalize_raw(OWNER, raw, mode=DetectionMode.ON_CHAIN_LOGS_ONLY)
        .trade.source_action_id
        is None
    )
    state.close()


@pytest.mark.asyncio
async def test_task_execution_persists_only_explicit_native_action():
    from pmcopy.orchestrators.v1_event_adapters import TradingContextTaskExecutionAdapter
    from tests.v1.test_v1_event_adapters import command, context

    follower = context()
    original = command()
    source = replace(
        original.source_event,
        trade=replace(original.source_event.trade, source_action_id="native-action"),
    )
    adapter = TradingContextTaskExecutionAdapter(contexts=[follower])
    await adapter.submit(replace(original, source_event=source))
    await adapter.submit(original)
    linked, legacy = follower.execution_coordinator.intents
    assert linked.recovery_metadata["source_action_id"] == "native-action"
    assert legacy.recovery_metadata.get("source_action_id") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["paper", "live", "unknown"])
async def test_both_submit_paths_preserve_the_original_execution_mode(mode):
    from pmcopy.orchestrators.trade_pipeline import _copy_entry_price_plan
    from pmcopy.orchestrators.v1_event_adapters import TradingContextTaskExecutionAdapter
    from tests.v1.test_v1_event_adapters import command, context

    follower = context()
    follower.config = SimpleNamespace(execution=SimpleNamespace(trading_mode=mode))
    _, metadata = _copy_entry_price_plan(follower, SimpleNamespace(price="0.5"), "BUY")
    assert metadata["execution_mode"] == mode
    await TradingContextTaskExecutionAdapter(contexts=[follower]).submit(command())
    assert follower.execution_coordinator.intents[0].recovery_metadata["execution_mode"] == mode
