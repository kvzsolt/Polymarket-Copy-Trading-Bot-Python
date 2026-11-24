"""Legacy entry normalization cannot invent source time from processing time."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pmcopy.orchestrators.pipeline import trade_pipeline
from tests.config_helpers import load_test_config
from tests.test_trade_pipeline_safety import FAKE_TOKEN_ID


@pytest.mark.parametrize("timestamp", [None, True, "NaN", "Infinity"])
def test_legacy_normalizer_preserves_missing_or_invalid_time(timestamp):
    raw = {
        "side": "BUY",
        "asset": FAKE_TOKEN_ID,
        "conditionId": "condition",
        "transactionHash": "tx",
        "price": "0.5",
        "size": "10",
        "timestamp": timestamp,
    }
    [(signal, _)] = trade_pipeline.normalize_signals([raw], "leader")
    assert signal.event_ts is None


def test_legacy_normalizer_honors_explicit_millisecond_units():
    raw = {
        "side": "BUY",
        "asset": FAKE_TOKEN_ID,
        "conditionId": "condition",
        "transactionHash": "tx",
        "price": "0.5",
        "size": "10",
        "timestamp": 100000,
        "timestamp_unit": "milliseconds",
    }
    [(signal, _)] = trade_pipeline.normalize_signals([raw], "leader")
    assert signal.event_ts == 100


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("timestamp", "reason"),
    [
        (None, "signal_age_unknown"),
        (1700000001, "signal_timestamp_future"),
        (1699999000, "signal_too_old"),
    ],
)
async def test_legacy_pipeline_rejects_entry_before_metadata_and_reservation(
    timestamp, reason, monkeypatch
):
    context = SimpleNamespace(
        user_id="account-a",
        config=load_test_config({}),
        leader_scorer=Mock(),
        copy_decision_service=Mock(),
        leader_performance=None,
        execution_service=Mock(),
        extra={},
        orders_repo=AsyncMock(),
    )
    monkeypatch.setattr(
        trade_pipeline, "reconcile_submitted_unconfirmed", AsyncMock(return_value=0)
    )
    monkeypatch.setattr(
        trade_pipeline, "_resolve_leader_tier", AsyncMock(return_value="live_follow")
    )
    metadata = AsyncMock(side_effect=AssertionError("Unusable entry reached metadata I/O"))
    monkeypatch.setattr(trade_pipeline, "_prefetch_market_data_for_signals", metadata)
    raw = {
        "side": "BUY",
        "asset": FAKE_TOKEN_ID,
        "conditionId": "condition",
        "transactionHash": "tx",
        "price": "0.5",
        "size": "10",
        "timestamp": timestamp,
    }
    [result] = await trade_pipeline._execute_trades_impl(context, "leader", [raw], 1700000000)
    assert result.reason == reason
    assert result.status == "skipped"
    metadata.assert_not_awaited()
    context.orders_repo.try_claim_processed_trade.assert_not_awaited()
