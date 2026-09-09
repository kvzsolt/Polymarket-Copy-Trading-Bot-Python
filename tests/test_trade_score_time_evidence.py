"""Optional trade scoring does not invent freshness or financial edge."""

import pytest

from services.tradeScoringService import TradeScoringService
from tests.config_helpers import load_test_config


@pytest.fixture
def config():
    return load_test_config({"FUNDER_ADDRESS": "0x" + "1" * 40, "RPC_URL": "https://rpc.invalid"})


@pytest.mark.parametrize(
    ("timestamp", "reason", "age"),
    [
        (None, "signal_age_unknown", None),
        (True, "signal_age_unknown", None),
        ("NaN", "signal_age_unknown", None),
        ("Infinity", "signal_age_unknown", None),
        (1001, "signal_timestamp_future", -1),
        (999, None, 1),
    ],
)
def test_trade_score_preserves_unknown_and_future_time(timestamp, reason, age, config):
    service = TradeScoringService(config, clock=lambda: 1000)
    result = service.score_buy_trade(
        {"price": "0.5", "usdcSize": "10", "timestamp": timestamp},
        live_price=0.5,
        leader_score=0.8,
    )
    assert result.hard_fail_reason == reason
    assert result.age_seconds == age


@pytest.mark.parametrize("price", [True, "NaN", "Infinity", "0", "1"])
def test_nonfinite_or_invalid_price_fails_closed(price, config):
    result = TradeScoringService(config).score_buy_trade(
        {"price": price, "timestamp": 1},
        live_price=0.5,
        leader_score=0.8,
    )
    assert result.hard_fail_reason == "invalid_reference_or_live_price"
