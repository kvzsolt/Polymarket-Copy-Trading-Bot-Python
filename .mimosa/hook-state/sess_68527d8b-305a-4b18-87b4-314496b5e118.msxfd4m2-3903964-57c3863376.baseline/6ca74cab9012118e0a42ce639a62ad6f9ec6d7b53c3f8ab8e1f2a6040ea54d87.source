"""Tests for ``services/strategy_profile/strategy_profiler.py`` (PR3)."""

from __future__ import annotations

import time

import pytest

from services.strategy_profile.leader_history_loader import LoadedHistory
from services.strategy_profile.strategy_profiler import compute_features, profile_leader


def _mk_activity(
    *,
    side: str = "BUY",
    price: float = 0.5,
    size: float = 10.0,
    condition_id: str = "cond-A",
    ts_ms: int | None = None,
    maker: bool = False,
) -> dict:
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    usdc = size * price
    return {
        "side": side,
        "price": str(price),
        "size": str(size),
        "usdcSize": str(usdc),
        "conditionId": condition_id,
        "timestamp": ts_ms,
        "maker": maker,
    }


def _mk_closed(
    *,
    pnl: float = 1.0,
    entry_size: float = 10.0,
    avg_price: float = 0.5,
    exit_price: float = 0.6,
    closed_ts_ms: int | None = None,
    opened_ts_ms: int | None = None,
) -> dict:
    now = int(time.time() * 1000)
    if closed_ts_ms is None:
        closed_ts_ms = now
    if opened_ts_ms is None:
        opened_ts_ms = now - 30 * 60_000  # 30 min hold
    return {
        "pnl": str(pnl),
        "entrySize": str(entry_size),
        "avgPrice": str(avg_price),
        "exitAvgPrice": str(exit_price),
        "closedAt": closed_ts_ms,
        "createdAt": opened_ts_ms,
    }


def test_compute_features_empty_history() -> None:
    history = LoadedHistory(wallet="0xabc")
    features = compute_features(history)
    # Trade counts are 0 (not None); most other features are None on empty.
    assert features["trade_count_7d"] == pytest.approx(0.0)
    assert features["trade_count_30d"] == pytest.approx(0.0)
    assert features["median_notional_usdc"] is None
    assert features["avg_entry_price"] is None
    assert features["market_concentration_hhi"] is None


def test_compute_features_basic_counts_and_notional() -> None:
    now = int(time.time() * 1000)
    activity = [
        _mk_activity(price=0.5, size=10, ts_ms=now, condition_id="A"),
        _mk_activity(price=0.5, size=20, ts_ms=now, condition_id="A"),
        _mk_activity(price=0.9, size=5, ts_ms=now, condition_id="B"),
    ]
    history = LoadedHistory(wallet="0xabc", activity=activity)
    features = compute_features(history)
    assert features["trade_count_30d"] == pytest.approx(3.0)
    assert features["median_notional_usdc"] == pytest.approx(5.0)  # median of [5, 5, 10] = 5.0
    assert features["avg_entry_price"] is not None
    # HHI: 2/3 A + 1/3 B -> (2/3)^2 + (1/3)^2 = 5/9
    assert abs(features["market_concentration_hhi"] - 5 / 9) < 1e-6


def test_low_price_entry_ratio() -> None:
    now = int(time.time() * 1000)
    activity = [
        _mk_activity(price=0.05, ts_ms=now),
        _mk_activity(price=0.08, ts_ms=now),
        _mk_activity(price=0.50, ts_ms=now),
        _mk_activity(price=0.90, ts_ms=now),
    ]
    features = compute_features(LoadedHistory(wallet="0xabc", activity=activity))
    # 2 of 4 entries below 0.10
    assert features["low_price_entry_ratio"] == pytest.approx(0.5)


def test_near_certainty_entry_ratio() -> None:
    now = int(time.time() * 1000)
    activity = [
        _mk_activity(price=0.80, ts_ms=now),
        _mk_activity(price=0.85, ts_ms=now),
        _mk_activity(price=0.50, ts_ms=now),
    ]
    features = compute_features(LoadedHistory(wallet="0xabc", activity=activity))
    # 2 of 3 entries >= 0.75
    assert abs(features["near_certainty_entry_ratio"] - 2 / 3) < 1e-6


def test_maker_taker_fill_ratio() -> None:
    now = int(time.time() * 1000)
    activity = [
        _mk_activity(maker=True, ts_ms=now),
        _mk_activity(maker=True, ts_ms=now),
        _mk_activity(maker=False, ts_ms=now),
    ]
    features = compute_features(LoadedHistory(wallet="0xabc", activity=activity))
    assert features["maker_fill_ratio"] == 2 / 3
    assert features["taker_fill_ratio"] == 1 / 3


def test_win_rate_and_pnl_from_closed_positions() -> None:
    now = int(time.time() * 1000)
    closed = [
        _mk_closed(pnl=10, closed_ts_ms=now),
        _mk_closed(pnl=-5, closed_ts_ms=now),
        _mk_closed(pnl=20, closed_ts_ms=now),
    ]
    features = compute_features(LoadedHistory(wallet="0xabc", closed_positions=closed))
    assert features["win_rate_7d"] == 2 / 3
    assert features["pnl_30d_usdc"] == pytest.approx(25.0)


def test_hold_time_from_closed_positions() -> None:
    now = int(time.time() * 1000)
    closed = [
        _mk_closed(opened_ts_ms=now - 60 * 60_000, closed_ts_ms=now),  # 60 min
        _mk_closed(opened_ts_ms=now - 30 * 60_000, closed_ts_ms=now),  # 30 min
    ]
    features = compute_features(LoadedHistory(wallet="0xabc", closed_positions=closed))
    assert features["avg_hold_time_minutes"] == pytest.approx(45.0)


def test_near_max_exit_ratio() -> None:
    now = int(time.time() * 1000)
    closed = [
        _mk_closed(exit_price=0.97, closed_ts_ms=now),
        _mk_closed(exit_price=0.50, closed_ts_ms=now),
        _mk_closed(exit_price=0.99, closed_ts_ms=now),
    ]
    features = compute_features(LoadedHistory(wallet="0xabc", closed_positions=closed))
    # 2 of 3 exits >= 0.95
    assert abs(features["near_max_exit_ratio"] - 2 / 3) < 1e-6


def test_grouped_signal_ratio() -> None:
    now = int(time.time() * 1000)
    # 3 trades within 5s of each other, 1 isolated.
    activity = [
        _mk_activity(ts_ms=now),
        _mk_activity(ts_ms=now + 1_000),
        _mk_activity(ts_ms=now + 2_000),
        _mk_activity(ts_ms=now + 100_000),
    ]
    features = compute_features(LoadedHistory(wallet="0xabc", activity=activity))
    # 3 of 4 trades are grouped (within 5s of a neighbour)
    assert features["grouped_signal_ratio"] == pytest.approx(0.75)


def test_crypto_fast_signal_ratio() -> None:
    now = int(time.time() * 1000)
    # 2 trades within 60s, 1 isolated.
    activity = [
        _mk_activity(ts_ms=now),
        _mk_activity(ts_ms=now + 30_000),
        _mk_activity(ts_ms=now + 200_000),
    ]
    features = compute_features(LoadedHistory(wallet="0xabc", activity=activity))
    # 1 of 3 trades is within 60s of the previous (the second)
    assert features["crypto_fast_signal_ratio"] == 1 / 3


def test_profile_leader_returns_profile_with_features() -> None:
    now = int(time.time() * 1000)
    activity = [_mk_activity(ts_ms=now) for _ in range(20)]
    history = LoadedHistory(wallet="0xabc", activity=activity, lookback_days=30)
    profile = profile_leader(history)
    assert profile.wallet == "0xabc"
    assert profile.lookback_days == 30
    assert "trade_count_30d" in profile.features
    assert profile.computed_at > 0
