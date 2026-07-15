from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from services.discovery.longitudinal import decay_weight, longitudinal_rank, shrink
from services.discovery.backfill import _date
from services.order_sizing import longitudinal_sizing_multiplier


def _config():
    return SimpleNamespace(
        prior_score=0.5,
        half_life_days=7.0,
        shrinkage_trade_count=30,
        trend_window=7,
        stability_window=4,
        sharpe_range=(-1.0, 5.0),
        roi_slope_range=(-1.0, 1.0),
        rank_slope_range=(-2.0, 2.0),
        drawdown_max_pct=50.0,
        weights=SimpleNamespace(
            sharpe=0.30,
            roi_slope=0.20,
            win_rate_stability=0.20,
            rank_trajectory=0.15,
            drawdown=0.15,
        ),
    )


def test_decay_half_life() -> None:
    assert decay_weight(7, 7) == pytest.approx(0.5)
    assert decay_weight(14, 7) == pytest.approx(0.25)


def test_shrink_pulls_small_samples_toward_prior() -> None:
    assert shrink(1.0, 0.5, 1, 30) < 0.52
    assert shrink(1.0, 0.5, 300, 30) > 0.90


def test_longitudinal_rank_rewards_improving_roi_and_rank() -> None:
    history = [
        {
            "ts": day * 86400,
            "sharpe": 2.0,
            "roi": float(day),
            "win_rate": 0.60,
            "rank": 20 - day,
            "drawdown_pct": 5.0,
            "trade_count": 100,
        }
        for day in range(10)
    ]
    assert longitudinal_rank(history, config=_config()) > 0.60


def test_rank_sizing_feedback_is_bounded() -> None:
    assert longitudinal_sizing_multiplier(
        0, minimum=0.75, maximum=1.25
    ) == pytest.approx(0.75)
    assert longitudinal_sizing_multiplier(
        1, minimum=0.75, maximum=1.25
    ) == pytest.approx(1.25)


def test_backfill_parses_string_epoch_seconds_and_milliseconds() -> None:
    expected = datetime(2026, 7, 15, tzinfo=timezone.utc)
    seconds = int(expected.timestamp())

    assert _date(str(seconds)) == expected.date()
    assert _date(str(seconds * 1000)) == expected.date()
