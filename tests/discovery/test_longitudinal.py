from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.discovery.longitudinal import decay_weight, longitudinal_rank, shrink
from services.discovery.backfill import LongitudinalBackfillService, _date
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


@pytest.mark.asyncio
async def test_live_metric_row_does_not_count_as_completed_backfill() -> None:
    repository = SimpleNamespace(
        has_completed_backfill=AsyncMock(return_value=False),
        has_metric_history=AsyncMock(return_value=True),
        mark_backfill_complete=AsyncMock(),
    )
    service = LongitudinalBackfillService(
        client=object(),  # type: ignore[arg-type]
        repository=repository,
        config=SimpleNamespace(backfill_concurrency=1),
        config_hash="config-hash",
    )
    service._backfill = AsyncMock()  # type: ignore[method-assign]

    assert await service.ensure_wallet("0xABC") is True
    assert repository.has_completed_backfill.await_count == 2
    repository.has_metric_history.assert_not_awaited()
    service._backfill.assert_awaited_once_with("0xABC")
    repository.mark_backfill_complete.assert_awaited_once_with(
        "0xABC",
        "config-hash",
    )


@pytest.mark.asyncio
async def test_backfill_completion_is_scoped_to_the_current_config_hash() -> None:
    """A completion marker only counts for the config that produced it.

    ``mark_backfill_complete`` records the config hash, so the completion probe
    must ask about the *current* hash. Otherwise a wallet backfilled under an
    older longitudinal config keeps its stale marker and the new, required
    backfill is skipped forever.
    """
    repository = SimpleNamespace(
        has_completed_backfill=AsyncMock(return_value=False),
        has_metric_history=AsyncMock(return_value=False),
        mark_backfill_complete=AsyncMock(),
    )
    service = LongitudinalBackfillService(
        client=object(),  # type: ignore[arg-type]
        repository=repository,
        config=SimpleNamespace(backfill_concurrency=1),
        config_hash="new-config-hash",
    )
    service._backfill = AsyncMock()  # type: ignore[method-assign]

    assert await service.ensure_wallet("0xABC") is True

    for call in repository.has_completed_backfill.await_args_list:
        assert call.args == ("0xABC", "new-config-hash")
    repository.mark_backfill_complete.assert_awaited_once_with(
        "0xABC",
        "new-config-hash",
    )


@pytest.mark.asyncio
async def test_completed_backfill_marker_skips_remote_history_fetch() -> None:
    repository = SimpleNamespace(
        has_completed_backfill=AsyncMock(return_value=True),
        mark_backfill_complete=AsyncMock(),
    )
    service = LongitudinalBackfillService(
        client=object(),  # type: ignore[arg-type]
        repository=repository,
        config=SimpleNamespace(backfill_concurrency=1),
        config_hash="config-hash",
    )
    service._backfill = AsyncMock()  # type: ignore[method-assign]

    assert await service.ensure_wallet("0xABC") is False
    service._backfill.assert_not_awaited()
    repository.mark_backfill_complete.assert_not_awaited()
