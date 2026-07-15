from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from services.discovery.scoring import clamp


def _normalize(value: float, bounds: Sequence[float]) -> float:
    lower, upper = float(bounds[0]), float(bounds[1])
    if upper <= lower:
        raise ValueError("longitudinal normalization range must increase")
    return clamp((float(value) - lower) / (upper - lower))


def rolling_sharpe(daily_pnl: Sequence[float], risk_free: float = 0.0) -> float:
    values = [float(value) for value in daily_pnl]
    if len(values) < 2:
        return 0.0
    deviation = statistics.stdev(values)
    return math.sqrt(365.0) * (statistics.fmean(values) - risk_free) / deviation if deviation > 0 else 0.0


def sortino(daily_pnl: Sequence[float], risk_free: float = 0.0) -> float:
    values = [float(value) for value in daily_pnl]
    downside = [value for value in values if value < risk_free]
    if len(downside) < 2:
        return 0.0
    deviation = statistics.stdev(downside)
    return math.sqrt(365.0) * (statistics.fmean(values) - risk_free) / deviation if deviation > 0 else 0.0


def max_drawdown(pnl: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in pnl:
        equity += float(value)
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return worst


def stake_cv(stakes: Sequence[float]) -> float:
    values = [float(value) for value in stakes if float(value) >= 0]
    if len(values) < 2:
        return 0.0 if values else 99.0
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if mean > 0 else 99.0


def trend_slope(values: Sequence[float]) -> float:
    points = [float(value) for value in values]
    count = len(points)
    if count < 2:
        return 0.0
    mean_x = (count - 1) / 2.0
    mean_y = statistics.fmean(points)
    numerator = sum((index - mean_x) * (value - mean_y) for index, value in enumerate(points))
    denominator = sum((index - mean_x) ** 2 for index in range(count))
    return numerator / denominator if denominator > 0 else 0.0


def entry_timing_skill(entry_price: float, subsequent_prices: Sequence[float], side: str) -> float:
    if not subsequent_prices:
        return 0.0
    forward = statistics.fmean(float(price) for price in subsequent_prices) - float(entry_price)
    return forward if str(side).upper() == "BUY" else -forward


def decay_weight(age_days: float, half_life_days: float = 7.0) -> float:
    return 0.5 ** (max(0.0, float(age_days)) / float(half_life_days))


def shrink(sample_score: float, prior: float, sample_size: int, prior_size: int = 30) -> float:
    weight = max(0, int(sample_size)) / (max(0, int(sample_size)) + max(1, int(prior_size)))
    return weight * float(sample_score) + (1.0 - weight) * float(prior)


def longitudinal_rank(
    history: Sequence[Mapping[str, Any]],
    *,
    config: Any,
) -> float:
    if not history:
        return float(config.prior_score)
    ordered = sorted(history, key=lambda row: _timestamp(row.get("ts")))
    newest = max(_timestamp(row.get("ts")) for row in ordered)
    weighted_total = 0.0
    total_weight = 0.0
    weights = config.weights
    for index, row in enumerate(ordered):
        age_days = max(0.0, (newest - _timestamp(row.get("ts"))) / 86400.0)
        time_weight = decay_weight(age_days, config.half_life_days)
        trend_start = max(0, index + 1 - int(config.trend_window))
        stability_start = max(0, index + 1 - int(config.stability_window))
        trend_rows = ordered[trend_start : index + 1]
        stability_rows = ordered[stability_start : index + 1]
        roi_slope = trend_slope([_number(item.get("roi")) for item in trend_rows])
        rank_slope = trend_slope([_number(item.get("rank")) for item in trend_rows])
        win_rates = [_rate(item.get("win_rate")) for item in stability_rows]
        win_rate_stability = _consistency(win_rates)
        component = (
            weights.sharpe
            * _normalize(_number(row.get("sharpe")), config.sharpe_range)
            + weights.roi_slope
            * _normalize(roi_slope, config.roi_slope_range)
            + weights.win_rate_stability * win_rate_stability
            + weights.rank_trajectory
            * _normalize(-rank_slope, config.rank_slope_range)
            - weights.drawdown
            * clamp(_number(row.get("drawdown_pct")) / config.drawdown_max_pct)
        )
        weighted_total += clamp(component) * time_weight
        total_weight += time_weight
    raw = weighted_total / total_weight if total_weight > 0 else float(config.prior_score)
    sample_size = int(ordered[-1].get("trade_count", 0) or 0)
    return clamp(
        shrink(raw, config.prior_score, sample_size, config.shrinkage_trade_count)
    )


def _timestamp(value: Any) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0
    return 0.0


def _number(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def _rate(value: Any) -> float:
    parsed = _number(value)
    return parsed / 100.0 if parsed > 1.0 else parsed


def _consistency(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 1.0
    mean = statistics.fmean(values)
    if mean <= 0:
        return 0.0
    return clamp(1.0 - statistics.pstdev(values) / mean)


__all__ = [
    "decay_weight",
    "entry_timing_skill",
    "longitudinal_rank",
    "max_drawdown",
    "rolling_sharpe",
    "shrink",
    "sortino",
    "stake_cv",
    "trend_slope",
]
