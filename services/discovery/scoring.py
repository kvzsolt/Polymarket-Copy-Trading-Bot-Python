from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from typing import Any

from integrations.falcon.client import row_value
from services.discovery.models import Candidate


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def normalize(value: float, bounds: Sequence[float]) -> float:
    lower, upper = float(bounds[0]), float(bounds[1])
    if upper <= lower:
        raise ValueError("normalization upper bound must exceed lower bound")
    return clamp((float(value) - lower) / (upper - lower))


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed == parsed and abs(parsed) != float("inf") else default


def win_rate_consistency(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 1.0
    mean = statistics.fmean(values)
    if mean <= 0:
        return 0.0
    return clamp(1.0 - statistics.pstdev(values) / mean)


class CompositeScorer:
    def score(self, candidate: Candidate, discovery: Any) -> float:
        config = discovery.scoring
        row = candidate.hscore
        h_score = as_float(row_value(row, "h_score", "hScore"))
        roi = as_float(row_value(row, "roi_15d", "roi_pct_15d"))
        win_rate = _rate(
            row_value(row, "win_rate_15d", "win_rate_pct_15d")
        )
        sharpe = as_float(row_value(row, "sharpe_15d", "sharpe_ratio_15d"))
        volume = as_float(row_value(row, "total_volume_15d", "volume_15d"))
        rank = max(
            1,
            int(as_float(row_value(row, "leaderboard_rank", "rank", default=config.rank_target), config.rank_target)),
        )

        quality = config.quality_weights
        falcon_quality = clamp(
            quality.h_score * normalize(h_score, config.h_score_range)
            + quality.roi * normalize(roi, config.roi_15d_range)
            + quality.win_rate * normalize(win_rate, config.win_rate_15d_range)
            + quality.sharpe * normalize(sharpe, config.sharpe_15d_range)
            + quality.baseline
        )
        copyability = clamp(
            len(candidate.recent_trades) / float(config.copyability_trade_target)
        )
        rates = [
            _rate(
                row_value(candidate.wallet360.get(window, {}), "win_rate", "win_rate_pct")
            )
            for window in discovery.wallet360.windows
        ]
        consistency = win_rate_consistency(rates)
        market_quality = clamp(as_float(candidate.market_quality.get("pass_ratio")))
        volume_fit = clamp(volume / float(config.volume_target_15d))
        rank_quality = clamp(1.0 - (rank - 1) / max(1, int(config.rank_target) - 1))
        window_15 = candidate.wallet360.get("15", {})
        bot_flag = _as_bool(
            row_value(window_15, "bot_flag", "is_bot", "botFlag", default=False)
        )
        drawdown = as_float(
            row_value(window_15, "max_drawdown_pct", "max_drawdown", default=100.0),
            100.0,
        )
        risk_inverse = 0.0 if bot_flag else clamp(
            1.0 - drawdown / float(config.risk_drawdown_max_pct)
        )

        candidate.metrics = {
            "sharpe": sharpe,
            "roi": roi,
            "win_rate": win_rate,
            "rank": rank,
            "drawdown_pct": drawdown,
            "trade_count": int(
                as_float(
                    row_value(
                        row,
                        "total_trades_15d",
                        "trade_count_15d",
                        default=len(candidate.recent_trades),
                    )
                )
            ),
        }

        candidate.sub_scores = {
            "falcon_quality": falcon_quality,
            "copyability": copyability,
            "recent_consistency": consistency,
            "market_quality": market_quality,
            "volume_fit": volume_fit,
            "rank_quality": rank_quality,
            "risk_inverse": risk_inverse,
        }
        weights: Mapping[str, float] = config.weights.model_dump()
        score = sum(weights[name] * value for name, value in candidate.sub_scores.items())
        candidate.base_score = clamp(score)
        candidate.score = candidate.base_score
        return candidate.base_score


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def _rate(value: Any) -> float:
    parsed = as_float(value)
    return parsed / 100.0 if parsed > 1.0 else parsed


__all__ = [
    "CompositeScorer",
    "as_float",
    "clamp",
    "normalize",
    "win_rate_consistency",
]
