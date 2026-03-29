"""Leader Scoring Service — wallet trust scores with optional polymarketanalytics enrichment."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from config.env import ENV


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


@dataclass(frozen=True)
class LeaderScore:
    wallet: str
    score: float
    balance_score: float
    analytics_score: float
    activity_score: float
    discovery_score: float
    sample_size: int
    details: dict[str, Any]


class LeaderScoringService:
    """Compute trust scores for leader wallets.

    When a TraderDiscoveryService is available, enriches the score with
    real PnL, win rate, and position data from polymarketanalytics.com.
    """

    def __init__(self, discovery_service=None):
        self._discovery = discovery_service

    def score_leader(
        self,
        wallet: str,
        *,
        leader_balance: float | None = None,
        analytics: dict[str, Any] | None = None,
        recent_trade_count: int | None = None,
    ) -> LeaderScore:
        analytics = analytics or {}
        sample_size = int(analytics.get("sample_size", 0) or 0)
        win_rate = float(analytics.get("win_rate", 50.0) or 50.0)
        net_pnl = float(analytics.get("net_pnl", 0.0) or 0.0)
        leader_balance = float(leader_balance or 0.0)
        recent_trade_count = int(recent_trade_count or 0)

        # ── Base scores (existing logic) ──
        balance_target = float(ENV.get("LEADER_SCORE_BALANCE_TARGET_USDC", 500.0) or 500.0)
        balance_score = 1.0 if balance_target <= 0 else _clamp(leader_balance / balance_target)

        win_component = _clamp(win_rate / 100.0)
        pnl_bonus_cap = float(ENV.get("LEADER_SCORE_PNL_BONUS_CAP", 0.1) or 0.1)
        pnl_bonus = _clamp(net_pnl / max(balance_target, 1.0), -pnl_bonus_cap, pnl_bonus_cap)
        analytics_score = _clamp(win_component + pnl_bonus)

        activity_target = max(int(ENV.get("LEADER_SCORE_ACTIVITY_TARGET", 8) or 8), 1)
        activity_score = _clamp(recent_trade_count / activity_target)

        # ── Discovery enrichment ──
        discovery_score = 0.0
        discovery_data = {}

        if self._discovery:
            profile = self._discovery.get_cached_profile(wallet)
            if profile:
                discovery_score = profile.composite_score
                discovery_data = {
                    "discovery_pnl": profile.overall_gain,
                    "discovery_win_rate": profile.win_rate,
                    "discovery_positions": profile.total_positions,
                    "discovery_rank": profile.rank,
                    "discovery_name": profile.name,
                }
                # Override analytics score with richer data if available
                if profile.overall_gain != 0 or profile.total_positions > 0:
                    wr = profile.win_rate
                    if wr > 1.0:
                        wr = wr / 100.0
                    analytics_score = _clamp(wr)
                    sample_size = max(sample_size, profile.total_positions)
                    if profile.total_positions > 0:
                        activity_score = _clamp(profile.total_positions / max(activity_target, 1))

        # ── Weighted combination ──
        weight_balance = float(ENV.get("LEADER_SCORE_WEIGHT_BALANCE", 0.35) or 0.35)
        weight_analytics = float(ENV.get("LEADER_SCORE_WEIGHT_ANALYTICS", 0.45) or 0.45)
        weight_activity = float(ENV.get("LEADER_SCORE_WEIGHT_ACTIVITY", 0.20) or 0.20)

        # If discovery data available, give it extra weight
        if discovery_score > 0:
            # Blend discovery score into the mix (30% discovery, scale down others)
            weight_discovery = 0.30
            scale = (1.0 - weight_discovery) / max(weight_balance + weight_analytics + weight_activity, 1e-9)
            weighted = (
                balance_score * weight_balance * scale
                + analytics_score * weight_analytics * scale
                + activity_score * weight_activity * scale
                + discovery_score * weight_discovery
            )
        else:
            weight_sum = max(weight_balance + weight_analytics + weight_activity, 1e-9)
            weighted = (
                balance_score * weight_balance
                + analytics_score * weight_analytics
                + activity_score * weight_activity
            ) / weight_sum

        score_floor = float(ENV.get("LEADER_SCORE_FLOOR", 0.10) or 0.10)
        final_score = _clamp(max(weighted, score_floor))

        details = {
            "leader_balance": leader_balance,
            "win_rate": win_rate,
            "net_pnl": net_pnl,
            "recent_trade_count": recent_trade_count,
        }
        details.update(discovery_data)

        return LeaderScore(
            wallet=wallet,
            score=final_score,
            balance_score=balance_score,
            analytics_score=analytics_score,
            activity_score=activity_score,
            discovery_score=discovery_score,
            sample_size=sample_size,
            details=details,
        )

    def trust_multiplier(self, score: float) -> float:
        min_mult = float(ENV.get("TRUST_SIZE_MULTIPLIER_MIN", 0.70) or 0.70)
        max_mult = float(ENV.get("TRUST_SIZE_MULTIPLIER_MAX", 1.25) or 1.25)
        score = _clamp(score)
        return min_mult + (max_mult - min_mult) * score
