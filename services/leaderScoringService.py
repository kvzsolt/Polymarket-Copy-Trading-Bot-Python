"""Leader Scoring Service — wallet trust scores with optional polymarketanalytics enrichment."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from config.app_config import AppConfig


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


@dataclass(frozen=True)
class LeaderScore:
    wallet: str
    score: float
    balance_score: float
    analytics_score: float
    activity_score: float
    sample_size: int
    details: dict[str, Any]
    # Optional enrichment from TraderDiscoveryService. Kept optional for backward
    # compatibility because many call sites/tests construct LeaderScore without it.
    discovery_score: float = 0.0


class LeaderScoringService:
    """Compute trust scores for leader wallets.

    When a TraderDiscoveryService is available, enriches the score with
    real PnL, win rate, and position data from polymarketanalytics.com.
    """

    def __init__(self, discovery_service=None, *, config: AppConfig | None = None):
        self._discovery = discovery_service

        if config is not None:
            lc = config.leader
            self._balance_target = lc.leader_score_balance_target_usdc
            self._pnl_bonus_cap = lc.leader_score_pnl_bonus_cap
            self._activity_target = max(int(lc.leader_score_activity_target), 1)
            self._weight_balance = lc.leader_score_weight_balance
            self._weight_analytics = lc.leader_score_weight_analytics
            self._weight_activity = lc.leader_score_weight_activity
            self._score_floor = lc.leader_score_floor
            self._trust_mult_enabled = config.sizing.enable_trust_size_multipliers
            self._trust_mult_min = config.sizing.trust_size_multiplier_min
            self._trust_mult_max = config.sizing.trust_size_multiplier_max
        else:
            # Defaults for backward compat (tests that don't pass config)
            self._balance_target = 500.0
            self._pnl_bonus_cap = 0.1
            self._activity_target = 8
            self._weight_balance = 0.35
            self._weight_analytics = 0.45
            self._weight_activity = 0.20
            self._score_floor = 0.10
            self._trust_mult_enabled = os.getenv("ENABLE_TRUST_SIZE_MULTIPLIERS", "true").strip().lower() in ("1", "true", "yes")
            self._trust_mult_min = float(os.getenv("TRUST_SIZE_MULTIPLIER_MIN", "0.50") or 0.50)
            self._trust_mult_max = float(os.getenv("TRUST_SIZE_MULTIPLIER_MAX", "1.25") or 1.25)

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
        balance_target = self._balance_target
        balance_score = 1.0 if balance_target <= 0 else _clamp(leader_balance / balance_target)

        win_component = _clamp(win_rate / 100.0)
        pnl_bonus_cap = self._pnl_bonus_cap
        pnl_bonus = _clamp(net_pnl / max(balance_target, 1.0), -pnl_bonus_cap, pnl_bonus_cap)
        analytics_score = _clamp(win_component + pnl_bonus)

        activity_target = self._activity_target
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
        weight_balance = self._weight_balance
        weight_analytics = self._weight_analytics
        weight_activity = self._weight_activity

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

        score_floor = self._score_floor
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
        if not self._trust_mult_enabled:
            return 1.0
        min_mult = self._trust_mult_min
        max_mult = self._trust_mult_max
        score = _clamp(score)
        return min_mult + (max_mult - min_mult) * score
