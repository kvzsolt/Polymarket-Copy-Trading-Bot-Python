"""Live leader trust scoring for copy decisions."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING

from pmcopy.domain.performance import PerformanceUnits

if TYPE_CHECKING:
    from pmcopy.config.app_config import AppConfig


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


@dataclass(frozen=True)
class LeaderScore:
    """Normalized leader quality with explicit missing runtime observations."""

    wallet: str
    score: float
    roster_score: float
    runtime_score: float
    activity_score: float
    sample_size: int
    details: Mapping[str, float | int | None]


class LeaderScoringService:
    """Low-latency runtime score used by CopyDecisionService.

    Discovery rank affects roster membership and sizing before a trading
    context is built; the hot decision path never reaches back into discovery.
    """

    def __init__(
        self,
        *,
        config: "AppConfig",
        roster_score: float | None = None,
    ) -> None:
        self._config = config
        self._trust_mult_enabled = config.sizing.enable_trust_size_multipliers
        self._trust_mult_min = config.sizing.trust_size_multiplier_min
        self._trust_mult_max = config.sizing.trust_size_multiplier_max
        scoring = config.discovery.scoring
        self._runtime_config = scoring
        self._roster_score = _clamp(
            scoring.runtime_default_roster_score if roster_score is None else float(roster_score)
        )

    def score_leader(
        self,
        wallet: str,
        *,
        leader_balance: float | None = None,
        analytics: Mapping[str, object] | None = None,
        recent_trade_count: int | None = None,
    ) -> LeaderScore:
        """Score scoped realized PnL and win rate in percentage points.

        Missing evidence uses the configured scoring prior, while details
        retain ``None``. Invalid supplied observations raise ``ValueError``.
        """
        del leader_balance
        analytics = analytics or {}
        wallet_l = str(wallet).lower()

        pnl_observation = PerformanceUnits.finite(analytics.get("net_pnl"))
        win_fraction = PerformanceUnits.win_rate_percent_to_fraction(analytics.get("win_rate"))
        runtime_pnl = None if pnl_observation is None else float(pnl_observation)
        runtime_wr = None if win_fraction is None else float(win_fraction * 100)
        activity = (
            recent_trade_count
            if recent_trade_count is not None
            else analytics.get("sample_size", 0)
        )
        if isinstance(activity, bool) or not isinstance(activity, int) or activity < 0:
            raise ValueError("Runtime activity must be a non-negative integer")
        runtime_activity = activity

        win_rate_score, pnl_score, activity_score = self._runtime_components(
            0.0 if runtime_pnl is None else runtime_pnl,
            50.0 if runtime_wr is None else runtime_wr,
            runtime_activity,
        )
        weights = self._runtime_config.runtime_weights
        final = _clamp(
            weights.roster_score * self._roster_score
            + weights.win_rate * win_rate_score
            + weights.pnl * pnl_score
            + weights.activity * activity_score
        )
        final = max(final, float(self._runtime_config.runtime_score_floor))
        fresh_weight = weights.win_rate + weights.pnl + weights.activity
        runtime_score = (
            (
                weights.win_rate * win_rate_score
                + weights.pnl * pnl_score
                + weights.activity * activity_score
            )
            / fresh_weight
            if fresh_weight > 0
            else self._roster_score
        )
        details: dict[str, float | int | None] = {
            "roster_score": self._roster_score,
            "runtime_pnl": runtime_pnl,
            "runtime_win_rate": runtime_wr,
            "recent_trade_count": runtime_activity,
            "runtime_score": runtime_score,
        }

        return LeaderScore(
            wallet=wallet_l,
            score=final,
            roster_score=self._roster_score,
            runtime_score=runtime_score,
            activity_score=activity_score,
            sample_size=runtime_activity,
            details=MappingProxyType(details),
        )

    def _runtime_components(
        self, pnl: float, win_rate: float, recent_trade_count: int
    ) -> tuple[float, float, float]:
        wr = win_rate / 100.0
        wr_score = _clamp(wr)
        pnl_target = float(self._runtime_config.runtime_pnl_target_usdc)
        pnl_score = _clamp(0.5 + pnl / (2.0 * pnl_target))
        activity_score = _clamp(
            recent_trade_count / max(int(self._runtime_config.runtime_activity_target), 1)
        )
        return wr_score, pnl_score, activity_score

    def trust_multiplier(self, score: float) -> float:
        """Return the configured sizing multiplier for a normalized score."""
        if not self._trust_mult_enabled:
            return 1.0
        score = _clamp(score)
        return self._trust_mult_min + (self._trust_mult_max - self._trust_mult_min) * score
