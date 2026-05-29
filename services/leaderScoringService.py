"""Live leader trust scoring for copy decisions."""

from __future__ import annotations

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
    discovery_score: float = 0.0


class LeaderScoringService:
    """Live trust score used by CopyDecisionService.

    The public API is intentionally unchanged. Discovery enrichment now comes
    from a synchronous in-memory trust_store backed by
    falcon_discovery_evaluations, never from an async DB query in the live path.
    """

    def __init__(
        self,
        discovery_service=None,
        *,
        config: "AppConfig",
        trust_store=None,
    ):
        self._config = config
        self._discovery = discovery_service
        self._trust_store = trust_store
        self._trust_mult_enabled = config.sizing.enable_trust_size_multipliers
        self._trust_mult_min = config.sizing.trust_size_multiplier_min
        self._trust_mult_max = config.sizing.trust_size_multiplier_max
        self._score_floor = config.leader.leader_score_floor

    def score_leader(
        self,
        wallet: str,
        *,
        leader_balance: float | None = None,
        analytics: dict[str, Any] | None = None,
        recent_trade_count: int | None = None,
    ) -> LeaderScore:
        analytics = analytics or {}
        wallet_l = str(wallet).lower()

        runtime_pnl = float(analytics.get("net_pnl", 0.0) or 0.0)
        runtime_wr = float(analytics.get("win_rate", 50.0) or 50.0)
        runtime_activity = int(recent_trade_count or analytics.get("sample_size", 0) or 0)

        runtime_score = self._runtime_score(runtime_pnl, runtime_wr, runtime_activity)
        discovery_score = 0.0
        details: dict[str, Any] = {
            "runtime_pnl": runtime_pnl,
            "runtime_win_rate": runtime_wr,
            "recent_trade_count": runtime_activity,
        }

        row = self._trust_store.get_latest(wallet_l) if self._trust_store else None
        if row is not None:
            discovery_score = float(row.final_score)
            details.update(
                {
                    "falcon_quality_score": row.falcon_quality_score,
                    "copyability_score": row.copyability_score,
                    "copyable_signal_ratio": row.copyable_signal_ratio,
                    "copyable_notional_ratio": row.copyable_notional_ratio,
                    "status": row.status,
                }
            )
        elif self._discovery is not None:
            profile = self._discovery.get_cached_profile(wallet_l)
            if profile is not None:
                discovery_score = float(profile.composite_score)
                details.update(
                    {
                        "discovery_pnl": profile.overall_gain,
                        "discovery_win_rate": profile.win_rate,
                        "discovery_positions": profile.total_positions,
                        "discovery_rank": profile.rank,
                    }
                )

        score = (
            0.70 * discovery_score + 0.30 * runtime_score
            if discovery_score > 0
            else runtime_score
        )
        final = _clamp(max(score, float(self._score_floor)))
        activity_score = _clamp(
            runtime_activity
            / max(int(self._config.leader.leader_score_activity_target), 1)
        )
        return LeaderScore(
            wallet=wallet_l,
            score=final,
            balance_score=0.0,
            analytics_score=runtime_score,
            activity_score=activity_score,
            discovery_score=discovery_score,
            sample_size=runtime_activity,
            details=details,
        )

    def _runtime_score(
        self, pnl: float, win_rate: float, recent_trade_count: int
    ) -> float:
        wr = win_rate / 100.0 if win_rate > 1.0 else win_rate
        wr_score = _clamp(wr)
        pnl_score = 0.5 + _clamp(pnl / 100.0, -0.5, 0.5)
        activity_score = _clamp(
            recent_trade_count
            / max(int(self._config.leader.leader_score_activity_target), 1)
        )
        return _clamp(0.50 * wr_score + 0.35 * pnl_score + 0.15 * activity_score)

    def trust_multiplier(self, score: float) -> float:
        if not self._trust_mult_enabled:
            return 1.0
        score = _clamp(score)
        return self._trust_mult_min + (self._trust_mult_max - self._trust_mult_min) * score

