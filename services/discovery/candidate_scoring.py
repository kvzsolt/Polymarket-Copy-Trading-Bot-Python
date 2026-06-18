from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any

from services.discovery.candidate_classifier import (
    MarketClass,
    is_fast_crypto_specialist,
)


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def normalize(value: float, low: float, high: float) -> float:
    if high <= low:
        return 0.0
    return clamp((float(value) - low) / (high - low))


@dataclass(frozen=True)
class RecentState:
    pnl_7d: Decimal = Decimal("0")
    pnl_3d: Decimal = Decimal("0")
    available: bool = True


@dataclass(frozen=True)
class FalconDiscoveryEvaluation:
    wallet: str
    status: str
    final_score: float
    falcon_quality_score: float
    copyability_score: float
    recent_state_score: float
    market_fit_score: float
    h_score: float
    roi_15d: float
    win_rate_15d: float
    sharpe_15d: float
    pnl_15d: float
    total_trades_15d: int
    markets_traded_15d: int
    trajectory: str
    total_grouped_signals: int
    meaningful_signals: int
    copyable_signals: int
    copyable_signal_ratio: float
    copyable_notional_ratio: float
    sports_esports_ratio: float
    fast_crypto_signal_ratio: float
    fast_crypto_notional_ratio: float
    pnl_7d: Decimal
    pnl_3d: Decimal
    rejection_reason: str | None
    eligible_live: bool
    eligible_shadow: bool
    balance_usdc: str = "0"
    config_hash: str = ""
    evaluation_context_json: dict[str, Any] = field(default_factory=dict)
    falcon_raw_json: dict[str, Any] = field(default_factory=dict)
    replay_json: dict[str, Any] = field(default_factory=dict)
    updated_at: float = field(default_factory=time.time)


class CandidateScoringService:
    def __init__(self, config):
        self.config = config

    async def evaluate(
        self,
        *,
        candidate,
        signals: list,
        market_classes: list[MarketClass],
        replay,
        recent_state: RecentState | None = None,
    ) -> FalconDiscoveryEvaluation:
        recent_state = recent_state or RecentState(available=False)
        falcon_score = falcon_quality_score(candidate)
        copy_score = copyability_score(replay)
        recent_score = recent_state_score(recent_state)
        fit_score = market_fit_score(market_classes)
        final_score = clamp(
            0.50 * falcon_score
            + 0.30 * copy_score
            + 0.15 * recent_score
            + 0.05 * fit_score
        )

        rejection_reason = self._rejection_reason(
            signals=signals,
            market_classes=market_classes,
            replay=replay,
            recent_state=recent_state,
        )
        score_ok = final_score >= self.config.leader.auto_discover_min_score
        if rejection_reason is None and not score_ok:
            rejection_reason = "below_live_score"

        eligible_live = rejection_reason is None and score_ok
        eligible_shadow = (not eligible_live) and (
            rejection_reason is None
            or rejection_reason
            in {
                "below_live_score",
                "insufficient_copyable_signal_ratio",
                "insufficient_copyable_notional_ratio",
                "recent_state_unavailable",
            }
        )
        if eligible_live:
            status = "live_eligible"
        elif eligible_shadow:
            status = "shadow"
        else:
            status = "rejected"

        sports_ratio = (
            sum(1 for c in market_classes if c.is_sports_esports) / len(market_classes)
            if market_classes
            else 0.0
        )
        fast_crypto_signal_ratio = (
            sum(1 for c in market_classes if c.is_fast_crypto) / len(market_classes)
            if market_classes
            else 0.0
        )
        fast_crypto_notional_ratio = _fast_crypto_notional_ratio(signals, market_classes)

        replay_json = _replay_to_json(replay)
        replay_details = getattr(replay, "details", {}) or {}
        return FalconDiscoveryEvaluation(
            wallet=candidate.wallet.lower(),
            status=status,
            final_score=final_score,
            falcon_quality_score=falcon_score,
            copyability_score=copy_score,
            recent_state_score=recent_score,
            market_fit_score=fit_score,
            h_score=float(candidate.h_score),
            roi_15d=float(candidate.roi_15d),
            win_rate_15d=float(candidate.win_rate_15d),
            sharpe_15d=float(candidate.sharpe_15d),
            pnl_15d=float(candidate.pnl_15d),
            total_trades_15d=int(candidate.total_trades_15d),
            markets_traded_15d=int(candidate.markets_traded_15d),
            trajectory=str(candidate.trajectory),
            total_grouped_signals=int(len(signals)),
            meaningful_signals=int(replay.meaningful_signals),
            copyable_signals=int(replay.copyable_signals),
            copyable_signal_ratio=float(replay.copyable_signal_ratio),
            copyable_notional_ratio=float(replay.copyable_notional_ratio),
            sports_esports_ratio=float(sports_ratio),
            fast_crypto_signal_ratio=float(fast_crypto_signal_ratio),
            fast_crypto_notional_ratio=float(fast_crypto_notional_ratio),
            pnl_7d=recent_state.pnl_7d,
            pnl_3d=recent_state.pnl_3d,
            rejection_reason=rejection_reason,
            eligible_live=eligible_live,
            eligible_shadow=eligible_shadow,
            balance_usdc=str(replay_details.get("balance_usdc", "0") or "0"),
            config_hash=str(replay_details.get("config_hash", "") or ""),
            evaluation_context_json=dict(
                replay_details.get("evaluation_context", {}) or {}
            ),
            falcon_raw_json=dict(candidate.raw),
            replay_json=replay_json,
        )

    def _rejection_reason(
        self,
        *,
        signals: list,
        market_classes: list[MarketClass],
        replay,
        recent_state: RecentState,
    ) -> str | None:
        if is_fast_crypto_specialist(market_classes, signals, self.config):
            return "fast_crypto_specialist"
        if (
            replay.copyable_signal_ratio
            < self.config.discovery.discovery_min_copyable_grouped_signal_ratio
        ):
            return "insufficient_copyable_signal_ratio"
        if (
            replay.copyable_notional_ratio
            < self.config.discovery.discovery_min_copyable_notional_ratio
        ):
            return "insufficient_copyable_notional_ratio"
        ok, reason = recent_state_ok(recent_state, self.config)
        if not ok:
            return reason
        return None


def falcon_quality_score(candidate) -> float:
    h = normalize(candidate.h_score, 0, 100)
    roi = normalize(candidate.roi_15d, -50, 200)
    win = normalize(candidate.win_rate_15d, 35, 75)
    sharpe = normalize(candidate.sharpe_15d, -1, 5)
    traj = {"improving": 1.0, "stable": 0.7, "declining": 0.2}.get(
        str(candidate.trajectory).lower(), 0.5
    )
    return clamp(0.50 * h + 0.20 * roi + 0.15 * win + 0.10 * sharpe + 0.05 * traj)


def copyability_score(replay) -> float:
    return clamp(
        0.60 * replay.copyable_signal_ratio + 0.40 * replay.copyable_notional_ratio
    )


def recent_state_ok(state: RecentState, config) -> tuple[bool, str | None]:
    if (
        getattr(config.discovery, "discovery_require_recent_state_for_live", True)
        and not state.available
    ):
        return False, "recent_state_unavailable"
    if getattr(config.discovery, "discovery_require_7d_non_negative", True):
        if state.pnl_7d < Decimal(
            str(getattr(config.discovery, "discovery_min_7d_pnl_usdc", 0))
        ):
            return False, "negative_7d_state"
    if state.pnl_3d < Decimal(
        str(getattr(config.discovery, "discovery_min_3d_pnl_usdc", -50))
    ):
        return False, "bad_3d_state"
    return True, None


def recent_state_score(state: RecentState) -> float:
    pnl_7d_score = 0.5 + clamp(float(state.pnl_7d) / 500.0, -0.5, 0.5)
    pnl_3d_score = 0.5 + clamp(float(state.pnl_3d) / 200.0, -0.5, 0.5)
    return clamp(0.70 * pnl_7d_score + 0.30 * pnl_3d_score)


def market_fit_score(classes: list[MarketClass]) -> float:
    if not classes:
        return 0.0
    sports = sum(1 for c in classes if c.is_sports_esports) / len(classes)
    non_crypto = sum(1 for c in classes if not c.is_crypto) / len(classes)
    non_politics = sum(1 for c in classes if not c.is_politics_macro) / len(classes)
    return clamp(0.50 * sports + 0.30 * non_crypto + 0.20 * non_politics)


def _fast_crypto_notional_ratio(signals: list, classes: list[MarketClass]) -> float:
    fast_crypto_notional = sum(
        (s.total_usdc_size for s, c in zip(signals, classes) if c.is_fast_crypto),
        start=Decimal("0"),
    )
    total_notional = sum((s.total_usdc_size for s in signals), start=Decimal("0"))
    return float(fast_crypto_notional / total_notional) if total_notional > 0 else 0.0


def _replay_to_json(replay) -> dict[str, Any]:
    return json.loads(json.dumps(asdict(replay), default=str))
