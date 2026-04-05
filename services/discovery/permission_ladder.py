"""Permission ladder: watchlist → paper_follow → live_follow.

Leaders progress through tiers based on composite score thresholds and
minimum time-in-state. Demotion happens when score drops below thresholds.

This module is policy only — it does NOT execute trades or manage state
persistence (that's the caller's job via leaders_repo).

Tier rules:
- WATCHLIST: Default tier. Leader is tracked but not followed.
- PAPER_FOLLOW: Leader's signals are evaluated as-if trades but not executed.
  Requires score >= promote_to_paper threshold.
- LIVE_FOLLOW: Leader's signals are executed as real trades.
  Requires score >= promote_to_live threshold AND time in PAPER_FOLLOW
  >= min_paper_seconds.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class FollowTier(Enum):
    """Leader follow permission tier."""
    WATCHLIST = "watchlist"
    PAPER_FOLLOW = "paper_follow"
    LIVE_FOLLOW = "live_follow"


# Order for comparisons
_TIER_ORDER = {
    FollowTier.WATCHLIST: 0,
    FollowTier.PAPER_FOLLOW: 1,
    FollowTier.LIVE_FOLLOW: 2,
}


@dataclass(frozen=True)
class PromotionThresholds:
    """Configurable thresholds for tier promotion/demotion."""
    promote_to_paper: float = 0.40
    promote_to_live: float = 0.55
    demote_to_paper: float = 0.35  # hysteresis below promote_to_live
    demote_to_watchlist: float = 0.25
    min_paper_seconds: int = 86400  # 24h default
    min_scored_positions: int = 2


@dataclass(frozen=True)
class TierDecision:
    """Result of a tier evaluation."""
    leader_id: str
    current_tier: FollowTier
    recommended_tier: FollowTier
    reason: str
    changed: bool

    @property
    def promoted(self) -> bool:
        return _TIER_ORDER[self.recommended_tier] > _TIER_ORDER[self.current_tier]

    @property
    def demoted(self) -> bool:
        return _TIER_ORDER[self.recommended_tier] < _TIER_ORDER[self.current_tier]


def evaluate_tier(
    leader_id: str,
    current_tier: FollowTier,
    aggregate_score: float | None,
    has_sufficient_coverage: bool,
    seconds_in_current_tier: int,
    thresholds: PromotionThresholds | None = None,
) -> TierDecision:
    """Evaluate whether a leader should change tiers.

    Parameters
    ----------
    leader_id:
        Leader wallet address.
    current_tier:
        Current permission tier.
    aggregate_score:
        Trader-level aggregate score, or None if not scoreable.
    has_sufficient_coverage:
        Whether enough positions were scored for reliable aggregate.
    seconds_in_current_tier:
        How long the leader has been in the current tier.
    thresholds:
        Configurable promotion/demotion thresholds.
    """
    t = thresholds or PromotionThresholds()

    # No score → can only be watchlist
    if aggregate_score is None:
        if current_tier != FollowTier.WATCHLIST:
            return TierDecision(
                leader_id=leader_id,
                current_tier=current_tier,
                recommended_tier=FollowTier.WATCHLIST,
                reason="no_score_available",
                changed=True,
            )
        return TierDecision(
            leader_id=leader_id,
            current_tier=current_tier,
            recommended_tier=FollowTier.WATCHLIST,
            reason="no_score_available",
            changed=False,
        )

    # Insufficient coverage → cannot promote beyond watchlist
    if not has_sufficient_coverage:
        if current_tier != FollowTier.WATCHLIST:
            return TierDecision(
                leader_id=leader_id,
                current_tier=current_tier,
                recommended_tier=FollowTier.WATCHLIST,
                reason="insufficient_coverage",
                changed=True,
            )
        return TierDecision(
            leader_id=leader_id,
            current_tier=current_tier,
            recommended_tier=FollowTier.WATCHLIST,
            reason="insufficient_coverage",
            changed=False,
        )

    # --- Demotion checks (from highest tier downward) ---
    if current_tier == FollowTier.LIVE_FOLLOW:
        if aggregate_score < t.demote_to_paper:
            return TierDecision(
                leader_id=leader_id,
                current_tier=current_tier,
                recommended_tier=FollowTier.PAPER_FOLLOW,
                reason=f"score {aggregate_score:.3f} < demote_to_paper {t.demote_to_paper}",
                changed=True,
            )
        if aggregate_score < t.demote_to_watchlist:
            return TierDecision(
                leader_id=leader_id,
                current_tier=current_tier,
                recommended_tier=FollowTier.WATCHLIST,
                reason=f"score {aggregate_score:.3f} < demote_to_watchlist {t.demote_to_watchlist}",
                changed=True,
            )
        # Stay live
        return TierDecision(
            leader_id=leader_id,
            current_tier=current_tier,
            recommended_tier=FollowTier.LIVE_FOLLOW,
            reason="score_adequate",
            changed=False,
        )

    if current_tier == FollowTier.PAPER_FOLLOW:
        if aggregate_score < t.demote_to_watchlist:
            return TierDecision(
                leader_id=leader_id,
                current_tier=current_tier,
                recommended_tier=FollowTier.WATCHLIST,
                reason=f"score {aggregate_score:.3f} < demote_to_watchlist {t.demote_to_watchlist}",
                changed=True,
            )
        # Promotion to live?
        if (
            aggregate_score >= t.promote_to_live
            and seconds_in_current_tier >= t.min_paper_seconds
        ):
            return TierDecision(
                leader_id=leader_id,
                current_tier=current_tier,
                recommended_tier=FollowTier.LIVE_FOLLOW,
                reason=f"score {aggregate_score:.3f} >= {t.promote_to_live} and "
                       f"paper_time {seconds_in_current_tier}s >= {t.min_paper_seconds}s",
                changed=True,
            )
        if aggregate_score >= t.promote_to_live and seconds_in_current_tier < t.min_paper_seconds:
            return TierDecision(
                leader_id=leader_id,
                current_tier=current_tier,
                recommended_tier=FollowTier.PAPER_FOLLOW,
                reason=f"score qualifies but paper_time {seconds_in_current_tier}s < {t.min_paper_seconds}s",
                changed=False,
            )
        # Stay paper
        return TierDecision(
            leader_id=leader_id,
            current_tier=current_tier,
            recommended_tier=FollowTier.PAPER_FOLLOW,
            reason="score_adequate",
            changed=False,
        )

    # current_tier == WATCHLIST
    if aggregate_score >= t.promote_to_paper:
        return TierDecision(
            leader_id=leader_id,
            current_tier=current_tier,
            recommended_tier=FollowTier.PAPER_FOLLOW,
            reason=f"score {aggregate_score:.3f} >= promote_to_paper {t.promote_to_paper}",
            changed=True,
        )

    return TierDecision(
        leader_id=leader_id,
        current_tier=current_tier,
        recommended_tier=FollowTier.WATCHLIST,
        reason=f"score {aggregate_score:.3f} < promote_to_paper {t.promote_to_paper}",
        changed=False,
    )
