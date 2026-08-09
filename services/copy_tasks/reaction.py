"""Kreo-compatible source-trade reaction strategies.

The engine is pure: it translates an immutable source trade into a follower
action using the task reaction policy. It performs no I/O and mutates no
state.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from services.copy_tasks.domain import CopySideFilter, ReactionPolicy, SourceTrade


class ReactionAction(StrEnum):
    """Follower action produced by a task reaction policy."""

    BUY = "BUY"
    SELL = "SELL"
    SKIP = "SKIP"


@dataclass(frozen=True, slots=True)
class ReactionDecision:
    """Pure translation from source action into follower action."""

    action: ReactionAction
    outcome_label: str
    reason: str


class CopyReactionEngine:
    """Translate leader actions while keeping every switch explicit."""

    def decide(self, policy: ReactionPolicy, trade: SourceTrade) -> ReactionDecision:
        """Return the follower reaction for one normalized source trade.

        Args:
            policy: Immutable reaction policy.
            trade: Vendor-neutral source trade.

        Returns:
            Immutable reaction decision with follower action and reason.
        """

        if not policy.enabled:
            return ReactionDecision(
                ReactionAction.SKIP,
                trade.outcome_label,
                "reaction_disabled",
            )

        source_outcome = trade.outcome_label.strip()
        target_outcome = (
            self._opposite(source_outcome) if policy.counter_trade else source_outcome
        )
        if not self._side_allowed(policy.copy_side, target_outcome):
            return ReactionDecision(
                ReactionAction.SKIP,
                target_outcome,
                "copy_side_filtered",
            )

        if trade.action == "BUY":
            if not policy.follow_buys:
                return ReactionDecision(
                    ReactionAction.SKIP,
                    target_outcome,
                    "follow_buys_disabled",
                )
            return ReactionDecision(ReactionAction.BUY, target_outcome, "follow_buy")

        if policy.buy_on_sells:
            return ReactionDecision(ReactionAction.BUY, target_outcome, "buy_on_sell")
        if policy.copy_sells:
            return ReactionDecision(ReactionAction.SELL, target_outcome, "copy_sell")
        return ReactionDecision(
            ReactionAction.SKIP,
            target_outcome,
            "sell_reaction_disabled",
        )

    @staticmethod
    def _opposite(outcome: str) -> str:
        normalized = outcome.strip().casefold()
        if normalized == "yes":
            return "NO"
        if normalized == "no":
            return "YES"
        raise ValueError("counter trade requires a binary YES/NO market")

    @staticmethod
    def _side_allowed(policy: CopySideFilter, outcome: str) -> bool:
        normalized = outcome.strip().casefold()
        if policy is CopySideFilter.BOTH:
            return True
        if policy is CopySideFilter.YES_ONLY:
            return normalized == "yes"
        return normalized == "no"


__all__ = ["CopyReactionEngine", "ReactionAction", "ReactionDecision"]