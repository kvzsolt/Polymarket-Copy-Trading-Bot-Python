"""Entry permission service — the single authoritative policy gate for BUY re-entry.

Every BUY path in the trade pipeline must call this service before submitting an order.
The service owns the full answer to: "may we re-enter this exact position right now?"

It reads the risk-block model, evaluates ordered clear rules, and returns a structured
``EntryDecision`` that callers can log, metric-increment, and assert in tests.

Clear rules (evaluated in order before denying):
    1. ``cooldown_expired``    — block's ``expires_at`` has passed.
                                 (handled transparently by the repo query)
    2. ``operator_cleared``    — explicit ``clear_risk_block()`` call set ``cleared_at``.
                                 (handled transparently by the repo query)
    3. ``leader_changed``      — the inbound signal comes from a different leader than
                                 the one who anchored the block (leader_anchor mismatch).
    4. ``fresh_leader_buy_after_net_flat`` — leader went to net-zero after the protective
                                 exit and has now re-entered with a fresh BUY.
                                 Detected by comparing ``leader_notional_at_trigger``
                                 stored in block metadata against the caller-supplied
                                 current leader notional.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from storage.sqlite.risk_blocks_repo import RiskBlocksRepo

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public result type
# ---------------------------------------------------------------------------

REASON_ALLOWED = "allowed_no_block"
REASON_CLEARED = "allowed_block_cleared"
REASON_DENIED = "denied_risk_block_active"


@dataclass
class EntryDecision:
    """Result of a single entry-permission check.

    Attributes:
        allowed: True if the BUY may proceed.
        reason: A short machine-readable code such as ``allowed_no_block``,
            ``allowed_block_cleared``, or ``denied_risk_block_active``.
        deny_detail: Human-readable detail of why the block is still active
            (populated only when ``allowed`` is False).
        clear_rule_applied: Which clear rule fired (populated only when the
            service auto-cleared a stale block before allowing entry).
        block_record: The raw block dict as returned by ``get_active_risk_block``,
            populated for any non-trivial decision so callers can log context.
    """

    allowed: bool
    reason: str
    deny_detail: str | None = None
    clear_rule_applied: str | None = None
    block_record: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class EntryPermissionService:
    """Authoritative BUY entry-permission gate.

    Usage::

        service = EntryPermissionService(risk_blocks_repo=ctx.risk_blocks_repo)
        decision = service.check_entry(
            user_id=ctx.user_id,
            condition_id=signal.condition_id,
            token_id=signal.token_id,
            leader_id=leader_id,
            leader_notional_usdc=float(signal.usdc_size),
        )
        if not decision.allowed:
            # deny the trade …

    The service never raises — it always returns an ``EntryDecision``.
    """

    def __init__(self, risk_blocks_repo: "RiskBlocksRepo") -> None:
        self._repo = risk_blocks_repo

    # ------------------------------------------------------------------
    # Main API
    # ------------------------------------------------------------------

    def check_entry(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        leader_id: str | None = None,
        leader_notional_usdc: float | None = None,
    ) -> EntryDecision:
        """Return an :class:`EntryDecision` for the given scoped position.

        Parameters:
            user_id:              Copying user.
            condition_id:         Polymarket condition ID.
            token_id:             Outcome token ID.
            leader_id:            Leader whose signal triggered this check.
                                  Used for the ``leader_changed`` clear rule.
            leader_notional_usdc: Current leader position size in USD, used
                                  for the ``fresh_leader_buy_after_net_flat``
                                  clear rule.  Pass ``None`` to skip the rule.
        """
        block = self._repo.get_active_risk_block(
            user_id=user_id,
            condition_id=condition_id,
            token_id=token_id,
        )

        if block is None:
            return EntryDecision(allowed=True, reason=REASON_ALLOWED)

        # --- evaluate auto-clear rules before denying -------------------
        clear_rule = self._evaluate_clear_rules(
            block=block,
            leader_id=leader_id,
            leader_notional_usdc=leader_notional_usdc,
        )
        if clear_rule is not None:
            cleared = self._repo.clear_risk_block(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                clear_reason=clear_rule,
            )
            if cleared:
                logger.info(
                    "entry_permission | auto-cleared block | user=%s condition=%s token=%s rule=%s",
                    user_id,
                    condition_id[:16],
                    token_id[:16],
                    clear_rule,
                )
                return EntryDecision(
                    allowed=True,
                    reason=REASON_CLEARED,
                    clear_rule_applied=clear_rule,
                    block_record=block,
                )

        # --- block still active: deny -----------------------------------
        deny_detail = str(block.get("reason") or "risk_block_active")
        logger.debug(
            "entry_permission | denied | user=%s condition=%s token=%s detail=%s",
            user_id,
            condition_id[:16],
            token_id[:16],
            deny_detail,
        )
        return EntryDecision(
            allowed=False,
            reason=REASON_DENIED,
            deny_detail=deny_detail,
            block_record=block,
        )

    # ------------------------------------------------------------------
    # Clear-rule evaluation
    # ------------------------------------------------------------------

    def _evaluate_clear_rules(
        self,
        block: dict[str, Any],
        leader_id: str | None,
        leader_notional_usdc: float | None,
    ) -> str | None:
        """Return the first matching clear-rule name, or ``None``.

        Clear rules are checked in priority order.  The first match wins.
        """
        # Rule 1: leader_changed
        # A different leader (not the one who anchored the block) is sending a
        # fresh BUY signal.  The block was scoped to the old leader's regime.
        if self._rule_leader_changed(block, leader_id):
            return "leader_changed"

        # Rule 2: fresh_leader_buy_after_net_flat
        # The same leader went net-zero after triggering the block and is now
        # re-buying, indicating a fresh position intent.
        if self._rule_fresh_leader_buy(block, leader_notional_usdc):
            return "fresh_leader_buy_after_net_flat"

        return None

    # ------------------------------------------------------------------
    # Individual clear rules
    # ------------------------------------------------------------------

    @staticmethod
    def _rule_leader_changed(block: dict[str, Any], leader_id: str | None) -> bool:
        """Fire when the inbound signal is from a different leader than the anchor."""
        if not leader_id:
            return False
        anchor = block.get("leader_anchor")
        if not anchor:
            # No anchor stored — cannot apply this rule safely.
            return False
        return anchor != leader_id

    @staticmethod
    def _rule_fresh_leader_buy(
        block: dict[str, Any],
        leader_notional_usdc: float | None,
    ) -> bool:
        """Fire when the leader went net-flat after the block and now has a fresh BUY.

        Requires ``leader_notional_at_trigger`` to have been stored in block metadata
        by the risk-monitor at block creation time.
        """
        if leader_notional_usdc is None:
            return False
        metadata = block.get("metadata") or {}
        trigger_notional_raw = metadata.get("leader_notional_at_trigger")
        if trigger_notional_raw is None:
            return False
        try:
            trigger_notional = float(trigger_notional_raw)
        except (TypeError, ValueError):
            return False
        # Leader had size > 0 at trigger, went to 0, and now has a fresh BUY
        return trigger_notional > 0 and leader_notional_usdc > 0 and leader_notional_usdc < trigger_notional * 0.1

    # ------------------------------------------------------------------
    # Convenience helpers for callers
    # ------------------------------------------------------------------

    def is_blocked(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
    ) -> bool:
        """Quick boolean probe without attempting any auto-clear."""
        return (
            self._repo.get_active_risk_block(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
            )
            is not None
        )
