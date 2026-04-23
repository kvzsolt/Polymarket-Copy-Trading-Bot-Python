"""Tests for EntryPermissionService — Issue #25.

Covers:
- allow when no block exists
- deny when block is active
- auto-clear on leader_changed
- auto-clear on fresh_leader_buy_after_net_flat
- deny when metadata missing (no spurious clears)
- cooldown expiry is handled by the repo layer (contract test)
- is_blocked() helper
"""
from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import MagicMock, call

from services.entryPermissionService import (
    REASON_ALLOWED,
    REASON_CLEARED,
    REASON_DENIED,
    EntryPermissionService,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_block(
    *,
    reason: str = "stop_loss",
    source: str = "risk_monitor",
    leader_anchor: str | None = "leader-A",
    metadata: dict | None = None,
    expires_at: float | None = None,
) -> dict:
    return {
        "user_id": "user1",
        "condition_id": "cond-1",
        "token_id": "tok-1",
        "reason": reason,
        "source": source,
        "triggered_at": time.time() - 60,
        "expires_at": expires_at,
        "cleared_at": None,
        "clear_reason": None,
        "leader_anchor": leader_anchor,
        "metadata": metadata or {},
    }


def _make_repo(block: dict | None) -> MagicMock:
    repo = MagicMock()
    repo.get_active_risk_block.return_value = block
    repo.clear_risk_block.return_value = True
    return repo



def _check_entry(svc: EntryPermissionService, **kwargs):
    return asyncio.run(svc.check_entry(**kwargs))


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestEntryPermissionServiceAllowed(unittest.TestCase):
    def test_no_block_returns_allowed(self):
        repo = _make_repo(block=None)
        svc = EntryPermissionService(risk_blocks_repo=repo)
        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
        )
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, REASON_ALLOWED)
        self.assertIsNone(decision.deny_detail)
        self.assertIsNone(decision.clear_rule_applied)
        self.assertIsNone(decision.block_record)


class TestEntryPermissionServiceDenied(unittest.TestCase):
    def test_active_block_no_clear_rules_returns_denied(self):
        block = _make_block(reason="stop_loss", leader_anchor="leader-A", metadata={})
        repo = _make_repo(block=block)
        svc = EntryPermissionService(risk_blocks_repo=repo)
        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
            leader_id="leader-A",          # same leader → no leader_changed
            leader_notional_usdc=50.0,     # no metadata → fresh_leader_buy can't fire
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, REASON_DENIED)
        self.assertEqual(decision.deny_detail, "stop_loss")
        self.assertIsNone(decision.clear_rule_applied)
        # clear_risk_block must NOT be called
        repo.clear_risk_block.assert_not_called()

    def test_denied_without_leader_id(self):
        block = _make_block()
        repo = _make_repo(block=block)
        svc = EntryPermissionService(risk_blocks_repo=repo)
        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, REASON_DENIED)


class TestLeaderChangedClearRule(unittest.TestCase):
    def test_different_leader_auto_clears(self):
        block = _make_block(leader_anchor="leader-OLD")
        repo = _make_repo(block=block)
        svc = EntryPermissionService(risk_blocks_repo=repo)

        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
            leader_id="leader-NEW",
        )
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, REASON_CLEARED)
        self.assertEqual(decision.clear_rule_applied, "leader_changed")
        repo.clear_risk_block.assert_called_once_with(
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
            clear_reason="leader_changed",
        )

    def test_same_leader_does_not_clear(self):
        block = _make_block(leader_anchor="leader-A")
        repo = _make_repo(block=block)
        svc = EntryPermissionService(risk_blocks_repo=repo)

        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
            leader_id="leader-A",
        )
        self.assertFalse(decision.allowed)
        repo.clear_risk_block.assert_not_called()

    def test_no_anchor_stored_does_not_fire(self):
        """Without a leader_anchor stored, the rule cannot fire safely."""
        block = _make_block(leader_anchor=None)
        repo = _make_repo(block=block)
        svc = EntryPermissionService(risk_blocks_repo=repo)

        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
            leader_id="leader-ANY",
        )
        self.assertFalse(decision.allowed)
        repo.clear_risk_block.assert_not_called()


class TestFreshLeaderBuyClearRule(unittest.TestCase):
    def _block_with_trigger_notional(self, trigger_notional: float) -> dict:
        return _make_block(
            leader_anchor="leader-A",
            metadata={"leader_notional_at_trigger": trigger_notional},
        )

    def test_clears_when_leader_went_flat_and_fresh_buy(self):
        """
        At trigger time leader had 100 USDC.
        Now they have 5 USDC (< 10% of trigger) → fresh BUY after net-flat.
        """
        block = self._block_with_trigger_notional(100.0)
        repo = _make_repo(block=block)
        svc = EntryPermissionService(risk_blocks_repo=repo)

        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
            leader_id="leader-A",
            leader_notional_usdc=5.0,  # < 10% of 100
        )
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, REASON_CLEARED)
        self.assertEqual(decision.clear_rule_applied, "fresh_leader_buy_after_net_flat")

    def test_does_not_clear_when_leader_still_has_large_position(self):
        """Leader still holds 80 USDC — not a fresh entry."""
        block = self._block_with_trigger_notional(100.0)
        repo = _make_repo(block=block)
        svc = EntryPermissionService(risk_blocks_repo=repo)

        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
            leader_id="leader-A",
            leader_notional_usdc=80.0,  # still > 10% of 100
        )
        self.assertFalse(decision.allowed)

    def test_does_not_clear_when_metadata_missing(self):
        """No metadata stored → rule cannot evaluate, must not clear."""
        block = _make_block(leader_anchor="leader-A", metadata={})
        repo = _make_repo(block=block)
        svc = EntryPermissionService(risk_blocks_repo=repo)

        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
            leader_id="leader-A",
            leader_notional_usdc=5.0,
        )
        self.assertFalse(decision.allowed)

    def test_does_not_clear_when_notional_not_supplied(self):
        block = self._block_with_trigger_notional(100.0)
        repo = _make_repo(block=block)
        svc = EntryPermissionService(risk_blocks_repo=repo)

        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
            leader_id="leader-A",
            leader_notional_usdc=None,
        )
        self.assertFalse(decision.allowed)


class TestLeaderChangedTakesPriorityOverFreshBuy(unittest.TestCase):
    def test_leader_changed_fires_first(self):
        """leader_changed has higher priority than fresh_leader_buy."""
        block = _make_block(
            leader_anchor="leader-OLD",
            metadata={"leader_notional_at_trigger": 100.0},
        )
        repo = _make_repo(block=block)
        svc = EntryPermissionService(risk_blocks_repo=repo)

        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
            leader_id="leader-NEW",
            leader_notional_usdc=5.0,
        )
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.clear_rule_applied, "leader_changed")


class TestCooldownExpiry(unittest.TestCase):
    def test_expired_block_not_returned_by_repo(self):
        """
        Contract test: the repo's get_active_risk_block filters expired blocks.
        When the repo returns None the service must allow entry.
        """
        repo = _make_repo(block=None)  # repo filters expired block → returns None
        svc = EntryPermissionService(risk_blocks_repo=repo)

        decision = _check_entry(svc, 
            user_id="user1",
            condition_id="cond-1",
            token_id="tok-1",
        )
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, REASON_ALLOWED)


class TestIsBlockedHelper(unittest.TestCase):
    def test_returns_true_when_block_exists(self):
        repo = _make_repo(block=_make_block())
        svc = EntryPermissionService(risk_blocks_repo=repo)
        self.assertTrue(svc.is_blocked(user_id="user1", condition_id="cond-1", token_id="tok-1"))

    def test_returns_false_when_no_block(self):
        repo = _make_repo(block=None)
        svc = EntryPermissionService(risk_blocks_repo=repo)
        self.assertFalse(svc.is_blocked(user_id="user1", condition_id="cond-1", token_id="tok-1"))


if __name__ == "__main__":
    unittest.main()
