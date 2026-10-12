"""Phase 2.8 — near-max trim must have an entry-time grace period.

The audit found the near-max trim/exit fires instantly with no entry-time
grace (only stop-loss has one). A fresh BUY at the ask with the risk mark at
the best bid satisfies the trim condition at the instant of the fill, selling
back across the spread for an instant loss. Add a min-hold grace like
stop-loss has so a freshly entered position is not trimmed before it has had
a chance to settle.
"""

from __future__ import annotations

import time


from pmcopy.services.positionManager import PositionManager


def _make_pm(*, near_max_grace_seconds: float = 30.0) -> PositionManager:
    pm = PositionManager()  # bare defaults branch
    pm._risk_master_enabled = True
    pm._enabled = True
    pm._enable_near_max = True
    pm._near_max_exit = 0.99
    pm._near_max_warn = 0.95
    pm._near_max_warn_ratio = 0.6
    pm._near_max_grace_period_seconds = near_max_grace_seconds
    # Disable other exits so only near-max is evaluated.
    pm._enable_stop_loss = False
    pm._profit_lock_mode = "disabled"
    pm._enable_trailing = False
    pm._enable_staged_tp = False
    pm._enable_leader_divergence = False
    return pm


def _add_position(pm, *, entry_price=0.50, size=100.0, cid="cond-1", entry_time=None):
    pm.track_buy(
        condition_id=cid,
        asset="token-1",
        entry_price=entry_price,
        size_tokens=size,
        usdc_cost=entry_price * size,
        leader_wallet="0x" + "1" * 40,
    )
    if entry_time is not None:
        # Force the tracked position's entry_time for the grace calculation.
        tracked = pm.get_position(cid, "token-1")
        if tracked is not None:
            tracked.entry_time = entry_time
    return cid


def test_near_max_warn_trim_suppressed_within_grace_period():
    """A freshly entered position (age < grace) at price >= near_max_warn must
    NOT trim — the bid/ask spread asymmetry would sell back across the spread
    for an instant loss."""
    pm = _make_pm(near_max_grace_seconds=30.0)
    now = time.time()
    cid = _add_position(pm, entry_price=0.50, entry_time=now)

    action = pm.evaluate_risk_action(cid, 0.96)

    assert action is None or action.get("reason") != "near_max_partial_trim", (
        f"near-max trim must not fire within the grace period, got {action}"
    )


def test_near_max_warn_trim_fires_after_grace_period():
    """After the grace period elapses, the near-max warn trim fires normally."""
    pm = _make_pm(near_max_grace_seconds=30.0)
    now = time.time()
    # Entry 60s ago — past the 30s grace.
    cid = _add_position(pm, entry_price=0.50, entry_time=now - 60.0)

    action = pm.evaluate_risk_action(cid, 0.96)

    assert action is not None
    assert action["reason"] == "near_max_partial_trim"


def test_near_max_full_exit_still_fires_within_grace():
    """The full exit at the exit threshold (0.99) is a protective cap, not a
    scalp-trim — it must still fire within the grace period to cap a position
    that has already reached near-resolution. Only the partial trim is
    grace-protected."""
    pm = _make_pm(near_max_grace_seconds=30.0)
    now = time.time()
    cid = _add_position(pm, entry_price=0.50, entry_time=now)

    action = pm.evaluate_risk_action(cid, 0.99)

    assert action is not None
    assert action["reason"] == "near_max_full_exit"
