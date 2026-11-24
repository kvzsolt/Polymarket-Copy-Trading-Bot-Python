"""Regression: MarketStateStore.invalidate_sizing_decision actually exists +
clears tick / min_order_size.

Pre-fix the method did not exist on MarketStateStore.
``clients/ws/market_ws.py:_handle_tick_size_change`` invoked it via
``_call_cache_optional("invalidate_sizing_decision", asset_id)``, which
silently returned False — meaning a tick-size-change event did NOT
invalidate any sizing decision derived from the old tick. A SELL whose
size was quantized to the previous tick could submit off-tick after the
event landed.

Post-fix: method exists and clears tick_size + min_order_size on the
token state, marks stale+dirty with a documented reason.
"""

from __future__ import annotations

from decimal import Decimal

from pmcopy.cache.market_state_store import MarketStateStore


def test_method_exists_and_marks_dirty_without_clearing_fresh_tick():
    """Contract: the WSS handler has already landed the NEW tick before
    invalidate_sizing_decision runs. The method must NOT wipe that
    fresh tick; it must mark the state dirty with a sizing-specific
    reason so sizing-derived caches can attribute the freshness signal.
    """
    store = MarketStateStore()
    store.on_tick_size_change("tok-1", Decimal("0.01"))
    state = store.get_state("tok-1")
    assert state is not None
    assert state.tick_size == Decimal("0.01")

    store.invalidate_sizing_decision("tok-1")

    state_after = store.get_state("tok-1")
    assert state_after is not None
    # Fresh tick MUST survive — only sizing-derived state is invalidated.
    assert state_after.tick_size == Decimal("0.01"), (
        "freshly-set tick must not be wiped by sizing invalidation"
    )
    assert state_after.dirty is True
    assert "tick_size_change" in (state_after.dirty_reason or "")


def test_invalidate_sizing_decision_noop_on_unknown_token():
    """Calling with a token never seen by the cache must not raise."""
    store = MarketStateStore()
    store.invalidate_sizing_decision("never-seen")
