"""P1-e regression: once the trailing stop is armed (in profit), a reversal
straight through the entry price must still trigger it.

The original guard ``cp <= trail_trigger and cp > pos.entry_price`` suppressed
the trailing stop the moment price fell below entry, so a position that gave
back all of its gains *and then some* was only catchable by the opt-in,
default-off absolute stop-loss — the classic "round-tripped the whole move and
then some" hole. The trailing trigger is derived from the in-profit peak; it
must fire on the retrace regardless of where entry sits.
"""

import pmcopy.services.positionManager as pm_mod


def _make_trailing_pm():
    pm = pm_mod.PositionManager()
    pm._risk_master_enabled = True
    pm._enabled = True
    pm._enable_stop_loss = False
    pm._enable_near_max = False
    pm._profit_lock_mode = "disabled"
    pm._enable_staged_tp = False
    pm._enable_leader_divergence = False
    pm._enable_trailing = True
    pm._trailing_activation_auto = False
    pm._trailing_activation_pct = 0.10  # arm once +10% in profit
    pm._trailing_distance = 0.15
    return pm


def _add(pm, cid="cid-trail", entry=0.50):
    pm.track_buy(
        condition_id=cid,
        asset="token_abc",
        entry_price=entry,
        size_tokens=100.0,
        usdc_cost=entry * 100.0,
        leader_wallet="0x1234567890abcdef1234567890abcdef12345678",
    )
    return cid


def test_trailing_stop_fires_when_price_reverses_below_entry():
    pm = _make_trailing_pm()
    cid = _add(pm, entry=0.50)

    # Go into profit to arm the trailing stop; peak recorded at 0.90.
    pm.evaluate_risk_action(cid, 0.90)
    pos = next(iter(pm._positions.values()))
    assert pos.exit_state.trailing_active, "trailing should arm while in profit"

    # Reverse straight through entry to a loss (0.40 < entry 0.50).
    action = pm.evaluate_risk_action(cid, 0.40)

    assert action is not None, "armed trailing stop must fire below entry"
    assert action["reason"] == "trailing_stop"
    assert action["size_fraction"] == 1.0


def test_trailing_stop_still_fires_above_entry():
    # Guard against over-correction: the normal in-profit retrace still fires.
    pm = _make_trailing_pm()
    cid = _add(pm, entry=0.50)
    pm.evaluate_risk_action(cid, 0.90)  # arm, peak 0.90
    action = pm.evaluate_risk_action(cid, 0.74)  # <= 0.90*0.85=0.765, still > entry
    assert action is not None
    assert action["reason"] == "trailing_stop"
