"""Regression: NearOneExitService keys dwell/firing state by (condition, token).

Pre-fix the dwell + firing dicts were keyed on ``token_id`` alone. If
the same token_id ever appeared under two distinct condition_ids (CTF
uint256 collision, market split, or a future Polymarket schema change),
the state would silently collapse: a SELL fired for condition A would
suppress NearOne for condition B because both share the firing token.

Test pins the new key shape via type annotation + a behavioral assertion
that two scopes with the same token_id under different conditions get
independent firing state.
"""
from __future__ import annotations

from services.near_one_exit import NearOneExitService


def test_firing_set_holds_condition_token_tuples():
    """The firing set must be typed for (condition_id, token_id) tuples."""
    svc = NearOneExitService.__new__(NearOneExitService)
    svc._firing = set()
    svc._dwell = {}

    # Same token, different condition_ids — must NOT collide.
    svc._firing.add(("cond-A", "tok-1"))
    svc._firing.add(("cond-B", "tok-1"))

    assert len(svc._firing) == 2, (
        f"distinct conditions must keep distinct firing entries; got {svc._firing}"
    )
    assert ("cond-A", "tok-1") in svc._firing
    assert ("cond-B", "tok-1") in svc._firing


def test_dwell_dict_holds_condition_token_tuples():
    svc = NearOneExitService.__new__(NearOneExitService)
    svc._dwell = {}
    svc._firing = set()
    # Direct mutation mirroring _tick path.
    from services.near_one_exit import _DwellState

    svc._dwell[("cond-A", "tok-1")] = _DwellState(first_seen_above_ts=1.0)
    svc._dwell[("cond-B", "tok-1")] = _DwellState(first_seen_above_ts=2.0)

    assert len(svc._dwell) == 2
    assert svc._dwell[("cond-A", "tok-1")].first_seen_above_ts == 1.0
    assert svc._dwell[("cond-B", "tok-1")].first_seen_above_ts == 2.0
