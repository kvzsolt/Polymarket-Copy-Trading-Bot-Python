"""Phase 2.8 — sub-minimum leader sell fractions must fail closed when configured.

The audit found sub-minimum sell fractions round UP to the venue minimum
(``orchestrators/trade_pipeline.py:1117-1119``): a leader's 2% nibble becomes
a near-full exit on small positions. Add a configurable policy: "round_up"
(legacy) or "skip" (fail-closed — drop the copy sell rather than overshoot).
"""

from __future__ import annotations

from decimal import Decimal


from orchestrators.trade_pipeline import _resolve_leader_sell_tokens


def test_round_up_legacy_default():
    """Legacy behavior: a sub-minimum target rounds up to the venue minimum."""
    result = _resolve_leader_sell_tokens(
        requested_sell_tokens=Decimal("0.2"),
        max_sell_tokens=Decimal("100"),
        market_min_size=5.0,
        sub_min_sell_policy="round_up",
    )
    assert result == Decimal("5")


def test_skip_fail_closed_drops_sub_minimum_sell():
    """Fail-closed: a sub-minimum proportional sell is dropped (returns 0)
    rather than overshooting to the venue minimum."""
    result = _resolve_leader_sell_tokens(
        requested_sell_tokens=Decimal("0.2"),
        max_sell_tokens=Decimal("100"),
        market_min_size=5.0,
        sub_min_sell_policy="skip",
    )
    assert result == Decimal("0")


def test_skip_still_sells_when_target_meets_minimum():
    result = _resolve_leader_sell_tokens(
        requested_sell_tokens=Decimal("6"),
        max_sell_tokens=Decimal("100"),
        market_min_size=5.0,
        sub_min_sell_policy="skip",
    )
    assert result == Decimal("6")


def test_skip_caps_at_max_sell_tokens():
    result = _resolve_leader_sell_tokens(
        requested_sell_tokens=Decimal("200"),
        max_sell_tokens=Decimal("50"),
        market_min_size=5.0,
        sub_min_sell_policy="skip",
    )
    assert result == Decimal("50")


def test_round_up_uses_one_usdc_floor_when_larger_than_share_minimum() -> None:
    """At ten cents, six shares round to ten so notional reaches one USDC."""

    result = _resolve_leader_sell_tokens(
        requested_sell_tokens=Decimal("6"),
        max_sell_tokens=Decimal("100"),
        market_min_size=5.0,
        live_price=Decimal("0.10"),
        sub_min_sell_policy="round_up",
    )

    assert result == Decimal("10")


def test_round_up_fails_closed_when_holdings_cannot_reach_one_usdc() -> None:
    """Do not submit a known sub-minimum SELL when only eight shares are held."""

    result = _resolve_leader_sell_tokens(
        requested_sell_tokens=Decimal("6"),
        max_sell_tokens=Decimal("8"),
        market_min_size=5.0,
        live_price=Decimal("0.10"),
        sub_min_sell_policy="round_up",
    )

    assert result == Decimal("0")
