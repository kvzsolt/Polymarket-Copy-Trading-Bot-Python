"""P2-f / P2-p: exit-trigger PnL comparisons must stay in Decimal.

The exit evaluator previously cast the PnL ratio to ``float`` before comparing
it against the stop-loss / take-profit / trailing thresholds. Float rounding
made the boundary non-deterministic: a PnL sitting a few ULPs away from the
configured percentage could flip the decision, so a stop-loss placed exactly at
the threshold was not guaranteed to fire (and a value fractionally inside the
threshold could spuriously fire). These tests pin the Decimal-exact boundary.
"""

from __future__ import annotations

from decimal import Decimal

from services.risk.exit_policy import evaluate_exit


# ---------------------------------------------------------------------------
# Determinism: a PnL sitting exactly on the threshold must fire.
# ---------------------------------------------------------------------------


def test_stop_loss_fires_exactly_at_threshold():
    # PnL == -10% exactly; stop_loss at 10% must trigger deterministically.
    decision = evaluate_exit(Decimal("100"), Decimal("90"), stop_loss_pct=0.10)
    assert decision.should_exit is True
    assert decision.reason == "stop_loss"


def test_take_profit_fires_exactly_at_threshold():
    # PnL == +10% exactly; take_profit at 10% must trigger deterministically.
    decision = evaluate_exit(Decimal("100"), Decimal("110"), take_profit_pct=0.10)
    assert decision.should_exit is True
    assert decision.reason == "take_profit"


def test_trailing_stop_fires_exactly_at_threshold():
    # drop_from_high == 10% exactly; trailing_stop at 10% must trigger.
    decision = evaluate_exit(
        Decimal("100"),
        Decimal("90"),
        trailing_stop_pct=0.10,
        trailing_high=Decimal("100"),
    )
    assert decision.should_exit is True
    assert decision.reason == "trailing_stop"


# ---------------------------------------------------------------------------
# Float-drift removed: a PnL fractionally *inside* (less negative than) the
# stop threshold must NOT fire. Under the old float-cast comparison this exact
# case spuriously triggered because ``float(ratio) <= -stop`` rounded to True;
# the Decimal-exact comparison correctly evaluates it as not-yet-triggered.
# ---------------------------------------------------------------------------


def test_stop_loss_does_not_spuriously_fire_just_inside_threshold():
    entry = Decimal("0.59")
    current = Decimal("0.56")
    # |loss| as a float operator-configured threshold; the exact Decimal ratio
    # is -0.050847457627118644... which is fractionally *less* negative than
    # -Decimal("0.05084745762711865"), so the stop must not fire.
    stop_loss_pct = 0.05084745762711865

    exact_ratio = (current - entry) / entry
    # Guard the fixture: prove the old float path WOULD have (wrongly) fired,
    # so this test genuinely pins the regression.
    assert float(exact_ratio) <= -stop_loss_pct  # old, lossy comparison == True
    assert exact_ratio > -Decimal(str(stop_loss_pct))  # exact == not triggered

    decision = evaluate_exit(entry, current, stop_loss_pct=stop_loss_pct)
    assert decision.should_exit is False
    assert decision.reason == "ok"


def test_stop_loss_fires_just_past_threshold():
    # One tick more loss than the threshold must still fire.
    decision = evaluate_exit(Decimal("100"), Decimal("89.99"), stop_loss_pct=0.10)
    assert decision.should_exit is True
    assert decision.reason == "stop_loss"


def test_invalid_prices_never_exit():
    assert evaluate_exit(Decimal("0"), Decimal("1"), stop_loss_pct=0.10).reason == (
        "invalid_prices"
    )
    assert evaluate_exit(Decimal("1"), Decimal("0"), stop_loss_pct=0.10).reason == (
        "invalid_prices"
    )
