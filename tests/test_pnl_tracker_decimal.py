"""Regression: PnLTracker must accumulate money as Decimal, not float.

Pre-fix: TradeRecord stored price/size/usdc/pnl as float; PnLTracker
accumulated _total_invested / _total_returned / _peak_value / _max_drawdown
as float. A long-running session with many small fills drifted the running
totals away from the true sum due to IEEE-754 rounding. The drawdown
calculation that drives risk halt decisions inherited that drift.

The test pins:
  * Accumulating 1000 BUYs of $0.10 yields exactly 100.00 (not 99.999…).
  * record_sell with Decimal inputs preserves precision through the
    realized_pnl field.
  * metrics() never returns a value with > 2 decimal places of slop
    relative to the exact Decimal sum.
"""
from __future__ import annotations

from decimal import Decimal

from services.pnlTracker import PnLTracker, TradeRecord


def _is_decimal_or_zero(v) -> bool:
    return isinstance(v, Decimal)


def test_accumulator_fields_are_decimal(tmp_path):
    tracker = PnLTracker(log_path=str(tmp_path / "pnl.jsonl"))
    assert _is_decimal_or_zero(tracker._total_invested), (
        f"_total_invested must be Decimal; got {type(tracker._total_invested).__name__}"
    )
    assert _is_decimal_or_zero(tracker._total_returned)
    assert _is_decimal_or_zero(tracker._peak_value)
    assert _is_decimal_or_zero(tracker._max_drawdown)


def test_trade_record_money_fields_are_decimal():
    rec = TradeRecord(
        timestamp=1.0,
        condition_id="c1",
        side="BUY",
        price=Decimal("0.10"),
        size_tokens=Decimal("1"),
        usdc_value=Decimal("0.10"),
    )
    assert isinstance(rec.price, Decimal)
    assert isinstance(rec.size_tokens, Decimal)
    assert isinstance(rec.usdc_value, Decimal)


def test_thousand_buys_of_ten_cents_sum_exactly(tmp_path):
    """1000 * $0.10 must be exactly $100.00 — float would give 99.999…"""
    tracker = PnLTracker(log_path=str(tmp_path / "pnl.jsonl"))
    for _ in range(1000):
        tracker.record_buy(
            condition_id="c",
            price=Decimal("0.10"),
            size_tokens=Decimal("1"),
            usdc_value=Decimal("0.10"),
        )
    assert tracker._total_invested == Decimal("100.00"), (
        f"expected Decimal('100.00'), got {tracker._total_invested!r}"
    )


def test_realized_pnl_preserves_precision(tmp_path):
    tracker = PnLTracker(log_path=str(tmp_path / "pnl.jsonl"))
    # entry=0.30, exit=0.31, size=10  →  realized = (0.31 - 0.30) * 10 = 0.10
    realized = (Decimal("0.31") - Decimal("0.30")) * Decimal("10")
    assert realized == Decimal("0.10")
    tracker.record_sell(
        condition_id="c",
        price=Decimal("0.31"),
        size_tokens=Decimal("10"),
        usdc_value=Decimal("3.10"),
        entry_price=Decimal("0.30"),
        realized_pnl=realized,
    )
    metrics = tracker.metrics()
    # Total returned must be exact Decimal even after round to 2dp display.
    assert tracker._total_returned == Decimal("3.10")
    # net_pnl reported as a number that equals exactly 0.10 (no float slop)
    assert Decimal(str(metrics["net_pnl"])) == Decimal("0.10")


def test_float_inputs_are_coerced_via_str_to_avoid_decimal_float_trap(tmp_path):
    """Accepting float inputs is allowed for legacy callers but the
    conversion must go through ``str(x)`` so ``Decimal(0.1)`` !=
    ``Decimal('0.1')`` traps do not contaminate the ledger."""
    tracker = PnLTracker(log_path=str(tmp_path / "pnl.jsonl"))
    tracker.record_buy(
        condition_id="c",
        price=0.10,
        size_tokens=1.0,
        usdc_value=0.10,
    )
    # If the conversion went straight Decimal(0.10), we'd see
    # Decimal('0.10000000000000000555…'). The str() trip keeps us clean.
    assert tracker._total_invested == Decimal("0.10")
