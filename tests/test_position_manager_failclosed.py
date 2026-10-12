"""Review fix (cubic P1 + greptile P1 on PR #110): the Phase 2 Decimal
sweep introduced a ``_to_dec`` helper whose broad ``except Exception:
return default`` turned the prior fail-closed conversion into a fail-open
one. A non-numeric ``entry_price`` would silently become ``Decimal("0")``,
then either crash ``evaluate_risk_action`` with ``DivisionByZero`` or
inflate PnL on the next sell.

These tests assert:
1. ``_to_dec`` re-raises on non-coercible values (only ``None`` returns
   the default, which is the contract evaluate_risk_action depends on).
2. ``track_buy`` rejects zero / negative money fields with ``ValueError``
   before they corrupt the position store.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

import pytest

from pmcopy.services.positionManager import PositionManager, _to_dec


# --- _to_dec contract ------------------------------------------------------


def test_to_dec_none_returns_default() -> None:
    assert _to_dec(None) == Decimal("0")
    assert _to_dec(None, default=Decimal("42")) == Decimal("42")


def test_to_dec_accepts_int_float_str_decimal() -> None:
    assert _to_dec(5) == Decimal("5")
    assert _to_dec(0.25) == Decimal("0.25")
    assert _to_dec("1.5") == Decimal("1.5")
    assert _to_dec(Decimal("3.14")) == Decimal("3.14")


def test_to_dec_raises_on_garbage_string() -> None:
    with pytest.raises(InvalidOperation):
        _to_dec("not-a-number")


def test_to_dec_raises_on_dict() -> None:
    with pytest.raises((InvalidOperation, TypeError)):
        _to_dec({"price": 0.5})


def test_to_dec_raises_on_empty_string() -> None:
    with pytest.raises(InvalidOperation):
        _to_dec("")


# --- track_buy positive-value guard ---------------------------------------


def test_track_buy_rejects_zero_entry_price() -> None:
    pm = PositionManager()
    with pytest.raises(ValueError, match="requires positive"):
        pm.track_buy("cond-z", "asset-1", 0, 100, 10)


def test_track_buy_rejects_negative_entry_price() -> None:
    pm = PositionManager()
    with pytest.raises(ValueError, match="requires positive"):
        pm.track_buy("cond-z", "asset-1", -0.1, 100, 10)


def test_track_buy_rejects_zero_size_tokens() -> None:
    pm = PositionManager()
    with pytest.raises(ValueError, match="requires positive"):
        pm.track_buy("cond-z", "asset-1", 0.5, 0, 10)


def test_track_buy_rejects_zero_usdc_cost() -> None:
    pm = PositionManager()
    with pytest.raises(ValueError, match="requires positive"):
        pm.track_buy("cond-z", "asset-1", 0.5, 100, 0)


def test_track_buy_rejects_garbage_entry_price() -> None:
    pm = PositionManager()
    with pytest.raises(InvalidOperation):
        pm.track_buy("cond-z", "asset-1", "nope", 100, 10)
