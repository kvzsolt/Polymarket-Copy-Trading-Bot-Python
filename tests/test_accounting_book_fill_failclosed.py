"""SHP-10: AccountingService.book_fill must fail closed on missing or
non-decimal money fields. Pre-fix the call silently inserted ``"None"`` or
``""`` strings into the ledger when the upstream doc was incomplete,
corrupting downstream PnL replay that re-parses size/price/usdc_value as
Decimal.
"""
from __future__ import annotations

from decimal import InvalidOperation

import pytest

from services.accountingService import _required_decimal_str


def test_required_decimal_str_accepts_str_decimal_and_float() -> None:
    assert _required_decimal_str({"x": "1.23"}, "x") == "1.23"
    assert _required_decimal_str({"x": 1.5}, "x") == "1.5"
    assert _required_decimal_str({"x": "0"}, "x") == "0"


def test_required_decimal_str_rejects_none() -> None:
    with pytest.raises(ValueError, match="missing required money field"):
        _required_decimal_str({"x": None}, "x")


def test_required_decimal_str_rejects_missing_key() -> None:
    with pytest.raises(ValueError, match="missing required money field"):
        _required_decimal_str({}, "x")


def test_required_decimal_str_rejects_empty_string() -> None:
    with pytest.raises(ValueError, match="missing required money field"):
        _required_decimal_str({"x": ""}, "x")


def test_required_decimal_str_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="not a valid decimal"):
        _required_decimal_str({"x": "nope"}, "x")
