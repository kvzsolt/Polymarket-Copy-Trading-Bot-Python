"""Unit tests for quote/spread validation policy."""
import pytest
from decimal import Decimal
from services.execution.quote_policy import evaluate_quote_quality

def test_no_quotes_fails():
    result = evaluate_quote_quality()
    assert not result.acceptable
    assert result.reason == "no_quotes"

def test_zero_quotes_fails():
    result = evaluate_quote_quality(best_bid=Decimal("0"), best_ask=Decimal("0"))
    assert not result.acceptable

def test_wide_spread_fails():
    result = evaluate_quote_quality(
        best_bid=Decimal("0.40"), best_ask=Decimal("0.60"),
        max_spread_bps=500,
    )
    assert not result.acceptable
    assert result.reason == "spread_too_wide"

def test_acceptable_spread_passes():
    result = evaluate_quote_quality(
        best_bid=Decimal("0.49"), best_ask=Decimal("0.51"),
        max_spread_bps=500,
    )
    assert result.acceptable
    assert result.reason == "ok"

def test_tight_spread_passes():
    result = evaluate_quote_quality(
        best_bid=Decimal("0.499"), best_ask=Decimal("0.501"),
    )
    assert result.acceptable
