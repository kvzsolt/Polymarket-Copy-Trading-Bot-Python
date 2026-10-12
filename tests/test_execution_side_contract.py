"""Characterize the existing side validator before narrowing its return type."""

import pytest

from pmcopy.services.executionService import _normalize_side


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("BUY", "BUY"),
        ("buy", "BUY"),
        (" BUY ", "BUY"),
        ("SELL", "SELL"),
        ("sell", "SELL"),
        ("\tSELL\n", "SELL"),
    ],
)
def test_valid_sides_preserve_normalization(raw: str, expected: str) -> None:
    """Case and surrounding whitespace retain the established interpretation."""
    assert _normalize_side(raw) == expected


@pytest.mark.parametrize("raw", ["", "hold", "BUY SELL", "0"])
def test_unknown_sides_preserve_rejection(raw: str) -> None:
    """Unsupported input keeps the existing exception and message."""
    with pytest.raises(ValueError) as error:
        _normalize_side(raw)
    assert str(error.value) == f"Unsupported side: {raw}"
