"""Validators for typed config settings.

Each validator accepts a raw value and a field name, returns the validated
value, or raises ``EnvValidationError`` with a human-readable message.
"""

from __future__ import annotations

from config.env import EnvValidationError


def validate_ratio(value: float, field: str) -> float:
    """Value must be in [0, 1]."""
    if not (0.0 <= value <= 1.0):
        raise EnvValidationError(f"{field} must be in [0, 1], got {value}")
    return value


def validate_price_level(value: float, field: str) -> float:
    """Share price level must be in [0, 1]."""
    if not (0.0 <= value <= 1.0):
        raise EnvValidationError(f"{field} must be in [0, 1], got {value}")
    return value


def validate_bps(value: int, field: str) -> int:
    """Basis points must be >= 0."""
    if value < 0:
        raise EnvValidationError(f"{field} must be >= 0, got {value}")
    return value


def validate_usdc(value: float, field: str, *, allow_zero: bool = False) -> float:
    """USDC amount must be > 0 (or >= 0 when *allow_zero* is True)."""
    if allow_zero:
        if value < 0.0:
            raise EnvValidationError(f"{field} must be >= 0, got {value}")
    else:
        if value <= 0.0:
            raise EnvValidationError(f"{field} must be > 0, got {value}")
    return value


def validate_positive_float(value: float, field: str) -> float:
    """Value must be > 0."""
    if value <= 0.0:
        raise EnvValidationError(f"{field} must be > 0, got {value}")
    return value


def validate_non_negative_float(value: float, field: str) -> float:
    """Value must be >= 0."""
    if value < 0.0:
        raise EnvValidationError(f"{field} must be >= 0, got {value}")
    return value
