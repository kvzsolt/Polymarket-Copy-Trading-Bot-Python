"""Transport-level exceptions for the CCXT-only Polymarket venue."""

from __future__ import annotations


class CcxtPredictionUnavailable(RuntimeError):
    """Raised when the mandatory CCXT prediction dependency is unavailable."""


class CcxtPredictionConfigurationError(ValueError):
    """Raised when a live-capable CCXT client would be ambiguously configured."""


__all__ = [
    "CcxtPredictionConfigurationError",
    "CcxtPredictionUnavailable",
]
