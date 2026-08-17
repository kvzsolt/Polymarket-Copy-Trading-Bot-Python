"""Fail-closed version gate for the installed CCXT prediction namespace."""

from __future__ import annotations

from typing import Any

from integrations.polymarket.ccxt_compat.exceptions import CcxtPredictionUnavailable

_MINIMUM_CCXT_VERSION = (4, 5, 70)
_MAXIMUM_EXCLUSIVE_MAJOR = 5
_VERSION_COMPONENTS = 3


def parse_ccxt_version(value: Any) -> tuple[int, ...]:
    """Return the leading numeric version components, ignoring junk."""

    parts: list[int] = []
    for part in str(value or "").split("."):
        digits = "".join(ch for ch in part if ch.isdigit())
        if not digits:
            break
        parts.append(int(digits))
        if len(parts) == _VERSION_COMPONENTS:
            break
    return tuple(parts)


def ensure_supported_ccxt_version(version: Any) -> None:
    """Reject CCXT installs outside the supported ``>=4.5.70,<5`` window.

    Raises:
        CcxtPredictionUnavailable: When the version is missing, older than the
            required minimum, or at/above the next breaking major.
    """

    parsed = parse_ccxt_version(version)
    if not parsed:
        raise CcxtPredictionUnavailable(
            f"CCXT version could not be determined from {version!r}; "
            ">= 4.5.70, < 5 is required"
        )
    if parsed < _MINIMUM_CCXT_VERSION:
        raise CcxtPredictionUnavailable(
            f"CCXT {version} is too old; >= 4.5.70, < 5 is required"
        )
    if parsed[0] >= _MAXIMUM_EXCLUSIVE_MAJOR:
        raise CcxtPredictionUnavailable(
            f"CCXT {version} is unsupported; >= 4.5.70, < 5 is required"
        )


__all__ = [
    "ensure_supported_ccxt_version",
    "parse_ccxt_version",
]
