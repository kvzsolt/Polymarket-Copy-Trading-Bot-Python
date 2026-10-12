"""Shared unit invariants; policy thresholds belong in typed configuration."""

from __future__ import annotations

from decimal import Decimal
from typing import Final

BASIS_POINTS_PER_UNIT: Final[Decimal] = Decimal("10000")
SECONDS_PER_DAY: Final[int] = 86_400
MILLISECONDS_PER_SECOND: Final[int] = 1_000
MILLISECONDS_PER_DAY: Final[int] = SECONDS_PER_DAY * MILLISECONDS_PER_SECOND
UNIX_MILLISECONDS_THRESHOLD: Final[int] = 1_000_000_000_000
SECRET_FILE_MODE: Final[int] = 0o600


__all__ = [
    "BASIS_POINTS_PER_UNIT",
    "MILLISECONDS_PER_DAY",
    "MILLISECONDS_PER_SECOND",
    "SECONDS_PER_DAY",
    "SECRET_FILE_MODE",
    "UNIX_MILLISECONDS_THRESHOLD",
]
