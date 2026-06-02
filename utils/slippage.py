"""Slippage conversion helpers.

Centralizes percent→basis-points conversion so every call site uses the same
Decimal-based rounding instead of float arithmetic. ``slippage_tolerance`` is a
float in ``AppConfig`` (external SDK boundary), but ``int(float * 100)`` can
truncate the wrong way — e.g. ``0.29 * 100 == 28.999999999999996 -> 28``.
"""

from __future__ import annotations

from decimal import Decimal


def slippage_pct_to_bps(value: float | str | Decimal) -> int:
    """Convert a slippage *fraction-of-1 percent* tolerance to integer bps.

    Uses ``Decimal(str(value))`` so the conversion is exact at the boundary and
    floor-truncation happens on the exact decimal, not an IEEE-754 artifact.
    """
    return int(Decimal(str(value)) * 100)
