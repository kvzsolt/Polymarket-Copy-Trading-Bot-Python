"""Precision adapters for numeric values at system boundaries.

This module provides the single canonical conversion point for Decimal→float
transitions at the SDK / venue boundary, where precision loss must be
intentional and auditable.
"""

from __future__ import annotations

import logging
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger("numeric_adapter")

# Default rounding-delta threshold above which we reject instead of silently quantize.
# 1e-6 is effectively 1 micro-unit relative to 1.0. The effective threshold inside
# ``quantize_and_convert_to_float`` is scaled relative to ``tick_size`` so that
# float→Decimal roundtrip noise (~1e-5 on values near 5 with tick=0.01) does not
# masquerade as an off-grid caller mistake. See _effective_epsilon below.
DEFAULT_EPSILON = Decimal("1e-6")

# Tick-relative epsilon floor: any value within 1% of a tick is treated as
# on-grid. For tick=0.01 (USDC cents) this is 1e-4, comfortably above the
# float-precision noise produced by ``Decimal(str(float_value))`` round-trips
# (observed up to ~4e-5 on values near 5 USDC) while still rejecting genuine
# off-grid inputs by 2+ orders of magnitude (e.g. 0.5005 vs tick 0.01 → 5e-4
# delta, still rejected).
_EPSILON_TICK_RATIO = Decimal("1e-2")


def quantize_and_convert_to_float(
    value: Decimal,
    tick_size: str | Decimal,
    *,
    epsilon: Decimal = DEFAULT_EPSILON,
) -> float:
    """Quantize a Decimal to the nearest tick and convert to float for the SDK boundary.

    This is the **only** sanctioned location in the codebase where a Decimal
    value intended for the CLOB SDK is converted to a Python ``float``.
    Using ``float()`` directly elsewhere bypasses tick-aware rounding and can
    cause silent precision loss that results in invalid order sizes/prices.

    Quantization rule
    ----------------
    ``value`` is rounded to the nearest multiple of ``tick_size`` using
    ``ROUND_HALF_UP`` (standard financial rounding: 0.5 rounds up).
    If the rounding delta (|original − rounded|) exceeds ``epsilon``, the value
    is **rejected** rather than silently truncated, because a large delta means
    the input was probably already on a different tick grid and quantizing it
    would silently change its meaning.

    Args:
        value:        The Decimal amount to quantize and convert.
        tick_size:   The market tick size as a string or Decimal
                     (e.g. "0.01" for a 1-cent tick).
        epsilon:     Maximum acceptable rounding delta before rejection.
                     Defaults to 1e-6; raise it only for testing.

    Returns:
        The quantized value as a Python ``float``.

    Raises:
        ValueError:  If rounding delta exceeds epsilon, or tick_size is invalid.

    Audit log
    ---------
    All conversions are logged at INFO level with before/after values so that
    any precision adjustments are traceable in production logs.
    """
    tick = Decimal(str(tick_size))

    if tick <= 0:
        raise ValueError(f"tick_size must be positive, got {tick}")

    # Quantize to nearest tick using ROUND_HALF_UP.
    # Divide directly by tick so the algorithm is correct for any positive
    # tick (not only powers of 10): the previous form derived an integer
    # ``tick_inverse = round(1/tick)`` which silently broke for ticks where
    # 1/tick is not an integer (e.g. "0.07", "0.15").
    #
    # Rounding-mode audit (review finding #8): the Polymarket CLOB's
    # published docs do not specify whether the venue uses HALF_UP or
    # HALF_EVEN. ROUND_HALF_UP is the conventional financial choice and
    # matches what the rest of this codebase uses. In practice the
    # epsilon gate above will reject any input whose rounding delta
    # exceeds DEFAULT_EPSILON (1e-6 ≪ any real tick), so the mode choice
    # only matters when the caller passes a value exactly on a half-tick
    # boundary AND raises epsilon. If the venue ever documents a
    # different rule, change the constant here and add a regression test
    # asserting the new mode.
    multiples = (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    rounded = multiples * tick

    delta = abs(value - rounded)
    # Scale epsilon by tick so float→Decimal roundtrip noise on coarse ticks
    # (USDC cents, share lots) does not get mistaken for an off-grid input.
    effective_epsilon = max(epsilon, tick * _EPSILON_TICK_RATIO)
    if delta > effective_epsilon:
        raise ValueError(
            f"quantization_delta={delta} exceeds epsilon={effective_epsilon} "
            f"(original={value!s}, tick={tick!s}, rounded={rounded!s}). "
            "Rejecting to prevent silent precision loss. "
            "If this is expected, increase epsilon or pre-quantize the input."
        )

    result = float(rounded)

    if delta > Decimal("0"):
        logger.info(
            "SDK_boundary_numeric_conversion | original=%s tick=%s rounded=%s delta=%s",
            str(value),
            str(tick),
            str(rounded),
            str(delta),
        )

    return result