"""Issue #92 — the Decimal→float SDK boundary must be exact and auditable.

``utils.numeric.quantize_and_convert_to_float`` is the single sanctioned place
where a Decimal bound for the CLOB SDK becomes a Python float. These tests pin
the contract the audit demanded:

* the value handed to the signer is exactly the tick-quantized value;
* inputs already on the tick grid pass through unchanged;
* an input off the grid by more than ``epsilon`` is *rejected*, never silently
  truncated;
* repeating decimals, 0.01 / 0.001 ticks, the min-size boundary and near-one
  prices all round-trip to the expected tick multiple.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from utils.numeric import DEFAULT_EPSILON, quantize_and_convert_to_float


class TestExactBoundary:
    @pytest.mark.parametrize(
        "value, tick, expected",
        [
            ("0.50", "0.01", 0.50),
            ("0.55", "0.01", 0.55),
            ("0.999", "0.001", 0.999),   # near-one price, fine tick
            ("1.0", "0.01", 1.0),        # min-size boundary
            ("10", "0.01", 10.0),
            ("0.123", "0.001", 0.123),
        ],
    )
    def test_on_grid_values_pass_through(self, value, tick, expected):
        out = quantize_and_convert_to_float(Decimal(value), tick)
        assert out == pytest.approx(expected)
        # The float handed to the signer must equal float() of the quantized
        # Decimal — no extra precision drift introduced by the adapter.
        assert out == float(Decimal(value))

    def test_round_half_up_to_nearest_tick(self):
        # 0.505 on a 0.01 tick → 0.51 (ROUND_HALF_UP). Within epsilon, accepted.
        out = quantize_and_convert_to_float(
            Decimal("0.505"), "0.01", epsilon=Decimal("0.01")
        )
        assert out == pytest.approx(0.51)

    def test_repeating_decimal_quantizes_to_tick(self):
        # 1/3 on a 0.01 tick rounds to 0.33; delta ≈ 0.0033 > default epsilon,
        # so it must be rejected unless epsilon is widened intentionally.
        with pytest.raises(ValueError):
            quantize_and_convert_to_float(Decimal(1) / Decimal(3), "0.01")
        out = quantize_and_convert_to_float(
            Decimal(1) / Decimal(3), "0.01", epsilon=Decimal("0.01")
        )
        assert out == pytest.approx(0.33)


class TestRejection:
    def test_off_grid_beyond_epsilon_is_rejected(self):
        # 0.5005 on a 0.01 tick is 0.0005 off → exceeds default 1e-6 epsilon.
        with pytest.raises(ValueError) as exc:
            quantize_and_convert_to_float(Decimal("0.5005"), "0.01")
        assert "quantization_delta" in str(exc.value)

    def test_tiny_within_epsilon_accepted(self):
        # On-grid plus a sub-epsilon perturbation stays accepted.
        out = quantize_and_convert_to_float(
            Decimal("0.50") + DEFAULT_EPSILON / 2, "0.01"
        )
        assert out == pytest.approx(0.50)

    def test_non_positive_tick_raises(self):
        with pytest.raises(ValueError):
            quantize_and_convert_to_float(Decimal("0.5"), "0")
        with pytest.raises(ValueError):
            quantize_and_convert_to_float(Decimal("0.5"), "-0.01")


class TestIdempotentQuantization:
    def test_quantizing_an_already_quantized_value_is_stable(self):
        first = quantize_and_convert_to_float(Decimal("0.55"), "0.01")
        # Feeding the result back in must not drift.
        second = quantize_and_convert_to_float(Decimal(str(first)), "0.01")
        assert first == second


class TestArbitraryTickSize:
    """Tick math must be correct for any positive tick, not only powers of 10.

    The previous algorithm derived ``tick_inverse = round(1/tick)`` which
    silently broke when ``1/tick`` was not an integer (e.g. ``"0.07"`` → 14
    instead of 14.2857…). Audit follow-up (PR #105 review).
    """

    @pytest.mark.parametrize(
        "value, tick, expected",
        [
            # 0.05 tick — power-of-10 numerator but not 10^n; on-grid passes.
            ("0.55", "0.05", 0.55),
            ("0.5", "0.05", 0.50),
            # 0.25 tick — fractional tick that previously rejected on-grid
            # values because tick_inverse rounded 4.0 → 4 (lucky), but
            # higher precision multiples drifted.
            ("0.75", "0.25", 0.75),
            ("1.50", "0.25", 1.50),
        ],
    )
    def test_on_grid_non_power_of_ten_tick_passes(self, value, tick, expected):
        out = quantize_and_convert_to_float(Decimal(value), tick)
        assert out == pytest.approx(expected)
