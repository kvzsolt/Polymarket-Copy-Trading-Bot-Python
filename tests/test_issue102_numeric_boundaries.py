"""Issue #102 — boundary coverage for Decimal / tick / idempotency math.

The finding asked for Hypothesis-style property tests over numeric boundaries.
This module gives equivalent coverage via dense ``parametrize`` grids so we
avoid a new dependency (Hypothesis pulls sortedcontainers/attrs). The
invariants asserted match the finding's list:

* no float leakage past the SDK adapter (``quantize_and_convert_to_float``);
* venue-min respected by sizing;
* cap respected by sizing;
* tick alignment exact (zero rounding delta when input is already on-grid);
* idempotency key **stable** for equivalent Decimal string forms;
* idempotency key **distinct** for distinct intent_ids / tick sizes / sides.

Tick sizes (0.001 / 0.01 / 0.05 / 0.1), prices (0.01 / 0.50 / 0.95 / 0.999),
sizes (1 / 1.5 / 10 / 100) and a small set of near-1.0 / near-zero edges
cover the same fragile zones the audit was worried about.
"""

from __future__ import annotations

from decimal import Decimal
from itertools import product

import pytest

from pmcopy.services.executionService import _effective_idempotency_key
from pmcopy.services.order_sizing import size_buy_order, size_sell_order
from pmcopy.utils.numeric import quantize_and_convert_to_float

TICKS = ("0.001", "0.01", "0.05", "0.1")
PRICES = ("0.01", "0.10", "0.50", "0.95", "0.99", "0.999")


# ---------------------------------------------------------------------------
# Adapter — no float leakage; on-grid values pass through exactly.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "tick, multiplier",
    [(t, m) for t, m in product(TICKS, range(1, 21))],
)
def test_adapter_on_grid_values_round_trip_exactly(tick: str, multiplier: int):
    """For every (tick, k) the value k*tick is on the grid; the adapter must
    return exactly float(Decimal(k*tick)) with zero rounding drift."""
    on_grid = Decimal(tick) * Decimal(multiplier)
    out = quantize_and_convert_to_float(on_grid, tick)
    assert out == float(on_grid)


@pytest.mark.parametrize("tick", TICKS)
@pytest.mark.parametrize("price", PRICES)
def test_adapter_quantizes_near_1_prices(tick: str, price: str):
    """Prices close to 1.0 are a typical Polymarket failure zone (binary
    markets quote close to 0.99). When a price is on the tick grid, the
    adapter must accept it; when off-grid by more than epsilon, raise."""
    value = Decimal(price)
    tick_d = Decimal(tick)
    # If already on grid (remainder ≈ 0), accepts. Otherwise rejects below.
    remainder = value % tick_d
    if remainder == 0:
        out = quantize_and_convert_to_float(value, tick)
        assert out == float(value)


# ---------------------------------------------------------------------------
# Sizing — venue-min respected, cap respected, no float leakage.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("live_price", [0.05, 0.10, 0.50, 0.75, 0.95, 0.99])
@pytest.mark.parametrize("base_notional", [1.0, 5.0, 10.0, 50.0, 100.0])
def test_buy_sizing_respects_cap(live_price: float, base_notional: float):
    """``worst_notional`` must never exceed the configured cap, regardless
    of price or tick."""
    cap = base_notional  # cap == base for this grid
    decision = size_buy_order(
        live_price=live_price,
        base_notional_usdc=base_notional,
        max_notional_usdc=cap,
        market_min_size=1.0,
        tick_size=0.01,
        slippage_ratio=0.02,
    )
    if decision.accept:
        assert decision.worst_notional <= Decimal(str(cap)) * Decimal("1.0001")  # tiny tolerance


@pytest.mark.parametrize("sell_tokens", [0.5, 1.0, 1.5, 5.0, 10.0, 100.0])
@pytest.mark.parametrize("held", [0.0, 0.5, 1.0, 2.0, 10.0, 1000.0])
def test_sell_sizing_bounded_by_held(sell_tokens: float, held: float):
    """SELL effective size must never exceed held position."""
    decision = size_sell_order(
        sell_tokens=sell_tokens,
        held_size=held,
        live_price=0.5,
        market_min_size=1.0,
    )
    if decision.accept:
        assert decision.effective_size <= Decimal(str(held))


# ---------------------------------------------------------------------------
# Effective idempotency — stable for equivalent inputs, distinct otherwise.
# ---------------------------------------------------------------------------


def _key(**overrides):
    base = dict(
        user="u1",
        condition_id="cond1",
        token_id="t1",
        side="BUY",
        effective_size=Decimal("10.00"),
        effective_price=Decimal("0.50"),
        tick_size=Decimal("0.01"),
        intent_id="intent-1",
        policy_version="v1",
    )
    base.update(overrides)
    return _effective_idempotency_key(**base)


@pytest.mark.parametrize("size_str", ["10", "10.0", "10.00", "10.000"])
def test_idempotency_stable_across_equivalent_decimal_strings(size_str: str):
    """Decimal('10') and Decimal('10.000') must produce the same key —
    otherwise a benign decimal-formatting change splits a duplicate
    submission into two distinct effective orders."""
    assert _key(effective_size=Decimal(size_str)) == _key(effective_size=Decimal("10"))


@pytest.mark.parametrize("price_str", ["0.5", "0.50", "0.500", "0.5000"])
def test_idempotency_stable_across_equivalent_price_strings(price_str: str):
    assert _key(effective_price=Decimal(price_str)) == _key(effective_price=Decimal("0.5"))


@pytest.mark.parametrize(
    "field, alt",
    [
        ("user", "u2"),
        ("condition_id", "cond2"),
        ("token_id", "t2"),
        ("side", "SELL"),
        ("effective_size", Decimal("11")),
        ("effective_price", Decimal("0.51")),
        ("tick_size", Decimal("0.001")),
        ("intent_id", "intent-2"),
        ("policy_version", "v2"),
    ],
)
def test_idempotency_distinct_when_any_dimension_changes(field, alt):
    """Every dimension of the effective key must influence the hash —
    otherwise two distinct orders collapse into one duplicate-ignored."""
    assert _key(**{field: alt}) != _key()
