"""P2-e: BUY affordability gate must be fee-aware.

The upstream cash gates (``headroom_for_buy`` / balance regime) compare the
candidate BUY against the *bare* notional and ignore venue fees, so a BUY that
is affordable on notional alone but NOT once fees are added could slip through.
``trade_pipeline`` now computes ``required_cash = notional * (1 + fee_bps/1e4)``
in Decimal and fails closed when it does not fit live free cash.

These tests pin the fee-aware affordability math and the fee-source guard. The
arithmetic is intentionally isolated in ``_buy_required_cash_with_fees`` so the
boundary (affordable-on-notional / unaffordable-with-fees) is covered without
standing up the full Postgres-backed pipeline harness.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace


from pmcopy.orchestrators.trade_pipeline import (
    _buy_fee_bps_or_none,
    _buy_required_cash_with_fees,
)


# ---------------------------------------------------------------------------
# Required-cash math is Decimal and fee-inclusive.
# ---------------------------------------------------------------------------


def test_required_cash_adds_fees_in_decimal():
    # 100 USDC notional at 20 bps -> 100 * 1.002 = 100.20, exact Decimal.
    required = _buy_required_cash_with_fees(Decimal("100"), 20)
    assert required == Decimal("100.200")
    assert isinstance(required, Decimal)


def test_required_cash_zero_fee_equals_notional():
    assert _buy_required_cash_with_fees(Decimal("100"), 0) == Decimal("100")


def test_required_cash_nonpositive_notional_is_zero():
    assert _buy_required_cash_with_fees(Decimal("0"), 50) == Decimal("0")
    assert _buy_required_cash_with_fees(Decimal("-5"), 50) == Decimal("0")


def test_buy_affordable_on_notional_but_not_with_fees_is_rejected():
    """The exact gate predicate: required_cash > free_cash once fees apply."""
    free_cash = Decimal("100.00")
    notional = Decimal("100.00")  # affordable on notional alone (100 <= 100)
    fee_bps = 20

    # Without fees the BUY fits exactly.
    assert notional <= free_cash

    # With fees it no longer fits -> the gate must reject (required > free).
    required_with_fees = _buy_required_cash_with_fees(notional, fee_bps)
    assert required_with_fees == Decimal("100.200")
    assert required_with_fees > free_cash  # gate condition -> skip BUY


def test_buy_fits_with_fees_is_allowed():
    free_cash = Decimal("100.50")
    notional = Decimal("100.00")
    required_with_fees = _buy_required_cash_with_fees(notional, 20)
    assert required_with_fees == Decimal("100.200")
    assert required_with_fees <= free_cash  # gate condition -> allow BUY


# ---------------------------------------------------------------------------
# Fee-source guard: None when no fee service is wired (configless/legacy/test),
# a usable int otherwise. None disables the fee tightening so the pre-existing
# no-fee cash gates stay authoritative (no regression of allowed trades).
# ---------------------------------------------------------------------------


def _run(coro):
    return asyncio.run(coro)


def test_fee_bps_none_when_no_execution_service():
    ctx = SimpleNamespace(execution_service=None)
    assert _run(_buy_fee_bps_or_none(ctx, "tok")) is None


def test_fee_bps_none_when_no_fee_service():
    ctx = SimpleNamespace(execution_service=SimpleNamespace(fee_service=None))
    assert _run(_buy_fee_bps_or_none(ctx, "tok")) is None


def test_fee_bps_returns_int_from_fee_service():
    class _FeeSvc:
        async def get_fee_bps(self, token_id):  # noqa: D401 - test stub
            return 30

    ctx = SimpleNamespace(execution_service=SimpleNamespace(fee_service=_FeeSvc()))
    assert _run(_buy_fee_bps_or_none(ctx, "tok")) == 30


def test_fee_bps_none_when_fee_service_raises():
    class _Boom:
        async def get_fee_bps(self, token_id):
            raise RuntimeError("venue down")

    ctx = SimpleNamespace(execution_service=SimpleNamespace(fee_service=_Boom()))
    assert _run(_buy_fee_bps_or_none(ctx, "tok")) is None
