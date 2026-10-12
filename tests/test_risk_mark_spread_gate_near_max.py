"""Phase 2.8 — risk mark spread gate must work near 1.0 and on the REST path.

The audit found the spread sanity gate (``risk_mark_max_spread_bps``) is
mathematically inert in the near-max band: with bid >= 0.82 the max possible
spread is < 2000 bps of mid, so the gate never fires exactly where near-max
triggers live. And the REST fallback mark path bypasses the gate entirely.

Fix: add an absolute bid/ask distance check (the gap between bid and ask in
price points, not bps-of-mid) so a wide absolute spread near 1.0 is still
rejected, and apply the sanity gate to the REST mark path.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pmcopy.services.risk.risk_monitor import (
    _fetch_and_store_price,
    _risk_mark_passes_sanity,
    RiskPriceCache,
)


def _ctx(*, max_spread_bps: int = 2000, max_abs_distance: float = 0.0) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(
            risk=SimpleNamespace(
                risk_mark_max_spread_bps=max_spread_bps,
                risk_mark_max_abs_distance=max_abs_distance,
            )
        ),
        extra={},
    )


class TestSpreadGateNearMax:
    def test_bps_gate_inert_near_1_0_without_abs_distance(self):
        """bid=0.95, ask=0.97 -> spread = 200 bps of mid (0.96), under the
        2000 bps cap, so the bps gate passes. Without an absolute distance
        check this wide-near-1.0 book is accepted."""
        ctx = _ctx(max_spread_bps=2000, max_abs_distance=0.0)
        assert (
            _risk_mark_passes_sanity(
                ctx, token_id="t1", best_bid=Decimal("0.95"), best_ask=Decimal("0.97")
            )
            is True
        )

    def test_abs_distance_gate_rejects_wide_spread_near_1_0(self):
        """With an absolute distance cap of 0.02, a 0.03-wide book near 1.0
        (bid=0.95, ask=0.98) is rejected — the bps gate is inert here but the
        absolute gap exceeds the limit."""
        ctx = _ctx(max_spread_bps=2000, max_abs_distance=0.02)
        # gap = 0.98 - 0.95 = 0.03 > cap -> reject
        assert (
            _risk_mark_passes_sanity(
                ctx, token_id="t1", best_bid=Decimal("0.95"), best_ask=Decimal("0.98")
            )
            is False
        )

    def test_abs_distance_gate_accepts_tight_book_near_1_0(self):
        ctx = _ctx(max_spread_bps=2000, max_abs_distance=0.02)
        assert (
            _risk_mark_passes_sanity(
                ctx, token_id="t1", best_bid=Decimal("0.95"), best_ask=Decimal("0.955")
            )
            is True
        )

    def test_crossed_book_still_rejected(self):
        ctx = _ctx(max_spread_bps=2000, max_abs_distance=0.05)
        assert (
            _risk_mark_passes_sanity(
                ctx, token_id="t1", best_bid=Decimal("0.97"), best_ask=Decimal("0.95")
            )
            is False
        )


class TestRestMarkPathAppliesSanityGate:
    """The REST fallback ``_fetch_and_store_price`` returns only a best-bid
    (no ask), so the bps/abs-distance gates cannot apply (they need both
    sides). The REST path's staleness tightening is covered separately
    (M8). Here we pin that a one-sided REST bid is still cached (rejecting it
    would blind risk monitoring on thin markets)."""

    @pytest.mark.asyncio
    async def test_rest_path_caches_one_sided_bid(self):
        import asyncio
        from unittest.mock import AsyncMock

        cache = RiskPriceCache(max_size=10)
        client = MagicMock()
        client.get_price = AsyncMock(return_value={"price": "0.95", "size": "10"})

        await _fetch_and_store_price(
            client,
            "t1",
            cache,
            asyncio.Semaphore(1),
        )

        entry = cache.get_fresh("t1", stale_after=100.0)
        assert entry is not None
        assert entry.price == Decimal("0.95")


class TestAbsDistanceGateStrictComparison:
    """A book whose gap equals the cap is a normal book, not a wide one.

    Live sports markets routinely quote a 3-cent gap (e.g. 0.26/0.29); with
    ``risk_mark_max_abs_distance: 0.03`` the previous ``>=`` comparison
    rejected every such book and degraded protective-exit marks to REST
    polling. The gate is strict: only a gap *wider* than the cap is rejected.
    """

    def test_gap_equal_to_cap_is_accepted(self):
        ctx = _ctx(max_spread_bps=0, max_abs_distance=0.03)
        assert (
            _risk_mark_passes_sanity(
                ctx, token_id="t1", best_bid=Decimal("0.26"), best_ask=Decimal("0.29")
            )
            is True
        )

    def test_gap_wider_than_cap_is_rejected(self):
        ctx = _ctx(max_spread_bps=0, max_abs_distance=0.03)
        assert (
            _risk_mark_passes_sanity(
                ctx, token_id="t1", best_bid=Decimal("0.13"), best_ask=Decimal("0.98")
            )
            is False
        )
