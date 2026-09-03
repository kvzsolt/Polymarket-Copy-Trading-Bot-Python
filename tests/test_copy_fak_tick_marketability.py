"""Tick snapping for copied FAK limits must preserve marketability.

A copied FAK either crosses on arrival or the venue kills it with
``no orders found to match with FAK order``. Snapping a BUY limit *down* onto
the tick grid therefore spends part of the band that makes the order
executable: on 2026-08-27 a 0.6846 cap was signed as 0.68 against a 0.70 ask.
Rounding toward the book fixes that, but it may overshoot the requested price
by up to one tick, so the absolute share-price ceiling is re-asserted after the
snap rather than before it.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.executionService import _copy_fak_limit_on_tick


class TestMarketableSnap:
    """The snap moves toward the book on both sides."""

    def test_buy_rounds_up_to_the_tick(self) -> None:
        assert _copy_fak_limit_on_tick(
            price_limit=Decimal("0.6846"),
            tick_size=Decimal("0.01"),
            side="BUY",
            max_share_price=Decimal("0.90"),
        ) == Decimal("0.69")

    def test_sell_rounds_down_to_the_tick(self) -> None:
        assert _copy_fak_limit_on_tick(
            price_limit=Decimal("0.5225"),
            tick_size=Decimal("0.01"),
            side="SELL",
            max_share_price=Decimal("0.90"),
        ) == Decimal("0.52")

    def test_an_on_grid_price_is_unchanged(self) -> None:
        assert _copy_fak_limit_on_tick(
            price_limit=Decimal("0.75"),
            tick_size=Decimal("0.01"),
            side="BUY",
            max_share_price=Decimal("0.90"),
        ) == Decimal("0.75")

    @pytest.mark.parametrize("tick", ["0.1", "0.01", "0.005", "0.0025", "0.001", "0.0001"])
    def test_buy_snap_never_lands_below_the_requested_cap(self, tick: str) -> None:
        price = Decimal("0.7319")
        result = _copy_fak_limit_on_tick(
            price_limit=price,
            tick_size=Decimal(tick),
            side="BUY",
            max_share_price=Decimal("0.90"),
        )
        assert result >= price
        assert (result / Decimal(tick)) == (result / Decimal(tick)).to_integral_value()


class TestAbsoluteCeilingIsReassertedAfterTheSnap:
    """Rounding up must not smuggle a price past the hard share-price cap."""

    def test_ceiling_clamps_a_snap_that_would_overshoot_it(self) -> None:
        """0.898 ceils to 0.90 at tick 0.01 — exactly the cap, so it stands."""
        assert _copy_fak_limit_on_tick(
            price_limit=Decimal("0.898"),
            tick_size=Decimal("0.01"),
            side="BUY",
            max_share_price=Decimal("0.90"),
        ) == Decimal("0.90")

    def test_ceiling_clamps_to_an_on_grid_price_below_itself(self) -> None:
        """A cap that is not on the grid clamps down to the tick below it."""
        assert _copy_fak_limit_on_tick(
            price_limit=Decimal("0.894"),
            tick_size=Decimal("0.01"),
            side="BUY",
            max_share_price=Decimal("0.895"),
        ) == Decimal("0.89")

    def test_ceiling_does_not_apply_to_a_sell_floor(self) -> None:
        assert _copy_fak_limit_on_tick(
            price_limit=Decimal("0.9525"),
            tick_size=Decimal("0.01"),
            side="SELL",
            max_share_price=Decimal("0.90"),
        ) == Decimal("0.95")

    def test_a_disabled_ceiling_is_ignored(self) -> None:
        assert _copy_fak_limit_on_tick(
            price_limit=Decimal("0.9412"),
            tick_size=Decimal("0.01"),
            side="BUY",
            max_share_price=Decimal("0"),
        ) == Decimal("0.95")


class TestDegradedInputs:
    """A missing tick must not silently invent a grid."""

    def test_non_positive_tick_returns_the_price_unchanged(self) -> None:
        assert _copy_fak_limit_on_tick(
            price_limit=Decimal("0.6846"),
            tick_size=Decimal("0"),
            side="BUY",
            max_share_price=Decimal("0.90"),
        ) == Decimal("0.6846")
