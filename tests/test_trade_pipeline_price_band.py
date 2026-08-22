"""Leader-price band applied to copy FAK entries.

Root cause #3: the pipeline used to hand the executor the leader's own fill
price as the FAK ``price_limit``. ``_apply_marketable_price_protection`` then
clamped the marketable price down to it, so every copy whose book had moved
against the leader was submitted at a non-marketable price and expired
unfilled. The limit is now an adjustable band around the leader price,
configured by ``execution.max_entry_price_drift_pct``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from domain.execution import LeaderPriceBand
from orchestrators.trade_pipeline import _copy_entry_price_plan


def _context(drift_pct: float) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(execution=SimpleNamespace(max_entry_price_drift_pct=drift_pct))
    )


def _signal(price: float) -> SimpleNamespace:
    return SimpleNamespace(price=price)


class TestLeaderPriceBand:
    def test_buy_cap_is_leader_price_plus_drift(self) -> None:
        band = LeaderPriceBand(leader_price=Decimal("0.45"), drift_pct=Decimal("5"))
        assert band.limit_for("BUY") == Decimal("0.4725")

    def test_sell_floor_is_leader_price_minus_drift(self) -> None:
        band = LeaderPriceBand(leader_price=Decimal("0.55"), drift_pct=Decimal("5"))
        assert band.limit_for("SELL") == Decimal("0.5225")

    def test_limits_are_quantized_to_four_decimal_places(self) -> None:
        band = LeaderPriceBand(leader_price=Decimal("0.4123"), drift_pct=Decimal("7.5"))
        buy = band.limit_for("BUY")
        sell = band.limit_for("SELL")
        assert buy == Decimal("0.4433")  # 0.44322... rounded up
        assert sell == Decimal("0.3813")  # 0.38137... rounded down

    def test_zero_drift_disables_the_band(self) -> None:
        band = LeaderPriceBand(leader_price=Decimal("0.45"), drift_pct=Decimal("0"))
        assert band.enabled is False
        assert band.limit_for("BUY") == Decimal("0.45")

    def test_non_positive_leader_price_disables_the_band(self) -> None:
        assert LeaderPriceBand(leader_price=Decimal("0"), drift_pct=Decimal("5")).enabled is False

    def test_buy_quote_above_the_cap_is_outside_the_band(self) -> None:
        band = LeaderPriceBand(leader_price=Decimal("0.45"), drift_pct=Decimal("5"))
        assert band.excludes(Decimal("0.53"), "BUY") is True
        assert band.excludes(Decimal("0.46"), "BUY") is False
        assert band.excludes(Decimal("0.4725"), "BUY") is False

    def test_sell_quote_below_the_floor_is_outside_the_band(self) -> None:
        band = LeaderPriceBand(leader_price=Decimal("0.55"), drift_pct=Decimal("5"))
        assert band.excludes(Decimal("0.50"), "SELL") is True
        assert band.excludes(Decimal("0.54"), "SELL") is False

    def test_disabled_band_never_excludes(self) -> None:
        band = LeaderPriceBand(leader_price=Decimal("0.45"), drift_pct=Decimal("0"))
        assert band.excludes(Decimal("0.99"), "BUY") is False


class TestCopyEntryPricePlan:
    def test_buy_limit_and_metadata(self) -> None:
        price_limit, metadata = _copy_entry_price_plan(_context(5.0), _signal(0.45), "BUY")

        assert price_limit == Decimal("0.4725")
        assert metadata["price_limit"] == "0.4725"
        assert metadata["leader_price"] == "0.45"
        assert metadata["price_drift_pct"] == "5.0"
        assert metadata["order_type"] == "FAK"
        assert metadata["source"] == "copy_trade"

    def test_sell_limit_and_metadata(self) -> None:
        price_limit, metadata = _copy_entry_price_plan(_context(5.0), _signal(0.55), "SELL")

        assert price_limit == Decimal("0.5225")
        assert metadata["price_limit"] == "0.5225"
        assert metadata["leader_price"] == "0.55"

    def test_zero_drift_passes_the_leader_price_through(self) -> None:
        price_limit, metadata = _copy_entry_price_plan(_context(0.0), _signal(0.45), "BUY")

        assert price_limit == Decimal("0.45")
        assert metadata["price_limit"] == "0.45"
        assert metadata["price_drift_pct"] == "0.0"

    def test_missing_config_falls_back_to_the_shipped_default(self) -> None:
        price_limit, _ = _copy_entry_price_plan(SimpleNamespace(config=None), _signal(0.45), "BUY")

        assert price_limit == Decimal("0.4725")
