"""Leader-price tolerance planning for one-shot copy FAK entries."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from pmcopy.domain.execution import LeaderPriceBand
from pmcopy.orchestrators.pipeline.trade_pipeline import _copy_entry_price_plan


def _context(
    drift_pct: float,
    *,
    max_share_price: str = "0.90",
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(
            execution=SimpleNamespace(
                max_entry_price_drift_pct=drift_pct,
                max_share_price=max_share_price,
            )
        )
    )


def _signal(price: str) -> SimpleNamespace:
    return SimpleNamespace(price=price)


class TestLeaderPriceBand:
    """Characterize the pure reference-price band calculation."""

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
    """Verify the pipeline derives one bounded FAK limit from leader data."""

    def test_buy_limit_and_metadata(self) -> None:
        """Widen a BUY by the configured tolerance and preserve its audit data."""
        price_limit, metadata = _copy_entry_price_plan(_context(5.0), _signal("0.45"), "BUY")

        assert price_limit == Decimal("0.4725")
        assert metadata["price_limit"] == "0.4725"
        assert metadata["leader_price"] == "0.45"
        assert metadata["price_drift_pct"] == "5.0"
        assert metadata["unclamped_price_limit"] == "0.4725"
        assert metadata["order_type"] == "FAK"
        assert metadata["source"] == "copy_trade"

    def test_sell_limit_and_metadata(self) -> None:
        """Widen a SELL downwards without applying the BUY-only hard cap."""
        price_limit, metadata = _copy_entry_price_plan(_context(5.0), _signal("0.55"), "SELL")

        assert price_limit == Decimal("0.5225")
        assert metadata["price_limit"] == "0.5225"
        assert metadata["leader_price"] == "0.55"
        assert metadata["price_drift_pct"] == "5.0"

    def test_ten_percent_is_operator_configurable(self) -> None:
        """Honor the upper accepted tolerance without book-derived repricing."""
        price_limit, metadata = _copy_entry_price_plan(_context(10.0), _signal("0.45"), "BUY")

        assert price_limit == Decimal("0.4950")
        assert metadata["price_drift_pct"] == "10.0"

    def test_buy_limit_is_clamped_to_the_global_price_ceiling(self) -> None:
        """Do not let tolerance weaken the configured maximum BUY price."""
        leader_price = "0.8844257692428440966707673026"

        price_limit, metadata = _copy_entry_price_plan(
            _context(5.0, max_share_price="0.90"),
            _signal(leader_price),
            "BUY",
        )

        assert price_limit == Decimal("0.90")
        assert metadata["leader_price"] == leader_price
        assert metadata["unclamped_price_limit"] == "0.9287"
        assert metadata["max_share_price"] == "0.90"

    def test_reported_failures_gain_a_marketable_tolerance(self) -> None:
        """Reproduce the three stale-price examples from the operator log."""
        cases = (
            ("0.84", Decimal("0.8820")),
            ("0.8844257692428440966707673026", Decimal("0.90")),
            ("0.1260365854", Decimal("0.1324")),
        )

        for leader_price, expected_limit in cases:
            price_limit, _ = _copy_entry_price_plan(
                _context(5.0, max_share_price="0.90"),
                _signal(leader_price),
                "BUY",
            )
            assert price_limit == expected_limit

    def test_missing_config_uses_the_shipped_defaults(self) -> None:
        """Keep the safe five-percent behavior for lightweight contexts."""
        price_limit, metadata = _copy_entry_price_plan(
            SimpleNamespace(config=None),
            _signal("0.45"),
            "BUY",
        )

        assert price_limit == Decimal("0.4725")
        assert metadata["price_drift_pct"] == "5.0"


def _leader_aware_context(
    drift_by_leader: dict[str, float],
    default_drift: float,
    *,
    max_share_price: str = "0.90",
) -> SimpleNamespace:
    """Build a context whose config resolves the band per leader."""

    def _resolve(wallet: str) -> float:
        return drift_by_leader.get(str(wallet).lower(), default_drift)

    return SimpleNamespace(
        config=SimpleNamespace(
            execution=SimpleNamespace(
                max_entry_price_drift_pct=default_drift,
                max_share_price=max_share_price,
            ),
            entry_price_drift_pct_for_leader=_resolve,
        )
    )


SWEEPING_LEADER = "0xbca08c1bc204a34f2fddbe47b438b9bd42ac9705"
PASSIVE_LEADER = "0x" + "d" * 40


class TestPerLeaderEntryBand:
    """The FAK band is resolved per leader, not from one global constant."""

    def test_leader_specific_band_widens_only_that_leader(self) -> None:
        context = _leader_aware_context({SWEEPING_LEADER: 10.0}, 5.0)

        swept, swept_meta = _copy_entry_price_plan(
            context, _signal("0.45"), "BUY", leader_id=SWEEPING_LEADER
        )
        passive, passive_meta = _copy_entry_price_plan(
            context, _signal("0.45"), "BUY", leader_id=PASSIVE_LEADER
        )

        assert swept == Decimal("0.4950")
        assert passive == Decimal("0.4725")
        assert swept_meta["price_drift_pct"] == "10.0"
        assert passive_meta["price_drift_pct"] == "5.0"

    def test_leader_matching_is_case_insensitive(self) -> None:
        context = _leader_aware_context({SWEEPING_LEADER: 10.0}, 5.0)

        price_limit, _ = _copy_entry_price_plan(
            context, _signal("0.45"), "BUY", leader_id=SWEEPING_LEADER.upper()
        )

        assert price_limit == Decimal("0.4950")

    def test_sell_floor_also_uses_the_leader_band(self) -> None:
        context = _leader_aware_context({SWEEPING_LEADER: 10.0}, 5.0)

        price_limit, _ = _copy_entry_price_plan(
            context, _signal("0.55"), "SELL", leader_id=SWEEPING_LEADER
        )

        assert price_limit == Decimal("0.4950")

    def test_config_without_the_resolver_falls_back_to_execution(self) -> None:
        """A fake or degraded config still yields the execution-layer band."""
        price_limit, _ = _copy_entry_price_plan(
            _context(5.0), _signal("0.45"), "BUY", leader_id=SWEEPING_LEADER
        )

        assert price_limit == Decimal("0.4725")


class TestReplayOfLoggedRejections:
    """Replay the 2026-08-27 live rejections against the widened band.

    The asks quoted here were reconstructed from Polymarket's public trade
    history immediately after each of this leader's own fills, which had swept
    the top of the book.
    """

    def test_first_rejection_becomes_marketable_at_ten_percent(self) -> None:
        context = _leader_aware_context({SWEEPING_LEADER: 10.0}, 5.0)

        price_limit, _ = _copy_entry_price_plan(
            context,
            _signal("0.6519110295241325993939021673"),
            "BUY",
            leader_id=SWEEPING_LEADER,
        )

        assert price_limit == Decimal("0.7172")
        assert price_limit > Decimal("0.70")  # the ask that killed the 0.68 order

    def test_second_rejection_becomes_marketable_at_ten_percent(self) -> None:
        context = _leader_aware_context({SWEEPING_LEADER: 10.0}, 5.0)

        price_limit, _ = _copy_entry_price_plan(
            context, _signal("0.72"), "BUY", leader_id=SWEEPING_LEADER
        )

        assert price_limit == Decimal("0.7920")
        assert price_limit > Decimal("0.77")  # the ask that killed the 0.75 order

    def test_third_rejection_stays_capped_by_max_share_price(self) -> None:
        """The 0.82 -> 0.94 gap is beyond any band; the global ceiling holds."""
        context = _leader_aware_context({SWEEPING_LEADER: 10.0}, 5.0)

        price_limit, metadata = _copy_entry_price_plan(
            context, _signal("0.82"), "BUY", leader_id=SWEEPING_LEADER
        )

        assert metadata["unclamped_price_limit"] == "0.9020"
        assert price_limit == Decimal("0.90")
        assert price_limit < Decimal("0.94")  # correctly refuses to chase
