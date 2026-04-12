"""Category 3: Position math.

Covers:
- track_buy creates new position correctly
- Pyramiding: weighted average entry price
- Pyramiding: peak_price updates correctly
- Partial sell: size_tokens decreases, realized PnL calculated
- Partial sell: position stays open when tokens remain
- Full sell: position moves to closed_positions
- Oversell protection: selling more tokens than held
- Summary stats: open count, closed count, PnL, win rate
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import RiskSettings
from services.positionManager import PositionManager, TrackedPosition


def _default_risk() -> RiskSettings:
    """Create default risk settings for position math tests."""
    return RiskSettings(
        stop_loss_ratio=0.20,
        take_profit_ratio=0.70,
        daily_drawdown_ratio=0.20,
        profit_lock_trigger_ratio=0.70,
        profit_lock_drawdown_ratio=0.15,
        profit_lock_sell_ratio=0.50,
        trailing_trigger_ratio=0.20,
        trailing_distance_ratio=0.05,
        near_max_price_level=0.95,
        near_max_warn_level=0.92,
    )


def _make_pm() -> PositionManager:
    """Create a PositionManager with default risk settings and features off."""
    return PositionManager(risk_settings=_default_risk())


# ---------------------------------------------------------------------------
# track_buy creates new position correctly
# ---------------------------------------------------------------------------


class TestTrackBuyNewPosition:
    def test_creates_position(self):
        pm = _make_pm()
        pos = pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        assert pos.condition_id == "cond-1"
        assert pos.asset == "asset-1"
        assert pos.side == "BUY"
        assert pos.entry_price == pytest.approx(0.50)
        assert pos.size_tokens == pytest.approx(100.0)
        assert pos.usdc_cost == pytest.approx(50.0)

    def test_peak_price_initialized_to_entry(self):
        pm = _make_pm()
        pos = pm.track_buy("cond-1", "asset-1", 0.45, 200.0, 90.0)
        assert pos.peak_price == pytest.approx(0.45)

    def test_position_in_open_positions(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        assert "cond-1" in pm.open_positions
        assert len(pm.open_positions) == 1

    def test_metadata_preserved(self):
        pm = _make_pm()
        pos = pm.track_buy(
            "cond-1", "asset-1", 0.50, 100.0, 50.0,
            leader_wallet="0xABC",
            event_slug="test-event",
            title="Test Market",
            category="politics",
        )
        assert pos.leader_wallet == "0xABC"
        assert pos.event_slug == "test-event"
        assert pos.title == "Test Market"
        assert pos.category == "politics"


# ---------------------------------------------------------------------------
# Pyramiding: weighted average entry price
# ---------------------------------------------------------------------------


class TestPyramiding:
    def test_two_buys_weighted_average(self):
        pm = _make_pm()
        # Buy 1: 100 tokens @ $0.50
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        # Buy 2: 100 tokens @ $0.70
        pos = pm.track_buy("cond-1", "asset-1", 0.70, 100.0, 70.0)
        # Weighted avg = (0.50*100 + 0.70*100) / 200 = 0.60
        assert pos.entry_price == pytest.approx(0.60)
        assert pos.size_tokens == pytest.approx(200.0)
        assert pos.usdc_cost == pytest.approx(120.0)

    def test_three_buys_weighted_average(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.40, 50.0, 20.0)
        pm.track_buy("cond-1", "asset-1", 0.60, 50.0, 30.0)
        pos = pm.track_buy("cond-1", "asset-1", 0.80, 100.0, 80.0)
        # avg = (0.40*50 + 0.60*50 + 0.80*100) / 200 = (20+30+80)/200 = 0.65
        assert pos.entry_price == pytest.approx(0.65)
        assert pos.size_tokens == pytest.approx(200.0)
        assert pos.usdc_cost == pytest.approx(130.0)

    def test_unequal_sizes_weighted_average(self):
        pm = _make_pm()
        # Small first buy, large second buy
        pm.track_buy("cond-1", "asset-1", 0.30, 10.0, 3.0)
        pos = pm.track_buy("cond-1", "asset-1", 0.50, 90.0, 45.0)
        # avg = (0.30*10 + 0.50*90) / 100 = (3+45)/100 = 0.48
        assert pos.entry_price == pytest.approx(0.48)

    def test_still_single_position_after_pyramiding(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pm.track_buy("cond-1", "asset-1", 0.60, 100.0, 60.0)
        assert len(pm.open_positions) == 1
        assert "cond-1" in pm.open_positions


# ---------------------------------------------------------------------------
# Pyramiding: peak_price updates correctly
# ---------------------------------------------------------------------------


class TestPeakPriceUpdate:
    def test_peak_price_updates_on_higher_buy(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pos = pm.track_buy("cond-1", "asset-1", 0.70, 100.0, 70.0)
        assert pos.peak_price == pytest.approx(0.70)

    def test_peak_price_stays_on_lower_buy(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.70, 100.0, 70.0)
        pos = pm.track_buy("cond-1", "asset-1", 0.40, 100.0, 40.0)
        assert pos.peak_price == pytest.approx(0.70)

    def test_peak_price_same_price(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pos = pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        assert pos.peak_price == pytest.approx(0.50)


# ---------------------------------------------------------------------------
# Partial sell
# ---------------------------------------------------------------------------


class TestPartialSell:
    def test_size_decreases(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        result = pm.track_sell("cond-1", 0.60, 40.0)
        # Not fully closed (60 tokens remain)
        assert result is None
        pos = pm.get_position("cond-1")
        assert pos is not None
        assert pos.size_tokens == pytest.approx(60.0)

    def test_realized_pnl_calculated(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        # Sell 40 tokens at $0.60 -> realized = (0.60-0.50) * 40 = $4.00
        pm.track_sell("cond-1", 0.60, 40.0)
        pos = pm.get_position("cond-1")
        assert pos.realized_pnl == pytest.approx(4.00)

    def test_partial_sell_loss(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        # Sell 40 tokens at $0.40 -> realized = (0.40-0.50) * 40 = -$4.00
        pm.track_sell("cond-1", 0.40, 40.0)
        pos = pm.get_position("cond-1")
        assert pos.realized_pnl == pytest.approx(-4.00)

    def test_position_stays_open(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pm.track_sell("cond-1", 0.60, 50.0)
        assert "cond-1" in pm.open_positions
        assert len(pm.closed_positions) == 0

    def test_multiple_partial_sells_accumulate_pnl(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pm.track_sell("cond-1", 0.60, 30.0)  # +$3.00
        pm.track_sell("cond-1", 0.70, 20.0)  # +$4.00
        pos = pm.get_position("cond-1")
        assert pos.realized_pnl == pytest.approx(7.00)
        assert pos.size_tokens == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# Full sell: position moves to closed_positions
# ---------------------------------------------------------------------------


class TestFullSell:
    def test_position_closed(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        result = pm.track_sell("cond-1", 0.60, 100.0)
        assert result is not None
        assert result.exit_price == pytest.approx(0.60)
        assert result.realized_pnl == pytest.approx(10.00)
        assert result.exit_reason == "leader_sell"

    def test_removed_from_open(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pm.track_sell("cond-1", 0.60, 100.0)
        assert "cond-1" not in pm.open_positions
        assert len(pm.closed_positions) == 1

    def test_exit_fields_populated(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        result = pm.track_sell("cond-1", 0.80, 100.0, reason="take_profit")
        assert result.exit_price == pytest.approx(0.80)
        assert result.exit_time is not None
        assert result.exit_reason == "take_profit"
        assert result.realized_pnl == pytest.approx(30.00)

    def test_size_zeroed_on_close(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        result = pm.track_sell("cond-1", 0.60, 100.0)
        assert result.size_tokens == 0.0

    def test_sell_nonexistent_position_returns_none(self):
        pm = _make_pm()
        result = pm.track_sell("cond-nonexist", 0.60, 100.0)
        assert result is None


# ---------------------------------------------------------------------------
# Oversell protection: selling more tokens than held
# ---------------------------------------------------------------------------


class TestOversellProtection:
    def test_sell_more_than_held_closes_position(self):
        """Selling more tokens than held should close the position (size goes to 0).
        PnL should be based on actual held amount, not the oversold amount."""
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        # Sell 150 tokens when only 100 held -> should close
        result = pm.track_sell("cond-1", 0.60, 150.0)
        assert result is not None  # position closed
        assert result.size_tokens == 0.0
        assert "cond-1" not in pm.open_positions
        # PnL should be based on held amount (100), not oversold amount (150)
        assert result.realized_pnl == pytest.approx(10.0)  # (0.60 - 0.50) * 100

    def test_sell_exactly_threshold_closes(self):
        """Selling down to the threshold (0.001) should close the position."""
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        # Sell exactly 100 tokens -> remaining 0, which is <= 0.001
        result = pm.track_sell("cond-1", 0.60, 100.0)
        assert result is not None
        assert result.size_tokens == 0.0

    def test_sell_just_above_threshold_stays_open(self):
        """Selling down to just above threshold keeps position open."""
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        # Sell 99.5 tokens -> remaining 0.5 > 0.001, stays open
        result = pm.track_sell("cond-1", 0.60, 99.5)
        assert result is None  # not closed
        pos = pm.get_position("cond-1")
        assert pos.size_tokens == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Summary stats
# ---------------------------------------------------------------------------


class TestSummaryStats:
    def test_empty_summary(self):
        pm = _make_pm()
        s = pm.summary()
        assert s["open_positions"] == 0
        assert s["closed_positions"] == 0
        assert s["total_realized_pnl"] == 0.0
        assert s["wins"] == 0
        assert s["losses"] == 0

    def test_open_count(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pm.track_buy("cond-2", "asset-2", 0.30, 200.0, 60.0)
        s = pm.summary()
        assert s["open_positions"] == 2

    def test_closed_count(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pm.track_sell("cond-1", 0.60, 100.0)
        s = pm.summary()
        assert s["open_positions"] == 0
        assert s["closed_positions"] == 1

    def test_realized_pnl(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pm.track_sell("cond-1", 0.70, 100.0)  # +$20.00
        pm.track_buy("cond-2", "asset-2", 0.40, 100.0, 40.0)
        pm.track_sell("cond-2", 0.30, 100.0)  # -$10.00
        s = pm.summary()
        assert s["total_realized_pnl"] == pytest.approx(10.00)

    def test_win_rate(self):
        pm = _make_pm()
        # Win 1
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pm.track_sell("cond-1", 0.70, 100.0)  # profit
        # Win 2
        pm.track_buy("cond-2", "asset-2", 0.30, 100.0, 30.0)
        pm.track_sell("cond-2", 0.40, 100.0)  # profit
        # Loss
        pm.track_buy("cond-3", "asset-3", 0.60, 100.0, 60.0)
        pm.track_sell("cond-3", 0.40, 100.0)  # loss
        s = pm.summary()
        assert s["wins"] == 2
        assert s["losses"] == 1
        assert s["win_rate"] == pytest.approx(66.7, abs=0.1)

    def test_total_invested(self):
        pm = _make_pm()
        pm.track_buy("cond-1", "asset-1", 0.50, 100.0, 50.0)
        pm.track_buy("cond-2", "asset-2", 0.30, 200.0, 60.0)
        s = pm.summary()
        assert s["total_invested"] == pytest.approx(110.0)

    def test_win_rate_no_trades(self):
        pm = _make_pm()
        s = pm.summary()
        assert s["win_rate"] == 0.0
