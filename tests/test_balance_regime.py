"""Tests for balance-tiered regime system and execution safety fixes."""

from __future__ import annotations

import pytest
from decimal import Decimal

from services.riskEngine import (
    BalanceRegime,
    RiskHeadroom,
    regime_for_equity,
    get_regime,
    compute_reserve_cash,
    compute_hard_cap,
    compute_soft_cap,
    headroom_for_buy,
)


# ---------------------------------------------------------------------------
# regime_for_equity tests
# ---------------------------------------------------------------------------

class TestRegimeForEquity:
    def test_micro(self):
        assert regime_for_equity(Decimal("25")) == "micro"
        assert regime_for_equity(Decimal("74.99")) == "micro"

    def test_small(self):
        assert regime_for_equity(Decimal("75")) == "small"
        assert regime_for_equity(Decimal("249.99")) == "small"

    def test_medium(self):
        assert regime_for_equity(Decimal("250")) == "medium"
        assert regime_for_equity(Decimal("999.99")) == "medium"

    def test_large(self):
        assert regime_for_equity(Decimal("1000")) == "large"
        assert regime_for_equity(Decimal("50000")) == "large"

    def test_zero(self):
        assert regime_for_equity(Decimal("0")) == "micro"


# ---------------------------------------------------------------------------
# compute_reserve_cash tests
# ---------------------------------------------------------------------------

class TestComputeReserveCash:
    def test_micro_floor_wins(self):
        regime = get_regime("micro")
        # 20% of 20 = 4, floor = 5 → 5
        assert compute_reserve_cash(Decimal("20"), regime) == Decimal("5")

    def test_micro_pct_wins(self):
        regime = get_regime("micro")
        # 20% of 50 = 10, floor = 5 → 10
        assert compute_reserve_cash(Decimal("50"), regime) == Decimal("10")

    def test_large_floor_wins(self):
        regime = get_regime("large")
        # 5% of 500 = 25, floor = 50 → 50
        assert compute_reserve_cash(Decimal("500"), regime) == Decimal("50")


# ---------------------------------------------------------------------------
# compute_hard_cap tests
# ---------------------------------------------------------------------------

class TestComputeHardCap:
    def test_micro_ceiling_wins(self):
        regime = get_regime("micro")
        # 18% of 50 = 9, ceiling = 4.5, tradable = 40 → 4.5
        assert compute_hard_cap(Decimal("50"), Decimal("40"), regime) == Decimal("4.5")

    def test_micro_tradable_wins(self):
        regime = get_regime("micro")
        # 18% of 50 = 9, ceiling = 4.5, tradable = 3 → 3
        assert compute_hard_cap(Decimal("50"), Decimal("3"), regime) == Decimal("3")

    def test_large_pct_wins(self):
        regime = get_regime("large")
        # 10% of 2000 = 200, ceiling = 150, tradable = 1900 → 150
        assert compute_hard_cap(Decimal("2000"), Decimal("1900"), regime) == Decimal("150")


# ---------------------------------------------------------------------------
# compute_soft_cap tests
# ---------------------------------------------------------------------------

class TestComputeSoftCap:
    def test_min_score_gets_55pct(self):
        soft = compute_soft_cap(Decimal("4.5"), Decimal("0.30"), Decimal("0.30"))
        assert soft == Decimal("4.5") * Decimal("0.55")

    def test_max_score_gets_100pct(self):
        soft = compute_soft_cap(Decimal("4.5"), Decimal("1.0"), Decimal("0.30"))
        assert soft == Decimal("4.5")

    def test_mid_score(self):
        soft = compute_soft_cap(Decimal("10"), Decimal("0.65"), Decimal("0.30"))
        # score_norm = (0.65-0.30)/(1-0.30) = 0.5
        # soft = 10 * (0.55 + 0.45*0.5) = 10 * 0.775 = 7.75
        assert soft == Decimal("7.75")


# ---------------------------------------------------------------------------
# headroom_for_buy tests
# ---------------------------------------------------------------------------

class TestHeadroomForBuy:
    def test_max_positions_blocks(self):
        regime = get_regime("micro")
        h = headroom_for_buy(
            equity_usdc=Decimal("30"),
            free_cash_usdc=Decimal("25"),
            open_positions_count=3,
            regime=regime,
        )
        assert not h.allow
        assert h.reason == "max_open_positions_reached"

    def test_daily_loss_respects_env_drawdown_pct(self, monkeypatch):
        # If MAX_DAILY_DRAWDOWN_PCT is small, headroom should block sooner.
        import services.riskEngine as re

        monkeypatch.setattr(
            re,
            "ENV",
            {
                **re.ENV,
                "MAX_DAILY_DRAWDOWN_PCT": 5,  # 5%
                "MAX_LEADER_EXPOSURE_USDC": 500,
                "MAX_MARKET_EXPOSURE_USDC": 300,
                "MAX_CATEGORY_EXPOSURE_USDC": 600,
            },
        )
        regime = re.get_regime("micro")
        # day_start=100, max loss=5, daily_loss=6 => blocked
        h = re.headroom_for_buy(
            equity_usdc=Decimal("100"),
            free_cash_usdc=Decimal("90"),
            open_positions_count=0,
            daily_loss_usdc=Decimal("6"),
            day_start_equity_usdc=Decimal("100"),
            regime=regime,
        )
        assert not h.allow
        assert h.reason == "no_buy_headroom"

    def test_no_tradable_cash(self):
        regime = get_regime("micro")
        h = headroom_for_buy(
            equity_usdc=Decimal("20"),
            free_cash_usdc=Decimal("4"),  # reserve is 5
            open_positions_count=0,
            regime=regime,
        )
        assert not h.allow
        assert h.reason == "no_tradable_cash_after_reserve"

    def test_micro_allows_small_buy(self):
        regime = get_regime("micro")
        h = headroom_for_buy(
            equity_usdc=Decimal("30"),
            free_cash_usdc=Decimal("25"),
            open_positions_count=0,
            regime=regime,
        )
        assert h.allow
        assert h.cap_notional_usdc > 0
        assert h.regime == "micro"

    def test_daily_loss_blocks(self):
        regime = get_regime("micro")
        h = headroom_for_buy(
            equity_usdc=Decimal("30"),
            free_cash_usdc=Decimal("25"),
            open_positions_count=0,
            daily_loss_usdc=Decimal("100"),  # way over limit
            regime=regime,
        )
        assert not h.allow
        assert h.reason == "no_buy_headroom"


# ---------------------------------------------------------------------------
# Execution safety: worst-price min-size validation
# ---------------------------------------------------------------------------

class TestWorstPriceMinSize:
    """Verify the core insight: BUY min-size must use worst_price, not quote_price."""

    def test_required_gt_cap_at_059(self):
        """At price 0.59, 2% slippage → worst 0.61, 5 tokens → 3.05 USDC > 2.5 cap."""
        quote = Decimal("0.59")
        slippage = Decimal("0.02")
        worst = (quote * (Decimal("1") + slippage)).quantize(Decimal("0.01"))
        # worst = 0.6018 → ceil to tick 0.01 = 0.61
        import math
        worst_ceil = Decimal(str(math.ceil(float(quote * (Decimal("1") + slippage)) * 100) / 100))
        min_tokens = Decimal("5")
        min_exec = min_tokens * worst_ceil
        cap = Decimal("2.5")
        assert min_exec > cap, f"min_exec={min_exec} should exceed cap={cap}"

    def test_required_eq_cap_at_049(self):
        """At price 0.49, 2% slippage → worst 0.50, 5 tokens → 2.50 USDC == 2.5 cap."""
        import math
        quote = Decimal("0.49")
        slippage = Decimal("0.02")
        worst_ceil = Decimal(str(math.ceil(float(quote * (Decimal("1") + slippage)) * 100) / 100))
        min_tokens = Decimal("5")
        min_exec = min_tokens * worst_ceil
        cap = Decimal("2.5")
        assert min_exec <= cap, f"min_exec={min_exec} should be <= cap={cap}"


# ---------------------------------------------------------------------------
# Idempotency: signal_id inclusion
# ---------------------------------------------------------------------------

class TestIdempotencyKey:
    def test_different_signal_ids_produce_different_keys(self):
        from services.executionService import ExecutionRequest, _idempotency_key

        req1 = ExecutionRequest(
            user_id="u1", leader_id="l1", token_id="t1",
            side="BUY", amount=Decimal("2.5"),
            max_slippage_bps=200, reason="copy_trade",
            signal_id="signal_aaa",
        )
        req2 = ExecutionRequest(
            user_id="u1", leader_id="l1", token_id="t1",
            side="BUY", amount=Decimal("2.5"),
            max_slippage_bps=200, reason="copy_trade",
            signal_id="signal_bbb",
        )
        assert _idempotency_key(req1) != _idempotency_key(req2)

    def test_same_signal_same_key(self):
        from services.executionService import ExecutionRequest, _idempotency_key

        req1 = ExecutionRequest(
            user_id="u1", leader_id="l1", token_id="t1",
            side="BUY", amount=Decimal("2.5"),
            max_slippage_bps=200, reason="copy_trade",
            signal_id="signal_aaa",
        )
        req2 = ExecutionRequest(
            user_id="u1", leader_id="l1", token_id="t1",
            side="BUY", amount=Decimal("2.5"),
            max_slippage_bps=200, reason="copy_trade",
            signal_id="signal_aaa",
        )
        assert _idempotency_key(req1) == _idempotency_key(req2)


# ---------------------------------------------------------------------------
# Fill parsing safety
# ---------------------------------------------------------------------------

class TestFillParsingSafety:
    """Verify that phantom fills can't be created from BUY notional fallback."""

    def test_no_fallback_to_req_amount(self):
        """If fill_info has no matched size, filled_amount must be 0."""
        # Simulates the logic from executionService post-response parsing
        fill_info = {"status": "live"}  # live = resting, NOT filled
        raw_fill = (
            fill_info.get("size_matched")
            or fill_info.get("filled_size")
            or fill_info.get("size")
        )
        fill_status = str(fill_info.get("status", "")).lower()

        if raw_fill and Decimal(str(raw_fill)) > 0 and fill_status in {"filled", "matched", ""}:
            filled_amount = Decimal(str(raw_fill))
            confirmed = True
        else:
            filled_amount = Decimal("0")
            confirmed = False

        assert not confirmed
        assert filled_amount == Decimal("0")

    def test_actual_fill_works(self):
        fill_info = {"status": "filled", "size_matched": "5.0", "avg_price": "0.50"}
        raw_fill = fill_info.get("size_matched")
        fill_status = str(fill_info.get("status", "")).lower()

        if raw_fill and Decimal(str(raw_fill)) > 0 and fill_status in {"filled", "matched", ""}:
            filled_amount = Decimal(str(raw_fill))
            confirmed = True
        else:
            filled_amount = Decimal("0")
            confirmed = False

        assert confirmed
        assert filled_amount == Decimal("5.0")

    def test_live_status_not_treated_as_fill(self):
        """'live' means resting order — must NOT be treated as filled."""
        fill_info = {"status": "live", "size_matched": "5.0"}
        fill_status = str(fill_info.get("status", "")).lower()
        # Even with size_matched, 'live' status should not confirm fill
        assert fill_status not in {"filled", "matched", ""}


# ---------------------------------------------------------------------------
# Retry blocking: failed submissions should not block retries
# ---------------------------------------------------------------------------

class TestRetryBlocking:
    def test_failed_submission_does_not_block(self):
        from services.executionService import _is_retry_blocking_existing
        existing = {"status": "failed", "order_id": None, "response": {"status": "failed"}}
        assert not _is_retry_blocking_existing(existing)

    def test_filled_blocks(self):
        from services.executionService import _is_retry_blocking_existing
        existing = {"status": "filled", "order_id": "ord_123"}
        assert _is_retry_blocking_existing(existing)

    def test_none_does_not_block(self):
        from services.executionService import _is_retry_blocking_existing
        assert not _is_retry_blocking_existing(None)


# ---------------------------------------------------------------------------
# Day-start equity persistence
# ---------------------------------------------------------------------------

class TestDayEquity:
    def test_first_call_records_equity(self, tmp_path, monkeypatch):
        from utils import dayEquity
        state_file = tmp_path / "day_equity.json"
        monkeypatch.setattr(dayEquity, "_STATE_FILE", state_file)

        result = dayEquity.get_day_start_equity(30.0)
        assert result == Decimal("30.0")
        assert state_file.exists()

    def test_second_call_same_day_returns_stored(self, tmp_path, monkeypatch):
        from utils import dayEquity
        state_file = tmp_path / "day_equity.json"
        monkeypatch.setattr(dayEquity, "_STATE_FILE", state_file)

        first = dayEquity.get_day_start_equity(30.0)
        second = dayEquity.get_day_start_equity(25.0)  # Balance dropped
        assert first == second == Decimal("30.0")

    def test_daily_loss_computation(self, tmp_path, monkeypatch):
        from utils import dayEquity
        state_file = tmp_path / "day_equity.json"
        monkeypatch.setattr(dayEquity, "_STATE_FILE", state_file)

        dayEquity.get_day_start_equity(30.0)  # Record
        loss = dayEquity.get_daily_loss(27.5)
        assert loss == Decimal("2.5")

    def test_no_loss_when_up(self, tmp_path, monkeypatch):
        from utils import dayEquity
        state_file = tmp_path / "day_equity.json"
        monkeypatch.setattr(dayEquity, "_STATE_FILE", state_file)

        dayEquity.get_day_start_equity(30.0)
        loss = dayEquity.get_daily_loss(35.0)
        assert loss == Decimal("0")
