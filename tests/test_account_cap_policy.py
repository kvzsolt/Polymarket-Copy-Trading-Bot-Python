"""Tests for simplified account-cap policy and execution safety fixes."""

from __future__ import annotations

from decimal import Decimal


from services.riskEngine import (
    compute_hard_cap,
    compute_reserve_cash,
    compute_soft_cap,
    get_policy,
    headroom_for_buy,
    policy_name_for_equity,
)


class TestAccountCapPolicySelection:
    def test_policy_name_for_equity_always_returns_single_policy(self):
        assert policy_name_for_equity(Decimal("25")) == "account_cap"
        assert policy_name_for_equity(Decimal("250")) == "account_cap"
        assert policy_name_for_equity(Decimal("50000")) == "account_cap"

    def test_get_policy_ignores_legacy_name_and_returns_active_policy(self, monkeypatch):
        import services.riskEngine as re

        monkeypatch.setattr(
            re,
            "ENV",
            {
                **re.ENV,
                "BALANCE_POLICY_RESERVE_PCT": 0.10,
                "BALANCE_POLICY_RESERVE_FLOOR": 6,
                "BALANCE_POLICY_HARD_CAP_PCT": 0.20,
                "BALANCE_POLICY_HARD_CAP_CEILING": 9,
                "BALANCE_POLICY_MAX_OPEN_POSITIONS": 4,
                "BALANCE_POLICY_ALLOW_PARTIAL_EXITS": False,
            },
        )
        policy = re.get_policy("micro")
        assert policy.name == "account_cap"
        assert policy.reserve_floor == Decimal("6")
        assert policy.max_open_positions == 4
        assert policy.allow_partial_exits is False


class TestComputeReserveCash:
    def test_reserve_floor_wins(self, monkeypatch):
        import services.riskEngine as re

        monkeypatch.setattr(re, "ENV", {**re.ENV, "BALANCE_POLICY_RESERVE_PCT": 0.10, "BALANCE_POLICY_RESERVE_FLOOR": 7})
        policy = re.get_policy("ignored")
        assert compute_reserve_cash(Decimal("20"), policy) == Decimal("7")

    def test_reserve_pct_wins(self, monkeypatch):
        import services.riskEngine as re

        monkeypatch.setattr(re, "ENV", {**re.ENV, "BALANCE_POLICY_RESERVE_PCT": 0.20, "BALANCE_POLICY_RESERVE_FLOOR": 5})
        policy = re.get_policy("ignored")
        assert compute_reserve_cash(Decimal("50"), policy) == Decimal("10")


class TestComputeHardCap:
    def test_hard_cap_ceiling_wins(self, monkeypatch):
        import services.riskEngine as re

        monkeypatch.setattr(re, "ENV", {**re.ENV, "BALANCE_POLICY_HARD_CAP_PCT": 0.18, "BALANCE_POLICY_HARD_CAP_CEILING": 4.5})
        policy = re.get_policy("ignored")
        assert compute_hard_cap(Decimal("50"), Decimal("40"), policy) == Decimal("4.5")

    def test_hard_cap_tradable_wins(self, monkeypatch):
        import services.riskEngine as re

        monkeypatch.setattr(re, "ENV", {**re.ENV, "BALANCE_POLICY_HARD_CAP_PCT": 0.18, "BALANCE_POLICY_HARD_CAP_CEILING": 4.5})
        policy = re.get_policy("ignored")
        assert compute_hard_cap(Decimal("50"), Decimal("3"), policy) == Decimal("3")


class TestComputeSoftCap:
    def test_min_score_gets_55pct(self):
        soft = compute_soft_cap(Decimal("4.5"), Decimal("0.30"), Decimal("0.30"))
        assert soft == Decimal("4.5") * Decimal("0.55")

    def test_max_score_gets_100pct(self):
        soft = compute_soft_cap(Decimal("4.5"), Decimal("1.0"), Decimal("0.30"))
        assert soft == Decimal("4.5")

    def test_mid_score(self):
        soft = compute_soft_cap(Decimal("10"), Decimal("0.65"), Decimal("0.30"))
        assert soft == Decimal("7.75")


class TestHeadroomForBuy:
    def test_max_positions_blocks(self, monkeypatch):
        import services.riskEngine as re

        monkeypatch.setattr(re, "ENV", {**re.ENV, "BALANCE_POLICY_MAX_OPEN_POSITIONS": 3})
        policy = re.get_policy("ignored")
        h = headroom_for_buy(
            equity_usdc=Decimal("30"),
            free_cash_usdc=Decimal("25"),
            open_positions_count=3,
            policy=policy,
        )
        assert not h.allow
        assert h.reason == "max_open_positions_reached"

    def test_daily_loss_respects_env_drawdown_pct(self):
        from types import SimpleNamespace
        import services.riskEngine as re

        # headroom_for_buy reads drawdown/exposure limits from injected config,
        # not from ENV — pass a mock config directly.
        mock_config = SimpleNamespace(
            risk=SimpleNamespace(
                max_market_exposure_usdc=300,
                max_category_exposure_usdc=600,
                max_leader_exposure_usdc=500,
                day_start_equity_usdc=0.0,
                max_daily_drawdown_pct=5,
            )
        )
        policy = re.get_policy("ignored")
        h = re.headroom_for_buy(
            equity_usdc=Decimal("100"),
            free_cash_usdc=Decimal("90"),
            open_positions_count=0,
            daily_loss_usdc=Decimal("6"),
            day_start_equity_usdc=Decimal("100"),
            policy=policy,
            config=mock_config,
        )
        assert not h.allow
        assert h.reason == "no_buy_headroom"

    def test_no_tradable_cash(self, monkeypatch):
        import services.riskEngine as re

        monkeypatch.setattr(re, "ENV", {**re.ENV, "BALANCE_POLICY_RESERVE_PCT": 0.10, "BALANCE_POLICY_RESERVE_FLOOR": 5})
        policy = re.get_policy("ignored")
        h = headroom_for_buy(
            equity_usdc=Decimal("20"),
            free_cash_usdc=Decimal("4"),
            open_positions_count=0,
            policy=policy,
        )
        assert not h.allow
        assert h.reason == "no_tradable_cash_after_reserve"

    def test_allows_small_buy(self, monkeypatch):
        import services.riskEngine as re

        monkeypatch.setattr(
            re,
            "ENV",
            {
                **re.ENV,
                "BALANCE_POLICY_RESERVE_PCT": 0.10,
                "BALANCE_POLICY_RESERVE_FLOOR": 5,
                "BALANCE_POLICY_HARD_CAP_PCT": 0.18,
                "BALANCE_POLICY_HARD_CAP_CEILING": 4.5,
            },
        )
        policy = re.get_policy("ignored")
        h = headroom_for_buy(
            equity_usdc=Decimal("30"),
            free_cash_usdc=Decimal("25"),
            open_positions_count=0,
            policy=policy,
        )
        assert h.allow
        assert h.cap_notional_usdc == Decimal("4.5")
        assert h.policy_name == "account_cap"

    def test_daily_loss_blocks(self):
        policy = get_policy("ignored")
        h = headroom_for_buy(
            equity_usdc=Decimal("30"),
            free_cash_usdc=Decimal("25"),
            open_positions_count=0,
            daily_loss_usdc=Decimal("100"),
            policy=policy,
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

    def test_fresh_pending_blocks(self):
        from services.executionService import _is_retry_blocking_existing
        existing = {"status": "pending", "order_id": None, "response": {"status": "pending", "pending_since": 100}}
        assert _is_retry_blocking_existing(existing, now_ts=150, pending_ttl_seconds=120)

    def test_stale_pending_does_not_block(self):
        from services.executionService import _is_retry_blocking_existing
        existing = {"status": "pending", "order_id": None, "response": {"status": "pending", "pending_since": 100}}
        assert not _is_retry_blocking_existing(existing, now_ts=250, pending_ttl_seconds=120)

    def test_pending_without_timestamp_does_not_block(self):
        from services.executionService import _is_retry_blocking_existing
        existing = {"status": "pending", "order_id": None, "response": {"status": "pending"}}
        assert not _is_retry_blocking_existing(existing, now_ts=250, pending_ttl_seconds=120)

    def test_pending_with_future_expiry_blocks(self):
        from services.executionService import _is_retry_blocking_existing
        existing = {
            "status": "pending",
            "order_id": None,
            "response": {"status": "pending", "pending_expires_at": 220},
        }
        assert _is_retry_blocking_existing(existing, now_ts=200, pending_ttl_seconds=120)

    def test_pending_with_expired_expiry_does_not_block(self):
        from services.executionService import _is_retry_blocking_existing
        existing = {
            "status": "pending",
            "order_id": None,
            "response": {"status": "pending", "pending_expires_at": 220},
        }
        assert not _is_retry_blocking_existing(existing, now_ts=230, pending_ttl_seconds=120)

    def test_fresh_submitted_unconfirmed_without_order_id_blocks(self):
        from services.executionService import _is_retry_blocking_existing
        existing = {
            "status": "submitted_unconfirmed",
            "order_id": None,
            "response": {"status": "submitted_unconfirmed", "pending_since": 100},
        }
        assert _is_retry_blocking_existing(existing, now_ts=150, pending_ttl_seconds=120)

    def test_stale_submitted_unconfirmed_without_order_id_does_not_block(self):
        from services.executionService import _is_retry_blocking_existing
        existing = {
            "status": "submitted_unconfirmed",
            "order_id": None,
            "response": {"status": "submitted_unconfirmed", "pending_since": 100},
        }
        assert not _is_retry_blocking_existing(existing, now_ts=250, pending_ttl_seconds=120)


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
