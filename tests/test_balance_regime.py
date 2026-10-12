"""Phase 4 — balance regime gate tests."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from types import SimpleNamespace

import pytest

from pmcopy.services.balance_regime import (
    BalanceDecision,
    BalanceRegime,
    _DailySpendLedger,
    _to_decimal,
)


class TestDailySpendLedger:
    def test_zero_records_skipped(self) -> None:
        ledger = _DailySpendLedger()
        ledger.record(Decimal("0"))
        ledger.record(Decimal("-5"))
        assert ledger.total() == Decimal("0")

    def test_accumulates(self) -> None:
        ledger = _DailySpendLedger()
        ledger.record(Decimal("10"))
        ledger.record(Decimal("15.5"))
        assert ledger.total() == Decimal("25.5")

    def test_evicts_old_entries(self) -> None:
        ledger = _DailySpendLedger(window_seconds=10.0)
        base = time.time()
        ledger.record(Decimal("5"), now=base - 100)  # well outside window
        ledger.record(Decimal("7"), now=base - 1)
        assert ledger.total(now=base) == Decimal("7")

    def test_reset_clears(self) -> None:
        ledger = _DailySpendLedger()
        ledger.record(Decimal("100"))
        ledger.reset()
        assert ledger.total() == Decimal("0")


class TestBalanceRegimeDisabledByDefault:
    def test_zero_thresholds_allow_when_funded(self) -> None:
        """All caps disabled (0) → gate is a no-op when free cash covers the BUY."""
        regime = BalanceRegime()
        decision = regime.check_buy(notional_usdc=10_000, free_cash_usdc=10_000)
        assert decision.allow is True
        assert decision.reason == "ok"

    def test_zero_thresholds_still_block_overdraft(self) -> None:
        """Even with all knobs disabled, a BUY that would drive free cash
        below zero is rejected. ``MIN_BALANCE_HEADROOM_USDC=0`` must not
        authorise overdrawing the wallet."""
        regime = BalanceRegime()
        decision = regime.check_buy(notional_usdc=10_000, free_cash_usdc=10)
        assert decision.allow is False
        assert decision.skip_code == "balance_regime_insufficient_free_cash"

    def test_zero_amount_always_rejected(self) -> None:
        regime = BalanceRegime(max_position_size_usdc=100)
        decision = regime.check_buy(notional_usdc=0, free_cash_usdc=1000)
        assert decision.allow is False
        assert decision.skip_code == "balance_regime_zero_amount"


class TestBalanceRegimePositionCap:
    def test_within_cap_allowed(self) -> None:
        regime = BalanceRegime(max_position_size_usdc=50)
        decision = regime.check_buy(notional_usdc="49", free_cash_usdc="1000")
        assert decision.allow is True

    def test_at_cap_allowed(self) -> None:
        regime = BalanceRegime(max_position_size_usdc=50)
        decision = regime.check_buy(notional_usdc="50", free_cash_usdc="1000")
        assert decision.allow is True

    def test_above_cap_rejected(self) -> None:
        regime = BalanceRegime(max_position_size_usdc=50)
        decision = regime.check_buy(notional_usdc="50.01", free_cash_usdc="1000")
        assert decision.allow is False
        assert decision.policy_name == "max_position_size_usdc"
        assert decision.skip_code == "balance_regime_position_size_exceeded"


class TestBalanceRegimeDailySpend:
    def test_first_buy_within_ceiling(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        decision = regime.check_buy(notional_usdc="60", free_cash_usdc="1000")
        assert decision.allow is True

    def test_cumulative_blocks_next_buy(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        regime.record_buy(Decimal("70"))
        decision = regime.check_buy(notional_usdc="31", free_cash_usdc="1000")
        assert decision.allow is False
        assert decision.policy_name == "max_daily_spend_usdc"
        assert decision.skip_code == "balance_regime_daily_spend_exceeded"

    def test_cumulative_allows_remaining_budget(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        regime.record_buy(Decimal("70"))
        decision = regime.check_buy(notional_usdc="30", free_cash_usdc="1000")
        assert decision.allow is True
        assert decision.daily_remaining_usdc == Decimal("30")

    def test_reset_unlocks_daily_budget(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        regime.record_buy(Decimal("100"))
        regime.reset_daily_ledger()
        decision = regime.check_buy(notional_usdc="50", free_cash_usdc="1000")
        assert decision.allow is True


class TestBalanceRegimeHeadroom:
    def test_above_floor_allowed(self) -> None:
        regime = BalanceRegime(min_balance_headroom_usdc=10)
        decision = regime.check_buy(notional_usdc="50", free_cash_usdc="100")
        assert decision.allow is True

    def test_below_floor_rejected(self) -> None:
        regime = BalanceRegime(min_balance_headroom_usdc=10)
        decision = regime.check_buy(notional_usdc="95", free_cash_usdc="100")
        assert decision.allow is False
        assert decision.policy_name == "min_balance_headroom_usdc"
        assert decision.skip_code == "balance_regime_headroom_floor"


class TestBalanceRegimeOrdering:
    """Per-position cap is checked before daily-spend and headroom so the
    first-tripped rule wins and skip codes stay deterministic."""

    def test_position_cap_wins_over_daily(self) -> None:
        regime = BalanceRegime(
            max_position_size_usdc=10,
            max_daily_spend_usdc=10,
        )
        decision = regime.check_buy(notional_usdc="50", free_cash_usdc="1000")
        assert decision.skip_code == "balance_regime_position_size_exceeded"

    def test_daily_wins_over_headroom(self) -> None:
        regime = BalanceRegime(
            max_daily_spend_usdc=10,
            min_balance_headroom_usdc=999,
        )
        decision = regime.check_buy(notional_usdc="20", free_cash_usdc="100")
        assert decision.skip_code == "balance_regime_daily_spend_exceeded"


class TestBalanceRegimeFromConfig:
    def test_reads_execution_config(self) -> None:
        cfg = SimpleNamespace(
            max_position_size_usdc=75,
            max_daily_spend_usdc=500,
            min_balance_headroom_usdc=25,
        )
        regime = BalanceRegime.from_config(cfg)
        thresholds = regime.thresholds()
        assert thresholds["max_position_size_usdc"] == Decimal("75")
        assert thresholds["max_daily_spend_usdc"] == Decimal("500")
        assert thresholds["min_balance_headroom_usdc"] == Decimal("25")

    def test_missing_fields_default_to_zero(self) -> None:
        regime = BalanceRegime.from_config(SimpleNamespace())
        thresholds = regime.thresholds()
        assert thresholds["max_position_size_usdc"] == Decimal("0")
        assert thresholds["max_daily_spend_usdc"] == Decimal("0")
        assert thresholds["min_balance_headroom_usdc"] == Decimal("0")


class TestHydrateLedger:
    def test_hydrate_seeds_ledger(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=200)
        now = time.time()
        regime.hydrate([(now - 60, Decimal("80")), (now - 120, Decimal("50"))])
        assert regime.daily_spent_usdc == Decimal("130")

    def test_hydrate_drops_stale_entries(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=200)
        now = time.time()
        # Older than 24h — must be evicted on next total() lookup.
        regime.hydrate([(now - (25 * 3600), Decimal("80"))])
        assert regime.daily_spent_usdc == Decimal("0")


class TestBalanceDecisionFields:
    def test_decision_exposes_diagnostic_fields(self) -> None:
        regime = BalanceRegime(max_position_size_usdc=100, max_daily_spend_usdc=500)
        regime.record_buy(Decimal("75"))
        decision: BalanceDecision = regime.check_buy(notional_usdc="40", free_cash_usdc="200")
        assert decision.daily_spent_usdc == Decimal("75")
        assert decision.daily_remaining_usdc == Decimal("425")
        assert decision.headroom_usdc == Decimal("160")


class TestToDecimal:
    """Bad input is rejected loudly and NaN/Inf never reach the gate."""

    def test_invalid_string_returns_default(self) -> None:
        assert _to_decimal("not-a-number") == Decimal("0")
        assert _to_decimal("not-a-number", default="5") == Decimal("5")

    def test_none_returns_default(self) -> None:
        assert _to_decimal(None) == Decimal("0")

    def test_nan_rejected(self) -> None:
        assert _to_decimal("NaN") == Decimal("0")
        assert _to_decimal(float("nan")) == Decimal("0")

    def test_infinity_rejected(self) -> None:
        assert _to_decimal("Infinity") == Decimal("0")
        assert _to_decimal(float("inf")) == Decimal("0")
        assert _to_decimal(float("-inf")) == Decimal("0")

    def test_valid_inputs_pass_through(self) -> None:
        assert _to_decimal(42) == Decimal("42")
        assert _to_decimal("3.14") == Decimal("3.14")
        assert _to_decimal(Decimal("1.5")) == Decimal("1.5")


class TestNegativeHeadroomGuard:
    """Headroom-floor=0 must NOT authorise overdrawing the wallet."""

    def test_overdraft_rejected_with_all_caps_disabled(self) -> None:
        regime = BalanceRegime()
        decision = regime.check_buy(notional_usdc="100", free_cash_usdc="50")
        assert decision.allow is False
        assert decision.skip_code == "balance_regime_insufficient_free_cash"

    def test_exact_match_still_allowed(self) -> None:
        regime = BalanceRegime()
        decision = regime.check_buy(notional_usdc="50", free_cash_usdc="50")
        assert decision.allow is True


class TestRecordCancel:
    def test_record_cancel_rolls_back_ledger(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        regime.record_buy(Decimal("70"))
        assert regime.daily_spent_usdc == Decimal("70")
        regime.record_cancel(Decimal("70"))
        assert regime.daily_spent_usdc == Decimal("0")

    def test_record_cancel_unblocks_next_buy(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        regime.record_buy(Decimal("80"))
        # Cumulative cap exceeded before cancel.
        d_blocked = regime.check_buy(notional_usdc="30", free_cash_usdc="1000")
        assert d_blocked.allow is False
        regime.record_cancel(Decimal("80"))
        d_allowed = regime.check_buy(notional_usdc="30", free_cash_usdc="1000")
        assert d_allowed.allow is True

    def test_cancel_by_order_id_uses_recorded_amount(self) -> None:
        regime = BalanceRegime()
        regime.record_buy(Decimal("42.50"), order_id="ord-1")
        # Caller passes a garbage amount — order_id mapping wins.
        regime.record_cancel(Decimal("999"), order_id="ord-1")
        assert regime.daily_spent_usdc == Decimal("0")

    def test_double_cancel_does_not_go_negative(self) -> None:
        regime = BalanceRegime()
        regime.record_buy(Decimal("50"))
        regime.record_cancel(Decimal("50"))
        regime.record_cancel(Decimal("50"))  # bug-tolerant
        assert regime.daily_spent_usdc == Decimal("0")


class TestPartialFill:
    def test_partial_fill_reduces_ledger_by_unfilled(self) -> None:
        regime = BalanceRegime()
        regime.record_buy(Decimal("100"))
        regime.record_partial_fill(recorded_usdc=Decimal("100"), actual_usdc=Decimal("60"))
        assert regime.daily_spent_usdc == Decimal("60")

    def test_full_fill_leaves_ledger_unchanged(self) -> None:
        regime = BalanceRegime()
        regime.record_buy(Decimal("100"))
        regime.record_partial_fill(recorded_usdc=Decimal("100"), actual_usdc=Decimal("100"))
        assert regime.daily_spent_usdc == Decimal("100")

    def test_over_fill_does_not_inflate_ledger(self) -> None:
        regime = BalanceRegime()
        regime.record_buy(Decimal("100"))
        # actual > recorded — bizarre, but must never re-add spend.
        regime.record_partial_fill(recorded_usdc=Decimal("100"), actual_usdc=Decimal("150"))
        assert regime.daily_spent_usdc == Decimal("100")


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


class TestReserveCommitRelease:
    def test_reserve_allows_first_buy_and_records(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        decision = _run(regime.reserve(order_id="o1", notional_usdc="60", free_cash_usdc="1000"))
        assert decision.allow is True
        assert regime.daily_spent_usdc == Decimal("60")
        assert regime.reserved_total_usdc() == Decimal("60")

    def test_reserve_blocks_when_cap_exceeded(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        _run(regime.reserve(order_id="o1", notional_usdc="70", free_cash_usdc="1000"))
        d = _run(regime.reserve(order_id="o2", notional_usdc="40", free_cash_usdc="1000"))
        assert d.allow is False
        assert d.skip_code == "balance_regime_daily_spend_exceeded"
        # Failed reservation does NOT record.
        assert regime.daily_spent_usdc == Decimal("70")

    def test_release_rolls_reservation_back(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        _run(regime.reserve(order_id="o1", notional_usdc="80", free_cash_usdc="1000"))
        _run(regime.release(order_id="o1"))
        assert regime.daily_spent_usdc == Decimal("0")
        assert regime.reserved_total_usdc() == Decimal("0")

    def test_commit_full_fill_keeps_ledger(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        _run(regime.reserve(order_id="o1", notional_usdc="80", free_cash_usdc="1000"))
        _run(regime.commit(order_id="o1", actual_filled_usdc=Decimal("80")))
        assert regime.daily_spent_usdc == Decimal("80")
        assert regime.reserved_total_usdc() == Decimal("0")

    def test_commit_partial_fill_refunds_unfilled(self) -> None:
        regime = BalanceRegime(max_daily_spend_usdc=100)
        _run(regime.reserve(order_id="o1", notional_usdc="80", free_cash_usdc="1000"))
        _run(regime.commit(order_id="o1", actual_filled_usdc=Decimal("30")))
        assert regime.daily_spent_usdc == Decimal("30")

    def test_concurrent_reserves_serialise(self) -> None:
        """Two reservations at the same time must not both pass when only one
        fits — the asyncio lock serialises check-and-record."""
        regime = BalanceRegime(max_daily_spend_usdc=100)

        async def race():
            results = await asyncio.gather(
                regime.reserve(order_id="o1", notional_usdc="60", free_cash_usdc="1000"),
                regime.reserve(order_id="o2", notional_usdc="60", free_cash_usdc="1000"),
            )
            return results

        d1, d2 = asyncio.run(race())
        # Exactly one allowed, one rejected — total ledger stays at 60.
        assert sorted([d1.allow, d2.allow]) == [False, True]
        assert regime.daily_spent_usdc == Decimal("60")

    def test_reserve_requires_order_id(self) -> None:
        regime = BalanceRegime()
        with pytest.raises(ValueError):
            _run(regime.reserve(order_id="", notional_usdc="10", free_cash_usdc="100"))
