"""Phase 4 — balance regime gate tests."""
from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace

import pytest

from services.balance_regime import (
    BalanceDecision,
    BalanceRegime,
    _DailySpendLedger,
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
    def test_zero_thresholds_allow_everything(self) -> None:
        regime = BalanceRegime()
        decision = regime.check_buy(notional_usdc=10_000, free_cash_usdc=10)
        assert decision.allow is True
        assert decision.reason == "ok"

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
        decision: BalanceDecision = regime.check_buy(
            notional_usdc="40", free_cash_usdc="200"
        )
        assert decision.daily_spent_usdc == Decimal("75")
        assert decision.daily_remaining_usdc == Decimal("425")
        assert decision.headroom_usdc == Decimal("160")
