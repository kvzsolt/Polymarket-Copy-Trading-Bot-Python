"""Deterministic account-wide admission and restart safety for provider usage."""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from pmcopy.domain.provider_budget import BudgetSettings, ProviderConfigurationError
from pmcopy.storage.provider_budget import SqliteProviderBudget


class FakeClock:
    """Advance billing and throughput independently without wall-clock sleeps."""

    def __init__(self):
        self.instant = datetime(2026, 9, 17, tzinfo=UTC)
        self.tick = 0

    def now(self):
        return self.instant

    def monotonic_ns(self):
        return self.tick


class TestProviderBudget:
    """Every caller shares durable informational usage accounting."""

    def settings(self, **values):
        return replace(BudgetSettings(free_plan_verified=True, baseline_verified=True), **values)

    def test_plan_and_baseline_flags_do_not_block_accounting(self, tmp_path):
        for settings in (BudgetSettings(), BudgetSettings(free_plan_verified=True)):
            with SqliteProviderBudget(tmp_path / "usage.sqlite", settings, FakeClock()) as ledger:
                ledger.admit_rpc("alchemy", "eth_call")

    def test_pending_is_counted_without_reserving_provider_capacity(self, tmp_path):
        settings = self.settings(alchemy_monthly_limit=100, essential_reserve=60)
        with SqliteProviderBudget(tmp_path / "usage.sqlite", settings, FakeClock()) as ledger:
            ledger.charge("alchemy", 40, category="pending")
            ledger.charge("alchemy", 1, category="pending")
            ledger.charge("alchemy", 60, category="essential")
            assert ledger.snapshot()["alchemy"]["used"] == 101
            assert ledger.snapshot()["alchemy"]["remaining"] is None

    def test_backfill_has_no_local_capacity_reservation(self, tmp_path):
        settings = self.settings(alchemy_monthly_limit=100, essential_reserve=60)
        with SqliteProviderBudget(tmp_path / "usage.sqlite", settings, FakeClock()) as ledger:
            ledger.charge("alchemy", 41, category="backfill")
            assert ledger.snapshot()["alchemy"]["used"] == 41

    def test_delivered_bytes_are_recorded_even_after_optional_ceiling(self, tmp_path):
        settings = self.settings(pending_daily_limit=10)
        with SqliteProviderBudget(tmp_path / "usage.sqlite", settings, FakeClock()) as ledger:
            assert ledger.stream_cost(300) == 14
            ledger.charge("alchemy", 14, category="pending", already_delivered=True)
            assert ledger.snapshot()["alchemy"]["used"] == 14
            assert ledger.snapshot()["pending_daily_used"] == 14

    def test_existing_usage_is_seeded_once_and_persists_after_restart(self, tmp_path):
        settings = self.settings(initial_alchemy_used=500, initial_infura_used=100)
        path = tmp_path / "usage.sqlite"
        clock = FakeClock()
        with SqliteProviderBudget(path, settings, clock) as ledger:
            ledger.charge("alchemy", 20)
        with SqliteProviderBudget(path, settings, clock) as ledger:
            snapshot = ledger.snapshot()
            assert snapshot["alchemy"]["used"] == 520
            assert snapshot["infura"]["used"] == 100
            assert snapshot["estimate_only"] is True

    def test_billing_day_change_requires_explicit_migration(self, tmp_path):
        path = tmp_path / "usage.sqlite"
        with SqliteProviderBudget(path, self.settings(), FakeClock()):
            pass
        with pytest.raises(ProviderConfigurationError, match="billing"):
            SqliteProviderBudget(path, self.settings(alchemy_billing_day=17), FakeClock())

    def test_daily_and_monthly_periods_do_not_reset_together(self, tmp_path):
        clock = FakeClock()
        with SqliteProviderBudget(tmp_path / "usage.sqlite", self.settings(), clock) as ledger:
            ledger.charge("alchemy", 20, category="pending")
            ledger.charge("infura", 80)
            clock.instant = datetime(2026, 9, 18, tzinfo=UTC)
            snapshot = ledger.snapshot()
            assert snapshot["alchemy"]["used"] == 20
            assert snapshot["infura"]["used"] == 0
            assert snapshot["pending_daily_used"] == 0
            clock.instant = datetime(2026, 10, 1, tzinfo=UTC)
            assert ledger.snapshot()["alchemy"]["used"] == 0

    def test_two_connections_count_all_usage_without_enforcing_old_limit(self, tmp_path):
        settings = self.settings(alchemy_monthly_limit=100, essential_reserve=20)
        path = tmp_path / "usage.sqlite"
        with SqliteProviderBudget(path, settings, FakeClock()) as first:
            with SqliteProviderBudget(path, settings, FakeClock()) as second:
                first.charge("alchemy", 80)
                second.charge("alchemy", 21)
                assert second.snapshot()["alchemy"]["used"] == 101

    @pytest.mark.parametrize("amount", [True, -1, 1.5, Decimal("1")])
    def test_noninteger_or_negative_charges_are_rejected(self, tmp_path, amount):
        with SqliteProviderBudget(
            tmp_path / "usage.sqlite", self.settings(), FakeClock()
        ) as ledger:
            with pytest.raises(ValueError):
                ledger.charge("alchemy", amount)

    def test_required_read_methods_are_priced_unknown_methods_fail_closed(self, tmp_path):
        with SqliteProviderBudget(
            tmp_path / "usage.sqlite", self.settings(), FakeClock()
        ) as ledger:
            assert ledger.rpc_cost("alchemy", "eth_call") == 26
            assert ledger.rpc_cost("infura", "eth_call") == 80
            assert ledger.rpc_cost("alchemy", "eth_getCode") == 20
            with pytest.raises(ProviderConfigurationError):
                ledger.rpc_cost("alchemy", "eth_sendRawTransaction")
            with pytest.raises(ProviderConfigurationError):
                ledger.rpc_cost("unapproved-provider", "eth_call")
