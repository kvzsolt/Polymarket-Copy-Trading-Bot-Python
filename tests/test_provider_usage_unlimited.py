"""Local provider accounting cannot suspend RPC or stream consumption."""

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from pmcopy.domain.provider_budget import BudgetSettings
from pmcopy.services.provider_gateway import ProviderGateway
from pmcopy.storage.provider_budget import SqliteProviderBudget


class FixedClock:
    """Hold billing time fixed to exercise bursts without actual waiting."""

    def now(self) -> datetime:
        """Return a fixed timezone-aware billing instant."""
        return datetime(2026, 9, 19, tzinfo=UTC)

    def monotonic_ns(self) -> int:
        """Return a fixed monotonic reading."""
        return 0


class TestUnlimitedProviderUsage:
    """Exercise production accounting through its actual dispatch boundary."""

    @pytest.mark.parametrize("provider", ["alchemy"])
    @pytest.mark.parametrize("category", ["essential", "pending", "backfill", "research"])
    async def test_old_caps_and_reserves_never_block_dispatch(
        self, tmp_path: Path, provider: str, category: str
    ) -> None:
        """All read categories dispatch beyond old quotas, including after restart."""
        settings = BudgetSettings(
            free_plan_verified=True,
            baseline_verified=True,
            alchemy_monthly_limit=100,
            essential_reserve=99,
            pending_daily_limit=1,
            initial_alchemy_used=100,
        )
        path = tmp_path / "usage.sqlite"
        transport = AsyncMock()
        for _ in range(2):
            with SqliteProviderBudget(path, settings, FixedClock()) as ledger:
                await ProviderGateway(ledger, transport).call(
                    provider, "eth_getLogs", [], category=category
                )
                snapshot = ledger.snapshot()
        assert transport.request.await_count == 2
        assert snapshot[provider]["used"] == 100 + 2 * ledger.rpc_cost(provider, "eth_getLogs")
        assert snapshot[provider]["limit"] is None
        assert snapshot[provider]["remaining"] is None
        assert snapshot["limits_enforced"] is False

    @pytest.mark.parametrize("provider", ["alchemy"])
    async def test_bursts_have_no_local_rate_throttle(self, tmp_path: Path, provider: str) -> None:
        """A fixed-clock burst exceeds previous per-second ceilings."""
        transport = AsyncMock()
        settings = BudgetSettings(free_plan_verified=True, baseline_verified=True)
        with SqliteProviderBudget(tmp_path / "usage.sqlite", settings, FixedClock()) as ledger:
            gateway = ProviderGateway(ledger, transport)
            for _ in range(20):
                await gateway.call(provider, "eth_getLogs", [])
            assert transport.request.await_count == 20
            assert ledger.snapshot()[provider]["used"] == 20 * ledger.rpc_cost(
                provider, "eth_getLogs"
            )

    def test_usage_accounting_needs_no_plan_approval(self, tmp_path: Path) -> None:
        """Fresh defaults permit provider reads without local approval flags."""
        with SqliteProviderBudget(
            tmp_path / "usage.sqlite", BudgetSettings(), FixedClock()
        ) as ledger:
            ledger.admit_rpc("alchemy", "eth_call")
            assert ledger.snapshot()["alchemy"]["used"] == 26

    def test_stream_usage_over_old_cap_continues_and_is_counted(self, tmp_path: Path) -> None:
        """Delivered frames remain usable after monthly and pending thresholds."""
        settings = BudgetSettings(
            free_plan_verified=True,
            baseline_verified=True,
            initial_alchemy_used=26_000_000,
            pending_daily_limit=1,
        )
        with SqliteProviderBudget(tmp_path / "usage.sqlite", settings, FixedClock()) as ledger:
            for _ in range(2):
                ledger.charge_stream("alchemy", 300, category="pending")
            assert ledger.snapshot()["alchemy"]["used"] == 26_000_028
            assert ledger.snapshot()["pending_daily_used"] == 28

    @pytest.mark.parametrize("value", [0, 100_000_000])
    def test_legacy_limit_values_are_inert(self, tmp_path: Path, value: int) -> None:
        """Existing configuration remains loadable without imposing Free-plan ceilings."""
        settings = replace(
            BudgetSettings(),
            alchemy_monthly_limit=value,
            alchemy_per_second=value,
        )
        with SqliteProviderBudget(tmp_path / "usage.sqlite", settings, FixedClock()) as ledger:
            ledger.admit_rpc("alchemy", "eth_getLogs")
