"""Provider reads require admission before dispatch and never leak URL failures."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from pmcopy.domain.provider_budget import BudgetSettings
from pmcopy.services.provider_gateway import ProviderGateway, ProviderUnavailable
from pmcopy.storage.provider_budget import SqliteProviderBudget


class Clock:
    """Fixed deterministic billing clock."""

    def __init__(self):
        self.instant = datetime(2026, 9, 17, tzinfo=UTC)

    def now(self):
        return self.instant

    def monotonic_ns(self):
        return 0


class Transport:
    """Record dispatched attempts, optionally failing with a sensitive message."""

    def __init__(self):
        self.calls = []
        self.failures = 0

    async def request(self, provider, method, params):
        self.calls.append((provider, method, params))
        if self.failures:
            self.failures -= 1
            raise OSError("https://provider.example/credential-must-not-escape")
        return {"number": "0x1"}


class TestProviderGateway:
    """Retrying a read spends quota again and never changes the provider set."""

    @pytest.mark.asyncio
    async def test_old_quota_never_prevents_transport(self, tmp_path):
        settings = replace(
            BudgetSettings(free_plan_verified=True, baseline_verified=True),
            alchemy_monthly_limit=100,
            essential_reserve=60,
            initial_alchemy_used=90,
        )
        transport = Transport()
        with SqliteProviderBudget(tmp_path / "budget.sqlite", settings, Clock()) as ledger:
            gateway = ProviderGateway(ledger, transport)
            await gateway.call("alchemy", "eth_getTransactionReceipt", ["0x1"])
            assert ledger.snapshot()["alchemy"]["used"] == 110
        assert transport.calls == [("alchemy", "eth_getTransactionReceipt", ["0x1"])]

    @pytest.mark.asyncio
    async def test_failed_request_is_charged_and_not_implicitly_retried(self, tmp_path):
        transport = Transport()
        transport.failures = 1
        settings = BudgetSettings(free_plan_verified=True, baseline_verified=True)
        with SqliteProviderBudget(tmp_path / "budget.sqlite", settings, Clock()) as ledger:
            gateway = ProviderGateway(ledger, transport)
            with pytest.raises(ProviderUnavailable) as error:
                await gateway.call("alchemy", "eth_getTransactionReceipt", ["0x1"])
            assert "credential" not in str(error.value)
            assert error.value.__cause__ is None
            assert len(transport.calls) == 1
            assert ledger.snapshot()["alchemy"]["used"] == 20
            assert await gateway.call("alchemy", "eth_getTransactionReceipt", ["0x1"]) == {
                "number": "0x1"
            }
            assert ledger.snapshot()["alchemy"]["used"] == 40

    @pytest.mark.asyncio
    async def test_shared_calls_are_counted_without_a_weighted_rate_limit(self, tmp_path):
        settings = BudgetSettings(free_plan_verified=True, baseline_verified=True)
        clock = Clock()
        path = tmp_path / "budget.sqlite"
        transport = Transport()
        with SqliteProviderBudget(path, settings, clock) as first:
            with SqliteProviderBudget(path, settings, clock) as second:
                a = ProviderGateway(first, transport)
                b = ProviderGateway(second, transport)
                await a.call("infura", "eth_getLogs", [])
                await b.call("infura", "eth_getLogs", [])
                assert second.snapshot()["infura"]["used"] == 510
                clock.instant += timedelta(seconds=1)
                await b.call("infura", "eth_getLogs", [])
                assert len(transport.calls) == 3

    @pytest.mark.asyncio
    async def test_cancel_propagates_without_retry(self, tmp_path):
        import asyncio

        class CancelTransport(Transport):
            async def request(self, provider, method, params):
                raise asyncio.CancelledError

        settings = BudgetSettings(free_plan_verified=True, baseline_verified=True)
        with SqliteProviderBudget(tmp_path / "budget.sqlite", settings, Clock()) as ledger:
            with pytest.raises(asyncio.CancelledError):
                await ProviderGateway(ledger, CancelTransport()).call("alchemy", "eth_chainId", [])

    @pytest.mark.asyncio
    async def test_wall_second_boundary_does_not_throttle_reads(self, tmp_path):
        clock = Clock()
        clock.instant += timedelta(microseconds=900000)
        settings = BudgetSettings(free_plan_verified=True, baseline_verified=True)
        transport = Transport()
        with SqliteProviderBudget(tmp_path / "budget.sqlite", settings, clock) as ledger:
            gateway = ProviderGateway(ledger, transport)
            await gateway.call("infura", "eth_getLogs", [])
            clock.instant += timedelta(microseconds=200000)
            await gateway.call("infura", "eth_getLogs", [])
            clock.instant += timedelta(microseconds=800000)
            await gateway.call("infura", "eth_getLogs", [])
            assert len(transport.calls) == 3
