"""Existing synchronous Web3 reads must use the same durable admission ledger."""

from datetime import UTC, datetime
import json
import logging

import httpx
import pytest
from web3 import Web3

from pmcopy.clients.network.provider_transport import ProviderEndpoints
from pmcopy.clients.network.provider_web3 import MeteredWeb3Provider
from pmcopy.domain.provider_budget import BudgetSettings, ProviderConfigurationError
from pmcopy.services.provider_gateway import ProviderUnavailable
from pmcopy.storage.provider_budget import SqliteProviderBudget


class Clock:
    """Deterministic provider billing clock."""

    def now(self):
        return datetime(2026, 9, 17, tzinfo=UTC)

    def monotonic_ns(self):
        return 0


class TestMeteredWeb3:
    """Web3 formatting stays in the SDK while every request is admitted once."""

    def test_web3_balance_is_charged_and_never_logs_key(self, tmp_path, caplog):
        caplog.set_level(logging.DEBUG)
        calls = []

        def response(request):
            body = json.loads(request.content)
            calls.append(body)
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": "0x7"})

        with SqliteProviderBudget(
            tmp_path / "budget",
            BudgetSettings(free_plan_verified=True, baseline_verified=True),
            Clock(),
        ) as ledger:
            provider = MeteredWeb3Provider(
                ledger,
                ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/test-key"),
                "alchemy",
                http_transport=httpx.MockTransport(response),
            )
            assert Web3(provider).eth.get_balance("0x" + "11" * 20) == 7
            assert ledger.snapshot()["alchemy"]["used"] == 20
        assert len(calls) == 1
        assert "test-key" not in caplog.text

    def test_failed_attempt_charged_once_and_no_raw_exception(self, tmp_path):
        calls = []

        def response(request):
            calls.append(request)
            raise httpx.ConnectError("sensitive-endpoint")

        with SqliteProviderBudget(
            tmp_path / "budget",
            BudgetSettings(free_plan_verified=True, baseline_verified=True),
            Clock(),
        ) as ledger:
            provider = MeteredWeb3Provider(
                ledger,
                ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/test-key"),
                "alchemy",
                http_transport=httpx.MockTransport(response),
            )
            with pytest.raises(ProviderUnavailable) as error:
                provider.make_request("eth_getCode", ["0x" + "11" * 20, "latest"])
            assert ledger.snapshot()["alchemy"]["used"] == 20
            assert error.value.__cause__ is None
        assert len(calls) == 1
        assert "sensitive" not in str(error.value)

    def test_submit_rejected_without_io(self, tmp_path):
        with SqliteProviderBudget(
            tmp_path / "budget",
            BudgetSettings(free_plan_verified=True, baseline_verified=True),
            Clock(),
        ) as ledger:
            provider = MeteredWeb3Provider(
                ledger,
                ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/test-key"),
                "alchemy",
                http_transport=httpx.MockTransport(lambda r: pytest.fail("network dispatched")),
            )
            with pytest.raises(ProviderConfigurationError):
                provider.make_request("eth_sendRawTransaction", ["0x"])
            assert ledger.snapshot()["alchemy"]["used"] == 0
