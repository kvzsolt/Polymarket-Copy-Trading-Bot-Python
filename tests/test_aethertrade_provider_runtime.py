"""Shared provider composition is explicit, persisted, and fail-closed before I/O."""

from dataclasses import replace
from datetime import UTC, datetime

import httpx
import pytest

from pmcopy.config.app_config import AppConfig, load_config
from pmcopy.config.provider_budget import ProviderBudgetConfig
from pmcopy.domain.provider_budget import BudgetSettings, ProviderConfigurationError
from pmcopy.orchestrators.bootstrap.provider_runtime import ProviderRuntime


class Clock:
    """Fixed provider clock."""

    def now(self):
        return datetime(2026, 9, 17, tzinfo=UTC)

    def monotonic_ns(self):
        return 0


class TestProviderRuntime:
    """No callers may silently use a different provider or unverified budget."""

    def config(self, tmp_path):
        base = AppConfig()
        return replace(
            base,
            polymarket=replace(
                base.polymarket, rpc_url="https://polygon-mainnet.g.alchemy.com/v2/test-key"
            ),
            provider_budget=ProviderBudgetConfig(
                ledger_path=str(tmp_path / "budget.sqlite"),
                limits=BudgetSettings(free_plan_verified=True, baseline_verified=True),
            ),
        )

    def test_default_accounting_needs_no_plan_or_baseline_approval(self, tmp_path):
        config = self.config(tmp_path)
        config = replace(
            config, provider_budget=replace(config.provider_budget, limits=BudgetSettings())
        )
        runtime = ProviderRuntime.from_config(config, clock=Clock())
        try:
            assert runtime.budget.snapshot()["limits_enforced"] is False
            assert (tmp_path / "budget.sqlite").exists()
        finally:
            runtime.close()

    @pytest.mark.asyncio
    async def test_compatibility_envelope_uses_same_admission(self, tmp_path):
        config = self.config(tmp_path)
        runtime = ProviderRuntime.from_config(
            config,
            clock=Clock(),
            http_transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"jsonrpc": "2.0", "id": 1, "result": "0x89"}
                )
            ),
        )
        try:
            result = await runtime.post_json(
                config.polymarket.rpc_url,
                json_body={"jsonrpc": "2.0", "id": 73, "method": "eth_chainId", "params": []},
                silent=True,
            )
            assert result == {"jsonrpc": "2.0", "id": 73, "result": "0x89"}
            with pytest.raises(ProviderConfigurationError):
                await runtime.post_json(
                    "https://evil.test/",
                    json_body={"method": "eth_chainId", "params": [], "id": 74},
                    silent=True,
                )
            assert runtime.budget.snapshot()["alchemy"]["used"] == 0
            runtime.budget.charge_stream("alchemy", 100, category="pending")
            assert runtime.budget.snapshot()["alchemy"]["used"] == 5
        finally:
            runtime.close()

    def test_shared_yaml_section_preserves_safe_defaults(self, tmp_path):
        cfgdir = tmp_path / "config"
        cfgdir.mkdir()
        (cfgdir / "provider_budget.yml").write_text(
            "ledger_path: /tmp/test-ledger\nlimits:\n  alchemy_monthly_limit: 25000000\n"
        )
        config = load_config(
            {
                "database": {"dsn": "postgresql://localhost/test"},
                "wallet": {"funder_address": "0x" + "11" * 20},
                "polymarket": {"rpc_url": "https://polygon-mainnet.g.alchemy.com/v2/test-key"},
                "detection": {
                    "onchain": {
                        "alchemy_wss_url": "wss://polygon-mainnet.g.alchemy.com/v2/test-key"
                    }
                },
            },
            config_dir=cfgdir,
            secrets_path=tmp_path / "absent-private-env",
        )
        assert config.provider_budget.ledger_path == "/tmp/test-ledger"
        assert config.provider_budget.limits.alchemy_monthly_limit == 25000000
        assert config.provider_budget.limits.free_plan_verified is False

    def test_runtime_resources_are_immutable_after_composition(self, tmp_path):
        from dataclasses import FrozenInstanceError

        runtime = ProviderRuntime.from_config(self.config(tmp_path), clock=Clock())
        try:
            with pytest.raises(FrozenInstanceError):
                runtime.primary = "unmetered"
        finally:
            runtime.close()
