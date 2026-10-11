"""Standalone diagnostics and settlement reads share admitted provider resources."""

from argparse import Namespace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from config.app_config import AppConfig
from orchestration.redemption_runtime import RedemptionRuntime
from services.multiUserConfig import RuntimeUser
from tests.test_cli_redeem import TestRedeemCommand as RedeemFixture
from tests.test_redemption_runtime import FakeSession
from tests.test_redemption_service import FakeClock, FakeGateway, FakeJournal


class TestAdmittedCli:
    """No provider credentials or network are used by CLI composition fixtures."""

    async def test_doctor_composes_and_closes_shared_runtime(self, monkeypatch):
        from bot_cli.commands import doctor

        config = AppConfig()
        runtime = SimpleNamespace(aclose=AsyncMock())
        checks = AsyncMock(return_value={"status": "ok", "checks": []})
        monkeypatch.setattr(doctor, "load_config", lambda: config)
        monkeypatch.setattr(doctor, "run_doctor_checks", checks)
        monkeypatch.setattr("orchestrators.provider_runtime.ProviderRuntime.from_config", lambda value: runtime)
        assert await doctor._run_doctor_async(Namespace(live=False)) == 0
        checks.assert_awaited_once_with("paper", config=config, provider_runtime=runtime)
        runtime.aclose.assert_awaited_once()

    async def test_doctor_admission_failure_is_sanitized_and_precedes_checks(self, monkeypatch, capsys):
        from bot_cli.commands import doctor

        checks = AsyncMock()
        monkeypatch.setattr(doctor, "load_config", lambda: AppConfig())
        monkeypatch.setattr(doctor, "run_doctor_checks", checks)
        def reject(value):
            raise ValueError("synthetic-private-endpoint-key")
        monkeypatch.setattr("orchestrators.provider_runtime.ProviderRuntime.from_config", reject)
        assert await doctor._run_doctor_async(Namespace(live=False)) == 1
        assert "synthetic-private-endpoint-key" not in capsys.readouterr().out
        checks.assert_not_awaited()

    async def test_settlement_injects_same_admission_and_closes_it(self, monkeypatch):
        admitted = SimpleNamespace(post_json=AsyncMock(), aclose=AsyncMock())
        chain = Mock()
        monkeypatch.setattr("orchestration.redemption_runtime.PolygonSettlementReader", chain)
        monkeypatch.setattr("orchestration.redemption_runtime.PolymarketSettlementGateway", lambda *a, **k: FakeGateway(FakeJournal()))
        runtime = RedemptionRuntime(session_factory=AsyncMock(return_value=FakeSession()), clock=FakeClock(), provider_factory=lambda config: admitted)
        config = RedeemFixture.config()
        await runtime.run(config, RuntimeUser("user", config.wallet), execute=False)
        assert chain.call_args.args[0] is admitted
        admitted.aclose.assert_awaited_once()

    async def test_settlement_closes_admission_if_session_fails(self):
        admitted = SimpleNamespace(post_json=AsyncMock(), aclose=AsyncMock())
        runtime = RedemptionRuntime(session_factory=AsyncMock(side_effect=RuntimeError("session failed")), clock=FakeClock(), provider_factory=lambda config: admitted)
        config = RedeemFixture.config()
        with pytest.raises(RuntimeError, match="session failed"):
            await runtime.run(config, RuntimeUser("user", config.wallet), execute=False)
        admitted.aclose.assert_awaited_once()

    def test_redemption_cli_selects_provider_runtime_factory(self, monkeypatch):
        from bot_cli.commands import redeem
        from orchestrators.provider_runtime import ProviderRuntime

        constructor = Mock(return_value=object())
        monkeypatch.setattr(redeem, "RedemptionRuntime", constructor)
        monkeypatch.setattr(redeem, "RedemptionCommand", lambda *a: SimpleNamespace(run=AsyncMock(return_value=0)))
        assert redeem.run_redeem(Namespace()) == 0
        assert constructor.call_args.kwargs["provider_factory"] == ProviderRuntime.from_config
