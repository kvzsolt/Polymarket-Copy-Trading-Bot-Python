"""Production compatibility ports must delegate to injected metered resources."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from clients.network.web3_receipt_fetcher import Web3ReceiptStatusFetcher
from adapters.polygon.historical_erc1155_balance import Web3HistoricalErc1155BalanceReader
from orchestrators.startup_health import _check_rpc
from utils.getMyBalance import get_my_balance, get_native_balance_wei


class TestMeteredConsumers:
    """No production seam may construct the former unmetered fallback provider."""

    @pytest.mark.asyncio
    async def test_collateral_balance_uses_injected_reader(self):
        reader = SimpleNamespace(collateral=AsyncMock(return_value=Decimal("12.345678")))
        runtime = SimpleNamespace(balances=reader)
        assert await get_my_balance("0x" + "11" * 20, provider_runtime=runtime) == Decimal(
            "12.345678"
        )
        reader.collateral.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_native_balance_uses_injected_reader(self):
        reader = SimpleNamespace(native=AsyncMock(return_value=123))
        assert (
            await get_native_balance_wei(
                "0x" + "11" * 20, provider_runtime=SimpleNamespace(balances=reader)
            )
            == 123
        )
        reader.native.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_health_rpc_uses_admitted_read_and_preserves_failure(self):
        client = SimpleNamespace(
            post_json=AsyncMock(return_value={"jsonrpc": "2.0", "id": 1, "result": "0x89"})
        )
        report = await _check_rpc("synthetic-opaque-url", provider_runtime=client)
        assert report.status == "ok"
        client.post_json.assert_awaited_once()
        client.post_json.side_effect = RuntimeError("private-credential")
        report = await _check_rpc("synthetic-opaque-url", provider_runtime=client)
        assert report.status == "error"
        assert "private-credential" not in repr(report)

    def test_receipt_reader_uses_shared_web3(self):
        web3 = SimpleNamespace()
        runtime = SimpleNamespace(web3=lambda: web3)
        reader = Web3ReceiptStatusFetcher.from_rpc_url("unused", provider_runtime=runtime)
        assert reader._adapter.web3 is web3

    def test_historical_balance_uses_shared_web3(self):
        contract = object()
        runtime = SimpleNamespace(
            web3=lambda: SimpleNamespace(eth=SimpleNamespace(contract=lambda **kwargs: contract))
        )
        reader = Web3HistoricalErc1155BalanceReader.from_rpc_url("unused", provider_runtime=runtime)
        assert reader._contract is contract


class TestProviderBalanceReader:
    """Balance reads retain exact integer units and unknown failures."""

    @pytest.mark.asyncio
    async def test_exact_collateral_and_unknown_transport(self):
        from services.provider_balances import ProviderBalanceReader

        gateway = SimpleNamespace(
            call=AsyncMock(side_effect=["0x6000", "0x" + format(9007199254740993123456, "064x")])
        )
        reader = ProviderBalanceReader(gateway, "alchemy", ("0x" + "22" * 20,))
        assert await reader.collateral("0x" + "11" * 20) == Decimal("9007199254740993.123456")
        assert gateway.call.await_args_list[1].args[1] == "eth_call"
        gateway.call.side_effect = RuntimeError("secret")
        assert await reader.collateral("0x" + "11" * 20) is None

    @pytest.mark.asyncio
    async def test_native_and_invalid_address_do_not_fabricate_zero(self):
        from services.provider_balances import ProviderBalanceReader

        gateway = SimpleNamespace(call=AsyncMock(return_value="0x7"))
        reader = ProviderBalanceReader(gateway, "alchemy", ())
        assert await reader.native("bad") is None
        gateway.call.assert_not_awaited()
        assert await reader.native("0x" + "11" * 20) == 7
        gateway.call.return_value = None
        assert await reader.native("0x" + "11" * 20) is None


class TestProviderComposition:
    """Funds and reservations reuse the one admitted account reader."""

    @pytest.mark.asyncio
    async def test_discovery_funds_carry_runtime(self, monkeypatch):
        import bootstrap
        from dataclasses import replace
        from config.app_config import AppConfig
        from tests.test_bootstrap_balance_configuration import _FakePool

        cfg = AppConfig()
        cfg = replace(cfg, wallet=replace(cfg.wallet, funder_address="0x" + "11" * 20))
        monkeypatch.setattr(bootstrap, "init_pool", AsyncMock(return_value=_FakePool()))
        monkeypatch.setattr(bootstrap, "apply_migrations", AsyncMock())
        runtime = SimpleNamespace(
            balances=SimpleNamespace(collateral=AsyncMock(return_value=Decimal("9")))
        )
        application = await bootstrap.build_app(config=cfg, provider_runtime=runtime)
        assert application.provider_runtime is runtime
        assert await application.discovery_service._get_funds() == Decimal("9")
        runtime.balances.collateral.assert_awaited_once_with("0x" + "11" * 20)

    @pytest.mark.asyncio
    async def test_reservation_balance_carries_runtime(self, monkeypatch):
        from orchestrators import runtime_bootstrap
        from config.app_config import AppConfig
        from unittest.mock import Mock

        runtime = SimpleNamespace(
            balances=SimpleNamespace(collateral=AsyncMock(return_value=Decimal("7")))
        )
        reservation = Mock()
        monkeypatch.setattr(
            "services.execution.reservation_service.ReservationService", reservation
        )
        runtime_bootstrap._build_execution_coordinator(
            pool=Mock(),
            positions_repo=Mock(),
            execution_service=Mock(),
            funder_address="0x" + "11" * 20,
            config=AppConfig(),
            provider_runtime=runtime,
        )
        callback = reservation.call_args.kwargs["get_balance_func"]
        assert await callback("0x" + "11" * 20) == Decimal("7")
        runtime.balances.collateral.assert_awaited_once()


class TestBalanceExactness:
    """Raw uint256 evidence cannot be rounded by ambient Decimal context."""

    @pytest.mark.asyncio
    async def test_full_uint256_precision(self):
        from services.provider_balances import ProviderBalanceReader

        amount = 2**256 - 1
        gateway = SimpleNamespace(
            call=AsyncMock(side_effect=["0x6000", "0x" + format(amount, "064x")])
        )
        result = await ProviderBalanceReader(gateway, "alchemy", ("0x" + "22" * 20,)).collateral(
            "0x" + "11" * 20
        )
        assert str(result) == f"{amount // 1000000}.{amount % 1000000:06d}"


class TestProviderDoctorInjection:
    """The startup doctor cannot perform a pre-admission RPC probe."""

    @pytest.mark.asyncio
    async def test_doctor_uses_shared_rpc_before_startup(self, monkeypatch, tmp_path):
        from dataclasses import replace
        from config.app_config import AppConfig
        from bot_cli.commands import doctor
        from orchestrators import startup_health

        cfg = AppConfig()
        cfg = replace(cfg, polymarket=replace(cfg.polymarket, enforce_geoblock=False))
        monkeypatch.setattr(
            "storage.postgres.connection.init_pool",
            AsyncMock(side_effect=RuntimeError("fake unavailable")),
        )
        monkeypatch.setattr(doctor, "_repo_root", lambda: tmp_path)
        monkeypatch.setattr(doctor, "_resolve_log_dir", lambda *args: tmp_path)
        monkeypatch.setattr(doctor, "_resolve_jsonl_path", lambda *args: tmp_path / "test.jsonl")
        probe = AsyncMock(return_value=SimpleNamespace(status="ok", details={}))
        monkeypatch.setattr(startup_health, "_check_rpc", probe)
        runtime = object()
        await doctor.run_doctor_checks("paper", config=cfg, provider_runtime=runtime)
        probe.assert_awaited_once_with(cfg.polymarket.rpc_url, provider_runtime=runtime)

    @pytest.mark.asyncio
    async def test_startup_health_passes_runtime_to_rpc(self, monkeypatch):
        from orchestrators import startup_health

        class StopAfterProbes(Exception):
            pass

        ok = startup_health.StartupHealthCheckItem(status="ok", message="fake")
        probe = AsyncMock(return_value=ok)
        monkeypatch.setattr(startup_health, "_check_rpc", probe)
        monkeypatch.setattr(startup_health, "_check_polymarket_api", AsyncMock(return_value=ok))
        monkeypatch.setattr(startup_health, "_check_clob_gateway", AsyncMock(return_value=ok))
        monkeypatch.setattr(
            startup_health,
            "reconcile_persisted_positions_to_live",
            AsyncMock(side_effect=StopAfterProbes),
        )
        runtime = object()
        with pytest.raises(StopAfterProbes):
            await startup_health.perform_startup_health_check(
                wallet="0x" + "11" * 20,
                user_id="test",
                pool=object(),
                positions_repo=object(),
                live_positions=[],
                available_cash=Decimal("0"),
                database_ok=True,
                rpc_url="opaque",
                provider_runtime=runtime,
            )
        probe.assert_awaited_once_with("opaque", provider_runtime=runtime)
