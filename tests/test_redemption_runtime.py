"""Standalone resource composition never activates signing or storage for previews."""

from unittest.mock import AsyncMock, Mock

import pytest

from pmcopy.orchestrators.settlement.redemption_runtime import RedemptionRuntime
from tests.test_cli_redeem import TestRedeemCommand as RedeemFixture
from tests.test_redemption_service import FakeClock, FakeGateway, FakeJournal
from pmcopy.services.accounts.multi_user_config import RuntimeUser


class FakeSession:
    """Track session teardown without networking."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        """Record bounded command cleanup."""
        self.closed = True


class TestRedemptionRuntime:
    """Runtime enforces the live gate again and owns every resource it opens."""

    async def test_rpc_uses_dedicated_single_attempt_transport(self, monkeypatch) -> None:
        """RPC recovery must not multiply the generic HTTP client's four retries."""
        chain = Mock()
        gateway = FakeGateway(FakeJournal())
        monkeypatch.setattr("pmcopy.orchestrators.settlement.redemption_runtime.PolygonSettlementReader", chain)
        monkeypatch.setattr(
            "pmcopy.orchestrators.settlement.redemption_runtime.PolymarketSettlementGateway", lambda *a, **k: gateway
        )
        clock = FakeClock()
        runtime = RedemptionRuntime(
            session_factory=AsyncMock(return_value=FakeSession()), clock=clock
        )
        config = RedeemFixture.config()
        await runtime.run(config, RuntimeUser("user", config.wallet), execute=False)
        assert type(chain.call_args.args[0]).__name__ == "SettlementRpcHttp"
        assert chain.call_args.kwargs["clock"] is clock
        assert callable(chain.call_args.kwargs["jitter"])

    async def test_preview_constructs_no_signer_or_database(self, monkeypatch) -> None:
        """Verify preview constructs no signer or database."""
        session = FakeSession()
        pool_factory = AsyncMock(side_effect=AssertionError("preview touched database"))
        close_pool = AsyncMock()
        journal = FakeJournal()
        gateway = FakeGateway(journal)
        monkeypatch.setattr(
            "pmcopy.orchestrators.settlement.redemption_runtime.PolymarketSettlementGateway", lambda *a, **k: gateway
        )
        monkeypatch.setattr(
            "pmcopy.orchestrators.settlement.redemption_runtime.DepositWalletCodec",
            lambda *a: pytest.fail("preview signer"),
        )
        runtime = RedemptionRuntime(
            session_factory=AsyncMock(return_value=session),
            pool_factory=pool_factory,
            close_pool=close_pool,
            clock=FakeClock(),
        )
        config = RedeemFixture.config(True, True)
        report = await runtime.run(config, RuntimeUser("user", config.wallet), execute=False)
        assert report.success and session.closed
        pool_factory.assert_not_called()
        close_pool.assert_not_called()

    async def test_runtime_paper_gate_precedes_all_io(self) -> None:
        """Verify runtime paper gate precedes all io."""
        session_factory = AsyncMock()
        runtime = RedemptionRuntime(session_factory=session_factory, clock=FakeClock())
        config = RedeemFixture.config(False, True)
        with pytest.raises(ValueError, match="live"):
            await runtime.run(config, RuntimeUser("user", config.wallet), execute=True)
        session_factory.assert_not_called()

    async def test_session_is_closed_if_database_initialization_fails(self) -> None:
        """Verify session is closed if database initialization fails."""
        session = FakeSession()
        close_pool = AsyncMock()
        runtime = RedemptionRuntime(
            session_factory=AsyncMock(return_value=session),
            pool_factory=AsyncMock(side_effect=RuntimeError("database down")),
            close_pool=close_pool,
            clock=FakeClock(),
        )
        config = RedeemFixture.config(True, True)
        with pytest.raises(RuntimeError):
            await runtime.run(config, RuntimeUser("user", config.wallet), execute=True)
        assert session.closed
        close_pool.assert_not_called()
