"""Runtime bootstrap connects inventory convergence to monitoring refresh."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
import pytest
from pmcopy.orchestrators.bootstrap import runtime_bootstrap


@pytest.mark.asyncio
async def test_manual_sync_builder_uses_the_injected_refresh_callback():
    callback = AsyncMock()
    service = runtime_bootstrap._build_manual_sync_service(
        funder_address="wallet",
        user_id="user",
        positions_repo=Mock(),
        market_store=Mock(),
        fills_repo=None,
        on_positions_synced=callback,
    )
    with (
        patch.object(service, "_sync_manual_trades", AsyncMock()),
        patch.object(service, "_sync_positions_and_tracking", AsyncMock()),
    ):
        assert await service.sync_now(reason="test")
    callback.assert_awaited_once()


def test_reconciler_factory_binds_the_account_holdings_reader():
    from pmcopy.orchestrators.pipeline.trade_pipeline import get_reconciliation_service

    reader = Mock()
    context = SimpleNamespace(
        extra={},
        user_id="user",
        wallet_address="wallet",
        holdings_reader=reader,
        orders_repo=Mock(),
        fills_repo=Mock(),
        positions_repo=Mock(),
        config=None,
    )
    with patch("pmcopy.orchestrators.pipeline.trade_pipeline.ReconciliationService") as factory:
        get_reconciliation_service(context)
    recovery = factory.call_args.kwargs["recovery_inventory"]
    assert recovery is not None
