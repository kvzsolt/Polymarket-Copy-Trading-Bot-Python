"""Startup SELL recovery requires current zero inventory without replaying deltas."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from services.execution.fill_accounting import AccountedOrderFill
from services.execution.reconciliation_service import ReconciliationService
from services.execution.recovery_inventory import StartupInventoryRecoveryUnavailable
from tests.test_reconciler_idempotent_transitions import _make_service, _record


@pytest.fixture
def recovered_service(monkeypatch: pytest.MonkeyPatch):
    """Use fixed time and deterministic ledger and inventory boundaries."""
    monkeypatch.setattr("services.execution.reconciliation_service.time.time", lambda: 1234)
    service = _make_service()
    service._recovery_inventory = SimpleNamespace(zero_inventory=AsyncMock(return_value=True))
    service._accounted_fill_reader = SimpleNamespace(
        accounted_order_fill=AsyncMock(return_value=AccountedOrderFill())
    )
    service.fills_repo.append_recovered_sell = AsyncMock(return_value=(1, None))
    return service


class TestRecoveredSell:
    """Keep startup-only absence recovery separate from ordinary fill application."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("risk_exit", [False, True])
    async def test_confirmed_sell_records_ledger_without_subtracting_absent_inventory(
        self, recovered_service, risk_exit: bool
    ) -> None:
        """The recovered eleven-share SELL keeps zero position and unknown basis."""
        service = recovered_service
        tracker = Mock()
        result = await self._reconcile(service, risk_exit=risk_exit, pnl_tracker=tracker)

        assert result is True
        service.fills_repo.append_recovered_sell.assert_awaited_once()
        fill = service.fills_repo.append_recovered_sell.call_args.args[0]
        assert (fill.side, fill.size_tokens, fill.price, fill.dedup_key) == (
            "SELL",
            Decimal("11"),
            Decimal("0.23"),
            "11",
        )
        assert fill.accounted_shares == Decimal(0)
        assert service.positions_repo.applied == []
        service._recovery_inventory.zero_inventory.assert_awaited_once_with(
            "user-1", "cond-idem", "0xtok-idem"
        )
        assert tracker.record_sell.call_args.kwargs["entry_price"] is None
        assert tracker.record_sell.call_args.kwargs["realized_pnl"] is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("risk_exit", [False, True])
    @pytest.mark.parametrize("evidence", [False, None, RuntimeError("credential-secret")])
    async def test_missing_or_contradictory_inventory_keeps_order_unresolved(
        self, recovered_service, risk_exit: bool, evidence: object, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An outage, incomplete read, or positive shares cannot authorize recovery."""
        service = recovered_service
        reader = service._recovery_inventory.zero_inventory
        if isinstance(evidence, Exception):
            reader.side_effect = evidence
        else:
            reader.return_value = evidence

        with pytest.raises(
            StartupInventoryRecoveryUnavailable,
            match="Startup SELL inventory recovery is unresolved",
        ):
            await self._reconcile(service, risk_exit=risk_exit)
        service.fills_repo.append_recovered_sell.assert_not_awaited()
        assert service.positions_repo.applied == []
        assert service.orders_repo.mark_processed_calls == []
        assert "credential-secret" not in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("risk_exit", [False, True])
    async def test_existing_ledger_fill_is_not_recorded_again(
        self, recovered_service, risk_exit: bool
    ) -> None:
        """Repeated recovery reads durable eleven-share accounting exactly once."""
        service = recovered_service
        reader = service._accounted_fill_reader.accounted_order_fill
        reader.return_value = AccountedOrderFill(Decimal("11"), Decimal("2.53"), None)
        for _ in range(2):
            assert await self._reconcile(service, risk_exit=risk_exit) is True
        service.fills_repo.append_recovered_sell.assert_not_awaited()
        assert service.positions_repo.applied == []
        assert service._recovery_inventory.zero_inventory.await_count == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize("risk_exit", [False, True])
    async def test_missing_accounted_ledger_reader_fails_closed(
        self, recovered_service, risk_exit: bool
    ) -> None:
        """Order-response watermarks alone cannot authorize ledger-only settlement."""
        recovered_service._accounted_fill_reader = None
        with pytest.raises(
            StartupInventoryRecoveryUnavailable,
            match="Startup SELL inventory recovery is unresolved",
        ):
            await self._reconcile(recovered_service, risk_exit=risk_exit)
        recovered_service.fills_repo.append_recovered_sell.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("risk_exit", [False, True])
    async def test_inflight_fill_cannot_use_zero_inventory_recovery(
        self, recovered_service, risk_exit: bool
    ) -> None:
        """Matched and mined status still need final authoritative fill evidence."""
        with pytest.raises(
            StartupInventoryRecoveryUnavailable,
            match="Startup SELL inventory recovery is unresolved",
        ):
            await self._reconcile(recovered_service, risk_exit=risk_exit, status="mined")
        recovered_service.fills_repo.append_recovered_sell.assert_not_awaited()
        assert recovered_service.positions_repo.applied == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("risk_exit", [False, True])
    async def test_normal_reconciliation_keeps_the_existing_position_delta_path(
        self, recovered_service, risk_exit: bool
    ) -> None:
        """Only an explicitly marked startup sweep can omit the position delta."""
        assert (
            await self._reconcile(recovered_service, risk_exit=risk_exit, startup_recovery=False)
            is True
        )
        recovered_service.fills_repo.append_recovered_sell.assert_not_awaited()
        recovered_service._recovery_inventory.zero_inventory.assert_not_awaited()
        assert len(recovered_service.positions_repo.applied) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("risk_exit", [False, True])
    async def test_failed_atomic_recovery_blocks_startup_readiness(
        self, recovered_service, risk_exit: bool
    ) -> None:
        """A position or ledger change after the snapshot remains a startup failure."""
        recovered_service.fills_repo.append_recovered_sell.side_effect = ValueError(
            "private provider detail"
        )
        with pytest.raises(
            StartupInventoryRecoveryUnavailable,
            match="Startup SELL inventory recovery is unresolved",
        ):
            await self._reconcile(recovered_service, risk_exit=risk_exit)
        assert recovered_service.orders_repo.mark_processed_calls == []

    @staticmethod
    async def _reconcile(
        service: ReconciliationService,
        *,
        risk_exit: bool,
        status: str = "confirmed",
        startup_recovery: bool = True,
        pnl_tracker: object = None,
    ) -> bool:
        arguments = dict(
            confirm_fill=AsyncMock(
                return_value={"status": status, "size_matched": "11", "avg_price": "0.23"}
            ),
            get_position=AsyncMock(return_value=None),
            startup_recovery=startup_recovery,
            pnl_tracker=pnl_tracker,
        )
        if risk_exit:
            return await service.reconcile_risk_record(
                _record(),
                user_id="user-1",
                condition_id="cond-idem",
                token_id="0xtok-idem",
                **arguments,
            )
        return await service.reconcile_trade_record(_record(), "SELL", **arguments)
