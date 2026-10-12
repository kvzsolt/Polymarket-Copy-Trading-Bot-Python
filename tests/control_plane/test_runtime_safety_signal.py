"""Fail-closed runtime safety application tests."""

from __future__ import annotations

import pytest

from integrations.polymarket.models import VenueCancelResult, VenueOrderState


class FakeGateway:
    """Return scripted open orders and cancellation outcomes."""

    def __init__(
        self,
        order_ids: tuple[str, ...],
        *,
        failed_order_id: str | None = None,
    ) -> None:
        self._order_ids = order_ids
        self._failed_order_id = failed_order_id
        self.list_calls = 0
        self.cancel_calls: list[str] = []

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]:
        """Return the configured complete open-order snapshot."""

        assert order_id is None and market is None
        self.list_calls += 1
        return [VenueOrderState(order_id=value, status="open") for value in self._order_ids]

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        """Return a deterministic cancellation result."""

        self.cancel_calls.append(order_id)
        return VenueCancelResult(
            success=order_id != self._failed_order_id,
            order_id=order_id,
            status="canceled" if order_id != self._failed_order_id else "cancel_failed",
        )


class RecordingTelemetry:
    """Capture non-blocking typed runtime events."""

    def __init__(self) -> None:
        self.events: list[tuple[object, object]] = []

    def emit(self, *, producer: object, payload: object) -> object:
        """Capture one event and return an ignored result marker."""

        self.events.append((producer, payload))
        return object()


@pytest.mark.asyncio
async def test_entry_pause_does_not_cancel_protective_order_capacity() -> None:
    """Only a full stop cancels resting orders; entry pause remains risk-reducing."""

    from services.control_plane.runtime import TradingControlRuntimeSignal

    gateway = FakeGateway(("order-a",))
    telemetry = RecordingTelemetry()
    signal = TradingControlRuntimeSignal(gateways=(gateway,), telemetry=telemetry)

    await signal.apply(entries_paused=True, full_stop=False)

    assert gateway.list_calls == 0
    assert gateway.cancel_calls == []
    assert len(telemetry.events) == 1


@pytest.mark.asyncio
async def test_full_stop_cancels_every_resting_order_across_accounts() -> None:
    """The durable stop is active before every discovered order is canceled."""

    from services.control_plane.runtime import TradingControlRuntimeSignal

    first = FakeGateway(("order-a", "order-b"))
    second = FakeGateway(("order-c",))
    signal = TradingControlRuntimeSignal(
        gateways=(first, second),
        telemetry=RecordingTelemetry(),
    )

    await signal.apply(entries_paused=True, full_stop=True)

    assert first.cancel_calls == ["order-a", "order-b"]
    assert second.cancel_calls == ["order-c"]


@pytest.mark.asyncio
async def test_full_stop_attempts_all_cancels_then_reports_failure() -> None:
    """One failed venue cancel cannot prevent attempts for remaining accounts."""

    from services.control_plane.runtime import RuntimeSafetyApplyError
    from services.control_plane.runtime import TradingControlRuntimeSignal

    first = FakeGateway(("order-a", "order-b"), failed_order_id="order-a")
    second = FakeGateway(("order-c",))
    signal = TradingControlRuntimeSignal(
        gateways=(first, second),
        telemetry=RecordingTelemetry(),
    )

    with pytest.raises(RuntimeSafetyApplyError, match="resting order"):
        await signal.apply(entries_paused=True, full_stop=True)

    assert first.cancel_calls == ["order-a", "order-b"]
    assert second.cancel_calls == ["order-c"]
