"""Durable full-stop order-cancel apply owned by the trading core.

The only live side effect after a durable full stop is cancel-all-resting-orders
per follower gateway. That apply used to live in ``services/control_plane``
and was reachable only through the remote control-plane command. These tests
pin the relocated trading-core signal and the local ``bot trading full-stop``
path so the money-path safety apply survives control-plane removal.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from integrations.polymarket.models import VenueCancelResult, VenueOrderState
from storage.postgres.trading_control_repo import TradingControlState


NOW_STATE = TradingControlState(
    enabled=False,
    generation=3,
    reason="runtime.full_stop",
    updated_at=datetime(2026, 10, 11, 12, 0, tzinfo=UTC),
    updated_by="operator-a",
)


class FakeGateway:
    """Scripted open-order and cancellation venue boundary."""

    def __init__(
        self,
        order_ids: tuple[str, ...],
        *,
        failed_order_id: str | None = None,
        fail_list: bool = False,
    ) -> None:
        self._order_ids = order_ids
        self._failed_order_id = failed_order_id
        self._fail_list = fail_list
        self.list_calls = 0
        self.cancel_calls: list[str] = []

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]:
        assert order_id is None and market is None
        self.list_calls += 1
        if self._fail_list:
            raise RuntimeError("venue read failed")
        return [VenueOrderState(order_id=value, status="open") for value in self._order_ids]

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        self.cancel_calls.append(order_id)
        succeeded = order_id != self._failed_order_id
        return VenueCancelResult(
            success=succeeded,
            order_id=order_id,
            status="canceled" if succeeded else "cancel_failed",
        )


class RecordingTelemetry:
    """Capture the bounded runtime telemetry events."""

    def __init__(self) -> None:
        self.events: list[tuple[object, object]] = []

    def emit(self, *, producer: object, payload: object) -> object:
        self.events.append((producer, payload))
        return object()


@pytest.mark.asyncio
async def test_core_full_stop_cancels_every_resting_order_across_gateways() -> None:
    """A durable full stop cancels resting orders on every follower gateway."""

    from services.trading_control import TradingControlRuntimeSignal

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
async def test_core_plain_pause_never_cancels_resting_orders() -> None:
    """Entry pause keeps risk-reducing capacity; only full stop cancels."""

    from services.trading_control import TradingControlRuntimeSignal

    gateway = FakeGateway(("order-a",))
    telemetry = RecordingTelemetry()
    signal = TradingControlRuntimeSignal(gateways=(gateway,), telemetry=telemetry)

    await signal.apply(entries_paused=True, full_stop=False)

    assert gateway.list_calls == 0
    assert gateway.cancel_calls == []
    assert len(telemetry.events) == 1


@pytest.mark.asyncio
async def test_core_full_stop_attempts_all_cancels_then_reports_failure() -> None:
    """One failed venue interaction cannot skip the remaining accounts."""

    from services.trading_control import RuntimeSafetyApplyError, TradingControlRuntimeSignal

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


@pytest.mark.asyncio
async def test_core_null_runtime_telemetry_never_raises() -> None:
    """Processes without a telemetry service still apply safety state."""

    from services.trading_control import NullRuntimeTelemetry, TradingControlRuntimeSignal

    gateway = FakeGateway(("order-a",))
    signal = TradingControlRuntimeSignal(gateways=(gateway,), telemetry=NullRuntimeTelemetry())

    await signal.apply(entries_paused=True, full_stop=True)

    assert gateway.cancel_calls == ["order-a"]


def test_legacy_control_plane_runtime_module_reexports_the_core_signal() -> None:
    """The old import path stays working until the control plane is deleted."""

    from services.control_plane.runtime import (
        OpenOrderControlGateway as LegacyGateway,
        RuntimeSafetyApplyError as LegacyError,
        TradingControlRuntimeSignal as LegacySignal,
    )
    from services.trading_control import (
        OpenOrderControlGateway,
        RuntimeSafetyApplyError,
        TradingControlRuntimeSignal,
    )

    assert LegacySignal is TradingControlRuntimeSignal
    assert LegacyError is RuntimeSafetyApplyError
    assert LegacyGateway is OpenOrderControlGateway


class RecordingControlRepo:
    """Persist calls without touching PostgreSQL.

    The CLI constructs its own instance from the patched class, so calls are
    recorded on a class-level ledger that each test clears first.
    """

    enabled_calls: list[tuple[bool, str, str]] = []

    def __init__(self, pool) -> None:
        self.pool = pool
        type(self).enabled_calls = []

    async def get(self) -> TradingControlState:
        return NOW_STATE

    async def set_enabled(self, enabled, *, reason, updated_by) -> TradingControlState:
        type(self).enabled_calls.append((enabled, reason, updated_by))
        return TradingControlState(
            enabled=enabled,
            generation=NOW_STATE.generation + len(type(self).enabled_calls),
            reason=reason,
            updated_at=NOW_STATE.updated_at,
            updated_by=updated_by,
        )


def _fake_pool() -> SimpleNamespace:
    conn = object()

    @asynccontextmanager
    async def acquire():
        yield conn

    return SimpleNamespace(acquire=acquire)


def _config(*, trading_mode: str) -> SimpleNamespace:
    return SimpleNamespace(
        database=SimpleNamespace(
            dsn="postgresql://example.invalid/db",
            command_timeout_seconds=5.0,
        ),
        execution=SimpleNamespace(trading_mode=trading_mode),
        app=SimpleNamespace(multi_user_config_file=""),
    )


def _patch_cli_environment(monkeypatch, *, trading_mode: str, built_runtimes):
    import bot_cli.commands.trading as trading_command
    import integrations.polymarket.venue_runtime as venue_runtime
    import services.multiUserConfig as multi_user_config
    import storage.postgres.connection as pg_connection
    import storage.postgres.migrations as pg_migrations
    import storage.postgres.trading_control_repo as trading_control_repo

    pool = _fake_pool()

    async def fake_init_pool(*args, **kwargs):
        return pool

    async def fake_shutdown_pool():
        return None

    async def fake_apply_migrations(conn):
        return None

    async def fake_build_polymarket_runtime(*, runtime_user, config, **kwargs):
        gateway = FakeGateway(("order-a", "order-b"))
        runtime = SimpleNamespace(
            gateway=gateway,
            close=AsyncMock(),
        )
        built_runtimes.append((runtime_user, runtime))
        return runtime

    def fake_load_runtime_users(leaders, config):
        return [SimpleNamespace(user_id="user-1", wallet=object())]

    monkeypatch.setattr(trading_command, "load_config", lambda: _config(trading_mode=trading_mode))
    monkeypatch.setattr(pg_connection, "init_pool", fake_init_pool)
    monkeypatch.setattr(pg_connection, "shutdown_pool", fake_shutdown_pool)
    monkeypatch.setattr(pg_migrations, "apply_migrations", fake_apply_migrations)
    monkeypatch.setattr(trading_control_repo, "TradingControlRepo", RecordingControlRepo)
    monkeypatch.setattr(venue_runtime, "build_polymarket_runtime", fake_build_polymarket_runtime)
    monkeypatch.setattr(multi_user_config, "load_runtime_users", fake_load_runtime_users)


def _run_trading(monkeypatch, *, action: str, trading_mode: str = "live", built_runtimes=None):
    import argparse

    import bot_cli.commands.trading as trading_command

    built_runtimes = [] if built_runtimes is None else built_runtimes
    _patch_cli_environment(
        monkeypatch,
        trading_mode=trading_mode,
        built_runtimes=built_runtimes,
    )
    args = argparse.Namespace(
        trading_action=action,
        reason="incident",
        operator="operator-a",
        confirm="LIVE",
    )
    exit_code = trading_command.run_trading(args)
    return RecordingControlRepo.enabled_calls, exit_code


def test_local_cli_full_stop_persists_then_cancels_resting_orders(monkeypatch, capsys) -> None:
    """The local full-stop command cancels resting orders through follower gateways."""

    built: list = []
    enabled_calls, exit_code = _run_trading(
        monkeypatch,
        action="full-stop",
        trading_mode="live",
        built_runtimes=built,
    )

    assert exit_code == 0
    assert enabled_calls == [(False, "runtime.full_stop", "operator-a")]
    assert len(built) == 1
    gateway = built[0][1].gateway
    assert gateway.cancel_calls == ["order-a", "order-b"]
    assert "runtime.full_stop" in capsys.readouterr().out


def test_local_cli_pause_never_cancels_resting_orders(monkeypatch) -> None:
    """Plain pause preserves existing semantics: state only, no venue cancel."""

    built: list = []
    enabled_calls, exit_code = _run_trading(
        monkeypatch,
        action="pause",
        trading_mode="live",
        built_runtimes=built,
    )

    assert exit_code == 0
    assert enabled_calls == [(False, "incident", "operator-a")]
    assert built == []


def test_local_cli_full_stop_skips_venue_apply_in_paper_mode(monkeypatch, capsys) -> None:
    """Paper mode persists the stop but never builds authenticated venues."""

    built: list = []
    enabled_calls, exit_code = _run_trading(
        monkeypatch,
        action="full-stop",
        trading_mode="paper",
        built_runtimes=built,
    )

    assert exit_code == 0
    assert enabled_calls == [(False, "runtime.full_stop", "operator-a")]
    assert built == []


def test_local_cli_full_stop_keeps_durable_state_when_a_cancel_fails(monkeypatch, capsys) -> None:
    """A failed cancel is loud but never rolls back the durable full stop."""

    import argparse

    import bot_cli.commands.trading as trading_command
    import integrations.polymarket.venue_runtime as venue_runtime

    pool = _fake_pool()

    async def fake_init_pool(*args, **kwargs):
        return pool

    async def fake_shutdown_pool():
        return None

    async def fake_apply_migrations(conn):
        return None

    async def fake_build_polymarket_runtime(*, runtime_user, config, **kwargs):
        return SimpleNamespace(
            gateway=FakeGateway(("order-a", "order-b"), failed_order_id="order-b"),
            close=AsyncMock(),
        )

    monkeypatch.setattr(
        trading_command, "load_config", lambda: _config(trading_mode="live")
    )
    monkeypatch.setattr(
        "storage.postgres.connection.init_pool", fake_init_pool
    )
    monkeypatch.setattr(
        "storage.postgres.connection.shutdown_pool", fake_shutdown_pool
    )
    monkeypatch.setattr(
        "storage.postgres.migrations.apply_migrations", fake_apply_migrations
    )
    monkeypatch.setattr(
        "storage.postgres.trading_control_repo.TradingControlRepo",
        RecordingControlRepo,
    )
    monkeypatch.setattr(
        venue_runtime, "build_polymarket_runtime", fake_build_polymarket_runtime
    )
    monkeypatch.setattr(
        "services.multiUserConfig.load_runtime_users",
        lambda leaders, config: [SimpleNamespace(user_id="user-1", wallet=object())],
    )

    args = argparse.Namespace(
        trading_action="full-stop",
        reason="incident",
        operator="operator-a",
        confirm="LIVE",
    )
    exit_code = trading_command.run_trading(args)

    assert exit_code != 0
    assert RecordingControlRepo.enabled_calls == [
        (False, "runtime.full_stop", "operator-a")
    ]
    output = capsys.readouterr().out
    assert "runtime.full_stop" in output
