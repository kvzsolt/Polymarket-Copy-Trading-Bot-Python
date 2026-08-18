"""Composition-root regression tests for telemetry lifecycle wiring."""

from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import config.app_config as app_config_mod
import orchestrators.runtime_bootstrap as runtime_bootstrap
from application.trading_context import TradingContext
from orchestrators.runtime_bootstrap import build_trading_context, default_follow
from services.healthServer import ReadinessTracker
from services.metrics import MetricsRegistry
from services.telemetry import ExecutionTelemetryEmitter, TelemetryEmitters
from tests.config_helpers import load_test_config


_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40


def _config(**overrides: str):
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "USER_ADDRESS": _VALID_USER,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


def _runtime_user() -> SimpleNamespace:
    return SimpleNamespace(
        user_id="user-1",
        wallet=SimpleNamespace(funder_address=_VALID_FUNDER),
    )


async def test_application_starts_and_registers_one_telemetry_owner() -> None:
    """The outer resource registry owns the same telemetry runtime it exposes."""

    from app import _start_telemetry_runtime

    resources: list[object] = []
    application = SimpleNamespace(readiness=ReadinessTracker())
    registry = MetricsRegistry()

    runtime = await _start_telemetry_runtime(
        application=application,
        client_registry=resources,
        metrics_registry=registry,
    )

    assert resources == [runtime]
    assert runtime.service.started is True
    assert runtime.service.metrics_registry is registry

    await runtime.close()


async def test_emitter_bundle_is_built_from_the_started_telemetry_runtime() -> None:
    """The composition root turns the telemetry runtime into one shared bundle."""

    from app import _build_telemetry_emitters, _start_telemetry_runtime

    resources: list[object] = []
    runtime = await _start_telemetry_runtime(
        application=SimpleNamespace(readiness=ReadinessTracker()),
        client_registry=resources,
        metrics_registry=MetricsRegistry(),
    )

    emitters = _build_telemetry_emitters(runtime)
    assert isinstance(emitters, TelemetryEmitters)
    assert emitters.enabled is True
    assert emitters.detection.signal_received("polling", "0x" + "ab" * 20, "0x" + "cd" * 32) is True
    assert len(runtime.service.history_snapshot()) == 2

    await runtime.close()


def test_emitter_bundle_falls_back_to_the_disabled_null_object() -> None:
    """A missing telemetry runtime yields a total no-op bundle, never ``None``."""

    from app import _build_telemetry_emitters

    emitters = _build_telemetry_emitters(None)

    assert isinstance(emitters, TelemetryEmitters)
    assert emitters.enabled is False


def test_run_application_builds_the_bundle_and_threads_it_into_user_state() -> None:
    """``app._run_application`` composes telemetry before per-user wiring."""

    import app

    source = inspect.getsource(app._run_application)

    assert "_build_telemetry_emitters(telemetry_runtime)" in source
    assert "telemetry=telemetry_emitters" in source
    assert source.index("_build_telemetry_emitters(") < source.index("build_user_state(")


def test_trading_context_defaults_to_a_disabled_telemetry_bundle() -> None:
    """Existing ``TradingContext`` constructions keep working without telemetry."""

    context = TradingContext(
        user_id="user-1",
        wallet_address=_VALID_FUNDER,
        client=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
        risk_blocks_repo=object(),
        pnl_tracker=object(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=object(),
    )

    assert isinstance(context.telemetry, TelemetryEmitters)
    assert context.telemetry.enabled is False


def test_build_user_state_threads_the_bundle_into_execution_and_state() -> None:
    """``build_user_state`` injects the execution emitter and keeps the bundle."""

    cfg = _config()
    emitters = TelemetryEmitters.disabled()

    with (
        patch("orchestrators.runtime_bootstrap.PositionManager"),
        patch("orchestrators.runtime_bootstrap.ExecutionService") as execution_service_cls,
    ):
        state = runtime_bootstrap.build_user_state(
            _runtime_user(),
            client=object(),
            pool=MagicMock(),
            config=cfg,
            clob_gateway=object(),
            telemetry=emitters,
        )

    assert execution_service_cls.call_args.kwargs["telemetry"] is emitters.execution
    assert state.telemetry is emitters


def test_build_user_state_without_telemetry_uses_the_disabled_bundle() -> None:
    """Omitting telemetry must not leave ``None`` on the execution boundary."""

    cfg = _config()

    with (
        patch("orchestrators.runtime_bootstrap.PositionManager"),
        patch("orchestrators.runtime_bootstrap.ExecutionService") as execution_service_cls,
    ):
        state = runtime_bootstrap.build_user_state(
            _runtime_user(),
            client=object(),
            pool=MagicMock(),
            config=cfg,
            clob_gateway=object(),
        )

    injected = execution_service_cls.call_args.kwargs["telemetry"]
    assert isinstance(injected, ExecutionTelemetryEmitter)
    assert injected.enabled is False
    assert state.telemetry.enabled is False


def test_build_trading_context_copies_the_bundle_from_the_user_state() -> None:
    """The trading context reuses the follower's bundle, never a new one."""

    cfg = _config()
    follow = default_follow("user-1", "leader-1", cfg)
    emitters = TelemetryEmitters.disabled()
    user_state = SimpleNamespace(
        user=SimpleNamespace(
            user_id="user-1", wallet=SimpleNamespace(funder_address=_VALID_FUNDER)
        ),
        client=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
        risk_blocks_repo=object(),
        pnl_tracker=object(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=object(),
        execution_service=object(),
        clob_gateway=None,
        risk_triggers_repo=None,
        telemetry=emitters,
    )

    with patch(
        "orchestrators.runtime_bootstrap.rehydrate_position_manager",
        new=AsyncMock(return_value=None),
    ):
        context = asyncio.run(build_trading_context(user_state, follow, cfg, readiness=object()))

    assert context.telemetry is emitters


def test_execution_service_stores_the_injected_telemetry_emitter() -> None:
    """The execution service holds the emitter without using it yet."""

    from services.executionService import ExecutionService

    cfg = _config()
    emitter = TelemetryEmitters.disabled().execution
    service = ExecutionService(
        clob_client=object(),
        config=cfg,
        clob_gateway=object(),
        telemetry=emitter,
    )

    assert service._telemetry is emitter

    default_service = ExecutionService(
        clob_client=object(),
        config=cfg,
        clob_gateway=object(),
    )

    assert default_service._telemetry is None
