"""Regression coverage for repeated operator interrupts during async cleanup."""

from __future__ import annotations

import argparse
import ast
import asyncio
import signal
from pathlib import Path
from types import FunctionType
from unittest.mock import AsyncMock, MagicMock

import pytest

import pmcopy.app as app
from pmcopy.bot_cli.commands.run import run_run
from pmcopy.orchestrators.user_data_runtime import UserDataRuntime
from pmcopy.services.manual_sync_service import ManualSyncService


class ShutdownScenario:
    """Drive shutdown callbacks deterministically without delivering OS signals."""

    def __init__(self, first_signal: signal.Signals, repeat_signal: signal.Signals) -> None:
        self.first_signal = first_signal
        self.repeat_signal = repeat_signal
        self.completed = False
        self.manual = ManualSyncService(
            funder_address="test-funder",
            positions_repo=MagicMock(),
            market_store=MagicMock(),
        )
        self.fallback = AsyncMock()
        self.manual.sync_now = self.fallback
        self.runtime = UserDataRuntime(
            market_store=MagicMock(),
            readiness=MagicMock(),
            api_creds={},
            user_id="test-user",
            manual_sync_service=self.manual,
        )
        self.runtime._ws_client = MagicMock(
            stop=AsyncMock(side_effect=self.runtime._on_disconnected)
        )

    async def run(self, *, auto_trader: bool = False) -> int:
        """Interrupt once in normal operation and again during cleanup."""
        loop = asyncio.get_running_loop()
        handler = signal.getsignal(self.first_signal)
        assert callable(handler)
        loop.call_soon(handler, self.first_signal, None)
        try:
            await asyncio.Future()
        finally:
            repeat_handler = signal.getsignal(self.repeat_signal)
            assert callable(repeat_handler)
            loop.call_soon(repeat_handler, self.repeat_signal, None)
            barrier = asyncio.Event()
            loop.call_soon(barrier.set)
            await barrier.wait()
            await app._shutdown_runtime(
                monitor_tasks=[],
                supervised=[],
                market_data_runtimes={},
                user_data_runtimes=[self.runtime],
                metrics_runner=None,
            )
            self.completed = True
        return 0


@pytest.mark.parametrize("repeat_signal", [signal.SIGINT, signal.SIGTERM])
@pytest.mark.parametrize("first_signal", [signal.SIGINT, signal.SIGTERM])
@pytest.mark.parametrize("entrypoint", ["cli", "app"])
@pytest.mark.parametrize("loop_kind", ["stdlib", "installed"])
def test_repeated_interrupt_finishes_runtime_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    first_signal: signal.Signals,
    repeat_signal: signal.Signals,
    entrypoint: str,
    loop_kind: str,
) -> None:
    """Repeated signals must not skip manual-sync stop or database closure."""
    scenario = ShutdownScenario(first_signal, repeat_signal)
    if loop_kind == "stdlib":
        monkeypatch.setattr("pmcopy.utils.event_loop.new_event_loop", asyncio.new_event_loop)
    close_pool = AsyncMock()
    monkeypatch.setattr(app, "main", scenario.run)
    monkeypatch.setattr("pmcopy.storage.postgres.connection.shutdown_pool", close_pool)
    monkeypatch.setattr("pmcopy.utils.fetchData.close_session", AsyncMock())
    original_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}

    try:
        if entrypoint == "cli":
            assert run_run(argparse.Namespace(auto_trader=False)) == 0
        else:
            assert app.__file__ is not None
            source = ast.parse(Path(app.__file__).read_text(encoding="utf-8"))
            entry = ast.Module(body=[source.body[-1]], type_ignores=[])
            namespace = {
                "__name__": "__main__",
                "main": scenario.run,
                "asyncio": asyncio,
                "signal": signal,
                "logger": MagicMock(),
                "print_error": MagicMock(),
            }
            with pytest.raises(SystemExit) as exited:
                # Bind the compiled app entry block to the sandbox namespace
                # and call it directly: identical execution to the previous
                # exec() call, without a dynamic code-execution primitive.
                entrypoint = FunctionType(compile(entry, app.__file__, "exec"), namespace)
                entrypoint()
            assert exited.value.code == 0
        assert scenario.completed, "Repeated interrupt aborted runtime cleanup"
        assert {sig: signal.getsignal(sig) for sig in original_handlers} == original_handlers
    finally:
        for sig, handler in original_handlers.items():
            signal.signal(sig, handler)

    assert scenario.manual._stopped
    assert scenario.manual._rest_task is None
    close_pool.assert_awaited_once()
    scenario.fallback.assert_not_awaited()


@pytest.mark.parametrize("exit_code", [0, 1])
def test_cli_drains_remaining_tasks_before_restoring_signal_handlers(
    monkeypatch: pytest.MonkeyPatch, exit_code: int
) -> None:
    """Loop finalization awaits pending tasks even if another interrupt arrives."""
    completed: list[str] = []
    original_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}

    async def application(*, auto_trader: bool) -> int:
        started = asyncio.Event()

        async def pending_network_operation() -> None:
            try:
                started.set()
                await asyncio.Future()
            finally:
                loop = asyncio.get_running_loop()
                handler = signal.getsignal(signal.SIGINT)
                assert callable(handler)
                assert handler != original_handlers[signal.SIGINT]
                loop.call_soon(handler, signal.SIGINT, None)
                barrier = asyncio.Event()
                loop.call_soon(barrier.set)
                await barrier.wait()
                completed.append("network_closed")

        asyncio.create_task(pending_network_operation())
        await started.wait()
        return exit_code

    monkeypatch.setattr(app, "main", application)

    assert run_run(argparse.Namespace(auto_trader=False)) == exit_code
    assert completed == ["network_closed"]
    assert {sig: signal.getsignal(sig) for sig in original_handlers} == original_handlers


def test_cli_preserves_application_failure_and_restores_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An application failure must not be reported as an operator-requested exit."""
    original_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}

    async def failed_application(*, auto_trader: bool) -> int:
        raise RuntimeError("test application failed")

    monkeypatch.setattr(app, "main", failed_application)

    with pytest.raises(RuntimeError, match="test application failed"):
        run_run(argparse.Namespace(auto_trader=False))
    assert {sig: signal.getsignal(sig) for sig in original_handlers} == original_handlers
