"""Task 22 — ``--copier`` CLI selection limits routing AND runtime ownership.

The selector must reach the application and filter the loaded roster at
BOTH user-load sites (the auto-trader path and the normal startup path) so
the selected copier is the only account constructed, leased, and routed
signals for. An unknown id fails startup with a clear operator error
instead of running an unexpected subset (addon §7).
"""

from __future__ import annotations

import argparse
import inspect
from unittest.mock import AsyncMock

import pytest

import pmcopy.app as app_module
from pmcopy.bot_cli.commands import run as run_command


def test_run_parser_accepts_copier_selector() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_command.register_run_command(subparsers)

    args = parser.parse_args(["run", "--copier", "secondary"])
    assert args.copier == "secondary"

    # Absent selector → no filtering (all configured copiers start).
    args = parser.parse_args(["run"])
    assert args.copier is None


@pytest.mark.asyncio
async def test_run_forwards_copier_to_application(monkeypatch: pytest.MonkeyPatch) -> None:
    from pmcopy.app import main as app_main

    mocked_main = AsyncMock(wraps=app_main)
    mocked_main.return_value = 0
    monkeypatch.setattr("pmcopy.app.main", mocked_main)

    result = await run_command._run_bot(copier="secondary")

    assert result == 0
    mocked_main.assert_awaited_once_with(auto_trader=False, copier="secondary")


@pytest.mark.asyncio
async def test_run_run_async_passes_args_copier(monkeypatch: pytest.MonkeyPatch) -> None:
    forwarded: dict[str, object] = {}

    async def _fake_run_bot(*, auto_trader: bool = False, copier: str | None = None) -> int:
        forwarded["auto_trader"] = auto_trader
        forwarded["copier"] = copier
        return 0

    monkeypatch.setattr(run_command, "_run_bot", _fake_run_bot)
    args = argparse.Namespace(auto_trader=False, copier="primary")
    assert await run_command._run_run_async(args) == 0
    assert forwarded == {"auto_trader": False, "copier": "primary"}


@pytest.mark.asyncio
async def test_app_main_threads_copier_to_run_application(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mocked_run = AsyncMock(return_value=0)
    monkeypatch.setattr(app_module, "_run_application", mocked_run)

    assert await app_module.main(copier="secondary") == 0

    kwargs = mocked_run.await_args.kwargs
    assert kwargs.get("copier") == "secondary"


def test_run_application_filters_both_user_load_sites() -> None:
    """Selection limits routing AND ownership at BOTH roster load sites."""
    source = inspect.getsource(app_module._run_application)
    assert source.count("select_runtime_copiers(") == 2, (
        "expected the --copier selector applied to both the auto-trader "
        "roster load and the normal startup roster load"
    )
    # The filter must run before any runtime is constructed from the users.
    assert source.index("select_runtime_copiers(") < source.index(
        "user_states[runtime_user.user_id] = build_user_state("
    )
    # An unknown id is an operator error: fail-closed startup, not a subset.
    assert "unknown --copier" in source or "print_error" in source


def test_app_main_signature_has_copier_parameter() -> None:
    parameters = inspect.signature(app_module.main).parameters
    assert "copier" in parameters
    assert parameters["copier"].default is None
