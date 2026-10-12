"""Focused CLI tests for the automatic-trader startup override."""

from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock

import pytest

from pmcopy.bot_cli.commands import run as run_command


def test_run_parser_accepts_auto_trader_flag() -> None:
    """The main-loop command exposes the requested explicit startup flag."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_command.register_run_command(subparsers)

    args = parser.parse_args(["run", "--auto-trader"])

    assert args.auto_trader is True


def test_auto_follow_is_the_same_flag_without_a_second_count_setting() -> None:
    import argparse
    from pmcopy.bot_cli.commands.run import register_run_command

    parser = argparse.ArgumentParser()
    register_run_command(parser.add_subparsers(dest="command"))
    assert parser.parse_args(["run", "--auto-follow"]).auto_trader is True


@pytest.mark.asyncio
async def test_run_forwards_auto_trader_flag_to_application(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI glue passes the opt-in through without changing persistent config."""
    from pmcopy.app import main as app_main

    mocked_main = AsyncMock(wraps=app_main)
    mocked_main.return_value = 0
    monkeypatch.setattr("pmcopy.app.main", mocked_main)

    result = await run_command._run_bot(auto_trader=True)

    assert result == 0
    mocked_main.assert_awaited_once_with(auto_trader=True)


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, asyncio.CancelledError])
def test_run_run_returns_zero_on_operator_interrupt(
    monkeypatch: pytest.MonkeyPatch, interrupt: type[BaseException]
) -> None:
    """Ctrl-C on ``bot run`` is a clean operator stop, not a crash.

    ``asyncio.Runner`` re-raises ``KeyboardInterrupt`` (or lets
    ``CancelledError`` escape) after graceful teardown; the CLI must map
    that to exit code 0 like ``python app.py`` already does.
    """

    async def interrupted_main(*, auto_trader: bool) -> int:
        raise interrupt()

    monkeypatch.setattr("pmcopy.app.main", interrupted_main)
    args = argparse.Namespace(auto_trader=False)
    assert run_command.run_run(args) == 0
