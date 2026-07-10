from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from bot_cli.commands.trading import register_trading_command
from services.executionService import ExecutionRequest, ExecutionService
from services.trading_permission import TradingPausedError, TradingPermissionService
from storage.postgres.trading_control_repo import TradingControlState
from tests.conftest import make_exec_config


def state(enabled: bool) -> TradingControlState:
    return TradingControlState(
        enabled=enabled,
        generation=3,
        reason="operator_test",
        updated_at=datetime.now(timezone.utc),
        updated_by="pytest",
    )


@pytest.mark.asyncio
async def test_live_permission_defaults_to_paused_state() -> None:
    repo = SimpleNamespace(get=AsyncMock(return_value=state(False)))
    service = TradingPermissionService(repo, live_mode=True)
    with pytest.raises(TradingPausedError, match="operator_test"):
        await service.require_submission_allowed()


@pytest.mark.asyncio
async def test_database_failure_blocks_live_submission() -> None:
    repo = SimpleNamespace(get=AsyncMock(side_effect=RuntimeError("db down")))
    service = TradingPermissionService(repo, live_mode=True)
    with pytest.raises(TradingPausedError, match="unavailable"):
        await service.require_submission_allowed()


@pytest.mark.asyncio
async def test_paper_mode_does_not_query_live_control() -> None:
    repo = SimpleNamespace(get=AsyncMock())
    service = TradingPermissionService(repo, live_mode=False)
    assert await service.require_submission_allowed() is None
    repo.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_execution_stops_before_market_or_gateway_when_paused() -> None:
    permission = SimpleNamespace(
        require_submission_allowed=AsyncMock(
            side_effect=TradingPausedError("paused by operator")
        )
    )
    market_data = SimpleNamespace(require_fresh_data=AsyncMock())
    gateway = SimpleNamespace(submit_marketable_order=AsyncMock())
    service = ExecutionService(
        MagicMock(),
        config=make_exec_config(),
        clob_gateway=gateway,
        market_data_service=market_data,
        trading_permission=permission,
    )
    result = await service.execute(
        ExecutionRequest(
            user_id="user",
            leader_id="leader",
            token_id="token",
            side="BUY",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="signal",
            condition_id="condition",
        )
    )
    assert result.accepted is False
    assert result.status == "trading_paused"
    market_data.require_fresh_data.assert_not_awaited()
    gateway.submit_marketable_order.assert_not_awaited()


def test_trading_cli_requires_explicit_live_confirmation() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    register_trading_command(subparsers)
    args = parser.parse_args(
        ["trading", "resume", "--confirm", "LIVE", "--reason", "validated"]
    )
    assert args.trading_action == "resume"
    assert args.confirm == "LIVE"


def test_migration_creates_paused_singleton() -> None:
    from storage.postgres.migrations import _CORE_DDL

    ddl = "\n".join(_CORE_DDL)
    assert "CREATE TABLE IF NOT EXISTS trading_control" in ddl
    assert "VALUES (1, false" in ddl
