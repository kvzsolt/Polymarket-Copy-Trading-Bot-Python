"""PR 6.1: Live mode requires ExecutionCoordinator.

Pins:
  - Live trade_pipeline copy with coordinator=None skips fail-closed.
  - Paper-mode fallback still works (existing tests must keep functioning).
  - Live near-one exit with coordinator=None skips fail-closed.
  - Live risk dispatch with coordinator=None skips fail-closed.
  - runtime_bootstrap wires execution_coordinator for normal user contexts.
"""
from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.execution.coordinator import is_live_mode


# ---------------------------------------------------------------------------
# is_live_mode helper
# ---------------------------------------------------------------------------

def test_is_live_mode_none_config_is_paper():
    assert is_live_mode(None) is False


def test_is_live_mode_dry_run_is_paper():
    cfg = SimpleNamespace(dry_run=True, execution=SimpleNamespace(paper_trade_only=False))
    assert is_live_mode(cfg) is False


def test_is_live_mode_paper_trade_only_is_paper():
    cfg = SimpleNamespace(dry_run=False, execution=SimpleNamespace(paper_trade_only=True))
    assert is_live_mode(cfg) is False


def test_is_live_mode_real_config_is_live():
    cfg = SimpleNamespace(dry_run=False, execution=SimpleNamespace(paper_trade_only=False))
    assert is_live_mode(cfg) is True


# ---------------------------------------------------------------------------
# trade_pipeline copy fallback fail-closed in live mode
# ---------------------------------------------------------------------------

def _live_config() -> SimpleNamespace:
    return SimpleNamespace(
        dry_run=False,
        execution=SimpleNamespace(paper_trade_only=False),
    )


def _paper_config() -> SimpleNamespace:
    return SimpleNamespace(
        dry_run=False,
        execution=SimpleNamespace(paper_trade_only=True),
    )


def _exec_req() -> object:
    return SimpleNamespace(
        user_id="u1",
        leader_id="0xleader",
        token_id="0xtok",
        side="BUY",
        amount=Decimal("5"),
        max_slippage_bps=200,
        reason="copy_trade",
        signal_id="sig-1",
        condition_id="cond-1",
        recovery_metadata=None,
    )


def _signal(side: str = "BUY") -> SimpleNamespace:
    return SimpleNamespace(
        token_id="0xtok",
        condition_id="cond-1",
        side=side,
        tx_hash="0xtx",
        price=Decimal("0.5"),
        usdc_size=Decimal("2.5"),
    )


@pytest.mark.asyncio
async def test_live_trade_pipeline_skips_when_coordinator_missing():
    from orchestrators.trade_pipeline import _submit_copy_through_coordinator

    exec_svc = MagicMock()
    exec_svc.execute = AsyncMock()
    ctx = SimpleNamespace(
        user_id="u1",
        execution_coordinator=None,
        execution_service=exec_svc,
        config=_live_config(),
    )

    result = await _submit_copy_through_coordinator(
        ctx, _exec_req(), _signal(), leader_id="0xleader"
    )

    assert result.accepted is False
    assert result.status == "execution_coordinator_missing"
    exec_svc.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_trade_pipeline_skips_for_sell_too():
    from orchestrators.trade_pipeline import _submit_copy_through_coordinator

    exec_svc = MagicMock()
    exec_svc.execute = AsyncMock()
    ctx = SimpleNamespace(
        user_id="u1",
        execution_coordinator=None,
        execution_service=exec_svc,
        config=_live_config(),
    )
    req = _exec_req()
    req.side = "SELL"

    result = await _submit_copy_through_coordinator(
        ctx, req, _signal(side="SELL"), leader_id="0xleader"
    )

    assert result.status == "execution_coordinator_missing"
    exec_svc.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_paper_mode_trade_pipeline_falls_back_to_execution_service():
    from orchestrators.trade_pipeline import _submit_copy_through_coordinator

    fake_result = SimpleNamespace(accepted=True, status="submitted_unconfirmed")
    exec_svc = MagicMock()
    exec_svc.execute = AsyncMock(return_value=fake_result)
    ctx = SimpleNamespace(
        user_id="u1",
        execution_coordinator=None,
        execution_service=exec_svc,
        config=_paper_config(),
    )

    result = await _submit_copy_through_coordinator(
        ctx, _exec_req(), _signal(), leader_id="0xleader"
    )

    assert result is fake_result
    exec_svc.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_config_trade_pipeline_falls_back():
    from orchestrators.trade_pipeline import _submit_copy_through_coordinator

    fake_result = SimpleNamespace(accepted=True, status="submitted_unconfirmed")
    exec_svc = MagicMock()
    exec_svc.execute = AsyncMock(return_value=fake_result)
    ctx = SimpleNamespace(
        user_id="u1",
        execution_coordinator=None,
        execution_service=exec_svc,
        config=None,
    )

    result = await _submit_copy_through_coordinator(
        ctx, _exec_req(), _signal(), leader_id="0xleader"
    )

    assert result is fake_result
    exec_svc.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# risk monitor _dispatch_risk_exit fail-closed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_risk_dispatch_skips_when_coordinator_missing():
    from services.riskMonitor import _dispatch_risk_exit

    exec_svc = MagicMock()
    exec_svc.execute = AsyncMock()
    exec_svc._config = _live_config()

    result = await _dispatch_risk_exit(
        coordinator=None,
        execution_service=exec_svc,
        exec_req=_exec_req(),
        user_id="u1",
        condition_id="cond-1",
        token_id="0xtok",
        side="SELL",
        price=0.5,
        notional=2.5,
        source="risk_exit",
        leader_wallet="0xleader",
        signal_id="sig-1",
    )

    assert result.status == "execution_coordinator_missing"
    exec_svc.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_paper_risk_dispatch_falls_back():
    from services.riskMonitor import _dispatch_risk_exit

    fake = SimpleNamespace(accepted=True, status="submitted_unconfirmed", filled_amount=Decimal("0"))
    exec_svc = MagicMock()
    exec_svc.execute = AsyncMock(return_value=fake)
    exec_svc._config = _paper_config()

    result = await _dispatch_risk_exit(
        coordinator=None,
        execution_service=exec_svc,
        exec_req=_exec_req(),
        user_id="u1",
        condition_id="cond-1",
        token_id="0xtok",
        side="SELL",
        price=0.5,
        notional=2.5,
        source="risk_exit",
        leader_wallet="0xleader",
        signal_id="sig-1",
    )

    assert result is fake
    exec_svc.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# near_one_exit fail-closed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_near_one_exit_skips_when_coordinator_missing():
    from cache.market_state_store import TickSnapshot
    from services.near_one_exit import NearOneExitService

    cfg = SimpleNamespace(
        dry_run=False,
        execution=SimpleNamespace(paper_trade_only=False, slippage_tolerance=2.0),
        near_one_exit=SimpleNamespace(
            enable_near_one_exit=True,
            threshold=Decimal("0.99"),
            dwell_seconds=0,
            check_interval=1,
        ),
    )
    mds = MagicMock()
    mds.get_tick_or_none = AsyncMock(
        return_value=TickSnapshot(
            token_id="tok-1",
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source="wss",
            monotonic_ts=time.monotonic(),
        )
    )
    exec_svc = MagicMock()
    exec_svc.execute = AsyncMock()

    svc = NearOneExitService(
        position_manager=MagicMock(),
        execution_service=exec_svc,
        market_data_service=mds,
        config=cfg,
        readiness=MagicMock(),
        user_id="u1",
        funder_address="0xfunder",
        execution_coordinator=None,
    )

    await svc._fire(
        condition_id="cond-1",
        token_id="tok-1",
        size_tokens=5.0,
        live_price=0.995,
        dwell_elapsed=0.1,
        slippage_bps=200,
    )

    exec_svc.execute.assert_not_awaited()


# ---------------------------------------------------------------------------
# bootstrap wires coordinator
# ---------------------------------------------------------------------------

def test_runtime_bootstrap_helper_returns_coordinator(tmp_path):
    """``_build_execution_coordinator`` must return a coordinator instance when
    funder_address is set so live runtime contexts are always guarded."""
    from orchestrators.runtime_bootstrap import _build_execution_coordinator
    from storage.sqlite.connection import create_connection
    from storage.sqlite.migrations import apply_migrations
    from storage.sqlite.positions_repo import PositionsRepo

    db_path = str(tmp_path / "bootstrap.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
    coord = _build_execution_coordinator(
        db_path=db_path,
        positions_repo=PositionsRepo(db_path),
        order_repo=MagicMock(),
        fills_repo=MagicMock(),
        execution_service=MagicMock(),
        funder_address="0xfunder",
    )
    assert coord is not None
    assert hasattr(coord, "submit_copy_trade")
    assert hasattr(coord, "submit_risk_exit")
    assert hasattr(coord, "submit_near_one_exit")
    assert hasattr(coord, "submit_protective_exit")


def test_runtime_bootstrap_helper_returns_none_without_funder(tmp_path):
    from orchestrators.runtime_bootstrap import _build_execution_coordinator

    coord = _build_execution_coordinator(
        db_path=str(tmp_path / "x.db"),
        positions_repo=MagicMock(),
        order_repo=MagicMock(),
        fills_repo=MagicMock(),
        execution_service=MagicMock(),
        funder_address="",
    )
    assert coord is None
