"""PR 6.1: Live mode requires ExecutionCoordinator.

Pins:
  - Live trade_pipeline copy with coordinator=None skips fail-closed.
  - Paper-mode fallback still works (existing tests must keep functioning).
  - Live risk dispatch with coordinator=None skips fail-closed.
  - runtime_bootstrap wires execution_coordinator for normal user contexts.
"""

from __future__ import annotations

import contextlib
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.services.execution.coordinator import is_live_mode


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
    from pmcopy.orchestrators.trade_pipeline import _submit_copy_through_coordinator

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
    from pmcopy.orchestrators.trade_pipeline import _submit_copy_through_coordinator

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
    from pmcopy.orchestrators.trade_pipeline import _submit_copy_through_coordinator

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
    from pmcopy.orchestrators.trade_pipeline import _submit_copy_through_coordinator

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
    from pmcopy.services.risk.risk_monitor import _dispatch_risk_exit

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
    from pmcopy.services.risk.risk_monitor import _dispatch_risk_exit

    fake = SimpleNamespace(
        accepted=True, status="submitted_unconfirmed", filled_amount=Decimal("0")
    )
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
# bootstrap wires coordinator
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from tests.integration_postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    ctr = PostgresContainer("postgres:16-alpine")
    try:
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


@pytest.mark.asyncio
async def test_runtime_bootstrap_helper_returns_coordinator():
    """``_build_execution_coordinator`` must return a coordinator instance when
    funder_address is set so live runtime contexts are always guarded."""
    from pmcopy.orchestrators.runtime_bootstrap import _build_execution_coordinator
    from pmcopy.storage.postgres.positions_repo import PositionsRepo

    async with _pg_pool() as pool:
        coord = _build_execution_coordinator(
            pool=pool,
            positions_repo=PositionsRepo(pool),
            execution_service=MagicMock(),
            funder_address="0xfunder",
        )
        assert coord is not None
        assert hasattr(coord, "submit_copy_trade")
        assert hasattr(coord, "submit_risk_exit")
        assert hasattr(coord, "submit_protective_exit")
        assert coord.reservation_service.enabled is False
        assert coord.reservations_repo is None


def test_runtime_bootstrap_helper_returns_none_without_funder():
    from pmcopy.orchestrators.runtime_bootstrap import _build_execution_coordinator

    coord = _build_execution_coordinator(
        pool=MagicMock(),
        positions_repo=MagicMock(),
        execution_service=MagicMock(),
        funder_address="",
    )
    assert coord is None
