"""2026-07-04 exit-storm incident — Defect B regression suite.

A terminal (rejected/cancelled) risk-exit submission must arm a scope-keyed
cooldown so the stateless near-max/stop-loss decision cannot re-dispatch the
same doomed SELL at the risk-loop cadence (observed: 1 Hz for ~3 minutes
against a phantom position, one risk_triggers spam row per attempt).

Unit layer drives the helpers directly (no docker needed); the integration
layer (bottom) replays the storm through ``run_risk_check_for_context`` with
the same real-PG harness as tests/test_issue72_risk_block_correctness.py.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from types import SimpleNamespace

import pytest

from services.risk.risk_monitor import (
    _arm_risk_exit_reject_cooldown,
    _risk_exit_cooldown_active,
    _risk_exit_reject_cooldown_seconds,
    _should_arm_risk_exit_reject_cooldown,
)

_COND = "cond-storm"
_TOKEN = "token-storm"


def _ctx(config=None):
    if config is None:
        return SimpleNamespace(extra={})
    return SimpleNamespace(extra={}, config=config)


class TestCooldownHelpers:
    def test_arm_then_active_within_ttl_and_expired_after(self):
        ctx = _ctx()
        _arm_risk_exit_reject_cooldown(ctx, condition_id=_COND, token_id=_TOKEN, now=1000.0)
        assert (
            _risk_exit_cooldown_active(ctx, condition_id=_COND, token_id=_TOKEN, now=1030.0) is True
        )
        # Default TTL is 60s — expired one tick after.
        assert (
            _risk_exit_cooldown_active(ctx, condition_id=_COND, token_id=_TOKEN, now=1060.1)
            is False
        )

    def test_scope_keyed_not_global(self):
        ctx = _ctx()
        _arm_risk_exit_reject_cooldown(ctx, condition_id=_COND, token_id=_TOKEN, now=1000.0)
        assert (
            _risk_exit_cooldown_active(ctx, condition_id="other-cond", token_id=_TOKEN, now=1001.0)
            is False
        )
        assert (
            _risk_exit_cooldown_active(ctx, condition_id=_COND, token_id="other-token", now=1001.0)
            is False
        )

    def test_typed_zero_disables(self):
        ctx = _ctx(SimpleNamespace(risk=SimpleNamespace(risk_exit_reject_cooldown_seconds=0)))
        _arm_risk_exit_reject_cooldown(ctx, condition_id=_COND, token_id=_TOKEN, now=1000.0)
        assert (
            _risk_exit_cooldown_active(ctx, condition_id=_COND, token_id=_TOKEN, now=1000.1)
            is False
        )

    def test_typed_short_ttl_expiry(self):
        ctx = _ctx(SimpleNamespace(risk=SimpleNamespace(risk_exit_reject_cooldown_seconds=0.05)))
        _arm_risk_exit_reject_cooldown(ctx, condition_id=_COND, token_id=_TOKEN, now=1000.0)
        assert (
            _risk_exit_cooldown_active(ctx, condition_id=_COND, token_id=_TOKEN, now=1000.04)
            is True
        )
        assert (
            _risk_exit_cooldown_active(ctx, condition_id=_COND, token_id=_TOKEN, now=1000.06)
            is False
        )

    def test_typed_config_is_used(self):
        cfg = SimpleNamespace(risk=SimpleNamespace(risk_exit_reject_cooldown_seconds=5.0))
        assert _risk_exit_reject_cooldown_seconds(_ctx(config=cfg)) == 5.0

    def test_default_is_60(self):
        assert _risk_exit_reject_cooldown_seconds(_ctx()) == 60.0

    def test_appconfig_field_typed_default(self):
        # AppConfig-drift precedent: the key must live on the typed surface.
        from config.app_config import RiskConfig

        field = {f.name: f for f in dataclasses.fields(RiskConfig)}[
            "risk_exit_reject_cooldown_seconds"
        ]
        assert field.default == 60.0

    @pytest.mark.parametrize("status", ["rejected", "cancelled"])
    def test_terminal_statuses_arm_cooldown(self, status):
        assert _should_arm_risk_exit_reject_cooldown(status, "opaque") is True

    def test_failed_insufficient_balance_arms_cooldown(self):
        assert (
            _should_arm_risk_exit_reject_cooldown(
                "failed",
                "insufficient_balance | not enough balance / allowance",
            )
            is True
        )

    def test_failed_transient_error_does_not_arm_cooldown(self):
        assert _should_arm_risk_exit_reject_cooldown("failed", "timeout") is False


# --------------------------------------------------------------------------
# Integration: storm replay through run_risk_check_for_context (real PG,
# skipped when docker is unavailable — same policy as issue72 harness).
# --------------------------------------------------------------------------

from storage.postgres.fills_repo import FillRecord, FillsRepo  # noqa: E402
from storage.postgres.positions_repo import PositionsRepo  # noqa: E402
from storage.postgres.risk_triggers_repo import RiskTriggersRepo  # noqa: E402

from services.executionService import ExecutionResult  # noqa: E402
from services.risk.risk_monitor import (  # noqa: E402
    rehydrate_position_manager,
    run_risk_check_for_context,
)
from tests.test_issue72_risk_block_correctness import (  # noqa: E402
    FAKE_TOKEN_ID,
    _make_context,
    _pg_pool,
)

# NOTE: applied per-test (not module-level pytestmark) so the helper unit
# tests above stay in the default (non-integration) selection.
_integration = pytest.mark.integration


class _CountingExecutionService:
    """Configurable terminal/transient responder that counts dispatches."""

    def __init__(
        self,
        status: str = "rejected",
        accepted: bool = False,
        error: str | None = "synthetic reject",
        order_id: str | None = None,
    ):
        self.calls = 0
        self._status = status
        self._accepted = accepted
        self._error = error
        self._order_id = order_id

    async def execute(self, req):
        self.calls += 1
        return ExecutionResult(
            accepted=self._accepted,
            order_id=self._order_id,
            status=self._status,
            requested_amount=Decimal(str(req.amount)),
            filled_amount=Decimal("0"),
            avg_price=None,
            fees_paid=Decimal("0"),
            error=self._error,
        )

    async def _confirm_fill(self, order_id: str):
        return {
            "status": "cancelled",
            "size_matched": "0",
            "avg_price": None,
            "orderID": order_id,
        }


async def _seed_position(pool):
    fills_repo = FillsRepo(pool)
    positions_repo = PositionsRepo(pool)
    await fills_repo.append_fill(
        FillRecord(
            user_id="u1",
            leader_id="l1",
            order_id="buy-1",
            condition_id="cond-1",
            token_id=FAKE_TOKEN_ID,
            side="BUY",
            size_tokens=Decimal("10"),
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    await positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)
    return positions_repo


@_integration
@pytest.mark.asyncio
async def test_storm_regression_second_cycle_suppressed():
    """The 03:16 storm: rejected exit re-dispatched every cycle. Post-fix:
    one dispatch, one trigger row, second cycle silent."""
    async with _pg_pool() as pool:
        await _seed_position(pool)
        risk_triggers_repo = RiskTriggersRepo(pool)
        exec_svc = _CountingExecutionService(
            status="rejected",
            error="insufficient_balance | {'error': 'not enough balance / allowance'}",
        )
        context = _make_context(
            pool, execution_service=exec_svc, risk_triggers_repo=risk_triggers_repo
        )
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)
        await run_risk_check_for_context(context)

        assert exec_svc.calls == 1, "cooldown must suppress the second dispatch"
        triggers = await risk_triggers_repo.list_triggers_for_position(
            user_id="u1", condition_id="cond-1", token_id=FAKE_TOKEN_ID
        )
        assert len(triggers) == 1, "no risk_triggers spam row on suppressed cycle"


@_integration
@pytest.mark.asyncio
async def test_opaque_error_still_arms_cooldown():
    """Defect-B independence: even the pre-fix opaque error string arms the
    cooldown — status is the signal, not the error text."""
    async with _pg_pool() as pool:
        await _seed_position(pool)
        exec_svc = _CountingExecutionService(status="rejected", error="VenueInvalidOrderError")
        context = _make_context(pool, execution_service=exec_svc)
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)
        await run_risk_check_for_context(context)

        assert exec_svc.calls == 1


@_integration
@pytest.mark.asyncio
async def test_transient_failure_not_suppressed():
    """status='failed' (retries exhausted, transient) must keep the normal
    cadence — a genuine exit may succeed next cycle."""
    async with _pg_pool() as pool:
        await _seed_position(pool)
        exec_svc = _CountingExecutionService(status="failed", error="timeout")
        context = _make_context(pool, execution_service=exec_svc)
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)
        await run_risk_check_for_context(context)

        assert exec_svc.calls == 2


@_integration
@pytest.mark.asyncio
async def test_failed_insufficient_balance_suppressed():
    """Observed CLOB shape: status='failed' with balance/allowance text."""
    async with _pg_pool() as pool:
        await _seed_position(pool)
        exec_svc = _CountingExecutionService(
            status="failed",
            error="insufficient_balance | {'error': 'not enough balance / allowance'}",
        )
        context = _make_context(pool, execution_service=exec_svc)
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)
        await run_risk_check_for_context(context)

        assert exec_svc.calls == 1


@_integration
@pytest.mark.asyncio
async def test_typed_zero_disables_suppression():
    async with _pg_pool() as pool:
        await _seed_position(pool)
        exec_svc = _CountingExecutionService(status="rejected")
        context = _make_context(pool, execution_service=exec_svc)
        context.config = SimpleNamespace(risk=SimpleNamespace(risk_exit_reject_cooldown_seconds=0))
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)
        await run_risk_check_for_context(context)

        assert exec_svc.calls == 2


@_integration
@pytest.mark.asyncio
async def test_zero_fill_accepted_also_arms_cooldown():
    """Accepted-but-zero-fill (fully killed FAK) has the same 1 Hz refire
    shape — both branches arm the cooldown."""
    async with _pg_pool() as pool:
        await _seed_position(pool)
        exec_svc = _CountingExecutionService(
            status="cancelled", accepted=True, error=None, order_id="ord-zf-1"
        )
        context = _make_context(pool, execution_service=exec_svc)
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)
        await run_risk_check_for_context(context)

        assert exec_svc.calls == 1
