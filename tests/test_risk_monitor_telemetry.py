"""Operator telemetry for protective-exit transitions in the risk monitor.

Three transitions are reported, all strictly off the copy hot path (the risk
loop runs on its own cadence):

* a recorded trigger -> ``exit_triggered``;
* a filled protective SELL -> ``exit_filled``;
* an accepted-but-zero-fill SELL -> ``exit_not_filled`` (retryable failure).

The harness is the real-Postgres one from the risk-block correctness suite, so
these run through ``run_risk_check_for_context`` rather than a stub.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from pmcopy.services.execution.types import ExecutionResult
from pmcopy.services.risk.risk_monitor import (
    rehydrate_position_manager,
    run_risk_check_for_context,
)
from pmcopy.services.telemetry import (
    FailureComponent,
    FailureEvent,
    OrderOutcome,
    ProtectiveExitEvent,
    ProtectiveOutcome,
    TelemetryProducer,
    safe_ref,
)
from pmcopy.storage.postgres.fills_repo import FillRecord, FillsRepo
from pmcopy.storage.postgres.positions_repo import PositionsRepo
from tests.telemetry_helpers import raising_emitters, recording_emitters
from tests.test_issue72_risk_block_correctness import (
    FAKE_TOKEN_ID,
    _make_context,
    _pg_pool,
    _FakeExecutionService,
)

pytestmark = pytest.mark.integration

_CONDITION_ID = "cond-1"
_POSITION_REF = safe_ref(f"{_CONDITION_ID}:{FAKE_TOKEN_ID}")


class _CountingExecutionService(_FakeExecutionService):
    """Filling execution service that records how often it was asked to act."""

    def __init__(self) -> None:
        """Start with no recorded submissions."""

        self.calls = 0

    async def execute(self, req):
        """Count the submission, then fill it like the shared fake does."""

        self.calls += 1
        return await super().execute(req)


class _ZeroFillExecutionService:
    """Accepted submission that terminally matched nothing (killed FAK)."""

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, req):
        self.calls += 1
        return ExecutionResult(
            accepted=True,
            order_id="risk-exit-zero-fill",
            status="cancelled",
            requested_amount=Decimal(str(req.amount)),
            filled_amount=Decimal("0"),
            avg_price=None,
            fees_paid=Decimal("0"),
            error=None,
        )

    async def _confirm_fill(self, order_id: str):
        return {
            "status": "cancelled",
            "size_matched": "0",
            "avg_price": None,
            "orderID": order_id,
        }


async def _seed_position(pool) -> None:
    fills_repo = FillsRepo(pool)
    positions_repo = PositionsRepo(pool)
    await fills_repo.append_fill(
        FillRecord(
            user_id="u1",
            leader_id="l1",
            order_id="buy-1",
            condition_id=_CONDITION_ID,
            token_id=FAKE_TOKEN_ID,
            side="BUY",
            size_tokens=Decimal("10"),
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    await positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)


@pytest.mark.asyncio
async def test_filled_protective_exit_reports_triggered_then_filled() -> None:
    async with _pg_pool() as pool:
        await _seed_position(pool)
        emitters, service = recording_emitters()
        context = _make_context(pool, execution_service=_FakeExecutionService())
        context.telemetry = emitters
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)

        events = service.payloads(ProtectiveExitEvent)
        assert [event.outcome for event in events] == [
            ProtectiveOutcome.TRIGGERED,
            ProtectiveOutcome.FILLED,
        ]
        assert {event.task_ref for event in events} == {_POSITION_REF}
        assert {call.producer for call in service.calls} == {TelemetryProducer.RISK}


@pytest.mark.asyncio
async def test_protective_exit_keeps_unknown_fee_in_accounting_and_evidence() -> None:
    """Immediate risk exits must not publish absent venue fees as measured zero."""
    from pmcopy.storage.postgres.fill_evidence_projection import PostgresFillEvidenceProjection
    from tests.test_aethertrade_fill_evidence import FillClock
    from tests.test_aethertrade_fill_evidence_postgres import events

    class UnknownFeeExecution(_FakeExecutionService):
        async def execute(self, request):
            result = await super().execute(request)
            result.fees_paid = None
            return result

    async with _pg_pool() as pool:
        await _seed_position(pool)
        context = _make_context(pool, execution_service=UnknownFeeExecution())
        context.fills_repo = FillsRepo(
            pool, evidence_projection=PostgresFillEvidenceProjection(clock=FillClock())
        )
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)

        fills = await context.fills_repo.list_fills_for_user("u1")
        exits = [fill for fill in fills if fill["side"] == "SELL"]
        assert len(exits) == 1
        assert exits[0]["fee"] is None
        observed = await events(pool)
        assert len(observed) == 1
        assert observed[0]["payload"].get("fee") is None
        assert observed[0]["payload"].get("source_action_id") is None


@pytest.mark.asyncio
async def test_zero_fill_protective_exit_reports_a_retryable_failure() -> None:
    async with _pg_pool() as pool:
        await _seed_position(pool)
        emitters, service = recording_emitters()
        exec_svc = _ZeroFillExecutionService()
        context = _make_context(pool, execution_service=exec_svc)
        context.telemetry = emitters
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)

        assert exec_svc.calls == 1
        [triggered] = service.payloads(ProtectiveExitEvent)
        assert triggered.outcome is ProtectiveOutcome.TRIGGERED
        [failure] = service.payloads(FailureEvent)
        assert failure.component is FailureComponent.PROTECTIVE_EXIT
        assert failure.retryable is True
        assert failure.order_outcome is OrderOutcome.NOT_FILLABLE
        assert failure.code.startswith("exit_not_filled")


@pytest.mark.asyncio
async def test_raising_telemetry_backend_never_breaks_the_risk_loop() -> None:
    async with _pg_pool() as pool:
        await _seed_position(pool)
        emitters, service = raising_emitters()
        exec_svc = _FakeExecutionService()
        context = _make_context(pool, execution_service=exec_svc)
        context.telemetry = emitters
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)

        assert service.calls >= 1
        position = await context.positions_repo.get_position(
            user_id="u1", condition_id=_CONDITION_ID, token_id=FAKE_TOKEN_ID
        )
        assert position is None or position.size_tokens == Decimal("0")


@pytest.mark.asyncio
async def test_context_without_telemetry_still_runs_the_risk_loop() -> None:
    """A context with no emitters must still take the protective action.

    Asserting only that the call returns would pass even if the absent
    telemetry attribute short-circuited the whole risk loop, so this mirrors
    the raising-backend sibling: the exit is submitted and the position closes.
    """

    async with _pg_pool() as pool:
        await _seed_position(pool)
        exec_svc = _CountingExecutionService()
        context = _make_context(pool, execution_service=exec_svc)
        # The composition default: every emitter is a no-op returning False.
        assert context.telemetry.risk.exit_triggered(_CONDITION_ID, FAKE_TOKEN_ID, "x") is False
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)

        assert exec_svc.calls >= 1
        position = await context.positions_repo.get_position(
            user_id="u1", condition_id=_CONDITION_ID, token_id=FAKE_TOKEN_ID
        )
        assert position is None or position.size_tokens == Decimal("0")
