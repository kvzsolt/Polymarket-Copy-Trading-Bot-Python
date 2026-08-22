"""Post-submission telemetry projection for ``ExecutionService.execute``.

``execute`` is a thin wrapper around ``_execute_impl``: it projects the
returned ``ExecutionResult`` onto exactly one bounded operator event after the
result exists, and reports an escaped exception before re-raising it. The
wrapper is the only place venue outcomes are published, which is what keeps a
single copy from being counted both here and in the pipeline projection.

The fakes are imported from the act-time kill-switch suite so this module adds
no new venue harness.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInsufficientBalanceError,
    VenueInvalidOrderError,
)
from integrations.polymarket.models import VenueSubmitResult
import services.executionService as execution_module
from services.execution.types import ExecutionResult
from services.executionService import ExecutionService
from services.telemetry import (
    AmbiguousEvent,
    AmbiguousOperation,
    FailureComponent,
    FailureEvent,
    OrderOutcome,
    TelemetryProducer,
    TradeEvent,
    TradeSide,
    safe_ref,
)
from tests.telemetry_helpers import raising_emitters, recording_emitters
from tests.test_execution_kill_switch_acttime import (
    _FeeSvc,
    _StubClob,
    _make_exec_config,
    _make_mds,
    _mock_order_repo,
    _req,
    _success_gateway,
)


def _service(gateway, telemetry) -> ExecutionService:
    return ExecutionService(
        _StubClob(),
        _FeeSvc(),
        _mock_order_repo(),
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(),
        telemetry=telemetry,
    )


def _fixed_result(**overrides) -> ExecutionResult:
    fields: dict[str, object] = {
        "accepted": True,
        "order_id": "ord-1",
        "status": "submitted_unconfirmed",
        "requested_amount": Decimal("5"),
        "filled_amount": Decimal("0"),
        "avg_price": None,
        "fees_paid": Decimal("0"),
        "error": None,
    }
    fields.update(overrides)
    return ExecutionResult(**fields)  # type: ignore[arg-type]


async def _project(result: ExecutionResult, telemetry):
    """Drive the wrapper with a fixed implementation result."""
    svc = _service(_success_gateway(), telemetry)
    with patch.object(ExecutionService, "_execute_impl", AsyncMock(return_value=result)):
        return await svc.execute(_req("BUY", reason="copy_trade"))


async def _project_fresh(result_factory, telemetry):
    """Drive the wrapper with a *newly built* result on every call.

    ``_project`` returns the very object it was handed, so comparing two runs
    would compare an object with itself. This variant builds an
    independent-but-equal ``ExecutionResult`` per run.
    """
    svc = _service(_success_gateway(), telemetry)

    async def _impl(_self, _request):
        # Patched onto the class, so the descriptor hands us ``self`` too.
        return result_factory()

    with patch.object(ExecutionService, "_execute_impl", _impl):
        return await svc.execute(_req("BUY", reason="copy_trade"))


class TestOrderResultProjection:
    @pytest.mark.asyncio
    async def test_accepted_submission_reports_a_submitted_trade_event(self) -> None:
        emitters, service = recording_emitters()

        returned = await _project(_fixed_result(), emitters.execution)

        assert returned.status == "submitted_unconfirmed"
        [event] = service.payloads(TradeEvent)
        assert event.order_outcome is OrderOutcome.SUBMITTED
        assert event.side is TradeSide.BUY
        assert event.order_ref == safe_ref("ord-1")
        assert event.task_ref == safe_ref("sig-BUY")
        assert service.calls[0].producer is TelemetryProducer.EXECUTION

    @pytest.mark.asyncio
    async def test_ambiguous_submission_reports_reconciliation_required(self) -> None:
        emitters, service = recording_emitters()

        await _project(
            _fixed_result(status="submitted_unconfirmed", error="network timeout after POST"),
            emitters.execution,
        )

        [event] = service.payloads(AmbiguousEvent)
        assert event.operation is AmbiguousOperation.ORDER_SUBMISSION
        assert event.reconciliation_required is True

    @pytest.mark.asyncio
    async def test_local_gate_reports_a_failure_without_a_venue_outcome(self) -> None:
        """A kill switch is a local refusal, never a venue rejection."""
        emitters, service = recording_emitters()
        gw = _success_gateway()
        svc = ExecutionService(
            _StubClob(),
            _FeeSvc(),
            _mock_order_repo(),
            config=_make_exec_config(global_kill=True),
            clob_gateway=gw,
            market_data_service=_make_mds(),
            telemetry=emitters.execution,
        )

        result = await svc.execute(_req("BUY"))

        assert result.status == "kill_switch"
        gw.submit_marketable_order.assert_not_awaited()
        [event] = service.payloads(FailureEvent)
        assert event.component is FailureComponent.EXECUTION
        assert event.code == "kill_switch"
        assert event.order_outcome is None

    @pytest.mark.asyncio
    async def test_venue_rejection_reports_a_venue_order_outcome(self) -> None:
        emitters, service = recording_emitters()

        await _project(
            _fixed_result(accepted=False, order_id=None, status="rejected", error="nope"),
            emitters.execution,
        )

        [event] = service.payloads(FailureEvent)
        assert event.order_outcome is OrderOutcome.REJECTED

    @pytest.mark.asyncio
    async def test_a_real_submission_still_reports_exactly_one_event(self) -> None:
        """End-to-end through the real implementation, not a patched result."""
        emitters, service = recording_emitters()
        gw = _success_gateway()
        gw.submit_marketable_order = AsyncMock(
            return_value=VenueSubmitResult(success=True, order_id="ord-live", status="submitted")
        )
        svc = _service(gw, emitters.execution)

        result = await svc.execute(_req("BUY"))

        assert result.accepted is True
        assert len(service.calls) == 1
        assert service.calls[0].producer is TelemetryProducer.EXECUTION


class TestExecutionFailure:
    @pytest.mark.asyncio
    async def test_escaped_exception_is_reported_and_re_raised(self) -> None:
        emitters, service = recording_emitters()
        svc = _service(_success_gateway(), emitters.execution)

        with patch.object(
            ExecutionService,
            "_execute_impl",
            AsyncMock(side_effect=RuntimeError("venue exploded")),
        ):
            with pytest.raises(RuntimeError, match="venue exploded"):
                await svc.execute(_req("BUY"))

        [event] = service.payloads(FailureEvent)
        assert event.component is FailureComponent.EXECUTION
        assert "exploded" not in event.code
        assert service.calls[0].correlation_id == safe_ref("sig-BUY")

    @pytest.mark.asyncio
    async def test_cancellation_is_not_reported_as_an_execution_failure(self) -> None:
        emitters, service = recording_emitters()
        svc = _service(_success_gateway(), emitters.execution)

        with patch.object(
            ExecutionService,
            "_execute_impl",
            AsyncMock(side_effect=asyncio.CancelledError()),
        ):
            with pytest.raises(asyncio.CancelledError):
                await svc.execute(_req("BUY"))

        assert service.calls == []


class TestSafety:
    @pytest.mark.asyncio
    async def test_raising_telemetry_backend_never_alters_the_result(self) -> None:
        emitters, service = raising_emitters()

        def _result() -> ExecutionResult:
            return _fixed_result(status="filled", filled_amount=Decimal("5"))

        baseline = await _project_fresh(_result, None)
        returned = await _project_fresh(_result, emitters.execution)

        assert returned is not baseline
        assert returned == baseline == _result()
        assert service.calls == 1

    @pytest.mark.asyncio
    async def test_no_event_is_published_before_the_implementation_runs(self) -> None:
        """Zero pre-submit cost: the projection happens strictly afterwards."""
        emitters, service = recording_emitters()
        svc = _service(_success_gateway(), emitters.execution)
        seen: list[int] = []

        async def _impl(_self, _request):
            seen.append(len(service.calls))
            return _fixed_result()

        with patch.object(ExecutionService, "_execute_impl", _impl):
            await svc.execute(_req("BUY"))

        assert seen == [0]
        assert len(service.calls) == 1

    @pytest.mark.asyncio
    async def test_no_event_is_published_before_a_raising_implementation_runs(self) -> None:
        """The failure projection is also strictly post-attempt."""
        emitters, service = recording_emitters()
        svc = _service(_success_gateway(), emitters.execution)
        seen: list[int] = []

        async def _impl(_self, _request):
            seen.append(len(service.calls))
            raise RuntimeError("venue exploded")

        with patch.object(ExecutionService, "_execute_impl", _impl):
            with pytest.raises(RuntimeError):
                await svc.execute(_req("BUY"))

        assert seen == [0]
        assert len(service.calls) == 1

    @pytest.mark.asyncio
    async def test_service_without_telemetry_behaves_as_before(self) -> None:
        gw = _success_gateway()
        svc = _service(gw, None)

        result = await svc.execute(_req("BUY"))

        assert result.accepted is True
        gw.submit_marketable_order.assert_awaited()

    @pytest.mark.asyncio
    async def test_telemetry_defaults_to_none(self) -> None:
        svc = ExecutionService(
            _StubClob(),
            _FeeSvc(),
            _mock_order_repo(),
            config=_make_exec_config(),
            clob_gateway=_success_gateway(),
            market_data_service=_make_mds(),
        )

        assert svc._telemetry is None


def _rejecting_service(exc: BaseException, repo) -> ExecutionService:
    """Build a service whose venue submission raises ``exc``."""
    gateway = _success_gateway()
    gateway.submit_marketable_order = AsyncMock(side_effect=exc)
    return ExecutionService(
        _StubClob(),
        _FeeSvc(),
        repo,
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(),
    )


class TestVenueRejectionReason:
    """A terminal CLOB rejection must say *why*, not just name the class.

    ``redact_error_detail`` collapses a ``VenueInvalidOrderError`` to its class
    name, which erased the CLOB body the gateway had just preserved — the
    operator saw ``VenueInvalidOrderError`` with no cause.
    """

    @pytest.mark.asyncio
    async def test_invalid_order_reason_reaches_result_record_and_log(self) -> None:
        repo = _mock_order_repo()
        svc = _rejecting_service(
            VenueInvalidOrderError("Polymarket CCXT order rejected (InvalidOrder): invalid amount"),
            repo,
        )

        # The bot's logger tree sets ``propagate = False`` below the root, so
        # ``caplog`` never sees this record; capture the call itself instead.
        with patch.object(execution_module.logger, "warning") as warn:
            result = await svc.execute(_req("BUY"))

        assert result.accepted is False
        assert "invalid amount" in (result.error or "")

        record = repo.save_submission.await_args.args[0]
        assert "invalid amount" in str(record.response_payload.get("error"))
        assert "invalid amount" in str(record.response_payload.get("venue_reason"))
        assert "invalid amount" in str(record.last_error)

        rejections = [
            call for call in warn.call_args_list if "non-retryable CLOB rejection" in call.args[0]
        ]
        assert rejections and "invalid amount" in str(rejections[0].args[1])

    @pytest.mark.asyncio
    async def test_insufficient_balance_keeps_its_drift_marker_and_gains_the_reason(self) -> None:
        """The drift detector matches on the prefix; the reason rides along."""
        repo = _mock_order_repo()
        svc = _rejecting_service(
            VenueInsufficientBalanceError("Polymarket balance/allowance is insufficient"),
            repo,
        )

        result = await svc.execute(_req("BUY"))

        assert (result.error or "").startswith("insufficient_balance | ")
        assert "insufficient" in (result.error or "")

    @pytest.mark.asyncio
    async def test_auth_rejection_stays_opaque(self) -> None:
        """Auth failures keep the category-only redaction: bodies carry keys."""
        repo = _mock_order_repo()
        svc = _rejecting_service(VenueAuthError("POLY_API_KEY deadbeef is not authorized"), repo)

        result = await svc.execute(_req("BUY"))

        assert result.accepted is False
        assert "deadbeef" not in (result.error or "")
