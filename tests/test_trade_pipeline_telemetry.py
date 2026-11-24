"""Post-submission telemetry projection for the copy pipeline.

``execute_trades`` is a thin wrapper around ``_execute_trades_impl``: it does
zero work before the implementation runs, projects the returned
``TradePipelineResult`` rows onto bounded operator events afterwards, and
reports an escaped exception as a worker failure before re-raising it.

The pipeline owns *skips* only. Venue outcomes belong to the execution-service
wrapper, so an executed row must produce no trade event here — otherwise a
single copy would be counted twice.

The unit layer drives the wrapper with a patched implementation (deterministic,
no container). The integration layer at the bottom replays the real pipeline
through the same Postgres harness the market-data-authority suite uses.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from pmcopy.orchestrators.pipeline.trade_pipeline import (
    TradePipelineResult,
    build_trade_key,
    execute_trades,
    reconcile_submitted_unconfirmed,
)
from pmcopy.services.telemetry import (
    FailureComponent,
    FailureEvent,
    ReconciliationEvent,
    ReconciliationOutcome,
    SkipEvent,
    SkipReason,
    TelemetryEmitters,
    TelemetryProducer,
    TradeEvent,
    safe_ref,
)
from tests.telemetry_helpers import raising_emitters, recording_emitters


LEADER = "0x" + "dd" * 20
TX_HASH = "0x" + "ee" * 32
TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"
USER_ID = "u1"


def _raw(tx_hash: str = TX_HASH, side: str = "BUY") -> dict[str, object]:
    return {
        "transactionHash": tx_hash,
        "side": side,
        "asset": TOKEN_ID,
        "conditionId": "0x" + "cc" * 32,
        "size": 10,
        "price": 0.5,
        "usdcSize": 5,
        "timestamp": 1_700_000_000,
    }


def _context(telemetry: TelemetryEmitters) -> SimpleNamespace:
    return SimpleNamespace(user_id=USER_ID, telemetry=telemetry)


def _row(raw: dict[str, object], status: str, reason: str | None) -> TradePipelineResult:
    return TradePipelineResult(
        trade_key=build_trade_key(USER_ID, LEADER, raw),
        status=status,
        reason=reason,
        order_id=None,
    )


async def _run(context, rows, *, signals):
    impl = AsyncMock(return_value=rows)
    with patch("pmcopy.orchestrators.pipeline.trade_pipeline._execute_trades_impl", impl):
        return await execute_trades(context, LEADER, signals, 1_700_000_000.0)


async def _run_fresh(context, rows_factory, *, signals):
    """Drive the wrapper with a *newly built* result list on every call.

    ``_run`` hands the same list object back each time, which would make an
    equality assertion between two runs compare an object with itself. This
    variant builds independent-but-equal rows per run so the comparison is a
    real value check.
    """

    async def _impl(_context, _leader, _signals, _now):
        return rows_factory()

    with patch("pmcopy.orchestrators.pipeline.trade_pipeline._execute_trades_impl", _impl):
        return await execute_trades(context, LEADER, signals, 1_700_000_000.0)


class TestPipelineProjection:
    @pytest.mark.asyncio
    async def test_already_processed_row_reports_a_duplicate_skip(self) -> None:
        emitters, service = recording_emitters()
        raw = _raw()
        rows = [_row(raw, "skipped", "already_processed")]

        results = await _run(_context(emitters), rows, signals=[raw])

        assert results == rows
        [event] = service.payloads(SkipEvent)
        assert event.reason is SkipReason.DUPLICATE
        assert event.task_ref == safe_ref(build_trade_key(USER_ID, LEADER, raw))
        assert service.calls[0].producer is TelemetryProducer.WORKER
        assert service.calls[0].correlation_id == safe_ref(TX_HASH)

    @pytest.mark.asyncio
    async def test_paper_tier_row_reports_a_paper_only_skip(self) -> None:
        emitters, service = recording_emitters()
        raw = _raw()

        await _run(_context(emitters), [_row(raw, "paper", "tier_paper_follow")], signals=[raw])

        [event] = service.payloads(SkipEvent)
        assert event.reason is SkipReason.PAPER_ONLY

    @pytest.mark.asyncio
    async def test_executed_row_produces_no_pipeline_trade_event(self) -> None:
        """Venue outcomes belong to the execution wrapper — never counted twice."""
        emitters, service = recording_emitters()
        raw = _raw()

        await _run(_context(emitters), [_row(raw, "executed", None)], signals=[raw])

        assert service.payloads(TradeEvent) == []
        assert service.calls == []

    @pytest.mark.asyncio
    async def test_correlation_is_recovered_for_every_transaction_hash_alias(self) -> None:
        emitters, service = recording_emitters()
        raw = {**_raw(), "transactionHash": None, "tx_hash": TX_HASH}
        rows = [_row(raw, "skipped", "already_processed")]

        await _run(_context(emitters), rows, signals=[raw])

        assert service.calls[0].correlation_id == safe_ref(TX_HASH)

    @pytest.mark.asyncio
    async def test_context_without_a_telemetry_field_still_returns_results(self) -> None:
        """Older contexts (and non-``TradingContext`` fakes) must keep working."""
        raw = _raw()
        rows = [_row(raw, "skipped", "already_processed")]

        results = await _run(SimpleNamespace(user_id=USER_ID), rows, signals=[raw])

        assert results == rows


class TestPipelineFailure:
    @pytest.mark.asyncio
    async def test_escaped_exception_is_reported_and_re_raised(self) -> None:
        emitters, service = recording_emitters()
        impl = AsyncMock(side_effect=ValueError("boom"))

        with patch("pmcopy.orchestrators.pipeline.trade_pipeline._execute_trades_impl", impl):
            with pytest.raises(ValueError, match="boom"):
                await execute_trades(_context(emitters), LEADER, [_raw()], 1.0)

        [event] = service.payloads(FailureEvent)
        assert event.component is FailureComponent.WORKER
        assert event.retryable is False
        # Only the exception class reaches the stream — never its message.
        assert "boom" not in event.code
        assert service.calls[0].correlation_id == safe_ref(LEADER)

    @pytest.mark.asyncio
    async def test_cancellation_is_not_reported_as_a_worker_failure(self) -> None:
        """Cancellation is a shutdown signal, not a pipeline defect."""
        emitters, service = recording_emitters()
        impl = AsyncMock(side_effect=asyncio.CancelledError())

        with patch("pmcopy.orchestrators.pipeline.trade_pipeline._execute_trades_impl", impl):
            with pytest.raises(asyncio.CancelledError):
                await execute_trades(_context(emitters), LEADER, [_raw()], 1.0)

        assert service.calls == []


class TestLatencyAndSafety:
    @pytest.mark.asyncio
    async def test_raising_telemetry_backend_leaves_results_byte_identical(self) -> None:
        """The latency/safety proof: telemetry cannot change what the copy did."""
        raw = _raw()

        def _rows() -> list[TradePipelineResult]:
            # Fresh objects per run so the comparison below is a value check,
            # not a list compared with itself.
            return [
                _row(raw, "skipped", "already_processed"),
                _row(_raw(side="SELL"), "paper", "tier_paper_follow"),
                _row(_raw(tx_hash="0x" + "ff" * 32), "executed", None),
            ]

        baseline = await _run_fresh(_context(TelemetryEmitters.disabled()), _rows, signals=[raw])

        emitters, service = raising_emitters()
        with_telemetry = await _run_fresh(_context(emitters), _rows, signals=[raw])

        assert with_telemetry is not baseline
        assert all(a is not b for a, b in zip(with_telemetry, baseline, strict=True))
        assert with_telemetry == baseline
        assert [(r.trade_key, r.status, r.reason, r.order_id) for r in with_telemetry] == [
            (r.trade_key, r.status, r.reason, r.order_id) for r in baseline
        ]
        # The backend really was exercised and really did fail.
        assert service.calls == 2

    @pytest.mark.asyncio
    async def test_no_event_is_published_before_the_implementation_runs(self) -> None:
        """Zero pre-submit cost: the projection happens strictly afterwards."""
        emitters, service = recording_emitters()
        raw = _raw()
        seen: list[int] = []

        async def _impl(_context, _leader, _signals, _now):
            seen.append(len(service.calls))
            return [_row(raw, "skipped", "already_processed")]

        with patch("pmcopy.orchestrators.pipeline.trade_pipeline._execute_trades_impl", _impl):
            await execute_trades(_context(emitters), LEADER, [raw], 1.0)

        assert seen == [0]
        assert len(service.calls) == 1


class TestReconciliationSweep:
    @pytest.mark.asyncio
    async def test_completed_sweep_is_reported(self) -> None:
        from pmcopy.domain.position_key import PositionKey
        from pmcopy.services.positions.position_coordinator import PositionCoordinator

        emitters, service = recording_emitters()
        record = {
            "user_id": USER_ID,
            "condition_id": "c1",
            "asset": "1234567890",
            "order_id": "o1",
        }
        context = SimpleNamespace(
            user_id=USER_ID,
            execution_service=object(),
            orders_repo=SimpleNamespace(
                get_unconfirmed_buys=AsyncMock(return_value=[record]),
                get_unconfirmed_sells=AsyncMock(return_value=[]),
            ),
            position_coordinator=PositionCoordinator(),
            config=None,
            extra={},
            telemetry=emitters,
        )
        assert PositionKey.from_reconciliation_record(record)

        with patch("pmcopy.orchestrators.pipeline.trade_pipeline._reconcile_one", AsyncMock(return_value=True)):
            reconciled = await reconcile_submitted_unconfirmed(context)

        assert reconciled == 1
        [event] = service.payloads(ReconciliationEvent)
        assert event.outcome is ReconciliationOutcome.RUN
        assert event.affected_count == 1

    @pytest.mark.asyncio
    async def test_empty_sweep_reports_nothing(self) -> None:
        from pmcopy.services.positions.position_coordinator import PositionCoordinator

        emitters, service = recording_emitters()
        context = SimpleNamespace(
            user_id=USER_ID,
            execution_service=object(),
            orders_repo=SimpleNamespace(
                get_unconfirmed_buys=AsyncMock(return_value=[]),
                get_unconfirmed_sells=AsyncMock(return_value=[]),
            ),
            position_coordinator=PositionCoordinator(),
            config=None,
            extra={},
            telemetry=emitters,
        )

        assert await reconcile_submitted_unconfirmed(context) == 0
        assert service.calls == []


# ---------------------------------------------------------------------------
# Integration: the real pipeline through the market-data-authority harness.
# Skipped when Docker is unavailable, same policy as the harness it reuses.
# ---------------------------------------------------------------------------

# ``tests/conftest.py`` keeps every Testcontainers-backed test out of the
# portable default tier. This module reaches Docker only transitively (through
# the imported ``_pg_pool``), which the file-source heuristic cannot see, so the
# marker is applied by hand to honour that policy.
_integration = pytest.mark.integration


@pytest.fixture
def _lower_decision_gates(monkeypatch):
    """Mirror the harness module's own autouse gates (they do not travel)."""
    monkeypatch.setenv("COPY_DECISION_MIN_SCORE", "0.01")
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "999999")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_RATIO", "1.0")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_RATIO", "1.0")
    monkeypatch.setenv("COPY_MIN_SHARES", "0.001")
    monkeypatch.setenv("COPY_MIN_NOTIONAL_USDC", "0.01")
    monkeypatch.setenv("BUY_MIN_AMOUNT", "0.01")
    monkeypatch.setenv("MAX_POSITION_PCT", "0")
    monkeypatch.setenv("ENABLE_BALANCE_TIERED_POLICY", "false")
    monkeypatch.setenv("MARKET_DATA_SUBSCRIBE_TIMEOUT_SECONDS", "0.4")


@pytest.fixture
def _mock_balance(monkeypatch):
    async def _fake_balance(_addr):
        return 10000.0

    monkeypatch.setattr("pmcopy.orchestrators.pipeline.trade_pipeline.get_my_balance", _fake_balance)


@_integration
@pytest.mark.asyncio
async def test_real_pipeline_reports_the_duplicate_and_no_trade_event(
    tmp_path,
    _lower_decision_gates,
    _mock_balance,
) -> None:
    """First run executes (no pipeline trade event); the replay is a duplicate."""
    # Imported lazily: the harness module pulls in the Postgres repo stack and
    # is only reachable on the Docker tier, so the default tier never pays for
    # it and never risks an import-time failure on a container-less machine.
    from tests.test_trade_pipeline_market_data_authority import (
        _exec_result_filled,
        _make_bundle,
        _make_context,
        _pg_pool,
        _signal,
        _FakeMDS,
        _RecordingDecisionService,
    )

    async with _pg_pool() as pool:
        emitters, service = recording_emitters()
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result_filled()
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)

        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
            decision_svc=_RecordingDecisionService(),
        )
        ctx.telemetry = emitters

        raw = _signal()
        first = await execute_trades(ctx, "leader1", [raw], time.time())
        assert [row.status for row in first] == ["executed"]
        assert service.calls == [], "the pipeline must not report venue outcomes"

        second = await execute_trades(ctx, "leader1", [raw], time.time())

        assert [row.status for row in second] == ["skipped"]
        assert [row.reason for row in second] == ["already_processed"]
        [event] = service.payloads(SkipEvent)
        assert event.reason is SkipReason.DUPLICATE
        assert event.task_ref == safe_ref(build_trade_key(ctx.user_id, "leader1", raw))
        assert service.payloads(TradeEvent) == []
