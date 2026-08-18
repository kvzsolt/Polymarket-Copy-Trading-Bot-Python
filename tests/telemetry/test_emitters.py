"""Contract tests for the synchronous, exception-guarded telemetry emitters.

Every emitter method is a hot-path call site: it must be synchronous, must
never await, must never raise, and must return an explicit boolean so a
producer can assert delivery without inspecting hub internals.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from services.execution.types import ExecutionRequest, ExecutionResult
from services.metrics import METRIC_TELEMETRY_EMIT_FAILURE, MetricsRegistry
from services.telemetry import (
    AmbiguousEvent,
    AmbiguousOperation,
    DetectionCountersSnapshot,
    DetectionTelemetryEmitter,
    ExecutionTelemetryEmitter,
    FailureComponent,
    FailureEvent,
    FillEvent,
    FillOutcome,
    LeaderEvent,
    LeaderOutcome,
    OrderOutcome,
    ProtectiveExitEvent,
    ProtectiveOutcome,
    ReconciliationEvent,
    ReconciliationOutcome,
    RiskEvent,
    RiskOutcome,
    RiskTelemetryEmitter,
    RosterEvent,
    SkipEvent,
    SkipReason,
    TelemetryEmitters,
    TelemetryHub,
    TelemetryProducer,
    TelemetryService,
    TradeEvent,
    TradeOutcome,
    TradeSide,
    TradeTelemetryEmitter,
    WssEvent,
    WssOutcome,
    WssTelemetryEmitter,
    is_safe_ref,
    safe_ref,
    skip_reason_for_code,
)


LEADER_WALLET = "0xc4ac" + "0" * 34 + "9722"
TX_HASH = "0x" + "ab" * 32
CONDITION_ID = "0x" + "cd" * 32
TOKEN_ID = "7" * 77
UNSAFE_TRADE_KEY = "u|l|0xabc|BUY|tok"


class FixedClock:
    """Return a deterministic UTC instant."""

    def now(self) -> datetime:
        """Return the configured instant."""
        return datetime(2026, 8, 18, 12, 0, tzinfo=UTC)


class SequenceRandom:
    """Return deterministic opaque identifiers."""

    def __init__(self) -> None:
        self.value = 0

    def token_hex(self, nbytes: int) -> str:
        """Return a fixed-width hexadecimal token."""
        self.value += 1
        return f"{self.value:0{nbytes * 2}x}"


class StepClock:
    """Return a monotonically increasing float wall clock."""

    def __init__(self) -> None:
        self.value = 1_000.0

    def __call__(self) -> float:
        """Return the next deterministic tick."""
        self.value += 1.0
        return self.value


class RaisingService:
    """Fake telemetry service whose ``emit`` always fails."""

    def __init__(self, registry: MetricsRegistry) -> None:
        self.metrics_registry = registry
        self.calls = 0

    def emit(self, **_kwargs: object) -> object:
        """Fail exactly the way a degraded telemetry hub would."""
        self.calls += 1
        raise RuntimeError("telemetry backend unavailable")


async def _started_service(registry: MetricsRegistry | None = None) -> TelemetryService:
    resolved = registry if registry is not None else MetricsRegistry()
    hub = TelemetryHub(ingress_capacity=512, metrics=resolved)
    service = TelemetryService(
        hub=hub,
        metrics=resolved,
        clock=FixedClock(),
        rng=SequenceRandom(),
        history_capacity=512,
    )
    await service.start()
    return service


def _request(side: str = "BUY", signal_id: str | None = None) -> ExecutionRequest:
    return ExecutionRequest(
        user_id="user-1",
        leader_id=LEADER_WALLET,
        token_id=TOKEN_ID,
        side=side,
        amount=Decimal("5"),
        max_slippage_bps=150,
        reason="copy_trade",
        signal_id=signal_id,
        condition_id=CONDITION_ID,
    )


def _result(
    *,
    accepted: bool,
    status: str,
    order_id: str | None = "order-1",
    error: str | None = None,
    filled: str = "0",
) -> ExecutionResult:
    return ExecutionResult(
        accepted=accepted,
        order_id=order_id,
        status=status,
        requested_amount=Decimal("5"),
        filled_amount=Decimal(filled),
        avg_price=None,
        fees_paid=Decimal("0"),
        error=error,
    )


class _PipelineOutcome:
    """Structural stand-in for ``orchestrators.trade_pipeline.TradePipelineResult``."""

    def __init__(self, trade_key: str, status: str, reason: str | None) -> None:
        self.trade_key = trade_key
        self.status = status
        self.reason = reason
        self.order_id: str | None = None


class _Signal:
    """Structural stand-in for ``domain.models.TradeSignal``."""

    def __init__(self, side: str = "BUY", tx_hash: str = TX_HASH) -> None:
        self.side = side
        self.tx_hash = tx_hash


def _emit_calls(bundle: TelemetryEmitters) -> list[tuple[object, tuple[object, ...]]]:
    """Return every publishing emitter method with representative arguments."""
    return [
        (bundle.detection.signal_received, ("polling", LEADER_WALLET, TX_HASH)),
        (bundle.detection.signal_deduplicated, ("polling", LEADER_WALLET)),
        (bundle.detection.roster_updated, (2,)),
        (bundle.trade.pipeline_results, ("user-1", LEADER_WALLET, (), {})),
        (bundle.trade.pipeline_failed, ("user-1", LEADER_WALLET, RuntimeError("boom"))),
        (bundle.trade.reconciliation_run, (3,)),
        (
            bundle.execution.order_result,
            (_request(), _result(accepted=True, status="submitted_unconfirmed")),
        ),
        (bundle.execution.order_exception, (_request(), RuntimeError("boom"))),
        (bundle.execution.fill_observed, ("order-1", Decimal("1.5"))),
        (bundle.execution.reconciliation_drift, ("order-1",)),
        (bundle.execution.ambiguous_released, ("order-1",)),
        (bundle.risk.exit_triggered, (CONDITION_ID, TOKEN_ID, "stop_loss")),
        (bundle.risk.exit_filled, (CONDITION_ID, TOKEN_ID, "stop_loss")),
        (bundle.risk.exit_not_filled, (CONDITION_ID, TOKEN_ID, "stop_loss")),
        (bundle.risk.risk_rejected, ("task-1",)),
        (bundle.wss.connected, ("market",)),
        (bundle.wss.reconnected, ("market",)),
        (bundle.wss.disconnected, ("market",)),
        (bundle.wss.stalled, ("market",)),
    ]


def test_every_emitter_method_is_synchronous() -> None:
    """No emitter method may be a coroutine function on the hot path."""
    bundle = TelemetryEmitters.disabled()
    emitters = (bundle.detection, bundle.trade, bundle.execution, bundle.risk, bundle.wss)

    for emitter in emitters:
        for name in dir(emitter):
            if name.startswith("_"):
                continue
            attribute = getattr(type(emitter), name, None)
            if isinstance(attribute, property):
                continue
            member = getattr(emitter, name)
            if not callable(member):
                continue
            assert inspect.iscoroutinefunction(member) is False, name


def test_disabled_bundle_returns_false_without_raising() -> None:
    """A null-object bundle is a total no-op that still reports delivery."""
    bundle = TelemetryEmitters.disabled()

    assert bundle.enabled is False
    for method, args in _emit_calls(bundle):
        outcome = method(*args)
        assert outcome is False, method.__name__

    assert bundle.detection.attach_connection_probe(lambda: True) is True
    assert bundle.detection.attach_connection_probe(None) is False


async def test_enabled_bundle_reports_accepted_publications() -> None:
    """Every emitter method returns ``True`` once the hub accepts the event."""
    service = await _started_service()
    bundle = TelemetryEmitters.build(service)

    assert bundle.enabled is True
    for method, args in _emit_calls(bundle):
        assert method(*args) is True, method.__name__

    await service.close()


async def test_raising_service_is_contained_and_counted() -> None:
    """A failing telemetry backend never propagates into a producer."""
    registry = MetricsRegistry()
    service = RaisingService(registry)
    emitter = DetectionTelemetryEmitter(service)  # type: ignore[arg-type]

    assert emitter.signal_received("polling", LEADER_WALLET, TX_HASH) is False
    assert emitter.signal_deduplicated("polling", LEADER_WALLET) is False

    counters, _ = registry.snapshot()
    assert counters[f'{METRIC_TELEMETRY_EMIT_FAILURE}{{producer="detection"}}'] == 2
    assert service.calls == 2


def test_safe_ref_passes_safe_tokens_and_hashes_unsafe_values_deterministically() -> None:
    """Unsafe identifiers become a stable, opaque, contract-safe digest."""
    assert safe_ref(LEADER_WALLET) == LEADER_WALLET
    assert safe_ref(TX_HASH) == TX_HASH

    expected = "h:" + hashlib.blake2b(UNSAFE_TRADE_KEY.encode(), digest_size=16).hexdigest()
    assert safe_ref(UNSAFE_TRADE_KEY) == expected
    assert safe_ref(UNSAFE_TRADE_KEY) == safe_ref(UNSAFE_TRADE_KEY)
    assert is_safe_ref(UNSAFE_TRADE_KEY) is False
    assert is_safe_ref(safe_ref(UNSAFE_TRADE_KEY)) is True


async def test_unsafe_trade_key_is_hashed_and_the_event_is_accepted() -> None:
    """A skip carrying a raw trade key still produces a valid bounded event."""
    service = await _started_service()
    emitter = TradeTelemetryEmitter(service)

    results = [_PipelineOutcome(UNSAFE_TRADE_KEY, "skipped", "market_data_stale")]
    signals = {UNSAFE_TRADE_KEY: _Signal()}

    assert emitter.pipeline_results("user-1", LEADER_WALLET, results, signals) is True

    [event] = service.history_snapshot()
    assert isinstance(event.payload, SkipEvent)
    assert event.payload.task_ref == safe_ref(UNSAFE_TRADE_KEY)
    assert event.payload.reason is SkipReason.STALE_MARKET_DATA
    assert event.correlation_id == TX_HASH
    assert event.producer is TelemetryProducer.WORKER

    await service.close()


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("market_data_stale", SkipReason.STALE_MARKET_DATA),
        ("market_data_timeout", SkipReason.STALE_MARKET_DATA),
        ("market_resolved", SkipReason.MARKET_NOT_ALLOWED),
        ("entry_policy_unavailable", SkipReason.RISK_REJECTED),
        ("market_risk_quarantined", SkipReason.RISK_REJECTED),
        ("entry_denied_market_block", SkipReason.RISK_REJECTED),
        ("insufficient_balance", SkipReason.INSUFFICIENT_BALANCE),
        ("insufficient_cash_with_fees", SkipReason.INSUFFICIENT_BALANCE),
        ("book_no_liquidity", SkipReason.CAPACITY),
        ("book_spread_wide", SkipReason.CAPACITY),
        ("invalid_price", SkipReason.PRICE_LIMIT),
        ("sell_size_zero", SkipReason.BELOW_MINIMUM_SIZE),
        ("below_minimum_size", SkipReason.BELOW_MINIMUM_SIZE),
        ("already_processed", SkipReason.DUPLICATE),
        ("duplicate_ignored", SkipReason.DUPLICATE),
        ("tier_watchlist", SkipReason.OTHER),
        (None, SkipReason.OTHER),
        ("", SkipReason.OTHER),
    ],
)
def test_skip_code_maps_to_bounded_skip_reasons(code: str | None, expected: SkipReason) -> None:
    """Every pipeline skip code lands on a bounded contract reason."""
    assert skip_reason_for_code(code) is expected


async def test_paper_tier_results_emit_the_paper_only_skip_reason() -> None:
    """A paper-follow copy is a bounded skip, never a venue outcome."""
    service = await _started_service()
    emitter = TradeTelemetryEmitter(service)

    results = [
        _PipelineOutcome("key-paper", "paper", "tier_paper_follow"),
        _PipelineOutcome("key-executed", "executed", None),
        _PipelineOutcome("key-failed", "failed", "venue said no"),
        _PipelineOutcome("key-submitted", "submitted_unconfirmed", "accepted_no_fill_confirmed"),
    ]

    assert emitter.pipeline_results("user-1", LEADER_WALLET, results, {}) is True

    payloads = [event.payload for event in service.history_snapshot()]
    assert len(payloads) == 1
    assert isinstance(payloads[0], SkipEvent)
    assert payloads[0].reason is SkipReason.PAPER_ONLY

    await service.close()


async def test_pipeline_failure_emits_a_bounded_worker_failure_without_exception_text() -> None:
    """A raised pipeline error becomes a bounded code, never raw text."""
    service = await _started_service()
    emitter = TradeTelemetryEmitter(service)

    assert emitter.pipeline_failed("user-1", LEADER_WALLET, ValueError("secret 0xdeadbeef")) is True

    [event] = service.history_snapshot()
    assert isinstance(event.payload, FailureEvent)
    assert event.payload.component is FailureComponent.WORKER
    assert event.payload.code == "value_error"
    assert event.payload.retryable is False
    assert "secret" not in event.payload.code

    await service.close()


async def test_reconciliation_run_reports_the_affected_count() -> None:
    """A reconciliation sweep publishes its bounded affected count."""
    service = await _started_service()
    emitter = TradeTelemetryEmitter(service)

    assert emitter.reconciliation_run(4) is True

    [event] = service.history_snapshot()
    assert isinstance(event.payload, ReconciliationEvent)
    assert event.payload.outcome is ReconciliationOutcome.RUN
    assert event.payload.affected_count == 4

    await service.close()


async def test_execution_result_status_maps_to_bounded_order_outcomes() -> None:
    """Each ``ExecutionResult`` shape maps to exactly one bounded payload."""
    service = await _started_service()
    emitter = ExecutionTelemetryEmitter(service)

    assert (
        emitter.order_result(_request(), _result(accepted=True, status="submitted_unconfirmed"))
        is True
    )
    assert (
        emitter.order_result(_request(), _result(accepted=False, status="rejected", order_id=None))
        is True
    )
    assert (
        emitter.order_result(_request(), _result(accepted=False, status="expired", order_id=None))
        is True
    )
    assert (
        emitter.order_result(
            _request(),
            _result(
                accepted=True,
                status="submitted_unconfirmed",
                error="connection reset after POST",
            ),
        )
        is True
    )

    submitted, rejected, expired, ambiguous = (
        event.payload for event in service.history_snapshot()
    )

    assert isinstance(submitted, TradeEvent)
    assert submitted.order_outcome is OrderOutcome.SUBMITTED
    assert submitted.side is TradeSide.BUY
    assert submitted.outcome is TradeOutcome.EXECUTED

    assert isinstance(rejected, FailureEvent)
    assert rejected.component is FailureComponent.EXECUTION
    assert rejected.order_outcome is OrderOutcome.REJECTED
    assert rejected.retryable is False

    assert isinstance(expired, FailureEvent)
    assert expired.order_outcome is OrderOutcome.NOT_FILLABLE

    assert isinstance(ambiguous, AmbiguousEvent)
    assert ambiguous.operation is AmbiguousOperation.ORDER_SUBMISSION
    assert ambiguous.reconciliation_required is True

    await service.close()


async def test_execution_emitter_publishes_fill_drift_and_ambiguous_release() -> None:
    """Post-submission execution evidence stays bounded and deduplicable."""
    service = await _started_service()
    emitter = ExecutionTelemetryEmitter(service)

    assert emitter.fill_observed("order-1", Decimal("2.5")) is True
    assert emitter.reconciliation_drift("order-1") is True
    assert emitter.ambiguous_released("order-1") is True

    fill, drift, released = (event.payload for event in service.history_snapshot())

    assert isinstance(fill, FillEvent)
    assert fill.outcome is FillOutcome.OBSERVED
    assert fill.order_ref == "order-1"
    assert fill.fill_ref == safe_ref("order-1:2.5")

    assert isinstance(drift, ReconciliationEvent)
    assert drift.outcome is ReconciliationOutcome.DRIFT
    assert drift.affected_count == 1

    assert isinstance(released, AmbiguousEvent)
    assert released.reconciliation_required is False

    await service.close()


async def test_risk_emitter_publishes_protective_and_rejection_payloads() -> None:
    """Protective exits and risk rejections use the bounded risk contract."""
    service = await _started_service()
    emitter = RiskTelemetryEmitter(service)

    assert emitter.exit_triggered(CONDITION_ID, TOKEN_ID, "stop_loss") is True
    assert emitter.exit_filled(CONDITION_ID, TOKEN_ID, "stop_loss") is True
    assert emitter.exit_not_filled(CONDITION_ID, TOKEN_ID, "stop_loss") is True
    assert emitter.risk_rejected(UNSAFE_TRADE_KEY) is True

    triggered, filled, not_filled, rejected = (
        event.payload for event in service.history_snapshot()
    )

    assert isinstance(triggered, ProtectiveExitEvent)
    assert triggered.outcome is ProtectiveOutcome.TRIGGERED
    assert isinstance(filled, ProtectiveExitEvent)
    assert filled.outcome is ProtectiveOutcome.FILLED
    assert triggered.task_ref == filled.task_ref

    assert isinstance(not_filled, FailureEvent)
    assert not_filled.component is FailureComponent.PROTECTIVE_EXIT
    assert not_filled.code == "exit_not_filled_stop_loss"
    assert not_filled.order_outcome is OrderOutcome.NOT_FILLABLE

    assert isinstance(rejected, RiskEvent)
    assert rejected.outcome is RiskOutcome.REJECTED
    assert rejected.task_ref == safe_ref(UNSAFE_TRADE_KEY)

    await service.close()


async def test_wss_emitter_publishes_bounded_stream_lifecycle() -> None:
    """WSS lifecycle events stay inside the bounded stream vocabulary."""
    service = await _started_service()
    emitter = WssTelemetryEmitter(service)

    assert emitter.connected("market") is True
    assert emitter.reconnected("user") is True
    assert emitter.disconnected("leader") is True
    assert emitter.stalled("market") is True
    assert emitter.connected("not-a-stream") is False

    payloads = [event.payload for event in service.history_snapshot()]
    assert all(isinstance(payload, WssEvent) for payload in payloads)
    assert [payload.outcome for payload in payloads] == [
        WssOutcome.CONNECTED,
        WssOutcome.RECONNECTED,
        WssOutcome.DISCONNECTED,
        WssOutcome.STALLED,
    ]

    await service.close()


async def test_detection_emitter_publishes_leader_and_roster_events() -> None:
    """Detection publishes exactly one bounded event per signal."""
    service = await _started_service()
    emitter = DetectionTelemetryEmitter(service)

    assert emitter.signal_received("on_chain_pending", LEADER_WALLET, TX_HASH) is True
    assert emitter.signal_deduplicated("on_chain_pending", LEADER_WALLET) is True
    assert emitter.roster_updated(2) is True

    received, deduped, roster = (event.payload for event in service.history_snapshot())

    assert isinstance(received, LeaderEvent)
    assert received.outcome is LeaderOutcome.RECEIVED
    assert received.leader_ref == LEADER_WALLET
    assert received.source == "on_chain_pending"
    assert isinstance(deduped, LeaderEvent)
    assert deduped.outcome is LeaderOutcome.DEDUP
    assert isinstance(roster, RosterEvent)
    assert roster.active_count == 2

    events = service.history_snapshot()
    assert events[0].correlation_id == TX_HASH
    assert all(event.producer is TelemetryProducer.DETECTION for event in events)

    await service.close()


def test_detection_snapshot_counts_sources_and_bounds_the_leader_map() -> None:
    """The snapshot stays bounded and never grows with leader cardinality."""
    clock = StepClock()
    emitter = DetectionTelemetryEmitter(None, max_leaders=256, clock=clock)

    emitter.signal_received("polling", LEADER_WALLET, TX_HASH)
    emitter.signal_received("polling", LEADER_WALLET, TX_HASH)
    emitter.signal_deduplicated("polling", LEADER_WALLET)
    emitter.signal_received("on_chain_pending", LEADER_WALLET, TX_HASH)

    snapshot = emitter.snapshot()
    assert isinstance(snapshot, DetectionCountersSnapshot)
    assert snapshot.sources["polling"].signals_total == 2
    assert snapshot.sources["polling"].deduplicated_total == 1
    assert snapshot.sources["on_chain_pending"].signals_total == 1
    # ``last_signal_at`` tracks the newest observation of a source, accepted or
    # deduplicated, so an all-dedup window still proves detection is alive.
    assert snapshot.sources["polling"].last_signal_at == 1003.0
    assert snapshot.sources["on_chain_pending"].last_signal_at == 1004.0
    assert snapshot.leaders[LEADER_WALLET].signals_total == 3
    assert snapshot.leaders[LEADER_WALLET].deduplicated_total == 1
    assert snapshot.leader_ws_connected is None
    assert snapshot.active_leader_count is None

    for index in range(300):
        emitter.signal_received("polling", f"0x{index:040x}", TX_HASH)

    bounded = emitter.snapshot()
    assert len(bounded.leaders) == 256
    with pytest.raises(TypeError):
        bounded.leaders["injected"] = None  # type: ignore[index]


def test_detection_snapshot_reports_the_attached_connection_probe() -> None:
    """The late-bound WSS probe is read at snapshot time and never trusted."""
    emitter = DetectionTelemetryEmitter(None)

    assert emitter.snapshot().leader_ws_connected is None

    emitter.attach_connection_probe(lambda: True)
    assert emitter.snapshot().leader_ws_connected is True

    emitter.attach_connection_probe(lambda: None)
    assert emitter.snapshot().leader_ws_connected is None

    def _broken_probe() -> bool:
        raise RuntimeError("probe unavailable")

    emitter.attach_connection_probe(_broken_probe)
    assert emitter.snapshot().leader_ws_connected is None

    emitter.roster_updated(2)
    assert emitter.snapshot().active_leader_count == 2


async def test_foreign_thread_emit_is_scheduled_on_the_owning_loop() -> None:
    """A call from a non-loop thread schedules instead of touching the hub."""
    service = await _started_service()
    emitter = DetectionTelemetryEmitter(service)

    scheduled = await asyncio.to_thread(
        emitter.signal_received,
        "polling",
        LEADER_WALLET,
        TX_HASH,
    )
    assert scheduled is True

    for _ in range(50):
        if service.history_snapshot():
            break
        await asyncio.sleep(0)

    [event] = service.history_snapshot()
    assert isinstance(event.payload, LeaderEvent)
    assert emitter.snapshot().sources["polling"].signals_total == 1

    await service.close()


async def test_bundle_shares_one_service_and_exposes_every_producer() -> None:
    """The bundle is frozen and binds one producer identity per emitter."""
    service = await _started_service()
    bundle = TelemetryEmitters.build(service)

    assert isinstance(bundle.detection, DetectionTelemetryEmitter)
    assert isinstance(bundle.trade, TradeTelemetryEmitter)
    assert isinstance(bundle.execution, ExecutionTelemetryEmitter)
    assert isinstance(bundle.risk, RiskTelemetryEmitter)
    assert isinstance(bundle.wss, WssTelemetryEmitter)
    assert {
        bundle.detection.producer,
        bundle.trade.producer,
        bundle.execution.producer,
        bundle.risk.producer,
        bundle.wss.producer,
    } == {
        TelemetryProducer.DETECTION,
        TelemetryProducer.WORKER,
        TelemetryProducer.EXECUTION,
        TelemetryProducer.RISK,
        TelemetryProducer.WSS,
    }

    with pytest.raises(FrozenInstanceError):
        bundle.detection = None  # type: ignore[misc]

    await service.close()
