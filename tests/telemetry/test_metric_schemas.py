"""Characterization tests for low-cardinality telemetry metric schemas."""

from __future__ import annotations

import math

import pytest

from pmcopy.services.metrics import (
    ActiveEntity,
    FalconDiscoveryState,
    GAUGE_TELEMETRY_ACTIVE_ENTITIES,
    GAUGE_TELEMETRY_FALCON_DISCOVERY_STATE,
    GAUGE_TELEMETRY_QUEUE_DEPTH,
    GAUGE_TELEMETRY_QUEUE_UTILIZATION,
    GAUGE_TELEMETRY_RECONCILIATION_AGE,
    METRIC_TELEMETRY_COPY_OUTCOME,
    METRIC_TELEMETRY_LATENCY,
    METRIC_TELEMETRY_LEADER_QUEUE_OVERFLOW,
    METRIC_TELEMETRY_LEADER_SIGNAL,
    METRIC_TELEMETRY_ORDER_OUTCOME,
    TELEMETRY_COPY_OUTCOME_VALUES,
    TELEMETRY_LATENCY_BUCKETS,
    TELEMETRY_METRIC_SCHEMAS,
    LatencyPhase,
    MetricKind,
    MetricsRegistry,
    TelemetryQueue,
)
from pmcopy.services.telemetry import (
    AmbiguousEvent,
    AmbiguousOperation,
    DiscoveryEvent,
    DiscoveryOutcome,
    FailureComponent,
    FailureEvent,
    FillEvent,
    FillOutcome,
    LeaderEvent,
    LeaderOutcome,
    LockEvent,
    LockOutcome,
    OrderOutcome,
    ProtectiveExitEvent,
    ProtectiveOutcome,
    ReconciliationEvent,
    ReconciliationOutcome,
    SkipEvent,
    SkipReason,
    TelemetryHub,
    TelemetryProducer,
    TelemetryService,
    TradeEvent,
    TradeOutcome,
    TradeSide,
    WssEvent,
    WssOutcome,
)


EXPECTED_LATENCY_PHASES = {
    "source_detection_to_normalized",
    "normalized_to_worker_start",
    "leader_queue_wait",
    "copy_decision",
    "sizing",
    "risk",
    "reservation",
    "database_acquisition",
    "metadata_cache_miss",
    "market_reconnect_recovery",
    "order_construction",
    "venue_submission",
    "submission_to_first_ack",
    "submission_to_first_fill_evidence",
    "submission_to_terminal_reconciliation",
    "protective_exit_decision",
    "falcon_discovery_cycle",
}


def test_registered_schema_covers_exact_latency_phases_and_operator_families() -> None:
    schemas = {schema.name: schema for schema in TELEMETRY_METRIC_SCHEMAS}
    latency = schemas[METRIC_TELEMETRY_LATENCY]

    assert latency.kind is MetricKind.HISTOGRAM
    assert latency.buckets == TELEMETRY_LATENCY_BUCKETS
    assert set(LatencyPhase) == EXPECTED_LATENCY_PHASES
    assert dict(latency.allowed_label_values)["phase"] == tuple(LatencyPhase)

    assert {
        "telemetry_leader_signal_total",
        "telemetry_candidates_evaluated_total",
        "telemetry_copy_outcome_total",
        "telemetry_order_outcome_total",
        "telemetry_fill_total",
        "telemetry_protective_exit_total",
        "telemetry_reentry_lock_total",
        "telemetry_falcon_total",
        "telemetry_wss_reconnect_total",
        "telemetry_leader_queue_overflow_total",
        "telemetry_queue_overflow_total",
        "telemetry_reconciliation_total",
        "telemetry_active_entities",
        "telemetry_queue_depth",
        "telemetry_queue_utilization_ratio",
        "telemetry_subscriptions",
        "telemetry_cache_entries",
        "telemetry_wss_age_seconds",
        "telemetry_reconciliation_age_seconds",
        "telemetry_falcon_discovery_state",
    } <= set(schemas)
    assert {"received", "dedup", "promoted", "retracted"} <= set(
        dict(schemas[METRIC_TELEMETRY_LEADER_SIGNAL].allowed_label_values)["outcome"]
    )
    # The copy-outcome label set is exactly ``TradeOutcome`` minus the
    # candidate outcome, which has its own counter. Pinning it to the enum
    # keeps a new outcome from silently escaping the bounded label contract.
    copy_outcomes = {
        outcome.value for outcome in TradeOutcome if outcome is not TradeOutcome.CANDIDATE_EVALUATED
    }
    schema_outcomes = set(
        dict(schemas[METRIC_TELEMETRY_COPY_OUTCOME].allowed_label_values)["outcome"]
    )
    assert schema_outcomes == copy_outcomes
    assert set(TELEMETRY_COPY_OUTCOME_VALUES) == copy_outcomes
    assert {"submitted", "canceled", "rejected", "not_fillable", "ambiguous"} == set(
        dict(schemas[METRIC_TELEMETRY_ORDER_OUTCOME].allowed_label_values)["outcome"]
    )


def test_registered_metrics_reject_high_cardinality_or_unknown_label_values() -> None:
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)

    with pytest.raises(ValueError, match="label names"):
        registry.inc(
            METRIC_TELEMETRY_COPY_OUTCOME,
            labels={"outcome": "executed", "reason": "none", "leader_id": "0xabc"},
        )
    with pytest.raises(ValueError, match="bounded value"):
        registry.inc(
            METRIC_TELEMETRY_COPY_OUTCOME,
            labels={"outcome": "skipped", "reason": "raw-user-controlled-reason"},
        )


def test_histogram_exports_count_sum_cumulative_buckets_and_percentiles() -> None:
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS, sample_capacity=8)
    labels = {"phase": LatencyPhase.COPY_DECISION.value}
    for value in (1.0, 2.0, 3.0, 4.0, 5.0):
        registry.observe(
            METRIC_TELEMETRY_LATENCY,
            value,
            buckets=TELEMETRY_LATENCY_BUCKETS,
            labels=labels,
        )

    rendered = registry.render_prometheus()
    assert 'telemetry_latency_seconds_count{phase="copy_decision"} 5' in rendered
    assert 'telemetry_latency_seconds_sum{phase="copy_decision"} 15.0' in rendered
    assert 'telemetry_latency_seconds_bucket{le="+Inf",phase="copy_decision"} 5' in rendered

    [snapshot] = registry.histogram_snapshots()
    assert snapshot.count == 5
    assert snapshot.total == 15.0
    assert snapshot.sample_count == 5
    assert snapshot.p50 == 3.0
    assert snapshot.p95 == pytest.approx(4.8)
    assert snapshot.p99 == pytest.approx(4.96)
    assert snapshot.buckets[-1] == ("+Inf", 5)


def test_histogram_percentile_window_is_bounded_and_numeric_inputs_are_finite() -> None:
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS, sample_capacity=3)
    labels = {"phase": LatencyPhase.RISK.value}
    for value in (1.0, 2.0, 3.0, 4.0):
        registry.observe(
            METRIC_TELEMETRY_LATENCY,
            value,
            buckets=TELEMETRY_LATENCY_BUCKETS,
            labels=labels,
        )

    [snapshot] = registry.histogram_snapshots()
    assert snapshot.count == 4
    assert snapshot.sample_count == 3
    assert snapshot.p50 == 3.0

    for invalid in (math.inf, -math.inf, math.nan):
        with pytest.raises(ValueError, match="finite"):
            registry.observe(
                METRIC_TELEMETRY_LATENCY,
                invalid,
                buckets=TELEMETRY_LATENCY_BUCKETS,
                labels=labels,
            )

    with pytest.raises(ValueError, match="non-negative"):
        registry.observe(
            METRIC_TELEMETRY_LATENCY,
            -0.1,
            buckets=TELEMETRY_LATENCY_BUCKETS,
            labels=labels,
        )


@pytest.mark.asyncio
async def test_telemetry_service_owns_exact_counter_latency_and_gauge_updates() -> None:
    registry = MetricsRegistry()
    service = TelemetryService(
        hub=TelemetryHub(ingress_capacity=64, metrics=registry),
        metrics=registry,
    )
    await service.start()

    payloads = (
        LeaderEvent(
            outcome=LeaderOutcome.RECEIVED,
            leader_ref="leader-01",
            source="market_wss",
        ),
        LeaderEvent(
            outcome=LeaderOutcome.DEDUP,
            leader_ref="leader-01",
            source="market_wss",
        ),
        LeaderEvent(
            outcome=LeaderOutcome.PROMOTED,
            leader_ref="leader-01",
            source="market_wss",
        ),
        LeaderEvent(
            outcome=LeaderOutcome.RETRACTED,
            leader_ref="leader-01",
            source="market_wss",
        ),
        TradeEvent(
            outcome=TradeOutcome.EXECUTED,
            task_ref="task-01",
            side=TradeSide.BUY,
            order_ref="order-01",
            order_outcome=OrderOutcome.SUBMITTED,
        ),
        TradeEvent(
            outcome=TradeOutcome.PARTIAL,
            task_ref="task-01",
            side=TradeSide.BUY,
            order_ref="order-01",
            order_outcome=OrderOutcome.CANCELED,
        ),
        SkipEvent(reason=SkipReason.RISK_REJECTED, task_ref="task-02"),
        FailureEvent(
            component=FailureComponent.EXECUTION,
            code="order_not_fillable",
            order_outcome=OrderOutcome.NOT_FILLABLE,
        ),
        FailureEvent(
            component=FailureComponent.EXECUTION,
            code="venue_rejected",
            order_outcome=OrderOutcome.REJECTED,
        ),
        AmbiguousEvent(operation=AmbiguousOperation.ORDER_SUBMISSION, order_ref="order-02"),
        FillEvent(outcome=FillOutcome.OBSERVED, order_ref="order-01", fill_ref="fill-01"),
        FillEvent(outcome=FillOutcome.DUPLICATE, order_ref="order-01", fill_ref="fill-01"),
        ProtectiveExitEvent(outcome=ProtectiveOutcome.TRIGGERED, task_ref="task-01"),
        ProtectiveExitEvent(outcome=ProtectiveOutcome.FILLED, task_ref="task-01"),
        LockEvent(outcome=LockOutcome.CREATED, lock_ref="lock-01"),
        LockEvent(outcome=LockOutcome.BLOCKED, lock_ref="lock-01"),
        LockEvent(outcome=LockOutcome.RESET, lock_ref="lock-01"),
        ReconciliationEvent(outcome=ReconciliationOutcome.RUN),
        ReconciliationEvent(outcome=ReconciliationOutcome.DRIFT, affected_count=1),
        DiscoveryEvent(outcome=DiscoveryOutcome.REQUEST),
        DiscoveryEvent(outcome=DiscoveryOutcome.FAILURE),
        DiscoveryEvent(outcome=DiscoveryOutcome.SCHEMA),
        DiscoveryEvent(outcome=DiscoveryOutcome.COMPLETED, candidate_count=7),
        WssEvent(outcome=WssOutcome.RECONNECTED, stream="market"),
    )
    for payload in payloads:
        result = service.emit(producer=TelemetryProducer.WORKER, payload=payload)
        assert result.accepted is True

    service.observe_latency(phase=LatencyPhase.RISK, seconds=0.25)
    service.set_active_count(entity=ActiveEntity.LEADERS, value=3)
    service.set_queue_state(queue=TelemetryQueue.LEADER, depth=2, capacity=8)
    service.set_reconciliation_age(seconds=4.5)
    service.set_falcon_discovery_state(FalconDiscoveryState.RUNNING)
    service.record_leader_queue_overflow()

    counters, gauges = registry.snapshot()
    assert counters['telemetry_leader_signal_total{outcome="received"}'] == 1
    assert counters['telemetry_leader_signal_total{outcome="dedup"}'] == 1
    assert counters['telemetry_leader_signal_total{outcome="promoted"}'] == 1
    assert counters['telemetry_leader_signal_total{outcome="retracted"}'] == 1
    assert counters['telemetry_copy_outcome_total{outcome="executed",reason="none"}'] == 1
    assert counters['telemetry_copy_outcome_total{outcome="partial",reason="none"}'] == 1
    assert counters['telemetry_copy_outcome_total{outcome="skipped",reason="risk_rejected"}'] == 1
    assert counters['telemetry_copy_outcome_total{outcome="failed",reason="none"}'] == 2
    assert counters['telemetry_copy_outcome_total{outcome="ambiguous",reason="none"}'] == 1
    assert counters['telemetry_order_outcome_total{outcome="submitted"}'] == 1
    assert counters['telemetry_order_outcome_total{outcome="canceled"}'] == 1
    assert counters['telemetry_order_outcome_total{outcome="rejected"}'] == 1
    assert counters['telemetry_order_outcome_total{outcome="not_fillable"}'] == 1
    assert counters['telemetry_order_outcome_total{outcome="ambiguous"}'] == 1
    assert counters["telemetry_candidates_evaluated_total"] == 7
    assert counters['telemetry_fill_total{outcome="observed"}'] == 1
    assert counters['telemetry_fill_total{outcome="duplicate"}'] == 1
    assert counters['telemetry_protective_exit_total{outcome="triggered"}'] == 1
    assert counters['telemetry_protective_exit_total{outcome="filled"}'] == 1
    assert counters['telemetry_reentry_lock_total{outcome="created"}'] == 1
    assert counters['telemetry_reentry_lock_total{outcome="blocked"}'] == 1
    assert counters['telemetry_reentry_lock_total{outcome="reset"}'] == 1
    assert counters['telemetry_falcon_total{outcome="request"}'] == 1
    assert counters['telemetry_falcon_total{outcome="failure"}'] == 1
    assert counters['telemetry_falcon_total{outcome="schema"}'] == 1
    assert counters['telemetry_wss_reconnect_total{stream="market"}'] == 1
    assert counters['telemetry_reconciliation_total{outcome="run"}'] == 1
    assert counters['telemetry_reconciliation_total{outcome="drift"}'] == 1
    assert counters[METRIC_TELEMETRY_LEADER_QUEUE_OVERFLOW] == 1
    assert gauges[f'{GAUGE_TELEMETRY_ACTIVE_ENTITIES}{{entity="leaders"}}'] == 3
    assert gauges[f'{GAUGE_TELEMETRY_QUEUE_DEPTH}{{queue="leader"}}'] == 2
    assert gauges[f'{GAUGE_TELEMETRY_QUEUE_UTILIZATION}{{queue="leader"}}'] == 0.25
    assert gauges[GAUGE_TELEMETRY_RECONCILIATION_AGE] == 4.5
    assert gauges[f'{GAUGE_TELEMETRY_FALCON_DISCOVERY_STATE}{{state="running"}}'] == 1
    assert registry.histogram_snapshots()[0].p50 == 0.25

    await service.close()
