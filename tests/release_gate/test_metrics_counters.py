"""Release-gate: operator metric names are defined and emit cleanly (Issue #76).

Smoke-checks the new ops-facing metric constants and confirms ``inc_sync``
records counter values that surface through ``snapshot()`` and the prometheus
renderer (so dashboards built against these names will not silently miss).
"""
from __future__ import annotations

from services.metrics import (
    COPY_LATENCY_BUCKETS,
    GAUGE_PENDING_INTENTS,
    GAUGE_RESERVATION_ACTIVE,
    GAUGE_STALE_LOCKS_ACTIVE,
    METRIC_CACHE_STALE_EVICTIONS,
    METRIC_COPY_LATENCY,
    METRIC_ORDERS_REJECTED,
    METRIC_ORDERS_SUBMISSION_AMBIGUOUS,
    METRIC_ORDERS_SUBMITTED,
    METRIC_RECONCILIATION_DELTA_APPLIED,
    METRIC_RISK_EXIT_TRIGGERED,
    METRIC_WSS_HEARTBEAT_STALL,
    METRIC_WSS_RECONNECT,
    MetricsRegistry,
)


def test_required_metric_constants_present() -> None:
    expected = {
        METRIC_ORDERS_SUBMITTED,
        METRIC_ORDERS_REJECTED,
        METRIC_ORDERS_SUBMISSION_AMBIGUOUS,
        METRIC_RECONCILIATION_DELTA_APPLIED,
        METRIC_RISK_EXIT_TRIGGERED,
        METRIC_WSS_RECONNECT,
        METRIC_WSS_HEARTBEAT_STALL,
        METRIC_CACHE_STALE_EVICTIONS,
        GAUGE_RESERVATION_ACTIVE,
        GAUGE_STALE_LOCKS_ACTIVE,
        GAUGE_PENDING_INTENTS,
    }
    for name in expected:
        assert isinstance(name, str) and name


def test_inc_sync_records_counter() -> None:
    reg = MetricsRegistry()
    reg.inc_sync(METRIC_WSS_RECONNECT, labels={"stream": "user"})
    reg.inc_sync(METRIC_WSS_RECONNECT, labels={"stream": "user"})
    reg.inc_sync(METRIC_CACHE_STALE_EVICTIONS, labels={"reason": "fill"})

    counters, _ = reg.snapshot()
    user_key = f'{METRIC_WSS_RECONNECT}{{stream="user"}}'
    fill_key = f'{METRIC_CACHE_STALE_EVICTIONS}{{reason="fill"}}'
    assert counters[user_key] == 2
    assert counters[fill_key] == 1


def test_inc_records_counter() -> None:
    reg = MetricsRegistry()
    reg.inc(METRIC_WSS_RECONNECT, labels={"stream": "market"})
    reg.inc(METRIC_WSS_RECONNECT, labels={"stream": "market"})

    counters, _ = reg.snapshot()
    market_key = f'{METRIC_WSS_RECONNECT}{{stream="market"}}'
    assert counters[market_key] == 2


def test_prometheus_render_includes_inc_sync() -> None:
    reg = MetricsRegistry()
    reg.inc_sync(
        METRIC_RECONCILIATION_DELTA_APPLIED, value=1.5, labels={"side": "BUY"}
    )
    rendered = reg.render_prometheus()
    assert METRIC_RECONCILIATION_DELTA_APPLIED in rendered
    assert "1.5" in rendered


def test_copy_latency_histogram_renders_cumulative_buckets() -> None:
    reg = MetricsRegistry()
    labels = {"side": "BUY", "state": "submitted"}
    reg.observe(
        METRIC_COPY_LATENCY,
        0.20,
        buckets=COPY_LATENCY_BUCKETS,
        labels=labels,
    )
    reg.observe(
        METRIC_COPY_LATENCY,
        0.75,
        buckets=COPY_LATENCY_BUCKETS,
        labels=labels,
    )

    rendered = reg.render_prometheus()
    assert '# TYPE copy_latency_seconds histogram' in rendered
    assert 'copy_latency_seconds_bucket{le="0.25",side="BUY",state="submitted"} 1' in rendered
    assert 'copy_latency_seconds_bucket{le="1.0",side="BUY",state="submitted"} 2' in rendered
    assert 'copy_latency_seconds_count{side="BUY",state="submitted"} 2' in rendered
