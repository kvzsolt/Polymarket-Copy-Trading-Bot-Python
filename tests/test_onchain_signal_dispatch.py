"""Tests for ``orchestrators.onchain_signal_dispatch.OnchainSignalDispatchAdapter``.

The adapter replaces the ``_onchain_signal_sink`` closure in ``app.main``:
it logs per-signal provenance, counts the signal per source, and routes it
either to the v1 copy-task runtime or to the legacy per-context pipeline.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from types import SimpleNamespace

import pytest

from orchestrators.onchain_signal_dispatch import OnchainSignalDispatchAdapter
from services.metrics import METRIC_DETECTION_SIGNALS, MetricsRegistry, TELEMETRY_METRIC_SCHEMAS
from services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
)

_LEADER = "0x" + "ab" * 20
_TX = "0x" + "cd" * 32


def _signal(*, provisional: bool = False) -> OnchainTradeSignal:
    return OnchainTradeSignal(
        leader_wallet=_LEADER,
        token_id="123456",
        side="BUY",
        size=Decimal("10"),
        price=Decimal("0.5"),
        usdc_size=Decimal("5"),
        tx_hash=_TX,
        confidence=(
            DetectionConfidence.PENDING_PROBABLE
            if provisional
            else DetectionConfidence.MINED_AUTHORITATIVE
        ),
        maker_side=0,
        watched_role="maker",
        event_ts=1_700_000_000,
        log_index=None if provisional else 7,
        condition_id="0x" + "ef" * 32,
        provisional=provisional,
        exchange="0x" + "11" * 20,
        order_hash=None,
    )


class _Pipeline:
    def __init__(self, fail_for: set[str] | None = None) -> None:
        self.calls: list[tuple[str, str, list, float]] = []
        self._fail_for = fail_for or set()

    async def __call__(self, ctx, leader_id, raws, now_ts):
        self.calls.append((ctx.user_id, leader_id, raws, now_ts))
        if ctx.user_id in self._fail_for:
            raise RuntimeError("boom")
        return []


class _V1Runtime:
    def __init__(self) -> None:
        self.signals: list[OnchainTradeSignal] = []

    async def submit_onchain(self, signal) -> None:
        self.signals.append(signal)


def _ctx(user_id: str) -> SimpleNamespace:
    return SimpleNamespace(user_id=user_id)


def _adapter(
    contexts_by_leader,
    *,
    v1_runtime=None,
    pipeline=None,
    metrics=None,
) -> tuple[OnchainSignalDispatchAdapter, _Pipeline, MetricsRegistry]:
    pipeline = pipeline or _Pipeline()
    metrics = metrics or MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    adapter = OnchainSignalDispatchAdapter(
        contexts_by_leader=contexts_by_leader,
        v1_runtime=v1_runtime,
        execute_trades=pipeline,
        clock=lambda: 1234.5,
        metrics=metrics,
        logger=logging.getLogger("OnchainSignalDispatch"),
    )
    return adapter, pipeline, metrics


@pytest.mark.asyncio
async def test_dispatches_to_all_follower_contexts_case_insensitive() -> None:
    checksummed = "0x" + "AB" * 20
    contexts = {checksummed: [_ctx("u1"), _ctx("u2")], "0x" + "99" * 20: [_ctx("u3")]}
    adapter, pipeline, _ = _adapter(contexts)

    await adapter(_signal())

    assert [call[0] for call in pipeline.calls] == ["u1", "u2"]
    for _user, leader_id, raws, now_ts in pipeline.calls:
        assert leader_id == _LEADER
        assert now_ts == 1234.5
        assert len(raws) == 1
        assert raws[0]["tx_hash"] == _TX
        assert raws[0]["side"] == "BUY"


@pytest.mark.asyncio
async def test_no_followers_is_a_noop() -> None:
    adapter, pipeline, _ = _adapter({"0x" + "99" * 20: [_ctx("u3")]})

    await adapter(_signal())

    assert pipeline.calls == []


@pytest.mark.asyncio
async def test_prefers_v1_runtime_when_present() -> None:
    v1 = _V1Runtime()
    adapter, pipeline, _ = _adapter({_LEADER: [_ctx("u1")]}, v1_runtime=v1)

    signal = _signal()
    await adapter(signal)

    assert v1.signals == [signal]
    assert pipeline.calls == []


@pytest.mark.asyncio
async def test_logs_provenance_and_increments_metric(caplog: pytest.LogCaptureFixture) -> None:
    adapter, _pipeline, metrics = _adapter({_LEADER: [_ctx("u1")]})

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=True))
        await adapter(_signal(provisional=False))

    lines = [record.getMessage() for record in caplog.records if "detection_signal |" in record.getMessage()]
    assert len(lines) == 2
    assert lines[0].startswith("detection_signal | source=on_chain_pending ")
    assert f"leader={_LEADER}" in lines[0]
    assert f"tx={_TX}" in lines[0]
    assert "side=BUY" in lines[0]
    assert "token=123456" in lines[0]
    assert "confidence=pending_probable" in lines[0]
    assert "provisional=True" in lines[0]
    assert lines[1].startswith("detection_signal | source=on_chain_confirmed ")
    assert "confidence=mined_authoritative" in lines[1]
    assert "provisional=False" in lines[1]

    assert metrics.counters[f'{METRIC_DETECTION_SIGNALS}{{source="on_chain_pending"}}'] == 1
    assert metrics.counters[f'{METRIC_DETECTION_SIGNALS}{{source="on_chain_confirmed"}}'] == 1


@pytest.mark.asyncio
async def test_pipeline_error_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    pipeline = _Pipeline(fail_for={"u1"})
    adapter, _pipeline, _ = _adapter({_LEADER: [_ctx("u1"), _ctx("u2")]}, pipeline=pipeline)

    with caplog.at_level(logging.WARNING, logger="OnchainSignalDispatch"):
        await adapter(_signal())

    assert [call[0] for call in pipeline.calls] == ["u1", "u2"]
    warnings = [r.getMessage() for r in caplog.records if "onchain_signal_pipeline_error" in r.getMessage()]
    assert len(warnings) == 1
    assert "user=u1" in warnings[0]


def test_detection_signals_metric_schema_is_bounded() -> None:
    from services.metrics import MetricKind

    schema = {s.name: s for s in TELEMETRY_METRIC_SCHEMAS}[METRIC_DETECTION_SIGNALS]
    assert schema.kind is MetricKind.COUNTER
    assert schema.allowed_label_values == (
        ("source", ("polling", "on_chain_pending", "on_chain_confirmed")),
    )
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    with pytest.raises(ValueError):
        registry.inc(METRIC_DETECTION_SIGNALS, labels={"source": "mempool"})
