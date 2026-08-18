"""Tests for ``services.onchain_detection.pending_copy_confirmation_tracker``.

In ``detection.mode=on_chain_pending`` a copy is placed off a *mempool
sighting*. That transaction may never mine, may revert, or may be replaced —
and nothing else in the runtime notices, because the mined subscription is off
in that mode and REST polling never sees an unmined fill. The tracker is the
only component that closes the loop: it polls the leader transaction's receipt
and reports every copy that could not be confirmed.

Every test is deterministic: an injected clock, an injected receipt fetcher,
and the ``run_once(now)`` seam instead of the background task.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from services.metrics import (
    GAUGE_PENDING_COPY_TRACKED,
    METRIC_PENDING_COPY_CONFIRMED,
    METRIC_PENDING_COPY_TRACKER_DROPPED,
    METRIC_PENDING_COPY_UNCONFIRMED,
    MetricsRegistry,
    TELEMETRY_METRIC_SCHEMAS,
)
from services.onchain_detection.pending_copy_confirmation_tracker import (
    PendingCopyConfirmationTracker,
    TrackedPendingCopy,
)

_LEADER = "0x" + "ab" * 20
_TOKEN = "123456"
_CONDITION = "0x" + "ef" * 32


def _tx(index: int = 1) -> str:
    return "0x" + f"{index:064x}"


def _entry(
    *, index: int = 1, recorded_at: float = 0.0, leader: str = _LEADER
) -> TrackedPendingCopy:
    return TrackedPendingCopy(
        tx_hash=_tx(index),
        leader_wallet=leader,
        user_ids=("u1",),
        token_id=_TOKEN,
        condition_id=_CONDITION,
        side="BUY",
        recorded_at=recorded_at,
    )


class _FakeClock:
    """Monotonic-shaped clock the test advances explicitly."""

    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _FakeFetcher:
    """Receipt fetcher returning scripted statuses (or raising) per call."""

    def __init__(self, script: list[object] | None = None) -> None:
        self.calls: list[str] = []
        self._script = list(script or [])

    async def fetch_status(self, tx_hash: str) -> int | None:
        self.calls.append(tx_hash)
        if not self._script:
            return None
        outcome = self._script.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome  # type: ignore[return-value]


class _RecordingTelemetry:
    """Captures ``emit`` kwargs; optionally raises to prove the guard."""

    def __init__(self, *, raises: bool = False) -> None:
        self.emitted: list[dict[str, object]] = []
        self._raises = raises

    def emit(self, **kwargs: object) -> None:
        self.emitted.append(kwargs)
        if self._raises:
            raise RuntimeError("telemetry exploded")


class _RecordingHook:
    def __init__(self, *, raises: bool = False) -> None:
        self.calls: list[tuple[TrackedPendingCopy, str]] = []
        self._raises = raises

    async def request_unwind(self, entry: TrackedPendingCopy, *, reason: str) -> None:
        self.calls.append((entry, reason))
        if self._raises:
            raise RuntimeError("unwind exploded")


def _tracker(
    *,
    fetcher: _FakeFetcher | None = None,
    clock: _FakeClock | None = None,
    metrics: MetricsRegistry | None = None,
    telemetry: object | None = None,
    unwind_hook: object | None = None,
    deadline_seconds: float = 60.0,
    max_tracked: int = 2048,
    poll_interval_seconds: float = 0.01,
) -> tuple[PendingCopyConfirmationTracker, _FakeFetcher, MetricsRegistry, _FakeClock]:
    fetcher = fetcher or _FakeFetcher()
    clock = clock or _FakeClock()
    metrics = metrics or MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    tracker = PendingCopyConfirmationTracker(
        receipt_fetcher=fetcher,
        clock=clock,
        metrics=metrics,
        telemetry=telemetry,
        logger=logging.getLogger("PendingCopyConfirmation"),
        deadline_seconds=deadline_seconds,
        max_tracked=max_tracked,
        poll_interval_seconds=poll_interval_seconds,
        unwind_hook=unwind_hook,
    )
    return tracker, fetcher, metrics, clock


# ---------------------------------------------------------------------------
# track(): synchronous, bounded, never raises
# ---------------------------------------------------------------------------


def test_track_is_synchronous_and_dedups_on_tx_and_leader() -> None:
    tracker, _fetcher, metrics, _clock = _tracker()

    assert tracker.track(_entry()) is True
    assert tracker.track(_entry()) is False
    assert tracker.track(_entry(index=2)) is True
    assert tracker.track(_entry(leader="0x" + "cd" * 20)) is True

    assert tracker.tracked_count == 3
    assert metrics.gauges[GAUGE_PENDING_COPY_TRACKED] == 3


def test_track_dedup_is_case_insensitive() -> None:
    tracker, _fetcher, _metrics, _clock = _tracker()
    tracker.track(_entry())

    upper = TrackedPendingCopy(
        tx_hash=_tx().upper().replace("0X", "0x"),
        leader_wallet=_LEADER.upper().replace("0X", "0x"),
        user_ids=("u1",),
        token_id=_TOKEN,
        condition_id=_CONDITION,
        side="BUY",
        recorded_at=0.0,
    )

    assert tracker.track(upper) is False
    assert tracker.tracked_count == 1


def test_track_evicts_the_oldest_entry_when_full_and_counts_the_drop() -> None:
    tracker, _fetcher, metrics, _clock = _tracker(max_tracked=2)

    tracker.track(_entry(index=1))
    tracker.track(_entry(index=2))
    tracker.track(_entry(index=3))

    assert tracker.tracked_count == 2
    assert metrics.counters[f'{METRIC_PENDING_COPY_TRACKER_DROPPED}{{reason="full"}}'] == 1
    assert metrics.gauges[GAUGE_PENDING_COPY_TRACKED] == 2
    # The *oldest* entry went: re-tracking it is accepted again, while the
    # newest is still deduped. Eviction of the newest would invert both.
    assert tracker.track(_entry(index=1)) is True
    assert tracker.track(_entry(index=3)) is False
    assert metrics.counters[f'{METRIC_PENDING_COPY_TRACKER_DROPPED}{{reason="full"}}'] == 2


def test_track_never_raises_on_a_broken_metrics_registry() -> None:
    """``track`` runs right after the copy dispatch; it must never propagate."""

    class _BrokenMetrics(MetricsRegistry):
        def set_gauge(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("metrics exploded")

    tracker, _fetcher, _metrics, _clock = _tracker(metrics=_BrokenMetrics())

    assert tracker.track(_entry()) is True
    assert tracker.tracked_count == 1


def test_track_rejects_an_entry_without_a_tx_hash() -> None:
    tracker, _fetcher, _metrics, _clock = _tracker()

    blank = TrackedPendingCopy(
        tx_hash="",
        leader_wallet=_LEADER,
        user_ids=("u1",),
        token_id=_TOKEN,
        condition_id=_CONDITION,
        side="BUY",
        recorded_at=0.0,
    )

    assert tracker.track(blank) is False
    assert tracker.tracked_count == 0


# ---------------------------------------------------------------------------
# run_once(): poll schedule and terminal outcomes
# ---------------------------------------------------------------------------


async def test_polls_only_on_the_configured_offsets() -> None:
    tracker, fetcher, _metrics, _clock = _tracker()
    tracker.track(_entry())

    for now, expected_calls in (
        (1.0, 0),
        (4.9, 0),
        (5.0, 1),
        (6.0, 1),
        (14.0, 1),
        (15.0, 2),
        (29.0, 2),
        (30.0, 3),
        (59.0, 3),
    ):
        await tracker.run_once(now)
        assert len(fetcher.calls) == expected_calls, now

    await tracker.run_once(60.0)
    assert len(fetcher.calls) == 4
    # The last offset coincides with the deadline: the copy is now terminal.
    assert tracker.tracked_count == 0


async def test_status_one_confirms_and_drops_the_entry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    tracker, fetcher, metrics, _clock = _tracker(fetcher=_FakeFetcher([1]))
    tracker.track(_entry())

    with caplog.at_level(logging.DEBUG, logger="PendingCopyConfirmation"):
        await tracker.run_once(5.0)

    assert fetcher.calls == [_tx()]
    assert metrics.counters[METRIC_PENDING_COPY_CONFIRMED] == 1
    assert tracker.tracked_count == 0
    assert metrics.gauges[GAUGE_PENDING_COPY_TRACKED] == 0
    lines = [r.getMessage() for r in caplog.records if "pending_copy_confirmed" in r.getMessage()]
    assert len(lines) == 1
    assert f"tx={_tx()}" in lines[0]


async def test_status_zero_reports_reverted_immediately(
    caplog: pytest.LogCaptureFixture,
) -> None:
    telemetry = _RecordingTelemetry()
    tracker, _fetcher, metrics, _clock = _tracker(
        fetcher=_FakeFetcher([0]),
        telemetry=telemetry,
    )
    tracker.track(_entry())

    with caplog.at_level(logging.WARNING, logger="PendingCopyConfirmation"):
        await tracker.run_once(5.0)

    assert metrics.counters[f'{METRIC_PENDING_COPY_UNCONFIRMED}{{reason="reverted"}}'] == 1
    assert tracker.tracked_count == 0
    warnings = [
        r.getMessage() for r in caplog.records if "pending_copy_unconfirmed" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert warnings[0] == (
        f"pending_copy_unconfirmed | tx={_tx()} leader={_LEADER} age_s=5.0 reason=reverted"
    )
    assert len(telemetry.emitted) == 1


async def test_not_found_is_retried_until_the_deadline_then_not_mined() -> None:
    tracker, fetcher, metrics, _clock = _tracker(fetcher=_FakeFetcher([None, None, None, None]))
    tracker.track(_entry())

    await tracker.run_once(5.0)
    await tracker.run_once(15.0)
    await tracker.run_once(30.0)
    assert tracker.tracked_count == 1
    assert metrics.counters[f'{METRIC_PENDING_COPY_UNCONFIRMED}{{reason="not_mined"}}'] == 0

    await tracker.run_once(60.0)

    assert len(fetcher.calls) == 4
    assert metrics.counters[f'{METRIC_PENDING_COPY_UNCONFIRMED}{{reason="not_mined"}}'] == 1
    assert tracker.tracked_count == 0


async def test_fetch_errors_are_retried_then_reported_as_rpc_error() -> None:
    script: list[object] = [
        RuntimeError("rpc down"),
        RuntimeError("rpc down"),
        RuntimeError("rpc down"),
        RuntimeError("rpc down"),
    ]
    tracker, fetcher, metrics, _clock = _tracker(fetcher=_FakeFetcher(script))
    tracker.track(_entry())

    await tracker.run_once(5.0)
    await tracker.run_once(15.0)
    assert tracker.tracked_count == 1

    await tracker.run_once(30.0)
    await tracker.run_once(60.0)

    assert len(fetcher.calls) == 4
    assert metrics.counters[f'{METRIC_PENDING_COPY_UNCONFIRMED}{{reason="rpc_error"}}'] == 1
    assert metrics.counters[f'{METRIC_PENDING_COPY_UNCONFIRMED}{{reason="not_mined"}}'] == 0
    assert tracker.tracked_count == 0


async def test_a_late_confirmation_still_wins_over_an_earlier_error() -> None:
    """An RPC outage before the deadline must not condemn a mined copy."""
    tracker, _fetcher, metrics, _clock = _tracker(fetcher=_FakeFetcher([RuntimeError("rpc"), 1]))
    tracker.track(_entry())

    await tracker.run_once(5.0)
    await tracker.run_once(15.0)

    assert metrics.counters[METRIC_PENDING_COPY_CONFIRMED] == 1
    assert metrics.counters[f'{METRIC_PENDING_COPY_UNCONFIRMED}{{reason="rpc_error"}}'] == 0
    assert tracker.tracked_count == 0


async def test_concurrent_polls_are_bounded() -> None:
    """``max_concurrent_fetches`` caps the RPC fan-out of one cycle."""
    in_flight = 0
    peak = 0

    class _SlowFetcher:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def fetch_status(self, tx_hash: str) -> int | None:
            nonlocal in_flight, peak
            self.calls.append(tx_hash)
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0)
            in_flight -= 1
            return 1

    fetcher = _SlowFetcher()
    tracker = PendingCopyConfirmationTracker(
        receipt_fetcher=fetcher,
        clock=_FakeClock(),
        metrics=MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        telemetry=None,
        logger=logging.getLogger("PendingCopyConfirmation"),
        max_concurrent_fetches=2,
    )
    for index in range(6):
        tracker.track(_entry(index=index + 1))

    await tracker.run_once(5.0)

    assert len(fetcher.calls) == 6
    assert peak <= 2


# ---------------------------------------------------------------------------
# Telemetry + unwind hook
# ---------------------------------------------------------------------------


async def test_unconfirmed_emits_a_bounded_detection_failure_event() -> None:
    from services.telemetry.contracts import (
        FailureComponent,
        FailureEvent,
        TelemetryProducer,
    )
    from services.telemetry.hub import TelemetryHub
    from services.telemetry.service import TelemetryService

    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    service = TelemetryService(
        hub=TelemetryHub(ingress_capacity=16, metrics=registry),
        metrics=registry,
    )
    await service.start()
    try:
        tracker, _fetcher, _metrics, _clock = _tracker(
            fetcher=_FakeFetcher([0]),
            metrics=registry,
            telemetry=service,
        )
        tracker.track(_entry())

        await tracker.run_once(5.0)

        events = service.history_snapshot()
        failures = [event for event in events if isinstance(event.payload, FailureEvent)]
        assert len(failures) == 1
        payload = failures[0].payload
        assert payload.component is FailureComponent.DETECTION
        assert payload.code == "pending_copy_reverted"
        assert payload.retryable is False
        assert failures[0].producer is TelemetryProducer.DETECTION
    finally:
        await service.close()


async def test_telemetry_failures_never_break_the_tracker() -> None:
    telemetry = _RecordingTelemetry(raises=True)
    tracker, _fetcher, metrics, _clock = _tracker(
        fetcher=_FakeFetcher([0]),
        telemetry=telemetry,
    )
    tracker.track(_entry())

    await tracker.run_once(5.0)

    assert metrics.counters[f'{METRIC_PENDING_COPY_UNCONFIRMED}{{reason="reverted"}}'] == 1
    assert tracker.tracked_count == 0


async def test_unwind_hook_is_called_only_when_injected() -> None:
    tracker, _fetcher, _metrics, _clock = _tracker(fetcher=_FakeFetcher([0]))
    tracker.track(_entry())
    await tracker.run_once(5.0)  # no hook -> nothing to assert beyond "no raise"

    hook = _RecordingHook()
    hooked, _fetcher2, _metrics2, _clock2 = _tracker(
        fetcher=_FakeFetcher([0]),
        unwind_hook=hook,
    )
    hooked.track(_entry(index=2))

    await hooked.run_once(5.0)

    assert len(hook.calls) == 1
    entry, reason = hook.calls[0]
    assert entry.tx_hash == _tx(2)
    assert reason == "reverted"


async def test_unwind_hook_errors_are_swallowed() -> None:
    hook = _RecordingHook(raises=True)
    tracker, _fetcher, metrics, _clock = _tracker(
        fetcher=_FakeFetcher([0]),
        unwind_hook=hook,
    )
    tracker.track(_entry())

    await tracker.run_once(5.0)

    assert len(hook.calls) == 1
    assert metrics.counters[f'{METRIC_PENDING_COPY_UNCONFIRMED}{{reason="reverted"}}'] == 1


async def test_a_confirmed_copy_never_calls_the_unwind_hook() -> None:
    hook = _RecordingHook()
    tracker, _fetcher, _metrics, _clock = _tracker(
        fetcher=_FakeFetcher([1]),
        unwind_hook=hook,
    )
    tracker.track(_entry())

    await tracker.run_once(5.0)

    assert hook.calls == []


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_start_spawns_the_named_task_and_stop_cancels_it() -> None:
    tracker, _fetcher, _metrics, _clock = _tracker()

    await tracker.start()
    names = {task.get_name() for task in asyncio.all_tasks()}
    assert "pending-copy-confirmation" in names
    assert tracker.running is True

    await tracker.stop()

    assert tracker.running is False
    assert "pending-copy-confirmation" not in {task.get_name() for task in asyncio.all_tasks()}


async def test_start_is_idempotent() -> None:
    tracker, _fetcher, _metrics, _clock = _tracker()

    await tracker.start()
    await tracker.start()
    try:
        tasks = [t for t in asyncio.all_tasks() if t.get_name() == "pending-copy-confirmation"]
        assert len(tasks) == 1
    finally:
        await tracker.stop()


async def test_stop_without_start_is_a_noop() -> None:
    tracker, _fetcher, _metrics, _clock = _tracker()

    await tracker.stop()

    assert tracker.running is False


async def test_the_background_loop_polls_and_survives_fetch_errors() -> None:
    class _ExplodingFetcher:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_status(self, tx_hash: str) -> int | None:
            self.calls += 1
            raise RuntimeError("rpc down")

    fetcher = _ExplodingFetcher()
    clock = _FakeClock(now=1000.0)
    tracker = PendingCopyConfirmationTracker(
        receipt_fetcher=fetcher,
        clock=clock,
        metrics=MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        telemetry=None,
        logger=logging.getLogger("PendingCopyConfirmation"),
        poll_interval_seconds=0.0,
    )
    tracker.track(
        TrackedPendingCopy(
            tx_hash=_tx(),
            leader_wallet=_LEADER,
            user_ids=("u1",),
            token_id=_TOKEN,
            condition_id=_CONDITION,
            side="BUY",
            recorded_at=990.0,
        )
    )

    await tracker.start()
    for _ in range(10):
        await asyncio.sleep(0)
    await tracker.stop()

    assert fetcher.calls >= 1
