"""Runtime-to-control-plane projection tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from services.control_plane.contracts import EntryControlState
from services.healthServer import ReadinessTracker


NOW = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)


class FixedClock:
    """Return a deterministic UTC wall-clock instant."""

    def now(self) -> datetime:
        """Return the fixed instant."""

        return NOW


class FixedMonotonicClock:
    """Return a deterministic monotonic instant."""

    def monotonic(self) -> float:
        """Return the fixed monotonic seconds."""

        return 125.5


class FixedTaskCounter:
    """Return a deterministic active-task count through an async query port."""

    def __init__(self, value: object) -> None:
        self._value = value

    async def count_active_tasks(self) -> int:
        """Return the configured value, including invalid test values."""

        return self._value  # type: ignore[return-value]


class FixedEntryControlReader:
    """Return one deterministic canonical runtime safety state."""

    def __init__(self, state: EntryControlState) -> None:
        self._state = state

    async def get(self) -> EntryControlState:
        """Return the configured durable state."""

        return self._state


class FixedWorkerStateReader:
    """Expose deterministic bounded worker statistics."""

    def __init__(self, *, worker_count: object, queue_depth: object) -> None:
        self._worker_count = worker_count
        self._queue_depth = queue_depth

    @property
    def worker_count(self) -> int:
        """Return the configured worker count, including invalid values."""

        return self._worker_count  # type: ignore[return-value]

    @property
    def queue_depth(self) -> int:
        """Return the configured queue depth, including invalid values."""

        return self._queue_depth  # type: ignore[return-value]


async def test_system_projection_exposes_required_safe_runtime_state() -> None:
    """System status contains no secrets and reflects the readiness truth."""

    from orchestrators.control_plane_runtime import RuntimeSystemQueryAdapter

    readiness = ReadinessTracker(
        {
            "db_ok": True,
            "api_ok": True,
            "gateway_ok": True,
            "wss_market_ok": True,
            "wss_user_ok": False,
            "reconciler_ok": True,
            "market_data_ready": True,
            "drift_ok": True,
            "circuit_breaker_state": "closed",
            "monitored_leader_count": 3,
            "pending_intents_count": 2,
            "active_reservations_count": 4,
            "stale_locks_count": 1,
            "last_reconcile_ts": int(NOW.timestamp()),
            "last_trade_poll_ts": int(NOW.timestamp()),
        }
    )
    readiness.register_check(
        "telemetry_ok",
        required=False,
        ready=True,
    )
    adapter = RuntimeSystemQueryAdapter(
        readiness=readiness,
        service_name="pmcopy",
        version="1.2.3",
        environment="paper",
        mode="paper",
        backend="ccxt",
        capabilities=("ccxt_backend", "live_stream"),
        active_task_counter=FixedTaskCounter(5),
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        started_monotonic=100.0,
    )

    system = await adapter.get_system()
    status = await adapter.get_readiness()
    capabilities = await adapter.get_capabilities()

    assert system.mode == "paper"
    assert system.backend == "ccxt"
    assert system.uptime_seconds == 25.5
    assert status.ready is False
    assert status.wss_market_ok is True
    assert status.wss_user_ok is False
    assert status.active_leader_count == 3
    assert status.active_task_count == 5
    assert status.pending_intents == 2
    assert status.reservations == 4
    assert status.stale_locks == 1
    assert status.last_reconciliation == datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
    assert status.last_source_event == datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
    assert capabilities.capabilities == ("ccxt_backend", "live_stream")


@pytest.mark.parametrize("invalid_count", [True, -1, 1.5, "2"])
async def test_runtime_projection_rejects_non_integer_active_task_counts(
    invalid_count: object,
) -> None:
    """Only genuine non-negative integers may enter the typed status DTO."""

    from orchestrators.control_plane_runtime import RuntimeSystemQueryAdapter

    adapter = RuntimeSystemQueryAdapter(
        readiness=ReadinessTracker(),
        service_name="pmcopy",
        version="1.2.3",
        environment="paper",
        mode="paper",
        backend="ccxt",
        capabilities=(),
        active_task_counter=FixedTaskCounter(invalid_count),
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        started_monotonic=100.0,
    )

    with pytest.raises(RuntimeError, match="active task count"):
        await adapter.get_readiness()


async def test_runtime_state_projection_combines_durable_control_and_worker_counts() -> None:
    """Runtime snapshot reads canonical safety state and local bounded counts."""

    from orchestrators.control_plane_runtime import RuntimeStateQueryAdapter

    entry_control = EntryControlState(
        entries_paused=True,
        full_stop=False,
        revision=7,
        reason_code="operator_pause",
        updated_at=NOW,
        updated_by="operator",
    )
    adapter = RuntimeStateQueryAdapter(
        entry_control=FixedEntryControlReader(entry_control),
        worker_state=FixedWorkerStateReader(worker_count=3, queue_depth=9),
        clock=FixedClock(),
    )

    snapshot = await adapter.get_runtime()

    assert snapshot.entry_control is entry_control
    assert snapshot.worker_count == 3
    assert snapshot.queue_depth == 9
    assert snapshot.observed_at == NOW


@pytest.mark.parametrize(
    ("worker_count", "queue_depth"),
    [(True, 0), (-1, 0), (0, True), (0, -1), (1.5, 0), (0, "2")],
)
async def test_runtime_state_projection_rejects_invalid_worker_counts(
    worker_count: object,
    queue_depth: object,
) -> None:
    """Reject booleans, negatives, and non-integers at the query boundary."""

    from orchestrators.control_plane_runtime import RuntimeStateQueryAdapter

    entry_control = EntryControlState(
        entries_paused=False,
        full_stop=False,
        revision=1,
        reason_code="enabled",
        updated_at=NOW,
        updated_by="system",
    )
    adapter = RuntimeStateQueryAdapter(
        entry_control=FixedEntryControlReader(entry_control),
        worker_state=FixedWorkerStateReader(
            worker_count=worker_count,
            queue_depth=queue_depth,
        ),
        clock=FixedClock(),
    )

    with pytest.raises(RuntimeError, match="worker state"):
        await adapter.get_runtime()


def test_inactive_worker_state_is_a_zero_count_reader() -> None:
    """Disabled v1 runtime remains substitutable for the worker read port."""

    from orchestrators.control_plane_runtime import InactiveWorkerStateReader

    reader = InactiveWorkerStateReader()

    assert reader.worker_count == 0
    assert reader.queue_depth == 0
