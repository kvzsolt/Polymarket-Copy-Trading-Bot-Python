"""Project application runtime state into safe control-plane contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from services.control_plane.contracts import (
    CapabilitiesView,
    EntryControlState,
    ReadinessView,
    RuntimeView,
    SystemView,
)
from services.healthServer import ReadinessTracker


class Clock(Protocol):
    """Provide aware wall-clock time for response timestamps."""

    def now(self) -> datetime:
        """Return the current aware instant."""

        ...


class MonotonicClock(Protocol):
    """Provide monotonic time for process uptime."""

    def monotonic(self) -> float:
        """Return monotonic seconds."""

        ...


class ActiveTaskCounter(Protocol):
    """Read the current durable active-task count."""

    async def count_active_tasks(self) -> int:
        """Return the current count through one bounded query."""

        ...


class EntryControlReader(Protocol):
    """Read the canonical durable trading-control projection."""

    async def get(self) -> EntryControlState:
        """Return the current entry-pause and full-stop state."""

        ...


class WorkerStateReader(Protocol):
    """Expose low-cardinality local copy-worker statistics."""

    @property
    def worker_count(self) -> int:
        """Return the allocated worker count."""

        ...

    @property
    def queue_depth(self) -> int:
        """Return the aggregate queued-event count."""

        ...


class InactiveWorkerStateReader:
    """Represent an intentionally absent copy-worker runtime."""

    @property
    def worker_count(self) -> int:
        """Return zero because no worker runtime was composed."""

        return 0

    @property
    def queue_depth(self) -> int:
        """Return zero because no event queues were composed."""

        return 0


class RuntimeSystemQueryAdapter:
    """Read the canonical readiness tracker without exposing mutable state."""

    def __init__(
        self,
        *,
        readiness: ReadinessTracker,
        service_name: str,
        version: str,
        environment: str,
        mode: str,
        backend: str,
        capabilities: tuple[str, ...],
        active_task_counter: ActiveTaskCounter,
        clock: Clock,
        monotonic_clock: MonotonicClock,
        started_monotonic: float,
    ) -> None:
        """Store immutable identity and injected runtime readers.

        Args:
            readiness: Existing application readiness truth source.
            service_name: Stable process service name.
            version: Current backend build version.
            environment: Explicit configuration environment.
            mode: Paper or live execution mode.
            backend: Active venue backend identifier.
            capabilities: Explicitly composed frontend capabilities.
            active_task_counter: Bounded durable active-task count reader.
            clock: Wall-clock timestamp source.
            monotonic_clock: Monotonic uptime source.
            started_monotonic: Monotonic process-composition start.
        """

        self._readiness = readiness
        self._service_name = str(service_name)
        self._version = str(version)
        self._environment = str(environment)
        self._mode = str(mode)
        self._backend = str(backend)
        self._capabilities = tuple(capabilities)
        self._active_task_counter = active_task_counter
        self._clock = clock
        self._monotonic_clock = monotonic_clock
        self._started_monotonic = float(started_monotonic)

    async def get_system(self) -> SystemView:
        """Return immutable process identity and uptime."""

        uptime = max(0.0, self._monotonic_clock.monotonic() - self._started_monotonic)
        return SystemView(
            service=self._service_name,
            version=self._version,
            environment=self._environment,
            mode=self._mode,
            backend=self._backend,
            uptime_seconds=uptime,
            observed_at=self._clock.now(),
        )

    async def get_readiness(self) -> ReadinessView:
        """Return strict readiness plus safe subsystem diagnostics."""

        snapshot = self._readiness.snapshot()
        ready, failed = self._readiness.is_ready(strict=True)
        checks = snapshot.get("checks")
        check_map = checks if isinstance(checks, dict) else {}
        active_task_count = await self._active_task_counter.count_active_tasks()
        if (
            isinstance(active_task_count, bool)
            or not isinstance(active_task_count, int)
            or active_task_count < 0
        ):
            raise RuntimeError("active task count reader returned an invalid value")
        return ReadinessView(
            live=True,
            ready=ready,
            failed_checks=tuple(failed),
            observed_at=self._clock.now(),
            circuit_breaker_state=str(snapshot.get("circuit_breaker_state") or "unknown"),
            wss_market_ok=bool(snapshot.get("wss_market_ok")),
            wss_user_ok=bool(snapshot.get("wss_user_ok")),
            reconciliation_ok=bool(snapshot.get("reconciler_ok")),
            detection_ok=self._timestamp(snapshot.get("last_trade_poll_ts")) is not None,
            discovery_ok=self._check_ready(check_map, "discovery_ok"),
            copy_workers_ok=self._check_ready(check_map, "copy_workers_ok"),
            control_plane_ok=self._check_ready(check_map, "control_plane_ok"),
            telemetry_ok=self._check_ready(check_map, "telemetry_ok"),
            active_leader_count=self._non_negative_count(snapshot.get("monitored_leader_count")),
            active_task_count=active_task_count,
            pending_intents=self._non_negative_count(snapshot.get("pending_intents_count")),
            reservations=self._non_negative_count(snapshot.get("active_reservations_count")),
            stale_locks=self._non_negative_count(snapshot.get("stale_locks_count")),
            last_reconciliation=self._timestamp(snapshot.get("last_reconcile_ts")),
            last_source_event=self._timestamp(snapshot.get("last_trade_poll_ts")),
        )

    async def get_capabilities(self) -> CapabilitiesView:
        """Return only the capabilities explicitly enabled by composition."""

        return CapabilitiesView(
            capabilities=self._capabilities,
            observed_at=self._clock.now(),
        )

    @staticmethod
    def _check_ready(checks: dict[object, object], key: str) -> bool:
        """Return one optional keyed readiness value."""

        raw = checks.get(key)
        return bool(raw.get("ready")) if isinstance(raw, dict) else False

    @staticmethod
    def _non_negative_count(value: object) -> int:
        """Normalize a safe non-negative readiness counter."""

        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            return 0
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _timestamp(value: object) -> datetime | None:
        """Convert a positive Unix timestamp to an aware UTC datetime."""

        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            return None
        try:
            timestamp = float(value or 0)
        except (TypeError, ValueError):
            return None
        if timestamp <= 0:
            return None
        try:
            return datetime.fromtimestamp(timestamp, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None


class RuntimeStateQueryAdapter:
    """Combine canonical durable controls with bounded local worker state."""

    def __init__(
        self,
        *,
        entry_control: EntryControlReader,
        worker_state: WorkerStateReader,
        clock: Clock,
    ) -> None:
        """Store narrow injected runtime readers.

        Args:
            entry_control: Canonical PostgreSQL-backed trading-control reader.
            worker_state: Local low-cardinality worker state reader.
            clock: Aware UTC response timestamp source.
        """

        self._entry_control = entry_control
        self._worker_state = worker_state
        self._clock = clock

    async def get_runtime(self) -> RuntimeView:
        """Return one validated current runtime snapshot.

        Returns:
            Canonical entry control paired with bounded worker statistics.

        Raises:
            RuntimeError: If a worker reader violates its non-negative integer
                contract.
        """

        worker_count = self._validated_count(self._worker_state.worker_count)
        queue_depth = self._validated_count(self._worker_state.queue_depth)
        return RuntimeView(
            entry_control=await self._entry_control.get(),
            worker_count=worker_count,
            queue_depth=queue_depth,
            observed_at=self._clock.now(),
        )

    @staticmethod
    def _validated_count(value: object) -> int:
        """Return a genuine non-negative integer or fail explicitly."""

        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RuntimeError("worker state reader returned an invalid value")
        return value


__all__ = [
    "ActiveTaskCounter",
    "EntryControlReader",
    "InactiveWorkerStateReader",
    "RuntimeStateQueryAdapter",
    "RuntimeSystemQueryAdapter",
    "WorkerStateReader",
]
