"""Detection telemetry counters and emitter."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import asyncio
import time
from types import MappingProxyType
from typing import TYPE_CHECKING

from pmcopy.services.telemetry.contracts import (
    LeaderEvent,
    LeaderOutcome,
    RosterEvent,
    RosterOutcome,
    TelemetryProducer,
    TelemetrySeverity,
)
from pmcopy.services.telemetry.emitter_base import (
    TelemetryEmitter,
    bounded_code,
    leader_ref,
    safe_ref,
)

if TYPE_CHECKING:
    from pmcopy.services.telemetry.service import TelemetryService


@dataclass(frozen=True, slots=True)
class DetectionCounters:
    """Immutable detection counters for one source or one leader."""

    signals_total: int = 0
    deduplicated_total: int = 0
    last_signal_at: float | None = None


@dataclass(frozen=True, slots=True)
class DetectionCountersSnapshot:
    """Immutable, bounded view of local detection state for operator reads."""

    sources: Mapping[str, DetectionCounters]
    leaders: Mapping[str, DetectionCounters]
    active_leader_count: int | None = None
    leader_ws_connected: bool | None = None


class _MutableCounters:
    """Private accumulator behind :class:`DetectionCounters`."""

    __slots__ = ("signals_total", "deduplicated_total", "last_signal_at")

    def __init__(self) -> None:
        self.signals_total = 0
        self.deduplicated_total = 0
        self.last_signal_at: float | None = None

    def frozen(self) -> DetectionCounters:
        """Return an immutable copy of the current counters."""
        return DetectionCounters(
            signals_total=self.signals_total,
            deduplicated_total=self.deduplicated_total,
            last_signal_at=self.last_signal_at,
        )


class DetectionTelemetryEmitter(TelemetryEmitter):
    """Emit leader-detection events and keep bounded local counters.

    This is the only pre-submit emit on the copy path: exactly one event per
    detected signal.

    Leader wallets are case-folded through :func:`leader_ref` before they reach
    an event or a counter, so the checksummed spelling the polling roster
    carries and the lower-cased spelling the on-chain dispatcher uses collapse
    onto one identity. ``roster_updated`` carries a count only and needs no
    such normalisation.

    Counters are plain in-memory integers in plain dicts, with no lock. Every
    production call site (the on-chain dispatch adapter and the polling leader
    dispatcher) runs on the single asyncio loop thread, so the read-modify-write
    is not concurrent there and a lock would be pure hot-path cost. A
    foreign-thread producer is tolerated but not synchronized: it may lose an
    increment under a race. :meth:`snapshot` copies each table with a single
    C-level ``list(dict.items())`` so a concurrent producer can never make an
    operator read raise. Neither case can affect the money path.

    Args:
        service: Telemetry service, or ``None`` for a null object.
        max_leaders: Hard bound on retained per-leader counters.
        clock: Injected monotonic-ish wall clock returning seconds.
        loop: Optional owning event loop override.
    """

    __slots__ = ("_max_leaders", "_clock", "_sources", "_leaders", "_probe", "_active_leaders")

    def __init__(
        self,
        service: "TelemetryService | None",
        *,
        max_leaders: int = 256,
        clock: Callable[[], float] = time.time,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(service, TelemetryProducer.DETECTION, loop)
        if max_leaders <= 0:
            raise ValueError("max_leaders must be positive")
        self._max_leaders = max_leaders
        self._clock = clock
        self._sources: dict[str, _MutableCounters] = {}
        self._leaders: dict[str, _MutableCounters] = {}
        self._probe: Callable[[], bool | None] | None = None
        self._active_leaders: int | None = None

    def signal_received(self, source: str, leader_wallet: str, tx_hash: str) -> bool:
        """Record and publish one accepted leader signal.

        Args:
            source: Detection source code, typically a ``DetectionMode`` value.
            leader_wallet: Leader identity in any casing; case-folded so every
                detection source publishes one leader under one reference,
                then hashed when not a safe token.
            tx_hash: Leader transaction hash used as the copy correlation id.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._signal(source, leader_wallet, tx_hash, dedup=False))

    def signal_deduplicated(self, source: str, leader_wallet: str) -> bool:
        """Record and publish one deduplicated leader signal.

        Args:
            source: Detection source code, typically a ``DetectionMode`` value.
            leader_wallet: Leader identity in any casing; case-folded so every
                detection source publishes one leader under one reference,
                then hashed when not a safe token.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._signal(source, leader_wallet, None, dedup=True))

    def _signal(
        self,
        source: str,
        leader_wallet: str,
        tx_hash: str | None,
        *,
        dedup: bool,
    ) -> bool:
        code = bounded_code(source)
        ref = leader_ref(leader_wallet)
        self._touch(code, ref, deduplicated=dedup)
        outcome = LeaderOutcome.DEDUP if dedup else LeaderOutcome.RECEIVED
        severity = TelemetrySeverity.DEBUG if dedup else TelemetrySeverity.INFO
        return self._publish(
            lambda: LeaderEvent(outcome=outcome, leader_ref=ref, source=code),
            severity=severity,
            correlation_id=safe_ref(tx_hash) if tx_hash else None,
        )

    def roster_updated(self, active_count: int) -> bool:
        """Publish the current active leader-roster size.

        Args:
            active_count: Number of leaders the runtime is following.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._roster(active_count))

    def _roster(self, active_count: int) -> bool:
        count = int(active_count)
        self._active_leaders = count
        outcome = RosterOutcome.UPDATED if count > 0 else RosterOutcome.EMPTY
        severity = TelemetrySeverity.INFO if count > 0 else TelemetrySeverity.WARNING
        return self._publish(
            lambda: RosterEvent(outcome=outcome, active_count=count),
            severity=severity,
        )

    def attach_connection_probe(self, probe: Callable[[], bool | None] | None) -> bool:
        """Bind the late-created leader WSS health probe.

        The detection transport is composed after this emitter, so the probe is
        injected once it exists and is read only at snapshot time.

        Args:
            probe: Callable returning connection state, or ``None`` to detach.

        Returns:
            ``True`` when a callable probe is now attached.
        """
        if probe is None or not callable(probe):
            self._probe = None
            return False
        self._probe = probe
        return True

    def _frozen_table(self, table: dict[str, _MutableCounters]) -> Mapping[str, DetectionCounters]:
        """Return a read-only frozen copy of one live counter table."""
        # ``list(dict.items())`` is a single C-level copy with no interpreter
        # checkpoint inside it, so a producer mutating the table from another
        # thread cannot make this raise "dictionary changed size during
        # iteration" the way a Python-level comprehension over the live dict can.
        items = list(table.items())
        return MappingProxyType({name: counters.frozen() for name, counters in items})

    def snapshot(self) -> DetectionCountersSnapshot:
        """Return an immutable, bounded view of local detection state.

        Returns:
            Per-source and per-leader counters plus the probed WSS state.
        """
        connected: bool | None = None
        probe = self._probe
        if probe is not None:
            try:
                raw = probe()
            except Exception:
                raw = None
            connected = None if raw is None else bool(raw)
        return DetectionCountersSnapshot(
            sources=self._frozen_table(self._sources),
            leaders=self._frozen_table(self._leaders),
            active_leader_count=self._active_leaders,
            leader_ws_connected=connected,
        )

    def _touch(self, source: str, leader_ref: str, *, deduplicated: bool) -> None:
        now = float(self._clock())
        self._bump(self._sources, source, now, deduplicated=deduplicated, limit=None)
        self._bump(
            self._leaders,
            leader_ref,
            now,
            deduplicated=deduplicated,
            limit=self._max_leaders,
        )

    @staticmethod
    def _bump(
        table: dict[str, _MutableCounters],
        key: str,
        now: float,
        *,
        deduplicated: bool,
        limit: int | None,
    ) -> None:
        counters = table.pop(key, None)
        if counters is None:
            counters = _MutableCounters()
            if limit is not None and len(table) >= limit:
                # Least-recently-active eviction: dict order is insertion order
                # and every update reinserts, so the head is the stalest key.
                table.pop(next(iter(table)), None)
        table[key] = counters
        if deduplicated:
            counters.deduplicated_total += 1
        else:
            counters.signals_total += 1
        counters.last_signal_at = now

