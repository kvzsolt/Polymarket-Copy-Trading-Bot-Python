"""Read-only monitoring queries over local process state.

These services answer account, detection-health, leader, and metrics reads
from in-memory runtime truth plus, at most, one injected database port. None
of them may perform a venue call, and none of them may block the copy path:
every collaborator here is either a bounded local snapshot or an existing
control-plane query repository.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol

from services.control_plane.contracts import (
    AccountView,
    DetectionSourceStatusView,
    DetectionStatusView,
    LeaderView,
    PendingConfirmationStatsView,
)


# Prometheus-style keys of the existing per-stream WSS reconnect counter.
_MARKET_RECONNECT_KEY = 'wss_reconnect_total{stream="market"}'
_USER_RECONNECT_KEY = 'wss_reconnect_total{stream="user"}'

# Hard ceiling on one leader listing, matching the operator UI page budget.
_MAX_LEADERS = 500


class Clock(Protocol):
    """Provide aware wall-clock time for response timestamps."""

    def now(self) -> datetime:
        """Return the current aware instant."""

        ...


class DetectionCountersLike(Protocol):
    """Read one immutable detection counter row."""

    @property
    def signals_total(self) -> int:
        """Return accepted signals seen for this key."""

        ...

    @property
    def deduplicated_total(self) -> int:
        """Return duplicate signals suppressed for this key."""

        ...

    @property
    def last_signal_at(self) -> float | None:
        """Return the last signal instant as Unix seconds."""

        ...


class DetectionCountersSnapshotLike(Protocol):
    """Read one immutable detection counter snapshot."""

    @property
    def sources(self) -> Mapping[str, DetectionCountersLike]:
        """Return per-source counters."""

        ...

    @property
    def leaders(self) -> Mapping[str, DetectionCountersLike]:
        """Return per-leader counters."""

        ...

    @property
    def active_leader_count(self) -> int | None:
        """Return the last published roster size, when known."""

        ...

    @property
    def leader_ws_connected(self) -> bool | None:
        """Return probed detection transport state, or ``None``."""

        ...


class DetectionCountersSource(Protocol):
    """Expose bounded local detection counters without any I/O."""

    def snapshot(self) -> DetectionCountersSnapshotLike:
        """Return one immutable counter snapshot."""

        ...


class ReadinessSource(Protocol):
    """Read the canonical process readiness state."""

    def snapshot(self) -> Mapping[str, object]:
        """Return a point-in-time readiness snapshot."""

        ...


class MetricsSource(Protocol):
    """Read counter and gauge values from the process metrics registry."""

    def snapshot(self) -> tuple[Mapping[str, float], Mapping[str, float]]:
        """Return isolated counter and gauge mappings."""

        ...


class MetricsSnapshotSource(Protocol):
    """Project the metrics registry into a JSON-safe mapping."""

    def metrics_snapshot(self) -> Mapping[str, object]:
        """Return counters, gauges, and histogram summaries."""

        ...


class LeaderRosterRecordLike(Protocol):
    """Read one persisted discovery roster row."""

    @property
    def wallet(self) -> str:
        """Return the roster wallet identity."""

        ...

    @property
    def tier(self) -> str:
        """Return the roster tier."""

        ...

    @property
    def score(self) -> float:
        """Return the current roster score."""

        ...


class LeaderRosterPort(Protocol):
    """Read the durable discovery roster."""

    async def list_roster(self) -> Sequence[LeaderRosterRecordLike]:
        """Return every roster row in ranking order."""

        ...


class LeaderCountsPort(Protocol):
    """Read bounded per-leader aggregates from durable trading truth."""

    async def leader_trade_counts(self) -> Mapping[str, int]:
        """Return recorded fill counts keyed by leader identity."""

        ...

    async def leader_open_position_counts(self) -> Mapping[str, int]:
        """Return open-position counts keyed by leader identity."""

        ...


class AccountQueryService:
    """Serve the follower accounts this process actually composed."""

    def __init__(self, accounts: tuple[AccountView, ...]) -> None:
        """Store the immutable composed account projections.

        Args:
            accounts: One view per follower account bound at startup.
        """

        self._accounts = tuple(accounts)

    async def list(self) -> tuple[AccountView, ...]:
        """Return every composed follower account."""

        return self._accounts


class DetectionQueryService:
    """Assemble detection health from counters, readiness, and metrics."""

    def __init__(
        self,
        *,
        counters: DetectionCountersSource,
        readiness: ReadinessSource,
        metrics: MetricsSource,
        detection_mode: str,
        pending_stats: Callable[[], PendingConfirmationStatsView | None] | None,
        clock: Clock,
    ) -> None:
        """Store the local-only collaborators for one detection read.

        Args:
            counters: Bounded detection counter snapshot source.
            readiness: Canonical process readiness truth.
            metrics: Process metrics registry.
            detection_mode: Selected ``detection.mode`` value.
            pending_stats: Optional provisional-copy tracker probe.
            clock: Aware UTC observation clock.
        """

        self._counters = counters
        self._readiness = readiness
        self._metrics = metrics
        self._detection_mode = str(detection_mode)
        self._pending_stats = pending_stats
        self._clock = clock

    async def get(self) -> DetectionStatusView:
        """Return the current detection-source health projection."""

        snapshot = self._counters.snapshot()
        readiness = self._readiness.snapshot()
        counters, _gauges = self._metrics.snapshot()
        sources = tuple(
            DetectionSourceStatusView(
                source=name,
                signals_total=row.signals_total,
                deduplicated_total=row.deduplicated_total,
                last_signal_at=_epoch_to_datetime(row.last_signal_at),
            )
            for name, row in sorted(snapshot.sources.items())
        )
        return DetectionStatusView(
            mode=self._detection_mode,
            sources=sources,
            leader_ws_connected=snapshot.leader_ws_connected,
            market_ws_ok=bool(readiness.get("wss_market_ok")),
            user_ws_ok=bool(readiness.get("wss_user_ok")),
            market_ws_reconnects=_counter_value(counters, _MARKET_RECONNECT_KEY),
            user_ws_reconnects=_counter_value(counters, _USER_RECONNECT_KEY),
            pending_confirmations=self._pending(),
            watched_leader_count=self._watched(snapshot, readiness),
            observed_at=self._clock.now(),
        )

    def _pending(self) -> PendingConfirmationStatsView | None:
        """Return tracker stats, degrading a defective probe to ``None``."""

        probe = self._pending_stats
        if probe is None:
            return None
        try:
            stats = probe()
        except Exception:
            return None
        return stats if isinstance(stats, PendingConfirmationStatsView) else None

    @staticmethod
    def _watched(
        snapshot: DetectionCountersSnapshotLike,
        readiness: Mapping[str, object],
    ) -> int:
        """Return the roster size published by detection, else readiness."""

        published = snapshot.active_leader_count
        if isinstance(published, int) and not isinstance(published, bool) and published >= 0:
            return published
        return _non_negative_int(readiness.get("monitored_leader_count"))


class LeaderQueryService:
    """Merge configured, discovered, monitored, and observed leader state."""

    def __init__(
        self,
        *,
        roster: LeaderRosterPort | None,
        counts: LeaderCountsPort | None,
        configured: tuple[str, ...],
        monitored: Callable[[], Iterable[str]],
        counters: DetectionCountersSource,
        clock: Clock,
        max_leaders: int = _MAX_LEADERS,
    ) -> None:
        """Store every leader-identity source used by the merge.

        Args:
            roster: Optional durable discovery roster reader.
            counts: Optional durable per-leader aggregate reader.
            configured: Operator-configured leader wallets.
            monitored: Late-bound reader of currently monitored wallets.
            counters: Bounded local detection counters.
            clock: Aware UTC clock reserved for signal timestamps.
            max_leaders: Hard ceiling on the merged listing.

        Raises:
            ValueError: If the leader ceiling is not positive.
        """

        if max_leaders <= 0:
            raise ValueError("max_leaders must be positive")
        self._roster = roster
        self._counts = counts
        self._configured = tuple(configured)
        self._monitored = monitored
        self._counters = counters
        self._clock = clock
        self._max_leaders = int(max_leaders)

    async def list(self) -> tuple[LeaderView, ...]:
        """Return one bounded merged leader listing."""

        monitored_wallets = tuple(self._monitored())
        configured = _normalized_set(self._configured)
        monitored = _normalized_set(monitored_wallets)
        snapshot = self._counters.snapshot()
        signals = {
            _normalize(wallet): row
            for wallet, row in snapshot.leaders.items()
            if _normalize(wallet)
        }
        roster = await self._roster_rows()
        trades = _normalized_counts(
            {} if self._counts is None else await self._counts.leader_trade_counts()
        )
        positions = _normalized_counts(
            {} if self._counts is None else await self._counts.leader_open_position_counts()
        )

        ordered: list[str] = []
        for wallet in (*self._configured, *monitored_wallets, *roster, *signals):
            normalized = _normalize(wallet)
            if normalized and normalized not in ordered:
                ordered.append(normalized)
            if len(ordered) >= self._max_leaders:
                break

        views: list[LeaderView] = []
        for wallet in ordered:
            row = signals.get(wallet)
            record = roster.get(wallet)
            views.append(
                LeaderView(
                    wallet=wallet,
                    configured=wallet in configured,
                    monitored=wallet in monitored,
                    tier=None if record is None else str(record.tier),
                    score=None if record is None else _decimal_text(record.score),
                    signals_total=0 if row is None else row.signals_total,
                    deduplicated_total=0 if row is None else row.deduplicated_total,
                    last_signal_at=None if row is None else _epoch_to_datetime(row.last_signal_at),
                    trade_count=_non_negative_int(trades.get(wallet)),
                    open_position_count=_non_negative_int(positions.get(wallet)),
                )
            )
        return tuple(views)

    async def _roster_rows(self) -> dict[str, LeaderRosterRecordLike]:
        """Return roster rows keyed by normalized wallet identity."""

        if self._roster is None:
            return {}
        records = await self._roster.list_roster()
        rows: dict[str, LeaderRosterRecordLike] = {}
        for record in records:
            wallet = _normalize(record.wallet)
            if wallet and wallet not in rows:
                rows[wallet] = record
        return rows


class MetricsQueryService:
    """Serve the metrics half of the telemetry projection on its own route."""

    def __init__(self, telemetry_query: MetricsSnapshotSource) -> None:
        """Store the existing telemetry query service.

        Args:
            telemetry_query: Owner of the JSON-safe metrics projection.
        """

        self._telemetry_query = telemetry_query

    def snapshot(self) -> Mapping[str, object]:
        """Return counters, gauges, and histograms without event history."""

        return self._telemetry_query.metrics_snapshot()


def _epoch_to_datetime(value: float | None) -> datetime | None:
    """Convert optional Unix seconds into an aware UTC timestamp."""

    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=UTC)
    except (OverflowError, OSError, TypeError, ValueError):
        return None


def _counter_value(counters: Mapping[str, float], key: str) -> int:
    """Return one non-negative integer counter value."""

    return _non_negative_int(counters.get(key))


def _non_negative_int(value: object) -> int:
    """Return a safe non-negative integer for a possibly absent value."""

    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return 0
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return 0


def _normalized_counts(counts: Mapping[str, int]) -> Mapping[str, int]:
    """Return one aggregate mapping keyed by normalized wallet identity."""

    return {_normalize(key): value for key, value in counts.items()}


def _decimal_text(value: object) -> str | None:
    """Return an exact decimal string for a finite numeric roster score."""

    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return format(parsed, "f") if parsed.is_finite() else None


def _normalize(wallet: object) -> str:
    """Return one case-folded trimmed wallet identity."""

    return str(wallet or "").strip().lower()


def _normalized_set(wallets: Iterable[str]) -> frozenset[str]:
    """Return the normalized identity set for a wallet collection."""

    return frozenset(filter(None, (_normalize(wallet) for wallet in wallets)))


__all__ = [
    "AccountQueryService",
    "Clock",
    "DetectionCountersSource",
    "DetectionQueryService",
    "LeaderCountsPort",
    "LeaderQueryService",
    "LeaderRosterPort",
    "MetricsQueryService",
    "MetricsSnapshotSource",
    "MetricsSource",
    "ReadinessSource",
]
