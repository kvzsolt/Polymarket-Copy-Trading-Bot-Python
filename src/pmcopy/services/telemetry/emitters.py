"""Synchronous, exception-guarded telemetry emitters for producer call sites.

Producers on the detection-to-submit path must never pay for observability.
Every method in this module is synchronous, never awaits, never raises, and
returns an explicit boolean so a call site can assert delivery without
inspecting hub internals. A ``None`` service turns an emitter into a total
null object, which is how paper runs and telemetry-disabled deployments stay
free of both cost and branching at the call site.

Layering: this module depends only on the telemetry contracts and the metrics
registry. Pipeline, execution, and risk collaborators are consumed through
local structural protocols so ``services.telemetry`` never imports
``orchestrators`` or ``application``.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol

from pmcopy.services.metrics import METRIC_TELEMETRY_EMIT_FAILURE, MetricsRegistry
from pmcopy.services.telemetry.latency import PipelineLatency
from pmcopy.services.telemetry.contracts import (
    AmbiguousEvent,
    AmbiguousOperation,
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
    RosterEvent,
    RosterOutcome,
    SkipEvent,
    SkipReason,
    TaskDecisionDetails,
    TelemetryPayload,
    TelemetryProducer,
    TelemetrySeverity,
    TradeEvent,
    TradeOutcome,
    TradeSide,
    WSS_STREAM_NAMES,
    WssEvent,
    WssOutcome,
    is_safe_code,
    is_safe_ref,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pmcopy.services.telemetry.service import TelemetryService
    from pmcopy.services.copy_tasks.domain import CopyTask
    from pmcopy.services.copy_tasks.event_routing import CopyTaskSourceEvent, TaskRouteResult


_LOGGER = logging.getLogger(__name__)

_CODE_SANITIZER_RE = re.compile(r"[^a-z0-9_.-]+")
_CODE_LEADING_RE = re.compile(r"^[^a-z0-9]+")
_MAX_CODE_CHARS = 64
_UNKNOWN_CODE = "unknown"
_REF_DIGEST_BYTES = 16

# Exact pipeline skip codes mapped onto the bounded contract vocabulary.
_SKIP_CODE_EXACT: Mapping[str, SkipReason] = MappingProxyType(
    {
        "market_data_stale": SkipReason.STALE_MARKET_DATA,
        "market_data_timeout": SkipReason.STALE_MARKET_DATA,
        "market_resolved": SkipReason.MARKET_NOT_ALLOWED,
        "entry_policy_unavailable": SkipReason.RISK_REJECTED,
        "market_risk_quarantined": SkipReason.RISK_REJECTED,
        "invalid_price": SkipReason.PRICE_LIMIT,
        "sell_size_zero": SkipReason.BELOW_MINIMUM_SIZE,
        "already_processed": SkipReason.DUPLICATE,
        "duplicate_ignored": SkipReason.DUPLICATE,
    }
)

# Prefix families, evaluated in declaration order after the exact table.
_SKIP_CODE_PREFIXES: tuple[tuple[str, SkipReason], ...] = (
    ("entry_denied", SkipReason.RISK_REJECTED),
    ("insufficient_", SkipReason.INSUFFICIENT_BALANCE),
    ("book_", SkipReason.CAPACITY),
    ("below_min", SkipReason.BELOW_MINIMUM_SIZE),
)

# ``ExecutionResult.status`` values that carry a venue-side order outcome.
# Anything else is a local pre-submit gate and reports no order outcome, so a
# blocked copy never inflates the venue rejection counters.
_ORDER_OUTCOME_BY_STATUS: Mapping[str, OrderOutcome] = MappingProxyType(
    {
        "rejected": OrderOutcome.REJECTED,
        "expired": OrderOutcome.NOT_FILLABLE,
        "not_fillable": OrderOutcome.NOT_FILLABLE,
        "order_not_fillable": OrderOutcome.NOT_FILLABLE,
        "cancelled": OrderOutcome.CANCELED,
        "canceled": OrderOutcome.CANCELED,
    }
)

# Copy-level outcome for an *accepted* execution result. Anything not listed
# here is a submission whose fill is still unknown, so it reports SUBMITTED:
# "executed" is owned by fill evidence and reconciliation, never by the act of
# submitting. Only a venue response that already carries matched size may claim
# a fill in the same call.
_TRADE_OUTCOME_BY_STATUS: Mapping[str, TradeOutcome] = MappingProxyType(
    {
        "filled": TradeOutcome.EXECUTED,
        "partially_filled": TradeOutcome.PARTIAL,
        "partial": TradeOutcome.PARTIAL,
    }
)

_UNCONFIRMED_STATUS = "submitted_unconfirmed"


class PipelineOutcomeLike(Protocol):
    """Structural read-only view of one ``TradePipelineResult`` row."""

    @property
    def trade_key(self) -> str:
        """Return the canonical copy identity for this row."""
        ...

    @property
    def status(self) -> str:
        """Return the pipeline status such as ``skipped`` or ``paper``."""
        ...

    @property
    def reason(self) -> str | None:
        """Return the internal skip or failure code, when present."""
        ...


class SignalLike(Protocol):
    """Structural read-only view of one normalized leader signal."""

    @property
    def side(self) -> str:
        """Return the leader trade side."""
        ...

    @property
    def tx_hash(self) -> str:
        """Return the leader transaction hash used for correlation."""
        ...


class OrderRequestLike(Protocol):
    """Structural read-only view of one ``ExecutionRequest``."""

    @property
    def user_id(self) -> str:
        """Return the follower account identity."""
        ...

    @property
    def leader_id(self) -> str:
        """Return the copied leader identity."""
        ...

    @property
    def token_id(self) -> str:
        """Return the outcome token identity."""
        ...

    @property
    def side(self) -> str:
        """Return the venue-neutral order side."""
        ...

    @property
    def signal_id(self) -> str | None:
        """Return the detection signal identity, when one exists."""
        ...


class OrderResultLike(Protocol):
    """Structural read-only view of one ``ExecutionResult``."""

    @property
    def accepted(self) -> bool:
        """Return whether the venue accepted the submission."""
        ...

    @property
    def order_id(self) -> str | None:
        """Return the venue order identity, when one exists."""
        ...

    @property
    def status(self) -> str:
        """Return the execution status code."""
        ...

    @property
    def error(self) -> str | None:
        """Return the sanitized error text, when present."""
        ...


def safe_ref(value: str) -> str:
    """Return a contract-safe opaque reference for an arbitrary identifier.

    Values that already satisfy the opaque-token contract are passed through
    unchanged so an operator still recognises a wallet or order id. Anything
    else (composite trade keys, delimiters, over-long identities) is replaced
    by a deterministic keyless digest, which keeps event references stable
    across processes without leaking structure.

    Args:
        value: Any identifier a producer wants to reference in an event.

    Returns:
        The value itself when safe, otherwise ``"h:"`` plus a BLAKE2b digest.
    """
    text = value if isinstance(value, str) else str(value)
    if is_safe_ref(text):
        return text
    digest = hashlib.blake2b(text.encode("utf-8"), digest_size=_REF_DIGEST_BYTES)
    return f"h:{digest.hexdigest()}"


def leader_ref(wallet: str) -> str:
    """Return the canonical opaque reference for one leader wallet.

    Wallet spellings differ per detection source: the REST-polling roster
    carries whatever casing the operator configured (often EIP-55 checksummed)
    while the on-chain dispatch path lower-cases before matching followers.
    ``_SAFE_TOKEN_RE`` accepts both spellings verbatim, so referencing them
    as-is would publish one leader under two identities and split every
    per-leader counter and UI grouping. Case-folding first makes the reference
    a function of the address alone.

    Args:
        wallet: Leader address in any casing.

    Returns:
        The lower-cased wallet when it is a safe token, otherwise a digest of
        the lower-cased value.
    """
    text = wallet if isinstance(wallet, str) else str(wallet)
    return safe_ref(text.strip().lower())


def bounded_code(value: str | None) -> str:
    """Return a bounded lowercase code accepted by the failure contract.

    Args:
        value: Candidate code, possibly mixed case or empty.

    Returns:
        A sanitized code of at most 64 characters, never empty.
    """
    if value and is_safe_code(value):
        # Hot path: detection sources and internal skip codes are already
        # bounded lowercase codes, so the common case is one regex match
        # instead of two substitutions.
        return value
    text = (value or "").strip().lower()
    text = _CODE_SANITIZER_RE.sub("_", text)
    text = _CODE_LEADING_RE.sub("", text)
    text = text[:_MAX_CODE_CHARS]
    return text or _UNKNOWN_CODE


def exception_code(exc: BaseException) -> str:
    """Return a bounded code for an exception without any message text.

    Only the exception class name is used. Messages can carry venue payloads,
    wallet addresses, or credentials and must never reach the event stream.

    Args:
        exc: Raised exception observed by a producer.

    Returns:
        A snake-case bounded code such as ``value_error``.
    """
    name = type(exc).__name__
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name)
    return bounded_code(snake)


def skip_reason_for_code(code: str | None) -> SkipReason:
    """Map one pipeline skip code onto the bounded contract vocabulary.

    Args:
        code: Free-form internal skip code, or ``None``.

    Returns:
        The bounded reason, defaulting to :attr:`SkipReason.OTHER`.
    """
    text = (code or "").strip().lower()
    if not text:
        return SkipReason.OTHER
    exact = _SKIP_CODE_EXACT.get(text)
    if exact is not None:
        return exact
    for prefix, reason in _SKIP_CODE_PREFIXES:
        if text.startswith(prefix):
            return reason
    return SkipReason.OTHER


def _skip_payload_factory(reason: SkipReason, task_ref: str) -> Callable[[], TelemetryPayload]:
    """Bind one skip payload eagerly so deferred delivery cannot rebind it."""

    def build() -> TelemetryPayload:
        return SkipEvent(reason=reason, task_ref=task_ref)

    return build


def _current_loop() -> asyncio.AbstractEventLoop | None:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


class TelemetryEmitter:
    """Publish bounded telemetry payloads without awaiting, blocking, or raising.

    Args:
        service: Telemetry service, or ``None`` for a total null object.
        producer: Stable producer identity used for FIFO event sequencing.
        loop: Event loop owning the telemetry service. Defaults to the loop
            running at construction time; ``None`` disables cross-thread
            scheduling entirely.
    """

    __slots__ = ("_service", "_producer", "_loop", "_metrics")

    def __init__(
        self,
        service: "TelemetryService | None",
        producer: TelemetryProducer,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._service = service
        self._producer = producer
        self._loop = loop if loop is not None else _current_loop()
        self._metrics = self._resolve_metrics(service)

    @property
    def enabled(self) -> bool:
        """Return whether a telemetry service is bound to this emitter."""
        return self._service is not None

    @property
    def producer(self) -> TelemetryProducer:
        """Return the stable producer identity of this emitter."""
        return self._producer

    def _guarded(self, emit: Callable[[], bool]) -> bool:
        """Run one complete emit path, argument handling included, under guard.

        Public emitter methods classify their arguments before they can build a
        payload, and that classification reads attributes and converts types
        that a hostile or simply wrong caller can make raise. Wrapping the whole
        method body keeps the promise that no emitter call can ever raise into a
        producer. A failure inside :meth:`_publish` is already absorbed there, so
        a single fault is counted exactly once.

        Args:
            emit: Zero-argument callable performing the full emit path.

        Returns:
            The emit result, or ``False`` when preparation failed.
        """
        try:
            return emit()
        except Exception:
            self._record_failure()
            return False

    def _publish(
        self,
        payload_factory: Callable[[], TelemetryPayload],
        *,
        severity: TelemetrySeverity,
        correlation_id: str | None = None,
    ) -> bool:
        """Build and enqueue one payload, absorbing every failure.

        Args:
            payload_factory: Zero-argument builder invoked inside the guard so
                a malformed payload can never escape into a producer.
            severity: Bounded operator severity for the event.
            correlation_id: Optional opaque token grouping related events.

        Returns:
            ``True`` when the event was accepted by the hub or handed to the
            owning loop for delivery, ``False`` on any drop or failure.
        """
        if self._service is None:
            return False
        loop = self._loop
        if loop is not None and _current_loop() is not loop:
            try:
                loop.call_soon_threadsafe(
                    self._emit,
                    payload_factory,
                    severity,
                    correlation_id,
                )
            except Exception:
                self._record_failure()
                return False
            return True
        return self._emit(payload_factory, severity, correlation_id)

    def _emit(
        self,
        payload_factory: Callable[[], TelemetryPayload],
        severity: TelemetrySeverity,
        correlation_id: str | None,
    ) -> bool:
        service = self._service
        if service is None:
            return False
        try:
            payload = payload_factory()
            result = service.emit(
                producer=self._producer,
                payload=payload,
                severity=severity,
                correlation_id=correlation_id,
            )
        except Exception:
            self._record_failure()
            return False
        return bool(getattr(result, "accepted", False))

    def _record_failure(self) -> None:
        metrics = self._metrics
        if metrics is None:
            return
        try:
            metrics.inc(
                METRIC_TELEMETRY_EMIT_FAILURE,
                labels={"producer": self._producer.value},
            )
        except Exception:
            # Last-resort guard: a broken metrics registry must not turn a
            # dropped telemetry event into a failed copy trade. The drop is
            # already visible through the emitter's ``False`` return value.
            return

    @staticmethod
    def _resolve_metrics(service: "TelemetryService | None") -> MetricsRegistry | None:
        if service is None:
            return None
        try:
            return service.metrics_registry
        except Exception:
            return None


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


class TradeTelemetryEmitter(TelemetryEmitter):
    """Project post-submission copy-pipeline outcomes onto bounded events.

    The pipeline owns skips only. Venue outcomes belong to
    :class:`ExecutionTelemetryEmitter`, so an accepted, failed, or unconfirmed
    pipeline row produces no event here and is never counted twice.

    Args:
        service: Telemetry service, or ``None`` for a null object.
        loop: Optional owning event loop override.
    """

    __slots__ = ()

    def __init__(
        self,
        service: "TelemetryService | None",
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(service, TelemetryProducer.WORKER, loop)

    def task_decision(
        self,
        task: "CopyTask",
        event: "CopyTaskSourceEvent",
        result: "TaskRouteResult",
        evaluated_at: datetime,
        max_signal_age_seconds: float,
    ) -> bool:
        """Publish actual task admission evidence without counting another venue outcome."""

        def payload() -> TelemetryPayload:
            runtime = result.runtime_decision
            envelope = None if runtime is None else runtime.envelope
            details = TaskDecisionDetails(
                account_ref=safe_ref(task.follower_account),
                task_version=str(task.version),
                reason_code=result.reason,
                source_price=event.trade.price,
                source_shares=event.trade.shares,
                source_timestamp=event.trade.venue_timestamp,
                evaluated_at=evaluated_at,
                max_signal_age_seconds=max_signal_age_seconds,
                price_limit=None if envelope is None else envelope.entry_price_decision.price_limit,
            )
            if not result.allowed:
                return SkipEvent(
                    reason=skip_reason_for_code(result.reason),
                    task_ref=safe_ref(task.task_id),
                    decision=details,
                )
            return TradeEvent(
                outcome=TradeOutcome.CANDIDATE_EVALUATED,
                task_ref=safe_ref(task.task_id),
                side=TradeSide(event.trade.side.value),
                order_outcome=None,
                decision=details,
            )

        return self._publish(payload, severity=TelemetrySeverity.INFO)

    def pipeline_results(
        self,
        user_id: str,
        leader_wallet: str,
        results: Iterable[PipelineOutcomeLike],
        signals_by_key: Mapping[str, SignalLike],
    ) -> bool:
        """Publish one bounded skip event per skipped or paper-tier result.

        Args:
            user_id: Follower account identity.
            leader_wallet: Leader the results belong to.
            results: Pipeline outcome rows, already returned by the pipeline.
            signals_by_key: Normalized signals keyed by trade key, used to
                recover the copy correlation id.

        Returns:
            ``True`` when every derived event was accepted; ``False`` when the
            emitter is disabled or any event was dropped.
        """
        if not self.enabled:
            return False
        return self._guarded(
            lambda: self._pipeline_results(user_id, leader_wallet, results, signals_by_key)
        )

    def _pipeline_results(
        self,
        user_id: str,
        leader_wallet: str,
        results: Iterable[PipelineOutcomeLike],
        signals_by_key: Mapping[str, SignalLike],
    ) -> bool:
        fallback_ref = safe_ref(f"{user_id}:{leader_wallet}")
        accepted = True
        for result in results:
            reason = self._skip_reason(result)
            if reason is None:
                continue
            trade_key = getattr(result, "trade_key", "") or ""
            task_ref = safe_ref(trade_key) if trade_key else fallback_ref
            severity = (
                TelemetrySeverity.INFO
                if reason is SkipReason.PAPER_ONLY
                else TelemetrySeverity.NOTICE
            )
            accepted = (
                self._publish(
                    _skip_payload_factory(reason, task_ref),
                    severity=severity,
                    correlation_id=self._correlation(signals_by_key, trade_key),
                )
                and accepted
            )
        return accepted

    def pipeline_failed(self, user_id: str, leader_wallet: str, exc: BaseException) -> bool:
        """Publish a bounded worker failure for an escaped pipeline exception.

        Args:
            user_id: Follower account identity.
            leader_wallet: Leader whose batch raised.
            exc: The escaped exception; only its class name is published.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._pipeline_failed(user_id, leader_wallet, exc))

    def _pipeline_failed(self, user_id: str, leader_wallet: str, exc: BaseException) -> bool:
        code = exception_code(exc)
        correlation = safe_ref(leader_wallet) if leader_wallet else safe_ref(user_id)
        return self._publish(
            lambda: FailureEvent(
                component=FailureComponent.WORKER,
                code=code,
                retryable=False,
            ),
            severity=TelemetrySeverity.ERROR,
            correlation_id=correlation,
        )

    def reconciliation_run(self, affected: int) -> bool:
        """Publish one completed reconciliation sweep.

        Args:
            affected: Number of records the sweep touched.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._publish(
            lambda: ReconciliationEvent(
                outcome=ReconciliationOutcome.RUN,
                affected_count=int(affected),
            ),
            severity=TelemetrySeverity.INFO,
        )

    @staticmethod
    def _skip_reason(result: PipelineOutcomeLike) -> SkipReason | None:
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status == "paper":
            return SkipReason.PAPER_ONLY
        if status == "skipped":
            return skip_reason_for_code(getattr(result, "reason", None))
        return None

    @staticmethod
    def _correlation(signals_by_key: Mapping[str, SignalLike], trade_key: str) -> str | None:
        signal = signals_by_key.get(trade_key) if signals_by_key else None
        tx_hash = getattr(signal, "tx_hash", None)
        return safe_ref(str(tx_hash)) if tx_hash else None


class ExecutionTelemetryEmitter(TelemetryEmitter):
    """Project venue-order outcomes and post-submission evidence.

    Args:
        service: Telemetry service, or ``None`` for a null object.
        loop: Optional owning event loop override.
    """

    __slots__ = ("_latency",)

    def __init__(
        self,
        service: "TelemetryService | None",
        loop: asyncio.AbstractEventLoop | None = None,
        *,
        latency: PipelineLatency | None = None,
    ) -> None:
        super().__init__(service, TelemetryProducer.EXECUTION, loop)
        self._latency = latency

    def order_result(self, request: OrderRequestLike, result: OrderResultLike) -> bool:
        """Publish exactly one bounded event for one execution attempt.

        Submission is never reported as a fill. An accepted result is
        ``TradeOutcome.SUBMITTED`` unless the venue itself already reported
        matched size in the same response, and an accepted submission whose POST
        outcome is ambiguous is reported as an ambiguous operation requiring
        reconciliation, never as a fill and never as a plain failure a caller
        might retry.

        Args:
            request: The execution request that was attempted.
            result: The result returned by the execution service.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._order_result(request, result))

    def _order_result(self, request: OrderRequestLike, result: OrderResultLike) -> bool:
        order_id = getattr(result, "order_id", None)
        order_ref = safe_ref(str(order_id)) if order_id else None
        status = str(getattr(result, "status", "") or "").strip().lower()
        accepted = bool(getattr(result, "accepted", False))
        error = getattr(result, "error", None)

        if accepted and status == _UNCONFIRMED_STATUS and error:
            return self._publish(
                lambda: AmbiguousEvent(
                    operation=AmbiguousOperation.ORDER_SUBMISSION,
                    order_ref=order_ref,
                    reconciliation_required=True,
                ),
                severity=TelemetrySeverity.WARNING,
            )
        task_ref = self._task_ref(request)
        if accepted:
            outcome = _TRADE_OUTCOME_BY_STATUS.get(status, TradeOutcome.SUBMITTED)
            return self._publish(
                lambda: TradeEvent(
                    outcome=outcome,
                    task_ref=task_ref,
                    side=self._side(request),
                    order_ref=order_ref,
                    order_outcome=OrderOutcome.SUBMITTED,
                ),
                severity=TelemetrySeverity.INFO,
            )
        return self._publish(
            lambda: FailureEvent(
                component=FailureComponent.EXECUTION,
                code=bounded_code(status or "failed"),
                retryable=False,
                order_outcome=_ORDER_OUTCOME_BY_STATUS.get(status),
            ),
            severity=TelemetrySeverity.WARNING,
        )

    def order_exception(self, request: OrderRequestLike, exc: BaseException) -> bool:
        """Publish a bounded failure for an exception escaping execution.

        Args:
            request: The execution request that was attempted.
            exc: The escaped exception; only its class name is published.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._order_exception(request, exc))

    def _order_exception(self, request: OrderRequestLike, exc: BaseException) -> bool:
        code = exception_code(exc)
        task_ref = self._task_ref(request)
        return self._publish(
            lambda: FailureEvent(
                component=FailureComponent.EXECUTION,
                code=code,
                retryable=False,
            ),
            severity=TelemetrySeverity.ERROR,
            correlation_id=task_ref,
        )

    def fill_observed(self, order_id: str, matched_total: object) -> bool:
        """Publish one observed fill keyed by its cumulative matched size.

        Args:
            order_id: Venue order identity.
            matched_total: Cumulative matched size or cost for the order.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        if self._latency is not None:
            self._latency.first_fill(order_id)
        return self._guarded(lambda: self._fill_observed(order_id, matched_total))

    def _fill_observed(self, order_id: str, matched_total: object) -> bool:
        order_ref = safe_ref(str(order_id))
        fill_ref = safe_ref(f"{order_id}:{matched_total}")
        return self._publish(
            lambda: FillEvent(
                outcome=FillOutcome.OBSERVED,
                order_ref=order_ref,
                fill_ref=fill_ref,
            ),
            severity=TelemetrySeverity.INFO,
        )

    def reconciliation_drift(self, order_id: str) -> bool:
        """Publish detected drift for one reconciled order.

        Args:
            order_id: Venue order identity used as the correlation token.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(
            lambda: self._publish(
                lambda: ReconciliationEvent(
                    outcome=ReconciliationOutcome.DRIFT,
                    affected_count=1,
                ),
                severity=TelemetrySeverity.WARNING,
                correlation_id=safe_ref(str(order_id)),
            )
        )

    def ambiguous_released(self, order_id: str) -> bool:
        """Publish the resolution of a previously ambiguous submission.

        Args:
            order_id: Venue order identity that reconciliation resolved.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(
            lambda: self._publish(
                lambda: AmbiguousEvent(
                    operation=AmbiguousOperation.ORDER_SUBMISSION,
                    order_ref=safe_ref(str(order_id)),
                    reconciliation_required=False,
                ),
                severity=TelemetrySeverity.NOTICE,
            )
        )

    @staticmethod
    def _task_ref(request: OrderRequestLike) -> str:
        signal_id = getattr(request, "signal_id", None)
        if signal_id:
            return safe_ref(str(signal_id))
        user_id = getattr(request, "user_id", "") or ""
        leader_id = getattr(request, "leader_id", "") or ""
        token_id = getattr(request, "token_id", "") or ""
        return safe_ref(f"{user_id}:{leader_id}:{token_id}")

    @staticmethod
    def _side(request: OrderRequestLike) -> TradeSide:
        return TradeSide(str(getattr(request, "side", "") or "").strip().upper())


class RiskTelemetryEmitter(TelemetryEmitter):
    """Project protective-exit and risk-decision transitions.

    Args:
        service: Telemetry service, or ``None`` for a null object.
        loop: Optional owning event loop override.
    """

    __slots__ = ()

    def __init__(
        self,
        service: "TelemetryService | None",
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(service, TelemetryProducer.RISK, loop)

    def exit_triggered(self, condition_id: str, token_id: str, reason: str) -> bool:
        """Publish a triggered protective exit.

        ``reason`` keeps one vocabulary across the three exit methods. The
        frozen ``ProtectiveExitEvent`` contract carries no free-text field, so
        the reason is only serialized on the not-filled failure path.

        Args:
            condition_id: Market condition identity.
            token_id: Outcome token identity.
            reason: Internal exit reason code.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(
            lambda: self._protective(
                condition_id,
                token_id,
                ProtectiveOutcome.TRIGGERED,
                TelemetrySeverity.WARNING,
            )
        )

    def exit_filled(self, condition_id: str, token_id: str, reason: str) -> bool:
        """Publish a filled protective exit.

        Args:
            condition_id: Market condition identity.
            token_id: Outcome token identity.
            reason: Internal exit reason code, not serialized.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(
            lambda: self._protective(
                condition_id,
                token_id,
                ProtectiveOutcome.FILLED,
                TelemetrySeverity.NOTICE,
            )
        )

    def _protective(
        self,
        condition_id: str,
        token_id: str,
        outcome: ProtectiveOutcome,
        severity: TelemetrySeverity,
    ) -> bool:
        task_ref = _position_ref(condition_id, token_id)
        return self._publish(
            lambda: ProtectiveExitEvent(outcome=outcome, task_ref=task_ref),
            severity=severity,
        )

    def exit_not_filled(self, condition_id: str, token_id: str, reason: str) -> bool:
        """Publish a protective exit that did not fill and stays retryable.

        Args:
            condition_id: Market condition identity.
            token_id: Outcome token identity.
            reason: Internal exit reason code folded into the bounded code.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(lambda: self._exit_not_filled(condition_id, token_id, reason))

    def _exit_not_filled(self, condition_id: str, token_id: str, reason: str) -> bool:
        code = bounded_code(f"exit_not_filled_{reason}" if reason else "exit_not_filled")
        correlation = _position_ref(condition_id, token_id)
        return self._publish(
            lambda: FailureEvent(
                component=FailureComponent.PROTECTIVE_EXIT,
                code=code,
                retryable=True,
                order_outcome=OrderOutcome.NOT_FILLABLE,
            ),
            severity=TelemetrySeverity.ERROR,
            correlation_id=correlation,
        )

    def risk_rejected(self, task_ref: str) -> bool:
        """Publish one risk rejection.

        Args:
            task_ref: Task or trade identity; hashed when not a safe token.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._guarded(
            lambda: self._publish(
                lambda: RiskEvent(
                    outcome=RiskOutcome.REJECTED,
                    task_ref=safe_ref(task_ref) if task_ref else None,
                ),
                severity=TelemetrySeverity.NOTICE,
            )
        )


class WssTelemetryEmitter(TelemetryEmitter):
    """Project bounded WebSocket lifecycle transitions.

    Args:
        service: Telemetry service, or ``None`` for a null object.
        loop: Optional owning event loop override.
    """

    __slots__ = ()

    def __init__(
        self,
        service: "TelemetryService | None",
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(service, TelemetryProducer.WSS, loop)

    def connected(self, stream: str) -> bool:
        """Publish a stream connection.

        Args:
            stream: One of ``leader``, ``market``, or ``user``.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._stream_event(stream, WssOutcome.CONNECTED, TelemetrySeverity.INFO)

    def reconnected(self, stream: str) -> bool:
        """Publish a stream reconnection.

        Args:
            stream: One of ``leader``, ``market``, or ``user``.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._stream_event(stream, WssOutcome.RECONNECTED, TelemetrySeverity.NOTICE)

    def disconnected(self, stream: str) -> bool:
        """Publish a stream disconnection.

        Args:
            stream: One of ``leader``, ``market``, or ``user``.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._stream_event(stream, WssOutcome.DISCONNECTED, TelemetrySeverity.WARNING)

    def stalled(self, stream: str) -> bool:
        """Publish a stalled stream heartbeat.

        Args:
            stream: One of ``leader``, ``market``, or ``user``.

        Returns:
            Whether the event was accepted or scheduled for delivery.
        """
        return self._stream_event(stream, WssOutcome.STALLED, TelemetrySeverity.WARNING)

    def _stream_event(
        self,
        stream: str,
        outcome: WssOutcome,
        severity: TelemetrySeverity,
    ) -> bool:
        try:
            name = str(stream or "").strip().lower()
        except Exception:
            self._record_failure()
            return False
        if name not in WSS_STREAM_NAMES:
            # A caller typo is a wiring bug, not a telemetry backend fault, so it
            # must not land on telemetry_emit_failure_total and make the operator
            # chase a healthy hub. DEBUG keeps it free in production.
            _LOGGER.debug("telemetry_wss_stream_rejected | stream=%s", name)
            return False
        return self._publish(
            lambda: WssEvent(outcome=outcome, stream=name),
            severity=severity,
        )


def _position_ref(condition_id: str, token_id: str) -> str:
    """Return a stable opaque reference for one market position."""
    return safe_ref(f"{condition_id}:{token_id}")


@dataclass(frozen=True, slots=True)
class TelemetryEmitters:
    """Immutable bundle of every producer-facing telemetry emitter.

    Composition passes this bundle down one path so no collaborator has to know
    whether telemetry is enabled: a disabled bundle is a total null object.
    """

    detection: DetectionTelemetryEmitter
    trade: TradeTelemetryEmitter
    execution: ExecutionTelemetryEmitter
    risk: RiskTelemetryEmitter
    wss: WssTelemetryEmitter
    latency: PipelineLatency | None = None

    @property
    def enabled(self) -> bool:
        """Return whether the bundle is bound to a telemetry service."""
        return self.detection.enabled

    @classmethod
    def build(
        cls,
        service: "TelemetryService | None",
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        latency: PipelineLatency | None = None,
    ) -> "TelemetryEmitters":
        """Build one bundle sharing a single telemetry service.

        Args:
            service: Telemetry service, or ``None`` for a disabled bundle.
            loop: Optional owning event loop override.

        Returns:
            A frozen bundle with one emitter per producer identity.
        """
        return cls(
            detection=DetectionTelemetryEmitter(service, loop=loop),
            trade=TradeTelemetryEmitter(service, loop),
            execution=ExecutionTelemetryEmitter(service, loop, latency=latency),
            risk=RiskTelemetryEmitter(service, loop),
            wss=WssTelemetryEmitter(service, loop),
            latency=latency,
        )

    @classmethod
    def disabled(cls) -> "TelemetryEmitters":
        """Return a bundle whose every method is a no-op returning ``False``."""
        return cls.build(None)


__all__ = [
    "DetectionCounters",
    "DetectionCountersSnapshot",
    "DetectionTelemetryEmitter",
    "ExecutionTelemetryEmitter",
    "OrderRequestLike",
    "OrderResultLike",
    "PipelineOutcomeLike",
    "RiskTelemetryEmitter",
    "SignalLike",
    "TelemetryEmitter",
    "TelemetryEmitters",
    "TradeTelemetryEmitter",
    "WssTelemetryEmitter",
    "bounded_code",
    "exception_code",
    "leader_ref",
    "safe_ref",
    "skip_reason_for_code",
]
