"""Base telemetry emitter protocols, bounded code helpers, and guard wrapper."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol

from pmcopy.services.observability.metrics import METRIC_TELEMETRY_EMIT_FAILURE, MetricsRegistry
from pmcopy.services.telemetry.contracts import (
    OrderOutcome,
    SkipEvent,
    SkipReason,
    TelemetryPayload,
    TelemetryProducer,
    TelemetrySeverity,
    TradeOutcome,
    is_safe_code,
    is_safe_ref,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pmcopy.services.telemetry.service import TelemetryService


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

