"""Immutable, secret-safe event contracts for the telemetry control plane."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import TypeAlias


_SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_SAFE_CODE_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")


def is_safe_ref(value: str) -> bool:
    """Return whether a value may be used verbatim as an opaque event reference.

    Args:
        value: Candidate reference such as a wallet, order, or task identity.

    Returns:
        ``True`` when the value matches the bounded opaque-token contract used
        by every ``*_ref`` field, ``False`` when it must be hashed first.
    """
    return _SAFE_TOKEN_RE.fullmatch(value) is not None


def _require_safe_token(name: str, value: str) -> None:
    if not _SAFE_TOKEN_RE.fullmatch(value):
        raise ValueError(f"{name} must be an opaque token of at most 128 characters")


def _require_safe_code(name: str, value: str) -> None:
    if not _SAFE_CODE_RE.fullmatch(value):
        raise ValueError(f"{name} must be a bounded lowercase code")


def _require_optional_ref(name: str, value: str | None) -> None:
    if value is not None:
        _require_safe_token(name, value)


class TelemetryCategory(StrEnum):
    """Stable categories supported by the v1 telemetry contract."""

    LEADER = "leader"
    TASK = "task"
    TRADE = "trade"
    FILL = "fill"
    SKIP = "skip"
    FAILURE = "failure"
    AMBIGUOUS = "ambiguous"
    PROTECTIVE = "protective"
    LOCK = "lock"
    RECONCILIATION = "reconciliation"
    DISCOVERY = "discovery"
    ROSTER = "roster"
    WSS = "wss"
    RUNTIME = "runtime"
    RISK = "risk"


class TelemetryEventType(StrEnum):
    """Stable event-type values exposed by snapshots and subscribers."""

    LEADER_SIGNAL = "leader_signal"
    TASK_DECISION = "task_decision"
    TRADE_SUBMITTED = "trade_submitted"
    TRADE_FILL = "trade_fill"
    TRADE_SKIP = "trade_skip"
    TRADE_FAILURE = "trade_failure"
    AMBIGUOUS_ORDER = "ambiguous_order"
    PROTECTIVE_EXIT = "protective_exit"
    REENTRY_LOCK = "reentry_lock"
    RECONCILIATION = "reconciliation"
    DISCOVERY_CYCLE = "discovery_cycle"
    LEADER_PROMOTED = "leader_promoted"
    LEADER_DEMOTED = "leader_demoted"
    WSS_STATE = "wss_state"
    RUNTIME_STATE = "runtime_state"
    RISK_STATE = "risk_state"


class TelemetryProducer(StrEnum):
    """Bounded producer identities used for ordering and aggregation."""

    DETECTION = "detection"
    NORMALIZATION = "normalization"
    WORKER = "worker"
    COPY_DECISION = "copy_decision"
    SIZING = "sizing"
    RISK = "risk"
    EXECUTION = "execution"
    RECONCILIATION = "reconciliation"
    PROTECTIVE_EXIT = "protective_exit"
    DISCOVERY = "discovery"
    WSS = "wss"
    RUNTIME = "runtime"
    CONTROL_PLANE = "control_plane"


class TelemetrySeverity(StrEnum):
    """Bounded operator severity for an event."""

    DEBUG = "debug"
    INFO = "info"
    NOTICE = "notice"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class LeaderOutcome(StrEnum):
    """Outcomes for canonical leader events."""

    RECEIVED = "received"
    DEDUP = "dedup"
    DEDUPLICATED = "dedup"
    PENDING = "pending"
    PROMOTED = "promoted"
    DEMOTED = "demoted"
    RETRACTED = "retracted"


class TaskOutcome(StrEnum):
    """Lifecycle outcomes for a copy task."""

    ENQUEUED = "enqueued"
    ADMITTED = "admitted"
    STARTED = "started"
    COMPLETED = "completed"
    CANCELED = "canceled"
    FAILED = "failed"


class TradeOutcome(StrEnum):
    """Bounded copy-trade outcomes."""

    CANDIDATE_EVALUATED = "candidate_evaluated"
    EXECUTED = "executed"
    PARTIAL = "partial"
    SKIPPED = "skipped"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


class TradeSide(StrEnum):
    """Canonical trade sides."""

    BUY = "BUY"
    SELL = "SELL"


class OrderOutcome(StrEnum):
    """Bounded venue-order lifecycle outcomes."""

    SUBMITTED = "submitted"
    CANCELED = "canceled"
    REJECTED = "rejected"
    NOT_FILLABLE = "not_fillable"
    AMBIGUOUS = "ambiguous"


class FillOutcome(StrEnum):
    """Deduplicated fill-observation outcomes."""

    OBSERVED = "observed"
    DUPLICATE = "duplicate"


class SkipReason(StrEnum):
    """Bounded reasons permitted on skip events and metric labels."""

    DUPLICATE = "duplicate"
    RISK_REJECTED = "risk_rejected"
    TASK_INACTIVE = "task_inactive"
    SCHEDULE_CLOSED = "schedule_closed"
    MARKET_NOT_ALLOWED = "market_not_allowed"
    REENTRY_LOCKED = "reentry_locked"
    BUDGET_EXHAUSTED = "budget_exhausted"
    INSUFFICIENT_BALANCE = "insufficient_balance"
    BELOW_MINIMUM_SIZE = "below_minimum_size"
    STALE_MARKET_DATA = "stale_market_data"
    PRICE_LIMIT = "price_limit"
    CAPACITY = "capacity"
    FINALITY_RETRACTED = "finality_retracted"
    UNSUPPORTED = "unsupported"
    # Paper-tier follows never reach the venue. They are a first-class skip so
    # an operator can tell a deliberate simulation apart from a lost copy.
    PAPER_ONLY = "paper_only"
    OTHER = "other"


class FailureComponent(StrEnum):
    """Bounded components that can report a sanitized failure code."""

    DETECTION = "detection"
    NORMALIZATION = "normalization"
    WORKER = "worker"
    COPY_DECISION = "copy_decision"
    SIZING = "sizing"
    RISK = "risk"
    EXECUTION = "execution"
    RECONCILIATION = "reconciliation"
    PROTECTIVE_EXIT = "protective_exit"
    DISCOVERY = "discovery"
    WSS = "wss"
    RUNTIME = "runtime"
    TELEMETRY = "telemetry"


class AmbiguousOperation(StrEnum):
    """Operations whose remote mutation result can be ambiguous."""

    ORDER_SUBMISSION = "order_submission"
    ORDER_CANCEL = "order_cancel"
    FILL_RECONCILIATION = "fill_reconciliation"


class ProtectiveOutcome(StrEnum):
    """Protective-exit lifecycle outcomes."""

    TRIGGERED = "triggered"
    FILLED = "filled"


class LockOutcome(StrEnum):
    """Durable reentry-lock outcomes."""

    CREATED = "created"
    BLOCKED = "blocked"
    RESET = "reset"


class ReconciliationOutcome(StrEnum):
    """Reconciliation cycle outcomes."""

    RUN = "run"
    DRIFT = "drift"


class DiscoveryOutcome(StrEnum):
    """Falcon discovery outcomes."""

    REQUEST = "request"
    FAILURE = "failure"
    SCHEMA = "schema"
    COMPLETED = "completed"


class RosterOutcome(StrEnum):
    """Dynamic leader-roster outcomes."""

    UPDATED = "updated"
    EMPTY = "empty"
    UNCHANGED = "unchanged"


class WssOutcome(StrEnum):
    """WebSocket lifecycle outcomes."""

    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTED = "reconnected"
    STALLED = "stalled"


class RuntimeOutcome(StrEnum):
    """Top-level runtime lifecycle outcomes."""

    STARTED = "started"
    READY = "ready"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"


class RiskOutcome(StrEnum):
    """Risk decision and protective state outcomes."""

    APPROVED = "approved"
    REJECTED = "rejected"
    LIMIT_REACHED = "limit_reached"
    EXIT_TRIGGERED = "exit_triggered"


@dataclass(frozen=True, slots=True, kw_only=True)
class LeaderEvent:
    """Describe one safe canonical leader-event transition."""

    outcome: LeaderOutcome
    leader_ref: str
    source: str
    category: TelemetryCategory = field(default=TelemetryCategory.LEADER, init=False)

    def __post_init__(self) -> None:
        _require_safe_token("leader_ref", self.leader_ref)
        _require_safe_code("source", self.source)


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskEvent:
    """Describe one copy-task lifecycle transition."""

    outcome: TaskOutcome
    task_ref: str
    leader_ref: str | None = None
    category: TelemetryCategory = field(default=TelemetryCategory.TASK, init=False)

    def __post_init__(self) -> None:
        _require_safe_token("task_ref", self.task_ref)
        _require_optional_ref("leader_ref", self.leader_ref)


@dataclass(frozen=True, slots=True, kw_only=True)
class TradeEvent:
    """Describe one copy-trade outcome without raw venue data."""

    outcome: TradeOutcome
    task_ref: str
    side: TradeSide
    order_ref: str | None = None
    order_outcome: OrderOutcome | None = OrderOutcome.SUBMITTED
    category: TelemetryCategory = field(default=TelemetryCategory.TRADE, init=False)

    def __post_init__(self) -> None:
        _require_safe_token("task_ref", self.task_ref)
        _require_optional_ref("order_ref", self.order_ref)


@dataclass(frozen=True, slots=True, kw_only=True)
class FillEvent:
    """Describe one observed or duplicate fill."""

    outcome: FillOutcome
    order_ref: str
    fill_ref: str
    category: TelemetryCategory = field(default=TelemetryCategory.FILL, init=False)

    def __post_init__(self) -> None:
        _require_safe_token("order_ref", self.order_ref)
        _require_safe_token("fill_ref", self.fill_ref)


@dataclass(frozen=True, slots=True, kw_only=True)
class SkipEvent:
    """Describe a bounded, operator-visible copy skip."""

    reason: SkipReason
    task_ref: str | None = None
    category: TelemetryCategory = field(default=TelemetryCategory.SKIP, init=False)

    def __post_init__(self) -> None:
        _require_optional_ref("task_ref", self.task_ref)


@dataclass(frozen=True, slots=True, kw_only=True)
class FailureEvent:
    """Describe a failure using only a bounded code, never raw exception text."""

    component: FailureComponent
    code: str
    retryable: bool = False
    order_outcome: OrderOutcome | None = None
    category: TelemetryCategory = field(default=TelemetryCategory.FAILURE, init=False)

    def __post_init__(self) -> None:
        _require_safe_code("code", self.code)


@dataclass(frozen=True, slots=True, kw_only=True)
class AmbiguousEvent:
    """Describe an operation that requires reconciliation before retry."""

    operation: AmbiguousOperation
    order_ref: str | None = None
    reconciliation_required: bool = True
    category: TelemetryCategory = field(default=TelemetryCategory.AMBIGUOUS, init=False)

    def __post_init__(self) -> None:
        _require_optional_ref("order_ref", self.order_ref)


@dataclass(frozen=True, slots=True, kw_only=True)
class ProtectiveExitEvent:
    """Describe a task protective-exit transition."""

    outcome: ProtectiveOutcome
    task_ref: str
    category: TelemetryCategory = field(default=TelemetryCategory.PROTECTIVE, init=False)

    def __post_init__(self) -> None:
        _require_safe_token("task_ref", self.task_ref)


@dataclass(frozen=True, slots=True, kw_only=True)
class LockEvent:
    """Describe a durable reentry-lock transition."""

    outcome: LockOutcome
    lock_ref: str
    category: TelemetryCategory = field(default=TelemetryCategory.LOCK, init=False)

    def __post_init__(self) -> None:
        _require_safe_token("lock_ref", self.lock_ref)


@dataclass(frozen=True, slots=True, kw_only=True)
class ReconciliationEvent:
    """Describe a reconciliation run or detected drift."""

    outcome: ReconciliationOutcome
    affected_count: int = 0
    category: TelemetryCategory = field(default=TelemetryCategory.RECONCILIATION, init=False)

    def __post_init__(self) -> None:
        if self.affected_count < 0:
            raise ValueError("affected_count must be non-negative")


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscoveryEvent:
    """Describe a Falcon discovery request or bounded failure class."""

    outcome: DiscoveryOutcome
    candidate_count: int = 0
    category: TelemetryCategory = field(default=TelemetryCategory.DISCOVERY, init=False)

    def __post_init__(self) -> None:
        if self.candidate_count < 0:
            raise ValueError("candidate_count must be non-negative")


@dataclass(frozen=True, slots=True, kw_only=True)
class RosterEvent:
    """Describe a dynamic leader-roster transition."""

    outcome: RosterOutcome
    active_count: int
    category: TelemetryCategory = field(default=TelemetryCategory.ROSTER, init=False)

    def __post_init__(self) -> None:
        if self.active_count < 0:
            raise ValueError("active_count must be non-negative")


@dataclass(frozen=True, slots=True, kw_only=True)
class WssEvent:
    """Describe a bounded WebSocket lifecycle event."""

    outcome: WssOutcome
    stream: str
    category: TelemetryCategory = field(default=TelemetryCategory.WSS, init=False)

    def __post_init__(self) -> None:
        _require_safe_code("stream", self.stream)
        if self.stream not in {"leader", "market", "user"}:
            raise ValueError("stream must be leader, market, or user")


@dataclass(frozen=True, slots=True, kw_only=True)
class RuntimeEvent:
    """Describe a top-level runtime lifecycle transition."""

    outcome: RuntimeOutcome
    category: TelemetryCategory = field(default=TelemetryCategory.RUNTIME, init=False)


@dataclass(frozen=True, slots=True, kw_only=True)
class RiskEvent:
    """Describe a risk decision without monetary or credential payloads."""

    outcome: RiskOutcome
    task_ref: str | None = None
    category: TelemetryCategory = field(default=TelemetryCategory.RISK, init=False)

    def __post_init__(self) -> None:
        _require_optional_ref("task_ref", self.task_ref)


TelemetryPayload: TypeAlias = (
    LeaderEvent
    | TaskEvent
    | TradeEvent
    | FillEvent
    | SkipEvent
    | FailureEvent
    | AmbiguousEvent
    | ProtectiveExitEvent
    | LockEvent
    | ReconciliationEvent
    | DiscoveryEvent
    | RosterEvent
    | WssEvent
    | RuntimeEvent
    | RiskEvent
)


def event_type_for_payload(payload: TelemetryPayload) -> TelemetryEventType:
    """Return the public event type for a typed safe payload.

    Args:
        payload: Immutable payload accepted by the telemetry contract.

    Returns:
        The stable event type serialized to subscribers and query snapshots.
    """
    if isinstance(payload, LeaderEvent):
        if payload.outcome is LeaderOutcome.PROMOTED:
            return TelemetryEventType.LEADER_PROMOTED
        if payload.outcome is LeaderOutcome.DEMOTED:
            return TelemetryEventType.LEADER_DEMOTED
        return TelemetryEventType.LEADER_SIGNAL
    if isinstance(payload, TaskEvent):
        return TelemetryEventType.TASK_DECISION
    if isinstance(payload, TradeEvent):
        return TelemetryEventType.TRADE_SUBMITTED
    if isinstance(payload, FillEvent):
        return TelemetryEventType.TRADE_FILL
    if isinstance(payload, SkipEvent):
        return TelemetryEventType.TRADE_SKIP
    if isinstance(payload, FailureEvent):
        return TelemetryEventType.TRADE_FAILURE
    if isinstance(payload, AmbiguousEvent):
        return TelemetryEventType.AMBIGUOUS_ORDER
    if isinstance(payload, ProtectiveExitEvent):
        return TelemetryEventType.PROTECTIVE_EXIT
    if isinstance(payload, LockEvent):
        return TelemetryEventType.REENTRY_LOCK
    if isinstance(payload, ReconciliationEvent):
        return TelemetryEventType.RECONCILIATION
    if isinstance(payload, DiscoveryEvent):
        return TelemetryEventType.DISCOVERY_CYCLE
    if isinstance(payload, RosterEvent):
        if payload.outcome is RosterOutcome.EMPTY:
            return TelemetryEventType.LEADER_DEMOTED
        return TelemetryEventType.LEADER_PROMOTED
    if isinstance(payload, WssEvent):
        return TelemetryEventType.WSS_STATE
    if isinstance(payload, RuntimeEvent):
        return TelemetryEventType.RUNTIME_STATE
    if isinstance(payload, RiskEvent):
        return TelemetryEventType.RISK_STATE
    raise TypeError("unsupported telemetry payload")


@dataclass(frozen=True, slots=True, kw_only=True)
class TelemetryEvent:
    """Immutable envelope ordered independently for each producer."""

    event_id: str
    event_type: TelemetryEventType
    timestamp: datetime
    severity: TelemetrySeverity
    correlation_id: str | None
    payload: TelemetryPayload
    producer: TelemetryProducer
    sequence: int

    def __post_init__(self) -> None:
        _require_safe_token("event_id", self.event_id)
        _require_optional_ref("correlation_id", self.correlation_id)
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        if self.sequence <= 0:
            raise ValueError("sequence must be positive")
        if self.event_type is not event_type_for_payload(self.payload):
            raise ValueError("event_type does not match payload")


# Descriptive aliases matching the public event-type vocabulary. The payload
# classes remain deliberately narrow and frozen; aliases do not introduce an
# unstructured dictionary escape hatch.
LeaderSignalEvent = LeaderEvent
TaskDecisionEvent = TaskEvent
TradeSubmittedEvent = TradeEvent
TradeFillEvent = FillEvent
TradeSkipEvent = SkipEvent
TradeFailureEvent = FailureEvent
AmbiguousOrderEvent = AmbiguousEvent
ReentryLockEvent = LockEvent
DiscoveryCycleEvent = DiscoveryEvent
WssStateEvent = WssEvent
RuntimeStateEvent = RuntimeEvent
RiskStateEvent = RiskEvent
LeaderPromotedEvent = LeaderEvent
LeaderDemotedEvent = LeaderEvent
