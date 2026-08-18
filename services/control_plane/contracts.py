"""Immutable application contracts for the operator control plane.

Financial values deliberately cross this boundary as validated decimal
strings.  Domain services may use :class:`~decimal.Decimal` internally, but
HTTP and persistence adapters never need to infer binary-float semantics.
"""

from __future__ import annotations

import math
import re
import hashlib
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Generic, TypeVar
from uuid import uuid4

from services.copy_tasks.domain import CopyTask


T = TypeVar("T")

# Accepted ``PnlView.unrealized_basis`` values. ``wss_mid`` means every open
# position carried a fresh two-sided cached quote; ``partial_wss_mid`` means
# only some did and no account total is published; ``unavailable`` means none
# did. There is no basis that permits inventing a mark.
PNL_VALUATION_BASES = ("wss_mid", "partial_wss_mid", "unavailable")


class ControlPlaneReason(StrEnum):
    """Stable, low-cardinality public failure reasons."""

    INVALID_REQUEST = "invalid_request"
    INVALID_PAGE = "invalid_page"
    INVALID_DECIMAL_STRING = "invalid_decimal_string"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    REVISION_MISMATCH = "revision_mismatch"
    TASK_DELETED = "task_deleted"
    TASK_RUNTIME_UNAVAILABLE = "task_runtime_unavailable"
    BULK_LIMIT_EXCEEDED = "bulk_limit_exceeded"
    DUPLICATE_BULK_TARGET = "duplicate_bulk_target"
    BULK_FAILED = "bulk_failed"
    PORTFOLIO_UNAVAILABLE = "portfolio_unavailable"
    PORTFOLIO_STALE = "portfolio_stale"
    DISCOVERY_BUSY = "discovery_busy"
    DISCOVERY_UNAVAILABLE = "discovery_unavailable"
    FULL_STOP_ACTIVE = "full_stop_active"
    RUNTIME_UNAVAILABLE = "runtime_unavailable"
    SETTINGS_PATH_FORBIDDEN = "settings_path_forbidden"
    SETTINGS_SYMLINK_FORBIDDEN = "settings_symlink_forbidden"
    SETTINGS_INVALID = "settings_invalid"
    SETTINGS_SECRET_FORBIDDEN = "settings_secret_forbidden"
    SETTINGS_UNAVAILABLE = "settings_unavailable"
    AUDIT_UNAVAILABLE = "audit_unavailable"
    INTERNAL_ERROR = "internal_error"

    @property
    def public_message(self) -> str:
        """Return a fixed message that contains no collaborator detail."""

        messages = {
            self.INVALID_REQUEST: "The request is invalid.",
            self.INVALID_PAGE: "The requested page is invalid.",
            self.INVALID_DECIMAL_STRING: "A financial value must be a decimal string.",
            self.NOT_FOUND: "The requested resource was not found.",
            self.CONFLICT: "The requested change conflicts with current state.",
            self.REVISION_MISMATCH: "The resource revision no longer matches.",
            self.TASK_DELETED: "A deleted task cannot be changed.",
            self.TASK_RUNTIME_UNAVAILABLE: ("The task cannot be activated by the current runtime."),
            self.BULK_LIMIT_EXCEEDED: "Bulk commands support between 1 and 100 items.",
            self.DUPLICATE_BULK_TARGET: "A bulk command contains a duplicate target.",
            self.BULK_FAILED: "The atomic bulk command was not applied.",
            self.PORTFOLIO_UNAVAILABLE: "A cached portfolio snapshot is unavailable.",
            self.PORTFOLIO_STALE: "The cached portfolio snapshot is stale.",
            self.DISCOVERY_BUSY: "A discovery run is already pending.",
            self.DISCOVERY_UNAVAILABLE: "Discovery cannot accept work right now.",
            self.FULL_STOP_ACTIVE: "Entry resume is blocked while full stop is active.",
            self.RUNTIME_UNAVAILABLE: "Runtime control is unavailable.",
            self.SETTINGS_PATH_FORBIDDEN: "The settings path is not operator-editable.",
            self.SETTINGS_SYMLINK_FORBIDDEN: "Symbolic links are forbidden for settings.",
            self.SETTINGS_INVALID: "The settings document is invalid.",
            self.SETTINGS_SECRET_FORBIDDEN: "Secrets cannot cross the settings boundary.",
            self.SETTINGS_UNAVAILABLE: "Settings storage is unavailable.",
            self.AUDIT_UNAVAILABLE: "The audit store is unavailable.",
            self.INTERNAL_ERROR: "The control plane could not complete the request.",
        }
        return messages[self]


class ControlPlaneError(RuntimeError):
    """Sanitized application error suitable for transport mapping."""

    def __init__(self, reason: ControlPlaneReason) -> None:
        """Create an error with a stable reason and fixed public message.

        Args:
            reason: Stable failure classification.
        """

        self.reason = reason
        self.public_message = reason.public_message
        super().__init__(self.public_message)


class ContractValidation:
    """Central validation rules for immutable transport contracts."""

    @staticmethod
    def require_text(value: object) -> str:
        """Return stripped non-empty text or reject the request."""

        if not isinstance(value, str) or not value.strip():
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        return value.strip()

    @staticmethod
    def require_revision(value: object) -> int:
        """Return a positive optimistic-concurrency revision."""

        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        return value

    @staticmethod
    def require_count(value: object) -> int:
        """Return a genuine non-negative integer count."""

        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        return value

    @staticmethod
    def require_aware_datetime(value: object) -> datetime:
        """Return a timezone-aware timestamp."""

        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        return value

    @staticmethod
    def require_decimal_string(value: object) -> str:
        """Validate a finite decimal without changing its string representation."""

        if not isinstance(value, str) or not value.strip():
            raise ControlPlaneError(ControlPlaneReason.INVALID_DECIMAL_STRING)
        try:
            parsed = Decimal(value)
        except (InvalidOperation, ValueError):
            raise ControlPlaneError(ControlPlaneReason.INVALID_DECIMAL_STRING) from None
        if not parsed.is_finite():
            raise ControlPlaneError(ControlPlaneReason.INVALID_DECIMAL_STRING)
        return value

    @staticmethod
    def optional_decimal_string(value: object | None) -> str | None:
        """Validate an optional finite decimal string."""

        if value is None:
            return None
        return ContractValidation.require_decimal_string(value)


@dataclass(frozen=True, slots=True)
class PageRequest:
    """Bounded opaque-cursor pagination request."""

    limit: int = 50
    cursor: str | None = None

    def __post_init__(self) -> None:
        """Reject unbounded limits and oversized cursor material."""

        if isinstance(self.limit, bool) or not isinstance(self.limit, int):
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE)
        if self.limit < 1 or self.limit > 100:
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE)
        if self.cursor is not None:
            if not isinstance(self.cursor, str) or not self.cursor or len(self.cursor) > 512:
                raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE)


@dataclass(frozen=True, slots=True)
class Page(Generic[T]):
    """Immutable query page with an optional opaque continuation cursor."""

    items: tuple[T, ...]
    next_cursor: str | None

    def __post_init__(self) -> None:
        """Keep the page bounded even when an adapter is defective."""

        if not isinstance(self.items, tuple) or len(self.items) > 100:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        if self.next_cursor is not None and (
            not isinstance(self.next_cursor, str)
            or not self.next_cursor
            or len(self.next_cursor) > 512
        ):
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)


@dataclass(frozen=True, slots=True)
class SystemView:
    """Process identity and environment snapshot."""

    service: str
    version: str
    environment: str
    observed_at: datetime
    mode: str = "paper"
    backend: str = "ccxt"
    uptime_seconds: float = 0.0

    def __post_init__(self) -> None:
        """Validate the process snapshot."""

        ContractValidation.require_text(self.service)
        ContractValidation.require_text(self.version)
        ContractValidation.require_text(self.environment)
        ContractValidation.require_text(self.mode)
        ContractValidation.require_text(self.backend)
        if not math.isfinite(self.uptime_seconds) or self.uptime_seconds < 0:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class ReadinessView:
    """Authoritative liveness/readiness projection from the runtime."""

    live: bool
    ready: bool
    failed_checks: tuple[str, ...]
    observed_at: datetime
    circuit_breaker_state: str = "unknown"
    wss_market_ok: bool = False
    wss_user_ok: bool = False
    reconciliation_ok: bool = False
    detection_ok: bool = False
    discovery_ok: bool = False
    copy_workers_ok: bool = False
    control_plane_ok: bool = False
    telemetry_ok: bool = False
    active_leader_count: int = 0
    active_task_count: int = 0
    pending_intents: int = 0
    reservations: int = 0
    stale_locks: int = 0
    last_reconciliation: datetime | None = None
    last_source_event: datetime | None = None

    def __post_init__(self) -> None:
        """Validate bounded readiness evidence."""

        if not isinstance(self.failed_checks, tuple) or len(self.failed_checks) > 32:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        for check in self.failed_checks:
            ContractValidation.require_text(check)
        ContractValidation.require_text(self.circuit_breaker_state)
        for value in (
            self.active_leader_count,
            self.active_task_count,
            self.pending_intents,
            self.reservations,
            self.stale_locks,
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        for timestamp in (self.last_reconciliation, self.last_source_event):
            if timestamp is not None:
                ContractValidation.require_aware_datetime(timestamp)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class CapabilitiesView:
    """Explicitly composed control-plane capabilities."""

    capabilities: tuple[str, ...]
    observed_at: datetime

    def __post_init__(self) -> None:
        """Reject duplicate or unbounded capability claims."""

        if (
            not isinstance(self.capabilities, tuple)
            or len(self.capabilities) > 64
            or len(set(self.capabilities)) != len(self.capabilities)
        ):
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        for capability in self.capabilities:
            ContractValidation.require_text(capability)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class SystemStatusView:
    """System identity paired with authoritative readiness state."""

    system: SystemView
    readiness: ReadinessView


@dataclass(frozen=True, slots=True)
class TaskView:
    """Safe task identity, lifecycle, and revision projection."""

    task_id: str
    name: str
    leader_wallet: str
    follower_account: str
    status: str
    monitoring_mode: str
    sizing_mode: str
    version: int
    notifications_muted: bool

    def __post_init__(self) -> None:
        """Validate the safe task projection."""

        for value in (
            self.task_id,
            self.name,
            self.leader_wallet,
            self.follower_account,
            self.status,
            self.monitoring_mode,
            self.sizing_mode,
        ):
            ContractValidation.require_text(value)
        ContractValidation.require_revision(self.version)

    @classmethod
    def from_domain(cls, task: CopyTask) -> TaskView:
        """Project an immutable domain task without financial float conversion."""

        return cls(
            task_id=task.task_id,
            name=task.name,
            leader_wallet=task.leader_wallet,
            follower_account=task.follower_account,
            status=task.status.value,
            monitoring_mode=task.monitoring_mode.value,
            sizing_mode=task.sizing_mode,
            version=task.version,
            notifications_muted=task.notifications_muted,
        )


@dataclass(frozen=True, slots=True)
class TaskTriggerView:
    """Exact task source-notional eligibility projection."""

    enabled: bool
    min_notional: str
    max_notional: str

    def __post_init__(self) -> None:
        """Validate trigger decimal strings."""

        ContractValidation.require_decimal_string(self.min_notional)
        ContractValidation.require_decimal_string(self.max_notional)


@dataclass(frozen=True, slots=True)
class TaskSpendView:
    """Exact task spend and duplicate-entry policy projection."""

    enabled: bool
    max_total: str
    max_daily: str
    max_per_market: str
    one_entry_per_market: bool
    global_one_entry_per_market: bool
    one_entry_per_sports_type: bool

    def __post_init__(self) -> None:
        """Validate spend decimal strings."""

        for value in (self.max_total, self.max_daily, self.max_per_market):
            ContractValidation.require_decimal_string(value)


@dataclass(frozen=True, slots=True)
class TaskPriceView:
    """Exact task source-price and execution-protection projection."""

    enabled: bool
    min_price: str
    max_price: str
    offset_mode: str
    offset_direction: str
    offset: str
    max_slippage_percent: str

    def __post_init__(self) -> None:
        """Validate price policy fields."""

        for value in (
            self.min_price,
            self.max_price,
            self.offset,
            self.max_slippage_percent,
        ):
            ContractValidation.require_decimal_string(value)
        ContractValidation.require_text(self.offset_mode)
        ContractValidation.require_text(self.offset_direction)


@dataclass(frozen=True, slots=True)
class TaskReactionView:
    """Task source-action reaction policy projection."""

    enabled: bool
    follow_buys: bool
    copy_sells: bool
    buy_on_sells: bool
    counter_trade: bool
    copy_side: str
    copy_merges: bool
    copy_splits: bool
    copy_conversions: bool

    def __post_init__(self) -> None:
        """Validate the side filter."""

        ContractValidation.require_text(self.copy_side)


@dataclass(frozen=True, slots=True)
class TaskMarketFilterView:
    """Task metadata-filter projection."""

    enabled: bool
    include_keywords: tuple[str, ...]
    exclude_keywords: tuple[str, ...]
    categories: tuple[str, ...]
    tokens: tuple[str, ...]
    timeframes: tuple[str, ...]
    min_liquidity_usdc: str
    min_volume_24h_usdc: str
    max_days_until_close: str
    allow_expired_unresolved: bool

    def __post_init__(self) -> None:
        """Validate exact numeric filters and bounded text collections."""

        for values in (
            self.include_keywords,
            self.exclude_keywords,
            self.categories,
            self.tokens,
            self.timeframes,
        ):
            if not isinstance(values, tuple) or len(values) > 256:
                raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
            for value in values:
                ContractValidation.require_text(value)
        for value in (
            self.min_liquidity_usdc,
            self.min_volume_24h_usdc,
            self.max_days_until_close,
        ):
            ContractValidation.require_decimal_string(value)


@dataclass(frozen=True, slots=True)
class TaskScheduleView:
    """Task schedule projection using IANA timezone and ISO local times."""

    enabled: bool
    timezone_name: str
    start: str
    end: str

    def __post_init__(self) -> None:
        """Validate schedule text."""

        for value in (self.timezone_name, self.start, self.end):
            ContractValidation.require_text(value)


@dataclass(frozen=True, slots=True)
class TaskSizingView:
    """Exact per-task sizing policy projection."""

    enabled: bool
    mode: str
    percentage: str
    fixed_amount: str
    trade_multiplier: str
    weighted_multiplier: str
    buy_min_amount: str
    buy_max_amount: str
    copyable_balance_percent: str
    proportional_balance_cap_enabled: bool
    require_leader_balance: bool
    strict_fixed_amount: bool
    strict_exact_mirror: bool

    def __post_init__(self) -> None:
        """Validate sizing mode and exact financial values."""

        ContractValidation.require_text(self.mode)
        for value in (
            self.percentage,
            self.fixed_amount,
            self.trade_multiplier,
            self.weighted_multiplier,
            self.buy_min_amount,
            self.buy_max_amount,
            self.copyable_balance_percent,
        ):
            ContractValidation.require_decimal_string(value)


@dataclass(frozen=True, slots=True)
class TaskConfigurationView:
    """Complete typed configuration projection for one copy task."""

    trigger: TaskTriggerView
    spend: TaskSpendView
    price: TaskPriceView
    reaction: TaskReactionView
    market_filter: TaskMarketFilterView
    schedule: TaskScheduleView
    sizing: TaskSizingView


@dataclass(frozen=True, slots=True)
class TaskDetailView:
    """Task summary and complete typed configuration projection."""

    task: TaskView
    configuration: TaskConfigurationView

    @classmethod
    def from_domain(cls, task: CopyTask) -> TaskDetailView:
        """Project all task policies without converting decimals to floats."""

        return cls(
            task=TaskView.from_domain(task),
            configuration=TaskConfigurationView(
                trigger=TaskTriggerView(
                    enabled=task.trigger.enabled,
                    min_notional=format(task.trigger.min_notional, "f"),
                    max_notional=format(task.trigger.max_notional, "f"),
                ),
                spend=TaskSpendView(
                    enabled=task.spend.enabled,
                    max_total=format(task.spend.max_total, "f"),
                    max_daily=format(task.spend.max_daily, "f"),
                    max_per_market=format(task.spend.max_per_market, "f"),
                    one_entry_per_market=task.spend.one_entry_per_market,
                    global_one_entry_per_market=task.spend.global_one_entry_per_market,
                    one_entry_per_sports_type=task.spend.one_entry_per_sports_type,
                ),
                price=TaskPriceView(
                    enabled=task.price.enabled,
                    min_price=format(task.price.min_price, "f"),
                    max_price=format(task.price.max_price, "f"),
                    offset_mode=task.price.offset_mode.value,
                    offset_direction=task.price.offset_direction.value,
                    offset=format(task.price.offset, "f"),
                    max_slippage_percent=format(task.price.max_slippage_percent, "f"),
                ),
                reaction=TaskReactionView(
                    enabled=task.reaction.enabled,
                    follow_buys=task.reaction.follow_buys,
                    copy_sells=task.reaction.copy_sells,
                    buy_on_sells=task.reaction.buy_on_sells,
                    counter_trade=task.reaction.counter_trade,
                    copy_side=task.reaction.copy_side.value,
                    copy_merges=task.reaction.copy_merges,
                    copy_splits=task.reaction.copy_splits,
                    copy_conversions=task.reaction.copy_conversions,
                ),
                market_filter=TaskMarketFilterView(
                    enabled=task.market_filter.enabled,
                    include_keywords=task.market_filter.include_keywords,
                    exclude_keywords=task.market_filter.exclude_keywords,
                    categories=task.market_filter.categories,
                    tokens=task.market_filter.tokens,
                    timeframes=task.market_filter.timeframes,
                    min_liquidity_usdc=format(task.market_filter.min_liquidity_usdc, "f"),
                    min_volume_24h_usdc=format(task.market_filter.min_volume_24h_usdc, "f"),
                    max_days_until_close=format(task.market_filter.max_days_until_close, "f"),
                    allow_expired_unresolved=task.market_filter.allow_expired_unresolved,
                ),
                schedule=TaskScheduleView(
                    enabled=task.schedule.enabled,
                    timezone_name=task.schedule.timezone_name,
                    start=task.schedule.start.isoformat(),
                    end=task.schedule.end.isoformat(),
                ),
                sizing=TaskSizingView(
                    enabled=task.sizing.enabled,
                    mode=task.sizing.mode.value,
                    percentage=format(task.sizing.percentage, "f"),
                    fixed_amount=format(task.sizing.fixed_amount, "f"),
                    trade_multiplier=format(task.sizing.trade_multiplier, "f"),
                    weighted_multiplier=format(task.sizing.weighted_multiplier, "f"),
                    buy_min_amount=format(task.sizing.buy_min_amount, "f"),
                    buy_max_amount=format(task.sizing.buy_max_amount, "f"),
                    copyable_balance_percent=format(task.sizing.copyable_balance_percent, "f"),
                    proportional_balance_cap_enabled=(task.sizing.proportional_balance_cap_enabled),
                    require_leader_balance=task.sizing.require_leader_balance,
                    strict_fixed_amount=task.sizing.strict_fixed_amount,
                    strict_exact_mirror=task.sizing.strict_exact_mirror,
                ),
            ),
        )


@dataclass(frozen=True, slots=True)
class PositionView:
    """Open-position projection with decimal strings."""

    follower_account: str
    condition_id: str
    token_id: str
    leader_wallet: str | None
    size: str
    avg_entry_price: str
    updated_at: datetime

    def __post_init__(self) -> None:
        """Validate position identity, money, and timestamp fields."""

        for value in (self.follower_account, self.condition_id, self.token_id):
            ContractValidation.require_text(value)
        if self.leader_wallet is not None:
            ContractValidation.require_text(self.leader_wallet)
        ContractValidation.require_decimal_string(self.size)
        ContractValidation.require_decimal_string(self.avg_entry_price)
        ContractValidation.require_aware_datetime(self.updated_at)


@dataclass(frozen=True, slots=True)
class OrderView:
    """Sanitized order projection without raw request or response payloads."""

    idempotency_key: str
    follower_account: str
    leader_wallet: str | None
    order_id: str | None
    status: str
    side: str | None
    amount: str | None
    price_limit: str | None
    matched_total: str
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        """Validate order projection fields."""

        for value in (self.idempotency_key, self.follower_account, self.status):
            ContractValidation.require_text(value)
        for optional_value in (self.leader_wallet, self.order_id, self.side):
            if optional_value is not None:
                ContractValidation.require_text(optional_value)
        ContractValidation.optional_decimal_string(self.amount)
        ContractValidation.optional_decimal_string(self.price_limit)
        ContractValidation.require_decimal_string(self.matched_total)
        ContractValidation.require_aware_datetime(self.created_at)
        ContractValidation.require_aware_datetime(self.updated_at)

    @property
    def size(self) -> str | None:
        """Return the legacy display alias for canonical order amount."""

        return self.amount

    @property
    def price(self) -> str | None:
        """Return the legacy display alias for canonical explicit price limit."""

        return self.price_limit


@dataclass(frozen=True, slots=True)
class ActivityView:
    """Append-only operator activity projection."""

    activity_id: str
    actor: str
    action: str
    target_type: str
    target_id: str
    outcome: str
    reason_code: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        """Validate activity fields."""

        for value in (
            self.activity_id,
            self.actor,
            self.action,
            self.target_type,
            self.target_id,
            self.outcome,
            self.reason_code,
        ):
            ContractValidation.require_text(value)
        ContractValidation.require_aware_datetime(self.occurred_at)


@dataclass(frozen=True, slots=True)
class DiscoveryCycleView:
    """Discovery-cycle state without provider payloads or raw errors."""

    cycle_id: str
    status: str
    started_at: datetime
    completed_at: datetime | None
    candidate_count: int
    kept_count: int
    config_revision: str

    def __post_init__(self) -> None:
        """Validate the discovery projection."""

        for value in (self.cycle_id, self.status, self.config_revision):
            ContractValidation.require_text(value)
        ContractValidation.require_aware_datetime(self.started_at)
        if self.completed_at is not None:
            ContractValidation.require_aware_datetime(self.completed_at)
        if self.candidate_count < 0 or self.kept_count < 0:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)


@dataclass(frozen=True, slots=True)
class DiscoveryStatusView:
    """Current discovery scheduler state without provider error material."""

    state: str
    current_run_id: str | None
    last_cycle_id: str | None
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate scheduler state fields."""

        ContractValidation.require_text(self.state)
        for value in (self.current_run_id, self.last_cycle_id):
            if value is not None:
                ContractValidation.require_text(value)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class DiscoveryCandidateView:
    """Provider-safe candidate score projection."""

    wallet: str
    score: str
    accepted: bool
    reason_code: str
    cycle_id: str

    def __post_init__(self) -> None:
        """Validate candidate identity and exact score text."""

        ContractValidation.require_text(self.wallet)
        ContractValidation.require_decimal_string(self.score)
        ContractValidation.require_text(self.reason_code)
        ContractValidation.require_text(self.cycle_id)


@dataclass(frozen=True, slots=True)
class DiscoveryRosterView:
    """Active or inactive discovery roster projection."""

    wallet: str
    tier: str
    active: bool
    score: str
    updated_at: datetime

    def __post_init__(self) -> None:
        """Validate roster fields."""

        ContractValidation.require_text(self.wallet)
        ContractValidation.require_text(self.tier)
        ContractValidation.require_decimal_string(self.score)
        ContractValidation.require_aware_datetime(self.updated_at)


@dataclass(frozen=True, slots=True)
class ReservationView:
    """Task-budget reservation projection with exact notional text."""

    reservation_id: str
    follower_account: str
    task_id: str
    market_id: str
    amount: str
    status: str
    reserved_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        """Validate reservation fields."""

        for value in (
            self.reservation_id,
            self.follower_account,
            self.task_id,
            self.market_id,
            self.status,
        ):
            ContractValidation.require_text(value)
        ContractValidation.require_decimal_string(self.amount)
        ContractValidation.require_aware_datetime(self.reserved_at)
        ContractValidation.require_aware_datetime(self.expires_at)


@dataclass(frozen=True, slots=True)
class ReentryLockView:
    """Protective-exit lock projection carrying a CAS revision."""

    follower_account: str
    task_id: str
    leader_wallet: str
    market_id: str
    reason: str
    fill_id: str
    locked_at: datetime
    revision: int
    lock_id: str = ""

    def __post_init__(self) -> None:
        """Validate lock fields."""

        for value in (
            self.follower_account,
            self.task_id,
            self.leader_wallet,
            self.market_id,
            self.reason,
            self.fill_id,
        ):
            ContractValidation.require_text(value)
        ContractValidation.require_aware_datetime(self.locked_at)
        ContractValidation.require_revision(self.revision)
        expected_lock_id = self.lock_id_for(
            follower_account=self.follower_account,
            task_id=self.task_id,
            leader_wallet=self.leader_wallet,
            market_id=self.market_id,
        )
        if self.lock_id and self.lock_id != expected_lock_id:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        object.__setattr__(self, "lock_id", expected_lock_id)

    @staticmethod
    def lock_id_for(
        *,
        follower_account: str,
        task_id: str,
        leader_wallet: str,
        market_id: str,
    ) -> str:
        """Return a stable opaque identifier for one complete lock identity.

        Args:
            follower_account: Follower account owning the lock.
            task_id: Copy task that created the lock.
            leader_wallet: Normalized leader wallet identity.
            market_id: Locked market identity.

        Returns:
            A fixed-width SHA-256 resource identifier safe for URL paths.
        """

        values = (follower_account, task_id, leader_wallet, market_id)
        for value in values:
            ContractValidation.require_text(value)
        material = "\x1f".join(
            (follower_account, task_id, leader_wallet.casefold(), market_id)
        ).encode("utf-8")
        return f"lock_{hashlib.sha256(material).hexdigest()}"


@dataclass(frozen=True, slots=True)
class PortfolioView:
    """Cached portfolio snapshot using exact decimal strings."""

    follower_account: str
    cash_usdc: str
    reserved_usdc: str
    positions_value_usdc: str
    equity_usdc: str
    observed_at: datetime
    stale: bool
    copyable_balance_usdc: str = "0"
    position_count: int = 0
    valuation_basis: str = "cost_basis"
    unrealized_pnl_usdc: str | None = None

    def __post_init__(self) -> None:
        """Validate cached portfolio fields."""

        ContractValidation.require_text(self.follower_account)
        for value in (
            self.cash_usdc,
            self.reserved_usdc,
            self.positions_value_usdc,
            self.equity_usdc,
            self.copyable_balance_usdc,
        ):
            ContractValidation.require_decimal_string(value)
        if self.position_count < 0:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        if self.valuation_basis not in {"cost_basis", "market_mark"}:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.optional_decimal_string(self.unrealized_pnl_usdc)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class AccountView:
    """One composed follower account and the leaders bound to it."""

    follower_account: str
    wallet_address: str
    leader_count: int

    def __post_init__(self) -> None:
        """Validate account identity and a non-negative leader count."""

        ContractValidation.require_text(self.follower_account)
        ContractValidation.require_text(self.wallet_address)
        ContractValidation.require_count(self.leader_count)


@dataclass(frozen=True, slots=True)
class DetectionSourceStatusView:
    """Local signal counters for exactly one detection source."""

    source: str
    signals_total: int
    deduplicated_total: int
    last_signal_at: datetime | None

    def __post_init__(self) -> None:
        """Validate the source code, counters, and optional timestamp."""

        ContractValidation.require_text(self.source)
        ContractValidation.require_count(self.signals_total)
        ContractValidation.require_count(self.deduplicated_total)
        if self.last_signal_at is not None:
            ContractValidation.require_aware_datetime(self.last_signal_at)


@dataclass(frozen=True, slots=True)
class PendingConfirmationStatsView:
    """Bounded state of the provisional-copy confirmation tracker."""

    tracked: int
    running: bool
    deadline_seconds: float
    max_tracked: int

    def __post_init__(self) -> None:
        """Validate tracker counts and a finite positive deadline."""

        ContractValidation.require_count(self.tracked)
        ContractValidation.require_count(self.max_tracked)
        if not math.isfinite(self.deadline_seconds) or self.deadline_seconds <= 0:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)


@dataclass(frozen=True, slots=True)
class DetectionStatusView:
    """Detection-source health assembled from local counters and readiness.

    ``leader_ws_connected`` is ``None`` when no detection transport probe is
    attached, which is different from a probed disconnected transport.
    """

    mode: str
    sources: tuple[DetectionSourceStatusView, ...]
    leader_ws_connected: bool | None
    market_ws_ok: bool
    user_ws_ok: bool
    market_ws_reconnects: int
    user_ws_reconnects: int
    pending_confirmations: PendingConfirmationStatsView | None
    watched_leader_count: int
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate the mode, bounded source list, and counters."""

        ContractValidation.require_text(self.mode)
        if not isinstance(self.sources, tuple) or len(self.sources) > 32:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_count(self.market_ws_reconnects)
        ContractValidation.require_count(self.user_ws_reconnects)
        ContractValidation.require_count(self.watched_leader_count)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class FillView:
    """One durable fill projection using exact decimal strings."""

    fill_id: str
    follower_account: str
    leader_wallet: str | None
    order_id: str | None
    condition_id: str | None
    token_id: str | None
    side: str | None
    size: str
    price: str
    fee: str
    filled_at: datetime
    created_at: datetime

    def __post_init__(self) -> None:
        """Validate identity, optional attribution, and exact money text."""

        ContractValidation.require_text(self.fill_id)
        ContractValidation.require_text(self.follower_account)
        for optional_value in (
            self.leader_wallet,
            self.order_id,
            self.condition_id,
            self.token_id,
            self.side,
        ):
            if optional_value is not None:
                ContractValidation.require_text(optional_value)
        for value in (self.size, self.price, self.fee):
            ContractValidation.require_decimal_string(value)
        ContractValidation.require_aware_datetime(self.filled_at)
        ContractValidation.require_aware_datetime(self.created_at)


@dataclass(frozen=True, slots=True)
class PnlSummaryView:
    """Session-scoped realized ledger metrics for one follower account.

    Every value comes from the in-memory :class:`~services.pnlTracker.PnLTracker`
    ledger, so it covers this process session only and is reset by a restart.
    """

    realized_pnl_usdc: str
    total_invested_usdc: str
    total_returned_usdc: str
    max_drawdown_usdc: str
    trade_count: int
    buy_count: int
    sell_count: int
    win_count: int
    loss_count: int
    session_started_at: datetime

    def __post_init__(self) -> None:
        """Validate exact ledger money text and non-negative counts."""

        for value in (
            self.realized_pnl_usdc,
            self.total_invested_usdc,
            self.total_returned_usdc,
            self.max_drawdown_usdc,
        ):
            ContractValidation.require_decimal_string(value)
        for count in (
            self.trade_count,
            self.buy_count,
            self.sell_count,
            self.win_count,
            self.loss_count,
        ):
            ContractValidation.require_count(count)
        ContractValidation.require_aware_datetime(self.session_started_at)


@dataclass(frozen=True, slots=True)
class PnlPositionView:
    """One open position valued only from a fresh cached WSS mid quote.

    ``mark_price``, ``market_value_usdc``, and ``unrealized_pnl_usdc`` stay
    ``None`` whenever no fresh two-sided quote exists. The control plane never
    substitutes a cost basis, a stale book, or a venue call for a mark.
    """

    condition_id: str
    token_id: str
    leader_wallet: str | None
    size: str
    avg_entry_price: str
    cost_basis_usdc: str
    mark_price: str | None
    market_value_usdc: str | None
    unrealized_pnl_usdc: str | None

    def __post_init__(self) -> None:
        """Validate identity plus exact required and optional money text."""

        ContractValidation.require_text(self.condition_id)
        ContractValidation.require_text(self.token_id)
        if self.leader_wallet is not None:
            ContractValidation.require_text(self.leader_wallet)
        for value in (self.size, self.avg_entry_price, self.cost_basis_usdc):
            ContractValidation.require_decimal_string(value)
        for optional_value in (
            self.mark_price,
            self.market_value_usdc,
            self.unrealized_pnl_usdc,
        ):
            ContractValidation.optional_decimal_string(optional_value)


@dataclass(frozen=True, slots=True)
class PnlView:
    """Realized session PnL paired with honestly marked open positions.

    ``unrealized_pnl_usdc`` is published only when every open position carries
    a fresh mark; a partially marked book reports ``None`` with
    ``unrealized_basis == "partial_wss_mid"``.
    """

    follower_account: str
    realized: PnlSummaryView
    positions: tuple[PnlPositionView, ...]
    position_count: int
    cost_basis_usdc: str
    unrealized_pnl_usdc: str | None
    unrealized_basis: str
    marked_position_count: int
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate the valuation basis, bounded positions, and money text."""

        ContractValidation.require_text(self.follower_account)
        if not isinstance(self.positions, tuple) or len(self.positions) > 500:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_count(self.position_count)
        ContractValidation.require_count(self.marked_position_count)
        ContractValidation.require_decimal_string(self.cost_basis_usdc)
        ContractValidation.optional_decimal_string(self.unrealized_pnl_usdc)
        if self.unrealized_basis not in PNL_VALUATION_BASES:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class RiskPolicyView:
    """Effective account-level risk limits as configured for this process."""

    name: str
    risk_management_enabled: bool
    tiered_policy_enabled: bool
    kill_switch: bool
    max_open_positions: int
    max_market_exposure_usdc: str
    max_category_exposure_usdc: str
    max_leader_exposure_usdc: str
    max_daily_drawdown_pct: str
    reserve_pct: str
    reserve_floor_usdc: str
    hard_cap_pct: str
    hard_cap_ceiling_usdc: str
    allow_partial_exits: bool
    allow_pyramiding: bool

    def __post_init__(self) -> None:
        """Validate the policy name, the position cap, and exact limits."""

        ContractValidation.require_text(self.name)
        ContractValidation.require_count(self.max_open_positions)
        for value in (
            self.max_market_exposure_usdc,
            self.max_category_exposure_usdc,
            self.max_leader_exposure_usdc,
            self.max_daily_drawdown_pct,
            self.reserve_pct,
            self.reserve_floor_usdc,
            self.hard_cap_pct,
            self.hard_cap_ceiling_usdc,
        ):
            ContractValidation.require_decimal_string(value)


@dataclass(frozen=True, slots=True)
class RiskBlockView:
    """One active durable risk block on a follower position."""

    follower_account: str
    condition_id: str
    token_id: str
    reason: str
    source: str
    leader_wallet: str | None
    triggered_at: datetime
    expires_at: datetime | None

    def __post_init__(self) -> None:
        """Validate block identity, reason vocabulary, and timestamps."""

        for value in (
            self.follower_account,
            self.condition_id,
            self.token_id,
            self.reason,
            self.source,
        ):
            ContractValidation.require_text(value)
        if self.leader_wallet is not None:
            ContractValidation.require_text(self.leader_wallet)
        ContractValidation.require_aware_datetime(self.triggered_at)
        if self.expires_at is not None:
            ContractValidation.require_aware_datetime(self.expires_at)


@dataclass(frozen=True, slots=True)
class RiskTriggerView:
    """One recorded protective-exit trigger and its fill status."""

    trigger_id: str
    follower_account: str
    condition_id: str
    token_id: str
    reason: str
    source: str
    fill_status: str
    fill_order_id: str | None
    triggered_at: datetime

    def __post_init__(self) -> None:
        """Validate trigger identity, status vocabulary, and timestamp."""

        for value in (
            self.trigger_id,
            self.follower_account,
            self.condition_id,
            self.token_id,
            self.reason,
            self.source,
            self.fill_status,
        ):
            ContractValidation.require_text(value)
        if self.fill_order_id is not None:
            ContractValidation.require_text(self.fill_order_id)
        ContractValidation.require_aware_datetime(self.triggered_at)


@dataclass(frozen=True, slots=True)
class RiskHeadroomView:
    """Account-level BUY headroom recomputed from one cached snapshot.

    The projection applies the account cap, reserve, and daily-drawdown gates
    only. Per-market, per-category, and per-leader exposure are unknown to a
    cached portfolio snapshot, so this is an upper bound on the cap the trade
    pipeline computes immediately before a real submission.
    """

    allowed: bool
    reason: str
    policy_name: str
    cap_notional_usdc: str
    equity_usdc: str
    free_cash_usdc: str
    daily_loss_usdc: str
    open_position_count: int

    def __post_init__(self) -> None:
        """Validate the decision vocabulary and exact money inputs."""

        ContractValidation.require_text(self.reason)
        ContractValidation.require_text(self.policy_name)
        for value in (
            self.cap_notional_usdc,
            self.equity_usdc,
            self.free_cash_usdc,
            self.daily_loss_usdc,
        ):
            ContractValidation.require_decimal_string(value)
        ContractValidation.require_count(self.open_position_count)


@dataclass(frozen=True, slots=True)
class RiskStateView:
    """Configured policy, active blocks, recent triggers, and headroom."""

    follower_account: str
    policy: RiskPolicyView
    active_blocks: tuple[RiskBlockView, ...]
    recent_triggers: tuple[RiskTriggerView, ...]
    headroom: RiskHeadroomView | None
    day_start_equity_usdc: str | None
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate bounded collections and the optional day-start equity."""

        for collection in (self.active_blocks, self.recent_triggers):
            if not isinstance(collection, tuple) or len(collection) > 200:
                raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_text(self.follower_account)
        ContractValidation.optional_decimal_string(self.day_start_equity_usdc)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class LeaderView:
    """One leader identity merged from configuration, roster, and counters."""

    wallet: str
    configured: bool
    monitored: bool
    tier: str | None
    score: str | None
    signals_total: int
    deduplicated_total: int
    last_signal_at: datetime | None
    trade_count: int
    open_position_count: int

    def __post_init__(self) -> None:
        """Validate wallet identity, optional roster fields, and counters."""

        ContractValidation.require_text(self.wallet)
        if self.tier is not None:
            ContractValidation.require_text(self.tier)
        ContractValidation.optional_decimal_string(self.score)
        for count in (
            self.signals_total,
            self.deduplicated_total,
            self.trade_count,
            self.open_position_count,
        ):
            ContractValidation.require_count(count)
        if self.last_signal_at is not None:
            ContractValidation.require_aware_datetime(self.last_signal_at)


@dataclass(frozen=True, slots=True)
class EntryControlState:
    """Durable global entry-pause and full-stop state."""

    entries_paused: bool
    full_stop: bool
    revision: int
    reason_code: str
    updated_at: datetime
    updated_by: str

    def __post_init__(self) -> None:
        """Validate runtime safety state."""

        ContractValidation.require_revision(self.revision)
        ContractValidation.require_text(self.reason_code)
        ContractValidation.require_aware_datetime(self.updated_at)
        ContractValidation.require_text(self.updated_by)
        if self.full_stop and not self.entries_paused:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)


@dataclass(frozen=True, slots=True)
class RuntimeView:
    """Bounded local runtime state."""

    entry_control: EntryControlState
    worker_count: int
    queue_depth: int
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate non-negative runtime counts."""

        if self.worker_count < 0 or self.queue_depth < 0:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class AuditEvent:
    """Minimal append-only audit event with allowlisted scalar detail."""

    event_id: str
    actor: str
    action: str
    target_type: str
    target_id: str
    outcome: str
    reason_code: str
    occurred_at: datetime
    follower_account: str | None = None
    details: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """Validate identifiers and bounded immutable details."""

        for value in (
            self.event_id,
            self.actor,
            self.action,
            self.target_type,
            self.target_id,
            self.outcome,
            self.reason_code,
        ):
            ContractValidation.require_text(value)
        ContractValidation.require_aware_datetime(self.occurred_at)
        if self.follower_account is not None:
            ContractValidation.require_text(self.follower_account)
        if len(self.details) > 16:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        forbidden = ("secret", "token", "password", "private_key", "credential")
        for key, value in self.details:
            normalized = ContractValidation.require_text(key).casefold()
            ContractValidation.require_text(value)
            if any(fragment in normalized for fragment in forbidden):
                raise ControlPlaneError(ControlPlaneReason.SETTINGS_SECRET_FORBIDDEN)

    @classmethod
    def record(
        cls,
        *,
        actor: str,
        action: str,
        target_type: str,
        target_id: str,
        outcome: str,
        reason_code: str,
        occurred_at: datetime,
        follower_account: str | None = None,
        details: tuple[tuple[str, str], ...] = (),
    ) -> AuditEvent:
        """Create one event with a process-unique opaque identity."""

        return cls(
            event_id=str(uuid4()),
            actor=actor,
            action=action,
            target_type=target_type,
            target_id=target_id,
            outcome=outcome,
            reason_code=reason_code,
            occurred_at=occurred_at,
            follower_account=follower_account,
            details=details,
        )


@dataclass(frozen=True, slots=True)
class TaskCreateCommand:
    """Request creation of a paused copy task."""

    task: CopyTask
    actor: str

    def __post_init__(self) -> None:
        """Validate the operator identity."""

        ContractValidation.require_text(self.actor)


@dataclass(frozen=True, slots=True)
class TaskDuplicateCommand:
    """Request a paused duplicate without copying runtime state."""

    source_task_id: str
    new_task_id: str
    new_name: str
    actor: str

    def __post_init__(self) -> None:
        """Validate duplicate identifiers."""

        for value in (self.source_task_id, self.new_task_id, self.new_name, self.actor):
            ContractValidation.require_text(value)


@dataclass(frozen=True, slots=True)
class TaskVersionCommand:
    """Optimistic-concurrency task lifecycle request."""

    task_id: str
    expected_version: int
    actor: str

    def __post_init__(self) -> None:
        """Validate task identity, revision, and actor."""

        ContractValidation.require_text(self.task_id)
        ContractValidation.require_revision(self.expected_version)
        ContractValidation.require_text(self.actor)


@dataclass(frozen=True, slots=True)
class TaskUpdateCommand:
    """Versioned safe-configuration update for one task."""

    task: CopyTask
    expected_version: int
    actor: str

    def __post_init__(self) -> None:
        """Validate update revision and actor."""

        ContractValidation.require_revision(self.expected_version)
        ContractValidation.require_text(self.actor)


class TaskAction(StrEnum):
    """Mutations supported by the atomic task bulk interface."""

    PAUSE = "pause"
    RESUME = "resume"
    DELETE = "delete"


@dataclass(frozen=True, slots=True)
class BulkTaskItem:
    """One versioned item inside an atomic bulk request."""

    task_id: str
    action: TaskAction
    expected_version: int

    def __post_init__(self) -> None:
        """Validate one bulk item."""

        ContractValidation.require_text(self.task_id)
        ContractValidation.require_revision(self.expected_version)


@dataclass(frozen=True, slots=True)
class BulkTaskCommand:
    """All-or-none task mutation batch validated before persistence."""

    items: tuple[BulkTaskItem, ...]
    actor: str

    def __post_init__(self) -> None:
        """Validate the immutable collection shape and actor."""

        if not isinstance(self.items, tuple):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        ContractValidation.require_text(self.actor)


@dataclass(frozen=True, slots=True)
class BulkTaskResult:
    """Successful atomic task mutation result."""

    tasks: tuple[TaskView, ...]
    atomic: bool = True


@dataclass(frozen=True, slots=True)
class DiscoveryRunCommand:
    """Asynchronous discovery job request."""

    actor: str
    force: bool = False

    def __post_init__(self) -> None:
        """Validate the requesting actor."""

        ContractValidation.require_text(self.actor)


@dataclass(frozen=True, slots=True)
class DiscoveryRunReceipt:
    """Immediate queue acknowledgement for a discovery request."""

    run_id: str
    accepted: bool
    reason_code: str

    def __post_init__(self) -> None:
        """Validate stable receipt fields."""

        ContractValidation.require_text(self.run_id)
        ContractValidation.require_text(self.reason_code)


@dataclass(frozen=True, slots=True)
class RuntimeControlCommand:
    """Versioned entry-pause or full-stop request."""

    expected_revision: int
    actor: str
    reason_code: str

    def __post_init__(self) -> None:
        """Validate runtime command fields."""

        ContractValidation.require_revision(self.expected_revision)
        ContractValidation.require_text(self.actor)
        ContractValidation.require_text(self.reason_code)


@dataclass(frozen=True, slots=True)
class LockResetCommand:
    """Versioned manual reset for one protective re-entry lock."""

    follower_account: str
    task_id: str
    leader_wallet: str
    market_id: str
    expected_revision: int
    actor: str
    reason_code: str

    def __post_init__(self) -> None:
        """Validate the exact lock identity and CAS revision."""

        for value in (
            self.follower_account,
            self.task_id,
            self.leader_wallet,
            self.market_id,
            self.actor,
            self.reason_code,
        ):
            ContractValidation.require_text(value)
        ContractValidation.require_revision(self.expected_revision)


class LockResetOutcome(StrEnum):
    """Stable repository outcomes for a lock-reset CAS operation."""

    RESET = "reset"
    NOT_FOUND = "not_found"
    REVISION_MISMATCH = "revision_mismatch"


@dataclass(frozen=True, slots=True)
class LockResetResult:
    """Result of a lock reset attempt."""

    outcome: LockResetOutcome
    released_at: datetime | None

    def __post_init__(self) -> None:
        """Validate an optional release timestamp."""

        if self.released_at is not None:
            ContractValidation.require_aware_datetime(self.released_at)


class SettingsValues(Mapping[str, object]):
    """Deeply immutable settings mapping with a redacted representation."""

    __slots__ = ("_items",)

    def __init__(self, items: tuple[tuple[str, object], ...]) -> None:
        """Store already-frozen key/value pairs in deterministic order."""

        self._items = items

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> SettingsValues:
        """Defensively copy and recursively freeze one mapping."""

        if not isinstance(values, Mapping):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        frozen: list[tuple[str, object]] = []
        for raw_key, raw_value in sorted(values.items(), key=lambda item: str(item[0])):
            if not isinstance(raw_key, str) or not raw_key.strip():
                raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID)
            frozen.append((raw_key, cls._freeze(raw_value)))
        return cls(tuple(frozen))

    @classmethod
    def _freeze(cls, value: object) -> object:
        """Freeze supported YAML-safe structures without retaining aliases."""

        if isinstance(value, Mapping):
            return cls.from_mapping(value)
        if isinstance(value, (list, tuple)):
            return tuple(cls._freeze(item) for item in value)
        if value is None or isinstance(value, (str, bool, int)):
            return value
        if isinstance(value, float) and math.isfinite(value):
            return value
        raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID)

    @classmethod
    def _thaw(cls, value: object) -> object:
        """Return a fresh YAML-safe mutable value for validation or dumping."""

        if isinstance(value, cls):
            return value.to_dict()
        if isinstance(value, tuple):
            return [cls._thaw(item) for item in value]
        return value

    def to_dict(self) -> dict[str, object]:
        """Return a fresh mutable mapping without exposing internal state."""

        return {key: self._thaw(value) for key, value in self._items}

    def __getitem__(self, key: str) -> object:
        """Return one immutable value by key."""

        for candidate, value in self._items:
            if candidate == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        """Iterate keys in deterministic order."""

        return (key for key, _ in self._items)

    def __len__(self) -> int:
        """Return the number of top-level keys."""

        return len(self._items)

    def __repr__(self) -> str:
        """Render keys only so command objects cannot reveal pending secrets."""

        return f"SettingsValues(keys={tuple(self)!r})"


@dataclass(frozen=True, slots=True)
class SettingsPath:
    """Allowlisted environment and service-section identity."""

    environment: str
    section: str

    def __post_init__(self) -> None:
        """Reject traversal and platform separator material."""

        pattern = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
        if not isinstance(self.environment, str) or not pattern.fullmatch(self.environment):
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_PATH_FORBIDDEN)
        if not isinstance(self.section, str) or not pattern.fullmatch(self.section):
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_PATH_FORBIDDEN)


@dataclass(frozen=True, slots=True)
class SettingsDocument:
    """Secret-free immutable settings document and content revision."""

    path: SettingsPath
    values: SettingsValues
    revision: str

    def __post_init__(self) -> None:
        """Validate a SHA-256 revision."""

        if not re.fullmatch(r"[0-9a-f]{64}", self.revision):
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)


@dataclass(frozen=True, slots=True)
class SettingsWriteCommand:
    """Revision-guarded replacement of one environment service section."""

    path: SettingsPath
    values: SettingsValues = field(repr=False)
    expected_revision: str
    actor: str

    def __post_init__(self) -> None:
        """Validate the expected SHA-256 revision and actor."""

        if not re.fullmatch(r"[0-9a-f]{64}", self.expected_revision):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        ContractValidation.require_text(self.actor)

    @classmethod
    def from_mapping(
        cls,
        *,
        path: SettingsPath,
        values: Mapping[str, object],
        expected_revision: str,
        actor: str,
    ) -> SettingsWriteCommand:
        """Defensively freeze an adapter-supplied mapping."""

        return cls(
            path=path,
            values=SettingsValues.from_mapping(values),
            expected_revision=expected_revision,
            actor=actor,
        )


@dataclass(frozen=True, slots=True)
class SettingsApplyMetadata:
    """Truthful write/apply metadata for restart-required settings."""

    previous_revision: str
    revision: str
    applied_at: datetime
    applied_by: str
    restart_required: bool
    live_applied: bool

    def __post_init__(self) -> None:
        """Require restart semantics and valid metadata."""

        for revision in (self.previous_revision, self.revision):
            if not re.fullmatch(r"[0-9a-f]{64}", revision):
                raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_aware_datetime(self.applied_at)
        ContractValidation.require_text(self.applied_by)
        if not self.restart_required or self.live_applied:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)


@dataclass(frozen=True, slots=True)
class SettingsWriteResult:
    """Secret-free written document and truthful apply metadata."""

    document: SettingsDocument
    metadata: SettingsApplyMetadata


__all__ = [
    "AccountView",
    "ActivityView",
    "AuditEvent",
    "BulkTaskCommand",
    "BulkTaskItem",
    "BulkTaskResult",
    "CapabilitiesView",
    "ContractValidation",
    "ControlPlaneError",
    "ControlPlaneReason",
    "DetectionSourceStatusView",
    "DetectionStatusView",
    "DiscoveryCandidateView",
    "DiscoveryCycleView",
    "DiscoveryRosterView",
    "DiscoveryRunCommand",
    "DiscoveryRunReceipt",
    "DiscoveryStatusView",
    "EntryControlState",
    "FillView",
    "LeaderView",
    "LockResetCommand",
    "LockResetOutcome",
    "LockResetResult",
    "OrderView",
    "PNL_VALUATION_BASES",
    "Page",
    "PageRequest",
    "PendingConfirmationStatsView",
    "PnlPositionView",
    "PnlSummaryView",
    "PnlView",
    "PortfolioView",
    "PositionView",
    "ReadinessView",
    "ReentryLockView",
    "ReservationView",
    "RiskBlockView",
    "RiskHeadroomView",
    "RiskPolicyView",
    "RiskStateView",
    "RiskTriggerView",
    "RuntimeControlCommand",
    "RuntimeView",
    "SettingsApplyMetadata",
    "SettingsDocument",
    "SettingsPath",
    "SettingsValues",
    "SettingsWriteCommand",
    "SettingsWriteResult",
    "SystemStatusView",
    "SystemView",
    "TaskAction",
    "TaskConfigurationView",
    "TaskCreateCommand",
    "TaskDetailView",
    "TaskDuplicateCommand",
    "TaskMarketFilterView",
    "TaskPriceView",
    "TaskReactionView",
    "TaskScheduleView",
    "TaskSizingView",
    "TaskSpendView",
    "TaskTriggerView",
    "TaskUpdateCommand",
    "TaskVersionCommand",
    "TaskView",
]
