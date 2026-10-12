"""Immutable domain models for v1 copy-trading tasks.

The module contains configuration and decision data only. It performs no
network, database, or exchange I/O and does not import vendor libraries.
"""

from __future__ import annotations

from pmcopy.services.copy_tasks.protective_exits import ProtectiveRiskPolicy

import re
from dataclasses import dataclass, field, replace
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pmcopy.domain.detection import DetectionFinality

_EVM_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
_ZERO = Decimal("0")
_ONE = Decimal("1")


def _decimal(value: object, *, name: str, minimum: Decimal | None = None) -> Decimal:
    """Return a finite decimal and enforce an optional inclusive minimum."""

    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be decimal-like") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    if minimum is not None and result < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return result


def _normalize_words(values: Iterable[str]) -> tuple[str, ...]:
    """Normalize configured words for deterministic matching."""

    return tuple(sorted({str(value).strip().casefold() for value in values if str(value).strip()}))


class CopyTaskStatus(StrEnum):
    """Persistent lifecycle state of one copy task."""

    ACTIVE = "active"
    PAUSED = "paused"
    DELETED = "deleted"


class MonitoringMode(StrEnum):
    """Source-finality policy used by one task."""

    NORMAL = "normal"
    PRIORITY = "priority"


class CopySideFilter(StrEnum):
    """Outcome labels accepted by the task."""

    BOTH = "both"
    YES_ONLY = "yes_only"
    NO_ONLY = "no_only"


class PriceOffsetMode(StrEnum):
    """How copied-price protection is shifted."""

    DISABLED = "disabled"
    FIXED = "fixed"
    PERCENTAGE = "percentage"


class PriceOffsetDirection(StrEnum):
    """Whether offset improves fill probability or price quality."""

    ABOVE = "above"
    BELOW = "below"


class TaskSizingMode(StrEnum):
    """Canonical per-task BUY sizing modes."""

    FIXED = "fixed"
    EXACT_MIRROR = "exact_mirror"
    EXACT_COPY = "exact_mirror"
    PROPORTIONAL = "proportional"
    PERCENTAGE = "proportional"
    BALANCE_WEIGHTED = "balance_weighted"
    PORTFOLIO_WEIGHTED = "portfolio_weighted"
    ADAPTIVE = "adaptive"
    TIERED = "tiered"


_SIZING_MODE_ALIASES = {
    "fixed": TaskSizingMode.FIXED,
    "fixed_amount": TaskSizingMode.FIXED,
    "fixed_buy_amount": TaskSizingMode.FIXED,
    "exact": TaskSizingMode.EXACT_MIRROR,
    "exact_mirror": TaskSizingMode.EXACT_MIRROR,
    "mirror": TaskSizingMode.EXACT_MIRROR,
    "proportional": TaskSizingMode.PROPORTIONAL,
    "percentage": TaskSizingMode.PROPORTIONAL,
    "percentage_based": TaskSizingMode.PROPORTIONAL,
    "balance": TaskSizingMode.BALANCE_WEIGHTED,
    "balance_weighted": TaskSizingMode.BALANCE_WEIGHTED,
    "portfolio": TaskSizingMode.PORTFOLIO_WEIGHTED,
    "portfolio_weighted": TaskSizingMode.PORTFOLIO_WEIGHTED,
    "adaptive": TaskSizingMode.ADAPTIVE,
    "tiered": TaskSizingMode.TIERED,
}


def _sizing_mode(value: object) -> TaskSizingMode:
    """Normalize one public sizing mode alias."""

    if isinstance(value, TaskSizingMode):
        return value
    normalized = str(value or "proportional").strip().lower().replace("-", "_")
    try:
        return _SIZING_MODE_ALIASES[normalized]
    except KeyError as exc:
        raise ValueError(f"unsupported task sizing mode: {value!r}") from exc


@dataclass(frozen=True, slots=True)
class TaskSizingPolicy:
    """Per-task BUY sizing overrides applied above global risk limits."""

    enabled: bool = True
    mode: TaskSizingMode = TaskSizingMode.PROPORTIONAL
    percentage: Decimal = Decimal("4")
    fixed_amount: Decimal = Decimal("3")
    trade_multiplier: Decimal = Decimal("1")
    weighted_multiplier: Decimal = Decimal("1")
    buy_min_amount: Decimal = Decimal("5")
    buy_max_amount: Decimal = Decimal("8")
    copyable_balance_percent: Decimal = Decimal("75")
    proportional_balance_cap_enabled: bool = True
    require_leader_balance: bool = True
    strict_fixed_amount: bool = True
    strict_exact_mirror: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "mode", _sizing_mode(self.mode))
        for name in (
            "percentage",
            "fixed_amount",
            "trade_multiplier",
            "weighted_multiplier",
            "buy_min_amount",
            "buy_max_amount",
            "copyable_balance_percent",
        ):
            object.__setattr__(
                self,
                name,
                _decimal(getattr(self, name), name=name, minimum=_ZERO),
            )
        if self.trade_multiplier <= 0:
            raise ValueError("trade_multiplier must be positive")
        if self.weighted_multiplier <= 0:
            raise ValueError("weighted_multiplier must be positive")
        if not _ZERO < self.copyable_balance_percent <= Decimal("100"):
            raise ValueError("copyable_balance_percent must be in (0, 100]")
        if self.buy_max_amount > 0 and self.buy_max_amount < self.buy_min_amount:
            raise ValueError("buy_max_amount must be zero/disabled or >= buy_min_amount")
        if self.mode is TaskSizingMode.FIXED and self.fixed_amount <= 0:
            raise ValueError("fixed sizing requires a positive fixed_amount")
        if (
            self.mode
            in {
                TaskSizingMode.PROPORTIONAL,
                TaskSizingMode.ADAPTIVE,
                TaskSizingMode.TIERED,
            }
            and self.percentage <= 0
        ):
            raise ValueError("percentage-based sizing requires a positive percentage")


@dataclass(frozen=True, slots=True)
class TaskSchedule:
    """Recurring IANA-timezone entry window.

    A disabled schedule is always open. Overnight windows are represented by a
    start later than the end, for example 22:00-06:00.
    """

    enabled: bool = False
    timezone_name: str = "UTC"
    start: time = time(0, 0)
    end: time = time(0, 0)

    def __post_init__(self) -> None:
        try:
            ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown schedule timezone: {self.timezone_name}") from exc

    @classmethod
    def from_strings(
        cls,
        *,
        enabled: bool,
        timezone_name: str,
        start: str,
        end: str,
    ) -> TaskSchedule:
        """Build a schedule from strict 24-hour ``HH:MM`` values."""

        try:
            start_time = time.fromisoformat(start)
            end_time = time.fromisoformat(end)
        except ValueError as exc:
            raise ValueError("schedule start/end must use HH:MM") from exc
        return cls(
            enabled=enabled,
            timezone_name=timezone_name,
            start=start_time,
            end=end_time,
        )

    def admits_entry(self, now: datetime) -> bool:
        """Return whether a new entry is allowed at ``now``."""

        if not self.enabled:
            return True
        if now.tzinfo is None:
            raise ValueError("schedule evaluation requires timezone-aware datetime")
        local_time = now.astimezone(ZoneInfo(self.timezone_name)).time().replace(tzinfo=None)
        if self.start == self.end:
            return True
        if self.start < self.end:
            return self.start <= local_time < self.end
        return local_time >= self.start or local_time < self.end


@dataclass(frozen=True, slots=True)
class TaskTriggerPolicy:
    """Allowed source-trade notional range."""

    enabled: bool = True
    min_notional: Decimal = _ZERO
    max_notional: Decimal = _ZERO

    def __post_init__(self) -> None:
        minimum = _decimal(self.min_notional, name="min_notional", minimum=_ZERO)
        maximum = _decimal(self.max_notional, name="max_notional", minimum=_ZERO)
        if maximum > 0 and maximum < minimum:
            raise ValueError("max_notional must be zero/disabled or >= min_notional")
        object.__setattr__(self, "min_notional", minimum)
        object.__setattr__(self, "max_notional", maximum)


@dataclass(frozen=True, slots=True)
class TaskSpendPolicy:
    """Persistent task-level spend and duplication limits."""

    enabled: bool = True
    max_total: Decimal = _ZERO
    max_daily: Decimal = _ZERO
    max_per_market: Decimal = _ZERO
    one_entry_per_market: bool = False
    global_one_entry_per_market: bool = False
    one_entry_per_sports_type: bool = False

    def __post_init__(self) -> None:
        for name in ("max_total", "max_daily", "max_per_market"):
            object.__setattr__(
                self,
                name,
                _decimal(getattr(self, name), name=name, minimum=_ZERO),
            )


@dataclass(frozen=True, slots=True)
class TaskPricePolicy:
    """Source-price eligibility and explicit execution protection."""

    enabled: bool = True
    min_price: Decimal = Decimal("0.20")
    max_price: Decimal = Decimal("0.80")
    offset_mode: PriceOffsetMode = PriceOffsetMode.DISABLED
    offset_direction: PriceOffsetDirection = PriceOffsetDirection.ABOVE
    offset: Decimal = _ZERO
    max_slippage_percent: Decimal = Decimal("5")

    def __post_init__(self) -> None:
        minimum = _decimal(self.min_price, name="min_price", minimum=_ZERO)
        maximum = _decimal(self.max_price, name="max_price", minimum=_ZERO)
        offset = _decimal(self.offset, name="offset", minimum=_ZERO)
        slippage = _decimal(
            self.max_slippage_percent,
            name="max_slippage_percent",
            minimum=_ZERO,
        )
        if not (_ZERO <= minimum <= _ONE and _ZERO <= maximum <= _ONE):
            raise ValueError("price band must be within [0, 1]")
        if minimum > maximum:
            raise ValueError("price band minimum cannot exceed maximum")
        if self.offset_mode is PriceOffsetMode.FIXED and offset > _ONE:
            raise ValueError("fixed price offset must be <= 1")
        object.__setattr__(self, "min_price", minimum)
        object.__setattr__(self, "max_price", maximum)
        object.__setattr__(self, "offset", offset)
        object.__setattr__(self, "max_slippage_percent", slippage)


@dataclass(frozen=True, slots=True)
class ReactionPolicy:
    """How source BUY/SELL events are translated into follower actions."""

    enabled: bool = True
    follow_buys: bool = True
    copy_sells: bool = True
    buy_on_sells: bool = False
    counter_trade: bool = False
    copy_side: CopySideFilter = CopySideFilter.BOTH
    copy_merges: bool = False
    copy_splits: bool = False
    copy_conversions: bool = False


@dataclass(frozen=True, slots=True)
class TaskMarketFilter:
    """Metadata-based task eligibility filters."""

    enabled: bool = True
    include_keywords: tuple[str, ...] = ()
    exclude_keywords: tuple[str, ...] = ()
    categories: tuple[str, ...] = ()
    tokens: tuple[str, ...] = ()
    timeframes: tuple[str, ...] = ()
    min_liquidity_usdc: Decimal = _ZERO
    min_volume_24h_usdc: Decimal = _ZERO
    max_days_until_close: Decimal = _ZERO
    allow_expired_unresolved: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "include_keywords",
            _normalize_words(self.include_keywords),
        )
        object.__setattr__(
            self,
            "exclude_keywords",
            _normalize_words(self.exclude_keywords),
        )
        object.__setattr__(self, "categories", _normalize_words(self.categories))
        object.__setattr__(self, "tokens", _normalize_words(self.tokens))
        object.__setattr__(self, "timeframes", _normalize_words(self.timeframes))
        for name in ("min_liquidity_usdc", "min_volume_24h_usdc"):
            object.__setattr__(
                self,
                name,
                _decimal(getattr(self, name), name=name, minimum=_ZERO),
            )
        object.__setattr__(
            self,
            "max_days_until_close",
            _decimal(
                self.max_days_until_close,
                name="max_days_until_close",
                minimum=_ZERO,
            ),
        )


@dataclass(frozen=True, slots=True)
class CopyTask:
    """One persisted operator copy-trading task.

    Attributes:
        task_id: Stable task identity.
        name: Human-readable task name.
        leader_wallet: 20-byte EVM leader wallet address.
        follower_account: Follower account scope key.
        status: Active/paused/deleted lifecycle state.
        monitoring_mode: Normal or priority source-finality policy.
        trigger: Source-trade notional eligibility.
        spend: Persistent task spend caps and one-entry rules.
        price: Source-price band and explicit execution protection.
        reaction: Source BUY/SELL reaction policy.
        market_filter: Metadata-based eligibility filters.
        schedule: IANA-timezone entry window.
        sizing: Per-task BUY sizing overrides.
        sizing_mode: Canonical sizing mode string persisted for queries.
        version: Optimistic concurrency version.
        notifications_muted: Whether notification transport is muted.
    """

    task_id: str
    name: str
    leader_wallet: str
    follower_account: str
    status: CopyTaskStatus = CopyTaskStatus.ACTIVE
    monitoring_mode: MonitoringMode = MonitoringMode.NORMAL
    trigger: TaskTriggerPolicy = field(default_factory=TaskTriggerPolicy)
    spend: TaskSpendPolicy = field(default_factory=TaskSpendPolicy)
    price: TaskPricePolicy = field(default_factory=TaskPricePolicy)
    reaction: ReactionPolicy = field(default_factory=ReactionPolicy)
    market_filter: TaskMarketFilter = field(default_factory=TaskMarketFilter)
    schedule: TaskSchedule = field(default_factory=TaskSchedule)
    sizing: TaskSizingPolicy = field(default_factory=TaskSizingPolicy)
    protective_risk: ProtectiveRiskPolicy = field(default_factory=ProtectiveRiskPolicy)
    sizing_mode: str = "proportional"
    version: int = 1
    notifications_muted: bool = False

    def __post_init__(self) -> None:
        if not str(self.task_id).strip():
            raise ValueError("task_id is required")
        if not str(self.name).strip():
            raise ValueError("task name is required")
        if not _EVM_ADDRESS.fullmatch(str(self.leader_wallet)):
            raise ValueError("leader_wallet must be a 20-byte EVM address")
        if not str(self.follower_account).strip():
            raise ValueError("follower_account is required")
        alias_mode = _sizing_mode(self.sizing_mode)
        default_sizing = TaskSizingPolicy()
        sizing = self.sizing
        if sizing == default_sizing and alias_mode is not default_sizing.mode:
            sizing = replace(default_sizing, mode=alias_mode)
            object.__setattr__(self, "sizing", sizing)
        elif (
            sizing != default_sizing
            and alias_mode is not default_sizing.mode
            and alias_mode is not sizing.mode
        ):
            raise ValueError("sizing_mode conflicts with sizing.mode")
        object.__setattr__(self, "sizing_mode", sizing.mode.value)
        if self.version < 1:
            raise ValueError("task version must be positive")

    @property
    def enabled(self) -> bool:
        """Return whether the task may process new source events."""

        return self.status is CopyTaskStatus.ACTIVE


@dataclass(frozen=True, slots=True)
class TaskAdmissionContext:
    """Immutable snapshot used by the pure task admission engine."""

    now: datetime
    action: str
    outcome_label: str
    leader_trade_notional: Decimal
    leader_price: Decimal
    market_id: str
    market_title: str
    category: str
    days_until_close: Decimal
    task_total_spend: Decimal
    task_daily_spend: Decimal
    task_market_spend: Decimal
    task_has_entered_market: bool
    global_market_has_entry: bool
    proposed_notional: Decimal | None = None
    market_is_expired: bool = False
    market_is_resolved: bool = False
    market_liquidity_usdc: Decimal = _ZERO
    market_volume_24h_usdc: Decimal = _ZERO
    token_symbol: str = ""
    market_timeframe: str = ""
    sports_market_type: str = ""
    task_has_entered_sports_type: bool = False

    def __post_init__(self) -> None:
        if self.now.tzinfo is None:
            raise ValueError("TaskAdmissionContext.now must be timezone-aware")
        for name in (
            "leader_trade_notional",
            "leader_price",
            "days_until_close",
            "task_total_spend",
            "task_daily_spend",
            "task_market_spend",
            "market_liquidity_usdc",
            "market_volume_24h_usdc",
        ):
            object.__setattr__(
                self,
                name,
                _decimal(getattr(self, name), name=name, minimum=_ZERO),
            )
        object.__setattr__(
            self,
            "token_symbol",
            str(self.token_symbol).strip().casefold(),
        )
        object.__setattr__(
            self,
            "market_timeframe",
            str(self.market_timeframe).strip().casefold(),
        )
        object.__setattr__(
            self,
            "sports_market_type",
            str(self.sports_market_type).strip().casefold(),
        )
        if self.proposed_notional is not None:
            object.__setattr__(
                self,
                "proposed_notional",
                _decimal(
                    self.proposed_notional,
                    name="proposed_notional",
                    minimum=_ZERO,
                ),
            )

    @property
    def intended_notional(self) -> Decimal:
        """Return the amount used for pre-risk task cap checks."""

        return self.proposed_notional or self.leader_trade_notional


@dataclass(frozen=True, slots=True)
class TaskAdmissionDecision:
    """Result of task-level admission before sizing/risk/execution."""

    allowed: bool
    reason: str


@dataclass(frozen=True, slots=True)
class SourceTrade:
    """Vendor-neutral source trade consumed by reaction policies."""

    leader_wallet: str
    market_id: str
    outcome_label: str
    action: str
    price: Decimal
    notional: Decimal
    shares: Decimal
    finality: DetectionFinality = DetectionFinality.CONFIRMED

    def __post_init__(self) -> None:
        if not _EVM_ADDRESS.fullmatch(str(self.leader_wallet)):
            raise ValueError("leader_wallet must be a 20-byte EVM address")
        action = str(self.action).upper()
        if action not in {"BUY", "SELL"}:
            raise ValueError("source action must be BUY or SELL")
        object.__setattr__(self, "action", action)
        finality = (
            self.finality
            if isinstance(self.finality, DetectionFinality)
            else DetectionFinality(str(self.finality))
        )
        object.__setattr__(self, "finality", finality)
        for name in ("price", "notional", "shares"):
            object.__setattr__(
                self,
                name,
                _decimal(getattr(self, name), name=name, minimum=_ZERO),
            )


__all__ = [
    "CopySideFilter",
    "CopyTask",
    "CopyTaskStatus",
    "MonitoringMode",
    "PriceOffsetDirection",
    "PriceOffsetMode",
    "ReactionPolicy",
    "SourceTrade",
    "TaskAdmissionContext",
    "TaskAdmissionDecision",
    "TaskMarketFilter",
    "TaskPricePolicy",
    "TaskSchedule",
    "TaskSizingMode",
    "TaskSizingPolicy",
    "TaskSpendPolicy",
    "TaskTriggerPolicy",
]
