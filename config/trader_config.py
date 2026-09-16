"""Strict immutable configuration for one followed trader.

Each ``config/traders/<name>.yml`` file is the single place where a followed
leader is described: wallet identity, whether it is enabled, per-leader copy
gates, and the full v1 copy-task strategy used to follow that wallet.

The models in this module only validate YAML shape. They deliberately do not
import the copy-task runtime domain; the v1 bootstrap converts a
:class:`TraderTaskConfig` into a ``services.copy_tasks.domain.CopyTask`` when
it seeds file-defined tasks.
"""

from __future__ import annotations

import re
from datetime import time
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_ETH_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_SLUG_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$")

_SIZING_MODE_ALIASES = {
    "fixed": "fixed",
    "fixed_amount": "fixed",
    "exact": "exact_mirror",
    "exact_mirror": "exact_mirror",
    "mirror": "exact_mirror",
    "exact_copy": "exact_mirror",
    "proportional": "proportional",
    "percentage": "proportional",
    "percentage_based": "proportional",
    "balance": "balance_weighted",
    "balance_weighted": "balance_weighted",
    "portfolio": "portfolio_weighted",
    "portfolio_weighted": "portfolio_weighted",
    "adaptive": "adaptive",
    "tiered": "tiered",
}


class _FrozenModel(BaseModel):
    """Shared strict immutable settings base."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)


class TraderTaskTriggerConfig(_FrozenModel):
    """Allowed source-trade notional range."""

    enabled: bool = True
    min_notional: Decimal = Decimal("0")
    max_notional: Decimal = Decimal("0")

    @model_validator(mode="after")
    def _validate_range(self) -> "TraderTaskTriggerConfig":
        if self.max_notional > 0 and self.max_notional < self.min_notional:
            raise ValueError("max_notional must be zero/disabled or >= min_notional")
        if self.min_notional < 0 or self.max_notional < 0:
            raise ValueError("notional limits must be >= 0")
        return self


class TraderTaskSpendConfig(_FrozenModel):
    """Persistent task-level spend and duplication limits."""

    enabled: bool = True
    max_total: Decimal = Decimal("0")
    max_daily: Decimal = Decimal("0")
    max_per_market: Decimal = Decimal("0")
    one_entry_per_market: bool = False
    global_one_entry_per_market: bool = False
    one_entry_per_sports_type: bool = False

    @model_validator(mode="after")
    def _validate_non_negative(self) -> "TraderTaskSpendConfig":
        for name in ("max_total", "max_daily", "max_per_market"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be >= 0")
        return self


class TraderTaskPriceConfig(_FrozenModel):
    """Source-price eligibility and explicit execution protection."""

    enabled: bool = True
    min_price: Decimal = Decimal("0.20")
    max_price: Decimal = Decimal("0.80")
    offset_mode: Literal["disabled", "fixed", "percentage"] = "disabled"
    offset_direction: Literal["above", "below"] = "above"
    offset: Decimal = Decimal("0")
    max_slippage_percent: Decimal = Decimal("5")

    @model_validator(mode="after")
    def _validate_band(self) -> "TraderTaskPriceConfig":
        if not (Decimal("0") <= self.min_price <= Decimal("1")):
            raise ValueError("min_price must be within [0, 1]")
        if not (Decimal("0") <= self.max_price <= Decimal("1")):
            raise ValueError("max_price must be within [0, 1]")
        if self.min_price > self.max_price:
            raise ValueError("min_price cannot exceed max_price")
        if self.offset < 0 or self.max_slippage_percent < 0:
            raise ValueError("offset and max_slippage_percent must be >= 0")
        if self.offset_mode == "fixed" and self.offset > Decimal("1"):
            raise ValueError("fixed price offset must be <= 1")
        return self


class TraderTaskReactionConfig(_FrozenModel):
    """How source BUY/SELL events are translated into follower actions."""

    enabled: bool = True
    follow_buys: bool = True
    copy_sells: bool = True
    buy_on_sells: bool = False
    counter_trade: bool = False
    copy_side: Literal["both", "yes_only", "no_only"] = "both"
    copy_merges: bool = False
    copy_splits: bool = False
    copy_conversions: bool = False


class TraderTaskMarketFilterConfig(_FrozenModel):
    """Metadata-based task eligibility filters."""

    enabled: bool = True
    include_keywords: tuple[str, ...] = ()
    exclude_keywords: tuple[str, ...] = ()
    categories: tuple[str, ...] = ()
    tokens: tuple[str, ...] = ()
    timeframes: tuple[str, ...] = ()
    min_liquidity_usdc: Decimal = Decimal("0")
    min_volume_24h_usdc: Decimal = Decimal("0")
    max_days_until_close: Decimal = Decimal("0")
    allow_expired_unresolved: bool = False

    @model_validator(mode="after")
    def _validate_non_negative(self) -> "TraderTaskMarketFilterConfig":
        for name in ("min_liquidity_usdc", "min_volume_24h_usdc", "max_days_until_close"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be >= 0")
        return self


class TraderTaskScheduleConfig(_FrozenModel):
    """Recurring IANA-timezone entry window.

    A disabled schedule is always open. Overnight windows use a start later
    than the end, for example ``22:00`` to ``06:00``.
    """

    enabled: bool = False
    timezone_name: str = "UTC"
    start: str = "00:00"
    end: str = "00:00"

    @model_validator(mode="after")
    def _validate_times(self) -> "TraderTaskScheduleConfig":
        for name in ("start", "end"):
            try:
                time.fromisoformat(str(getattr(self, name)))
            except ValueError as exc:
                raise ValueError(f"{name} must use HH:MM") from exc
        if not str(self.timezone_name).strip():
            raise ValueError("timezone_name is required")
        try:
            ZoneInfo(str(self.timezone_name))
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown schedule timezone: {self.timezone_name}") from exc
        return self


class TraderTaskSizingConfig(_FrozenModel):
    """Per-task BUY sizing overrides applied above global risk limits."""

    enabled: bool = True
    mode: str = "proportional"
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

    @model_validator(mode="after")
    def _validate_sizing(self) -> "TraderTaskSizingConfig":
        normalized = str(self.mode).strip().lower().replace("-", "_")
        if normalized not in _SIZING_MODE_ALIASES:
            raise ValueError(f"unsupported task sizing mode: {self.mode!r}")
        canonical = _SIZING_MODE_ALIASES[normalized]
        for name in (
            "percentage",
            "fixed_amount",
            "trade_multiplier",
            "weighted_multiplier",
            "buy_min_amount",
            "buy_max_amount",
            "copyable_balance_percent",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be >= 0")
        if not (Decimal("0") < self.copyable_balance_percent <= Decimal("100")):
            raise ValueError("copyable_balance_percent must be in (0, 100]")
        if self.trade_multiplier <= 0 or self.weighted_multiplier <= 0:
            raise ValueError("trade_multiplier and weighted_multiplier must be > 0")
        if self.buy_max_amount > 0 and self.buy_max_amount < self.buy_min_amount:
            raise ValueError("buy_max_amount must be zero/disabled or >= buy_min_amount")
        if canonical == "fixed" and self.fixed_amount <= 0:
            raise ValueError("fixed sizing requires a positive fixed_amount")
        if canonical in {"proportional", "adaptive", "tiered"} and self.percentage <= 0:
            raise ValueError("percentage-based sizing requires a positive percentage")
        if canonical != str(self.mode):
            return self.model_copy(update={"mode": canonical})
        return self


class TraderTaskConfig(_FrozenModel):
    """Complete copy-task strategy for one followed leader.

    Identity fields (``task_id``, ``leader_wallet``, ``follower_account``)
    are intentionally absent: the trader file already owns the first two and
    the follower account is a property of the running CLI/user, not of the
    trader. The v1 bootstrap fills those in when it seeds file-defined tasks.
    """

    monitoring_mode: Literal["normal", "priority"] = "normal"
    sizing_mode: str = "proportional"
    notifications_muted: bool = False
    trigger: TraderTaskTriggerConfig = Field(default_factory=TraderTaskTriggerConfig)
    spend: TraderTaskSpendConfig = Field(default_factory=TraderTaskSpendConfig)
    price: TraderTaskPriceConfig = Field(default_factory=TraderTaskPriceConfig)
    reaction: TraderTaskReactionConfig = Field(default_factory=TraderTaskReactionConfig)
    market_filter: TraderTaskMarketFilterConfig = Field(
        default_factory=TraderTaskMarketFilterConfig
    )
    schedule: TraderTaskScheduleConfig = Field(default_factory=TraderTaskScheduleConfig)
    sizing: TraderTaskSizingConfig = Field(default_factory=TraderTaskSizingConfig)

    @model_validator(mode="after")
    def _validate_sizing_mode_consistency(self) -> "TraderTaskConfig":
        normalized = str(self.sizing_mode).strip().lower().replace("-", "_")
        if normalized not in _SIZING_MODE_ALIASES:
            raise ValueError(f"unsupported task sizing mode: {self.sizing_mode!r}")
        canonical = _SIZING_MODE_ALIASES[normalized]
        sizing_mode_explicit = "sizing_mode" in self.model_fields_set
        sizing_explicit = "sizing" in self.model_fields_set

        # Mirror services/copy_tasks/domain.CopyTask.__post_init__: when only
        # one of the two sizing fields is explicit, derive the other from it.
        if sizing_mode_explicit and sizing_explicit and canonical != str(self.sizing.mode):
            raise ValueError(
                f"task sizing_mode {canonical!r} conflicts with sizing.mode {self.sizing.mode!r}"
            )
        if sizing_mode_explicit and not sizing_explicit and canonical != str(self.sizing.mode):
            return self.model_copy(
                update={
                    "sizing_mode": canonical,
                    "sizing": self.sizing.model_copy(update={"mode": canonical}),
                }
            )
        if sizing_explicit and not sizing_mode_explicit and canonical != str(self.sizing.mode):
            return self.model_copy(update={"sizing_mode": str(self.sizing.mode)})
        if canonical != str(self.sizing_mode):
            return self.model_copy(update={"sizing_mode": canonical})
        return self


class TraderConfig(_FrozenModel):
    """One followed leader, fully described in a single YAML file.

    The file name is the trader's fantasy name and supplies ``name`` and the
    default task id when those fields are omitted.
    """

    name: str = ""
    wallet: str
    enabled: bool = True
    # Per-leader copy gates. ``None`` inherits the layered copy_trading value.
    copy_decision_min_score: float | None = None
    copy_max_price_drift_pct: float | None = None
    copy_max_edge_decay_pct: float | None = None
    copy_max_signal_age_seconds: float | None = None
    copy_liquidity_target_usdc: float | None = None
    # One-shot copy-entry FAK band for this leader alone.
    max_entry_price_drift_pct: float | None = None
    # Full v1 copy-task strategy for following this leader.
    task: TraderTaskConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def _accept_wallet_aliases(cls, value: object) -> object:
        """Accept ``wallet_id``/``leader_wallet`` as aliases for ``wallet``."""
        if not isinstance(value, dict) or "wallet" in value:
            return value
        for alias in ("wallet_id", "leader_wallet"):
            if alias in value:
                data = {**value, "wallet": value[alias]}
                data.pop(alias, None)
                return data
        return value

    @model_validator(mode="after")
    def _validate_wallet(self) -> "TraderConfig":
        if not _ETH_ADDR_RE.match(str(self.wallet or "")):
            raise ValueError(f"trader wallet must be a 42-char Ethereum address: {self.wallet!r}")
        return self


def validate_trader_filename_stem(stem: str) -> str:
    """Return a validated trader file stem or raise ``ValueError``."""

    value = str(stem or "").strip()
    if not _SLUG_RE.match(value):
        raise ValueError(
            f"Trader file stem must be 1-64 chars of letters, digits, '_', '-', or '.': {stem!r}"
        )
    return value


__all__ = [
    "TraderConfig",
    "TraderTaskConfig",
    "TraderTaskMarketFilterConfig",
    "TraderTaskPriceConfig",
    "TraderTaskReactionConfig",
    "TraderTaskScheduleConfig",
    "TraderTaskSizingConfig",
    "TraderTaskSpendConfig",
    "TraderTaskTriggerConfig",
    "validate_trader_filename_stem",
]
