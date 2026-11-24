"""Falcon Wallet360 discovery policy, thresholds, and candidate evidence context."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from statistics import median
from typing import Any, Protocol, Sequence

from pmcopy.integrations.falcon.models import (
    MARKET_TOXICITY_FLAGS,
    LeaderboardEntry,
    Market360Snapshot,
    MarketSnapshot,
    TradeSnapshot,
    Wallet360Snapshot,
    WalletAllTimeSnapshot,
)
from pmcopy.services.discovery.bankroll_compatibility import BankrollCompatibilityPolicy
from pmcopy.services.discovery.bot_signals import WalletActivitySignals


_ZERO = Decimal("0")
_ONE = Decimal("1")

#: Leaderboard sort orders the provider agent accepts.
LEADERBOARD_SORT_MODES = frozenset({"h_score", "roi", "win_rate", "pnl", "sharpe", "trades"})

#: Rows the provider returns for one sort order, regardless of the requested
#: page size: offset 200 repeats ranks 1-200 with ``has_more=false``. Reaching
#: a larger shortlist therefore requires unioning several sort orders.
PROVIDER_LEADERBOARD_ROW_CAP = 200


class CapacityProbe(Protocol):
    """Define the CapacityProbe port used by application services."""

    async def visible_capacity_usdc(
        self,
        trade: TradeSnapshot,
        market: MarketSnapshot,
    ) -> Decimal | None:
        """Return executable visible capacity as an exact USDC amount."""
        ...


def _clamp(value: Decimal, low: Decimal = _ZERO, high: Decimal = _ONE) -> Decimal:
    return max(low, min(high, value))


def _median(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(str(median(values)))


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except Exception:
        return None
    return result if result.is_finite() else None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes"}


_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def _is_evm_address(value: str) -> bool:
    return bool(_EVM_ADDRESS_RE.fullmatch(str(value or "").strip()))


@dataclass(frozen=True, slots=True)
class FalconDiscoveryPolicy:
    """Evaluate provider evidence using bankroll-compatible safety policy."""

    candidate_limit: int = 500
    candidate_concurrency: int = 6
    wallet360_windows: tuple[int, ...] = (1, 3, 7, 15)
    required_wallet360_windows: int = 3
    max_wallet360_snapshot_age_hours: int = 72
    recent_trade_lookback_hours: int = 48
    max_last_trade_age_hours: int = 36
    activity_window_hours: int = 24
    min_activity_window_trades: int = 1
    min_recent_trades: int = 10
    max_recent_trades: int = 500
    max_recent_markets_per_wallet: int = 30
    max_depth_probes_per_wallet: int = 8
    max_price_jump_probes_per_wallet: int = 3
    price_jump_lookback_hours: int = 24
    price_jump_min_change_pct: Decimal = Decimal("10")
    max_recent_price_jumps: int = 8
    enable_price_jump_risk: bool = True
    short_market_max_days: int = 30
    min_short_trade_count: int = 0
    min_short_trade_share: Decimal = Decimal("0")
    min_h_score: Decimal = Decimal("60")
    min_liquidity_percentile: Decimal = Decimal("50")
    toxic_market_flags: tuple[str, ...] = ("liquidity_risk", "volume_collapse", "whale_control")
    max_toxic_market_share: Decimal = Decimal("0.50")
    max_market_concentration: Decimal = Decimal("0.65")
    max_missing_field_ratio: Decimal = Decimal("0.25")
    drawdown_gate_enabled: bool = False
    max_drawdown: Decimal = Decimal("0.35")
    max_sybil_score: Decimal = Decimal("20")
    reject_perfect_timing_flag: bool = True
    reject_suspicious_win_rate_flag: bool = True
    max_similar_wallets: int = 2
    max_trades_per_day: Decimal = Decimal("300")
    min_avg_trade_size_usdc: Decimal = Decimal("5")
    excluded_categories: tuple[str, ...] = ("Crypto",)
    max_excluded_category_trade_share: Decimal = Decimal("0.50")
    specialist_bonus_enabled: bool = True
    specialist_min_dominant_share: Decimal = Decimal("0.80")
    specialist_min_win_rate: Decimal = Decimal("0.57")
    specialist_bonus_weight: Decimal = Decimal("0.10")
    leaderboard_min_win_rate_15d: Decimal = Decimal("0.45")
    leaderboard_max_win_rate_15d: Decimal = Decimal("0.95")
    leaderboard_min_roi_15d: Decimal = Decimal("0")
    leaderboard_min_trades_15d: int = 15
    leaderboard_max_trades_15d: int = 3_000
    leaderboard_min_pnl_15d: Decimal = Decimal("0")
    leaderboard_sort_modes: tuple[str, ...] = (
        "h_score",
        "roi",
        "win_rate",
        "pnl",
        "sharpe",
        "trades",
    )
    bankroll: BankrollCompatibilityPolicy = field(default_factory=BankrollCompatibilityPolicy)

    def __post_init__(self) -> None:
        allowed_windows = {1, 3, 7, 15}
        try:
            windows = tuple(int(window) for window in self.wallet360_windows)
        except (TypeError, ValueError) as exc:
            raise ValueError("wallet360_windows must contain integers") from exc
        if not windows or any(window not in allowed_windows for window in windows):
            raise ValueError("wallet360_windows must be selected from 1, 3, 7 and 15")
        if len(set(windows)) != len(windows):
            raise ValueError("wallet360_windows must not contain duplicates")
        object.__setattr__(self, "wallet360_windows", windows)

        positive_ints = (
            "candidate_limit",
            "candidate_concurrency",
            "required_wallet360_windows",
            "max_wallet360_snapshot_age_hours",
            "recent_trade_lookback_hours",
            "max_last_trade_age_hours",
            "max_recent_trades",
            "max_recent_markets_per_wallet",
            "price_jump_lookback_hours",
            "short_market_max_days",
            "leaderboard_max_trades_15d",
            "activity_window_hours",
        )
        for name in positive_ints:
            value = getattr(self, name)
            if isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be positive")

        non_negative_ints = (
            "min_activity_window_trades",
            "max_similar_wallets",
            "min_recent_trades",
            "max_depth_probes_per_wallet",
            "max_price_jump_probes_per_wallet",
            "max_recent_price_jumps",
            "min_short_trade_count",
            "leaderboard_min_trades_15d",
        )
        for name in non_negative_ints:
            value = getattr(self, name)
            if isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must not be negative")

        if self.required_wallet360_windows > len(windows):
            raise ValueError(
                "required_wallet360_windows must not exceed configured wallet360_windows"
            )
        if self.min_recent_trades > self.max_recent_trades:
            raise ValueError("min_recent_trades must not exceed max_recent_trades")
        if self.min_short_trade_count > self.max_recent_trades:
            raise ValueError("min_short_trade_count must not exceed max_recent_trades")
        if self.leaderboard_min_trades_15d > self.leaderboard_max_trades_15d:
            raise ValueError(
                "leaderboard_min_trades_15d must not exceed leaderboard_max_trades_15d"
            )
        if self.activity_window_hours > self.recent_trade_lookback_hours:
            raise ValueError("activity_window_hours must not exceed recent_trade_lookback_hours")

        object.__setattr__(self, "toxic_market_flags", self._normalized_flags())
        object.__setattr__(self, "excluded_categories", self._normalized_categories())
        object.__setattr__(self, "leaderboard_sort_modes", self._normalized_sort_modes())

        decimal_fields = (
            "price_jump_min_change_pct",
            "max_trades_per_day",
            "min_avg_trade_size_usdc",
            "max_excluded_category_trade_share",
            "specialist_min_dominant_share",
            "specialist_min_win_rate",
            "specialist_bonus_weight",
            "min_short_trade_share",
            "min_h_score",
            "min_liquidity_percentile",
            "max_toxic_market_share",
            "max_market_concentration",
            "max_missing_field_ratio",
            "max_drawdown",
            "max_sybil_score",
            "leaderboard_min_win_rate_15d",
            "leaderboard_max_win_rate_15d",
            "leaderboard_min_roi_15d",
            "leaderboard_min_pnl_15d",
        )
        for name in decimal_fields:
            try:
                value = Decimal(str(getattr(self, name)))
            except Exception as exc:
                raise ValueError(f"{name} must be a decimal") from exc
            if not value.is_finite():
                raise ValueError(f"{name} must be finite")
            object.__setattr__(self, name, value)

        zero_to_one = (
            "min_short_trade_share",
            "max_excluded_category_trade_share",
            "specialist_min_dominant_share",
            "specialist_min_win_rate",
            "specialist_bonus_weight",
            "max_toxic_market_share",
            "max_market_concentration",
            "max_missing_field_ratio",
            "max_drawdown",
            "leaderboard_min_win_rate_15d",
            "leaderboard_max_win_rate_15d",
        )
        for name in zero_to_one:
            value = getattr(self, name)
            if not _ZERO <= value <= _ONE:
                raise ValueError(f"{name} must be between zero and one")
        if not _ZERO <= self.min_h_score <= Decimal("100"):
            raise ValueError("min_h_score must be between zero and 100")
        if not _ZERO <= self.min_liquidity_percentile <= Decimal("100"):
            raise ValueError("min_liquidity_percentile must be between zero and 100")
        if self.max_sybil_score < _ZERO:
            raise ValueError("max_sybil_score must not be negative")
        if self.price_jump_min_change_pct < _ZERO:
            raise ValueError("price_jump_min_change_pct must not be negative")
        if self.max_trades_per_day < _ZERO:
            raise ValueError("max_trades_per_day must not be negative")
        if self.min_avg_trade_size_usdc < _ZERO:
            raise ValueError("min_avg_trade_size_usdc must not be negative")
        if self.leaderboard_min_win_rate_15d > self.leaderboard_max_win_rate_15d:
            raise ValueError(
                "leaderboard_min_win_rate_15d must not exceed leaderboard_max_win_rate_15d"
            )
        if not isinstance(self.bankroll, BankrollCompatibilityPolicy):
            raise ValueError("bankroll must be a BankrollCompatibilityPolicy")

    def _normalized_flags(self) -> tuple[str, ...]:
        """Return the configured market-toxicity flag names, case-normalized.

        Returns:
            De-duplicated lowercase flag names in configuration order.

        Raises:
            ValueError: If the selection is empty or names an unknown flag.
        """
        try:
            names = tuple(str(item).strip().casefold() for item in self.toxic_market_flags)
        except TypeError as exc:
            raise ValueError("toxic_market_flags must be a sequence of names") from exc
        unique = tuple(dict.fromkeys(name for name in names if name))
        if not unique:
            raise ValueError("toxic_market_flags must select at least one flag")
        unknown = sorted(set(unique) - MARKET_TOXICITY_FLAGS)
        if unknown:
            raise ValueError(f"toxic_market_flags contains unknown flags: {', '.join(unknown)}")
        return unique

    def _normalized_categories(self) -> tuple[str, ...]:
        """Return the excluded top-level category names, case-normalized.

        Returns:
            De-duplicated lowercase category names in configuration order.

        Raises:
            ValueError: If any configured category name is blank.
        """
        try:
            names = tuple(str(item).strip().casefold() for item in self.excluded_categories)
        except TypeError as exc:
            raise ValueError("excluded_categories must be a sequence of names") from exc
        if any(not name for name in names):
            raise ValueError("excluded_categories must not contain blank names")
        return tuple(dict.fromkeys(names))

    def _normalized_sort_modes(self) -> tuple[str, ...]:
        """Return the leaderboard sort orders to union, case-normalized.

        Returns:
            De-duplicated lowercase sort-mode names in configuration order;
            the first entry drives shortlist precedence.

        Raises:
            ValueError: If the selection is empty or names an unknown mode.
        """
        try:
            names = tuple(str(item).strip().casefold() for item in self.leaderboard_sort_modes)
        except TypeError as exc:
            raise ValueError("leaderboard_sort_modes must be a sequence of names") from exc
        unique = tuple(dict.fromkeys(name for name in names if name))
        if not unique:
            raise ValueError("leaderboard_sort_modes must select at least one mode")
        unknown = sorted(set(unique) - LEADERBOARD_SORT_MODES)
        if unknown:
            raise ValueError(f"leaderboard_sort_modes contains unknown modes: {', '.join(unknown)}")
        return unique


@dataclass(slots=True)
class _CandidateContext:
    leaderboard: LeaderboardEntry
    wallet360: dict[int, Wallet360Snapshot]
    trades: list[TradeSnapshot]
    markets: dict[str, MarketSnapshot]
    market_quality: dict[str, Market360Snapshot]
    all_time: WalletAllTimeSnapshot | None
    pnl_rows: list[dict[str, Any]]
    source_errors: list[str]
    visible_capacity: dict[str, Decimal]
    price_jump_count: int
    official_leaderboard_rank: int | None
    signals: WalletActivitySignals

