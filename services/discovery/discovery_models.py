"""Typed outputs for bankroll-aware trader discovery."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True, slots=True)
class BankrollFit:
    funds_usdc: Decimal
    usable_bankroll_usdc: Decimal
    target_trade_budget_usdc: Decimal
    max_trade_budget_usdc: Decimal
    typical_trade_usdc: Decimal
    avg_position_size_usdc: Decimal | None
    trade_scale_ratio: Decimal
    position_scale_ratio: Decimal
    recommended_copy_ratio: Decimal
    recommended_stake_usdc: Decimal
    copyable_trade_share: Decimal
    median_scaled_trade_usdc: Decimal
    median_liquidity_percentile: Decimal | None
    fit_score: Decimal
    viable: bool
    reasons: tuple[str, ...] = ()

    @classmethod
    def empty(cls, funds: Decimal = Decimal("0"), reason: str = "not_evaluated") -> "BankrollFit":
        return cls(
            funds_usdc=funds,
            usable_bankroll_usdc=Decimal("0"),
            target_trade_budget_usdc=Decimal("0"),
            max_trade_budget_usdc=Decimal("0"),
            typical_trade_usdc=Decimal("0"),
            avg_position_size_usdc=None,
            trade_scale_ratio=Decimal("0"),
            position_scale_ratio=Decimal("0"),
            recommended_copy_ratio=Decimal("0"),
            recommended_stake_usdc=Decimal("0"),
            copyable_trade_share=Decimal("0"),
            median_scaled_trade_usdc=Decimal("0"),
            median_liquidity_percentile=None,
            fit_score=Decimal("0"),
            viable=False,
            reasons=(reason,),
        )


@dataclass(frozen=True, slots=True)
class DiscoveryEvidence:
    wallet360_windows: tuple[int, ...] = ()
    fresh_wallet360_windows: tuple[int, ...] = ()
    latest_wallet360_at: datetime | None = None
    last_trade_at: datetime | None = None
    recent_trade_count: int = 0
    today_trade_count: int = 0
    short_trade_count: int = 0
    short_trade_share: Decimal = Decimal("0")
    median_liquidity_percentile: Decimal | None = None
    toxic_market_share: Decimal = Decimal("0")
    missing_field_ratio: Decimal = Decimal("1")
    all_time_trades: int | None = None
    realized_pnl_rows: int = 0
    condition_count: int = 0
    recent_price_jump_count: int = 0
    official_leaderboard_rank: int | None = None
    source_errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RankedTrader:
    wallet: str
    score: Decimal
    source: str
    h_score: Decimal
    bankroll_fit: BankrollFit
    evidence: DiscoveryEvidence
    components: dict[str, Decimal] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def minimal(cls, *, wallet: str, score: Decimal, source: str) -> "RankedTrader":
        return cls(
            wallet=wallet.lower(),
            score=score,
            source=source,
            h_score=Decimal("0"),
            bankroll_fit=BankrollFit.empty(),
            evidence=DiscoveryEvidence(),
        )


@dataclass(frozen=True, slots=True)
class RejectedTrader:
    wallet: str
    reasons: tuple[str, ...]
    source: str
    h_score: Decimal = Decimal("0")
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    ranked: tuple[RankedTrader, ...]
    rejected: tuple[RejectedTrader, ...]
    source: str
    generated_at: datetime | None = None
    warnings: tuple[str, ...] = ()

    @property
    def best(self) -> RankedTrader | None:
        return self.ranked[0] if self.ranked else None


__all__ = [
    "BankrollFit",
    "DiscoveryEvidence",
    "DiscoveryResult",
    "RankedTrader",
    "RejectedTrader",
]
