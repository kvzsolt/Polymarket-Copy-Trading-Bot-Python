"""Bankroll-dependent copyability analysis.

A trader is not globally "best".  This module determines whether the trader's
recent order sizes can be reproduced by a follower's available funds without
turning most signals into dust or exceeding a safe per-position budget.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from statistics import median
from typing import Mapping, Sequence

from pmcopy.integrations.falcon.models import Market360Snapshot, TradeSnapshot
from pmcopy.services.discovery.discovery_models import BankrollFit

_ZERO = Decimal("0")
_ONE = Decimal("1")


def _clamp(value: Decimal, low: Decimal = _ZERO, high: Decimal = _ONE) -> Decimal:
    return max(low, min(high, value))


def _median(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return _ZERO
    return Decimal(str(median(values)))


@dataclass(frozen=True, slots=True)
class BankrollCompatibilityPolicy:
    """Validate exact-decimal limits for bankroll copyability analysis."""

    reserve_fraction: Decimal = Decimal("0.20")
    max_positions: int = 12
    max_position_fraction: Decimal = Decimal("0.10")
    target_trade_fraction: Decimal = Decimal("0.08")
    min_order_usdc: Decimal = Decimal("1")
    min_practical_copy_ratio: Decimal = Decimal("0.001")
    max_copy_ratio: Decimal = Decimal("1")
    min_copyable_trade_share: Decimal = Decimal("0.35")
    min_fit_score: Decimal = Decimal("0.45")
    visible_capacity_utilization: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if isinstance(self.max_positions, bool) or self.max_positions <= 0:
            raise ValueError("max_positions must be a positive integer")

        decimal_fields = (
            "reserve_fraction",
            "max_position_fraction",
            "target_trade_fraction",
            "min_order_usdc",
            "min_practical_copy_ratio",
            "max_copy_ratio",
            "min_copyable_trade_share",
            "min_fit_score",
            "visible_capacity_utilization",
        )
        for name in decimal_fields:
            try:
                value = Decimal(str(getattr(self, name)))
            except Exception as exc:
                raise ValueError(f"{name} must be a decimal") from exc
            if not value.is_finite():
                raise ValueError(f"{name} must be finite")
            object.__setattr__(self, name, value)

        if not _ZERO <= self.reserve_fraction < _ONE:
            raise ValueError("reserve_fraction must be between zero inclusive and one exclusive")
        if not _ZERO < self.max_position_fraction <= _ONE:
            raise ValueError(
                "max_position_fraction must be between zero exclusive and one inclusive"
            )
        if not _ZERO < self.target_trade_fraction <= _ONE:
            raise ValueError(
                "target_trade_fraction must be between zero exclusive and one inclusive"
            )
        if self.min_order_usdc <= _ZERO:
            raise ValueError("min_order_usdc must be positive")
        if not _ZERO < self.min_practical_copy_ratio <= _ONE:
            raise ValueError(
                "min_practical_copy_ratio must be between zero exclusive and one inclusive"
            )
        if not _ZERO < self.max_copy_ratio <= _ONE:
            raise ValueError("max_copy_ratio must be between zero exclusive and one inclusive")
        if self.min_practical_copy_ratio > self.max_copy_ratio:
            raise ValueError("min_practical_copy_ratio must not exceed max_copy_ratio")
        if not _ZERO <= self.min_copyable_trade_share <= _ONE:
            raise ValueError("min_copyable_trade_share must be between zero and one")
        if not _ZERO <= self.min_fit_score <= _ONE:
            raise ValueError("min_fit_score must be between zero and one")
        if not _ZERO < self.visible_capacity_utilization <= _ONE:
            raise ValueError(
                "visible_capacity_utilization must be between zero exclusive and one inclusive"
            )


def evaluate_bankroll_compatibility(
    *,
    funds: Decimal,
    recent_trades: Sequence[TradeSnapshot],
    market_quality: Mapping[str, Market360Snapshot],
    avg_position_size: Decimal | None,
    visible_capacity_usdc: Mapping[str, Decimal] | None = None,
    policy: BankrollCompatibilityPolicy | None = None,
) -> BankrollFit:
    """Evaluate whether recent leader trades fit a follower bankroll."""
    policy = policy or BankrollCompatibilityPolicy()
    funds = Decimal(str(funds))
    if funds <= 0:
        return BankrollFit.empty(funds, "non_positive_funds")
    if not recent_trades:
        return BankrollFit(
            funds_usdc=funds,
            usable_bankroll_usdc=funds * (_ONE - policy.reserve_fraction),
            target_trade_budget_usdc=_ZERO,
            max_trade_budget_usdc=_ZERO,
            typical_trade_usdc=_ZERO,
            avg_position_size_usdc=(
                Decimal(str(avg_position_size)) if avg_position_size is not None else None
            ),
            trade_scale_ratio=_ZERO,
            position_scale_ratio=_ZERO,
            recommended_copy_ratio=_ZERO,
            recommended_stake_usdc=_ZERO,
            copyable_trade_share=_ZERO,
            median_scaled_trade_usdc=_ZERO,
            median_liquidity_percentile=None,
            fit_score=_ZERO,
            viable=False,
            reasons=("no_recent_trades",),
        )

    usable = funds * (_ONE - policy.reserve_fraction)
    max_position_budget = min(usable, funds * policy.max_position_fraction)
    diversification_budget = usable / Decimal(policy.max_positions)
    target_budget = min(
        max_position_budget,
        max(diversification_budget, funds * policy.target_trade_fraction),
    )
    if target_budget < policy.min_order_usdc:
        return BankrollFit(
            funds_usdc=funds,
            usable_bankroll_usdc=usable,
            target_trade_budget_usdc=target_budget,
            max_trade_budget_usdc=max_position_budget,
            typical_trade_usdc=_ZERO,
            avg_position_size_usdc=(
                Decimal(str(avg_position_size)) if avg_position_size is not None else None
            ),
            trade_scale_ratio=_ZERO,
            position_scale_ratio=_ZERO,
            recommended_copy_ratio=_ZERO,
            recommended_stake_usdc=_ZERO,
            copyable_trade_share=_ZERO,
            median_scaled_trade_usdc=_ZERO,
            median_liquidity_percentile=None,
            fit_score=_ZERO,
            viable=False,
            reasons=("bankroll_below_minimum_executable_size",),
        )

    positive_trades = [trade for trade in recent_trades if trade.notional > 0]
    notionals = [trade.notional for trade in positive_trades]
    if not notionals:
        return BankrollFit.empty(funds, "no_positive_trade_notionals")
    typical_trade = _median(notionals)
    if typical_trade <= 0:
        return BankrollFit.empty(funds, "invalid_typical_trade")

    trade_scale_ratio = _clamp(
        target_budget / typical_trade,
        _ZERO,
        policy.max_copy_ratio,
    )
    normalized_avg_position = (
        Decimal(str(avg_position_size)) if avg_position_size is not None else None
    )
    if normalized_avg_position is not None and normalized_avg_position > 0:
        position_scale_ratio = _clamp(
            max_position_budget / normalized_avg_position,
            _ZERO,
            policy.max_copy_ratio,
        )
        ratio = min(trade_scale_ratio, position_scale_ratio)
    else:
        position_scale_ratio = trade_scale_ratio
        ratio = trade_scale_ratio
    scaled = [notional * ratio for notional in notionals]
    visible_capacity_usdc = visible_capacity_usdc or {}
    copyable: list[Decimal] = []
    capacity_rejections = 0
    for trade, scaled_notional in zip(positive_trades, scaled, strict=True):
        if scaled_notional < policy.min_order_usdc or scaled_notional > max_position_budget:
            continue
        capacity = visible_capacity_usdc.get(trade.condition_id)
        if (
            capacity is not None
            and scaled_notional > capacity * policy.visible_capacity_utilization
        ):
            capacity_rejections += 1
            continue
        copyable.append(scaled_notional)

    share = Decimal(len(copyable)) / Decimal(len(notionals))
    median_scaled = _median(scaled)
    recommended_stake = min(max_position_budget, _median(copyable) if copyable else median_scaled)

    percentiles = [
        item.liquidity_percentile
        for item in market_quality.values()
        if item.liquidity_percentile is not None
    ]
    median_liquidity = _median(percentiles) if percentiles else None
    liquidity_score = (
        _clamp((median_liquidity or Decimal("50")) / Decimal("100"))
        if market_quality
        else Decimal("0.5")
    )
    trade_ratio_score = (
        _ONE
        if trade_scale_ratio >= policy.min_practical_copy_ratio
        else _clamp(trade_scale_ratio / policy.min_practical_copy_ratio)
    )
    position_ratio_score = (
        _ONE
        if position_scale_ratio >= policy.min_practical_copy_ratio
        else _clamp(position_scale_ratio / policy.min_practical_copy_ratio)
    )
    utilization_score = _clamp(recommended_stake / target_budget) if target_budget > 0 else _ZERO
    fit_score = _clamp(
        share * Decimal("0.40")
        + trade_ratio_score * Decimal("0.20")
        + position_ratio_score * Decimal("0.15")
        + utilization_score * Decimal("0.10")
        + liquidity_score * Decimal("0.15")
    )

    reasons: list[str] = []
    if ratio < policy.min_practical_copy_ratio:
        reasons.append("copy_ratio_below_practical_floor")
    if position_scale_ratio < policy.min_practical_copy_ratio:
        reasons.append("position_scale_below_practical_floor")
    if share < policy.min_copyable_trade_share:
        reasons.append("insufficient_copyable_trade_share")
    if capacity_rejections:
        reasons.append("insufficient_visible_capacity")
    if median_liquidity is not None and median_liquidity < Decimal("50"):
        reasons.append("weak_market_liquidity")
    viable = (
        ratio >= policy.min_practical_copy_ratio
        and trade_scale_ratio >= policy.min_practical_copy_ratio
        and position_scale_ratio >= policy.min_practical_copy_ratio
        and share >= policy.min_copyable_trade_share
        and fit_score >= policy.min_fit_score
    )
    return BankrollFit(
        funds_usdc=funds,
        usable_bankroll_usdc=usable,
        target_trade_budget_usdc=target_budget,
        max_trade_budget_usdc=max_position_budget,
        typical_trade_usdc=typical_trade,
        avg_position_size_usdc=normalized_avg_position,
        trade_scale_ratio=trade_scale_ratio,
        position_scale_ratio=position_scale_ratio,
        recommended_copy_ratio=ratio,
        recommended_stake_usdc=max(_ZERO, recommended_stake),
        copyable_trade_share=share,
        median_scaled_trade_usdc=median_scaled,
        median_liquidity_percentile=median_liquidity,
        fit_score=fit_score,
        viable=viable,
        reasons=tuple(reasons),
    )


__all__ = [
    "BankrollCompatibilityPolicy",
    "evaluate_bankroll_compatibility",
]
