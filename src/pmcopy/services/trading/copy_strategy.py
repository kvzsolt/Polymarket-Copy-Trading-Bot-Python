"""Account-level BUY notional copy sizing strategies (`trade_pipeline` & paper cohort).

This module implements the account-level / non-task sizing contract consumed by
:meth:`pmcopy.application.trading_context.TradingContext.copy_notional`:

* Unknown strategy names normalize to ``"proportional"``;
* Tiered multipliers use explicit ``"min-max:multiplier"`` or ``"min+:multiplier"``
  ranges; and
* Sub-minimum notionals uplift to ``min_order_usdc`` with ``below_minimum=True``.

Mode-specific base notional calculation is encapsulated in narrow
:class:`LegacyNotionalStrategy` subclasses while shared multiplier and min/max
clamping stay in :func:`calculate_copy_notional`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import ClassVar, final, override

_DECIMAL_ZERO = Decimal("0")
_DECIMAL_ONE = Decimal("1")


@dataclass(frozen=True)
class MultiplierTier:
    """Range-bounded trade multiplier tier (`[min_usdc, max_usdc)`)."""

    min_usdc: Decimal
    max_usdc: Decimal | None
    multiplier: Decimal


@dataclass(frozen=True)
class CopySizingResult:
    """Account-level BUY notional sizing result and human-readable reasoning."""

    leader_notional_usdc: Decimal
    base_notional_usdc: Decimal
    final_notional_usdc: Decimal
    strategy: str
    multiplier: Decimal
    capped_by_max: bool
    below_minimum: bool
    reasoning: str


def _to_decimal(value: object, default: str = "0") -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        if value is None or value == "":
            return Decimal(default)
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def normalize_strategy(strategy: str | None) -> str:
    """Normalize strategy names under the permissive account-pipeline contract."""
    text = str(strategy or "proportional").strip().lower()
    if text in {"percentage", "percent"}:
        return "proportional"
    if text not in {"proportional", "fixed", "adaptive"}:
        return "proportional"
    return text


def _lerp(a: Decimal, b: Decimal, t: Decimal) -> Decimal:
    t = max(_DECIMAL_ZERO, min(_DECIMAL_ONE, t))
    return a + (b - a) * t


def calculate_trade_size_adaptive_percentage(
    *,
    base_percent: object,
    leader_notional_usdc: object,
    adaptive_min_percent: object = None,
    adaptive_max_percent: object = None,
    adaptive_threshold_usdc: object = None,
) -> Decimal:
    """Trade-size adaptive percentage logic (tapers down as whale size grows)."""
    base = _to_decimal(base_percent)
    leader_notional = max(_DECIMAL_ZERO, _to_decimal(leader_notional_usdc))
    min_percent = _to_decimal(adaptive_min_percent)
    max_percent = _to_decimal(adaptive_max_percent)
    threshold = _to_decimal(adaptive_threshold_usdc, "500")

    if min_percent <= _DECIMAL_ZERO:
        min_percent = base
    if max_percent <= _DECIMAL_ZERO:
        max_percent = base
    if threshold <= _DECIMAL_ZERO:
        threshold = Decimal("500")

    if leader_notional >= threshold:
        factor = min(_DECIMAL_ONE, (leader_notional / threshold) - _DECIMAL_ONE)
        return _lerp(base, min_percent, factor)

    factor = leader_notional / threshold
    return _lerp(max_percent, base, factor)


def parse_tiered_multipliers(tiers_str: str | None) -> list[MultiplierTier]:
    """Parse ``min-max:multiplier`` and ``min+:multiplier`` non-overlapping tiers."""
    if not tiers_str or not str(tiers_str).strip():
        return []

    tiers: list[MultiplierTier] = []
    defs = [part.strip() for part in str(tiers_str).split(",") if part.strip()]

    for raw_tier in defs:
        if ":" not in raw_tier:
            raise ValueError(
                f'Invalid tier format: "{raw_tier}". Expected "min-max:multiplier" or "min+:multiplier"'
            )

        range_part, multiplier_part = raw_tier.split(":", 1)
        multiplier = _to_decimal(multiplier_part, "-1")
        if multiplier < _DECIMAL_ZERO:
            raise ValueError(f'Invalid multiplier in tier "{raw_tier}": {multiplier_part}')

        range_part = range_part.strip()
        if range_part.endswith("+"):
            min_usdc = _to_decimal(range_part[:-1], "-1")
            if min_usdc < _DECIMAL_ZERO:
                raise ValueError(f'Invalid minimum value in tier "{raw_tier}": {range_part}')
            tiers.append(MultiplierTier(min_usdc=min_usdc, max_usdc=None, multiplier=multiplier))
            continue

        if "-" not in range_part:
            raise ValueError(f'Invalid range format in tier "{raw_tier}". Use "min-max" or "min+"')

        min_part, max_part = range_part.split("-", 1)
        min_usdc = _to_decimal(min_part, "-1")
        max_usdc = _to_decimal(max_part, "-1")
        if min_usdc < _DECIMAL_ZERO:
            raise ValueError(f'Invalid minimum value in tier "{raw_tier}": {min_part}')
        if max_usdc <= min_usdc:
            raise ValueError(
                f'Invalid maximum value in tier "{raw_tier}": {max_part} (must be > {min_usdc})'
            )

        tiers.append(MultiplierTier(min_usdc=min_usdc, max_usdc=max_usdc, multiplier=multiplier))

    tiers.sort(key=lambda tier: tier.min_usdc)

    for idx in range(len(tiers) - 1):
        current = tiers[idx]
        nxt = tiers[idx + 1]
        if current.max_usdc is None:
            raise ValueError(f"Tier with infinite upper bound must be last: {current.min_usdc}+")
        if current.max_usdc > nxt.min_usdc:
            raise ValueError(
                "Overlapping tiers: "
                f"[{current.min_usdc}-{current.max_usdc}] and [{nxt.min_usdc}-{nxt.max_usdc or '∞'}]"
            )

    return tiers


def get_trade_multiplier(
    *,
    leader_notional_usdc: object,
    tiered_multipliers: str | list[MultiplierTier] | None = None,
    trade_multiplier: object = None,
) -> Decimal:
    """Resolve the active range-tier or scalar trade multiplier."""
    tiers: list[MultiplierTier]
    if isinstance(tiered_multipliers, list):
        tiers = tiered_multipliers
    else:
        tiers = parse_tiered_multipliers(str(tiered_multipliers or ""))

    leader_notional = max(_DECIMAL_ZERO, _to_decimal(leader_notional_usdc))

    if tiers:
        for tier in tiers:
            if leader_notional >= tier.min_usdc:
                if tier.max_usdc is None or leader_notional < tier.max_usdc:
                    return tier.multiplier
        return tiers[-1].multiplier

    single = _to_decimal(trade_multiplier, "1")
    if single < _DECIMAL_ZERO:
        return _DECIMAL_ZERO
    return single if single > _DECIMAL_ZERO else _DECIMAL_ONE


@dataclass(frozen=True, slots=True)
class LegacyNotionalInputs:
    """Normalized exact-Decimal inputs passed to a :class:`LegacyNotionalStrategy`."""

    leader_notional: Decimal
    copy_pct: Decimal
    fixed_amt: Decimal
    adaptive_basis: str
    adaptive_min_percent: object
    adaptive_max_percent: object
    adaptive_threshold_usdc: object
    performance_win_rate: object


class LegacyNotionalStrategy(ABC):
    """Abstract base class for account-pipeline base notional calculation."""

    strategy_name: ClassVar[str]

    @abstractmethod
    def base_notional(self, inputs: LegacyNotionalInputs) -> tuple[Decimal, str]:
        """Return ``(base_notional_usdc, reasoning)`` before multipliers and caps."""
        raise NotImplementedError


@final
class FixedLegacyNotionalStrategy(LegacyNotionalStrategy):
    """Constant quote amount regardless of leader trade size."""

    strategy_name: ClassVar[str] = "fixed"

    @override
    def base_notional(self, inputs: LegacyNotionalInputs) -> tuple[Decimal, str]:
        return inputs.fixed_amt, f"fixed amount ${inputs.fixed_amt:.4f}"


@final
class AdaptiveLegacyNotionalStrategy(LegacyNotionalStrategy):
    """Adaptive percentage scaled by performance win-rate or leader trade size."""

    strategy_name: ClassVar[str] = "adaptive"

    @override
    def base_notional(self, inputs: LegacyNotionalInputs) -> tuple[Decimal, str]:
        basis = str(inputs.adaptive_basis or "trade_size").strip().lower()
        if basis == "performance":
            win_rate = _to_decimal(inputs.performance_win_rate, "50")
            if win_rate > _DECIMAL_ONE:
                win_rate = win_rate / Decimal("100")
            win_rate = max(_DECIMAL_ZERO, min(_DECIMAL_ONE, win_rate))
            adaptive_multiplier = Decimal("0.5") + (win_rate * Decimal("1.5"))
            base_notional = (
                inputs.leader_notional
                * (inputs.copy_pct / Decimal("100"))
                * adaptive_multiplier
            )
            reasoning = (
                f"adaptive-performance {inputs.copy_pct:.4f}% of ${inputs.leader_notional:.4f}"
                f" × {adaptive_multiplier:.4f} (win_rate={win_rate:.4f})"
            )
            return base_notional, reasoning

        adaptive_pct = calculate_trade_size_adaptive_percentage(
            base_percent=inputs.copy_pct,
            leader_notional_usdc=inputs.leader_notional,
            adaptive_min_percent=inputs.adaptive_min_percent,
            adaptive_max_percent=inputs.adaptive_max_percent,
            adaptive_threshold_usdc=inputs.adaptive_threshold_usdc,
        )
        base_notional = inputs.leader_notional * (adaptive_pct / Decimal("100"))
        reasoning = f"adaptive-trade-size {adaptive_pct:.4f}% of ${inputs.leader_notional:.4f}"
        return base_notional, reasoning


@final
class ProportionalLegacyNotionalStrategy(LegacyNotionalStrategy):
    """Fixed percentage of leader trade notional."""

    strategy_name: ClassVar[str] = "proportional"

    @override
    def base_notional(self, inputs: LegacyNotionalInputs) -> tuple[Decimal, str]:
        base_notional = inputs.leader_notional * (inputs.copy_pct / Decimal("100"))
        reasoning = f"proportional {inputs.copy_pct:.4f}% of ${inputs.leader_notional:.4f}"
        return base_notional, reasoning


_DEFAULT_LEGACY_STRATEGIES: Mapping[str, LegacyNotionalStrategy] = MappingProxyType(
    {
        s.strategy_name: s
        for s in (
            FixedLegacyNotionalStrategy(),
            AdaptiveLegacyNotionalStrategy(),
            ProportionalLegacyNotionalStrategy(),
        )
    }
)


def calculate_copy_notional(
    *,
    strategy: str,
    leader_notional_usdc: object,
    copy_percentage: object,
    fixed_amount: object,
    min_order_usdc: object,
    max_order_usdc: object,
    trade_multiplier: object = None,
    tiered_multipliers: str | list[MultiplierTier] | None = None,
    adaptive_basis: str = "trade_size",
    adaptive_min_percent: object = None,
    adaptive_max_percent: object = None,
    adaptive_threshold_usdc: object = None,
    performance_win_rate: object = None,
    rank_multiplier: object = None,
) -> CopySizingResult:
    """Compute account-pipeline copy notional with multipliers and min/max clamping."""
    strategy_name = normalize_strategy(strategy)
    leader_notional = max(_DECIMAL_ZERO, _to_decimal(leader_notional_usdc))
    copy_pct = max(_DECIMAL_ZERO, _to_decimal(copy_percentage))
    fixed_amt = max(_DECIMAL_ZERO, _to_decimal(fixed_amount))
    min_order = max(_DECIMAL_ONE, _to_decimal(min_order_usdc, "1"))
    max_order = max(min_order, _to_decimal(max_order_usdc, str(min_order)))

    strategy_obj = _DEFAULT_LEGACY_STRATEGIES[strategy_name]
    base_notional, reasoning = strategy_obj.base_notional(
        LegacyNotionalInputs(
            leader_notional=leader_notional,
            copy_pct=copy_pct,
            fixed_amt=fixed_amt,
            adaptive_basis=adaptive_basis,
            adaptive_min_percent=adaptive_min_percent,
            adaptive_max_percent=adaptive_max_percent,
            adaptive_threshold_usdc=adaptive_threshold_usdc,
            performance_win_rate=performance_win_rate,
        )
    )

    multiplier = get_trade_multiplier(
        leader_notional_usdc=leader_notional,
        tiered_multipliers=tiered_multipliers,
        trade_multiplier=trade_multiplier,
    )
    multiplier *= max(_DECIMAL_ZERO, _to_decimal(rank_multiplier, "1"))
    final_notional = base_notional * multiplier
    if multiplier != _DECIMAL_ONE:
        reasoning += f" → multiplier {multiplier:.4f}x"

    capped_by_max = False
    below_minimum = False

    if final_notional > max_order:
        final_notional = max_order
        capped_by_max = True
        reasoning += f" → capped at ${max_order:.4f}"

    if final_notional < min_order:
        final_notional = min_order
        below_minimum = True
        reasoning += f" → raised to minimum ${min_order:.4f}"

    return CopySizingResult(
        leader_notional_usdc=leader_notional,
        base_notional_usdc=base_notional,
        final_notional_usdc=final_notional,
        strategy=strategy_name,
        multiplier=multiplier,
        capped_by_max=capped_by_max,
        below_minimum=below_minimum,
        reasoning=reasoning,
    )
