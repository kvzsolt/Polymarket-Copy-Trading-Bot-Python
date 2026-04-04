from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


_DECIMAL_ZERO = Decimal("0")
_DECIMAL_ONE = Decimal("1")


@dataclass(frozen=True)
class MultiplierTier:
    min_usdc: Decimal
    max_usdc: Decimal | None
    multiplier: Decimal


@dataclass(frozen=True)
class CopySizingResult:
    leader_notional_usdc: Decimal
    base_notional_usdc: Decimal
    final_notional_usdc: Decimal
    strategy: str
    multiplier: Decimal
    capped_by_max: bool
    below_minimum: bool
    reasoning: str


def _to_decimal(value, default: str = "0") -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        if value is None or value == "":
            return Decimal(default)
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def normalize_strategy(strategy: str | None) -> str:
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
    base_percent,
    leader_notional_usdc,
    adaptive_min_percent=None,
    adaptive_max_percent=None,
    adaptive_threshold_usdc=None,
) -> Decimal:
    """Port of the TypeScript trade-size adaptive percentage logic.

    Small orders can be copied more aggressively, while large whale orders
    automatically taper down to a smaller percentage.
    """
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
            raise ValueError(f"Invalid multiplier in tier \"{raw_tier}\": {multiplier_part}")

        range_part = range_part.strip()
        if range_part.endswith("+"):
            min_usdc = _to_decimal(range_part[:-1], "-1")
            if min_usdc < _DECIMAL_ZERO:
                raise ValueError(f"Invalid minimum value in tier \"{raw_tier}\": {range_part}")
            tiers.append(MultiplierTier(min_usdc=min_usdc, max_usdc=None, multiplier=multiplier))
            continue

        if "-" not in range_part:
            raise ValueError(
                f'Invalid range format in tier "{raw_tier}". Use "min-max" or "min+"'
            )

        min_part, max_part = range_part.split("-", 1)
        min_usdc = _to_decimal(min_part, "-1")
        max_usdc = _to_decimal(max_part, "-1")
        if min_usdc < _DECIMAL_ZERO:
            raise ValueError(f"Invalid minimum value in tier \"{raw_tier}\": {min_part}")
        if max_usdc <= min_usdc:
            raise ValueError(
                f"Invalid maximum value in tier \"{raw_tier}\": {max_part} (must be > {min_usdc})"
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
    leader_notional_usdc,
    tiered_multipliers: str | list[MultiplierTier] | None = None,
    trade_multiplier=None,
) -> Decimal:
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


def calculate_copy_notional(
    *,
    strategy: str,
    leader_notional_usdc,
    copy_percentage,
    fixed_amount,
    min_order_usdc,
    max_order_usdc,
    trade_multiplier=None,
    tiered_multipliers: str | list[MultiplierTier] | None = None,
    adaptive_basis: str = "trade_size",
    adaptive_min_percent=None,
    adaptive_max_percent=None,
    adaptive_threshold_usdc=None,
    performance_win_rate=None,
) -> CopySizingResult:
    strategy_name = normalize_strategy(strategy)
    leader_notional = max(_DECIMAL_ZERO, _to_decimal(leader_notional_usdc))
    copy_pct = max(_DECIMAL_ZERO, _to_decimal(copy_percentage))
    fixed_amt = max(_DECIMAL_ZERO, _to_decimal(fixed_amount))
    min_order = max(_DECIMAL_ONE, _to_decimal(min_order_usdc, "1"))
    max_order = max(min_order, _to_decimal(max_order_usdc, str(min_order)))

    base_notional: Decimal
    reasoning: str

    if strategy_name == "fixed":
        base_notional = fixed_amt
        reasoning = f"fixed amount ${fixed_amt:.4f}"
    elif strategy_name == "adaptive":
        basis = str(adaptive_basis or "trade_size").strip().lower()
        if basis == "performance":
            win_rate = _to_decimal(performance_win_rate, "50")
            if win_rate > _DECIMAL_ONE:
                win_rate = win_rate / Decimal("100")
            win_rate = max(_DECIMAL_ZERO, min(_DECIMAL_ONE, win_rate))
            adaptive_multiplier = Decimal("0.5") + (win_rate * Decimal("1.5"))
            base_notional = leader_notional * (copy_pct / Decimal("100")) * adaptive_multiplier
            reasoning = (
                f"adaptive-performance {copy_pct:.4f}% of ${leader_notional:.4f}"
                f" × {adaptive_multiplier:.4f} (win_rate={win_rate:.4f})"
            )
        else:
            adaptive_pct = calculate_trade_size_adaptive_percentage(
                base_percent=copy_pct,
                leader_notional_usdc=leader_notional,
                adaptive_min_percent=adaptive_min_percent,
                adaptive_max_percent=adaptive_max_percent,
                adaptive_threshold_usdc=adaptive_threshold_usdc,
            )
            base_notional = leader_notional * (adaptive_pct / Decimal("100"))
            reasoning = (
                f"adaptive-trade-size {adaptive_pct:.4f}% of ${leader_notional:.4f}"
            )
    else:
        base_notional = leader_notional * (copy_pct / Decimal("100"))
        reasoning = f"proportional {copy_pct:.4f}% of ${leader_notional:.4f}"

    multiplier = get_trade_multiplier(
        leader_notional_usdc=leader_notional,
        tiered_multipliers=tiered_multipliers,
        trade_multiplier=trade_multiplier,
    )
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
