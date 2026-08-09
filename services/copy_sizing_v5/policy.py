"""Validated configuration for V5 copy-sizing modes."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

_MODE_ALIASES = {
    "fixed": "fixed",
    "fixed_amount": "fixed",
    "fixed_buy_amount": "fixed",
    "exact": "exact_mirror",
    "exact_mirror": "exact_mirror",
    "mirror": "exact_mirror",
    "proportional": "proportional",
    "percentage": "proportional",
    "percentage_based": "proportional",
    "portfolio": "portfolio_weighted",
    "portfolio_weighted": "portfolio_weighted",
    "balance": "balance_weighted",
    "balance_weighted": "balance_weighted",
    "adaptive": "adaptive",
    "tiered": "tiered",
}


def normalize_mode(value: object) -> str:
    normalized = str(value or "proportional").strip().lower().replace("-", "_")
    try:
        return _MODE_ALIASES[normalized]
    except KeyError as exc:
        raise ValueError(f"unsupported sizing mode: {value!r}") from exc


def _decimal(value: object, *, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{name} must be decimal-like") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _get(source: object, name: str, default: Any) -> Any:
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


@dataclass(frozen=True, slots=True)
class CopySizingPolicy:
    mode: str = "proportional"
    percentage: Decimal = Decimal("4.0")
    fixed_amount: Decimal = Decimal("3.0")
    trade_multiplier: Decimal = Decimal("1.0")
    tiered_trade_multipliers: str = ""
    adaptive_basis: str = "trade_size"
    adaptive_min_percent: Decimal = Decimal("0.0")
    adaptive_max_percent: Decimal = Decimal("0.0")
    adaptive_threshold_usdc: Decimal = Decimal("500.0")
    buy_min_amount: Decimal = Decimal("5.0")
    buy_max_amount: Decimal = Decimal("8.0")
    position_threshold: int = 0
    max_position_pct: Decimal = Decimal("0.10")
    enable_trust_size_multipliers: bool = False
    trust_size_multiplier_min: Decimal = Decimal("0.5")
    trust_size_multiplier_max: Decimal = Decimal("1.25")

    # V5 additions.
    copyable_balance_percent: Decimal = Decimal("75")
    proportional_balance_cap_enabled: bool = True
    require_leader_balance_for_proportional: bool = True
    portfolio_weight_multiplier: Decimal = Decimal("1")
    strict_fixed_amount: bool = True
    strict_exact_mirror: bool = True

    def __post_init__(self) -> None:
        canonical = normalize_mode(self.mode)
        object.__setattr__(self, "mode", canonical)
        decimal_fields = (
            "percentage",
            "fixed_amount",
            "trade_multiplier",
            "adaptive_min_percent",
            "adaptive_max_percent",
            "adaptive_threshold_usdc",
            "buy_min_amount",
            "buy_max_amount",
            "max_position_pct",
            "trust_size_multiplier_min",
            "trust_size_multiplier_max",
            "copyable_balance_percent",
            "portfolio_weight_multiplier",
        )
        for name in decimal_fields:
            value = _decimal(getattr(self, name), name=name)
            object.__setattr__(self, name, value)

        for name in (
            "percentage",
            "fixed_amount",
            "trade_multiplier",
            "adaptive_min_percent",
            "adaptive_max_percent",
            "adaptive_threshold_usdc",
            "buy_min_amount",
            "buy_max_amount",
            "portfolio_weight_multiplier",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        if not Decimal("0") < self.copyable_balance_percent <= Decimal("100"):
            raise ValueError("copyable_balance_percent must be in (0, 100]")
        if not Decimal("0") < self.max_position_pct <= Decimal("1"):
            raise ValueError("max_position_pct must be in (0, 1]")
        if self.buy_max_amount > 0 and self.buy_max_amount < self.buy_min_amount:
            raise ValueError("buy_max_amount must be zero/disabled or >= buy_min_amount")
        if self.trust_size_multiplier_min <= 0 or self.trust_size_multiplier_max <= 0:
            raise ValueError("trust multipliers must be positive")
        if self.trust_size_multiplier_min > self.trust_size_multiplier_max:
            raise ValueError("trust multiplier minimum cannot exceed maximum")
        if self.adaptive_min_percent > self.adaptive_max_percent and self.adaptive_max_percent > 0:
            raise ValueError("adaptive_min_percent cannot exceed adaptive_max_percent")
        if self.position_threshold < 0:
            raise ValueError("position_threshold must be non-negative")
        if self.adaptive_basis not in {"trade_size", "performance"}:
            raise ValueError("adaptive_basis must be trade_size or performance")
        if self.mode == "fixed" and self.fixed_amount < self.buy_min_amount:
            raise ValueError("fixed_amount must be >= buy_min_amount in fixed mode")
        if self.mode == "proportional" and self.percentage <= 0 and not self.proportional_balance_cap_enabled:
            raise ValueError("proportional mode needs a positive percentage or balance-derived sizing")

    @property
    def copyable_balance_fraction(self) -> Decimal:
        return self.copyable_balance_percent / Decimal("100")

    @property
    def configured_percentage_fraction(self) -> Decimal:
        return self.percentage / Decimal("100")


def copy_sizing_policy_from_config(
    source: object,
    overrides: Mapping[str, Any] | None = None,
) -> CopySizingPolicy:
    """Map the branch's existing ``SizingConfig`` plus optional V5 fields."""

    defaults = CopySizingPolicy()
    names = (
        "mode",
        "percentage",
        "fixed_amount",
        "trade_multiplier",
        "tiered_trade_multipliers",
        "adaptive_basis",
        "adaptive_min_percent",
        "adaptive_max_percent",
        "adaptive_threshold_usdc",
        "buy_min_amount",
        "buy_max_amount",
        "position_threshold",
        "max_position_pct",
        "enable_trust_size_multipliers",
        "trust_size_multiplier_min",
        "trust_size_multiplier_max",
        "copyable_balance_percent",
        "proportional_balance_cap_enabled",
        "require_leader_balance_for_proportional",
        "portfolio_weight_multiplier",
        "strict_fixed_amount",
        "strict_exact_mirror",
    )
    values = {name: _get(source, name, getattr(defaults, name)) for name in names}
    for name, value in dict(overrides or {}).items():
        if name in values:
            values[name] = value
    return CopySizingPolicy(**values)


__all__ = ["CopySizingPolicy", "copy_sizing_policy_from_config", "normalize_mode"]
