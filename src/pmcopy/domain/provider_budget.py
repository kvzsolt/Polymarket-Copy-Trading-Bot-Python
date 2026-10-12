"""Provider usage settings, compatibility fields, and read-boundary failures."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TypedDict


class ProviderConfigurationError(ValueError):
    """Reject invalid accounting, unknown methods, and incompatible billing history."""


class BudgetExhausted(RuntimeError):
    """Reject dispatch without exposing provider endpoints or credentials."""


class ProviderThrottled(BudgetExhausted):
    """A weighted provider throughput window has no remaining capacity."""


@dataclass(frozen=True, slots=True)
class BudgetSettings:
    """Configure usage estimates without locally limiting provider requests.

    Legacy quota, reserve, rate, and verification fields remain accepted for
    existing configuration files but do not gate startup or provider usage.
    """

    free_plan_verified: bool = False
    baseline_verified: bool = False
    alchemy_monthly_limit: int = 0
    essential_reserve: int = 0
    alchemy_billing_day: int = 1
    infura_daily_limit: int = 0
    infura_essential_reserve: int = 0
    pending_daily_limit: int = 0
    initial_alchemy_used: int = 0
    initial_infura_used: int = 0
    alchemy_per_second: int = 0
    infura_per_second: int = 0
    stream_margin: Decimal = Decimal("1.15")

    def __post_init__(self) -> None:
        """Validate accounting inputs without applying provider plan ceilings."""
        for value in (self.free_plan_verified, self.baseline_verified):
            if type(value) is not bool:
                raise ProviderConfigurationError("verification flags must be booleans")
        for name in (
            "alchemy_monthly_limit",
            "essential_reserve",
            "alchemy_billing_day",
            "infura_daily_limit",
            "infura_essential_reserve",
            "pending_daily_limit",
            "initial_alchemy_used",
            "initial_infura_used",
            "alchemy_per_second",
            "infura_per_second",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ProviderConfigurationError(f"{name} must be a nonnegative integer")
        if not 1 <= self.alchemy_billing_day <= 28:
            raise ProviderConfigurationError("invalid Alchemy billing day")
        if (
            not isinstance(self.stream_margin, Decimal)
            or not self.stream_margin.is_finite()
            or not Decimal("1") <= self.stream_margin <= Decimal("3")
        ):
            raise ProviderConfigurationError("stream margin must be an exact Decimal in [1, 3]")


class ProviderUsage(TypedDict):
    """Describe a provider's estimated consumption without a local allowance."""

    period: str
    unit: str
    used: int
    limit: None
    remaining: None


class ProviderUsageSnapshot(TypedDict):
    """Report durable accounting independently of provider-side plan enforcement."""

    estimate_only: bool
    limits_enforced: bool
    as_of: str
    other_app_usage_included: str
    pending_daily_used: int
    alchemy: ProviderUsage
    infura: ProviderUsage
