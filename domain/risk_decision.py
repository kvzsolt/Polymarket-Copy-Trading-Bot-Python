"""Risk decision types — result of evaluating a proposed trade against risk policy."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RiskDecision:
    """Result of evaluating a proposed trade against risk policy."""

    allow: bool
    reason: str
    adjusted_notional_usdc: Decimal | None = None
    policy_name: str = "account_cap"
