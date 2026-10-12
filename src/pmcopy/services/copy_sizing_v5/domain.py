"""Exact-decimal domain models for V5 copy sizing.

The engine sizes BUY intents only. Existing sell/partial-exit reconstruction
remains authoritative for exits so a sizing mode can never accidentally turn a
partial leader exit into a fresh short position.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any, Literal

CopySide = Literal["BUY", "SELL"]


@dataclass(frozen=True, slots=True)
class CopySizingRequest:
    leader_wallet: str
    leader_trade_notional: Decimal
    follower_pusd_balance: Decimal
    leader_pusd_balance: Decimal | None = None
    side: CopySide = "BUY"
    follower_market_exposure: Decimal = Decimal("0")
    follower_reserved_notional: Decimal = Decimal("0")
    leader_market_position_notional: Decimal | None = None
    leader_total_portfolio_notional: Decimal | None = None
    leader_market_weight: Decimal | None = None
    current_open_positions: int = 0
    is_existing_market: bool = False
    trust_multiplier: Decimal = Decimal("1")
    # Deprecated V5 compatibility inputs. V6 ignores both at runtime because
    # niche evidence is discovery-only once a wallet joins the roster.
    niche_multiplier: Decimal = Decimal("1")
    niche_maximum_copy_notional: Decimal | None = None
    adaptive_signal: Decimal | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def with_updates(self, **changes: Any) -> "CopySizingRequest":
        return replace(self, **changes)


@dataclass(frozen=True, slots=True)
class CopySizingDecision:
    allowed: bool
    mode: str
    reason: str
    requested_notional: Decimal
    final_notional: Decimal
    copyable_wallet_budget: Decimal
    available_wallet_budget: Decimal
    configured_percentage: Decimal | None
    effective_configured_percentage: Decimal | None
    balance_ratio_percent: Decimal | None
    balance_adjusted_copy_percent: Decimal | None
    effective_leader_trade_percent: Decimal | None
    effective_leader_balance_percent: Decimal | None
    effective_follower_balance_percent: Decimal | None
    market_target_notional: Decimal | None = None
    combined_multiplier: Decimal = Decimal("1")
    flags: tuple[str, ...] = ()
    audit: dict[str, Any] = field(default_factory=dict)


__all__ = ["CopySide", "CopySizingRequest", "CopySizingDecision"]
