"""Immutable per-owner observation policies and native V2 fill evidence."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from typing import Literal


@dataclass(frozen=True, slots=True)
class TraderDetectionPolicy:
    """One configured owner policy shared by every follower of that wallet."""

    wallet: str
    mode: Literal["mined", "pending"] = "mined"
    provider: Literal["alchemy"] = "alchemy"
    on_budget_exhausted: Literal["mined", "pause"] = "mined"
    enabled: bool = True

    def __post_init__(self) -> None:
        """Reject unsupported detection modes, providers, and ambiguous owners."""
        if not isinstance(self.wallet, str) or not re.fullmatch(r"0x[0-9a-fA-F]{40}", self.wallet):
            raise ValueError("wallet must be a 20-byte address")
        object.__setattr__(self, "wallet", self.wallet.lower())
        if self.mode not in ("mined", "pending"):
            raise ValueError("detection mode must be mined or pending")
        if self.provider != "alchemy":
            raise ValueError("unsupported detection provider")
        if self.on_budget_exhausted not in ("mined", "pause"):
            raise ValueError("budget fallback must be mined or pause")
        if type(self.enabled) is not bool:
            raise ValueError("enabled must be a boolean")


@dataclass(frozen=True, slots=True)
class NativeOwnerFill:
    """One included or removed order-owner log, with exact integer-unit economics."""

    owner: str
    counterparty: str
    exchange: str
    transaction_hash: str
    log_index: int
    block_number: int
    block_hash: str
    order_hash: str
    token_id: str
    side: Literal["BUY", "SELL"]
    shares: Decimal
    notional: Decimal
    price: Decimal
    fee: Decimal
    native_taker: bool
    removed: bool

    @property
    def action_id(self) -> str:
        """Match the pmrecon native-owner identity without collapsing sibling fills."""
        key = (
            f"137|{self.exchange}|{self.transaction_hash}|{self.log_index}|"
            f"{self.order_hash}|{self.owner}"
        )
        return sha256(key.encode()).hexdigest()

    @property
    def inclusion_id(self) -> str:
        """Keep a reorg's removed inclusion separate from later inclusion evidence."""
        return f"{self.action_id}:{self.block_hash}"


@dataclass(frozen=True, slots=True)
class TraderDetectionState:
    """Observed source health distinct from immutable desired configuration."""

    wallet: str
    desired_mode: Literal["mined", "pending"]
    effective_mode: Literal["mined", "pending", "paused"]
    provider: Literal["alchemy"]
    on_budget_exhausted: Literal["mined", "pause"]
    enabled: bool
    degraded_reason: str | None
    last_observation_ms: int | None
    restart_required: Literal[True] = True
