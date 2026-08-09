"""Canonical trade-detection domain models.

Vendor payloads are normalized into these immutable values before they reach
copy-decision, sizing, risk, persistence, or execution code. The module
performs no I/O and imports no vendor libraries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from hashlib import sha256


class DetectionMode(str, Enum):
    """Exclusive runtime source used to detect leader trades."""

    POLLING = "polling"
    ON_CHAIN_PENDING = "on_chain_pending"
    ON_CHAIN_CONFIRMED = "on_chain_confirmed"


class DetectionFinality(str, Enum):
    """Settlement confidence attached to a leader-trade event."""

    PROVISIONAL = "provisional"
    CONFIRMED = "confirmed"


class TradeSide(str, Enum):
    """Prediction-market trade direction from the leader's perspective."""

    BUY = "BUY"
    SELL = "SELL"


_HEX = frozenset("0123456789abcdef")


def _normalize_wallet(value: str) -> str:
    """Return a canonical lowercase 20-byte EVM address or raise."""

    normalized = str(value or "").strip().lower()
    if len(normalized) != 42 or not normalized.startswith("0x"):
        raise ValueError("leader_wallet must be a 20-byte hex address")
    if any(character not in _HEX for character in normalized[2:]):
        raise ValueError("leader_wallet must be hexadecimal")
    return normalized


def _finite_positive(value: Decimal, *, name: str) -> Decimal:
    """Return a finite positive Decimal or raise."""

    parsed = Decimal(str(value))
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{name} must be finite and > 0")
    return parsed


def _aware(value: datetime, *, name: str) -> datetime:
    """Return a timezone-aware datetime or raise."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


@dataclass(frozen=True, slots=True)
class LeaderTradeEvent:
    """One normalized economic fill or provisional fill intent.

    Attributes:
        leader_wallet: Canonical lowercase leader EVM wallet.
        token_id: Polymarket outcome token id.
        condition_id: Optional market condition id.
        side: Leader trade direction.
        price: Execution price in (0, 1).
        shares: Filled outcome shares.
        notional: pUSD notional (price * shares for BUY cost).
        mode: Detection source that produced the event.
        finality: Provisional or confirmed settlement confidence.
        detected_at: When the local pipeline observed the event.
        venue_timestamp: When the venue reports the event occurred.
        transaction_hash: Optional on-chain transaction hash.
        log_index: Optional event log index within a transaction.
        order_hash: Optional EIP-712 order hash.
        source_identity: Stable per-source row identity for exact dedup.
    """

    leader_wallet: str
    token_id: str
    condition_id: str | None
    side: TradeSide
    price: Decimal
    shares: Decimal
    notional: Decimal
    mode: DetectionMode
    finality: DetectionFinality
    detected_at: datetime
    venue_timestamp: datetime
    transaction_hash: str | None = None
    log_index: int | None = None
    order_hash: str | None = None
    source_identity: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "leader_wallet", _normalize_wallet(self.leader_wallet))
        token_id = str(self.token_id or "").strip()
        if not token_id:
            raise ValueError("token_id is required")
        object.__setattr__(self, "token_id", token_id)
        price = Decimal(str(self.price))
        if not price.is_finite() or not Decimal("0") < price < Decimal("1"):
            raise ValueError("price must be finite and in (0, 1)")
        object.__setattr__(self, "price", price)
        object.__setattr__(
            self,
            "shares",
            _finite_positive(self.shares, name="shares"),
        )
        object.__setattr__(
            self,
            "notional",
            _finite_positive(self.notional, name="notional"),
        )
        object.__setattr__(
            self,
            "detected_at",
            _aware(self.detected_at, name="detected_at"),
        )
        object.__setattr__(
            self,
            "venue_timestamp",
            _aware(self.venue_timestamp, name="venue_timestamp"),
        )
        if self.log_index is not None and int(self.log_index) < 0:
            raise ValueError("log_index must be non-negative")
        identity = str(self.source_identity or "").strip()
        if not identity:
            identity = self._fallback_source_identity()
        object.__setattr__(self, "source_identity", identity)

    @property
    def is_provisional(self) -> bool:
        """Return whether this signal may still be retracted."""

        return self.finality is DetectionFinality.PROVISIONAL

    @property
    def is_actionable(self) -> bool:
        """Return whether orchestration may create a copy intent."""

        return self.finality in {
            DetectionFinality.PROVISIONAL,
            DetectionFinality.CONFIRMED,
        }

    @property
    def exact_key(self) -> str:
        """Return the source-event identity used for exact duplicate removal."""

        payload = "|".join(
            (
                self.mode.value,
                self.source_identity,
                self.transaction_hash or "",
                "" if self.log_index is None else str(self.log_index),
            )
        )
        return sha256(payload.encode("utf-8")).hexdigest()

    @property
    def economic_key(self) -> str:
        """Return a cross-source key for pending-to-confirmed promotion."""

        if self.order_hash:
            payload = f"order|{self.transaction_hash or ''}|{self.order_hash.lower()}"
        else:
            payload = "|".join(
                (
                    "economic",
                    self.transaction_hash or "",
                    self.leader_wallet,
                    self.token_id,
                    self.side.value,
                    str(self.price.normalize()),
                    str(self.shares.normalize()),
                    str(self.notional.normalize()),
                )
            )
        return sha256(payload.encode("utf-8")).hexdigest()

    def _fallback_source_identity(self) -> str:
        parts = (
            self.transaction_hash or "no-tx",
            "no-log" if self.log_index is None else str(self.log_index),
            self.order_hash or "no-order",
            self.leader_wallet,
            self.token_id,
            self.side.value,
            str(self.price),
            str(self.shares),
        )
        return sha256("|".join(parts).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class LeaderTradeRetraction:
    """Retraction of a previously emitted provisional event.

    A retraction is bookkeeping evidence. It is never translated into an
    inverse trade automatically.
    """

    transaction_hash: str
    source_identity: str
    leader_wallet: str
    original_side: TradeSide
    reason: object
    observed_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "leader_wallet",
            _normalize_wallet(self.leader_wallet),
        )
        object.__setattr__(
            self,
            "observed_at",
            _aware(self.observed_at, name="observed_at"),
        )


__all__ = [
    "DetectionFinality",
    "DetectionMode",
    "LeaderTradeEvent",
    "LeaderTradeRetraction",
    "TradeSide",
]