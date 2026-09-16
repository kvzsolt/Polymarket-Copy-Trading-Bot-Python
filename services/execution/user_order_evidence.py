"""Normalize authenticated Polymarket user-channel order evidence.

The user channel uses ``id`` for different identities: an order event carries
the order hash while a trade event carries a trade UUID. This module keeps that
adapter-shaped parsing outside reconciliation and exposes immutable evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Mapping, Self


class UserOrderEvidenceKind(StrEnum):
    """Classify normalized authenticated user-channel evidence."""

    PLACED = "placed"
    UPDATED = "updated"
    CANCELLED = "cancelled"
    TRADE_MATCHED = "trade_matched"
    TRADE_MINED = "trade_mined"
    TRADE_CONFIRMED = "trade_confirmed"
    TRADE_FAILED = "trade_failed"


@dataclass(frozen=True, slots=True)
class UserOrderEvidence:
    """Represent one validated user-channel fact about an order."""

    order_id: str
    kind: UserOrderEvidenceKind
    size_matched: Decimal
    original_size: Decimal | None
    timestamp: int | None
    trade_id: str | None = None

    @property
    def is_terminal(self) -> bool:
        """Return whether this evidence proves the order can no longer fill."""

        return self.kind is UserOrderEvidenceKind.CANCELLED

    def as_payload(self) -> dict[str, object]:
        """Return a JSON-safe persistence payload using exact decimal strings."""

        return {
            "order_id": self.order_id,
            "kind": self.kind.value,
            "size_matched": str(self.size_matched),
            "original_size": (str(self.original_size) if self.original_size is not None else None),
            "timestamp": self.timestamp,
            "trade_id": self.trade_id,
            "terminal": self.is_terminal,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> Self | None:
        """Rehydrate validated evidence from a persisted JSON mapping.

        Args:
            payload: Stored evidence mapping.

        Returns:
            Immutable evidence, or ``None`` when the payload is malformed.
        """

        try:
            kind = UserOrderEvidenceKind(str(payload.get("kind") or ""))
            order_id = str(payload.get("order_id") or "").strip()
            size_matched = Decimal(str(payload.get("size_matched") or "0"))
        except (InvalidOperation, ValueError):
            return None
        if not order_id or not size_matched.is_finite() or size_matched < 0:
            return None
        original_size = UserOrderEvidenceParser.optional_decimal(payload.get("original_size"))
        timestamp = UserOrderEvidenceParser.optional_timestamp(payload.get("timestamp"))
        trade_id_raw = payload.get("trade_id")
        trade_id = str(trade_id_raw).strip() if trade_id_raw is not None else None
        return cls(
            order_id=order_id,
            kind=kind,
            size_matched=size_matched,
            original_size=original_size,
            timestamp=timestamp,
            trade_id=trade_id or None,
        )


class UserOrderEvidenceParser:
    """Validate raw user-channel messages and separate trade/order identities."""

    _TRADE_KINDS = {
        "MATCHED": UserOrderEvidenceKind.TRADE_MATCHED,
        "MINED": UserOrderEvidenceKind.TRADE_MINED,
        "CONFIRMED": UserOrderEvidenceKind.TRADE_CONFIRMED,
        "FAILED": UserOrderEvidenceKind.TRADE_FAILED,
    }

    def parse(self, event: Mapping[str, object]) -> UserOrderEvidence | None:
        """Return normalized evidence for a recognized authenticated event.

        Args:
            event: Raw user-channel event mapping.

        Returns:
            Validated immutable evidence, or ``None`` when identity or numeric
            fields are missing or malformed.
        """

        event_type = str(event.get("event_type") or "").strip().lower()
        order_id = self.order_id_for(event)
        if not order_id:
            return None
        raw_size_matched = event.get("size_matched")
        if raw_size_matched is None:
            raw_size_matched = event.get("matched_size")
        if raw_size_matched is None and event_type == "trade":
            raw_size_matched = event.get("size")
        raw_order_kind = str(event.get("type") or event.get("status") or "").upper()
        if event_type == "order" and raw_order_kind == "CANCELLATION":
            if raw_size_matched is None:
                return None
        size_matched = self.optional_decimal(raw_size_matched)
        if raw_size_matched is not None and size_matched is None:
            return None
        if size_matched is None:
            size_matched = Decimal("0")
        original_size = self.optional_decimal(event.get("original_size"))
        timestamp = self.optional_timestamp(event.get("timestamp"))

        if event_type == "order":
            if raw_order_kind == "CANCELLATION":
                kind = UserOrderEvidenceKind.CANCELLED
            elif raw_order_kind == "PLACEMENT":
                kind = UserOrderEvidenceKind.PLACED
            elif raw_order_kind == "UPDATE":
                kind = UserOrderEvidenceKind.UPDATED
            else:
                return None
            return UserOrderEvidence(
                order_id=order_id,
                kind=kind,
                size_matched=size_matched,
                original_size=original_size,
                timestamp=timestamp,
            )

        if event_type == "trade":
            status = str(event.get("status") or event.get("trade_status") or "").upper()
            trade_kind = self._TRADE_KINDS.get(status)
            if trade_kind is None:
                return None
            trade_id = str(event.get("id") or event.get("trade_id") or "").strip()
            return UserOrderEvidence(
                order_id=order_id,
                kind=trade_kind,
                size_matched=size_matched,
                original_size=original_size,
                timestamp=timestamp,
                trade_id=trade_id or None,
            )
        return None

    def order_id_for(self, event: Mapping[str, object]) -> str | None:
        """Return the order hash represented by ``event``, never its trade UUID.

        Args:
            event: Raw user-channel order or trade mapping.

        Returns:
            An explicit/taker order hash, a sole maker order hash, or ``None``
            when maker attribution is ambiguous.
        """

        explicit = event.get("order_id") or event.get("orderID")
        if explicit is not None and str(explicit).strip():
            return str(explicit).strip()
        event_type = str(event.get("event_type") or "").strip().lower()
        if event_type == "order":
            order_id = str(event.get("id") or "").strip()
            return order_id or None
        if event_type != "trade":
            return None
        taker = str(event.get("taker_order_id") or "").strip()
        if taker:
            return taker
        raw_makers = event.get("maker_orders")
        if not isinstance(raw_makers, list):
            return None
        maker_ids = {
            str(item.get("order_id") or "").strip()
            for item in raw_makers
            if isinstance(item, Mapping) and str(item.get("order_id") or "").strip()
        }
        if len(maker_ids) != 1:
            return None
        return next(iter(maker_ids))

    @staticmethod
    def optional_decimal(value: object) -> Decimal | None:
        """Parse a finite non-negative decimal boundary value."""

        if value is None or str(value).strip() == "":
            return None
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None
        if not parsed.is_finite() or parsed < 0:
            return None
        return parsed

    @staticmethod
    def optional_timestamp(value: object) -> int | None:
        """Parse a non-negative integral venue timestamp."""

        if value is None or str(value).strip() == "":
            return None
        try:
            parsed = int(str(value))
        except (TypeError, ValueError):
            return None
        return parsed if parsed >= 0 else None


__all__ = [
    "UserOrderEvidence",
    "UserOrderEvidenceKind",
    "UserOrderEvidenceParser",
]
