"""Pure order-book normalization and quality rules."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

THIN_BOOK_BID_CEILING = Decimal("0.03")
THIN_BOOK_ASK_FLOOR = Decimal("0.97")


def is_thin_or_empty_book(bid: Decimal | None, ask: Decimal | None) -> bool:
    if bid is None or ask is None:
        return True
    return bid <= THIN_BOOK_BID_CEILING and ask >= THIN_BOOK_ASK_FLOOR


def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def top_of_book_from_levels(levels: object, *, side: str) -> Decimal | None:
    if side not in {"bid", "ask"}:
        raise ValueError(f"Unsupported book side: {side}")
    values = levels.keys() if isinstance(levels, dict) else (levels or [])
    prices: list[Decimal] = []
    for level in values:
        candidate = level if isinstance(levels, dict) else getattr(level, "price", None)
        if candidate is None and isinstance(level, dict):
            candidate = level.get("price")
        price = _decimal(candidate)
        if price is not None:
            prices.append(price)
    if not prices:
        return None
    return max(prices) if side == "bid" else min(prices)


__all__ = [
    "THIN_BOOK_ASK_FLOOR",
    "THIN_BOOK_BID_CEILING",
    "is_thin_or_empty_book",
    "top_of_book_from_levels",
]
