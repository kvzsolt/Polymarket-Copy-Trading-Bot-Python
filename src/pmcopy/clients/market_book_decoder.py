"""Validate complete critical market messages before shared book mutation."""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True, slots=True)
class DecodedBookChange:
    """One exact outcome-level delta and optional executable top of book."""

    token_id: str
    side: str
    price: Decimal
    size: Decimal
    best_bid: Decimal | None
    best_ask: Decimal | None


class MarketBookDecoder:
    """Parse venue payloads atomically; malformed financial evidence is an error."""

    @classmethod
    def optional_quote(cls, payload: Mapping[str, object], *aliases: str) -> Decimal | None:
        """Read the first supplied quote alias without treating zero as missing."""
        for name in aliases:
            value = payload.get(name)
            if value is not None:
                return cls.quote_price(value)
        return None

    @classmethod
    def levels(cls, payload: object) -> dict[Decimal, Decimal]:
        """Return complete validated levels, preserving a legitimate empty side."""
        if isinstance(payload, Mapping):
            values = tuple(payload.items())
        elif isinstance(payload, list):
            values = tuple(
                (row.get("price"), row.get("size")) if isinstance(row, Mapping) else row
                for row in payload
            )
        else:
            raise ValueError("Book levels must be an array or mapping")
        levels: dict[Decimal, Decimal] = {}
        for pair in values:
            if not isinstance(pair, (list, tuple)) or len(pair) < 2:
                raise ValueError("Book level must contain price and shares")
            price, size = cls.price(pair[0]), cls.shares(pair[1])
            if size > 0:
                levels[price] = size
        return levels

    @classmethod
    def changes(cls, payload: object) -> tuple[DecodedBookChange, ...]:
        """Decode every delta before any price or quantity reaches shared state."""
        if not isinstance(payload, list):
            raise ValueError("Book changes must be an array")
        changes: list[DecodedBookChange] = []
        for row in payload:
            if not isinstance(row, Mapping):
                raise ValueError("Book change must be an object")
            token = (
                row.get("asset_id") or row.get("asset") or row.get("token_id") or row.get("token")
            )
            if not isinstance(token, str) or not token.strip():
                raise ValueError("Book change requires a token identity")
            side = str(row.get("side", "")).upper()
            sides = {"BUY": "BUY", "BID": "BUY", "SELL": "SELL", "ASK": "SELL"}
            if side not in sides:
                raise ValueError("Book change side is invalid")
            changes.append(
                DecodedBookChange(
                    token.strip(),
                    sides[side],
                    cls.price(row.get("price")),
                    cls.shares(row.get("size")),
                    cls.quote_price(row["best_bid"]) if row.get("best_bid") is not None else None,
                    cls.quote_price(row["best_ask"]) if row.get("best_ask") is not None else None,
                )
            )
        return tuple(changes)

    @classmethod
    def price(cls, value: object) -> Decimal:
        """Validate a finite probability price strictly between zero and one."""
        price = cls._decimal(value)
        if not Decimal(0) < price < Decimal(1):
            raise ValueError("Book price must be in (0, 1)")
        return price

    @classmethod
    def shares(cls, value: object) -> Decimal:
        """Validate finite non-negative shares; zero removes a book level."""
        shares = cls._decimal(value)
        if shares < 0:
            raise ValueError("Book shares cannot be negative")
        return shares

    @classmethod
    def quote_price(cls, value: object) -> Decimal:
        """Preserve venue top-of-book boundary markers zero and one."""
        price = cls._decimal(value)
        if not Decimal(0) <= price <= Decimal(1):
            raise ValueError("Quoted book boundary must be in [0, 1]")
        return price

    @staticmethod
    def _decimal(value: object) -> Decimal:
        if isinstance(value, bool) or not isinstance(value, (Decimal, str, int, float)):
            raise ValueError("Book numeric field is invalid")
        try:
            result = Decimal(str(value))
        except InvalidOperation:
            raise ValueError("Book numeric field is invalid") from None
        if not result.is_finite():
            raise ValueError("Book numeric field must be finite")
        return result
