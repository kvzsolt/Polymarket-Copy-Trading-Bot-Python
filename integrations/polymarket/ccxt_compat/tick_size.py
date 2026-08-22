"""Venue-neutral arbitrary tick-size support for Polymarket execution.

Polymarket market metadata can return any exact tick increment (``0.1``,
``0.01``, ``0.005``, ``0.0025``, ``0.001``, ``0.0001``).  Tick sizes are
``Decimal`` end to end, quantization rounds by integer multiples of the tick
(never by decimal places), and missing metadata fails closed — there is no
silent ``0.01`` default anywhere on this path.
"""

from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Literal, Protocol

OrderSide = Literal["BUY", "SELL"]

_DEFAULT_TTL_SECONDS = 300.0


class InvalidTickSize(ValueError):
    """Raised when a tick size is unusable for order construction."""


class TickSizeUnavailable(RuntimeError):
    """Raised when no authoritative source can provide a tick size."""


def _coerce_decimal(value: object, *, field: str, error: type[Exception]) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise error(f"{field} must be a finite decimal") from exc
    return result


@dataclass(frozen=True, slots=True)
class TickSize:
    """Validated venue tick size."""

    value: Decimal

    def __post_init__(self) -> None:
        if not self.value.is_finite():
            raise ValueError("Tick size must be finite")
        if self.value <= Decimal("0"):
            raise ValueError("Tick size must be greater than zero")


def quantize_price_to_tick(
    *,
    price: Decimal,
    tick_size: Decimal,
    side: OrderSide,
) -> Decimal:
    """Return an exact, side-conservative integer multiple of ``tick_size``.

    BUY limits round down so the signed order never pays above the caller's
    cap; SELL limits round up so the signed order never receives below the
    caller's floor.  Rounding happens in tick units because tick sizes such as
    ``0.0025`` are not representable by a fixed number of decimal places.
    """

    tick = _coerce_decimal(tick_size, field="tick_size", error=InvalidTickSize)
    if not tick.is_finite() or tick <= 0:
        raise InvalidTickSize("tick_size must be finite and greater than zero")
    raw_price = _coerce_decimal(price, field="price", error=ValueError)
    if not raw_price.is_finite():
        raise ValueError("price must be finite")
    normalized_side = str(side).upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported order side: {side!r}")

    tick_units = raw_price / tick
    if normalized_side == "BUY":
        rounded_units = tick_units.to_integral_value(rounding=ROUND_FLOOR)
    else:
        rounded_units = tick_units.to_integral_value(rounding=ROUND_CEILING)
    quantized = rounded_units * tick
    if quantized <= 0 or quantized >= 1:
        raise InvalidTickSize("tick-quantized price is outside the tradable range")
    return quantized


class TickSizeResolver(Protocol):
    """Resolve and cache the authoritative tick size for one outcome token."""

    async def resolve(self, token_id: str) -> Decimal: ...

    async def note_observed_tick(self, token_id: str, tick_size: object) -> None: ...

    def invalidate(self, token_id: str) -> None: ...


class MarketMetadataClient(Protocol):
    """Minimal metadata surface used as the tick-size fallback source."""

    async def get_tick_size(self, token_id: str) -> Decimal | None: ...


def _validated_tick(value: object, token_id: str) -> Decimal:
    tick = _coerce_decimal(value, field="tick_size", error=InvalidTickSize)
    if not tick.is_finite() or tick <= 0:
        raise InvalidTickSize(f"invalid tick size for token {token_id}")
    return tick


@dataclass(frozen=True, slots=True)
class _CachedTick:
    value: Decimal
    cached_at: float


class PolymarketTickSizeResolver:
    """Resolve tick sizes from CCXT metadata first, then the public CLOB API.

    Resolution order is explicit and testable:

    1. CCXT market metadata (``ccxt_client.get_tick_size``);
    2. the official public Polymarket market-metadata client;
    3. ``TickSizeUnavailable`` — never a hidden default.

    Resolved values are cached with a bounded TTL.  ``note_observed_tick``
    lets the execution gateway refresh the cache (invalidating a stale value)
    whenever a fresher venue book reports a different tick, which Polymarket
    can do while a market is live.
    """

    def __init__(
        self,
        *,
        ccxt_client: object | None = None,
        market_metadata_client: MarketMetadataClient | None = None,
        ttl_seconds: float = _DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero")
        self._ccxt_client = ccxt_client
        self._market_metadata_client = market_metadata_client
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._cache: dict[str, _CachedTick] = {}

    async def resolve(self, token_id: str) -> Decimal:
        token = str(token_id)
        cached = self._cache.get(token)
        if cached is not None and (self._clock() - cached.cached_at) < self._ttl_seconds:
            return cached.value

        tick = await self._source_tick(token)
        if tick is None:
            raise TickSizeUnavailable(f"tick size unavailable for token {token}")
        value = _validated_tick(tick, token)
        self._cache[token] = _CachedTick(value=value, cached_at=self._clock())
        return value

    async def note_observed_tick(self, token_id: str, tick_size: object) -> None:
        """Cache an authoritative tick observed from a fresh venue book."""

        token = str(token_id)
        value = _validated_tick(tick_size, token)
        self._cache[token] = _CachedTick(value=value, cached_at=self._clock())

    def invalidate(self, token_id: str) -> None:
        """Drop the cached tick so the next resolution goes to the sources."""

        self._cache.pop(str(token_id), None)

    async def _source_tick(self, token: str) -> object | None:
        if self._ccxt_client is not None:
            getter = getattr(self._ccxt_client, "get_tick_size", None)
            if callable(getter):
                tick = getter(token)
                if inspect.isawaitable(tick):
                    tick = await tick
                if tick is not None:
                    return tick
        if self._market_metadata_client is not None:
            tick = await self._market_metadata_client.get_tick_size(token)
            if tick is not None:
                return tick
        return None


_PUBLIC_CLOB_BOOK_URL = "https://clob.polymarket.com/book"


class PublicClobTickSizeSource:
    """Read ``tick_size`` from the official public CLOB book endpoint.

    Transport or schema failures return ``None`` so the resolver can fail
    closed; this source never substitutes a default tick.
    """

    def __init__(
        self,
        *,
        fetch_json: Callable[[str], Awaitable[object]] | None = None,
        base_url: str = _PUBLIC_CLOB_BOOK_URL,
    ) -> None:
        if fetch_json is None:
            from utils.fetchData import fetch_data

            fetch_json = fetch_data
        self._fetch_json = fetch_json
        self._base_url = base_url.rstrip("/")

    async def get_tick_size(self, token_id: str) -> Decimal | None:
        try:
            payload = await self._fetch_json(f"{self._base_url}?token_id={token_id}")
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        raw = payload.get("tick_size")
        if raw is None:
            return None
        try:
            value = Decimal(str(raw))
        except InvalidOperation:
            return None
        if not value.is_finite() or value <= 0:
            return None
        return value


__all__ = [
    "InvalidTickSize",
    "MarketMetadataClient",
    "PolymarketTickSizeResolver",
    "PublicClobTickSizeSource",
    "TickSize",
    "TickSizeResolver",
    "TickSizeUnavailable",
    "quantize_price_to_tick",
]
