"""Complete optional read/stream facade for ``ccxt.prediction.polymarket``.

The execution gateway remains the only mutation boundary.  This facade exposes
CCXT's prediction-native event, market-data, portfolio and websocket methods for
discovery and future OctoBot integration without leaking a crypto-style symbol
model into the application.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence


class CcxtPredictionMarketDataError(RuntimeError):
    pass


class CcxtPredictionCapabilityError(CcxtPredictionMarketDataError):
    pass


@dataclass(frozen=True, slots=True)
class VisibleCapacity:
    token_id: str
    side: str
    price_limit: Decimal
    shares: Decimal
    notional_usdc: Decimal
    levels: int


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CcxtPredictionMarketDataError(f"invalid {field}: {value!r}") from exc
    if not result.is_finite():
        raise CcxtPredictionMarketDataError(f"invalid {field}: non-finite")
    return result


async def _await_call(method: Any, *args: Any, **kwargs: Any) -> Any:
    result = method(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


class CcxtPredictionMarketData:
    """Prediction-native CCXT read and websocket surface."""

    def __init__(self, client_or_exchange: Any, *, timeout_seconds: float = 10.0) -> None:
        self._client = client_or_exchange
        self._exchange = getattr(client_or_exchange, "exchange", client_or_exchange)
        self._timeout_seconds = timeout_seconds

    @property
    def exchange(self) -> Any:
        return self._exchange

    def capabilities(self) -> dict[str, bool]:
        has = getattr(self._exchange, "has", {}) or {}
        names = (
            "fetchEvents",
            "fetchEvent",
            "fetchTicker",
            "fetchTickers",
            "fetchOrderBook",
            "fetchTrades",
            "fetchOHLCV",
            "fetchBalance",
            "fetchPositions",
            "fetchPosition",
            "fetchTradingFee",
            "fetchOpenInterest",
            "fetchMarkets",
            "fetchStatus",
            "fetchTime",
            "fetchMyTrades",
            "fetchOrderTrades",
            "fetchOpenOrders",
            "fetchOrder",
            "watchOrderBook",
            "watchTicker",
            "watchTrades",
            "watchOrders",
            "watchMyTrades",
        )
        return {name: bool(has.get(name, False)) for name in names}

    def _method(self, capability: str, snake_name: str) -> Any:
        has = getattr(self._exchange, "has", {}) or {}
        if has.get(capability) is False:
            raise CcxtPredictionCapabilityError(f"CCXT Polymarket does not support {capability}")
        method = getattr(self._exchange, snake_name, None)
        if not callable(method):
            raise CcxtPredictionCapabilityError(f"CCXT Polymarket does not expose {capability}")
        return method

    async def _call(self, capability: str, snake_name: str, *args: Any) -> Any:
        method = self._method(capability, snake_name)
        try:
            async with asyncio.timeout(self._timeout_seconds):
                return await _await_call(method, *args)
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except CcxtPredictionMarketDataError:
            raise
        except BaseException as exc:
            raise CcxtPredictionMarketDataError(
                f"{capability} failed: {type(exc).__name__}"
            ) from exc

    async def fetch_events(
        self,
        *,
        query: str | None = None,
        queries: Sequence[str] | None = None,
        tags: Sequence[str] | None = None,
        event_id: str | None = None,
        slug: str | None = None,
        status: str = "active",
        sort: str | None = None,
        limit: int = 100,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if not any((query, queries, tags, event_id, slug)):
            raise CcxtPredictionMarketDataError(
                "fetch_events requires query, queries, tags, event_id or slug"
            )
        request = dict(params or {})
        request.update(
            {
                key: value
                for key, value in {
                    "query": query,
                    "queries": list(queries) if queries else None,
                    "tags": list(tags) if tags else None,
                    "eventId": event_id,
                    "slug": slug,
                    "status": status,
                    "sort": sort,
                    "limit": limit,
                }.items()
                if value is not None
            }
        )
        value = await self._call("fetchEvents", "fetch_events", request)
        return list(value or ())

    async def fetch_event(
        self,
        event_id: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = await self._call("fetchEvent", "fetch_event", str(event_id), dict(params or {}))
        return dict(value or {})

    async def fetch_ticker(
        self, token_id: str, *, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        value = await self._call("fetchTicker", "fetch_ticker", str(token_id), dict(params or {}))
        return dict(value or {})

    async def fetch_tickers(
        self,
        token_ids: Sequence[str],
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not token_ids:
            raise CcxtPredictionMarketDataError("fetch_tickers requires token IDs")
        value = await self._call(
            "fetchTickers",
            "fetch_tickers",
            [str(token) for token in token_ids],
            dict(params or {}),
        )
        return dict(value or {})

    async def fetch_order_book(
        self,
        token_id: str,
        *,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = await self._call(
            "fetchOrderBook",
            "fetch_order_book",
            str(token_id),
            limit,
            dict(params or {}),
        )
        return dict(value or {})

    async def fetch_trades(
        self,
        token_id: str,
        *,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        value = await self._call(
            "fetchTrades",
            "fetch_trades",
            str(token_id),
            since,
            limit,
            dict(params or {}),
        )
        return list(value or ())

    async def fetch_ohlcv(
        self,
        token_id: str,
        timeframe: str = "1m",
        *,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[list[Any]]:
        value = await self._call(
            "fetchOHLCV",
            "fetch_ohlcv",
            str(token_id),
            timeframe,
            since,
            limit,
            dict(params or {}),
        )
        return list(value or ())

    async def fetch_balance(self, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        value = await self._call("fetchBalance", "fetch_balance", dict(params or {}))
        return dict(value or {})

    async def fetch_positions(
        self,
        token_ids: Sequence[str] | None = None,
        *,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        value = await self._call(
            "fetchPositions",
            "fetch_positions",
            [str(token) for token in token_ids] if token_ids else None,
            dict(params or {}),
        )
        return list(value or ())

    async def fetch_position(
        self,
        token_id: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = await self._call(
            "fetchPosition",
            "fetch_position",
            str(token_id),
            dict(params or {}),
        )
        return dict(value or {})

    async def fetch_trading_fee(
        self,
        token_id: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = await self._call(
            "fetchTradingFee",
            "fetch_trading_fee",
            str(token_id),
            dict(params or {}),
        )
        return dict(value or {})

    async def fetch_open_interest(
        self,
        token_id: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = await self._call(
            "fetchOpenInterest",
            "fetch_open_interest",
            str(token_id),
            dict(params or {}),
        )
        return dict(value or {})

    async def fetch_markets(
        self,
        *,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        value = await self._call("fetchMarkets", "fetch_markets", dict(params or {}))
        return list(value or ())

    async def fetch_status(
        self,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = await self._call("fetchStatus", "fetch_status", dict(params or {}))
        return dict(value or {})

    async def fetch_time(
        self,
        *,
        params: dict[str, Any] | None = None,
    ) -> int:
        value = await self._call("fetchTime", "fetch_time", dict(params or {}))
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise CcxtPredictionMarketDataError(f"invalid fetchTime result: {value!r}") from exc

    async def fetch_my_trades(
        self,
        token_id: str | None = None,
        *,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        value = await self._call(
            "fetchMyTrades",
            "fetch_my_trades",
            str(token_id) if token_id is not None else None,
            since,
            limit,
            dict(params or {}),
        )
        return list(value or ())

    async def fetch_order_trades(
        self,
        order_id: str,
        token_id: str | None = None,
        *,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        value = await self._call(
            "fetchOrderTrades",
            "fetch_order_trades",
            str(order_id),
            str(token_id) if token_id is not None else None,
            since,
            limit,
            dict(params or {}),
        )
        return list(value or ())

    async def fetch_open_orders(
        self,
        token_id: str | None = None,
        *,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        value = await self._call(
            "fetchOpenOrders",
            "fetch_open_orders",
            str(token_id) if token_id is not None else None,
            since,
            limit,
            dict(params or {}),
        )
        return list(value or ())

    async def fetch_order(
        self,
        order_id: str,
        token_id: str | None = None,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = await self._call(
            "fetchOrder",
            "fetch_order",
            str(order_id),
            str(token_id) if token_id is not None else None,
            dict(params or {}),
        )
        return dict(value or {})

    async def resolve_outcome(self, token_or_handle: str) -> dict[str, Any]:
        method = getattr(self._exchange, "load_outcome", None)
        if not callable(method):
            method = getattr(self._exchange, "loadOutcome", None)
        if not callable(method):
            raise CcxtPredictionCapabilityError("CCXT Polymarket does not expose loadOutcome")
        try:
            async with asyncio.timeout(self._timeout_seconds):
                value = await _await_call(method, str(token_or_handle))
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except BaseException as exc:
            raise CcxtPredictionMarketDataError(
                f"loadOutcome failed: {type(exc).__name__}"
            ) from exc
        if isinstance(value, dict):
            return dict(value)
        resolver = getattr(self._exchange, "outcome", None)
        if callable(resolver):
            resolved = resolver(str(token_or_handle))
            if isinstance(resolved, dict):
                return dict(resolved)
        raise CcxtPredictionMarketDataError("loadOutcome returned no outcome object")

    async def visible_capacity(
        self,
        token_id: str,
        *,
        side: str,
        price_limit: Decimal,
    ) -> VisibleCapacity:
        raw = await self.fetch_order_book(token_id)
        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise CcxtPredictionMarketDataError(f"unsupported side: {side!r}")
        limit = _decimal(price_limit, field="price_limit")
        source = raw.get("asks" if normalized_side == "BUY" else "bids") or []
        shares = Decimal("0")
        notional = Decimal("0")
        level_count = 0
        for level in source:
            if isinstance(level, dict):
                price_raw, size_raw = level.get("price"), level.get("size")
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price_raw, size_raw = level[0], level[1]
            else:
                continue
            price = _decimal(price_raw, field="book price")
            size = _decimal(size_raw, field="book size")
            qualifies = price <= limit if normalized_side == "BUY" else price >= limit
            if not qualifies:
                continue
            if size <= 0:
                continue
            shares += size
            notional += price * size
            level_count += 1
        return VisibleCapacity(
            token_id=str(token_id),
            side=normalized_side,
            price_limit=limit,
            shares=shares,
            notional_usdc=notional,
            levels=level_count,
        )

    async def watch_order_book(
        self,
        token_id: str,
        *,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._call(
            "watchOrderBook",
            "watch_order_book",
            str(token_id),
            limit,
            dict(params or {}),
        )

    async def watch_ticker(self, token_id: str, *, params: dict[str, Any] | None = None) -> Any:
        return await self._call(
            "watchTicker",
            "watch_ticker",
            str(token_id),
            dict(params or {}),
        )

    async def watch_trades(
        self,
        token_id: str,
        *,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._call(
            "watchTrades",
            "watch_trades",
            str(token_id),
            since,
            limit,
            dict(params or {}),
        )

    async def watch_orders(
        self,
        token_id: str | None = None,
        *,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._call(
            "watchOrders",
            "watch_orders",
            str(token_id) if token_id is not None else None,
            since,
            limit,
            dict(params or {}),
        )

    async def watch_my_trades(
        self,
        token_id: str | None = None,
        *,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._call(
            "watchMyTrades",
            "watch_my_trades",
            str(token_id) if token_id is not None else None,
            since,
            limit,
            dict(params or {}),
        )


__all__ = [
    "CcxtPredictionCapabilityError",
    "CcxtPredictionMarketData",
    "CcxtPredictionMarketDataError",
    "VisibleCapacity",
]
