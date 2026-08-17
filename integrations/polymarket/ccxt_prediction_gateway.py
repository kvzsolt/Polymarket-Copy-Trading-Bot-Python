"""Typed anti-corruption gateway for ``ccxt.prediction.polymarket``.

This adapter implements the repository's existing :class:`ClobGateway` port.
It is deliberately conservative:

* raw outcome token IDs are used to avoid symbol/handle ambiguity;
* execution metadata is mandatory and cached only after validation;
* market orders always carry an explicit price limit;
* BUY market size is treated as USDC cost, SELL market size as shares, matching
  the official SDK gateway contract;
* ambiguous POST failures are never retried blindly;
* account fills preserve maker/taker leg attribution.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from http import HTTPStatus
from typing import Any, Iterable, Literal, NoReturn

from domain.units import BASIS_POINTS_PER_UNIT
from integrations.polymarket.ccxt_compat.capabilities import build_ccxt_health_payload
from integrations.polymarket.ccxt_compat.precision import CcxtPrecisionError
from integrations.polymarket.ccxt_compat.tick_size import (
    InvalidTickSize,
    TickSizeResolver,
    quantize_price_to_tick,
)
from integrations.polymarket.ccxt_prediction_client import CcxtPredictionClient
from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInsufficientBalanceError,
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
    VenueOrderbookNotFoundError,
    VenueRateLimitError,
    VenueUnavailableError,
)
from integrations.polymarket.models import (
    LimitOrderRequest,
    MarketableOrderRequest,
    OrderBook,
    OrderBookLevel,
    PRICE_TOKEN_NOT_FOUND,
    VenueCancelResult,
    VenueFill,
    VenueOrderState,
    VenueSubmitResult,
)
from utils.logger import redact_error_detail

_READ_TIMEOUT_SECONDS = 10.0
_MAX_PRIVATE_HISTORY_PAGES = 1000
_PRIVATE_CURSOR_TERMINALS = {"", "LTE=", "END"}
_EXPECTED_CHAIN_ID = 137
_SUPPORTED_MARKET_ORDER_TYPES = {"FAK", "FOK"}
_NETWORK_ERRORS = {
    "NetworkError",
    "RequestTimeout",
    "ExchangeNotAvailable",
    "OnMaintenance",
    "OperationFailed",
}
_RATE_LIMIT_ERRORS = {"RateLimitExceeded", "DDoSProtection"}
_AUTH_ERRORS = {"AuthenticationError", "PermissionDenied"}
_INVALID_ERRORS = {
    "ArgumentsRequired",
    "BadRequest",
    "BadSymbol",
    "InvalidOrder",
}

OrderSide = Literal["BUY", "SELL"]


@dataclass(frozen=True, slots=True)
class ExecutionMetadata:
    """Describe authoritative metadata required to construct an order."""

    token_id: str
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool


def _exception_name(exc: BaseException) -> str:
    return type(exc).__name__


def _is_cancel(exc: BaseException) -> bool:
    return isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError))


def _decimal(value: Any, *, field: str, default: Decimal | None = None) -> Decimal:
    if value is None:
        if default is not None:
            return default
        raise VenueInvalidOrderError(f"missing {field}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise VenueInvalidOrderError(f"invalid {field}: {value!r}") from exc
    if not result.is_finite():
        raise VenueInvalidOrderError(f"invalid {field}: non-finite value")
    return result


def _maybe_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        raw = dump(mode="json", by_alias=True)
        return raw if isinstance(raw, dict) else {}
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        raw = to_dict()
        return raw if isinstance(raw, dict) else {}
    return {}


def _nested_dict(mapping: dict[str, Any], key: str) -> dict[str, Any]:
    """Return one raw nested dictionary with a precise adapter-local type."""

    value = mapping.get(key)
    return _maybe_dict(value) if isinstance(value, dict) else {}


def _nested_dict_or_self(mapping: dict[str, Any], key: str) -> dict[str, Any]:
    """Return a present nested dictionary or the original mapping."""

    value = mapping.get(key)
    return _maybe_dict(value) if isinstance(value, dict) else mapping


def _first(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _nested(mapping: dict[str, Any], *path: str) -> Any:
    current: Any = mapping
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _normalize_side(value: Any) -> OrderSide:
    side = str(value or "").upper()
    if side == "BUY":
        return "BUY"
    if side == "SELL":
        return "SELL"
    raise VenueInvalidOrderError(f"unsupported order side: {value!r}")


def _normalize_market_order_type(value: Any) -> str:
    order_type = str(value or "FOK").upper()
    if order_type not in _SUPPORTED_MARKET_ORDER_TYPES:
        raise VenueInvalidOrderError(f"unsupported market order type: {order_type}")
    return order_type


def _normalize_status(value: Any) -> str | None:
    status = str(value or "").strip().lower()
    if not status:
        return None
    aliases = {
        "live": "open",
        "order_status_live": "open",
        "placement": "open",
        "update": "open",
        "delayed": "open",
        "matched": "closed",
        "order_status_matched": "closed",
        "filled": "closed",
        "cancelled": "canceled",
        "order_status_cancelled": "canceled",
        "cancellation": "canceled",
        "cancelled_by_market": "canceled",
        "failed": "rejected",
        "order_status_failed": "rejected",
    }
    return aliases.get(status, status)


def _timestamp_ms(value: Any) -> int | None:
    if value is None:
        return None
    try:
        number = int(Decimal(str(value)))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if number < 10_000_000_000:
        number *= 1000
    return number


def _datetime(value: Any, timestamp: Any = None) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    ts = _timestamp_ms(timestamp)
    if ts is None:
        return None
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)


def _status_code(exc: BaseException) -> int | None:
    for name in ("status", "status_code", "http_status"):
        value = getattr(exc, name, None)
        if isinstance(value, int):
            return value
    return None


def _raise_read_error(exc: BaseException, *, not_found: str) -> NoReturn:
    if _is_cancel(exc):
        raise exc
    name = _exception_name(exc)
    status = _status_code(exc)
    if name in _RATE_LIMIT_ERRORS or status == HTTPStatus.TOO_MANY_REQUESTS:
        raise VenueRateLimitError("Polymarket CCXT rate limit exceeded") from exc
    if name in _AUTH_ERRORS or status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
        raise VenueAuthError("Polymarket CCXT authentication rejected") from exc
    if name == "OrderNotFound" or status == HTTPStatus.NOT_FOUND:
        error_type = (
            VenueOrderbookNotFoundError if not_found == "orderbook" else VenueOrderNotFoundError
        )
        raise error_type(f"{not_found}_not_found") from exc
    if name in _NETWORK_ERRORS or (status is not None and status >= 500):
        raise VenueUnavailableError(f"CCXT {name}") from exc
    if name in _INVALID_ERRORS:
        if not_found == "orderbook" and name == "BadSymbol":
            raise VenueOrderbookNotFoundError("orderbook_not_found") from exc
        raise VenueInvalidOrderError(f"CCXT {name}") from exc
    if name == "InsufficientFunds":
        raise VenueInsufficientBalanceError("CCXT InsufficientFunds") from exc
    raise VenueUnavailableError(f"CCXT {name}") from exc


def _submission_exception(exc: BaseException) -> VenueSubmitResult:
    if _is_cancel(exc):
        raise exc
    name = _exception_name(exc)
    status = _status_code(exc)
    if name == "OrderNotFillable":
        return VenueSubmitResult(
            success=False,
            status="canceled",
            error="order_not_fillable",
            non_retryable=True,
        )
    if name in _NETWORK_ERRORS or (status is None and name in {"TimeoutError", "ConnectionError"}):
        return VenueSubmitResult(
            success=False,
            error=f"ambiguous_{name.lower()}",
            ambiguous_post_failure=True,
        )
    if status is not None and status >= 500:
        return VenueSubmitResult(
            success=False,
            error=f"ambiguous_server_failure_{status}",
            ambiguous_post_failure=True,
        )
    if name in _RATE_LIMIT_ERRORS or status == HTTPStatus.TOO_MANY_REQUESTS:
        raise VenueRateLimitError("Polymarket CCXT rate limit exceeded") from exc
    if name in _AUTH_ERRORS or status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
        raise VenueAuthError("Polymarket CCXT authentication rejected") from exc
    if name == "InsufficientFunds":
        raise VenueInsufficientBalanceError("Polymarket balance/allowance is insufficient") from exc
    if name in _INVALID_ERRORS or (status is not None and 400 <= status < 500):
        raise VenueInvalidOrderError(f"Polymarket CCXT order rejected ({name})") from exc
    # Once create_order has been invoked, an unclassified exception is ambiguous:
    # the CLOB may have accepted the POST before the client failed to decode the
    # response.  Treating this as a retryable outage can duplicate a live order.
    return VenueSubmitResult(
        success=False,
        error=f"ambiguous_{name.lower()}",
        ambiguous_post_failure=True,
    )


async def _await_call(method: Any, *args: Any, **kwargs: Any) -> Any:
    result = method(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


class CcxtPredictionGateway:
    """Application gateway backed by ``ccxt.prediction.polymarket``."""

    def __init__(
        self,
        client: CcxtPredictionClient | Any,
        *,
        tick_size_resolver: TickSizeResolver | None = None,
    ) -> None:
        self._client = client
        self._exchange = client.exchange if isinstance(client, CcxtPredictionClient) else client
        self._metadata: dict[str, ExecutionMetadata] = {}
        self._tick_size_resolver = tick_size_resolver

    @property
    def client(self) -> CcxtPredictionClient | Any:
        """Return the wrapped CCXT prediction client."""
        return self._client

    @property
    def exchange(self) -> Any:
        """Return the wrapped CCXT exchange instance."""
        return self._exchange

    def _resolve_method(self, *names: str) -> Any:
        for name in names:
            method = getattr(self._exchange, name, None)
            if callable(method):
                return method
        return None

    async def _ensure_api_credentials(self) -> None:
        method = self._resolve_method("load_api_credentials", "loadApiCredentials")
        if method is None:
            return
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                await _await_call(method)
        except BaseException as exc:
            _raise_read_error(exc, not_found="order")

    async def _fetch_private_pages(
        self,
        *,
        raw_method_names: tuple[str, ...],
        fallback_method: Any,
        params: dict[str, Any],
        collection_keys: tuple[str, ...],
    ) -> list[Any]:
        """Read a complete authenticated cursor collection.

        CCXT 4.5.70's unified ``fetch_open_orders`` and ``fetch_my_trades``
        wrappers only consume the first CLOB response page. Reconciliation must
        not silently accept partial private history, so use CCXT's generated raw
        endpoint when present and walk ``next_cursor`` until the documented
        terminal value. The unified method remains a compatibility fallback for
        public/paper test doubles, but authenticated compatibility rejects a
        client that lacks the raw paginated methods.
        """

        raw_method = self._resolve_method(*raw_method_names)
        if raw_method is None:
            try:
                async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                    values = await _await_call(
                        fallback_method,
                        None,
                        None,
                        None,
                        dict(params),
                    )
            except BaseException as exc:
                _raise_read_error(exc, not_found="order")
            if isinstance(values, dict):
                for key in collection_keys:
                    collection = values.get(key)
                    if isinstance(collection, list):
                        return collection
                return []
            return list(values or ())

        await self._ensure_api_credentials()
        rows: list[Any] = []
        seen_cursors: set[str] = set()
        request = dict(params)
        options = getattr(self._exchange, "options", {}) or {}
        max_pages_raw = options.get("privateHistoryMaxPages", _MAX_PRIVATE_HISTORY_PAGES)
        try:
            max_pages = int(max_pages_raw)
        except (TypeError, ValueError):
            max_pages = _MAX_PRIVATE_HISTORY_PAGES
        if max_pages <= 0:
            raise VenueInvalidOrderError("privateHistoryMaxPages must be positive")

        for _page in range(max_pages):
            try:
                async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                    response = await _await_call(raw_method, dict(request))
            except BaseException as exc:
                _raise_read_error(exc, not_found="order")

            if isinstance(response, list):
                rows.extend(response)
                return rows

            payload = _maybe_dict(response)
            page_rows: list[Any] = []
            for key in collection_keys:
                value = payload.get(key)
                if isinstance(value, list):
                    page_rows = value
                    break
            rows.extend(page_rows)

            cursor_raw = _first(payload, "next_cursor", "nextCursor")
            cursor = str(cursor_raw or "").strip()
            if cursor in _PRIVATE_CURSOR_TERMINALS:
                return rows
            if cursor in seen_cursors:
                raise VenueUnavailableError(
                    "CCXT private-history pagination cursor repeated; "
                    "refusing partial reconciliation data"
                )
            seen_cursors.add(cursor)
            request = dict(params)
            request["next_cursor"] = cursor

        raise VenueUnavailableError(
            "CCXT private-history pagination exceeded privateHistoryMaxPages; "
            "refusing partial reconciliation data"
        )

    async def _fetch_raw_order_book(self, token_id: str) -> dict[str, Any]:
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                value = await _await_call(self._exchange.fetch_order_book, str(token_id))
        except BaseException as exc:
            _raise_read_error(exc, not_found="orderbook")
        return _maybe_dict(value)

    async def _fetch_raw_clob_book(self, token_id: str) -> dict[str, Any] | None:
        """Fetch the raw CLOB book, which carries the execution metadata.

        CCXT's unified ``fetch_order_book`` drops the raw response's
        ``tick_size``, ``min_order_size``, and ``neg_risk`` fields during
        parsing, so metadata reads use the generated implicit endpoint for the
        same CLOB route. Returns ``None`` when the implicit endpoint is absent
        (degraded installs and unified-only test doubles) so callers can fall
        back to the parsed book plus the outcome cache.
        """
        method = self._resolve_method("clobPublicGetBook", "clob_public_get_book")
        if method is None:
            return None
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                value = await _await_call(method, {"token_id": str(token_id)})
        except BaseException as exc:
            _raise_read_error(exc, not_found="orderbook")
        return _maybe_dict(value)

    async def _fetch_metadata_book(self, token_id: str) -> dict[str, Any]:
        """Return the freshest book usable as an execution-metadata source."""
        raw = await self._fetch_raw_clob_book(token_id)
        if raw is not None:
            return raw
        return await self._fetch_raw_order_book(token_id)

    async def _load_outcome(self, token_id: str) -> dict[str, Any]:
        method = getattr(self._exchange, "load_outcome", None)
        outcome: Any = None
        if callable(method):
            try:
                async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                    outcome = await _await_call(method, token_id)
            except BaseException as exc:
                _raise_read_error(exc, not_found="orderbook")
        if not isinstance(outcome, dict):
            resolver = getattr(self._exchange, "outcome", None)
            if callable(resolver):
                try:
                    outcome = resolver(token_id)
                except BaseException:
                    outcome = None
        return outcome if isinstance(outcome, dict) else {}

    @staticmethod
    def _metadata_from_sources(
        token_id: str,
        *sources: dict[str, Any],
    ) -> ExecutionMetadata | None:
        tick: Any = None
        minimum: Any = None
        neg_risk: Any = None
        for source in sources:
            if not source:
                continue
            info = _nested_dict(source, "info")
            if tick is None:
                tick = _first(source, "tick_size", "tickSize")
                tick = tick if tick is not None else _first(info, "tick_size", "tickSize")
                tick = tick if tick is not None else _nested(source, "precision", "price")
                # Gamma-market spellings carried in a CCXT outcome's raw info.
                tick = (
                    tick
                    if tick is not None
                    else _first(info, "orderPriceMinTickSize", "minimumTickSize")
                )
            if minimum is None:
                minimum = _first(source, "min_order_size", "minOrderSize")
                minimum = (
                    minimum
                    if minimum is not None
                    else _first(info, "min_order_size", "minOrderSize")
                )
                minimum = (
                    minimum if minimum is not None else _nested(source, "limits", "amount", "min")
                )
                # Gamma-market spelling carried in a CCXT outcome's raw info.
                minimum = minimum if minimum is not None else _first(info, "orderMinSize")
            if neg_risk is None:
                neg_risk = _first(source, "neg_risk", "negRisk")
                neg_risk = neg_risk if neg_risk is not None else _first(info, "neg_risk", "negRisk")

        if tick is None or minimum is None or not isinstance(neg_risk, bool):
            return None
        tick_dec = _decimal(tick, field="tick_size")
        min_dec = _decimal(minimum, field="min_order_size")
        if tick_dec <= 0 or min_dec <= 0:
            raise VenueInvalidOrderError("invalid execution metadata")
        return ExecutionMetadata(
            token_id=str(token_id),
            tick_size=tick_dec,
            min_order_size=min_dec,
            neg_risk=neg_risk,
        )

    async def _execution_metadata(
        self,
        token_id: str,
        raw_book: dict[str, Any] | None = None,
        *,
        refresh: bool = False,
    ) -> ExecutionMetadata:
        token = str(token_id)

        # A supplied book is always newer than the local cache. Polymarket can
        # change tick size while a market is live, so execution paths must not
        # keep signing against indefinitely cached metadata.
        if raw_book is not None:
            metadata = self._metadata_from_sources(token, raw_book)
            if metadata is None:
                outcome = await self._load_outcome(token)
                metadata = self._metadata_from_sources(token, raw_book, outcome)
            if metadata is None:
                raise VenueInvalidOrderError(
                    "CCXT omitted tick_size, min_order_size, or neg_risk; refusing execution"
                )
            self._metadata[token] = metadata
            return metadata

        if not refresh:
            cached = self._metadata.get(token)
            if cached is not None:
                return cached

        if refresh:
            # A fresh order-book snapshot is the execution authority. This is
            # intentionally done immediately before every signed order, using
            # the raw CLOB book because the parsed book drops the metadata.
            book = await self._fetch_metadata_book(token)
            return await self._execution_metadata(token, book)

        outcome = await self._load_outcome(token)
        metadata = self._metadata_from_sources(token, outcome)
        if metadata is None:
            # The CCXT outcome cache does not consistently include
            # min_order_size. Fall back to a fresh book and cache only after all
            # three execution-critical fields validate.
            book = await self._fetch_metadata_book(token)
            metadata = self._metadata_from_sources(token, book, outcome)
        if metadata is None:
            raise VenueInvalidOrderError(
                "CCXT omitted tick_size, min_order_size, or neg_risk; refusing execution"
            )
        self._metadata[token] = metadata
        return metadata

    async def get_orderbook(self, token_id: str) -> OrderBook:
        """Return a normalized order book for a raw outcome token."""
        raw = await self._fetch_raw_order_book(str(token_id))
        metadata = await self._execution_metadata(str(token_id), raw)

        def parse_levels(values: Any) -> list[OrderBookLevel]:
            levels: list[OrderBookLevel] = []
            for value in values or ():
                if isinstance(value, dict):
                    price = _first(value, "price")
                    size = _first(value, "size", "amount")
                elif isinstance(value, (list, tuple)) and len(value) >= 2:
                    price, size = value[0], value[1]
                else:
                    continue
                p = _decimal(price, field="orderbook price")
                s = _decimal(size, field="orderbook size")
                if s > 0:
                    levels.append(OrderBookLevel(price=p, size=s))
            return levels

        bids = sorted(parse_levels(raw.get("bids")), key=lambda level: level.price, reverse=True)
        asks = sorted(parse_levels(raw.get("asks")), key=lambda level: level.price)
        best_bid = bids[0].price if bids else None
        best_ask = asks[0].price if asks else None
        if best_bid is not None and best_ask is not None and best_ask < best_bid:
            raise VenueInvalidOrderError("inverted order book")
        spread_bps: int | None = None
        if best_bid is not None and best_ask is not None and best_bid > 0:
            midpoint = (best_bid + best_ask) / Decimal("2")
            if midpoint > 0:
                spread_bps = int(((best_ask - best_bid) / midpoint) * BASIS_POINTS_PER_UNIT)

        timestamp = _datetime(raw.get("datetime"), raw.get("timestamp")) or datetime.now(
            timezone.utc
        )
        return OrderBook(
            token_id=str(_first(raw, "outcomeId", "token_id", "asset_id") or token_id),
            tick_size=metadata.tick_size,
            min_order_size=metadata.min_order_size,
            neg_risk=metadata.neg_risk,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            spread_bps=spread_bps,
            timestamp=timestamp,
        )

    async def get_price(self, token_id: str, side: str) -> dict[str, str] | object:
        """Return the executable top-of-book price for legacy risk readers.

        Args:
            token_id: Raw Polymarket outcome token identity.
            side: ``BUY`` selects the best ask; ``SELL`` selects the best bid.

        Returns:
            A string-precise price mapping, zero for an empty side, or the
            existing terminal-market sentinel when the venue has no book.

        Raises:
            VenueInvalidOrderError: If ``side`` is neither BUY nor SELL.
        """

        normalized_side = _normalize_side(side)
        try:
            book = await self.get_orderbook(token_id)
        except VenueOrderbookNotFoundError:
            return PRICE_TOKEN_NOT_FOUND
        price = book.best_ask if normalized_side == "BUY" else book.best_bid
        return {"price": format(price if price is not None else Decimal("0"), "f")}

    async def get_market_info(self, token_id: str) -> dict[str, Any]:
        """Return normalized market information for an outcome token."""
        book = await self.get_orderbook(token_id)
        return {
            "token_id": book.token_id,
            "tick_size": str(book.tick_size),
            "min_order_size": str(book.min_order_size),
            "neg_risk": book.neg_risk,
            "best_bid": str(book.best_bid) if book.best_bid is not None else None,
            "best_ask": str(book.best_ask) if book.best_ask is not None else None,
        }

    async def get_execution_metadata(self, token_id: str) -> dict[str, Any]:
        """Return authoritative tick, minimum-size, and negative-risk metadata."""
        metadata = await self._execution_metadata(str(token_id))
        return {
            "token_id": metadata.token_id,
            "tick_size": str(metadata.tick_size),
            "min_order_size": str(metadata.min_order_size),
            "neg_risk": metadata.neg_risk,
        }

    @staticmethod
    def _validate_order(
        *,
        share_size: Decimal,
        price: Decimal,
        metadata: ExecutionMetadata,
    ) -> None:
        if share_size <= 0:
            raise VenueInvalidOrderError("order size must be positive")
        if share_size < metadata.min_order_size:
            raise VenueInvalidOrderError(
                f"order share size {share_size} is below minimum {metadata.min_order_size}"
            )
        if price <= 0 or price >= 1:
            raise VenueInvalidOrderError("price must be greater than 0 and less than 1")

    async def _resolved_tick_size(
        self,
        token_id: str,
        metadata: ExecutionMetadata,
    ) -> Decimal:
        """Return the tick size every signed order must be quantized against.

        The fresh execution-metadata fetch is the authoritative observation; it
        refreshes the resolver cache (invalidating a stale tick) and the
        resolver then returns the validated value that reaches the CCXT order
        parameters. Without a resolver the validated metadata tick is used
        directly — there is no default path either way.
        """

        if self._tick_size_resolver is None:
            return metadata.tick_size
        await self._tick_size_resolver.note_observed_tick(token_id, metadata.tick_size)
        return await self._tick_size_resolver.resolve(token_id)

    @staticmethod
    def _submission_from_order(value: Any) -> VenueSubmitResult:
        raw = _maybe_dict(value)
        info = _nested_dict(raw, "info")
        order_id = (
            str(_first(raw, "id", "orderID", "order_id") or _first(info, "orderID", "id") or "")
            or None
        )
        status = CcxtPredictionGateway._submission_status(raw=raw, info=info)
        success_marker = _first(raw, "success")
        if success_marker is None:
            success_marker = _first(info, "success")
        error = (
            str(
                _first(raw, "error", "errorMsg", "message")
                or _first(info, "error", "errorMsg", "message")
                or ""
            ).strip()
            or None
        )

        if success_marker is False or status in {"canceled", "rejected"}:
            return VenueSubmitResult(
                success=False,
                order_id=order_id,
                status=status or "rejected",
                raw=raw,
                error=error or "order_rejected",
                non_retryable=True,
            )
        if order_id is None:
            return VenueSubmitResult(
                success=False,
                status=status,
                raw=raw,
                error=error or "ambiguous_success_response_without_order_id",
                ambiguous_post_failure=True,
            )
        return VenueSubmitResult(
            success=True,
            order_id=order_id,
            status=status,
            raw=raw,
        )

    @staticmethod
    def _submission_status(*, raw: dict[str, Any], info: dict[str, Any]) -> str | None:
        """Return the raw CLOB lifecycle state when the POST preserved it."""

        venue_status = str(_first(info, "status") or "").strip().lower()
        if venue_status:
            return "canceled" if venue_status == "cancelled" else venue_status
        return _normalize_status(_first(raw, "status"))

    async def submit_limit_order(self, request: LimitOrderRequest) -> VenueSubmitResult:
        """Submit a validated limit order through CCXT Prediction."""
        if not request.condition_id:
            return VenueSubmitResult(
                success=False,
                error="missing_condition_id",
                non_retryable=True,
            )
        side = _normalize_side(request.side)
        metadata = await self._execution_metadata(request.token_id, refresh=True)
        tick_size = await self._resolved_tick_size(request.token_id, metadata)
        price = _decimal(request.price, field="price")
        size = _decimal(request.size, field="size")
        self._validate_order(
            share_size=size.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
            price=price,
            metadata=metadata,
        )
        try:
            quantized = quantize_price_to_tick(price=price, tick_size=tick_size, side=side)
        except (CcxtPrecisionError, InvalidTickSize, ValueError) as exc:
            raise VenueInvalidOrderError(str(exc)) from exc
        params = {
            "orderType": "GTC",
            "timeInForce": "GTC",
            "tickSize": format(tick_size, "f"),
            "negRisk": metadata.neg_risk,
            "postOnly": False,
        }
        try:
            value = await _await_call(
                self._exchange.create_order,
                request.token_id,
                "limit",
                side.lower(),
                size,
                quantized,
                params,
            )
        except BaseException as exc:
            return _submission_exception(exc)
        return self._submission_from_order(value)

    async def submit_marketable_order(self, request: MarketableOrderRequest) -> VenueSubmitResult:
        """Submit a price-protected FAK order through CCXT Prediction."""
        if not request.condition_id:
            return VenueSubmitResult(
                success=False,
                error="missing_condition_id",
                non_retryable=True,
            )
        if request.price_limit is None:
            raise VenueInvalidOrderError("price_limit is required for marketable orders")
        side = _normalize_side(request.side)
        order_type = _normalize_market_order_type(request.order_type)
        metadata = await self._execution_metadata(request.token_id, refresh=True)
        tick_size = await self._resolved_tick_size(request.token_id, metadata)
        size = _decimal(request.size, field="size")
        price_limit = _decimal(request.price_limit, field="price_limit")
        try:
            quantized = quantize_price_to_tick(
                price=price_limit,
                tick_size=tick_size,
                side=side,
            )
        except (CcxtPrecisionError, InvalidTickSize, ValueError) as exc:
            raise VenueInvalidOrderError(str(exc)) from exc
        signed_size = size.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        share_size = signed_size / quantized if side == "BUY" else signed_size
        self._validate_order(share_size=share_size, price=quantized, metadata=metadata)

        params: dict[str, Any] = {
            "orderType": order_type,
            "timeInForce": "IOC" if order_type == "FAK" else "FOK",
            "tickSize": format(tick_size, "f"),
            "negRisk": metadata.neg_risk,
        }
        # Preserve the official SDK gateway contract: BUY size is spend (USDC),
        # SELL size is shares. Passing cost together with an explicit price avoids
        # CCXT's cached-outcome-price fallback.
        if side == "BUY":
            params["cost"] = size

        try:
            value = await _await_call(
                self._exchange.create_order,
                request.token_id,
                "market",
                side.lower(),
                size,
                quantized,
                params,
            )
        except BaseException as exc:
            return _submission_exception(exc)
        return self._submission_from_order(value)

    @staticmethod
    def _order_state(value: Any) -> VenueOrderState:
        raw = _maybe_dict(value)
        info = _nested_dict(raw, "info")
        order_id = str(
            _first(raw, "id", "orderID", "order_id") or _first(info, "id", "orderID") or ""
        )
        if not order_id:
            raise VenueInvalidOrderError("CCXT order response omitted id")
        filled = _decimal(
            _first(raw, "filled", "size_matched") or _first(info, "size_matched"),
            field="filled",
            default=Decimal("0"),
        )
        average_raw = _first(raw, "average", "avg_price", "average_price")
        if average_raw is None:
            average_raw = _first(info, "avg_price", "average_price")
        average = _decimal(average_raw, field="average", default=Decimal("0"))
        return VenueOrderState(
            order_id=order_id,
            status=_normalize_status(_first(raw, "status") or _first(info, "status")) or "unknown",
            side=str(_first(raw, "side") or _first(info, "side") or "").upper(),
            price=_decimal(
                _first(raw, "price") or _first(info, "price"), field="price", default=Decimal("0")
            ),
            size=_decimal(
                _first(raw, "amount", "original_size") or _first(info, "original_size"),
                field="size",
                default=Decimal("0"),
            ),
            filled_size=filled,
            # Never pretend the limit price is a weighted average fill price.
            avg_fill_price=average,
            created_at=_datetime(
                _first(raw, "datetime", "created_at"),
                _first(raw, "timestamp") or _first(info, "created_at"),
            ),
            raw=raw,
        )

    async def get_order(self, order_id: str) -> VenueOrderState:
        """Return one normalized venue order when it exists."""
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                value = await _await_call(self._exchange.fetch_order, order_id)
        except BaseException as exc:
            _raise_read_error(exc, not_found="order")
        return self._order_state(value)

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]:
        """Return normalized open orders with pagination deduplication."""
        params: dict[str, Any] = {}
        if order_id:
            params["id"] = order_id
        if market:
            params["market"] = market
        values = await self._fetch_private_pages(
            raw_method_names=(
                "clob_private_get_data_orders",
                "clobPrivateGetDataOrders",
            ),
            fallback_method=self._exchange.fetch_open_orders,
            params=params,
            collection_keys=("data", "orders"),
        )
        result: list[VenueOrderState] = []
        for value in values or ():
            state = self._order_state(value)
            if order_id and state.order_id != order_id:
                continue
            if market:
                state_raw = _maybe_dict(state.raw)
                raw_info = _nested_dict(state_raw, "info")
                raw_market = _first(state_raw, "market") or _first(
                    raw_info, "market", "condition_id"
                )
                if raw_market is not None and str(raw_market) != market:
                    continue
            result.append(state)
        return result

    @staticmethod
    def _fee(value: dict[str, Any]) -> Decimal:
        fee = value.get("fee")
        if isinstance(fee, dict):
            raw = _first(fee, "cost", "amount")
            if raw is not None:
                return _decimal(raw, field="fee", default=Decimal("0"))
        info = _nested_dict_or_self(value, "info")
        raw = _first(info, "fee", "fee_amount", "feeAmount")
        if raw is not None and not isinstance(raw, dict):
            return _decimal(raw, field="fee", default=Decimal("0"))
        return Decimal("0")

    @staticmethod
    def _trade_legs(value: dict[str, Any]) -> Iterable[dict[str, Any]]:
        info = _nested_dict_or_self(value, "info")
        role = str(
            _first(info, "trader_side", "traderSide") or value.get("takerOrMaker") or "TAKER"
        ).upper()
        makers = _first(info, "maker_orders", "makerOrders")
        if role == "MAKER" and isinstance(makers, list) and makers:
            for maker in makers:
                if not isinstance(maker, dict):
                    continue
                yield {
                    "order_id": _first(maker, "order_id", "orderId", "id"),
                    "token_id": _first(maker, "token_id", "tokenId", "asset_id"),
                    "side": _first(maker, "side"),
                    "size": _first(maker, "matched_amount", "matchedAmount", "size"),
                    "price": _first(maker, "price"),
                    "role": role,
                }
            return
        yield {
            "order_id": _first(value, "order") or _first(info, "taker_order_id", "takerOrderId"),
            "token_id": _first(value, "outcomeId", "token_id", "asset_id")
            or _first(info, "asset_id", "token_id", "tokenId"),
            "side": _first(value, "side") or _first(info, "side"),
            "size": _first(value, "amount", "size") or _first(info, "size"),
            "price": _first(value, "price") or _first(info, "price"),
            "role": role,
        }

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ) -> list[VenueFill]:
        """Return normalized fills with exact maker and taker attribution."""
        params: dict[str, Any] = {}
        if market:
            params["market"] = market
        if trade_id:
            params["id"] = trade_id
        values = await self._fetch_private_pages(
            raw_method_names=(
                "clob_private_get_data_trades",
                "clobPrivateGetDataTrades",
            ),
            fallback_method=self._exchange.fetch_my_trades,
            params=params,
            collection_keys=("data", "trades"),
        )

        result: list[VenueFill] = []
        seen: set[tuple[str, str, int | None, str, str]] = set()
        for raw_value in values or ():
            value = _maybe_dict(raw_value)
            info = _nested_dict_or_self(value, "info")
            fill_id = str(
                _first(value, "id")
                or _first(info, "id", "transaction_hash", "transactionHash")
                or ""
            )
            if trade_id and fill_id != trade_id:
                continue
            bucket_raw = _first(info, "bucket_index", "bucketIndex")
            bucket_index = int(bucket_raw) if bucket_raw is not None else None
            timestamp_ms = _timestamp_ms(
                _first(value, "timestamp") or _first(info, "match_time", "matched_at", "timestamp")
            )
            timestamp = int((timestamp_ms or 0) / 1000)
            transaction_hash = (
                str(_first(info, "transaction_hash", "transactionHash") or "") or None
            )
            status = str(_first(info, "status") or _first(value, "status") or "") or None
            fee = self._fee(value)
            for leg in self._trade_legs(value):
                leg_order_id = str(leg.get("order_id") or "")
                if order_id and leg_order_id != order_id:
                    continue
                token_id = str(leg.get("token_id") or "")
                size = _decimal(leg.get("size"), field="fill size", default=Decimal("0"))
                price = _decimal(leg.get("price"), field="fill price", default=Decimal("0"))
                dedupe = (fill_id, leg_order_id, bucket_index, str(size), str(price))
                if dedupe in seen:
                    continue
                seen.add(dedupe)
                result.append(
                    VenueFill(
                        fill_id=fill_id,
                        order_id=leg_order_id,
                        token_id=token_id,
                        side=str(leg.get("side") or "").upper(),
                        size=size,
                        price=price,
                        fee=fee,
                        timestamp=timestamp,
                        status=status,
                        transaction_hash=transaction_hash,
                        trader_side=str(leg.get("role") or "") or None,
                        bucket_index=bucket_index,
                        raw=value,
                    )
                )
        return result

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        """Cancel an order idempotently through the venue adapter."""
        try:
            value = await _await_call(self._exchange.cancel_order, order_id)
        except BaseException as exc:
            if _exception_name(exc) == "OrderNotFound" or _status_code(exc) == HTTPStatus.NOT_FOUND:
                return VenueCancelResult(
                    success=True,
                    order_id=order_id,
                    status="already_gone",
                )
            _raise_read_error(exc, not_found="order")

        raw = _maybe_dict(value)
        if isinstance(value, list):
            raw = {"canceled": value}
        canceled = _first(raw, "canceled", "cancelled")
        if isinstance(canceled, list) and order_id in {str(item) for item in canceled}:
            return VenueCancelResult(success=True, order_id=order_id, status="canceled")
        status = _normalize_status(_first(raw, "status") or _nested(raw, "info", "status"))
        if status == "canceled":
            return VenueCancelResult(success=True, order_id=order_id, status="canceled")
        not_canceled = _first(raw, "not_canceled", "notCanceled")
        error = None
        if isinstance(not_canceled, dict):
            error = str(not_canceled.get(order_id) or "") or None
        return VenueCancelResult(
            success=False,
            order_id=order_id,
            status="cancel_failed",
            error=error or "order was not canceled",
        )

    async def check_compatibility(self) -> dict[str, Any]:
        """Validate installed CCXT Prediction capabilities fail closed.

        The payload uses CCXT transport terminology only. ``live_execution_ready``
        additionally requires authentication, a verified type-3 Deposit Wallet
        identity, and the paginated private-history endpoints reconciliation
        depends on.
        """

        try:
            has = getattr(self._exchange, "has", {}) or {}
            options = getattr(self._exchange, "options", {}) or {}
            chain_id = int(options.get("chainId", _EXPECTED_CHAIN_ID))
            signature_type = options.get("signatureType")
            if signature_type is not None:
                signature_type = int(signature_type)
            wrapper = self._client if isinstance(self._client, CcxtPredictionClient) else None
            authenticated = (
                bool(wrapper.authenticated)
                if wrapper is not None
                else bool(getattr(self._exchange, "privateKey", None))
            )
            required = {
                "createOrder": True,
                "fetchOrderBook": True,
                "fetchOrder": True,
                "fetchOpenOrders": True,
                "fetchMyTrades": True,
                "cancelOrder": True,
            }
            missing = [name for name, expected in required.items() if has.get(name) is not expected]
            private_methods = {
                "orders": self._resolve_method(
                    "clob_private_get_data_orders",
                    "clobPrivateGetDataOrders",
                ),
                "trades": self._resolve_method(
                    "clob_private_get_data_trades",
                    "clobPrivateGetDataTrades",
                ),
            }
            missing_private_history = [
                name for name, method in private_methods.items() if method is None
            ]
            private_history_paginated = not missing_private_history
            patch = getattr(self._exchange, "_pmcopy_precision_patch", None)
            wallet = (
                wrapper.wallet_address
                if wrapper is not None
                else getattr(self._exchange, "walletAddress", None)
            )
            identity_ok = not authenticated or (
                signature_type == 3 and bool(str(wallet or "").strip())
            )
            capabilities = {
                "create_order": has.get("createOrder") is True,
                "cancel_order": has.get("cancelOrder") is True,
                "fetch_order": has.get("fetchOrder") is True,
                "fetch_open_orders": has.get("fetchOpenOrders") is True,
                "fetch_my_trades": has.get("fetchMyTrades") is True,
            }
            available = (
                bool(has.get("prediction"))
                and chain_id == _EXPECTED_CHAIN_ID
                and not missing
                and bool(patch)
                and identity_ok
                and (not authenticated or private_history_paginated)
            )
            live_execution_ready = (
                available
                and authenticated
                and signature_type == 3
                and bool(str(wallet or "").strip())
                and private_history_paginated
            )
            return build_ccxt_health_payload(
                available=available,
                ccxt_version=wrapper.ccxt_version if wrapper is not None else None,
                authenticated=authenticated,
                live_execution_ready=live_execution_ready,
                capabilities=capabilities,
                error=None if available else "CCXT prediction compatibility gate failed",
                details={
                    "chain_id": chain_id,
                    "prediction": bool(has.get("prediction")),
                    "wallet": wallet,
                    "signature_type": signature_type,
                    "private_history_paginated": private_history_paginated,
                    "missing_private_history_methods": missing_private_history,
                    "missing_methods": missing,
                },
            )
        except Exception as exc:  # compatibility probes must never raise
            # A probe failure reports a redacted category, never a CCXT
            # exception body that may echo the request.
            return build_ccxt_health_payload(
                available=False,
                ccxt_version=None,
                authenticated=False,
                live_execution_ready=False,
                capabilities={},
                error=redact_error_detail(exc),
            )

    async def close(self) -> None:
        """Close the injected provider or venue client."""
        if isinstance(self._client, CcxtPredictionClient):
            await self._client.close()
            return
        close = getattr(self._exchange, "close", None)
        if close is not None:
            await _await_call(close, True)


__all__ = ["CcxtPredictionGateway", "ExecutionMetadata"]
