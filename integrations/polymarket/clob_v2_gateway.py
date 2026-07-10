"""Typed anti-corruption gateway for ``polymarket-client``.

The historical module name is retained for one release so application imports
stay stable, but there is no V2 SDK compatibility code here.  Every operation
uses the official unified async SDK and converts its models at this boundary.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from http import HTTPStatus
from types import SimpleNamespace
from typing import Any, Literal, NoReturn

from polymarket import (
    AcceptedOrder,
    InsufficientAllowanceError,
    InsufficientLiquidityError,
    PolymarketError,
    RateLimitError,
    RejectedOrder,
    RequestRejectedError,
    SigningError,
    TransportError,
    UnexpectedResponseError,
    UserInputError,
)
from domain.units import BASIS_POINTS_PER_UNIT

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
    VenueCancelResult,
    VenueFill,
    VenueOrderState,
    VenueSubmitResult,
)
from integrations.polymarket.sdk_client import PolymarketClient
from utils.logger import get_logger, redact_error_detail

logger = get_logger("PolymarketGateway")

BUY = "BUY"
SELL = "SELL"
_SDK_READ_TIMEOUT_SECONDS = 10.0
# Backward-compatible constant name used by one risk-loop timeout assertion.
_GET_ORDER_HTTP_TIMEOUT_SECONDS = _SDK_READ_TIMEOUT_SECONDS
_SDK_GET_TIMEOUT_SECONDS = _SDK_READ_TIMEOUT_SECONDS
_MAX_TRADE_PAGES = 20
_EXPECTED_CHAIN_ID = 137
OrderSide = Literal["BUY", "SELL"]
MarketOrderType = Literal["FAK", "FOK"]


def _normalize_order_side(value: object) -> OrderSide:
    normalized = str(value).upper()
    if normalized == "BUY":
        return "BUY"
    if normalized == "SELL":
        return "SELL"
    raise VenueInvalidOrderError(f"unsupported order side: {value!r}")


def _normalize_market_order_type(value: object) -> MarketOrderType:
    normalized = str(value or "FOK").upper()
    if normalized == "FAK":
        return "FAK"
    if normalized == "FOK":
        return "FOK"
    raise VenueInvalidOrderError(f"unsupported market order type: {normalized}")


def _raw_model(value: object) -> dict[str, Any]:
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        raw = dump(mode="json", by_alias=True)
        return raw if isinstance(raw, dict) else {}
    return {}


def _status_code(exc: BaseException) -> int | None:
    raw = getattr(exc, "status", None)
    return raw if isinstance(raw, int) else None


def _raise_read_error(exc: BaseException, *, not_found: str) -> NoReturn:
    if isinstance(exc, RateLimitError):
        raise VenueRateLimitError("Polymarket rate limit exceeded") from exc
    if isinstance(exc, RequestRejectedError):
        status = _status_code(exc)
        if status == HTTPStatus.NOT_FOUND:
            error_type = (
                VenueOrderbookNotFoundError
                if not_found == "orderbook"
                else VenueOrderNotFoundError
            )
            raise error_type(f"{not_found}_not_found") from exc
        if status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            raise VenueAuthError("Polymarket authentication rejected") from exc
        if status == HTTPStatus.TOO_MANY_REQUESTS:
            raise VenueRateLimitError("Polymarket rate limit exceeded") from exc
        if status is not None and status >= HTTPStatus.INTERNAL_SERVER_ERROR:
            raise VenueUnavailableError(f"Polymarket server error ({status})") from exc
        raise VenueInvalidOrderError(f"Polymarket request rejected ({status})") from exc
    if isinstance(exc, (TransportError, UnexpectedResponseError)):
        raise VenueUnavailableError(type(exc).__name__) from exc
    if isinstance(exc, (UserInputError, SigningError)):
        raise VenueInvalidOrderError(type(exc).__name__) from exc
    if isinstance(exc, PolymarketError):
        raise VenueUnavailableError(type(exc).__name__) from exc
    raise VenueUnavailableError(type(exc).__name__) from exc


def _submission_from_response(
    response: AcceptedOrder | RejectedOrder,
    *,
    side: str | None = None,
) -> VenueSubmitResult:
    if isinstance(response, AcceptedOrder):
        raw = _raw_model(response)
        normalized_side = str(side or "").upper()
        if response.status == "matched":
            if normalized_side == BUY and response.taking_amount > 0:
                raw["size_matched"] = str(response.taking_amount)
                raw["avg_price"] = str(response.making_amount / response.taking_amount)
            elif normalized_side == SELL and response.making_amount > 0:
                raw["size_matched"] = str(response.making_amount)
                raw["avg_price"] = str(response.taking_amount / response.making_amount)
        return VenueSubmitResult(
            success=True,
            order_id=response.order_id,
            status=response.status,
            raw=raw,
        )

    insufficient = response.code == "not_enough_balance"
    cancelled = response.code in {"unmatched", "fok_not_filled", "fak_not_filled"}
    return VenueSubmitResult(
        success=False,
        status="cancelled" if cancelled else "rejected",
        raw=_raw_model(response),
        error=f"{response.code}: {response.message}",
        non_retryable=True,
    ) if not insufficient else VenueSubmitResult(
        success=False,
        status="rejected",
        raw=_raw_model(response),
        error=f"insufficient_balance: {response.message}",
        non_retryable=True,
    )


def _submission_from_legacy_dict(raw: object) -> VenueSubmitResult:
    """Convert an injected test double without importing the removed SDK."""
    if not isinstance(raw, dict):
        return VenueSubmitResult(
            success=False,
            error="unexpected_response_type",
            non_retryable=True,
        )
    return VenueSubmitResult(
        success=bool(raw.get("success")),
        order_id=str(raw.get("orderID") or raw.get("order_id") or "") or None,
        status=str(raw.get("status") or "") or None,
        raw=raw,
        error=str(raw.get("error") or "") or None,
        ambiguous_post_failure=bool(raw.get("ambiguous_post_failure")),
        non_retryable=bool(raw.get("non_retryable")),
    )


def _submission_exception(exc: BaseException) -> VenueSubmitResult:
    """Classify a POST without ever retrying an ambiguous send."""
    if isinstance(exc, TransportError):
        return VenueSubmitResult(
            success=False,
            error="ambiguous_transport_failure",
            ambiguous_post_failure=True,
        )
    if isinstance(exc, RequestRejectedError):
        status = _status_code(exc)
        if status is None or status >= HTTPStatus.INTERNAL_SERVER_ERROR:
            return VenueSubmitResult(
                success=False,
                error=f"ambiguous_server_failure_{status or 'unknown'}",
                ambiguous_post_failure=True,
            )
        if status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            raise VenueAuthError("Polymarket authentication rejected") from exc
        if status == HTTPStatus.TOO_MANY_REQUESTS:
            raise VenueRateLimitError("Polymarket rate limit exceeded") from exc
        raise VenueInvalidOrderError(f"Polymarket order rejected ({status})") from exc
    if isinstance(exc, (InsufficientAllowanceError, InsufficientLiquidityError)):
        raise VenueInsufficientBalanceError(type(exc).__name__) from exc
    if isinstance(exc, (UserInputError, SigningError)):
        raise VenueInvalidOrderError(type(exc).__name__) from exc
    if isinstance(exc, UnexpectedResponseError):
        # A malformed successful response may follow an accepted POST.
        return VenueSubmitResult(
            success=False,
            error="ambiguous_unexpected_response",
            ambiguous_post_failure=True,
        )
    if isinstance(exc, RateLimitError):
        raise VenueRateLimitError("Polymarket rate limit exceeded") from exc
    if isinstance(exc, PolymarketError):
        raise VenueUnavailableError(type(exc).__name__) from exc
    raise VenueUnavailableError(type(exc).__name__) from exc


class PolymarketGateway:
    """Application gateway backed only by the official unified async SDK."""

    def __init__(self, client: PolymarketClient, config: Any = None) -> None:
        self._client = client
        self._config = config

    @property
    def client(self) -> PolymarketClient:
        return self._client

    async def get_orderbook(self, token_id: str) -> OrderBook:
        try:
            async with asyncio.timeout(_SDK_READ_TIMEOUT_SECONDS):
                raw = await self._client.public_client.get_order_book(token_id=token_id)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            _raise_read_error(exc, not_found="orderbook")

        tick_size = Decimal(raw.tick_size)
        min_order_size = Decimal(raw.min_order_size)
        if tick_size <= 0 or min_order_size <= 0:
            raise VenueInvalidOrderError("invalid execution metadata in order book")
        if not isinstance(raw.neg_risk, bool):
            raise VenueInvalidOrderError("order book omitted required neg_risk metadata")

        bids = [
            OrderBookLevel(price=Decimal(level.price), size=Decimal(level.size))
            for level in raw.bids
            if Decimal(level.size) > 0
        ]
        asks = [
            OrderBookLevel(price=Decimal(level.price), size=Decimal(level.size))
            for level in raw.asks
            if Decimal(level.size) > 0
        ]
        best_bid = max((level.price for level in bids), default=None)
        best_ask = min((level.price for level in asks), default=None)
        spread_bps: int | None = None
        if best_bid is not None and best_ask is not None and best_bid > 0:
            if best_ask < best_bid:
                raise VenueInvalidOrderError("inverted order book")
            midpoint = (best_bid + best_ask) / Decimal("2")
            if midpoint > 0:
                spread_bps = int(
                    ((best_ask - best_bid) / midpoint) * BASIS_POINTS_PER_UNIT
                )

        return OrderBook(
            token_id=str(raw.token_id),
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=raw.neg_risk,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            spread_bps=spread_bps,
            timestamp=raw.timestamp or datetime.now(timezone.utc),
        )

    async def get_market_info(self, token_id: str) -> dict[str, Any]:
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
        book = await self.get_orderbook(token_id)
        return {
            "token_id": book.token_id,
            "tick_size": str(book.tick_size),
            "min_order_size": str(book.min_order_size),
            "neg_risk": book.neg_risk,
        }

    async def submit_limit_order(self, request: LimitOrderRequest) -> VenueSubmitResult:
        if not request.condition_id:
            return VenueSubmitResult(
                success=False,
                error="missing_condition_id",
                non_retryable=True,
            )
        side = _normalize_order_side(request.side)
        if not isinstance(self._client, PolymarketClient):
            try:
                raw = await self._client.post_order(
                    SimpleNamespace(
                        token_id=request.token_id,
                        side=side,
                        price=request.price,
                        size=request.size,
                    ),
                    condition_id=request.condition_id,
                )
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                    raise
                return _submission_exception(exc)
            return _submission_from_legacy_dict(raw)

        secure = self._client.require_secure()
        try:
            response = await secure.place_limit_order(
                token_id=request.token_id,
                side=side,
                price=request.price,
                size=request.size,
            )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            logger.warning("limit order submission failed: %s", redact_error_detail(exc))
            return _submission_exception(exc)
        return _submission_from_response(response, side=side)

    async def submit_marketable_order(self, request: MarketableOrderRequest) -> VenueSubmitResult:
        if request.price_limit is None:
            raise VenueInvalidOrderError("price_limit is required for marketable orders")
        if request.tick_size is None or request.neg_risk is None:
            await self.get_execution_metadata(request.token_id)

        order_type = _normalize_market_order_type(request.order_type)
        side = _normalize_order_side(request.side)

        if not isinstance(self._client, PolymarketClient):
            try:
                signed = await self._client.create_market_order(
                    token_id=request.token_id,
                    side=side,
                    amount=request.size,
                    max_price=request.price_limit,
                    tick_size=str(request.tick_size),
                    neg_risk=request.neg_risk,
                    order_type=order_type,
                )
                raw = await self._client.post_signed_order(
                    signed,
                    order_type=order_type,
                )
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                    raise
                return _submission_exception(exc)
            return _submission_from_legacy_dict(raw)

        secure = self._client.require_secure()
        try:
            if side == "BUY":
                response = await secure.place_market_order(
                    token_id=request.token_id,
                    side="BUY",
                    amount=request.size,
                    max_price=request.price_limit,
                    order_type=order_type,
                )
            elif side == "SELL":
                response = await secure.place_market_order(
                    token_id=request.token_id,
                    side="SELL",
                    shares=request.size,
                    min_price=request.price_limit,
                    order_type=order_type,
                )
            else:
                raise VenueInvalidOrderError(f"unsupported order side: {request.side!r}")
        except VenueInvalidOrderError:
            raise
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            logger.warning("market order submission failed: %s", redact_error_detail(exc))
            return _submission_exception(exc)
        return _submission_from_response(response, side=side)

    async def get_order(self, order_id: str) -> VenueOrderState:
        secure = self._client.require_secure()
        try:
            async with asyncio.timeout(_SDK_READ_TIMEOUT_SECONDS):
                raw = await secure.get_order(order_id=order_id)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            _raise_read_error(exc, not_found="order")

        filled = Decimal(raw.size_matched)
        return VenueOrderState(
            order_id=raw.id,
            status=raw.status,
            side=raw.side,
            price=Decimal(raw.price),
            size=Decimal(raw.original_size),
            filled_size=filled,
            avg_fill_price=Decimal(raw.price) if filled > 0 else Decimal("0"),
            created_at=raw.created_at,
            raw=_raw_model(raw),
        )

    async def get_fills(self, market: str | None = None) -> list[VenueFill]:
        secure = self._client.require_secure()
        paginator = secure.list_account_trades(market=market)
        fills: list[VenueFill] = []
        try:
            async with asyncio.timeout(_SDK_READ_TIMEOUT_SECONDS):
                page_count = 0
                async for page in paginator:
                    page_count += 1
                    for raw in page.items:
                        fills.append(
                            VenueFill(
                                fill_id=raw.id,
                                order_id=raw.taker_order_id,
                                token_id=str(raw.token_id),
                                side=raw.side,
                                size=Decimal(raw.size),
                                price=Decimal(raw.price),
                                fee=Decimal("0"),
                                timestamp=int(raw.matched_at.timestamp()),
                                raw=_raw_model(raw),
                            )
                        )
                    if page_count >= _MAX_TRADE_PAGES:
                        break
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            _raise_read_error(exc, not_found="order")
        return fills

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        secure = self._client.require_secure()
        try:
            response = await secure.cancel_order(order_id=order_id)
        except RequestRejectedError as exc:
            if _status_code(exc) == HTTPStatus.NOT_FOUND:
                return VenueCancelResult(
                    success=True,
                    order_id=order_id,
                    status="already_gone",
                )
            _raise_read_error(exc, not_found="order")
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            _raise_read_error(exc, not_found="order")

        if order_id in response.canceled:
            return VenueCancelResult(success=True, order_id=order_id, status="cancelled")
        return VenueCancelResult(
            success=False,
            order_id=order_id,
            status="cancel_failed",
            error=response.not_canceled.get(order_id, "order was not cancelled"),
        )

    async def check_compatibility(self) -> dict[str, Any]:
        try:
            environment = self._client.public_client.environment
            secure = self._client.secure_client
            correct_chain = environment.chain_id == _EXPECTED_CHAIN_ID
            return {
                "sdk_available": correct_chain,
                "sdk": "polymarket-client",
                "chain_id": environment.chain_id,
                "exchange_address": environment.standard_exchange,
                "neg_risk_exchange_address": environment.neg_risk_exchange,
                "collateral_address": environment.collateral_token,
                "conditional_token_address": environment.conditional_tokens,
                "negative_risk_supported": True,
                "authenticated": secure is not None,
                "wallet": str(secure.wallet) if secure is not None else None,
                "wallet_type": secure.wallet_type if secure is not None else None,
                "error": None if correct_chain else "unexpected SDK chain",
            }
        except Exception as exc:  # compatibility probes must never raise
            return {
                "sdk_available": False,
                "authenticated": False,
                "error": redact_error_detail(exc),
            }

    async def close(self) -> None:
        await self._client.close()


# Transitional class name for existing application imports.  It is an alias,
# not a second adapter implementation.
ClobV2Gateway = PolymarketGateway

__all__ = ["BUY", "SELL", "ClobV2Gateway", "PolymarketGateway"]
