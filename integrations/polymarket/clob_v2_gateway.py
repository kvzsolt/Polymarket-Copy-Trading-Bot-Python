"""Single gateway for all CLOB V2 SDK access.

All CLOB constants and types must be imported from here, not from
py_clob_client_v2 directly, so that CLOB SDK usage is consolidated in one place.

The only allowed exception is ``utils/createClobClient.py`` which constructs
the raw CLOB client and wraps its full API behind ``ClobClientWrapper``.

This module also exposes the ``ClobV2Gateway`` class which wraps the
``ClobClientWrapper`` and maps raw SDK calls to canonical domain types
from ``integrations.polymarket.models`` and raises structured errors from
``integrations.polymarket.errors``.
"""

from __future__ import annotations

import asyncio
import aiohttp
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, NoReturn

# Lazy imports for py-clob-client-v2 — SDK may not be installed in test/lint environments.
try:
    from py_clob_client_v2.clob_types import OrderArgs, OrderPayload, OrderType, TradeParams  # type: ignore[import-untyped]
    from py_clob_client_v2.order_builder.constants import BUY, SELL  # type: ignore[import-untyped]
    from py_clob_client_v2.endpoints import GET_ORDER, TRADES  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover

    class OrderArgs:  # type: ignore[no-redef]
        """Fallback when py-clob-client-v2 is not installed."""
        def __init__(self, **kwargs: object) -> None:
            for k, v in kwargs.items():
                setattr(self, k, v)

    class OrderType:  # type: ignore[no-redef]
        """Fallback when py-clob-client-v2 is not installed."""
        GTC = "GTC"
        GTD = "GTD"
        FOK = "FOK"
        FAK = "FAK"

    from dataclasses import dataclass

    @dataclass
    class OrderPayload:  # type: ignore[no-redef]
        """Minimal fallback when py_clob_client_v2 is not installed."""

        orderID: str

    class TradeParams:  # type: ignore[no-redef]
        """Fallback when py-clob-client-v2 is not installed."""
        def __init__(self, **kwargs: object) -> None:
            for k, v in kwargs.items():
                setattr(self, k, v)

    BUY = "BUY"
    SELL = "SELL"
    GET_ORDER = "/data/order/"
    TRADES = "/data/trades"


from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
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
import logging

try:
    from utils.logger import get_logger as _get_logger
    logger = _get_logger("ClobV2Gateway")
except ImportError:  # pragma: no cover
    logger = logging.getLogger("ClobV2Gateway")

try:
    from utils.logger import redact_error_detail
except (ImportError, Exception):  # pragma: no cover
    def redact_error_detail(exc: Exception) -> str:  # type: ignore[misc]
        """Fallback that just returns the exception type name."""
        return type(exc).__name__

try:
    from utils.createClobClient import ClobClientWrapper  # type: ignore[import-untyped]
except (ImportError, Exception):  # pragma: no cover

    class ClobClientWrapper:  # type: ignore[no-redef]
        """Placeholder when SDK is not installed."""
        pass

try:
    from utils.numeric import quantize_and_convert_to_float  # type: ignore[import-untyped]
except (ImportError, Exception):  # pragma: no cover
    # Graceful fallback — the adapter is used only at the SDK boundary so
    # tests that mock the SDK client do not need it.
    def quantize_and_convert_to_float(value, tick_size, *, epsilon=None):  # type: ignore[misc]
        return float(value)

# Shared process-wide aiohttp session for true-async venue GETs (order status,
# fills). Module-level name so tests can patch ``clob_v2_gateway._get_session``.
try:
    from utils.fetchData import _get_session  # type: ignore[import-untyped]
except (ImportError, Exception):  # pragma: no cover
    _get_session = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Polymarket py-clob-client-v2 ROUNDING_CONFIG separates price, limit-order
# size, and market-order amount precision. Context7/docs example:
# ROUNDING_CONFIG["0.01"] == RoundConfig(price=2, size=2, amount=4).
# Marketable orders use MarketOrderArgs.amount: USDC for BUY, shares for SELL.
_MARKET_ORDER_AMOUNT_QUANTIZATION: str = "0.0001"
_MARKET_ORDER_AMOUNT_STEP = Decimal(_MARKET_ORDER_AMOUNT_QUANTIZATION)

# Hard wall-clock cap for outbound *GET* SDK calls executed in a thread
# executor (get_order / get_fills / get_order_book). py-clob-client-v2 never
# retries a GET and the sync SDK can block indefinitely on a wedged
# HTTP/2 keep-alive socket (Polymarket/Cloudflare half-open connection),
# which would otherwise stall the reconcile/confirm loop forever (P2-i).
# A GET is idempotent, so timing it out is safe: it is surfaced as the
# ambiguous/retryable ``VenueUnavailableError`` exactly like the SDK's own
# transport failures, letting the reconciler back off and retry instead of
# parking. Kept slightly above the SDK's internal httpx read timeout (10s,
# see utils/createClobClient._configure_sdk_http_transport) so the SDK's own
# timeout fires first when transport is configured.
_SDK_GET_TIMEOUT_SECONDS: float = 12.0

# Per-request wall-clock bound for the true-async (aiohttp) venue GETs
# (get_order / get_fills). These no longer use a thread executor, so a wedged
# socket cannot tie up a pool thread — but we still cap each request so the
# reconcile loop bails fast. Kept at/just below the SDK's internal 10s httpx
# read timeout referenced above; a breach surfaces as a retryable
# ``VenueUnavailableError`` exactly like a transport fault.
_GET_ORDER_HTTP_TIMEOUT_SECONDS: float = 10.0

# Safety cap on /data/trades pagination so a pathological cursor loop cannot
# stall the fills fetch (Polymarket returns newest-first; reconciliation only
# needs the recent page(s)).
_MAX_FILLS_PAGES: int = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _async_signed_get(
    client: Any,
    endpoint: str,
    *,
    params: dict[str, str] | None = None,
    timeout_seconds: float = _GET_ORDER_HTTP_TIMEOUT_SECONDS,
) -> Any:
    """L2-authenticated CLOB REST GET over true async I/O (aiohttp, no threads).

    Reuses the SDK's own header signing: ``client._l2_headers("GET", endpoint)``
    is pure-Python HMAC and performs NO network I/O (``_get_timestamp`` is a
    no-op because the bot builds the SDK client with ``use_server_time=False``),
    so it is safe to call on the event loop. The request itself goes through the
    shared aiohttp session, letting N concurrent GETs overlap on the loop instead
    of each blocking a thread-pool thread.

    A GET is idempotent, so a transport fault (or per-request timeout) is retried
    once before surfacing as the retryable ``VenueUnavailableError``. Returns the
    parsed JSON body; non-2xx is mapped to the same typed venue errors the rest of
    the gateway raises (notably 404 -> ``VenueOrderNotFoundError`` which the
    reconciler's consecutive-404 settlement guard depends on).
    """
    if _get_session is None:  # pragma: no cover - SDK/aiohttp absent in lint env
        raise VenueUnavailableError("async_http_session_unavailable")
    url = client.host + endpoint
    headers = client._l2_headers("GET", endpoint)
    session = _get_session()
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    attempts = 0
    while True:
        try:
            async with session.get(
                url, headers=headers, params=params or None, timeout=timeout
            ) as resp:
                status = resp.status
                if status == 200:
                    return await resp.json(content_type=None)
                if status == 404:
                    raise VenueOrderNotFoundError("order_not_found: status_404")
                if status == 429:
                    raise VenueRateLimitError("rate_limited: status_429")
                if status in (401, 403):
                    raise VenueAuthError(f"auth_failed: status_{status}")
                if status == 408 or 500 <= status <= 599:
                    # Retryable server/transport-ish error — one idempotent retry.
                    if attempts < 1:
                        attempts += 1
                        await asyncio.sleep(0.15)
                        continue
                    raise VenueUnavailableError(f"server_error_{status}")
                raise VenueInvalidOrderError(f"invalid_request: status_{status}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            # GET is idempotent — evict the dead keep-alive connection and retry
            # once on a fresh one (mirrors the prior SDK-path behaviour). NEVER
            # apply this to POST paths.
            if attempts < 1:
                attempts += 1
                await asyncio.sleep(0.15)
                continue
            raise VenueUnavailableError(
                f"poly_api_transport: {redact_error_detail(exc)}"
            ) from exc

def _safe_decimal(value: Any) -> Decimal | None:
    try:
        if value is None:
            return None
        return Decimal(str(value))
    except Exception:
        return None


def _quantize_marketable_amount_to_float(value: Decimal, side: str) -> float:
    """Quantize SDK MarketOrderArgs.amount without treating normal 4dp
    rounding as a terminal order error.

    BUY amount is USDC spend, so nearest 4dp preserves the intended notional.
    SELL amount is shares, so round down to avoid submitting more shares than
    the bot actually holds after venue fills with 4+ decimal precision.
    """
    if value <= 0:
        raise ValueError(f"marketable amount must be positive, got {value}")

    rounding = ROUND_DOWN if str(side).upper() == SELL else ROUND_HALF_UP
    rounded = value.quantize(_MARKET_ORDER_AMOUNT_STEP, rounding=rounding)
    if rounded <= 0:
        raise ValueError(
            f"marketable amount rounds to zero (original={value}, "
            f"step={_MARKET_ORDER_AMOUNT_STEP})"
        )

    delta = abs(value - rounded)
    if delta > Decimal("0"):
        logger.info(
            "SDK_boundary_marketable_amount_conversion | side=%s original=%s "
            "rounded=%s step=%s delta=%s",
            str(side).upper(),
            str(value),
            str(rounded),
            str(_MARKET_ORDER_AMOUNT_STEP),
            str(delta),
        )

    return float(rounded)


def _classify_error(exc: Exception) -> tuple[bool, str]:
    """Classify an exception as (retryable, reason).

    Classification is **status-code first** (P2-k): the venue's HTTP status
    code is the authoritative signal, so when the SDK exception carries one we
    map it directly (404→not_found, 401/403→auth, 400→invalid_order,
    429→rate_limited, 5xx→retryable server_error). Only when no status code is
    present do we fall back to the (less reliable) exception-name / message
    substring matching kept below for legacy/transport shapes. The one
    deliberate exception is ``PolyApiException`` with ``status_code is None``,
    which the SDK uses for *all* httpx transport failures — that is handled by
    its dedicated transport branch further down, not here.
    """
    exc_name = type(exc).__name__.lower()
    msg = str(exc).lower()
    # ``getattr(...) or getattr(...)`` is intentional: a real HTTP status is
    # always truthy (>= 100), and ``status_code`` taking the value ``None``
    # falls through to the camelCase alias. 0 is not a valid HTTP code.
    status_code = getattr(exc, "status_code", None) or getattr(exc, "statusCode", None)

    # --- Status-code-first dispatch (P2-k) ---------------------------------
    # Authoritative when present. ``PolyApiException(status_code=None)`` (the
    # SDK's transport-failure shape) deliberately has no code, so it skips
    # this block and is caught by the transport branch below.
    if isinstance(status_code, int):
        if status_code == 404:
            return False, "not_found"
        if status_code in (401, 403):
            return False, "auth_failure"
        if status_code == 429:
            return True, "rate_limited"
        if status_code == 400:
            return False, "invalid_order"
        if 500 <= status_code <= 599:
            return True, f"server_error_{status_code}"

    # Timeout / connection errors are ambiguous (order may have landed).
    # Match on either the exception type name OR the message — generic
    # ``RuntimeError("timeout after submit")``-style failures from the
    # underlying SDK wrapper would otherwise fall through to the catch-all
    # invalid-order branch and get marked non-retryable, losing the
    # submitted-unconfirmed safety net (issue #66).
    if any(
        kw in exc_name for kw in ("timeout", "connectionerror", "connectionreset", "connectionterminated", "disconnected")
    ) or any(
        kw in msg for kw in (
            "timeout",
            "connection reset",
            "connectionreset",
            "connection lost",
            "connection refused",
            "server disconnected",
            "connection terminated",
            "connectionterminated",
            "connection closed",
            "remote protocol",
            "network_error",
        )
    ):
        return True, "timeout_or_connection"

    # Rate-limit indicators.
    if any(kw in msg for kw in ("rate limit", "rate_limit", "429", "too many requests")):
        return True, "rate_limited"

    # Auth / permission errors.
    if any(kw in msg for kw in ("unauthorized", "forbidden", "invalid api key", "invalid credentials")):
        return False, "auth_failure"

    # Not-found via message (status-code-less 404 shapes). Terminal: the venue
    # has no such order. Used by cancel_order (P2-l) to treat an
    # already-gone order as a terminal success rather than retry forever.
    if any(kw in msg for kw in ("not found", "not_found", "order does not exist", "no order", "unknown order")):
        return False, "not_found"

    # Invalid order / validation errors.
    if any(kw in msg for kw in (
        "lower than the minimum",
        "minimum:",
        "min size:",
        "invalid amount",
        "tick size",
        "invalid order",
        "invalid payload",
        "crosses the book",
        "banned",
        "closed only",
        "not enough balance",
        "allowance",
        "insufficient balance",
    )):
        return False, "invalid_order"

    # 5xx / server errors via message substring — only reached when the
    # exception carried no numeric status_code (the int dispatch above already
    # handled coded 5xx). Kept for SDK shapes that embed the code in the text.
    if any(kw in msg for kw in ("500", "502", "503", "504", "internal server", "bad gateway", "service unavailable", "gateway timeout")):
        return True, "server_error"

    # py-clob-client-v2 collapses every httpx transport failure (RemoteProtocol
    # "Server disconnected", ConnectError, timeouts) into a bare
    # ``PolyApiException(error_msg="Request exception!")`` whose ``status_code``
    # is None — the original detail is discarded by the SDK before we see it
    # (http_helpers/helpers.py). A real HTTP rejection ALWAYS carries a
    # status_code, so ``PolyApiException`` + ``status_code is None`` is, by the
    # SDK's own construction, a network-level transport failure. This mirrors
    # the SDK's internal ``_is_transient_error`` heuristic. Classify it as
    # retryable transport so reconciliation backs off / retries instead of
    # spamming unknown-classification errors every poll. (Kept AFTER the
    # auth/invalid-order branches so a future SDK that sets a terminal
    # error_msg is still caught by those explicit checks first.)
    if exc_name == "polyapiexception" and status_code is None:
        return True, "poly_api_transport"

    return False, "unknown"


def _raise_classified(exc: Exception) -> NoReturn:
    """Raise the appropriate typed venue error based on classification.

    Issue #100: an *unknown* exception (no substring match in
    ``_classify_error``) is ambiguous, not terminal. After a SDK call,
    the order may have landed on the venue even though the call raised.
    Treating unknown as ``VenueInvalidOrderError`` (terminal) would let
    the upstream coordinator release the reservation and allow a duplicate
    submission. Default unknowns to ``VenueUnavailableError`` (ambiguous,
    retryable / submitted-unconfirmed safety net) so duplicate locks and
    reservations stay active until reconciliation proves the state.
    """
    retryable, reason = _classify_error(exc)
    safe_detail = redact_error_detail(exc)
    if retryable:
        if "rate" in reason:
            raise VenueRateLimitError(f"{reason}: {safe_detail}") from exc
        raise VenueUnavailableError(f"{reason}: {safe_detail}") from exc
    if "auth" in reason:
        raise VenueAuthError(f"{reason}: {safe_detail}") from exc
    if reason == "unknown":
        # Best-effort observability — never fatal.
        try:
            from services.metrics import metrics
            metrics.inc_sync(
                "clob_error_classification_unknown_total",
                labels={"exc": type(exc).__name__},
            )
        except Exception:  # pragma: no cover — defensive
            pass
        logger.error(
            "clob_error_classification_unknown | exc=%s detail=%s — "
            "treating as ambiguous (VenueUnavailableError) to preserve "
            "duplicate-lock / submitted-unconfirmed safety",
            type(exc).__name__, safe_detail,
        )
        raise VenueUnavailableError(f"unknown_ambiguous: {safe_detail}") from exc
    raise VenueInvalidOrderError(f"{reason}: {safe_detail}") from exc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_submit_result(result: dict) -> VenueSubmitResult:
    """Translate the underlying client's response dict to ``VenueSubmitResult``.

    Surfaces ``ambiguous_post_failure`` and ``non_retryable`` flags as named
    fields so callers do not need to inspect ``raw`` to make retry/persistence
    decisions.
    """
    data = result.get("data") if isinstance(result.get("data"), dict) else None

    def _pick_any(*keys: str) -> object | None:
        for key in keys:
            nested = data.get(key) if data is not None else None
            if nested is not None:
                return nested
            value = result.get(key)
            if value is not None:
                return value
        return None

    success = bool(result.get("success", False))
    order_id = _pick_any("orderID", "order_id", "id")
    status = _pick_any("status")
    error = result.get("error")

    return VenueSubmitResult(
        success=success,
        order_id=str(order_id) if order_id else None,
        status=str(status) if status else None,
        raw=result,
        error=error,
        ambiguous_post_failure=bool(result.get("ambiguous_post_failure", False)),
        non_retryable=bool(result.get("non_retryable", False)),
    )


# ---------------------------------------------------------------------------
# ClobV2Gateway
# ---------------------------------------------------------------------------

class ClobV2Gateway:
    """High-level gateway wrapping ``ClobClientWrapper``.

    Maps raw py-clob-client-v2 calls to the domain types defined in
    ``integrations.polymarket.models`` and raises structured errors from
    ``integrations.polymarket.errors`` on failures.
    """

    def __init__(self, clob_client: ClobClientWrapper, config: Any = None) -> None:
        self._client = clob_client
        self._config = config

    def _require_raw_client(self):
        """Return the raw ClobClient, raising VenueUnavailableError if unavailable."""
        client = getattr(self._client, "client", None)
        if client is None:
            raise VenueUnavailableError(
                "CLOB client not available (SDK not installed or client not initialized)"
            )
        return client

    # --- Market info -----------------------------------------------------------

    async def get_market_info(self, token_id: str) -> dict[str, Any]:
        """Fetch market metadata (tick_size, min_order_size, neg_risk, etc.)."""
        try:
            meta = await self._client.get_market_meta(token_id)
            return {
                "token_id": meta.token_id,
                "tick_size": str(meta.tick_size),
                "min_order_size": str(meta.min_order_size),
                "neg_risk": meta.neg_risk,
                "best_bid": str(meta.best_bid) if meta.best_bid is not None else None,
                "best_ask": str(meta.best_ask) if meta.best_ask is not None else None,
            }
        except Exception as exc:
            logger.error("get_market_info failed for %s (%s)", token_id, redact_error_detail(exc))
            _raise_classified(exc)

    async def get_execution_metadata(self, token_id: str) -> dict[str, Any]:
        """Fetch only order-construction metadata, without an orderbook snapshot."""
        client = self._require_raw_client()
        loop = asyncio.get_running_loop()
        try:
            tick_size_raw, neg_risk_raw = await asyncio.gather(
                loop.run_in_executor(None, lambda: client.get_tick_size(token_id)),
                loop.run_in_executor(None, lambda: client.get_neg_risk(token_id)),
            )
            tick_size = _safe_decimal(tick_size_raw)
            if tick_size is None or tick_size <= 0:
                raise VenueInvalidOrderError(f"invalid tick_size for {token_id}: {tick_size_raw!r}")
            return {
                "token_id": token_id,
                "tick_size": str(tick_size),
                "neg_risk": bool(neg_risk_raw),
            }
        except (VenueAuthError, VenueInvalidOrderError, VenueRateLimitError, VenueUnavailableError):
            raise
        except Exception as exc:
            logger.error(
                "get_execution_metadata failed for %s (%s)",
                token_id,
                redact_error_detail(exc),
            )
            _raise_classified(exc)

    # --- Order book ------------------------------------------------------------

    async def get_orderbook(self, token_id: str) -> OrderBook:
        """Fetch and return a snapshotted order book."""
        try:
            # Bound the outbound GET (P2-i): the wrapper ultimately runs the
            # sync SDK ``get_order_book`` in a thread executor with no timeout,
            # so a wedged keep-alive socket could hang a trading decision /
            # reconcile poll indefinitely. A book GET is idempotent → safe to
            # surface as ambiguous/retryable ``VenueUnavailableError``.
            async with asyncio.timeout(_SDK_GET_TIMEOUT_SECONDS):
                raw_book = await self._client.get_order_book(token_id)
        except TimeoutError as exc:
            logger.warning("get_orderbook timed out for %s after %ss", token_id, _SDK_GET_TIMEOUT_SECONDS)
            raise VenueUnavailableError(
                f"get_orderbook_timeout after {_SDK_GET_TIMEOUT_SECONDS}s"
            ) from exc
        except Exception as exc:
            logger.error("get_orderbook failed for %s (%s)", token_id, redact_error_detail(exc))
            _raise_classified(exc)

        bids: list[OrderBookLevel] = []
        asks: list[OrderBookLevel] = []

        for level in list(getattr(raw_book, "bids", []) or []):
            price = _safe_decimal(getattr(level, "price", None) if not isinstance(level, dict) else level.get("price"))
            size = _safe_decimal(getattr(level, "size", None) if not isinstance(level, dict) else level.get("size"))
            if price is not None and size is not None and size > 0:
                bids.append(OrderBookLevel(price=price, size=size))

        for level in list(getattr(raw_book, "asks", []) or []):
            price = _safe_decimal(getattr(level, "price", None) if not isinstance(level, dict) else level.get("price"))
            size = _safe_decimal(getattr(level, "size", None) if not isinstance(level, dict) else level.get("size"))
            if price is not None and size is not None and size > 0:
                asks.append(OrderBookLevel(price=price, size=size))

        best_bid = max((l.price for l in bids), default=None)
        best_ask = min((l.price for l in asks), default=None)

        spread_bps: int | None = None
        if best_bid is not None and best_ask is not None and best_bid > 0:
            if best_ask < best_bid:
                logger.warning(
                    "inverted spread detected: best_ask=%s < best_bid=%s for %s",
                    best_ask, best_bid, token_id,
                )
                spread_bps = None
            else:
                mid = (best_bid + best_ask) / Decimal("2")
                if mid > 0:
                    spread_bps = int(((best_ask - best_bid) / mid) * 10000)

        return OrderBook(
            token_id=token_id,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            spread_bps=spread_bps,
            timestamp=datetime.now(timezone.utc),
            tick_size=_safe_decimal(getattr(raw_book, "tick_size", None)),
            min_order_size=_safe_decimal(getattr(raw_book, "min_order_size", None)),
            neg_risk=bool(getattr(raw_book, "neg_risk", False)),
        )

    # --- Order submission ------------------------------------------------------

    async def submit_limit_order(self, request: LimitOrderRequest) -> VenueSubmitResult:
        """Submit a limit (GTC) order via the CLOB wrapper."""
        args = OrderArgs(
            token_id=request.token_id,
            side=request.side,
            size=str(request.size),
            price=str(request.price),
        )
        if not request.condition_id:
            # Fail closed: condition_id selects the neg-risk exchange at signing.
            # Missing it means we cannot prove the correct venue, so refuse to
            # submit rather than sign for a possibly-wrong market (P2-a).
            return VenueSubmitResult(success=False, error="missing_condition_id")
        try:
            result = await self._client.post_order(args, condition_id=request.condition_id)
        except Exception as exc:
            logger.error("submit_limit_order failed (%s)", redact_error_detail(exc))
            _raise_classified(exc)

        if not isinstance(result, dict):
            return VenueSubmitResult(success=False, error="unexpected_response_type")

        return _build_submit_result(result)

    async def submit_marketable_order(self, request: MarketableOrderRequest) -> VenueSubmitResult:
        """Submit a marketable (FOK/IOC) order via the CLOB wrapper."""
        # Use pre-fetched metadata when the caller supplied it (issue #66:
        # ExecutionService already fetches via MarketDataService for sizing
        # and slippage gates; a second fetch here is wasteful and can race).
        if request.tick_size is not None and request.neg_risk is not None:
            tick_size_str = str(request.tick_size)
            neg_risk = bool(request.neg_risk)
        else:
            try:
                meta = await self._client.get_market_meta(request.token_id)
                tick_size_str = str(meta.tick_size)
                neg_risk = meta.neg_risk
            except Exception as exc:
                logger.error("submit_marketable_order meta fetch failed (%s)", redact_error_detail(exc))
                _raise_classified(exc)

        price_limit = request.price_limit
        if price_limit is None:
            raise VenueInvalidOrderError("price_limit is required for marketable orders")
        order_type = request.order_type or getattr(OrderType, "FOK", "FOK")

        # Canonical Decimal→float conversion at the SDK boundary.
        # ``MarketOrderArgs.amount`` is not price-tick sized. Per SDK docs,
        # market-order amount precision is 4 decimals; price_limit still uses
        # the market tick.
        try:
            quantized_size = _quantize_marketable_amount_to_float(
                Decimal(str(request.size)),
                request.side,
            )
            quantized_price = quantize_and_convert_to_float(
                Decimal(str(price_limit)),
                tick_size_str,
            )
        except ValueError as exc:
            logger.error("submit_marketable_order quantization rejected: %s", exc)
            raise VenueInvalidOrderError(str(exc)) from exc

        try:
            signed = await self._client.create_market_order(
                token_id=request.token_id,
                side=request.side,
                amount=quantized_size,
                max_price=quantized_price,
                tick_size=tick_size_str,
                neg_risk=neg_risk,
                order_type=order_type,
            )
            result = await self._client.post_signed_order(
                signed,
                order_type=order_type,
            )
        except Exception as exc:
            logger.error("submit_marketable_order failed (%s)", redact_error_detail(exc))
            _raise_classified(exc)

        if not isinstance(result, dict):
            return VenueSubmitResult(success=False, error="unexpected_response_type")

        return _build_submit_result(result)

    # --- Order state -----------------------------------------------------------

    async def get_order(self, order_id: str) -> VenueOrderState:
        """Fetch the current state of an order from the venue.

        True async I/O: an L2-authenticated REST GET to ``/data/order/{id}`` via
        the shared aiohttp session (no thread executor), so many order-status
        fetches overlap on the event loop during reconciliation instead of each
        blocking a pool thread. ``_async_signed_get`` maps 404 ->
        ``VenueOrderNotFoundError`` (the reconciler's consecutive-poll settlement
        guard depends on it) and surfaces transport/5xx/timeout as the retryable
        ``VenueUnavailableError`` (with the same ``poly_api_transport`` /
        ``server_error_*`` detail), mirroring the prior SDK-path semantics.
        """
        client = self._require_raw_client()
        raw = await _async_signed_get(client, GET_ORDER + order_id)

        if not isinstance(raw, dict):
            raw = {}

        return VenueOrderState(
            order_id=str(raw.get("id") or raw.get("order_id") or raw.get("orderID") or order_id),
            status=str(raw.get("status", "unknown")),
            side=raw.get("side"),
            price=_safe_decimal(raw.get("price")),
            size=_safe_decimal(raw.get("original_size") or raw.get("size")),
            filled_size=_safe_decimal(
                raw.get("size_matched")
                or raw.get("filled_size")
                or raw.get("matched_size")
                or raw.get("matched_amount")
                or raw.get("sizeMatched")
                or raw.get("filledSize")
                or raw.get("matchedSize")
                or raw.get("matchedAmount")
            ) or Decimal("0"),
            avg_fill_price=_safe_decimal(
                raw.get("avg_price")
                or raw.get("avg_fill_price")
                or raw.get("avgFillPrice")
                or raw.get("average_price")
                or raw.get("averagePrice")
            ) or Decimal("0"),
            created_at=None,
            raw=raw,
        )

    # --- Fills -----------------------------------------------------------------

    async def get_fills(self, market: str | None = None) -> list[VenueFill]:
        """Fetch recent fills from the venue via true async REST (no threads).

        L2-authenticated GET ``/data/trades`` (the venue signs the bare path and
        takes ``market`` as a query param). Pages are followed via ``next_cursor``
        up to ``_MAX_FILLS_PAGES`` so a pathological cursor loop cannot stall the
        fetch. A 404 means "no trades" -> empty list.
        """
        client = self._require_raw_client()
        base_params: dict[str, str] = {"market": market} if market else {}

        raw_fills: list[Any] = []
        next_cursor: str | None = None
        try:
            for _ in range(_MAX_FILLS_PAGES):
                page_params = dict(base_params)
                if next_cursor:
                    page_params["next_cursor"] = next_cursor
                body = await _async_signed_get(client, TRADES, params=page_params or None)
                if isinstance(body, dict):
                    data = body.get("data") or []
                    next_cursor = body.get("next_cursor")
                else:
                    data = body or []
                    next_cursor = None
                if isinstance(data, list):
                    raw_fills.extend(item for item in data if isinstance(item, dict))
                # ``LTE=`` is Polymarket's end-of-pagination sentinel.
                if not next_cursor or next_cursor in ("LTE=", ""):
                    break
        except VenueOrderNotFoundError:
            return []

        if not isinstance(raw_fills, list):
            return []

        fills: list[VenueFill] = []
        for raw in raw_fills:
            if not isinstance(raw, dict):
                continue
            fills.append(VenueFill(
                fill_id=raw.get("id"),
                order_id=raw.get("order_id") or raw.get("orderID"),
                token_id=raw.get("asset_id") or raw.get("token_id"),
                side=raw.get("side"),
                size=_safe_decimal(raw.get("size")) or Decimal("0"),
                price=_safe_decimal(raw.get("price")) or Decimal("0"),
                fee=_safe_decimal(raw.get("fee")) or Decimal("0"),
                timestamp=raw.get("timestamp"),
                raw=raw,
            ))
        return fills

    # --- Cancel ----------------------------------------------------------------

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        """Cancel an order on the venue."""
        try:
            client = self._require_raw_client()
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None, lambda: client.cancel_order(OrderPayload(orderID=order_id))
            )
            if isinstance(response, dict) and response.get("success") is False:
                return VenueCancelResult(
                    success=False,
                    order_id=order_id,
                    status=str(response.get("status", "cancel_failed")),
                    error=response.get("error"),
                )
            return VenueCancelResult(
                success=True,
                order_id=order_id,
                status="cancelled",
            )
        except Exception as exc:
            retryable, reason = _classify_error(exc)
            # P2-l: a 404 / not-found on cancel means the order is already gone
            # from the venue (filled, already cancelled, or never indexed).
            # That is the desired end state for a cancel, so treat it as a
            # terminal SUCCESS instead of an ambiguous failure that the caller
            # would retry forever. Detect via the status-code-first
            # classification (``not_found``) so it works whether the SDK
            # surfaces a numeric 404 or a not-found message.
            if reason == "not_found":
                logger.info(
                    "cancel_order: order %s already gone (404/not_found) — terminal success",
                    order_id,
                )
                return VenueCancelResult(
                    success=True,
                    order_id=order_id,
                    status="already_gone",
                )
            if retryable:
                raise VenueUnavailableError(f"cancel failed: {reason}") from exc
            raise VenueInvalidOrderError(f"cancel failed: {reason}") from exc

    # --- Startup compatibility probe ------------------------------------------

    # Required minimum for live trading: every key in this set must be
    # populated and (where applicable) match an expected value, otherwise
    # ``sdk_available`` flips False and the startup health gate fails closed.
    _REQUIRED_COMPAT_FIELDS = ("exchange_address", "chain_id", "negative_risk_supported")
    _EXPECTED_CHAIN_ID = 137  # Polygon mainnet

    async def check_compatibility(self) -> dict[str, Any]:
        """Return a venue/SDK readiness summary for startup health gating.

        Never raises. Caller (``orchestrators/startup_health.py``) decides
        whether to fail closed in live mode based on ``sdk_available``.

        Issue #93 (high): probes every SDK-exposed contract/identity field
        and reports each structurally. The **hard-required** minimum
        (``_REQUIRED_COMPAT_FIELDS`` + ``chain_id == _EXPECTED_CHAIN_ID``)
        flips ``sdk_available`` to False when violated — that is the
        fail-closed contract the startup gate keys on.

        Per-trade live state (collateral/conditional-token **balance** and
        **allowance**) is intentionally NOT probed here: that data is
        wallet/RPC-bound and already gated on the per-trade balance-regime
        / balance-client path. This probe answers "is the SDK wired to the
        right contract family on the right chain" — process-startup scope.
        """
        # Structured discovery: keys with value None mean "not exposed by
        # this SDK build" (informational), not "broken". The required-set
        # check below converts the subset that must be present into errors.
        report: dict[str, Any] = {
            "sdk_available": False,
            "exchange_address": None,
            "neg_risk_exchange_address": None,
            "collateral_address": None,
            "conditional_token_address": None,
            "chain_id": None,
            "negative_risk_supported": False,
            "fee_rate_bps": None,
            "error": None,
        }
        try:
            client = self._require_raw_client()
        except VenueUnavailableError as exc:
            report["error"] = str(exc)
            return report
        except Exception as exc:  # pragma: no cover — defensive
            report["error"] = redact_error_detail(exc)
            return report

        report["sdk_available"] = True

        # Best-effort attribute discovery — each tuple is (sdk_attr, report_key,
        # is_int). Callable attrs are invoked. Missing attrs leave the field
        # at None (informational, not an error unless required below).
        probes: tuple[tuple[str, str, bool], ...] = (
            ("exchange_address", "exchange_address", False),
            ("get_exchange_address", "exchange_address", False),
            ("neg_risk_exchange_address", "neg_risk_exchange_address", False),
            ("get_neg_risk_exchange_address", "neg_risk_exchange_address", False),
            ("collateral_address", "collateral_address", False),
            ("get_collateral_address", "collateral_address", False),
            ("conditional_token_address", "conditional_token_address", False),
            ("get_conditional_token_address", "conditional_token_address", False),
            ("chain_id", "chain_id", True),
            ("get_chain_id", "chain_id", True),
            ("fee_rate_bps", "fee_rate_bps", True),
            ("get_fee_rate_bps", "fee_rate_bps", True),
        )
        for attr, key, is_int in probes:
            if report.get(key) is not None:
                continue
            try:
                value = getattr(client, attr, None)
                if callable(value):
                    value = value()
                if value is not None:
                    report[key] = int(value) if is_int else str(value)
            except Exception:  # pragma: no cover — best-effort discovery
                continue

        # py-clob-client-v2 does not expose contract addresses as client
        # instance attributes — they live in get_contract_config(chain_id).
        # Fall back to the SDK config module for any fields still missing.
        if any(report[k] is None for k in ("exchange_address", "chain_id")):
            try:
                from py_clob_client_v2.config import get_contract_config  # type: ignore[import-untyped]
                contract_cfg = get_contract_config(self._EXPECTED_CHAIN_ID)
                if report["exchange_address"] is None:
                    report["exchange_address"] = str(contract_cfg.exchange)
                if report["neg_risk_exchange_address"] is None:
                    report["neg_risk_exchange_address"] = str(contract_cfg.neg_risk_exchange)
                if report["collateral_address"] is None:
                    report["collateral_address"] = str(contract_cfg.collateral)
                if report["conditional_token_address"] is None:
                    report["conditional_token_address"] = str(contract_cfg.conditional_tokens)
                if report["chain_id"] is None:
                    report["chain_id"] = self._EXPECTED_CHAIN_ID
            except Exception:  # pragma: no cover — defensive, SDK import may vary
                pass

        report["negative_risk_supported"] = (
            hasattr(client, "create_market_order")
            or hasattr(client, "create_and_post_order")
        )

        # Hard fail-closed: exchange_address missing → SDK not wired to a
        # real CLOB endpoint. Stop here so the startup gate marks the
        # runtime unhealthy in live mode.
        if report["exchange_address"] is None:
            report["sdk_available"] = False
            report["error"] = (
                "exchange_address is unknown — SDK may not be connected to a CLOB endpoint"
            )
            return report

        # Hard fail-closed: wrong/missing chain id means the signer would use
        # the wrong domain separator and the venue would reject every order.
        # Previously this was only a log warning; #93 makes it terminal.
        if report["chain_id"] != self._EXPECTED_CHAIN_ID:
            report["sdk_available"] = False
            report["error"] = (
                f"unexpected chain_id={report['chain_id']!r} "
                f"(expected {self._EXPECTED_CHAIN_ID}/Polygon). "
                "Orders would be signed for the wrong domain."
            )
            return report

        if not report["negative_risk_supported"]:
            report["sdk_available"] = False
            report["error"] = (
                "SDK does not expose create_market_order / create_and_post_order — "
                "marketable order path unavailable."
            )
            return report

        return report


# ---------------------------------------------------------------------------
# Backward-compatible re-exports
# ---------------------------------------------------------------------------

__all__ = [
    # Gateway class
    "ClobV2Gateway",
    # Re-exported SDK constants and types
    "BUY",
    "OrderArgs",
    "OrderPayload",
    "OrderType",
    "SELL",
    "TradeParams",
]
