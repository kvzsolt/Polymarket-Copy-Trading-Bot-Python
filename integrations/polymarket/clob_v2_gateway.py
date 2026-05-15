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
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

# Lazy imports for py-clob-client-v2 — SDK may not be installed in test/lint environments.
try:
    from py_clob_client_v2.clob_types import OrderArgs, OrderPayload, OrderType, TradeParams  # type: ignore[import-untyped]
    from py_clob_client_v2.order_builder.constants import BUY, SELL  # type: ignore[import-untyped]
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

    from dataclasses import dataclass

    @dataclass
    class OrderPayload:
        """Minimal fallback when py_clob_client_v2 is not installed."""

        orderID: str

    class TradeParams:  # type: ignore[no-redef]
        """Fallback when py-clob-client-v2 is not installed."""
        def __init__(self, **kwargs: object) -> None:
            for k, v in kwargs.items():
                setattr(self, k, v)

    BUY = "BUY"
    SELL = "SELL"


from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInvalidOrderError,
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_decimal(value: Any) -> Decimal | None:
    try:
        if value is None:
            return None
        return Decimal(str(value))
    except Exception:
        return None


def _classify_error(exc: Exception) -> tuple[bool, str]:
    """Classify an exception as (retryable, reason).

    Heuristic classification based on exception type name and message.
    TODO: Replace substring matching with explicit SDK exception types
    once py_clob_client_v2 exports structured exception classes.
    """
    exc_name = type(exc).__name__.lower()
    msg = str(exc).lower()

    # Timeout / connection errors are ambiguous (order may have landed).
    # Match on either the exception type name OR the message — generic
    # ``RuntimeError("timeout after submit")``-style failures from the
    # underlying SDK wrapper would otherwise fall through to the catch-all
    # invalid-order branch and get marked non-retryable, losing the
    # submitted-unconfirmed safety net (issue #66).
    if any(
        kw in exc_name for kw in ("timeout", "connectionerror", "connectionreset", "connectionterminated", "disconnected")
    ) or any(
        kw in msg for kw in ("timeout", "connection reset", "connection lost", "connection refused", "server disconnected", "connection terminated", "connection closed")
    ):
        return True, "timeout_or_connection"

    # Rate-limit indicators.
    if any(kw in msg for kw in ("rate limit", "rate_limit", "429", "too many requests")):
        return True, "rate_limited"

    # Auth / permission errors.
    if any(kw in msg for kw in ("unauthorized", "forbidden", "invalid api key", "invalid credentials")):
        return False, "auth_failure"

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

    # 5xx / server errors — include status code in reason when available.
    status_code = getattr(exc, "status_code", None) or getattr(exc, "statusCode", None)
    if status_code in (500, 502, 503, 504):
        return True, f"server_error_{status_code}"
    if any(kw in msg for kw in ("500", "502", "503", "504", "internal server", "bad gateway", "service unavailable", "gateway timeout")):
        return True, "server_error"

    return False, "unknown"


def _raise_classified(exc: Exception) -> None:
    """Raise the appropriate typed venue error based on classification."""
    retryable, reason = _classify_error(exc)
    safe_detail = redact_error_detail(exc)
    if retryable:
        if "rate" in reason:
            raise VenueRateLimitError(f"{reason}: {safe_detail}") from exc
        raise VenueUnavailableError(f"{reason}: {safe_detail}") from exc
    if "auth" in reason:
        raise VenueAuthError(f"{reason}: {safe_detail}") from exc
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

    def _pick(key: str) -> object | None:
        nested = data.get(key) if data is not None else None
        return result.get(key) if nested is None else nested

    success = bool(result.get("success", False))
    order_id = _pick("orderID")
    status = _pick("status")
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

    # --- Order book ------------------------------------------------------------

    async def get_orderbook(self, token_id: str) -> OrderBook:
        """Fetch and return a snapshotted order book."""
        try:
            raw_book = await self._client.get_order_book(token_id)
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
        try:
            signed = await self._client.create_market_order(
                token_id=request.token_id,
                side=request.side,
                amount=float(request.size),
                max_price=float(price_limit),
                tick_size=tick_size_str,
                neg_risk=neg_risk,
            )
            result = await self._client.post_signed_order(signed)
        except Exception as exc:
            logger.error("submit_marketable_order failed (%s)", redact_error_detail(exc))
            _raise_classified(exc)

        if not isinstance(result, dict):
            return VenueSubmitResult(success=False, error="unexpected_response_type")

        return _build_submit_result(result)

    # --- Order state -----------------------------------------------------------

    async def get_order(self, order_id: str) -> VenueOrderState:
        """Fetch the current state of an order from the venue."""
        client = self._require_raw_client()
        loop = asyncio.get_running_loop()
        try:
            raw = await loop.run_in_executor(None, lambda: client.get_order(order_id))
        except Exception as exc:
            logger.error("get_order failed for %s (%s)", order_id, redact_error_detail(exc))
            _raise_classified(exc)

        if not isinstance(raw, dict):
            raw = {}

        return VenueOrderState(
            order_id=str(raw.get("id", order_id)),
            status=str(raw.get("status", "unknown")),
            side=raw.get("side"),
            price=_safe_decimal(raw.get("price")),
            size=_safe_decimal(raw.get("original_size") or raw.get("size")),
            filled_size=_safe_decimal(raw.get("size_matched") or raw.get("filled_size")) or Decimal("0"),
            avg_fill_price=_safe_decimal(raw.get("avg_price") or raw.get("average_price")) or Decimal("0"),
            created_at=None,
            raw=raw,
        )

    # --- Fills -----------------------------------------------------------------

    async def get_fills(self, market: str | None = None) -> list[VenueFill]:
        """Fetch recent fills from the venue."""
        client = self._require_raw_client()
        loop = asyncio.get_running_loop()
        try:
            if market:
                raw_fills = await loop.run_in_executor(
                    None, lambda: client.get_trades(params=TradeParams(market=market))
                )
            else:
                raw_fills = await loop.run_in_executor(None, lambda: client.get_trades())
        except Exception as exc:
            logger.error("get_fills failed (%s)", redact_error_detail(exc))
            _raise_classified(exc)

        if not isinstance(raw_fills, list):
            return []

        fills: list[VenueFill] = []
        for raw in raw_fills:
            if not isinstance(raw, dict):
                continue
            fills.append(VenueFill(
                fill_id=raw.get("id"),
                order_id=raw.get("order_id"),
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
            if retryable:
                raise VenueUnavailableError(f"cancel failed: {reason}") from exc
            raise VenueInvalidOrderError(f"cancel failed: {reason}") from exc

    # --- Startup compatibility probe ------------------------------------------

    async def check_compatibility(self) -> dict[str, Any]:
        """Return a venue/SDK readiness summary for startup health gating.

        Never raises. Caller (``orchestrators/startup_health.py``) decides
        whether to fail closed in live mode based on ``sdk_available``.
        """
        report: dict[str, Any] = {
            "sdk_available": False,
            "exchange_address": None,
            "chain_id": None,
            "negative_risk_supported": False,
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
        for attr, key in (
            ("exchange_address", "exchange_address"),
            ("get_exchange_address", "exchange_address"),
            ("chain_id", "chain_id"),
            ("get_chain_id", "chain_id"),
        ):
            try:
                value = getattr(client, attr, None)
                if callable(value):
                    value = value()
                if value is not None and report.get(key) is None:
                    report[key] = (
                        int(value) if key == "chain_id" else str(value)
                    )
            except Exception:  # pragma: no cover — best-effort discovery
                continue

        report["negative_risk_supported"] = hasattr(
            client, "create_market_order"
        ) or hasattr(client, "create_and_post_order")
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
