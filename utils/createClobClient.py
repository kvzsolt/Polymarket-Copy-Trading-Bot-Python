from __future__ import annotations

import asyncio
import random
import threading
import time
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, Decimal
from typing import Any


from py_clob_client_v2 import ApiCreds
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    MarketOrderArgs,
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
)
from py_clob_client_v2.exceptions import PolyApiException

from config.env import (
    SignatureType,
    WalletConfig,
    build_wallet_config,
    validate_wallet_config,
)
from services.wallet_signature_detector import detect_wallet_signature_type
from utils.fetchData import _get_session
from utils.logger import (
    get_logger,
    log_api_response,
    print_clob_error,
    redact_error_detail,
)

logger = get_logger("ClobClient")

# Module-level config. Hardcoded defaults match the previous env-read
# defaults; production paths override via ``configure_from_appconfig(cfg)``
# (called from ``orchestrators/runtime_bootstrap.py`` at startup).
CLOB_HTTP_URL = "https://clob.polymarket.com"
_SDK_HTTP_TRANSPORT_CONFIGURED = False
_SDK_HTTP_TRANSPORT_LOCK = threading.Lock()
_API_KEY_DERIVE_ATTEMPTS = 8
_API_KEY_DERIVE_BASE_DELAY_SECONDS = 0.75
_API_KEY_DERIVE_MAX_DELAY_SECONDS = 15.0
_API_KEY_DERIVE_JITTER_RATIO = 0.15
_API_KEY_DERIVE_RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_API_KEY_DERIVE_TRANSIENT_KEYWORDS = (
    "timeout",
    "connectionerror",
    "connection reset",
    "connectionreset",
    "connectionterminated",
    "connection terminated",
    "connection lost",
    "connection closed",
    "server disconnected",
    "remote protocol",
    "brokenpipe",
    "readtimeout",
    "connecttimeout",
    "request exception",
)


def _configure_sdk_http_transport(*, force: bool = False) -> None:
    """Replace py-clob-client-v2's process-wide HTTP/2 client.

    py-clob-client-v2 1.0.0 creates a module-level ``httpx.Client(http2=True)``
    in ``http_helpers.helpers``. In this bot the sync SDK is called from
    executor threads across multiple runtime paths, so every ClobClient shares
    one HTTP/2 connection pool. Polymarket/Cloudflare can reset idle or
    multiplexed streams, which surfaces as repeated h2 ``ConnectionTerminated``
    / SDK "request error" log lines.

    HTTP/1.1 is enough for the bot's low request volume and avoids the shared
    HTTP/2 stream state. Keepalive expiry is intentionally short so stale
    sockets are not retained long between polling cycles.
    """
    global _SDK_HTTP_TRANSPORT_CONFIGURED
    if _SDK_HTTP_TRANSPORT_CONFIGURED and not force:
        return

    with _SDK_HTTP_TRANSPORT_LOCK:
        if _SDK_HTTP_TRANSPORT_CONFIGURED and not force:
            return

        try:
            import httpx
            from py_clob_client_v2.http_helpers import helpers as sdk_http_helpers
        except Exception as exc:  # pragma: no cover - defensive startup logging
            logger.warning(
                "Unable to configure py_clob_client_v2 HTTP transport: %s",
                redact_error_detail(exc),
            )
            return

        old_client = getattr(sdk_http_helpers, "_http_client", None)
        new_client = httpx.Client(
            http2=False,
            limits=httpx.Limits(
                max_connections=50,
                max_keepalive_connections=10,
                keepalive_expiry=2.0,
            ),
            timeout=httpx.Timeout(10.0, connect=5.0),
        )
        sdk_http_helpers._http_client = new_client

        if old_client is not None and old_client is not new_client:
            try:
                old_client.close()
            except Exception:
                pass

        _SDK_HTTP_TRANSPORT_CONFIGURED = True
        logger.info(
            "Configured py_clob_client_v2 HTTP transport",
            extra={
                "type": "clob_sdk_transport",
                "http2": False,
                "keepalive_expiry_seconds": 2.0,
            },
        )


def _is_retryable_api_key_derivation_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code in _API_KEY_DERIVE_RETRYABLE_STATUS_CODES:
        return True
    if status_code is not None:
        return False
    if isinstance(exc, PolyApiException):
        # py-clob-client-v2 wraps raw httpx transport failures this way.
        return True
    detail = f"{type(exc).__name__} {exc}".lower()
    return any(keyword in detail for keyword in _API_KEY_DERIVE_TRANSIENT_KEYWORDS)


def _configured_api_creds_from_config(config: Any) -> ApiCreds | None:
    clob_config = getattr(config, "clob", None) if config is not None else None
    if clob_config is None:
        return None

    api_key = str(getattr(clob_config, "api_key", "") or "").strip()
    api_secret = str(getattr(clob_config, "api_secret", "") or "").strip()
    api_passphrase = str(getattr(clob_config, "api_passphrase", "") or "").strip()

    if api_key and api_secret and api_passphrase:
        return ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )

    if api_key or api_secret or api_passphrase:
        logger.warning(
            "Ignoring incomplete configured CLOB API credentials; set all of "
            "CLOB_API_KEY, CLOB_SECRET, and CLOB_PASSPHRASE to skip API key derivation",
            extra={"type": "clob_api_creds_incomplete"},
        )

    return None


def _api_key_derivation_retry_delay(
    attempt: int,
    *,
    base_delay_seconds: float,
    max_delay_seconds: float,
) -> float:
    if base_delay_seconds <= 0 or max_delay_seconds <= 0:
        return 0.0
    delay = min(max_delay_seconds, base_delay_seconds * (2 ** (attempt - 1)))
    jitter = delay * _API_KEY_DERIVE_JITTER_RATIO
    return delay + random.uniform(0.0, jitter)


async def _create_or_derive_api_key_with_retry(
    client: ClobClient,
    *,
    max_attempts: int = _API_KEY_DERIVE_ATTEMPTS,
    base_delay_seconds: float = _API_KEY_DERIVE_BASE_DELAY_SECONDS,
    max_delay_seconds: float = _API_KEY_DERIVE_MAX_DELAY_SECONDS,
) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await asyncio.to_thread(client.create_or_derive_api_key)
        except Exception as exc:
            last_exc = exc
            if (
                not _is_retryable_api_key_derivation_error(exc)
                or attempt >= max_attempts
            ):
                break

            _configure_sdk_http_transport(force=True)
            delay = _api_key_derivation_retry_delay(
                attempt,
                base_delay_seconds=base_delay_seconds,
                max_delay_seconds=max_delay_seconds,
            )
            logger.warning(
                "CLOB API credential create/derive failed; retrying | "
                "attempt=%s max_attempts=%s delay=%.2fs error=%s",
                attempt,
                max_attempts,
                delay,
                redact_error_detail(exc),
            )
            if delay > 0:
                await asyncio.sleep(delay)

    if last_exc is None:
        raise RuntimeError(
            "Unable to create or derive CLOB API credentials: no attempts made"
        )
    if _is_retryable_api_key_derivation_error(last_exc):
        raise RuntimeError(
            "Unable to create or derive CLOB API credentials after transient CLOB/API failures"
        ) from last_exc
    raise last_exc


class _PriceTokenNotFound:
    """Sentinel returned by ``get_price`` when the venue answers 404 on
    ``/price?token_id=X``. Per Polymarket OpenAPI 404 means "No orderbook
    exists for the requested token id" — i.e. the token is retired /
    market resolved. Callers must treat this as terminal (mark the token
    resolved upstream), distinct from ``None`` which signals transient
    venue unhealth (5xx, timeout, malformed payload). The sentinel is
    intentionally a unique object, not a string, so identity comparison
    via ``is`` is unambiguous.
    """

    _instance: "_PriceTokenNotFound | None" = None

    def __new__(cls) -> "_PriceTokenNotFound":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "PRICE_TOKEN_NOT_FOUND"


PRICE_TOKEN_NOT_FOUND = _PriceTokenNotFound()


def configure_from_appconfig(config: Any) -> None:
    """Override module-level CLOB constants from a typed AppConfig.

    Production bootstrap calls this once at startup so the module's
    ``CLOB_HTTP_URL`` + meta-cache TTLs follow the typed config rather
    than the raw env. Tests can leave the defaults in place.
    """
    global CLOB_HTTP_URL, _META_CACHE_TTL_GOOD, _META_CACHE_TTL_THIN
    CLOB_HTTP_URL = str(config.clob.http_url or "https://clob.polymarket.com").rstrip("/")
    _META_CACHE_TTL_GOOD = float(config.execution.meta_cache_ttl_good)
    _META_CACHE_TTL_THIN = float(config.execution.meta_cache_ttl_thin)


@dataclass(frozen=True)
class MarketMeta:
    token_id: str
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None


_META_CACHE_TTL_GOOD = 300.0
_META_CACHE_TTL_THIN = 30.0

# Thresholds for detecting garbage/placeholder orderbook state.
# bid <= this AND ask >= (1 - this) means the book is effectively empty.
_THIN_BOOK_BID_CEIL = Decimal("0.03")
_THIN_BOOK_ASK_FLOOR = Decimal("0.97")


def _is_thin_or_empty_book(bid: Decimal | None, ask: Decimal | None) -> bool:
    """Detect garbage/placeholder orderbook (e.g. bid=0.01, ask=0.99)."""
    if bid is None or ask is None:
        return True
    return bid <= _THIN_BOOK_BID_CEIL and ask >= _THIN_BOOK_ASK_FLOOR


def _safe_decimal(value: Any) -> Decimal | None:
    try:
        if value is None:
            return None
        return Decimal(str(value))
    except Exception:
        return None


def _resolve_neg_risk_fail_closed(
    asset: str,
    neg_risk_v2: bool | None,
    book_neg_risk: object | None,
) -> bool:
    """Resolve the market's neg_risk flag, failing closed when unknown (P2-j).

    neg_risk decides WHICH exchange contract an order is signed for
    (standard CTF exchange vs negative-risk exchange). Signing for the wrong
    one produces an order the venue will reject — or worse, mis-routes against
    the wrong contract family. So this value must never be *guessed*.

    Precedence:
    1. The authoritative V2 ``get_neg_risk`` result (``neg_risk_v2``) wins
       when present — including a pre-fetched value threaded by the caller.
    2. Otherwise fall back to the book's own ``neg_risk`` field when the book
       actually reported one (tri-state ``True``/``False``; ``None`` = omitted).
    3. If neither is determinable, raise. The caller (gateway → execution)
       then never signs an order with a fabricated neg_risk — submission fails
       closed instead of silently defaulting to ``False``.
    """
    if neg_risk_v2 is not None:
        return bool(neg_risk_v2)
    if book_neg_risk is not None:
        return bool(book_neg_risk)
    raise RuntimeError(
        f"neg_risk indeterminate for {asset}: get_neg_risk failed and the "
        "order book did not report neg_risk. Refusing to default to False — "
        "signing with a guessed neg_risk would target the wrong exchange."
    )


def _response_order_id(response: Any) -> Any:
    if not isinstance(response, dict):
        return None
    return response.get("orderID") or response.get("order_id") or response.get("id")


def _error_body_order_id(raw_body: str) -> str | None:
    for marker in ('"orderID":"', '"orderID": "', "'orderID': '", "'orderID':'"):
        if marker not in raw_body:
            continue
        start = raw_body.find(marker) + len(marker)
        end = raw_body.find(marker[-1], start)
        if end > start:
            return raw_body[start:end]
    return None


def quantize_to_tick(
    price: Decimal,
    tick_size: Decimal,
    side: str | None = None,
) -> Decimal:
    """Quantize price to the nearest tick_size boundary.

    When *side* is provided the rounding is direction-aware:
    - BUY  → round **up**   (ROUND_CEILING) to give more generous fill protection
    - SELL → round **down** (ROUND_FLOOR)   to stay conservative

    Without *side* the legacy ROUND_DOWN behaviour is preserved.
    """
    if tick_size <= 0:
        return price
    if side is not None:
        units = price / tick_size
        if side.upper() == "BUY":
            rounded_units = units.to_integral_value(rounding=ROUND_CEILING)
        else:
            rounded_units = units.to_integral_value(rounding=ROUND_FLOOR)
        return rounded_units * tick_size
    return price.quantize(tick_size, rounding=ROUND_DOWN)


def _top_of_book_from_levels(levels: list[Any], *, side: str) -> Decimal | None:
    prices = []
    if isinstance(levels, dict):
        for key in levels.keys():
            price = _safe_decimal(key)
            if price is not None:
                prices.append(price)
    else:
        for level in levels:
            price = _safe_decimal(getattr(level, "price", None))
            if price is None and isinstance(level, dict):
                price = _safe_decimal(level.get("price"))
            if price is not None:
                prices.append(price)

    if not prices:
        return None
    if side == "bid":
        return max(prices)
    if side == "ask":
        return min(prices)
    raise ValueError(f"Unsupported book side: {side}")





class _RawBook:
    """Minimal book object returned by the HTTP fallback so gateway attribute access works."""
    __slots__ = ("bids", "asks", "tick_size", "min_order_size", "neg_risk")

    def __init__(
        self,
        bids: list,
        asks: list,
        *,
        tick_size: object | None = None,
        min_order_size: object | None = None,
        neg_risk: object | None = None,
    ) -> None:
        self.bids = bids
        self.asks = asks
        self.tick_size = tick_size
        self.min_order_size = min_order_size
        # Preserve the tri-state (P2-j): ``None`` means the book DID NOT report
        # neg_risk (so it is unknown), distinct from an explicit ``False``.
        # Collapsing omitted→False here previously let a transport-failed
        # ``get_neg_risk`` silently sign a neg-risk order for the wrong
        # exchange. ``get_market_meta`` now fails closed on the ``None`` case.
        self.neg_risk = bool(neg_risk) if neg_risk is not None else None


class ClobClientWrapper:
    def __init__(
        self,
        client: ClobClient,
        wallet_config: WalletConfig,
        api_creds: Any | None = None,
    ):
        self.client = client
        self.wallet_config = wallet_config
        self.api_creds = api_creds
        self.signature_type = int(wallet_config.signature_type)
        self.proxy_wallet = wallet_config.funder_address
        self._market_meta_cache: dict[str, tuple[MarketMeta, float]] = {}

    async def get_order_book(self, asset: str) -> Any:
        http_result = await self._http_fallback_orderbook(asset)
        if http_result is not None:
            return http_result
        # SDK last resort only
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, lambda: self.client.get_order_book(asset))
        except Exception as exc:
            logger.warning("SDK book fetch failed for %s: %s", asset, redact_error_detail(exc))
            return _RawBook(bids=[], asks=[])

    async def _http_fallback_orderbook(self, asset: str) -> "_RawBook | None":
        url = f"{CLOB_HTTP_URL}/book?token_id={asset}"
        session = _get_session()
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    log_api_response(
                        logger,
                        method="GET",
                        url=url,
                        status=resp.status,
                        error=f"status_code={resp.status}",
                    )
                    return None
                data = await resp.json()
                bids = data.get("bids") or []
                asks = data.get("asks") or []
                log_api_response(logger, method="GET", url=url, status=resp.status, data=data)
                return _RawBook(
                    bids=bids,
                    asks=asks,
                    tick_size=data.get("tick_size") or data.get("minimum_tick_size"),
                    min_order_size=(
                        data.get("min_order_size")
                        or data.get("minimum_order_size")
                        or data.get("order_min_size")
                    ),
                    # Pass through ``None`` when the venue omits neg_risk so the
                    # tri-state is preserved (P2-j). Do NOT default to False.
                    neg_risk=data.get("neg_risk"),
                )
        except Exception as exc:
            logger.warning("HTTP fallback (book) failed for %s: %s", asset, redact_error_detail(exc))
            return None

    async def get_market_meta(self, asset: str, refresh: bool = False) -> MarketMeta:
        if refresh:
            self._market_meta_cache.pop(asset, None)
        else:
            cached = self._market_meta_cache.get(asset)
            if cached:
                meta, expiry = cached
                if time.monotonic() < expiry:
                    return meta
                # ``.pop(..., None)`` rather than ``del`` so two
                # concurrent coroutines that both observed the expired
                # entry cannot race on the same delete and raise
                # ``KeyError``. The asyncio.gather call below means
                # the race is real, not theoretical.
                self._market_meta_cache.pop(asset, None)

        loop = asyncio.get_running_loop()

        # Phase 6: V2 dedicated methods + orderbook fetched concurrently.
        # get_tick_size / get_neg_risk are authoritative V2 endpoints; orderbook
        # provides best_bid, best_ask, and min_order_size.  Each failure is
        # isolated — return_exceptions=True prevents one bad call from aborting all.
        ts_raw, nr_raw, ob_result = await asyncio.gather(
            loop.run_in_executor(None, lambda: self.client.get_tick_size(asset)),
            loop.run_in_executor(None, lambda: self.client.get_neg_risk(asset)),
            self.get_order_book(asset),
            return_exceptions=True,
        )

        tick_size_v2: Decimal | None = None
        neg_risk_v2: bool | None = None

        if not isinstance(ts_raw, BaseException):
            tick_size_v2 = _safe_decimal(str(ts_raw))
        else:
            logger.warning("get_tick_size failed for %s (%s)", asset, redact_error_detail(ts_raw))

        if not isinstance(nr_raw, BaseException):
            neg_risk_v2 = bool(nr_raw)
        else:
            logger.warning("get_neg_risk failed for %s (%s)", asset, redact_error_detail(nr_raw))

        if not isinstance(ob_result, BaseException):
            orderbook = ob_result
            bids = list(getattr(orderbook, "bids", []) or [])
            asks = list(getattr(orderbook, "asks", []) or [])
            meta = MarketMeta(
                token_id=asset,
                tick_size=tick_size_v2 if tick_size_v2 is not None else Decimal(str(getattr(orderbook, "tick_size", "0.01"))),
                min_order_size=Decimal(str(getattr(orderbook, "min_order_size", "0"))),
                # Fail closed if neg_risk cannot be determined (P2-j) instead of
                # defaulting to False. ``getattr(..., None)`` keeps the book's
                # tri-state: the SDK book always reports neg_risk; the HTTP
                # ``_RawBook`` yields None when the venue omitted it.
                neg_risk=_resolve_neg_risk_fail_closed(
                    asset, neg_risk_v2, getattr(orderbook, "neg_risk", None)
                ),
                best_bid=_top_of_book_from_levels(bids, side="bid"),
                best_ask=_top_of_book_from_levels(asks, side="ask"),
            )

            ttl = _META_CACHE_TTL_THIN if _is_thin_or_empty_book(meta.best_bid, meta.best_ask) else _META_CACHE_TTL_GOOD
            self._market_meta_cache[asset] = (meta, time.monotonic() + ttl)
            return meta

        logger.warning(
            "Orderbook metadata fallback to HTTP for %s (%s)",
            asset,
            redact_error_detail(ob_result),
        )

        http_meta = await self._http_fallback_market_meta(asset, tick_size_v2, neg_risk_v2)
        if http_meta is not None:
            return http_meta

        raise RuntimeError(f"Failed to fetch market metadata for {asset}: SDK error and HTTP fallback failed")

    async def _http_fallback_market_meta(
        self, asset: str, tick_size_v2: Decimal | None, neg_risk_v2: bool | None,
    ) -> MarketMeta | None:
        url = f"{CLOB_HTTP_URL}/book?token_id={asset}"
        session = _get_session()
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    log_api_response(
                        logger,
                        method="GET",
                        url=url,
                        status=resp.status,
                        error=f"status_code={resp.status}",
                    )
                    return None
                data = await resp.json()

                bids = (data.get("bids") or [])
                asks = (data.get("asks") or [])
                meta = MarketMeta(
                    token_id=asset,
                    tick_size=tick_size_v2 if tick_size_v2 is not None else Decimal(str(data.get("tick_size", "0.01"))),
                    min_order_size=Decimal(str(data.get("min_order_size", "0"))),
                    # Fail closed when neg_risk is indeterminate (P2-j): V2
                    # ``get_neg_risk`` failed AND the /book payload omitted
                    # neg_risk. ``data.get("neg_risk")`` (no False default)
                    # preserves the tri-state the resolver keys on.
                    neg_risk=_resolve_neg_risk_fail_closed(
                        asset, neg_risk_v2, data.get("neg_risk")
                    ),
                    best_bid=_top_of_book_from_levels(bids, side="bid"),
                    best_ask=_top_of_book_from_levels(asks, side="ask"),
                )
                ttl = _META_CACHE_TTL_THIN if _is_thin_or_empty_book(meta.best_bid, meta.best_ask) else _META_CACHE_TTL_GOOD
                self._market_meta_cache[asset] = (meta, time.monotonic() + ttl)
                log_api_response(logger, method="GET", url=url, status=resp.status, data=data)
                return meta
        except Exception as exc:
            logger.warning("HTTP fallback failed for %s: %s", asset, redact_error_detail(exc))
            return None

    async def get_min_order_size(self, asset: str) -> Decimal:
        # Return Decimal. ``MarketMeta.min_order_size`` is already
        # Decimal; the previous ``float()`` cast lost precision at the
        # boundary and violated CLAUDE.md "no floats in sizing logic".
        # No callers exist today (grep clean), so this is a free type
        # tightening.
        meta = await self.get_market_meta(asset)
        return meta.min_order_size

    def get_orders(self, *args: Any, **kwargs: Any) -> Any:
        return self.client.get_orders(*args, **kwargs)

    def cancel_orders(self, order_ids: list[str]) -> Any:
        return self.client.cancel_orders(order_ids)

    async def get_price(self, asset: str, side: str):
        try:
            api_side = "SELL" if side == "BUY" else "BUY"
            price_url = f"{CLOB_HTTP_URL}/price?token_id={asset}&side={api_side}"
            session = _get_session()
            async with session.get(price_url) as resp:
                data = await resp.json()
                if resp.status == 404:
                    # Per Polymarket OpenAPI: 404 = "No orderbook exists for
                    # the requested token id". Terminal signal — token is
                    # retired / market resolved. Distinct from 5xx (transient
                    # venue unhealth) which still returns None. Log once at
                    # debug, no Rich error panel; risk monitor handles the
                    # mark_resolved side-effect.
                    log_api_response(
                        logger,
                        method="GET",
                        url=price_url,
                        status=resp.status,
                        error="token_not_found",
                    )
                    return PRICE_TOKEN_NOT_FOUND
                if resp.status != 200:
                    log_api_response(
                        logger,
                        method="GET",
                        url=price_url,
                        status=resp.status,
                        error=f"status_code={resp.status}",
                    )
                    print_clob_error(
                        operation="get_price",
                        error=f"HTTP {resp.status} response",
                        asset=asset,
                        side=side,
                    )
                    return None

                log_api_response(logger, method="GET", url=price_url, status=resp.status, data=data)

                if isinstance(data, dict):
                    price = data.get("price", data.get("data", {}).get("price"))
                    size = data.get("size", data.get("amount", data.get("data", {}).get("size")))
                    if price is None:
                        return None
                    return {"price": str(price), "size": str(size) if size is not None else "0"}
                if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                    first_item = data[0]
                    price = first_item.get("price")
                    if price is None:
                        return None
                    return {"price": str(price), "size": str(first_item.get("size", "0"))}
                if data is None:
                    return None
                return {"price": str(data), "size": "0"}
        except Exception as exc:
            logger.error("Price fetch exception (%s)", redact_error_detail(exc))
            print_clob_error(operation="get_price", error=exc, asset=asset, side=side)
            return None

    async def create_market_order(
        self,
        *,
        token_id: str,
        side: str,
        amount: float,
        max_price: float,
        tick_size: str,
        neg_risk: bool,
        order_type: OrderType = OrderType.FOK,
    ):
        # INF-05 (CLAUDE.md no-float-money): keep the side-aware tick
        # quantization (BUY→CEILING, SELL→FLOOR) so the order price never
        # rounds against us at the venue, and route the final Decimal→float
        # conversion through ``quantize_and_convert_to_float`` so the
        # sanctioned epsilon guard fires at the SDK boundary instead of
        # falling out via raw ``float(...)``.
        quantized_d = quantize_to_tick(Decimal(str(max_price)), Decimal(tick_size), side=side)
        from utils.numeric import quantize_and_convert_to_float
        quantized_price = quantize_and_convert_to_float(quantized_d, tick_size)
        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side=side,
            price=quantized_price,
            order_type=order_type,
        )
        opts = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.client.create_market_order(order_args, options=opts),
        )

    async def post_signed_order(
        self,
        signed_order: Any,
        order_type: OrderType = OrderType.FOK,
    ) -> dict[str, Any]:
        try:
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.post_order(signed_order, order_type=order_type),
            )
            result = {
                "success": True,
                "data": response,
                "orderID": _response_order_id(response),
                "status": response.get("status") if isinstance(response, dict) else None,
            }
            log_api_response(
                logger,
                method="POST",
                url=f"{CLOB_HTTP_URL}/order",
                status=200,
                data=response,
                meta={"orderType": str(order_type)},
            )
            return result
        except Exception as exc:
            redacted_error = redact_error_detail(exc)
            # Pull the raw error body from the CLOB client if present. py-clob-client-v2
            # attaches the upstream response on `.error_msg` for HTTPException.
            # This is NOT a secret (it's the CLOB's own rejection reason like
            # "Size (0.13) lower than the minimum: 5") so we surface it to the
            # operator for diagnosis.
            raw_body = ""
            if hasattr(exc, "error_msg") and exc.error_msg:
                raw_body = str(exc.error_msg)[:240]
            elif hasattr(exc, "__cause__") and exc.__cause__:
                raw_body = str(exc.__cause__)[:240]
            else:
                raw_body = str(exc)[:240]

            # Build a human-friendly error code + detail for the TUI.
            friendly_error = redacted_error
            terminal_status = "failed"
            if raw_body:
                # Heuristic: surface common CLOB rejections with their key line.
                lowered = raw_body.lower()
                if "no orders found to match with fak order" in lowered:
                    friendly_error = f"fak_no_match | {raw_body}"
                    terminal_status = "cancelled"
                elif "fak orders are partially filled or killed if no match is found" in lowered:
                    friendly_error = f"fak_no_match | {raw_body}"
                    terminal_status = "cancelled"
                elif "order couldn't be fully filled" in lowered:
                    friendly_error = f"order_not_fully_fillable | {raw_body}"
                    terminal_status = "cancelled"
                elif "lower than the minimum" in lowered or "minimum:" in lowered:
                    friendly_error = f"size_below_min | {raw_body}"
                elif "not enough balance" in lowered or "allowance" in lowered:
                    friendly_error = f"insufficient_balance | {raw_body}"
                elif "tick size" in lowered:
                    friendly_error = f"tick_size_violation | {raw_body}"
                elif "invalid" in lowered:
                    friendly_error = f"invalid_order | {raw_body}"
                elif redacted_error == "request_failed":
                    # Only leak raw body when redaction would otherwise hide a
                    # real CLOB rejection reason.
                    friendly_error = f"clob_error | {raw_body}"

            logger.error(
                "Signed order submission failed (%s) | raw=%s",
                redacted_error,
                raw_body[:500],
            )
            log_api_response(
                logger,
                method="POST",
                url=f"{CLOB_HTTP_URL}/order",
                status=0,
                error=friendly_error,
            )
            # Classify the failure: timeouts and connection errors are AMBIGUOUS
            # because the exchange may have received the order.
            # Do NOT return a simple {success: False} for these — the caller
            # must know this is post-submission ambiguous, not pre-submission safe.
            exc_name = type(exc).__name__.lower()
            exc_text = str(exc).lower()
            raw_lower = raw_body.lower()
            # A 5xx returned AFTER the venue may have accepted/matched the order
            # is post-submission AMBIGUOUS, not a clean rejection: the matching
            # engine can hold the order even though the edge/CDN returned an
            # error. The SDK sets ``.status_code`` on PolyApiException (a raw
            # transport failure surfaces as status_code=None). Classify by
            # status code, not just by substring — a 5xx body ("Service
            # Unavailable" / "Bad Gateway" / "Internal Server Error") matches
            # none of the keyword set below, so without this a 5xx-after-accept
            # would be treated as a plain retryable failure, re-signed, and
            # re-POSTed as a DUPLICATE order. Routing it to ambiguous makes the
            # caller persist submitted_unconfirmed and defer to reconciliation.
            # 4xx validation rejections are intentionally excluded (terminal).
            status_code = getattr(exc, "status_code", None)
            is_ambiguous = (
                status_code in (500, 502, 503, 504)
                or (type(exc).__name__ == "PolyApiException" and status_code is None)
            )
            is_ambiguous = is_ambiguous or any(
                kw in exc_name or kw in exc_text or kw in raw_lower
                for kw in (
                    "timeout",
                    "connectionerror",
                    "connectionreset",
                    "connectionterminated",
                    "connection terminated",
                    "connection lost",
                    "connection closed",
                    "server disconnected",
                    "remote protocol",
                    "brokenpipe",
                    "readtimeout",
                    "connecttimeout",
                    "request exception",
                )
            )
            # Classify non-retryable validation errors so the caller can
            # fail fast instead of burning retries on an order CLOB will
            # reject every time (size/tick/balance/invalid).
            is_non_retryable = any(
                kw in raw_body.lower()
                for kw in (
                    "lower than the minimum",
                    "minimum:",
                    "min size:",
                    "invalid amount for a marketable",
                    "tick size",
                    "invalid order",
                    "invalid payload",
                    "crosses the book",
                    "no orders found to match with fak order",
                    "fak orders are partially filled or killed if no match is found",
                    "order couldn't be fully filled",
                    "banned",
                    "closed only",
                    "not enough balance",
                    "allowance",
                    "insufficient balance",
                )
            )
            return {
                "success": False,
                "status": terminal_status,
                "orderID": _error_body_order_id(raw_body),
                "error": friendly_error,
                "raw_error": raw_body,
                "ambiguous_post_failure": is_ambiguous,
                "non_retryable": is_non_retryable,
            }

    async def post_order(self, order_args: OrderArgs, condition_id: str = None):
        """Backward-compatible helper for existing codepaths."""
        try:
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.create_and_post_order(order_args),
            )
            if response is None:
                return {"success": False, "error": "No response from exchange"}
            result = {
                "success": True,
                "data": response,
                "orderID": _response_order_id(response),
                "status": response.get("status") if isinstance(response, dict) else None,
            }
            log_api_response(
                logger,
                method="POST",
                url=f"{CLOB_HTTP_URL}/order",
                status=200,
                data=response,
                meta={"conditionId": condition_id, "side": getattr(order_args, "side", None)},
            )
            return result
        except Exception as exc:
            redacted_error = redact_error_detail(exc)
            logger.error("Order submission failed (%s)", redacted_error)
            log_api_response(
                logger,
                method="POST",
                url=f"{CLOB_HTTP_URL}/order",
                status=0,
                error=redacted_error,
                meta={"conditionId": condition_id},
            )
            print_clob_error(
                operation="post_order",
                error=exc,
                asset=getattr(order_args, "token_id", None),
                side=getattr(order_args, "side", None),
                condition_id=condition_id,
            )
            return {"success": False, "error": redacted_error}


async def create_clob_client(
    *,
    wallet_config: WalletConfig | None = None,
    private_key: str | None = None,
    proxy_wallet: str | None = None,
    config: Any = None,
):
    # CLOB V2 gate — typed `AppConfig.clob.clob_v2_ready` is the only
    # source of truth; falls back to fail-closed False when no config
    # is passed. Production paths thread `config=app_config` from
    # `app.py:create_clob_client(wallet_config=..., config=app_config)`.
    clob_v2_ready = bool(getattr(getattr(config, "clob", None), "clob_v2_ready", False))
    if not clob_v2_ready:
        raise RuntimeError(
            "CLOB V2 migration not finalized — config.clob.clob_v2_ready is False. "
            "Set CLOB_V2_READY=true in your .env once smoke tests pass "
            "(see docs/v2_operator_runbook.md)."
        )
    _configure_sdk_http_transport()
    host = CLOB_HTTP_URL
    cfg = wallet_config or build_wallet_config()

    # Backward-compatible overrides for existing callers
    if private_key:
        cfg = WalletConfig(
            private_key=private_key,
            chain_id=cfg.chain_id,
            signature_type=cfg.signature_type,
            funder_address=proxy_wallet or cfg.funder_address,
            polygon_rpc_url=cfg.polygon_rpc_url,
        )
    elif proxy_wallet:
        cfg = WalletConfig(
            private_key=cfg.private_key,
            chain_id=cfg.chain_id,
            signature_type=cfg.signature_type,
            funder_address=proxy_wallet,
            polygon_rpc_url=cfg.polygon_rpc_url,
        )

    validate_wallet_config(cfg)
    detected_signature_type, wallet_type, signer_address = await detect_wallet_signature_type(cfg)
    runtime_cfg = WalletConfig(
        private_key=cfg.private_key,
        chain_id=cfg.chain_id,
        signature_type=detected_signature_type,
        funder_address=cfg.funder_address,
        polygon_rpc_url=cfg.polygon_rpc_url,
    )
    configured_api_creds = _configured_api_creds_from_config(config)
    client = ClobClient(
        host,
        key=runtime_cfg.private_key,
        chain_id=runtime_cfg.chain_id,
        signature_type=int(runtime_cfg.signature_type),
        funder=(runtime_cfg.funder_address if runtime_cfg.signature_type != SignatureType.EOA else None),
    )
    if configured_api_creds is not None:
        user_api_creds = configured_api_creds
        logger.info(
            "Using configured CLOB API credentials; skipping create_or_derive_api_key",
            extra={"type": "clob_api_creds_configured"},
        )
    else:
        # L1 signer client (secret key) -> create or derive L2/HMAC credentials.
        # py-clob-client-v2's wrapper handles both first-run wallets and wallets
        # that already have a CLOB API key.
        user_api_creds = await _create_or_derive_api_key_with_retry(client)
    client.set_api_creds(user_api_creds)
    logger.info(
        "CLOB client ready",
        extra={
            "type": "clob_client_init",
            "host": host,
            "chainId": runtime_cfg.chain_id,
            "signatureType": int(runtime_cfg.signature_type),
            "walletType": wallet_type,
            "funder": runtime_cfg.funder_address,
            "signer": signer_address,
        },
    )

    # V2 collateral readiness: pUSD balance + allowance check.
    # Only runs when CLOB_V2_READY is true (we're past the safety gate above).
    # FAIL CLOSED: if readiness fails, startup is aborted.
    from services.collateral_readiness import check_collateral_readiness

    funder_or_signer = runtime_cfg.funder_address or signer_address or ""
    if funder_or_signer:
        # Addresses must be set in typed config (no defaults). The
        # collateral check fail-closes on the historically wrong defaults
        # (Synthetix SNX masquerading as pUSD). Operator-supplied values
        # are validated by check_collateral_readiness itself.
        _network = getattr(config, "network", None) if config is not None else None
        _pusd_addr = (
            (getattr(_network, "pusd_contract_address", "") if _network else "")
            or (getattr(_network, "collateral_contract_address", "") if _network else "")
            or ""
        )
        _ctf_addr = (getattr(_network, "ctf_exchange_v2_address", "") if _network else "") or ""
        _neg_risk_addr = (
            getattr(_network, "neg_risk_ctf_exchange_v2_address", "") if _network else ""
        ) or ""
        collateral_status = await check_collateral_readiness(
            wallet_address=funder_or_signer,
            rpc_url=runtime_cfg.polygon_rpc_url,
            pusd_address=_pusd_addr,
            ctf_exchange_address=_ctf_addr,
            neg_risk_exchange_address=_neg_risk_addr,
        )
        if not collateral_status.ready:
            reasons = "; ".join(collateral_status.failures)
            raise RuntimeError(
                f"V2 collateral readiness FAILED — live trading blocked. "
                f"Resolve: {reasons}"
            )
        logger.info(
            "V2 collateral readiness OK — pusd_balance=%.2f",
            collateral_status.pusd_balance,
        )

    return ClobClientWrapper(
        client,
        wallet_config=runtime_cfg,
        api_creds=user_api_creds,
    )
