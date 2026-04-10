from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    MarketOrderArgs,
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
)

from config.env import (
    ENV,
    SignatureType,
    WalletConfig,
    build_wallet_config,
    validate_wallet_config,
)
from utils.fetchData import _get_session
from utils.logger import get_logger, log_api_response, print_clob_error, redact_error_detail

logger = get_logger("ClobClient")

CLOB_HTTP_URL = (ENV.get("CLOB_HTTP_URL") or "https://clob.polymarket.com").rstrip("/")


@dataclass(frozen=True)
class MarketMeta:
    token_id: str
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None


_META_CACHE_TTL_GOOD = float(ENV.get("META_CACHE_TTL_GOOD", 300))
_META_CACHE_TTL_THIN = float(ENV.get("META_CACHE_TTL_THIN", 30))

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


def _top_of_book_from_levels(levels: list[Any], *, side: str) -> Decimal | None:
    prices = []
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


class ClobClientWrapper:
    def __init__(self, client: ClobClient, wallet_config: WalletConfig):
        self.client = client
        self.wallet_config = wallet_config
        self.signature_type = int(wallet_config.signature_type)
        self.proxy_wallet = wallet_config.funder_address
        # Cache: asset -> (MarketMeta, expiry_timestamp)
        self._market_meta_cache: dict[str, tuple[MarketMeta, float]] = {}

    async def get_order_book(self, asset: str) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: self.client.get_order_book(asset))

    async def get_market_meta(self, asset: str) -> MarketMeta:
        cached = self._market_meta_cache.get(asset)
        if cached:
            meta, expiry = cached
            if time.monotonic() < expiry:
                return meta
            # expired — evict and re-fetch
            del self._market_meta_cache[asset]

        # Prefer py-clob-client orderbook API first.
        try:
            orderbook = await self.get_order_book(asset)
            bids = list(getattr(orderbook, "bids", []) or [])
            asks = list(getattr(orderbook, "asks", []) or [])
            meta = MarketMeta(
                token_id=asset,
                tick_size=Decimal(str(getattr(orderbook, "tick_size", "0.01"))),
                min_order_size=Decimal(str(getattr(orderbook, "min_order_size", "0"))),
                neg_risk=bool(getattr(orderbook, "neg_risk", False)),
                best_bid=_top_of_book_from_levels(bids, side="bid"),
                best_ask=_top_of_book_from_levels(asks, side="ask"),
            )
            ttl = _META_CACHE_TTL_THIN if _is_thin_or_empty_book(meta.best_bid, meta.best_ask) else _META_CACHE_TTL_GOOD
            self._market_meta_cache[asset] = (meta, time.monotonic() + ttl)
            return meta
        except Exception as exc:
            logger.warning(
                "Orderbook metadata fallback to HTTP for %s (%s)",
                asset,
                redact_error_detail(exc),
            )

        # HTTP fallback for resiliency.
        url = f"{CLOB_HTTP_URL}/book?token_id={asset}"
        session = _get_session()
        async with session.get(url) as resp:
            data = await resp.json()
            if resp.status != 200:
                log_api_response(
                    logger,
                    method="GET",
                    url=url,
                    status=resp.status,
                    error=f"status_code={resp.status}",
                )
                raise RuntimeError(f"Failed to fetch market metadata for {asset}: HTTP {resp.status}")

            bids = (data.get("bids") or [])
            asks = (data.get("asks") or [])
            meta = MarketMeta(
                token_id=asset,
                tick_size=Decimal(str(data.get("tick_size", "0.01"))),
                min_order_size=Decimal(str(data.get("min_order_size", "0"))),
                neg_risk=bool(data.get("neg_risk", False)),
                best_bid=_top_of_book_from_levels(bids, side="bid"),
                best_ask=_top_of_book_from_levels(asks, side="ask"),
            )
            ttl = _META_CACHE_TTL_THIN if _is_thin_or_empty_book(meta.best_bid, meta.best_ask) else _META_CACHE_TTL_GOOD
            self._market_meta_cache[asset] = (meta, time.monotonic() + ttl)
            log_api_response(logger, method="GET", url=url, status=resp.status, data=data)
            return meta

    async def get_min_order_size(self, asset: str) -> float:
        meta = await self.get_market_meta(asset)
        return float(meta.min_order_size)

    async def get_price(self, asset: str, side: str):
        try:
            api_side = "SELL" if side == "BUY" else "BUY"
            price_url = f"{CLOB_HTTP_URL}/price?token_id={asset}&side={api_side}"
            session = _get_session()
            async with session.get(price_url) as resp:
                data = await resp.json()
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
        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side=side,
            price=max_price,
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
                lambda: self.client.post_order(signed_order, orderType=order_type),
            )
            result = {
                "success": True,
                "data": response,
                "orderID": response.get("orderID") if isinstance(response, dict) else None,
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
            logger.error("Signed order submission failed (%s)", redacted_error)
            log_api_response(
                logger,
                method="POST",
                url=f"{CLOB_HTTP_URL}/order",
                status=0,
                error=redacted_error,
            )
            # Classify the failure: timeouts and connection errors are AMBIGUOUS
            # because the exchange may have received the order.
            # Do NOT return a simple {success: False} for these — the caller
            # must know this is post-submission ambiguous, not pre-submission safe.
            exc_name = type(exc).__name__.lower()
            is_ambiguous = any(
                kw in exc_name or kw in str(exc).lower()
                for kw in ("timeout", "connectionerror", "connectionreset", "brokenpipe", "readtimeout", "connecttimeout")
            )
            return {
                "success": False,
                "error": redacted_error,
                "ambiguous_post_failure": is_ambiguous,
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
                "orderID": response.get("orderID") if isinstance(response, dict) else None,
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
):
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
    logger.info(
        "Creating CLOB client host=%s chain_id=%s signature_type=%s funder=%s",
        host,
        cfg.chain_id,
        int(cfg.signature_type),
        cfg.funder_address,
    )

    client = ClobClient(
        host,
        key=cfg.private_key,
        chain_id=cfg.chain_id,
        signature_type=int(cfg.signature_type),
        funder=cfg.funder_address,
    )
    user_api_creds = client.create_or_derive_api_creds()
    client.set_api_creds(user_api_creds)
    logger.info("CLOB API credentials derived and set")
    return ClobClientWrapper(client, wallet_config=cfg)
