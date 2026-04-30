from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, Decimal
from typing import Any

from web3 import Web3

from utils.clob_v1_compat import (
    ClobClient,
    MarketOrderArgs,
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    derive_api_creds,
    submit_signed_order,
)

from config.env import (
    ENV,
    SignatureType,
    WalletConfig,
    build_wallet_config,
    validate_wallet_config,
)
from utils.fetchData import _get_session
from utils.logger import (
    get_logger,
    log_api_response,
    print_clob_error,
    redact_error_detail,
)

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


def _derive_signer_address(private_key: str) -> str | None:
    try:
        return str(Web3().eth.account.from_key(private_key).address)
    except Exception as exc:
        logger.warning("Failed to derive signer address (%s)", redact_error_detail(exc))
        return None


def _fetch_contract_code_hex(rpc_url: str, address: str) -> str:
    provider = Web3.HTTPProvider(rpc_url)
    w3 = Web3(provider)
    checksum = Web3.to_checksum_address(address)
    return str(w3.eth.get_code(checksum).hex())


def _classify_wallet_signature_type(
    *,
    signer_address: str | None,
    funder_address: str,
    funder_has_code: bool,
) -> tuple[SignatureType, str]:
    signer = str(signer_address or "").strip().lower()
    funder = str(funder_address or "").strip().lower()

    if signer and funder and signer == funder:
        return (SignatureType.EOA, "EOA")
    if funder_has_code:
        return (SignatureType.GNOSIS_SAFE, "GNOSIS_SAFE")
    if signer and funder and signer != funder:
        return (SignatureType.POLY_PROXY, "POLY_PROXY")
    return (SignatureType.EOA, "EOA")


async def detect_wallet_signature_type(wallet_config: WalletConfig) -> tuple[SignatureType, str, str | None]:
    signer_address = _derive_signer_address(wallet_config.private_key)
    funder_address = str(wallet_config.funder_address or "").strip()
    funder_has_code = False

    if signer_address and funder_address and signer_address.lower() != funder_address.lower():
        try:
            code_hex = await asyncio.to_thread(
                _fetch_contract_code_hex,
                wallet_config.polygon_rpc_url,
                funder_address,
            )
            funder_has_code = bool(code_hex and code_hex != "0x")
        except Exception as exc:
            logger.warning(
                "Wallet type RPC probe failed (%s)",
                redact_error_detail(exc),
            )

    signature_type, wallet_type = _classify_wallet_signature_type(
        signer_address=signer_address,
        funder_address=funder_address,
        funder_has_code=funder_has_code,
    )
    return (signature_type, wallet_type, signer_address)


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
            del self._market_meta_cache[asset]

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
                neg_risk=neg_risk_v2 if neg_risk_v2 is not None else bool(getattr(orderbook, "neg_risk", False)),
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

        # HTTP fallback for resiliency.
        url = f"{CLOB_HTTP_URL}/book?token_id={asset}"
        session = _get_session()
        async with session.get(url) as resp:
            if resp.status != 200:
                log_api_response(
                    logger,
                    method="GET",
                    url=url,
                    status=resp.status,
                    error=f"status_code={resp.status}",
                )
                raise RuntimeError(f"Failed to fetch market metadata for {asset}: HTTP {resp.status}")
            data = await resp.json()

            bids = (data.get("bids") or [])
            asks = (data.get("asks") or [])
            meta = MarketMeta(
                token_id=asset,
                tick_size=tick_size_v2 if tick_size_v2 is not None else Decimal(str(data.get("tick_size", "0.01"))),
                min_order_size=Decimal(str(data.get("min_order_size", "0"))),
                neg_risk=neg_risk_v2 if neg_risk_v2 is not None else bool(data.get("neg_risk", False)),
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
        quantized_price = float(quantize_to_tick(Decimal(str(max_price)), Decimal(tick_size), side=side))
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
                lambda: submit_signed_order(self.client, signed_order, order_type),
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
            # Pull the raw error body from the CLOB client if present. py_clob_client
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
            if raw_body:
                # Heuristic: surface common CLOB rejections with their key line.
                lowered = raw_body.lower()
                if "lower than the minimum" in lowered or "minimum:" in lowered:
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
            is_ambiguous = any(
                kw in exc_name or kw in str(exc).lower()
                for kw in ("timeout", "connectionerror", "connectionreset", "brokenpipe", "readtimeout", "connecttimeout")
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
                    "banned",
                    "closed only",
                    "not enough balance",
                    "allowance",
                    "insufficient balance",
                )
            )
            return {
                "success": False,
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
    if not ENV.get("CLOB_V2_READY", False):
        raise RuntimeError(
            "CLOB V2 migration not finalized — CLOB_V2_READY is not set. "
            "Set CLOB_V2_READY=true once smoke tests pass "
            "(see docs/v2_operator_runbook.md)."
        )
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
    client = ClobClient(
        host,
        key=runtime_cfg.private_key,
        chain_id=runtime_cfg.chain_id,
        signature_type=int(runtime_cfg.signature_type),
        funder=(runtime_cfg.funder_address if runtime_cfg.signature_type != SignatureType.EOA else None),
    )
    user_api_creds = derive_api_creds(client)
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
    return ClobClientWrapper(client, wallet_config=runtime_cfg)
