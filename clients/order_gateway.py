"""Legacy-shaped order port backed by the canonical typed gateway."""

from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from typing import Any

from domain.ports.clients import AbstractOrderGateway
from integrations.polymarket.clob_v2_gateway import PolymarketGateway
from integrations.polymarket.models import LimitOrderRequest, MarketableOrderRequest
from integrations.polymarket.sdk_client import PolymarketClient

_SUPPORTED_ORDER_TYPES = frozenset({"GTC", "FAK", "FOK"})


def _resolve_order_type(order_type: str) -> str:
    resolved = str(order_type).upper()
    if resolved not in _SUPPORTED_ORDER_TYPES:
        raise ValueError(f"Unsupported order_type: {order_type!r}")
    return resolved


class OrderGateway(AbstractOrderGateway):
    def __init__(self, client: PolymarketClient):
        self._gateway = PolymarketGateway(client)

    async def post_order(
        self,
        *,
        token_id: str,
        side: str,
        size: float,
        price: float,
        order_type: str,
        idempotency_key: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        resolved = _resolve_order_type(order_type)
        if resolved != "GTC":
            raise ValueError(
                f"Limit-order port supports GTC only; use post_market_order for {resolved}"
            )
        result = await self._gateway.submit_limit_order(
            LimitOrderRequest(
                token_id=token_id,
                side=side,
                size=Decimal(str(size)),
                price=Decimal(str(price)),
                condition_id=str((extra or {}).get("condition_id") or "") or None,
            )
        )
        payload = asdict(result)
        payload["idempotency_key"] = idempotency_key
        payload["order_type"] = resolved
        return payload

    async def post_market_order(
        self,
        *,
        token_id: str,
        side: str,
        amount: float,
        max_price: float,
        tick_size: str,
        neg_risk: bool,
        min_order_size: float,
        order_type: str = "FOK",
        idempotency_key: str = "",
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        amount_decimal = Decimal(str(amount))
        minimum = Decimal(str(min_order_size))
        if minimum <= 0:
            raise ValueError("min_order_size must be positive")
        if amount_decimal < minimum:
            return {
                "success": False,
                "error": f"size_below_min | amount {amount_decimal} < min_order_size {minimum}",
                "non_retryable": True,
                "idempotency_key": idempotency_key,
            }
        resolved = _resolve_order_type(order_type)
        if resolved not in {"FAK", "FOK"}:
            raise ValueError(f"Market order requires FAK or FOK, got {resolved}")
        result = await self._gateway.submit_marketable_order(
            MarketableOrderRequest(
                token_id=token_id,
                side=side,
                size=amount_decimal,
                price_limit=Decimal(str(max_price)),
                condition_id=str((extra or {}).get("condition_id") or "") or None,
                tick_size=Decimal(tick_size),
                neg_risk=neg_risk,
                order_type=resolved,
            )
        )
        payload = asdict(result)
        payload["idempotency_key"] = idempotency_key
        payload["order_type"] = resolved
        return payload

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        return asdict(await self._gateway.cancel_order(order_id))


__all__ = ["OrderGateway", "_resolve_order_type"]
