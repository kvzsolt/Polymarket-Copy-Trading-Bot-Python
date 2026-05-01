"""Order submission adapter — ``AbstractOrderGateway`` implementation.

Delegates signing + submission to the underlying CLOB wrapper's
``create_market_order`` / ``post_order`` path. The thick circuit-breaker
state currently lives in ``services/executionService.py`` and will be
re-homed (into this gateway's owning context) in Phase 4.
"""

from __future__ import annotations

from typing import Any

from py_clob_client.clob_types import OrderArgs, OrderType

from domain.ports.clients import AbstractOrderGateway
from utils.createClobClient import ClobClientWrapper


def _resolve_order_type(order_type: str) -> Any:
    """Resolve an order type across enum-like and attribute-based SDK shapes.

    Raises ``ValueError`` for unknown / unsupported order types so callers
    get a clear boundary error instead of an opaque SDK failure downstream.
    """
    resolved = getattr(OrderType, order_type, None)
    if resolved is not None:
        return resolved
    raise ValueError(f"Unsupported order_type: {order_type!r}")


class OrderGateway(AbstractOrderGateway):
    def __init__(self, wrapper: ClobClientWrapper):
        self._wrapper = wrapper

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
    ) -> Any:
        args = OrderArgs(
            token_id=token_id,
            side=side,
            size=size,
            price=price,
        )
        ot = _resolve_order_type(order_type)
        condition_id = (extra or {}).get("condition_id") if extra else None
        # NOTE: order_type is intentionally NOT forwarded to
        # ClobClientWrapper.post_order / create_and_post_order — the V2 SDK
        # defaults to GTC for limit orders submitted via that path. The
        # resolved ``ot`` is only recorded in the response metadata for
        # downstream auditing.
        result = await self._wrapper.post_order(args, condition_id=condition_id)
        if isinstance(result, dict):
            result.setdefault("idempotency_key", idempotency_key)
            result.setdefault("order_type", str(ot))
        return result

    async def post_market_order(
        self,
        *,
        token_id: str,
        side: str,
        amount: float,
        max_price: float,
        tick_size: str,
        neg_risk: bool,
        min_order_size: float = 0.0,
        order_type: str = "FOK",
        idempotency_key: str = "",
        extra: dict[str, Any] | None = None,
    ) -> Any:
        """Create and post a V2 market order with tick_size and neg_risk options.

        Rejects before hitting the exchange if amount < min_order_size.
        tick_size and neg_risk are forwarded into PartialCreateOrderOptions.
        """
        if min_order_size > 0 and amount < min_order_size:
            return {
                "success": False,
                "error": f"size_below_min | amount {amount} < min_order_size {min_order_size}",
                "non_retryable": True,
            }
        ot = _resolve_order_type(order_type)
        signed = await self._wrapper.create_market_order(
            token_id=token_id,
            side=side,
            amount=amount,
            max_price=max_price,
            tick_size=tick_size,
            neg_risk=neg_risk,
            order_type=ot,
        )
        result = await self._wrapper.post_signed_order(signed, order_type=ot)
        if isinstance(result, dict):
            result.setdefault("idempotency_key", idempotency_key)
            result.setdefault("order_type", str(ot))
        return result

    async def cancel_order(self, order_id: str) -> Any:
        import asyncio
        from py_clob_client_v2.clob_types import OrderPayload

        client = self._wrapper.client
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: client.cancel_order(OrderPayload(orderID=order_id))
        )
