"""Order submission adapter — ``AbstractOrderGateway`` implementation.

Delegates signing + submission to the underlying CLOB wrapper's
``create_market_order`` / ``post_order`` path. The thick circuit-breaker
state currently lives in ``services/executionService.py`` and will be
re-homed (into this gateway's owning context) in Phase 4.
"""

from __future__ import annotations

from typing import Any

from utils.clob_v1_compat import OrderArgs, OrderType
from domain.ports.clients import AbstractOrderGateway
from utils.createClobClient import ClobClientWrapper


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
        try:
            ot = OrderType[order_type]
        except KeyError as exc:
            raise ValueError(f"Unsupported order_type: {order_type!r}") from exc
        condition_id = (extra or {}).get("condition_id") if extra else None
        result = await self._wrapper.post_order(args, condition_id=condition_id)
        if isinstance(result, dict):
            result.setdefault("idempotency_key", idempotency_key)
            result.setdefault("order_type", str(ot))
        return result

    async def cancel_order(self, order_id: str) -> Any:
        import asyncio

        client = self._wrapper.client
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: client.cancel(order_id=order_id)
        )
