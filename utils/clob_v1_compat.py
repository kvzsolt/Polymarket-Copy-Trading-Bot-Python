"""CLOB Compatibility Seam — Central import boundary for py-clob-client-v2.

This module centralizes all CLOB SDK imports and re-exports into a single,
controlled boundary.  It currently wraps py-clob-client-v2==1.0.0 and provides
method-level adapters so callers can remain agnostic to V1 vs V2 API
differences.

Seam boundary guarantees:
- All active runtime code imports SDK types/constants from THIS module only
- Legacy/test-only code may import directly (e.g., utils/postOrder.py)
- No direct py_clob_client_v2 imports outside this module (enforced by tests)
"""

from __future__ import annotations

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    MarketOrderArgs,
    OrderArgs,
    OrderPayload,
    OrderType,
    PartialCreateOrderOptions,
)
from py_clob_client_v2.order_builder.constants import BUY, SELL

__all__ = [
    "ClobClient",
    "MarketOrderArgs",
    "OrderArgs",
    "OrderType",
    "PartialCreateOrderOptions",
    "BUY",
    "SELL",
    "derive_api_creds",
    "submit_signed_order",
    "cancel_order_by_id",
]


def _is_v2_client(client: object) -> bool:
    """Return True if *client* exposes a V2-shaped interface.

    Detection marker: V2 renamed ``create_or_derive_api_creds`` to
    ``create_or_derive_api_key``.  Any client that has the new name is
    treated as V2-shaped; all others fall back to V1 dispatch.
    """
    return hasattr(client, "create_or_derive_api_key")


def derive_api_creds(client: object) -> object:
    """Derive or create API credentials for the CLOB client.

    V1: client.create_or_derive_api_creds()
    V2: client.create_or_derive_api_key()
    """
    if _is_v2_client(client):
        return client.create_or_derive_api_key()  # type: ignore[union-attr]
    return client.create_or_derive_api_creds()  # type: ignore[union-attr]


def submit_signed_order(
    client: object,
    signed_order: object,
    order_type: OrderType,
) -> object:
    """Submit a pre-signed order to the CLOB.

    V1: client.post_order(signed_order, orderType=order_type)
    V2: client.post_order(signed_order, order_type=order_type)
    """
    if _is_v2_client(client):
        return client.post_order(signed_order, order_type=order_type)  # type: ignore[union-attr]
    return client.post_order(signed_order, orderType=order_type)  # type: ignore[union-attr]


def cancel_order_by_id(client: object, order_id: str) -> object:
    """Cancel an open order by its exchange-assigned ID.

    V1: client.cancel(order_id=order_id)
    V2: client.cancel_order(OrderPayload(orderID=order_id))
    """
    if _is_v2_client(client):
        return client.cancel_order(OrderPayload(orderID=order_id))  # type: ignore[union-attr]
    return client.cancel(order_id=order_id)  # type: ignore[union-attr]
