"""CLOB SDK import boundary — V2-native runtime helpers.

Centralises all py-clob-client-v2 imports and provides thin call-path
helpers for auth, order submission, and cancel.  All V1 dispatch branches
were removed in Phase 4; the runtime is V2-only.

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


def derive_api_creds(client: ClobClient) -> object:
    return client.create_or_derive_api_key()


def submit_signed_order(
    client: ClobClient,
    signed_order: object,
    order_type: OrderType,
) -> object:
    return client.post_order(signed_order, order_type=order_type)


def cancel_order_by_id(client: ClobClient, order_id: str) -> object:
    return client.cancel_order(OrderPayload(orderID=order_id))
