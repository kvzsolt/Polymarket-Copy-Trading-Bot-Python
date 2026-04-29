"""CLOB V1 Compatibility Seam — Central import boundary for py-clob-client v0.34.6.

This module exists to centralize all CLOB V1 (py-clob-client==0.34.6) imports and re-exports
into a single, controlled boundary. When the codebase migrates to py-clob-client-v2, this
module will become the adapter layer — implementations will switch from V1 to V2, but all
external call sites will continue importing from here unchanged.

This pattern limits the blast radius of the V2 migration to this single module,
avoiding the need for scattered, incremental changes across the codebase.

Seam boundary guarantees:
- All active runtime code imports V1 types/constants from THIS module only
- Legacy/test-only code may import directly (e.g., utils/postOrder.py)
- No direct py_clob_client imports outside this module (enforced by tests)

Phase 1 (Current): Pure re-export of V1 types
Phase 2+: Adapter implementations for V2 compatibility
"""

from __future__ import annotations

# Re-export all V1 types that active code depends on
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    MarketOrderArgs,
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
)
from py_clob_client.order_builder.constants import BUY, SELL

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


# ---------------------------------------------------------------------------
# Method-level adapter seams
#
# These functions wrap ClobClient method calls whose signatures change
# between V1 and V2.  Active code calls these adapters instead of the
# raw client methods, so the V2 switch is localised to this module.
#
# Phase 1 (current): delegate to V1 methods unchanged.
# Phase 2+: swap internals to V2 equivalents.
# ---------------------------------------------------------------------------


def derive_api_creds(client: ClobClient) -> object:
    """Derive or create API credentials for the CLOB client.

    V1: client.create_or_derive_api_creds()
    V2: client.create_or_derive_api_key()
    """
    return client.create_or_derive_api_creds()


def submit_signed_order(
    client: ClobClient,
    signed_order: object,
    order_type: OrderType,
) -> object:
    """Submit a pre-signed order to the CLOB.

    V1: client.post_order(signed_order, orderType=order_type)
    V2: client.post_order(signed_order, order_type=order_type)
    """
    return client.post_order(signed_order, orderType=order_type)


def cancel_order_by_id(client: ClobClient, order_id: str) -> object:
    """Cancel an open order by its exchange-assigned ID.

    V1: client.cancel(order_id=order_id)
    V2: client.cancel_order(OrderPayload(order_id))
    """
    return client.cancel(order_id=order_id)
