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

Phase 1: Pure re-export of V1 types
Phase 2a: API delta documentation
Phase 2b: Method adapter seams (hardcoded V1 delegation)
Phase 2c (Current): Package-adaptive seam — duck-type client shape at call time,
    dispatch to V1 or V2 method accordingly.  Runtime stays V1; V2 paths activate
    automatically when a V2-shaped client is detected.
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

# ---------------------------------------------------------------------------
# V2 type availability probe
#
# OrderPayload is introduced in V2.  We attempt to import it eagerly so the
# cancel adapter can use it without a per-call import.  On V1 SDK this will
# fail and _OrderPayload stays None.  Tests that exercise the V2 cancel path
# patch this module-level name directly.
# ---------------------------------------------------------------------------
try:
    from py_clob_client.clob_types import OrderPayload as _OrderPayload  # type: ignore[attr-defined]
except (ImportError, AttributeError):
    _OrderPayload = None  # type: ignore[assignment,misc]

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
# V2 client shape detection
# ---------------------------------------------------------------------------


def _is_v2_client(client: object) -> bool:
    """Return True if *client* exposes a V2-shaped interface.

    Detection marker: V2 renamed ``create_or_derive_api_creds`` to
    ``create_or_derive_api_key``.  Any client that has the new name is
    treated as V2-shaped; all others fall back to V1 dispatch.
    """
    return hasattr(client, "create_or_derive_api_key")


# ---------------------------------------------------------------------------
# Method-level adapter seams (package-adaptive)
#
# Each function probes the client shape at call time via _is_v2_client()
# and dispatches to the V1 or V2 method accordingly.  Active code calls
# these adapters; the V1/V2 distinction is invisible to callers.
# ---------------------------------------------------------------------------


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
    V2: client.cancel_order(OrderPayload(order_id))

    Raises RuntimeError if a V2-shaped client is detected but _OrderPayload
    is not importable (i.e. the V2 SDK is not installed yet).
    """
    if _is_v2_client(client):
        if _OrderPayload is None:
            raise RuntimeError(
                "V2-shaped client detected but py_clob_client.clob_types.OrderPayload "
                "is not importable. Install the V2 SDK to use V2 cancel semantics."
            )
        return client.cancel_order(_OrderPayload(order_id))  # type: ignore[union-attr]
    return client.cancel(order_id=order_id)  # type: ignore[union-attr]
