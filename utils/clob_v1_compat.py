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
]
